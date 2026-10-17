"""Run archive: every repl-calibrate and repl-run invocation becomes one immutable, self-describing directory.

    <archive>/runs/<run id>/
        manifest.json     what the run was: scenario, command, config, harness, builds, tools, hosts, outcome, gaps
        records.jsonl     one record per rep, as the run wrote them
        run.log           the run's log
        calibration.json  the calibration a calibrate run produced, or the one a sweep ran under
        launch/           every distinct cachecannon config the run started, as <sha256>.toml
        result.txt        the run's result table, written when it ended
        REPRODUCE.md      how to run it again on new hosts, from this directory and the objects it names
    <archive>/objects/<aa>/<sha256>.gz   content-addressed blobs: harness snapshots, binaries, build trees
    <archive>/index/                     lookups that save repacking (the source tree of a binary)
    <archive>/notes/<run id>.jsonl       notes added later (supersedes, comments); not part of the sealed run

An object's name is the sha256 of its uncompressed bytes, so a binary's object name is the sha256 the host
reported for it. A run is sealed when it ends: the manifest then lists every file with its sha256, and the files
are made read-only. Nothing in a sealed run changes; a correction is a new run, linked to the old one by a note.

The archive is local: OFFBENCH_ARCHIVE or --archive names it (default ~/offbench-runs; "off" disables it).
It holds host names, addresses and instance ids, so it is not for publishing as it is.
"""

from __future__ import annotations

import fnmatch
import getpass
import gzip
import hashlib
import io
import json
import os
import platform
import re
import secrets
import shlex
import socket
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from . import packing
from .hostfacts import first, run as _run_text

SCHEMA = 1
DEFAULT_ROOT = Path.home() / "offbench-runs"
OFF = ("off", "none", "no", "0", "false")
PACKAGE = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE.parent
SERVER_ROLES = ("primary", "replica")

# Scenarios the archive knows. Bump a version when its method changes, and say what it means here and in the doc.
SCENARIOS = {
    "replica-read-capacity": {
        "version": 1,
        "doc": "docs/replica-read-capacity.md",
        "question": ("At X writes/s and Y reads/s on the primary, how many reads/s can the replica serve before "
                     "it falls out of sync?"),
        "versions": {
            0: "calibrated runs where each build's calibration chose its own pipeline depths and the reference "
               "ran at P16",
            1: "every build's writers and primary readers at one pipeline depth per role in the reference and "
               "every probe; reps whose primary drifts more than 7% off X or Y are invalid; replica readers on "
               "one or more hosts",
        },
    },
}
STEPS = {"repl-calibrate": "calibrate", "repl-run": "sweep"}

# Never put into a harness snapshot, whatever git says: keys and host files. Names with .example are templates.
SECRET_LIKE = ("*.pem", "*.key", "*.p12", "*.pfx", "id_rsa*", "id_ecdsa*", "id_ed25519*", "known_hosts",
               "known_hosts.*", "hosts.json", ".env", "*.env", "credentials*", ".netrc")
# Left out of a harness tree that is not a git checkout (an unpacked snapshot): what .gitignore leaves out.
TREE_JUNK = ("__pycache__", "*.pyc", ".pytest_cache", ".git", "data", "dash/data", "dash/replica/data",
             "vendor/cachecannon/target")
BIN_KEYS = ("sha256", "bytes", "version", "build_id", "toolchain", "text_bytes", "libs")
# Harness files REPRODUCE.md points at; a snapshot records which it has, since older harnesses lack some.
HARNESS_FILES = ("offbench/archive.py", "scripts/host-userdata.sh", "scripts/pintune.sh")

ARCHIVE_MD = """# offbench run archive

One directory per benchmark invocation under runs/, content-addressed blobs under objects/. All of it is plain
files: manifest.json and records.jsonl are JSON, REPRODUCE.md and result.txt are text, and an object is a gzip
file named by the sha256 of what it holds (`gzip -dc objects/<aa>/<sha256>.gz`). Each run's REPRODUCE.md says
how to run it again on new hosts; start there.

The `runs` commands list, show and check runs. They need an offbench checkout that has `offbench/archive.py`.
A run's harness snapshot (`harness.snapshot.object` in its manifest.json) has it, unless the run is older than
the archive; then use the snapshot of the offbench that imported it (`archived_by.snapshot.object`):

    mkdir offbench && gzip -dc objects/<aa>/<sha256>.gz | tar -x -C offbench && cd offbench
    python3 -m offbench runs --archive <path of this directory> list
    python3 -m offbench runs --archive <path of this directory> show <run id>

Without --archive they read $OFFBENCH_ARCHIVE or ~/offbench-runs. Files in a sealed run are read-only: never
edit them. A correction is a new run; link it with `runs note <new> --supersedes <old> --text ...`.
This directory holds host names, addresses and instance ids: do not publish it as it is.
"""


class ArchiveError(RuntimeError):
    pass


def iso(t: Optional[float] = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() if t is None else t))


def resolve_root(arg: Optional[str] = None) -> Optional[Path]:
    """--archive, else OFFBENCH_ARCHIVE, else ~/offbench-runs; None when either says off."""
    v = arg if arg not in (None, "") else os.environ.get("OFFBENCH_ARCHIVE", "")
    if not v:
        return DEFAULT_ROOT
    if v.lower() in OFF:
        return None
    return Path(os.path.expanduser(v))


def is_secret_like(rel: str) -> bool:
    base = rel.rsplit("/", 1)[-1]
    return ".example" not in base and any(fnmatch.fnmatchcase(base, p) for p in SECRET_LIKE)


def write_atomic(path: Path, data) -> None:
    path = Path(path)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data.encode() if isinstance(data, str) else data)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def new_run_id(command: str, label: str, t: Optional[float] = None) -> str:
    """<UTC start>-<command>-<label>-<4 hex>: sorts by start time and says what ran."""
    lab = re.sub(r"[^A-Za-z0-9._-]+", "-", label or "").strip("-.")[:60] or "run"
    return f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(t))}-{command}-{lab}-{secrets.token_hex(2)}"


def _git(root, *args: str, binary: bool = False):
    """git's stdout in root (bytes when binary), or None when git fails."""
    try:
        r = subprocess.run(["git", "-C", str(root), *args], capture_output=True, timeout=120)
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode:
        return None
    return r.stdout if binary else r.stdout.decode(errors="replace").strip()


def strip_credentials(url: Optional[str]) -> Optional[str]:
    return re.sub(r"//[^@/]*@", "//", url) if url else url


def ec2_describe(ids: list, region: str, profile: Optional[str] = None) -> dict:
    """instance id -> {instance_type, ami_id, ami_name, az, placement_group, launched, arch}, from read-only
    `aws ec2 describe-instances` and `describe-images` on the orchestrator. Raises ArchiveError on failure."""
    tail = ["--region", region, "--output", "json"] + (["--profile", profile] if profile else [])

    def aws(*args: str) -> dict:
        try:
            r = subprocess.run(["aws", "ec2", *args, *tail], capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError) as e:
            raise ArchiveError(f"aws ec2 {args[0]}: {e}")
        if r.returncode:
            raise ArchiveError(f"aws ec2 {args[0]}: {(r.stderr or r.stdout).strip()[-200:]}")
        return json.loads(r.stdout or "{}")
    found = {}
    for res in aws("describe-instances", "--instance-ids", *ids).get("Reservations") or []:
        for i in res.get("Instances") or []:
            p = i.get("Placement") or {}
            found[i["InstanceId"]] = {"instance_id": i["InstanceId"], "instance_type": i.get("InstanceType"),
                                      "ami_id": i.get("ImageId"), "az": p.get("AvailabilityZone"),
                                      "placement_group": p.get("GroupName") or None, "launched": i.get("LaunchTime"),
                                      "arch": i.get("Architecture")}
    amis = sorted({v["ami_id"] for v in found.values() if v.get("ami_id")})
    if amis:
        names = {im["ImageId"]: im.get("Name") for im in aws("describe-images", "--image-ids", *amis).get("Images") or []}
        for v in found.values():
            v["ami_name"] = names.get(v.get("ami_id"))
    return found


class Archive:
    def __init__(self, root):
        self.root = Path(root)
        self.runs_dir = self.root / "runs"
        self.objects_dir = self.root / "objects"
        self.index_dir = self.root / "index"
        self.notes_dir = self.root / "notes"

    def ensure(self) -> "Archive":
        for d in (self.runs_dir, self.objects_dir, self.index_dir, self.notes_dir):
            d.mkdir(parents=True, exist_ok=True)
        readme = self.root / "ARCHIVE.md"
        if not readme.exists() or readme.read_text() != ARCHIVE_MD:
            write_atomic(readme, ARCHIVE_MD)
        return self

    # ---- objects ----------------------------------------------------------
    def object_path(self, sha: str) -> Path:
        return self.objects_dir / sha[:2] / f"{sha}.gz"

    def has(self, sha: Optional[str]) -> bool:
        return bool(sha) and self.object_path(sha).exists()

    def tmp(self) -> Path:
        self.ensure()
        fd, p = tempfile.mkstemp(dir=self.objects_dir, prefix=".in-", suffix=".gz")
        os.close(fd)
        return Path(p)

    def put_gz(self, gz: Path, sha: str, verify: bool = True) -> str:
        """Move a gzip file into the store under sha. verify: check first that its uncompressed bytes hash to sha
        (always, for anything that came over the network)."""
        gz = Path(gz)
        if verify:
            got = packing.gunzip_sha256(gz)
            if got != sha:
                gz.unlink(missing_ok=True)
                raise ArchiveError(f"object {sha[:12]}: content hashes to {got[:12]}")
        dest = self.object_path(sha)
        if dest.exists():
            gz.unlink(missing_ok=True)
            return sha
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(gz, 0o444)
        os.replace(gz, dest)
        return sha

    def put_file(self, path) -> str:
        tmp = self.tmp()
        return self.put_gz(tmp, packing.gzip_file(path, tmp)["sha256"], verify=False)

    def put_bytes(self, data: bytes) -> str:
        tmp = self.tmp()
        return self.put_gz(tmp, packing.gzip_bytes(data, tmp)["sha256"], verify=False)

    def read_bytes(self, sha: str) -> bytes:
        with gzip.open(self.object_path(sha), "rb") as f:
            return f.read()

    def check_object(self, sha: str) -> Optional[str]:
        """None when the object is there and intact, else what is wrong."""
        p = self.object_path(sha)
        if not p.exists():
            return f"object {sha[:12]} missing"
        try:
            got = packing.gunzip_sha256(p)
        except (OSError, EOFError) as e:
            return f"object {sha[:12]} unreadable: {e}"
        return None if got == sha else f"object {sha[:12]} content hashes to {got[:12]}"

    # ---- index ------------------------------------------------------------
    def index_get(self, kind: str, key: str) -> Optional[dict]:
        p = self.index_dir / kind / f"{key}.json"
        return json.loads(p.read_text()) if p.exists() else None

    def index_put(self, kind: str, key: str, value: dict) -> None:
        (self.index_dir / kind).mkdir(parents=True, exist_ok=True)
        write_atomic(self.index_dir / kind / f"{key}.json", json.dumps(value, indent=1) + "\n")

    # ---- runs -------------------------------------------------------------
    def run_dirs(self) -> list:
        return sorted(p for p in self.runs_dir.iterdir() if p.is_dir()) if self.runs_dir.exists() else []

    def manifest(self, run_dir: Path) -> dict:
        return json.loads((Path(run_dir) / "manifest.json").read_text())

    def manifests(self) -> list:
        out = []
        for d in self.run_dirs():
            try:
                out.append(self.manifest(d))
            except (OSError, ValueError):
                continue
        return out

    def resolve(self, ref: str) -> Path:
        """A run by id, unique id prefix, label (the newest run with it) or path."""
        if Path(ref).is_dir() and (Path(ref) / "manifest.json").exists():
            return Path(ref)
        dirs = self.run_dirs()
        exact = [d for d in dirs if d.name == ref]
        if exact:
            return exact[0]
        pre = [d for d in dirs if d.name.startswith(ref)]
        if len(pre) == 1:
            return pre[0]
        if len(pre) > 1:
            raise ArchiveError(f"{ref!r} matches {len(pre)} runs: " + ", ".join(d.name for d in pre[:8]))
        labelled = [d for d in dirs if (self.manifest(d).get("label") == ref)]
        if labelled:
            return labelled[-1]
        raise ArchiveError(f"no run {ref!r} in {self.runs_dir}")

    # ---- notes ------------------------------------------------------------
    def notes(self, run_id: str) -> list:
        p = self.notes_dir / f"{run_id}.jsonl"
        return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []

    def add_note(self, run_id: str, note: dict) -> None:
        self.ensure()
        with (self.notes_dir / f"{run_id}.jsonl").open("a") as f:
            f.write(json.dumps(dict(note, ts=iso(), by=getpass.getuser())) + "\n")


# ----------------------------------------------------------------- harness ----

def _gitignore(root: Path) -> tuple:
    pats = []
    for line in (root / ".gitignore").read_text().splitlines() if (root / ".gitignore").exists() else []:
        line = line.strip()
        if line and not line.startswith(("#", "!")):
            pats.append(line.strip("/"))
    return tuple(pats)


def harness_files(root: Path) -> tuple:
    """(relative paths, how they were listed). In a git checkout: tracked plus untracked files git does not ignore,
    read from the working tree. Elsewhere (an unpacked snapshot): every file but what .gitignore leaves out."""
    top = _git(root, "rev-parse", "--show-toplevel")
    if top and Path(top).resolve() == root:
        raw = _git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard", binary=True) or b""
        names = sorted({n for n in raw.decode(errors="surrogateescape").split("\0") if n})
        return [n for n in names if os.path.lexists(root / n)], "git"
    entries, _ = packing.walk(root, TREE_JUNK + _gitignore(root))
    return [rel for rel, _ in entries], "tree"


def vendored_cachecannon(hashes: dict, root: Path) -> dict:
    d = root / "vendor" / "cachecannon"
    vendor = (d / "VENDOR.md").read_text() if (d / "VENDOR.md").exists() else ""
    cargo = (d / "Cargo.toml").read_text() if (d / "Cargo.toml").exists() else ""
    pinned = re.search(r"Pinned commit:\s*([0-9a-f]{7,40})", vendor)
    upstream = re.search(r"Source:\s*(\S+)", vendor)
    version = re.search(r'^version\s*=\s*"([^"]+)"', cargo, re.M)
    files = {n: h for n, h in hashes.items() if n.startswith("vendor/cachecannon/")}
    tree = hashlib.sha256("".join(f"{n}\0{h}\n" for n, h in sorted(files.items())).encode()).hexdigest()
    return {"source": "vendor/cachecannon in the harness snapshot", "upstream": upstream.group(1) if upstream else None,
            "upstream_commit": pinned.group(1) if pinned else None, "version": version.group(1) if version else None,
            "cargo_lock_sha256": hashes.get("vendor/cachecannon/Cargo.lock"), "source_files": len(files),
            "source_tree_sha256": tree if files else None}


def harness(root: Path, archive: Archive) -> dict:
    """Snapshot the harness tree that is running into the archive, and say where it came from.

    The snapshot is a deterministic tar of the files as they are on disk (uncommitted edits and untracked files
    included, secret-like files never) plus HARNESS.json: the commit, branch, dirty flag and every file's sha256.
    Unpacking it gives a tree that, snapshotted again unchanged, yields the same object."""
    root = Path(root).resolve()
    names, how = harness_files(root)
    skipped = [n for n in names if is_secret_like(n)]
    names = [n for n in names if not is_secret_like(n) and n != "HARNESS.json"]
    hashes = {n: packing.sha256_entry(root / n) for n in names}
    info: dict = {"name": "offbench", "root": str(root), "listed_by": how}
    entries = [(n, root / n, None) for n in names]
    if how == "git":
        changed = _git(root, "diff", "HEAD", "--name-only") or ""
        untracked = _git(root, "ls-files", "--others", "--exclude-standard") or ""
        branches = _git(root, "branch", "-r", "--contains", "HEAD") or ""
        info.update(commit=_git(root, "rev-parse", "HEAD"), branch=_git(root, "rev-parse", "--abbrev-ref", "HEAD"),
                    describe=_git(root, "describe", "--always", "--dirty"), dirty=bool(changed),
                    changed=changed.splitlines(), untracked=[u for u in untracked.splitlines() if not is_secret_like(u)],
                    remote=strip_credentials(_git(root, "remote", "get-url", "origin")),
                    # From this checkout's remote-tracking refs, which can be behind the remote.
                    remote_branches_with_commit=[b.strip() for b in branches.splitlines() if "->" not in b])
        if changed:
            info["diff_object"] = archive.put_bytes(_git(root, "diff", "HEAD", "--binary", binary=True) or b"")
        stamp = {"name": "offbench", "commit": info["commit"], "branch": info["branch"], "dirty": info["dirty"],
                 "remote": info["remote"], "files": hashes}
        entries.append(("HARNESS.json", None, (json.dumps(stamp, indent=1, sort_keys=True) + "\n").encode()))
    elif (root / "HARNESS.json").exists():
        prev = json.loads((root / "HARNESS.json").read_text())
        pf = prev.get("files") or {}
        info.update(commit=prev.get("commit"), branch=prev.get("branch"), dirty=prev.get("dirty"),
                    remote=prev.get("remote"), unpacked_snapshot=True,
                    since_snapshot={"changed": sorted(n for n in names if n in pf and pf[n] != hashes[n]),
                                    "added": sorted(n for n in names if n not in pf),
                                    "missing": sorted(n for n in pf if n not in hashes)})
        entries.append(("HARNESS.json", root / "HARNESS.json", None))
    else:
        info.update(commit=None, dirty=None)
    tmp = archive.tmp()
    r = packing.tar_gz(entries, tmp)
    archive.put_gz(tmp, r["sha256"], verify=False)
    info["snapshot"] = {"object": r["sha256"], "files": r["files"], "bytes": r["bytes"], "gz_bytes": r["gz_bytes"],
                        "left_out_secret_like": skipped}
    info["code"] = {n: h for n, h in hashes.items() if n.startswith("offbench/") and n.endswith(".py")}
    info["docs"] = {n: h for n, h in hashes.items() if n.endswith(".md") and not n.startswith("vendor/")}
    info["has"] = {p: p in hashes for p in HARNESS_FILES}
    info["cachecannon_source"] = vendored_cachecannon(hashes, root)
    return info


def harness_from_commit(repo: Path, commit: str, archive: Archive) -> dict:
    """A snapshot of a harness commit as a clean checkout would have it (for runs recorded before the archive)."""
    full = _git(repo, "rev-parse", "--verify", f"{commit}^{{commit}}")
    if not full:
        raise ArchiveError(f"{repo}: no commit {commit}")
    with tempfile.TemporaryDirectory(dir=archive.objects_dir) as tmpdir:
        tree = Path(tmpdir) / "tree"
        tree.mkdir()
        tar = _git(repo, "archive", "--format=tar", full, binary=True)
        if tar is None:
            raise ArchiveError(f"{repo}: git archive {commit} failed")
        safe = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        with tarfile.open(fileobj=io.BytesIO(tar)) as t:
            for m in t.getmembers():
                if m.isfile() or m.isdir() or m.issym():
                    t.extract(m, tree, set_attrs=False, **safe)
        names = [rel for rel, _ in packing.walk(tree)[0]]
        skipped = [n for n in names if is_secret_like(n)]
        names = [n for n in names if not is_secret_like(n)]
        hashes = {n: packing.sha256_entry(tree / n) for n in names}
        stamp = {"name": "offbench", "commit": full, "branch": None, "dirty": False,
                 "remote": strip_credentials(_git(repo, "remote", "get-url", "origin")), "files": hashes}
        entries = [(n, tree / n, None) for n in names]
        entries.append(("HARNESS.json", None, (json.dumps(stamp, indent=1, sort_keys=True) + "\n").encode()))
        tmp = archive.tmp()
        r = packing.tar_gz(entries, tmp)
        archive.put_gz(tmp, r["sha256"], verify=False)
        refs = (_git(repo, "for-each-ref", "--contains", full, "--format=%(refname)") or "").splitlines()
        return {"name": "offbench", "commit": full, "dirty": False, "listed_by": "git archive", "remote": stamp["remote"],
                "refs_with_commit": refs,
                # From this clone's remote-tracking refs, which can be behind the remote.
                "remote_branches_with_commit": [r[len("refs/remotes/"):] for r in refs if r.startswith("refs/remotes/")],
                "snapshot": {"object": r["sha256"], "files": r["files"], "bytes": r["bytes"], "gz_bytes": r["gz_bytes"],
                             "left_out_secret_like": skipped},
                "code": {n: h for n, h in hashes.items() if n.startswith("offbench/") and n.endswith(".py")},
                "docs": {n: h for n, h in hashes.items() if n.endswith(".md") and not n.startswith("vendor/")},
                "has": {p: p in hashes for p in HARNESS_FILES},
                "cachecannon_source": vendored_cachecannon(hashes, tree)}


# --------------------------------------------------------- run environment ----

def orchestrator_facts() -> dict:
    return {"hostname": socket.gethostname(), "user": getpass.getuser(), "python": platform.python_version(),
            "platform": platform.platform(), "ssh": first(_run_text(["ssh", "-V"])),
            "rsync": first(_run_text(["rsync", "--version"])), "git": first(_run_text(["git", "--version"]))}


def env_facts() -> dict:
    """The environment variables offbench reads; key and known-hosts by file name only."""
    out = {}
    for k, v in sorted(os.environ.items()):
        if not (k.startswith("OFFBENCH_") or k in ("HOSTS_LEASE", "HOSTS_DIR")):
            continue
        if k in ("OFFBENCH_KEY", "OFFBENCH_KNOWN_HOSTS"):
            out[k] = {"file": Path(v).name}
        elif k == "HOSTS_LEASE":
            out[k] = "(set)"
        else:
            out[k] = v
    return out


def constants() -> dict:
    """Fixed settings in the harness code that shape every rep (the snapshot has them too; here for answers)."""
    from . import cell, repl, replcap

    def pick(mod, names):
        return {n: getattr(mod, n) for n in names if hasattr(mod, n)}
    return {"cell": pick(cell, ("PORT", "PIN_HK_CPUS", "PIN_MAIN_CPU", "PIN_IRQ_CPUS", "PIN_GEN_CPUS")),
            "repl": pick(repl, ("WRITER_CPUS", "READER_CPUS", "TRI_CPUS", "TRI_THREADS", "PAIR_CPUS", "PAIR_THREADS",
                                "SOLO_CPUS", "SOLO_THREADS", "DRIFT_TOL")),
            "replcap": pick(replcap, ("CAL_TOL", "AIM", "CONFIRM_ROUNDS", "MIN_CONNS", "APART", "K_SLOPE", "FLAT"))}


def scenario_for(command: str, calibrated: bool, version: Optional[int] = None) -> dict:
    """The scenario record of a run: name, question, defining doc, step, and the method version with what it means
    (the current version unless given)."""
    base = SCENARIOS["replica-read-capacity"]
    s = {k: v for k, v in base.items() if k != "versions"}
    s["name"] = "replica-read-capacity"
    s["version"] = base["version"] if version is None else version
    s["method"] = base["versions"].get(s["version"])
    s["step"] = STEPS[command] if (command != "repl-run" or calibrated) else "run without a calibration"
    return s


# --------------------------------------------------------------------- run ----

def _bin_entry(bf: dict) -> dict:
    return {k: bf.get(k) for k in BIN_KEYS}


class Run:
    """One archived invocation, from begin() to seal()."""

    def __init__(self, archive: Archive, run_id: str, manifest: Optional[dict] = None):
        self.archive, self.id = archive, run_id
        self.dir = archive.runs_dir / run_id
        self.records = self.dir / "records.jsonl"
        self.log_path = self.dir / "run.log"
        self.calibration = self.dir / "calibration.json"
        self.launch_dir = self.dir / "launch"
        self.manifest: dict = manifest or {}
        self._sources: dict = {}     # sha256 -> (role, absolute path on that host, label): where to fetch a binary
        self._started = time.time()

    @classmethod
    def begin(cls, archive: Archive, *, command: str, label: str, argv: list, args: dict, scenario: dict,
              config: Optional[dict] = None, tags=(), harness_root: Path = REPO_ROOT,
              log: Callable[[str], None] = print) -> "Run":
        archive.ensure()
        for _ in range(8):
            rid = new_run_id(command, label)
            try:
                (archive.runs_dir / rid).mkdir()
                break
            except FileExistsError:
                continue
        else:
            raise ArchiveError("could not pick a free run id")
        run = cls(archive, rid)
        run.launch_dir.mkdir()
        run.manifest = {
            "kind": "offbench-run", "schema": SCHEMA, "id": rid, "label": label, "status": "running",
            "started": iso(run._started), "ended": None, "tags": sorted(set(tags)),
            "scenario": scenario,
            "command": {"name": command, "argv": list(argv), "cwd": os.getcwd(), "args": args, "env": env_facts()},
            "config": config or {}, "constants": constants(), "orchestrator": orchestrator_facts(),
            "inputs": {}, "gaps": [], "warnings": [],
        }
        try:
            h = run.manifest["harness"] = harness(harness_root, archive)
            scenario["doc_sha256"] = h["docs"].get(scenario.get("doc"))
            if h.get("dirty"):
                run.warn(f"harness has uncommitted changes in {', '.join(h['changed'][:6])}: the snapshot holds them, "
                         "the commit does not")
            if h.get("commit") and h.get("listed_by") == "git" and not h.get("remote_branches_with_commit"):
                run.warn("harness commit is on no remote-tracking branch of this checkout: the snapshot is the "
                         "only copy known to the archive")
        except Exception as e:     # an archive problem must not stop the benchmark
            run.manifest["harness"] = None
            run.gap(f"harness snapshot failed: {type(e).__name__}: {e}")
        run.save()
        log(f"archive: run {rid} at {run.dir}")
        return run

    def gap(self, text: str) -> None:
        """Something the archive could not capture, in words a reader can act on."""
        if text not in self.manifest.setdefault("gaps", []):
            self.manifest["gaps"].append(text)

    def warn(self, text: str) -> None:
        if text not in self.manifest.setdefault("warnings", []):
            self.manifest["warnings"].append(text)

    def save(self) -> None:
        write_atomic(self.dir / "manifest.json", json.dumps(self.manifest, indent=1) + "\n")

    # ---- inputs -----------------------------------------------------------
    def put_input_calibration(self, path: Path, doc: dict) -> None:
        """The calibration a sweep runs under, copied in, with the archived run that produced it if known."""
        data = Path(path).read_bytes()
        write_atomic(self.calibration, data)
        produced_by = doc.get("archive_run") or find_calibration_run(self.archive, doc.get("id"))
        self.manifest["inputs"]["calibration"] = {
            "id": doc.get("id"), "file": "calibration.json", "sha256": hashlib.sha256(data).hexdigest(),
            "from": str(Path(path).resolve()), "archive_run": produced_by}
        if not produced_by:
            self.gap(f"calibration {doc.get('id')} was made before the archive (or elsewhere): its own run is not "
                     "archived, but this run's calibration.json is the exact file used")
        self.save()

    def put_calibration(self, doc: dict) -> None:
        write_atomic(self.calibration, json.dumps(doc, indent=1) + "\n")

    # ---- hosts, builds and tools --------------------------------------------
    def capture(self, ssh, roles: dict, bins: list, rtt: dict, log: Callable[[str], None] = print,
                fetch: bool = True, ec2: Optional[dict] = None) -> None:
        """Ask every host what it is and what it will run, then archive any binary and build tree not yet in the
        store. ec2: role -> {instance_id, region} from the host registry, for the AMI, zone and placement group,
        which a host does not know locally. Failures become gaps; the benchmark goes on."""
        t0 = time.time()
        m = self.manifest
        servers = sorted({b.server for b in bins})
        waits = {}
        for role, h in roles.items():
            srv = role in SERVER_ROLES
            try:
                waits[role] = ssh.start(h, {"action": "facts", "servers": servers if srv else [], "cli": srv,
                                            "gen": not srv})
            except Exception as e:
                self.gap(f"{h.name} ({role}): host facts not collected: {e}")
        facts: dict = {}
        for role, w in waits.items():
            try:
                facts[role] = w(timeout=300)
            except Exception as e:
                self.gap(f"{roles[role].name} ({role}): host facts not collected: {e}")
        m["rig"] = {"rtt_ms": rtt, "roles": {}}
        for role, h in roles.items():
            f = {k: v for k, v in (facts.get(role) or {}).items() if k not in ("servers", "cachecannon", "cli")}
            m["rig"]["roles"][role] = {"host": h.name, "ssh_user": h.ssh.split("@", 1)[0] if "@" in h.ssh else None,
                                       "private_ip": h.ip, "facts": f or None}
            if f and role in SERVER_ROLES and not (f.get("pintune") or {}).get("applied"):
                self.warn(f"{h.name} ({role}): pintune state not found at start")
        self._ec2(roles, ec2 or {})
        srv_roles = [r for r in roles if r in SERVER_ROLES]
        m["builds"] = []
        for b in bins:
            entry = {"name": b.name, "commit": b.commit, "server_path": b.server, "replica_args": b.replica_args,
                     "primary_args": b.primary_args, "share": b.share, "binaries": [], "source": None}
            groups: dict = {}
            for role in srv_roles:
                if role not in facts:
                    continue
                bf = facts[role]["servers"][servers.index(b.server)]
                if not bf.get("exists"):
                    self.gap(f"build {b.name}: no binary at {b.server} on {roles[role].name} ({role})")
                    continue
                g = groups.get(bf["sha256"])
                if g is None:
                    g = groups[bf["sha256"]] = dict(_bin_entry(bf), roles=[], build=bf.get("build"))
                    self._sources.setdefault(bf["sha256"], (role, bf["realpath"], f"valkey-server {b.name}"))
                g["roles"].append(role)
            entry["binaries"] = list(groups.values())
            if len(groups) > 1:
                self.warn(f"build {b.name}: the server hosts run different binaries for {b.commit[:12]} "
                          f"({', '.join(s[:12] for s in groups)}); separate builds differ in their build id, so "
                          "compare text_bytes and make settings")
            m["builds"].append(entry)
        m["tools"] = {"cachecannon": dict((m.get("harness") or {}).get("cachecannon_source") or {}),
                      "valkey-cli": {"source": "any valkey build; only the control connection uses it"}}
        for key, tool, where in (("cachecannon", "cachecannon", [r for r in roles if r not in SERVER_ROLES]),
                                 ("cli", "valkey-cli", srv_roles)):
            groups = {}
            for role in where:
                bf = (facts.get(role) or {}).get(key)
                if not bf or not bf.get("exists"):
                    continue
                g = groups.setdefault(bf["sha256"], dict(_bin_entry(bf), roles=[]))
                g["roles"].append(role)
                self._sources.setdefault(bf["sha256"], (role, bf["realpath"], tool))
            m["tools"][tool]["binaries"] = list(groups.values())
        if fetch:
            self._fetch(ssh, roles, log)
        else:
            self.gap("binaries were not archived (--no-archive-binaries): only their sha256 identifies them")
        self.save()
        log(f"archive: facts from {len(facts)}/{len(roles)} hosts, {len(self._sources)} binaries "
            f"({sum(self.archive.has(s) for s in self._sources)} in the store) in {time.time() - t0:.0f} s")

    def _ec2(self, roles: dict, ec2: dict) -> None:
        if not ec2:
            self.gap("the host registry has no EC2 instance ids: the hosts' AMI, zone and placement group were not "
                     "recorded")
            return
        by_region: dict = {}
        for e in ec2.values():
            by_region.setdefault(e["region"], set()).add(e["instance_id"])
        found: dict = {}
        try:
            for region, ids in by_region.items():
                found.update(ec2_describe(sorted(ids), region, os.environ.get("OFFBENCH_AWS_PROFILE")))
        except Exception as e:
            self.gap(f"EC2 describe failed ({e}): the hosts' AMI, zone and placement group were not recorded; set "
                     "OFFBENCH_AWS_PROFILE to a profile that can describe them")
            return
        for role, e in ec2.items():
            got = found.get(e["instance_id"])
            entry = self.manifest["rig"]["roles"].get(role)
            if got is None or entry is None:
                self.gap(f"{roles[role].name} ({role}): EC2 instance {e['instance_id']} not found")
                continue
            entry["ec2"] = got
            seen = ((entry.get("facts") or {}).get("instance") or {}).get("instance_id")
            if seen and seen != e["instance_id"]:
                self.warn(f"{roles[role].name} ({role}): the host says it is {seen}, the registry {e['instance_id']}")

    def _fetch(self, ssh, roles: dict, log: Callable[[str], None]) -> None:
        want: dict = {}      # role -> [(pack item, what it is, callback(result))]

        def ask(role, item, what, done):
            want.setdefault(role, []).append((item, what, done))
        for sha, (role, path, what) in self._sources.items():
            if not self.archive.has(sha):
                ask(role, {"kind": "file", "path": path, "level": 1}, what, lambda r: None)
        for entry in self.manifest["builds"]:
            for g in entry["binaries"]:
                known = self.archive.index_get("source-of-binary", g["sha256"])
                if known and self.archive.has(known["tree"]):
                    entry["source"] = known
                    break
            else:
                have = [(role, g) for g in entry["binaries"] for role in g["roles"]
                        if (g.get("build") or {}).get("source_present")]
                if not have:
                    self.gap(f"build {entry['name']}: no host has the source tree it was built from (binary only); "
                             f"rebuild from valkey commit {entry['commit']}")
                    continue
                role, g = have[0]

                def done(r, entry=entry, g=g, role=role):
                    src = {"tree": r["sha256"], "files": r["files"], "bytes": r["bytes"],
                           "left_out_over_8mb": r.get("skipped") or [], "from_role": role,
                           "from_path": g["build"]["root"], "packed": iso()}
                    entry["source"] = src
                    for gg in entry["binaries"]:
                        if gg["build"] and gg["build"].get("source_present"):
                            self.archive.index_put("source-of-binary", gg["sha256"], src)
                ask(role, {"kind": "tree", "path": g["build"]["root"], "exclude": "valkey-build", "max_bytes": 8 << 20},
                    f"source tree {entry['name']}", done)
        waits = {}
        for role, items in want.items():
            try:
                waits[role] = ssh.start(roles[role], {"action": "pack", "items": [i for i, _, _ in items]})
            except Exception as e:
                self.gap(f"{roles[role].name} ({role}): could not pack binaries or sources: {e}")
        for role, w in waits.items():
            try:
                got = w(timeout=1200)["items"]
            except Exception as e:
                self.gap(f"{roles[role].name} ({role}): could not pack binaries or sources: {e}")
                continue
            for (item, what, done), r in zip(want[role], got):
                tmp = self.archive.tmp()
                try:
                    ssh.fetch(roles[role], r["gz"], tmp)
                    self.archive.put_gz(tmp, r["sha256"])
                except Exception as e:
                    tmp.unlink(missing_ok=True)
                    self.gap(f"{what}: not archived ({roles[role].name}): {e}")
                    continue
                done(r)
                log(f"archive: {what}: {r['bytes'] / 1e6:.1f} MB as {r['gz_bytes'] / 1e6:.1f} MB from {roles[role].name}")

    def _recheck(self, ssh, roles: dict, bins: list) -> None:
        """The server binaries at the end are the ones described at the start."""
        servers = sorted({b.server for b in bins})
        start = {}
        for entry in self.manifest.get("builds") or []:
            for g in entry["binaries"]:
                for role in g["roles"]:
                    start[(role, entry["server_path"])] = g["sha256"]
        for role in [r for r in roles if r in SERVER_ROLES]:
            try:
                got = ssh.call(roles[role], {"action": "hash", "paths": servers}, timeout=120)["binaries"]
            except Exception as e:
                self.gap(f"{roles[role].name} ({role}): binaries not rechecked at the end: {e}")
                continue
            for s, bf in zip(servers, got):
                was = start.get((role, s))
                if was and bf.get("sha256") != was:
                    self.warn(f"{roles[role].name} ({role}): {s} changed during the run "
                              f"({was[:12]} -> {(bf.get('sha256') or 'missing')[:12]})")

    # ---- the end ------------------------------------------------------------
    def seal(self, status: str, exit_code: Optional[int] = None, error: Optional[str] = None, *, ssh=None,
             roles: Optional[dict] = None, bins: Optional[list] = None, result: Optional[str] = None,
             outputs: Optional[dict] = None, log: Callable[[str], None] = print, ended: Optional[str] = None,
             seconds: Optional[int] = None) -> None:
        """ended/seconds: when the run itself ended and how long it took, for a run sealed later (an import)."""
        m = self.manifest
        if ssh is not None and roles and bins:
            self._recheck(ssh, roles, bins)
        recs = []
        if self.records.exists():
            recs = [json.loads(l) for l in self.records.read_text().splitlines() if l.strip()]
        m["outputs"] = {"records": len(recs), "valid": sum(bool(r.get("valid")) for r in recs),
                        "errors": sum("error" in r for r in recs), **(m.get("outputs") or {}), **(outputs or {})}
        if result:
            write_atomic(self.dir / "result.txt", result.rstrip("\n") + "\n")
        for entry in m.get("builds") or []:
            for g in entry["binaries"]:
                g["archived"] = self.archive.has(g["sha256"])
        for tool in (m.get("tools") or {}).values():
            for g in tool.get("binaries") or []:
                g["archived"] = self.archive.has(g["sha256"])
        m.update(status=status, ended=ended or iso(), exit_code=exit_code, error=error,
                 seconds=seconds if seconds is not None else round(time.time() - self._started))
        log(f"archive: sealing {self.id} ({status})")
        detach = getattr(log, "detach", None)
        if detach:
            detach(self.log_path)      # run.log is hashed and frozen below; later lines go to the console only
        from .reproduce import reproduce_md
        write_atomic(self.dir / "REPRODUCE.md", reproduce_md(m, self.dir, self.archive))
        m["files"] = {}
        for p in sorted(self.dir.rglob("*")):
            if p.is_file() and p.name != "manifest.json":
                m["files"][p.relative_to(self.dir).as_posix()] = {"sha256": packing.sha256_file(p),
                                                                    "bytes": p.stat().st_size}
        self.save()
        for p in self.dir.rglob("*"):
            if p.is_file():
                os.chmod(p, 0o444)
        for p in [self.launch_dir, self.dir]:
            if p.exists():
                os.chmod(p, 0o555)
        log(f"archive: sealed {self.id} ({status}); questions: python3 -m offbench runs show {self.id}")


def find_calibration_run(archive: Archive, cal_id: Optional[str]) -> Optional[str]:
    if not cal_id:
        return None
    for m in archive.manifests():
        if (m.get("outputs") or {}).get("calibration_id") == cal_id and m.get("status") != "running":
            return m["id"]
    return None


# ----------------------------------------------------------- reading runs ----

def objects_of(m: dict) -> list:
    """(sha256, what) for every object a manifest relies on."""
    out = []
    h = m.get("harness") or {}
    if (h.get("snapshot") or {}).get("object"):
        out.append((h["snapshot"]["object"], "harness snapshot"))
    if h.get("diff_object"):
        out.append((h["diff_object"], "harness uncommitted diff"))
    own = ((m.get("archived_by") or {}).get("snapshot") or {}).get("object")
    if own and own != (h.get("snapshot") or {}).get("object"):
        out.append((own, "offbench that imported the run"))
    drv = (m.get("command") or {}).get("driver") or {}
    if drv.get("object"):
        out.append((drv["object"], "driver script"))
    ud = (m.get("rig") or {}).get("user_data") or {}
    if ud.get("object"):
        out.append((ud["object"], "host user data"))
    for role, r in ((m.get("rig") or {}).get("roles") or {}).items():
        ud = ((r.get("declared") or {}).get("user_data")) or {}
        if isinstance(ud, dict) and ud.get("object") and (ud["object"], "host user data") not in out:
            out.append((ud["object"], "host user data"))
    for b in m.get("builds") or []:
        for g in b.get("binaries") or []:
            if g.get("archived"):
                out.append((g["sha256"], f"valkey-server {b['name']}"))
        if (b.get("source") or {}).get("tree"):
            out.append((b["source"]["tree"], f"source tree {b['name']}"))
    for name, t in (m.get("tools") or {}).items():
        for g in t.get("binaries") or []:
            if g.get("archived"):
                out.append((g["sha256"], name))
    return out


def verify(archive: Archive, run_dir: Path, objects: bool = True) -> list:
    """What is wrong with a run: unsealed, files changed or missing since sealing, objects missing or corrupt."""
    m = archive.manifest(run_dir)
    problems = []
    if m.get("status") == "running":
        problems.append("not sealed: still running, or its process died before sealing")
    files = m.get("files") or {}
    for rel, f in files.items():
        p = Path(run_dir) / rel
        if not p.exists():
            problems.append(f"{rel}: missing")
        elif packing.sha256_file(p) != f["sha256"]:
            problems.append(f"{rel}: changed since the run was sealed")
    if files:
        for p in Path(run_dir).rglob("*"):
            rel = p.relative_to(run_dir).as_posix()
            if p.is_file() and p.name != "manifest.json" and rel not in files:
                problems.append(f"{rel}: added after sealing")
    if objects:
        for sha, what in objects_of(m):
            err = archive.check_object(sha)
            if err:
                problems.append(f"{what}: {err}")
    return problems


def _cores(m: dict) -> list:
    return list((m.get("config") or {}).get("cores") or [])


def _short(sha: Optional[str], n: int = 12) -> str:
    return sha[:n] if sha else "?"


def matches(m: dict, *, scenario=None, step=None, commit=None, binary=None, cores=None, tag=None, status=None,
            label=None, since=None) -> bool:
    sc = m.get("scenario") or {}
    builds = m.get("builds") or []
    bins = [g.get("sha256") or "" for b in builds for g in b.get("binaries") or []]
    bins += [g.get("sha256") or "" for t in (m.get("tools") or {}).values() for g in t.get("binaries") or []]
    return ((scenario is None or sc.get("name") == scenario) and (step is None or sc.get("step") == step)
            and (commit is None or any((b.get("commit") or "").startswith(commit) for b in builds)
                 or ((m.get("harness") or {}).get("commit") or "").startswith(commit))
            and (binary is None or any(s.startswith(binary) for s in bins))
            and (cores is None or cores in _cores(m)) and (tag is None or tag in (m.get("tags") or []))
            and (status is None or m.get("status") == status) and (label is None or label in (m.get("label") or ""))
            and (since is None or (m.get("started") or "") >= since))


def list_line(m: dict) -> str:
    o = m.get("outputs") or {}
    builds = " ".join(f"{b['name']}@{(b.get('commit') or '?')[:9]}" for b in m.get("builds") or [])
    cores = ",".join(str(c) for c in _cores(m)) or "?"
    return (f"{m['id']}  {m.get('status', '?'):<9} {(m.get('scenario') or {}).get('step', '?'):<9} "
            f"cores {cores:<5} {o.get('valid', 0)}/{o.get('records', 0)} valid  {builds}")


def _role_line(role: str, r: dict) -> str:
    f = r.get("facts") or {}
    if not f:
        d = r.get("declared") or {}
        if d:
            return (f"  {role:<8} {r.get('host')}: declared {d.get('instance_type') or '?'} {d.get('instance_id') or ''} "
                    f"{d.get('az') or '?'} pg {d.get('placement_group') or '-'}; {d.get('ami_id') or '?'} "
                    f"{d.get('ami_name') or ''}; user {d.get('user') or '?'}")
        return f"  {role:<8} {r.get('host')}: not recorded"
    i = {**(r.get("ec2") or {}), **{k: v for k, v in (f.get("instance") or {}).items() if v}}
    o = f.get("os") or {}
    pt = f.get("pintune") or {}
    clock = (f.get("clock") or {}).get("System time", "?")
    return (f"  {role:<8} {r.get('host')}: {i.get('instance_type') or '?'} {i.get('instance_id') or ''} "
            f"{i.get('az') or '?'} pg {i.get('placement_group') or '-'}; {i.get('ami_id') or '?'} "
            f"{o.get('release') or o.get('pretty') or '?'}; kernel {f.get('kernel')}; {f.get('cpu', {}).get('Model name', '?')} "
            f"x{f.get('cpu', {}).get('count')}; python {f.get('python')}; pintune {'yes' if pt.get('applied') else 'no'}; "
            f"clock {clock}")


def _binary_lines(g: dict, indent: str) -> list:
    tc = "; ".join(g.get("toolchain") or []) or "toolchain not recorded"
    out = [f"{indent}binary {_short(g.get('sha256'), 16)} ({(g.get('bytes') or 0) / 1e6:.1f} MB, .text "
           f"{g.get('text_bytes') or '?'}) on {'+'.join(g.get('roles') or [])}, "
           f"{'archived' if g.get('archived') else 'NOT archived'}",
           f"{indent}{g.get('version') or 'version not recorded'}", f"{indent}{tc}"]
    build = g.get("build") or {}
    if build.get("make_settings"):
        out.append(f"{indent}make settings: " + " ".join(l.strip() for l in build["make_settings"].splitlines() if l.strip()))
    return out


def show(m: dict, notes: list = (), run_dir: Optional[Path] = None) -> str:
    sc, h, c, o = m.get("scenario") or {}, m.get("harness") or {}, m.get("config") or {}, m.get("outputs") or {}
    cmd = m.get("command") or {}
    L = [f"run       {m['id']}  {m.get('status')}  {m.get('started')} .. {m.get('ended') or '(running)'}"
         + (f"  ({m['seconds'] // 60} min)" if m.get("seconds") else "")]
    L.append(f"scenario  {sc.get('name')} " + (f"v{sc['version']}" if sc.get("version") is not None else "version unknown")
             + f", step {sc.get('step')}: {sc.get('question')}")
    if sc.get("method"):
        L.append(f"          method v{sc['version']}: {sc['method']}"
                 + (" (version given at import)" if sc.get("version_given_at_import") else ""))
    L.append(f"          defined in {sc.get('doc')} (sha256 {_short(sc.get('doc_sha256'))}) of the harness snapshot")
    if cmd.get("argv") is not None:
        L.append(f"command   python3 -m offbench {shlex.join(cmd['argv'])}")
        if cmd.get("cwd"):
            L.append(f"          cwd {cmd['cwd']}")
        if cmd.get("source"):
            L.append(f"          from {cmd['source']} (not recorded by the run)")
        if (cmd.get("driver") or {}).get("object"):
            L.append(f"          driver {cmd['driver']['path']}, archived as object {_short(cmd['driver']['object'], 16)}")
    if h:
        state = "dirty" if h.get("dirty") else "clean" if h.get("dirty") is False else "state unknown"
        snap = h.get("snapshot") or {}
        L.append(f"harness   offbench {h.get('commit') or 'commit unknown'} ("
                 + (f"branch {h['branch']}, " if h.get("branch") else "") + f"{state}) {h.get('remote') or ''}")
        L.append(f"          snapshot {_short(snap.get('object'), 16)} ({snap.get('files')} files)"
                 + (f"; evidence: {h['evidence']}" if h.get("evidence") else ""))
    else:
        L.append("harness   not recorded")
    ab = m.get("archived_by") or {}
    if ab:
        L.append(f"imported  by offbench {ab.get('describe') or ab.get('commit') or '?'}, snapshot "
                 f"{_short((ab.get('snapshot') or {}).get('object'), 16)}")
    if c:
        bits = [f"cores {','.join(str(x) for x in c.get('cores') or [])}"]
        if c.get("replica_reader_conns") is not None:
            bits.append(f"replica readers {','.join(str(x) for x in c['replica_reader_conns'])} x "
                        f"P{','.join(str(x) for x in c.get('replica_reader_pipelines') or [])}")
        for k, lab in (("reps", "reps"), ("window_s", "window s")):
            if c.get(k) is not None:
                bits.append(f"{lab} {c[k]}")
        t = c.get("targets") or {}
        if isinstance(t, dict) and t.get("write_kcmd_s"):
            bits.append(f"targets X {t['write_kcmd_s']:.0f}k writes/s, Y {t['read_kcmd_s']:.0f}k reads/s")
        L.append("config    " + "; ".join(bits))
        for name, ld in (c.get("loads") or {}).items():
            words = {"writers": "writers", "primary_readers": "primary readers", "replica_args": "replica",
                     "primary_args": "primary"}
            parts = [f"{w} {ld[k]}" for k, w in words.items() if ld.get(k)]
            if ld.get("share"):
                parts.append("main shares a core")
            L.append(f"          {name}: " + (", ".join(parts) or "default load"))
    for b in m.get("builds") or []:
        args = " ".join(x for x in (b.get("replica_args") and f"replica {b['replica_args']}",
                                    b.get("primary_args") and f"primary {b['primary_args']}",
                                    b.get("share") and "main shares a core") if x)
        L.append(f"build     {b['name']}: valkey {b.get('commit')}" + (f" ({args})" if args else ""))
        if not b.get("binaries"):
            L.append("            binary not recorded")
        for g in b.get("binaries") or []:
            L += _binary_lines(g, "            ")
        s = b.get("source")
        L.append(f"            source tree {_short(s['tree'], 16)} ({s['files']} files, from {s['from_role']})" if s
                 else "            source tree not archived")
    for name, t in (m.get("tools") or {}).items():
        src = (f"{t.get('version') or ''} from {t.get('upstream') or ''} {(t.get('upstream_commit') or '')[:12]}, "
               f"source {_short(t.get('source_tree_sha256'))}" if name == "cachecannon" else t.get("source", ""))
        L.append(f"tool      {name}: {src}")
        for g in t.get("binaries") or []:
            L += _binary_lines(g, "            ")
    rig = m.get("rig") or {}
    if rig:
        L.append("hosts")
        L += [_role_line(role, r) for role, r in (rig.get("roles") or {}).items()]
        if rig.get("declared_source"):
            L.append(f"  declared: {rig['declared_source']}")
        rtt = rig.get("rtt_ms") or {}
        if rtt:
            L.append("  rtt median ms: " + ", ".join(f"{k} {(v or {}).get('median_ms', float('nan')):.3f}"
                                                    for k, v in rtt.items()))
    inp = (m.get("inputs") or {}).get("calibration")
    if inp:
        L.append(f"inputs    calibration {inp.get('id')} (calibration.json, sha256 {_short(inp.get('sha256'))}"
                 + (f", made by run {inp['archive_run']})" if inp.get("archive_run") else ")"))
    L.append(f"outputs   {o.get('records', 0)} records, {o.get('valid', 0)} valid, {o.get('errors', 0)} errors"
             + (f"; calibration {o['calibration_id']} ({'converged' if o.get('converged') else 'NOT converged'})"
                if o.get("calibration_id") else ""))
    for w in m.get("warnings") or []:
        L.append(f"warning   {w}")
    for g in m.get("gaps") or []:
        L.append(f"gap       {g}")
    for n in notes:
        L.append(f"note      {n.get('ts')} {n.get('by')}: " + (f"supersedes {n['supersedes']}: " if n.get("supersedes")
                                                             else f"superseded by {n['superseded_by']}: "
                                                             if n.get("superseded_by") else "") + (n.get("text") or ""))
    if run_dir:
        L.append(f"files     {run_dir} (REPRODUCE.md, result.txt, records.jsonl)")
    return "\n".join(L)


# --------------------------------------------------------- legacy imports ----

def _ts_id(ts: str) -> str:
    return re.sub(r"[-:]", "", ts.split(".")[0].rstrip("Z")) + "Z"


def import_records(archive: Archive, records: Path, *, label: Optional[str] = None,
                   calibration: Optional[Path] = None, log_file: Optional[Path] = None,
                   harness_commit: Optional[str] = None, harness_repo: Optional[Path] = None,
                   evidence: Optional[str] = None, notes: list = (), tags=(),
                   scenario_version: Optional[int] = None, host_facts: Optional[dict] = None,
                   argv: Optional[list] = None, argv_source: Optional[str] = None, cwd: Optional[str] = None,
                   driver: Optional[Path] = None) -> Run:
    """Archive a run recorded before the archive existed, from its records file (and calibration and log). What
    the records do not say (binaries, host facts, tool versions) is listed under gaps, not guessed; so is the
    scenario version, unless the importer gives it.

    host_facts: {"source": how they were found, "hosts": {host name: facts}}, kept per role under `declared`,
    apart from observed facts. argv: the command as the importer found it (argv_source says where); driver: the
    script that ran it, archived as an object."""
    from . import repl, replcap
    archive.ensure()
    records = Path(records)
    raw = records.read_bytes()
    recs = [json.loads(l) for l in raw.decode().splitlines() if l.strip()]
    recs = [r for r in recs if r.get("kind") == "replrw"]
    if not recs:
        raise ArchiveError(f"{records}: no replica records")
    command = "repl-calibrate" if any(r.get("phase") for r in recs) else "repl-run"
    calibrated = command == "repl-calibrate" or any(r.get("targets") for r in recs)
    times = sorted(r["ts"] for r in recs if r.get("ts"))
    label = label or records.stem
    rid = f"{_ts_id(times[0])}-{command}-{re.sub(r'[^A-Za-z0-9._-]+', '-', label)}-imported"
    run = Run(archive, rid)
    if run.dir.exists():
        raise ArchiveError(f"{rid} is already in the archive")
    run.dir.mkdir()
    run.launch_dir.mkdir()
    write_atomic(run.records, raw)
    if log_file and Path(log_file).exists():
        write_atomic(run.log_path, Path(log_file).read_bytes())
    m = run.manifest = {
        "kind": "offbench-run", "schema": SCHEMA, "id": rid, "label": label, "status": "imported",
        "started": times[0], "ended": times[-1], "tags": sorted(set(tags) | {"imported"}),
        "scenario": scenario_for(command, calibrated, scenario_version),
        "command": {"name": command, "argv": list(argv) if argv else None, "args": None, "cwd": cwd,
                    "source": argv_source if argv else None},
        "imported": {"at": iso(), "by": getpass.getuser(), "records": str(records.resolve()),
                     "records_sha256": hashlib.sha256(raw).hexdigest(), "notes": list(notes)},
        "inputs": {}, "gaps": [], "warnings": [],
    }
    run._started = time.time()
    if driver and Path(driver).exists():
        m["command"]["driver"] = {"path": str(Path(driver).resolve()), "object": archive.put_file(driver),
                                  "note": "the script's content when the run was imported"}
    missing = "host facts, binary hashes and tool versions" + ("" if argv else ", and the command line,")
    run.gap(f"recorded before the archive: {missing} were not captured at the time")
    m["scenario"]["version_given_at_import"] = scenario_version is not None
    if scenario_version is None:
        m["scenario"]["version"], m["scenario"]["method"] = None, None
        run.gap("scenario version not given at import: which method ran is in the harness commit and the notes")
    h = None
    if harness_commit and harness_repo:
        try:
            h = harness_from_commit(Path(harness_repo), harness_commit, archive)
        except ArchiveError as e:
            run.gap(f"harness {harness_commit}: {e}")
    m["harness"] = h or ({"name": "offbench", "commit": harness_commit} if harness_commit else None)
    if m["harness"] is not None:
        m["harness"]["evidence"] = evidence or "given at import, no evidence recorded"
        m["scenario"]["doc_sha256"] = (m["harness"].get("docs") or {}).get(m["scenario"]["doc"])
    else:
        run.gap("harness commit unknown")
    try:          # the offbench doing the import: it has the `runs` commands an older harness lacks
        own = harness(Path(__file__).resolve().parent.parent, archive)
        m["archived_by"] = {k: own[k] for k in ("commit", "describe", "dirty", "remote", "remote_branches_with_commit",
                                                "snapshot", "has") if k in own}
    except Exception as e:      # noqa: BLE001 - provenance only; the import goes on
        run.gap(f"the importing offbench could not be archived: {type(e).__name__}: {e}")
    first_rec = recs[0]
    declared = json.loads(json.dumps((host_facts or {}).get("hosts") or {}))    # a copy: user_data paths become objects
    m["rig"] = {"rtt_ms": first_rec.get("rtt_ms"),
                "roles": {role: {"host": name, "facts": None, **({"declared": declared[name]} if name in declared else {})}
                          for role, name in (first_rec.get("hosts") or {}).items()}}
    ud_objects: dict = {}
    for name, d in declared.items():
        ud = d.get("user_data")
        if isinstance(ud, str):
            if ud not in ud_objects:
                ud_objects[ud] = archive.put_file(ud) if Path(ud).exists() else None
            d["user_data"] = ({"path": str(Path(ud).resolve()), "object": ud_objects[ud]} if ud_objects[ud]
                              else {"path": ud, "object": None})
    if declared:
        m["rig"]["declared_source"] = host_facts.get("source")
        unknown = [r for r, v in m["rig"]["roles"].items() if "declared" not in v]
        if unknown:
            run.gap(f"no declared facts for {', '.join(unknown)}")
    seen: dict = {}
    for r in recs:
        b = r.get("bin") or {}
        seen.setdefault(b.get("name"), b)
    m["builds"] = [{"name": n, "commit": b.get("commit"), "server_path": f"~/offbench/builds/{b.get('commit')}/src/valkey-server",
                    "replica_args": b.get("replica_args", ""), "primary_args": b.get("primary_args", ""),
                    "share": b.get("share", 0), "binaries": [], "source": None} for n, b in seen.items()]
    m["tools"] = {"cachecannon": dict((h or {}).get("cachecannon_source") or {}, binaries=[]),
                  "valkey-cli": {"source": "any valkey build; only the control connection uses it", "binaries": []}}
    cells = [r["cell"] for r in recs if r.get("cell")]
    m["config"] = {"cores": sorted({c["io_threads"] - (c.get("pin_share") or 0) for c in cells}),
                   "reps": max((r.get("rep") or 0) for r in recs), "window_s": first_rec.get("window_s"),
                   "pinned": all(c.get("pin") for c in cells) if cells else None}
    cal = None
    if calibration and Path(calibration).exists():
        cal = json.loads(Path(calibration).read_text())
        if command == "repl-calibrate":
            run.put_calibration(cal)
            m["outputs"] = {"calibration_id": cal.get("id"), "converged": cal.get("converged"),
                            "targets": cal.get("targets")}
        else:
            run.put_input_calibration(Path(calibration), cal)
    if command == "repl-run":
        m["config"].update(replica_reader_conns=sorted({c["conns"] for c in cells}),
                           replica_reader_pipelines=sorted({c["pipeline"] for c in cells}),
                           targets=first_rec.get("targets"), calibration=(first_rec.get("calibration") or {}).get("id"))
        m["config"]["loads"] = {}
        for r in recs:
            c, b = r.get("cell") or {}, r.get("bin") or {}
            m["config"]["loads"][b.get("name")] = {
                "writers": f"{c.get('write_conns') or c.get('conns')}xP{c.get('write_pipeline') or c.get('pipeline')}",
                "primary_readers": f"{c.get('p_conns') or 0}xP{c.get('p_pipeline') or c.get('pipeline')}",
                "share": b.get("share", 0), "replica_args": b.get("replica_args", ""),
                "primary_args": b.get("primary_args", "")}
    result = None
    try:
        loaded = [repl.revalidate(dict(r)) for r in recs]
        if command == "repl-run":
            result = replcap.report(loaded, None) if calibrated else repl.sweep_summary(loaded)
        elif cal:
            result = calibration_result(cal)
    except Exception as e:
        run.gap(f"no result table: the records did not load with this harness ({type(e).__name__}: {e})")

    def epoch(ts: str) -> float:
        import calendar
        return calendar.timegm(time.strptime(ts.split(".")[0].rstrip("Z"), "%Y-%m-%dT%H:%M:%S"))
    run.seal("imported", result=result, log=lambda s: None, ended=times[-1],
             seconds=round(epoch(times[-1]) - epoch(times[0])))
    return run


def calibration_result(doc: dict) -> str:
    t = doc.get("targets") or {}
    L = [f"calibration {doc.get('id')}: {'converged' if doc.get('converged') else 'NOT converged'}; "
         f"targets X {t.get('write_kcmd_s', 0):.0f}k writes/s, Y {t.get('read_kcmd_s', 0):.0f}k reads/s"]
    for name, v in (doc.get("builds") or {}).items():
        s = v.get("shape") or {}
        x, y = t.get("write_kcmd_s") or 0, t.get("read_kcmd_s") or 0
        L.append(f"{name:<10} writers {s.get('w')}xP{s.get('wp')}  primary readers {s.get('p')}xP{s.get('pp')}  "
                 f"{v.get('write_kcmd_s', 0):.0f}k writes" + (f" {v['write_kcmd_s'] / x - 1:+.1%}" if x else "")
                 + f"  {v.get('read_kcmd_s', 0):.0f}k reads" + (f" {v['read_kcmd_s'] / y - 1:+.1%}" if y else "")
                 + ("" if v.get("converged") else "  NOT CONVERGED"))
    return "\n".join(L)


def bundle(archive: Archive, run_dir: Path, dest: Path) -> dict:
    """One tar holding runs/<id>/ and every object the run relies on, laid out as in the archive, so its
    REPRODUCE.md works wherever the tar is unpacked."""
    m = archive.manifest(run_dir)
    names = [(f"runs/{m['id']}/{p.relative_to(run_dir).as_posix()}", p)
             for p in sorted(Path(run_dir).rglob("*")) if p.is_file()]
    missing = []
    for sha, what in objects_of(m):
        op = archive.object_path(sha)
        if op.exists():
            names.append((op.relative_to(archive.root).as_posix(), op))
        else:
            missing.append(what)
    with tarfile.open(dest, "w") as tar:
        info = tarfile.TarInfo("ARCHIVE.md")
        data = ARCHIVE_MD.encode()
        info.size, info.mode, info.mtime = len(data), 0o444, 0
        tar.addfile(info, io.BytesIO(data))
        for name, p in names:
            tar.add(p, arcname=name, recursive=False)
    return {"files": len(names) + 1, "bytes": Path(dest).stat().st_size, "missing": missing}
