"""Run archive: packing, the store, harness snapshots, host facts, archived repl runs, REPRODUCE.md, imports."""

import gzip
import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

from offbench import archive, cli, packing, repl, reproduce


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _tree(root: Path, files: dict, mtime: int = 0) -> Path:
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        if mtime:
            os.utime(p, (mtime, mtime))
    return root


def _names(gz: Path) -> list:
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(gz.read_bytes()))) as t:
        return t.getnames()


# ------------------------------------------------------------------ packing ----

def test_tar_gz_gives_the_same_sha256_for_the_same_files_whatever_their_mtimes(tmp_path):
    files = {"a.txt": b"one", "src/b.c": b"int x;", "src/run.sh": b"#!/bin/sh\n"}
    t1, t2 = _tree(tmp_path / "t1", files, 1_000), _tree(tmp_path / "t2", files, 2_000_000)
    os.chmod(t1 / "src/run.sh", 0o755)
    os.chmod(t2 / "src/run.sh", 0o700)          # any executable bit is 755 in the tar
    os.symlink("b.c", t1 / "src/link")
    os.symlink("b.c", t2 / "src/link")
    r = [packing.tar_gz([(n, p, None) for n, p in packing.walk(t)[0]], tmp_path / f"{t.name}.gz") for t in (t1, t2)]
    assert r[0]["sha256"] == r[1]["sha256"] and r[0]["files"] == 4
    assert packing.gunzip_sha256(tmp_path / "t1.gz") == r[0]["sha256"]
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress((tmp_path / "t1.gz").read_bytes()))) as t:
        m = {i.name: i for i in t.getmembers()}
    assert m["src/run.sh"].mode == 0o755 and m["a.txt"].mode == 0o644 and m["src/link"].issym()
    assert {i.mtime for i in m.values()} == {0} and {i.uid for i in m.values()} == {0}


def test_walk_leaves_out_excluded_names_and_oversized_files(tmp_path):
    t = _tree(tmp_path / "v", {"src/server.c": b"c", "src/server.o": b"o", "src/valkey-server": b"bin",
                               "deps/x/lib.a": b"a", "tests/tmp/x": b"t", "big.dat": b"x" * 100})
    entries, skipped = packing.walk(t, packing.VALKEY_TREE_EXCLUDE, max_bytes=50)
    assert [n for n, _ in entries] == ["src/server.c"] and skipped == ["big.dat"]


def test_gzip_file_round_trips_and_names_the_content(tmp_path):
    src = tmp_path / "f"
    src.write_bytes(b"payload" * 1000)
    r = packing.gzip_file(src, tmp_path / "f.gz")
    assert r["sha256"] == _sha(src.read_bytes()) and gzip.decompress((tmp_path / "f.gz").read_bytes()) == src.read_bytes()
    assert packing.gzip_bytes(src.read_bytes(), tmp_path / "g.gz")["sha256"] == r["sha256"]


# -------------------------------------------------------------------- store ----

def test_store_keeps_objects_by_content_and_refuses_a_mismatch(tmp_path):
    store = archive.Archive(tmp_path / "a").ensure()
    sha = store.put_bytes(b"hello")
    assert sha == _sha(b"hello") and store.has(sha) and store.read_bytes(sha) == b"hello"
    assert store.check_object(sha) is None and store.put_bytes(b"hello") == sha
    bad = store.tmp()
    packing.gzip_bytes(b"other", bad)
    with pytest.raises(archive.ArchiveError):
        store.put_gz(bad, sha)
    assert not bad.exists() and store.read_bytes(sha) == b"hello"
    assert "missing" in store.check_object("0" * 64)


def test_resolve_by_id_prefix_or_label(tmp_path):
    store = archive.Archive(tmp_path / "a").ensure()
    for rid, label in (("20261016T000000Z-repl-run-x-aaaa", "x"), ("20261016T000001Z-repl-run-x-bbbb", "x"),
                       ("20261017T000000Z-repl-run-y-cccc", "y")):
        (store.runs_dir / rid).mkdir()
        (store.runs_dir / rid / "manifest.json").write_text(json.dumps({"id": rid, "label": label}))
    assert store.resolve("20261017").name.endswith("cccc")
    assert store.resolve("x").name.endswith("bbbb")            # a label means its newest run
    with pytest.raises(archive.ArchiveError, match="matches 2 runs"):
        store.resolve("20261016")
    with pytest.raises(archive.ArchiveError, match="no run"):
        store.resolve("nope")


def test_archive_root_comes_from_flag_env_or_default(monkeypatch, tmp_path):
    monkeypatch.delenv("OFFBENCH_ARCHIVE", raising=False)
    assert archive.resolve_root(None) == archive.DEFAULT_ROOT
    monkeypatch.setenv("OFFBENCH_ARCHIVE", str(tmp_path))
    assert archive.resolve_root(None) == tmp_path and archive.resolve_root("off") is None
    monkeypatch.setenv("OFFBENCH_ARCHIVE", "off")
    assert archive.resolve_root(None) is None and archive.resolve_root(str(tmp_path / "b")) == tmp_path / "b"


def test_secret_like_names_never_enter_a_snapshot():
    for n in ("k.pem", "rig/valkey-bench.pem", "known_hosts", "hosts.json", "id_ed25519", ".env", "credentials.json"):
        assert archive.is_secret_like(n), n
    for n in ("known_hosts.example", "hosts.example.json", "README.md", "offbench/hosts.py"):
        assert not archive.is_secret_like(n), n


# ------------------------------------------------------------------ harness ----

def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
                          capture_output=True, text=True, check=True).stdout.strip()


def _repo(tmp_path: Path) -> Path:
    root = _tree(tmp_path / "offbench", {
        "offbench/x.py": b"X = 1\n", "docs/replica-read-capacity.md": b"# Replica read capacity\n",
        "vendor/cachecannon/VENDOR.md": b"Source: https://github.com/cachecannon/cachecannon.git\n"
                                        b"Pinned commit: 341254afe3d7596588cafdc230832dc57b14c5d0\n",
        "vendor/cachecannon/Cargo.toml": b'[package]\nname = "cachecannon"\nversion = "0.0.25-alpha.0"\n',
        "vendor/cachecannon/Cargo.lock": b"lock\n", ".gitignore": b"data/\n__pycache__/\n*.pem\n"})
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "one")
    return root


def test_harness_snapshot_holds_the_working_tree_as_it_ran(tmp_path):
    root = _repo(tmp_path)
    store = archive.Archive(tmp_path / "a").ensure()
    clean = archive.harness(root, store)
    assert clean["listed_by"] == "git" and clean["dirty"] is False and clean["commit"] == _git(root, "rev-parse", "HEAD")
    assert clean["cachecannon_source"]["upstream_commit"].startswith("341254afe")
    assert clean["cachecannon_source"]["version"] == "0.0.25-alpha.0"
    assert clean["docs"]["docs/replica-read-capacity.md"] == _sha(b"# Replica read capacity\n")
    (root / "offbench/x.py").write_text("X = 2\n")
    (root / "new.py").write_text("N = 1\n")
    (root / "key.pem").write_text("secret")            # ignored
    (root / "hosts.json").write_text("{}")             # untracked, secret-like
    (root / "data").mkdir()
    (root / "data/r.jsonl").write_text("{}")
    dirty = archive.harness(root, store)
    assert dirty["dirty"] and dirty["changed"] == ["offbench/x.py"] and dirty["untracked"] == ["new.py"]
    assert dirty["snapshot"]["left_out_secret_like"] == ["hosts.json"]
    assert b"X = 2" in store.read_bytes(dirty["diff_object"])
    names = _names(store.object_path(dirty["snapshot"]["object"]))
    assert "new.py" in names and "HARNESS.json" in names
    assert not {"hosts.json", "key.pem", "data/r.jsonl"} & set(names)
    assert dirty["snapshot"]["object"] != clean["snapshot"]["object"]


def test_an_unpacked_snapshot_snapshots_to_the_same_object(tmp_path):
    root = _repo(tmp_path)
    store = archive.Archive(tmp_path / "a").ensure()
    first = archive.harness(root, store)
    out = tmp_path / "unpacked"
    out.mkdir()
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(store.object_path(first["snapshot"]["object"]).read_bytes()))) as t:
        t.extractall(out, **({"filter": "data"} if hasattr(tarfile, "data_filter") else {}))
    (out / "data").mkdir()
    (out / "data/run.jsonl").write_text("{}")          # what a reproduction writes is not part of the harness
    (out / "hosts.json").write_text("{}")
    again = archive.harness(out, store)
    assert again["listed_by"] == "tree" and again["unpacked_snapshot"] and again["commit"] == first["commit"]
    assert again["since_snapshot"] == {"changed": [], "added": [], "missing": []}
    assert again["snapshot"]["object"] == first["snapshot"]["object"]
    (out / "offbench/x.py").write_text("X = 3\n")
    assert archive.harness(out, store)["since_snapshot"]["changed"] == ["offbench/x.py"]


# --------------------------------------------------------------- host side ----

def _fake_build(root: Path, commit: str) -> Path:
    """A valkey build tree as repl-run finds one: a server that prints a version, make settings, release header."""
    build = _tree(root / "builds" / commit, {
        "src/server.c": b"int main;\n", "src/server.o": b"obj", "src/release.h":
            b'#define VALKEY_GIT_SHA1 "00000000"\n#define VALKEY_GIT_DIRTY "0"\n#define VALKEY_BUILD_ID_RAW "h-1"\n',
        "src/.make-settings": b"STD=-pedantic\nOPT=-O3\nMALLOC=jemalloc\nCFLAGS=\n", "build.log": b"cc ...\n",
        "deps/jemalloc/VERSION": b"5.3.0-0-g0\n",
        "src/valkey-server": b"#!/bin/sh\necho 'Valkey server v=9.0.0 sha=00000000:0 malloc=jemalloc-5.3.0 bits=64'\n"})
    os.chmod(build / "src/valkey-server", 0o755)
    return build / "src/valkey-server"


def test_host_facts_describe_a_build_and_never_raise(tmp_path):
    from offbench import hostfacts
    server = _fake_build(tmp_path, "c" * 40)
    f = hostfacts.collect(servers=[str(server), str(tmp_path / "missing/valkey-server")], cli=str(server))
    s, gone = f["servers"]
    assert s["sha256"] == _sha(server.read_bytes()) and s["version"].startswith("Valkey server v=9.0.0")
    b = s["build"]
    assert b["source_present"] and "OPT=-O3" in b["make_settings"] and b["release_h"]["VALKEY_GIT_DIRTY"] == "0"
    assert b["jemalloc"] == "5.3.0-0-g0" and b["build_log"]["sha256"] == _sha(b"cc ...\n")
    assert gone == {"path": str(tmp_path / "missing/valkey-server"), "exists": False, "build": gone["build"]}
    assert f["cli"]["sha256"] == s["sha256"] and f["cachecannon"] is None
    for k in ("instance", "os", "kernel", "cpu", "packages", "tools", "sysctl", "thp", "pintune", "nics", "limits"):
        assert k in f
    assert isinstance(f["pintune"]["applied"], bool) and f["limits"]["nofile"]
    json.dumps(f)


def test_agent_packs_files_and_build_trees_and_hashes_binaries(tmp_path, monkeypatch):
    monkeypatch.setattr(repl, "WORK", tmp_path / "work")
    server = _fake_build(tmp_path, "d" * 40)
    got = repl.agent({"action": "pack", "items": [
        {"kind": "file", "path": str(server)},
        {"kind": "tree", "path": str(server.parent.parent), "exclude": "valkey-build", "max_bytes": 1 << 20}]})["items"]
    f, t = got
    assert f["sha256"] == _sha(server.read_bytes()) and gzip.decompress(Path(f["gz"]).read_bytes()) == server.read_bytes()
    names = _names(Path(t["gz"]))
    assert "src/server.c" in names and "src/.make-settings" in names and "build.log" in names
    assert "src/server.o" not in names and "src/valkey-server" not in names
    h = repl.agent({"action": "hash", "paths": [str(server)]})["binaries"][0]
    assert h["sha256"] == f["sha256"] and "version" not in h


# ------------------------------------------------- archived runs, end to end ----

COMMIT = "1" * 40
FACTS = {
    "host": {"hostname": "ip-10-0-0-1", "user": "ec2-user", "home": "/home/ec2-user"},
    "instance": {"instance_id": "i-1234567890abcdef0", "instance_type": "c8g.metal-24xl", "ami_id": "ami-0abc",
                 "az": "us-east-1b", "placement_group": "bench-pg", "region": "us-east-1"},
    "os": {"pretty": "Amazon Linux 2023.6.20241010", "release": "Amazon Linux release 2023.6.20241010 (Amazon Linux)"},
    "kernel": "6.1.112-122.189.amzn2023.aarch64", "arch": "aarch64",
    "cpu": {"count": 96, "Model name": "Neoverse-V2"}, "mem_kb": 197_000_000, "python": "3.9.20",
    "packages": {"ethtool": ["6.2-1.amzn2023"], "gcc": ["11.4.1-2.amzn2023"], "python3": ["3.9.20-1.amzn2023"],
                 "tmux": None},
    "tools": {"gcc": "gcc (GCC) 11.4.1", "rustc": None},
    "sysctl": {"net.core.somaxconn": "4096", "net.core.netdev_max_backlog": "50000"},
    "thp": {"enabled": "[always] madvise never"},
    "pintune": {"irqbalance": "inactive", "lo_rps_cpus": "00000001,fffe0000", "workqueue_cpumask": "1,fffe0001",
                "irq_affinity": {"17-32": 140}, "applied": True},
    "nics": [], "clock": {"System time": "0.000001 seconds fast of NTP time"}, "limits": {"nofile": [65535, 65535]},
}


def _snap(t, info, main=0.0, io_=0.0):
    return {"t": t, "ut": t, "info": info, "ticks": {"main": main, "io": io_},
            "ena": {k: 0 for k in repl.ALLOWANCE_FIELDS}}


class _Rig:
    """The host side of archived repl runs, in memory: three hosts P (primary), R (replica) and G (generators),
    with a build tree, valkey-cli and cachecannon on disk under tmp."""

    def __init__(self, tmp: Path, monkeypatch):
        from offbench.cell import CLK_TCK
        monkeypatch.setattr(repl, "WORK", tmp / "work")
        self.server = _fake_build(tmp / "host", COMMIT)
        self.cli = _tree(tmp / "host", {"base/src/valkey-cli": b"#!/bin/sh\necho 'valkey-cli 9.0.0'\n"}) / "base/src/valkey-cli"
        self.cc = _tree(tmp / "host", {"cc/cachecannon": b"#!/bin/sh\necho 'cachecannon 0.0.25-alpha.0'\n"}) / "cc/cachecannon"
        for p in (self.cli, self.cc):
            os.chmod(p, 0o755)
        self.calls = []
        pa = {"total_commands_processed": "0", "cmdstat_set": "calls=0,usec=1", "master_repl_offset": "10000000",
              "sync_full": "1", "connected_clients": "2400", "used_active_time_main_thread": "0"}
        pb = dict(pa, total_commands_processed="18000000", cmdstat_set="calls=12000000,usec=1",
                  used_active_time_main_thread="6.0", master_repl_offset="600000000")
        ra = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_link_status": "up",
              "master_sync_in_progress": "0", "connected_clients": "2400", "slave_repl_offset": "0"}
        rb = dict(ra, total_commands_processed="42000000", cmdstat_set="calls=12000000", slave_repl_offset="590000000")
        self.snaps = {"P": {"a": _snap(0.0, pa), "b": _snap(6.0, pb, main=6 * CLK_TCK)},
                      "R": {"a": _snap(0.0, ra), "b": _snap(6.0, rb)}}

    def local(self, path: str) -> Path:
        return self.server if path.endswith(f"/builds/{COMMIT}/src/valkey-server") else Path(path)

    def call(self, role, payload, timeout=120):
        from offbench import hostfacts
        self.calls.append((role.name, payload["action"]))
        act = payload["action"]
        if act == "facts":
            out = dict(FACTS, servers=[dict(hostfacts.binary(str(self.local(s))), build=hostfacts.valkey_build(str(self.local(s))))
                                       for s in payload["servers"]],
                       cli=hostfacts.binary(str(self.cli)) if payload.get("cli") else None,
                       cachecannon=hostfacts.binary(str(self.cc)) if payload.get("gen") else None)
            return json.loads(json.dumps(out))
        if act == "hash":
            return {"binaries": [hostfacts.binary(str(self.local(p)), quick=True) for p in payload["paths"]]}
        if act == "pack":
            return repl.agent(payload)
        if act == "status":
            return {"master_link_status": "up", "dbsize": 100000, "connected_clients": "2400",
                    "instantaneous_ops_per_sec": "20000"}
        if act == "server_start":
            return {"pid": 1, "argv": ["taskset", "-c", "0", "setsid", "-f", payload["server"], "--port", "7400"],
                    "post": [["config", "set", "client-output-buffer-limit", "replica 0 0 0"]],
                    "pins": {"main": "1", "io_thd_1": "2"}}
        if act == "gen_start":
            return {"alive": True, "argv": ["taskset", "-c", payload["cpus"], "cachecannon", payload["name"] + ".toml"],
                    "toml": f"[connection]\nconnections = {payload['conns']}\npipeline_depth = {payload['pipeline']}\n"}
        if act == "gen_alive":
            return {g: True for g in payload["gens"]}
        if act == "measure":
            if payload.get("server"):
                return self.snaps[role.name]
            z = {k: 0 for k in repl.ALLOWANCE_FIELDS}
            return {"a": {"gt": 0.0, "gen_ticks": {g: 0 for g in payload["gens"]}, "ena": z},
                    "b": {"gt": 6.0, "gen_ticks": {g: 600 for g in payload["gens"]}, "ena": z}}
        if act == "gen_stop":
            return {g: {"alive_at_stop": True, "timeouts": 0, "err_pct": 0.0, "err_tail": "",
                        "latency": {"p50_us": 50, "p99_us": 100, "p999_us": 200, "max_us": 900}} for g in payload["gens"]}
        return {"ok": True, "asserts": 0, "client_closed_for_obuf": 0}

    def start(self, role, payload):
        out = self.call(role, payload)
        return lambda timeout=120: out

    def fetch(self, role, remote_path, dest, timeout=900):
        self.calls.append((role.name, "fetch"))
        Path(dest).write_bytes(Path(remote_path).read_bytes())
        os.unlink(remote_path)


ROLES = {"primary": repl.Role("P", "ec2-user@192.0.2.1", "10.0.0.1"),
         "replica": repl.Role("R", "ec2-user@192.0.2.2", "10.0.0.2"),
         "gen": repl.Role("G", "ec2-user@192.0.2.3", "10.0.0.3")}
RTT = {"gen_primary": {"median_ms": 0.08, "min_ms": 0.07}, "gen_replica": {"median_ms": 0.09, "min_ms": 0.08},
       "primary_replica": {"median_ms": 0.08, "min_ms": 0.07}}
HOSTS = ["--primary", "P", "--replica", "R", "--gen", "G", "--key", "/keys/bench.pem", "--known-hosts", "/keys/kh"]


@pytest.fixture
def rig(tmp_path, monkeypatch):
    import contextlib
    import types
    from offbench import lease
    r = _Rig(tmp_path, monkeypatch)
    reg = types.SimpleNamespace(hosts={}, path=tmp_path / "registry.json")
    monkeypatch.setattr(lease, "held", lambda *a, **k: contextlib.nullcontext())
    monkeypatch.setattr(cli, "_repl_hosts", lambda a, cal=None: ({"primary": "P", "replica": "R", "gen": "G"}, reg))
    monkeypatch.setattr(cli, "_repl_open", lambda a, repl_, reg_, names, bins, log: (r, ROLES, RTT))
    monkeypatch.setattr(cli, "_sigterm_raises", lambda: None)
    monkeypatch.setattr(repl.time, "sleep", lambda s: None)
    monkeypatch.delenv("OFFBENCH_ARCHIVE_TAGS", raising=False)
    return r


def _calibrate(tmp_path, store_root):
    out = tmp_path / "data" / "c2-calib.jsonl"
    argv = ["repl-calibrate", *HOSTS, "--archive", str(store_root), "--bins", f"a={COMMIT}", "--iot", "2",
            "--targets", "2000,1000", "--out", str(out)]
    return cli.main(argv), out.with_suffix(".json"), argv


def _sweep(tmp_path, store_root, cal: Path, *extra):
    out = tmp_path / "data" / "c2.jsonl"
    argv = ["repl-run", *HOSTS, "--archive", str(store_root), "--calibration", str(cal), "--conns", "0,100",
            "--pipelines", "16", "--reps", "1", "--tag", "test", "--out", str(out), *extra]
    return cli.main(argv), out, argv


def _run_dir(store_root: Path, command: str) -> Path:
    dirs = [d for d in (store_root / "runs").iterdir() if f"-{command}-" in d.name]
    assert len(dirs) == 1, dirs
    return dirs[0]


def test_calibrate_and_sweep_archive_what_ran_and_how(tmp_path, rig):
    store_root = tmp_path / "arch"
    rc, cal_path, cal_argv = _calibrate(tmp_path, store_root)
    assert rc == 0
    cdir = _run_dir(store_root, "repl-calibrate")
    cm = json.loads((cdir / "manifest.json").read_text())
    doc = json.loads(cal_path.read_text())
    assert doc["converged"] and doc["archive_run"] == cm["id"] == cdir.name
    assert json.loads((cdir / "calibration.json").read_text())["id"] == doc["id"] == cm["outputs"]["calibration_id"]
    assert cm["status"] == "complete" and cm["scenario"]["step"] == "calibrate" and cm["command"]["argv"] == cal_argv
    assert "converged" in (cdir / "result.txt").read_text()

    rc, out, argv = _sweep(tmp_path, store_root, cal_path)
    assert rc == 0
    d = _run_dir(store_root, "repl-run")
    m = json.loads((d / "manifest.json").read_text())
    store = archive.Archive(store_root)
    # what binary: by sha256, on which hosts, archived, with its build settings and source tree
    (b,) = m["builds"]
    (g,) = b["binaries"]
    assert b["commit"] == COMMIT and g["sha256"] == _sha(rig.server.read_bytes()) and g["roles"] == ["primary", "replica"]
    assert g["archived"] and store.read_bytes(g["sha256"]) == rig.server.read_bytes()
    assert g["version"].startswith("Valkey server v=9.0.0") and "OPT=-O3" in g["build"]["make_settings"]
    assert "src/server.c" in _names(store.object_path(b["source"]["tree"]))
    # with what tools and versions; on what hosts
    cc, vcli = m["tools"]["cachecannon"], m["tools"]["valkey-cli"]
    assert cc["upstream_commit"].startswith("341254afe") and cc["binaries"][0]["roles"] == ["gen"]
    assert cc["binaries"][0]["archived"] and vcli["binaries"][0]["roles"] == ["primary", "replica"]
    assert m["rig"]["roles"]["primary"]["facts"]["instance"]["instance_type"] == "c8g.metal-24xl"
    assert "servers" not in m["rig"]["roles"]["primary"]["facts"] and m["rig"]["rtt_ms"] == RTT
    # what scenario, defined where; what config; what came in and out
    assert m["scenario"]["name"] == "replica-read-capacity" and m["scenario"]["step"] == "sweep"
    assert m["scenario"]["doc_sha256"] == m["harness"]["docs"]["docs/replica-read-capacity.md"]
    assert store.has(m["harness"]["snapshot"]["object"]) and m["harness"]["cachecannon_source"]["version"]
    assert m["command"]["argv"] == argv and m["command"]["args"]["key"] == {"file": "bench.pem"}
    assert m["config"]["cores"] == [2] and m["config"]["replica_reader_conns"] == [0, 100] and m["tags"] == ["test"]
    assert m["inputs"]["calibration"]["archive_run"] == cdir.name
    assert m["outputs"]["records"] == 2 and m["outputs"]["valid"] == 2 and m["status"] == "complete"
    assert m["outputs"]["legacy_out"] == str(out.resolve())
    # the records, the generator configs and the log
    lines = (d / "records.jsonl").read_text().splitlines()
    assert lines == out.read_text().splitlines()
    for rec in map(json.loads, lines):
        assert rec["archive_run"] == m["id"] and "_tomls" not in rec and rec["launch"]["primary"]["pins"]["main"] == "1"
        for gen in rec["launch"]["gens"]:
            text = (d / "launch" / f"{gen['toml_sha256']}.toml").read_text()
            assert _sha(text.encode()) == gen["toml_sha256"] and gen["role"] == "gen"
    log = (d / "run.log").read_text()
    assert "calibration cal-" in log and "archive: facts from 3/3 hosts" in log and "rep1" in log
    assert (d / "result.txt").read_text().strip()
    # sealed: every file listed with its sha256, read-only, and verify agrees
    assert set(m["files"]) >= {"records.jsonl", "run.log", "calibration.json", "result.txt", "REPRODUCE.md"}
    assert archive.verify(store, d) == []
    if os.geteuid() != 0:
        assert not os.access(d / "records.jsonl", os.W_OK)
    os.chmod(d, 0o755)
    os.chmod(d / "records.jsonl", 0o644)
    with (d / "records.jsonl").open("a") as f:
        f.write("{}\n")
    (d / "extra.txt").write_text("x")
    assert archive.verify(store, d) == ["records.jsonl: changed since the run was sealed", "extra.txt: added after sealing"]


def test_a_second_run_reuses_archived_binaries_and_sources(tmp_path, rig):
    store_root = tmp_path / "arch"
    _, cal_path, _ = _calibrate(tmp_path, store_root)
    packs = [c for c in rig.calls if c[1] in ("pack", "fetch")]
    assert packs, "the first run archives its binaries"
    rig.calls.clear()
    assert _sweep(tmp_path, store_root, cal_path)[0] == 0
    assert not [c for c in rig.calls if c[1] in ("pack", "fetch")]
    m = json.loads((_run_dir(store_root, "repl-run") / "manifest.json").read_text())
    assert m["builds"][0]["source"]["tree"] and m["builds"][0]["binaries"][0]["archived"]


def test_no_archive_binaries_records_hashes_only(tmp_path, rig):
    store_root = tmp_path / "arch"
    _, cal_path, _ = _calibrate(tmp_path, tmp_path / "other")
    rig.calls.clear()
    assert _sweep(tmp_path, store_root, cal_path, "--no-archive-binaries")[0] == 0
    m = json.loads((_run_dir(store_root, "repl-run") / "manifest.json").read_text())
    assert not [c for c in rig.calls if c[1] in ("pack", "fetch")]
    assert m["builds"][0]["binaries"][0]["sha256"] and not m["builds"][0]["binaries"][0]["archived"]
    assert any("not archived" in g for g in m["gaps"])
    # the calibration came from another archive, so this one only has the file
    assert m["inputs"]["calibration"]["archive_run"] == json.loads(cal_path.read_text())["archive_run"]


def test_archive_off_writes_nothing(tmp_path, rig):
    _, cal_path, _ = _calibrate(tmp_path, "off")
    rc, out, _ = _sweep(tmp_path, "off", cal_path)
    assert rc == 0 and len(out.read_text().splitlines()) == 2
    assert "archive_run" not in json.loads(out.read_text().splitlines()[0])
    assert not (tmp_path / "off").exists()


# ------------------------------------------------- reading, reproducing, importing ----

def test_reproduce_md_names_hosts_tools_binaries_and_the_exact_command(tmp_path, rig):
    store_root = tmp_path / "arch"
    _, cal_path, _ = _calibrate(tmp_path, store_root)
    _, _, argv = _sweep(tmp_path, store_root, cal_path)
    d = _run_dir(store_root, "repl-run")
    m = json.loads((d / "manifest.json").read_text())
    doc = (d / "REPRODUCE.md").read_text()
    store = archive.Archive(store_root)
    sha = m["builds"][0]["binaries"][0]["sha256"]
    rel = '"$RUN"/' + os.path.relpath(store.object_path(sha), d)
    for want in (f"RUN={d}", "c8g.metal-24xl", "ami-0abc", "Amazon Linux release 2023.6.20241010",
                 "6.1.112-122.189.amzn2023.aarch64", "Neoverse-V2", "sudo dnf install -y ethtool gcc python3",
                 "| ethtool | 6.2-1.amzn2023 |", "sudo bash ~/offbench-src/scripts/pintune.sh",
                 '"name": "P", "ssh": "ec2-user@<address of the new primary host>"', "deploy --source-only --hosts P,R,G",
                 rel, f"~/offbench/builds/{COMMIT}/src/valkey-server", "~/base/src/valkey-cli",
                 "vendor/cachecannon/target/release/cachecannon", "OPT=-O3", "calibration.json",
                 "python3 -m offbench " + " ".join(argv[:1]), "--calibration data/repl/c2-calib.json",
                 "--out data/repl/c2-repro.jsonl", "repl-capacity data/repl/c2-repro.jsonl"):
        assert want in doc, want
    assert "/keys/bench.pem" not in doc.split("The original command")[1].split("With this checkout")[1]
    snap = m["harness"]["snapshot"]["object"]
    assert f"gzip -dc \"$RUN\"/{os.path.relpath(store.object_path(snap), d)} | tar -x -C offbench" in doc


def test_rewrite_argv_swaps_machine_paths_for_checkout_paths():
    argv = ["repl-run", "--calibration=/x/c.json", "--out", "/x/data/s.jsonl", "--key", "/k.pem",
            "--known-hosts", "/kh", "--archive", "/a", "--conns", "0,50", "--hosts-file", "/h.json"]
    assert reproduce.rewrite_argv(argv, "repl-run", "s") == [
        "repl-run", "--calibration", "data/repl/s-calib.json", "--out", "data/repl/s-repro.jsonl", "--conns", "0,50"]
    assert reproduce.rewrite_argv(["repl-calibrate", "--out", "/c.jsonl"], "repl-calibrate", "c") == [
        "repl-calibrate", "--out", "data/repl/c-repro.jsonl"]


def test_runs_cli_lists_shows_verifies_bundles_and_notes(tmp_path, rig, capsys):
    store_root = tmp_path / "arch"
    _, cal_path, _ = _calibrate(tmp_path, store_root)
    _sweep(tmp_path, store_root, cal_path)
    capsys.readouterr()
    runs = lambda *a: (cli.main(["runs", "--archive", str(store_root), *a]), capsys.readouterr().out)
    rc, out = runs("list")
    assert rc == 0 and out.count("\n") == 2 and "repl-calibrate" in out and f"a@{COMMIT[:9]}" in out
    assert runs("list", "--step", "sweep")[1].count("\n") == 1
    assert runs("list", "--commit", "2222")[1].startswith("no runs")
    sha = json.loads((_run_dir(store_root, "repl-run") / "manifest.json").read_text())["builds"][0]["binaries"][0]["sha256"]
    assert runs("list", "--binary", sha[:10], "--cores", "2")[1].count("\n") == 2
    rid = _run_dir(store_root, "repl-run").name
    rc, out = runs("show", rid[:-5])           # both runs start in the same second; the command makes the prefix unique
    for want in ("scenario  replica-read-capacity v1, step sweep", f"build     a: valkey {COMMIT}", sha[:16],
                 "OPT=-O3", "tool      cachecannon: 0.0.25-alpha.0", "primary  P: c8g.metal-24xl", "pintune yes",
                 "inputs    calibration cal-", "outputs   2 records, 2 valid, 0 errors"):
        assert want in out, want
    assert json.loads(runs("show", rid, "--json")[1])["id"] == rid
    assert runs("verify")[0] == 0 and runs("reproduce", rid)[1].startswith(f"# Reproduce {rid}")
    rc, out = runs("bundle", rid, str(tmp_path / "b.tar"))
    with tarfile.open(tmp_path / "b.tar") as t:
        names = t.getnames()
    assert f"runs/{rid}/manifest.json" in names and f"runs/{rid}/REPRODUCE.md" in names
    assert any(n.startswith("objects/") and sha in n for n in names) and "ARCHIVE.md" in names
    cid = _run_dir(store_root, "repl-calibrate").name
    assert runs("note", rid, "--supersedes", cid, "--text", "test supersede")[0] == 0
    assert "superseded by " + rid in runs("show", cid)[1] and "supersedes " + cid in runs("show", rid)[1]
    assert archive.verify(archive.Archive(store_root), store_root / "runs" / cid) == []   # notes live outside the run


def test_import_records_archives_an_old_run_and_lists_what_it_lacks(tmp_path):
    from offbench.cell import CLK_TCK
    store = archive.Archive(tmp_path / "arch")
    rec = {"kind": "replrw", "schema": 2, "run_id": "20261006T231108Z", "ts": "2026-10-06T23:12:00Z",
           "bin": {"name": "stock", "commit": "d" * 40, "replica_args": "", "primary_args": "", "share": 0},
           "cell": {"pipeline": 16, "conns": 50, "io_threads": 16, "pin_share": 0, "write_conns": 20,
                    "write_pipeline": 8, "p_conns": 42, "p_pipeline": 1},
           "rep": 1, "window_s": 6, "hosts": {"primary": "metal-2", "replica": "metal-7", "gen": "metal-8"},
           "rtt_ms": {"gen_primary": {"median_ms": 0.08}}, "valid": True, "invalid": [],
           "targets": {"write_kcmd_s": 1020.0, "read_kcmd_s": 294.0, "tol": 0.07},
           "calibration": {"id": "cal-20261006T231108Z"}}
    src = tmp_path / "pinned-c16-rerun.jsonl"
    src.write_text(json.dumps(rec) + "\n" + json.dumps(dict(rec, rep=2, ts="2026-10-06T23:13:00Z")) + "\n")
    cal = tmp_path / "pinned-c16-rerun-calib.json"
    cal.write_text(json.dumps({"id": "cal-20261006T231108Z", "converged": True,
                               "targets": {"write_kcmd_s": 1020.0, "read_kcmd_s": 294.0}}))
    run = archive.import_records(store, src, calibration=cal, harness_commit="732c61b", evidence="driver log line")
    m = store.manifest(run.dir)
    assert run.id == "20261006T231200Z-repl-run-pinned-c16-rerun-imported" and m["status"] == "imported"
    assert m["ended"] == "2026-10-06T23:13:00Z" and m["seconds"] == 60 and m["scenario"]["version"] is None
    assert any(g.startswith("scenario version not given") for g in m["gaps"])
    assert m["config"]["loads"]["stock"] == {"writers": "20xP8", "primary_readers": "42xP1", "share": 0,
                                             "replica_args": "", "primary_args": ""}
    assert m["builds"][0]["commit"] == "d" * 40 and not m["builds"][0]["binaries"]
    assert m["harness"] == {"name": "offbench", "commit": "732c61b", "evidence": "driver log line"}
    assert m["config"]["cores"] == [16] and m["config"]["calibration"] == "cal-20261006T231108Z"
    assert any("recorded before the archive" in g for g in m["gaps"])
    assert any(g.startswith("no result table") for g in m["gaps"])    # these records lack the metrics to judge
    assert (run.dir / "records.jsonl").read_bytes() == src.read_bytes() and (run.dir / "calibration.json").exists()
    assert "recorded before the archive existed" in (run.dir / "REPRODUCE.md").read_text()
    assert archive.verify(store, run.dir) == []
    with pytest.raises(archive.ArchiveError, match="already in the archive"):
        archive.import_records(store, src)


def test_harness_from_commit_snapshots_a_clean_checkout(tmp_path):
    root = _repo(tmp_path)
    commit = _git(root, "rev-parse", "HEAD")
    (root / "offbench/x.py").write_text("X = 9\n")             # not committed: not in the snapshot
    store = archive.Archive(tmp_path / "a").ensure()
    h = archive.harness_from_commit(root, commit[:7], store)
    assert h["commit"] == commit and h["dirty"] is False and h["code"]["offbench/x.py"] == _sha(b"X = 1\n")
    assert h["remote_branches_with_commit"] == []                  # the test repo has no remote
    stamp = json.loads(tarfile.open(fileobj=io.BytesIO(store.read_bytes(h["snapshot"]["object"]))).extractfile("HARNESS.json").read())
    assert stamp["commit"] == commit


def test_capture_failures_are_gaps_and_the_run_goes_on(tmp_path, rig, monkeypatch):
    real = rig.call

    def call(role, payload, timeout=120):
        if payload["action"] == "facts" and role.name == "G":
            raise repl.CellError("G facts: CellError: boom")
        return real(role, payload, timeout)
    monkeypatch.setattr(rig, "call", call)
    store_root = tmp_path / "arch"
    rc, cal_path, _ = _calibrate(tmp_path, store_root)
    m = json.loads((_run_dir(store_root, "repl-calibrate") / "manifest.json").read_text())
    assert rc == 0 and m["status"] == "complete" and m["rig"]["roles"]["gen"]["facts"] is None
    assert any(g.startswith("G (gen): host facts not collected") for g in m["gaps"])
    assert not m["tools"]["cachecannon"]["binaries"] and m["builds"][0]["binaries"][0]["archived"]
    monkeypatch.setattr(archive.Run, "capture", lambda self, *a, **k: 1 / 0)
    rc, _, _ = _sweep(tmp_path, store_root, cal_path)
    m = json.loads((_run_dir(store_root, "repl-run") / "manifest.json").read_text())
    assert rc == 0 and m["status"] == "complete" and m["outputs"]["records"] == 2
    assert any(g.startswith("host facts and binaries not captured: ZeroDivisionError") for g in m["gaps"])
