"""Runner: one worker on a metal host, FIFO over the queue directory.

Boundary-only bookkeeping (status.json, queue moves) so nothing touches the
host during a measurement window. A task's per-rep records are appended to
results.jsonl as they land, so a crash mid-task loses nothing already measured.

The Builder produces both binaries the runner needs on the host: the
valkey-server under test (git archive of a commit, plain make, cached by
commit) and the vendored cachecannon load generator (cargo --release, cached).
Cachecannon is owned in-tree at offbench/vendor/cachecannon with auth support
already in source, so the runner never patches or depends on an external
checkout.

Zero-server / zero-cachecannon is asserted at every rep boundary (in cell.py)
and again at each task boundary here, so a leaked process from a prior run can
never contaminate a measurement and none is left billing after one.
"""

from __future__ import annotations

import json
import os
import platform
import socket
import subprocess
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .cell import CellError, Paths, assert_none, kill_matching, run_rep
from .queue import Queue, Task
from .schema import Bin, Record, append_records

# Vendored cachecannon lives in-tree, pinned in source with auth support.
VENDOR_CACHECANNON = Path(__file__).resolve().parent.parent / "vendor" / "cachecannon"
CACHECANNON_REL = Path("target") / "release" / "cachecannon"

Log = Callable[[str], None]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _imds(token: str, field: str) -> Optional[str]:
    try:
        out = subprocess.run(
            ["curl", "-s", "-m", "1", "-H", f"X-aws-ec2-metadata-token: {token}",
             f"http://169.254.169.254/latest/meta-data/{field}"],
            capture_output=True, text=True).stdout
        return out or None
    except Exception:
        return None


def host_identity() -> dict:
    inst = itype = az = None
    try:
        # IMDSv2 (metadata service); harmless when not on EC2.
        tok = subprocess.run(
            ["curl", "-s", "-m", "1", "-X", "PUT", "http://169.254.169.254/latest/api/token",
             "-H", "X-aws-ec2-metadata-token-ttl-seconds: 60"], capture_output=True, text=True).stdout
        if tok:
            inst = _imds(tok, "instance-id")
            itype = _imds(tok, "instance-type")
            az = _imds(tok, "placement/availability-zone")
    except Exception:
        pass
    return {"id": inst, "type": itype, "az": az, "kernel": platform.release(),
            "hostname": socket.gethostname(), "ncpu": os.cpu_count()}


def text_bytes(binary: Path) -> Optional[int]:
    """.text section size, a build-parity fingerprint stored on the Bin."""
    try:
        out = subprocess.run(["size", "-A", str(binary)], capture_output=True, text=True).stdout
    except Exception:
        return None
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == ".text":
            try:
                return int(parts[1])
            except ValueError:
                return None
    return None


class Builder:
    """Build the host's binaries: valkey-server by commit, cachecannon vendored.

    Server: ``git archive`` a commit from a local clone, plain ``make`` (no
    CFLAGS override so numbers stay comparable), cached under ``builds/<key>``.
    Cachecannon: ``cargo build --release`` of the in-tree vendored source, built
    once and cached at its own ``target/release/cachecannon``.
    """

    def __init__(self, repo: Path, builds: Path, legacy_root: Path,
                 cachecannon_src: Path = VENDOR_CACHECANNON, jobs: int = 0):
        self.repo, self.builds, self.legacy_root = Path(repo), Path(builds), Path(legacy_root)
        self.cachecannon_src = Path(cachecannon_src)
        self.jobs = jobs or (os.cpu_count() or 8)

    # ---- server -----------------------------------------------------------
    def resolve(self, b: Bin, log: Log = print) -> Path:
        if b.commit:
            return self.build(b, log)
        legacy = self.legacy_root / b.name / "src" / "valkey-server"
        if legacy.exists():
            return legacy
        raise CellError(f"bin {b.name}: no commit and no {legacy}")

    def build(self, b: Bin, log: Log = print) -> Path:
        key = b.commit + (("-" + "".join(ch for ch in b.make_args if ch.isalnum())) if b.make_args else "")
        dst = self.builds / key
        binary = dst / "src" / "valkey-server"
        if binary.exists():
            if b.text_bytes is None:
                b.text_bytes = text_bytes(binary)
            return binary
        log(f"building server {b.name} @ {b.commit} -> {dst}")
        dst.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "-C", str(self.repo), "fetch", "--all", "-q"], check=False)
        archive = subprocess.run(["git", "-C", str(self.repo), "archive", b.commit],
                                 capture_output=True, check=True)
        subprocess.run(["tar", "-x", "-C", str(dst)], input=archive.stdout, check=True)
        env = {k: v for k, v in os.environ.items()
               if k not in ("CFLAGS", "CPPFLAGS", "CXXFLAGS", "LDFLAGS")}
        cmd = ["make", f"-j{self.jobs}", *([b.make_args] if b.make_args else []), "valkey-server"]
        with (dst / "build.log").open("w") as f:
            subprocess.run(cmd, cwd=dst, stdout=f, stderr=subprocess.STDOUT, env=env, check=True)
        if not binary.exists():
            raise CellError(f"server build for {b.name} @ {b.commit} produced no {binary}")
        b.text_bytes = text_bytes(binary)
        return binary

    # ---- cachecannon ------------------------------------------------------
    def resolve_cachecannon(self, log: Log = print) -> Path:
        """Build (once) and return the vendored cachecannon release binary."""
        binary = self.cachecannon_src / CACHECANNON_REL
        if binary.exists():
            return binary
        src = self.cachecannon_src
        if not (src / "Cargo.toml").exists():
            raise CellError(f"vendored cachecannon source missing at {src} (expected Cargo.toml)")
        log(f"building cachecannon (vendored) @ {src}")
        env = dict(os.environ)
        cargo_bin = Path.home() / ".cargo" / "bin"
        if cargo_bin.exists():
            env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"
        with (src / "build.log").open("w") as f:
            subprocess.run(["cargo", "build", "--release", "-q"],
                           cwd=src, stdout=f, stderr=subprocess.STDOUT, env=env, check=True)
        if not binary.exists():
            raise CellError(f"cachecannon build produced no {binary}")
        return binary


class Runner:
    def __init__(self, root: Path, repo: Path, cli: Path,
                 cachecannon: Optional[Path] = None,
                 cachecannon_src: Path = VENDOR_CACHECANNON,
                 legacy_root: Optional[Path] = None,
                 poll_s: float = 5.0, dry_run: bool = False):
        self.root = Path(root)
        self.queue = Queue(self.root)
        self.results = self.root / "results.jsonl"
        self.builder = Builder(repo, self.root / "builds", legacy_root or Path.home(),
                               cachecannon_src=cachecannon_src)
        # A prebuilt cachecannon path may be passed in; otherwise the builder
        # produces the vendored one lazily on first task.
        self.cachecannon: Optional[Path] = Path(cachecannon) if cachecannon else None
        self.cli = Path(cli)
        self.poll_s, self.dry_run = poll_s, dry_run
        self.host = host_identity()
        self.log_path = self.root / "runner.log"

    def log(self, msg: str) -> None:
        line = f"{now()} {msg}"
        print(line, flush=True)
        with self.log_path.open("a") as f:
            f.write(line + "\n")

    # ---- durable status ---------------------------------------------------
    def status(self, state: str, task: Optional[Task] = None, **extra) -> None:
        doc = {"state": state, "ts": now(), "host": self.host,
               "task": task.short() if task else None,
               "task_id": task.task_id if task else None,
               "pending": len(self.queue.list()), **extra}
        tmp = self.root / "status.json.tmp"
        tmp.write_text(json.dumps(doc, indent=1))
        tmp.replace(self.root / "status.json")

    # ---- invariant --------------------------------------------------------
    def assert_clean(self, when: str, binaries: List[Path], cachecannon: Path) -> None:
        """Guarantee zero of THIS task's binaries at a task boundary.

        cell.run_rep asserts this at every rep, matched on the exact binary
        path (``^<path>``); this re-asserts it around the whole task so a
        process leaked by a crashed prior run is killed and caught here rather
        than silently contaminating the first rep. Matching is anchored to the
        concrete server/cachecannon binary paths only, never a bare
        ``valkey-server``/``cachecannon`` name: a name match over ``pgrep -f``
        would also hit any unrelated shell whose command line merely mentions
        the string (including this worker's own launcher).
        """
        for pat in self._proc_patterns(binaries, cachecannon):
            kill_matching(pat)
        for pat in self._proc_patterns(binaries, cachecannon):
            assert_none(pat, f"{Path(pat[1:]).name} ({when})")

    @staticmethod
    def _proc_patterns(binaries: List[Path], cachecannon: Path) -> List[str]:
        seen: Dict[str, None] = {}
        for p in [cachecannon, *binaries]:
            seen[f"^{p}"] = None
        return list(seen)

    def _resolve_cachecannon(self) -> Path:
        if self.cachecannon and Path(self.cachecannon).exists():
            return Path(self.cachecannon)
        if self.dry_run:
            return self.cachecannon or (self.builder.cachecannon_src / CACHECANNON_REL)
        self.cachecannon = self.builder.resolve_cachecannon(self.log)
        return self.cachecannon

    def _cc_version(self, cachecannon: Path) -> Optional[str]:
        try:
            return subprocess.run([str(cachecannon), "--version"], capture_output=True,
                                  text=True, timeout=5).stdout.strip() or None
        except Exception:
            return None

    # ---- one task ---------------------------------------------------------
    def run_task(self, task: Task) -> None:
        cachecannon = self._resolve_cachecannon()
        if self.dry_run:
            binaries: Dict[str, Path] = {
                b.name: Path("/dry-run") / (b.commit or b.name) / "valkey-server" for b in task.bins}
        else:
            binaries = {b.name: self.builder.resolve(b, self.log) for b in task.bins}
        cc_version = self._cc_version(cachecannon) if not self.dry_run else None

        if not self.dry_run:
            self.assert_clean("task start", list(binaries.values()), cachecannon)

        plan = task.plan()
        self.status("running", task, progress=0, total=len(plan))
        try:
            for i, (cell, b, rep) in enumerate(plan, 1):
                paths = Paths(server=binaries[b.name], cli=self.cli, cachecannon=cachecannon,
                              workdir=self.root / "work")
                t0 = time.time()
                try:
                    metrics = run_rep(paths, cell, window_s=task.window_s, perf=task.perf,
                                      dry_run=self.dry_run, log=self.log)
                except CellError as exc:
                    self.log(f"[{i}/{len(plan)}] {b.name} {cell.key()} rep{rep}: CELL ERROR {exc}")
                    raise
                rec = Record(bin=b, cell=cell, rep=rep, metrics=metrics, generator="cachecannon",
                             generator_version=cc_version, ts=now(), task_id=task.task_id, note=task.note,
                             source={"kind": "runner", "dry_run": self.dry_run,
                                     "part": task.part, "parent_task_id": task.parent_task_id},
                             host=self.host)
                append_records(self.results, [rec])   # durable: each rep survives a mid-task crash
                why = "" if rec.valid else "  INVALID: " + "; ".join(rec.invalid_reasons())
                self.log(f"[{i}/{len(plan)}] {b.name:<12} {cell.key():<24} rep{rep}: "
                         f"{metrics.kcmd_s:7.0f} k cmd/s  main_active={metrics.main_active_pct or 0:5.1f}%  "
                         f"({time.time() - t0:.0f}s){why}")
                self.status("running", task, progress=i, total=len(plan), last=rec.to_dict())
        finally:
            if not self.dry_run:
                self.assert_clean("task end", list(binaries.values()), cachecannon)

    # ---- loop -------------------------------------------------------------
    def loop(self, once: bool = False) -> None:
        self.log(f"runner up on {self.host.get('hostname')} root={self.root} dry_run={self.dry_run}")
        while True:
            task = self.queue.next()
            if task is None:
                self.status("idle")
                if once:
                    return
                time.sleep(self.poll_s)
                continue
            self.queue.claim(task)
            self.log(f"start {task.task_id}: {task.short()}")
            try:
                self.run_task(task)
            except Exception as exc:  # record and move on; never wedge the queue
                err = f"{type(exc).__name__}: {exc}"
                self.log(f"FAILED {task.task_id}: {err}\n{traceback.format_exc()}")
                self.queue.finish(task, ok=False, error=err)
                self.status("failed", task, error=err)
            else:
                self.queue.finish(task, ok=True)
                self.log(f"done {task.task_id}")
                self.status("completed", task)
            if once:
                return
