"""Pull per-host state back and fold it into the shared results.

``sync`` rsyncs each registered host's ``results.jsonl``, ``status.json`` and
queue dirs into ``data/hosts/<name>/``, then merges that host's results into the
shared ``data/results.jsonl`` (deduped on record_id, so re-syncing is a no-op).
``status`` reads each host's mirrored ``status.json`` and reports pending /
running / done plus the last heartbeat.

Read-only on the hosts: only rsync-from and never rsync-to. Every remote step is
built as an argv list (never string-interpolated) so a test can inspect the
commands without a network, matching deploy.py.
"""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

from .hosts import Host
from .merge import MergeResult, merge_host_results

# files pulled back from each host's --root
RESULTS_NAME = "results.jsonl"
STATUS_NAME = "status.json"
QUEUE_DIRS = ("queue", "running", "done", "failed")


def _ssh_opts(key: Optional[str], known_hosts: Optional[str],
             strict_host_key: bool = False) -> list[str]:
    opts: list[str] = ["-o", "BatchMode=yes"]
    if strict_host_key:
        opts += ["-o", "StrictHostKeyChecking=yes"]
    else:
        opts += ["-o", "StrictHostKeyChecking=accept-new"]
    if known_hosts:
        opts += ["-o", f"UserKnownHostsFile={known_hosts}"]
    if key:
        opts += ["-i", key]
    return opts


@dataclass
class SyncConfig:
    key: Optional[str] = None
    known_hosts: Optional[str] = None
    strict_host_key: bool = False

    def ssh_opts(self) -> list[str]:
        return _ssh_opts(self.key, self.known_hosts, self.strict_host_key)


def _remote_root(host: Host) -> str:
    return (host.remote_root or "~/offbench").rstrip("/")


def rsync_pull_cmd(cfg: SyncConfig, host: Host, rel: str, dest: Path,
                  is_dir: bool = False) -> list[str]:
    """rsync argv pulling ``<remote_root>/<rel>`` from ``host`` into ``dest``.

    For a directory (``is_dir``) a trailing slash is added to both sides so the
    contents land under ``dest`` rather than a nested dir.
    """
    ssh = "ssh " + " ".join(shlex.quote(o) for o in cfg.ssh_opts())
    src = f"{host.ssh}:{_remote_root(host)}/{rel}"
    dst = str(dest)
    if is_dir:
        src += "/"
        if not dst.endswith("/"):
            dst += "/"
    return [
        "rsync", "-az",
        "--exclude", "__pycache__", "--exclude", "*.pyc",
        "-e", ssh,
        src, dst,
    ]


@dataclass
class HostSyncResult:
    host: str
    ok: bool
    steps: list[str] = field(default_factory=list)
    merge: Optional[MergeResult] = None
    error: Optional[str] = None


def _run(cmd: Sequence[str], runner):
    return runner(list(cmd))


def _default_runner(cmd: list[str]):
    import subprocess
    return subprocess.run(cmd, capture_output=True, text=True)


def sync_host(
    host: Host,
    cfg: SyncConfig,
    data_dir: Path,
    runner=_default_runner,
) -> HostSyncResult:
    """Pull ``host``'s results/status/queue dirs into ``data/hosts/<name>/`` and
    merge its results into ``data/results.jsonl``.

    Missing files on a host are not fatal: a host that has produced nothing yet
    has no results.jsonl. The rsync of results is treated as best-effort; the
    merge only runs when the local mirror exists.
    """
    res = HostSyncResult(host=host.name, ok=False)
    data_dir = Path(data_dir)
    host_dir = data_dir / "hosts" / host.name
    host_dir.mkdir(parents=True, exist_ok=True)

    # results.jsonl (best effort: absent until the host runs something)
    cp = _run(rsync_pull_cmd(cfg, host, RESULTS_NAME, host_dir / RESULTS_NAME), runner)
    if cp.returncode == 0:
        res.steps.append(f"pulled {RESULTS_NAME}")
    else:
        res.steps.append(f"no {RESULTS_NAME} yet ({(cp.stderr or cp.stdout).strip()[:60]})")

    # status.json (best effort)
    cp = _run(rsync_pull_cmd(cfg, host, STATUS_NAME, host_dir / STATUS_NAME), runner)
    if cp.returncode == 0:
        res.steps.append(f"pulled {STATUS_NAME}")
    else:
        res.steps.append(f"no {STATUS_NAME} yet")

    # queue dirs (best effort)
    for d in QUEUE_DIRS:
        cp = _run(rsync_pull_cmd(cfg, host, d, host_dir / d, is_dir=True), runner)
        if cp.returncode == 0:
            res.steps.append(f"pulled {d}/")

    # merge results into the shared file (idempotent on record_id)
    host_results = host_dir / RESULTS_NAME
    if host_results.exists():
        try:
            mr = merge_host_results(host_results, host, data_dir / RESULTS_NAME)
            res.merge = mr
            res.steps.append(f"merged {mr.added} new / {mr.skipped} dup of {mr.total}")
        except Exception as exc:  # noqa: BLE001 - report, do not crash the whole sweep
            res.error = f"merge failed: {exc}"
            return res

    res.ok = True
    return res


def sync_hosts(
    hosts: Sequence[Host],
    cfg: SyncConfig,
    data_dir: Path,
    runner=_default_runner,
) -> list[HostSyncResult]:
    return [sync_host(h, cfg, data_dir, runner=runner) for h in hosts]


# ------------------------------------------------------------------ status ----

@dataclass
class HostStatus:
    host: str
    pending: Optional[int] = None
    running: Optional[int] = None
    done: Optional[int] = None
    failed: Optional[int] = None
    heartbeat: Optional[str] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _read_status_json(path: Path) -> dict:
    return json.loads(path.read_text())


def read_host_status(host: Host, data_dir: Path) -> HostStatus:
    """Read the mirrored ``data/hosts/<name>/status.json`` written by the last
    sync. Tolerant of the field names the runner may use for each count."""
    st = HostStatus(host=host.name)
    path = Path(data_dir) / "hosts" / host.name / STATUS_NAME
    if not path.exists():
        st.error = "no status.json (not synced yet)"
        return st
    try:
        doc = _read_status_json(path)
    except Exception as exc:  # noqa: BLE001
        st.error = f"unreadable status.json: {exc}"
        return st

    def pick(*names):
        for n in names:
            if n in doc and isinstance(doc[n], int):
                return doc[n]
        return None

    st.pending = pick("pending", "queued", "queue")
    st.running = pick("running", "in_progress")
    st.done = pick("done", "finished", "completed")
    st.failed = pick("failed", "errors")
    hb = doc.get("heartbeat") or doc.get("updated") or doc.get("ts") or doc.get("last_heartbeat")
    st.heartbeat = str(hb) if hb is not None else None
    return st


def status_hosts(hosts: Sequence[Host], data_dir: Path) -> list[HostStatus]:
    return [read_host_status(h, data_dir) for h in hosts]
