"""Split one task across several hosts and push each subtask to its host's queue.

``dispatch`` partitions a Task into one subtask per host (queue.Task.partition),
so every cell (all reps and its interleaved A/B pair) stays whole on one host,
then writes each subtask as ``task_<id>.json`` into that host's remote
``<remote_root>/queue/`` via a tmp + fsync + rename on the host. Assignment is
positional: subtask k goes to host k.

Pure stdlib. Every remote step is an argv list (never string-interpolated) so a
test can inspect the commands and the local subtask JSON without a network.
"""

from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

from .hosts import Host
from .queue import Task


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
class DispatchConfig:
    key: Optional[str] = None
    known_hosts: Optional[str] = None
    strict_host_key: bool = False

    def ssh_opts(self) -> list[str]:
        return _ssh_opts(self.key, self.known_hosts, self.strict_host_key)


def _remote_root(host: Host) -> str:
    return (host.remote_root or "~/offbench").rstrip("/")


def mkqueue_cmd(cfg: DispatchConfig, host: Host) -> list[str]:
    """ssh argv that makes sure ``<remote_root>/queue`` exists on the host."""
    root = _remote_root(host)
    return ["ssh", *cfg.ssh_opts(), host.ssh,
            "bash -lc " + shlex.quote(f"mkdir -p {root}/queue")]


def scp_task_cmd(cfg: DispatchConfig, host: Host, local_path: Path, task_id: str) -> list[str]:
    """scp argv pushing a local subtask JSON to a ``.task_<id>.tmp`` on the host."""
    root = _remote_root(host)
    dest = f"{host.ssh}:{root}/queue/.task_{task_id}.json.tmp"
    return ["scp", *cfg.ssh_opts(), str(local_path), dest]


def install_task_cmd(cfg: DispatchConfig, host: Host, task_id: str) -> list[str]:
    """ssh argv that fsyncs and atomically renames the staged tmp into place.

    Mirrors queue.Queue.import_document's durability on the remote side so a
    half-copied task never heads the queue.
    """
    root = _remote_root(host)
    tmp = f"{root}/queue/.task_{task_id}.json.tmp"
    final = f"{root}/queue/task_{task_id}.json"
    script = (
        f"set -eu; "
        f"tmp={shlex.quote(tmp)}; final={shlex.quote(final)}; "
        # fsync the file then rename atomically; sync the dir so the rename lands.
        # expanduser in python so a '~' remote_root is resolved (scp expands it,
        # os.replace does not).
        f"python3 - \"$tmp\" \"$final\" <<'PY'\n"
        f"import os, sys\n"
        f"tmp = os.path.expanduser(sys.argv[1]); final = os.path.expanduser(sys.argv[2])\n"
        f"fd = os.open(tmp, os.O_RDONLY); os.fsync(fd); os.close(fd)\n"
        f"os.replace(tmp, final)\n"
        f"d = os.open(os.path.dirname(final), os.O_RDONLY); os.fsync(d); os.close(d)\n"
        f"PY"
    )
    return ["ssh", *cfg.ssh_opts(), host.ssh, "bash -lc " + shlex.quote(script)]


@dataclass
class HostDispatch:
    host: str
    task_id: str
    part: Optional[str]
    ok: bool = False
    steps: list[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class DispatchResult:
    parent_task_id: str
    subtasks: list[Task]
    hosts: list[HostDispatch] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.hosts) and all(h.ok for h in self.hosts)


def check_invariants(task: Task, subtasks: Sequence[Task], hosts: Sequence[Host]) -> None:
    """Raise ValueError unless the partition is a clean per-host split.

    - one subtask per host (positional assignment),
    - every subtask carries parent_task_id == task.task_id and part == 'k/n',
    - no cell (hence no interleaved A/B pair or rep set) is split across hosts:
      the subtasks' cell keys are disjoint, and together they cover exactly the
      parent's intended cell space.

    The parent's intended space is expanded across an acl span the parent
    encodes as acl == -1 (meaning "both"), so an acl=-1 parent compares against
    the acl=0 and acl=1 children rather than against a bogus acl=-1 cell key.
    """
    if len(subtasks) != len(hosts):
        raise ValueError(
            f"partition produced {len(subtasks)} subtask(s) for {len(hosts)} host(s); "
            f"cannot assign one subtask per host without splitting a cell"
        )
    n = len(subtasks)
    acl_span = [0, 1] if task.acl < 0 else [task.acl]
    parent_keys: list[str] = []
    for acl in acl_span:
        for cell in task.cells():
            cell.acl = acl
            parent_keys.append(cell.key())
    parent_keys.sort()

    seen: set[str] = set()
    union: list[str] = []
    for k, sub in enumerate(subtasks, start=1):
        if sub.parent_task_id != task.task_id:
            raise ValueError(f"subtask {k} missing parent_task_id")
        if sub.part != f"{k}/{n}":
            raise ValueError(f"subtask {k} has part={sub.part!r}, expected {k}/{n!r}")
        keys = [c.key() for c in sub.cells()]
        for key in keys:
            if key in seen:
                raise ValueError(f"cell {key} appears on more than one host (cell split)")
            seen.add(key)
        union += keys
    if sorted(union) != parent_keys:
        raise ValueError("union of subtask cells does not equal parent cells")


def _default_runner(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def dispatch_task(
    task: Task,
    hosts: Sequence[Host],
    cfg: DispatchConfig,
    stage_dir: Optional[Path] = None,
    runner=_default_runner,
) -> DispatchResult:
    """Partition ``task`` across ``hosts`` and push each subtask to its host queue.

    Splits into ``len(hosts)`` subtasks, verifies the split invariants (raising
    before touching any host if they fail), writes each subtask JSON locally
    under ``stage_dir`` (default: data/dispatch/<parent_id>/), and copies it into
    that host's ``queue/`` with a fsync + atomic rename. Host k receives subtask
    k. Serial across hosts; a failure on one host leaves the others' subtasks in
    place and is reported per host.
    """
    if not hosts:
        raise ValueError("dispatch needs at least one host")

    subtasks = task.partition(len(hosts))
    check_invariants(task, subtasks, hosts)

    # partition() mints a fresh id per subtask, but ids are millisecond
    # timestamps: subtasks built in the same millisecond collide. Make them
    # unique so each host's queue file (and the local stage) never clobbers a
    # sibling.
    seen_ids: set[str] = set()
    for k, sub in enumerate(subtasks, start=1):
        if sub.task_id in seen_ids:
            sub.task_id = f"{sub.task_id}-p{k}"
        seen_ids.add(sub.task_id)

    stage = Path(stage_dir) if stage_dir else Path("data") / "dispatch" / task.task_id
    stage.mkdir(parents=True, exist_ok=True)

    result = DispatchResult(parent_task_id=task.task_id, subtasks=list(subtasks))

    for host, sub in zip(hosts, subtasks):
        hd = HostDispatch(host=host.name, task_id=sub.task_id, part=sub.part)
        local = stage / f"task_{sub.task_id}.json"
        _write_atomic(local, sub.to_dict())
        hd.steps.append(f"staged subtask {sub.part} -> {local}")

        cp = runner(mkqueue_cmd(cfg, host))
        if cp.returncode != 0:
            hd.error = f"mkdir queue failed: {(cp.stderr or cp.stdout).strip()}"
            result.hosts.append(hd)
            continue
        cp = runner(scp_task_cmd(cfg, host, local, sub.task_id))
        if cp.returncode != 0:
            hd.error = f"scp subtask failed: {(cp.stderr or cp.stdout).strip()}"
            result.hosts.append(hd)
            continue
        cp = runner(install_task_cmd(cfg, host, sub.task_id))
        if cp.returncode != 0:
            hd.error = f"install subtask failed: {(cp.stderr or cp.stdout).strip()}"
            result.hosts.append(hd)
            continue
        hd.ok = True
        hd.steps.append(f"queued task_{sub.task_id}.json on {host.name} ({_remote_root(host)}/queue)")
        result.hosts.append(hd)

    return result


def _write_atomic(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    import os
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
