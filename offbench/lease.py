"""Host leases for the commands that use hosts, through the shared hosts tool.

``hosts.py claim`` keeps hosts the caller already holds, leases the free ones and refuses when another
session holds any of them. ``held`` ties the new leases to this process for the duration of one
command; ``claim(..., job_pid=None)`` takes session leases for work that outlives the command (a
dispatched task, a deployed worker). Leases exist only for hosts from the shared registry: with a
local hosts file, or without the tool, commands run unleased with a warning.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from .hosts import shared_path


class LeaseError(RuntimeError):
    pass


def tool() -> Path:
    return Path(os.environ.get("HOSTS_TOOL") or Path.home() / ".kiro/crew/skills/hosts/hosts.py")


def claim(names: list[str], label: str, *, registry: Path, job_pid: Optional[int] = None,
          probe: bool = True) -> list[str]:
    """Lease the named hosts; returns the ones newly taken. probe refuses hosts with work already running."""
    if Path(registry).resolve() != shared_path().resolve():
        print(f"warning: hosts come from {registry}, not the shared registry; running without leases", file=sys.stderr)
        return []
    if not tool().exists():
        print(f"warning: {tool()} not found, running without host leases", file=sys.stderr)
        return []
    cmd = [sys.executable, str(tool()), "claim", "--hosts", ",".join(names), "--label", label]
    if job_pid:
        cmd += ["--pid", str(job_pid)]
    if probe:
        cmd.append("--probe")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        msg = r.stderr.strip() or f"hosts.py claim exited {r.returncode}"
        if r.returncode == 3:
            msg += (f"\nif that work is yours: hosts.py acquire {','.join(names)} --ignore-busy --label ..."
                    ", then rerun")
        raise LeaseError(msg)
    return json.loads(r.stdout.strip().splitlines()[-1])["taken"]


def release(names: list[str], job_pid: int) -> None:
    if names and tool().exists():
        subprocess.run([sys.executable, str(tool()), "release", ",".join(names), "--pid", str(job_pid), "--quiet"],
                       capture_output=True, text=True, timeout=60)


@contextmanager
def held(names: list[str], label: str, *, registry: Path, probe: bool = True):
    """Hold the hosts while the block runs; leases taken here are released after it (or lapse with the process)."""
    pid = os.getpid()
    taken = claim(names, label, registry=registry, job_pid=pid, probe=probe)
    try:
        yield taken
    finally:
        release(taken, pid)
