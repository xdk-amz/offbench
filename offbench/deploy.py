"""Push the offbench source to a metal host and start its worker.

One host at a time: rsync the project root (the inner ``offbench/`` package and
its sibling ``vendor/cachecannon`` source) to ``~/offbench-src``, make sure a
``~/valkey`` clone exists (origin = upstream, ``fork`` = the xdk-amz fork), then
launch the worker in a ``tmux`` session named ``offbench-worker``. The worker
runs ``cd ~/offbench-src && python3 -m offbench``, so the package resolves and
its ``parent.parent/vendor`` finds the vendored cachecannon. The worker's
Builder compiles that owned copy from source on first task; auth support is
baked into the source, so there is no deploy-time patch and no external
checkout.

A host running a live ``matrix`` tmux session is refused: that is a full-matrix
bash run in progress and a second server on the same box would contaminate it.

Pure stdlib. Every remote step is a shell snippet run over ssh, so the ssh/scp
commands are built as argv lists (never string-interpolated) and returned by
helpers that a test can inspect without a network.
"""

from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

from .hosts import Host

# cachecannon commit the vendored copy is pinned to (see vendor/cachecannon).
CC_COMMIT = "341254afe3d7596588cafdc230832dc57b14c5d0"

VALKEY_ORIGIN = "https://github.com/valkey-io/valkey.git"
VALKEY_FORK = "https://github.com/xdk-amz/valkey.git"

WORKER_SESSION = "offbench-worker"
MATRIX_SESSION = "matrix"

# Vendored cachecannon source, relative to the synced package on the host.
VENDOR_CACHECANNON_REL = "vendor/cachecannon"


@dataclass
class SshConfig:
    """Everything the ssh/scp/rsync commands need beyond the host itself."""
    key: Optional[str] = None
    known_hosts: Optional[str] = None
    src_dest: str = "~/offbench-src"          # where the package lands on the host
    remote_root: str = "~/offbench"           # the worker's --root
    repo: str = "~/valkey"                    # local valkey clone the runner archives from
    cli: str = "~/base/src/valkey-cli"
    # cachecannon is the owned copy under {src_dest}/vendor/cachecannon; the
    # worker builds and invokes it, so no host-level cachecannon path is set.
    cachecannon: Optional[str] = None
    strict_host_key: bool = False             # StrictHostKeyChecking

    def ssh_opts(self) -> list[str]:
        opts: list[str] = ["-o", "BatchMode=yes"]
        if self.strict_host_key:
            opts += ["-o", "StrictHostKeyChecking=yes"]
        else:
            opts += ["-o", "StrictHostKeyChecking=accept-new"]
        if self.known_hosts:
            opts += ["-o", f"UserKnownHostsFile={self.known_hosts}"]
        if self.key:
            opts += ["-i", self.key]
        return opts

    def worker_cachecannon(self) -> str:
        """The owned vendored cachecannon binary path on the host."""
        base = self.src_dest.rstrip("/")
        return f"{base}/{VENDOR_CACHECANNON_REL}/target/release/cachecannon"


def ssh_cmd(cfg: SshConfig, host: Host, remote_script: str) -> list[str]:
    """ssh argv that runs ``remote_script`` under ``bash -lc`` on the host."""
    return ["ssh", *cfg.ssh_opts(), host.ssh, "bash -lc " + shlex.quote(remote_script)]


def rsync_cmd(cfg: SshConfig, host: Host, local_src: Path, dest: str) -> list[str]:
    """rsync argv pushing ``local_src`` (a dir, trailing slash added) to ``dest``."""
    ssh = "ssh " + " ".join(shlex.quote(o) for o in cfg.ssh_opts())
    src = str(local_src)
    if not src.endswith("/"):
        src += "/"
    return [
        "rsync", "-az", "--delete",
        "--exclude", "__pycache__", "--exclude", ".pytest_cache",
        "--exclude", "*.pyc", "--exclude", ".git",
        # runtime/local-only root dirs and secrets: the worker's --root is a
        # separate remote dir, so results/dashboards/registries never ship.
        "--exclude", "data", "--exclude", "dash",
        "--exclude", "hosts.json", "--exclude", "known_hosts",
        # the vendored cachecannon build output rebuilds on the host.
        "--exclude", "vendor/cachecannon/target",
        "-e", ssh,
        src, f"{host.ssh}:{dest}",
    ]


def ensure_clone_script(cfg: SshConfig) -> str:
    """Remote snippet: create the valkey clone with both remotes if it is absent,
    otherwise make sure the remotes are set the way the runner expects."""
    repo = cfg.repo
    return (
        f"set -eu; "
        f"if [ ! -d {repo}/.git ]; then "
        f"  git clone -q {VALKEY_ORIGIN} {repo}; "
        f"  git -C {repo} remote add fork {VALKEY_FORK} 2>/dev/null || "
        f"    git -C {repo} remote set-url fork {VALKEY_FORK}; "
        f"else "
        f"  git -C {repo} remote set-url origin {VALKEY_ORIGIN}; "
        f"  git -C {repo} remote add fork {VALKEY_FORK} 2>/dev/null || "
        f"    git -C {repo} remote set-url fork {VALKEY_FORK}; "
        f"fi; "
        f"git -C {repo} remote -v"
    )


def vendored_cachecannon_check_script(cfg: SshConfig) -> str:
    """Remote snippet asserting the vendored cachecannon source is present with
    auth support integrated. Prints ``VENDOR_OK`` when the Cargo.toml and the
    auth_user config field are both present, ``VENDOR_MISSING`` otherwise."""
    base = cfg.src_dest.rstrip("/")
    src = f"{base}/{VENDOR_CACHECANNON_REL}"
    mod = f"{src}/src/config/mod.rs"
    return (
        f"if [ -f {src}/Cargo.toml ] "
        f"&& [ \"$(grep -c auth_user {mod} 2>/dev/null || echo 0)\" -gt 0 ]; "
        f"then echo VENDOR_OK; else echo VENDOR_MISSING; fi"
    )


def worker_start_script(cfg: SshConfig) -> str:
    """Remote snippet: start the worker in tmux session ``offbench-worker`` unless
    one is already running. Refuses if a ``matrix`` session is live.

    The worker is given the owned vendored cachecannon binary path; its Builder
    compiles that source (auth baked in) on first task if the binary is absent.
    """
    run = (
        f"cd {cfg.src_dest} && "
        f"python3 -m offbench --root {cfg.remote_root} run "
        f"--cachecannon {cfg.worker_cachecannon()} "
        f"--cli {cfg.cli} "
        f"--repo {cfg.repo}"
    )
    return (
        f"set -eu; "
        f"if tmux has-session -t {MATRIX_SESSION} 2>/dev/null; then "
        f"  echo MATRIX_LIVE; exit 3; "
        f"fi; "
        f"if tmux has-session -t {WORKER_SESSION} 2>/dev/null; then "
        f"  echo WORKER_ALREADY_RUNNING; "
        f"else "
        f"  tmux new-session -d -s {WORKER_SESSION} {shlex.quote(run)}; "
        f"  echo WORKER_STARTED; "
        f"fi"
    )


def has_matrix_session_script() -> str:
    return (
        f"if tmux has-session -t {MATRIX_SESSION} 2>/dev/null; "
        f"then echo MATRIX_LIVE; else echo NO_MATRIX; fi"
    )


@dataclass
class HostDeployResult:
    host: str
    ok: bool
    steps: list[str] = field(default_factory=list)   # human log lines
    error: Optional[str] = None
    skipped: bool = False                             # matrix live -> refused


def _run(cmd: Sequence[str], runner) -> subprocess.CompletedProcess:
    return runner(list(cmd))


def _default_runner(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def deploy_host(
    host: Host,
    cfg: SshConfig,
    package_dir: Path,
    runner=_default_runner,
) -> HostDeployResult:
    """Deploy the offbench source and start the worker on ``host``.

    ``package_dir`` is the local project root to rsync: it holds the inner
    ``offbench/`` package and its sibling ``vendor/cachecannon`` source, so the
    synced ``~/offbench-src`` contains both. ``runner`` runs one argv list and
    returns a CompletedProcess; the default shells out, tests inject a fake so
    no network is touched. Steps run in order and abort on the first non-zero
    exit; a live ``matrix`` session short circuits to a skipped (refused)
    result.
    """
    res = HostDeployResult(host=host.name, ok=False)

    # 0. refuse up-front if a full-matrix run is live on the host
    cp = _run(ssh_cmd(cfg, host, has_matrix_session_script()), runner)
    if cp.returncode != 0:
        res.error = f"probe matrix session failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    if "MATRIX_LIVE" in (cp.stdout or ""):
        res.skipped = True
        res.error = "refused: live 'matrix' tmux session on host"
        res.steps.append("matrix session live -> refused")
        return res
    res.steps.append("no live matrix session")

    # 1. rsync the package (incl. vendor/cachecannon source) to ~/offbench-src
    cp = _run(rsync_cmd(cfg, host, package_dir, cfg.src_dest), runner)
    if cp.returncode != 0:
        res.error = f"rsync source failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    res.steps.append(f"synced source -> {cfg.src_dest}")

    # 2. ensure the valkey clone with both remotes
    cp = _run(ssh_cmd(cfg, host, ensure_clone_script(cfg)), runner)
    if cp.returncode != 0:
        res.error = f"ensure clone failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    res.steps.append(f"valkey clone ready at {cfg.repo}")

    # 3. assert the owned vendored cachecannon source is present with auth
    #    support baked in (no deploy-time patch, no external checkout). The
    #    worker's Builder compiles it lazily on first task.
    cp = _run(ssh_cmd(cfg, host, vendored_cachecannon_check_script(cfg)), runner)
    if cp.returncode != 0:
        res.error = f"vendored cachecannon check failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    if "VENDOR_OK" not in (cp.stdout or ""):
        res.error = (
            f"vendored cachecannon missing or lacks auth support under "
            f"{cfg.src_dest.rstrip('/')}/{VENDOR_CACHECANNON_REL}"
        )
        return res
    res.steps.append("vendored cachecannon present (auth baked in, builds on host)")

    # 4. start the worker in tmux (refuses again if matrix appeared meanwhile)
    cp = _run(ssh_cmd(cfg, host, worker_start_script(cfg)), runner)
    if cp.returncode == 3 or "MATRIX_LIVE" in (cp.stdout or ""):
        res.skipped = True
        res.error = "refused: live 'matrix' tmux session on host"
        res.steps.append("matrix session live at worker start -> refused")
        return res
    if cp.returncode != 0:
        res.error = f"worker start failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    started = "WORKER_STARTED" in (cp.stdout or "")
    res.steps.append("started worker in tmux 'offbench-worker'" if started
                     else "worker already running in tmux 'offbench-worker'")

    res.ok = True
    return res


def deploy_hosts(
    hosts: Sequence[Host],
    cfg: SshConfig,
    package_dir: Path,
    runner=_default_runner,
) -> list[HostDeployResult]:
    """Deploy to each host in turn (serial: they may share a binary/port)."""
    return [deploy_host(h, cfg, package_dir, runner=runner) for h in hosts]


def sync_source(host: Host, cfg: SshConfig, package_dir: Path, runner=_default_runner) -> HostDeployResult:
    """Only the rsync step: the code repl-run's host side runs, without a valkey clone or a worker, so the
    host does not look busy afterwards."""
    res = HostDeployResult(host=host.name, ok=False)
    cp = _run(rsync_cmd(cfg, host, package_dir, cfg.src_dest), runner)
    if cp.returncode != 0:
        res.error = f"rsync source failed: {(cp.stderr or cp.stdout).strip()}"
        return res
    res.steps.append(f"synced source -> {cfg.src_dest}")
    res.ok = True
    return res
