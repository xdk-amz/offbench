"""What a host is and what it runs, for the run archive. repl-agent's facts action calls collect() on every host
before the first rep of an archived run.

Everything here only reads, and is best-effort: a missing tool, file or metadata service gives None, never an
exception, so collecting facts cannot fail a run. Pure stdlib; runs on the hosts.
"""

from __future__ import annotations

import getpass
import os
import platform
import re
import resource
import shutil
import socket
import subprocess
import time
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Optional

from .packing import sha256_file

# Packages the harness needs on a host, or whose version shapes a measurement (rpm -q).
PACKAGES = ("system-release", "kernel", "glibc", "gcc", "make", "binutils", "git", "python3", "util-linux",
            "procps-ng", "iputils", "ethtool", "chrony", "irqbalance", "tmux", "rsync", "tar", "gzip",
            "openssl-libs", "systemd", "perf")
SYSCTLS = ("net.core.netdev_max_backlog", "net.core.somaxconn", "net.ipv4.tcp_max_syn_backlog",
           "net.core.rps_sock_flow_entries", "net.ipv4.ip_local_port_range", "net.core.rmem_max",
           "net.core.wmem_max", "net.ipv4.tcp_rmem", "net.ipv4.tcp_wmem", "net.ipv4.tcp_congestion_control",
           "vm.overcommit_memory", "vm.swappiness", "kernel.numa_balancing", "fs.file-max", "fs.nr_open")
# Tool -> arguments that print its version.
TOOLS = {"gcc": ["--version"], "make": ["--version"], "git": ["--version"], "readelf": ["--version"],
         "ethtool": ["--version"], "taskset": ["--version"], "ping": ["-V"], "tmux": ["-V"], "rsync": ["--version"],
         "chronyc": ["--version"], "perf": ["--version"], "cargo": ["--version"], "rustc": ["-V"],
         "python3": ["--version"], "ssh": ["-V"]}
LSCPU_KEYS = ("Architecture", "Vendor ID", "Model name", "Model", "Stepping", "CPU(s)", "Thread(s) per core",
              "Core(s) per socket", "Socket(s)", "NUMA node(s)", "L1d cache", "L1i cache", "L2 cache", "L3 cache",
              "BogoMIPS")
IMDS = "http://169.254.169.254/latest"
IMDS_FIELDS = (("instance_id", "instance-id"), ("instance_type", "instance-type"), ("ami_id", "ami-id"),
               ("az", "placement/availability-zone"), ("az_id", "placement/availability-zone-id"),
               ("region", "placement/region"), ("placement_group", "placement/group-name"),
               ("private_ip", "local-ipv4"))


def iso(t: Optional[float] = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() if t is None else t))


def run(argv: list, timeout: float = 10) -> Optional[str]:
    """A command's stdout (or its stderr when stdout is empty: some tools print versions there), or None."""
    try:
        r = subprocess.run([str(x) for x in argv], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() or r.stderr.strip() or None


def first(text: Optional[str]) -> Optional[str]:
    return text.splitlines()[0].strip() if text else None


def read(path, limit: int = 65536) -> Optional[str]:
    try:
        return " ".join(Path(path).read_text(errors="replace")[:limit].split("\t")).strip() or None
    except OSError:
        return None


def imds(timeout: float = 1.0) -> dict:
    """EC2 instance metadata (IMDSv2), or {} off EC2."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        req = urllib.request.Request(f"{IMDS}/api/token", method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = opener.open(req, timeout=timeout).read().decode()
    except Exception:
        return {}
    out = {}
    for key, path in IMDS_FIELDS:
        try:
            req = urllib.request.Request(f"{IMDS}/meta-data/{path}", headers={"X-aws-ec2-metadata-token": token})
            out[key] = opener.open(req, timeout=timeout).read().decode().strip()
        except Exception:
            out[key] = None
    return out


def os_release() -> dict:
    d = {}
    for line in (read("/etc/os-release") or "").splitlines():
        k, sep, v = line.partition("=")
        if sep:
            d[k.strip()] = v.strip().strip('"')
    return {"pretty": d.get("PRETTY_NAME"), "id": d.get("ID"), "version_id": d.get("VERSION_ID"),
            "release": read("/etc/amazon-linux-release") or read("/etc/system-release")}


def cpu() -> dict:
    out = {"count": os.cpu_count(), "online": read("/sys/devices/system/cpu/online")}
    for line in (run(["lscpu"]) or "").splitlines():
        k, sep, v = line.partition(":")
        if sep and k.strip() in LSCPU_KEYS:
            out[k.strip()] = v.strip()
    return out


def mem_kb() -> Optional[int]:
    m = re.search(r"^MemTotal:\s+(\d+)", read("/proc/meminfo") or "", re.M)
    return int(m.group(1)) if m else None


def tools() -> dict:
    cargo_bin = Path.home() / ".cargo" / "bin"

    def find(name: str) -> Optional[str]:
        return shutil.which(name) or (str(cargo_bin / name) if (cargo_bin / name).exists() else None)
    out = {name: first(run([find(name), *args])) if find(name) else None for name, args in TOOLS.items()}
    out["glibc"] = run(["getconf", "GNU_LIBC_VERSION"])
    return out


def packages() -> Optional[dict]:
    """name -> installed versions (several for kernel), None when not installed; None without rpm."""
    if not shutil.which("rpm"):
        return None
    out: dict = {p: [] for p in PACKAGES}
    text = run(["rpm", "-q", "--qf", "%{NAME}\t%{VERSION}-%{RELEASE}.%{ARCH}\n", *PACKAGES], timeout=30) or ""
    for line in text.splitlines():
        name, sep, ver = line.partition("\t")
        if sep and name in out:
            out[name].append(ver.strip())
    return {k: v or None for k, v in out.items()}


def sysctls() -> dict:
    return {k: read("/proc/sys/" + k.replace(".", "/")) for k in SYSCTLS}


def thp() -> dict:
    base = Path("/sys/kernel/mm/transparent_hugepage")
    return {"enabled": read(base / "enabled"), "defrag": read(base / "defrag")}


def pintune_state() -> dict:
    """The CPU and interrupt layout scripts/pintune.sh sets up, as found."""
    affinity: Counter = Counter()
    for d in Path("/proc/irq").glob("[0-9]*"):
        v = read(d / "smp_affinity_list")
        if v:
            affinity[v] += 1
    ena = sorted(Path("/sys/class/net").glob("en*/queues/rx-0/rps_cpus"))
    out = {"irqbalance": run(["systemctl", "is-active", "irqbalance"]),
           "lo_rps_cpus": read("/sys/class/net/lo/queues/rx-0/rps_cpus"),
           "ena_rps_cpus": read(ena[0]) if ena else None,
           "rps_sock_flow_entries": read("/proc/sys/net/core/rps_sock_flow_entries"),
           "workqueue_cpumask": read("/sys/devices/virtual/workqueue/cpumask"),
           "default_smp_affinity": read("/proc/irq/default_smp_affinity"),
           "irq_affinity": dict(affinity.most_common())}
    # the test cell.check_pintune applies before a pinned rep
    out["applied"] = out["irqbalance"] != "active" and bool((out["lo_rps_cpus"] or "").replace(",", "").strip("0"))
    return out


def nics() -> list:
    out = []
    net = Path("/sys/class/net")
    for p in sorted(net.iterdir()) if net.exists() else []:
        if p.name == "lo":
            continue
        info = {"name": p.name, "mtu": read(p / "mtu"), "speed": read(p / "speed"),
                "rx_queues": len(list((p / "queues").glob("rx-*")))}
        for line in (run(["ethtool", "-i", p.name]) or "").splitlines():
            k, sep, v = line.partition(":")
            if sep and k.strip() in ("driver", "version", "firmware-version", "bus-info"):
                info[k.strip()] = v.strip()
        out.append(info)
    return out


def clock() -> Optional[dict]:
    """chrony's view of this host's clock, or what chronyc said instead (e.g. no daemon)."""
    text = run(["chronyc", "tracking"]) or ""
    out = {}
    for line in text.splitlines():
        k, sep, v = line.partition(":")
        if sep and k.strip() in ("Reference ID", "Stratum", "System time", "Last offset", "RMS offset", "Leap status"):
            out[k.strip()] = v.strip()
    if not out and text:
        out["error"] = first(text)
    return out or None


def elf(path) -> dict:
    """Build id, toolchain strings (.comment), .text size and shared libraries of an ELF binary."""
    out: dict = {}
    m = re.search(r"Build ID:\s*([0-9a-f]+)", run(["readelf", "-n", path]) or "")
    out["build_id"] = m.group(1) if m else None
    comment = run(["readelf", "-p", ".comment", path]) or ""
    out["toolchain"] = sorted({m.group(1).strip() for m in re.finditer(r"^\s*\[\s*[0-9a-f]+\]\s+(.*)$", comment, re.M)}) or None
    out["text_bytes"] = None
    for line in (run(["size", "-A", path]) or "").splitlines():
        parts = line.split()
        if parts[:1] == [".text"] and len(parts) > 1 and parts[1].isdigit():
            out["text_bytes"] = int(parts[1])
    libs = []
    for line in (run(["ldd", path]) or "").splitlines():
        parts = line.split()
        if not parts or parts[0].startswith("linux-vdso"):
            continue
        libs.append(" ".join(parts[:3]) if "=>" in parts else parts[0])
    out["libs"] = libs or None
    return out


def binary(path: str, quick: bool = False) -> dict:
    """Identity of one binary: sha256 and size always; version, build id, toolchain and libraries unless quick."""
    p = Path(os.path.expanduser(path))
    out: dict = {"path": str(p)}
    if not p.is_file():
        out["exists"] = False
        return out
    st = p.stat()
    out.update(exists=True, realpath=str(p.resolve()), bytes=st.st_size, mtime=iso(st.st_mtime), sha256=sha256_file(p))
    if not quick:
        out["version"] = first(run([p, "--version"], timeout=5))
        out.update(elf(p))
    return out


def valkey_build(server: str) -> dict:
    """The build tree a server binary sits in (~/offbench/builds/<sha>/src/valkey-server): its make settings,
    release header, build log and whether its sources are there to archive."""
    src = Path(os.path.expanduser(server)).parent
    root = src.parent
    release = {}
    for line in (read(src / "release.h") or "").splitlines():
        m = re.match(r'#define\s+(\w+)\s+"?([^"]*)"?', line.strip())
        if m:
            release[m.group(1)] = m.group(2)
    log = root / "build.log"
    git = None
    if (root / ".git").exists():
        git = {"head": run(["git", "-C", root, "rev-parse", "HEAD"]),
               "dirty": bool(run(["git", "-C", root, "status", "--porcelain", "--untracked-files=no"]))}
    return {"root": str(root), "source_present": (src / "server.c").is_file(),
            "make_settings": read(src / ".make-settings"), "release_h": release or None,
            "build_log": {"bytes": log.stat().st_size, "sha256": sha256_file(log)} if log.is_file() else None,
            "jemalloc": read(root / "deps" / "jemalloc" / "VERSION"), "git": git}


def collect(servers: list = (), cachecannon: Optional[str] = None, cli: Optional[str] = None) -> dict:
    t0 = time.time()
    out = {
        "collected": iso(t0),
        "host": {"hostname": socket.gethostname(), "user": getpass.getuser(), "home": str(Path.home())},
        "instance": imds(), "os": os_release(), "kernel": platform.release(), "arch": platform.machine(),
        "cmdline": read("/proc/cmdline"), "cpu": cpu(), "mem_kb": mem_kb(), "python": platform.python_version(),
        "packages": packages(), "tools": tools(), "sysctl": sysctls(), "thp": thp(), "pintune": pintune_state(),
        "nics": nics(), "clock": clock(), "limits": {"nofile": list(resource.getrlimit(resource.RLIMIT_NOFILE))},
        "servers": [dict(binary(s), build=valkey_build(s)) for s in servers],
        "cachecannon": binary(cachecannon) if cachecannon else None,
        "cli": binary(cli) if cli else None,
    }
    out["seconds"] = round(time.time() - t0, 2)
    return out
