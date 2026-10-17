"""Run one cell repetition on the rig host. Port of rig/cccell.sh.

Number reported = server-side total_commands_processed delta over the window.
Also samples main-thread active time (INFO used_active_time_main_thread),
main-thread CPU (/proc/<pid>/task/<pid>/stat), generator CPU, and optionally
perf stat on the main thread for the same window. Every rep starts and ends
with zero valkey-server / cachecannon processes or the rep is an error.
"""

from __future__ import annotations

import json
import os
import re
import statistics
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .schema import Cell, Latency, Metrics, Perf

HOST, PORT = "127.0.0.1", 7400
CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
PERF_EVENTS = "cycles:u,instructions:u,L1-icache-load-misses:u,L1-dcache-load-misses:u,branch-misses:u"


class CellError(RuntimeError):
    pass


# PIN=1 layout, same as rig/ccmatrix_pin.sh + pintune.sh: cpu0 housekeeping (server
# start, bio/jemalloc threads), cpu1 main, io_thd_i on cpu i+1, IRQ/softirq on 17-32,
# cachecannon on 33-95 with one worker per core.
PIN_HK_CPUS = "0"
PIN_MAIN_CPU = 1
PIN_IRQ_CPUS = "17-32"
PIN_GEN_CPUS = "33-95"


def cpu_count(cpus: str) -> int:
    n = 0
    for part in cpus.split(","):
        a, _, b = part.partition("-")
        n += int(b or a) - int(a) + 1
    return n


def all_cpus() -> str:
    """Every online CPU. An unpinned cell runs on all of them, not on the affinity the worker inherited
    (pintune.sh moves every process, sshd included, onto the housekeeping and IRQ CPUs)."""
    return f"0-{(os.cpu_count() or 1) - 1}"


def server_launch_argv(server_cmd_list: list[str], cell: Cell) -> list[str]:
    pfx = ["taskset", "-c", PIN_HK_CPUS if cell.pin else all_cpus()]
    return [*pfx, "setsid", "-f", *server_cmd_list]


def generator_launch_argv(cachecannon: Path, toml: Path, cell: Cell) -> list[str]:
    pfx = ["taskset", "-c", PIN_GEN_CPUS if cell.pin else all_cpus()]
    return [*pfx, "setsid", "-f", str(cachecannon), str(toml)]


def check_pintune() -> None:
    """A pinned cell is only valid with IRQ/softirq isolated off the server and
    generator cores (pintune.sh): irqbalance stopped and loopback RPS steered."""
    active = _run(["systemctl", "is-active", "irqbalance"]).stdout.strip()
    try:
        rps = Path("/sys/class/net/lo/queues/rx-0/rps_cpus").read_text().strip()
    except OSError:
        rps = ""
    if active == "active" or not rps.replace(",", "").strip("0"):
        raise CellError(f"pinned cell without pintune: irqbalance={active} lo rps_cpus={rps!r}")


@dataclass
class Paths:
    server: Path            # valkey-server binary under test
    cli: Path               # any valkey-cli
    cachecannon: Path
    workdir: Path           # scratch: toml, generator json, server log


def _run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, **kw)


def pids_matching(pattern: str) -> list[int]:
    out = _run(["pgrep", "-f", pattern]).stdout.split()
    return [int(x) for x in out]


def kill_matching(pattern: str, grace: float = 0.5) -> None:
    for sig in ("-TERM", "-KILL"):
        pids = pids_matching(pattern)
        if not pids:
            return
        _run(["kill", sig, *map(str, pids)])
        time.sleep(grace)


def assert_none(pattern: str, what: str) -> None:
    pids = pids_matching(pattern)
    if pids:
        raise CellError(f"{what} still running before/after rep: pids {pids}")


def server_cmd(server: Path, cell: Cell, log: Path, extra_server_args: str = "", bind: str = HOST) -> list[str]:
    """Build the valkey-server startup command for a cell. Port of cccell.sh.

    DIV-1: --tcp-backlog 65535 (default 511 overflows at 7000 concurrent conns).
    DIV-2: no ACL in startup args; ACL is applied post-start (see acl_setuser_cmds).
    """
    return [
        str(server), "--port", str(PORT), "--bind", *bind.split(), "--protected-mode", "no",
        "--save", "", "--io-threads", str(cell.io_threads), "--maxclients", "65000",
        "--tcp-backlog", "65535", "--logfile", str(log), *shlex.split(extra_server_args),
    ]


def acl_setuser_cmds(cli_path: str, port: int) -> list[list[str]]:
    """Post-start ACL setup, applied via a plain (non-auth) cli. Port of cccell.sh.

    Order matters: create/enable bench FIRST, then disable default, so the
    control connection running the second command is still authorized.
    Key patterns are a real per-key walk (~x:* then ~?*), NOT allkeys (~*),
    which the server short-circuits and would defeat the ACL cell.
    """
    base = [cli_path, "-h", HOST, "-p", str(port)]
    return [
        base + ["acl", "setuser", "bench", "on", ">benchpw", "~x:*", "~?*", "&*", "+@all"],
        base + ["acl", "setuser", "default", "off"],
    ]


def cli(paths: Paths, *args: str, acl: bool = False) -> str:
    auth = ["--user", "bench", "-a", "benchpw", "--no-auth-warning"] if acl else []
    r = _run([str(paths.cli), "-h", HOST, "-p", str(PORT), *auth, *args])
    return r.stdout


def info_int(paths: Paths, section: str, field: str, acl: bool = False) -> Optional[int]:
    m = re.search(rf"^{re.escape(field)}:(\d+)", cli(paths, "info", section, acl=acl), re.M)
    return int(m.group(1)) if m else None


def info_float(paths: Paths, section: str, field: str, acl: bool = False) -> Optional[float]:
    m = re.search(rf"^{re.escape(field)}:([\d.]+)", cli(paths, "info", section, acl=acl), re.M)
    return float(m.group(1)) if m else None


def thread_cpu_ticks(pid: int, tid: Optional[int] = None) -> float:
    st = Path(f"/proc/{pid}/task/{tid or pid}/stat").read_text().rsplit(")", 1)[1].split()
    return float(st[11]) + float(st[12])   # utime + stime after comm


def proc_cpu_ticks(pids: list[int]) -> float:
    total = 0.0
    for p in pids:
        try:
            st = Path(f"/proc/{p}/stat").read_text().rsplit(")", 1)[1].split()
            total += float(st[11]) + float(st[12])
        except FileNotFoundError:
            pass
    return total


def thread_ticks(pid: int) -> dict[int, tuple[str, float]]:
    """tid -> (thread name, utime + stime) for every thread of a process; main is named main."""
    out: dict[int, tuple[str, float]] = {}
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return out
    for t in tids:
        try:
            st = Path(f"/proc/{pid}/task/{t}/stat").read_text()
        except OSError:
            continue
        comm = st[st.index("(") + 1:st.rindex(")")]
        f = st.rsplit(")", 1)[1].split()
        out[int(t)] = ("main" if int(t) == pid else comm, float(f[11]) + float(f[12]))
    return out


def thread_cpu(a: dict[int, tuple[str, float]], b: dict[int, tuple[str, float]], seconds: float) -> dict[str, float]:
    """CPU % per thread between two thread_ticks readings, for threads alive at both and above 0.1%.
    A name that more than one thread carries gets its tid appended."""
    names = [b[t][0] for t in b if t in a]
    out = {}
    for t, (name, ticks) in sorted(b.items()):
        if t not in a or seconds <= 0:
            continue
        pct = (ticks - a[t][1]) / CLK_TCK / seconds * 100.0
        if pct >= 0.1:
            out[name if names.count(name) == 1 else f"{name}:{t}"] = round(pct, 1)
    return out


def cachecannon_toml(cell: Cell, window_s: int = 6, warmup_s: int = 3, timeout: str = "1s",
                     threads: Optional[int] = None, nproc: int = 1, cpu_list: Optional[str] = None,
                     endpoint: str = f"{HOST}:{PORT}", prefill: bool = True,
                     duration_s: Optional[int] = None, readonly: bool = False, rate_burst_us: int = 0,
                     timestamps: str = "userspace") -> str:
    """Emit the cachecannon TOML for a cell. Port of cccell.sh mkcfg.

    DIV-5: duration is derived from the window (window_s + 18) so warmup (3) +
    settle (3) + window + margin always fit inside the generator run, not fixed
    at 24. DIV-6: no `delete = 0` line under [workload.commands]. DIV-8:
    request_timeout defaults to '1s' (the script's CCTO default).

    connections, rate_limit and threads are split by nproc (one generator
    process per share); nproc=1 is the canary reference and leaves them whole.
    With a rate, rate_burst_us bounds how far the generator may run ahead after falling behind (that many
    microseconds of requests) and starts it with none banked; 0 keeps cachecannon's default of a full
    second, banked from the start. timestamps is cachecannon's [timestamps] mode: "software" times each
    request from the kernel's send to its receive timestamp, leaving out the generator's own scheduling.
    """
    g, s = {"get": (100, 0), "set": (0, 100), "mixed": (80, 20)}[cell.workload]
    lines = ["[general]", f'duration = "{duration_s or window_s + 18}s"', f'warmup = "{warmup_s}s"']
    if threads:
        lines.append(f"threads = {threads // nproc}")
    if cpu_list:
        lines.append(f'cpu_list = "{cpu_list}"')
    lines += [
        "[target]", f'endpoints = ["{endpoint}"]', 'protocol = "resp"',
    ]
    if cell.acl:
        lines += ['auth_user = "bench"', 'auth_password = "benchpw"']
    if readonly:
        lines.append("readonly = true")
    lines += [
        "[connection]", f"connections = {cell.conns // nproc}", f"pipeline_depth = {cell.pipeline}",
        f'request_timeout = "{timeout}"',
        "[workload]", f"prefill = {'true' if prefill else 'false'}", 'prefill_settle = "0s"',
        *([f"rate_limit = {cell.rate // nproc}"] if cell.rate else []),
        *([f"rate_burst = {max(1, cell.rate // nproc * rate_burst_us // 1_000_000)}"] if cell.rate and rate_burst_us else []),
        "[workload.keyspace]", "length = 16", f"count = {cell.keys}", 'distribution = "uniform"',
        "[workload.commands]", f"get = {g}", f"set = {s}",
        "[workload.values]", f"length = {cell.value_size}",
        "[timestamps]", "enabled = true", f'mode = "{timestamps}"',
        "[admin]", 'format = "json"', 'color = "never"',
    ]
    return "\n".join(lines) + "\n"


def latency_from_samples(files: list[Path], w_a: float, w_b: float) -> Optional[Latency]:
    """Aggregate cachecannon's per-second "sample" JSON lines that fall inside the window."""
    return latency_from_window(window_samples(files, w_a, w_b))


LATENCY_FIELDS = ("p50_us", "p90_us", "p99_us", "p999_us", "p9999_us")


def window_samples(files: list[Path], w_a: float, w_b: float) -> list[dict]:
    """The per-second samples inside the window, each with its time as t."""
    out = []
    for f in files:
        for line in f.read_text(errors="replace").splitlines():
            if not line.startswith("{") or '"sample"' not in line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if d.get("type") != "sample":
                continue
            ts = d["ts"].replace("Z", "+00:00")
            m = re.match(r"(.*\.\d{1,6})\d*([+-]\d\d:\d\d)$", ts)
            t = datetime.fromisoformat(m.group(1) + m.group(2) if m else ts).timestamp()
            if t < w_a + 1.0 or t > w_b + 0.05:
                continue
            out.append({"t": t, **{k: d[k] for k in LATENCY_FIELDS + ("max_us", "req_s")}})
    return out


def latency_from_window(samples: list[dict]) -> Latency:
    """Same rule as rig/cclat.py: per second, the median of each percentile across generators, the max of their
    maxima and the sum of their rates; then the median across seconds and the max of maxima. Samples tagged
    with a src (one per host) merge only over the seconds every src reported: hosts emit at different phases,
    so an edge second can hold one host's sample alone."""
    by_sec: dict[int, list[dict]] = {}
    for s in samples:
        by_sec.setdefault(int(s["t"]), []).append(s)
    srcs = {s.get("src") for s in samples}
    if len(srcs) > 1:
        by_sec = {k: ss for k, ss in by_sec.items() if {s.get("src") for s in ss} == srcs} or by_sec
    if not by_sec:
        return Latency(samples=0)
    rows = []
    for _, ss in sorted(by_sec.items()):
        row = {k: statistics.median(x[k] for x in ss) for k in LATENCY_FIELDS}
        row["max_us"] = max(x["max_us"] for x in ss)
        row["req_s"] = sum(x["req_s"] for x in ss)
        rows.append(row)
    return Latency(
        **{k: statistics.median(r[k] for r in rows) for k in LATENCY_FIELDS},
        max_us=max(r["max_us"] for r in rows), samples=len(rows),
        gen_kreq_s=statistics.fmean(r["req_s"] for r in rows) / 1000.0,
    )


def parse_perf_stat(text: str, cmds: float) -> Perf:
    def ev(name: str) -> Optional[float]:
        m = re.search(rf"^\s*([\d,]+)\s+{re.escape(name)}", text, re.M)
        return float(m.group(1).replace(",", "")) if m else None
    cyc, ins = ev("cycles:u"), ev("instructions:u")
    l1i, l1d, br = ev("L1-icache-load-misses:u"), ev("L1-dcache-load-misses:u"), ev("branch-misses:u")
    return Perf(
        cyc_per_cmd=cyc / cmds if cyc and cmds else None,
        ins_per_cmd=ins / cmds if ins and cmds else None,
        ipc=ins / cyc if ins and cyc else None,
        l1i_miss_per_cmd=l1i / cmds if l1i is not None and cmds else None,
        l1d_miss_per_cmd=l1d / cmds if l1d is not None and cmds else None,
        br_miss_per_cmd=br / cmds if br is not None and cmds else None,
    )


def info_text(paths: Paths, section: str, acl: bool = False) -> dict:
    """Parse a whole INFO <section> into {field: value}. Used to snapshot the
    fastpath staged_* fields and commandstats at the window edges."""
    out: dict = {}
    for line in cli(paths, "info", section, acl=acl).splitlines():
        k, sep, v = line.strip().partition(":")
        if sep and not k.startswith("#"):
            out[k] = v
    return out


# main-wall parts and worker phases, same names the staged INFO fastpath exposes.
# streamwait is the time main blocks at the stream close; stream is the client
# reply-streaming worker phase.
STAGED_MAIN_PARTS = ("beforesleep", "poll", "read", "exec", "events", "aof",
                     "streamwait", "reply", "finish", "crons")
STAGED_PHASES = ("read", "reply", "stream", "sync", "drain", "apply")


def staged_split(fp_a: dict, fp_b: dict) -> Optional[dict]:
    """Delta of every staged_* INFO fastpath counter over the window, plus the
    derived main-wall per-pass split (each staged_main_<part>_us as a fraction of
    the total main wall) and the per-phase critical/worker us. None when the
    server reports no staged_* fields (not a staged build).

    staged_stream_phases is surfaced at top level so a reader can tell whether
    client reply streaming engaged. The reported main split is
    read / exec / streamwait / reply / other per iteration."""
    keys = sorted(k for k in fp_b if k.startswith("staged_"))
    if not keys:
        return None

    def iv(info, k):
        try:
            return int(info.get(k) or 0)
        except (TypeError, ValueError):
            return None

    fields = {}
    for k in keys:
        va, vb = iv(fp_a, k), iv(fp_b, k)
        fields[k] = {"a": va, "b": vb, "delta": (vb - va) if (va is not None and vb is not None) else None}

    def d(part_key):
        return (fields.get(part_key) or {}).get("delta") or 0

    iters = d("staged_iterations")
    main_parts_us = {p: d(f"staged_main_{p}_us") for p in STAGED_MAIN_PARTS}
    total_us = sum(main_parts_us.values())
    main_pct = {p: (100.0 * main_parts_us[p] / total_us if total_us else None) for p in STAGED_MAIN_PARTS}
    main_us_per_iter = {p: (main_parts_us[p] / iters if iters else None) for p in STAGED_MAIN_PARTS}
    phases = {ph: {"phases": d(f"staged_{ph}_phases"),
                   "worker_us": d(f"staged_{ph}_worker_us"),
                   "critical_us": d(f"staged_{ph}_critical_us")} for ph in STAGED_PHASES}
    return {
        "fields": fields,
        "iterations": iters,
        "main_parts_us": main_parts_us,
        "main_total_us": total_us,
        "main_pct": main_pct,
        "main_us_per_iter": main_us_per_iter,
        "phases": phases,
        "stream_phases": d("staged_stream_phases"),
        "us_per_iter": (total_us / iters if iters else None),
        "streamed": d("staged_stream_phases") > 0,
    }


def cmdstats_delta(cs_a: dict, cs_b: dict) -> dict:
    """calls and usec deltas for get and set over the window, plus ns/call for
    each. GET mixes main-executed and IO-thread-executed GETs; SET is main-only.
    None entries where the command had no calls."""
    def parse(cs, cmd):
        raw = cs.get(f"cmdstat_{cmd}", "")
        calls = re.search(r"calls=(\d+)", raw)
        usec = re.search(r"usec=(\d+)", raw)
        return (int(calls.group(1)) if calls else 0, int(usec.group(1)) if usec else 0)

    out = {}
    for cmd in ("get", "set"):
        ca, ua = parse(cs_a, cmd)
        cb, ub = parse(cs_b, cmd)
        dc, du = cb - ca, ub - ua
        out[cmd] = {"calls": dc, "usec": du, "ns_call": round(du * 1000.0 / dc, 1) if dc else None}
    return out


def _cpuset(spec: str) -> set[int]:
    """'1-2' and '1,2' name the same cpus."""
    out: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        if lo:
            out |= set(range(int(lo), int(hi or lo) + 1))
    return out


def pin_server_threads(sp: int, io_threads: int, srv_pat: str, share_main: bool = False) -> dict:
    """Move main to PIN_MAIN_CPU and io_thd_i to cpu i+PIN_MAIN_CPU, then verify each. Returns thread name -> cpus.

    share_main puts io_thd_1 on main's core and io_thd_i on cpu i-1+PIN_MAIN_CPU, so io-threads N
    uses N-1 cores; the epoll thread shares those cores."""
    _run(["taskset", "-pc", str(PIN_MAIN_CPU), str(sp)])
    want = {str(sp): str(PIN_MAIN_CPU)}
    names = {str(sp): "main"}
    epoll_tids = []
    sender_tids = []
    for line in _run(["ps", "-L", "-o", "tid,comm", "-p", str(sp)]).stdout.splitlines():
        m = re.match(r"\s*(\d+)\s+io_thd_(\d+)", line)
        if m:
            cpu = str(int(m.group(2)) + PIN_MAIN_CPU - (1 if share_main else 0))
            _run(["taskset", "-pc", cpu, m.group(1)])
            want[m.group(1)] = cpu
            names[m.group(1)] = f"io_thd_{m.group(2)}"
        e = re.match(r"\s*(\d+)\s+io_epoll\b", line)
        if e:
            epoll_tids.append(e.group(1))
        s = re.match(r"\s*(\d+)\s+repl_sender\b", line)
        if s:
            sender_tids.append(s.group(1))
    if len(want) != io_threads:
        kill_matching(srv_pat)
        raise CellError(f"pin: expected {io_threads - 1} io threads, found {len(want) - 1}")
    # The staged loop's epoll thread shares the IO-thread cores, never main's.
    io_cpus = f"{PIN_MAIN_CPU + 1}-{PIN_MAIN_CPU + io_threads - 1}" if io_threads > 2 else str(PIN_MAIN_CPU + 1)
    if share_main:
        io_cpus = f"{PIN_MAIN_CPU}-{PIN_MAIN_CPU + io_threads - 2}" if io_threads > 2 else str(PIN_MAIN_CPU)
    for i, tid in enumerate(epoll_tids):
        _run(["taskset", "-pc", io_cpus, tid])
        want[tid] = io_cpus
        names[tid] = "io_epoll" if len(epoll_tids) == 1 else f"io_epoll_{i + 1}"
    # The replica stream sender shares the IO-thread cores like io_epoll, never main's
    # core or the housekeeping core. Absent (sender off / no thread) is fine.
    for tid in sender_tids:
        _run(["taskset", "-pc", io_cpus, tid])
        want[tid] = io_cpus
        names[tid] = "repl_sender"
    for tid, cpu in want.items():
        got = _run(["taskset", "-pc", tid]).stdout.strip().rsplit(" ", 1)[-1]
        if _cpuset(got) != _cpuset(cpu):
            kill_matching(srv_pat)
            raise CellError(f"pin: thread {tid} on cpu {got!r}, expected {cpu}")
    return {names[tid]: cpu for tid, cpu in want.items()}


def run_rep(paths: Paths, cell: Cell, *, window_s: int = 6, perf: bool = False, extra_server_args: str = "",
            cc_threads: Optional[int] = None, dry_run: bool = False, log=print) -> Metrics:
    srv_pat, cc_pat = f"^{paths.server}", f"^{paths.cachecannon}"
    paths.workdir.mkdir(parents=True, exist_ok=True)
    server_log = paths.workdir / "server.log"
    toml = paths.workdir / "cc.toml"
    cc_out, cc_err = paths.workdir / "cc.json", paths.workdir / "cc.err"

    server_cmd_list = server_cmd(paths.server, cell, server_log, extra_server_args)

    cpu_list = PIN_GEN_CPUS if cell.pin else None
    if cell.pin and not cc_threads:
        cc_threads = cpu_count(PIN_GEN_CPUS)
    toml.write_text(cachecannon_toml(cell, window_s=window_s, threads=cc_threads, cpu_list=cpu_list))

    if dry_run:
        log("DRY  " + " ".join(server_cmd_list))
        log("DRY  " + f"{paths.cachecannon} {toml}")
        return Metrics(kcmd_s=0.0)

    if cell.pin:
        check_pintune()
    kill_matching(cc_pat)
    kill_matching(srv_pat)
    assert_none(srv_pat, "valkey-server")
    assert_none(cc_pat, "cachecannon")
    server_log.unlink(missing_ok=True)

    subprocess.Popen(server_launch_argv(server_cmd_list, cell), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL)
    time.sleep(0.7)
    sp = pids_matching(srv_pat)
    if len(sp) != 1:
        raise CellError(f"expected one server pid, got {sp}")
    sp = sp[0]
    if cell.acl:
        for setuser in acl_setuser_cmds(str(paths.cli), PORT):
            _run(setuser)
    if cell.pin:
        pin_server_threads(sp, cell.io_threads, srv_pat)

    with cc_out.open("w") as o, cc_err.open("w") as e:
        subprocess.Popen(generator_launch_argv(paths.cachecannon, toml, cell), stdin=subprocess.DEVNULL, stdout=o, stderr=e)

    # readiness: all connections up, keyspace filled, commands flowing.
    # Fail loud if the generator process exits before the gate is satisfied
    # (cccell.sh blind-relaunches once; here an early exit is a hard error so a
    # dead generator can never be measured as a valid rep).
    ready = False
    cl = db = ops = 0
    for _ in range(60):
        time.sleep(1)
        if not pids_matching(cc_pat):
            kill_matching(srv_pat)
            tail = ""
            if cc_err.exists():
                lines = [l for l in cc_err.read_text(errors="replace").splitlines() if "INFO" not in l]
                tail = lines[-1] if lines else ""
            raise CellError(f"cachecannon exited before gate (clients={cl} dbsize={db} ops={ops}): {tail}")
        cl = info_int(paths, "clients", "connected_clients", acl=cell.acl) or 0
        db = int(re.sub(r"\D", "", cli(paths, "dbsize", acl=cell.acl) or "0") or 0)
        ops = info_int(paths, "stats", "instantaneous_ops_per_sec", acl=cell.acl) or 0
        if cl >= cell.conns and db >= cell.keys and ops > 100_000:
            ready = True
            break
    if not ready:
        kill_matching(cc_pat)
        kill_matching(srv_pat)
        raise CellError(f"generator never became ready (clients={cl} dbsize={db} ops={ops})")
    time.sleep(3)

    cc_pids = pids_matching(cc_pat)
    a_cmds = info_int(paths, "stats", "total_commands_processed", acl=cell.acl) or 0
    w_a = time.time()
    a_act = info_float(paths, "all", "used_active_time_main_thread", acl=cell.acl) or 0.0
    u_a = time.time()
    a_cpu = thread_cpu_ticks(sp)
    a_cc = proc_cpu_ticks(cc_pids)
    a_thr = thread_ticks(sp)
    # Window-open snapshots of the staged INFO fastpath and commandstats, so the
    # per-rep record carries the main-wall split and SET/GET ns/call as deltas.
    a_fp = info_text(paths, "fastpath", acl=cell.acl)
    a_cs = info_text(paths, "commandstats", acl=cell.acl)

    perf_text = ""
    if perf:
        perf_text = _run(["perf", "stat", "-e", PERF_EVENTS, "-t", str(sp), "--", "sleep", str(window_s)]).stderr
    else:
        time.sleep(window_s)

    b_cmds = info_int(paths, "stats", "total_commands_processed", acl=cell.acl) or 0
    w_b = time.time()
    b_act = info_float(paths, "all", "used_active_time_main_thread", acl=cell.acl) or 0.0
    u_b = time.time()
    b_cpu = thread_cpu_ticks(sp)
    b_cc = proc_cpu_ticks(cc_pids)
    b_thr = thread_ticks(sp)
    # Window-close snapshots paired with a_fp / a_cs above.
    b_fp = info_text(paths, "fastpath", acl=cell.acl)
    b_cs = info_text(paths, "commandstats", acl=cell.acl)
    cc_threads_n = sum(len(os.listdir(f"/proc/{p}/task")) for p in cc_pids if Path(f"/proc/{p}/task").exists())
    clients = info_int(paths, "clients", "connected_clients", acl=cell.acl)
    fp_batches = info_int(paths, "all", "fastpath_batches", acl=cell.acl)

    time.sleep(1.2)   # let the generator flush the sample for the last window second
    kill_matching(cc_pat)
    latency = latency_from_samples([cc_out], w_a, w_b) if cc_out.exists() else None
    asserts = 0
    if server_log.exists():
        asserts = len(re.findall(r"crashed by|ASSERT", server_log.read_text(errors="replace"), re.I))
    kill_matching(srv_pat)
    assert_none(srv_pat, "valkey-server")
    assert_none(cc_pat, "cachecannon")

    timeouts = err_pct = None
    if cc_out.exists():
        m = re.search(r'"timeouts":\s*(\d+)', cc_out.read_text(errors="replace"))
        timeouts = int(m.group(1)) if m else None
        m = re.search(r'"err_pct":\s*([\d.]+)', cc_out.read_text(errors="replace"))
        err_pct = float(m.group(1)) if m else None

    cmds = b_cmds - a_cmds
    # Rates use the measured spans, as cccell.sh does: the counter span (w_a..w_b)
    # and the /proc span (u_a..u_b). An INFO round trip against thousands of
    # saturating clients takes tens of ms, so the nominal window understates both.
    span, uspan = w_b - w_a, u_b - u_a
    thr = thread_cpu(a_thr, b_thr, uspan)
    return Metrics(
        kcmd_s=cmds / span / 1000.0,
        main_active_pct=(b_act - a_act) / span * 100.0 if b_act else None,
        main_cpu_pct=(b_cpu - a_cpu) / CLK_TCK / uspan * 100.0,
        clients=clients,
        gen_cores=(b_cc - a_cc) / CLK_TCK / uspan,
        span_s=span,
        gen_cores_total=cc_threads_n or None,
        asserts=asserts,
        fp_batches_s=fp_batches,   # cumulative counter if present; runner keeps the raw value
        gen_timeouts=timeouts,
        gen_err_pct=err_pct,
        perf=parse_perf_stat(perf_text, cmds) if perf_text else None,
        latency=latency,
        server_cpu_cores=round(sum(thr.values()) / 100.0, 2) if thr else None,
        thread_cpu_pct=thr or None,
        staged=staged_split(a_fp, b_fp),
        cmdstats=cmdstats_delta(a_cs, b_cs),
    )
