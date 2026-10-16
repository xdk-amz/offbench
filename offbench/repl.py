"""Primary/replica read-write split across hosts.

Topology per rep: a primary on one host, its replica on a second host, and one
generator host running two cachecannon processes on disjoint cores: a writer
(SET only) against the primary and a reader (GET only) against the replica.
Both sides get the same connection count, pipeline depth and io-threads.

The orchestrator runs locally and drives each host over ssh through the
``repl-agent`` subcommand (this module's ``agent``). The measurement window is
not timed over ssh: both server hosts are told an absolute start time and take
their own INFO/proc snapshots, so ssh latency never lands inside the window
(hosts are chrony-synced to well under a millisecond).

Replica reads are ``total_commands_processed`` minus the replicated SETs the
replica applied (``cmdstat_set``), so the fast path is counted even where it
bypasses commandstats. A rep is invalid if the replication link dropped or
resynced, the replica fell behind the primary's writes, a server asserted, the
generator timed out, or a host exceeded its ENA allowance inside the window.

A closed-loop client delivers at most in-flight / round trip, so a run first
measures the generator's round trip to both servers and refuses to start if
either exceeds --max-rtt-ms: a far generator measures the network, not the server.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Callable, Optional

from .cell import (CLK_TCK, PORT, CellError, Paths, _run, acl_setuser_cmds, assert_none,
                   cachecannon_toml, check_pintune, kill_matching, latency_from_window,
                   pids_matching, pin_server_threads, server_cmd, server_launch_argv, window_samples)
from .schema import Cell

# Generator host: cpu0 housekeeping, 17-32 IRQ set (pintune.sh), the rest split
# between the writer and the reader.
WRITER_CPUS = "1-16,33-55"
READER_CPUS = "56-95"
# Three generators (writer, primary reader, replica reader) split the same cores three ways. The replica
# reader carries the largest load (14.6M GET/s needed all 26 cores of an even split), so it gets the most.
TRI_CPUS = {"writer": "1-8", "preader": "9-16,33-44", "reader": "45-95"}
TRI_THREADS = {"writer": 8, "preader": 20, "reader": 51}
# With --reader-gen the replica readers get a host of their own (everything but cpu0 and the IRQ set), or
# several with their connections split evenly, and the writer and primary readers split the generator host.
PAIR_CPUS = {"writer": "1-16", "preader": "33-95"}
PAIR_THREADS = {"writer": 16, "preader": 63}
SOLO_CPUS, SOLO_THREADS = "1-16,33-95", 79
ALLOWANCE_FIELDS = ("bw_in_allowance_exceeded", "bw_out_allowance_exceeded",
                    "pps_allowance_exceeded", "conntrack_allowance_exceeded")
# invalid_reasons() prefix for a replica that did not keep up with the primary's writes.
LAGGING = "replica fell behind"
# A rep whose primary delivered its target writes or reads more than this far off is invalid.
DRIFT_TOL = 0.07
# In a --bins spec, a |config with this prefix goes to the primary instead of the replica.
PRIMARY_CFG = "primary:"


def reader_roles(n: int) -> list[str]:
    """Role names for n replica-reader hosts: rgen, rgen2, rgen3, ..."""
    return ["rgen"] + [f"rgen{i}" for i in range(2, n + 1)]


def reader_keys(roles: dict) -> list[str]:
    """The replica-reader roles present, in order."""
    return [k for k in reader_roles(len(roles)) if k in roles]


def rtt_path(i: int) -> str:
    """The round-trip path from the i-th replica-reader host (from 0) to the replica."""
    return "gen_replica" if i == 0 else f"gen_replica{i + 1}"


def split_conns(conns: int, n: int) -> list[int]:
    """conns over n hosts as evenly as possible, the first ones taking the remainder."""
    q, r = divmod(conns, n)
    return [q + (i < r) for i in range(n)]


def fell_behind(rec: dict) -> bool:
    """The replica fell behind in this rep, whatever else made it invalid. One such rep puts its whole
    step out of sync: the replica cannot be relied on at that read load."""
    return any(w.startswith(LAGGING) for w in rec.get("invalid") or [])

HOME = Path.home()
CLI = HOME / "base" / "src" / "valkey-cli"
CACHECANNON = HOME / "offbench-src" / "vendor" / "cachecannon" / "target" / "release" / "cachecannon"
WORK = HOME / "offbench" / "repl-work"
PACKAGE = Path(__file__).resolve().parent


def code_hashes(pkg: Path = PACKAGE) -> dict:
    """sha256 of every module in the package, so the orchestrator can tell a host runs the same code."""
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(pkg.glob("*.py"))}


# ------------------------------------------------------------ host side ----

def _cli(*args: str, acl: bool = False) -> str:
    auth = ["--user", "bench", "-a", "benchpw", "--no-auth-warning"] if acl else []
    return _run([str(CLI), "-h", "127.0.0.1", "-p", str(PORT), *auth, *args]).stdout


def parse_info(text: str) -> dict:
    out: dict = {}
    for line in text.splitlines():
        k, sep, v = line.strip().partition(":")
        if sep and not k.startswith("#"):
            out[k] = v
    return out


def cmdstat_calls(info: dict, cmd: str) -> int:
    m = re.search(r"calls=(\d+)", info.get(f"cmdstat_{cmd}", ""))
    return int(m.group(1)) if m else 0


def ena_counters() -> dict:
    nics = [n for n in os.listdir("/sys/class/net") if n.startswith("en")]
    out = {k: 0 for k in ALLOWANCE_FIELDS}
    for n in nics:
        for line in _run(["ethtool", "-S", n]).stdout.splitlines():
            k, _, v = line.strip().partition(":")
            if k in out:
                out[k] += int(v.strip() or 0)
    return out


def thread_ticks(pid: int) -> dict:
    """CPU ticks for main and the sum over io_thd_* threads of one server."""
    main = io = 0.0
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            st = Path(f"/proc/{pid}/task/{tid}/stat").read_text()
        except FileNotFoundError:
            continue
        comm = st[st.index("(") + 1:st.rindex(")")]
        f = st.rsplit(")", 1)[1].split()
        t = float(f[11]) + float(f[12])
        if tid == str(pid):
            main = t
        elif comm.startswith("io_thd_"):
            io += t
    return {"main": main, "io": io}


def proc_ticks(pattern: str) -> float:
    total = 0.0
    for p in pids_matching(pattern):
        try:
            f = Path(f"/proc/{p}/stat").read_text().rsplit(")", 1)[1].split()
            total += float(f[11]) + float(f[12])
        except FileNotFoundError:
            pass
    return total


def rtt_stats(ping_out: str) -> Optional[dict]:
    """Median/min/max of the per-probe times in `ping -n` output, in ms (None if no replies)."""
    ts = sorted(float(x) for x in re.findall(r"time=([\d.]+) ms", ping_out))
    if not ts:
        return None
    return {"median_ms": statistics.median(ts), "min_ms": ts[0], "max_ms": ts[-1], "n": len(ts)}


def _srv_pat(server: str) -> str:
    return f"^{server}"


def _gen_pat(name: str) -> str:
    return f"^{CACHECANNON} {WORK / (name + '.toml')}"


def _snapshot(a: dict) -> dict:
    snap: dict = {}
    if a.get("server"):
        text = _cli("info", "all", acl=a.get("acl", False))
        snap["t"] = time.time()
        snap["info"] = {k: v for k, v in parse_info(text).items()
                        if k in INFO_KEEP or k.startswith(("cmdstat_get", "cmdstat_set", "slave0"))}
        snap["ticks"] = thread_ticks(a["pid"])
        snap["ut"] = time.time()
    if a.get("gens"):
        snap["gen_ticks"] = {g: proc_ticks(_gen_pat(g)) for g in a["gens"]}
        snap["gt"] = time.time()
    snap["ena"] = ena_counters()
    return snap


INFO_KEEP = {
    "total_commands_processed", "used_active_time_main_thread", "connected_clients", "role",
    "master_link_status", "master_sync_in_progress", "master_repl_offset", "slave_repl_offset",
    "sync_full", "sync_partial_ok", "connected_slaves", "mem_clients_slaves", "fastpath_batches",
    "master_last_io_seconds_ago", "used_memory", "instantaneous_ops_per_sec",
    "io_threaded_primary_drain_jobs", "io_threaded_primary_drain_bytes",
}


def agent(a: dict) -> dict:
    """Execute one host-side action and return a JSON-able result."""
    act = a["action"]
    WORK.mkdir(parents=True, exist_ok=True)
    if a.get("server"):
        a["server"] = os.path.expanduser(a["server"]) if isinstance(a["server"], str) else a["server"]
    a["servers"] = [os.path.expanduser(x) for x in a.get("servers", [])]

    if act == "ip":
        return {"ip": _run(["hostname", "-I"]).stdout.split()[0]}

    if act == "preflight":
        out = {"code": code_hashes(), "cli": CLI.exists(), "cachecannon": CACHECANNON.exists(),
               "missing_servers": [s for s in a["servers"] if not Path(s).exists()]}
        if a.get("pintune"):
            try:
                check_pintune()
                out["pintune"] = True
            except CellError as e:
                out["pintune"] = str(e)
        return out

    if act == "facts":
        from . import hostfacts
        out = hostfacts.collect(servers=a["servers"], cachecannon=str(CACHECANNON) if a.get("gen") else None,
                                cli=str(CLI) if a.get("cli") else None)
        out["code"] = code_hashes()
        return out

    if act == "hash":
        from . import hostfacts
        return {"binaries": [hostfacts.binary(p, quick=True) for p in a["paths"]]}

    if act == "pack":
        # Each item becomes one gzip file under WORK for the orchestrator to fetch (and delete): a file as it is,
        # a tree as a deterministic tar. Named by the sha256 of the uncompressed bytes.
        from . import packing
        items = []
        tag = os.urandom(4).hex()
        for i, it in enumerate(a["items"]):
            dest = WORK / f"pack-{tag}-{i}.gz"
            src = Path(os.path.expanduser(it["path"]))
            if it["kind"] == "file":
                r = packing.gzip_file(src, dest, level=it.get("level", 1))
            else:
                exclude = packing.VALKEY_TREE_EXCLUDE if it.get("exclude") == "valkey-build" else ()
                entries, skipped = packing.walk(src, exclude, max_bytes=it.get("max_bytes"))
                r = packing.tar_gz([(n, p, None) for n, p in entries], dest, level=it.get("level", 6))
                r["skipped"] = skipped
            items.append(dict(r, gz=str(dest)))
        return {"items": items}

    if act == "rtt":
        procs = {ip: subprocess.Popen(["ping", "-n", "-c", str(a.get("count", 25)), "-i", "0.2", ip],
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                 for ip in a["ips"]}
        return {ip: rtt_stats(p.communicate(timeout=60)[0]) for ip, p in procs.items()}

    if act == "clean":
        pats = [_srv_pat(s) for s in a.get("servers", [])] + [f"^{CACHECANNON}"]
        for p in pats:
            kill_matching(p)
        for p in pats:
            assert_none(p, p)
        return {"ok": True, "ena": ena_counters()}

    if act == "server_start":
        cell = Cell(workload="get", pipeline=1, conns=0, io_threads=a["io_threads"], acl=a.get("acl", 0),
                    pin=a.get("pin", 1))
        if cell.pin:
            check_pintune()
        log = WORK / f"{a['role']}.log"
        log.unlink(missing_ok=True)
        extra = "--repl-diskless-sync-delay 0"
        if a["role"] == "replica" and a.get("replica_args"):
            extra += " " + a["replica_args"]
        if a["role"] == "primary" and a.get("primary_args"):
            extra += " " + a["primary_args"]
        argv = server_cmd(Path(a["server"]), cell, log, extra, bind=f"127.0.0.1 {a['bind_ip']}")
        pat = _srv_pat(a["server"])
        assert_none(pat, "valkey-server")
        launch = server_launch_argv(argv, cell)
        subprocess.Popen(launch, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.7)
        sp = pids_matching(pat)
        if len(sp) != 1:
            raise CellError(f"expected one server pid, got {sp}")
        sp = sp[0]
        post = []     # what the control connection ran after start, as valkey-cli arguments

        def cli_post(*args: str, acl: bool) -> str:
            post.append(list(args))
            return _cli(*args, acl=acl)
        if cell.acl:
            for cmd in acl_setuser_cmds(str(CLI), PORT):
                post.append(cmd[5:])
                _run(cmd)
        pins = None
        if cell.pin:
            pins = pin_server_threads(sp, cell.io_threads, pat, share_main=bool(a.get("pin_share")))
        # Closed-loop writes outrun any replica, so the default replica output
        # buffer limit turns every rep into a disconnect/full-resync loop. Lift it
        # and report the lag instead.
        r = cli_post("config", "set", "client-output-buffer-limit", "replica 0 0 0", acl=cell.acl)
        if "OK" not in r:
            raise CellError(f"config set client-output-buffer-limit failed: {r.strip()}")
        if a["role"] == "replica":
            if cell.acl:
                # the replica authenticates to the primary as the bench user
                cli_post("config", "set", "masteruser", "bench", acl=True)
                cli_post("config", "set", "masterauth", "benchpw", acl=True)
            r = cli_post("replicaof", a["primary_ip"], str(PORT), acl=cell.acl)
            if "OK" not in r:
                raise CellError(f"replicaof failed: {r.strip()}")
        return {"pid": sp, "argv": launch, "post": post, "pins": pins}

    if act == "status":
        info = parse_info(_cli("info", "all", acl=a.get("acl", False)))
        dbsize = int(re.sub(r"\D", "", _cli("dbsize", acl=a.get("acl", False))) or 0)
        return {"dbsize": dbsize, **{k: info.get(k) for k in INFO_KEEP}}

    if act == "gen_start":
        cell = Cell(workload=a["workload"], pipeline=a["pipeline"], conns=a["conns"],
                    keys=a["keys"], value_size=a["value_size"], acl=a.get("acl", 0), pin=1)
        toml = WORK / f"{a['name']}.toml"
        text = cachecannon_toml(cell, window_s=a["window_s"], threads=a["threads"], cpu_list=a["cpus"],
                                endpoint=f"{a['target_ip']}:{PORT}", prefill=a["prefill"], duration_s=a["duration_s"])
        toml.write_text(text)
        out, err = WORK / f"{a['name']}.json", WORK / f"{a['name']}.err"
        launch = ["taskset", "-c", a["cpus"], "setsid", "-f", str(CACHECANNON), str(toml)]
        with out.open("w") as o, err.open("w") as e:
            subprocess.Popen(launch, stdin=subprocess.DEVNULL, stdout=o, stderr=e)
        time.sleep(0.5)
        return {"alive": bool(pids_matching(_gen_pat(a["name"]))), "argv": launch, "toml": text}

    if act == "gen_alive":
        return {g: bool(pids_matching(_gen_pat(g))) for g in a["gens"]}

    if act == "measure":
        delay = a["at"] - time.time()
        if delay < 0:
            raise CellError(f"measure start already passed by {-delay:.3f}s")
        time.sleep(delay)
        s_a = _snapshot(a)
        time.sleep(max(0.0, a["at"] + a["window_s"] - time.time()))
        s_b = _snapshot(a)
        return {"a": s_a, "b": s_b}

    if act == "gen_stop":
        res = {}
        for g in a["gens"]:
            alive = bool(pids_matching(_gen_pat(g)))
            kill_matching(_gen_pat(g))
            out = WORK / f"{g}.json"
            text = out.read_text(errors="replace") if out.exists() else ""
            samples = window_samples([out], a["w_a"], a["w_b"]) if out.exists() else []
            lat = latency_from_window(samples) if out.exists() else None
            m = re.search(r'"timeouts":\s*(\d+)', text)
            e = re.search(r'"err_pct":\s*([\d.]+)', text)
            tail = ""
            errf = WORK / f"{g}.err"
            if errf.exists():
                lines = [l for l in errf.read_text(errors="replace").splitlines() if "INFO" not in l]
                tail = lines[-1] if lines else ""
            res[g] = {"alive_at_stop": alive, "latency": asdict(lat) if lat else None,
                      "timeouts": int(m.group(1)) if m else None,
                      "err_pct": float(e.group(1)) if e else None, "err_tail": tail, "samples": samples}
        return res

    if act == "server_stop":
        pat = _srv_pat(a["server"])
        kill_matching(pat)
        assert_none(pat, "valkey-server")
        log = WORK / f"{a['role']}.log"
        text = log.read_text(errors="replace") if log.exists() else ""
        return {"asserts": len(re.findall(r"crashed by|ASSERT", text, re.I)),
                "client_closed_for_obuf": len(re.findall(r"scheduled to be closed ASAP for overcoming of output buffer", text)),
                "log_tail": text.splitlines()[-5:]}

    raise CellError(f"unknown action {act}")


def agent_main(arg: str) -> int:
    try:
        print(json.dumps(agent(json.loads(arg))))
        return 0
    except Exception as exc:  # the orchestrator reads the error from stdout
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1


# ------------------------------------------------------ orchestrator side ----

@dataclass
class Role:
    name: str       # registry name, e.g. metal-2
    ssh: str        # user@public-ip
    ip: str         # private ip the other hosts reach


@dataclass
class ReplCell:
    pipeline: int
    conns: int
    io_threads: int
    keys: int = 100000
    value_size: int = 64
    acl: int = 0
    pin: int = 1
    # Writer shape; 0 means the same as the reader (pipeline/conns above).
    write_conns: int = 0
    write_pipeline: int = 0
    # Where the GET readers connect: "replica" (read/write split) or "primary" (mixed load).
    read_target: str = "replica"
    # Extra GET readers on the primary alongside replica readers (0 = none).
    p_conns: int = 0
    # Main and io_thd_1 share one core (io-threads N on N-1 cores).
    pin_share: int = 0
    # Primary readers' pipeline; 0 means the same as the replica readers (pipeline above).
    p_pipeline: int = 0

    @property
    def w_conns(self) -> int:
        return self.write_conns or self.conns

    @property
    def w_pipeline(self) -> int:
        return self.write_pipeline or self.pipeline

    @property
    def pp_pipeline(self) -> int:
        return self.p_pipeline or self.pipeline

    @property
    def cores(self) -> int:
        """Cores per server: a shared-core build runs one more IO thread than it has cores."""
        return self.io_threads - self.pin_share

    def key(self) -> str:
        if not (self.write_conns or self.write_pipeline):
            return f"rw-P{self.pipeline}-c{self.conns}-iot{self.io_threads}" + ("-acl" if self.acl else "")
        return (f"w{self.w_conns}xP{self.w_pipeline}-r{self.conns}xP{self.pipeline}"
                + ("@primary" if self.read_target == "primary" else "")
                + (f"-p{self.p_conns}xP{self.pp_pipeline}" if self.p_conns else "") + f"-iot{self.io_threads}"
                + ("-shared" if self.pin_share else "")
                + ("-acl" if self.acl else ""))

    @property
    def gens(self) -> list[str]:
        return ["writer"] + (["preader"] if self.p_conns else []) + (["reader"] if self.conns else [])

    @property
    def primary_clients(self) -> int:
        return self.w_conns + self.p_conns + (self.conns if self.read_target == "primary" else 0)

    @property
    def replica_clients(self) -> int:
        return 0 if self.read_target == "primary" else self.conns

    def cpus(self, gen: str, reader_host: bool = False) -> tuple[str, int]:
        if reader_host:
            return (SOLO_CPUS, SOLO_THREADS) if gen == "reader" else (PAIR_CPUS[gen], PAIR_THREADS[gen])
        if self.p_conns:
            return TRI_CPUS[gen], TRI_THREADS[gen]
        return (WRITER_CPUS, 39) if gen == "writer" else (READER_CPUS, 40)

    def threads(self, gen: str, reader_host: bool = False) -> int:
        """cachecannon threads for one generator: one per core, and at least one connection per thread."""
        conns = {"writer": self.w_conns, "preader": self.p_conns, "reader": self.conns}[gen]
        return min(self.cpus(gen, reader_host)[1], conns)


@dataclass
class ReplBin:
    name: str
    commit: str      # full sha; server at ~/offbench/builds/<commit>/src/valkey-server
    replica_args: str = ""  # extra server arguments on the replica only, e.g. "--keyspace-shard-bits 10"
    # Runs one more IO thread than the cell with main and io_thd_1 on one core, so it uses the same cores.
    share: int = 0
    # This build's primary load, overriding the cell's (0 = the cell's): set per build so every build's
    # primary delivers the same writes/s and reads/s (repl-calibrate).
    write_conns: int = 0
    p_conns: int = 0
    write_pipeline: int = 0
    p_pipeline: int = 0
    primary_args: str = ""  # extra server arguments on the primary only

    def cell_for(self, c: ReplCell) -> ReplCell:
        if self.share:
            c = replace(c, io_threads=c.io_threads + 1, pin_share=1)
        over = {k: getattr(self, k) for k in ("write_conns", "p_conns", "write_pipeline", "p_pipeline")
                if getattr(self, k)}
        return replace(c, **over) if over else c

    @classmethod
    def parse(cls, spec: str) -> "ReplBin":
        """name=sha[|config value|primary:config value...]; a config goes to the replica, or to the primary
        when it starts with primary:."""
        name, rest = spec.split("=", 1)
        commit, *cfg = rest.split("|")
        cfg = [c.strip() for c in cfg if c.strip()]
        prim = [c[len(PRIMARY_CFG):].strip() for c in cfg if c.startswith(PRIMARY_CFG)]
        rep = [c for c in cfg if not c.startswith(PRIMARY_CFG)]
        return cls(name, commit, " ".join(f"--{c}" for c in rep), primary_args=" ".join(f"--{c}" for c in prim))

    @property
    def server(self) -> str:
        return f"~/offbench/builds/{self.commit}/src/valkey-server"   # expanded on the host


def plan(cells: list[ReplCell], bins: list[ReplBin], reps: int) -> list[tuple[ReplCell, ReplBin, int]]:
    """Every build runs back to back inside a cell; the build order rotates per rep."""
    out = []
    for c in cells:
        for r in range(1, reps + 1):
            k = (r - 1) % len(bins)
            for b in bins[k:] + bins[:k]:
                out.append((c, b, r))
    return out


class Ssh:
    def __init__(self, key: str, known_hosts: str, src: str = "~/offbench-src"):
        self.key, self.known_hosts, self.src = key, known_hosts, src

    def _ssh(self, role: Role, remote: str) -> list[str]:
        return ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", f"UserKnownHostsFile={self.known_hosts}", "-i", self.key,
                "-o", "ControlMaster=auto", "-o", "ControlPath=/tmp/obr-%C", "-o", "ControlPersist=120",
                role.ssh, "bash -lc " + shlex.quote(remote)]

    def argv(self, role: Role, payload: dict) -> list[str]:
        return self._ssh(role, f"cd {self.src} && python3 -m offbench repl-agent {shlex.quote(json.dumps(payload))}")

    def fetch(self, role: Role, remote_path: str, dest: Path, timeout: float = 900) -> None:
        """Copy an absolute remote path to dest over the same connection, then delete the remote copy."""
        q = shlex.quote(remote_path)
        with Path(dest).open("wb") as f:
            r = subprocess.run(self._ssh(role, f"cat {q} && rm -f {q}"), stdout=f, stderr=subprocess.PIPE,
                               timeout=timeout)
        if r.returncode:
            raise CellError(f"{role.name}: fetch {remote_path} failed rc={r.returncode} "
                            f"{r.stderr.decode(errors='replace').strip()[-300:]}")

    def _parse(self, role: Role, payload: dict, r: subprocess.CompletedProcess) -> dict:
        lines = [l for l in r.stdout.splitlines() if l.startswith("{")]
        if not lines:
            raise CellError(f"{role.name} {payload['action']}: no reply rc={r.returncode} {r.stderr.strip()[-300:]}")
        out = json.loads(lines[-1])
        if "error" in out:
            raise CellError(f"{role.name} {payload['action']}: {out['error']}")
        return out

    def call(self, role: Role, payload: dict, timeout: float = 120) -> dict:
        r = subprocess.run(self.argv(role, payload), capture_output=True, text=True, timeout=timeout)
        return self._parse(role, payload, r)

    def start(self, role: Role, payload: dict) -> Callable[[], dict]:
        p = subprocess.Popen(self.argv(role, payload), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        def wait(timeout: float = 120) -> dict:
            out, err = p.communicate(timeout=timeout)
            return self._parse(role, payload, subprocess.CompletedProcess(p.args, p.returncode, out, err))
        return wait


def measure_rtt(ssh: Ssh, roles: dict) -> dict:
    """Median ICMP round trip on every path a rep uses, over the private IPs."""
    P, R, G = roles["primary"], roles["replica"], roles["gen"]
    readers = [roles[k] for k in reader_keys(roles)] or [G]
    paths = {"gen_primary": (G, P), **{rtt_path(i): (h, R) for i, h in enumerate(readers)},
             "primary_replica": (P, R)}
    waits = {k: (ssh.start(src, {"action": "rtt", "ips": [dst.ip]}), dst.ip) for k, (src, dst) in paths.items()}
    return {k: w(timeout=120)[ip] for k, (w, ip) in waits.items()}


def far_paths(rtt: dict, max_ms: float) -> dict:
    """Generator paths whose median round trip exceeds max_ms (or that got no replies)."""
    return {k: (v or {}).get("median_ms") for k, v in rtt.items()
            if k.startswith("gen_") and (v is None or v["median_ms"] > max_ms)}


SYNC_HINT = "sync it with: python3 -m offbench deploy --source-only --hosts <hosts>"


def preflight(ssh: Ssh, roles: dict, bins: list[ReplBin], pin: int = 1) -> list[str]:
    """What is missing or stale on the hosts before a run; empty when it can start.

    Server hosts need every build, valkey-cli and (pinned) pintune; generator hosts need cachecannon built
    under ~/offbench-src; every host must run the same offbench code as this checkout, since the host side
    of a rep is this package's repl-agent."""
    local = code_hashes()
    servers = sorted({b.server for b in bins})
    waits = {}
    for role, h in roles.items():
        srv = role in ("primary", "replica")
        waits[role] = ssh.start(h, {"action": "preflight", "servers": servers if srv else [],
                                    "pintune": bool(pin) and srv})
    problems = []
    for role, w in waits.items():
        h = roles[role]
        try:
            got = w(timeout=60)
        except CellError as e:
            problems.append(f"{h.name}: host offbench predates preflight; {SYNC_HINT}" if "unknown action" in str(e)
                            else f"{h.name}: {e}")
            continue
        stale = sorted(f for f, digest in local.items() if got["code"].get(f) != digest)
        if stale:
            problems.append(f"{h.name}: ~/offbench-src differs from this checkout in {', '.join(stale)}; {SYNC_HINT}")
        if role in ("primary", "replica"):
            problems += [f"{h.name}: no build at {s}" for s in got["missing_servers"]]
            if not got["cli"]:
                problems.append(f"{h.name}: no valkey-cli at ~/base/src/valkey-cli")
            if got.get("pintune") not in (True, None):
                problems.append(f"{h.name}: {got['pintune']}")
        elif not got["cachecannon"]:
            problems.append(f"{h.name}: cachecannon not built at ~/offbench-src/vendor/cachecannon/target/release")
    return problems


def _delta(a: dict, b: dict, k: str) -> float:
    return float(b["info"].get(k) or 0) - float(a["info"].get(k) or 0)


def server_metrics(m: dict) -> dict:
    a, b = m["a"], m["b"]
    span, uspan = b["t"] - a["t"], b["ut"] - a["ut"]
    total = _delta(a, b, "total_commands_processed")
    sets = cmdstat_calls(b["info"], "set") - cmdstat_calls(a["info"], "set")
    gets = cmdstat_calls(b["info"], "get") - cmdstat_calls(a["info"], "get")
    act = _delta(a, b, "used_active_time_main_thread")
    return {
        "span_s": span,
        "total_kcmd_s": total / span / 1000.0,
        "set_kcmd_s": sets / span / 1000.0,
        "get_kcmd_s_cmdstat": gets / span / 1000.0,
        "main_active_pct": act / span * 100.0 if b["info"].get("used_active_time_main_thread") else None,
        "main_cpu_pct": (b["ticks"]["main"] - a["ticks"]["main"]) / CLK_TCK / uspan * 100.0,
        "io_cores": (b["ticks"]["io"] - a["ticks"]["io"]) / CLK_TCK / uspan,
        "clients": int(b["info"].get("connected_clients") or 0),
        "repl_mb_s": _delta(a, b, "master_repl_offset") / span / 1e6,
        "sync_full_delta": _delta(a, b, "sync_full"),
        "link": [a["info"].get("master_link_status"), b["info"].get("master_link_status")],
        "sync_in_progress": b["info"].get("master_sync_in_progress"),
        "master_repl_offset_a": int(a["info"].get("master_repl_offset") or 0),
        "master_repl_offset_b": int(b["info"].get("master_repl_offset") or 0),
        "slave_repl_offset_a": int(a["info"].get("slave_repl_offset") or 0),
        "slave_repl_offset_b": int(b["info"].get("slave_repl_offset") or 0),
        "slave0": b["info"].get("slave0"),
        "mem_clients_slaves_b": int(b["info"].get("mem_clients_slaves") or 0),
        "fastpath_batches_delta": _delta(a, b, "fastpath_batches") if b["info"].get("fastpath_batches") else None,
        "primary_drain_jobs_delta": (_delta(a, b, "io_threaded_primary_drain_jobs")
                                     if b["info"].get("io_threaded_primary_drain_jobs") else None),
        "primary_drain_bytes_delta": (_delta(a, b, "io_threaded_primary_drain_bytes")
                                      if b["info"].get("io_threaded_primary_drain_bytes") else None),
        "ena": {k: b["ena"][k] - a["ena"][k] for k in ALLOWANCE_FIELDS},
    }


def build_record(cell: ReplCell, b: ReplBin, rep: int, roles: dict, pm: dict, rm: dict, gm: dict,
                 gens: dict, stops: dict, window_s: int, run_id: str, rgms: Optional[list] = None) -> dict:
    """rgms: one measurement per replica-reader host, in the order of the reader's parts."""
    p, r = server_metrics(pm), server_metrics(rm)
    # The replica's total includes the SETs it applied from the replication stream.
    r["read_kcmd_s"] = r["total_kcmd_s"] - r["set_kcmd_s"]
    p["read_kcmd_s"] = p["total_kcmd_s"] - p["set_kcmd_s"]

    def cores(m: dict) -> dict:
        span = m["b"]["gt"] - m["a"]["gt"]
        return {g: (m["b"]["gen_ticks"][g] - m["a"]["gen_ticks"][g]) / CLK_TCK / span for g in m["a"]["gen_ticks"]}

    def ena(m: dict) -> dict:
        return {k: m["b"]["ena"][k] - m["a"]["ena"][k] for k in ALLOWANCE_FIELDS}
    rgms = rgms or []
    gen_cores, rcores = cores(gm), [cores(m) for m in rgms]
    for g in (rcores[0] if rcores else {}):
        gen_cores[g] = sum(c.get(g, 0.0) for c in rcores)
    for part, m, c in zip((gens.get("reader") or {}).get("parts") or [], rgms, rcores):
        part["cores"], part["ena"] = c.get("reader"), ena(m)
    gen_ena = ena(gm)
    lag = p["master_repl_offset_b"] - r["slave_repl_offset_b"]
    lag_a = p["master_repl_offset_a"] - r["slave_repl_offset_a"]
    p["write_kcmd_s"] = p["set_kcmd_s"]
    r["apply_deficit_pct"] = (1 - r["set_kcmd_s"] / p["set_kcmd_s"]) * 100.0 if p["set_kcmd_s"] else None
    rec = {
        "kind": "replrw", "schema": 1, "run_id": run_id, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bin": asdict(b), "cell": asdict(cell), "rep": rep, "window_s": window_s,
        "hosts": {k: v.name for k, v in roles.items()},
        "primary": p, "replica": r,
        "writer": {**gens["writer"], "cores": gen_cores.get("writer")},
        "reader": ({**gens["reader"], "cores": gen_cores.get("reader")} if "reader" in gens else None),
        "preader": ({**gens["preader"], "cores": gen_cores.get("preader")} if "preader" in gens else None),
        "gen_ena": gen_ena,
        "rgen_ena": {k: sum(ena(m)[k] for m in rgms) for k in ALLOWANCE_FIELDS} if rgms else None,
        "repl_lag_bytes_a": lag_a,
        "repl_lag_bytes_b": lag,
        # Lag growth over the window; ~0 means the replica applies writes as fast as they arrive.
        "lag_growth_mb_s": (lag - lag_a) / p["span_s"] / 1e6,
        "stops": stops,
    }
    rec["schema"] = 2
    rec["invalid"] = invalid_reasons(rec)
    rec["valid"] = not rec["invalid"]
    return rec


def invalid_reasons(rec: dict) -> list[str]:
    why = []
    p, r = rec["primary"], rec["replica"]
    if r["link"] != ["up", "up"] or r["sync_in_progress"] not in ("0", None):
        why.append(f"replica link {r['link']} sync_in_progress={r['sync_in_progress']}")
    if p["sync_full_delta"]:
        why.append(f"full resync during window ({p['sync_full_delta']:.0f})")
    for side in ("primary", "replica"):
        s = rec["stops"].get(side, {})
        if s.get("asserts"):
            why.append(f"{side} asserted")
        if s.get("client_closed_for_obuf"):
            why.append(f"{side} closed {s['client_closed_for_obuf']} clients for output buffer")
    for g in ("writer", "reader", "preader"):
        gg = rec.get(g)
        if gg is None:
            continue
        if not gg.get("alive_at_stop"):
            why.append(f"{g} generator died: {gg.get('err_tail')}")
        if gg.get("timeouts"):
            why.append(f"{g} timeouts={gg['timeouts']}")
        if gg.get("err_pct"):
            why.append(f"{g} reported {gg['err_pct']:g}% errors")
    for host, ena in (("primary", p["ena"]), ("replica", r["ena"]), ("gen", rec["gen_ena"]),
                      ("reader gen", rec.get("rgen_ena") or {})):
        hit = {k: v for k, v in ena.items() if v}
        if hit:
            why.append(f"{host} ENA allowance exceeded {hit}")
    c = rec["cell"]
    wc, rc = c.get("write_conns") or c["conns"], c["conns"]
    pw, rw = (wc + rc, 0) if c.get("read_target") == "primary" else (wc, rc)
    pw += c.get("p_conns") or 0
    if p["clients"] < pw or r["clients"] < rw:
        why.append(f"clients primary={p['clients']}<{pw} or replica={r['clients']}<{rw}")
    # A calibrated run holds the primary at X writes/s and Y reads/s; a rep that drifted answers another question.
    t = rec.get("targets") or {}
    for k, what in (("write_kcmd_s", "writes"), ("read_kcmd_s", "reads")):
        if t.get(k) and abs(p[k] / t[k] - 1) > t.get("tol", DRIFT_TOL):
            why.append(f"primary {what} {p[k]:.0f}k/s, {p[k] / t[k] - 1:+.1%} off target {t[k]:.0f}k/s")
    # A replica that skips writes frees its own capacity for reads, so its read rate is not a result.
    if not keeps_up(rec):
        applied = 100.0 * r["set_kcmd_s"] / p["write_kcmd_s"] if p["write_kcmd_s"] else 0.0
        why.append(f"{LAGGING}: applied {applied:.0f}% of primary writes, lag +{rec['lag_growth_mb_s']:.1f} MB/s")
    return why


def run_one(ssh: Ssh, roles: dict, cell: ReplCell, b: ReplBin, rep: int, *, window_s: int,
            all_servers: list[str], run_id: str, log: Callable[[str], None]) -> dict:
    P, R, G = roles["primary"], roles["replica"], roles["gen"]
    acl = bool(cell.acl)
    for h in roles.values():
        ssh.call(h, {"action": "clean", "servers": all_servers})
    common = {"io_threads": cell.io_threads, "acl": cell.acl, "pin": cell.pin, "server": b.server,
              "pin_share": cell.pin_share}
    pp = ssh.call(P, {"action": "server_start", "role": "primary", "bind_ip": P.ip,
                      "primary_args": b.primary_args, **common})
    rp = ssh.call(R, {"action": "server_start", "role": "replica", "bind_ip": R.ip, "primary_ip": P.ip,
                      "replica_args": b.replica_args, **common})

    def wait_for(what: str, fn: Callable[[], bool], limit: float) -> None:
        t0 = time.time()
        while not fn():
            if time.time() - t0 > limit:
                raise CellError(f"timeout waiting for {what}")
            time.sleep(0.5)

    wait_for("replica link", lambda: ssh.call(R, {"action": "status", "acl": acl}).get("master_link_status") == "up", 30)
    gen = {"keys": cell.keys, "value_size": cell.value_size, "acl": cell.acl, "window_s": window_s}
    # What this rep started where: each server's argv, post-start commands and thread pinning, and each
    # generator's argv and config (by sha256; run_matrix writes the texts to its launch_dir).
    role_of = {v.name: k for k, v in roles.items()}
    launch = {side: {k: got.get(k) for k in ("argv", "post", "pins")} for side, got in (("primary", pp), ("replica", rp))}
    launch["gens"] = []
    tomls: dict = {}

    def started(h: Role, name: str, conns: int, got: dict) -> bool:
        text = got.get("toml")
        sha = hashlib.sha256(text.encode()).hexdigest() if text else None
        if text:
            tomls[sha] = text
        launch["gens"].append({"name": name, "role": role_of.get(h.name), "conns": conns, "toml_sha256": sha,
                               "argv": got.get("argv")})
        return bool(got.get("alive"))
    # Replica readers run on their own host or hosts when given (--reader-gen), with the connections split
    # evenly; the rest on the generator host.
    RGs = [roles[k] for k in reader_keys(roles)]
    own = bool(RGs)
    g_gens = [g for g in cell.gens if not (own and g == "reader")]
    readers = [(h, n) for h, n in zip(RGs, split_conns(cell.conns, len(RGs))) if n] if own else []
    reader_cap = cell.cpus("reader", own)[1]
    # cachecannon needs at least one connection per thread
    if not started(G, "writer", cell.w_conns,
                   ssh.call(G, {"action": "gen_start", "name": "writer", "workload": "set", "target_ip": P.ip,
                                "pipeline": cell.w_pipeline, "conns": cell.w_conns, "prefill": True,
                                "cpus": cell.cpus("writer", own)[0],
                                "threads": cell.threads("writer", own), "duration_s": 240, **gen})):
        raise CellError("writer did not start")
    wait_for("replica prefill", lambda: ssh.call(R, {"action": "status", "acl": acl})["dbsize"] >= cell.keys, 60)
    rt = P if cell.read_target == "primary" else R
    for h, n in readers or ([(G, cell.conns)] if cell.conns else []):
        if not started(h, "reader", n,
                       ssh.call(h, {"action": "gen_start", "name": "reader", "workload": "get", "target_ip": rt.ip,
                                    "pipeline": cell.pipeline, "conns": n, "prefill": False,
                                    "cpus": cell.cpus("reader", own)[0], "threads": min(reader_cap, n),
                                    "duration_s": 200, **gen})):
            raise CellError(f"reader did not start on {h.name}")
    if cell.p_conns and not started(G, "preader", cell.p_conns,
                                    ssh.call(G, {"action": "gen_start", "name": "preader", "workload": "get",
                                                 "target_ip": P.ip, "pipeline": cell.pp_pipeline,
                                                 "conns": cell.p_conns, "prefill": False,
                                                 "cpus": cell.cpus("preader", own)[0],
                                                 "threads": cell.threads("preader", own),
                                                 "duration_s": 200, **gen})):
        raise CellError("primary reader did not start")

    def ready() -> bool:
        alive = ssh.call(G, {"action": "gen_alive", "gens": g_gens})
        for h, _ in readers:
            alive[f"reader@{h.name}"] = ssh.call(h, {"action": "gen_alive", "gens": ["reader"]})["reader"]
        if not all(alive.values()):
            raise CellError(f"generator exited before gate: {alive}")
        ps, rs = ssh.call(P, {"action": "status", "acl": acl}), ssh.call(R, {"action": "status", "acl": acl})
        pw, rw = cell.primary_clients, cell.replica_clients
        return (int(ps["connected_clients"] or 0) >= pw and int(rs["connected_clients"] or 0) >= rw
                and all(int(s["instantaneous_ops_per_sec"] or 0) > 10_000 for s in (ps, rs)))
    wait_for("both sides ready", ready, 90)
    time.sleep(3)

    def measure_window(lead: float) -> list:
        """All hosts snapshot at the same absolute time; every started call is drained before reporting."""
        at = time.time() + lead
        calls = [(P, {"server": True, "pid": pp["pid"], "acl": acl}), (R, {"server": True, "pid": rp["pid"], "acl": acl}),
                 (G, {"gens": g_gens})] + [(h, {"gens": ["reader"]}) for h, _ in readers]
        waits = [ssh.start(h, {"action": "measure", "at": at, "window_s": window_s, **extra}) for h, extra in calls]
        out, err = [], None
        for w in waits:
            try:
                out.append(w())
            except CellError as e:
                err = err or e
                out.append(None)
        if err:
            raise err
        return out

    try:
        pm, rm, gm, *rgms = measure_window(2.5)
    except CellError as e:
        # A host whose ssh session starts late misses the shared start time; open a new window with more lead.
        if "already passed" not in str(e):
            raise
        log(f"measure start missed ({e}); retrying the window with a 10 s lead")
        pm, rm, gm, *rgms = measure_window(10.0)
    time.sleep(1.2)
    stop = {"w_a": max(pm["a"]["t"], rm["a"]["t"]), "w_b": min(pm["b"]["t"], rm["b"]["t"])}
    gens = ssh.call(G, {"action": "gen_stop", "gens": g_gens, **stop})
    for g in gens:
        gens[g]["threads"] = cell.threads(g, own)
    parts = [{**ssh.call(h, {"action": "gen_stop", "gens": ["reader"], **stop})["reader"],
              "host": h.name, "conns": n, "threads": min(reader_cap, n)} for h, n in readers]
    if len(parts) == 1:
        gens["reader"] = {k: v for k, v in parts[0].items() if k not in ("host", "conns")}
    elif parts:
        gens["reader"] = merge_readers(parts)
    for g in gens.values():
        g.pop("samples", None)
    stops = {"primary": ssh.call(P, {"action": "server_stop", "role": "primary", "server": b.server}),
             "replica": ssh.call(R, {"action": "server_stop", "role": "replica", "server": b.server})}
    rec = build_record(cell, b, rep, roles, pm, rm, gm, gens, stops, window_s, run_id, rgms=rgms)
    rec["launch"] = launch
    rec["_tomls"] = tomls     # run_matrix keeps these out of the record
    return rec


def merge_readers(parts: list[dict]) -> dict:
    """One replica-reader result from generators on several hosts. Latency is merged from their window samples
    by the same rule as one host's generators, over the seconds every host reported; err_pct is the worst
    host's."""
    timeouts = [p["timeouts"] for p in parts if p.get("timeouts") is not None]
    errs = [p["err_pct"] for p in parts if p.get("err_pct") is not None]
    keep = ("host", "conns", "threads", "alive_at_stop", "timeouts", "err_pct", "err_tail", "latency")
    samples = [dict(s, src=p["host"]) for p in parts for s in p.get("samples") or []]
    return {"alive_at_stop": all(p.get("alive_at_stop") for p in parts),
            "latency": asdict(latency_from_window(samples)),
            "timeouts": sum(timeouts) if timeouts else None,
            "err_pct": max(errs) if errs else None,
            "err_tail": next((p["err_tail"] for p in parts if p.get("err_tail")), ""),
            "threads": sum(p["threads"] for p in parts),
            "parts": [{k: p.get(k) for k in keep} for p in parts]}


def cleanup(ssh: Ssh, roles: dict, all_servers: list[str]) -> None:
    for h in roles.values():
        try:
            ssh.call(h, {"action": "clean", "servers": all_servers}, timeout=60)
        except Exception:
            pass


def run_matrix(ssh: Ssh, roles: dict, cells: list[ReplCell], bins: list[ReplBin], reps: int, out: Path,
               window_s: int = 6, log: Callable[[str], None] = print, meta: Optional[dict] = None,
               run_id: Optional[str] = None, collect: Optional[list] = None, tee: Optional[Path] = None,
               launch_dir: Optional[Path] = None) -> int:
    """tee: a second file every record is appended to (the archive's records.jsonl); launch_dir: where each
    distinct generator config is written once, as <sha256>.toml."""
    run_id = run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    items = plan(cells, bins, reps)
    all_servers = [b.server for b in bins]
    out.parent.mkdir(parents=True, exist_ok=True)
    log(f"run {run_id}: {len(items)} reps, roles " + ", ".join(f"{k}={v.name}" for k, v in roles.items()))
    errors_in_row = 0
    for i, (c, b, rep) in enumerate(items, 1):
        c = b.cell_for(c)
        t0 = time.time()
        try:
            rec = run_one(ssh, roles, c, b, rep, window_s=window_s, all_servers=all_servers, run_id=run_id, log=log)
            errors_in_row = 0
        except Exception as exc:
            cleanup(ssh, roles, all_servers)
            rec = {"kind": "replrw", "schema": 1, "run_id": run_id, "bin": asdict(b), "cell": asdict(c), "rep": rep,
                   "error": f"{type(exc).__name__}: {exc}", "valid": False,
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            errors_in_row += 1
        rec.update(meta or {})
        revalidate(rec)   # meta can carry targets the rep is judged against
        tomls = rec.pop("_tomls", None) or {}
        if launch_dir is not None:
            for sha, text in tomls.items():
                if not (launch_dir / f"{sha}.toml").exists():
                    (launch_dir / f"{sha}.toml").write_text(text)
        line = json.dumps(rec) + "\n"
        for path in [out] + ([tee] if tee is not None else []):
            with path.open("a") as f:
                f.write(line)
        if collect is not None:
            collect.append(rec)
        if "error" in rec:
            log(f"[{i}/{len(items)}] {b.name:<8} {c.key():<22} rep{rep}: ERROR {rec['error']}")
            if errors_in_row >= 3:
                log("3 errors in a row, stopping")
                return 1
            continue
        p, r = rec["primary"], rec["replica"]
        log(f"[{i}/{len(items)}] {b.name:<8} {c.key():<22} rep{rep}: write {p['write_kcmd_s']:7.0f}k "
            f"p.read {p['read_kcmd_s']:7.0f}k (main {p['main_active_pct'] or 0:5.1f}% cpu {p['main_cpu_pct']:5.1f}%)  read {r['read_kcmd_s']:7.0f}k "
            f"(main {r['main_active_pct'] or 0:5.1f}% cpu {r['main_cpu_pct']:5.1f}%)  lag {rec['repl_lag_bytes_b']/1e6:.1f}MB "
            f"+{rec['lag_growth_mb_s']:.1f}MB/s "
            f"applied {r['set_kcmd_s']:6.0f}k ({time.time() - t0:.0f}s)" + ("" if rec["valid"] else "  INVALID: " + "; ".join(rec["invalid"])))
    cleanup(ssh, roles, all_servers)
    log(f"run {run_id} done")
    return 0


def load(path: Path, run_id: Optional[str] = None) -> list[dict]:
    recs = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    recs = [r for r in recs if r.get("kind") == "replrw" and (run_id is None or r.get("run_id") == run_id)]
    for r in recs:
        revalidate(r)
    return recs


def revalidate(r: dict) -> dict:
    """Apply the current validity rules to a stored record (older records predate the lag rule)."""
    if "error" not in r and r.get("schema", 1) >= 2:
        r["invalid"] = invalid_reasons(r)
        r["valid"] = not r["invalid"]
    return r


def summary(recs: list[dict]) -> str:
    """Median per (cell, bin) across valid reps."""
    groups: dict = {}
    for r in recs:
        if "error" in r:
            continue
        k = (r["cell"]["io_threads"], r["cell"]["pipeline"], r["cell"]["conns"], r["bin"]["name"])
        groups.setdefault(k, []).append(r)
    med = lambda xs: statistics.median(xs) if xs else float("nan")
    rows = ["iot  P  conns  build     valid  write k/s  p.main%  read k/s  r.main%  r.cpu%  apply k/s  lagMB  w.p99us  r.p99us"]
    for k in sorted(groups):
        rs = groups[k]
        v = [x for x in rs if x["valid"]]
        if not v:
            why = next((x["invalid"][0] for x in rs if x.get("invalid")), "")
            rows.append(f"{k[0]:>3} {k[1]:>2} {k[2]:>6}  {k[3]:<8} 0/{len(rs)}  INVALID: {why}")
            continue
        rows.append(
            f"{k[0]:>3} {k[1]:>2} {k[2]:>6}  {k[3]:<8} {sum(x['valid'] for x in rs)}/{len(rs)}  "
            f"{med([x['primary']['write_kcmd_s'] for x in v]):9.0f}  {med([x['primary']['main_active_pct'] or 0 for x in v]):7.1f}  "
            f"{med([x['replica']['read_kcmd_s'] for x in v]):8.0f}  {med([x['replica']['main_active_pct'] or 0 for x in v]):7.1f}  "
            f"{med([x['replica']['main_cpu_pct'] for x in v]):6.1f}  {med([x['replica']['set_kcmd_s'] for x in v]):9.0f}  {med([x['repl_lag_bytes_b'] / 1e6 for x in v]):5.1f}  "
            f"{med([(x['writer']['latency'] or {}).get('p99_us') or 0 for x in v]):7.0f}  "
            f"{med([(x['reader']['latency'] or {}).get('p99_us') or 0 for x in v]):7.0f}")
    return "\n".join(rows)


def keeps_up(r: dict) -> bool:
    """Replica applied >= 99% of the primary's writes and its lag did not grow."""
    p, rr = r["primary"], r["replica"]
    return (p["write_kcmd_s"] > 0 and rr["set_kcmd_s"] >= 0.99 * p["write_kcmd_s"]
            and r["lag_growth_mb_s"] <= max(0.5, 0.01 * p["repl_mb_s"]))


def sweep_summary(recs: list[dict]) -> str:
    """Read-client sweep: per point medians, then each build's sustainable read ceiling.

    Points are grouped by cores per server, the primary's load (writers, primary readers) and build, so a
    calibration probe never merges into a sweep step. A point is invalid when the replica fell behind in any
    rep, or when fewer than a majority of its reps are valid. The ceiling comes from points below the first
    one where the replica fell behind."""
    groups: dict = {}
    for r in recs:
        if "error" in r or r.get("schema", 1) < 2:
            continue
        c = r["cell"]
        k = (c["io_threads"] - (c.get("pin_share") or 0), c["pipeline"], r["bin"]["name"],
             c.get("write_conns") or c["conns"], c.get("p_conns") or 0, c["conns"])
        groups.setdefault(k, []).append(r)
    med = lambda xs: statistics.median(xs) if xs else float("nan")
    rows = ["cores  P  build     wconn pconn rconns  valid  write k/s  apply k/s  read k/s  r.main%  lag+MB/s  r.p99us"]
    ceil: dict = {}
    broken: set = set()
    for k in sorted(groups):
        rs = groups[k]
        v = [x for x in rs if x["valid"]]
        behind = [x for x in rs if fell_behind(x)]
        head = f"{k[0]:>5} {k[1]:>2}  {k[2]:<8} {k[3]:>5} {k[4]:>5} {k[5]:>6}  {len(v)}/{len(rs)}"
        ck = k[:5]
        if behind or len(v) * 2 <= len(rs):
            why = (f"{LAGGING} in {len(behind)}/{len(rs)} reps" if behind
                   else next((x["invalid"][0] for x in rs if x.get("invalid")), "no valid reps"))
            rows.append(f"{head}  INVALID: {why}")
            if behind and k[5]:
                broken.add(ck)
            continue
        read = med([x["replica"]["read_kcmd_s"] for x in v])
        rows.append(
            f"{head}  {med([x['primary']['write_kcmd_s'] for x in v]):9.0f}  {med([x['replica']['set_kcmd_s'] for x in v]):9.0f}  "
            f"{read:8.0f}  {med([x['replica']['main_cpu_pct'] for x in v]):7.1f}  "
            f"{med([x['lag_growth_mb_s'] for x in v]):8.1f}  "
            f"{med([((x['reader'] or {}).get('latency') or {}).get('p99_us') or 0 for x in v]):7.0f}")
        if k[5] and ck not in broken and (ck not in ceil or read > ceil[ck][0]):
            ceil[ck] = (read, k[5])
    rows += ["", "sustainable read ceiling (highest median read rate over points in sync, below the first point "
                 "where the replica fell behind)",
             "cores  P  build     wconn pconn  read k/s  at rconns"]
    for ck in sorted({k[:5] for k in groups}):
        c = ceil.get(ck)
        rows.append(f"{ck[0]:>5} {ck[1]:>2}  {ck[2]:<8} {ck[3]:>5} {ck[4]:>5} "
                    + (f"{c[0]:8.0f}  {c[1]:>9}" if c else "       -  never"))
    return "\n".join(rows)
