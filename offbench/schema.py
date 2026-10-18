"""One record per measured repetition. Everything else (compare, export,
dashboard) reads this shape and nothing else."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional

SCHEMA = 1

# A cell counts as a server comparison only when main is pinned. Below this the
# generator, not the server, set the number.
MAIN_SATURATION_PCT = 98.0


@dataclass
class Bin:
    name: str                      # directory name on the rig (base, branchfix, wlnew ...)
    commit: Optional[str] = None   # short sha; None when unknown
    branch: Optional[str] = None
    label: Optional[str] = None    # human label for charts
    role: str = "candidate"        # stock | candidate | variant
    make_args: str = ""
    text_bytes: Optional[int] = None
    server_args: str = ""          # extra valkey-server arguments for this build, e.g. "--keyspace-shard-bits 10"


@dataclass
class Cell:
    workload: str                  # get | set | mixed
    pipeline: int
    conns: int
    io_threads: int = 16
    keys: int = 100000
    value_size: int = 64
    acl: int = 0
    pin: int = 0
    rate: int = 0                  # fixed offered load in cmd/s; 0 = closed loop (saturation)

    def key(self) -> str:
        return f"{self.workload}-P{self.pipeline}-c{self.conns}-iot{self.io_threads}" + (
            "-acl" if self.acl else "") + (f"-r{self.rate // 1000}k" if self.rate else "")


@dataclass
class Perf:
    cyc_per_cmd: Optional[float] = None
    ins_per_cmd: Optional[float] = None
    ipc: Optional[float] = None
    l1i_miss_per_cmd: Optional[float] = None
    l1d_miss_per_cmd: Optional[float] = None
    br_miss_per_cmd: Optional[float] = None


@dataclass
class Latency:
    """Client-observed request latency from cachecannon's per-second samples inside the
    measurement window: median across seconds for the percentiles, max of the maxima."""
    p50_us: Optional[float] = None
    p90_us: Optional[float] = None
    p99_us: Optional[float] = None
    p999_us: Optional[float] = None
    p9999_us: Optional[float] = None
    max_us: Optional[float] = None
    samples: int = 0                    # seconds inside the window that carried a sample
    gen_kreq_s: Optional[float] = None  # generator-side mean req/s over those seconds


# A fixed-rate cell is valid when the generator actually delivered the rate.
RATE_TOLERANCE_PCT = 5.0


@dataclass
class Metrics:
    kcmd_s: float                  # server-side total_commands_processed delta / window
    main_active_pct: Optional[float] = None
    main_cpu_pct: Optional[float] = None
    clients: Optional[int] = None
    gen_cores: Optional[float] = None
    gen_cores_total: Optional[int] = None
    asserts: int = 0
    fp_batches_s: Optional[float] = None
    gen_timeouts: Optional[int] = None
    gen_err_pct: Optional[float] = None
    perf: Optional[Perf] = None
    latency: Optional[Latency] = None
    span_s: Optional[float] = None       # measured counter span the rates divide by
    server_cpu_cores: Optional[float] = None    # CPU of every server thread over the window, in cores
    thread_cpu_pct: Optional[dict] = None       # per server thread (name, tid when names repeat): CPU % over the window


@dataclass
class Record:
    bin: Bin
    cell: Cell
    rep: int
    metrics: Metrics
    generator: str = "cachecannon"  # cachecannon | valkey-benchmark
    generator_version: Optional[str] = None
    ts: Optional[str] = None
    task_id: Optional[str] = None
    note: str = ""
    source: dict = field(default_factory=dict)   # {"kind": "runner"} | {"kind": "backfill", "file", "line"}
    host: dict = field(default_factory=dict)     # {"id", "type", "kernel"}
    schema: int = SCHEMA

    # ---- validity -------------------------------------------------------
    def invalid_reasons(self) -> list[str]:
        r = []
        m = self.metrics
        if m.asserts:
            r.append(f"asserts={m.asserts}")
        if self.cell.rate:
            target = self.cell.rate / 1000.0
            if abs(m.kcmd_s - target) / target * 100.0 > RATE_TOLERANCE_PCT:
                r.append(f"rate {m.kcmd_s:.0f}k vs target {target:.0f}k (>{RATE_TOLERANCE_PCT:.0f}%)")
        elif m.main_active_pct is None:
            r.append("main_active unknown")
        elif m.main_active_pct < MAIN_SATURATION_PCT:
            r.append(f"main_active {m.main_active_pct:.1f}% < {MAIN_SATURATION_PCT:.0f}%")
        if m.clients is not None and m.clients < self.cell.conns:
            r.append(f"clients {m.clients} < conns {self.cell.conns}")
        if m.gen_timeouts:
            r.append(f"gen_timeouts={m.gen_timeouts}")
        return r

    @property
    def valid(self) -> bool:
        return not self.invalid_reasons()

    # ---- (de)serialisation ------------------------------------------------
    def to_dict(self) -> dict:
        d = asdict(self)
        d["valid"] = self.valid
        d["invalid_reasons"] = self.invalid_reasons()
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, d: dict) -> "Record":
        d = dict(d)
        d.pop("valid", None)
        d.pop("invalid_reasons", None)
        m = dict(d.pop("metrics"))
        perf = m.pop("perf", None)
        lat = m.pop("latency", None)
        return cls(
            bin=Bin(**d.pop("bin")),
            cell=Cell(**d.pop("cell")),
            metrics=Metrics(perf=Perf(**perf) if perf else None, latency=Latency(**lat) if lat else None, **m),
            **d,
        )


def append_records(path: Path, records: Iterable[Record]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(r.to_json() + "\n")
            n += 1
    return n


def read_records(path: Path) -> Iterator[Record]:
    if not path.exists():
        return
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield Record.from_dict(json.loads(line))
