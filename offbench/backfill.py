"""Parse the rig's cccell.sh / ccmatrix_full.sh / batchdiag.sh logs into Records.

This is how all data so far was produced: the runner has never been deployed on
a rig, so the bash-script logs under ../rig/c8g/*.log are the source of truth.
Each measured rep becomes one schema.Record tagged
source={'kind':'backfill','file','line'}.

Line shapes handled (all produced on the c8g.metal-24xl):

  stock-dcc66eff8 acl=0 P1  c2400  get   rep1:  206 k cmd/s  main_active= 98.3%  main_cpu=101.5%  clients=2401  gen_cores=1.7/106
    server :1986245 :0  gen_err_s_sum=0
    lat rate=0 samples=5 gen_kreq_s=203.6 p50=11796 p90=11862 p99=11862 p999=11862 p9999=23592 max=23724
    asserts=0
  base P1 c2400 get rep1:  1524 k cmd/s  main_active= 75.5%  ... fp_batches/s=0
    perf(main): cyc/cmd=757 ins/cmd=2860.9 IPC=3.78 L1i-miss/cmd=0.31 L1d-miss/cmd=7.96 br-miss/cmd=2.595
    cachecannon: samples=23 sample_errors=0  result={...json...}
  cc P16 c2400 get ccbase-rep1  4214 k cmd/s ... main_active=106.2%  fp_clients=2400   (batchdiag)
    main perf: cycles/cmd=663  ins/cmd=2731  IPC=4.12 ...                               (batchdiag: precedes cell line)

"=== IOT=N" / "=== PIN=B" headers set io_threads / pin for the following lines
(what ccmatrix_full.sh emits per io-threads block).
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from .schema import Bin, Cell, Latency, Metrics, Perf, Record

REP_RE = re.compile(
    r"^(?P<bin>\S+)\s+(?:acl=(?P<acl>\d)\s+)?P(?P<p>\d+)\s+c(?P<c>\d+)\s+(?P<w>get|set|mixed)\s+"
    r"rep(?P<rep>\d+):\s+(?P<k>\d+)\s+k cmd/s\s+main_active=\s*(?P<ma>[\d.]+)%"
    r"(?:\s+main_cpu=\s*(?P<mc>[\d.]+)%)?(?:\s+clients=(?P<cl>\d+))?"
    r"(?:\s+gen_cores=(?P<gc>[\d.]+)/(?P<gct>\d+))?(?:\s+fp_batches/s=(?P<fpb>[\d.]+))?"
)
BATCHDIAG_RE = re.compile(
    r"^(?P<gen>cc|vb)\s+P(?P<p>\d+)\s+c(?P<c>\d+)\s+(?P<w>get|set|mixed)\s+(?P<bin>[\w.-]+?)-r(?:ep)?(?P<rep>\d+)\s+"
    r"(?P<k>\d+)\s+k cmd/s.*?main_active=\s*(?P<ma>[\d.]+)%(?:\s+fp_clients=(?P<cl>\d+))?"
)
PERF_AFTER_RE = re.compile(
    r"^\s*perf\(main\):\s+cyc/cmd=(?P<cyc>[\d.]+)\s+ins/cmd=(?P<ins>[\d.]+)\s+IPC=(?P<ipc>[\d.]+)"
    r"(?:\s+L1i-miss/cmd=(?P<l1i>[\d.]+))?(?:\s+L1d-miss/cmd=(?P<l1d>[\d.]+))?(?:\s+br-miss/cmd=(?P<br>[\d.]+))?"
)
PERF_BEFORE_RE = re.compile(
    r"^\s*main perf:\s+cycles/cmd=(?P<cyc>[\d.]+)\s+ins/cmd=(?P<ins>[\d.]+)\s+IPC=(?P<ipc>[\d.]+)"
)
ASSERTS_RE = re.compile(r"^\s*asserts=(?P<n>\d+)")
IOT_RE = re.compile(r"^===\s+IOT=(?P<iot>\d+)")
PIN_RE = re.compile(r"^===\s+PIN=(?P<pin>[01])")
L1I_RAW_RE = re.compile(r"^\s*(?P<n>[\d,]+)\s+L1-icache-load-misses:u")
CC_RESULT_RE = re.compile(r"^\s*cachecannon:.*?result=(?P<json>\{.*\})\s*$")
LAT_RE = re.compile(
    r"^\s*lat\s+rate=(?P<rate>\d+)\s+samples=(?P<n>\d+)"
    r"(?:\s+gen_kreq_s=(?P<kr>[\d.]+)\s+p50=(?P<p50>\d+)\s+p90=(?P<p90>\d+)\s+p99=(?P<p99>\d+)"
    r"\s+p999=(?P<p999>\d+)\s+p9999=(?P<p9999>\d+)\s+max=(?P<max>\d+))?"
)

# The c8g.metal-24xl that produced every log so far.
DEFAULT_HOST = {"id": "metal-1", "type": "c8g.metal-24xl", "kernel": None}


def load_bins(path: Path) -> dict[str, Bin]:
    """Optional bin catalogue: {name: {commit, branch, label, role, ...}}."""
    raw = json.loads(Path(path).read_text())
    return {name: Bin(name=name, **spec) for name, spec in raw.items()}


def _bin(name: str, bins: dict[str, Bin]) -> Bin:
    return bins.get(name) or Bin(name=name, label=name)


def generator_for(path: Path, first_token: Optional[str] = None) -> str:
    if first_token == "vb":
        return "valkey-benchmark"
    if first_token == "cc":
        return "cachecannon"
    return "valkey-benchmark" if path.name.startswith("matrix_") else "cachecannon"


def parse_log(path: Path, bins: Optional[dict[str, Bin]] = None,
              host: Optional[dict] = None) -> Iterator[Record]:
    """Yield one Record per measured rep in a single bash-script log."""
    path = Path(path)
    bins = bins or {}
    host = host or DEFAULT_HOST
    ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    io_threads = 16
    pin = 0
    last: Optional[Record] = None
    pending_perf: Optional[Perf] = None

    with path.open(encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip("\n")

            m = IOT_RE.match(line)
            if m:
                io_threads = int(m["iot"])
                continue

            m = PIN_RE.match(line)
            if m:
                pin = int(m["pin"])
                continue

            m = PERF_BEFORE_RE.match(line)
            if m:
                pending_perf = Perf(float(m["cyc"]), float(m["ins"]), float(m["ipc"]))
                continue

            m = REP_RE.match(line) or BATCHDIAG_RE.match(line)
            if m:
                g = m.groupdict()
                if last is not None:
                    yield last
                cell = Cell(
                    workload=g["w"], pipeline=int(g["p"]), conns=int(g["c"]),
                    io_threads=io_threads, acl=int(g.get("acl") or 0), pin=pin,
                )
                metrics = Metrics(
                    kcmd_s=float(g["k"]),
                    main_active_pct=float(g["ma"]),
                    main_cpu_pct=float(g["mc"]) if g.get("mc") else None,
                    clients=int(g["cl"]) if g.get("cl") else None,
                    gen_cores=float(g["gc"]) if g.get("gc") else None,
                    gen_cores_total=int(g["gct"]) if g.get("gct") else None,
                    fp_batches_s=float(g["fpb"]) if g.get("fpb") else None,
                    perf=pending_perf,
                )
                pending_perf = None
                last = Record(
                    bin=_bin(g["bin"], bins), cell=cell, rep=int(g["rep"]), metrics=metrics,
                    generator=generator_for(path, g.get("gen")), ts=ts,
                    source={"kind": "backfill", "file": path.name, "line": lineno},
                    host=host, note=f"backfilled from {path.name} (ts = file mtime)",
                )
                continue

            if last is None:
                continue

            m = PERF_AFTER_RE.match(line)
            if m:
                last.metrics.perf = Perf(
                    float(m["cyc"]), float(m["ins"]), float(m["ipc"]),
                    float(m["l1i"]) if m["l1i"] else None,
                    float(m["l1d"]) if m["l1d"] else None,
                    float(m["br"]) if m["br"] else None,
                )
                continue

            m = ASSERTS_RE.match(line)
            if m:
                last.metrics.asserts = int(m["n"])
                continue

            m = LAT_RE.match(line)
            if m:
                last.cell.rate = int(m["rate"])
                if m["p50"]:
                    last.metrics.latency = Latency(
                        p50_us=float(m["p50"]), p90_us=float(m["p90"]), p99_us=float(m["p99"]),
                        p999_us=float(m["p999"]), p9999_us=float(m["p9999"]), max_us=float(m["max"]),
                        samples=int(m["n"]), gen_kreq_s=float(m["kr"]),
                    )
                else:
                    last.metrics.latency = Latency(samples=0)
                continue

            m = L1I_RAW_RE.match(line)
            if m and last.metrics.perf and last.metrics.perf.l1i_miss_per_cmd is None and last.metrics.kcmd_s:
                # batchdiag prints a raw perf stat block after its cell line; window is 6 s.
                last.metrics.perf.l1i_miss_per_cmd = int(m["n"].replace(",", "")) / (last.metrics.kcmd_s * 1000 * 6)
                continue

            m = CC_RESULT_RE.match(line)
            if m:
                try:
                    res = json.loads(m["json"])
                    last.metrics.gen_timeouts = int(res.get("timeouts") or 0)
                    last.metrics.gen_err_pct = float(res.get("err_pct") or 0.0)
                except (json.JSONDecodeError, ValueError):
                    pass
                continue

    if last is not None:
        yield last


def backfill_file(path: Path, bins: Optional[dict[str, Bin]] = None,
                  host: Optional[dict] = None) -> list[Record]:
    """Parse one bash-script log into a list of Records (source-tagged backfill)."""
    return list(parse_log(path, bins, host))


def group_by_cell(records: list[Record]) -> "OrderedDict[str, list[Record]]":
    """Group records by cell.key(), preserving first-seen order."""
    out: "OrderedDict[str, list[Record]]" = OrderedDict()
    for r in records:
        out.setdefault(r.cell.key(), []).append(r)
    return out


def cell_means(records: list[Record]) -> "OrderedDict[str, float]":
    """Per-cell mean kcmd_s, keyed by cell.key()."""
    out: "OrderedDict[str, float]" = OrderedDict()
    for key, reps in group_by_cell(records).items():
        vals = [r.metrics.kcmd_s for r in reps]
        out[key] = sum(vals) / len(vals) if vals else 0.0
    return out


def parse_dir(root: Path, bins: Optional[dict[str, Bin]] = None,
              pattern: str = "*.log") -> list[Record]:
    out: list[Record] = []
    for p in sorted(Path(root).glob(pattern)):
        out.extend(parse_log(p, bins))
    return out


def _main(argv: Optional[list[str]] = None) -> int:
    """CLI: parse one or more logs, print per-cell kcmd_s means, one block per log."""
    import argparse

    ap = argparse.ArgumentParser(prog="backfill", description="Parse rig logs into per-cell means.")
    ap.add_argument("logs", nargs="+", help="bash-script log file(s)")
    ap.add_argument("--bins", help="optional bins catalogue json")
    ns = ap.parse_args(argv)
    bins = load_bins(Path(ns.bins)) if ns.bins else {}

    for log in ns.logs:
        recs = backfill_file(Path(log), bins)
        print(f"# {log}: {len(recs)} reps")
        for key, mean in cell_means(recs).items():
            n = len(group_by_cell(recs)[key])
            print(f"  {key:36s} {mean:8.1f} kcmd/s  (n={n})")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
