"""The replica read capacity page's data: runs of docs/replica-read-capacity.md, judged as repl-capacity
judges them.

A registry (repl_view.json) lists the builds, by commit, and the run files to show. Every build in a run
must match exactly one registry entry, so nothing unlabelled is exported. The output carries rates,
latencies and connection counts only: no host names or addresses.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import repl, replcap

LAT = ("p50_us", "p90_us", "p99_us", "p999_us", "p9999_us", "max_us")
BUILD_KEYS = ("id", "label", "commit", "branch", "note")


class ViewError(ValueError):
    pass


def _num(v) -> Optional[float]:
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else v


def _med(xs) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def match(bin_: dict, builds: list) -> dict:
    """The registry entry a run's build is: the same commit (an entry may give a prefix) and, when the
    entry names them, the same replica and primary arguments and the same share (main on an IO thread's
    core)."""
    commit = bin_.get("commit") or ""
    hits = [b for b in builds if commit and commit.startswith(b["commit"])
            and ("replica_args" not in b or (bin_.get("replica_args") or "") == b["replica_args"])
            and ("primary_args" not in b or (bin_.get("primary_args") or "") == b["primary_args"])
            and ("share" not in b or int(bin_.get("share") or 0) == int(b["share"]))]
    if len(hits) != 1:
        raise ViewError(f"build {bin_.get('name')} ({commit[:9] or 'no commit'}) matches {len(hits)} "
                        f"registry entries, needs exactly 1")
    return hits[0]


def _lat(recs: list, gen: str) -> Optional[dict]:
    """One load generator's request latency over recs, as the throughput page aggregates it: percentiles are
    the median across reps, max_us the max across them, range each field's lowest and highest rep, and n the
    reps that recorded latency."""
    lats = [l for l in (((r.get(gen) or {}).get("latency")) for r in recs) if l]
    if not lats:
        return None

    def col(k):
        return [l[k] for l in lats if l.get(k) is not None]
    out = {k: _med(col(k)) for k in LAT if k != "max_us"}
    out["max_us"] = max(col("max_us"), default=None)
    out["range"] = {k: [min(c), max(c)] for k in LAT if (c := col(k))}
    out["n"] = len(lats)
    return out


def _step(s: replcap.Step, role: str) -> dict:
    """One load step. Only an in-sync step carries numbers. lat is the replica readers' latency."""
    out = {"readers": s.readers, "n": len(s.recs), "valid": len(s.valid), "behind": s.behind, "role": role,
           "note": s.note()}
    if not s.ok:
        return out
    v = s.valid
    out["x"] = _med(replcap.delivered(r)[0] for r in v)
    out["y"] = _med(replcap.delivered(r)[1] for r in v)
    out["writer_lat"] = _lat(v, "writer")
    out["primary_reader_lat"] = _lat(v, "preader")
    if s.readers:
        out["read"] = s.read
        out["lat"] = _lat(v, "reader")
    out["primary_main"] = _med((r.get("primary") or {}).get("main_active_pct") for r in v)
    out["replica_main"] = _med((r.get("replica") or {}).get("main_active_pct") for r in v)
    out["replica_io_cores"] = _med((r.get("replica") or {}).get("io_cores") for r in v)
    return out


def _sweep(sw: replcap.Sweep, builds: list) -> dict:
    o = replcap.judge(sw.steps)
    rate, more = replcap.outcome(sw.steps)
    again = {s.readers for s in o.again}

    def role(s: replcap.Step) -> str:
        if not s.readers:
            return "base"
        if s.behind:
            return "behind"
        if not s.ok:
            return "invalid"
        return "again" if s.readers in again else "curve"

    x, y = sw.delivered
    return {
        "build": match(sw.bin, builds)["id"],
        "shape": asdict(sw.shape), "pipeline": sw.pipeline,
        "x": _num(x), "y": _num(y),
        "capacity": {"read": o.top.read, "readers": o.top.readers, "at_least": o.at_least} if o.top else None,
        "outcome": {"kind": o.kind, "rate": rate, "more": more,
                    "last": o.last.readers if o.last else None,
                    "first_behind": o.first.readers if o.first else None},
        "steps": [_step(s, role(s)) for s in sw.steps],
    }


def _run(run: replcap.Run, builds: list) -> dict:
    ts = sorted(r["ts"] for r in run.recs if r.get("ts"))
    rtt = {k: _med(((r.get("rtt_ms") or {}).get(k) or {}).get("median_ms") for r in run.recs)
           for k in ("gen_primary", "gen_replica", "gen_replica2", "primary_replica")}
    t = run.targets
    order = {b["id"]: i for i, b in enumerate(builds)}
    pins = {bool((r.get("cell") or {}).get("pin")) for r in run.recs}
    if len(pins) > 1:
        raise ViewError(f"{run.cores}-core run {run.calibration or 'without calibration'} mixes pinned and "
                        f"unpinned reps")
    return {
        "id": f"{run.cores}c-{run.calibration or (ts[0] if ts else 'undated')}",
        "cores": run.cores, "calibration": run.calibration, "pinned": pins == {True},
        "first_ts": ts[0] if ts else None, "last_ts": ts[-1] if ts else None,
        "targets": {"x": t["write_kcmd_s"], "y": t["read_kcmd_s"], "tol": t.get("tol", repl.DRIFT_TOL)} if t else None,
        "rtt_ms": {k: v for k, v in rtt.items() if v is not None},
        "sweeps": sorted((_sweep(sw, builds) for sw in run.sweeps), key=lambda s: order[s["build"]]),
    }


def _runs_of(entry: dict, root: Path, builds: list) -> list:
    recs = repl.load(root / entry["file"], entry.get("run_id"))
    if "keep" in entry:
        recs = [r for r in recs if (r.get("bin") or {}).get("name") in entry["keep"]]
    t = entry.get("targets")
    return [_run(run, builds) for run in replcap.runs(recs, tuple(t) if t else None)]


def _join(run: dict, add: dict, root: Path, builds: list) -> dict:
    """run with the sweeps of one run measured separately at the same core count and targets. Each joined
    sweep carries measured: its own dates, calibration and round trips."""
    got = _runs_of(add, root, builds)
    if len(got) != 1:
        raise ViewError(f"{add['file']} holds {len(got)} runs, needs exactly 1 to join a run")
    other = got[0]
    a, b = run["targets"], other["targets"]
    if other["cores"] != run["cores"]:
        raise ViewError(f"{add['file']} ran at {other['cores']} cores, the run it joins at {run['cores']}")
    if not (a and b and all(abs(b[k] / a[k] - 1) < 1e-6 for k in ("x", "y")) and a["tol"] == b["tol"]):
        raise ViewError(f"{add['file']} targets {b} differ from the run it joins ({a})")
    if other["pinned"] != run["pinned"]:
        raise ViewError(f"{add['file']} pinned={other['pinned']}, the run it joins pinned={run['pinned']}")
    have = {sw["build"] for sw in run["sweeps"]}
    for sw in other["sweeps"]:
        if sw["build"] in have:
            raise ViewError(f"{add['file']}: {sw['build']} is already in the run it joins")
        sw["measured"] = {k: other[k] for k in ("calibration", "first_ts", "last_ts", "rtt_ms")}
    order = {b["id"]: i for i, b in enumerate(builds)}
    run["sweeps"] = sorted(run["sweeps"] + other["sweeps"], key=lambda s: order[s["build"]])
    return run


def build(registry: dict, root: Path) -> dict:
    """The page's JSON for the runs the registry lists. A run entry: file (under root), optional run_id
    to pick one run out of the file, optional targets [X, Y] for records that carry none, optional keep
    (the build names to show from the file), and optional add: entries of the same form, each one run
    measured separately at the same core count and targets, whose sweeps join this run."""
    builds = registry.get("builds") or []
    runs = []
    for entry in registry.get("runs") or []:
        found = _runs_of(entry, root, builds)
        for add in entry.get("add") or []:
            if len(found) != 1:
                raise ViewError(f"{entry['file']} holds {len(found)} runs; add needs exactly 1")
            found[0] = _join(found[0], add, root, builds)
        runs += found
    used = {sw["build"] for r in runs for sw in r["sweeps"]}
    return {
        "metadata": {"generated": datetime.now(timezone.utc).isoformat(), "runs": len(runs),
                     "drift_tol": repl.DRIFT_TOL, "rising": replcap.RISING},
        "ref": registry.get("ref"),
        "cores": sorted(set(registry.get("cores") or []) | {r["cores"] for r in runs}),
        "builds": [{k: b[k] for k in BUILD_KEYS if k in b} for b in builds if b["id"] in used],
        "runs": sorted(runs, key=lambda r: (r["cores"], r["first_ts"] or "")),
    }
