"""Turn records.jsonl into the JSON the compare page reads."""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from . import analysis
from .schema import MAIN_SATURATION_PCT, Record

LAT_FIELDS = ("p50_us", "p90_us", "p99_us", "p999_us", "p9999_us", "max_us")


def session_id(r: Record) -> str:
    if r.source.get("kind") == "backfill":
        return Path(r.source["file"]).stem
    return r.task_id or "runner"


def build(records: Iterable[Record], queue_root: Optional[Path] = None, bins_registry: Optional[dict] = None,
          builds_registry: Optional[dict] = None) -> dict:
    recs = list(records)
    return {
        "metadata": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "records": len(recs),
            "valid": sum(1 for r in recs if r.valid),
            "sessions": len({session_id(r) for r in recs}),
            "saturation_pct": MAIN_SATURATION_PCT,
        },
        "compare": build_compare(recs, builds_registry or {"builds": []}),
    }


def build_compare(records: Iterable[Record], builds_registry: dict) -> dict:
    """Per (build, acl, cell) summary for the cross-branch compare page.

    Every rep is included in the mean (the page shows main busy % next to it and dims
    unsaturated cells); n_valid says how many passed the gate. For each build the first
    listed source that has reps for a cell wins, so cells are never pooled across sessions.
    Latency percentiles are the median across reps, max is the max; range carries the
    lowest and highest rep for every field so the page can show the spread.
    """
    recs = list(records)
    by_src: dict[tuple[str, str], list[Record]] = defaultdict(list)
    for r in recs:
        by_src[(session_id(r), r.bin.name)].append(r)

    builds = builds_registry.get("builds", [])
    cells = []
    dims = {"workload": set(), "pipeline": set(), "conns": set(), "io_threads": set(), "acl": set(), "rate": set(), "pin": set()}
    for b in builds:
        taken: set[tuple] = set()
        for src in b["sources"]:
            per_cell: dict[tuple, list[Record]] = defaultdict(list)
            for r in by_src.get((src["session"], src["bin"]), []):
                c = r.cell
                per_cell[(c.acl, c.workload, c.pipeline, c.conns, c.io_threads, c.rate, c.pin)].append(r)
            for key, rs in per_cell.items():
                if key in taken:
                    continue
                taken.add(key)
                # a rep where the generator never came up (no clients, ~0 cmd/s) is a harness failure,
                # not a measurement: count it, never average it
                failed = [r for r in rs if r.metrics.kcmd_s < 1.0
                          or (r.metrics.clients is not None and r.metrics.clients < r.cell.conns)]
                rs = [r for r in rs if r not in failed]
                if not rs:
                    continue
                vals = [r.metrics.kcmd_s for r in rs]
                mains = [r.metrics.main_active_pct for r in rs if r.metrics.main_active_pct is not None]
                s = analysis.Sample(values=vals)
                acl, w, p, conns, iot, rate, pin = key
                dims["acl"].add(acl); dims["workload"].add(w); dims["pipeline"].add(p)
                dims["conns"].add(conns); dims["io_threads"].add(iot); dims["rate"].add(rate); dims["pin"].add(pin)
                lats = [r.metrics.latency for r in rs if r.metrics.latency and r.metrics.latency.samples]
                latency = None
                if lats:
                    col = lambda k: [getattr(l, k) for l in lats]
                    latency = {k: statistics.median(col(k)) for k in LAT_FIELDS if k != "max_us"}
                    latency["max_us"] = max(col("max_us"))
                    latency["range"] = {k: [min(col(k)), max(col(k))] for k in LAT_FIELDS}
                    latency["n"] = len(lats)
                    latency["p99_values"] = col("p99_us")
                cells.append({
                    "build": b["id"], "acl": acl, "workload": w, "pipeline": p, "conns": conns, "io_threads": iot, "rate": rate, "pin": pin,
                    "latency": latency,
                    "session": src["session"], "bin": src["bin"],
                    "n": len(rs), "n_valid": sum(1 for r in rs if r.valid), "n_failed": len(failed),
                    "mean": s.mean, "ci95_pct": s.ci95_pct, "min": min(vals), "max": max(vals),
                    "main_mean": (sum(mains) / len(mains)) if mains else None,
                    "values": vals, "valid": [r.valid for r in rs],
                })
    wl_order = {"get": 0, "set": 1, "mixed": 2}
    return {
        "builds": [{k: b.get(k) for k in ("id", "label", "commit", "branch", "ref")} for b in builds],
        "dims": {k: sorted(v, key=(lambda x: wl_order.get(x, 9)) if k == "workload" else None) for k, v in dims.items()},
        "cells": cells,
    }


def write(data: dict, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1, default=str))
    tmp.replace(out)
    return out
