"""Summaries, Welch A/B compare, and the two offload acceptance checks.

Pure stdlib: Student's t CDF via the regularized incomplete beta function, so
the runner host needs nothing beyond python3.
"""

from __future__ import annotations

import math
import statistics as st
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Optional

from .schema import Record

# ---------------------------------------------------------------- t-dist ----


def _betacf(a: float, b: float, x: float) -> float:
    tiny, eps = 1e-300, 3e-14
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / (c if abs(c) > tiny else tiny)
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / (c if abs(c) > tiny else tiny)
        de = d * c
        h *= de
        if abs(de - 1.0) < eps:
            break
    return h


def betainc(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    ln = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1.0 - x)
    bt = math.exp(ln)
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def t_cdf(t: float, df: float) -> float:
    x = df / (df + t * t)
    p = 0.5 * betainc(df / 2.0, 0.5, x)
    return 1.0 - p if t > 0 else p


def t_ppf(q: float, df: float) -> float:
    lo, hi = 0.0, 1000.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_cdf(mid, df) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


# --------------------------------------------------------------- summary ----


@dataclass
class Sample:
    values: list[float] = field(default_factory=list)
    invalid: int = 0
    main_active: list[float] = field(default_factory=list)
    perf: dict = field(default_factory=lambda: defaultdict(list))

    @property
    def n(self) -> int:
        return len(self.values)

    @property
    def mean(self) -> Optional[float]:
        return st.fmean(self.values) if self.values else None

    @property
    def stdev(self) -> Optional[float]:
        return st.stdev(self.values) if self.n >= 2 else None

    @property
    def ci95(self) -> Optional[float]:
        if self.n < 2:
            return None
        return t_ppf(0.975, self.n - 1) * self.stdev / math.sqrt(self.n)

    @property
    def ci95_pct(self) -> Optional[float]:
        return (self.ci95 / self.mean * 100.0) if self.ci95 is not None and self.mean else None

    def perf_means(self) -> dict:
        return {k: st.fmean(v) for k, v in self.perf.items() if v}

    def to_dict(self) -> dict:
        return {
            "n": self.n, "invalid": self.invalid, "mean": self.mean, "stdev": self.stdev,
            "ci95": self.ci95, "ci95_pct": self.ci95_pct,
            "min": min(self.values) if self.values else None,
            "max": max(self.values) if self.values else None,
            "main_active_mean": st.fmean(self.main_active) if self.main_active else None,
            "values": self.values, "perf": self.perf_means(),
        }


def group(records: Iterable[Record], include_invalid: bool = False) -> dict[tuple[str, str], Sample]:
    """(cell key, bin name) -> Sample of valid reps. Invalid reps are counted, not used."""
    out: dict[tuple[str, str], Sample] = defaultdict(Sample)
    for r in records:
        s = out[(r.cell.key(), r.bin.name)]
        if not r.valid and not include_invalid:
            s.invalid += 1
            continue
        s.values.append(r.metrics.kcmd_s)
        if r.metrics.main_active_pct is not None:
            s.main_active.append(r.metrics.main_active_pct)
        if r.metrics.perf:
            for k, v in r.metrics.perf.__dict__.items():
                if v is not None:
                    s.perf[k].append(v)
    return out


# --------------------------------------------------------------- compare ----


@dataclass
class Comparison:
    cell: str
    a: Sample
    b: Sample
    delta_pct: Optional[float] = None
    delta_ci95_pct: Optional[float] = None
    p_value: Optional[float] = None
    layout_hint: Optional[str] = None   # "layout" | "work" | None

    def to_dict(self) -> dict:
        return {
            "cell": self.cell, "a": self.a.to_dict(), "b": self.b.to_dict(),
            "delta_pct": self.delta_pct, "delta_ci95_pct": self.delta_ci95_pct,
            "p_value": self.p_value, "layout_hint": self.layout_hint,
            "valid": self.a.n >= 1 and self.b.n >= 1,
        }


def welch(a: Sample, b: Sample) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """delta% of B over A, 95% CI half-width in % of A's mean, two-sided p."""
    if a.n < 1 or b.n < 1 or not a.mean:
        return None, None, None
    delta = (b.mean - a.mean) / a.mean * 100.0
    if a.n < 2 or b.n < 2:
        return delta, None, None
    va, vb = a.stdev ** 2 / a.n, b.stdev ** 2 / b.n
    se = math.sqrt(va + vb)
    if se == 0:
        return delta, 0.0, 0.0 if delta else 1.0
    df = (va + vb) ** 2 / (va ** 2 / (a.n - 1) + vb ** 2 / (b.n - 1))
    t = (b.mean - a.mean) / se
    p = 2.0 * (1.0 - t_cdf(abs(t), df))
    ci = t_ppf(0.975, df) * se / a.mean * 100.0
    return delta, ci, p


def layout_hint(a: Sample, b: Sample) -> Optional[str]:
    """Flat instructions/cmd with moving cycles and L1i misses reads as code
    placement, not work (see conductress docs/layout-noise.md)."""
    pa, pb = a.perf_means(), b.perf_means()
    if "ins_per_cmd" not in pa or "ins_per_cmd" not in pb or "cyc_per_cmd" not in pa or "cyc_per_cmd" not in pb:
        return None
    d_ins = (pb["ins_per_cmd"] - pa["ins_per_cmd"]) / pa["ins_per_cmd"] * 100
    d_cyc = (pb["cyc_per_cmd"] - pa["cyc_per_cmd"]) / pa["cyc_per_cmd"] * 100
    l1i_moved = False
    if pa.get("l1i_miss_per_cmd") and pb.get("l1i_miss_per_cmd") is not None:
        l1i_moved = abs(pb["l1i_miss_per_cmd"] - pa["l1i_miss_per_cmd"]) / pa["l1i_miss_per_cmd"] > 0.5
    if abs(d_cyc) < 1.0:
        return None
    if abs(d_ins) < 1.5 and (l1i_moved or abs(d_cyc) > 2.0):
        return "layout"
    return "work"


def compare(records: Iterable[Record], bin_a: str, bin_b: str) -> list[Comparison]:
    g = group(records)
    cells = sorted({c for (c, b) in g if b in (bin_a, bin_b)}, key=_cell_sort_key)
    out = []
    for c in cells:
        a, b = g.get((c, bin_a), Sample()), g.get((c, bin_b), Sample())
        d, ci, p = welch(a, b)
        out.append(Comparison(c, a, b, d, ci, p, layout_hint(a, b)))
    return out


def _cell_sort_key(key: str):
    w, p, c, iot = key.split("-")[:4]
    return ({"get": 0, "set": 1, "mixed": 2}.get(w, 9), int(p[1:]), int(c[1:]), int(iot[3:]), key)


# ------------------------------------------------------------ acceptance ----


def acceptance(records: Iterable[Record], stock: str, cand: str, conns: Optional[int] = None,
               stock_p: int = 16, cand_p: int = 1, io_threads: int = 16) -> list[dict]:
    """Candidate at P1 vs stock at P16, per workload (and per conns if not fixed).
    The hard goal for the offload work: ratio >= 1.0."""
    recs = list(records)
    g = group(recs)
    conn_set = {r.cell.conns for r in recs} if conns is None else {conns}
    out = []
    for w in ("get", "set", "mixed"):
        for c in sorted(conn_set):
            a = g.get((f"{w}-P{stock_p}-c{c}-iot{io_threads}", stock))
            b = g.get((f"{w}-P{cand_p}-c{c}-iot{io_threads}", cand))
            if not a or not b or not a.n or not b.n:
                continue
            ratio = b.mean / a.mean
            ci = None
            if a.ci95_pct is not None and b.ci95_pct is not None:
                ci = ratio * math.sqrt(a.ci95_pct ** 2 + b.ci95_pct ** 2) / 100.0
            out.append({
                "workload": w, "conns": c, "stock_bin": stock, "cand_bin": cand,
                "stock_p16": a.to_dict(), "cand_p1": b.to_dict(),
                "ratio": ratio, "ratio_ci95": ci, "pass": ratio >= 1.0,
            })
    return out


def invariance(records: Iterable[Record], bin_name: str, io_threads: int = 16) -> list[dict]:
    """Throughput of one bin across pipeline depths. The offload design says
    main never sees per-client depth, so a correct build is flat here."""
    g = group(records)
    by: dict[tuple[str, int], dict[int, Sample]] = defaultdict(dict)
    for (key, b), s in g.items():
        if b != bin_name or not s.n:
            continue
        w, p, c, iot = key.split("-")[:4]
        if int(iot[3:]) != io_threads:
            continue
        by[(w, int(c[1:]))][int(p[1:])] = s
    out = []
    for (w, c), ps in sorted(by.items()):
        if len(ps) < 2:
            continue
        means = {p: s.mean for p, s in ps.items()}
        lo, hi = min(means.values()), max(means.values())
        out.append({
            "bin": bin_name, "workload": w, "conns": c,
            "by_pipeline": {str(p): ps[p].to_dict() for p in sorted(ps)},
            "spread_pct": (hi - lo) / lo * 100.0 if lo else None,
            "flat": bool(lo) and (hi - lo) / lo < 0.05,
        })
    return out


# --------------------------------------------------------------- render -----


def fmt_compare(comps: list[Comparison], bin_a: str, bin_b: str) -> str:
    lines = [f"A: {bin_a}   B: {bin_b}",
             f"{'cell':<26} {'mean A':>9} {'±CI%':>6} {'mean B':>9} {'±CI%':>6} {'delta':>8} {'±CI%':>6} {'p':>7} {'n':>5}  note"]
    sig = 0
    for c in comps:
        def f(x, w, fmt):
            return format(x, fmt).rjust(w) if x is not None else "-".rjust(w)
        note = []
        if c.a.invalid or c.b.invalid:
            note.append(f"invalid reps {c.a.invalid}/{c.b.invalid}")
        if c.layout_hint:
            note.append(c.layout_hint)
        if c.p_value is not None and c.p_value < 0.05:
            sig += 1
        lines.append(
            f"{c.cell:<26} {f(c.a.mean, 9, ',.0f')} {f(c.a.ci95_pct, 6, '.1f')} {f(c.b.mean, 9, ',.0f')} "
            f"{f(c.b.ci95_pct, 6, '.1f')} {f(c.delta_pct, 7, '+.2f')}% {f(c.delta_ci95_pct, 6, '.2f')} "
            f"{f(c.p_value, 7, '.4f')} {c.a.n:>2}/{c.b.n:<2}  {' '.join(note)}"
        )
    lines.append(f"significant (p<0.05): {sig}/{len(comps)}   (k cmd/s, valid reps only; main >= 98%)")
    return "\n".join(lines)


def fmt_acceptance(rows: list[dict]) -> str:
    if not rows:
        return "acceptance: no matching stock-P16 / candidate-P1 pairs"
    out = ["acceptance  (candidate P1 vs stock P16, ratio >= 1.00 passes)"]
    for r in rows:
        ci = f" ± {r['ratio_ci95']:.3f}" if r["ratio_ci95"] is not None else ""
        out.append(
            f"  {r['workload']:<6} c{r['conns']:<5} stock P16 {r['stock_p16']['mean']:>8,.0f}  "
            f"cand P1 {r['cand_p1']['mean']:>8,.0f}  ratio {r['ratio']:.3f}{ci}  {'PASS' if r['pass'] else 'FAIL'}"
        )
    return "\n".join(out)


def fmt_invariance(rows: list[dict]) -> str:
    if not rows:
        return "invariance: need the same bin at >= 2 pipeline depths"
    out = ["pipeline invariance  (same bin across P; flat = spread < 5%)"]
    for r in rows:
        cells = "  ".join(f"P{p}={s['mean']:,.0f}" for p, s in r["by_pipeline"].items())
        out.append(f"  {r['bin']:<12} {r['workload']:<6} c{r['conns']:<5} {cells}  spread {r['spread_pct']:.1f}%  "
                   f"{'FLAT' if r['flat'] else 'NOT FLAT'}")
    return "\n".join(out)
