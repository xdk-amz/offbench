"""Replica read capacity (docs/replica-read-capacity.md): calibration and the result table.

At X writes/s and Y reads/s on the primary, how many reads/s does the replica serve while it stays in sync?
repl-run measures one rep at a time. This module holds the two steps around it: finding each build's
primary load for (X, Y) with no replica readers (calibrate), and reducing a sweep of replica read load to
the doc's result table (report). Both run locally; nothing here executes on a host.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, fields, replace
from typing import Callable, Optional

from . import repl

# Calibration accepts a build whose primary delivers X and Y within this; a sweep rep is invalid past
# repl.DRIFT_TOL, which leaves room for rep-to-rep noise.
CAL_TOL = 0.05
# One-rep probes search toward this fraction of the tolerance; the confirmation median must be within it.
AIM = 0.6
CONFIRM_ROUNDS = 3
# Below this many connections one connection is a coarse step (one rr writer was 22% of X at 4 cores),
# so the generator's pipeline is halved for a finer one.
MIN_CONNS = 10
# A step's replica read rate counts as still rising when it beats the previous in-sync step by more than this.
RISING = 0.05
GEN_BUSY = 0.95


# ------------------------------------------------------------- shapes ----

@dataclass(frozen=True)
class Shape:
    """One build's primary load: writer and primary reader connections, each with its pipeline depth."""
    w: int
    wp: int
    p: int
    pp: int

    @property
    def fw(self) -> int:
        return self.w * self.wp

    @property
    def fp(self) -> int:
        return self.p * self.pp

    def label(self) -> str:
        return f"{self.w}xP{self.wp} writers + {self.p}xP{self.pp} primary readers"


def shape_of(rec: dict) -> Shape:
    c = rec["cell"]
    return Shape(c.get("write_conns") or c["conns"], c.get("write_pipeline") or c["pipeline"],
                 c.get("p_conns") or 0, c.get("p_pipeline") or c["pipeline"])


def delivered(rec: dict) -> tuple[float, float]:
    """The primary's writes/s and reads/s in a rep, in k/s."""
    return rec["primary"]["write_kcmd_s"], rec["primary"]["read_kcmd_s"]


def off(w: float, r: float, x: float, y: float) -> float:
    """Worse of the two relative misses against (X, Y)."""
    return max(abs(w / x - 1), abs(r / y - 1))


def _merged(points: list) -> list:
    """One (shape, W, R) per shape: repeated measurements of a shape are averaged."""
    by: dict = {}
    for s, w, r in points:
        by.setdefault(s, []).append((w, r))
    return [(s, statistics.fmean(v[0] for v in vs), statistics.fmean(v[1] for v in vs)) for s, vs in by.items()]


def best(points: list, x: float, y: float) -> Optional[tuple]:
    pts = _merged(points)
    return min(pts, key=lambda t: off(t[1], t[2], x, y)) if pts else None


def next_x(pts: list, target: float) -> float:
    """Next abscissa for an increasing, concave f with f(0) = 0, from measured (x, f(x)) points.

    Inside the tightest bracket around the target, interpolate, but bisect when the interpolated point falls
    outside the bracket's middle half: throughput flattens once a server saturates, and on that plateau
    interpolation alone creeps toward the target. With nothing above the target yet, extrapolate linearly,
    at most doubling."""
    above = [p for p in pts if p[1] >= target]
    if not above:
        x, fx = max(pts)
        return min(2 * x, x * target / fx) if fx > 0 else 2 * x
    xh, fh = min(above)
    xl, fl = max([p for p in pts if p[1] < target and p[0] < xh], default=(0.0, 0.0))
    x = xl + (target - fl) * (xh - xl) / (fh - fl) if fh > fl else (xl + xh) / 2
    q = (xh - xl) / 4
    if not xl + q <= x <= xh - q:
        x = (xl + xh) / 2
    return x


def propose(points: list, x: float, y: float, min_conns: int = MIN_CONNS) -> Optional[Shape]:
    """Next shape to measure for one build, or None when no untried shape is left near the target.

    Requests in flight, not connections, set a closed-loop primary's rates. The total in flight is searched
    against X + Y, and the split between writers and readers comes from the closest point so far:
    writes/reads there is k x (writes in flight / reads in flight), and the split is set so k x split = X/Y.
    """
    pts = _merged(points)
    s, w, r = min(pts, key=lambda t: off(t[1], t[2], x, y))
    tried = {t[0] for t in pts}
    if r > 0 and w > 0:
        k = (w / r) / (s.fw / s.fp)
        q = (x / y) / k
        n = next_x([(t[0].fw + t[0].fp, t[1] + t[2]) for t in pts], x + y)
        fw, fp = n * q / (1 + q), n / (1 + q)
        wp, pp = s.wp, s.pp
        while wp > 1 and fw / wp < min_conns:
            wp //= 2
        while pp > 1 and fp / pp < min_conns:
            pp //= 2
        cand = Shape(max(1, round(fw / wp)), wp, max(1, round(fp / pp)), pp)
        if cand not in tried:
            return cand
    # Rounded onto a shape already measured: from the closest point, step the count whose rate is further
    # off by one connection toward its target.
    dw, dr = w / x - 1, r / y - 1
    for _, which, d in sorted([(abs(dw), "w", -1 if dw > 0 else 1), (abs(dr), "p", -1 if dr > 0 else 1)],
                              reverse=True):
        c = replace(s, w=s.w + d) if which == "w" else replace(s, p=s.p + d)
        if c.w >= 1 and c.p >= 1 and c not in tried:
            return c
    return None


# Measure callback: {build: shape} and reps -> {build: [records]}, every build at its shape with no replica
# readers, builds interleaved per rep.
Measure = Callable[[dict, int, str], dict]


def reference(measure: Measure, builds: list[str], shape: Shape, reps: int) -> dict:
    """Each build's median writes/s and reads/s at one shared load; X and Y are the lowest of each."""
    recs = measure({b: shape for b in builds}, reps, "reference")
    per = {}
    for b in builds:
        v = [r for r in recs.get(b, []) if r.get("valid")]
        if len(v) * 2 <= reps:
            raise repl.CellError(f"{b}: no majority of valid reference reps")
        per[b] = {"write_kcmd_s": statistics.median(delivered(r)[0] for r in v),
                  "read_kcmd_s": statistics.median(delivered(r)[1] for r in v), "reps": len(v)}
    return {"shape": asdict(shape), "reps": reps, "builds": per,
            "targets": {"write_kcmd_s": min(p["write_kcmd_s"] for p in per.values()),
                        "read_kcmd_s": min(p["read_kcmd_s"] for p in per.values())}}


def calibrate(measure: Measure, builds: list[str], x: float, y: float, start: dict, *, tol: float = CAL_TOL,
              max_probes: int = 12, confirm_reps: int = 3, log: Callable[[str], None] = print,
              seed: Optional[dict] = None) -> dict:
    """Per build, the shape whose primary delivers X and Y within tol, probed one rep at a time and then
    confirmed with confirm_reps interleaved reps. seed: earlier points per build (e.g. the reference reps).

    One-rep probes are noisy (~1%), so the search aims inside AIM x tol and only the confirmation's median
    has to land within tol."""
    aim = AIM * tol
    points = {b: list((seed or {}).get(b, [])) for b in builds}
    probes = {b: 0 for b in builds}
    shape = {b: start[b] for b in builds}
    chosen: dict = {}
    retried: set = set()

    def search(todo: list) -> None:
        cur = {}
        for b in todo:
            bp = best(points[b], x, y)
            if bp and off(bp[1], bp[2], x, y) <= aim:
                chosen[b] = bp[0]
            else:
                cur[b] = propose(points[b], x, y) if points[b] else shape[b]
        while cur:
            cur = {b: s for b, s in cur.items() if s is not None and probes[b] < max_probes}
            if not cur:
                break
            recs = measure(cur, 1, "probe")
            nxt = {}
            for b, s in cur.items():
                probes[b] += 1
                v = [r for r in recs.get(b, []) if r.get("valid")]
                for r in v:
                    points[b].append((s, *delivered(r)))
                if v:
                    log(f"{b}: {s.label()} -> {v[0]['primary']['write_kcmd_s']:.0f}k writes, "
                        f"{v[0]['primary']['read_kcmd_s']:.0f}k reads")
                else:
                    why = [r.get("error") or "; ".join(r.get("invalid") or []) for r in recs.get(b, [])]
                    log(f"{b}: {s.label()} -> invalid: {why}")
                bp = best(points[b], x, y)
                if bp and off(bp[1], bp[2], x, y) <= aim:
                    chosen[b] = bp[0]
                elif not v and (b, s) not in retried:
                    retried.add((b, s))       # an invalid probe is measured again, once
                    nxt[b] = s
                else:
                    nxt[b] = propose(points[b], x, y) if points[b] else None
            cur = nxt
        for b in todo:
            if b not in chosen:
                bp = best(points[b], x, y)
                chosen[b] = bp[0] if bp else None

    todo = list(builds)
    result: dict = {}
    for rnd in range(CONFIRM_ROUNDS):
        search(todo)
        missing = [b for b in todo if chosen.get(b) is None]
        if missing:
            raise repl.CellError(f"no valid calibration probe for {', '.join(missing)}")
        recs = measure({b: chosen[b] for b in todo}, confirm_reps, "confirm")
        retry = []
        for b in todo:
            v = [r for r in recs.get(b, []) if r.get("valid")]
            for r in v:
                points[b].append((chosen[b], *delivered(r)))
            ok = len(v) * 2 > confirm_reps
            w = statistics.median(delivered(r)[0] for r in v) if v else float("nan")
            r_ = statistics.median(delivered(r)[1] for r in v) if v else float("nan")
            e = off(w, r_, x, y) if v else float("inf")
            result[b] = {"shape": asdict(chosen[b]), "write_kcmd_s": w, "read_kcmd_s": r_, "valid_reps": len(v),
                         "reps": confirm_reps, "off": e, "probes": probes[b], "converged": ok and e <= tol}
            log(f"{b}: confirm {chosen[b].label()}: {w:.0f}k writes {w / x - 1:+.1%}, {r_:.0f}k reads "
                f"{r_ / y - 1:+.1%} ({len(v)}/{confirm_reps} valid)")
            if not result[b]["converged"]:
                retry.append(b)
                del chosen[b]
        if not retry:
            break
        todo = retry
    return {"targets": {"write_kcmd_s": x, "read_kcmd_s": y}, "tol": tol, "builds": result,
            "converged": all(v["converged"] for v in result.values()),
            "points": {b: [[*asdict(s).values(), w, r] for s, w, r in points[b]] for b in builds}}


# ------------------------------------------------------------- report ----

def fmt_rate(k: float) -> str:
    """k/s as 782k or 6.82M."""
    return f"{k / 1000:.2f}M" if k >= 1000 else f"{k:.0f}k"


def _med(xs) -> float:
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else float("nan")


def cores(rec: dict) -> int:
    c = rec["cell"]
    return c["io_threads"] - (c.get("pin_share") or 0)


def lagging_only(rec: dict) -> bool:
    return bool(rec.get("invalid")) and all(w.startswith(repl.LAGGING) for w in rec["invalid"])


def reader_threads(rec: dict) -> int:
    g = rec.get("reader") or {}
    if g.get("threads"):
        return g["threads"]
    names = {f.name for f in fields(repl.ReplCell)}
    cell = repl.ReplCell(**{k: v for k, v in rec["cell"].items() if k in names})
    return cell.threads("reader", "rgen" in (rec.get("hosts") or {}))


def rtt_flag(valid: list) -> bool:
    """The replica readers' mean request latency (connections x pipeline / read rate) is under twice the
    minimum ping from their generator to the replica: the round trip is over half of each request."""
    ratios = []
    for r in valid:
        rtt = ((r.get("rtt_ms") or {}).get("gen_replica") or {}).get("min_ms")
        rate = r["replica"]["read_kcmd_s"] * 1000.0
        if rtt and rate:
            ratios.append(r["cell"]["conns"] * r["cell"]["pipeline"] / rate * 1e6 / (rtt * 1000.0))
    return bool(ratios) and statistics.median(ratios) < 2


def gen_flag(valid: list) -> bool:
    """The replica readers' generator used 95% or more of its threads (one per core): a lower bound."""
    used = [(r.get("reader") or {}).get("cores") / reader_threads(r) for r in valid
            if (r.get("reader") or {}).get("cores") is not None]
    return bool(used) and statistics.median(used) >= GEN_BUSY


@dataclass
class Step:
    readers: int
    recs: list

    @property
    def valid(self) -> list:
        return [r for r in self.recs if r.get("valid")]

    @property
    def behind(self) -> int:
        return sum(lagging_only(r) for r in self.recs)

    @property
    def ok(self) -> bool:
        return len(self.valid) * 2 > len(self.recs)

    @property
    def read(self) -> float:
        return _med(r["replica"]["read_kcmd_s"] for r in self.valid)

    def note(self) -> str:
        n = len(self.recs)
        if not self.ok:
            if self.behind:
                return f"invalid, fell behind {self.behind}/{n}"
            other = next((r.get("error") or r["invalid"][0] for r in self.recs if not r.get("valid")), "")
            return f"invalid: {other}"
        tags = []
        if self.readers and rtt_flag(self.valid):
            tags.append("RTT")
        if self.readers and gen_flag(self.valid):
            tags.append("GEN")
        if self.behind:
            tags.append(f"{self.behind} rep{'s' if self.behind > 1 else ''} fell behind")
        others = n - len(self.valid) - self.behind
        if others:
            tags.append(f"{others} other invalid")
        return ", ".join(tags)


def outcome(steps: list) -> tuple[str, str]:
    """(replica reads/s in sync, what more offered reads did) for one build's sweep."""
    load = [s for s in steps if s.readers]
    ins = [s for s in load if s.ok]
    oos = [s for s in load if not s.ok and s.behind]
    if not ins:
        first = min(oos, key=lambda s: s.readers) if oos else None
        return "none", (f"out of sync from {first.readers} readers" if first else "no valid step")
    top = max(ins, key=lambda s: s.read)
    i = ins.index(top)
    rising = top is ins[-1] and (i == 0 or top.read > (1 + RISING) * ins[i - 1].read)
    rate = ("at least " if rising else "") + fmt_rate(top.read)
    at = f"{rate} at {top.readers} readers"
    if gen_flag(top.valid):
        return f"at least {fmt_rate(top.read)} at {top.readers} readers", \
            "not reached: the replica readers' generator ran out of cores"
    if not oos:
        return at, ("in sync at every step; reads still rising, extend the sweep" if rising
                    else "in sync at every step; reads level off")
    first = min(oos, key=lambda s: s.readers)
    again = [s.readers for s in ins if s.readers > first.readers]
    if again:
        where = ", ".join(f"{s.readers} ({s.behind}/{len(s.recs)} fell behind)" for s in oos)
        return at, f"unstable: out of sync at {where}, in sync at {', '.join(map(str, again))}"
    last = max(s.readers for s in ins)
    return at, f"falls out of sync between {last} and {first.readers} readers" + (
        "" if rising else "; reads had levelled off")


def _sweep_of(recs: list) -> tuple[list, int]:
    """The records of one build's sweep: the primary shape with the most reader steps; and how many
    records of other shapes (reference or calibration probes in the same file) were left out."""
    by: dict = {}
    for r in recs:
        by.setdefault((shape_of(r), r["cell"]["pipeline"]), []).append(r)
    pick = max(by.values(), key=lambda rs: (len({r["cell"]["conns"] for r in rs}), len(rs)))
    return pick, len(recs) - len(pick)


def report(recs: list, targets: Optional[tuple] = None, order: Optional[list] = None) -> str:
    """The doc's result table and per-step raw table, per calibration and core count in recs."""
    recs = [dict(r) for r in recs if r.get("kind") == "replrw"]
    if targets:
        for r in recs:
            if not r.get("targets"):
                r["targets"] = {"write_kcmd_s": targets[0], "read_kcmd_s": targets[1]}
    for r in recs:
        repl.revalidate(r)
    order = list(order or dict.fromkeys(r["bin"]["name"] for r in recs))
    groups: dict = {}
    for r in recs:
        groups.setdefault(((r.get("calibration") or {}).get("id"), cores(r)), []).append(r)
    out = []
    for (cal, c), rs in sorted(groups.items(), key=lambda kv: (kv[0][1], str(kv[0][0]))):
        t = next((r["targets"] for r in rs if r.get("targets")), None)
        out.append(f"## {c} cores" + (f", calibration {cal}" if cal else ""))
        out.append(f"targets: X {fmt_rate(t['write_kcmd_s'])} writes/s, Y {fmt_rate(t['read_kcmd_s'])} reads/s, "
                   f"reps more than {t.get('tol', repl.DRIFT_TOL):.0%} off are invalid" if t
                   else "no targets recorded: X and Y are as delivered, and drift is not checked")
        names = [b for b in order if any(r["bin"]["name"] == b for r in rs)]
        res = ["| build | X writes/s | Y reads/s | replica reads/s in sync | more offered reads |",
               "|---|---:|---:|---|---|"]
        raw = ["| build | readers | valid | writes/s | primary reads/s | replica reads/s | reader p99 | note |",
               "|---|---:|---:|---:|---:|---:|---:|---|"]
        left = 0
        for b in names:
            mine, skipped = _sweep_of([r for r in rs if r["bin"]["name"] == b])
            left += skipped
            steps = [Step(n, [r for r in mine if r["cell"]["conns"] == n])
                     for n in sorted({r["cell"]["conns"] for r in mine})]
            v = [r for s in steps for r in s.valid]
            x, y = _med(delivered(r)[0] for r in v), _med(delivered(r)[1] for r in v)
            rate, more = outcome(steps)
            res.append(f"| {b} | {fmt_rate(x)} | {fmt_rate(y)} | {rate} | {more} |")
            sh = shape_of(mine[0])
            label = f"{b} {sh.w}/{sh.p}" + (f" (P{sh.wp}/P{sh.pp})" if (sh.wp, sh.pp) != (16, 16) else "")
            for s in steps:
                if s.ok:
                    sv = s.valid
                    p99 = _med(((r.get("reader") or {}).get("latency") or {}).get("p99_us") for r in sv)
                    raw.append(f"| {label} | {s.readers} | {len(sv)}/{len(s.recs)} | "
                               f"{fmt_rate(_med(delivered(r)[0] for r in sv))} | {fmt_rate(_med(delivered(r)[1] for r in sv))} | "
                               + (f"{fmt_rate(s.read)} | {p99:.0f} us" if s.readers else " | ") + f" | {s.note()} |")
                else:
                    raw.append(f"| {label} | {s.readers} | {len(s.valid)}/{len(s.recs)} | | | | | {s.note()} |")
                label = b
        out += ["", *res, "", *raw]
        if left:
            out.append(f"\n{left} records with other primary loads (reference or calibration probes) left out")
        out.append("")
    return "\n".join(out)
