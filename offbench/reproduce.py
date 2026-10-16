"""REPRODUCE.md for an archived run: what someone with only this archive needs to run the test again on new hosts.

Generated from the manifest when the run is sealed. Every fact in it comes from the manifest; what the manifest
does not know is said to be unknown, never filled in.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path

ROLE_USE = {"primary": "primary server", "replica": "replica server",
            "gen": "writers and primary readers (and the replica readers when no rgen host is given)",
            "rgen": "replica readers"}
# What the harness needs on a host (the run's preflight checks some; the rest it calls).
NEEDED = ("binutils", "ethtool", "gcc", "git", "make", "python3", "rsync", "tar", "gzip", "tmux", "iputils",
          "procps-ng", "util-linux", "chrony")
# Flags whose values name files on the original machine, and what to use in a fresh checkout instead.
PATH_FLAGS = ("--out", "--calibration", "--hosts-file", "--key", "--known-hosts", "--archive")


def _rel(archive, run_dir: Path, sha: str) -> str:
    return '"$RUN"/' + os.path.relpath(archive.object_path(sha), run_dir)


def _use(role: str) -> str:
    return ROLE_USE.get(role) or ("replica readers (an even share of each step's connections)"
                                  if role.startswith("rgen") else role)


def _facts(m: dict, role: str) -> dict:
    return (((m.get("rig") or {}).get("roles") or {}).get(role) or {}).get("facts") or {}


def _table(head: list, rows: list) -> list:
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(str(c) if c not in (None, "") else "?" for c in r) + " |" for r in rows]
    return out


def rewrite_argv(argv: list, command: str, label: str) -> list:
    """The original arguments with this checkout's paths: --out and --calibration under data/repl, hosts from
    OFFBENCH_HOSTS_FILE, key and known hosts from the environment, the archive at its default."""
    new = {"--out": f"data/repl/{label}-repro.jsonl",
           "--calibration": f"data/repl/{label}-calib.json" if command == "repl-run" else None}
    out, i = [], 0
    while i < len(argv):
        a = argv[i]
        flag, eq, val = a.partition("=")
        if flag in PATH_FLAGS:
            takes_next = not eq and i + 1 < len(argv)
            if new.get(flag):
                out += [flag, new[flag]]
            i += 2 if takes_next else 1
            continue
        out.append(a)
        i += 1
    return out


def _hosts(m: dict) -> list:
    L = []
    roles = (m.get("rig") or {}).get("roles") or {}
    if not roles:
        return ["The hosts were not recorded."]
    rows = []
    for role, r in roles.items():
        f = r.get("facts") or {}
        i, o = f.get("instance") or {}, f.get("os") or {}
        rows.append([role, f"`{r.get('host')}`", _use(role), i.get("instance_type"), i.get("ami_id"),
                     o.get("release") or o.get("pretty"), f.get("kernel")])
    L.append(f"Launch {len(roles)} hosts like the original ones, one per role:")
    L.append("")
    L += _table(["role", "original host", "used for", "instance type", "AMI", "OS release", "kernel"], rows)
    L.append("")
    f0 = next((r.get("facts") for r in roles.values() if r.get("facts")), None) or {}
    i0 = f0.get("instance") or {}
    rtt = (m.get("rig") or {}).get("rtt_ms") or {}
    gate = ((m.get("command") or {}).get("args") or {}).get("max_rtt_ms")
    azs = sorted({((r.get("facts") or {}).get("instance") or {}).get("az") or "?" for r in roles.values()})
    pgs = sorted({((r.get("facts") or {}).get("instance") or {}).get("placement_group") or "none" for r in roles.values()})
    L.append(f"- One availability zone and one cluster placement group (originally {', '.join(azs)}; placement group "
             f"{', '.join(pgs)}). The run refuses a generator-to-server round trip over "
             f"{gate if gate is not None else '0.15'} ms."
             + (" Originally, median ms: " + ", ".join(f"{k} {(v or {}).get('median_ms', float('nan')):.3f}"
                                                        for k, v in rtt.items()) + "." if rtt else ""))
    cpu = f0.get("cpu") or {}
    if cpu:
        L.append(f"- CPUs: {cpu.get('Model name') or cpu.get('Model') or '?'}, {cpu.get('count')} of them, "
                 f"{(f0.get('mem_kb') or 0) / 1048576:.0f} GiB. The harness's CPU layout assumes this count: cpu 0 "
                 "housekeeping, cpu 1 main, IO threads from cpu 2, IRQs on 17-32, generators on 33-95.")
    users = sorted({((r.get("facts") or {}).get("host") or {}).get("user") or "?" for r in roles.values()})
    L.append(f"- Log in as `{', '.join(users)}`: the harness works under that user's home (`~/offbench`, "
             "`~/offbench-src`, `~/base`).")
    clocks = [f"{role} {((r.get('facts') or {}).get('clock') or {}).get('System time', '?')}" for role, r in roles.items()]
    L.append("- Clocks synced by chrony: every host takes its measurement snapshot at the same absolute time, so "
             f"offsets must stay far below a millisecond. Originally: {'; '.join(clocks)}.")
    if i0.get("instance_type") is None:
        L.append("- The instance type and AMI were not recorded.")
    return L


def _packages(m: dict) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    per = {role: (r.get("facts") or {}).get("packages") or {} for role, r in roles.items()}
    if not any(per.values()):
        return ["Package versions were not recorded. The harness needs: " + ", ".join(NEEDED) + "."]
    have = sorted({p for pk in per.values() for p, v in pk.items() if v and p in NEEDED + ("perf",)})
    L = ["On every host:", "", "    sudo dnf install -y " + " ".join(p for p in have if p != "perf"), "",
         "Versions the original hosts had (`rpm -q`); the AMI's release pins the repository, so the same AMI gives "
         "the same versions:", ""]
    rows = []
    for p in sorted({p for pk in per.values() for p in pk}):
        vals = {role: ", ".join(pk.get(p) or ["not installed"]) for role, pk in per.items() if pk}
        same = len(set(vals.values())) == 1
        rows.append([p, next(iter(vals.values())) if same else "; ".join(f"{r}: {v}" for r, v in vals.items())])
    L += _table(["package", "version"], rows)
    tools = {role: (r.get("facts") or {}).get("tools") or {} for role, r in roles.items()}
    names = sorted({t for tl in tools.values() for t, v in tl.items() if v})
    if names:
        L += ["", "Tool versions they reported:", ""]
        rows = []
        for t in names:
            vals = {role: tl.get(t) or "missing" for role, tl in tools.items() if tl}
            rows.append([t, next(iter(vals.values())) if len(set(vals.values())) == 1
                         else "; ".join(f"{r}: {v}" for r, v in vals.items())])
        L += _table(["tool", "version"], rows)
    return L


def _harness(m: dict, archive, run_dir: Path) -> list:
    h = m.get("harness") or {}
    o = m.get("orchestrator") or {}
    L = [f"Python 3.9 or newer, ssh and rsync (originally Python {o.get('python', '?')} on {o.get('platform', '?')})."
         if o else "Python 3.9 or newer, ssh and rsync.", ""]
    snap = (h.get("snapshot") or {}).get("object")
    if snap and archive.has(snap):
        L += ["In any writable directory:", "", "    mkdir offbench",
              f"    gzip -dc {_rel(archive, run_dir, snap)} | tar -x -C offbench", "    cd offbench", ""]
        what = f"offbench commit `{h.get('commit')}`" if h.get("commit") else "an offbench tree of unknown commit"
        L.append(f"That is {what}" + (f" (branch {h['branch']})" if h.get("branch") else "")
                 + (f" of {h['remote']}" if h.get("remote") else "")
                 + (", with uncommitted changes" if h.get("dirty") else "")
                 + ", exactly as it ran: `HARNESS.json` in it lists every file's sha256. Run every command below "
                   "from that directory.")
        if h.get("remote") and h.get("commit"):
            L += ["", f"The same with git: `git clone {h['remote']} offbench && git -C offbench checkout {h['commit']}`"
                  + (f", then `gzip -dc {_rel(archive, run_dir, h['diff_object'])} | git -C offbench apply` for the "
                     "uncommitted changes" if h.get("diff_object") else "")
                  + (". Untracked files it ran with: " + ", ".join(f"`{u}`" for u in h["untracked"]) + "."
                     if h.get("untracked") else ".")]
    elif h.get("commit"):
        L.append(f"No snapshot of the harness was archived. It was offbench commit `{h['commit']}`"
                 + (f" (evidence: {h['evidence']})" if h.get("evidence") else "") + ".")
    else:
        L.append("The harness that ran is unknown.")
    return L


def _hosts_file(m: dict) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    users = {role: (((r.get("facts") or {}).get("host") or {}).get("user") or r.get("ssh_user") or "ec2-user")
             for role, r in roles.items()}
    entries = ",\n".join(f'  {{"name": "{r["host"]}", "ssh": "{users[role]}@<address of the new {role} host>"}}'
                         for role, r in roles.items())
    return ["The run names its hosts. Give the new hosts the same names, so the command"
            + (" and `calibration.json`" if (m.get("inputs") or {}).get("calibration") or
               (m.get("scenario") or {}).get("step") == "calibrate" else "")
            + " apply as they are. Write `hosts.json` in the harness directory:", "", "    {\"hosts\": [",
            *("    " + l for l in entries.splitlines()), "    ]}", "",
            "    export OFFBENCH_HOSTS_FILE=$PWD/hosts.json",
            "    export OFFBENCH_KEY=<private key whose public key is in each host's ~/.ssh/authorized_keys>",
            "    export OFFBENCH_KNOWN_HOSTS=$PWD/known_hosts   # every host's key; known_hosts.example says how",
            "", "With a hosts file named this way offbench takes no leases. The README's \"Hosts and leases\" section "
            "has the details."]


def _binaries(m: dict, archive, run_dir: Path) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    names = ",".join(r["host"] for r in roles.values())
    L = ["Copy the harness to every host (rsync only; it starts nothing):", "",
         f"    python3 -m offbench deploy --source-only --hosts {names}", "",
         "Then put each binary where the run expects it. This helper unpacks an object and writes it on a host, "
         "printing the sha256 it landed with:", "",
         "    put() { gzip -dc \"$1\" | ssh -i \"$OFFBENCH_KEY\" -o UserKnownHostsFile=\"$OFFBENCH_KNOWN_HOSTS\" \"$2\" "
         "\"mkdir -p \\$(dirname $3) && cat > $3 && chmod 755 $3 && sha256sum $3\"; }", "",
         "for example `put <object> ec2-user@<address> '~/offbench/builds/<commit>/src/valkey-server'`, quoted so the "
         "`~` is the host's. The printed sha256 must equal the table's.", ""]
    rows, missing = [], []
    for b in m.get("builds") or []:
        path = b.get("server_path") or f"~/offbench/builds/{b.get('commit')}/src/valkey-server"
        if not b.get("binaries"):
            missing.append(f"valkey-server {b['name']} (commit {b.get('commit')})")
        for g in b.get("binaries") or []:
            obj = f"`{_rel(archive, run_dir, g['sha256'])}`" if g.get("archived") else "not archived"
            rows.append([", ".join(g.get("roles") or []), f"`{path}`", obj, f"`{g['sha256']}`",
                         f"{b['name']}: {g.get('version') or ''}"])
            if not g.get("archived"):
                missing.append(f"valkey-server {b['name']} ({g['sha256'][:12]})")
    for tool, path in (("valkey-cli", "~/base/src/valkey-cli"),
                       ("cachecannon", "~/offbench-src/vendor/cachecannon/target/release/cachecannon")):
        t = (m.get("tools") or {}).get(tool) or {}
        if not t.get("binaries"):
            missing.append(tool)
        for g in t.get("binaries") or []:
            obj = f"`{_rel(archive, run_dir, g['sha256'])}`" if g.get("archived") else "not archived"
            rows.append([", ".join(g.get("roles") or []), f"`{path}`", obj, f"`{g['sha256']}`",
                         f"{tool}: {g.get('version') or ''}"])
            if not g.get("archived"):
                missing.append(f"{tool} ({g['sha256'][:12]})")
    if rows:
        L += _table(["on roles", "path", "object", "sha256", "what"], rows)
        L.append("")
    if missing:
        L.append("Not archived, so build these (below): " + "; ".join(missing) + ".")
        L.append("")
    L.append("To build instead of copying: a new build differs in its build id, so compare `.text` sizes, not sha256.")
    for b in m.get("builds") or []:
        g = (b.get("binaries") or [{}])[0]
        build = g.get("build") or {}
        src = b.get("source") or {}
        how = (f"unpack `{_rel(archive, run_dir, src['tree'])}` (the exact files compiled) into "
               f"`~/offbench/builds/{b.get('commit')}` on each server host" if src.get("tree") and archive.has(src["tree"])
               else f"put valkey commit `{b.get('commit')}` (fork https://github.com/xdk-amz/valkey) at "
                    f"`~/offbench/builds/{b.get('commit')}` on each server host (`git archive {b.get('commit')} | tar -x`)")
        L.append(f"- valkey {b['name']}: {how}, then `make -j$(nproc) valkey-server` there with no CFLAGS in the "
                 "environment"
                 + (f"; original make settings: `{' '.join(build['make_settings'].split())}`" if build.get("make_settings") else "")
                 + (f"; compiler: {'; '.join(g['toolchain'])}" if g.get("toolchain") else "")
                 + (f"; original `.text` {g['text_bytes']} bytes" if g.get("text_bytes") else "") + ".")
    cc = (m.get("tools") or {}).get("cachecannon") or {}
    ccb = (cc.get("binaries") or [{}])[0]
    L.append("- cachecannon: `cargo build --release` in `vendor/cachecannon` of the harness on each generator host"
             + (f" (originally built by: {'; '.join(ccb['toolchain'])})" if ccb.get("toolchain") else "")
             + (f"; vendored from {cc.get('upstream')} {cc.get('upstream_commit')}" if cc.get("upstream") else "") + ".")
    L.append("- valkey-cli: `make valkey-cli` in any valkey tree, copied to `~/base/src/valkey-cli` on the server hosts.")
    return L


def _tuning(m: dict) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    tuned = [role for role, r in roles.items() if ((r.get("facts") or {}).get("pintune") or {}).get("applied")]
    L = [f"On {', '.join(tuned) if tuned else 'the server hosts'} (where the original had it), after every boot:", "",
         "    sudo bash ~/offbench-src/scripts/pintune.sh", "",
         "It stops irqbalance and moves IRQs, loopback RPS and unbound kernel work onto CPUs 17-32. The run checks "
         "it on the server hosts and refuses to start without it. What the original hosts had:", ""]
    rows = []
    for role, r in roles.items():
        f = r.get("facts") or {}
        p, t = f.get("pintune") or {}, f.get("thp") or {}
        aff = p.get("irq_affinity") or {}
        rows.append([role, p.get("irqbalance"), p.get("lo_rps_cpus"), p.get("workqueue_cpumask"),
                     ", ".join(f"{k} x{v}" for k, v in list(aff.items())[:3]), t.get("enabled"),
                     " ".join(str(x) for x in (f.get("limits") or {}).get("nofile") or [])])
    L += _table(["role", "irqbalance", "lo rps_cpus", "workqueue cpumask", "IRQ affinity (most common)", "THP",
                 "nofile soft hard"], rows)
    sysctl = {role: (r.get("facts") or {}).get("sysctl") or {} for role, r in roles.items()}
    keys = sorted({k for s in sysctl.values() for k in s})
    if keys:
        L += ["", "Kernel settings (AMI defaults unless pintune.sh sets them):", ""]
        L += _table(["sysctl"] + list(sysctl), [[k] + [sysctl[r].get(k) for r in sysctl] for k in keys])
    return L


def _run(m: dict, run_dir: Path) -> list:
    cmd = m.get("command") or {}
    name, label = cmd.get("name"), m.get("label") or "run"
    L = []
    inp = (m.get("inputs") or {}).get("calibration")
    if inp:
        L += [f"This sweep ran under calibration `{inp.get('id')}`"
              + (f", made by run `{inp['archive_run']}`" if inp.get("archive_run") else "")
              + ". Copy it into the harness directory so every build gets the same primary load:", "",
              f"    mkdir -p data/repl && cp \"$RUN\"/calibration.json data/repl/{label}-calib.json", ""]
    if cmd.get("argv") is None:
        L.append("The command line was not recorded. `manifest.json` has the configuration the records show "
                 "(`config`), and `records.jsonl` every rep's cell.")
        return L
    L += [f"The original command (run from `{cmd.get('cwd')}`):", "",
          "    python3 -m offbench " + shlex.join(cmd["argv"]), "", "With this checkout's paths:", "",
          "    python3 -m offbench " + shlex.join(rewrite_argv(cmd["argv"], name, label)), ""]
    if m.get("seconds") is not None:
        s = m["seconds"]
        L.append(f"It took {s // 60} min." if s >= 60 else f"It took {s} s.")
    if name == "repl-calibrate":
        t = (m.get("outputs") or {}).get("targets") or {}
        L.append("The calibration chooses each build's load from what it measures, so a rerun can land on slightly "
                 "different connection counts. To measure at exactly this run's loads, run the sweep with this run's "
                 "`calibration.json`." + (f" To search again against the same targets, add `--targets "
                                          f"{t['write_kcmd_s']:.0f},{t['read_kcmd_s']:.0f}`." if t.get("write_kcmd_s") else ""))
    return L


def _compare(m: dict) -> list:
    label = m.get("label") or "run"
    if (m.get("command") or {}).get("name") == "repl-calibrate":
        first = f"Compare the new `data/repl/{label}-repro.json` (targets and each build's `shape`) with `result.txt`."
    else:
        first = (f"    python3 -m offbench repl-capacity data/repl/{label}-repro.jsonl\n\n"
                 "and set its table beside `result.txt`.")
    return [first, "",
            "The new run archives itself as well (`python3 -m offbench runs list`). `python3 -m offbench runs show` on "
            "both runs should agree on the harness snapshot, every binary's sha256 and the host facts above; where "
            "they differ, that difference is the first suspect for any difference in the numbers."]


def reproduce_md(m: dict, run_dir: Path, archive) -> str:
    sc, o = m.get("scenario") or {}, m.get("outputs") or {}
    L = [f"# Reproduce {m['id']}", "", f"> {sc.get('question', '')}", "",
         f"This directory is one run: step `{sc.get('step')}` of scenario `{sc.get('name')}` (version "
         f"{sc.get('version')}), defined in `{sc.get('doc')}` of the harness below. It ran from {m.get('started')} to "
         f"{m.get('ended')} and ended `{m.get('status')}`. `result.txt` holds its result, `records.jsonl` its "
         f"{o.get('records', 0)} reps ({o.get('valid', 0)} valid), and `manifest.json` every detail used here.", "",
         "Commands below refer to this directory as `$RUN`, and to the archive's objects relative to it. Set it "
         "first (this is where the run was archived; use its new place if the archive moved):", "",
         f"    RUN={run_dir}", "",
         "An object is a gzip file named by the sha256 of what it holds: `gzip -dc <object>` gives that file (or "
         "tar). `python3 -m offbench runs bundle` packs a run with its objects for another machine."]
    if m.get("status") == "imported":
        L += ["", "This run was recorded before the archive existed, so parts of what follows are unknown; the Gaps "
              "section lists them."]
    sections = [("1. Hosts", _hosts(m)), ("2. Packages", _packages(m)),
                ("3. The harness, where you run offbench", _harness(m, archive, run_dir)),
                ("4. Host file, key and known hosts", _hosts_file(m)),
                ("5. Code and binaries on the hosts", _binaries(m, archive, run_dir)),
                ("6. Host tuning", _tuning(m)), ("7. Run", _run(m, run_dir)), ("8. Compare", _compare(m))]
    for title, body in sections:
        L += ["", f"## {title}", "", *body]
    if m.get("warnings"):
        L += ["", "## Warnings", "", *[f"- {w}" for w in m["warnings"]]]
    L += ["", "## Gaps", ""] + ([f"- {g}" for g in m["gaps"]] if m.get("gaps") else ["None recorded."])
    return "\n".join(L) + "\n"
