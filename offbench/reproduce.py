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
# Packages every Amazon Linux host has as part of the OS: shown with their versions, never installed by hand.
BASE = ("system-release", "kernel", "glibc", "systemd", "openssl-libs")
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


def _hosts(m: dict, archive, run_dir: Path) -> list:
    L = []
    roles = (m.get("rig") or {}).get("roles") or {}
    if not roles:
        return ["The hosts were not recorded."]

    def fact(r: dict, observed, declared_key: str):
        f = r.get("facts") or {}
        v = observed(f) if f else None
        return v if v not in (None, "") else (r.get("declared") or {}).get(declared_key)
    inst = lambda k: (lambda f: (f.get("instance") or {}).get(k))
    rows = []
    for role, r in roles.items():
        rows.append([role, f"`{r.get('host')}`", _use(role), fact(r, inst("instance_type"), "instance_type"),
                     fact(r, inst("ami_id"), "ami_id"),
                     fact(r, lambda f: (f.get("os") or {}).get("release") or (f.get("os") or {}).get("pretty"), "ami_name"),
                     fact(r, lambda f: f.get("kernel"), "kernel")])
    h = m.get("harness") or {}
    has_ud = (h.get("has") or {}).get("scripts/host-userdata.sh")
    groups: dict = {}             # user-data object (None: launched without) -> roles, from declared facts
    for role, r in roles.items():
        d = r.get("declared") or {}
        if not r.get("facts") and "user_data" in d:
            groups.setdefault((d["user_data"] or {}).get("object"), []).append(role)
    script = ("`scripts/host-userdata.sh` from the harness (section 3)" if has_ud else
              "`scripts/host-userdata.sh` of a newer offbench checkout (this run's harness predates it)")
    how = "with the user data below" if groups else f"with {script} as user data"
    L.append(f"Launch {len(roles)} hosts like the original ones, one per role, {how}:")
    L.append("")
    L += _table(["role", "original host", "used for", "instance type", "AMI", "OS release", "kernel"], rows)
    L.append("")
    for obj, rs in groups.items():
        if obj:
            L.append(f"- User data of {', '.join(rs)}: `gzip -dc {_rel(archive, run_dir, obj)}`.")
        else:
            L.append(f"- {', '.join(rs)} had no user data; how they were set up was not recorded. {script[0].upper()}"
                     f"{script[1:]} gives them the toolchain and limits the other hosts had.")
    observed = any(r.get("facts") for r in roles.values())
    declared = any(r.get("declared") for r in roles.values())
    if not observed and declared:
        L.append(f"- These host facts were not recorded by the run; they come from {m['rig'].get('declared_source')}.")
    elif not observed:
        L.append("- The run did not record its hosts. Take the instance type, AMI and zone from the rig's own records, "
                 "or match a newer archived run on the same hosts (`runs list`, `runs show`), and note what you used.")
    readers = [c for c in (m.get("config") or {}).get("replica_reader_conns") or [] if c]
    rgens = [r for r in roles if r.startswith("rgen")]
    if readers and not rgens:
        L.append("- This run had no host of its own for the replica readers: they ran on the gen host. The scenario "
                 "doc's Setup gives them one (two from 16 cores up); reproduce it as it ran, with the same roles.")
    elif len(rgens) > 1:
        L.append(f"- The replica readers were split evenly across {len(rgens)} hosts, as the scenario doc's Setup does "
                 "from 16 cores up.")
    f0 = next((r.get("facts") for r in roles.values() if r.get("facts")), None) or {}
    rtt = (m.get("rig") or {}).get("rtt_ms") or {}
    gate = ((m.get("command") or {}).get("args") or {}).get("max_rtt_ms")
    azs = sorted({fact(r, inst("az"), "az") or "?" for r in roles.values()})
    pgs = sorted({fact(r, inst("placement_group"), "placement_group") or "none" for r in roles.values()})
    L.append(f"- Every host in one availability zone (originally {', '.join(azs)}); placement group originally "
             f"{', '.join(pgs)}. A cluster placement group keeps round trips lowest. The run refuses a "
             f"generator-to-server round trip over {gate if gate is not None else '0.15'} ms."
             + (" Originally, median ms: " + ", ".join(f"{k} {(v or {}).get('median_ms', float('nan')):.3f}"
                                                        for k, v in rtt.items()) + "." if rtt else ""))
    cpu = f0.get("cpu") or {}
    L.append("- CPUs: " + (f"{cpu.get('Model name') or cpu.get('Model') or '?'}, {cpu.get('count')} of them, "
                           f"{(f0.get('mem_kb') or 0) / 1048576:.0f} GiB. " if cpu else "not recorded. ")
             + "The harness's CPU layout assumes 96: cpu 0 housekeeping, cpu 1 main, IO threads from cpu 2, IRQs on "
               "17-32, generators on 33-95.")
    users = sorted({fact(r, lambda f: (f.get("host") or {}).get("user"), "user") or r.get("ssh_user") or "?"
                    for r in roles.values()})
    L.append(f"- Log in as `{', '.join(users)}`: the harness works under that user's home (`~/offbench`, "
             "`~/offbench-src`, `~/base`).")
    clocks = [f"{role} {((r.get('facts') or {}).get('clock') or {}).get('System time', '?')}" for role, r in roles.items()]
    L.append("- Clocks synced by chrony: every host takes its measurement snapshot at the same absolute time, so "
             "offsets must stay far below a millisecond."
             + (f" Originally: {'; '.join(clocks)}." if observed else " The original offsets were not recorded."))
    return L


def _packages(m: dict) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    per = {role: (r.get("facts") or {}).get("packages") or {} for role, r in roles.items()}
    if not any(per.values()):
        amis = sorted({((r.get("declared") or {}).get("ami_id"), (r.get("declared") or {}).get("ami_name"))
                       for r in roles.values() if (r.get("declared") or {}).get("ami_id")})
        L = ["Package versions were not recorded."]
        if amis:
            L[0] += (" The hosts ran " + ", ".join(f"{a} ({n})" for a, n in amis) + ": its release fixes the "
                     "package repository, so installing from it gives the versions the run had, unless the hosts "
                     "were updated after launch.")
        return L + ["", "The harness needs: " + ", ".join(NEEDED) + "; the user data in section 1 installs the "
                    "build toolchain the rig used."]
    have = sorted({p for pk in per.values() for p, v in pk.items() if v and p not in BASE})
    L = ["On every host (the user data does most of this):", "", "    sudo dnf install -y " + " ".join(have), "",
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
                   "from that directory: it is the repository root, the same tree a clone gives, with the Python "
                   "package in its `offbench/` subdirectory.")
        if h.get("remote") and h.get("commit") and h.get("remote_branches_with_commit"):
            L += ["", f"The same with git: `git clone {h['remote']} offbench && git -C offbench checkout {h['commit']}`"
                  + (f", then `gzip -dc {_rel(archive, run_dir, h['diff_object'])} | git -C offbench apply` for the "
                     "uncommitted changes" if h.get("diff_object") else "")
                  + (". Untracked files it ran with: " + ", ".join(f"`{u}`" for u in h["untracked"]) + "."
                     if h.get("untracked") else ".")]
        elif h.get("commit"):
            L += ["", f"No branch of {h.get('remote') or 'its remote'} known here holds `{h['commit']}`, so the "
                      "snapshot is the copy to use." + (f" Evidence for the commit: {h['evidence']}" if h.get("evidence") else "")]
    elif h.get("commit"):
        L.append(f"No snapshot of the harness was archived. It was offbench commit `{h['commit']}`"
                 + (f" (evidence: {h['evidence']})" if h.get("evidence") else "") + ".")
    else:
        L.append("The harness that ran is unknown.")
    return L


def _hosts_file(m: dict) -> list:
    roles = (m.get("rig") or {}).get("roles") or {}
    users = {role: (((r.get("facts") or {}).get("host") or {}).get("user") or (r.get("declared") or {}).get("user")
                    or r.get("ssh_user") or "ec2-user")
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
    if not any(r.get("facts") for r in roles.values()):
        L = ["On the server hosts, after every boot:", "", "    sudo bash ~/offbench-src/scripts/pintune.sh", "",
             "It stops irqbalance and moves IRQs, loopback RPS and unbound kernel work onto CPUs 17-32. The hosts' "
             "tuning was not recorded."]
        if (m.get("config") or {}).get("pinned"):
            L.append("Every rep of this run was pinned, and the harness refuses a pinned rep on a server host without "
                     "pintune, so the server hosts had it.")
        return L
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
        L += ["", "Kernel settings the original hosts had:", ""]
        L += _table(["sysctl"] + list(sysctl), [[k] + [sysctl[r].get(k) for r in sysctl] for k in keys])
        first = next(s for s in sysctl.values() if s)
        same = {k: v for k, v in first.items() if v is not None and all(s.get(k) == v for s in sysctl.values() if s)}
        if same:
            L += ["", "To set the ones they shared (pintune.sh sets netdev_max_backlog again):", "",
                  "    sudo sysctl -w " + " ".join(shlex.quote(f"{k}={v}") for k, v in sorted(same.items()))]
    nofile = {tuple((((r.get("facts") or {}).get("limits") or {}).get("nofile") or [])) for r in roles.values()} - {()}
    if nofile:
        L += ["", "Open-file limit of the login shell (soft, hard): " + "; ".join(f"{a} {b}" for a, b in sorted(nofile))
              + ". The user data raises it in /etc/security/limits.conf; it applies from the next login."]
    return L


def _run(m: dict, run_dir: Path, archive=None) -> list:
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
    if cmd.get("source"):
        L += [f"The run did not record its command; this one is from {cmd['source']}"
              + (f" The driver script is archived: `gzip -dc {_rel(archive, run_dir, cmd['driver']['object'])}`."
                 if archive is not None and (cmd.get("driver") or {}).get("object") else ""), ""]
    L += [f"The original command (run from `{cmd.get('cwd') or '?'}`):", "",
          "    python3 -m offbench " + shlex.join(cmd["argv"]), "",
          "With this checkout's paths, in the shell where section 4's three exports are set (the command needs "
          "`OFFBENCH_KEY` and `OFFBENCH_KNOWN_HOSTS`, and finds the hosts through `OFFBENCH_HOSTS_FILE`):", "",
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
    h = m.get("harness") or {}
    if (h.get("has") or {}).get("offbench/archive.py") is False:
        tail = ("This run's harness predates the archive, so the new run is not archived: its records are the "
                "`--out` file and the `.log` beside it. Compare the builds' commits, the host facts above and the "
                "result table.")
    else:
        tail = ("The new run archives itself under `$OFFBENCH_ARCHIVE` or `~/offbench-runs` (`python3 -m offbench runs "
                "list`). `runs show` on it and `runs --archive \"$RUN\"/../.. show` on this run should agree on the "
                "harness snapshot, every binary's sha256 and the host facts above; where they differ, that difference "
                "is the first suspect for any difference in the numbers.")
    return [first, "", tail]


def reproduce_md(m: dict, run_dir: Path, archive) -> str:
    sc, o = m.get("scenario") or {}, m.get("outputs") or {}
    h = m.get("harness") or {}
    v = sc.get("version")
    given = ", assigned when it was imported" if sc.get("version_given_at_import") else ""
    version = (f"version {v}{given}: {sc['method']}" if v is not None and sc.get("method")
               else f"version {v}{given}" if v is not None else "version unknown")
    where = ("from the harness of section 3" if (h.get("has") or {}).get("offbench/archive.py") is not False
             else "from a newer offbench checkout (this run's harness predates the archive and has no `runs`)")
    L = [f"# Reproduce {m['id']}", "", f"> {sc.get('question', '')}", "",
         f"This directory is one run: step `{sc.get('step')}` of scenario `{sc.get('name')}` ({version}). The "
         f"scenario is defined in `{sc.get('doc')}` of the harness below; its versions are listed in that doc in "
         f"newer harnesses. It ran from {m.get('started')} to "
         f"{m.get('ended')} and ended `{m.get('status')}`. `result.txt` holds its result, `records.jsonl` its "
         f"{o.get('records', 0)} reps ({o.get('valid', 0)} valid), and `manifest.json` every detail used here.", "",
         "Commands below refer to this directory as `$RUN`, and to the archive's objects relative to it. Set it "
         "first (this is where the run was archived; use its new place if the archive moved):", "",
         f"    RUN={run_dir}", "",
         "The archive is `\"$RUN\"/../..`. The `runs` commands read it only when given `--archive \"$RUN\"/../..`; "
         f"without it they read `$OFFBENCH_ARCHIVE` or `~/offbench-runs`. For example, {where}: "
         f"`python3 -m offbench runs --archive \"$RUN\"/../.. show {m['id']}`.", "",
         "An object is a gzip file named by the sha256 of what it holds: `gzip -dc <object>` gives that file (or "
         "tar). `python3 -m offbench runs bundle` packs a run with its objects for another machine."]
    if m.get("status") == "imported":
        L += ["", "This run was recorded before the archive existed, so parts of what follows are unknown; the Gaps "
              "section lists them."]
    sections = [("1. Hosts", _hosts(m, archive, run_dir)), ("2. Packages", _packages(m)),
                ("3. The harness, where you run offbench", _harness(m, archive, run_dir)),
                ("4. Host file, key and known hosts", _hosts_file(m)),
                ("5. Code and binaries on the hosts", _binaries(m, archive, run_dir)),
                ("6. Host tuning", _tuning(m)), ("7. Run", _run(m, run_dir, archive)), ("8. Compare", _compare(m))]
    for title, body in sections:
        L += ["", f"## {title}", "", *body]
    if m.get("warnings"):
        L += ["", "## Warnings", "", *[f"- {w}" for w in m["warnings"]]]
    L += ["", "## Gaps", ""] + ([f"- {g}" for g in m["gaps"]] if m.get("gaps") else ["None recorded."])
    return "\n".join(L) + "\n"
