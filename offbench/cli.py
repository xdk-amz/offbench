"""offbench command line.

  offbench queue add --bins stock=e082696cf cand=a8f50bd2c --workloads get,set --pipelines 1,16 --conns 2400,7000
  offbench queue list | remove <id> | clear
  offbench run --repo ~/valkey   (on the rig; builds the in-tree vendor/cachecannon)
  offbench backfill <logdir>          parse existing cccell/metalcell logs into results.jsonl
  offbench compare <binA> <binB>      Welch table (+ acceptance/invariance) from results.jsonl
  offbench export                     write dash/data/dashboard.json
  offbench publish                    export, then commit + push dash/ into the ratatoskr view-model repo
  offbench serve [--port 8765]        dashboard on 127.0.0.1
  offbench push <task.json> / sync    move tasks to and results from the rig over ssh
"""

from __future__ import annotations

import argparse
import functools
import http.server
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from . import analysis, backfill, export
from . import deploy as deploy_mod
from . import dispatch as dispatch_mod
from . import hosts as hosts_mod
from . import sync as sync_mod
from .queue import Queue, Task
from .schema import Bin, read_records

HERE = Path(__file__).resolve().parent.parent          # .../offbench (package root)
DEFAULT_ROOT = Path(os.environ.get("OFFBENCH_ROOT", HERE / "data"))
DEFAULT_BINS = Path(os.environ.get("OFFBENCH_BINS", HERE / "bins.json"))
DASH_DIR = HERE / "dash"


def _ints(s: str) -> list[int]:
    return [int(x) for x in s.split(",") if x]


def _strs(s: str) -> list[str]:
    return [x for x in s.split(",") if x]


def _bins(specs: list[str], registry: dict) -> list[Bin]:
    """name[=commit][@branch][:stock]  or a bare registry name."""
    out = []
    for spec in specs:
        role = "candidate"
        if spec.endswith(":stock"):
            spec, role = spec[:-6], "stock"
        name, _, rest = spec.partition("=")
        commit, _, branch = rest.partition("@")
        if name in registry and not commit:
            b = Bin(name=name, **registry[name])
        else:
            b = Bin(name=name, commit=commit or None, branch=branch or None, label=name, role=role)
        if role == "stock":
            b.role = "stock"
        out.append(b)
    return out


def load_registry(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


# ------------------------------------------------------------------ cmds ----


def cmd_queue(a: argparse.Namespace) -> int:
    q = Queue(a.root)
    if a.qcmd == "add":
        t = Task(kind=a.kind, bins=_bins(a.bins, load_registry(a.bins_file)), workloads=_strs(a.workloads),
                 pipelines=_ints(a.pipelines), conns=_ints(a.conns), io_threads=_ints(a.io_threads), reps=a.reps,
                 order=a.order, perf=a.perf, keys=a.keys, value_size=a.value_size, acl=a.acl, pin=a.pin,
                 window_s=a.window, note=a.note, submitted_by=os.environ.get("USER", ""))
        if a.dry_run:
            print(t.short())
            for i, (cell, b, rep) in enumerate(t.plan(), 1):
                print(f"  {i:3d} {b.name:<12} {cell.key():<26} rep{rep}")
            return 0
        p = q.submit(t)
        print(f"queued {t.task_id}: {t.short()}  (~{t.estimated_seconds() / 60:.0f} min)\n  {p}")
        if a.emit:
            Path(a.emit).write_text(json.dumps(t.to_dict(), indent=2))
            print(f"  also wrote {a.emit} (push it with: offbench push {a.emit})")
        return 0
    if a.qcmd == "list":
        rt = q.running_task()
        if rt:
            print(f"RUNNING {rt.task_id}: {rt.short()}")
        for t in q.list():
            print(f"{t.task_id}: {t.short()}  (~{t.estimated_seconds() / 60:.0f} min)")
        return 0
    if a.qcmd == "remove":
        print("removed" if q.remove(a.task_id) else "not found")
        return 0
    if a.qcmd == "clear":
        print(f"removed {q.clear()} tasks")
        return 0
    return 2


def cmd_run(a: argparse.Namespace) -> int:
    from .runner import Runner
    r = Runner(a.root, repo=a.repo, cachecannon=a.cachecannon, cli=a.cli, legacy_root=a.legacy_root,
               poll_s=a.poll, dry_run=a.dry_run)
    r.loop(once=a.once)
    return 0


def cmd_backfill(a: argparse.Namespace) -> int:
    bins = backfill.load_bins(a.bins_file) if a.bins_file.exists() else {}
    recs = []
    for src in a.sources:
        p = Path(src)
        recs.extend(backfill.parse_dir(p, bins) if p.is_dir() else backfill.parse_log(p, bins))
    out = Path(a.root) / "results.jsonl"
    if a.replace and out.exists():
        out.unlink()
    from .schema import append_records
    n = append_records(out, recs)
    valid = sum(1 for r in recs if r.valid)
    print(f"backfilled {n} records ({valid} valid, {n - valid} below saturation/asserts) -> {out}")
    return 0


def _records(root: Path):
    return list(read_records(Path(root) / "results.jsonl"))


def cmd_compare(a: argparse.Namespace) -> int:
    recs = _records(a.root)
    if a.session:
        recs = [r for r in recs if export.session_id(r) == a.session]
    comps = analysis.compare(recs, a.a, a.b)
    print(analysis.fmt_compare(comps, a.a, a.b))
    print()
    print(analysis.fmt_acceptance(analysis.acceptance(recs, a.a, a.b)))
    print()
    print(analysis.fmt_invariance(analysis.invariance(recs, a.b)))
    return 0


def cmd_export(a: argparse.Namespace) -> int:
    builds_file = HERE / "builds.json"
    builds_reg = json.loads(builds_file.read_text()) if builds_file.exists() else {"builds": []}
    recs = _records(a.root)
    only = getattr(a, "builds", None)
    if only:
        keep = set(only.split(","))
        builds_reg = {"builds": [b for b in builds_reg["builds"] if b["id"] in keep]}
        missing = keep - {b["id"] for b in builds_reg["builds"]}
        if missing:
            print(f"unknown build id(s): {', '.join(sorted(missing))}", file=sys.stderr)
            return 2
        src = {(s["session"], s["bin"]) for b in builds_reg["builds"] for s in b["sources"]}
        recs = [r for r in recs if (export.session_id(r), r.bin.name) in src]
    iot = getattr(a, "io_threads", None)
    if iot:
        keep_iot = {int(x) for x in iot.split(",")}
        recs = [r for r in recs if r.cell.io_threads in keep_iot]
    data = export.build(recs, queue_root=a.root, bins_registry=load_registry(a.bins_file),
                        builds_registry=builds_reg)
    out = export.write(data, DASH_DIR / "data" / "dashboard.json")
    print(f"wrote {out}: {data['metadata']['records']} records, {data['metadata']['sessions']} sessions, "
          f"{len(data['compare']['builds'])} builds, {len(data['compare']['cells'])} build×cell points")
    if a.standalone:
        html = (DASH_DIR / "index.html").read_text()
        blob = json.dumps(data, separators=(",", ":"), default=str).replace("</", "<\\/")
        html = html.replace("<script>\nconst css", f"<script>window.OFFBENCH_DATA = {blob};</script>\n<script>\nconst css", 1)
        Path(a.standalone).write_text(html)
        print(f"wrote standalone {a.standalone} ({Path(a.standalone).stat().st_size // 1024} KiB; open it in any browser)")
    return 0


PUBLISH_DIR = Path(os.environ.get("OFFBENCH_PUBLISH_DIR", HERE.parent.parent / "ratatoskr"))
LEAK_PATTERNS = (re.compile(r"\bi-[0-9a-f]{17}\b"), re.compile(r"\.pem\b"), re.compile(r"\bec2-user@"))


def cmd_publish(a: argparse.Namespace) -> int:
    """Export, copy the page + data into the view-model repo, commit, push."""
    import shutil
    rc = cmd_export(argparse.Namespace(root=a.root, bins_file=a.bins_file, standalone=None, builds=a.builds,
                                       io_threads=a.io_threads))
    if rc:
        return rc
    repo = Path(a.repo)
    if not (repo / ".git").exists():
        print(f"{repo} is not a git checkout (clone the view-model repo there or set --repo)", file=sys.stderr)
        return 2
    dest = repo / a.subdir
    (dest / "data").mkdir(parents=True, exist_ok=True)
    shutil.copy2(DASH_DIR / "index.html", dest / "index.html")
    shutil.copy2(DASH_DIR / "data" / "dashboard.json", dest / "data" / "dashboard.json")
    for f in (dest / "index.html", dest / "data" / "dashboard.json"):
        text = f.read_text()
        for pat in LEAK_PATTERNS:
            if pat.search(text):
                print(f"refusing to publish: {f.name} matches {pat.pattern!r}", file=sys.stderr)
                return 3
    git = ["git", "-C", str(repo)]
    subprocess.run(git + ["add", "-A", a.subdir], check=True)
    if subprocess.run(git + ["diff", "--cached", "--quiet"]).returncode == 0:
        print("nothing changed; not committing")
        return 0
    msg = a.message or f"{a.subdir}: refresh data"
    subprocess.run(git + ["commit", "-q", "-m", msg], check=True)
    if not a.push:
        print(f"committed to {repo} ({a.branch}); not pushed (pass --push after approval)")
        return 0
    subprocess.run(git + ["push", "origin", f"HEAD:{a.branch}"], check=True)
    print(f"published {a.subdir}/ to origin/{a.branch}")
    return 0


def cmd_serve(a: argparse.Namespace) -> int:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DASH_DIR))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", a.port), handler)
    print(f"dashboard at http://127.0.0.1:{a.port}/  (ctrl-c to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def _ssh_base(a: argparse.Namespace) -> list[str]:
    return ["-i", a.key, "-o", f"UserKnownHostsFile={a.known_hosts}", "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10"]


def _push_doc(a: argparse.Namespace, doc: dict, host: str, remote_root: str) -> str:
    """Copy one task document into a host's queue atomically (tmp name, then mv)."""
    task = Task.from_dict(doc)
    remote_tmp = f"{remote_root}/queue/.task_{task.task_id}.tmp"
    remote = f"{remote_root}/queue/task_{task.task_id}.json"
    fd, local_tmp = tempfile.mkstemp(prefix=f"task_{task.task_id}.", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
        subprocess.run(["ssh", *_ssh_base(a), host, f"mkdir -p {remote_root}/queue"], check=True)
        subprocess.run(["scp", *_ssh_base(a), local_tmp, f"{host}:{remote_tmp}"], check=True)
        subprocess.run(["ssh", *_ssh_base(a), host, f"mv {remote_tmp} {remote}"], check=True)
    finally:
        os.unlink(local_tmp)
    return remote


def cmd_push(a: argparse.Namespace) -> int:
    """Copy a task file into the rig queue atomically (tmp name, then mv)."""
    from . import lease as lease_mod
    doc = json.loads(Path(a.task_file).read_text())
    task = Task.from_dict(doc)
    reg = hosts_mod.load_registry()
    names = [h.name for h in reg.list() if h.ssh == a.host]
    if names:
        try:
            lease_mod.claim(names, f"offbench push {task.task_id}", registry=reg.path)
        except lease_mod.LeaseError as e:
            print(f"push: {e}", file=sys.stderr)
            return 2
    else:
        print(f"warning: {a.host} is not a registered host, pushing without a lease", file=sys.stderr)
    remote = _push_doc(a, doc, a.host, a.remote_root)
    print(f"pushed {task.task_id} -> {a.host}:{remote}")
    return 0


def cmd_dispatch(a: argparse.Namespace) -> int:
    """Split a task across hosts and push one subtask to each.

    Delegates to dispatch.dispatch_task, which partitions along one axis so no
    cell is split across hosts, checks the per-host split invariants before
    touching any host, stages each subtask locally, and installs it into the
    host queue with a fsync + atomic rename. Host k receives subtask k.
    """
    doc = json.loads(Path(a.task_file).read_text())
    task = Task.from_dict(doc)
    reg = _hosts_registry(a)
    names = _strs(a.hosts)
    if not names:
        print("no hosts given (--hosts a,b[,c])", file=sys.stderr)
        return 2
    try:
        targets = reg.resolve(names)
    except KeyError as e:
        print(str(e), file=sys.stderr)
        return 2

    print(f"dispatch {task.task_id}: {task.short()}  (~{task.estimated_seconds() / 60:.0f} min total)")
    if a.dry_run:
        try:
            subtasks = task.partition(len(targets))
            dispatch_mod.check_invariants(task, subtasks, targets)
        except ValueError as e:
            print(f"dispatch: {e}", file=sys.stderr)
            return 2
        for host, sub in zip(targets, subtasks):
            print(f"  {host.name:<10} {host.ssh:<28} part={sub.part} {sub.short()}  "
                  f"(~{sub.estimated_seconds() / 60:.0f} min)")
        return 0

    cfg = dispatch_mod.DispatchConfig(key=a.key or None, known_hosts=a.known_hosts or None,
                                      strict_host_key=_strict_known_hosts(a.known_hosts))
    from . import lease as lease_mod
    try:
        lease_mod.claim([h.name for h in targets], f"offbench dispatch {task.task_id}", registry=reg.path)
    except lease_mod.LeaseError as e:
        print(f"dispatch: {e}", file=sys.stderr)
        return 2
    try:
        res = dispatch_mod.dispatch_task(task, targets, cfg)
    except ValueError as e:
        print(f"dispatch: {e}", file=sys.stderr)
        return 2

    rc = 0
    for hd in res.hosts:
        head = "OK" if hd.ok else "FAIL"
        print(f"[{head}] {hd.host} part={hd.part} task_id={hd.task_id}")
        for step in hd.steps:
            print(f"    - {step}")
        if hd.error:
            print(f"    ! {hd.error}", file=sys.stderr)
            rc = 1
    return rc


def _strict_known_hosts(known_hosts: str) -> bool:
    """A real known-hosts file means verify against it (StrictHostKeyChecking=yes)."""
    return bool(known_hosts) and known_hosts != "/dev/null"


def _resolve_all_or_hosts(reg, a: argparse.Namespace):
    """Resolve the host set for a --all/--hosts command. Returns (hosts, rc)."""
    if getattr(a, "all", False):
        hs = reg.list()
        if not hs:
            print("no hosts registered", file=sys.stderr)
            return None, 2
        return hs, 0
    names = _strs(a.hosts) if getattr(a, "hosts", None) else []
    if names:
        try:
            return reg.resolve(names), 0
        except KeyError as e:
            print(str(e), file=sys.stderr)
            return None, 2
    print("one of --all or --hosts is required", file=sys.stderr)
    return None, 2


def cmd_sync(a: argparse.Namespace) -> int:
    """Pull each host's results/status/queue into data/hosts/<name>/ and merge
    its results into data/results.jsonl (deduped on record_id, so re-syncing is
    a no-op). Read-only on the hosts.
    """
    reg = _hosts_registry(a)
    targets, rc = _resolve_all_or_hosts(reg, a)
    if targets is None:
        return rc
    cfg = sync_mod.SyncConfig(key=a.key or None, known_hosts=a.known_hosts or None,
                              strict_host_key=_strict_known_hosts(a.known_hosts))
    data_dir = Path(a.root)
    results = sync_mod.sync_hosts(targets, cfg, data_dir)
    out = 0
    total_added = 0
    for r in results:
        head = "OK" if r.ok else "FAIL"
        added = r.merge.added if r.merge else 0
        total_added += added
        print(f"[{head}] {r.host} (+{added} records)")
        for step in r.steps:
            print(f"    - {step}")
        if r.error:
            print(f"    ! {r.error}", file=sys.stderr)
            out = 1
    print(f"merged {total_added} new record(s) into {data_dir / 'results.jsonl'}")
    return out


def cmd_status(a: argparse.Namespace) -> int:
    """Per-host pending/running/done/failed + last heartbeat from the mirrored
    status.json each sync pulled back."""
    reg = _hosts_registry(a)
    targets, rc = _resolve_all_or_hosts(reg, a)
    if targets is None:
        return rc
    statuses = sync_mod.status_hosts(targets, Path(a.root))
    print(f"{'host':<12} {'pending':>8} {'running':>8} {'done':>8} {'failed':>8}  heartbeat")
    for st in statuses:
        if not st.ok:
            print(f"{st.host:<12} {st.error}")
            continue

        def _c(v):
            return "-" if v is None else str(v)

        print(f"{st.host:<12} {_c(st.pending):>8} {_c(st.running):>8} "
              f"{_c(st.done):>8} {_c(st.failed):>8}  {st.heartbeat or '-'}")
    return 0


def cmd_deploy(a: argparse.Namespace) -> int:
    """rsync the project root to each host and start its worker in tmux.

    Delegates to deploy.deploy_hosts. The synced tree carries the inner
    ``offbench/`` package and its sibling owned ``vendor/cachecannon`` (auth
    baked in); the worker builds/invokes it on the host, so there is no
    external checkout and no deploy-time patch.
    """
    reg = _hosts_registry(a)
    names = _strs(a.hosts)
    if not names:
        print("deploy: --hosts is required (comma-separated host names)", file=sys.stderr)
        return 2
    try:
        targets = reg.resolve(names)
    except KeyError as e:
        print(f"deploy: {e}", file=sys.stderr)
        return 2
    from . import lease as lease_mod
    try:
        lease_mod.claim([h.name for h in targets], "offbench deploy (worker)", registry=reg.path)
    except lease_mod.LeaseError as e:
        print(f"deploy: {e}", file=sys.stderr)
        return 2
    cfg = deploy_mod.SshConfig(
        key=a.key or None,
        known_hosts=a.known_hosts or None,
        strict_host_key=_strict_known_hosts(a.known_hosts),
        src_dest=a.src_dest,
        remote_root=a.remote_root,
        repo=a.repo,
        cli=a.cli,
    )
    # The synced tree must mirror the project root: it holds the inner
    # ``offbench/`` package AND the sibling ``vendor/cachecannon`` source, so
    # ``cd {src_dest} && python3 -m offbench`` resolves the package and the
    # runner's ``parent.parent/vendor`` finds the vendored cachecannon. The
    # package dir alone (this file's parent) lacks vendor/, so default to the
    # project root (its parent).
    package_dir = Path(a.package_dir) if a.package_dir else Path(__file__).resolve().parent.parent
    results = deploy_mod.deploy_hosts(targets, cfg, package_dir)
    rc = 0
    for r in results:
        head = "OK" if r.ok else ("SKIP" if r.skipped else "FAIL")
        print(f"[{head}] {r.host}")
        for step in r.steps:
            print(f"    - {step}")
        if r.error:
            print(f"    ! {r.error}", file=sys.stderr)
        if not r.ok:
            rc = 1
    return rc


# ------------------------------------------------------------------ hosts ---


def _hosts_registry(a: argparse.Namespace) -> "hosts_mod.Registry":
    return hosts_mod.load_registry(getattr(a, "hosts_file", None))


def _ssh_run(a: argparse.Namespace, ssh: str, remote_cmd: str, timeout: int = 20) -> subprocess.CompletedProcess:
    """Run one remote command over ssh, capturing output. Never raises on rc."""
    return subprocess.run(["ssh", *_ssh_base(a), ssh, remote_cmd],
                          capture_output=True, text=True, timeout=timeout)


def _probe_host(a: argparse.Namespace, host: "hosts_mod.Host") -> dict:
    """SSH-check one host. Returns a dict of findings; never raises."""
    out: dict = {"name": host.name, "ssh": host.ssh, "reachable": False}
    # One round trip gathers everything, so a host is probed with a single ssh.
    # The owned cachecannon is vendored in-tree and synced under ~/offbench-src.
    cc_bin = "~/offbench-src/vendor/cachecannon/target/release/cachecannon"
    cc_mod = "~/offbench-src/vendor/cachecannon/src/config/mod.rs"
    script = (
        "echo __TMUX__; tmux ls 2>/dev/null; "
        f"echo __CC__; {cc_bin} --version 2>/dev/null; "
        f"echo __AUTH__; grep -c auth_user {cc_mod} 2>/dev/null; "
        "echo __NOFILE__; ulimit -n; "
        "echo __VALKEY__; pgrep -af valkey-server 2>/dev/null; "
        "echo __CCPROC__; pgrep -af '[c]achecannon' 2>/dev/null; "
        "echo __QUEUE__; ls -1 " + host.remote_root + "/queue/*.json 2>/dev/null | wc -l; "
        "echo __END__"
    )
    try:
        cp = _ssh_run(a, host.ssh, script, timeout=25)
    except subprocess.TimeoutExpired:
        out["error"] = "ssh timeout"
        return out
    if cp.returncode != 0 and "__END__" not in cp.stdout:
        out["reachable"] = False
        out["error"] = (cp.stderr.strip() or f"ssh rc={cp.returncode}")[:200]
        return out
    out["reachable"] = True

    def _section(text: str, tag: str, nxt: str) -> str:
        start = text.find(f"__{tag}__")
        if start < 0:
            return ""
        start += len(tag) + 4
        end = text.find(f"__{nxt}__", start)
        return text[start:end if end >= 0 else None].strip()

    body = cp.stdout
    tmux = _section(body, "TMUX", "CC")
    out["tmux_sessions"] = [ln.split(":", 1)[0] for ln in tmux.splitlines() if ln.strip()]
    out["matrix_session"] = any(s == "matrix" or s.startswith("matrix") for s in out["tmux_sessions"])
    out["cachecannon_version"] = _section(body, "CC", "AUTH").splitlines()[0] if _section(body, "CC", "AUTH") else ""
    auth = _section(body, "AUTH", "NOFILE")
    out["auth_support"] = (auth.isdigit() and int(auth) > 0)
    out["auth_user_hits"] = int(auth) if auth.isdigit() else 0
    nofile = _section(body, "NOFILE", "VALKEY")
    out["nofile"] = int(nofile) if nofile.isdigit() else nofile
    out["valkey_server"] = [ln for ln in _section(body, "VALKEY", "CCPROC").splitlines() if ln.strip()]
    out["cachecannon_procs"] = [ln for ln in _section(body, "CCPROC", "QUEUE").splitlines() if ln.strip()]
    qd = _section(body, "QUEUE", "END")
    out["queue_depth"] = int(qd) if qd.isdigit() else 0
    return out


def cmd_hosts(a: argparse.Namespace) -> int:
    reg = _hosts_registry(a)
    if a.hcmd == "list":
        hs = reg.list()
        if not hs:
            print("(no hosts registered)")
            return 0
        for h in hs:
            tags = f"  [{','.join(h.tags)}]" if h.tags else ""
            iid = f"  {h.instance_id}" if h.instance_id else ""
            print(f"{h.name:<10} {h.ssh:<28} {h.remote_root}{iid}{tags}")
        return 0
    if a.hcmd == "add":
        h = hosts_mod.Host(name=a.name, ssh=a.ssh, instance_id=a.instance_id, az=a.az,
                           remote_root=a.remote_root, tags=_strs(a.tags) if a.tags else [])
        try:
            reg.add(h, replace=a.replace)
        except RuntimeError as e:
            print(f"hosts add: {e}", file=sys.stderr)
            return 2
        print(f"added {h.name} ({h.ssh}) -> {reg.path}")
        return 0
    if a.hcmd == "rm":
        try:
            print(f"removed {a.name}" if reg.remove(a.name) else f"no host named {a.name!r}")
        except RuntimeError as e:
            print(f"hosts rm: {e}", file=sys.stderr)
            return 2
        return 0
    if a.hcmd == "probe":
        names = _strs(a.names) if a.names else [h.name for h in reg.list()]
        if not names:
            print("(no hosts to probe)")
            return 0
        rc = 0
        for name in names:
            try:
                host = reg.get(name)
            except KeyError as e:
                print(f"{name}: {e}")
                rc = 1
                continue
            r = _probe_host(a, host)
            if not r["reachable"]:
                print(f"{name} ({host.ssh}): UNREACHABLE - {r.get('error', '?')}")
                rc = 1
                continue
            mx = " matrix!" if r["matrix_session"] else ""
            auth = "ok" if r["auth_support"] else f"MISSING ({r['auth_user_hits']} hits)"
            print(f"{name} ({host.ssh}): reachable"
                  f"  tmux={r['tmux_sessions'] or '-'}{mx}"
                  f"  cc={r['cachecannon_version'] or '?'}"
                  f"  auth={auth}"
                  f"  nofile={r['nofile']}"
                  f"  valkey={len(r['valkey_server'])}"
                  f"  ccproc={len(r['cachecannon_procs'])}"
                  f"  queue={r['queue_depth']}")
            if a.json:
                print("  " + json.dumps(r, default=str))
        return rc
    return 2


def cmd_repl_agent(a: argparse.Namespace) -> int:
    from . import repl
    return repl.agent_main(a.payload)


def cmd_repl_run(a: argparse.Namespace) -> int:
    from . import lease as lease_mod
    from . import repl
    registry = hosts_mod.load_registry(a.hosts_file)
    reg = registry.hosts
    names = {"primary": a.primary, "replica": a.replica, "gen": a.gen, **({"rgen": a.reader_gen} if a.reader_gen else {})}
    if len(set(names.values())) != len(names):
        print("primary, replica, gen and reader-gen must be distinct hosts", file=sys.stderr)
        return 2
    missing = [n for n in names.values() if n not in reg]
    if missing:
        print(f"no host named {', '.join(missing)}; known: {', '.join(sorted(reg)) or '(none)'}", file=sys.stderr)
        return 2
    try:
        with lease_mod.held(list(names.values()), f"offbench repl-run {Path(a.out).stem}", registry=registry.path):
            return _repl_run(a, repl, reg, names)
    except lease_mod.LeaseError as e:
        print(f"repl-run: {e}", file=sys.stderr)
        return 2


def _repl_run(a: argparse.Namespace, repl, reg: dict, names: dict) -> int:
    ssh = repl.Ssh(a.key, a.known_hosts)
    roles = {}
    for role, name in names.items():
        h = reg[name]
        tmp = repl.Role(h.name, h.ssh, "")
        roles[role] = repl.Role(h.name, h.ssh, ssh.call(tmp, {"action": "ip"})["ip"])
    bins = [repl.ReplBin.parse(kv) for kv in a.bins.split(",")]
    share = {s for s in a.share_bins.split(",") if s}
    if share - {b.name for b in bins} or (share and a.pin_share):
        print("--share-bins must name builds in --bins and cannot be combined with --pin-share", file=sys.stderr)
        return 2
    for b in bins:
        b.share = int(b.name in share)
    bwc = dict(kv.split("=", 1) for kv in a.bin_write_conns.split(",") if kv)
    if set(bwc) - {b.name for b in bins}:
        print("--bin-write-conns must name builds in --bins", file=sys.stderr)
        return 2
    for b in bins:
        b.write_conns = int(bwc.get(b.name, 0))
    ints = lambda s: [int(x) for x in s.split(",")]
    cells = [repl.ReplCell(pipeline=p, conns=c, io_threads=t, acl=acl, write_conns=wc, write_pipeline=a.write_pipeline,
                           read_target=a.read_target, p_conns=a.p_conns, pin_share=int(a.pin_share))
             for acl in ints(a.acl) for t in ints(a.iot) for p in ints(a.pipelines)
             for wc in ints(a.write_conns) for c in ints(a.conns)]
    if a.limit:
        cells = cells[:a.limit]
    log_path = Path(a.out).with_suffix(".log")

    def log(msg: str) -> None:
        line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
        print(line, flush=True)
        with log_path.open("a") as f:
            f.write(line + "\n")
    rtt = repl.measure_rtt(ssh, roles)
    log("median rtt ms: " + ", ".join(f"{k} {(v or {}).get('median_ms', float('nan')):.3f}" for k, v in rtt.items()))
    far = repl.far_paths(rtt, a.max_rtt_ms)
    if a.max_rtt_ms and far:
        log(f"refusing: generator round trip above {a.max_rtt_ms} ms on {far}; a closed-loop client then "
            "measures the network, not the server (pick closer hosts or pass --max-rtt-ms 0)")
        return 2
    return repl.run_matrix(ssh, roles, cells, bins, a.reps, Path(a.out), window_s=a.window, log=log,
                           meta={"rtt_ms": rtt})


def cmd_repl_summary(a: argparse.Namespace) -> int:
    from . import repl
    recs = repl.load(Path(a.path), a.run_id)
    print(repl.sweep_summary(recs) if a.sweep else repl.summary(recs))
    return 0


# ------------------------------------------------------------------ main ----


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="offbench", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="queue + results root (default %(default)s)")
    p.add_argument("--bins-file", type=Path, default=DEFAULT_BINS)
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("queue")
    qs = q.add_subparsers(dest="qcmd", required=True)
    qa = qs.add_parser("add")
    qa.add_argument("--kind", default="matrix", choices=("matrix", "sweep", "canary"))
    qa.add_argument("--bins", nargs="+", required=True, help="name[=commit][@branch][:stock] ...")
    qa.add_argument("--workloads", default="get,set,mixed")
    qa.add_argument("--pipelines", default="1,16")
    qa.add_argument("--conns", default="2400,7000")
    qa.add_argument("--io-threads", default="16")
    qa.add_argument("--reps", type=int, default=3)
    qa.add_argument("--order", default="interleaved", choices=("interleaved", "grouped"))
    qa.add_argument("--perf", action="store_true", help="perf stat the main thread each rep")
    qa.add_argument("--keys", type=int, default=100000)
    qa.add_argument("--value-size", type=int, default=64)
    qa.add_argument("--acl", type=int, default=0)
    qa.add_argument("--pin", type=int, default=0)
    qa.add_argument("--window", type=int, default=6)
    qa.add_argument("--note", default="")
    qa.add_argument("--emit", help="also write the task JSON here (for offbench push)")
    qa.add_argument("--dry-run", action="store_true", help="print the expanded plan, queue nothing")
    qa.set_defaults(fn=cmd_queue)
    qs.add_parser("list").set_defaults(fn=cmd_queue)
    qr = qs.add_parser("remove"); qr.add_argument("task_id"); qr.set_defaults(fn=cmd_queue)
    qs.add_parser("clear").set_defaults(fn=cmd_queue)

    r = sub.add_parser("run", help="runner loop (on the rig host)")
    r.add_argument("--repo", type=Path, default=Path.home() / "valkey")
    r.add_argument("--cachecannon", type=Path, default=None,
                   help="prebuilt cachecannon binary (default: build the in-tree vendor/cachecannon)")
    r.add_argument("--cli", type=Path, default=Path.home() / "base/src/valkey-cli")
    r.add_argument("--legacy-root", type=Path, default=Path.home(), help="where ~/<bin>/src/valkey-server dirs live")
    r.add_argument("--poll", type=float, default=5.0)
    r.add_argument("--once", action="store_true")
    r.add_argument("--dry-run", action="store_true")
    r.set_defaults(fn=cmd_run)

    b = sub.add_parser("backfill")
    b.add_argument("sources", nargs="+")
    b.add_argument("--replace", action="store_true", help="truncate results.jsonl first")
    b.set_defaults(fn=cmd_backfill)

    c = sub.add_parser("compare")
    c.add_argument("a"); c.add_argument("b"); c.add_argument("--session")
    c.set_defaults(fn=cmd_compare)

    e = sub.add_parser("export"); e.add_argument("--standalone", help="also write one self-contained HTML file here")
    e.add_argument("--builds", default=None, help="comma-separated build ids to include (default: all in builds.json)")
    e.add_argument("--io-threads", default=None, help="comma-separated io-thread counts to include (default: all)")
    e.set_defaults(fn=cmd_export)
    pb = sub.add_parser("publish", help="export, then commit the page and data into the view-model repo")
    pb.add_argument("--repo", default=str(PUBLISH_DIR))
    pb.add_argument("--subdir", default="offload")
    pb.add_argument("--branch", default="gh-pages")
    pb.add_argument("--builds", default="stock", help="comma-separated build ids to publish (default: stock)")
    pb.add_argument("--io-threads", default=None, help="comma-separated io-thread counts to publish (default: all)")
    pb.add_argument("-m", "--message", default=None)
    pb.add_argument("--push", action="store_true", help="push to origin after committing (requires explicit approval)")
    pb.set_defaults(fn=cmd_publish)
    s = sub.add_parser("serve"); s.add_argument("--port", type=int, default=8765); s.set_defaults(fn=cmd_serve)

    h = sub.add_parser("hosts", help="host registry: list|add|rm|probe")
    h.add_argument("--hosts-file", type=Path, default=None, help="registry path (default: the shared host registry)")
    hs = h.add_subparsers(dest="hcmd", required=True)
    hs.add_parser("list").set_defaults(fn=cmd_hosts)
    ha = hs.add_parser("add")
    ha.add_argument("name"); ha.add_argument("ssh", help="user@ip")
    ha.add_argument("--instance-id", default=""); ha.add_argument("--az", default="")
    ha.add_argument("--remote-root", default=hosts_mod.DEFAULT_REMOTE_ROOT)
    ha.add_argument("--tags", default="", help="comma-separated")
    ha.add_argument("--replace", action="store_true", help="overwrite an existing entry")
    ha.set_defaults(fn=cmd_hosts)
    hr = hs.add_parser("rm"); hr.add_argument("name"); hr.set_defaults(fn=cmd_hosts)
    hp = hs.add_parser("probe")
    hp.add_argument("names", nargs="?", default="", help="comma-separated host names (default: all)")
    hp.add_argument("--key", default=os.environ.get("OFFBENCH_KEY", ""))
    hp.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS", "/dev/null"))
    hp.add_argument("--json", action="store_true", help="also print the full probe dict per host")
    hp.set_defaults(fn=cmd_hosts)

    x = sub.add_parser("push")
    x.add_argument("task_file")
    x.add_argument("--host", default=os.environ.get("OFFBENCH_HOST", "ec2-user@192.0.2.10"))
    x.add_argument("--key", default=os.environ.get("OFFBENCH_KEY", ""))
    x.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS", "/dev/null"))
    x.add_argument("--remote-root", default="~/offbench")
    x.set_defaults(fn=cmd_push)

    sy = sub.add_parser("sync", help="pull results/status/queue from hosts and merge (deduped)")
    syg = sy.add_mutually_exclusive_group(required=True)
    syg.add_argument("--all", action="store_true", help="every registered host")
    syg.add_argument("--hosts", default=None, help="comma-separated registered host names")
    sy.add_argument("--hosts-file", type=Path, default=None,
                    help="registry path (default: the shared host registry)")
    sy.add_argument("--key", default=os.environ.get("OFFBENCH_KEY", ""))
    sy.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS", "/dev/null"))
    sy.set_defaults(fn=cmd_sync)

    stp = sub.add_parser("status", help="per-host pending/running/done + heartbeat")
    stg = stp.add_mutually_exclusive_group(required=True)
    stg.add_argument("--all", action="store_true", help="every registered host")
    stg.add_argument("--hosts", default=None, help="comma-separated registered host names")
    stp.add_argument("--hosts-file", type=Path, default=None,
                     help="registry path (default: the shared host registry)")
    stp.set_defaults(fn=cmd_status)

    d = sub.add_parser("dispatch", help="split a task across registered hosts and push to each")
    d.add_argument("task_file")
    d.add_argument("--hosts", required=True, help="comma-separated registered host names (a,b[,c])")
    d.add_argument("--hosts-file", type=Path, default=None, help="registry path (default: the shared host registry)")
    d.add_argument("--key", default=os.environ.get("OFFBENCH_KEY", ""))
    d.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS", "/dev/null"))
    d.add_argument("--dry-run", action="store_true", help="print the per-host split, push nothing")
    d.set_defaults(fn=cmd_dispatch)

    dep = sub.add_parser("deploy", help="rsync source + start the worker on --hosts")
    dep.add_argument("--hosts", required=True, help="comma-separated registered host names")
    dep.add_argument("--hosts-file", type=Path, default=None,
                     help="registry path (default: the shared host registry)")
    dep.add_argument("--key", default=os.environ.get("OFFBENCH_KEY", ""))
    dep.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS", "/dev/null"))
    dep.add_argument("--package-dir", default=None,
                     help="local project root to rsync (default: the offbench project root; holds the package + vendor/)")
    dep.add_argument("--src-dest", default="~/offbench-src", help="remote dir the source lands in")
    dep.add_argument("--remote-root", default="~/offbench", help="worker --root on the host")
    dep.add_argument("--repo", default="~/valkey", help="valkey clone the runner archives from")
    dep.add_argument("--cli", default="~/base/src/valkey-cli", help="valkey-cli on the host")
    dep.set_defaults(fn=cmd_deploy)

    ra = sub.add_parser("repl-agent", help="host side of repl-run (one JSON action)")
    ra.add_argument("payload"); ra.set_defaults(fn=cmd_repl_agent)
    rr = sub.add_parser("repl-run", help="primary/replica write/read split across three hosts")
    rr.add_argument("--primary", required=True); rr.add_argument("--replica", required=True)
    rr.add_argument("--gen", required=True)
    rr.add_argument("--reader-gen", default="", help="host for the replica readers (default: --gen)")
    rr.add_argument("--bins", required=True, help="name=fullsha,... (built under ~/offbench/builds on the hosts)")
    rr.add_argument("--pipelines", default="1,16"); rr.add_argument("--conns", default="2400,7000")
    rr.add_argument("--iot", default="2,4,8,16"); rr.add_argument("--acl", default="0")
    rr.add_argument("--write-conns", default="0", help="fixed writer connections (list); 0 = same as --conns")
    rr.add_argument("--write-pipeline", type=int, default=0, help="fixed writer pipeline; 0 = same as reader")
    rr.add_argument("--read-target", choices=("replica", "primary"), default="replica")
    rr.add_argument("--p-conns", type=int, default=0, help="extra GET readers on the primary (P = --pipelines)")
    rr.add_argument("--pin-share", action="store_true", help="main and io_thd_1 share one core")
    rr.add_argument("--share-bins", default="",
                    help="builds that run --iot+1 IO threads with main and io_thd_1 on one core (same cores as the rest)")
    rr.add_argument("--bin-write-conns", default="",
                    help="name=N,...: writer connections for those builds, overriding --write-conns")
    rr.add_argument("--reps", type=int, default=3); rr.add_argument("--window", type=int, default=6)
    rr.add_argument("--limit", type=int, default=0, help="only the first N cells (canary)")
    rr.add_argument("--max-rtt-ms", type=float, default=0.15,
                    help="refuse to run if the generator's median round trip to either server exceeds this (0 = off)")
    rr.add_argument("--out", default="data/repl/results.jsonl")
    rr.add_argument("--hosts-file", type=Path, default=None, help="registry path (default: the shared host registry)")
    rr.add_argument("--key", default=os.environ.get("OFFBENCH_KEY"))
    rr.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS"))
    rr.set_defaults(fn=cmd_repl_run)
    rs = sub.add_parser("repl-summary"); rs.add_argument("path", nargs="?", default="data/repl/results.jsonl")
    rs.add_argument("--run-id"); rs.add_argument("--sweep", action="store_true", help="read-client sweep view")
    rs.set_defaults(fn=cmd_repl_summary)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
