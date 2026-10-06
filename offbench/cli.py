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
  offbench repl-calibrate / repl-run / repl-capacity / repl-summary
                                      primary/replica runs across hosts (docs/replica-read-capacity.md)
  offbench deploy --source-only       rsync this checkout to hosts for repl-run, no worker
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
from typing import Optional

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
    # The synced tree must mirror the project root: it holds the inner
    # ``offbench/`` package AND the sibling ``vendor/cachecannon`` source, so
    # ``cd {src_dest} && python3 -m offbench`` resolves the package and the
    # runner's ``parent.parent/vendor`` finds the vendored cachecannon. The
    # package dir alone (this file's parent) lacks vendor/, so default to the
    # project root (its parent).
    package_dir = Path(a.package_dir) if a.package_dir else Path(__file__).resolve().parent.parent
    if a.source_only:
        cfg = deploy_mod.SshConfig(key=a.key or None, known_hosts=a.known_hosts or None,
                                   strict_host_key=_strict_known_hosts(a.known_hosts), src_dest=a.src_dest)
        try:
            with lease_mod.held([h.name for h in targets], "offbench deploy --source-only", registry=reg.path):
                results = [deploy_mod.sync_source(h, cfg, package_dir) for h in targets]
        except lease_mod.LeaseError as e:
            print(f"deploy: {e}", file=sys.stderr)
            return 2
        return _print_deploy(results)
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
    return _print_deploy(deploy_mod.deploy_hosts(targets, cfg, package_dir))


def _print_deploy(results) -> int:
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


KEY_MSG = ("{cmd} needs the ssh key and known-hosts file: pass --key and --known-hosts, or set OFFBENCH_KEY and "
           "OFFBENCH_KNOWN_HOSTS")
# Flags a calibration file sets itself; repl-run refuses them alongside --calibration.
CAL_OWNED = ("bins", "iot", "share_bins", "bin_write_conns", "bin_p_conns")


def _repl_names(a: argparse.Namespace, cal: Optional[dict] = None):
    """Role -> host name from the flags, filled from (and checked against) a calibration's hosts."""
    flags = {"primary": a.primary, "replica": a.replica, "gen": a.gen, "rgen": a.reader_gen}
    opt = {"primary": "--primary", "replica": "--replica", "gen": "--gen", "rgen": "--reader-gen"}
    for role, host in ((cal or {}).get("hosts") or {}).items():
        if flags.get(role) and flags[role] != host:
            return f"{opt[role]} {flags[role]} differs from the calibration's {host}: it holds on the hosts it ran on"
        flags[role] = host
    if not all(flags[k] for k in ("primary", "replica", "gen")):
        return "--primary, --replica and --gen are required"
    return {k: v for k, v in flags.items() if v}


def _repl_hosts(a: argparse.Namespace, cal: Optional[dict] = None):
    """(names, registry) for a repl command, or an error message."""
    names = _repl_names(a, cal)
    if isinstance(names, str):
        return names
    if len(set(names.values())) != len(names):
        return "primary, replica, gen and reader-gen must be distinct hosts"
    registry = hosts_mod.load_registry(a.hosts_file)
    missing = [n for n in names.values() if n not in registry.hosts]
    if missing:
        return f"no host named {', '.join(missing)}; known: {', '.join(sorted(registry.hosts)) or '(none)'}"
    return names, registry


def _repl_log(out: Path):
    log_path = out.with_suffix(".log")

    def log(msg: str) -> None:
        line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
        print(line, flush=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as f:
            f.write(line + "\n")
    return log


def _repl_open(a: argparse.Namespace, repl, reg: dict, names: dict, bins: list, log):
    """Resolve the hosts' private IPs, check they can run these builds, and gate on the generators' round
    trip. Returns (ssh, roles, rtt), or an exit code."""
    ssh = repl.Ssh(a.key, a.known_hosts)
    roles = {}
    for role, name in names.items():
        h = reg[name]
        roles[role] = repl.Role(h.name, h.ssh, ssh.call(repl.Role(h.name, h.ssh, ""), {"action": "ip"})["ip"])
    problems = repl.preflight(ssh, roles, bins, pin=1)
    if problems:
        for p in problems:
            log(f"preflight: {p}")
        log("refusing: fix the hosts above first")
        return 2
    rtt = repl.measure_rtt(ssh, roles)
    log("median rtt ms: " + ", ".join(f"{k} {(v or {}).get('median_ms', float('nan')):.3f}" for k, v in rtt.items()))
    far = repl.far_paths(rtt, a.max_rtt_ms)
    if a.max_rtt_ms and far:
        log(f"refusing: generator round trip above {a.max_rtt_ms} ms on {far}; a closed-loop client then "
            "measures the network, not the server (pick closer hosts or pass --max-rtt-ms 0)")
        return 2
    return ssh, roles, rtt


def _kv(spec: Optional[str]) -> dict:
    return dict(kv.split("=", 1) for kv in (spec or "").split(",") if kv)


def cmd_repl_run(a: argparse.Namespace) -> int:
    from . import lease as lease_mod
    from . import repl
    if not (a.key and a.known_hosts):
        print(KEY_MSG.format(cmd="repl-run"), file=sys.stderr)
        return 2
    cal = None
    if a.calibration:
        cal = json.loads(Path(a.calibration).read_text())
        given = [f"--{k.replace('_', '-')}" for k in CAL_OWNED if getattr(a, k) is not None]
        if given:
            print(f"--calibration sets the builds, cores and each build's primary load; drop {', '.join(given)}",
                  file=sys.stderr)
            return 2
        if not cal.get("converged"):
            bad = [b for b, v in cal["builds"].items() if not v["converged"]]
            print(f"calibration {a.calibration} did not converge for {', '.join(bad)}; rerun repl-calibrate",
                  file=sys.stderr)
            return 2
    got = _repl_hosts(a, cal)
    if isinstance(got, str):
        print(got, file=sys.stderr)
        return 2
    names, registry = got
    try:
        with lease_mod.held(list(names.values()), f"offbench repl-run {Path(a.out).stem}", registry=registry.path):
            return _repl_run(a, repl, registry.hosts, names, cal)
    except lease_mod.LeaseError as e:
        print(f"repl-run: {e}", file=sys.stderr)
        return 2


def _repl_bins(a: argparse.Namespace, repl, cal: Optional[dict]):
    """The builds of a run, each with its own primary load; (bins, error)."""
    if cal:
        bins = [repl.ReplBin.parse(f"{n}={spec}") for n, spec in cal["bins"].items()]
        for b in bins:
            s = cal["builds"][b.name]["shape"]
            b.share = int(b.name in cal.get("share_bins", []))
            b.write_conns, b.write_pipeline, b.p_conns, b.p_pipeline = s["w"], s["wp"], s["p"], s["pp"]
        return bins, None
    if not a.bins:
        return None, "--bins is required (or --calibration)"
    bins = [repl.ReplBin.parse(kv) for kv in a.bins.split(",")]
    known = {b.name for b in bins}
    share = {s for s in (a.share_bins or "").split(",") if s}
    if share - known or (share and a.pin_share):
        return None, "--share-bins must name builds in --bins and cannot be combined with --pin-share"
    bwc, bpc = _kv(a.bin_write_conns), _kv(a.bin_p_conns)
    if set(bwc) - known or set(bpc) - known:
        return None, "--bin-write-conns and --bin-p-conns must name builds in --bins"
    for b in bins:
        b.share = int(b.name in share)
        b.write_conns, b.p_conns = int(bwc.get(b.name, 0)), int(bpc.get(b.name, 0))
    return bins, None


def _repl_run(a: argparse.Namespace, repl, reg: dict, names: dict, cal: Optional[dict] = None) -> int:
    bins, err = _repl_bins(a, repl, cal)
    if err:
        print(err, file=sys.stderr)
        return 2
    ints = lambda s: [int(x) for x in s.split(",")]
    iots = [cal["iot"]] if cal else ints(a.iot or "2,4,8,16")
    cells = [repl.ReplCell(pipeline=p, conns=c, io_threads=t, acl=acl, write_conns=wc, write_pipeline=a.write_pipeline,
                           read_target=a.read_target, p_conns=a.p_conns, pin_share=int(a.pin_share),
                           p_pipeline=a.p_pipeline)
             for acl in ints(a.acl) for t in iots for p in ints(a.pipelines)
             for wc in ints(a.write_conns) for c in ints(a.conns)]
    if a.limit:
        cells = cells[:a.limit]
    log = _repl_log(Path(a.out))
    opened = _repl_open(a, repl, reg, names, bins, log)
    if isinstance(opened, int):
        return opened
    ssh, roles, rtt = opened
    meta = {"rtt_ms": rtt}
    if cal:
        meta["targets"] = dict(cal["targets"], tol=repl.DRIFT_TOL)
        meta["calibration"] = {"id": cal["id"], "path": str(a.calibration)}
        log(f"calibration {cal['id']}: targets {cal['targets']['write_kcmd_s']:.0f}k writes/s, "
            f"{cal['targets']['read_kcmd_s']:.0f}k reads/s; " + ", ".join(
                f"{b.name} {b.write_conns}xP{b.write_pipeline}/{b.p_conns}xP{b.p_pipeline}" for b in bins))
    return repl.run_matrix(ssh, roles, cells, bins, a.reps, Path(a.out), window_s=a.window, log=log, meta=meta)


def cmd_repl_calibrate(a: argparse.Namespace) -> int:
    from . import lease as lease_mod
    from . import repl
    if not (a.key and a.known_hosts):
        print(KEY_MSG.format(cmd="repl-calibrate"), file=sys.stderr)
        return 2
    got = _repl_hosts(a)
    if isinstance(got, str):
        print(got, file=sys.stderr)
        return 2
    names, registry = got
    try:
        with lease_mod.held(list(names.values()), f"offbench repl-calibrate {Path(a.out).stem}",
                            registry=registry.path):
            return _repl_calibrate(a, repl, registry.hosts, names)
    except lease_mod.LeaseError as e:
        print(f"repl-calibrate: {e}", file=sys.stderr)
        return 2


def _repl_calibrate(a: argparse.Namespace, repl, reg: dict, names: dict) -> int:
    from dataclasses import replace as _replace

    from . import replcap
    bins, err = _repl_bins(argparse.Namespace(bins=a.bins, share_bins=a.share_bins, pin_share=False,
                                              bin_write_conns=None, bin_p_conns=None), repl, None)
    if err:
        print(err, file=sys.stderr)
        return 2
    out = Path(a.out)
    if out.suffix != ".jsonl":
        print("--out must end in .jsonl (the calibration is written beside it as .json)", file=sys.stderr)
        return 2
    log = _repl_log(out)
    opened = _repl_open(a, repl, reg, names, bins, log)
    if isinstance(opened, int):
        return opened
    ssh, roles, rtt = opened
    cal_id = "cal-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    cores = int(a.iot)
    base = repl.ReplCell(pipeline=a.p_pipeline, conns=0, io_threads=cores, write_pipeline=a.write_pipeline, p_conns=1)
    runs = []

    def measure(shapes: dict, reps: int, phase: str) -> dict:
        run = [_replace(b, write_conns=shapes[b.name].w, write_pipeline=shapes[b.name].wp,
                        p_conns=shapes[b.name].p, p_pipeline=shapes[b.name].pp) for b in bins if b.name in shapes]
        runs.append(phase)
        got: list = []
        repl.run_matrix(ssh, roles, [base], run, reps, out, window_s=a.window, log=log,
                        meta={"rtt_ms": rtt, "phase": phase, "calib_id": cal_id},
                        run_id=f"{cal_id}-{len(runs):02d}-{phase}", collect=got)
        res: dict = {}
        for r in got:
            res.setdefault(r["bin"]["name"], []).append(r)
        return res

    builds = [b.name for b in bins]
    ref_shape = replcap.ref_shape(cores, a.write_pipeline, a.p_pipeline, a.ref_writers, a.ref_p_conns)
    ref = None
    try:
        if a.targets:
            x, y = (float(v) for v in a.targets.split(","))
            seed = {}
        else:
            ref = replcap.reference(measure, builds, ref_shape, a.ref_reps)
            x, y = ref["targets"]["write_kcmd_s"], ref["targets"]["read_kcmd_s"]
            seed = {b: [(ref_shape, v["write_kcmd_s"], v["read_kcmd_s"])] for b, v in ref["builds"].items()}
            log(f"reference {ref_shape.label()}: " + ", ".join(
                f"{b} {v['write_kcmd_s']:.0f}k writes {v['read_kcmd_s']:.0f}k reads" for b, v in ref["builds"].items())
                + f"; targets X {x:.0f}k, Y {y:.0f}k")
        res = replcap.calibrate(measure, builds, x, y, {b: ref_shape for b in builds}, tol=a.tol,
                                max_probes=a.max_probes, confirm_reps=a.confirm_reps, log=log, seed=seed)
    except repl.CellError as e:
        log(f"calibration failed: {e}")
        return 1
    finally:
        repl.cleanup(ssh, roles, [b.server for b in bins])
    doc = {"kind": "repl-calibration", "schema": 1, "id": cal_id,
           "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "records": str(out),
           "hosts": names, "bins": {kv.split("=", 1)[0]: kv.split("=", 1)[1] for kv in a.bins.split(",")},
           "share_bins": sorted(b.name for b in bins if b.share), "iot": cores, "reference": ref, **res}
    path = out.with_suffix(".json")
    path.write_text(json.dumps(doc, indent=1) + "\n")
    for b, v in res["builds"].items():
        s = replcap.Shape(**v["shape"])
        log(f"{b}: {s.label()} -> {v['write_kcmd_s']:.0f}k writes {v['write_kcmd_s'] / x - 1:+.1%}, "
            f"{v['read_kcmd_s']:.0f}k reads {v['read_kcmd_s'] / y - 1:+.1%}" + ("" if v["converged"] else "  NOT CONVERGED"))
    log(f"calibration written to {path}" + ("" if res["converged"] else " (not converged: repl-run will refuse it)"))
    if not res["converged"] and res.get("suggest"):
        d = {"write_pipeline": a.write_pipeline, "p_pipeline": a.p_pipeline, **res["suggest"]}
        log(f"one connection was a coarse step: rerun with --write-pipeline {d['write_pipeline']} "
            f"--p-pipeline {d['p_pipeline']} (the reference is measured again at those depths)")
    if res["converged"]:
        log(f"next: python3 -m offbench repl-run --calibration {path} --conns 0,50,100,200,400,800 --pipelines 16 "
            f"--reps 3 --out <sweep>.jsonl")
    return 0 if res["converged"] else 1


def cmd_repl_capacity(a: argparse.Namespace) -> int:
    from . import repl, replcap
    recs = repl.load(Path(a.path), a.run_id)
    targets = tuple(float(v) for v in a.targets.split(",")) if a.targets else None
    print(replcap.report(recs, targets))
    return 0


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
    dep.add_argument("--source-only", action="store_true",
                     help="only rsync the source (what repl-run's host side runs): no clone, no worker, no 12 h lease")
    dep.set_defaults(fn=cmd_deploy)

    ra = sub.add_parser("repl-agent", help="host side of repl-run (one JSON action)")
    ra.add_argument("payload"); ra.set_defaults(fn=cmd_repl_agent)

    def repl_common(sp: argparse.ArgumentParser, hosts_required: bool = True) -> None:
        sp.add_argument("--primary", required=hosts_required); sp.add_argument("--replica", required=hosts_required)
        sp.add_argument("--gen", required=hosts_required,
                        help="host for the writers and primary readers (and the replica readers without --reader-gen)")
        sp.add_argument("--reader-gen", default="", help="host for the replica readers (default: --gen)")
        sp.add_argument("--window", type=int, default=6, help="measurement window per rep, seconds")
        sp.add_argument("--max-rtt-ms", type=float, default=0.15,
                        help="refuse to run if the generator's median round trip to either server exceeds this (0 = off)")
        sp.add_argument("--hosts-file", type=Path, default=None, help="registry path (default: the shared host registry)")
        sp.add_argument("--key", default=os.environ.get("OFFBENCH_KEY"), help="ssh key (default $OFFBENCH_KEY)")
        sp.add_argument("--known-hosts", default=os.environ.get("OFFBENCH_KNOWN_HOSTS"),
                        help="ssh known-hosts file (default $OFFBENCH_KNOWN_HOSTS)")

    rr = sub.add_parser("repl-run", help="primary/replica write/read split across three or four hosts")
    repl_common(rr, hosts_required=False)
    rr.add_argument("--calibration", default=None,
                    help="repl-calibrate's .json: sets the hosts, builds, cores and each build's primary load, and "
                         "judges every rep against its targets")
    rr.add_argument("--bins", default=None, help="name=fullsha[|config value...],... (built under ~/offbench/builds "
                                                 "on the hosts; the |configs go to the replica only)")
    rr.add_argument("--pipelines", default="1,16", help="replica reader pipeline (list)")
    rr.add_argument("--conns", default="2400,7000", help="replica reader connections (list)")
    rr.add_argument("--iot", default=None, help="io-threads, one per core (list; default 2,4,8,16)")
    rr.add_argument("--acl", default="0")
    rr.add_argument("--write-conns", default="0", help="fixed writer connections (list); 0 = same as --conns")
    rr.add_argument("--write-pipeline", type=int, default=0, help="fixed writer pipeline; 0 = same as reader")
    rr.add_argument("--read-target", choices=("replica", "primary"), default="replica")
    rr.add_argument("--p-conns", type=int, default=0, help="GET readers on the primary, all builds (0 = none)")
    rr.add_argument("--p-pipeline", type=int, default=0, help="primary reader pipeline; 0 = same as --pipelines")
    rr.add_argument("--pin-share", action="store_true", help="main and io_thd_1 share one core")
    rr.add_argument("--share-bins", default=None,
                    help="builds that run --iot+1 IO threads with main and io_thd_1 on one core (same cores as the rest)")
    rr.add_argument("--bin-write-conns", default=None,
                    help="name=N,...: writer connections for those builds, overriding --write-conns")
    rr.add_argument("--bin-p-conns", default=None,
                    help="name=N,...: primary reader connections for those builds, overriding --p-conns")
    rr.add_argument("--reps", type=int, default=3)
    rr.add_argument("--limit", type=int, default=0, help="only the first N cells (canary)")
    rr.add_argument("--out", default="data/repl/results.jsonl", help="appended; a .log is written beside it")
    rr.set_defaults(fn=cmd_repl_run)

    rc = sub.add_parser("repl-calibrate", help="per build, the primary load that delivers X writes/s and Y reads/s")
    repl_common(rc)
    rc.add_argument("--bins", required=True, help="as repl-run")
    rc.add_argument("--share-bins", default="", help="as repl-run")
    rc.add_argument("--iot", type=int, required=True, help="cores per server (io-threads; --share-bins builds get one more)")
    rc.add_argument("--write-pipeline", type=int, default=16,
                    help="writers' pipeline depth, for every build in the reference and every probe")
    rc.add_argument("--p-pipeline", type=int, default=16,
                    help="primary readers' pipeline depth, for every build in the reference and every probe")
    rc.add_argument("--targets", default="", help="X,Y in k/s; default: the lowest writes and reads at the reference load")
    rc.add_argument("--ref-writers", type=int, default=0,
                    help="reference writer connections (default 96 per core / --write-pipeline: 6 per core at P16)")
    rc.add_argument("--ref-p-conns", type=int, default=0,
                    help="reference primary reader connections (default 400 / --p-pipeline: 25 at P16)")
    rc.add_argument("--ref-reps", type=int, default=3)
    rc.add_argument("--tol", type=float, default=0.05, help="accept a build within this of X and Y")
    rc.add_argument("--max-probes", type=int, default=12, help="one-rep probes per build")
    rc.add_argument("--confirm-reps", type=int, default=3)
    rc.add_argument("--out", required=True, help="records (.jsonl, appended); the calibration is written beside it as .json")
    rc.set_defaults(fn=cmd_repl_calibrate)

    rp = sub.add_parser("repl-capacity", help="the replica read capacity result table from a calibrated sweep")
    rp.add_argument("path"); rp.add_argument("--run-id")
    rp.add_argument("--targets", default="", help="X,Y in k/s for records that carry none (older runs)")
    rp.set_defaults(fn=cmd_repl_capacity)
    rs = sub.add_parser("repl-summary"); rs.add_argument("path", nargs="?", default="data/repl/results.jsonl")
    rs.add_argument("--run-id"); rs.add_argument("--sweep", action="store_true", help="read-client sweep view")
    rs.set_defaults(fn=cmd_repl_summary)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
