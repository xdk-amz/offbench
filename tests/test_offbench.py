import json
import math
from pathlib import Path

import pytest

from offbench import analysis, backfill, export, hosts
from offbench.queue import Queue, Task
from offbench.schema import Bin, Cell, Metrics, Perf, Record

RIG_LOGS = Path(__file__).resolve().parents[2] / "rig" / "c8g"
BINS = Path(__file__).resolve().parents[1] / "bins.json"


def rec(bin_name, cell, rep, k, main=100.0, role="candidate", perf=None, asserts=0):
    return Record(bin=Bin(name=bin_name, role=role, commit=bin_name[:9]), cell=cell, rep=rep,
                  metrics=Metrics(kcmd_s=k, main_active_pct=main, clients=cell.conns + 1, asserts=asserts, perf=perf),
                  source={"kind": "backfill", "file": "t.log", "line": rep})


# ------------------------------------------------------------------ schema ----

def test_validity_gate():
    c = Cell("get", 1, 2400)
    assert rec("a", c, 1, 3000, main=99.0).valid
    assert "main_active" in rec("a", c, 1, 3000, main=90.0).invalid_reasons()[0]
    assert not rec("a", c, 1, 3000, asserts=2).valid
    r = rec("a", c, 1, 3000)
    r.metrics.clients = 100
    assert any("clients" in x for x in r.invalid_reasons())


def test_record_roundtrip():
    r = rec("a", Cell("set", 16, 7000), 2, 2500.5, perf=Perf(700, 2700, 3.9, 0.1, None, 2.0))
    r2 = Record.from_dict(json.loads(r.to_json()))
    assert r2.metrics.perf.cyc_per_cmd == 700 and r2.cell.key() == "set-P16-c7000-iot16" and r2.valid


# ------------------------------------------------------------------- queue ----

def test_task_plan_interleaved():
    t = Task(kind="matrix", bins=[Bin("a"), Bin("b")], workloads=["get"], pipelines=[1, 16], conns=[2400], reps=2)
    plan = t.plan()
    assert len(plan) == 8
    assert [b.name for _, b, _ in plan[:4]] == ["a", "b", "b", "a"]
    assert [rep for _, _, rep in plan[:4]] == [1, 1, 2, 2]
    t3 = Task(kind="matrix", bins=[Bin("a"), Bin("b"), Bin("c")], workloads=["get"], pipelines=[1], conns=[2400], reps=3)
    order = [b.name for _, b, _ in t3.plan()]
    assert order == ["a", "b", "c", "b", "c", "a", "c", "a", "b"]


def test_queue_fifo_claim_finish_and_parking(tmp_path):
    q = Queue(tmp_path)
    t1 = Task(kind="matrix", bins=[Bin("a")], task_id="20260101T000000.000Z")
    t2 = Task(kind="matrix", bins=[Bin("a")], task_id="20260101T000001.000Z")
    q.submit(t2)
    q.submit(t1)
    assert [t.task_id for t in q.list()] == [t1.task_id, t2.task_id]
    # unsupported document gets parked, not looped on
    (q.pending / "task_00000000T000000.000Z.json").write_text(json.dumps({"kind": "bogus", "bins": [{"name": "x"}]}))
    nxt = q.next()
    assert nxt.task_id == t1.task_id
    assert list(q.pending.glob("*.unsupported"))
    q.claim(nxt)
    assert q.running_task().task_id == t1.task_id
    q.finish(nxt, ok=False, error="boom")
    assert q.running_task() is None
    assert (tmp_path / "failed_tasks.jsonl").read_text().count("boom") == 1
    assert q.next().task_id == t2.task_id


def test_import_rejects_conflicting_duplicate(tmp_path):
    q = Queue(tmp_path)
    t = Task(kind="matrix", bins=[Bin("a")], task_id="20260101T000000.000Z")
    q.submit(t)
    d = t.to_dict()
    d["note"] = "different"
    with pytest.raises(ValueError):
        q.import_document(d)


# ------------------------------------------------------------------- hosts ----

def test_host_registry_add_list_rm_roundtrip(tmp_path):
    path = tmp_path / "hosts.json"
    reg = hosts.load_registry(path)
    assert reg.list() == [] and not path.exists()

    reg.add(hosts.Host(name="metal-9", ssh="ec2-user@10.0.0.9", instance_id="i-abc",
                       tags=["c8g", "canary"]))
    assert path.exists()
    assert [h.name for h in reg.list()] == ["metal-9"]

    # persistence: a fresh registry reads the same entry back with the same shape
    got = hosts.load_registry(path).get("metal-9")
    assert got.ssh == "ec2-user@10.0.0.9" and got.instance_id == "i-abc"
    assert got.remote_root == hosts.DEFAULT_REMOTE_ROOT and got.tags == ["c8g", "canary"]
    doc = json.loads(path.read_text())
    assert list(doc) == ["hosts"] and doc["hosts"][0]["name"] == "metal-9"

    # duplicate add is rejected unless replace=True
    with pytest.raises(ValueError):
        reg.add(hosts.Host(name="metal-9", ssh="ec2-user@10.0.0.99"))
    reg.add(hosts.Host(name="metal-9", ssh="ec2-user@10.0.0.99"), replace=True)
    assert hosts.load_registry(path).get("metal-9").ssh == "ec2-user@10.0.0.99"

    # remove: returns True once, persists the deletion, then False and empty
    assert reg.remove("metal-9") is True
    assert reg.list() == [] and hosts.load_registry(path).list() == []
    assert json.loads(path.read_text())["hosts"] == []
    assert reg.remove("metal-9") is False


def test_host_registry_get_and_resolve(tmp_path):
    reg = hosts.load_registry(tmp_path / "hosts.json")
    reg.add(hosts.Host(name="a", ssh="u@1"))
    reg.add(hosts.Host(name="b", ssh="u@2"))
    assert [h.name for h in reg.resolve(["b", "a"])] == ["b", "a"]
    with pytest.raises(KeyError):
        reg.get("nope")


# ---------------------------------------------------------------- analysis ----

def test_t_distribution_matches_tables():
    assert math.isclose(analysis.t_ppf(0.975, 2), 4.303, rel_tol=1e-3)
    assert math.isclose(analysis.t_ppf(0.975, 4), 2.776, rel_tol=1e-3)
    assert math.isclose(analysis.t_cdf(0.0, 5), 0.5, abs_tol=1e-9)
    assert math.isclose(analysis.t_cdf(2.015, 5), 0.95, abs_tol=2e-3)


def test_welch_and_invalid_exclusion():
    c = Cell("get", 16, 2400)
    recs = [rec("a", c, i, v) for i, v in enumerate([4000, 4010, 3990], 1)]
    recs += [rec("b", c, i, v) for i, v in enumerate([3830, 3840, 3820], 1)]
    recs.append(rec("b", c, 4, 9999, main=50.0))            # invalid rep must not move the mean
    comp = analysis.compare(recs, "a", "b")[0]
    assert comp.b.n == 3 and comp.b.invalid == 1
    assert math.isclose(comp.delta_pct, -4.25, abs_tol=0.05)
    assert comp.p_value < 0.001


def test_layout_hint():
    a = analysis.Sample(values=[1], perf={"ins_per_cmd": [2728], "cyc_per_cmd": [663], "l1i_miss_per_cmd": [0.07]})
    b = analysis.Sample(values=[1], perf={"ins_per_cmd": [2760], "cyc_per_cmd": [700], "l1i_miss_per_cmd": [4.9]})
    assert analysis.layout_hint(a, b) == "layout"
    w = analysis.Sample(values=[1], perf={"ins_per_cmd": [2400], "cyc_per_cmd": [600], "l1i_miss_per_cmd": [0.07]})
    assert analysis.layout_hint(a, w) == "work"


def test_acceptance_and_invariance():
    recs = []
    for w in ("get", "set"):
        recs += [rec("stock", Cell(w, 16, 2400), i, 3500 + i, role="stock") for i in range(1, 4)]
        recs += [rec("cand", Cell(w, 1, 2400), i, 3700 + i) for i in range(1, 4)]
        recs += [cand16 := rec("cand", Cell(w, 16, 2400), 1, 4200)]
    acc = analysis.acceptance(recs, "stock", "cand")
    assert [a["workload"] for a in acc] == ["get", "set"] and all(a["pass"] for a in acc)
    inv = analysis.invariance(recs, "cand")
    assert inv and not inv[0]["flat"] and inv[0]["spread_pct"] > 10


# ---------------------------------------------------------------- backfill ----

@pytest.mark.skipif(not RIG_LOGS.exists(), reason="rig logs not present")
def test_backfill_clientcontrol_matches_report():
    bins = backfill.load_bins(BINS)
    recs = list(backfill.parse_log(RIG_LOGS / "clientcontrol-ab.log", bins))
    assert len(recs) == 54
    comp = {c.cell: c for c in analysis.compare(recs, "ccbase", "cccandidate")}
    assert math.isclose(comp["get-P16-c2400-iot16"].delta_pct, -4.26, abs_tol=0.01)
    assert math.isclose(comp["set-P1-c7000-iot16"].delta_pct, -14.62, abs_tol=0.01)
    assert recs[0].bin.commit == "2b5d42831"


@pytest.mark.skipif(not RIG_LOGS.exists(), reason="rig logs not present")
def test_backfill_perf_lines_and_sweep_headers():
    bins = backfill.load_bins(BINS)
    wl = list(backfill.parse_log(RIG_LOGS / "ccmatrix_wl.log", bins))
    assert all(r.metrics.perf and r.metrics.perf.cyc_per_cmd for r in wl)
    sweep = list(backfill.parse_log(RIG_LOGS / "ccsweep_p1.log", bins))
    assert sorted({r.cell.io_threads for r in sweep}) == [2, 4, 6, 8, 10, 12, 14, 16]
    acl = list(backfill.parse_log(RIG_LOGS / "ccmatrix_acl.log", bins))
    assert {r.cell.acl for r in acl} == {0, 1}
    vb = list(backfill.parse_log(RIG_LOGS / "matrix_np4.log", bins))
    assert vb[0].generator == "valkey-benchmark"


def test_export_shape(tmp_path):
    c1, c16 = Cell("get", 1, 2400), Cell("get", 16, 2400)
    recs = [rec("stock", c16, i, 3500, role="stock") for i in range(1, 4)] + [rec("cand", c1, i, 3700) for i in range(1, 4)]
    data = export.build(recs, queue_root=tmp_path, builds_registry={"builds": [
        {"id": "stock", "label": "s", "commit": "e08", "branch": "u", "sources": [{"session": "t", "bin": "stock"}]},
        {"id": "cand", "label": "c", "commit": "abc", "branch": "b", "sources": [{"session": "t", "bin": "cand"}]}]})
    assert set(data) == {"metadata", "compare"}
    assert data["metadata"]["records"] == 6 and data["metadata"]["sessions"] == 1
    assert [b["id"] for b in data["compare"]["builds"]] == ["stock", "cand"]
    assert {c["build"] for c in data["compare"]["cells"]} == {"stock", "cand"}


def test_lat_line_rate_cell_and_latency_aggregation(tmp_path):
    log = tmp_path / "ccmatrix_latency.log"
    log.write_text(
        "base   P1  c2400  get   rep1:   507 k cmd/s  main_active= 36.4%  main_cpu=101.5%  clients=2401  gen_cores=80.4/106\n"
        "  server :5245197 :0  gen_err_s_sum=0\n"
        "  lat rate=500000 samples=5 gen_kreq_s=500.0 p50=145 p90=208 p99=333 p999=577 p9999=733 max=3293\n"
        "  asserts=0\n"
        "base   P1  c2400  get   rep2:   540 k cmd/s  main_active= 40.0%  main_cpu=101.5%  clients=2401  gen_cores=80.4/106\n"
        "  lat rate=500000 samples=6 gen_kreq_s=540.0 p50=150 p90=210 p99=340 p999=600 p9999=800 max=4000\n"
        "  asserts=0\n"
        "base   P16 c2400  get   rep1:  3510 k cmd/s  main_active=100.2%  main_cpu=100.3%  clients=2401  gen_cores=40.1/106\n"
        "  lat rate=0 samples=0\n"
        "  asserts=0\n"
    )
    recs = list(backfill.parse_log(log, {}))
    assert [r.cell.key() for r in recs] == ["get-P1-c2400-iot16-r500k", "get-P1-c2400-iot16-r500k", "get-P16-c2400-iot16"]
    # fixed-rate cell: main below the gate is fine, missing the rate by 8% is not
    assert recs[0].valid and recs[0].metrics.latency.p99_us == 333
    assert not recs[1].valid and "rate" in recs[1].invalid_reasons()[0]
    assert recs[2].valid and recs[2].metrics.latency.samples == 0
    d = export.build_compare(recs, {"builds": [{"id": "stock", "label": "s", "commit": "e08", "branch": "u",
                                                "sources": [{"session": "ccmatrix_latency", "bin": "base"}]}]})
    rate_cell = next(c for c in d["cells"] if c["rate"] == 500000)
    assert rate_cell["latency"]["p99_us"] == 336.5 and rate_cell["latency"]["max_us"] == 4000
    assert rate_cell["latency"]["range"]["p99_us"] == [333, 340] and rate_cell["latency"]["range"]["max_us"] == [3293, 4000]
    assert next(c for c in d["cells"] if c["rate"] == 0)["latency"] is None
    assert d["dims"]["rate"] == [0, 500000]


# --------------------------------------------------------------- multi-host ----
# Dedupe/merge is owned by offbench.merge (record_id = task_id|bin|cell|rep|host);
# see the merge tests below. Registry, partition, cell-parity and dispatch tests
# follow.


# ------------------------------------------------------------- partitioning ----
# Multi-host partition/dispatch adopted from the staging rewrite (Task 2). These
# exercise queue.Task.partition's parent_task_id/part metadata + even split, the
# offbench.merge dedupe, cell.py parity vs cccell.sh, and dispatch's per-host
# split + remote argv. All network-free (fake ssh runner).

import subprocess as _subprocess
from pathlib import Path as _Path

from offbench import cell as cellmod
from offbench import dispatch, merge
from offbench.schema import append_records, read_records


def _abpair_task(**over):
    """Two bins (A/B) so interleaved plans carry adjacent A/B pairs per cell/rep."""
    kw = dict(
        kind="matrix",
        bins=[Bin(name="A", role="stock"), Bin(name="B", role="candidate")],
        workloads=["get", "set", "mixed"],
        pipelines=[1, 16],
        conns=[2400, 7000],
        io_threads=[16],
        reps=3,
    )
    kw.update(over)
    return Task(**kw)


def _cell_of(entry):
    cell, _bin, _rep = entry
    return cell.key()


def test_partition_metadata_and_shape():
    t = _abpair_task(workloads=["get", "set", "mixed", "incr"])
    subs = t.partition(2)
    assert len(subs) == 2
    for k, s in enumerate(subs, start=1):
        assert s.parent_task_id == t.task_id
        assert s.part == f"{k}/2"


def test_partition_union_equals_parent_plan():
    t = _abpair_task(workloads=["get", "set", "mixed", "incr"])
    subs = t.partition(2)
    parent = t.plan()
    union = [e for s in subs for e in s.plan()]

    def norm(entry):
        c, b, rep = entry
        return (c.key(), b.name, rep)

    assert sorted(map(norm, union)) == sorted(map(norm, parent))
    assert len(union) == len(parent)


def test_partition_no_cell_split_across_subtasks():
    t = _abpair_task(workloads=["get", "set", "mixed", "incr"])
    subs = t.partition(2)
    owners = {}
    for i, s in enumerate(subs):
        for cell_key in {_cell_of(e) for e in s.plan()}:
            assert cell_key not in owners, f"cell {cell_key} on subtasks {owners.get(cell_key)} and {i}"
            owners[cell_key] = i
    parent_cells = {c.key() for c in t.cells()}
    assert set(owners) == parent_cells


def test_partition_ab_pairs_stay_together():
    t = _abpair_task(workloads=["get", "set", "mixed", "incr"])
    for s in t.partition(2):
        plan = s.plan()
        assert len(plan) % 2 == 0
        for i in range(0, len(plan), 2):
            c0, b0, r0 = plan[i]
            c1, b1, r1 = plan[i + 1]
            assert c0.key() == c1.key() and r0 == r1
            assert {b0.name, b1.name} == {"A", "B"}


def test_partition_even_by_estimated_runtime():
    t = _abpair_task(workloads=["get", "set", "mixed", "incr"])
    subs = t.partition(2)
    ests = [s.estimated_seconds() for s in subs]
    assert max(ests) - min(ests) < 1e-6
    assert abs(sum(ests) - t.estimated_seconds()) < 1e-6

    t3 = _abpair_task(workloads=["get", "set", "mixed"])
    subs3 = t3.partition(2)
    ests3 = sorted(s.estimated_seconds() for s in subs3)
    per_value = t3.estimated_seconds() / 3
    assert ests3[-1] - ests3[0] <= per_value + 1e-6


def test_partition_falls_through_to_next_axis():
    t = _abpair_task(workloads=["get"], io_threads=[16, 8], acl=0)
    subs = t.partition(2)
    assert len(subs) == 2
    iot = sorted(v for s in subs for v in s.io_threads)
    assert iot == [8, 16]
    union = [(_cell_of(e),) for s in subs for e in s.plan()]
    assert len(union) == len(t.plan())


def test_partition_n1_is_identity_with_part_metadata():
    t = _abpair_task()
    (only,) = t.partition(1)
    assert only.part == "1/1"
    assert only.parent_task_id == t.task_id

    def norm(entry):
        c, b, rep = entry
        return (c.key(), b.name, rep)

    assert sorted(map(norm, only.plan())) == sorted(map(norm, t.plan()))


def test_partition_rejects_unsplittable():
    t = _abpair_task(workloads=["get"], io_threads=[16], acl=0)
    with pytest.raises(ValueError):
        t.partition(2)
    with pytest.raises(ValueError):
        t.partition(0)


# ------------------------------------------------------------------- merge ----

def _mrecord(rep, workload="get", task_id="t1", bin_name="stock"):
    return Record(
        bin=Bin(name=bin_name, role="stock"),
        cell=Cell(workload=workload, pipeline=1, conns=2400, io_threads=16),
        rep=rep,
        metrics=Metrics(kcmd_s=100.0 + rep, main_active_pct=99.0, clients=2400),
        task_id=task_id,
    )


def test_merge_host_results_idempotent(tmp_path):
    host = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2", az="us-east-1b")
    host_file = tmp_path / "host_results.jsonl"
    results_path = tmp_path / "data" / "results.jsonl"

    recs = [_mrecord(0), _mrecord(1), _mrecord(2)]
    append_records(host_file, recs)
    n = len(recs)

    r1 = merge.merge_host_results(host_file, host, results_path)
    assert r1.added == n and r1.skipped == 0 and r1.total == n
    assert results_path.exists()
    line_count = len(results_path.read_text().strip().splitlines())
    assert line_count == n

    merged = list(read_records(results_path))
    assert len(merged) == n
    assert all(m.host.get("id") == "metal-2" for m in merged)

    r2 = merge.merge_host_results(host_file, host, results_path)
    assert r2.added == 0 and r2.skipped == n and r2.total == n
    assert len(results_path.read_text().strip().splitlines()) == line_count


def test_merge_into_data_results_twice_adds_once(tmp_path):
    host = hosts.Host(name="metal-3", ssh="ec2-user@10.0.0.3", az="us-east-1b")
    host_file = tmp_path / "host_results.jsonl"
    data_results = tmp_path / "data" / "results.jsonl"

    recs = [_mrecord(0), _mrecord(1)]
    append_records(host_file, recs)
    n = len(recs)

    merge.merge_host_results(host_file, host, data_results)
    count_after_first = len(list(read_records(data_results)))
    assert count_after_first == n
    assert all(r.host.get("id") == "metal-3" for r in read_records(data_results))

    merge.merge_host_results(host_file, host, data_results)
    count_after_second = len(list(read_records(data_results)))
    assert count_after_second == count_after_first


def test_merge_distinct_hosts_do_not_dedupe(tmp_path):
    host_file = tmp_path / "host_results.jsonl"
    append_records(host_file, [_mrecord(1)])
    dest = tmp_path / "results.jsonl"
    h2 = hosts.Host(name="metal-2", ssh="u@2")
    h3 = hosts.Host(name="metal-3", ssh="u@3")
    assert merge.merge_host_results(host_file, h2, dest).added == 1
    assert merge.merge_host_results(host_file, h3, dest).added == 1
    assert merge.merge_host_results(host_file, h2, dest).added == 0
    assert {m.host["id"] for m in read_records(dest)} == {"metal-2", "metal-3"}


# ----------------------------------------------------------- cell parity ----

def test_server_cmd_parity_acl():
    c = Cell(workload="get", pipeline=1, conns=7000, io_threads=16, acl=1)
    cmd = cellmod.server_cmd(_Path("/opt/valkey/valkey-server"), c, _Path("/tmp/server.log"))
    assert "--tcp-backlog" in cmd
    assert cmd[cmd.index("--tcp-backlog") + 1] == "65535"
    assert "--user" not in cmd
    assert "~*" not in cmd
    assert "default" not in cmd
    assert cmd[cmd.index("--io-threads") + 1] == "16"


def test_acl_setuser_cmds_parity():
    cmds = cellmod.acl_setuser_cmds("/opt/valkey/valkey-cli", cellmod.PORT)
    assert len(cmds) == 2
    bench, default = cmds
    assert bench[bench.index("setuser") + 1] == "bench"
    assert "on" in bench and ">benchpw" in bench
    assert "~x:*" in bench and "~?*" in bench and "&*" in bench and "+@all" in bench
    assert "~*" not in bench
    assert default[default.index("setuser") + 1] == "default"
    assert "off" in default


def test_cachecannon_toml_parity_acl():
    c = Cell(workload="get", pipeline=1, conns=7000, io_threads=16, acl=1)
    toml = cellmod.cachecannon_toml(c)
    assert 'duration = "24s"' in toml
    assert 'warmup = "3s"' in toml
    assert 'request_timeout = "1s"' in toml
    assert "prefill = true" in toml
    assert "[timestamps]" in toml
    assert "enabled = true" in toml
    assert 'mode = "userspace"' in toml
    assert "[admin]" in toml
    assert 'format = "json"' in toml
    assert 'color = "never"' in toml
    assert 'auth_user = "bench"' in toml
    assert 'auth_password = "benchpw"' in toml
    assert "get = 100" in toml and "set = 0" in toml
    assert "delete = 0" not in toml


def test_cachecannon_toml_no_auth_for_non_acl():
    c = Cell(workload="get", pipeline=1, conns=2400, io_threads=16, acl=0)
    toml = cellmod.cachecannon_toml(c)
    assert "auth_user" not in toml and "auth_password" not in toml


def test_cell_parity_render_acl():
    c = Cell(workload="get", pipeline=1, conns=7000, io_threads=16, acl=1)
    window_s = 6
    cmd = cellmod.server_cmd(_Path("/opt/valkey/valkey-server"), c, _Path("/tmp/server.log"))
    assert "--tcp-backlog" in cmd and cmd[cmd.index("--tcp-backlog") + 1] == "65535"
    assert "--user" not in cmd and "~*" not in cmd
    setuser = cellmod.acl_setuser_cmds("/opt/valkey/valkey-cli", cellmod.PORT)
    bench = next(s for s in setuser if "bench" in s)
    default = next(s for s in setuser if "default" in s)
    assert "~x:*" in bench and "~?*" in bench and "&*" in bench and "+@all" in bench
    assert "~*" not in bench
    assert "off" in default
    toml = cellmod.cachecannon_toml(c, window_s=window_s)
    assert f'duration = "{window_s + 18}s"' in toml
    assert "delete = 0" not in toml
    assert 'format = "json"' in toml
    assert "warmup = " in toml
    assert "request_timeout = " in toml
    assert "prefill = true" in toml
    assert "[timestamps]" in toml
    assert 'auth_user = "bench"' in toml and 'auth_password = "benchpw"' in toml


# ---------------------------------------------------------------- dispatch ----

def _canary_task():
    return Task(
        kind="canary",
        bins=[Bin(name="stock-dcc66eff8",
                  commit="dcc66eff8b759f90372f861b166b5edccfac26c7",
                  branch="unstable", role="stock")],
        workloads=["get"],
        pipelines=[1, 16],
        conns=[2400, 7000],
        io_threads=[16],
        acl=-1,
        reps=3,
        note="canary",
    )


def _fake_runner():
    calls = []

    def run(cmd):
        calls.append(cmd)
        return _subprocess.CompletedProcess(cmd, 0, "", "")

    return run, calls


def test_dispatch_partitions_one_subtask_per_host(tmp_path):
    task = _canary_task()
    reg = hosts.load_registry(tmp_path / "hosts.json")
    reg.add(hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2"))
    reg.add(hosts.Host(name="metal-3", ssh="ec2-user@10.0.0.3"))
    targets = reg.resolve(["metal-2", "metal-3"])

    run, calls = _fake_runner()
    stage = tmp_path / "stage"
    res = dispatch.dispatch_task(
        task, targets, dispatch.DispatchConfig(key="k.pem", known_hosts="kh"),
        stage_dir=stage, runner=run,
    )
    assert res.ok
    assert res.parent_task_id == task.task_id
    assert len(res.hosts) == 2
    assert [h.host for h in res.hosts] == ["metal-2", "metal-3"]
    assert [h.part for h in res.hosts] == ["1/2", "2/2"]
    assert len({h.task_id for h in res.hosts}) == 2

    staged = sorted(stage.glob("task_*.json"))
    assert len(staged) == 2
    subs = [Task.from_dict(json.loads(p.read_text())) for p in staged]
    for k, s in enumerate(sorted(subs, key=lambda x: x.part), start=1):
        assert s.parent_task_id == task.task_id
        assert s.part == f"{k}/2"
    assert sorted(s.acl for s in subs) == [0, 1]


def test_dispatch_no_cell_split_across_hosts(tmp_path):
    task = _canary_task()
    reg = hosts.load_registry(tmp_path / "hosts.json")
    reg.add(hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2"))
    reg.add(hosts.Host(name="metal-3", ssh="ec2-user@10.0.0.3"))
    targets = reg.resolve(["metal-2", "metal-3"])

    run, _ = _fake_runner()
    res = dispatch.dispatch_task(
        task, targets, dispatch.DispatchConfig(),
        stage_dir=tmp_path / "stage", runner=run,
    )
    seen = set()
    for sub in res.subtasks:
        for cell_key in {c.key() for c in sub.cells()}:
            assert cell_key not in seen, f"cell {cell_key} split across hosts"
            seen.add(cell_key)
    expected = set()
    for acl in (0, 1):
        for c in task.cells():
            c.acl = acl
            expected.add(c.key())
    assert seen == expected


def test_dispatch_remote_argv_pushes_to_queue(tmp_path):
    task = _canary_task()
    reg = hosts.load_registry(tmp_path / "hosts.json")
    reg.add(hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2"))
    reg.add(hosts.Host(name="metal-3", ssh="ec2-user@10.0.0.3"))
    targets = reg.resolve(["metal-2", "metal-3"])

    run, calls = _fake_runner()
    dispatch.dispatch_task(
        task, targets, dispatch.DispatchConfig(key="id.pem", known_hosts="kh"),
        stage_dir=tmp_path / "stage", runner=run,
    )
    scps = [c for c in calls if c[0] == "scp"]
    assert len(scps) == 2
    for scp in scps:
        assert "id.pem" in scp
        assert scp[-1].endswith(".json.tmp")
        assert "/queue/" in scp[-1]
    ssh_installs = [c for c in calls if c[0] == "ssh" and "os.replace" in c[-1]]
    assert len(ssh_installs) == 2


def test_dispatch_refuses_host_count_mismatch(tmp_path):
    task = _canary_task()
    reg = hosts.load_registry(tmp_path / "hosts.json")
    for n in ("metal-1", "metal-2", "metal-3"):
        reg.add(hosts.Host(name=n, ssh=f"ec2-user@10.0.0.{n[-1]}"))
    targets = reg.resolve(["metal-1", "metal-2", "metal-3"])

    run, _ = _fake_runner()
    with pytest.raises(ValueError):
        dispatch.dispatch_task(
            task, targets, dispatch.DispatchConfig(),
            stage_dir=tmp_path / "stage", runner=run,
        )


# --- regression: module entrypoint and deploy vendor-path resolution --------
# These lock in the two defects that broke the prior multi-host run: the missing
# module entrypoint (python3 -m offbench could not start, so no worker claimed
# the queued canaries) and the deploy package_dir pointing at the inner package
# (which lacks vendor/), so the vendored cachecannon never reached the host.

import subprocess as _sp
import sys as _sys

from offbench import cli as _cli
from offbench import deploy as _deploy


_OFFBENCH_ROOT = _Path(_cli.__file__).resolve().parent.parent


def test_module_entrypoint_runs_as_python_m():
    # `python3 -m offbench --help` must resolve and exit 0 from the project root.
    cp = _sp.run(
        [_sys.executable, "-m", "offbench", "--help"],
        cwd=str(_OFFBENCH_ROOT), capture_output=True, text=True,
    )
    assert cp.returncode == 0, cp.stderr
    assert "offbench" in cp.stdout
    assert "run" in cp.stdout


def test_deploy_package_dir_default_contains_vendor():
    # cmd_deploy's default package_dir is the project root, which must hold the
    # sibling vendor/cachecannon source with auth support baked in.
    default = _Path(_cli.__file__).resolve().parent.parent
    assert (default / "vendor" / "cachecannon" / "Cargo.toml").exists()
    mod = default / "vendor" / "cachecannon" / "src" / "config" / "mod.rs"
    assert mod.exists()
    assert "auth_user" in mod.read_text()


def _scripted_deploy_runner():
    """Runner that answers each remote script the way a healthy host would:
    no live matrix, vendored cachecannon present, worker started. Records argv."""
    calls = []

    def run(cmd):
        calls.append(cmd)
        script = cmd[-1] if cmd and cmd[0] == "ssh" else ""
        out = ""
        if "MATRIX_LIVE" in script and "has-session" in script and "new-session" not in script:
            out = "NO_MATRIX"
        elif "VENDOR_OK" in script:
            out = "VENDOR_OK"
        elif "new-session" in script:
            out = "WORKER_STARTED"
        return _sp.CompletedProcess(cmd, 0, out, "")

    return run, calls


def test_deploy_host_rsyncs_root_and_vendor_check_passes():
    host = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2")
    cfg = _deploy.SshConfig(key="id.pem", known_hosts="kh")
    run, calls = _scripted_deploy_runner()

    res = _deploy.deploy_host(host, cfg, _OFFBENCH_ROOT, runner=run)

    assert res.ok, res.error
    # the rsync must push the project ROOT (which contains vendor/), not the
    # inner package dir.
    rsyncs = [c for c in calls if c and c[0] == "rsync"]
    assert len(rsyncs) == 1
    src = rsyncs[0][-2]
    assert src.rstrip("/") == str(_OFFBENCH_ROOT)
    assert (_Path(src) / "vendor" / "cachecannon").is_dir()
    # the worker is pointed at the owned vendored binary under ~/offbench-src.
    assert cfg.worker_cachecannon() == \
        "~/offbench-src/vendor/cachecannon/target/release/cachecannon"


def test_deploy_host_fails_when_vendor_missing():
    host = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2")
    cfg = _deploy.SshConfig()

    def run(cmd):
        script = cmd[-1] if cmd and cmd[0] == "ssh" else ""
        out = "NO_MATRIX" if ("has-session" in script and "new-session" not in script) \
            else ("VENDOR_MISSING" if "VENDOR_OK" in script else "")
        return _sp.CompletedProcess(cmd, 0, out, "")

    res = _deploy.deploy_host(host, cfg, _OFFBENCH_ROOT, runner=run)
    assert not res.ok
    assert "vendored cachecannon missing" in (res.error or "")


# --- regression: real-ssh option and remote-quoting hazards -----------------

import argparse as _argparse
import shlex as _shlex

from offbench import dispatch as _dispatch
from offbench import sync as _sync


def test_strict_known_hosts_derivation():
    assert _cli._strict_known_hosts("/rig/known_hosts") is True
    assert _cli._strict_known_hosts("/dev/null") is False
    assert _cli._strict_known_hosts("") is False


def _assert_strict(argv, kh):
    joined = " ".join(argv)
    assert "accept-new" not in joined
    assert "StrictHostKeyChecking=yes" in joined
    assert f"UserKnownHostsFile={kh}" in joined


def test_real_known_hosts_yields_strict_ssh_and_rsync(tmp_path):
    kh = "/rig/known_hosts"
    strict = _cli._strict_known_hosts(kh)
    h = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2")
    dcfg = _deploy.SshConfig(key="id.pem", known_hosts=kh, strict_host_key=strict)
    _assert_strict(_deploy.ssh_cmd(dcfg, h, "true"), kh)
    _assert_strict(_deploy.rsync_cmd(dcfg, h, tmp_path, "~/offbench-src"), kh)
    pcfg = _dispatch.DispatchConfig(key="id.pem", known_hosts=kh, strict_host_key=strict)
    _assert_strict(_dispatch.mkqueue_cmd(pcfg, h), kh)
    _assert_strict(_dispatch.scp_task_cmd(pcfg, h, tmp_path / "t.json", "x"), kh)
    _assert_strict(_dispatch.install_task_cmd(pcfg, h, "x"), kh)
    scfg = _sync.SyncConfig(key="id.pem", known_hosts=kh, strict_host_key=strict)
    _assert_strict(_sync.rsync_pull_cmd(scfg, h, "results.jsonl", tmp_path / "r"), kh)


def test_cli_deploy_sync_dispatch_pass_strict(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(_deploy, "deploy_hosts", lambda t, cfg, p: seen.setdefault("deploy", cfg) and [])
    monkeypatch.setattr(_sync, "sync_hosts", lambda t, cfg, d: seen.setdefault("sync", cfg) and [])
    reg = tmp_path / "hosts.json"
    r = hosts.load_registry(reg)
    r.add(hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2"))
    r.save() if hasattr(r, "save") else None
    kh = str(tmp_path / "known_hosts")
    _cli.main(["deploy", "--hosts", "metal-2", "--hosts-file", str(reg), "--known-hosts", kh])
    _cli.main(["--root", str(tmp_path), "sync", "--hosts", "metal-2", "--hosts-file", str(reg),
               "--known-hosts", kh])
    assert seen["deploy"].strict_host_key is True
    assert seen["sync"].strict_host_key is True


def test_remote_script_is_one_quoted_bash_lc_argument():
    h = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2")
    cfg = _deploy.SshConfig(key="id.pem", known_hosts="/kh", strict_host_key=True)
    script = _deploy.worker_start_script(cfg)
    argv = _deploy.ssh_cmd(cfg, h, script)
    assert argv[-2] == h.ssh
    # OpenSSH joins trailing args with spaces and the remote shell re-parses
    # them: the command must split into exactly bash, -lc, <script>.
    assert _shlex.split(argv[-1]) == ["bash", "-lc", script]
    pcfg = _dispatch.DispatchConfig(key="id.pem", known_hosts="/kh", strict_host_key=True)
    for a in (_dispatch.mkqueue_cmd(pcfg, h), _dispatch.install_task_cmd(pcfg, h, "x")):
        assert a[-2] == h.ssh
        parts = _shlex.split(a[-1])
        assert parts[:2] == ["bash", "-lc"] and len(parts) == 3


# --- pinned cells: same layout as rig/ccmatrix_pin.sh + pintune.sh ----------

from offbench import cell as _cell
from offbench.schema import Cell as _Cell


def test_pinned_launch_layout_matches_script():
    c = _Cell("get", 16, 7000, 8, pin=1)
    srv = _cell.server_launch_argv(["/b/valkey-server", "--port", "7400"], c)
    assert srv[:5] == ["taskset", "-c", "0", "setsid", "-f"]
    gen = _cell.generator_launch_argv(_Path("/cc"), _Path("/t.toml"), c)
    assert gen[:3] == ["taskset", "-c", "33-95"]
    toml = _cell.cachecannon_toml(c, threads=_cell.cpu_count(_cell.PIN_GEN_CPUS), cpu_list=_cell.PIN_GEN_CPUS)
    assert "threads = 63" in toml and 'cpu_list = "33-95"' in toml


def test_unpinned_launch_is_unchanged():
    c = _Cell("get", 16, 7000, 8)
    assert _cell.server_launch_argv(["/b/s"], c) == ["setsid", "-f", "/b/s"]
    assert _cell.generator_launch_argv(_Path("/cc"), _Path("/t"), c) == ["setsid", "-f", "/cc", "/t"]
    assert "cpu_list" not in _cell.cachecannon_toml(c)


# ---- primary/replica read-write split --------------------------------------

def test_repl_plan_interleaves_and_rotates_builds():
    from offbench import repl
    bins = [repl.ReplBin("a", "1"), repl.ReplBin("b", "2"), repl.ReplBin("c", "3")]
    items = repl.plan([repl.ReplCell(1, 2400, 2)], bins, 3)
    assert [(b.name, r) for _, b, r in items] == [
        ("a", 1), ("b", 1), ("c", 1), ("b", 2), ("c", 2), ("a", 2), ("c", 3), ("a", 3), ("b", 3)]


def test_repl_server_cmd_binds_each_address_separately():
    from pathlib import Path
    from offbench.cell import server_cmd
    from offbench.schema import Cell
    argv = server_cmd(Path("/s"), Cell("get", 1, 1), Path("/l"), bind="127.0.0.1 10.0.0.5")
    i = argv.index("--bind")
    assert argv[i + 1:i + 3] == ["127.0.0.1", "10.0.0.5"] and argv[i + 3] == "--protected-mode"


def test_repl_agent_argv_is_one_bash_lc_argument():
    import json, shlex
    from offbench import repl
    ssh = repl.Ssh("/k.pem", "/kh")
    argv = ssh.argv(repl.Role("h", "u@1.2.3.4", "10.0.0.1"), {"action": "status", "x": "a b"})
    assert argv[-2] == "u@1.2.3.4" and argv[-1].startswith("bash -lc ")
    inner = shlex.split(argv[-1])[2]
    assert json.loads(shlex.split(inner)[-1]) == {"action": "status", "x": "a b"}


def _snap(t, info, main=0.0, io=0.0, ena=0):
    from offbench.repl import ALLOWANCE_FIELDS
    return {"t": t, "ut": t, "info": info, "ticks": {"main": main, "io": io},
            "ena": {k: ena for k in ALLOWANCE_FIELDS}}


def test_repl_record_counts_reads_net_of_applied_writes_and_flags_link_loss():
    from offbench import repl
    from offbench.cell import CLK_TCK
    pa = {"total_commands_processed": "0", "cmdstat_set": "calls=0,usec=1", "master_repl_offset": "10000000",
          "sync_full": "1", "connected_clients": "2400", "used_active_time_main_thread": "0"}
    pb = dict(pa, total_commands_processed="12000000", cmdstat_set="calls=12000000,usec=1",
              used_active_time_main_thread="6.0", master_repl_offset="600000000")
    ra = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_link_status": "up",
          "master_sync_in_progress": "0", "connected_clients": "2400", "slave_repl_offset": "0"}
    rb = dict(ra, total_commands_processed="42000000", cmdstat_set="calls=12000000", slave_repl_offset="590000000")
    pm = {"a": _snap(0.0, pa), "b": _snap(6.0, pb, main=6 * CLK_TCK)}
    rm = {"a": _snap(0.0, ra), "b": _snap(6.0, rb)}
    gm = {"a": {"gt": 0.0, "gen_ticks": {"writer": 0, "reader": 0}, "ena": {k: 0 for k in repl.ALLOWANCE_FIELDS}},
          "b": {"gt": 6.0, "gen_ticks": {"writer": 0, "reader": 0}, "ena": {k: 0 for k in repl.ALLOWANCE_FIELDS}}}
    gens = {g: {"alive_at_stop": True, "timeouts": 0, "latency": None} for g in ("writer", "reader")}
    stops = {"primary": {"asserts": 0}, "replica": {"asserts": 0}}
    roles = {k: repl.Role(k, "", "") for k in ("primary", "replica", "gen")}
    rec = repl.build_record(repl.ReplCell(1, 2400, 2), repl.ReplBin("a", "1"), 1, roles, pm, rm, gm,
                            gens, stops, 6, "r")
    assert rec["valid"], rec["invalid"]
    assert rec["primary"]["write_kcmd_s"] == 2000.0 and rec["replica"]["read_kcmd_s"] == 5000.0
    assert rec["primary"]["main_cpu_pct"] == 100.0 and rec["repl_lag_bytes_b"] == 10_000_000
    rb["master_link_status"] = "down"
    rec = repl.build_record(repl.ReplCell(1, 2400, 2), repl.ReplBin("a", "1"), 1, roles, pm, rm, gm,
                            gens, stops, 6, "r")
    assert not rec["valid"] and "replica link" in rec["invalid"][0]


def test_repl_sweep_zero_readers_lag_growth_and_ceiling():
    from offbench import repl
    from offbench.cell import CLK_TCK
    cell = repl.ReplCell(1, 0, 4, write_conns=64, write_pipeline=1)
    assert cell.gens == ["writer"] and cell.w_conns == 64 and cell.key() == "w64xP1-r0xP1-iot4"
    assert repl.ReplCell(16, 256, 4, write_conns=64, write_pipeline=1).w_pipeline == 1
    pa = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_repl_offset": "1000",
          "sync_full": "1", "connected_clients": "64", "used_active_time_main_thread": "0"}
    pb = dict(pa, total_commands_processed="1800000", cmdstat_set="calls=1800000",
              used_active_time_main_thread="3.0", master_repl_offset="60001000")
    ra = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_link_status": "up",
          "master_sync_in_progress": "0", "connected_clients": "1", "slave_repl_offset": "1000"}
    rb = dict(ra, total_commands_processed="1800000", cmdstat_set="calls=1800000", slave_repl_offset="60001000")
    pm = {"a": _snap(0.0, pa), "b": _snap(6.0, pb, main=3 * CLK_TCK)}
    rm = {"a": _snap(0.0, ra), "b": _snap(6.0, rb)}
    z = {k: 0 for k in repl.ALLOWANCE_FIELDS}
    gm = {"a": {"gt": 0.0, "gen_ticks": {"writer": 0}, "ena": z}, "b": {"gt": 6.0, "gen_ticks": {"writer": 0}, "ena": z}}
    gens = {"writer": {"alive_at_stop": True, "timeouts": 0, "latency": None}}
    stops = {"primary": {"asserts": 0}, "replica": {"asserts": 0}}
    roles = {k: repl.Role(k, "", "") for k in ("primary", "replica", "gen")}
    rec = repl.build_record(cell, repl.ReplBin("a", "1"), 1, roles, pm, rm, gm, gens, stops, 6, "r")
    assert rec["valid"], rec["invalid"]
    assert rec["reader"] is None and rec["replica"]["read_kcmd_s"] == 0.0
    assert rec["lag_growth_mb_s"] == 0.0 and repl.keeps_up(rec)
    # replica falls 30MB further behind over 6s and applies only half the writes
    rb2 = dict(rb, cmdstat_set="calls=900000", total_commands_processed="900000", slave_repl_offset="30001000")
    rm2 = {"a": _snap(0.0, ra), "b": _snap(6.0, rb2)}
    rec2 = repl.build_record(cell, repl.ReplBin("a", "1"), 1, roles, pm, rm2, gm, gens, stops, 6, "r")
    assert rec2["lag_growth_mb_s"] == 5.0 and not repl.keeps_up(rec2)
    assert not rec2["valid"] and rec2["invalid"] == [f"{repl.LAGGING}: applied 50% of primary writes, lag +5.0 MB/s"]
    out = repl.sweep_summary([rec, rec2])
    assert "sustainable read ceiling" in out and " 1/2 " in out
    assert "INVALID: replica fell behind in 1/1 reps" in repl.sweep_summary([rec2])
    assert "0/1  INVALID: replica fell behind" in repl.summary([rec2])


def test_repl_rtt_stats_and_far_generator_gate():
    from offbench import repl
    out = ("PING 172.31.0.9 (172.31.0.9) 56(84) bytes of data.\n"
           "64 bytes from 172.31.0.9: icmp_seq=1 ttl=127 time=0.412 ms\n"
           "64 bytes from 172.31.0.9: icmp_seq=2 ttl=127 time=0.090 ms\n"
           "64 bytes from 172.31.0.9: icmp_seq=3 ttl=127 time=0.101 ms\n")
    s = repl.rtt_stats(out)
    assert s == {"median_ms": 0.101, "min_ms": 0.09, "max_ms": 0.412, "n": 3}
    assert repl.rtt_stats("PING x\n--- x ping statistics ---\n3 packets transmitted, 0 received") is None
    rtt = {"gen_primary": {"median_ms": 0.09}, "gen_replica": {"median_ms": 0.42}, "primary_replica": {"median_ms": 0.5}}
    assert repl.far_paths(rtt, 0.15) == {"gen_replica": 0.42}
    assert repl.far_paths(dict(rtt, gen_replica=None), 0.15) == {"gen_replica": None}
    assert repl.far_paths(dict(rtt, gen_replica={"median_ms": 0.1}), 0.15) == {}


def test_repl_read_target_primary_key_and_client_gate():
    from offbench import repl
    c = repl.ReplCell(16, 25, 4, write_conns=16, write_pipeline=16, read_target="primary")
    assert c.key() == "w16xP16-r25xP16@primary-iot4"
    assert repl.ReplCell(16, 25, 4, write_conns=16).key() == "w16xP16-r25xP16-iot4"


def test_repl_primary_and_replica_readers_split_generator_cores():
    from offbench import repl
    c = repl.ReplCell(16, 100, 4, write_conns=16, write_pipeline=16, p_conns=25)
    assert c.gens == ["writer", "preader", "reader"]
    assert c.primary_clients == 41 and c.replica_clients == 100
    assert c.key() == "w16xP16-r100xP16-p25xP16-iot4"
    sets = [c.cpus(g)[0] for g in c.gens]
    cpus = set()
    for g, spec in zip(c.gens, sets):
        mine = set()
        for part in spec.split(","):
            lo, _, hi = part.partition("-")
            mine |= set(range(int(lo), int(hi or lo) + 1))
        assert not (cpus & mine) and len(mine) == c.cpus(g)[1]
        cpus |= mine
    assert not (cpus & set(range(17, 33))) and 0 not in cpus
    assert c.cpus("reader")[1] > c.cpus("preader")[1] > c.cpus("writer")[1]
    assert repl.ReplCell(16, 0, 4, write_conns=16, p_conns=25).gens == ["writer", "preader"]
    assert repl.ReplCell(16, 100, 4, write_conns=16).cpus("reader") == (repl.READER_CPUS, 40)


def test_pin_share_main_puts_first_io_thread_on_main_core(monkeypatch):
    from offbench import cell as C
    calls, placed = [], {}
    ps = "  TID COMMAND\n  100 valkey-server\n  101 io_thd_1\n  102 io_thd_2\n  103 io_epoll\n"
    def fake_run(argv, *a, **k):
        class R: stdout = ""
        if argv[:2] == ["ps", "-L"]:
            R.stdout = ps
        elif argv[:2] == ["taskset", "-pc"] and len(argv) == 4:
            placed[argv[3]] = argv[2]
        elif argv[:2] == ["taskset", "-pc"]:
            R.stdout = f"pid {argv[2]}'s current affinity list: {placed[argv[2]].replace('-', ',')}"
        calls.append(argv)
        return R
    monkeypatch.setattr(C, "_run", fake_run)
    C.pin_server_threads(100, 3, "^x", share_main=True)
    assert placed == {"100": "1", "101": "1", "102": "2", "103": "1-2"}
    placed.clear()
    C.pin_server_threads(100, 3, "^x")
    assert placed == {"100": "1", "101": "2", "102": "3", "103": "2-3"}


def test_repl_bin_parse_replica_args():
    from offbench import repl
    b = repl.ReplBin.parse("pa=abc123|keyspace-shard-bits 10|replica-parallel-apply no")
    assert (b.name, b.commit) == ("pa", "abc123")
    assert b.replica_args == "--keyspace-shard-bits 10 --replica-parallel-apply no"
    assert repl.ReplBin.parse("stock=def").replica_args == ""


def test_repl_share_bin_gets_one_more_io_thread_on_the_same_cores():
    from offbench import repl
    c = repl.ReplCell(16, 400, 4, write_conns=16, p_conns=25)
    s = repl.ReplBin("io", "1", share=1).cell_for(c)
    assert (s.io_threads, s.pin_share) == (5, 1) and s.key().endswith("-iot5-shared")
    assert repl.ReplBin("rr", "2").cell_for(c) is c and c.io_threads == 4


def test_repl_bin_write_conns_override_matches_write_rate_per_build():
    from offbench import repl
    c = repl.ReplCell(16, 400, 4, write_conns=16, write_pipeline=16, p_conns=25)
    w = repl.ReplBin("stock", "1", write_conns=9).cell_for(c)
    assert (w.write_conns, w.io_threads, w.p_conns) == (9, 4, 25) and w.key().startswith("w9xP16-")
    ws = repl.ReplBin("custom", "2", share=1, write_conns=11).cell_for(c)
    assert (ws.write_conns, ws.io_threads, ws.pin_share) == (11, 5, 1) and c.write_conns == 16


def test_repl_reader_host_layout_and_its_ena_counters():
    from offbench import repl
    from offbench.cell import CLK_TCK

    def cpuset(spec):
        out = set()
        for part in spec.split(","):
            lo, _, hi = part.partition("-")
            out |= set(range(int(lo), int(hi or lo) + 1))
        return out
    c = repl.ReplCell(16, 1600, 16, write_conns=96, write_pipeline=16, p_conns=25)
    got = {g: c.cpus(g, reader_host=True) for g in ("writer", "preader", "reader")}
    assert not (cpuset(got["writer"][0]) & cpuset(got["preader"][0]))
    for spec, n in got.values():
        assert len(cpuset(spec)) == n and 0 not in cpuset(spec) and not (cpuset(spec) & set(range(17, 33)))
    assert got["reader"][1] == 79
    z = {k: 0 for k in repl.ALLOWANCE_FIELDS}
    pa = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_repl_offset": "0",
          "sync_full": "1", "connected_clients": "121", "used_active_time_main_thread": "0"}
    pb = dict(pa, total_commands_processed="600000", cmdstat_set="calls=600000", master_repl_offset="30000000")
    ra = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_link_status": "up",
          "master_sync_in_progress": "0", "connected_clients": "1600", "slave_repl_offset": "0"}
    rb = dict(ra, total_commands_processed="6600000", cmdstat_set="calls=600000", slave_repl_offset="30000000")
    pm = {"a": _snap(0.0, pa), "b": _snap(6.0, pb, main=3 * CLK_TCK)}
    rm = {"a": _snap(0.0, ra), "b": _snap(6.0, rb)}
    gm = {"a": {"gt": 0.0, "gen_ticks": {"writer": 0, "preader": 0}, "ena": z},
          "b": {"gt": 6.0, "gen_ticks": {"writer": 6 * CLK_TCK, "preader": 0}, "ena": z}}
    rgm = {"a": {"gt": 0.0, "gen_ticks": {"reader": 0}, "ena": z},
           "b": {"gt": 6.0, "gen_ticks": {"reader": 60 * CLK_TCK}, "ena": z}}
    gens = {g: {"alive_at_stop": True, "timeouts": 0, "latency": None} for g in ("writer", "preader", "reader")}
    stops = {"primary": {"asserts": 0}, "replica": {"asserts": 0}}
    roles = {k: repl.Role(k, "", "") for k in ("primary", "replica", "gen", "rgen")}
    rec = repl.build_record(c, repl.ReplBin("a", "1"), 1, roles, pm, rm, gm, gens, stops, 6, "r", rgm=rgm)
    assert rec["valid"], rec["invalid"]
    assert rec["writer"]["cores"] == 1.0 and rec["reader"]["cores"] == 10.0 and rec["hosts"]["rgen"] == "rgen"
    rgm["b"]["ena"] = dict(z, pps_allowance_exceeded=7)
    rec = repl.build_record(c, repl.ReplBin("a", "1"), 1, roles, pm, rm, gm, gens, stops, 6, "r", rgm=rgm)
    assert rec["invalid"] == ["reader gen ENA allowance exceeded {'pps_allowance_exceeded': 7}"]


# ---- host leases ------------------------------------------------------------

import os  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

from offbench import cli, lease  # noqa: E402

HOSTS_TOOL = Path.home() / ".kiro/crew/skills/hosts/hosts.py"
needs_tool = pytest.mark.skipif(not HOSTS_TOOL.exists(), reason="hosts tool not installed")


@pytest.fixture(autouse=True)
def _no_real_host_registry(tmp_path_factory, monkeypatch):
    """No test reads or leases hosts in the real shared registry."""
    monkeypatch.setenv("HOSTS_DIR", str(tmp_path_factory.mktemp("no-shared-hosts")))


def _shared(tmp_path, monkeypatch, names=("p", "r", "g")):
    d = tmp_path / "hostsdir"
    d.mkdir()
    monkeypatch.setenv("HOSTS_DIR", str(d))
    monkeypatch.setenv("KIRO_SESSION_ID", "offbench-test")
    monkeypatch.delenv("OFFBENCH_HOSTS_FILE", raising=False)
    monkeypatch.delenv("HOSTS_LEASE", raising=False)
    doc = {"version": 1, "ssh_options": {}, "leases": {}, "rtt": {"pairs": {}},
           "hosts": [{"name": n, "ssh": "ec2-user@192.0.2.1", "instance_id": "", "az": "", "private_ip": "192.0.2.1",
                      "remote_root": "~/offbench", "tags": [], "notes": "x"} for n in names]}
    (d / "registry.json").write_text(json.dumps(doc))
    return d / "registry.json"


def test_shared_registry_is_default_and_read_only(tmp_path, monkeypatch, capsys):
    path = _shared(tmp_path, monkeypatch)
    assert hosts.default_path() == path
    reg = hosts.load_registry()
    assert reg.get("p").ssh == "ec2-user@192.0.2.1"
    with pytest.raises(RuntimeError):
        reg.add(hosts.Host(name="z", ssh="ec2-user@192.0.2.9"))
    assert cli.main(["hosts", "add", "z", "ec2-user@192.0.2.9"]) == 2
    assert "shared registry" in capsys.readouterr().err
    monkeypatch.setenv("OFFBENCH_HOSTS_FILE", str(tmp_path / "local.json"))
    assert hosts.default_path() == tmp_path / "local.json"


@needs_tool
def test_lease_held_takes_and_releases(tmp_path, monkeypatch):
    path = _shared(tmp_path, monkeypatch)
    with lease.held(["p", "r"], "test", registry=path, probe=False) as taken:
        assert taken == ["p", "r"]
        lz = json.loads(path.read_text())["leases"]
        assert lz["p"]["holder"]["pid"] == os.getpid() and lz["p"]["kind"] == "job"
    assert json.loads(path.read_text())["leases"] == {}


@needs_tool
def test_lease_keeps_what_the_caller_already_holds(tmp_path, monkeypatch):
    path = _shared(tmp_path, monkeypatch)
    subprocess.run([sys.executable, str(HOSTS_TOOL), "acquire", "p", "--label", "mine", "--ignore-busy"],
                   check=True, capture_output=True)
    with lease.held(["p", "r"], "test", registry=path, probe=False) as taken:
        assert taken == ["r"]
    lz = json.loads(path.read_text())["leases"]
    assert set(lz) == {"p"} and lz["p"]["owner"]["label"] == "mine"


def test_local_hosts_file_runs_unleased(tmp_path, monkeypatch, capsys):
    path = _shared(tmp_path, monkeypatch)
    local = tmp_path / "hosts.json"
    local.write_text(json.dumps({"hosts": [{"name": "p", "ssh": "ec2-user@192.0.2.1"}]}))
    with lease.held(["p"], "test", registry=local, probe=False) as taken:
        assert taken == []
    assert "without leases" in capsys.readouterr().err
    assert json.loads(path.read_text())["leases"] == {}


@needs_tool
def test_repl_run_refuses_hosts_leased_elsewhere(tmp_path, monkeypatch, capsys):
    path = _shared(tmp_path, monkeypatch)
    monkeypatch.setenv("KIRO_SESSION_ID", "someone-else")
    subprocess.run([sys.executable, str(HOSTS_TOOL), "acquire", "r", "--label", "theirs", "--ignore-busy"],
                   check=True, capture_output=True)
    monkeypatch.setenv("KIRO_SESSION_ID", "offbench-test")
    t0 = time.time()
    rc = cli.main(["repl-run", "--primary", "p", "--replica", "r", "--gen", "g", "--bins", "a=" + "0" * 40,
                   "--key", "/k.pem", "--known-hosts", "/kh", "--out", str(tmp_path / "o.jsonl")])
    assert rc == 2 and time.time() - t0 < 10  # refused before any ssh to the unroutable test address
    assert "held by another session" in capsys.readouterr().err
    assert set(json.loads(path.read_text())["leases"]) == {"r"}


def test_cli_imports_without_the_lease_module():
    """Hosts get cli.py synced on its own, so importing it must not pull in lease.py."""
    r = subprocess.run([sys.executable, "-c", "import sys, offbench.cli; print('offbench.lease' in sys.modules)"],
                       cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == "False", r.stderr


# ---- replica read capacity: calibration, report, preflight -------------------

def test_repl_run_and_calibrate_require_the_ssh_key(capsys, monkeypatch):
    monkeypatch.delenv("OFFBENCH_KEY", raising=False)
    monkeypatch.delenv("OFFBENCH_KNOWN_HOSTS", raising=False)
    assert cli.main(["repl-run", "--primary", "p", "--replica", "r", "--gen", "g", "--bins", "a=1"]) == 2
    assert "pass --key and --known-hosts, or set OFFBENCH_KEY" in capsys.readouterr().err
    assert cli.main(["repl-calibrate", "--primary", "p", "--replica", "r", "--gen", "g", "--bins", "a=1",
                     "--iot", "4", "--out", "x.jsonl"]) == 2
    assert "repl-calibrate needs the ssh key" in capsys.readouterr().err


def test_repl_run_calibration_owns_builds_and_must_have_converged(tmp_path, capsys):
    cal = {"id": "cal-1", "converged": False, "hosts": {"primary": "p", "replica": "r", "gen": "g"},
           "builds": {"a": {"converged": False}}, "bins": {"a": "1"}, "iot": 4,
           "targets": {"write_kcmd_s": 1.0, "read_kcmd_s": 1.0}}
    path = tmp_path / "c.json"
    path.write_text(json.dumps(cal))
    base = ["repl-run", "--calibration", str(path), "--key", "/k", "--known-hosts", "/kh"]
    assert cli.main(base + ["--bins", "a=1"]) == 2
    assert "drop --bins" in capsys.readouterr().err
    assert cli.main(base) == 2
    assert "did not converge for a" in capsys.readouterr().err
    import argparse
    a = argparse.Namespace(primary="x", replica=None, gen=None, reader_gen="")
    assert "differs from the calibration's p" in cli._repl_names(a, cal)


def test_repl_bin_carries_its_own_primary_load():
    from offbench import repl
    c = repl.ReplCell(16, 400, 4, write_conns=24, write_pipeline=16, p_conns=25)
    b = repl.ReplBin("rr", "1", write_conns=18, write_pipeline=4, p_conns=13, p_pipeline=8)
    got = b.cell_for(c)
    assert (got.w_conns, got.w_pipeline, got.p_conns, got.pp_pipeline, got.pipeline) == (18, 4, 13, 8, 16)
    assert got.key() == "w18xP4-r400xP16-p13xP8-iot4"
    assert c.key() == "w24xP16-r400xP16-p25xP16-iot4"
    assert got.threads("writer", True) == 16 and got.threads("preader", True) == 13
    assert got.threads("reader", True) == 79 and repl.ReplCell(16, 50, 4).threads("reader") == 40
    s = repl.ReplBin("custom", "2", share=1).cell_for(c)
    assert (s.io_threads, s.cores) == (5, 4)


def _rec_with(**over):
    """A valid build_record() rep (2M writes/s, 5M replica reads/s), then fields overridden."""
    from offbench import repl
    from offbench.cell import CLK_TCK
    pa = {"total_commands_processed": "0", "cmdstat_set": "calls=0,usec=1", "master_repl_offset": "10000000",
          "sync_full": "1", "connected_clients": "2400", "used_active_time_main_thread": "0"}
    pb = dict(pa, total_commands_processed="18000000", cmdstat_set="calls=12000000,usec=1",
              used_active_time_main_thread="6.0", master_repl_offset="600000000")
    ra = {"total_commands_processed": "0", "cmdstat_set": "calls=0", "master_link_status": "up",
          "master_sync_in_progress": "0", "connected_clients": "2400", "slave_repl_offset": "0"}
    rb = dict(ra, total_commands_processed="42000000", cmdstat_set="calls=12000000", slave_repl_offset="590000000")
    z = {k: 0 for k in repl.ALLOWANCE_FIELDS}
    gm = {"a": {"gt": 0.0, "gen_ticks": {"writer": 0, "reader": 0}, "ena": z},
          "b": {"gt": 6.0, "gen_ticks": {"writer": 0, "reader": 0}, "ena": z}}
    gens = {g: {"alive_at_stop": True, "timeouts": 0, "err_pct": 0.0, "latency": None} for g in ("writer", "reader")}
    gens["writer"].update(over.pop("writer", {}))
    roles = {k: repl.Role(k, "", "") for k in ("primary", "replica", "gen")}
    rec = repl.build_record(repl.ReplCell(1, 2400, 2), repl.ReplBin("a", "1"), 1, roles,
                            {"a": _snap(0.0, pa), "b": _snap(6.0, pb, main=6 * CLK_TCK)},
                            {"a": _snap(0.0, ra), "b": _snap(6.0, rb)}, gm, gens,
                            {"primary": {"asserts": 0}, "replica": {"asserts": 0}}, 6, "r")
    rec.update(over)
    return repl.revalidate(rec)


def test_repl_generator_errors_and_target_drift_invalidate():
    rec = _rec_with()
    assert rec["valid"], rec["invalid"]
    assert (rec["primary"]["write_kcmd_s"], rec["primary"]["read_kcmd_s"]) == (2000.0, 1000.0)
    assert _rec_with(writer={"err_pct": 0.5})["invalid"] == ["writer reported 0.5% errors"]
    ok = _rec_with(targets={"write_kcmd_s": 1900.0, "read_kcmd_s": 1060.0, "tol": 0.07})
    assert ok["valid"], ok["invalid"]
    bad = _rec_with(targets={"write_kcmd_s": 2200.0, "read_kcmd_s": 1000.0})
    assert bad["invalid"] == ["primary writes 2000k/s, -9.1% off target 2200k/s"]


def test_repl_preflight_names_what_each_host_lacks(monkeypatch):
    from offbench import repl
    local = repl.code_hashes()
    replies = {
        "p": {"code": local, "cli": True, "cachecannon": False, "missing_servers": [], "pintune": True},
        "r": {"code": dict(local, **{"repl.py": "old"}), "cli": False, "cachecannon": False,
              "missing_servers": ["/home/u/offbench/builds/2/src/valkey-server"],
              "pintune": "pinned cell without pintune: irqbalance=active"},
        "g": {"code": local, "cli": False, "cachecannon": False, "missing_servers": []},
        "x": None,
    }
    sent = []

    class FakeSsh:
        def start(self, role, payload):
            sent.append((role.name, payload))

            def wait(timeout=0):
                if replies[role.name] is None:
                    raise repl.CellError(f"{role.name} preflight: CellError: unknown action preflight")
                return replies[role.name]
            return wait
    roles = {"primary": repl.Role("p", "", ""), "replica": repl.Role("r", "", ""),
             "gen": repl.Role("g", "", ""), "rgen": repl.Role("x", "", "")}
    got = repl.preflight(FakeSsh(), roles, [repl.ReplBin("a", "1"), repl.ReplBin("b", "2")])
    assert got[0] == f"r: ~/offbench-src differs from this checkout in repl.py; {repl.SYNC_HINT}"
    assert got[1:4] == ["r: no build at /home/u/offbench/builds/2/src/valkey-server",
                        "r: no valkey-cli at ~/base/src/valkey-cli", "r: pinned cell without pintune: irqbalance=active"]
    assert got[4] == "g: cachecannon not built at ~/offbench-src/vendor/cachecannon/target/release"
    assert got[5].startswith("x: host offbench predates preflight")
    assert len(got) == 6
    by = dict(sent)
    assert by["p"]["servers"] == ["~/offbench/builds/1/src/valkey-server", "~/offbench/builds/2/src/valkey-server"]
    assert by["g"]["servers"] == [] and by["p"]["pintune"] and not by["g"]["pintune"]


def _sim_primary(rtt_w, rtt_r, sw, sr):
    """Closed-loop primary: writes and reads share one server queue. Rates in k/s for (fw, fp) in flight."""
    def rates(fw, fp):
        def busy(d):
            return fw * sw / (rtt_w + sw + d) + fp * sr / (rtt_r + sr + d)
        lo, hi = 0.0, 1.0
        if busy(0.0) > 1:
            for _ in range(80):
                mid = (lo + hi) / 2
                lo, hi = (mid, hi) if busy(mid) > 1 else (lo, mid)
        d = hi if busy(0.0) > 1 else 0.0
        return fw / (rtt_w + sw + d) / 1000, fp / (rtt_r + sr + d) / 1000
    return rates


def _sim_split(rtt_w, rtt_r, sw, sr):
    """Writes and reads each on their own server, so one role saturates alone (rr's SET path on main)."""
    wq, rq = _sim_primary(rtt_w, rtt_r, sw, 0.0), _sim_primary(rtt_w, rtt_r, 0.0, sr)
    return lambda fw, fp: (wq(fw, 0)[0], rq(0, fp)[1])


def _sim_measure(sims, noise, calls):
    def measure(shapes, reps, phase):
        calls.append((phase, dict(shapes), reps))
        out = {}
        for b, s in shapes.items():
            out[b] = []
            for _ in range(reps):
                w, r = sims[b](s.fw, s.fp)
                out[b].append({"valid": True, "primary": {"write_kcmd_s": w * (1 + noise.gauss(0, 0.01)),
                                                          "read_kcmd_s": r * (1 + noise.gauss(0, 0.01))}})
        return out
    return measure


def _sim_calibrate(sims, ref, seed=7, **kw):
    import random
    from offbench import replcap
    calls = []
    measure = _sim_measure(sims, random.Random(seed), calls)
    builds = list(sims)
    r0 = replcap.reference(measure, builds, ref, 3)
    x, y = r0["targets"]["write_kcmd_s"], r0["targets"]["read_kcmd_s"]
    assert x == min(v["write_kcmd_s"] for v in r0["builds"].values())
    seed_pts = {b: [(ref, v["write_kcmd_s"], v["read_kcmd_s"])] for b, v in r0["builds"].items()}
    logs = []
    res = replcap.calibrate(measure, builds, x, y, {b: ref for b in builds}, seed=seed_pts, log=logs.append, **kw)
    return res, calls, (x, y), logs


def test_replcap_calibration_matches_each_simulated_build_to_x_and_y():
    from offbench import replcap
    sims = {"stock": _sim_primary(100e-6, 100e-6, 1 / 2.0e6, 1 / 2.0e6),
            "rr": _sim_primary(100e-6, 100e-6, 1 / 1.4e6, 1 / 12e6),
            "custom": _sim_primary(500e-6, 100e-6, 1 / 1.5e6, 1 / 6e6)}
    builds = list(sims)
    ref = replcap.ref_shape(4, 4, 4)
    res, calls, _, _ = _sim_calibrate(sims, ref)
    assert res["converged"], res["builds"]
    for b, v in res["builds"].items():
        assert v["off"] <= replcap.CAL_TOL and v["probes"] <= 10 and not v["coarse"], (b, v)
    assert res["suggest"] == {}
    assert all(list(c[1]) == [b for b in builds if b in c[1]] for c in calls)
    # every build keeps the run's depths; only connection counts move
    assert {(s.wp, s.pp) for c in calls for s in c[1].values()} == {(4, 4)}
    assert [c[0] for c in calls].count("confirm") == 1 and calls[0][0] == "reference"
    assert all(c[2] == 1 for c in calls if c[0] == "probe")
    for b in builds:
        pts = res["points"][b]
        assert pts[0]["kind"] == "reference" and set(pts[0]) == {"kind", "w", "wp", "p", "pp", "write_kcmd_s",
                                                                  "read_kcmd_s"}
        assert [p["kind"] for p in pts[-3:]] == ["confirm"] * 3 and {p["kind"] for p in pts[1:-3]} <= {"probe"}


def test_replcap_calibration_cuts_a_role_on_its_own_plateau():
    """rr's writes saturate on their own (main's SET path) while its reads are cheap: the search must take
    writers down while writes are above X, even when reads are below Y (the 8-core rr failure)."""
    from offbench import replcap
    sims = {"stock": _sim_primary(90e-6, 90e-6, 1 / 1.7e6, 1 / 1.7e6),
            "rr": _sim_split(100e-6, 45e-6, 1 / 1.25e6, 1 / 7e6),
            "custom": _sim_primary(300e-6, 150e-6, 1 / 1.1e6, 1 / 3e6)}
    ref = replcap.ref_shape(8, 8, 2)
    assert ref == replcap.Shape(96, 8, 200, 2)
    res, calls, (x, y), _ = _sim_calibrate(sims, ref)
    assert res["converged"], res["builds"]
    assert {(s.wp, s.pp) for c in calls for s in c[1].values()} == {(8, 2)}
    assert res["builds"]["rr"]["probes"] <= 10
    # each role moves only toward its own target from the closest point so far
    seen = []
    for p in res["points"]["rr"]:
        s = replcap.Shape(p["w"], p["wp"], p["p"], p["pp"])
        if p["kind"] == "probe" and seen:
            b, w, r = replcap.best(seen, x, y)
            if w > x * (1 + replcap.AIM * replcap.CAL_TOL):
                assert s.w <= b.w, (s, b, w)
            if r < y * (1 - replcap.AIM * replcap.CAL_TOL):
                assert s.p >= b.p, (s, b, r)
        seen.append((s, p["write_kcmd_s"], p["read_kcmd_s"]))


def test_replcap_calibration_names_a_coarse_step():
    """At P16 rr carries X and Y with a handful of connections: calibration says so and names the depths."""
    from offbench import replcap
    sims = {"stock": _sim_primary(100e-6, 100e-6, 1 / 2.0e6, 1 / 2.0e6),
            "rr": _sim_primary(100e-6, 100e-6, 1 / 1.4e6, 1 / 12e6),
            "custom": _sim_primary(500e-6, 100e-6, 1 / 1.5e6, 1 / 6e6)}
    res, calls, _, logs = _sim_calibrate(sims, replcap.ref_shape(4, 16, 16))
    assert {(s.wp, s.pp) for c in calls for s in c[1].values()} == {(16, 16)}
    rr = res["builds"]["rr"]
    s = replcap.Shape(**rr["shape"])
    assert rr["coarse"] and s.w < replcap.MIN_CONNS
    d = rr["coarse"]["write_pipeline"]
    assert d < 16 and s.fw / d >= replcap.MIN_CONNS and s.fw / (2 * d) < replcap.MIN_CONNS
    # every build shares the depths, so the run's suggestion is the lowest any build needs
    assert res["suggest"]["write_pipeline"] == min(v["coarse"]["write_pipeline"] for v in res["builds"].values()
                                                   if "write_pipeline" in v["coarse"])
    assert any("rr:" in m and "coarse step" in m and f"--write-pipeline {d}" in m for m in logs)


def test_replcap_steps_from_the_point_closest_on_both_roles():
    """rr at 2 cores, P4 writers and P16 primary readers (2026-10-06): its reference is 7.7% over on both roles,
    every lighter probe is far over on one. The search must step from (15, 8), not back next to the reference."""
    from offbench import replcap
    pts = [(replcap.Shape(w, 4, p, 16), W, R) for w, p, W, R in
           [(48, 25, 420, 1427), (24, 13, 390, 1463), (12, 6, 449, 1185), (17, 9, 438, 1334), (15, 8, 426, 1301),
            (13, 7, 433, 1256), (47, 25, 408, 1449), (48, 24, 435, 1381)]]
    s = replcap.propose(pts, 390.0, 1327.0)
    assert s.wp == 4 and s.pp == 16 and s.w <= 16 and 7 <= s.p <= 9, s


def test_replcap_split_follows_k_with_the_load():
    """k = (writes/reads) / (writes in flight / reads in flight) rises as rr's 2-core load lightens: carried
    from the closest point to the total the search picks, not held at the closest point's value."""
    from offbench import replcap
    pts = [(replcap.Shape(w, 4, p, 16), W, R) for w, p, W, R in
           [(48, 25, 419, 1428), (24, 12, 407, 1392), (12, 6, 438, 1202), (17, 9, 434, 1325), (15, 8, 427, 1318)]]
    s, w, r = pts[1]
    k = (w / r) / (s.fw / s.fp)
    assert replcap._split_at(pts, s, w, r, s.fw + s.fp) == k
    assert k < replcap._split_at(pts, s, w, r, 180) <= k * (288 / 180) ** replcap.K_SLOPE
    flat = [(t[0], t[1], t[2] * 1.0) for t in pts[:1]] + [pts[1]]
    assert replcap._split_at(flat, s, w, r, 180) == k            # too few points to follow a slope


def test_replcap_ref_shape_keeps_requests_in_flight_at_any_depth():
    from offbench import replcap
    assert replcap.ref_shape(4, 16, 16) == replcap.Shape(24, 16, 25, 16)
    assert replcap.ref_shape(8, 8, 2) == replcap.Shape(96, 8, 200, 2)
    assert replcap.ref_shape(2, 4, 16) == replcap.Shape(48, 4, 25, 16)
    assert replcap.ref_shape(2, 4, 16, writers=10, readers=7) == replcap.Shape(10, 4, 7, 16)
    assert replcap.coarse(replcap.Shape(6, 16, 2, 16)) == {"write_pipeline": 8, "p_pipeline": 2}
    assert replcap.coarse(replcap.Shape(12, 8, 14, 2)) == {}


def test_replcap_next_x_brackets_and_bisects_on_a_plateau():
    from offbench import replcap
    assert replcap.next_x([(10, 50.0)], 100.0) == 20          # extrapolate, at most doubling
    assert replcap.next_x([(10, 90.0)], 99.0) == 11.0
    # bracket (0, 0)..(784, 1927): interpolation lands at 720, outside the middle half, so bisect
    assert replcap.next_x([(784, 1927.0)], 1770.0) == 392.0
    assert replcap.next_x([(196, 1600.0), (392, 1870.0), (784, 1927.0)], 1770.0) == 196 + 170 * 196 / 270


def test_replcap_role_next_tells_a_plateau_from_a_slope():
    import math
    from offbench import replcap
    # one point: Little's law, unless that is a creep down the top quarter
    assert replcap.role_next([(400, 7000.0)], 558.0) == 400 * 558.0 / 7000.0
    assert replcap.role_next([(768, 1250.0)], 1100.0) == 384
    # two points above: a slope is followed, a plateau (an rr SET path at its limit) is halved
    e = math.log(7000.0 / 711.0) / math.log(400 / 32)
    assert replcap.role_next([(32, 711.0), (400, 7000.0)], 558.0) == 32 * (558.0 / 711.0) ** (1 / e)
    assert replcap.role_next([(547, 1250.0), (768, 1251.0)], 890.0) == 547 / 2
    # a measured bracket is next_x's
    assert replcap.role_next([(64, 675.0), (137, 1250.0)], 890.0) == replcap.next_x([(64, 675.0), (137, 1250.0)],
                                                                                    890.0)


def _cap(build, readers, read, state="ok", cores=None, threads=79, rtt=0.05, w=800.0, r=1000.0, shape=(7, 9)):
    from offbench import repl
    inv = {"ok": [], "behind": [f"{repl.LAGGING}: applied 80% of primary writes, lag +9.0 MB/s"],
           "bad": ["primary asserted"],
           "behind_drift": ["primary writes 900k/s, +15.1% off target 782k/s",
                            f"{repl.LAGGING}: applied 90% of primary writes, lag +3.0 MB/s"]}[state]
    return {"kind": "replrw", "schema": 1, "bin": {"name": build}, "rep": 1, "hosts": {"rgen": "x"},
            "cell": {"conns": readers, "pipeline": 16, "io_threads": 4, "pin_share": 0, "write_conns": shape[0],
                     "p_conns": shape[1], "write_pipeline": 16, "p_pipeline": 16},
            "primary": {"write_kcmd_s": w, "read_kcmd_s": r}, "replica": {"read_kcmd_s": read},
            "reader": {"cores": cores, "threads": threads, "latency": {"p99_us": 500}} if readers else None,
            "rtt_ms": {"gen_replica": {"min_ms": rtt}}, "valid": state == "ok", "invalid": inv}


def test_replcap_report_outcomes_and_flags():
    from offbench import replcap
    recs = []
    for n, z in ((0, 0), (50, 5000), (100, 5800), (200, 6400), (400, 6700), (800, 6800)):
        recs += [_cap("custom", n, z, rtt=0.1) for _ in range(3)]
    recs += [_cap("stock", 0, 0)] * 3 + [_cap("stock", 50, 2260)] * 3
    recs += [_cap("stock", n, 2500, "behind") for n in (100, 200) for _ in range(3)]
    recs += [_cap("rr", 50, 6450)] * 3 + [_cap("rr", 100, 6630)] * 3
    recs += [_cap("rr", 200, 6500), _cap("rr", 200, 6600, "behind"), _cap("rr", 200, 6600, "behind")]
    recs += [_cap("rr", 400, 6560)] * 2 + [_cap("rr", 400, 6700, "behind")] + [_cap("rr", 800, 6430)] * 3
    recs += [_cap("wide", 400, 5000)] * 3 + [_cap("wide", 800, 6000)] * 3
    recs += [_cap("gen", 400, 15000, cores=30)] * 3 + [_cap("gen", 800, 22000, cores=76)] * 3
    recs += [_cap("custom", 0, 0, shape=(24, 25))] * 3   # a reference probe in the same file
    # one lagging rep (also off target) puts 200 out of sync, and the in-sync 400 above it does not set the rate
    recs += [_cap("late", 50, 4900)] * 3 + [_cap("late", 100, 5000)] * 3
    recs += [_cap("late", 200, 5500)] * 2 + [_cap("late", 200, 5600, "behind_drift")] + [_cap("late", 400, 7000)] * 3
    recs += [_cap("flaky", 100, 5000)] * 2 + [_cap("flaky", 100, 5000, "bad")] + [_cap("flaky", 200, 5100)] * 3
    out = replcap.report(recs, targets=(782.0, 988.0))
    lines = out.splitlines()
    top = lines.index("| build | X writes/s | Y reads/s | replica reads/s in sync | more offered reads |")
    table = lines[top + 2:lines.index("", top)]
    rows = {line.split(" | ")[0][2:]: line for line in table}
    assert "| 6.80M at 800 readers | in sync at every step; reads level off |" in rows["custom"]
    assert "| at least 2.26M at 50 readers | falls out of sync between 50 and 100 readers |" in rows["stock"]
    assert ("| 6.63M at 100 readers | unstable: out of sync at 200 (2/3 fell behind), 400 (1/3 fell behind), "
            "in sync at 800 |" in rows["rr"])
    assert "| 5.00M at 100 readers | unstable: out of sync at 200 (1/3 fell behind), in sync at 400 |" in rows["late"]
    assert "| at least 6.00M at 800 readers | in sync at every step; reads still rising" in rows["wide"]
    assert "| at least 22.00M at 800 readers | not reached:" in rows["gen"]
    raw = out.splitlines()
    assert any(l.startswith("| custom 7/9 | 0 | 3/3 |") for l in raw)
    assert any(l.startswith("| custom | 50 | 3/3 |") and l.endswith("| RTT |") for l in raw)
    assert any(l.startswith("| custom | 100 | 3/3 |") and l.endswith("|  |") for l in raw)
    assert any(l.startswith("| stock | 100 | 0/3 |") and l.endswith("invalid, fell behind 3/3 |") for l in raw)
    assert any(l.startswith("| rr | 400 | 2/3 | | | | |") and l.endswith("invalid, fell behind 1/3 |") for l in raw)
    assert any(l.startswith("| late | 200 | 2/3 | | | | |") and l.endswith("invalid, fell behind 1/3 |") for l in raw)
    assert any(l.startswith("| flaky 7/9 | 100 | 2/3 | 800k |") and l.endswith("| 1/3 invalid: primary asserted |")
               for l in raw)
    assert any(l.startswith("| gen | 800 |") and "GEN" in l for l in raw)
    assert "3 records with other primary loads" in out
    assert "targets: X 782k writes/s, Y 988k reads/s" in out
    assert [replcap.fmt_rate(k) for k in (999.4, 999.6, 6846.0)] == ["999k", "1.00M", "6.85M"]


def test_sweep_summary_one_lagging_rep_fails_the_point_and_caps_the_ceiling():
    import copy
    from offbench import repl
    base = _rec_with()

    def at(n, read, behind=False):
        r = copy.deepcopy(base)
        r["cell"].update(conns=n, write_conns=64)
        r["replica"]["read_kcmd_s"] = read
        if behind:
            r["valid"], r["invalid"] = False, [f"{repl.LAGGING}: applied 90% of primary writes, lag +3.0 MB/s"]
        return r
    recs = [at(100, 5000.0) for _ in range(3)]
    recs += [at(200, 5500.0), at(200, 5500.0), at(200, 5600.0, behind=True)]
    recs += [at(400, 7000.0) for _ in range(3)]
    out = repl.sweep_summary(recs)
    assert "2/3  INVALID: replica fell behind in 1/3 reps" in out
    assert out.splitlines()[-1].split()[-2:] == ["5000", "100"]


def test_sweep_summary_needs_a_majority_and_keeps_primary_loads_apart():
    from offbench import repl
    recs = [_rec_with() for _ in range(3)]
    recs[1] = _rec_with(targets={"write_kcmd_s": 3000.0, "read_kcmd_s": 1000.0})
    recs[2] = _rec_with(targets={"write_kcmd_s": 3000.0, "read_kcmd_s": 1000.0})
    out = repl.sweep_summary(recs)
    assert "1/3  INVALID: primary writes 2000k/s" in out
    other = _rec_with()
    other["cell"] = dict(other["cell"], write_conns=99)
    out = repl.sweep_summary([recs[0], other])
    assert out.count("1/1") == 2


def test_deploy_source_only_rsyncs_and_nothing_else():
    host = hosts.Host(name="metal-2", ssh="ec2-user@10.0.0.2")
    calls = []

    def run(cmd):
        calls.append(cmd)
        return _subprocess.CompletedProcess(cmd, 0, "", "")
    res = _deploy.sync_source(host, _deploy.SshConfig(key="id.pem", known_hosts="kh"), _OFFBENCH_ROOT, runner=run)
    assert res.ok and [c[0] for c in calls] == ["rsync"]
    assert calls[0][-1] == "ec2-user@10.0.0.2:~/offbench-src"
