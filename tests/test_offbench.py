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
