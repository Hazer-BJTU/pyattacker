"""Suite behavior across shared scheduling, persistent ownership and both layouts."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import shutil
from pathlib import Path

import pytest

from pyattacker import ExperimentSpec, Pool, Resource, SuiteSpec, SuiteStore, pipeline, task
from pyattacker.cli import main
from pyattacker.declarative import load_spec
from pyattacker.errors import ConfigError
from pyattacker.export import ROW_KINDS, export_store, iter_rows
from pyattacker.monitor import read_snapshot
from pyattacker.server import StatsServer
from pyattacker.store import SqliteStore
from pyattacker.tasks import echo


@pytest.fixture(params=["combined", "by_experiment"])
def layout(request):
    return request.param


def make_suite(tmp_path, layout, *, experiments=None, pools=None):
    template = pipeline("same", echo)
    if experiments is None:
        experiments = [
            ExperimentSpec(
                eid, lambda: template.map([{"id": 1}, {"id": 2}], key_of=lambda x: str(x["id"])), "v1"
            )
            for eid in ("a", "b")
        ]
    return SuiteSpec("comparison", experiments, str(tmp_path / "out"), layout, pools=pools or [])


def run(suite, *, selected=None, resume=False, **kwargs):
    with suite.runner(handle_signals=False, **kwargs) as runner:
        return runner.run(suite.pipelines(runner.store, experiments=selected), resume=resume)


def test_identity_resume_cumulative_and_all_exports(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    first = run(suite)
    second = run(suite, selected=["a"], resume=True)
    assert second.skipped == 2
    assert second.stats["attempts_total"] == 0
    store = SuiteStore(suite.output_root, read_only=True)
    try:
        rows = list(store.export_rows())
        assert len(rows) == len({r["pipeline_id"] for r in rows}) == 4
        assert {r["local_key"] for r in rows} == {"1", "2"}
        assert all(r["key"] == r["pipeline_id"] for r in rows)
        assert read_snapshot(store)["pipelines"]["total"] == 4
        assert store.stats(second.run_id)["pipelines"]["total"] == 2
        assert len(store.experiments(first.run_id)) == 2
        assert len(store.experiments(second.run_id)) == 1
        for kind in ROW_KINDS:
            exported = list(iter_rows(store, kind=kind))
            assert exported
            for row in exported:
                if row.get("pipeline_id"):
                    assert row["suite_id"] == suite.id
                    assert row["experiment_id"] in ("a", "b")
    finally:
        store.close()
    selected = SuiteStore(suite.output_root, read_only=True, experiment="a")
    try:
        for kind in ROW_KINDS:
            assert all(r["experiment_id"] == "a" for r in iter_rows(selected, kind=kind))
    finally:
        selected.close()


def test_shared_actual_pool_local_names_and_global_concurrency(tmp_path, layout):
    active = maximum = 0
    seen = []

    @task("use", resource="apis")
    async def use(seed, ctx):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            # Explicit name lookup as well as the default declaration must resolve
            # against the member's aliases, never Runner's global dictionary.
            async with ctx.acquire("apis") as lease:
                seen.append((ctx.experiment_id, lease.pool, ctx.local_key))
                await asyncio.sleep(0.002)
                assert Path(ctx.output_dir).is_dir()
                ctx.report_metric("phase", "ok", display="text")
                return seed
        finally:
            active -= 1

    pool = Pool("shared", [Resource.create("api", id="one", capacity=2)])
    isolated = Pool("private", [Resource.create("api", id="two", capacity=1)])
    tpl = pipeline("same", use)
    exps = [
        ExperimentSpec(eid, lambda: tpl.map(range(5)), "v1", {"apis": target})
        for eid, target in [("a", "shared"), ("b", "shared"), ("c", "private")]
    ]
    suite = make_suite(tmp_path, layout, experiments=exps, pools=[pool, isolated])
    report = run(suite, concurrency=3)
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 15}
    assert maximum <= 3
    assert {id(p) for e, p, _ in seen if e in ("a", "b")} == {id(pool)}
    assert {id(p) for e, p, _ in seen if e == "c"} == {id(isolated)}
    assert all(p.stats().active == 0 for p in (pool, isolated))


def test_source_failure_continues_other_member_and_retains_exhaustion(tmp_path, layout):
    tpl = pipeline("same", echo)
    admissions = []

    def broken():
        admissions.append("a")
        yield tpl.bind(1)
        raise ValueError("bad row")

    def good():
        for i in range(3):
            admissions.append("b")
            yield tpl.bind(i)

    suite = make_suite(
        tmp_path, layout, experiments=[ExperimentSpec("a", broken, "v1"), ExperimentSpec("b", good, "v1")]
    )
    report = run(suite, concurrency=1)
    assert admissions[:2] == ["a", "b"]
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 4}
    assert report.stats["source_errors"] == 1
    a, b = report.stats["experiments"]
    assert a["state"] == "failed" and not a["source_exhausted"]
    assert "bad row" in a["source_error"]
    assert b["state"] == "completed" and b["source_exhausted"]


def test_limits_do_not_claim_source_exhaustion(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    with suite.runner(handle_signals=False) as runner:
        report = runner.run(suite.pipelines(runner.store, limit=1))
    assert report.stats["pipelines"]["total"] == 1
    assert all(not e["source_exhausted"] for e in report.stats["experiments"])
    assert all(e["state"] == "incomplete" for e in report.stats["experiments"])
    assert run(suite, resume=True).stats["pipelines"]["total"] == 4


def test_stop_closes_source_before_store_and_resume(tmp_path, layout):
    tpl = pipeline("same", echo)
    suite = make_suite(tmp_path, layout, experiments=[ExperimentSpec("a", lambda: tpl.map(range(100)), "v1")])
    with suite.runner(handle_signals=False, concurrency=1) as runner:
        runner.on_pipeline_finished = lambda *_: runner.stop()
        specs = suite.pipelines(runner.store)
        report = runner.run(specs)
        assert report.status == "interrupted"
        assert not report.stats["experiments"][0]["source_exhausted"]
    specs.close()  # already closed; no late write into a closed catalog
    assert run(suite, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 99, "skipped": 1}


def test_definition_change_rejected_addition_reordering_safe(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    run(suite)
    suite.experiments = list(reversed(suite.experiments))
    suite.experiments[0].label = "renamed"
    assert run(suite).skipped == 4
    template = pipeline("extra", echo)
    suite.experiments.append(ExperimentSpec("c", lambda: template.map([3]), "v1"))
    assert run(suite).skipped == 4
    suite.experiments[0].definition_digest = "changed"
    with pytest.raises(ConfigError, match="definition changed"):
        suite.runner()
    store = SuiteStore(suite.output_root, read_only=True)
    assert store.stats()["pipelines"]["total"] == 5
    store.close()


def test_metrics_and_callbacks_have_experiment_context(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    seen = []
    with suite.runner(handle_signals=False) as runner:

        def finished(runner, record, artifact):
            seen.append(record.experiment_id)
            runner.report_metric(
                "accuracy",
                0.8 if record.experiment_id == "a" else 0.6,
                experiment_id=record.experiment_id,
                display="percent",
            )

        runner.on_pipeline_finished = finished
        report = runner.run(suite.pipelines(runner.store))
    assert sorted(seen) == ["a", "a", "b", "b"]
    assert [e["reported_metrics"][0]["value"] for e in report.stats["experiments"]] == [0.8, 0.6]
    server = StatsServer(suite.output_root)
    assert server.payload("/stats", {})[1]["pipelines"]["total"] == 4
    assert server.payload("/stats", {"experiment": ["a"]})[1]["pipelines"]["total"] == 2
    assert server.payload("/metrics", {"experiment": ["b"]})[1]["rows"][0]["value"] == 0.6
    assert len(server.payload("/experiments", {})[1]["rows"]) == 2


def write_config(tmp_path, layout="combined", *, source=None):
    child = tmp_path / "configs" / "child.json"
    child.parent.mkdir(exist_ok=True)
    child.write_text(
        json.dumps(
            {
                "run": {"store": "ignored.db", "concurrency": 200},
                "pipeline": {"name": "echo", "tasks": [{"use": "echo"}]},
                "source": source or {"kind": "range", "n": 3},
            }
        )
    )
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "suite": {"id": "demo"},
                "run": {"concurrency": 2},
                "output": {"root": "out", "layout": layout},
                "experiments": {eid: {"config": "configs/child.json"} for eid in ("a", "b")},
            }
        )
    )
    return suite_path


def test_config_cli_paths_and_every_output_command(tmp_path, layout, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    Path("rows.jsonl").write_text('{"x": 1}\n{"x": 2}\n')
    config = write_config(tmp_path, layout, source={"kind": "jsonl", "path": "rows.jsonl"})
    assert main(["validate", "-c", str(config)]) == 0
    assert not (tmp_path / "out").exists()
    description = json.loads(capsys.readouterr().out)
    assert description["experiments"][0]["ignored_run_fields"] == ["concurrency", "store"]
    assert main(["run", "-c", str(config), "--no-signals"]) == 0
    assert not Path("ignored.db").exists()
    out = str(tmp_path / "out")
    assert main(["resume", "-c", str(config), "--no-signals", "--experiment", "b"]) == 0
    assert main(["report", out, "--experiment", "a", "--json"]) == 0
    assert main(["watch", out, "--iterations", "1", "--no-clear"]) == 0
    result = str(tmp_path / "all.jsonl")
    assert main(["export", out, result, "--rows", "results"]) == 0
    assert len(Path(result).read_text().splitlines()) == 4
    assert main(["export", out, str(tmp_path / "split"), "--by-experiment", "--rows", "results"]) == 0
    assert len((tmp_path / "split/a/results.jsonl").read_text().splitlines()) == 2
    assert main(["run", "-c", str(config), "--shards", "2"]) == 2
    assert main(["run", "-c", str(config), "--store", "other.db"]) == 2
    assert main(["run", "-c", str(config), "--experiment", "unknown"]) == 2


@task("pool-task", resource="apis")
async def pool_task(seed, ctx):
    async with ctx.acquire("apis") as lease:
        return lease.resource.id


def test_config_pool_bindings_explicit_and_private(tmp_path):
    config = write_config(tmp_path)
    raw = json.loads(config.read_text())
    raw["pools"] = {"shared": {"resources": [{"id": "shared-api"}]}}
    raw["experiments"]["a"]["pool_bindings"] = {"apis": "shared"}
    config.write_text(json.dumps(raw))
    child = tmp_path / "configs/child.json"
    member = json.loads(child.read_text())
    member["pools"] = {"apis": {"resources": [{"id": "private-api"}]}}
    member["pipeline"]["tasks"] = [{"use": f"{__name__}:pool_task"}]
    child.write_text(json.dumps(member))
    suite = load_spec(config)
    assert suite.experiments[0].pool_aliases == {"apis": "shared"}
    assert suite.experiments[1].pool_aliases == {"apis": "experiment:b:apis"}
    run(suite)
    store = SuiteStore(suite.output_root, read_only=True)
    rows = list(iter_rows(store, kind="results"))
    assert {r["result"] for r in rows if r["experiment_id"] == "a"} == {"shared-api"}
    assert {r["result"] for r in rows if r["experiment_id"] == "b"} == {"private-api"}
    store.close()


def test_bounded_connections_reopen_and_missing_child(tmp_path):
    tpl = pipeline("many", echo)
    suite = make_suite(
        tmp_path,
        "by_experiment",
        experiments=[ExperimentSpec(f"e{i}", lambda: tpl.map(range(2)), "v1") for i in range(12)],
    )
    with suite.runner(handle_signals=False) as runner:
        runner.store.max_open = 2
        report = runner.run(suite.pipelines(runner.store))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 24}
        assert len(runner.store._children) <= 2
    for child in (Path(suite.output_root) / "experiments").glob("*/state.db"):
        store = SqliteStore(str(child), read_only=True)
        assert store.get_run(report.run_id).status == "completed"
        assert all(p.suite_id == suite.id for p in store.pipelines())
        store.close()
    missing = Path(suite.output_root) / "experiments/e0/state.db"
    missing.unlink()
    with pytest.raises(ConfigError, match="missing experiment database"):
        suite.runner()
    assert not missing.exists()


def test_export_failure_keeps_old_file(tmp_path):
    suite = make_suite(tmp_path, "combined")
    run(suite)
    store = SuiteStore(suite.output_root, read_only=True)
    destination = tmp_path / "results.jsonl"
    destination.write_text("previous\n")
    with pytest.raises(ConfigError):
        export_store(store, str(destination), kind="invalid")
    assert destination.read_text() == "previous\n"
    store.close()


@pytest.mark.parametrize("invalid", ["../oops", "a/b", "CON", "a.b", "", "a" * 81])
def test_invalid_ids(invalid):
    with pytest.raises(ConfigError):
        ExperimentSpec(invalid, lambda: (), "v1")


def test_manifest_never_contains_expanded_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("SUITE_TEST_SECRET", "unique-secret-marker")
    config = write_config(tmp_path)
    raw = json.loads(config.read_text())
    raw["pools"] = {"apis": {"resources": [{"options": {"token": "${SUITE_TEST_SECRET}"}}]}}
    raw["experiments"]["a"]["pool_bindings"] = {"apis": "apis"}
    config.write_text(json.dumps(raw))
    run(load_spec(config))
    assert "unique-secret-marker" not in (tmp_path / "out/manifest.json").read_text()


def test_resume_preserves_completed_task_checkpoint(tmp_path, layout):
    prepared = []
    fail = {"enabled": True}

    @task("prepare")
    def prepare(seed, ctx):
        prepared.append((ctx.experiment_id, seed))
        return seed + 10

    @task("finish")
    def finish(seed):
        if fail["enabled"]:
            raise ValueError("temporary")
        return seed

    tpl = pipeline("checkpoint", prepare, finish)
    suite = make_suite(
        tmp_path, layout, experiments=[ExperimentSpec(eid, lambda: tpl.map([1]), "v1") for eid in ("a", "b")]
    )
    first = run(suite)
    assert first.stats["pipelines"]["by_state"] == {"failed": 2}
    fail["enabled"] = False
    second = run(suite, resume=True)
    assert second.stats["pipelines"]["by_state"] == {"succeeded": 2}
    assert sorted(prepared) == [("a", 1), ("b", 1)]


def test_control_transitions_and_visit_routes(tmp_path, layout):
    from pyattacker import Handoff

    seen = []

    @task("first")
    def first(seed):
        return seed + 1

    @task("decide")
    def decide(seed, ctx):
        seen.append((ctx.experiment_id, ctx.visit))
        return Handoff.rewind("first", seed) if seed < 2 else seed

    tpl = pipeline("visits", first, decide, control={"rewind": {"decide": ["first"]}, "max_handoffs": 4})
    suite = make_suite(
        tmp_path, layout, experiments=[ExperimentSpec(eid, lambda: tpl.map([0]), "v1") for eid in ("a", "b")]
    )
    report = run(suite)
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 2}
    assert report.stats["handoffs_total"] == 2
    assert run(suite, resume=True).skipped == 2


def test_local_file_artifacts_survive_moving_output_root(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    run(suite, artifact_backend="file")
    moved = tmp_path / "moved"
    shutil.move(suite.output_root, moved)
    suite.output_root = str(moved)
    store = SuiteStore(str(moved), read_only=True)
    try:
        assert all(
            row["artifact_available"] and row["result"]["id"] in (1, 2)
            for row in iter_rows(store, kind="results")
        )
    finally:
        store.close()
    assert run(suite, resume=True).skipped == 4


def test_recover_from_stale_top_level_state_without_replaying_success(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    report = run(suite)
    root = Path(suite.output_root)
    catalog = SqliteStore(str(root / ("state.db" if layout == "combined" else "suite.db")))
    catalog._conn.execute("UPDATE suite_experiments SET state='running',source_exhausted=0")
    catalog._conn.execute("UPDATE runs SET status='running',heartbeat_at=0 WHERE run_id=?", (report.run_id,))
    catalog._conn.commit()
    catalog.close()
    assert run(suite, resume=True).skipped == 4


def test_invalid_selection_and_limits_are_configuration_errors(tmp_path, capsys):
    config = write_config(tmp_path)
    assert main(["run", "-c", str(config), "--limit", "-1"]) == 2


def test_suite_cannot_be_opened_with_different_identity_or_layout(tmp_path):
    suite = make_suite(tmp_path, "combined")
    run(suite)
    with pytest.raises(ConfigError, match="identity or layout"):
        dataclasses.replace(suite, id="different").runner()
    with pytest.raises(ConfigError, match="identity or layout"):
        dataclasses.replace(suite, layout="by_experiment").runner()


def test_single_writer_lock_allows_readers_and_releases_on_close(tmp_path):
    suite = make_suite(tmp_path, "combined")
    with suite.runner(handle_signals=False) as runner:
        with pytest.raises(ConfigError, match="active writer"):
            suite.runner()
        reader = SuiteStore(suite.output_root, read_only=True)
        reader.close()
        runner.run(suite.pipelines(runner.store))
    assert run(suite, resume=True).skipped == 4


def test_process_crash_preserves_child_checkpoint_and_pending_admissions(tmp_path, layout):
    import subprocess
    import sys

    suite = make_suite(tmp_path, layout)
    code = """
import os, sys
from pyattacker import SuiteSpec, ExperimentSpec, pipeline
from pyattacker.tasks import echo
p = pipeline("same", echo)
def source():
    return p.map([{"id":1}, {"id":2}], key_of=lambda x: str(x["id"]))
suite = SuiteSpec("comparison", [ExperimentSpec(e, source, "v1") for e in ("a", "b")], sys.argv[1], sys.argv[2])
with suite.runner(handle_signals=False, concurrency=1, on_pipeline_finished=lambda *args: os._exit(23)) as runner:
    runner.run(suite.pipelines(runner.store))
"""
    child = subprocess.run(
        [sys.executable, "-c", code, suite.output_root, layout], timeout=15, capture_output=True
    )
    assert child.returncode == 23, child.stderr.decode()
    reader = SuiteStore(suite.output_root, read_only=True)
    assert reader.stats()["pipelines"]["by_state"].get("succeeded") == 1
    crashed_run = reader.latest_run_id()
    assert reader.stats(crashed_run)["pipelines"]["by_state"].get("succeeded") == 1
    assert reader.stats(crashed_run)["attempts_total"] == 1
    reader.close()
    report = run(suite, resume=True)
    assert report.skipped == 1
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 3, "skipped": 1}
    reader = SuiteStore(suite.output_root, read_only=True)
    assert reader.stats(crashed_run)["pipelines"]["by_state"].get("succeeded") == 1
    reader.close()


def test_suite_guide_python_example_runs(tmp_path, monkeypatch):
    import re

    guide = Path(__file__).resolve().parents[1] / "docs/suites.md"
    code = re.search(r"```python\n(.*?)```", guide.read_text(), re.S).group(1)
    monkeypatch.chdir(tmp_path)
    example = tmp_path / "suite_guide_example.py"
    example.write_text(code)
    exec(compile(code, str(example), "exec"), {})
    reader = SuiteStore("runs/numbers", read_only=True)
    assert reader.stats()["pipelines"]["by_state"] == {"succeeded": 6}
    reader.close()


def test_merge_preserves_distinct_members_and_deduplicates_copies(tmp_path, layout):
    from pyattacker.merge import merge_reports

    suite = make_suite(tmp_path, layout)
    run(suite)
    reader = SuiteStore(suite.output_root, read_only=True)
    try:
        merged = merge_reports([reader, reader])
        assert len(merged.rows) == 4
        assert merged.duplicates == 4
        assert merged.attempts_total == 4
        assert {r["experiment_id"] for r in merged.rows} == {"a", "b"}
    finally:
        reader.close()


def test_sample_example_configuration(tmp_path):
    original = Path(__file__).resolve().parents[1] / "examples/suites/suite.json"
    suite = load_spec(original)
    suite.output_root = str(tmp_path / "example")
    result = run(suite)
    assert result.stats["pipelines"]["by_state"] == {"succeeded": 12}


def test_failed_rerun_has_no_stale_final_result(tmp_path, layout):
    fail = {"enabled": False}

    @task("maybe")
    def maybe(seed):
        if fail["enabled"]:
            raise ValueError("fail")
        return seed

    tpl = pipeline("maybe", maybe)
    suite = make_suite(tmp_path, layout, experiments=[ExperimentSpec("a", lambda: tpl.map([1]), "v1")])
    run(suite)
    fail["enabled"] = True
    run(suite, retry_succeeded=True)
    reader = SuiteStore(suite.output_root, read_only=True)
    row = next(iter_rows(reader, kind="results"))
    assert row["state"] == "failed" and row["result"] is None
    reader.close()


def test_immediate_cli_resume_after_mid_pipeline_process_crash(tmp_path, layout, monkeypatch):
    import os
    import subprocess
    import sys

    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "suite_crash_tasks", raising=False)
    module = tmp_path / "suite_crash_tasks.py"
    module.write_text("""
import os
from pathlib import Path
from pyattacker import task
@task("prepare")
def prepare(seed):
    with Path(__file__).with_suffix(".count").open("a") as f:
        f.write("prepared\\n")
    return seed
@task("finish")
def finish(seed):
    if os.environ.get("SUITE_CRASH_NOW") == "1":
        os._exit(24)
    return seed
""")
    config = write_config(tmp_path, layout)
    member_file = tmp_path / "configs/child.json"
    member = json.loads(member_file.read_text())
    member["source"] = {"kind": "range", "n": 1}
    member["pipeline"]["tasks"] = [{"use": "suite_crash_tasks:prepare"}, {"use": "suite_crash_tasks:finish"}]
    member_file.write_text(json.dumps(member))
    env = dict(os.environ, SUITE_CRASH_NOW="1", PYTHONPATH=str(tmp_path))
    child = subprocess.run(
        [sys.executable, "-m", "pyattacker", "run", "-c", str(config), "--concurrency", "1", "--no-signals"],
        env=env,
        timeout=15,
        capture_output=True,
    )
    assert child.returncode == 24, child.stderr.decode()
    assert main(["run", "-c", str(config), "--resume", "--no-signals"]) == 0
    assert module.with_suffix(".count").read_text().splitlines() == ["prepared", "prepared"]


def test_selective_resume_does_not_change_unselected_running_member(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    run(suite)
    store = SuiteStore(suite.output_root)
    b = next(row for row in store.pipelines() if row.experiment_id == "b")
    store.upsert_pipeline(dataclasses.replace(b, state="running"))
    store.close()
    run(suite, selected=["a"], resume=True)
    reader = SuiteStore(suite.output_root, read_only=True)
    assert reader.get_pipeline(b.pipeline_id).state == "running"
    reader.close()


def test_custom_store_rejected_in_both_layouts(tmp_path, layout):
    from pyattacker import MemoryStore

    suite = make_suite(tmp_path, layout)
    with pytest.raises(ConfigError, match="SuiteStore"):
        suite.pipelines(MemoryStore())


def test_missing_member_config_is_a_configuration_error(tmp_path):
    config = write_config(tmp_path)
    (tmp_path / "configs/child.json").unlink()
    assert main(["validate", "-c", str(config)]) == 2


def test_run_outcomes_survive_resume_and_skips(tmp_path, layout):
    failing = {"yes": True}

    @task("flaky")
    def flaky(seed):
        if failing["yes"]:
            raise ValueError("first invocation failure")
        return seed + 1

    tpl = pipeline("history", flaky)
    suite = make_suite(tmp_path, layout, experiments=[ExperimentSpec("a", lambda: tpl.map([1]), "v1")])
    first = run(suite)
    reader = SuiteStore(suite.output_root, read_only=True)
    before = list(iter_rows(reader, run_id=first.run_id))
    before_stats = reader.stats(first.run_id)
    first_tasks = list(iter_rows(reader, kind="tasks", run_id=first.run_id))
    reader.close()
    assert before[0]["state"] == "failed"
    # Deliberately age cumulative timestamps: resumed execution latency must use
    # this invocation's own start, not the original checkpoint's start.
    store = SuiteStore(suite.output_root)
    record = store.pipelines()[0]
    record.started_at -= 86400
    store.upsert_pipeline(record)
    store.close()
    failing["yes"] = False
    second = run(suite, resume=True)
    third = run(suite, resume=True)
    reader = SuiteStore(suite.output_root, read_only=True)
    try:
        old = list(iter_rows(reader, run_id=first.run_id))
        assert old[0]["state"] == "failed"
        for key in ("run_id", "error_message", "started_at", "finished_at", "duration_ms", "attempts_total"):
            assert old[0][key] == before[0][key]
        assert reader.stats(first.run_id) == before_stats
        assert list(iter_rows(reader, kind="tasks", run_id=first.run_id)) == first_tasks
        assert next(iter_rows(reader, kind="results", run_id=first.run_id))["result"] is None
        assert reader.stats(second.run_id)["pipelines"]["duration_ms"]["max"] < 10000
        assert reader.pipelines(run_id=second.run_id)[0].attempts_total == 1
        skipped = reader.pipelines(run_id=third.run_id)[0]
        assert skipped.state == "skipped" and skipped.started_at is None and skipped.attempts_total == 0
        assert reader.stats(third.run_id)["attempts_total"] == 0
        assert reader.pipelines()[0].state == "succeeded"
    finally:
        reader.close()


def test_selective_run_leaves_other_database_bytes_unchanged(tmp_path):
    suite = make_suite(tmp_path, "by_experiment")
    run(suite)
    path = Path(suite.output_root) / "experiments/b/state.db"
    before = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns
    with suite.runner(handle_signals=False) as runner:
        runner.store.max_open = 1
        report = runner.run(suite.pipelines(runner.store, experiments=["a"]), resume=True)
        runner.store.stats()  # full reads must not register an unselected child
        runner.store.heartbeat(report.run_id)
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == before_mtime
    child = SqliteStore(str(path), read_only=True)
    assert child.get_run(report.run_id) is None
    child.close()


@pytest.mark.parametrize("shared", [False, True])
def test_pool_credentials_can_rotate_without_changing_identity(tmp_path, monkeypatch, shared):
    config = write_config(tmp_path)
    child = tmp_path / "configs/child.json"
    target = config if shared else child
    raw = json.loads(target.read_text())
    raw["pools"] = {"apis": {"resources": [{"id": "api", "options": {"api_key": "${ROTATING_TOKEN}"}}]}}
    if shared:
        raw["experiments"]["a"]["pool_bindings"] = {"apis": "apis"}
    target.write_text(json.dumps(raw))
    monkeypatch.setenv("ROTATING_TOKEN", "first-secret")
    first = load_spec(config)
    run(first)
    monkeypatch.setenv("ROTATING_TOKEN", "second-secret")
    second = load_spec(config)
    assert [e.definition_digest for e in first.experiments] == [
        e.definition_digest for e in second.experiments
    ]
    assert run(second, resume=True).skipped == 6
    raw["pools"]["apis"]["resources"][0]["id"] = "different-resource"
    target.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match="definition changed"):
        run(load_spec(config), resume=True)


def test_backend_override_cannot_change_persisted_interpretation(tmp_path, layout):
    suite = make_suite(tmp_path, layout)
    run(suite, artifact_backend="file")
    manifest = (Path(suite.output_root) / "manifest.json").read_bytes()
    for read_only in (False, True):
        with pytest.raises(ConfigError, match="backend conflicts"):
            SuiteStore(suite.output_root, backend="inline", read_only=read_only)
    with pytest.raises(ConfigError, match="backend conflicts"):
        run(suite, resume=True, artifact_backend="inline")
    assert (Path(suite.output_root) / "manifest.json").read_bytes() == manifest
    assert run(suite, resume=True, artifact_backend="file").skipped == 4
    reader = SuiteStore(suite.output_root, read_only=True)
    assert all(r["result"] for r in iter_rows(reader, kind="results"))
    reader.close()


def test_split_export_names_follow_row_kind(tmp_path):
    suite = make_suite(tmp_path, "combined")
    run(suite)
    destination = tmp_path / "exports"
    assert main(["export", suite.output_root, str(destination), "--by-experiment", "--rows", "attempts"]) == 0
    assert len((destination / "a/attempts.jsonl").read_text().splitlines()) == 2
    assert not (destination / "a/results.jsonl").exists()


def test_admitted_spec_repairs_repeat_for_migrated_success(tmp_path):
    from pyattacker import Runner

    tpl = pipeline("repeat", echo)
    specs = list(tpl.map([1], repeats=2))
    path = str(tmp_path / "old.db")
    with Runner(store=path, handle_signals=False) as runner:
        runner.run(specs)
    store = SqliteStore(path)
    store._conn.execute("ALTER TABLE pipelines DROP COLUMN repeat")
    store._conn.commit()
    store.close()
    # Migration cannot infer unknown repeats. The next admitted spec can.
    store = SqliteStore(path)
    assert all(row.repeat == 0 for row in store.pipelines())
    store.close()
    with Runner(store=path, handle_signals=False) as runner:
        assert runner.run(specs, resume=True).skipped == 2
    store = SqliteStore(path, read_only=True)
    assert sorted(row.repeat for row in store.pipelines()) == [0, 1]
    store.close()


def test_outcome_updates_rollback_with_checkpoint_transaction(tmp_path, layout):
    from pyattacker.store.base import RunRecord

    suite = make_suite(tmp_path, layout)
    store = SuiteStore.create(suite, write_behind=False)
    stream = suite.pipelines(store, experiments=["a"])
    stream.prepare_run(store)
    store.start_run(RunRecord("atomic-test"))
    try:
        spec = next(stream)
        store.admit(spec, "atomic-test")
        child = store._child("a")
        record = store.get_pipeline(spec.pipeline_id)
        with pytest.raises(RuntimeError, match="rollback"), child._visit_atomic(spec.pipeline_id):
            child._write_pipeline(dataclasses.replace(record, state="running", attempts_total=1))
            raise RuntimeError("rollback")
        assert store.get_pipeline(spec.pipeline_id).state == "pending"
        outcome = store.pipelines(run_id="atomic-test")[0]
        assert outcome.state == "pending" and outcome.attempts_total == 0
        store.upsert_pipeline(dataclasses.replace(record, state="running", attempts_total=1))
        store.finish_pipeline(spec.pipeline_id, "failed", error=ValueError("durable"))
        reader = SuiteStore(suite.output_root, read_only=True)
        outcome = reader.pipelines(run_id="atomic-test")[0]
        assert outcome.state == "failed" and outcome.error_message == "durable"
        assert outcome.attempts_total == 1
        reader.close()
    finally:
        stream.close()
        store.close()
