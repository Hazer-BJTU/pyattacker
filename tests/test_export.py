"""Export, merge and the shard-aware CLI: rows out of a store, de-duplicated across stores.

Coverage
* ``ROW_KINDS`` / ``FORMATS`` and ``iter_rows`` for every kind on a tiny finished run
  (exact row counts plus concrete field values; JSON artifact payloads come back decoded)
* ``run_id=`` / ``limit=`` filtering and the ``flatten`` CSVsafe rules
* ``write_rows`` in ``jsonl`` / ``json`` / ``csv`` (including the ``extra`` column for keys that
  appear only after ``header_rows``) and ``export_store`` / ``export_stores``
* ``merge_reports``: de-duplication by ``pipeline_id``, the winner rule (best state, then latest
  ``finished_at``), recomputed pipeline stats with ``duplicates_folded``, stores *and* paths
* ``pyattacker.cli.main``: ``run --shard i/N`` partitioning one dataset, the JSON summary,
  ``report`` / ``export`` over several shard stores, the missing-store config error, and the
  process-spawning ``run --shards N`` form
"""

from __future__ import annotations

import csv
import json
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from pyattacker import (
    ConfigError,
    RetryableError,
    Retrying,
    Runner,
    SqliteStore,
    load_spec,
    pipeline,
    task,
)
from pyattacker.cli import main
from pyattacker.export import (
    FORMATS,
    ROW_KINDS,
    export_store,
    export_stores,
    flatten,
    iter_rows,
    write_rows,
)
from pyattacker.merge import merge_reports
from pyattacker.shard import shard_specs
from pyattacker.tasks import flaky

# 2 pipelines x 2 tasks, all succeeding: the counts below are exact for that fixture.
EXPECTED_COUNTS = {"pipelines": 2, "tasks": 4, "attempts": 4, "events": 7, "artifacts": 6}


@task("export.inc")
def inc(value, ctx):
    return {"n": value["n"] + 1}


TWO_STEP = pipeline("export-two-step", inc | inc)
RETRY_TEMPLATE = pipeline("export-retry", flaky(1))

FAILING = {"on": True}


@task("export.maybe", retry=Retrying(max_attempts=1))
def maybe_fail(value, ctx):
    if FAILING["on"]:
        raise RetryableError("upstream unavailable", error_class="upstream")
    return {"n": value["n"]}


MAYBE_FAIL = pipeline("export-maybe", maybe_fail)


# ------------------------------------------------------------------------ helpers
def _run(db: Path, template, seeds) -> object:
    """Run one template into a fresh file store and return its report."""
    runner = Runner(store=str(db), concurrency=2, handle_signals=False)
    try:
        return runner.run(template.map(seeds))
    finally:
        runner.close()


def _open(db: Path | str) -> SqliteStore:
    return SqliteStore(str(db), read_only=True)


@pytest.fixture()
def two_step(tmp_path):
    """A small finished run: seeds n=0,1 through ``inc | inc``, so every state is `succeeded`."""
    db = tmp_path / "two_step.db"
    report = _run(db, TWO_STEP, [{"n": 0}, {"n": 1}])
    store = _open(db)
    try:
        yield SimpleNamespace(store=store, report=report, db=db, path=str(db))
    finally:
        store.close()


# --------------------------------------------------------------------- row kinds
def test_row_kinds_and_formats_are_the_documented_sets():
    assert ROW_KINDS == ("pipelines", "tasks", "attempts", "events", "artifacts")
    assert FORMATS == ("jsonl", "json", "csv")


def test_iter_rows_pipelines_nest_tasks_and_decode_artifacts(two_step):
    rows = list(iter_rows(two_step.store))

    assert len(rows) == EXPECTED_COUNTS["pipelines"]
    assert {row["name"] for row in rows} == {"export-two-step"}
    assert {row["state"] for row in rows} == {"succeeded"}
    assert {row["run_id"] for row in rows} == {two_step.report.run_id}
    assert all(len(row["pipeline_id"]) == 32 for row in rows)
    assert all(row["n_tasks_total"] == 2 and row["n_tasks_done"] == 2 for row in rows)

    assert all(len(row["tasks"]) == 2 for row in rows)
    assert all([item["state"] for item in row["tasks"]] == ["succeeded", "succeeded"] for row in rows)

    # artifacts: the seed (seq -1) plus one per task, and only the last one is final
    assert all([art["seq"] for art in row["artifacts"]] == [-1, 0, 1] for row in rows)
    assert all([art["is_final"] for art in row["artifacts"]] == [False, False, True] for row in rows)
    assert all(art["codec"] == "json" and art["type"] == "dict" for row in rows for art in row["artifacts"])
    assert sorted(art["payload"]["n"] for row in rows for art in row["artifacts"]) == [0, 1, 1, 2, 2, 3]


def test_iter_rows_tasks_carry_name_state_and_attempts(two_step):
    rows = list(iter_rows(two_step.store, kind="tasks"))

    assert len(rows) == EXPECTED_COUNTS["tasks"]
    assert {row["name"] for row in rows} == {"export.inc"}
    assert {row["state"] for row in rows} == {"succeeded"}
    assert {row["attempts_used"] for row in rows} == {1}
    assert {row["seq"] for row in rows} == {0, 1}
    # the first task consumes the seed artifact (seq -1); the second consumes the first's output
    assert sorted((row["seq"], row["input_artifact_id"].endswith(":-1")) for row in rows) == [
        (0, True),
        (0, True),
        (1, False),
        (1, False),
    ]
    assert all(row["error_type"] is None and row["metrics"] == {} for row in rows)
    assert all(row["duration_ms"] >= 0.0 for row in rows)


def test_iter_rows_attempts_carry_a_reasoned_decision(two_step):
    rows = list(iter_rows(two_step.store, kind="attempts"))

    assert len(rows) == EXPECTED_COUNTS["attempts"]
    assert {row["task_name"] for row in rows} == {"export.inc"}
    assert {row["outcome"] for row in rows} == {"succeeded"}
    assert {row["attempt_no"] for row in rows} == {1}
    assert {row["decision"]["reason"] for row in rows} == {"ok"}
    assert {row["decision"]["retry"] for row in rows} == {False}


def test_iter_rows_attempts_record_retry_decisions(tmp_path):
    db = tmp_path / "retry.db"
    _run(db, RETRY_TEMPLATE, [{"n": 0}, {"n": 1}])
    store = _open(db)
    try:
        rows = list(iter_rows(store, kind="attempts"))
    finally:
        store.close()

    assert len(rows) == 4  # 2 pipelines x (1 failed attempt + 1 succeeding retry)
    assert sorted((row["outcome"], row["decision"]["reason"]) for row in rows) == [
        ("failed", "retryable"),
        ("failed", "retryable"),
        ("succeeded", "ok"),
        ("succeeded", "ok"),
    ]

    failed = [row for row in rows if row["outcome"] == "failed"]
    assert {row["attempt_no"] for row in failed} == {1}
    assert {row["error_class"] for row in failed} == {"retryable"}
    assert {row["decision"]["error_class"] for row in failed} == {"retryable"}
    assert all(row["decision"]["retry"] is True for row in failed)
    assert all(row["decision"]["delay_s"] >= 0.0 for row in failed)
    assert all(row["retry_delay_s"] >= 0.0 for row in failed)

    succeeded = [row for row in rows if row["outcome"] == "succeeded"]
    assert {row["attempt_no"] for row in succeeded} == {2}
    assert all(row["decision"]["retry"] is False for row in succeeded)


def test_iter_rows_events_carry_scope_and_kind(two_step):
    rows = list(iter_rows(two_step.store, kind="events"))

    assert len(rows) == EXPECTED_COUNTS["events"]
    assert sorted(row["kind"] for row in rows) == [
        "pipeline.succeeded",
        "pipeline.succeeded",
        "run.finished",
        "task.succeeded",
        "task.succeeded",
        "task.succeeded",
        "task.succeeded",
    ]
    assert {row["scope"] for row in rows} == {"pipeline", "run"}

    run_rows = [row for row in rows if row["scope"] == "run"]
    assert len(run_rows) == 1
    assert run_rows[0]["kind"] == "run.finished"
    assert run_rows[0]["run_id"] == two_step.report.run_id

    task_rows = [row for row in rows if row["kind"] == "task.succeeded"]
    assert {row["data"]["task"] for row in task_rows} == {"export.inc"}
    assert {row["data"]["seq"] for row in task_rows} == {0, 1}
    assert {row["data"]["attempt"] for row in task_rows} == {1}

    pipeline_rows = [row for row in rows if row["kind"] == "pipeline.succeeded"]
    assert {row["data"]["tasks"] for row in pipeline_rows} == {2}
    assert all(row["pipeline_id"] and row["task_run_id"] is None for row in pipeline_rows)


def test_iter_rows_artifacts_decode_json_payloads(two_step):
    rows = list(iter_rows(two_step.store, kind="artifacts"))

    assert len(rows) == EXPECTED_COUNTS["artifacts"]
    assert {row["type_name"] for row in rows} == {"dict"}
    assert {row["codec"] for row in rows} == {"json"}
    assert {row["size"] for row in rows} == {7}  # every payload is b'{"n": X}'
    assert all(len(row["digest"]) == 32 for row in rows)
    assert all(row["available"] is True for row in rows)
    # payload was decoded from JSON, not base64-encoded
    assert sorted(row["payload"]["n"] for row in rows) == [0, 1, 1, 2, 2, 3]
    assert {row["task_name"] for row in rows if row["task_name"] == "__seed__"} == {"__seed__"}
    assert len([row for row in rows if row["task_name"] == "__seed__"]) == 2
    assert {row["seq"] for row in rows if row["is_final"]} == {1}


def test_iter_rows_filter_by_run_id(two_step):
    run_id = two_step.report.run_id
    for kind in ROW_KINDS:
        assert len(list(iter_rows(two_step.store, kind=kind, run_id=run_id))) == EXPECTED_COUNTS[kind]
        assert list(iter_rows(two_step.store, kind=kind, run_id="run-00000000-000000")) == []


def test_iter_rows_limit_truncates(two_step):
    assert len(list(iter_rows(two_step.store, kind="tasks", limit=3))) == 3
    assert len(list(iter_rows(two_step.store, kind="attempts", limit=1))) == 1
    assert len(list(iter_rows(two_step.store, kind="events", limit=2))) == 2
    assert len(list(iter_rows(two_step.store, kind="tasks", limit=100))) == EXPECTED_COUNTS["tasks"]


# ------------------------------------------------------------------------ flatten
def test_flatten_makes_values_csv_safe():
    assert flatten(None) == ""
    assert flatten(True) == "true"
    assert flatten(False) == "false"
    assert flatten(3) == 3
    assert flatten(0) == 0
    assert flatten(2.5) == 2.5
    assert flatten("text") == "text"
    assert flatten({"a": 1, "b": [1, 2]}) == '{"a": 1, "b": [1, 2]}'
    assert flatten(["x", None, {"y": True}]) == '["x", null, {"y": true}]'


# --------------------------------------------------------------------- write_rows
def test_write_rows_jsonl_writes_one_object_per_line(tmp_path):
    rows = [{"a": 1, "b": {"x": [1, 2]}}, {"a": 2, "b": None, "u": "héllo"}]
    out = tmp_path / "rows.jsonl"

    assert write_rows(rows, out, fmt="jsonl") == 2
    text = out.read_text(encoding="utf-8")
    assert text.endswith("\n")
    lines = text.splitlines()
    assert len(lines) == 2
    assert [json.loads(line) for line in lines] == rows
    assert "héllo" in lines[1]  # ensure_ascii=False keeps unicode literal


def test_write_rows_json_wraps_rows_in_one_object(tmp_path):
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    out = tmp_path / "rows.json"

    assert write_rows(rows, out, fmt="json", title="export") == 3
    text = out.read_text(encoding="utf-8")
    assert text.startswith("{") and text.rstrip().endswith("}")
    payload = json.loads(text)
    assert payload["title"] == "export"
    assert isinstance(payload["rows"], list)
    assert payload["rows"] == rows

    untitled = tmp_path / "untitled.json"
    assert write_rows(rows, untitled, fmt="json") == 3
    assert json.loads(untitled.read_text(encoding="utf-8"))["title"] == ""


def test_write_rows_csv_folds_late_keys_into_extra(tmp_path):
    rows = [{"a": 1, "b": {"nested": True}}, {"a": 2, "b": None, "c": "late"}]
    out = tmp_path / "rows.csv"

    # header_rows=1: `c` only shows up in the second row, so it is folded into `extra`
    assert write_rows(rows, out, fmt="csv", header_rows=1) == 2
    with out.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        parsed = list(reader)
        assert reader.fieldnames == ["a", "b", "extra"]
    assert parsed == [
        {"a": "1", "b": '{"nested": true}', "extra": ""},
        {"a": "2", "b": "", "extra": '{"c": "late"}'},
    ]

    # with the (default, wide) header window, `c` earns its own column and `extra` stays empty
    wide = tmp_path / "rows_wide.csv"
    assert write_rows(rows, wide, fmt="csv") == 2
    with wide.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        parsed = list(reader)
        assert reader.fieldnames == ["a", "b", "c", "extra"]
    assert parsed[1]["c"] == "late" and parsed[1]["extra"] == ""
    assert parsed[0]["b"] == '{"nested": true}'


# ------------------------------------------------------------ export_store(s)
def test_export_store_writes_files_and_returns_row_count(two_step, tmp_path):
    out = tmp_path / "nested" / "tasks.jsonl"
    assert export_store(two_step.store, str(out), kind="tasks") == EXPECTED_COUNTS["tasks"]
    assert len(out.read_text(encoding="utf-8").splitlines()) == EXPECTED_COUNTS["tasks"]

    assert export_store(two_step.store, str(tmp_path / "pipes.jsonl")) == EXPECTED_COUNTS["pipelines"]

    empty = tmp_path / "none.jsonl"
    assert export_store(two_step.store, str(empty), run_id="run-00000000-000000") == 0
    assert empty.read_text(encoding="utf-8") == ""


def test_export_stores_concatenates_several_stores(tmp_path):
    db1, db2 = tmp_path / "s1.db", tmp_path / "s2.db"
    seeds1, seeds2 = [{"n": 0}, {"n": 1}], [{"n": 10}, {"n": 11}]
    report1 = _run(db1, TWO_STEP, seeds1)
    report2 = _run(db2, TWO_STEP, seeds2)
    store1, store2 = _open(db1), _open(db2)
    try:
        out = tmp_path / "union.jsonl"
        assert export_stores([store1, store2], str(out)) == 4
        rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 4
        assert {row["pipeline_id"] for row in rows} == {
            spec.pipeline_id for spec in TWO_STEP.map(seeds1 + seeds2)
        }
        assert {row["run_id"] for row in rows} == {report1.run_id, report2.run_id}

        tasks_out = tmp_path / "union_tasks.jsonl"
        assert export_stores([store1, store2], str(tasks_out), kind="tasks") == 8
        assert len(tasks_out.read_text(encoding="utf-8").splitlines()) == 8

        assert export_stores([store1, store2], str(tmp_path / "none.jsonl"), run_id="run-x") == 0
    finally:
        store1.close()
        store2.close()


def test_unknown_kind_and_format_raise_config_error(two_step, tmp_path):
    with pytest.raises(ConfigError, match="unknown row kind 'bogus'"):
        list(iter_rows(two_step.store, kind="bogus"))
    with pytest.raises(ConfigError, match="unknown format 'bogus'"):
        write_rows([], tmp_path / "rows.out", fmt="bogus")
    with pytest.raises(ConfigError, match="unknown row kind 'bogus'"):
        export_store(two_step.store, str(tmp_path / "rows.out"), kind="bogus")
    with pytest.raises(ConfigError, match="unknown format 'bogus'"):
        export_store(two_step.store, str(tmp_path / "rows.out"), fmt="bogus")


# ------------------------------------------------------------------------ merge
def test_merge_reports_deduplicates_pipeline_ids_across_stores(tmp_path):
    db1, db2 = tmp_path / "a.db", tmp_path / "b.db"
    seeds = [{"n": 0}, {"n": 1}]
    report1 = _run(db1, TWO_STEP, seeds)
    time.sleep(0.02)
    report2 = _run(db2, TWO_STEP, seeds)
    store1, store2 = _open(db1), _open(db2)
    try:
        merged = merge_reports([store1, store2])
    finally:
        store1.close()
        store2.close()

    assert len(merged.rows) == 2  # not the 4 rows the two stores hold together
    assert merged.duplicates == 2
    assert merged.sources == [str(db1), str(db2)]
    assert set(merged.run_ids) == {report1.run_id, report2.run_id}
    assert {row["pipeline_id"] for row in merged.rows} == {
        spec.pipeline_id for spec in TWO_STEP.map(seeds)
    }

    stats = merged.stats()
    assert stats["sources"] == 2
    assert stats["pipelines"]["total"] == 2 == len(merged.rows)
    assert stats["pipelines"]["by_state"] == {"succeeded": 2}
    assert stats["duplicates_folded"] == 2
    assert stats["tasks"]["by_name"] == {"export.inc": 4}

    # same state in both stores -> the later finished_at wins
    assert {row["run_id"] for row in merged.rows} == {report2.run_id}


def test_merge_prefers_succeeded_over_failed(tmp_path):
    seeds = [{"n": 0}, {"n": 1}]
    FAILING["on"] = True
    db_failed = tmp_path / "failed.db"
    try:
        report_failed = _run(db_failed, MAYBE_FAIL, seeds)
    finally:
        FAILING["on"] = False
    db_fixed = tmp_path / "fixed.db"
    report_fixed = _run(db_fixed, MAYBE_FAIL, seeds)

    store_failed, store_fixed = _open(db_failed), _open(db_fixed)
    try:
        failed_rows = list(store_failed.export_rows())
        fixed_rows = list(store_fixed.export_rows())
        assert {row["state"] for row in failed_rows} == {"failed"}
        assert {row["state"] for row in fixed_rows} == {"succeeded"}
        # the pipeline key is content-addressed, so both stores hold the same two pipeline ids
        assert {row["pipeline_id"] for row in failed_rows} == {row["pipeline_id"] for row in fixed_rows}
        assert len(store_failed.errors(run_id=report_failed.run_id)) == 2

        merged = merge_reports([store_failed, store_fixed])
        reversed_merged = merge_reports([store_fixed, store_failed])
    finally:
        store_failed.close()
        store_fixed.close()

    for report in (merged, reversed_merged):
        assert len(report.rows) == 2
        assert report.duplicates == 2
        assert {row["state"] for row in report.rows} == {"succeeded"}  # order-independent
        assert {row["run_id"] for row in report.rows} == {report_fixed.run_id}
    assert merged.errors() == []
    assert [row["pipeline_id"] for row in merged.rows] == [
        row["pipeline_id"] for row in reversed_merged.rows
    ]


def test_merge_tie_breaks_on_latest_finished_at(tmp_path):
    seeds = [{"n": 0}, {"n": 1}]
    db_early, db_late = tmp_path / "early.db", tmp_path / "late.db"
    report_early = _run(db_early, TWO_STEP, seeds)
    time.sleep(0.02)  # guarantee a strictly later finished_at on every pipeline
    report_late = _run(db_late, TWO_STEP, seeds)

    store_early, store_late = _open(db_early), _open(db_late)
    try:
        early_rows = {row["pipeline_id"]: row for row in store_early.export_rows()}
        late_rows = {row["pipeline_id"]: row for row in store_late.export_rows()}
        assert all(late_rows[key]["finished_at"] > early_rows[key]["finished_at"] for key in early_rows)
        merged = merge_reports([store_early, store_late])
        reversed_merged = merge_reports([store_late, store_early])
    finally:
        store_early.close()
        store_late.close()

    for report in (merged, reversed_merged):
        assert len(report.rows) == 2
        assert report.duplicates == 2
        assert {row["state"] for row in report.rows} == {"succeeded"}
        assert {row["run_id"] for row in report.rows} == {report_late.run_id}
    assert {row["finished_at"] for row in merged.rows} == {
        row["finished_at"] for row in late_rows.values()
    }
    assert {row["finished_at"] for row in merged.rows} != {
        row["finished_at"] for row in early_rows.values()
    }
    assert report_early.run_id != report_late.run_id


def test_merge_accepts_stores_and_paths_and_recomputes_stats(tmp_path):
    db1, db2 = tmp_path / "p1.db", tmp_path / "p2.db"
    _run(db1, TWO_STEP, [{"n": 0}, {"n": 1}])
    _run(db2, TWO_STEP, [{"n": 20}, {"n": 21}])  # disjoint pipeline ids: nothing to fold

    from_paths = merge_reports([str(db1), str(db2)])
    store1, store2 = _open(db1), _open(db2)
    try:
        from_stores = merge_reports([store1, store2])
        mixed = merge_reports([store1, str(db2)])
    finally:
        store1.close()
        store2.close()

    for report in (from_paths, from_stores, mixed):
        assert len(report.rows) == 4
        assert report.duplicates == 0
        assert report.sources == [str(db1), str(db2)]
        stats = report.stats()
        assert stats["pipelines"]["total"] == 4
        assert stats["pipelines"]["by_state"] == {"succeeded": 4}
        assert stats["tasks"]["by_name"] == {"export.inc": 8}
        assert stats["duplicates_folded"] == 0

    assert [row["pipeline_id"] for row in from_paths.rows] == [
        row["pipeline_id"] for row in from_stores.rows
    ]

    summary = from_paths.summary()
    assert "merged 4 pipelines from 2 store(s)" in summary
    assert "folded" not in summary  # nothing was de-duplicated, so nothing is claimed

    out = tmp_path / "merged.jsonl"
    assert from_paths.export(str(out), fmt="jsonl") == 4
    assert [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()] == from_paths.rows


def test_merged_summary_reports_how_many_rows_were_folded(tmp_path):
    db1, db2 = tmp_path / "d1.db", tmp_path / "d2.db"
    seeds = [{"n": 0}, {"n": 1}]
    time.sleep(0.01)
    _run(db1, TWO_STEP, seeds)
    time.sleep(0.02)
    _run(db2, TWO_STEP, seeds)
    merged = merge_reports([str(db1), str(db2)])

    summary = merged.summary()
    assert "merged 2 pipelines from 2 store(s)  (folded 2 duplicate rows)" in summary
    assert "succeeded=2" in summary
    assert f"source: {db1}" in summary
    assert f"source: {db2}" in summary


# -------------------------------------------------------------------------- CLI
CLI_CONFIG = """
source:
  kind: range
  n: 6
pipeline:
  name: cli-shard
  tasks:
    - use: echo
run:
  concurrency: 2
"""


@pytest.fixture()
def cli_config(tmp_path) -> Path:
    # The CLI section below drives the real entry point with a YAML config, so it needs the optional
    # extra; the store-level tests above it run without it.
    pytest.importorskip("yaml", reason="needs the optional yaml extra")
    path = tmp_path / "shard.yaml"
    path.write_text(textwrap.dedent(CLI_CONFIG), encoding="utf-8")
    return path


@pytest.fixture()
def sharded(cli_config, tmp_path):
    """One dataset, two shards, two stores — written by the real CLI entry point."""
    db0, db1 = str(tmp_path / "shard0.db"), str(tmp_path / "shard1.db")
    rc0 = main(["run", "-c", str(cli_config), "--shard", "0/2", "--store", db0])
    rc1 = main(["run", "-c", str(cli_config), "--shard", "1/2", "--store", db1])
    specs = list(load_spec(cli_config).pipelines())
    return SimpleNamespace(
        cfg=cli_config,
        db0=db0,
        db1=db1,
        rc0=rc0,
        rc1=rc1,
        expected_ids={spec.pipeline_id for spec in specs},
        counts=tuple(len(list(shard_specs(specs, index, 2))) for index in range(2)),
    )


def test_cli_shard_run_partitions_one_dataset(sharded):
    assert (sharded.rc0, sharded.rc1) == (0, 0)
    store0, store1 = _open(sharded.db0), _open(sharded.db1)
    try:
        ids0 = {record.pipeline_id for record in store0.pipelines()}
        ids1 = {record.pipeline_id for record in store1.pipelines()}
    finally:
        store0.close()
        store1.close()

    assert len(ids0) == sharded.counts[0] and len(ids1) == sharded.counts[1]
    assert ids0 and ids1  # both shards own real work for this dataset
    assert not (ids0 & ids1)  # disjoint
    assert ids0 | ids1 == sharded.expected_ids  # together they hold every pipeline
    assert len(ids0 | ids1) == 6


def test_cli_shard_summary_json_is_one_object_with_store_and_shard(cli_config, sharded, tmp_path, capsys):
    db = str(tmp_path / "summary.db")
    capsys.readouterr()  # discard fixture output

    assert main(
        ["run", "-c", str(cli_config), "--shard", "0/2", "--store", db, "--summary-format", "json"]
    ) == 0
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len([line for line in lines if line.startswith("{")]) == 1  # exactly one JSON object
    payload = json.loads(lines[-1])

    store = _open(db)
    try:
        stored = len(store.pipelines())
    finally:
        store.close()

    assert payload["store"] == db
    assert payload["shard"] == "0/2"
    assert payload["status"] == "completed"
    assert payload["pipelines"]["by_state"] == {"succeeded": stored}
    assert payload["pipelines"]["total"] == stored == sharded.counts[0]
    assert sum(payload["pipelines"]["by_state"].values()) == payload["pipelines"]["total"]


def test_cli_report_over_shard_stores_prints_merged_summary(sharded, capsys):
    capsys.readouterr()
    assert main(["report", sharded.db0, sharded.db1]) == 0
    out = capsys.readouterr().out
    assert "merged 6 pipelines from 2 store(s)" in out
    assert "succeeded=6" in out

    capsys.readouterr()
    assert main(["report", sharded.db0, sharded.db1, "--json"]) == 0
    out = capsys.readouterr().out
    assert "merged 6 pipelines from 2 store(s)" in out
    assert '"total": 6' in out


def test_cli_export_merges_stores_into_one_file(sharded, tmp_path, capsys):
    out_path = tmp_path / "merged.jsonl"
    capsys.readouterr()

    assert main(["export", sharded.db0, sharded.db1, str(out_path)]) == 0
    captured = capsys.readouterr().out
    assert "exported 6 merged pipelines from 2 stores" in captured

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 6
    assert {row["pipeline_id"] for row in rows} == sharded.expected_ids
    assert {row["state"] for row in rows} == {"succeeded"}


def test_cli_shard_without_store_exits_two(cli_config, capsys):
    assert main(["run", "-c", str(cli_config), "--shard", "0/2"]) == 2
    err = capsys.readouterr().err
    assert "Config error" in err
    assert "--shard needs a file-backed store" in err


def test_cli_run_shards_spawns_children_and_merges(cli_config, tmp_path, capsys):
    base = tmp_path / "multi.db"
    capsys.readouterr()
    started = time.monotonic()
    try:
        rc = main(["run", "-c", str(cli_config), "--shards", "2", "--jobs", "2", "--store", str(base)])
    except OSError as exc:  # pragma: no cover - only on an environment that cannot spawn a child
        pytest.skip(f"cannot spawn shard children in this environment: {exc}")
    elapsed = time.monotonic() - started
    captured = capsys.readouterr().out

    assert rc == 0
    assert elapsed < 30.0  # bounded: the dataset is 6 pipelines (pytest-timeout is not installed)

    shard0, shard1 = tmp_path / "multi.shard0of2.db", tmp_path / "multi.shard1of2.db"
    assert shard0.exists() and shard1.exists()
    assert "merged 6 pipelines from 2 store(s)" in captured

    store0, store1 = _open(shard0), _open(shard1)
    try:
        ids0 = {record.pipeline_id for record in store0.pipelines()}
        ids1 = {record.pipeline_id for record in store1.pipelines()}
    finally:
        store0.close()
        store1.close()
    assert not (ids0 & ids1)
    assert ids0 | ids1 == {spec.pipeline_id for spec in load_spec(cli_config).pipelines()}


# ---------------------------------------------- --shards + --strict-env (${ENV} preflight)

SHARD_PROVIDED_VAR_CONFIG = """
source:
  kind: range
  n: 6
pipeline:
  name: cli-shard
  tasks:
    - use: echo
run:
  concurrency: 2
  label: "${PYATACKER_SHARD}"
"""

ACTUALLY_MISSING_VAR_CONFIG = """
source:
  kind: range
  n: 6
pipeline:
  name: cli-shard
  tasks:
    - use: echo
run:
  concurrency: 2
  label: "${CLI_SHARD_TEST_DEFINITELY_UNSET}"
"""


def test_cli_run_shards_strict_env_accepts_the_shard_provided_variable(tmp_path, capsys, monkeypatch):
    """${PYATACKER_SHARD} is only ever set inside a shard child (see shard_env()); the parent's
    own --strict-env preflight must not treat it as missing, or every --shards run referencing it
    would fail before a single child started."""
    pytest.importorskip("yaml", reason="needs the optional yaml extra")
    monkeypatch.delenv("PYATACKER_SHARD", raising=False)
    cfg = tmp_path / "shard.yaml"
    cfg.write_text(textwrap.dedent(SHARD_PROVIDED_VAR_CONFIG), encoding="utf-8")
    base = tmp_path / "multi.db"
    capsys.readouterr()

    try:
        rc = main(["run", "-c", str(cfg), "--shards", "2", "--jobs", "2", "--store", str(base), "--strict-env"])
    except OSError as exc:  # pragma: no cover - only on an environment that cannot spawn a child
        pytest.skip(f"cannot spawn shard children in this environment: {exc}")

    assert rc == 0
    assert "Warning: unresolved environment variables" not in capsys.readouterr().err
    assert (tmp_path / "multi.shard0of2.db").exists()
    assert (tmp_path / "multi.shard1of2.db").exists()


def test_cli_run_shards_strict_env_still_rejects_a_genuinely_missing_variable(tmp_path, capsys, monkeypatch):
    pytest.importorskip("yaml", reason="needs the optional yaml extra")
    monkeypatch.delenv("CLI_SHARD_TEST_DEFINITELY_UNSET", raising=False)
    cfg = tmp_path / "shard.yaml"
    cfg.write_text(textwrap.dedent(ACTUALLY_MISSING_VAR_CONFIG), encoding="utf-8")
    base = tmp_path / "multi.db"
    capsys.readouterr()

    rc = main(["run", "-c", str(cfg), "--shards", "2", "--store", str(base), "--strict-env"])

    assert rc == 2
    assert "CLI_SHARD_TEST_DEFINITELY_UNSET" in capsys.readouterr().err
    assert not (tmp_path / "multi.shard0of2.db").exists()  # failed before any child was spawned


def test_child_argv_propagates_strict_env_to_shard_children():
    from pyattacker.cli import _child_argv

    args = SimpleNamespace(config="spec.yaml", limit=None, concurrency=None, journal=None, label=None,
                            stop_after_failures=None, retry_succeeded=False, strict_leases=False,
                            no_write_behind=False, no_signals=False, strict_env=True)
    argv = _child_argv(args, 0, 2, "shard0.db", resume=False)
    assert "--strict-env" in argv
