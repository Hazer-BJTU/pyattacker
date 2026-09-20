"""Export, merge and the shard-aware CLI: rows out of a store, de-duplicated across stores.

Coverage
* ``ROW_KINDS`` / ``FORMATS`` and ``iter_rows`` for every kind on a tiny finished run
  (exact row counts plus concrete field values; JSON artifact payloads come back decoded)
* ``run_id=`` / ``limit=`` filtering — one limit rule per kind, complete exports by default —
  and the ``flatten`` CSVsafe rules
* streaming: a 100_001-event store exports first to last with nothing duplicated, and the SQL
  trace proves the store reads it in bounded batches instead of one ``fetchall``
* the compatibility rule for stores: the paged extension is preferred, the list API is the fallback
* ``write_rows`` in ``jsonl`` / ``json`` / ``csv`` (including the ``extra`` column for keys that
  appear only after ``header_rows``) and ``export_store`` / ``export_stores``
* ``merge_reports``: de-duplication by ``pipeline_id``, the winner rule (best state, then latest
  ``finished_at``), recomputed pipeline stats with ``duplicates_folded``, stores *and* paths, and the
  counter scopes of issue #59 — ``attempts_total``/``handoffs_total`` recomputed from the surviving rows
  (so a store given twice, or one pipeline in two shards, cannot inflate them) next to the deliberately
  raw ``source_events_total``
* ``pyattacker.cli.main``: ``run --shard i/N`` partitioning one dataset, the JSON summary,
  ``report`` / ``export`` over several shard stores, the missing-store config error, and the
  process-spawning ``run --shards N`` form
"""

from __future__ import annotations

import csv
import gc
import json
import textwrap
import time
import tracemalloc
from math import ceil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pyattacker import (
    Artifact,
    AttemptRecord,
    ConfigError,
    EventRecord,
    MemoryStore,
    PipelineRecord,
    RetryableError,
    Retrying,
    Runner,
    SqliteStore,
    load_spec,
    open_store,
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
from pyattacker.store import ITER_BATCH_SIZE, PagedStore, Store, TaskRecord
from pyattacker.store.writebehind import WriteBehindStore
from pyattacker.tasks import flaky

# 2 pipelines x 2 tasks, all succeeding: the counts below are exact for that fixture.
EXPECTED_COUNTS = {"pipelines": 2, "tasks": 4, "attempts": 4, "events": 7, "artifacts": 6, "results": 2}


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
    assert ROW_KINDS == ("pipelines", "tasks", "attempts", "events", "artifacts", "results")
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


@pytest.mark.parametrize("kind", ROW_KINDS)
def test_iter_rows_limit_means_the_same_thing_for_every_kind(two_step, kind):
    """One rule per kind: the cap counts exported rows and truncates the documented order.

    ``artifacts`` used to count *pipelines* (so ``limit=1`` returned a whole pipeline's artifacts)
    and ``pipelines`` ignored the limit outright; ``events`` used to take the newest rows.
    """
    everything = list(iter_rows(two_step.store, kind=kind))
    assert everything  # the fixture run has rows of every kind

    assert list(iter_rows(two_step.store, kind=kind, limit=None)) == everything
    assert list(iter_rows(two_step.store, kind=kind, limit=0)) == []
    assert list(iter_rows(two_step.store, kind=kind, limit=1)) == everything[:1]
    assert list(iter_rows(two_step.store, kind=kind, limit=len(everything))) == everything
    assert list(iter_rows(two_step.store, kind=kind, limit=len(everything) + 5)) == everything
    assert (
        list(iter_rows(two_step.store, kind=kind, run_id=two_step.report.run_id, limit=2))
        == everything[:2]
    )


@pytest.mark.parametrize("kind", ROW_KINDS)
def test_iter_rows_rejects_a_negative_limit(two_step, kind):
    with pytest.raises(ConfigError, match="limit must be >= 0"):
        list(iter_rows(two_step.store, kind=kind, limit=-1))


# ------------------------------------------- streaming export (issue #34 / >100k events)
# A store one row past the 100_000-event cap the export used to apply silently. Module-scoped: the
# table is built once and read by both tests below.
BIG_EVENT_COUNT = 100_001


@pytest.fixture(scope="module")
def big_event_store(tmp_path_factory):
    path = tmp_path_factory.mktemp("streaming-export") / "events.db"
    writer = SqliteStore(str(path))
    insert = (
        "INSERT INTO events (ts,scope,kind,run_id,pipeline_id,task_run_id,pool,resource_id,data_json) "
        "VALUES (?,?,?,?,?,?,?,?,?)"
    )
    now = time.time()
    try:
        writer._conn.execute("BEGIN")  # one transaction, or 100k rows cost 100k commits
        for start in range(0, BIG_EVENT_COUNT, 10_000):
            writer._conn.executemany(
                insert,
                [
                    (now + index * 1e-6, "pipeline", f"event.{index}", "run-big", None, None, None,
                     None, "{}")
                    for index in range(start, min(start + 10_000, BIG_EVENT_COUNT))
                ],
            )
        writer._conn.commit()
    finally:
        writer.close()

    store = SqliteStore(str(path), read_only=True)
    try:
        yield store
    finally:
        store.close()


def test_events_export_past_one_hundred_thousand_is_complete(big_event_store):
    """The acceptance criterion: >100000 events, first and last present, nothing duplicated."""
    rows = list(iter_rows(big_event_store, kind="events"))

    assert len(rows) == BIG_EVENT_COUNT  # not the newest 100_000 the hidden default kept
    assert rows[0]["kind"] == "event.0"
    assert rows[-1]["kind"] == f"event.{BIG_EVENT_COUNT - 1}"

    event_ids = [row["event_id"] for row in rows]
    assert len(set(event_ids)) == BIG_EVENT_COUNT  # no duplicates
    assert event_ids == sorted(event_ids)  # oldest first, the documented order

    filtered = list(iter_rows(big_event_store, kind="events", run_id="run-big"))
    assert len(filtered) == BIG_EVENT_COUNT
    assert list(iter_rows(big_event_store, kind="events", run_id="run-other")) == []


def test_events_export_reads_bounded_batches_not_the_whole_table(big_event_store):
    """The batching claim, measured: SQLite's own trace of the statements the export really runs.

    One high-water query plus one bounded page per batch (``ORDER BY event_id LIMIT
    ITER_BATCH_SIZE``, restricted to the mark), and reaching the first row costs exactly one page —
    so the Python-side working set is a batch, not the table.
    """
    statements: list[str] = []
    big_event_store._conn.set_trace_callback(statements.append)

    def selects(*needles: str) -> list[str]:
        return [sql for sql in statements if all(needle in sql for needle in needles)]

    try:
        rows = iter_rows(big_event_store, kind="events")
        first = next(rows)
        after_first_row = selects("FROM events", "ORDER BY event_id")
        consumed = 1 + sum(1 for _ in rows)
        pages = selects("FROM events", "ORDER BY event_id")
        marks = selects("FROM events", "MAX(event_id)")
        statements_seen = len(statements)
    finally:
        big_event_store._conn.set_trace_callback(None)

    mark = big_event_store._conn.execute("SELECT MAX(event_id) FROM events").fetchone()[0]
    assert first["kind"] == "event.0"
    assert consumed == BIG_EVENT_COUNT
    assert len(after_first_row) == 1  # the first row costs one bounded page
    assert marks == ["SELECT MAX(event_id) FROM events"]  # the live-store bound, taken once
    assert pages and all(
        f"ORDER BY event_id LIMIT {ITER_BATCH_SIZE}" in sql and f"event_id<={mark}" in sql
        for sql in pages
    )
    assert len(pages) == ceil(BIG_EVENT_COUNT / ITER_BATCH_SIZE)
    assert statements_seen == len(pages) + 1  # pages plus the single mark query


def _peak_bytes(work: Any) -> int:
    """Peak Python allocations (bytes) while ``work()`` runs — stdlib ``tracemalloc``, no deps."""
    gc.collect()
    tracemalloc.start()
    try:
        work()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def test_events_export_memory_is_a_batch_not_the_table(big_event_store):
    """The acceptance criterion, measured: peak memory does not follow the table's row count.

    The bound is calibrated inside the test against the *same* store read through the list API —
    the shape the export used to have — so it is a ratio, not a machine-specific number. Measured
    here: ~0.9 MiB streaming against ~66 MiB for the 100_001 events.
    """
    materialized = _peak_bytes(lambda: len(big_event_store.events(limit=BIG_EVENT_COUNT)))
    streamed = _peak_bytes(lambda: sum(1 for _ in iter_rows(big_event_store, kind="events")))

    assert streamed * 8 < materialized  # one bounded batch against the whole table


@pytest.fixture()
def tied_store(tmp_path):
    """One pipeline holding ``ITER_BATCH_SIZE + 1`` tasks/artifacts that all share ``(pid, seq)``.

    The schema allows it — ``tasks`` is keyed by ``task_run_id`` and ``artifacts`` by
    ``artifact_id``, and neither ``(pipeline_id, seq)`` nor ``seq`` is unique — and nothing in the
    store API forbids writing such rows. The keyset cursor therefore must not be the non-unique
    prefix, or a page boundary inside the tie silently drops the rest of it.
    """
    store = SqliteStore(str(tmp_path / "ties.db"))
    try:
        store.upsert_pipeline(
            PipelineRecord(pipeline_id="p1", run_id="run-1", name="qa", key="k1", created_at=1000.0)
        )
        for index in range(ITER_BATCH_SIZE + 1):
            store.record_task(
                TaskRecord(
                    task_run_id=f"p1:0:{index:04d}",
                    pipeline_id="p1",
                    run_id="run-1",
                    name="ask",
                    seq=0,
                )
            )
            store.put_artifact(
                Artifact(
                    id=f"p1:0:{index:04d}",
                    pipeline_id="p1",
                    task_name="ask",
                    seq=0,
                    type_name="dict",
                    codec="json",
                    digest=f"d{index:04d}",
                    size=2,
                    payload=b"{}",
                    created_at=1000.0,
                )
            )
        yield store
    finally:
        store.close()


def test_ties_on_the_non_unique_cursor_prefix_do_not_lose_rows(tied_store):
    """The page boundary lands inside a tie: every tied row must still be exported exactly once."""
    tasks = list(iter_rows(tied_store, kind="tasks"))
    assert len(tasks) == ITER_BATCH_SIZE + 1  # the whole tie, not just the first page
    assert len({row["task_run_id"] for row in tasks}) == ITER_BATCH_SIZE + 1
    assert [row["task_run_id"] for row in tasks] == sorted(row["task_run_id"] for row in tasks)

    artifacts = list(iter_rows(tied_store, kind="artifacts"))
    assert len(artifacts) == ITER_BATCH_SIZE + 1
    assert len({row["artifact_id"] for row in artifacts}) == ITER_BATCH_SIZE + 1
    assert [row["artifact_id"] for row in artifacts] == sorted(
        row["artifact_id"] for row in artifacts
    )


def test_events_export_is_bounded_to_the_mark_taken_when_it_starts(tmp_path):
    """A live store: ``event_id`` is monotonic, so the export is bounded by its high-water mark.

    Without the bound the iterator chases a moving tail: the page after the producer's write would
    pick up rows that did not exist when the export began.
    """
    store = SqliteStore(str(tmp_path / "live.db"))
    try:
        for index in range(ITER_BATCH_SIZE):
            store.emit_event(EventRecord(ts=float(index), kind=f"event.{index}", run_id="run-1"))

        rows = iter_rows(store, kind="events")
        first_page = [next(rows) for _ in range(ITER_BATCH_SIZE)]  # exactly one full page
        store.emit_event(EventRecord(ts=1.0, kind="event.late", run_id="run-1"))
        rest = [*rows]

        exported = [*first_page, *rest]
        assert len(exported) == ITER_BATCH_SIZE
        assert "event.late" not in {row["kind"] for row in exported}

        # the mark is per export, not permanent: a later export does see the new event
        later = [row["kind"] for row in iter_rows(store, kind="events")]
        assert len(later) == ITER_BATCH_SIZE + 1 and later[-1] == "event.late"
    finally:
        store.close()


def test_task_export_is_a_best_effort_traversal_of_a_live_store(tmp_path):
    """``tasks`` has no monotonic key, so it is documented as a traversal, not a snapshot.

    A row appended ahead of the cursor is exported; one appended behind it is not. Both directions
    are pinned here so the contract cannot drift silently.
    """
    store = SqliteStore(str(tmp_path / "live.db"))
    try:
        for index in range(ITER_BATCH_SIZE):
            store.record_task(
                TaskRecord(
                    task_run_id=f"p1:{index:04d}",
                    pipeline_id="p1",
                    run_id="run-1",
                    name="ask",
                    seq=index,
                )
            )

        rows = iter_rows(store, kind="tasks")
        first_page = [next(rows) for _ in range(ITER_BATCH_SIZE)]
        store.record_task(
            TaskRecord(
                task_run_id="p1:9999",
                pipeline_id="p1",
                run_id="run-1",
                name="ask",
                seq=ITER_BATCH_SIZE,
            )
        )
        store.record_task(
            TaskRecord(task_run_id="p1:-001", pipeline_id="p1", run_id="run-1", name="ask", seq=-1)
        )
        rest = [*rows]

        assert len(first_page) == ITER_BATCH_SIZE
        assert [row["task_run_id"] for row in rest] == ["p1:9999"]  # ahead of the cursor: exported
        assert "p1:-001" not in {
            row["task_run_id"] for row in [*first_page, *rest]
        }  # behind it: not exported
    finally:
        store.close()


@pytest.fixture()
def paged_store(tmp_path):
    """2500 pipelines that all share one ``created_at`` — a tie across every batch boundary."""
    store = SqliteStore(str(tmp_path / "paged.db"))
    try:
        for index in range(2500):
            pipeline_id = f"p{index:04d}"
            store.upsert_pipeline(
                PipelineRecord(
                    pipeline_id=pipeline_id,
                    run_id="run-1",
                    name="qa",
                    key=f"k{index:04d}",
                    state="succeeded",
                    created_at=1000.0,
                    n_tasks_total=1,
                    n_tasks_done=1,
                )
            )
            store.put_artifact(
                Artifact(
                    id=f"{pipeline_id}:0",
                    pipeline_id=pipeline_id,
                    task_name="ask",
                    seq=0,
                    type_name="dict",
                    codec="json",
                    digest="d",
                    size=2,
                    payload=b"{}",
                    created_at=1000.0,
                    is_final=True,
                )
            )
        yield store
    finally:
        store.close()


def test_pipelines_and_artifacts_exports_page_the_pipeline_table(paged_store):
    """Neither kind may read the whole pipelines table up front, and the tie must not lose a row.

    Instrumented at the SQL layer again: ``artifacts`` used to collect every pipeline id with one
    unbounded SELECT before reading any artifact. The pipeline cursor is already total —
    ``pipeline_id`` is the primary key — so this tie is safe by construction; the non-unique
    ``tasks``/``artifacts`` cursors are covered by ``test_ties_on_the_non_unique_cursor_prefix_...``.
    """
    statements: list[str] = []
    paged_store._conn.set_trace_callback(statements.append)

    def pipeline_selects() -> list[str]:
        return [sql for sql in statements if "FROM pipelines" in sql]

    try:
        artifact_rows = iter_rows(paged_store, kind="artifacts")
        first_artifact = next(artifact_rows)
        after_first_row = pipeline_selects()
        artifacts = [first_artifact, *artifact_rows]
        artifacts_queries = len(pipeline_selects())

        pipeline_rows = list(iter_rows(paged_store, kind="pipelines"))
        total_queries = len(pipeline_selects())
    finally:
        paged_store._conn.set_trace_callback(None)

    assert first_artifact["pipeline_id"] == "p0000"
    assert len(after_first_row) == 1 and "LIMIT" in after_first_row[0]
    assert artifacts_queries == ceil(2500 / ITER_BATCH_SIZE)
    assert total_queries == 2 * ceil(2500 / ITER_BATCH_SIZE)

    assert [row["pipeline_id"] for row in artifacts] == [f"p{i:04d}" for i in range(2500)]
    assert len({row["artifact_id"] for row in artifacts}) == 2500
    assert [row["pipeline_id"] for row in pipeline_rows] == [f"p{i:04d}" for i in range(2500)]


def test_events_export_is_complete_in_every_format_with_filters_and_several_stores(tmp_path):
    """Correctness of the paged path: jsonl/json/csv, ``run_id``, write-behind and two stores."""
    db1, db2 = tmp_path / "e1.db", tmp_path / "e2.db"
    report1 = _run(db1, TWO_STEP, [{"n": 0}, {"n": 1}])
    _run(db2, TWO_STEP, [{"n": 10}, {"n": 11}])

    live = open_store(str(db1))  # auto write-behind: the store the CLI and Runner use
    other = _open(db2)
    try:
        assert isinstance(live, WriteBehindStore)
        expected = list(iter_rows(live, kind="events"))
        assert len(expected) == EXPECTED_COUNTS["events"]

        jsonl = tmp_path / "events.jsonl"
        assert export_store(live, str(jsonl), kind="events", fmt="jsonl") == len(expected)
        assert [
            json.loads(line) for line in jsonl.read_text(encoding="utf-8").splitlines()
        ] == expected

        as_json = tmp_path / "events.json"
        assert export_store(live, str(as_json), kind="events", fmt="json") == len(expected)
        assert json.loads(as_json.read_text(encoding="utf-8"))["rows"] == expected

        as_csv = tmp_path / "events.csv"
        assert export_store(live, str(as_csv), kind="events", fmt="csv") == len(expected)
        with as_csv.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            csv_rows = list(reader)
        assert len(csv_rows) == len(expected)
        assert "kind" in fieldnames
        assert {row["kind"] for row in csv_rows} == {row["kind"] for row in expected}

        both = tmp_path / "both.jsonl"
        assert export_stores([live, other], str(both), kind="events") == 2 * len(expected)

        filtered = tmp_path / "filtered.jsonl"
        assert (
            export_stores([live, other], str(filtered), kind="events", run_id=report1.run_id)
            == len(expected)
        )
        assert export_store(live, str(tmp_path / "none.jsonl"), kind="events", run_id="run-x") == 0
    finally:
        live.close()
        other.close()


def test_export_of_a_write_behind_store_flushes_buffered_facts_first():
    """The write-behind wrapper buffers attempts/events until a batch fills; the export must flush.

    The paged reads are delegated through explicit methods for exactly this reason — a plain
    ``__getattr__`` passthrough would read the inner store and silently drop the last batch.
    """
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=1000, flush_interval=999.0)
    try:
        for index in range(3):
            store.emit_event(EventRecord(ts=float(index), kind=f"event.{index}", run_id="run-1"))
        store.record_attempt(
            AttemptRecord(
                pipeline_id="p1",
                run_id="run-1",
                task_run_id="p1:0",
                task_name="ask",
                seq=0,
                attempt_no=1,
                started_at=0.0,
                outcome="succeeded",
            )
        )
        assert store.pending == 4  # nothing has reached the inner store yet

        assert [row["kind"] for row in iter_rows(store, kind="events")] == [
            "event.0",
            "event.1",
            "event.2",
        ]
        assert [row["outcome"] for row in iter_rows(store, kind="attempts")] == ["succeeded"]
        assert store.pending == 0
    finally:
        store.close()


# ------------------------------------------- third-party store compatibility (issue #34)
class _ListOnlyStore:
    """A third-party store without the paged extension: the required API, and no ``iter_*`` at all.

    A real one defines the ``Store`` members; this proxy forwards them through ``__getattr__`` and
    raises ``AttributeError`` for the optional paged family, which is what the fallback must survive.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.journal = inner.journal

    def __getattr__(self, name: str) -> Any:
        if name.startswith("iter_"):
            raise AttributeError(name)  # the extension this store never implemented
        return getattr(self._inner, name)


class _RecordingStore:
    """A store that *does* implement the extension and records which paged methods were asked for."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.journal = inner.journal
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def iter_pipelines(self, **kwargs: Any) -> Any:
        self.calls.append("iter_pipelines")
        return self._inner.iter_pipelines(**kwargs)

    def iter_tasks(self, **kwargs: Any) -> Any:
        self.calls.append("iter_tasks")
        return self._inner.iter_tasks(**kwargs)

    def iter_attempts(self, **kwargs: Any) -> Any:
        self.calls.append("iter_attempts")
        return self._inner.iter_attempts(**kwargs)

    def iter_events(self, **kwargs: Any) -> Any:
        self.calls.append("iter_events")
        return self._inner.iter_events(**kwargs)

    def iter_artifacts(self, **kwargs: Any) -> Any:
        self.calls.append("iter_artifacts")
        return self._inner.iter_artifacts(**kwargs)


def test_a_list_only_third_party_store_keeps_working(two_step):
    """The documented fallback: a store without the extension is read through its list API."""
    proxy = _ListOnlyStore(two_step.store)
    assert not hasattr(proxy, "iter_events")  # the extension really is absent
    assert not isinstance(proxy, PagedStore)

    for kind in ROW_KINDS:
        expected = list(iter_rows(two_step.store, kind=kind))
        assert list(iter_rows(proxy, kind=kind)) == expected
        assert list(iter_rows(proxy, kind=kind, limit=1)) == expected[:1]

    # ...and the extension stays optional: it is not a member of the required protocol, so a store
    # written against `Store` alone remains a `Store`.
    assert isinstance(two_step.store, Store)
    assert not hasattr(Store, "iter_events")


def test_paged_extension_is_used_when_the_store_has_one(two_step):
    """The other half of the rule: a native paged method wins over the list-API fallback."""
    proxy = _RecordingStore(two_step.store)
    assert isinstance(proxy, PagedStore)

    list(iter_rows(proxy, kind="pipelines"))
    assert proxy.calls == []  # nested rows go through the required `export_rows` (paged inside)

    expected_calls = {
        "tasks": {"iter_tasks"},
        "attempts": {"iter_attempts"},
        "events": {"iter_events"},
        "artifacts": {"iter_pipelines", "iter_artifacts"},
    }
    for kind, expected in expected_calls.items():
        proxy.calls.clear()
        list(iter_rows(proxy, kind=kind))
        assert set(proxy.calls) == expected


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
    # Two full copies of the same two pipelines: the de-duplicated counters describe the two survivors,
    # not the four rows that were read (issue #59).
    assert stats["attempts_total"] == EXPECTED_COUNTS["attempts"] == 4
    assert stats["handoffs_total"] == 0
    assert "attempts=4" in merged.summary()

    # same state in both stores -> the later finished_at wins
    assert {row["run_id"] for row in merged.rows} == {report2.run_id}


def test_merge_reports_counters_survive_the_same_store_twice(tmp_path):
    """Issue #59: folding duplicates must not inflate the de-duplicated counters.

    Passing one store twice — as two paths, or as two objects on one file — has always left
    ``pipelines.total`` alone, because rows are de-duplicated by ``pipeline_id``. The workload counters
    used to be summed per source anyway, so they doubled while the row count did not, contradicting the
    documented promise that merging is idempotent. The event log is the documented exception: an event
    does not hang off a pipeline row, so nothing in the merged rows says which copy owns it. That one is
    reported raw, under a name that says so.
    """
    db = tmp_path / "once.db"
    _run(db, TWO_STEP, [{"n": 0}, {"n": 1}])

    single = merge_reports([str(db)]).stats()
    assert single["pipelines"]["total"] == 2
    assert single["attempts_total"] == EXPECTED_COUNTS["attempts"] == 4
    assert single["source_events_total"] == EXPECTED_COUNTS["events"] == 7

    twice = merge_reports([str(db), str(db)])
    twice_stats = twice.stats()
    assert twice.duplicates == 2
    assert twice_stats["pipelines"]["total"] == single["pipelines"]["total"]
    assert twice_stats["attempts_total"] == single["attempts_total"]
    assert twice_stats["handoffs_total"] == single["handoffs_total"]
    assert twice_stats["duplicates_folded"] == 2
    # Raw by contract, and named `source_*`: it follows the sources, not the surviving pipelines.
    assert twice_stats["source_events_total"] == 2 * single["source_events_total"]
    assert "attempts=4 source_events=14" in twice.summary()

    # A store object and a path to the same file are the same source, and must agree exactly.
    store_a, store_b = _open(db), _open(db)
    try:
        from_stores = merge_reports([store_a, store_b]).stats()
    finally:
        store_a.close()
        store_b.close()
    assert from_stores == twice_stats


def test_merge_reports_recomputes_attempts_when_a_pipeline_lives_in_two_shards(tmp_path):
    """Issue #59, the case sharding actually produces: a shard-count change leaves one pipeline in two
    stores. Summing per source counted the folded copy's attempts as if they were extra work."""
    db1, db2 = tmp_path / "shard0.db", tmp_path / "shard1.db"
    _run(db1, TWO_STEP, [{"n": 0}])  # holds one of the two pipelines
    _run(db2, TWO_STEP, [{"n": 0}, {"n": 1}])  # holds both, so one row is a genuine duplicate

    merged = merge_reports([str(db1), str(db2)])
    assert merged.duplicates == 1
    assert len(merged.rows) == 2
    assert merged.stats()["attempts_total"] == 4  # 2 pipelines x 2 tasks, not 3 x 2
    assert merged.stats()["attempts_total"] == sum(row["attempts_total"] for row in merged.rows)

    # `source_events_total` is the raw sum over the given sources, duplicates included, by definition.
    store1, store2 = _open(db1), _open(db2)
    try:
        raw = store1.stats()["events_total"] + store2.stats()["events_total"]
    finally:
        store1.close()
        store2.close()
    assert merged.stats()["source_events_total"] == raw


def test_merged_handoff_count_is_de_duplicated_and_follows_the_report_scope():
    """Handoffs are read off each surviving pipeline's nested ledger, so two things hold: folding the
    same pipeline twice counts its jumps once, and a ``run_id``-filtered report counts that run's jumps
    — the ledger is nested whole, because it outlives the run that wrote it."""
    row = {
        "pipeline_id": "pipe-1",
        "run_id": "run-2",
        "state": "succeeded",
        "attempts_total": 3,
        "handoffs": [{"run_id": "run-1", "handoff_id": 1}, {"run_id": "run-2", "handoff_id": 2}],
    }

    class _Source:
        """Only the surface ``merge_reports`` reads from a store object."""

        path = "stub.db"

        def stats(self, run_id=None):
            # Deliberately wrong about attempts/handoffs: nothing but the event log may be taken from a
            # source's own aggregate, so a regression that sums these again fails loudly here.
            return {"events_total": 1, "attempts_total": 999, "handoffs_total": 999}

        def export_rows(self, *, run_id=None):
            return [row] if run_id in (None, "run-2") else []

        def events(self, **kwargs):
            return []

    source = _Source()
    assert merge_reports([source]).stats()["handoffs_total"] == 2
    assert merge_reports([source], run_id="run-2").stats()["handoffs_total"] == 1
    assert merge_reports([source, source]).stats()["handoffs_total"] == 2
    assert merge_reports([source]).stats()["attempts_total"] == 3
    assert merge_reports([source]).stats()["source_events_total"] == 1


def test_a_custom_store_without_the_nested_ledger_is_read_through_its_handoffs_capability():
    """The ledger is an optional capability. A store that has ``handoffs()`` but does not nest it in its
    pipeline rows still gets its jumps counted — recomputing from rows must not require a shape only the
    built-ins happen to produce."""
    from pyattacker import HandoffRecord

    row = {"pipeline_id": "pipe-1", "run_id": "run-1", "state": "succeeded", "attempts_total": 2}
    records = [
        HandoffRecord(pipeline_id="pipe-1", run_id="run-1", from_seq=0, from_task="a", to_seq=2,
                      to_task="b", entry_seq=0, entry_artifact_id="art-1", reason="skip"),
        HandoffRecord(pipeline_id="pipe-1", run_id="run-2", from_seq=0, from_task="a", to_seq=2,
                      to_task="b", entry_seq=0, entry_artifact_id="art-1", reason="an earlier run"),
    ]

    class _LedgerOnly:
        path = "ledger.db"

        def stats(self, run_id=None):
            return {"events_total": 0}

        def export_rows(self, *, run_id=None):
            return [row]

        def handoffs(self, *, pipeline_id=None, run_id=None, limit=None):
            return [hop for hop in records if hop.pipeline_id == pipeline_id]

        def events(self, **kwargs):
            return []

    merged = merge_reports([_LedgerOnly()])
    assert merged.stats()["handoffs_total"] == 2  # both runs, because the report is not run-filtered
    assert merge_reports([_LedgerOnly()], run_id="run-2").stats()["handoffs_total"] == 1
    assert merged.rows[0]["handoffs"][0]["from_task"] == "a"


def test_a_pipeline_row_without_attempts_total_is_refused_not_counted_as_zero():
    """The field is required now that the counters come off the rows: an incomplete row must not produce a
    wrong number that looks like a real one."""
    row = {"pipeline_id": "pipe-7", "run_id": "run-1", "state": "succeeded"}

    class _NoAttempts:
        path = "incomplete.db"

        def stats(self, run_id=None):
            return {"events_total": 0, "attempts_total": 4}

        def export_rows(self, *, run_id=None):
            return [row]

        def events(self, **kwargs):
            return []

    with pytest.raises(ConfigError) as excinfo:
        merge_reports([_NoAttempts()])
    message = str(excinfo.value)
    assert "'pipe-7'" in message and "incomplete.db" in message and "attempts_total" in message


def test_a_store_without_any_ledger_capability_simply_has_no_jumps():
    """No nested ledger *and* no ``handoffs()``: a store that predates the feature, which is 0 — not an
    error, because the absence is the truth about that store."""
    row = {"pipeline_id": "pipe-1", "run_id": "run-1", "state": "succeeded", "attempts_total": 1}

    class _Legacy:
        path = "legacy.db"

        def stats(self, run_id=None):
            return {"events_total": 0}

        def export_rows(self, *, run_id=None):
            return [row]

        def events(self, **kwargs):
            return []

    merged = merge_reports([_Legacy()])
    assert merged.stats()["handoffs_total"] == 0
    assert "handoffs=" not in merged.summary()


def test_the_old_events_total_name_survives_on_the_object_only(tmp_path):
    """A deprecated Python-level alias, deliberately not a second key in ``stats()``: the rename exists so
    that a de-duplicated counter and a raw log total cannot be confused, and the JSON schema should say one
    thing. The alias only spares a caller an attribute rename.

    It also pins why the rename is *only* a rename: for one store, the merged report's raw total is that
    store's ``stats()["events_total"]`` — the same measurement, under a name that says where it comes from."""
    db = tmp_path / "alias.db"
    _run(db, TWO_STEP, [{"n": 0}, {"n": 1}])
    store = _open(db)
    try:
        per_store = store.stats()["events_total"]
    finally:
        store.close()

    merged = merge_reports([str(db)])
    assert per_store == EXPECTED_COUNTS["events"] == 7
    assert merged.events_total == merged.source_events_total == per_store
    assert "events_total" not in merged.stats()



def test_merge_reports_deduplicates_repair_failures_across_stores(tmp_path):
    """Issue #46: merge_reports must not double-count a pipeline's repair failures across stores."""
    from pyattacker.store import SqliteStore

    db1, db2 = tmp_path / "a.db", tmp_path / "b.db"
    seeds = [{"n": 0}]
    _run(db1, TWO_STEP, seeds)
    time.sleep(0.02)
    _run(db2, TWO_STEP, seeds)

    # Inject a fake terminal_repair_failed event into both stores
    pid = next(iter(TWO_STEP.map(seeds))).pipeline_id
    for db in (db1, db2):
        store = SqliteStore(str(db))
        store.emit_event(EventRecord(
            ts=time.time(), kind="pipeline.terminal_repair_failed",
            run_id="some-repair-run", pipeline_id=pid,
            data={"phase": "test", "error": "test error"},
        ))
        store.close()

    store1, store2 = _open(db1), _open(db2)
    try:
        merged = merge_reports([store1, store2])
    finally:
        store1.close()
        store2.close()

    # The same pipeline appears in both stores, but repair_failures should be 1, not 2.
    assert merged.stats()["repair_failures"] == 1


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
        # Nothing was folded, so the recomputed counters equal the plain sum of the two runs.
        assert stats["attempts_total"] == 2 * EXPECTED_COUNTS["attempts"] == 8
        assert stats["source_events_total"] == 2 * EXPECTED_COUNTS["events"] == 14
        assert stats["handoffs_total"] == 0

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
    # The tests that request this fixture — or `sharded`, which depends on it — carry the
    # `requires_yaml` mark: writing the file never needs the extra, reading it back does.
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


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
def test_cli_shard_without_store_exits_two(cli_config, capsys):
    assert main(["run", "-c", str(cli_config), "--shard", "0/2"]) == 2
    err = capsys.readouterr().err
    assert "Config error" in err
    assert "--shard needs a file-backed store" in err


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
def test_cli_run_shards_strict_env_accepts_the_shard_provided_variable(tmp_path, capsys, monkeypatch):
    """${PYATACKER_SHARD} is only ever set inside a shard child (see shard_env()); the parent's
    own --strict-env preflight must not treat it as missing, or every --shards run referencing it
    would fail before a single child started."""
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


@pytest.mark.requires_yaml
def test_cli_run_shards_strict_env_still_rejects_a_genuinely_missing_variable(tmp_path, capsys, monkeypatch):
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
