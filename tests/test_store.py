"""Storage-layer consistency tests: SqliteStore and MemoryStore must behave identically.

Coverage
* run lifecycle: start_run / heartbeat / finish_run / get_run
* pipeline: upsert (the same id does not create a second row) + finish_pipeline (error_type/message/traceback/failed_task)
* artifact: put/get round trip, content-addressed metadata, mark_final, artifacts ordering
* journal="summary": payload is None after persisting (metadata is retained)
* interrupt_stale: running pipeline with an expired heartbeat → interrupted; keep_run_id protection
* record_attempt: appends instead of overwriting (every attempt of the same task is kept)
* events: run / pipeline filtering, limit returns the most recent entries, event_id increases
* paged iteration (the optional PagedStore extension): same rows, order and filters as the list APIs
* export_rows: structure (key set / tasks / artifacts / duration_ms / payload decoding)
* stats: pipelines.by_state, tasks.by_name, attempts_total, events_total, duration percentiles

Both backends are parameterized with the same set of assertions, so any semantic drift is exposed immediately.
Note: MemoryStore has no public attempts query interface, so we read back through the private container/`_conn`;
this is the only verifiable way to check "was the record really appended?".
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Iterator
from typing import Any

import pytest

from pyattacker import Artifact, MemoryStore, SqliteStore
from pyattacker.artifact import DEFAULT_REGISTRY
from pyattacker.errors import ArtifactCodecError, RetryableError
from pyattacker.store import (
    ITER_BATCH_SIZE,
    AttemptRecord,
    EventRecord,
    PagedStore,
    PipelineRecord,
    RunRecord,
    TaskRecord,
)

# --------------------------------------------------------------------- helpers


def _make_store(kind: str, tmp_path: Any, *, journal: str = "full"):
    if kind == "memory":
        return MemoryStore(journal=journal)
    return SqliteStore(str(tmp_path / "store.db"), journal=journal)


@pytest.fixture(params=["sqlite", "memory"])
def store(request: pytest.FixtureRequest, tmp_path):
    """The same assertions run against both backends; each gets its own tmp_path."""
    backend = _make_store(request.param, tmp_path)
    try:
        yield backend
    finally:
        backend.close()


def _run(run_id: str = "run-1", *, started_at: float = 1000.0, **kwargs: Any) -> RunRecord:
    return RunRecord(run_id=run_id, started_at=started_at, **kwargs)


def _pipeline(pipeline_id: str = "p1", *, run_id: str = "run-1", **kwargs: Any) -> PipelineRecord:
    return PipelineRecord(
        pipeline_id=pipeline_id,
        run_id=run_id,
        name="qa",
        key=f"key-{pipeline_id}",
        created_at=kwargs.pop("created_at", 1000.0),
        **kwargs,
    )


def _artifact(
    *,
    pipeline_id: str = "p1",
    seq: int = 0,
    value: Any = None,
    task: str = "ask",
) -> Artifact:
    """Encode a real payload with the default codec; metadata round-trips through the store exactly."""
    value = {"x": 1} if value is None else value
    encoded = DEFAULT_REGISTRY.dump(value)
    return Artifact(
        id=Artifact.build_id(pipeline_id, seq),
        pipeline_id=pipeline_id,
        task_name=task,
        seq=seq,
        type_name=encoded.type_name,
        codec=encoded.codec,
        digest=encoded.digest,
        size=encoded.size,
        payload=encoded.data,
        created_at=1000.0,
    )


def _attempt(
    *,
    attempt_no: int,
    outcome: str = "running",
    task_name: str = "ask",
    error_type: str | None = None,
    run_id: str = "run-1",
    pipeline_id: str = "p1",
    task_run_id: str = "p1:0",
) -> AttemptRecord:
    return AttemptRecord(
        pipeline_id=pipeline_id,
        run_id=run_id,
        task_run_id=task_run_id,
        task_name=task_name,
        seq=0,
        attempt_no=attempt_no,
        started_at=1000.0,
        ended_at=1000.5,
        duration_ms=500.0,
        outcome=outcome,
        error_type=error_type,
    )


def _attempt_rows(backend: Any) -> list[dict[str, Any]]:
    """Read every row of the attempts table (the protocol has no attempts() query, so this is the only way to verify append semantics)."""
    if isinstance(backend, SqliteStore):
        rows = backend._conn.execute(
            "SELECT attempt_id, attempt_no, task_name, outcome, error_type, duration_ms "
            "FROM attempts ORDER BY attempt_id"
        ).fetchall()
        return [dict(row) for row in rows]
    return [
        {
            "attempt_id": item.attempt_id,
            "attempt_no": item.attempt_no,
            "task_name": item.task_name,
            "outcome": item.outcome,
            "error_type": item.error_type,
            "duration_ms": item.duration_ms,
        }
        for item in backend.attempts()
    ]


# ---------------------------------------------------------------------- runs


def test_run_lifecycle_start_heartbeat_finish(store):
    returned = store.start_run(
        _run(label="lab", host="node-1", config={"concurrency": 4})
    )
    assert returned.heartbeat_at == 1000.0  # start_run writes a heartbeat immediately

    got = store.get_run("run-1")
    assert (got.run_id, got.label, got.status) == ("run-1", "lab", "running")
    assert (got.started_at, got.heartbeat_at, got.ended_at) == (1000.0, 1000.0, None)
    assert got.host == "node-1"
    assert got.config == {"concurrency": 4}

    store.heartbeat("run-1", ts=1234.5)
    assert store.get_run("run-1").heartbeat_at == 1234.5
    assert store.get_run("run-1").status == "running"

    store.finish_run("run-1", "completed", ended_at=2000.0)
    done = store.get_run("run-1")
    assert (done.status, done.ended_at) == ("completed", 2000.0)

    # unknown run: reads return None, writes are silent no-ops (neither raises)
    assert store.get_run("missing") is None
    store.heartbeat("missing", ts=1.0)
    store.finish_run("missing", "failed", ended_at=1.0)
    assert store.get_run("missing") is None


def test_start_run_by_same_id_replaces_record(store):
    store.start_run(_run(label="first", started_at=100.0, status="failed", ended_at=150.0))
    store.start_run(_run(label="second", started_at=500.0))

    got = store.get_run("run-1")  # the same id overwrites, it does not add a row
    assert (got.label, got.status, got.started_at, got.ended_at) == ("second", "running", 500.0, None)
    assert got.heartbeat_at == 500.0


# ----------------------------------------------------------------- pipelines


def test_pipeline_upsert_then_finish_records_error(store):
    store.upsert_pipeline(
        _pipeline(state="running", started_at=1000.0, n_tasks_total=2, seed_digest="sd", spec_digest="sp")
    )
    got = store.get_pipeline("p1")
    assert (got.name, got.key, got.run_id, got.state) == ("qa", "key-p1", "run-1", "running")
    assert (got.n_tasks_total, got.seed_digest, got.spec_digest) == (2, "sd", "sp")

    # upserting the same id again updates instead of inserting
    store.upsert_pipeline(
        _pipeline(state="running", started_at=1000.0, n_tasks_total=2, n_tasks_done=1, attempts_total=3)
    )
    assert len(store.pipelines()) == 1
    assert store.get_pipeline("p1").attempts_total == 3

    store.finish_pipeline(
        "p1",
        "failed",
        n_tasks_done=1,
        error=RetryableError("upstream 502"),
        failed_task="ask",
        traceback="Traceback (most recent call last): ...",
    )
    failed = store.get_pipeline("p1")
    assert failed.state == "failed"
    assert failed.n_tasks_done == 1
    assert failed.error_type == "RetryableError"
    assert failed.error_message == "upstream 502"
    assert failed.failed_task == "ask"
    assert failed.traceback == "Traceback (most recent call last): ..."
    assert 1000.0 <= failed.finished_at <= time.time()

    # finishing an unknown pipeline must not blow up (idempotent/defensive)
    store.finish_pipeline("nope", "succeeded")
    assert store.get_pipeline("nope") is None


def test_finish_pipeline_success_without_error_keeps_error_fields_none(store):
    store.upsert_pipeline(_pipeline(state="running", started_at=1000.0))
    store.finish_pipeline("p1", "succeeded", n_tasks_done=2)
    got = store.get_pipeline("p1")
    assert (got.state, got.n_tasks_done) == ("succeeded", 2)
    assert (got.error_type, got.error_message, got.failed_task, got.traceback) == (None, None, None, None)


def test_finish_pipeline_truncates_long_error_message(store):
    store.upsert_pipeline(_pipeline(state="running", started_at=1000.0))
    store.finish_pipeline("p1", "failed", error=RetryableError("x" * 2500), failed_task="ask")
    assert store.get_pipeline("p1").error_message == "x" * 2000


# ----------------------------------------------------------------- artifacts


def test_artifact_roundtrip_and_mark_final(store):
    store.put_artifact(_artifact(seq=0, value={"b": 2, "a": 1}))
    store.put_artifact(_artifact(seq=1, value=[1, 2, 3], task="judge"))

    first = store.get_artifact("p1", 0)
    assert (first.id, first.pipeline_id, first.task_name, first.seq) == ("p1:0", "p1", "ask", 0)
    assert first.codec == "json"
    assert first.type_name == "dict"
    assert first.payload == b'{"a":1,"b":2}'  # canonical_json: keys sorted, no whitespace
    assert first.size == len(b'{"a":1,"b":2}')
    assert first.available is True
    assert first.is_final is False

    assert [a.seq for a in store.artifacts("p1")] == [0, 1]
    assert [a.task_name for a in store.artifacts("p1")] == ["ask", "judge"]
    assert store.artifacts("other") == []
    assert store.get_artifact("p1", 99) is None

    store.mark_final("p1", 1)
    assert store.get_artifact("p1", 1).is_final is True
    assert store.get_artifact("p1", 0).is_final is False
    assert store.artifacts("p1")[1].is_final is True
    # a missing seq is a no-op, it does not raise
    store.mark_final("p1", 99)
    assert store.get_artifact("p1", 99) is None


def test_put_artifact_same_key_overwrites_without_duplicating(store):
    store.put_artifact(_artifact(seq=0, value={"v": 1}))
    store.put_artifact(_artifact(seq=0, value={"v": 2}))
    arts = store.artifacts("p1")
    assert len(arts) == 1
    assert arts[0].payload == b'{"v":2}'


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_summary_journal_drops_payload_after_write(kind, tmp_path):
    backend = _make_store(kind, tmp_path, journal="summary")
    try:
        original = _artifact(seq=0, value={"keep": "metadata", "drop": "payload"})
        stored = backend.put_artifact(original)

        # the return value of put_artifact is the persisted form
        assert stored.payload is None
        assert stored.available is False
        # the metadata required for content addressing must be retained
        assert (stored.id, stored.digest, stored.size, stored.codec, stored.type_name) == (
            original.id,
            original.digest,
            original.size,
            original.codec,
            original.type_name,
        )

        read_back = backend.get_artifact("p1", 0)
        assert read_back.payload is None
        assert read_back.available is False
        assert read_back.digest == original.digest
        assert read_back.size == original.size
        with pytest.raises(ArtifactCodecError) as excinfo:
            read_back.encoded()  # no payload → it must not pretend it can decode
        assert "p1:0" in str(excinfo.value)
    finally:
        backend.close()


# ------------------------------------------------------------ interrupt_stale


def test_interrupt_stale_marks_only_expired_running_pipelines(store):
    now = time.time()
    store.start_run(_run("run-stale", started_at=now - 120.0))
    store.start_run(_run("run-fresh", started_at=now))
    store.upsert_pipeline(_pipeline("p-stale", run_id="run-stale", state="running", started_at=now - 120.0))
    store.upsert_pipeline(_pipeline("p-fresh", run_id="run-fresh", state="running", started_at=now))
    store.upsert_pipeline(_pipeline("p-done", run_id="run-stale", state="succeeded", started_at=now - 120.0))

    count = store.interrupt_stale(stale_after_s=30.0)

    assert count == 1
    assert store.get_pipeline("p-stale").state == "interrupted"
    assert store.get_pipeline("p-fresh").state == "running"
    assert store.get_pipeline("p-done").state == "succeeded"  # non-running rows are left alone


def test_interrupt_stale_keep_run_id_excludes_that_run(store):
    now = time.time()
    store.start_run(_run("run-a", started_at=now - 300.0))
    store.start_run(_run("run-b", started_at=now - 300.0))
    store.upsert_pipeline(_pipeline("p-a", run_id="run-a", state="running", started_at=now - 300.0))
    store.upsert_pipeline(_pipeline("p-b", run_id="run-b", state="running", started_at=now - 300.0))

    count = store.interrupt_stale(stale_after_s=30.0, keep_run_id="run-b")

    assert count == 1
    assert store.get_pipeline("p-a").state == "interrupted"
    assert store.get_pipeline("p-b").state == "running"


def test_interrupt_stale_after_finish_run_marks_pipelines(store):
    """When the run has already finished (status != running), even a fresh heartbeat counts as stale."""
    now = time.time()
    store.start_run(_run("run-ended", started_at=now))
    store.upsert_pipeline(_pipeline("p1", run_id="run-ended", state="running", started_at=now))
    store.finish_run("run-ended", "failed", ended_at=now)

    assert store.interrupt_stale(stale_after_s=30.0) == 1
    assert store.get_pipeline("p1").state == "interrupted"


# -------------------------------------------------------------------- attempts


def test_record_attempt_appends_not_overwrites(store):
    first = store.record_attempt(_attempt(attempt_no=1, outcome="failed", error_type="RetryableError"))
    second = store.record_attempt(_attempt(attempt_no=2, outcome="succeeded"))

    assert (first.attempt_id, second.attempt_id) == (1, 2)  # ids start at 1; the 2nd row does not overwrite the 1st

    rows = _attempt_rows(store)
    assert len(rows) == 2  # the 2nd attempt did not overwrite the 1st
    assert [r["attempt_no"] for r in rows] == [1, 2]
    assert [r["outcome"] for r in rows] == ["failed", "succeeded"]
    assert [r["attempt_id"] for r in rows] == [first.attempt_id, second.attempt_id]
    assert rows[0]["error_type"] == "RetryableError"
    assert rows[1]["error_type"] is None
    assert all(r["task_name"] == "ask" for r in rows)


# ---------------------------------------------------------------------- events


def test_events_query_filters_orders_and_limits(store):
    store.emit_event(EventRecord(ts=1.0, kind="task.succeeded", run_id="run-1", pipeline_id="p1", data={"n": 1}))
    store.emit_event(EventRecord(ts=2.0, kind="task.failed", run_id="run-1", pipeline_id="p2", data={"n": 2}))
    store.emit_event(EventRecord(ts=3.0, kind="pipeline.succeeded", run_id="run-2", pipeline_id="p1", data={"n": 3}))

    everything = store.events()
    assert [e.kind for e in everything] == ["task.succeeded", "task.failed", "pipeline.succeeded"]
    assert [e.event_id for e in everything] == [1, 2, 3]  # in write order, ids increase
    assert everything[0].scope == "pipeline"
    assert everything[0].data == {"n": 1}
    assert everything[2].run_id == "run-2"

    assert [e.kind for e in store.events(run_id="run-1")] == ["task.succeeded", "task.failed"]
    assert [e.run_id for e in store.events(pipeline_id="p1")] == ["run-1", "run-2"]
    assert store.events(run_id="nope") == []

    # limit takes the "most recent N", but still returns them in ascending time order
    assert [e.event_id for e in store.events(limit=2)] == [2, 3]
    assert [e.kind for e in store.events(run_id="run-1", limit=1)] == ["task.failed"]


# ------------------------------------------------------------- paged iteration


def test_paged_iterators_match_the_list_apis(store):
    """The optional PagedStore extension yields the list APIs' rows, in the same order and filters."""
    assert isinstance(store, PagedStore)
    store.upsert_pipeline(_pipeline("p1", created_at=1000.0, state="succeeded"))
    store.upsert_pipeline(_pipeline("p2", run_id="run-2", created_at=1001.0, state="failed"))
    store.record_task(TaskRecord(task_run_id="p1:0", pipeline_id="p1", run_id="run-1", name="ask", seq=0))
    store.record_task(TaskRecord(task_run_id="p2:0", pipeline_id="p2", run_id="run-2", name="ask", seq=0))
    store.record_task(TaskRecord(task_run_id="p1:1", pipeline_id="p1", run_id="run-1", name="ask", seq=1))
    store.record_attempt(_attempt(attempt_no=1, outcome="failed", run_id="run-1", pipeline_id="p1"))
    store.record_attempt(_attempt(attempt_no=1, outcome="succeeded", run_id="run-2", pipeline_id="p2"))
    store.emit_event(EventRecord(ts=1.0, kind="task.succeeded", run_id="run-1", pipeline_id="p1"))
    store.emit_event(EventRecord(ts=2.0, kind="pipeline.succeeded", run_id="run-2", pipeline_id="p2"))
    store.put_artifact(_artifact(pipeline_id="p1", seq=0))
    store.put_artifact(_artifact(pipeline_id="p1", seq=1))
    store.put_artifact(_artifact(pipeline_id="p2", seq=0))

    assert [p.pipeline_id for p in store.iter_pipelines()] == [
        p.pipeline_id for p in store.pipelines()
    ] == ["p1", "p2"]
    assert [p.pipeline_id for p in store.iter_pipelines(run_id="run-2")] == ["p2"]
    assert [p.pipeline_id for p in store.iter_pipelines(state="failed")] == ["p2"]

    assert [t.task_run_id for t in store.iter_tasks()] == [
        t.task_run_id for t in store.tasks()
    ] == ["p1:0", "p1:1", "p2:0"]
    assert [t.task_run_id for t in store.iter_tasks("p1")] == [
        t.task_run_id for t in store.tasks("p1")
    ] == ["p1:0", "p1:1"]
    assert [t.task_run_id for t in store.iter_tasks(run_id="run-2")] == ["p2:0"]

    assert [a.attempt_id for a in store.iter_attempts()] == [
        a.attempt_id for a in store.attempts()
    ] == [1, 2]
    assert [a.attempt_id for a in store.iter_attempts(run_id="run-1", pipeline_id="p1")] == [1]
    assert [a.attempt_id for a in store.iter_attempts(run_id="run-2")] == [2]

    assert [e.event_id for e in store.iter_events()] == [
        e.event_id for e in store.events()
    ] == [1, 2]
    assert [e.kind for e in store.iter_events(run_id="run-2")] == ["pipeline.succeeded"]
    assert [e.kind for e in store.iter_events(pipeline_id="p1")] == ["task.succeeded"]

    assert [a.seq for a in store.iter_artifacts(pipeline_id="p1")] == [
        a.seq for a in store.artifacts("p1")
    ] == [0, 1]

    # a lazy stream, not a materialized list — that is the whole point of the extension
    assert isinstance(store.iter_events(), Iterator)


def test_paged_iterators_break_cursor_ties_on_the_primary_key(store):
    """Rows may share ``(pipeline_id, seq)`` / ``seq``; the cursor must end in a unique key.

    The schema keys ``tasks`` by ``task_run_id`` and ``artifacts`` by ``artifact_id``, and the list
    APIs order by the non-unique prefix alone, so this pins the iterator contract directly instead
    of comparing it with an order the store never promised.
    """
    store.upsert_pipeline(_pipeline("p1", created_at=1000.0))
    store.record_task(
        TaskRecord(task_run_id="p1:0", pipeline_id="p1", run_id="run-1", name="ask", seq=0)
    )
    store.record_task(
        TaskRecord(task_run_id="p1:0-alt", pipeline_id="p1", run_id="run-1", name="ask", seq=0)
    )
    store.put_artifact(_artifact(pipeline_id="p1", seq=0))
    store.put_artifact(dataclasses.replace(_artifact(pipeline_id="p1", seq=0), id="p1:0-alt"))

    assert [t.task_run_id for t in store.iter_tasks()] == ["p1:0", "p1:0-alt"]

    # MemoryStore keys artifacts by (pipeline_id, seq), so it stores one of the two; SqliteStore
    # keys them by artifact_id and stores both. Either way the iterator's order is the documented
    # key, `(seq, artifact_id)`.
    stored = store.artifacts("p1")
    assert [a.id for a in store.iter_artifacts(pipeline_id="p1")] == [
        a.id for a in sorted(stored, key=lambda a: (a.seq, a.id))
    ]


def test_paged_iterators_mirror_the_live_store_semantics(store):
    """``events``/``attempts`` are bounded by the mark taken when iteration starts.

    Both backends have to agree, and a full page is consumed before the append so the producer's
    write lands after the iterator's last page: without the bound the next page would pick it up.
    The mark is per iterator, not permanent — a new read sees the new rows.
    """
    for index in range(ITER_BATCH_SIZE):
        store.emit_event(EventRecord(ts=float(index), kind=f"event.{index}", run_id="run-1"))
        store.record_attempt(_attempt(attempt_no=index + 1, run_id="run-1"))

    events = store.iter_events()
    attempts = store.iter_attempts()
    first_events = [next(events) for _ in range(ITER_BATCH_SIZE)]  # the marks are fixed here
    first_attempts = [next(attempts) for _ in range(ITER_BATCH_SIZE)]
    assert (first_events[0].kind, first_attempts[0].attempt_no) == ("event.0", 1)

    store.emit_event(EventRecord(ts=1.0, kind="event.late", run_id="run-1"))
    store.record_attempt(_attempt(attempt_no=ITER_BATCH_SIZE + 1, run_id="run-1"))

    assert list(events) == []  # bounded: the late rows are not part of this traversal
    assert list(attempts) == []

    # a fresh iterator does see them
    assert [e.kind for e in store.iter_events()][-1] == "event.late"
    assert [a.attempt_no for a in store.iter_attempts()][-1] == ITER_BATCH_SIZE + 1


# ----------------------------------------------------------------- export_rows

_EXPORT_KEYS = {
    "pipeline_id",
    "key",
    "name",
    "run_id",
    "state",
    "tags",
    "n_tasks_done",
    "n_tasks_total",
    "attempts_total",
    "started_at",
    "finished_at",
    "duration_ms",
    "failed_task",
    "error_type",
    "error_message",
    "tasks",
    "artifacts",
}
_TASK_ROW_KEYS = {
    "name",
    "seq",
    "state",
    "attempts_used",
    "duration_ms",
    "error_class",
    "error_type",
    "error_message",
    "output_artifact_id",
}
_ARTIFACT_ROW_KEYS = {"task", "seq", "type", "codec", "digest", "is_final", "payload"}


def test_export_rows_structure(store):
    store.upsert_pipeline(
        _pipeline(
            "p1",
            state="succeeded",
            created_at=1000.0,
            started_at=1000.0,
            finished_at=1000.5,
            n_tasks_total=1,
            n_tasks_done=1,
            attempts_total=2,
            tags={"suite": "store"},
        )
    )
    store.record_task(
        TaskRecord(
            task_run_id="p1:0",
            pipeline_id="p1",
            run_id="run-1",
            name="ask",
            seq=0,
            state="succeeded",
            attempts_used=2,
            started_at=1000.0,
            ended_at=1000.5,
            duration_ms=500.0,
            output_artifact_id="p1:0",
        )
    )
    store.put_artifact(_artifact(seq=0, value={"answer": 42}, task="ask"))
    store.mark_final("p1", 0)
    store.upsert_pipeline(
        _pipeline(
            "p2",
            run_id="run-2",
            state="failed",
            created_at=2000.0,
            started_at=2000.0,
            finished_at=2001.5,
            error_type="RetryableError",
            error_message="boom",
            failed_task="ask",
        )
    )

    rows = list(store.export_rows())
    assert [r["pipeline_id"] for r in rows] == ["p1", "p2"]  # ascending by created_at

    row = rows[0]
    assert set(row) == _EXPORT_KEYS
    assert (row["key"], row["name"], row["run_id"], row["state"]) == ("key-p1", "qa", "run-1", "succeeded")
    assert row["tags"] == {"suite": "store"}
    assert (row["n_tasks_done"], row["n_tasks_total"], row["attempts_total"]) == (1, 1, 2)
    assert (row["started_at"], row["finished_at"]) == (1000.0, 1000.5)
    assert row["duration_ms"] == 500.0
    assert (row["failed_task"], row["error_type"], row["error_message"]) == (None, None, None)

    assert len(row["tasks"]) == 1
    task_row = row["tasks"][0]
    assert set(task_row) == _TASK_ROW_KEYS
    assert (task_row["name"], task_row["seq"], task_row["state"], task_row["attempts_used"]) == (
        "ask",
        0,
        "succeeded",
        2,
    )
    assert task_row["output_artifact_id"] == "p1:0"
    assert task_row["duration_ms"] == 500.0

    assert len(row["artifacts"]) == 1
    artifact_row = row["artifacts"][0]
    assert set(artifact_row) == _ARTIFACT_ROW_KEYS
    assert (artifact_row["task"], artifact_row["seq"], artifact_row["type"]) == ("ask", 0, "dict")
    assert artifact_row["codec"] == "json"
    assert artifact_row["is_final"] is True
    assert artifact_row["payload"] == {"answer": 42}  # journal=full: the payload is already decoded to JSON
    assert artifact_row["digest"] == store.get_artifact("p1", 0).digest

    failed_row = rows[1]
    assert (failed_row["state"], failed_row["error_type"], failed_row["error_message"]) == (
        "failed",
        "RetryableError",
        "boom",
    )
    assert failed_row["tasks"] == []
    assert failed_row["artifacts"] == []
    assert failed_row["duration_ms"] == 1500.0

    assert [r["pipeline_id"] for r in store.export_rows(run_id="run-2")] == ["p2"]
    assert list(store.export_rows(run_id="nope")) == []


def test_export_rows_decodes_payload_for_full_journal(store):
    store.upsert_pipeline(_pipeline("p1", state="succeeded", created_at=1000.0))
    store.put_artifact(_artifact(seq=0, value={"nested": {"ok": [1, 2]}}, task="ask"))

    artifact_row = next(iter(store.export_rows()))["artifacts"][0]
    assert artifact_row["payload"] == {"nested": {"ok": [1, 2]}}
    assert artifact_row["type"] == "dict"
    assert artifact_row["codec"] == "json"


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_export_rows_payload_is_none_with_summary_journal(kind, tmp_path):
    backend = _make_store(kind, tmp_path, journal="summary")
    try:
        backend.upsert_pipeline(_pipeline("p1", state="succeeded", created_at=1000.0))
        backend.put_artifact(_artifact(seq=0, value={"secret": 1}, task="ask"))

        artifact_row = next(iter(backend.export_rows()))["artifacts"][0]
        assert artifact_row["payload"] is None  # summary mode: the payload is not persisted, so the export cannot conjure one
        assert artifact_row["digest"] == _artifact(seq=0, value={"secret": 1}, task="ask").digest
    finally:
        backend.close()


# ----------------------------------------------------------------------- stats


def _seed_stats_fixture(backend: Any) -> None:
    """Two pipelines + two tasks + three attempts; attempts_used matches the number of attempt rows."""
    backend.upsert_pipeline(
        _pipeline(
            "p1",
            state="succeeded",
            created_at=1000.0,
            started_at=1000.0,
            finished_at=1000.5,
            n_tasks_total=1,
            n_tasks_done=1,
        )
    )
    backend.upsert_pipeline(
        _pipeline(
            "p2",
            run_id="run-1",
            state="failed",
            created_at=2000.0,
            started_at=2000.0,
            finished_at=2001.5,
            n_tasks_total=1,
            error_type="RetryableError",
            error_message="boom",
        )
    )
    backend.record_task(
        TaskRecord(
            task_run_id="p1:0",
            pipeline_id="p1",
            run_id="run-1",
            name="ask",
            seq=0,
            state="succeeded",
            attempts_used=2,
        )
    )
    backend.record_task(
        TaskRecord(
            task_run_id="p2:0",
            pipeline_id="p2",
            run_id="run-1",
            name="judge",
            seq=0,
            state="failed",
            attempts_used=1,
        )
    )
    backend.record_attempt(_attempt(attempt_no=1, outcome="failed", task_name="ask"))
    backend.record_attempt(_attempt(attempt_no=2, outcome="succeeded", task_name="ask"))
    backend.record_attempt(_attempt(attempt_no=1, outcome="failed", task_name="judge"))
    backend.emit_event(EventRecord(ts=1.0, kind="task.failed", run_id="run-1", pipeline_id="p2"))
    backend.emit_event(EventRecord(ts=2.0, kind="task.succeeded", run_id="run-1", pipeline_id="p1"))
    backend.emit_event(EventRecord(ts=3.0, kind="pipeline.succeeded", run_id="run-1", pipeline_id="p1"))


def test_stats_counts(store):
    _seed_stats_fixture(store)

    stats = store.stats("run-1")
    assert stats["pipelines"]["total"] == 2
    assert stats["pipelines"]["by_state"] == {"succeeded": 1, "failed": 1}
    # only these two pipelines have started_at/finished_at: 500ms and 1500ms
    assert stats["pipelines"]["duration_ms"] == {"p50": 500.0, "p95": 500.0, "max": 1500.0}
    assert stats["tasks"]["by_name"] == {"ask": 1, "judge": 1}
    assert stats["tasks"]["attempts_by_name"] == {"ask": 2, "judge": 1}
    assert stats["attempts_total"] == 3
    assert stats["events_total"] == 3

    assert store.stats()["pipelines"]["total"] == 2  # no run_id = the whole store


def test_stats_empty_store_is_zeroed(store):
    stats = store.stats()
    assert stats["pipelines"] == {
        "total": 0,
        "by_state": {},
        "duration_ms": {"p50": None, "p95": None, "max": None},
    }
    assert stats["tasks"] == {"by_name": {}, "attempts_by_name": {}}
    assert (stats["attempts_total"], stats["events_total"]) == (0, 0)


def test_stats_run_id_scopes_pipelines_tasks_and_attempts(store):
    _seed_stats_fixture(store)
    store.upsert_pipeline(
        _pipeline("p3", run_id="run-2", state="running", created_at=3000.0, started_at=3000.0)
    )
    store.record_task(
        TaskRecord(
            task_run_id="p3:0",
            pipeline_id="p3",
            run_id="run-2",
            name="other",
            seq=0,
            state="running",
            attempts_used=1,
        )
    )
    store.record_attempt(
        _attempt(attempt_no=1, outcome="running", task_name="other", run_id="run-2", pipeline_id="p3", task_run_id="p3:0")
    )
    store.emit_event(EventRecord(ts=4.0, kind="task.running", run_id="run-2", pipeline_id="p3"))

    scoped = store.stats("run-1")
    assert scoped["pipelines"]["total"] == 2
    assert scoped["tasks"]["by_name"] == {"ask": 1, "judge": 1}
    assert scoped["attempts_total"] == 3

    assert scoped["events_total"] == 3  # consistent with SqliteStore: filtered by run


# --------------------------------------------------------------------- resources


def test_resources_roundtrip_and_pool_filter(store):
    store.upsert_resource("apis", "api-1", "llm", {"model": "gpt-4o"}, "ready", {"active": 1})
    store.upsert_resource("apis", "api-2", "llm", {"model": "gpt-4o-mini"}, "degraded", {"active": 0})
    store.upsert_resource("workers", "w-1", "local", {}, "ready", {"active": 2})

    all_rows = store.resources()
    assert {row["resource_id"] for row in all_rows} == {"api-1", "api-2", "w-1"}

    apis = store.resources(pool="apis")
    assert {row["resource_id"] for row in apis} == {"api-1", "api-2"}
    row = next(r for r in apis if r["resource_id"] == "api-1")
    assert row["pool"] == "apis"
    assert row["kind"] == "llm"
    assert row["spec"] == {"model": "gpt-4o"}
    assert row["state"] == "ready"
    assert row["stats"] == {"active": 1}

    # upserting the same (pool, resource_id) updates in place instead of duplicating
    store.upsert_resource("apis", "api-1", "llm", {"model": "gpt-4o"}, "dead", {"active": 0})
    assert len(store.resources(pool="apis")) == 2
    assert next(r for r in store.resources(pool="apis") if r["resource_id"] == "api-1")["state"] == "dead"
