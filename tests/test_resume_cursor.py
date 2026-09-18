"""Terminal-cursor recovery: both torn-finalization shapes, the corruption bound, and the write ordering.

Coverage:
* a `failed`/`interrupted` row whose cursor reached the end is repaired to `succeeded` **without
  re-running any task**, with the terminal artifact marked final and the previous failure preserved on
  the `pipeline.terminal_repaired` event;
* the repair verifies what an ordinary resumed checkpoint verifies: a present-but-undecodable terminal
  payload restarts the pipeline (`pipeline.checkpoint_unusable`) instead of being promoted to success, and a
  dropped payload does the same with `pipeline.checkpoint_missing`;
* a failed repair attempt leaves the row, its cursor and its **original** failure untouched, so the next
  attempt still reports the original provenance rather than the repair's own error;
* a cursor *past* the end is reported as corruption (`CorruptCheckpoint` + `pipeline.corrupt_cursor`),
  never promoted to success, never indexed, and its stored value is left untouched;
* the last task's finality mark is committed before the terminal state, and no durable `running` row ever
  carries `n_tasks_done == n_tasks`, so a crash between the two writes can only re-run the final task
  (the documented at-least-once boundary) — never strand a `succeeded` pipeline with no final artifact;
* a legacy `running` row with the cursor at the end is repaired through the real stale-interruption path.

The poison rows are produced by a real run through a store that fails one chosen write, not by hand-editing
rows, so the tests pin the behaviour that actually creates them.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any

import pytest

from pyattacker import Artifact, FileBackend, PipelineRecord, Runner, SqliteStore, pipeline, task
from pyattacker.artifact import DEFAULT_REGISTRY
from pyattacker.store.base import RunRecord

CALLS: dict[str, int] = {"fetch": 0, "ask": 0}


@task("t.fetch")
def t_fetch(value, ctx):
    CALLS["fetch"] += 1
    return {**value, "fetch": 1}


@task("t.ask")
def t_ask(value, ctx):
    CALLS["ask"] += 1
    return {**value, "ask": 1}


TEMPLATE = pipeline("cursor-two", t_fetch | t_ask)
SEED = {"row": 1}


class ScriptedStore(SqliteStore):
    """A store that records the calls it receives and can fail one chosen write once."""

    def __init__(self, path: str, *, journal: str = "full", backend: Any = None) -> None:
        super().__init__(path, journal=journal, backend=backend)
        self.calls: list[str] = []
        self.upserts: list[tuple[str, int]] = []
        self.fail_succeeded_finish = False
        self.fail_mark_final = False
        self.fail_settle_pipeline = False
        self.fail_upsert_pipeline = False

    def upsert_pipeline(self, record) -> None:  # type: ignore[override]
        self.calls.append("upsert_pipeline")
        self.upserts.append((record.state, record.n_tasks_done))
        if self.fail_upsert_pipeline:
            self.fail_upsert_pipeline = False
            raise RuntimeError("injected store failure while cleaning up the terminal row")
        return super().upsert_pipeline(record)

    def mark_final(self, pipeline_id: str, seq: int) -> None:  # type: ignore[override]
        self.calls.append("mark_final")
        if self.fail_mark_final:
            self.fail_mark_final = False
            raise RuntimeError("injected store failure while marking the final artifact")
        return super().mark_final(pipeline_id, seq)

    def settle_pipeline(  # type: ignore[override]
        self, pipeline_id: str, *, state: str, n_tasks_done: int, run_id: str
    ) -> None:
        self.calls.append(f"settle_pipeline:{state}")
        if self.fail_settle_pipeline:
            self.fail_settle_pipeline = False
            raise RuntimeError("injected store failure while settling the terminal row")
        return super().settle_pipeline(
            pipeline_id, state=state, n_tasks_done=n_tasks_done, run_id=run_id
        )

    def finish_pipeline(  # type: ignore[override]
        self,
        pipeline_id: str,
        state: str,
        *,
        n_tasks_done: int | None = None,
        error: BaseException | None = None,
        failed_task: str | None = None,
        traceback: str | None = None,
    ) -> None:
        self.calls.append(f"finish_pipeline:{state}")
        if state == "succeeded" and self.fail_succeeded_finish:
            self.fail_succeeded_finish = False
            raise RuntimeError("injected store failure while finalizing the pipeline")
        return super().finish_pipeline(
            pipeline_id,
            state,
            n_tasks_done=n_tasks_done,
            error=error,
            failed_task=failed_task,
            traceback=traceback,
        )


@pytest.fixture(autouse=True)
def _reset_calls():
    CALLS.update(fetch=0, ask=0)


def _kinds(store, pipeline_id: str, run_id: str | None = None) -> list[str]:
    """Event kinds for one pipeline, optionally narrowed to one run's own events.

    Run 1's events stay in the store, so "this run did not raise a framework error" has to be
    asserted against the run that did the repairing, not against the whole history.
    """
    return [
        event.kind
        for event in store.events(pipeline_id=pipeline_id, limit=200)
        if run_id is None or event.run_id == run_id
    ]


def _poison_row(tmp_path, name: str = "poison.db", *, journal: str = "full") -> tuple[ScriptedStore, str]:
    """Run a pipeline whose terminal write fails, leaving `failed` with cursor == n_tasks."""
    store = ScriptedStore(str(tmp_path / name), journal=journal)
    store.fail_succeeded_finish = True
    spec = TEMPLATE.bind(SEED)
    report = Runner(store=store, handle_signals=False).run([spec])
    row = store.get_pipeline(spec.pipeline_id)
    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert (row.state, row.n_tasks_done, row.n_tasks_total) == ("failed", 2, 2)
    assert CALLS == {"fetch": 1, "ask": 1}
    store.close()
    return store, spec.pipeline_id


# --------------------------------------------------- repair: cursor == n_tasks
def test_a_failed_row_with_the_cursor_at_the_end_is_repaired_not_rerun(tmp_path):
    _poison_row(tmp_path)
    store = ScriptedStore(str(tmp_path / "poison.db"))
    spec = TEMPLATE.bind(SEED)

    report = Runner(store=store, handle_signals=False).run([spec])

    # nothing re-ran: every task's artifact was already durable, so the artifacts are the truth
    assert CALLS == {"fetch": 1, "ask": 1}
    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done, row.n_tasks_total) == ("succeeded", 2, 2)
    assert row.error_type is None and row.failed_task is None
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert report.leases_leaked == 0

    artifacts = store.artifacts(spec.pipeline_id)
    assert [a.seq for a in artifacts] == [-1, 0, 1]
    assert [a.is_final for a in artifacts] == [False, False, True]

    assert _kinds(store, spec.pipeline_id, report.run_id) == [
        "pipeline.terminal_repaired",
        "pipeline.succeeded",
    ]
    # the failed run had already marked the artifact final, so the repair only settles the row
    assert "mark_final" not in store.calls
    assert "settle_pipeline:succeeded" in store.calls
    repaired = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.terminal_repaired")
    assert repaired.data["previous_state"] == "failed"
    assert repaired.data["previous_error_type"] == "RuntimeError"
    assert repaired.data["previous_error_message"].startswith("injected store failure")
    assert repaired.data["previous_failed_task"] == "t.ask"
    assert repaired.data["previous_run_id"]
    assert repaired.data["artifact_seq"] == 1
    store.close()


def test_a_repaired_pipeline_is_skipped_on_the_next_run(tmp_path):
    _poison_row(tmp_path)
    spec = TEMPLATE.bind(SEED)
    store = ScriptedStore(str(tmp_path / "poison.db"))
    Runner(store=store, handle_signals=False).run([spec])
    store.close()

    store = ScriptedStore(str(tmp_path / "poison.db"))
    report = Runner(store=store, handle_signals=False).run([spec])
    assert report.skipped == 1
    assert CALLS == {"fetch": 1, "ask": 1}
    assert "pipeline.skipped" in _kinds(store, spec.pipeline_id)
    store.close()


def test_an_unusable_terminal_artifact_restarts_the_pipeline(tmp_path):
    _poison_row(tmp_path, name="summary.db", journal="summary")
    store = ScriptedStore(str(tmp_path / "summary.db"), journal="summary")
    spec = TEMPLATE.bind(SEED)

    report = Runner(store=store, handle_signals=False).run([spec])

    # the payload was never kept, so the documented restart rule applies instead of a crash
    assert CALLS == {"fetch": 2, "ask": 2}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.checkpoint_missing" in kinds
    assert "pipeline.terminal_repaired" not in kinds
    store.close()


def test_a_deleted_terminal_blob_falls_back_to_a_restart(tmp_path):
    """The same fallback as `journal=summary`, reached through a missing blob instead of a dropped payload."""
    path = str(tmp_path / "blob.db")
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    spec = TEMPLATE.bind(SEED)
    store = ScriptedStore(path, backend=backend)
    store.fail_succeeded_finish = True
    report = Runner(store=store, handle_signals=False).run([spec])
    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert CALLS == {"fetch": 1, "ask": 1}

    terminal = store.get_artifact(spec.pipeline_id, TEMPLATE.n_tasks - 1)
    assert terminal.available is True
    assert backend.delete(terminal.blob_ref) is True
    assert store.get_artifact(spec.pipeline_id, TEMPLATE.n_tasks - 1).available is False
    store.close()

    store = ScriptedStore(path, backend=FileBackend(root=str(tmp_path / "blobs"), min_bytes=1))
    report = Runner(store=store, handle_signals=False).run([spec])

    assert CALLS == {"fetch": 2, "ask": 2}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.checkpoint_missing" in kinds
    assert "pipeline.terminal_repaired" not in kinds
    store.close()


# ------------------------------------------- terminal checkpoint that cannot be restored
def test_an_undecodable_terminal_artifact_restarts_the_pipeline(tmp_path):
    """`available` only means the bytes are there; a payload that no longer decodes is not a checkpoint."""
    _poison_row(tmp_path, name="undecodable.db")
    path = str(tmp_path / "undecodable.db")
    spec = TEMPLATE.bind(SEED)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE artifacts SET payload=? WHERE pipeline_id=? AND seq=?",
            (b"this is no longer the encoded value", spec.pipeline_id, TEMPLATE.n_tasks - 1),
        )
    store = ScriptedStore(path)
    assert store.get_artifact(spec.pipeline_id, TEMPLATE.n_tasks - 1).available is True

    report = Runner(store=store, handle_signals=False).run([spec])

    # the bytes are present but unusable, so the pipeline restarts rather than handing consumers a value
    # they cannot restore (and the two causes stay distinguishable)
    assert CALLS == {"fetch": 2, "ask": 2}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.checkpoint_unusable" in kinds
    assert "pipeline.terminal_repaired" not in kinds
    store.close()


def _poison_row_without_finality(tmp_path, name: str) -> tuple[str, str, str]:
    """A repairable row whose terminal artifact was never marked: run the first write, fail there.

    Returns `(path, pipeline_id, run_id)`. This is the shape in which a later repair has to call
    `mark_final` itself, unlike the usual poison row (whose failed run had already marked the artifact).
    """
    path = str(tmp_path / name)
    spec = TEMPLATE.bind(SEED)
    store = ScriptedStore(path)
    store.fail_mark_final = True
    Runner(store=store, handle_signals=False).run([spec])
    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("failed", 2)
    assert row.error_type == "RuntimeError"
    assert store.get_artifact(spec.pipeline_id, TEMPLATE.n_tasks - 1).is_final is False
    store.close()
    return path, spec.pipeline_id, row.run_id


def test_a_failed_repair_attempt_keeps_the_original_failure_provenance(tmp_path):
    """A repair that dies during `mark_final` must not overwrite the failure it was repairing."""
    path, pipeline_id, original_run = _poison_row_without_finality(tmp_path, "repair-fails.db")
    spec = TEMPLATE.bind(SEED)

    store = ScriptedStore(path)
    store.fail_mark_final = True
    report = Runner(store=store, handle_signals=False).run([spec])

    row = store.get_pipeline(pipeline_id)
    assert (row.state, row.n_tasks_done) == ("failed", 2), "the repairable shape must survive"
    assert row.error_type == "RuntimeError"
    assert row.error_message.startswith("injected store failure while marking the final artifact")
    assert row.failed_task == "t.ask"
    assert row.run_id == original_run, "a failed repair must not take ownership of the row"
    assert report.stats["pipelines"]["by_state"] == {}
    assert (report.repair_failures, report.to_dict()["repair_failures"]) == (1, 1)
    kinds = _kinds(store, pipeline_id, report.run_id)
    assert "pipeline.terminal_repair_failed" in kinds
    assert "runner.internal_error" not in kinds
    failed = next(e for e in store.events(pipeline_id=pipeline_id) if e.kind == "pipeline.terminal_repair_failed")
    assert failed.data["phase"] == "mark_final"
    assert failed.data["previous_error_type"] == "RuntimeError"
    assert failed.data["error"].startswith("RuntimeError: injected store failure while marking")
    store.close()

    # the next attempt succeeds and still reports the *original* failure, not the repair's error
    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])
    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.calls.index("mark_final") < store.calls.index("settle_pipeline:succeeded")
    repaired = next(e for e in store.events(pipeline_id=pipeline_id) if e.kind == "pipeline.terminal_repaired")
    assert repaired.data["previous_error_type"] == "RuntimeError"
    assert repaired.data["previous_error_message"].startswith("injected store failure while marking")
    assert repaired.data["previous_failed_task"] == "t.ask"
    assert repaired.data["previous_run_id"] == original_run
    store.close()


def test_a_failed_terminal_settle_keeps_the_original_failure_provenance(tmp_path):
    """`mark_final` succeeds and the settle fails: the row must survive as it was, provenance included."""
    _poison_row(tmp_path, name="settle-fails.db")
    path = str(tmp_path / "settle-fails.db")
    spec = TEMPLATE.bind(SEED)
    store = ScriptedStore(path)
    original = store.get_pipeline(spec.pipeline_id)
    original_run = original.run_id

    store.fail_settle_pipeline = True
    report = Runner(store=store, handle_signals=False).run([spec])

    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("failed", 2)
    assert (row.error_type, row.failed_task, row.run_id) == (
        original.error_type,
        original.failed_task,
        original_run,
    )
    assert row.error_message == original.error_message
    assert report.stats["pipelines"]["by_state"] == {}
    assert report.repair_failures == 1
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.terminal_repair_failed" in kinds
    assert "pipeline.terminal_repaired" not in kinds
    assert "runner.internal_error" not in kinds
    failed = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.terminal_repair_failed")
    assert failed.data["phase"] == "settle"
    assert failed.data["error"].startswith("RuntimeError: injected store failure while settling")
    assert failed.data["previous_error_message"].startswith("injected store failure while finalizing")
    store.close()

    # the artifact stayed final from the earlier run, and the retry reports the original failure
    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])
    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert "mark_final" not in store.calls
    repaired = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.terminal_repaired")
    assert repaired.data["previous_error_type"] == "RuntimeError"
    assert repaired.data["previous_error_message"].startswith("injected store failure while finalizing")
    assert repaired.data["previous_failed_task"] == "t.ask"
    assert repaired.data["previous_run_id"] == original_run
    store.close()


def test_a_failed_fallback_settle_keeps_the_original_failure_provenance(tmp_path, monkeypatch):
    """The same guarantee on the no-`settle_pipeline` path a third-party store takes."""
    _poison_row(tmp_path, name="fallback-fails.db")
    path = str(tmp_path / "fallback-fails.db")
    spec = TEMPLATE.bind(SEED)
    monkeypatch.delattr(ScriptedStore, "settle_pipeline")
    monkeypatch.delattr(SqliteStore, "settle_pipeline")

    store = ScriptedStore(path)
    original = store.get_pipeline(spec.pipeline_id)
    original_run = original.run_id
    store.fail_succeeded_finish = True
    report = Runner(store=store, handle_signals=False).run([spec])

    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("failed", 2)
    assert (row.error_type, row.error_message, row.failed_task) == (
        original.error_type,
        original.error_message,
        original.failed_task,
    )
    assert row.run_id == original_run
    assert report.stats["pipelines"]["by_state"] == {}
    assert report.repair_failures == 1
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.terminal_repair_failed" in kinds
    assert "runner.internal_error" not in kinds
    failed = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.terminal_repair_failed")
    assert failed.data["phase"] == "settle"
    store.close()

    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])
    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert report.repair_failures == 0
    repaired = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.terminal_repaired")
    assert repaired.data["previous_error_type"] == "RuntimeError"
    assert repaired.data["previous_error_message"].startswith("injected store failure while finalizing")
    assert repaired.data["previous_run_id"] == original_run
    store.close()


def test_a_failed_fallback_cleanup_is_not_a_failed_repair(tmp_path, monkeypatch):
    """`finish_pipeline("succeeded")` landed, only the metadata cleanup failed: the pipeline *is* repaired.

    A succeeded row is skipped forever, so that cleanup can never be retried — which is why it must not be
    reported as a failed repair. It is the documented degraded guarantee of a store without the native
    `settle_pipeline` capability.
    """
    _poison_row(tmp_path, name="cleanup-fails.db")
    path = str(tmp_path / "cleanup-fails.db")
    spec = TEMPLATE.bind(SEED)
    monkeypatch.delattr(ScriptedStore, "settle_pipeline")
    monkeypatch.delattr(SqliteStore, "settle_pipeline")

    store = ScriptedStore(path)
    original = store.get_pipeline(spec.pipeline_id)
    store.fail_upsert_pipeline = True
    report = Runner(store=store, handle_signals=False).run([spec])

    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("succeeded", 2)
    # only the metadata is stale: the original failure text and owning run survive, as documented
    assert (row.error_type, row.error_message, row.failed_task) == (
        original.error_type,
        original.error_message,
        original.failed_task,
    )
    assert row.run_id == original.run_id
    kinds = _kinds(store, spec.pipeline_id, report.run_id)
    assert "pipeline.terminal_cleanup_failed" in kinds
    assert "pipeline.terminal_repair_failed" not in kinds
    assert report.repair_failures == 0
    # the row still belongs to the earlier run (rebinding the run is part of the cleanup that failed), so
    # this run's run-scoped view has no row for it — the store-level state is what says `succeeded`
    assert report.stats["pipelines"]["by_state"] == {}
    artifacts = store.artifacts(spec.pipeline_id)
    assert [a.is_final for a in artifacts] == [False, False, True]
    store.close()

    # and a later resume skips it, because the durable state is what counts
    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])
    assert report.skipped == 1
    assert CALLS == {"fetch": 1, "ask": 1}
    store.close()


def test_a_repair_does_not_re_mark_an_already_final_artifact(tmp_path):
    """`mark_final` is skipped when the artifact is already final, so a custom store sees one call at most."""
    _poison_row(tmp_path, name="already-final.db")
    spec = TEMPLATE.bind(SEED)
    store = ScriptedStore(str(tmp_path / "already-final.db"))
    assert store.get_artifact(spec.pipeline_id, TEMPLATE.n_tasks - 1).is_final is True

    report = Runner(store=store, handle_signals=False).run([spec])

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert "mark_final" not in store.calls
    assert "settle_pipeline:succeeded" in store.calls
    store.close()


def _put_artifact(store, spec, task_name: str, seq: int, value) -> None:
    encoded = DEFAULT_REGISTRY.dump(value)
    store.put_artifact(
        Artifact(
            id=Artifact.build_id(spec.pipeline_id, seq),
            pipeline_id=spec.pipeline_id,
            task_name=task_name,
            seq=seq,
            type_name=encoded.type_name,
            codec=encoded.codec,
            digest=encoded.digest,
            size=encoded.size,
            payload=encoded.data,
            created_at=time.time(),
        )
    )


def test_a_killed_run_left_running_at_the_end_is_repaired_after_interrupt_stale(tmp_path):
    """The documented real path for a killed run: `running` + cursor == n -> interrupt_stale -> repaired.

    The old write order could leave that row durably (`upsert_pipeline(cursor=n)` before the terminal
    write), so a store written by an older version can still contain one.
    """
    path = str(tmp_path / "stale.db")
    spec = TEMPLATE.bind(SEED)
    store = SqliteStore(path)
    store.start_run(RunRecord(run_id="run-killed", status="running", started_at=time.time()))
    store.upsert_pipeline(
        PipelineRecord(
            pipeline_id=spec.pipeline_id,
            run_id="run-killed",
            name=spec.name,
            key=spec.key,
            state="running",
            n_tasks_total=spec.n_tasks,
            n_tasks_done=spec.n_tasks,
            seed_digest=spec.seed_digest,
            spec_digest=spec.spec_digest,
        )
    )
    _put_artifact(store, spec, "t.fetch", 0, {"row": 1, "fetch": 1})
    _put_artifact(store, spec, "t.ask", 1, {"row": 1, "fetch": 1, "ask": 1})
    store.finish_run("run-killed", "interrupted")
    assert store.get_pipeline(spec.pipeline_id).state == "running"
    store.close()

    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec], resume=True)

    assert CALLS == {"fetch": 0, "ask": 0}
    assert store.get_pipeline(spec.pipeline_id).state == "succeeded"
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert _kinds(store, spec.pipeline_id, report.run_id) == [
        "pipeline.terminal_repaired",
        "pipeline.succeeded",
    ]
    store.close()


# ---------------------------------------------------- corruption: cursor > n_tasks
def test_a_cursor_past_the_end_is_reported_as_corruption(tmp_path):
    path = str(tmp_path / "corrupt.db")
    spec = TEMPLATE.bind(SEED)
    store = SqliteStore(path)
    store.upsert_pipeline(
        PipelineRecord(
            pipeline_id=spec.pipeline_id,
            run_id="run-corrupt",
            name=spec.name,
            key=spec.key,
            n_tasks_total=spec.n_tasks,
            seed_digest=spec.seed_digest,
            spec_digest=spec.spec_digest,
        )
    )
    store.finish_pipeline(spec.pipeline_id, "interrupted", n_tasks_done=5)
    store.close()

    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])

    assert CALLS == {"fetch": 0, "ask": 0}
    row = store.get_pipeline(spec.pipeline_id)
    assert row.state == "failed"
    assert row.n_tasks_done == 5, "the corrupt value is the evidence and must be left in place"
    assert row.error_type == "CorruptCheckpoint"
    assert "exceeds the 2 task(s)" in row.error_message
    assert report.stats["pipelines"]["by_state"] == {"failed": 1}

    assert _kinds(store, spec.pipeline_id, report.run_id) == [
        "pipeline.corrupt_cursor",
        "pipeline.failed",
    ]
    corrupted = next(e for e in store.events(pipeline_id=spec.pipeline_id) if e.kind == "pipeline.corrupt_cursor")
    assert corrupted.data["n_tasks_done"] == 5
    assert corrupted.data["previous_state"] == "interrupted"
    store.close()


# --------------------------------------------------------- write ordering
def test_the_repair_settles_through_the_write_behind_wrapper(tmp_path):
    """A file-backed store is wrapped in `WriteBehindStore`; the capability must still be found and used."""
    _poison_row(tmp_path, name="wrapped.db")
    spec = TEMPLATE.bind(SEED)
    runner = Runner(store=str(tmp_path / "wrapped.db"), handle_signals=False)
    # state writes are delegated, not buffered: the wrapper exposes the inner store's capability
    assert hasattr(runner.store, "settle_pipeline")

    report = runner.run([spec])

    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    row = runner.store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done, row.error_type) == ("succeeded", 2, None)
    runner.close()


def test_a_store_without_the_capability_uses_the_ordered_fallback(tmp_path, monkeypatch):
    """The documented fallback: terminal transition first, cleanup second — never the other way round.

    The capability is removed from the class rather than hidden behind a wrapper: `Store` is a
    ``runtime_checkable`` protocol and `open_store` uses ``isinstance``, so a delegating wrapper is not a
    faithful stand-in for a third-party store across Python versions.
    """
    _poison_row(tmp_path, name="fallback.db")
    spec = TEMPLATE.bind(SEED)
    monkeypatch.delattr(ScriptedStore, "settle_pipeline")
    monkeypatch.delattr(SqliteStore, "settle_pipeline")
    store = ScriptedStore(str(tmp_path / "fallback.db"))
    assert getattr(store, "settle_pipeline", None) is None

    report = Runner(store=store, handle_signals=False).run([spec])

    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done, row.error_type) == ("succeeded", 2, None)
    # the artifact was already final, so the fallback is the terminal write plus the cleanup
    assert "mark_final" not in store.calls
    assert "finish_pipeline:succeeded" in store.calls
    assert "settle_pipeline:succeeded" not in store.calls
    store.close()


def test_finality_is_marked_before_the_terminal_state(tmp_path):
    store = ScriptedStore(str(tmp_path / "order.db"))
    spec = TEMPLATE.bind(SEED)

    Runner(store=store, handle_signals=False).run([spec])

    assert store.calls.count("mark_final") == 1
    assert store.calls.index("mark_final") < store.calls.index("finish_pipeline:succeeded")
    # the fix for the torn forward direction: no durable running row ever carries cursor == n_tasks
    assert ("running", TEMPLATE.n_tasks) not in store.upserts
    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("succeeded", TEMPLATE.n_tasks)
    store.close()


def test_a_failed_finality_mark_leaves_a_repairable_row(tmp_path):
    path = str(tmp_path / "mark-failed.db")
    store = ScriptedStore(path)
    store.fail_mark_final = True
    spec = TEMPLATE.bind(SEED)

    Runner(store=store, handle_signals=False).run([spec])

    row = store.get_pipeline(spec.pipeline_id)
    assert (row.state, row.n_tasks_done) == ("failed", 2)
    assert row.error_type == "RuntimeError"
    store.close()

    # the row is the repairable shape, so the next run settles it without re-running a task
    store = ScriptedStore(path)
    report = Runner(store=store, handle_signals=False).run([spec])
    assert CALLS == {"fetch": 1, "ask": 1}
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    artifacts = store.artifacts(spec.pipeline_id)
    assert [a.is_final for a in artifacts] == [False, False, True]
    store.close()
