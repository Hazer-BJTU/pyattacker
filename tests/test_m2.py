"""M2 end to end, through `Runner`: scheduling and persistence behaviour.

Two claims are checked here that unit tests cannot make:

1. **A retry backoff does not hold a worker slot.** With `concurrency=1` and one pipeline
   parked for a retry, another pipeline must still complete in the meantime — visible in the
   structured event log, so the assertion does not depend on timing luck.
2. **Batched facts still reach disk.** Write-behind buffers attempts/events, but a finished run
   must leave every one of them in the file, and a stop must record parked pipelines as
   interrupted rather than dropping them.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time

from helpers import run

from pyattacker import RetryableError, Retrying, Runner, pipeline, task
from pyattacker.store.writebehind import WriteBehindStore


@task("m2.flaky_once", retry=Retrying(max_attempts=2, base=0.2, cap=0.2, jitter="none"))
def flaky_once(seed, ctx):
    if ctx.attempt == 1:
        raise RetryableError("first attempt fails on purpose")
    return {"ok": seed}


@task("m2.fast")
def fast(seed, ctx):
    return {"ok": seed}


@task("m2.slow_retry", retry=Retrying(max_attempts=3, base=30.0, cap=30.0, jitter="none"))
def slow_retry(seed, ctx):
    raise RetryableError("always fails, with a long backoff")


def _event_order(store, limit: int = 500) -> list[tuple[str, str, int | None]]:
    """(pipeline_id, kind, attempt) in write order — the structured log is the ground truth."""
    out = []
    for event in store.events(limit=limit):
        attempt = event.data.get("attempt") if isinstance(event.data, dict) else None
        out.append((event.pipeline_id or "", event.kind, attempt))
    return out


def test_backoff_does_not_hold_a_worker_slot():
    runner = Runner(store=":memory:", concurrency=1, handle_signals=False)
    slow = next(iter(pipeline("m2-slow", flaky_once).map([{"q": "a"}])))
    quick = next(iter(pipeline("m2-quick", fast).map([{"q": "b"}])))

    started = time.monotonic()
    report = runner.run([slow, quick])
    elapsed = time.monotonic() - started

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 2}
    assert elapsed >= 0.2, "the retry backoff really happened"
    assert elapsed < 2.0, "and the other pipeline did not have to wait it out"

    order = _event_order(runner.store)
    index_of = {(pid, kind): i for i, (pid, kind, _) in enumerate(order)}
    quick_done = index_of[(quick.pipeline_id, "task.succeeded")]
    defer = order.index((slow.pipeline_id, "pipeline.deferred", 1))
    retry_done = index_of[(slow.pipeline_id, "task.succeeded")]

    # ★ the ordering that proves the worker was released: the retry was scheduled, the *other*
    # pipeline ran to completion, and only then did the parked pipeline come back.
    assert defer < quick_done < retry_done
    assert any(kind == "task.retry_scheduled" for pid, kind, _ in order if pid == slow.pipeline_id)


def test_deferred_pipeline_survives_a_stop_and_is_resumable(tmp_path):
    async def _case():
        db = str(tmp_path / "deferred.db")
        runner = Runner(store=db, concurrency=1, handle_signals=False)
        try:
            spec = next(iter(pipeline("m2-stop", slow_retry).map([{"q": "x"}])))
            running = asyncio.create_task(runner.run_async([spec]))

            # wait until the pipeline is parked in the delay queue, then stop the run
            for _ in range(400):
                if runner.stats()["delayed_pipelines"] == 1:
                    break
                await asyncio.sleep(0.01)
            assert runner.stats()["delayed_pipelines"] == 1, "pipeline should be parked for its backoff"
            runner.stop("test")
            report = await asyncio.wait_for(running, 5.0)

            assert report.status == "interrupted"
            assert report.stats["pipelines"]["by_state"] == {"interrupted": 1}
            record = runner.store.get_pipeline(spec.pipeline_id)
            assert record.state == "interrupted"
            kinds = [e.kind for e in runner.store.events(pipeline_id=spec.pipeline_id, limit=100)]
            assert "pipeline.deferred_interrupted" in kinds
            # exactly one attempt was recorded before the stop, and it is still on disk
            # (read through a second connection: WAL lets a reader look while the writer is open)
            conn = sqlite3.connect(db)
            try:
                rows = conn.execute(
                    "SELECT attempt_no, outcome FROM attempts WHERE pipeline_id=?",
                    (spec.pipeline_id,),
                ).fetchall()
            finally:
                conn.close()
            assert rows == [(1, "failed")]
        finally:
            runner.close()

    run(_case())


def test_write_behind_reaches_disk_for_a_finished_run(tmp_path):
    db = tmp_path / "batched.db"
    runner = Runner(store=str(db), concurrency=4, handle_signals=False)
    try:
        assert isinstance(runner.store, WriteBehindStore)
        report = runner.run(pipeline("m2-batch", fast).map([{"i": i} for i in range(6)]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 6}
        assert runner.store.pending == 0, "a finished run must not leave facts in the buffer"
        assert runner.store.flushes >= 1
    finally:
        runner.close()

    conn = sqlite3.connect(str(db))
    try:
        attempts = conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
        events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        pipelines = conn.execute("SELECT COUNT(*) FROM pipelines").fetchone()[0]
    finally:
        conn.close()
    assert (attempts, pipelines) == (6, 6)
    assert events > 0


def test_write_behind_can_be_disabled(tmp_path):
    db = tmp_path / "sync.db"
    runner = Runner(store=str(db), concurrency=2, handle_signals=False, write_behind=False)
    try:
        assert not isinstance(runner.store, WriteBehindStore)
        report = runner.run(pipeline("m2-sync", fast).map([{"i": i} for i in range(3)]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 3}
    finally:
        runner.close()

    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 3
    finally:
        conn.close()


def test_run_manifest_records_that_batching_was_on(tmp_path):
    runner = Runner(store=str(tmp_path / "manifest.db"), handle_signals=False)
    try:
        report = runner.run(pipeline("m2-manifest", fast).map([{"i": 0}]))
        run_record = runner.store.get_run(report.run_id)
        assert run_record.config["write_behind"] is True
        assert run_record.config["concurrency"] == runner.config.concurrency
    finally:
        runner.close()


def test_store_stats_expose_buffering_only_when_batching():
    plain = Runner(store=":memory:", handle_signals=False)
    assert plain.stats()["buffered"] is None

    batched = Runner(store=":memory:", write_behind=True, handle_signals=False)
    try:
        assert batched.stats()["buffered"]["pending"] == 0
    finally:
        batched.close()
