"""Worker liveness: a worker that dies outside its own handlers must not hang the run.

Issue #44. Run completion used to be counter-based only (``pipelines_done`` against
``pipelines_admitted``), so a ``BaseException`` that escaped the worker killed the task and left
the run waiting for a condition that could never become true: no error, no exit code, no terminal
row and no event. These tests pin what the fix promises -- a bounded return, a terminal row, a
recorded cause, and an explicit run-level error -- and the paths that must *not* change: an
ordinary internal ``Exception``, cancellation, a pipeline that was already terminal, and a healthy
run.

Every case is bounded with ``asyncio.wait_for``: the regression being tested is a hang, so a
regression has to fail fast instead of parking CI.
"""

from __future__ import annotations

import asyncio

import pytest
from helpers import run

from pyattacker import (
    MemoryStore,
    Retrying,
    Runner,
    SqliteStore,
    StoreUnavailable,
    WorkerCrashed,
    flaky,
    open_store,
    pipeline,
    task,
)
from pyattacker import runner as runner_module

BOUND = 5.0


class WorkerDied(BaseException):
    """The issue's reproducer: a direct ``BaseException`` subclass, so no ``except Exception``
    anywhere in the worker (or in the framework's task handling) can contain it."""


@task("liveness.echo")
def echo(seed, ctx):
    return {"i": seed["i"]}


class DyingStore(MemoryStore):
    """A store whose final-write hook dies the way a broken third-party backend would."""

    def __init__(self, exc: BaseException) -> None:
        super().__init__()
        self.exc = exc

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        raise self.exc


def _spec(name: str, i: int = 0):
    return next(iter(pipeline(name, echo).map([{"i": i}])))


def _kinds(store: MemoryStore, run_id: str | None) -> list[str]:
    return [event.kind for event in store.events(run_id=run_id, limit=100)]


def _raiser(exc: BaseException):
    """A hook that raises ``exc`` -- patched onto a live store, which is how a third-party store
    failure reaches framework code without the framework being able to predict it."""

    def boom(*args, **kwargs):
        raise exc

    return boom


def test_a_base_exception_from_a_store_hook_is_recorded_and_raised():
    async def _case():
        store = DyingStore(WorkerDied("framework-level crash inside a store call"))
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        spec = _spec("liveness-crash")

        with pytest.raises(WorkerCrashed) as caught:
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        # the run-level error names the pipeline it lost and keeps the real fault as its cause
        assert caught.value.pipeline_id == spec.pipeline_id
        assert "WorkerDied" in str(caught.value)
        assert isinstance(caught.value.__cause__, WorkerDied)

        # the pipeline is terminal, and the crash is what it records -- not a leftover "running"
        record = store.get_pipeline(spec.pipeline_id)
        assert record.state == "failed"
        assert record.error_type == "WorkerCrashed"
        assert "WorkerDied" in record.error_message
        assert "WorkerDied" in record.traceback

        # the event names the in-flight pipeline, the exception and its traceback
        events = store.events(run_id=runner.run_id, limit=100)
        crashed = [event for event in events if event.kind == "runner.worker_crashed"]
        assert len(crashed) == 1
        assert crashed[0].pipeline_id == spec.pipeline_id
        assert crashed[0].data["error"] == "WorkerDied: framework-level crash inside a store call"
        assert "WorkerDied" in crashed[0].data["traceback"]
        assert crashed[0].data["during_shutdown"] is False

        # and the run itself is closed rather than abandoned mid-flight
        assert store.get_run(runner.run_id).status == "interrupted"
        assert "run.finished" in _kinds(store, runner.run_id)

    run(_case())


def test_a_worker_death_cannot_strand_the_producer_on_a_full_queue():
    """The other half of the same hang: the producer parks in ``queue.put`` once the queue is full,
    and a dead worker can never drain it. Releasing the completion wait is not enough -- the
    producer has to be released too, or the run never reaches its wind-down at all.
    """

    async def _case():
        store = DyingStore(WorkerDied("died while the producer was still feeding"))
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        # 8 specs, a queue of capacity 2 and one worker: the producer is still admitting when the
        # worker dies, and the specs behind the first one can never be picked up.
        specs = list(pipeline("liveness-full-queue", echo).map([{"i": i} for i in range(8)]))

        with pytest.raises(WorkerCrashed):
            await asyncio.wait_for(runner.run_async(specs), timeout=BOUND)

        # only the pipeline that was actually in flight ever got a row; nothing was left "running"
        rows = [store.get_pipeline(spec.pipeline_id) for spec in specs]
        assert [row.state for row in rows if row is not None] == ["failed"]

    run(_case())


def test_a_worker_death_after_the_pipeline_finished_does_not_rewrite_the_row():
    """The death can land in worker housekeeping, after the pipeline is already terminal. Blaming a
    pipeline that succeeded would corrupt history, so the row keeps the state it earned.
    """

    async def _case():
        store = MemoryStore()
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        spec = _spec("liveness-after-finish")
        real_drive = runner._drive

        async def drive_then_die(state):
            await real_drive(state)
            raise WorkerDied("died after the pipeline was recorded as succeeded")

        runner._drive = drive_then_die

        with pytest.raises(WorkerCrashed):
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        record = store.get_pipeline(spec.pipeline_id)
        assert record.state == "succeeded"  # not rewritten into a failure
        assert record.error_type is None
        assert "pipeline.succeeded" in _kinds(store, runner.run_id)

    run(_case())


@pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
def test_an_interrupt_that_kills_the_loop_still_records_the_pipeline(exc_type, tmp_path, monkeypatch):
    """asyncio re-raises ``KeyboardInterrupt``/``SystemExit`` out of the task, which tears the loop
    down before any done-callback can run -- so the primary observation point never sees them. The
    worker's own guard has to record them, while the interrupt itself still propagates unchanged:
    a Ctrl-C must keep stopping the process.

    The interrupt therefore surfaces from ``asyncio.run`` rather than from the awaited coroutine, so
    the assertions live outside the loop that died -- and they read the file back through a fresh
    connection, on a store that batches events, so "the record survived" means the crash flushed it
    rather than that nothing was buffered.
    """
    db = tmp_path / f"{exc_type.__name__}.db"
    real_open_store = runner_module.open_store

    def open_with_dying_hook(spec, **kwargs):
        store = real_open_store(spec, **kwargs)
        store.mark_final = _raiser(exc_type("store hook interrupted"))
        return store

    monkeypatch.setattr(runner_module, "open_store", open_with_dying_hook)
    runner = Runner(store=str(db), write_behind=True, concurrency=1, handle_signals=False)
    spec = _spec(f"liveness-{exc_type.__name__}")

    async def _case():
        await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

    with pytest.raises(exc_type):
        run(_case())

    reopened = SqliteStore(str(db))
    record = reopened.get_pipeline(spec.pipeline_id)
    assert (record.state, record.error_type) == ("failed", "WorkerCrashed")
    assert "runner.worker_crashed" in [event.kind for event in reopened.events(limit=50)]
    reopened.close()


def test_a_worker_death_during_a_stop_is_recorded_as_interrupted():
    """A death while the run is already winding down is still recorded -- as ``interrupted``, which
    is what every other pipeline in a stopping run becomes, rather than as a fresh failure.
    """

    async def _case():
        store = DyingStore(WorkerDied("died while the run was already stopping"))
        runner = Runner(store=store, concurrency=1, handle_signals=False)

        @task("liveness.stop-then-die")
        def stop_then_die(seed, ctx):
            runner.stop("user")
            return {"i": seed["i"]}

        spec = next(iter(pipeline("liveness-stop", stop_then_die).map([{"i": 0}])))

        with pytest.raises(WorkerCrashed):
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        assert store.get_pipeline(spec.pipeline_id).state == "interrupted"
        crashed = [
            event
            for event in store.events(run_id=runner.run_id, limit=100)
            if event.kind == "runner.worker_crashed"
        ]
        assert crashed[0].data["during_shutdown"] is True

    run(_case())


def test_a_worker_death_before_the_row_exists_is_still_recorded():
    """The internal-error path creates a row for a pipeline that never got one; a crash before the
    row is written has to do the same, or the pipeline silently vanishes from the report.
    """

    async def _case():
        store = MemoryStore()
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        spec = _spec("liveness-no-row")

        def boom_open(spec, run_id):
            raise WorkerDied("died before the pipeline row was written")

        runner._open_pipeline = boom_open

        with pytest.raises(WorkerCrashed) as caught:
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        assert caught.value.pipeline_id == spec.pipeline_id
        record = store.get_pipeline(spec.pipeline_id)
        assert record is not None
        assert (record.state, record.error_type, record.n_tasks_total) == (
            "failed",
            "WorkerCrashed",
            1,
        )
        assert "runner.worker_crashed" in _kinds(store, runner.run_id)

    run(_case())


def test_peer_workers_are_stopped_when_one_dies():
    """Losing a worker stops the run: a peer must not keep going, and the pipeline it was holding
    must not be left ``running`` either.
    """

    async def _case():
        store = DyingStore(WorkerDied("one worker died"))
        runner = Runner(store=store, concurrency=2, handle_signals=False)
        peer_started = asyncio.Event()

        @task("liveness.peer")
        async def peer(seed, ctx):
            peer_started.set()
            await asyncio.Event().wait()  # runs until cancelled

        @task("liveness.crash-after-peer")
        async def crash_after_peer(seed, ctx):
            # the crashing pipeline waits for the peer to be in flight, so "the peer was stopped"
            # is a statement about a worker that really was running
            await asyncio.wait_for(peer_started.wait(), timeout=BOUND)
            return {"i": seed["i"]}

        crashing = next(iter(pipeline("liveness-peer-crash", crash_after_peer).map([{"i": 0}])))
        blocked = next(iter(pipeline("liveness-peer-blocked", peer).map([{"i": 0}])))

        with pytest.raises(WorkerCrashed):
            await asyncio.wait_for(runner.run_async([crashing, blocked]), timeout=BOUND)

        assert store.get_pipeline(crashing.pipeline_id).state == "failed"
        assert store.get_pipeline(blocked.pipeline_id).state == "interrupted"  # the peer was cut off
        assert len([k for k in _kinds(store, runner.run_id) if k == "runner.worker_crashed"]) == 1

    run(_case())


def test_a_crash_that_cannot_be_persisted_takes_the_fatal_path():
    """If the store cannot record the crash either, retrying it would just fail again: the run takes
    the existing ``StoreUnavailable`` path, and the crash that started it stays visible as the cause.

    The second failure is a ``BaseException`` on purpose: the store that killed the worker is the
    prime suspect, so the crash handler must not be the one thing a broken store can still take down.
    """

    async def _case():
        class BrokenRecording(DyingStore):
            def finish_pipeline(self, *args, **kwargs):
                raise WorkerDied("the store is broken too")

        store = BrokenRecording(WorkerDied("framework-level crash inside a store call"))
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        spec = _spec("liveness-fatal")

        with pytest.raises(StoreUnavailable) as caught:
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        assert "could not be persisted" in str(caught.value)
        assert "the store is broken too" in str(caught.value)
        assert isinstance(caught.value.__cause__, WorkerDied)

    run(_case())


def test_an_ordinary_internal_error_still_takes_the_recovery_path():
    """The supervision must not swallow the existing recovery path: an internal ``Exception`` is
    still recorded on the pipeline, and the run still returns a report instead of raising.
    """

    async def _case():
        store = MemoryStore()
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        spec = _spec("liveness-internal-error")

        def boom_open(spec, run_id):
            raise RuntimeError("framework surprise")

        runner._open_pipeline = boom_open

        report = await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        assert report.status == "completed"
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert store.get_pipeline(spec.pipeline_id).error_type == "RuntimeError"
        kinds = _kinds(store, runner.run_id)
        assert "runner.internal_error" in kinds
        assert "runner.worker_crashed" not in kinds

    run(_case())


def test_a_cancelled_worker_keeps_its_cancellation_semantics():
    """A deliberate cancel is not a crash: the worker still marks its pipeline interrupted and the
    caller still gets ``CancelledError``, with no worker-crash record left behind.
    """

    async def _case():
        store = MemoryStore()
        runner = Runner(store=store, concurrency=1, handle_signals=False)

        @task("liveness.hold")
        async def hold(seed, ctx):
            await asyncio.Event().wait()

        spec = next(iter(pipeline("liveness-cancel", hold).map([{"i": 0}])))
        running = asyncio.create_task(runner.run_async([spec]))
        for _ in range(200):
            await asyncio.sleep(0)
            if store.get_pipeline(spec.pipeline_id) is not None:
                break

        running.cancel()
        results = await asyncio.gather(running, return_exceptions=True)

        # the cancellation reaches the caller instead of being swallowed by the supervision
        assert isinstance(results[0], asyncio.CancelledError)
        assert store.get_pipeline(spec.pipeline_id).state == "interrupted"
        assert "runner.worker_crashed" not in _kinds(store, runner.run_id)

    run(_case())


def test_a_crash_after_a_durable_terminal_write_keeps_that_row(tmp_path):
    """The persisted row decides, not the worker's in-memory state.

    A pipeline that retried comes back through the delay queue as an ``_RunState``, and
    ``finish_pipeline`` is not required to write back into that record -- SQLite updates the row
    only, so ``state.record.state`` still says ``"running"`` after a successful terminal write. A
    crash immediately after that write (here: the success event) must not turn a durable
    ``succeeded`` row into ``failed / WorkerCrashed``.
    """
    db = tmp_path / "durable-terminal.db"
    retried = flaky(1, retry=Retrying(max_attempts=2, base=0.01, jitter="none"))
    spec = next(iter(pipeline("liveness-durable", retried).map([{"i": 0}])))

    async def _case():
        store = open_store(str(db))
        runner = Runner(store=store, concurrency=1, handle_signals=False)
        real_emit = runner._emit

        def emit_then_die(kind, **kwargs):
            real_emit(kind, **kwargs)
            if kind == "pipeline.succeeded":
                raise WorkerDied("died right after the terminal write")

        runner._emit = emit_then_die

        with pytest.raises(WorkerCrashed):
            await asyncio.wait_for(runner.run_async([spec]), timeout=BOUND)

        # the pipeline really did come back as a parked-then-requeued _RunState, so the crash was
        # handled from an in-memory record the store never wrote back into
        assert [event.kind for event in store.events(limit=50)].count("pipeline.deferred") == 1

    run(_case())

    # ... and the row it earned is what is on disk, read back through a fresh connection
    reopened = SqliteStore(str(db))
    record = reopened.get_pipeline(spec.pipeline_id)
    assert (record.state, record.error_type, record.n_tasks_done) == ("succeeded", None, 1)
    kinds = [event.kind for event in reopened.events(limit=50)]
    assert "pipeline.succeeded" in kinds  # the terminal write happened before the crash
    assert "runner.worker_crashed" in kinds  # ... and the crash is still recorded
    reopened.close()
