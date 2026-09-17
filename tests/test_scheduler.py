"""Infrastructure added in M2: delayed continuations (scheduler.py) and write-behind batching (store/writebehind.py).

Both are pure infrastructure, so they are tested directly rather than through a run:
the runner-level behaviour (a worker is freed during backoff, buffered facts survive a run)
is covered in test_runner.py.
"""

from __future__ import annotations

import asyncio

import pytest
from helpers import FakeClock, run

from pyattacker import Artifact, MemoryStore
from pyattacker.scheduler import DelayQueue, interruptible_sleep
from pyattacker.store.base import AttemptRecord, EventRecord, PagedStore, PipelineRecord, TaskRecord
from pyattacker.store.writebehind import WriteBehindStore, wrap_write_behind


def _attempt(attempt_no: int = 1, **kwargs) -> AttemptRecord:
    base = {
        "pipeline_id": "p1",
        "run_id": "r1",
        "task_run_id": "p1:0",
        "task_name": "ask",
        "seq": 0,
        "attempt_no": attempt_no,
        "started_at": 0.0,
    }
    return AttemptRecord(**{**base, **kwargs})


def _artifact(seq: int = 0) -> Artifact:
    return Artifact(
        id=f"p1:{seq}",
        pipeline_id="p1",
        task_name="ask",
        seq=seq,
        type_name="dict",
        codec="json",
        digest="d",
        size=2,
        payload=b"{}",
        created_at=0.0,
    )


# ------------------------------------------------------------------ DelayQueue
def test_items_are_released_in_deadline_order():
    async def _case():
        clock = FakeClock()
        queue = DelayQueue(clock=clock)
        sink: asyncio.Queue = asyncio.Queue()
        pump = asyncio.create_task(queue.pump(sink))
        try:
            queue.push("late", delay=10.0)
            queue.push("soon", delay=0.5)
            queue.push("now", delay=0.0)
            assert [await asyncio.wait_for(sink.get(), 1.0) for _ in range(3)] == [
                "now",
                "soon",
                "late",
            ]
            assert queue.popped == 3
            assert len(queue) == 0
        finally:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)

    run(_case())


def test_pushing_into_an_empty_queue_wakes_the_pump():
    async def _case():
        clock = FakeClock()
        queue = DelayQueue(clock=clock)
        sink: asyncio.Queue = asyncio.Queue()
        pump = asyncio.create_task(queue.pump(sink))
        try:
            await asyncio.sleep(0)  # let the pump park on an empty heap
            assert len(queue) == 0
            queue.push("first", delay=0.0)
            assert await asyncio.wait_for(sink.get(), 1.0) == "first"
        finally:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)

    run(_case())


def test_a_sooner_item_cuts_a_longer_wait_short():
    """Real-clock path: a 30s timer must not delay an item pushed a moment later with a 10ms timer."""

    async def _case():
        clock = _RealClock()
        queue = DelayQueue(clock=clock)
        sink: asyncio.Queue = asyncio.Queue()
        queue.push("slow", delay=30.0)
        pump = asyncio.create_task(queue.pump(sink))
        try:
            await asyncio.sleep(0.01)  # let the pump start sleeping on the 30s timer
            queue.push("fast", delay=0.01)
            first = await asyncio.wait_for(sink.get(), 2.0)
            assert first == "fast"
        finally:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)

    run(_case())


def test_cancelling_the_pump_does_not_lose_pending_items():
    async def _case():
        clock = FakeClock()
        queue = DelayQueue(clock=clock)
        sink: asyncio.Queue = asyncio.Queue()
        queue.push("a", delay=5.0)
        queue.push("b", delay=1.0)
        pump = asyncio.create_task(queue.pump(sink))
        await asyncio.sleep(0)  # let it observe the heap and start waiting
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)
        # nothing was handed to the sink, and nothing vanished
        assert sink.empty()
        assert sorted(queue.drain()) == ["a", "b"]
        assert len(queue) == 0

    run(_case())


def test_drain_reports_the_next_deadline():
    clock = FakeClock()
    queue = DelayQueue(clock=clock)
    assert queue.stats()["next_in_s"] is None
    queue.push("x", delay=3.0)
    assert queue.stats()["next_in_s"] == pytest.approx(3.0)
    assert queue.peek_deadline() == pytest.approx(3.0)
    assert len(queue) == 1
    assert queue.drain() == ["x"]
    assert queue.stats()["pending"] == 0


def test_interruptible_sleep_returns_early_only_when_woken():
    async def _case():
        clock = _RealClock()
        wake = asyncio.Event()

        async def _wake_soon():
            await asyncio.sleep(0.01)
            wake.set()

        waker = asyncio.create_task(_wake_soon())
        try:
            assert await interruptible_sleep(clock, 5.0, wake) is True
        finally:
            await waker

        wake.clear()
        assert await interruptible_sleep(clock, 0.01, wake) is False
        assert await interruptible_sleep(clock, 0.0, wake) is False

    run(_case())


# ------------------------------------------------------------ WriteBehindStore
def test_attempts_and_events_stay_buffered_until_the_batch_is_full():
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=3, flush_interval=999.0)
    store.record_attempt(_attempt(1))
    store.record_attempt(_attempt(2))
    assert (len(inner.attempts()), store.pending) == (0, 2)
    store.emit_event(EventRecord(ts=0.0, kind="task.failed"))  # third buffered fact hits the batch
    assert len(inner.attempts()) == 2
    assert len(inner.all_events()) == 1
    assert store.pending == 0
    assert store.flushes == 1


def test_read_apis_flush_first():
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=64, flush_interval=999.0)
    store.upsert_pipeline(PipelineRecord(pipeline_id="p1", run_id="r1", name="n", key="k"))
    store.record_attempt(_attempt(1, outcome="succeeded"))
    store.emit_event(EventRecord(ts=0.0, kind="task.succeeded", pipeline_id="p1"))
    assert inner.attempts() == []

    rows = store.pipelines(run_id="r1")
    assert len(rows) == 1
    assert len(inner.attempts()) == 1
    assert len(store.events(pipeline_id="p1")) == 1
    assert store.pending == 0


def test_flush_interval_triggers_on_the_next_buffered_write():
    clock = FakeClock()
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=1000, flush_interval=1.0, clock=clock)
    store.record_attempt(_attempt(1))
    assert inner.attempts() == []
    clock.t += 1.5  # time passes between writes
    store.record_attempt(_attempt(2))
    assert len(inner.attempts()) == 2


def test_state_writes_are_synchronous():
    """Checkpoints and artifacts must never sit in a buffer — resume reads them back."""
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=1000, flush_interval=999.0)
    store.upsert_pipeline(PipelineRecord(pipeline_id="p1", run_id="r1", name="n", key="k"))
    store.put_artifact(_artifact(0))
    store.record_task(
        TaskRecord(task_run_id="p1:0", pipeline_id="p1", run_id="r1", name="ask", seq=0)
    )
    assert inner.get_pipeline("p1") is not None
    assert inner.get_artifact("p1", 0) is not None
    assert store.pending == 0
    assert inner.get_artifact("p1", 0).payload == b"{}"


def test_ids_are_assigned_when_the_batch_is_flushed():
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=64, flush_interval=999.0)
    first = store.record_attempt(_attempt(1))
    second = store.record_attempt(_attempt(2))
    assert (first.attempt_id, second.attempt_id) == (None, None)
    store.flush()
    assert (first.attempt_id, second.attempt_id) == (1, 2)


def test_paged_iterators_flush_first():
    """The paged reads are delegated explicitly, so a buffered fact can never be missed.

    ``__getattr__`` would forward ``iter_events`` to the inner store without the flush that every
    list API performs, and an export would silently lose the last batch.
    """
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=64, flush_interval=999.0)
    store.emit_event(EventRecord(ts=0.0, kind="task.succeeded", pipeline_id="p1", run_id="r1"))
    store.record_attempt(_attempt(1, outcome="succeeded"))
    assert (inner.all_events(), inner.attempts()) == ([], [])

    assert [event.kind for event in store.iter_events()] == ["task.succeeded"]
    assert [item.outcome for item in store.iter_attempts()] == ["succeeded"]
    assert store.pending == 0
    assert isinstance(store, PagedStore)


def test_finish_run_and_close_never_leave_facts_buffered():
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=1000, flush_interval=999.0)
    store.record_attempt(_attempt(1))
    store.finish_run("r1", "completed")
    assert len(inner.attempts()) == 1

    store.record_attempt(_attempt(2))
    store.close()
    assert len(inner.attempts()) == 2


def test_wrapping_twice_is_idempotent():
    inner = MemoryStore()
    once = wrap_write_behind(inner, batch_size=8)
    twice = wrap_write_behind(once, batch_size=8)
    assert once is twice


def test_unknown_attributes_pass_through_to_the_inner_store():
    inner = MemoryStore()
    store = WriteBehindStore(inner, batch_size=8)
    # a store-specific extra that is not part of the Store protocol
    assert store.export_rows is not None
    assert callable(store.__getattr__("all_events"))
    assert store.journal == inner.journal


class _RealClock:
    """Real monotonic clock, used where the fake one would freeze the pump."""

    @staticmethod
    def now() -> float:
        return asyncio.get_running_loop().time()

    @staticmethod
    async def sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)
