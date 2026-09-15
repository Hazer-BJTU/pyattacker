"""Delayed continuations — waiting without occupying a worker.

The problem this solves: a retry backoff must *not* hold a concurrency slot.
If a worker simply `await sleep(30)`-ed through a backoff, that slot would be dead
weight for 30 seconds, and `concurrency` would stop meaning "how many attempts run
at once".

So the runner never sleeps in a worker. When an attempt decides to retry, the worker
hands the pipeline state to a :class:`DelayQueue` and immediately picks up other work;
a single pump task moves the state back into the work queue once its timer expires.

Timers are driven through the injectable :class:`Clock`, which keeps tests
deterministic: with a fake clock the pump advances instantly instead of sleeping.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

__all__ = ["DelayQueue", "interruptible_sleep"]

T = TypeVar("T")


@dataclass(order=True)
class _Entry(Generic[T]):
    ready_at: float
    seq: int
    item: T = field(compare=False)


async def interruptible_sleep(clock: Any, seconds: float, wake: asyncio.Event) -> bool:
    """Sleep for ``seconds`` on ``clock``, returning early if ``wake`` is set.

    Returns ``True`` if we were woken early. Racing the clock's own sleep against a
    wakeup event gives both behaviours we need:

    * real clock -> a newly pushed, sooner item can cut a long wait short;
    * fake clock -> ``clock.sleep`` advances virtual time and returns at once, so tests
      stay fast and deterministic.
    """
    if seconds <= 0:
        return False
    sleep_task = asyncio.ensure_future(clock.sleep(seconds))
    wake_task = asyncio.ensure_future(wake.wait())
    try:
        done, _ = await asyncio.wait({sleep_task, wake_task}, return_when=asyncio.FIRST_COMPLETED)
        return wake_task in done and sleep_task not in done
    finally:
        for task in (sleep_task, wake_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(sleep_task, wake_task, return_exceptions=True)


class DelayQueue(Generic[T]):
    """A min-heap of items that become available at a monotonic deadline.

    Items are only removed from the heap **after** they have been handed to the sink,
    so a cancellation mid-handoff never loses work silently: whatever is left can be
    recovered with :meth:`drain` (the runner uses it to mark deferred pipelines as
    interrupted at shutdown).
    """

    def __init__(self, *, clock: Any = None, name: str = "delay") -> None:
        if clock is None:
            raise TypeError("DelayQueue requires a clock (pass runner.clock)")
        self.clock = clock
        self.name = name
        self._heap: list[_Entry[T]] = []
        self._counter = itertools.count()
        self._wake = asyncio.Event()
        self.pushed = 0
        self.popped = 0
        self.late = 0  # how many items were handed over past their deadline

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def push(self, item: T, delay: float = 0.0) -> float:
        """Schedule ``item`` to be released ``delay`` seconds from now. Returns its deadline."""
        ready_at = self.clock.now() + max(0.0, delay)
        heapq.heappush(self._heap, _Entry(ready_at, next(self._counter), item))
        self.pushed += 1
        self._wake.set()
        return ready_at

    def peek_deadline(self) -> float | None:
        return self._heap[0].ready_at if self._heap else None

    def drain(self) -> list[T]:
        """Remove and return every pending item (used at shutdown)."""
        items = [entry.item for entry in sorted(self._heap)]
        self._heap.clear()
        return items

    def stats(self) -> dict[str, Any]:
        deadline = self.peek_deadline()
        return {
            "pending": len(self._heap),
            "pushed": self.pushed,
            "popped": self.popped,
            "late": self.late,
            "next_in_s": None if deadline is None else round(deadline - self.clock.now(), 4),
        }

    async def pump(self, sink: "asyncio.Queue[Any]") -> None:
        """Release ready items into ``sink`` until cancelled.

        Cancellation is the shutdown signal; callers should afterwards use :meth:`drain`
        to recover anything still pending.
        """
        while True:
            if not self._heap:
                self._wake.clear()
                await self._wake.wait()
                continue
            delay = self._heap[0].ready_at - self.clock.now()
            if delay > 0:
                self._wake.clear()
                await interruptible_sleep(self.clock, delay, self._wake)
                continue
            entry = self._heap[0]
            await sink.put(entry.item)  # hand off first...
            heapq.heappop(self._heap)  # ...then remove, so cancellation cannot drop work
            self.popped += 1
            if self.clock.now() > entry.ready_at:
                self.late += 1
