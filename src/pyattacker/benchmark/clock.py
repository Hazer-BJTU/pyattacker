"""Simulated time for the benchmark harness.

Why not the wall clock, and why not the test suite's `FakeClock`:

* The wall clock cannot be used because the scenarios worth measuring span minutes of provider time
  (a capacity cycle, a failure storm, a quota window). Waiting for them for real would make every
  algorithm comparison unusably slow, which is why a full benchmark has a wall-clock budget rather
  than a simulated one.
* `tests/helpers.py`'s `FakeClock` cannot be used because it advances time *inside* the sleeper:
  ``t += seconds`` and return. That is exactly right for the unit tests it was written for (one
  coroutine, one controllable number) and exactly wrong here — with 32 workers sleeping 1s at the
  same time it lands at 32 simulated seconds, and every throughput, latency and utilization number
  derived from it is off by the concurrency factor.

So this clock keeps a timer heap and moves the whole simulation to the next deadline, but only when
the harness reports that every worker is parked. `sleep()` is therefore a real await on a future:
the caller blocks, the driver decides when the deadline arrives.

The advance rule, and why it is sound
-------------------------------------
Time advances only when all three hold:

1. at least one timer is pending, and
2. every registered worker is parked — either inside `sleep()` or inside `blocked()`, the harness's
   marker around an await that only a release or the passage of time can end (a lease acquire), and
3. the event loop has run ``idle_ticks`` consecutive probe callbacks with no observable activity
   (no sleep registered or resolved, no worker started, finished or entered `blocked()`).

Condition 2 alone is not enough, because a coroutine can sit inside an await that is *already*
resolvable — a broadcast that fired while it was queued, a lease that was just released — and such a
coroutine is runnable without any time passing. Condition 3 closes that window: the probe re-arms
itself with ``loop.call_soon``, which appends to the tail of the ready queue, so any pending
continuation runs *before* the next probe tick. A run of ``idle_ticks`` ticks with no activity
therefore means nothing was pending. (Nothing here is preemptive: a coroutine's synchronous stretch
cannot be interrupted by a probe, so a worker in the middle of a step is never mistaken for parked.)

How much simulated time the workers see is unaffected by the tick count: it is a function of the
schedules, not of when the driver notices it is idle.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import time
from collections.abc import Coroutine, Iterator
from typing import Any

from ..errors import ConfigError

__all__ = ["ScaledClock", "VirtualClock"]


class _Timer:
    """One pending wake-up.

    `fired` is the agreement between the driver and the sleeper about who returns the worker's
    "parked" count: whoever sees it unset first owns the cleanup. Without it, a sleep that is
    cancelled *after* the driver resolved its future would be counted twice, or (before this
    existed) a cancelled sleep would leak a parked count forever and make the clock advance early.
    """

    __slots__ = ("counted", "deadline", "fired", "future")

    def __init__(self, deadline: float, future: asyncio.Future[None], *, counted: bool) -> None:
        self.deadline = deadline
        self.future = future
        self.counted = counted
        self.fired = False


class VirtualClock:
    """A clock whose ``now()`` only moves when the simulation has nothing else to do."""

    def __init__(self, *, idle_ticks: int = 16) -> None:
        if idle_ticks < 1:
            raise ValueError("idle_ticks must be at least 1")
        self.idle_ticks = idle_ticks
        self._now = 0.0
        self._timers: list[tuple[float, int, _Timer]] = []
        self._seq = itertools.count()
        self._workers: set[asyncio.Task[Any]] = set()
        self._parked = 0  # workers inside sleep()
        self._blocked = 0  # workers inside blocked() (an acquire, say)
        self._quiet = 0
        self._armed = False
        self.advances = 0  # how many times time actually moved; reported, and asserted on in tests

    # ------------------------------------------------------------------ clock protocol
    def now(self) -> float:
        """Simulated seconds since the start of the run."""
        return self._now

    async def sleep(self, seconds: float) -> None:
        """Wait ``seconds`` of simulated time. Zero or negative delays yield without moving the clock."""
        if seconds <= 0:
            # A zero delay still means "give the other workers a turn" — but it must not consume
            # time, or a retry_after=0 provider could push the simulation forward for free.
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        timer = _Timer(self._now + seconds, future, counted=asyncio.current_task() in self._workers)
        heapq.heappush(self._timers, (timer.deadline, next(self._seq), timer))
        if timer.counted:
            self._parked += 1
        self._activity()
        try:
            await future
        finally:
            # Exactly one return of the parked count per take, whichever side gets there first.
            if not timer.fired:
                if timer.counted:
                    self._parked -= 1
                for index, entry in enumerate(self._timers):
                    if entry[2] is timer:
                        del self._timers[index]
                        heapq.heapify(self._timers)
                        break
                self._activity()

    # ------------------------------------------------------------------ harness protocol
    def spawn(self, coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Create a worker task and register it. Workers must be spawned without awaiting in between."""
        task = asyncio.get_running_loop().create_task(coroutine)
        self.add_worker(task)
        task.add_done_callback(self.remove_worker)
        return task

    def add_worker(self, task: asyncio.Task[Any]) -> None:
        self._workers.add(task)
        self._activity()

    def remove_worker(self, task: asyncio.Task[Any]) -> None:
        self._workers.discard(task)
        self._activity()

    @contextlib.contextmanager
    def blocked(self) -> Iterator[None]:
        """Mark the current worker as inside an await that needs a release or more time to finish.

        Used around a lease acquire. The count is only observably raised while the worker is inside
        an await: the synchronous part of an acquire cannot be interleaved with a probe, and a
        context that returns without awaiting leaves no trace for the driver to see.
        """
        counted = asyncio.current_task() in self._workers
        if counted:
            self._blocked += 1
            self._activity()
        try:
            yield
        finally:
            if counted:
                self._blocked -= 1
                self._activity()

    # ------------------------------------------------------------------ observability
    @property
    def workers(self) -> int:
        return len(self._workers)

    @property
    def blocked_workers(self) -> int:
        """How many workers are parked on time or on a release right now."""
        return self._parked + self._blocked

    @property
    def pending(self) -> int:
        """Timers waiting to fire."""
        return len(self._timers)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"VirtualClock(now={self._now:.3f}, workers={self.workers}, "
            f"blocked={self.blocked_workers}, timers={self.pending}, advances={self.advances})"
        )

    # ------------------------------------------------------------------ the driver
    def _activity(self) -> None:
        self._quiet = 0
        self._arm()

    def _arm(self) -> None:
        if self._armed:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # outside a loop there is nothing to drive
            return
        self._armed = True
        loop.call_soon(self._probe)

    def _probe(self) -> None:
        self._armed = False
        if not self._timers:
            self._quiet = 0
            return
        if self.blocked_workers < self.workers:
            self._quiet = 0  # a worker is runnable; it will re-arm us when it parks
            return
        self._quiet += 1
        if self._quiet < self.idle_ticks:
            self._arm()
            return
        self._quiet = 0
        self._advance()

    def _advance(self) -> None:
        deadline = self._timers[0][0]
        if deadline > self._now:
            self._now = deadline
        self.advances += 1
        while self._timers and self._timers[0][0] <= self._now:
            _, _, timer = heapq.heappop(self._timers)
            timer.fired = True
            if timer.counted:
                self._parked -= 1  # runnable again: the probe must not advance a second time yet
            if not timer.future.done():
                timer.future.set_result(None)
        self._quiet = 0
        self._arm()


class ScaledClock:
    """A reference clock: real time, compressed by `speedup`.

    This exists to keep `VirtualClock` honest. It has none of the virtual clock's machinery — the
    event loop orders everything, sleeps are real (just short), and simulated time is real elapsed
    time multiplied by `speedup` — so it is correct by construction and slow by construction. Running
    the same scenario on both and comparing the headline metrics is how a bug in the advance rule
    would show up as a number that is off by the concurrency factor instead of as a hang.

    Its known distortion is the mirror image: CPU time is real time, so a run that spends a second of
    CPU at speedup 20 ages the simulation by 20 seconds. That is why it is a reference, not the
    default.
    """

    def __init__(self, speedup: float = 10.0) -> None:
        if speedup <= 0:
            # A CLI flag, so the same error type as every other impossible budget: the CLI prints the
            # message and exits 2 instead of showing a traceback.
            raise ConfigError(f"speedup must be positive, got {speedup}")
        self.speedup = speedup
        self._started = time.monotonic()

    def now(self) -> float:
        return (time.monotonic() - self._started) * self.speedup

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds) / self.speedup)

    def spawn(self, coroutine):  # same shape as VirtualClock, so the harness does not care
        return asyncio.get_running_loop().create_task(coroutine)

    @contextlib.contextmanager
    def blocked(self):  # nothing to count: the event loop already knows who is runnable
        yield
