"""The virtual clock: hand-computed timelines, and the two ways a simulator can lie.

Every assertion here is arithmetic a reader can check by hand. That is the point: a clock that
advances too eagerly produces numbers that *look* plausible (throughput, p99, utilization) while
being wrong by the concurrency factor, and no aggregate metric would ever reveal it.
"""

from __future__ import annotations

import asyncio

import pytest
from helpers import run

from pyattacker.benchmark.clock import VirtualClock


def test_concurrent_sleeps_overlap_instead_of_adding_up():
    """32 workers sleeping 1s at the same time cost 1 simulated second, not 32.

    This is the property the test suite's FakeClock deliberately does not have (it advances inside
    the sleeper), and the reason this clock exists at all.
    """
    clock = VirtualClock()
    finished: list[float] = []

    async def worker() -> None:
        await clock.sleep(1.0)
        finished.append(clock.now())

    async def main() -> float:
        await asyncio.gather(*(clock.spawn(worker()) for _ in range(32)))
        return clock.now()

    elapsed = run(main())

    assert elapsed == pytest.approx(1.0)
    assert finished == [1.0] * 32


def test_sequential_sleeps_add_up():
    clock = VirtualClock()

    async def worker() -> None:
        for _ in range(3):
            await clock.sleep(0.5)

    async def main() -> float:
        await clock.spawn(worker())
        await asyncio.sleep(0)  # let the worker finish before reading the clock
        while clock.workers:
            await asyncio.sleep(0)
        return clock.now()

    assert run(main()) == pytest.approx(1.5)


def test_timers_fire_in_deadline_order():
    clock = VirtualClock()
    woken: list[tuple[float, str]] = []

    async def sleeper(name: str, delay: float) -> None:
        await clock.sleep(delay)
        woken.append((round(clock.now(), 6), name))

    async def main() -> None:
        await asyncio.gather(
            clock.spawn(sleeper("late", 1.5)), clock.spawn(sleeper("middle", 1.0)), clock.spawn(sleeper("early", 0.5))
        )

    run(main())

    assert woken == [(0.5, "early"), (1.0, "middle"), (1.5, "late")]


def test_a_worker_that_is_still_working_holds_time_back():
    """A runnable worker must not have time moved under it while it yields between steps.

    The eager-clock failure mode in one test: `other` sleeps 5s, so any clock that advances inside a
    sleeper would show 5.0 in `marks` even though this worker never asked to wait.
    """
    clock = VirtualClock()
    marks: list[float] = []

    async def busy() -> None:
        marks.append(clock.now())
        for _ in range(5):
            await asyncio.sleep(0)  # a yield, not a wait: eight of these are still zero seconds
            marks.append(clock.now())

    async def other() -> None:
        await clock.sleep(5.0)

    async def main() -> None:
        await asyncio.gather(clock.spawn(busy()), clock.spawn(other()))

    run(main())

    assert marks == [0.0] * 6


def test_zero_delay_sleeps_do_not_consume_time():
    """`retry_after=0` and zero-jitter backoff must not be able to push the simulation forward."""
    clock = VirtualClock()

    async def spinner() -> None:
        for _ in range(50):
            await clock.sleep(0.0)
        await clock.sleep(0.0)

    async def scheduled() -> None:
        await clock.sleep(1.0)

    async def main() -> None:
        await asyncio.gather(clock.spawn(spinner()), clock.spawn(scheduled()))

    run(main())

    assert clock.now() == pytest.approx(1.0)


def test_a_release_that_is_already_queued_does_not_let_time_jump_the_queue():
    """The false-quiescence case: a worker is "blocked" but its await resolves without any time.

    `waiter` counts as blocked in the acquire sense, and its release is already queued on the loop.
    If the driver advanced on the mere fact that every worker looked blocked, it would move the
    clock to `other`'s 10s deadline and the waiter would observe 10.0 for a wait that took no time
    at all.
    """
    clock = VirtualClock()
    seen: list[float] = []

    async def waiter() -> None:
        gate = asyncio.Event()
        asyncio.get_running_loop().call_soon(gate.set)  # the release is already on its way
        with clock.blocked():
            await gate.wait()
        seen.append(clock.now())

    async def other() -> None:
        await clock.sleep(10.0)

    async def main() -> None:
        await asyncio.gather(clock.spawn(waiter()), clock.spawn(other()))

    run(main())

    assert seen == [0.0]
    assert clock.now() == pytest.approx(10.0)


def test_only_yields_means_time_never_moves_and_nothing_hangs():
    clock = VirtualClock()

    async def busy() -> None:
        for _ in range(20):
            await asyncio.sleep(0)

    async def main() -> None:
        await clock.spawn(busy())

    run(main())

    assert clock.now() == 0.0
    assert clock.advances == 0


def test_a_cancelled_sleep_leaves_the_driver_consistent():
    """Cancellation must not leak a "parked" count, which would make the clock advance too eagerly."""
    clock = VirtualClock()
    woken: list[float] = []

    async def long_sleeper() -> None:
        await clock.sleep(5.0)

    async def short_sleeper() -> None:
        await clock.sleep(1.0)
        woken.append(clock.now())

    async def main() -> None:
        doomed = clock.spawn(long_sleeper())
        await asyncio.gather(clock.spawn(short_sleeper()), doomed)

    async def main_cancelling() -> None:
        doomed = clock.spawn(long_sleeper())
        await asyncio.gather(clock.spawn(short_sleeper()))
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed

    run(main_cancelling())

    assert woken == [1.0]
    assert clock.now() == pytest.approx(1.0)
    assert clock.blocked_workers == 0
    assert clock.pending == 0  # the cancelled timer left the heap with it
    run(main())  # and a fresh schedule still works


def test_the_same_schedule_produces_the_same_timeline_twice():
    """Determinism is the whole reason for a simulated clock: comparisons must be reproducible."""
    import random

    def schedule() -> list[tuple[float, int]]:
        clock = VirtualClock()
        log: list[tuple[float, int]] = []

        async def worker(worker_id: int, rng: random.Random) -> None:
            for _ in range(6):
                await clock.sleep(rng.uniform(0.05, 0.5))
                log.append((round(clock.now(), 9), worker_id))

        async def main() -> None:
            rngs = [random.Random(1000 + i) for i in range(8)]
            await asyncio.gather(*(clock.spawn(worker(i, rngs[i])) for i in range(8)))

        run(main())
        return log

    first, second = schedule(), schedule()

    assert first == second
    assert len(first) == 48


def test_the_reference_clock_compresses_real_time_and_reports_scaled_time():
    from pyattacker.benchmark.clock import ScaledClock

    clock = ScaledClock(speedup=50.0)

    async def main() -> float:
        started = clock.now()
        await clock.sleep(10.0)  # a fifth of a real second
        return clock.now() - started

    elapsed = run(main())

    assert 10.0 <= elapsed < 11.0, "simulated time is real elapsed time times the speedup"


def test_the_two_clocks_agree_on_the_headline_numbers():
    """The differential check the virtual clock is answerable to.

    A broken advance rule — the "add up every sleep" failure mode, say — shows up here as a gap of
    the concurrency factor, not as a subtle few percent. The tolerances are therefore wide on
    purpose: this is a smoke alarm for a lying simulator, not a precision claim, and the reference
    clock has its own distortion (CPU time is real time, so it ages the simulation).
    """
    from pyattacker.benchmark import Harness, get_scenario
    from pyattacker.benchmark.clock import ScaledClock

    scenario = get_scenario("bursty_provider").with_overrides(jobs=120, concurrency=6, horizon_s=90.0)

    virtual = Harness(scenario, "wait", seed=3).run().metrics
    real = Harness(scenario, "wait", seed=3, clock=ScaledClock(speedup=10.0)).run().metrics

    assert abs(virtual["refusal_rate"] - real["refusal_rate"]) < 0.15, (virtual["refusal_rate"], real["refusal_rate"])
    assert abs(virtual["jobs_done"] - real["jobs_done"]) <= max(5.0, 0.3 * real["jobs_done"]), (
        virtual["jobs_done"],
        real["jobs_done"],
    )
    assert abs(virtual["utilization"] - real["utilization"]) < 0.2, (virtual["utilization"], real["utilization"])
    assert abs(virtual["error_rate"] - real["error_rate"]) < 0.05, (virtual["error_rate"], real["error_rate"])
