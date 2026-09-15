"""★ The lease safety contract — the framework's most critical guarantee.

Covers:
* ``async with ctx.acquire(...)`` releases synchronously on both the success and the error path;
* acquire/release in a loop inside one task does not accumulate holdings (the concurrency slot is really handed back);
* leases leaked through the escape hatch ``await ctx.acquire_lease()`` are **force-reclaimed** when the task ends, and the fact is recorded;
* cancellation (CancelledError) and timeouts never skip reclamation either;
* resource health: consecutive failures → degrade/circuit-break → automatic recovery once the cooldown ends;
* the pool's publish/subscribe event stream;
* attempt-level detail records expose lease usage.
"""

from __future__ import annotations

import asyncio

import pytest
from helpers import FakeClock, make_pool, run

from pyattacker import Pool, Resource, RetryableError, Runner, pipeline, task
from pyattacker.runner import RunConfig
from pyattacker.tasks import leaky


@task("acq.ok", resource="apis")
async def acquire_ok(seed, ctx):
    async with ctx.acquire() as lease:
        return {"resource": lease.resource.id}


@task("acq.boom", resource="apis")
async def acquire_then_boom(seed, ctx):
    async with ctx.acquire() as lease:
        assert lease is not None
        raise RetryableError("failure occurred while holding the lease", error_class="upstream")


@task("acq.loop", resource="apis")
async def acquire_in_loop(seed, ctx):
    seen = []
    for _ in range(5):
        async with ctx.acquire():
            seen.append(ctx._resolve_pool(None).stats().active)
            await ctx.clock.sleep(0)
    return {"active_during_loop": seen}


@task("acq.hold", resource="apis")
async def hold_forever(seed, ctx):
    async with ctx.acquire():
        await asyncio.sleep(3600)


@task("acq.hold_with_timeout", resource="apis")
async def hold_with_timeout(seed, ctx):
    async with ctx.acquire():
        await asyncio.sleep(3600)


def _runner(pool: Pool, **kwargs) -> Runner:
    return Runner(
        store=":memory:",
        pools=[pool],
        clock=FakeClock(),
        config=RunConfig(store=":memory:", handle_signals=False, **kwargs),
    )


def _events(runner: Runner) -> list[str]:
    return [event.kind for event in runner.store.events(limit=500)]


def test_context_manager_releases_on_success():
    pool = make_pool(capacity=1)
    runner = _runner(pool, concurrency=3)
    report = runner.run(pipeline("p", acquire_ok).map([{"i": i} for i in range(4)]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 4}
    assert pool.stats().active == 0
    assert report.leases_leaked == 0
    kinds = _events(runner)
    assert "resource.leased" in kinds and "resource.released" in kinds
    assert "lease.leaked" not in kinds


def test_exception_mid_task_still_releases_lease():
    pool = make_pool(capacity=1)
    runner = _runner(pool)
    report = runner.run(pipeline("p", acquire_then_boom).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert pool.stats().active == 0  # ★ the error path must release it too
    # Resource health is reported explicitly by the task (lease.report(ok=False)); the framework
    # does not guess for the user whether "this failure was the resource's fault"
    assert pool.stats().failed_total == 0
    assert runner.store.attempts()[0].leases[0]["released"] is True
    assert "lease.leaked" not in _events(runner)

    attempts = runner.store.attempts()
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].error_class == "upstream"
    assert attempts[0].decision["retry"] is False
    assert attempts[0].decision["reason"] == "attempts_exhausted"  # default max_attempts=1


def test_loop_acquire_release_does_not_accumulate():
    pool = make_pool(capacity=1)
    runner = _runner(pool, concurrency=2)
    report = runner.run(pipeline("p", acquire_in_loop).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert pool.stats().active == 0
    assert pool.stats().leases_total == 5  # each iteration of the loop is a separate lease
    row = runner.store.export_rows().__next__()
    assert row["artifacts"][-1]["payload"]["active_during_loop"] == [1, 1, 1, 1, 1]


def test_escaped_lease_is_force_reclaimed_and_recorded():
    pool = make_pool(capacity=1)
    runner = _runner(pool)
    report = runner.run(pipeline("p", leaky(pool="apis")).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert pool.stats().active == 0  # ★ force-reclaim
    assert pool.stats().leaked_total == 1
    assert report.leases_leaked == 1
    assert "lease.leaked" in _events(runner)


def test_strict_leases_turns_leak_into_failure():
    pool = make_pool(capacity=1)
    runner = _runner(pool, strict_leases=True)
    report = runner.run(pipeline("p", leaky(pool="apis")).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert pool.stats().active == 0
    attempts = runner.store.attempts()
    assert attempts[0].error_type == "LeaseLeakError"


def test_attempt_records_show_lease_usage():
    pool = make_pool(capacity=2)
    runner = _runner(pool)
    runner.run(pipeline("p", acquire_ok).map([{"i": 0}]))

    attempt = runner.store.attempts()[0]
    assert attempt.outcome == "succeeded"
    assert len(attempt.leases) == 1
    assert attempt.leases[0]["pool"] == "apis"
    assert attempt.leases[0]["resource"] == "apis-1"
    assert attempt.leases[0]["released"] is True


def test_cancellation_reclaims_lease():
    async def _case():
        pool = make_pool(capacity=1)
        runner = Runner(
            store=":memory:",
            pools=[pool],
            concurrency=1,
            clock=FakeClock(),
            config=RunConfig(store=":memory:", handle_signals=False),
        )
        running = asyncio.create_task(runner.run_async(pipeline("p", hold_forever).map([{"i": 0}])))
        for _ in range(200):
            if pool.stats().active == 1:
                break
            await asyncio.sleep(0)
        assert pool.stats().active == 1, "the task should have acquired a lease by now"

        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        assert pool.stats().active == 0  # ★ cancellation must release it too
        assert pool.stats().leaked_total == 0  # and it is released normally, not force-reclaimed

    run(_case())


def test_timeout_reclaims_lease():
    pool = make_pool(capacity=1)
    runner = _runner(pool)
    timed = hold_with_timeout.with_overrides(timeout_s=0.05)
    report = runner.run(pipeline("p", timed).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert pool.stats().active == 0
    assert runner.store.attempts()[0].error_class == "timeout"
    assert "lease.leaked" not in _events(runner)


def test_release_is_idempotent():
    pool = make_pool(capacity=1)
    lease = pool.try_acquire()
    assert lease is not None
    assert lease.release_now() is True
    assert lease.release_now() is False
    assert pool.stats().active == 0
    assert pool.stats().leases_total == 1


def test_acquire_raises_when_pool_is_full_and_algorithm_is_immediate():
    async def _case():
        pool = make_pool(capacity=1, algorithm="immediate")
        first = pool.try_acquire()
        assert first is not None
        with pytest.raises(Exception) as excinfo:
            await pool.acquire()
        assert "no resource available" in str(excinfo.value)
        first.release_now()

    run(_case())


def test_backoff_algorithm_waits_for_release():
    from pyattacker import Backoff

    async def _case():
        # A real clock is used deliberately here: the fake clock would burn through the backoff
        # budget instantly, while the release happens in real time
        pool = Pool(
            "apis",
            [Resource.create("llm", id="only", capacity=1)],
            algorithm=Backoff(base=0.01, cap=0.05, jitter="none"),
        )
        holder = pool.try_acquire()
        assert holder is not None

        async def _release_soon():
            await asyncio.sleep(0.03)
            holder.release_now()

        releaser = asyncio.create_task(_release_soon())
        try:
            lease = await pool.acquire(timeout=5.0)
        finally:
            await releaser
        assert lease is not None
        lease.release_now()
        assert pool.stats().active == 0

    run(_case())


def test_degrade_then_recover_then_dead():
    clock = FakeClock()
    pool = make_pool(count=1, capacity=1, degrade_after=2, dead_after=4, cooldown_s=10.0)
    pool.clock = clock

    first = pool.try_acquire()
    first.report(ok=False)
    first.release_now()
    assert pool.snapshot()[0]["state"] == "ready"  # only 1 failure so far, below the degrade threshold

    second = pool.try_acquire()
    second.report(ok=False)
    second.release_now()
    assert pool.snapshot()[0]["state"] == "degraded"
    assert pool.try_acquire() is None  # unavailable during the cooldown window

    clock.t += 11.0
    third = pool.try_acquire()
    assert third is not None  # available again automatically once the cooldown ends
    third.report(ok=True)
    third.release_now()
    assert pool.snapshot()[0]["state"] == "ready"
    assert pool.snapshot()[0]["consecutive_failures"] == 0  # only a success resets it

    # Consecutive failures accumulate across cooldown windows: four more → DEAD
    for _ in range(4):
        clock.t += 11.0
        lease = pool.try_acquire()
        assert lease is not None
        lease.report(ok=False)
        lease.release_now()
    assert pool.snapshot()[0]["state"] == "dead"
    assert pool.try_acquire() is None


def test_pool_publish_and_subscribe():
    async def _case():
        pool = make_pool(count=0)
        received = []

        async def consumer():
            async for event in pool.subscribe(["resource.published"]):
                received.append(event)
                return

        waiter = asyncio.create_task(consumer())
        await asyncio.sleep(0)
        pool.add(Resource.create("llm", id="fresh", options={"model": "m"}))
        await asyncio.wait_for(waiter, timeout=1.0)

        assert len(received) == 1
        assert received[0].resource_id == "fresh"
        assert received[0].pool == "apis"
        assert received[0].data["capacity"] == 1

    run(_case())


def test_pool_stats_reflect_capacity_and_utilization():
    pool = make_pool(count=3, capacity=2)
    leases = [pool.try_acquire() for _ in range(4)]
    stats = pool.stats()
    assert stats.total == 3
    assert stats.capacity == 6
    assert stats.active == 4
    assert stats.utilization == pytest.approx(4 / 6, abs=1e-4)
    for lease in leases:
        lease.release_now()
    assert pool.stats().active == 0
