"""Resource acquisition strategies (M2) and the metrics they feed.

Covers `sticky`, `quota_aware`, `least_busy`, `failover`, pool wait-time metrics, the slow-wait
event, and the targeted-wakeup predicate. The wakeup *path* is exercised by
`test_lease_safety.py::test_backoff_algorithm_waits_for_release`; here we test the decision
logic that decides who gets woken.
"""

from __future__ import annotations

import asyncio

import pytest
from helpers import FakeClock, run

from pyattacker import (
    Backoff,
    Failover,
    Immediate,
    LeastBusy,
    Pool,
    QuotaAware,
    Resource,
    Sticky,
    Wait,
)
from pyattacker.errors import ResourceUnavailable
from pyattacker.resource import _Waiter


class _Ctx:
    """Minimal task-context stand-in: the algorithms only need ``meta`` and ``_untrack``."""

    def __init__(self) -> None:
        self.meta: dict = {}
        self.task_name = "test"

    def _untrack(self, lease) -> None:  # pragma: no cover - nothing to untrack here
        return None

    def emit(self, *args, **kwargs) -> None:  # pragma: no cover
        return None

    def holds_from(self, pool) -> bool:
        return False

    def held_resource_ids(self, pool) -> set:
        return set()


def _quota_pool(algorithm="quota_aware") -> Pool:
    return Pool(
        "apis",
        [
            Resource.create("llm", id="small", capacity=1, options={"quota": {"tokens": 100}}),
            Resource.create("llm", id="large", capacity=1, options={"quota": {"tokens": 10_000}}),
        ],
        algorithm=algorithm,
    )


# --------------------------------------------------------------------- sticky
def test_sticky_keeps_a_pipeline_on_the_same_resource():
    async def _case():
        pool = Pool(
            "s",
            [
                Resource.create("llm", id="a", capacity=4),
                Resource.create("llm", id="b", capacity=4),
            ],
            algorithm=Sticky(),
        )
        ctx = _Ctx()
        picked = []
        for _ in range(3):
            lease = await pool.acquire(ctx=ctx)
            picked.append(lease.resource.id)
            lease.release_now()
        assert picked == ["a", "a", "a"]  # one pipeline, one endpoint: prefix caches stay warm
        assert ctx.meta["sticky"]["s"] == "a"

        other = _Ctx()
        lease = await pool.acquire(ctx=other)
        assert lease.resource.id == "b"  # affinity never leaks across pipelines
        assert other.meta["sticky"]["s"] == "b"
        lease.release_now()

    run(_case())


def test_sticky_falls_back_when_the_preferred_resource_disappears():
    async def _case():
        pool = Pool(
            "s",
            [
                Resource.create("llm", id="a", capacity=1),
                Resource.create("llm", id="b", capacity=1),
            ],
            algorithm=Sticky(),
        )
        ctx = _Ctx()
        first = await pool.acquire(ctx=ctx)
        assert first.resource.id == "a"
        first.release_now()

        pool.revoke("a", reason="endpoint went away")
        lease = await pool.acquire(ctx=ctx)
        assert lease.resource.id == "b"
        assert ctx.meta["sticky"]["s"] == "b"
        lease.release_now()

    run(_case())


# --------------------------------------------------------------- quota_aware
def test_quota_aware_prefers_the_resource_with_the_most_headroom():
    async def _case():
        pool = _quota_pool()
        ctx = _Ctx()

        lease = await pool.acquire(ctx=ctx)
        assert lease.resource.id == "large"  # 10_000 tokens of headroom beats 100
        lease.report(usage={"tokens": 9_800})  # ... until it doesn't
        lease.release_now()

        second = await pool.acquire(ctx=ctx)
        assert second.resource.id == "small"
        second.release_now()

    run(_case())


def test_quota_aware_ranks_unknown_quota_last():
    async def _case():
        pool = Pool(
            "apis",
            [
                Resource.create("llm", id="no-quota", capacity=1),
                Resource.create("llm", id="metered", capacity=1, options={"quota": {"tokens": 500}}),
            ],
            algorithm=QuotaAware(),
        )
        lease = await pool.acquire()
        assert lease.resource.id == "metered"
        lease.release_now()

    run(_case())


def test_quota_aware_is_a_preference_not_a_hard_limit():
    """When everything is exhausted the best candidate is still handed out: refusing to work is worse."""

    async def _case():
        pool = _quota_pool()
        ctx = _Ctx()
        for _ in range(2):  # exhaust whichever resource is offered first
            lease = await pool.acquire(ctx=ctx)
            lease.report(usage={"tokens": 50_000})
            lease.release_now()

        # Both are far past quota, yet a lease still comes back: quota is a preference.
        lease = await pool.acquire(ctx=ctx)
        assert lease.resource.id in {"small", "large"}
        lease.release_now()

    run(_case())


def test_quota_aware_score_is_monotonic_in_remaining_quota():
    algo = QuotaAware(metric="tokens")
    resource = Resource.create("llm", id="r", options={"quota": {"tokens": 1000}})

    class _Stats:
        def __init__(self, used: float) -> None:
            self.usage = {"tokens": used}

    scores = [algo.score(resource, _Stats(used)) for used in (0, 400, 900, 1000, 2000)]
    assert scores == sorted(scores, reverse=True)
    assert scores[0][0] == pytest.approx(1.0) and scores[0][1] == pytest.approx(1000)
    assert scores[-1][0] < scores[-2][0] < 0
    # equal ratio -> the resource with more absolute headroom wins
    big = Resource.create("llm", id="big", options={"quota": {"tokens": 100_000}})
    assert algo.score(big, _Stats(0)) > algo.score(resource, _Stats(0))


# ---------------------------------------------------------------- least_busy
def test_least_busy_picks_the_idlest_resource():
    async def _case():
        pool = Pool(
            "apis",
            [
                Resource.create("llm", id="busy", capacity=4),
                Resource.create("llm", id="idle", capacity=4),
            ],
            algorithm=LeastBusy(),
        )
        first = await pool.acquire()
        assert first.resource.id == "busy"
        # make "busy" genuinely busier, then ask again
        pool.try_acquire(where=lambda r: r.id == "busy")
        second = await pool.acquire()
        assert second.resource.id == "idle"
        for lease in (first, second):
            lease.release_now()

    run(_case())


# ------------------------------------------------------------------ failover
def test_failover_walks_to_the_next_pool():
    async def _case():
        primary = Pool("primary", [Resource.create("llm", id="p1", capacity=1)], algorithm="immediate")
        backup = Pool("backup", [Resource.create("llm", id="b1", capacity=1)], algorithm="immediate")
        ctx = _Ctx()
        ctx.pools = {"primary": primary, "backup": backup}

        holder = primary.try_acquire()  # saturate the primary pool
        assert holder is not None

        lease = await primary.acquire(ctx=ctx, algorithm=Failover(pools=["primary", "backup"]))
        assert lease.resource.id == "b1"  # failed over instead of blocking
        assert pool_name(lease) == "backup"
        lease.release_now()
        holder.release_now()

    run(_case())


def pool_name(lease) -> str:
    return lease.pool.name


def test_failover_fallback_only_waits_on_the_first_pool_not_the_whole_list():
    """Documented behavior (algorithm.py docstring, design.md): once every pool has been tried
    immediately and none had capacity, ``fallback`` parks on ``pools[0]`` only. Freeing up a
    *later* pool must not wake it — only freeing the first pool does."""
    async def _case():
        primary = Pool("primary", [Resource.create("llm", id="p1", capacity=1)], algorithm="immediate")
        backup = Pool("backup", [Resource.create("llm", id="b1", capacity=1)], algorithm="immediate")
        ctx = _Ctx()
        ctx.pools = {"primary": primary, "backup": backup}

        primary_holder = primary.try_acquire()
        backup_holder = backup.try_acquire()
        assert primary_holder is not None and backup_holder is not None

        task = asyncio.ensure_future(
            primary.acquire(ctx=ctx, algorithm=Failover(pools=["primary", "backup"]))
        )
        await asyncio.sleep(0)  # let it walk both pools immediately and start waiting on fallback

        backup_holder.release_now()  # frees the *second* listed pool, not pools[0]
        await asyncio.sleep(0)
        assert not task.done()  # fallback is bound to "primary"; a free backup slot doesn't wake it

        primary_holder.release_now()  # frees pools[0]
        lease = await asyncio.wait_for(task, timeout=1.0)
        assert pool_name(lease) == "primary"
        lease.release_now()

    run(_case())


def test_failover_reports_why_every_pool_failed():
    async def _case():
        primary = Pool("primary", [Resource.create("llm", id="p1", capacity=1)], algorithm="immediate")
        ctx = _Ctx()
        ctx.pools = {"primary": primary}
        holder = primary.try_acquire()
        assert holder is not None
        # fallback=Immediate so a saturated pool raises instead of blocking forever
        algo = Failover(pools=["primary", "missing"], fallback=Immediate())
        with pytest.raises(ResourceUnavailable) as excinfo:
            await primary.acquire(ctx=ctx, algorithm=algo)
        assert "no resource available" in str(excinfo.value)
        holder.release_now()

    run(_case())


# ------------------------------------------------------------------- metrics
def test_wait_metrics_capture_time_spent_blocked():
    async def _case():
        pool = Pool(
            "apis",
            [Resource.create("llm", id="only", capacity=1)],
            algorithm=Wait(),
        )
        holder = pool.try_acquire()
        assert holder is not None
        assert pool.stats().waits_total == 0

        async def _release_soon():
            await asyncio.sleep(0.02)
            holder.release_now()

        releaser = asyncio.create_task(_release_soon())
        try:
            lease = await pool.acquire(timeout=5.0)
        finally:
            await releaser
        lease.release_now()

        stats = pool.stats()
        assert stats.waits_total == 1
        assert stats.wait_ms_avg is not None and stats.wait_ms_avg > 0
        assert stats.wait_ms_max is not None and stats.wait_ms_max >= stats.wait_ms_avg
        assert stats.wait_ms_p50 is not None and stats.wait_ms_p95 is not None

    run(_case())


def test_slow_wait_emits_an_event():
    async def _case():
        seen: list = []
        pool = Pool(
            "apis",
            [Resource.create("llm", id="only", capacity=1)],
            algorithm=Wait(),
            on_event=seen.append,
        )
        pool.slow_wait_ms = 0.001  # anything measurable counts as slow
        holder = pool.try_acquire()
        assert holder is not None

        async def _release_soon():
            await asyncio.sleep(0.01)
            holder.release_now()

        releaser = asyncio.create_task(_release_soon())
        try:
            lease = await pool.acquire(timeout=5.0, kind="llm")
        finally:
            await releaser
        lease.release_now()

        slow = [event for event in seen if event.kind == "acquire.slow_wait"]
        assert len(slow) == 1
        assert slow[0].data["waited_ms"] > 0
        assert slow[0].data["selector"] == {"kind": "llm"}

    run(_case())


# ------------------------------------------------------- targeted wakeups
def test_waiter_matching_is_selector_and_predicate_aware():
    llm = Resource.create("llm", id="a", tags={"tier": "gold"}, options={"model": "gpt-4o"})
    judge = Resource.create("judge", id="j", tags={"tier": "bronze"})

    assert _Waiter(selector={}, where=None).matches(llm) is True
    assert _Waiter(selector={"kind": "llm"}, where=None).matches(llm) is True
    assert _Waiter(selector={"kind": "judge"}, where=None).matches(llm) is False
    assert _Waiter(selector={"tier": "gold"}, where=None).matches(llm) is True
    assert _Waiter(selector={"model": "gpt-4o"}, where=None).matches(llm) is True
    assert _Waiter(selector={"tier": "bronze"}, where=None).matches(llm) is False
    assert _Waiter(selector={}, where=lambda r: r.id == "j").matches(llm) is False
    assert _Waiter(selector={}, where=lambda r: r.id == "j").matches(judge) is True


async def _settle(times: int = 5) -> None:
    """Let scheduled waiters actually resume (wait_for needs more than one loop turn)."""
    for _ in range(times):
        await asyncio.sleep(0)


def test_releasing_one_resource_wakes_only_matching_waiters():
    """White-box on purpose: the whole point of targeted wakeups is *who* gets woken."""

    async def _case():
        pool = Pool(
            "mixed",
            [
                Resource.create("llm", id="a", capacity=1),
                Resource.create("judge", id="j", capacity=1),
            ],
            algorithm=Wait(),
        )
        # Saturate both resources first, otherwise wait_slot returns immediately.
        held_llm = pool.try_acquire(kind="llm")
        held_judge = pool.try_acquire(kind="judge")
        assert held_llm is not None and held_judge is not None

        llm_waiter = asyncio.create_task(pool.wait_slot(5.0, selector={"kind": "llm"}))
        judge_waiter = asyncio.create_task(pool.wait_slot(5.0, selector={"kind": "judge"}))
        await _settle()
        assert len(pool._waiters) == 2, "both waiters should be registered"

        held_llm.release_now()
        await _settle()

        assert await asyncio.wait_for(llm_waiter, 1.0) is True
        assert not judge_waiter.done(), "an llm release must not resolve a judge waiter"
        assert len(pool._waiters) == 1
        assert pool._waiters[0].event.is_set() is False, "the judge waiter must not even be woken"

        held_judge.release_now()
        assert await asyncio.wait_for(judge_waiter, 1.0) is True
        assert len(pool._waiters) == 0

    run(_case())


# ------------------------------------------------------- client factory failures
def test_factory_failure_never_hands_out_a_clientless_lease():
    """A broken factory must fail loudly, not hand out a lease whose ``client`` is None.

    Regression: the old guard skipped the factory once it had failed, so the *next* lease on that
    resource returned ``client=None`` and the user's code blew up with an AttributeError deep in a
    request instead of a clear ResourceUnavailable.
    """
    attempts = {"n": 0}

    def broken_factory(resource):
        attempts["n"] += 1
        raise OSError("cannot reach the provider")

    async def _case():
        pool = Pool("apis", [Resource.create("llm", id="api-1", factory=broken_factory)])
        for _ in range(2):
            with pytest.raises(ResourceUnavailable) as excinfo:
                await pool.acquire()
            assert "factory" in str(excinfo.value)
        assert attempts["n"] == 1, "the factory must not be retried on every lease"
        assert pool.snapshot()[0]["failed"] == 2, "every refused lease counts against the resource"

    run(_case())


def test_repeated_factory_failures_eventually_kill_the_resource():
    def broken_factory(resource):
        raise OSError("nope")

    async def _case():
        # degrade_after == dead_after: every failure goes straight through the DEAD check
        # before the DEGRADED branch could kick in, so the resource stays a candidate for
        # every attempt up to dead_after instead of getting cooled down after the first one.
        pool = Pool(
            "apis",
            [Resource.create("llm", id="api-1", factory=broken_factory, dead_after=2, degrade_after=2)],
        )
        for _ in range(2):
            with pytest.raises(ResourceUnavailable):
                await pool.acquire()
        assert pool.snapshot()[0]["state"] == "dead"
        # A dead resource is no longer a candidate: the pool is now simply empty.
        with pytest.raises(ResourceUnavailable) as excinfo:
            await pool.acquire(algorithm="immediate")
        assert "no resource available" in str(excinfo.value)

    run(_case())


def test_a_transient_factory_failure_recovers_after_cooldown():
    """DEGRADED must be a genuine second chance, not just a delay before DEAD.

    Regression: once `_lease` sees a stored `client_error`, it used to keep refusing the
    resource forever without ever calling the factory again — so a resource that recovered
    (e.g. a transient network blip during client construction) stayed unusable until it
    accumulated enough failures to hit `dead_after`, even though the *next* factory call
    would have succeeded.
    """
    calls = {"n": 0}

    def flaky_factory(resource):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient")
        return f"client-{calls['n']}"

    async def _case():
        clock = FakeClock()
        pool = Pool(
            "apis",
            [Resource.create("llm", id="api-1", factory=flaky_factory, degrade_after=1, dead_after=5, cooldown_s=10.0)],
            clock=clock,
        )
        with pytest.raises(ResourceUnavailable):
            await pool.acquire(algorithm="immediate")
        assert pool.snapshot()[0]["state"] == "degraded"

        # still within cooldown: the resource is not yet a candidate
        with pytest.raises(ResourceUnavailable):
            await pool.acquire(algorithm="immediate")
        assert calls["n"] == 1, "the factory must not be retried before cooldown expires"

        clock.t += 10.0  # cooldown elapses
        lease = await pool.acquire(algorithm="immediate")
        assert lease.client == "client-2"
        assert pool.snapshot()[0]["state"] == "ready"

    run(_case())


@pytest.mark.parametrize("algorithm", [Wait(), Backoff(base=0.001, cap=0.01)])
def test_wait_and_backoff_skip_a_broken_resource_instead_of_raising(algorithm):
    """Regression for a pool with one broken resource and other healthy ones.

    Previously, `Wait`/`Backoff` handed the *single* candidate `select()` picked straight to
    `_lease`; if that candidate's factory failed, the `ResourceUnavailable` propagated out of
    `acquire()` untouched even though other healthy resources were sitting idle. A pool must
    keep serving out of its healthy resources regardless of which algorithm is used.
    """

    def broken_factory(resource):
        raise OSError("cannot reach the provider")

    async def _case():
        pool = Pool(
            "apis",
            [
                Resource.create("llm", id="bad", factory=broken_factory, capacity=4),
                Resource.create("llm", id="good", capacity=4),
            ],
            algorithm=algorithm,
        )
        for _ in range(4):
            lease = await pool.acquire()
            assert lease.resource.id == "good"
            lease.release_now()

    run(_case())
