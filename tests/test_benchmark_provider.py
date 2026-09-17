"""The simulated provider: its own dynamics, and the two properties that make it a fair world.

Fairness, not just realism, is what these tests are about. A provider that changes its mind depending
on which algorithm is asking would make every comparison meaningless, and the failure would be
invisible in the results — the numbers would simply favour whichever algorithm the world liked.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from helpers import run

from pyattacker.benchmark import SCENARIOS, EndpointProfile, FailureProfile, LatencyProfile, RateLimitProfile
from pyattacker.benchmark.provider import ProviderError, SimulatedProvider
from pyattacker.benchmark.scenario import Scenario
from pyattacker.errors import error_class_of


class ManualClock:
    """A clock the test drives: `now` moves only when the test moves it, sleeps park until released."""

    def __init__(self) -> None:
        self.t = 0.0
        self._parked: list[asyncio.Future[None]] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        future = asyncio.get_running_loop().create_future()
        self._parked.append(future)
        await future

    def release(self) -> None:
        parked, self._parked = self._parked, []
        for future in parked:
            if not future.done():
                future.set_result(None)

    async def settle(self) -> None:
        """Let every runnable task reach its next await."""
        for _ in range(10):
            await asyncio.sleep(0)


_BUCKET = RateLimitProfile(per_window=600, window_s=60.0, burst=10.0)


def _scenario(
    *,
    capacity: int = 2,
    latency: LatencyProfile | None = None,
    failures: FailureProfile | None = None,
    rate_limit: RateLimitProfile | None = _BUCKET,
    quota_units: int | None = 600,
) -> Scenario:
    """A one-endpoint world, so a test can vary exactly one assumption at a time."""
    endpoint = EndpointProfile(
        id="only",
        capacity=capacity,
        latency=latency or LatencyProfile(median_s=1.0, sigma=0.0, tail_rate=0.0),
        failures=failures or FailureProfile(error_rate=0.0, storm_rate=0.0),
        rate_limit=rate_limit,
        quota_units=quota_units,
    )
    return Scenario(name="unit", summary="one endpoint, one worker", endpoints=(endpoint,), jobs=10, concurrency=2)


# ------------------------------------------------------------------ the world's own dynamics


def test_an_endpoint_that_is_full_refuses_with_a_429_and_a_retry_after():
    """Concurrency limits are what a provider enforces *before* it meters you."""
    clock = ManualClock()
    scenario = _scenario(capacity=1, latency=LatencyProfile(median_s=1.0, sigma=0.0, tail_rate=0.0))
    provider = SimulatedProvider(scenario, clock)

    async def main() -> ProviderError | None:
        first = asyncio.ensure_future(provider.perform("only"))
        await clock.settle()  # the first request is now in flight
        try:
            await provider.perform("only")
        except ProviderError as exc:
            return exc
        finally:
            clock.release()
            await first
        return None

    error = run(main())

    assert error is not None
    assert error.status == 429
    assert error.retry_after is not None
    assert error_class_of(error) == "rate_limit"
    assert provider.stats.endpoints["only"].refused_capacity == 1
    assert provider.stats.endpoints["only"].admitted == 1


def test_an_exhausted_bucket_refuses_and_tightens_the_allowance():
    clock = ManualClock()
    scenario = _scenario(rate_limit=RateLimitProfile(per_window=1, window_s=60.0, burst=1.0, tightening=0.5))
    provider = SimulatedProvider(scenario, clock)

    async def one() -> None:
        task = asyncio.ensure_future(provider.perform("only"))
        await clock.settle()
        clock.release()
        await task

    async def main() -> ProviderError | None:
        await one()  # spends the only token in the burst
        try:
            await provider.perform("only")
        except ProviderError as exc:
            return exc
        return None

    error = run(main())

    assert error is not None and error.status == 429
    assert "quota" in str(error)
    assert provider.stats.endpoints["only"].refused_quota == 1
    assert provider._allowance["only"] < 1.0, "being refused has to cost future allowance"
    assert provider.stats.endpoints["only"].tokens_used == 1.0


def test_the_allowance_recovers_when_the_client_stops_pushing():
    clock = ManualClock()
    scenario = _scenario(rate_limit=RateLimitProfile(per_window=1, window_s=60.0, burst=1.0, recovery_per_window=1.0))
    provider = SimulatedProvider(scenario, clock)

    async def main() -> bool:
        task = asyncio.ensure_future(provider.perform("only"))
        await clock.settle()
        clock.release()
        await task
        with pytest.raises(ProviderError):
            await provider.perform("only")
        clock.t += 120.0  # two windows of quiet
        task = asyncio.ensure_future(provider.perform("only"))
        await clock.settle()
        clock.release()
        await task
        return True

    assert run(main())
    assert provider.stats.endpoints["only"].admitted == 2
    assert provider.stats.endpoints["only"].refused_quota == 1


def test_failures_look_like_provider_errors_the_framework_can_classify():
    """A scenario raises what a vendor SDK raises, and the kernel's own classifier decides retry."""
    clock = ManualClock()
    scenario = _scenario(
        latency=LatencyProfile(median_s=0.01, sigma=0.0, tail_rate=0.0),
        failures=FailureProfile(error_rate=1.0, storm_rate=0.0),
        rate_limit=None,
        quota_units=None,
    )
    provider = SimulatedProvider(scenario, clock)
    classes: list[str] = []

    async def main() -> None:
        for _ in range(30):
            task = asyncio.ensure_future(provider.perform("only"))
            await clock.settle()
            clock.release()
            try:
                await task
            except Exception as exc:
                classes.append(error_class_of(exc))

    run(main())

    assert set(classes) <= {"upstream", "connection", "timeout"}
    assert len(classes) == 30
    assert provider.stats.endpoints["only"].failed == 30


def test_a_storm_is_a_property_of_time_not_of_traffic():
    """The same (endpoint, window) always makes the same weather decision, whoever asks."""
    scenario = _scenario(
        failures=FailureProfile(error_rate=0.0, storm_rate=0.5, storm_check_s=10.0, storm_duration_s=30.0),
        rate_limit=None,
        quota_units=None,
    )
    decisions: list[list[float]] = []
    for _ in range(2):
        clock = ManualClock()
        provider = SimulatedProvider(scenario, clock, seed=4242)

        async def drive(provider=provider, clock=clock) -> list[float]:
            storms = []
            for window in range(8):
                clock.t = window * 10.0
                task = asyncio.ensure_future(provider.perform("only"))
                await clock.settle()
                clock.release()
                try:
                    await task
                except Exception:
                    pass
                finally:
                    storms.append(provider._storm_until["only"])
            return storms

        decisions.append(run(drive()))

    assert decisions[0] == decisions[1]


# ------------------------------------------------------------------ fairness


def test_each_request_sees_the_world_by_its_ordinal_whatever_the_timing():
    """Common random numbers: the j-th request to an endpoint gets the same draws for every client."""
    scenario = _scenario(
        latency=LatencyProfile(median_s=1.0, sigma=0.5, tail_rate=0.3, tail_factor=4.0),
        rate_limit=None,
        quota_units=None,
    )

    def latencies(order: list[str]) -> dict[tuple[str, int], float]:
        clock = ManualClock()
        provider = SimulatedProvider(scenario, clock, seed=99)
        seen: dict[tuple[str, int], float] = {}

        async def drive() -> None:
            for endpoint_id in order:
                ordinal = provider.stats.endpoints[endpoint_id].requests + 1
                task = asyncio.ensure_future(provider.perform(endpoint_id))
                await clock.settle()
                clock.release()
                seen[(endpoint_id, ordinal)] = (await task)["latency_s"]

        run(drive())
        return seen

    # Two endpoints, two different visiting orders, same seed: the shared ordinals must agree.
    scenario = scenario.with_overrides(endpoints=(scenario.endpoints[0], scenario.endpoints[0].__class__(
        id="second",
        capacity=2,
        latency=LatencyProfile(median_s=1.0, sigma=0.5, tail_rate=0.3, tail_factor=4.0),
        failures=FailureProfile(error_rate=0.0, storm_rate=0.0),
    )))
    first = latencies(["only", "only", "second", "only", "second"])
    second = latencies(["second", "only", "second", "second", "only"])

    for key, value in first.items():
        if key in second:
            assert second[key] == pytest.approx(value)


def test_two_runs_with_the_same_seed_are_identical():
    scenario = SCENARIOS["bursty_provider"].with_overrides(jobs=40, concurrency=4)

    def once() -> dict[str, float]:
        clock = ManualClock()

        async def drive() -> dict[str, float]:
            provider = SimulatedProvider(scenario, clock, seed=7)
            for _ in range(20):
                task = asyncio.ensure_future(provider.perform(scenario.endpoints[0].id))
                await clock.settle()
                clock.release()
                with contextlib.suppress(Exception):  # refusals and failures are part of the trace
                    await task
                clock.t += 0.25
            stats = provider.close()
            return {
                "requests": float(stats.requests),
                "admitted": float(stats.admitted),
                "refusals": float(stats.refusals),
                "busy_s": stats.busy_s,
                "offered_s": stats.offered_capacity_s,
            }

        return run(drive())

    assert once() == once()


# ------------------------------------------------------------------ statistics


def test_the_stats_distinguish_offered_capacity_from_served_work():
    clock = ManualClock()
    scenario = _scenario(latency=LatencyProfile(median_s=2.0, sigma=0.0, tail_rate=0.0), rate_limit=None, quota_units=None)
    provider = SimulatedProvider(scenario, clock)

    async def main() -> None:
        task = asyncio.ensure_future(provider.perform("only"))
        await clock.settle()
        clock.release()
        await task
        clock.t += 8.0  # the ManualClock only moves when the test moves it

    run(main())
    stats = provider.close()

    assert stats.endpoints["only"].busy_s == pytest.approx(2.0), "the provider counts the latency it served"
    # capacity 2 over 8 seconds, on a cycle that is at its trough at t=0: the offer is positive,
    # finite, and larger than the work actually served.
    assert stats.offered_capacity_s > 0
    assert 0.0 < stats.utilization < 1.0
    assert stats.elapsed_s == pytest.approx(8.0)
    assert stats.endpoints["only"].peak_in_flight == 1
