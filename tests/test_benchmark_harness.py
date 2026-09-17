"""The harness end to end: accounting, determinism, the wall-clock guard, and the black box.

The black-box property is the one that makes the whole exercise mean anything — an algorithm that
could read the provider's schedule would be tuned to the schedule. Two tests defend it from opposite
sides: nothing the algorithm is handed reaches the environment (`test_the_algorithm_is_handed...`),
and two different algorithms meet the same world draw for draw
(`test_two_algorithms_meet_the_same_world`).
"""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path

import pytest

from pyattacker.benchmark import (
    BenchmarkTimeout,
    Harness,
    ProviderError,
    SimulatedProvider,
    get_scenario,
)
from pyattacker.benchmark.report import run_benchmark
from pyattacker.errors import ResourceUnavailable


def _small(**overrides):
    defaults = {"jobs": 40, "concurrency": 4, "horizon_s": 120.0}
    defaults.update(overrides)
    return get_scenario("bursty_provider").with_overrides(**defaults)


# ------------------------------------------------------------------ accounting and correctness


def test_every_job_is_accounted_for_exactly_once():
    result = Harness(_small(), "wait", seed=11).run()
    metrics = result.metrics

    assert metrics["jobs_done"] + metrics["jobs_failed"] + metrics["jobs_unstarted"] == 40


def test_the_run_ends_with_no_lease_held():
    """A leaked lease is a correctness bug in the harness, and it would also corrupt the metrics."""
    result = Harness(_small(), "wait", seed=12).run()

    assert result.metrics["leases_active_at_end"] == 0


def test_the_horizon_stops_new_jobs_instead_of_truncating_one():
    scenario = _small(jobs=5000, concurrency=2, horizon_s=5.0)

    result = Harness(scenario, "wait", seed=13).run()

    assert result.metrics["jobs_unstarted"] > 0
    assert result.metrics["makespan_s"] >= 5.0
    assert result.metrics["makespan_s"] < 60.0, "the horizon has to bound the run, not merely gate it"


def test_the_same_seed_and_scenario_reproduce_the_same_numbers():
    """Determinism is the reason for a simulated clock: a comparison has to be repeatable."""
    scenario = _small()

    first = Harness(scenario, "wait", seed=14).run()
    second = Harness(scenario, "wait", seed=14).run()

    assert {k: v for k, v in first.metrics.items() if k != "wall_s"} == {
        k: v for k, v in second.metrics.items() if k != "wall_s"
    }
    assert first.endpoint_admitted == second.endpoint_admitted


def test_a_different_seed_is_a_different_world():
    scenario = _small()

    first = Harness(scenario, "wait", seed=15).run()
    second = Harness(scenario, "wait", seed=16).run()

    assert first.metrics["requests"] != second.metrics["requests"] or first.metrics["makespan_s"] != second.metrics["makespan_s"]


def test_run_benchmark_covers_every_algorithm_and_seed():
    report = run_benchmark(_small(), ["wait", "immediate"], seeds=2, wall_budget=120.0)

    assert report.seeds == [_small().seed, _small().seed + 1]
    assert len(report.runs) == 4
    assert report.algorithms == ["wait", "immediate"]


# ------------------------------------------------------------------ the wall-clock guard


def test_the_wall_budget_is_real_time_and_reported_as_a_failure_not_a_result():
    with pytest.raises(BenchmarkTimeout) as excinfo:
        Harness(get_scenario("bursty_provider"), "wait", wall_budget=0.05).run()

    message = str(excinfo.value)
    assert "budget" in message
    assert "simulated time" in message


def test_a_stalled_simulation_says_so_instead_of_timing_out_silently(monkeypatch):
    """A worker waiting for something that cannot happen must not look like "just slow"."""

    async def hang(self, endpoint_id):
        await asyncio.Future()

    monkeypatch.setattr(SimulatedProvider, "perform", hang)

    with pytest.raises(BenchmarkTimeout) as excinfo:
        Harness(_small(jobs=1, concurrency=1), "wait", wall_budget=0.2).run()

    assert "never advanced" in str(excinfo.value)
    assert "stalled" in str(excinfo.value)


# ------------------------------------------------------------------ the black box


class _SpyAlgorithm:
    """Stands in for the algorithm under test and records exactly what it is handed."""

    name = "spy"

    def __init__(self) -> None:
        self.seen: list[tuple[object, object]] = []

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None):
        self.seen.append((ctx, pool))
        lease = pool.try_acquire(ctx=ctx, where=where, **(selector or {}))
        if lease is None:
            raise ResourceUnavailable("the spy declines to wait")
        return lease


def _references_environment(value: object, scenario, provider) -> bool:
    """A shallow search for the environment hiding inside something the algorithm was handed."""
    for candidate in list(vars(value).values()) if hasattr(value, "__dict__") else []:
        if candidate is scenario or candidate is provider:
            return True
        if isinstance(candidate, dict) and any(item is scenario or item is provider for item in candidate.values()):
            return True
    return False


def test_the_algorithm_is_handed_the_pool_but_never_the_environment():
    scenario = _small(jobs=6, concurrency=2)
    spy = _SpyAlgorithm()
    harness = Harness(scenario, spy, seed=17)

    harness.run()

    assert spy.seen, "the algorithm under test has to be asked for something"
    for ctx, pool in spy.seen:
        assert not _references_environment(pool, scenario, harness.provider)
        assert not _references_environment(ctx, scenario, harness.provider)
        assert pool.name == "providers"
        assert "providers" in ctx.pools


class _RecordingProvider(SimulatedProvider):
    """Records the world's answer per (endpoint, ordinal), so two algorithms can be compared."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.outcomes: dict[tuple[str, int], str] = {}

    async def perform(self, endpoint_id: str) -> dict[str, float]:
        ordinal = self.stats.endpoints[endpoint_id].requests + 1
        try:
            outcome = await super().perform(endpoint_id)
        except ProviderError as exc:
            self.outcomes[(endpoint_id, ordinal)] = f"refused:{exc.status}"
            raise
        except Exception as exc:
            self.outcomes[(endpoint_id, ordinal)] = type(exc).__name__
            raise
        self.outcomes[(endpoint_id, ordinal)] = f"served:{outcome['latency_s']:.9f}"
        return outcome


def test_two_algorithms_meet_the_same_world():
    """Common random numbers, observed from outside: for the requests both algorithms got served, the
    world's draws are identical, however differently the two visited it."""
    scenario = _small(jobs=60, concurrency=4)
    recorded: dict[str, dict[tuple[str, int], str]] = {}
    for algorithm in ("wait", "quota_aware"):
        clock_provider = _RecordingProvider(scenario, None)  # clock is injected by the harness below
        harness = Harness(scenario, algorithm, seed=18)
        harness.provider = clock_provider
        clock_provider.clock = harness.clock
        harness.run()
        recorded[algorithm] = clock_provider.outcomes

    shared = set(recorded["wait"]) & set(recorded["quota_aware"])
    comparable = [
        key
        for key in shared
        if recorded["wait"][key].startswith("served") and recorded["quota_aware"][key].startswith("served")
    ]

    assert len(comparable) >= 20, f"the two runs barely overlapped ({len(comparable)} shared requests)"
    for key in comparable:
        assert recorded["wait"][key] == recorded["quota_aware"][key], f"different world for {key}"


# ------------------------------------------------------------------ locality


def test_the_benchmark_opens_no_network_connections(monkeypatch):
    """Requirement three: it is a simulation. Any real network call is a bug.

    `socket.socket` itself is left alone on purpose — asyncio builds its own event loop out of a
    socketpair — so what is blocked here is exactly the part that could reach out: connecting.
    """

    def _no_connect(*args, **kwargs):
        raise AssertionError("the benchmark must not open a network connection")

    monkeypatch.setattr(socket.socket, "connect", _no_connect)
    monkeypatch.setattr(socket, "create_connection", _no_connect)

    result = Harness(_small(jobs=12, concurrency=2), "wait", seed=19).run()

    assert result.metrics["jobs_done"] > 0


def test_the_benchmark_package_imports_nothing_that_can_reach_the_network():
    """The static half of the same guarantee: a future edit cannot smuggle in an HTTP client."""
    import re

    import pyattacker.benchmark as package

    root = Path(package.__file__).parent
    forbidden = ("socket", "http.client", "httplib", "urllib", "requests", "httpx", "aiohttp", "ssl", "subprocess")
    offenders = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for name in forbidden:
            if re.search(rf"^\s*(import|from)\s+{re.escape(name)}\b", text, re.MULTILINE):
                offenders.append(f"{path.name}: {name}")
    assert offenders == []
