"""The scenario's arithmetic, and the sanity of the base scenario itself."""

from __future__ import annotations

import math

import pytest

from pyattacker.benchmark import SCENARIOS, LoadCycle, Scenario, get_scenario
from pyattacker.errors import ConfigError


def test_the_capacity_cycle_stays_between_trough_and_peak_and_starts_at_the_trough():
    cycle = LoadCycle(period_s=100.0, trough=0.2, peak=1.0)

    assert cycle.factor(0.0) == pytest.approx(0.2)
    assert cycle.factor(50.0) == pytest.approx(1.0)
    assert cycle.factor(100.0) == pytest.approx(0.2)  # periodic
    values = [cycle.factor(t) for t in range(0, 300)]
    assert min(values) >= 0.2 - 1e-9
    assert max(values) <= 1.0 + 1e-9


def test_the_closed_form_integral_matches_a_numeric_one():
    """The utilisation metric rests on this: an offer that is a property of the interval, not of when
    the client happened to send requests."""
    cycle = LoadCycle(period_s=120.0, trough=0.35, peak=1.0, phase_s=7.0)

    def numeric(start: float, end: float, steps: int = 20000) -> float:
        width = (end - start) / steps
        return sum(cycle.factor(start + (index + 0.5) * width) for index in range(steps)) * width

    for start, end in ((0.0, 60.0), (13.0, 71.5), (0.0, 3.0), (200.0, 200.0)):
        assert cycle.integral(start, end) == pytest.approx(numeric(start, end), rel=1e-6, abs=1e-9)


def test_an_overridden_scenario_is_a_copy_and_leaves_the_registry_alone():
    base = get_scenario("bursty_provider")

    small = base.with_overrides(jobs=5)

    assert small.jobs == 5
    assert base.jobs != 5
    assert SCENARIOS["bursty_provider"] is base


def test_unknown_scenario_names_list_what_is_available():
    """A bad scenario name has to reach the CLI as a config error (exit code 2), not as a traceback."""
    with pytest.raises(ConfigError) as excinfo:
        get_scenario("nope")

    assert "bursty_provider" in str(excinfo.value)


def test_the_base_scenario_covers_the_problems_it_claims_to():
    """Each assumption in the docstring has to be present in the data, or the summary is fiction."""
    scenario = get_scenario("bursty_provider")

    assert len(scenario.endpoints) >= 3
    assert len({endpoint.id for endpoint in scenario.endpoints}) == len(scenario.endpoints)
    # a capacity cycle that actually moves
    assert scenario.cycle.trough < scenario.cycle.peak
    assert scenario.cycle.period_s < scenario.horizon_s, "the run must span at least one cycle"
    # characters that differ, or the pool is a single provider wearing three hats
    capacities = {endpoint.capacity for endpoint in scenario.endpoints}
    medians = {endpoint.latency.median_s for endpoint in scenario.endpoints}
    error_rates = {endpoint.failures.error_rate for endpoint in scenario.endpoints}
    assert len(capacities) == len(scenario.endpoints)
    assert len(medians) == len(scenario.endpoints)
    assert len(error_rates) >= 2
    # every endpoint is metered, and at least one is metered hard enough to be worth routing around
    assert all(endpoint.rate_limit is not None for endpoint in scenario.endpoints)
    assert min(endpoint.rate_limit.per_window for endpoint in scenario.endpoints) < 300
    # storms exist, and latency has a tail
    assert any(endpoint.failures.storm_rate > 0 for endpoint in scenario.endpoints)
    assert all(endpoint.latency.tail_rate > 0 for endpoint in scenario.endpoints)


def test_scenario_budgets_are_consistent():
    scenario = get_scenario("bursty_provider")

    assert scenario.jobs > 0
    assert scenario.concurrency > 0
    assert scenario.steps_per_job >= 1
    assert scenario.calls_per_step >= 2, "sticky needs at least two calls in one task context to matter"
    assert scenario.horizon_s > 0
    assert scenario.retry.max_attempts > 1, "a one-attempt client would measure nothing but luck"


def test_endpoint_effective_capacity_scales_with_the_cycle_and_never_reaches_zero():
    scenario = get_scenario("bursty_provider")
    endpoint = scenario.endpoints[0]

    assert endpoint.effective_capacity(1.0) == max(1, round(endpoint.capacity * endpoint.weight))
    assert endpoint.effective_capacity(0.01) >= 1, "a provider that accepts nothing is a hung benchmark"
    assert endpoint.effective_capacity(0.35) < endpoint.effective_capacity(1.0)


def test_a_scenario_can_be_built_by_hand():
    """The dataclasses are the API: a user can write their own world without touching this module."""
    scenario = Scenario(
        name="by-hand",
        summary="one endpoint, no cycle",
        endpoints=(get_scenario("bursty_provider").endpoints[0],),
        cycle=LoadCycle(period_s=0.0),
        jobs=3,
        concurrency=1,
    )

    assert scenario.cycle.factor(12.3) == scenario.cycle.peak
    assert math.isclose(scenario.cycle.integral(0.0, 10.0), 10.0 * scenario.cycle.peak)
