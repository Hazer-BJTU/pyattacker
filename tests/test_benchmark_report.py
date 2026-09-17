"""Metrics, aggregation, and the table: mainly guards against the documentation drifting.

`METRICS` is the single declaration of what a run measures. If the harness grows a metric that is not
declared there, the table silently drops a column, the JSON grows a key nobody documents and the
markdown loses a row — so the two lists are asserted equal, in both directions.
"""

from __future__ import annotations

import json

import pytest

from pyattacker.benchmark import METRICS, RunResult, aggregate, get_scenario, percentile, run_benchmark
from pyattacker.benchmark.report import BenchmarkReport, default_algorithms


def _small():
    return get_scenario("bursty_provider").with_overrides(jobs=25, concurrency=3, horizon_s=60.0)


def _report(algorithms=("wait", "immediate"), seeds=1) -> BenchmarkReport:
    return run_benchmark(_small(), list(algorithms), seeds=seeds, wall_budget=120.0)


# ------------------------------------------------------------------ the declaration


def test_the_harness_emits_exactly_the_declared_metrics():
    result = _report(("wait",), 1).runs[0]

    assert set(result.metrics) == set(METRICS)


def test_every_metric_declares_a_unit_and_a_direction():
    for name, metric in METRICS.items():
        assert metric.name == name
        assert metric.unit, name
        assert metric.better in ("higher", "lower", "neutral"), name
        assert len(metric.description) > 20, f"{name} needs a description a reader can use"


# ------------------------------------------------------------------ aggregation


def test_aggregate_reports_mean_min_max_and_spread():
    runs = [{"jobs_done": value} for value in (10.0, 20.0, 30.0)]

    combined = aggregate(runs)["jobs_done"]

    assert combined["mean"] == pytest.approx(20.0)
    assert combined["min"] == 10.0
    assert combined["max"] == 30.0
    assert combined["stdev"] == pytest.approx(10.0)


def test_a_single_run_has_no_spread():
    assert aggregate([{"jobs_done": 5.0}])["jobs_done"]["stdev"] == 0.0
    assert aggregate([]) == {}


def test_percentile_matches_the_textbook_definition():
    values = [float(value) for value in range(1, 101)]

    assert percentile(values, 0.5) == pytest.approx(50.5)
    assert percentile(values, 0.99) == pytest.approx(99.01)
    assert percentile([], 0.5) == 0.0
    assert percentile([7.0], 0.9) == 7.0


# ------------------------------------------------------------------ winners


def _handmade(metrics: dict[str, dict[str, float]]) -> BenchmarkReport:
    """A report assembled by hand, so the winner logic can be tested at its edges."""
    runs = [
        RunResult(
            scenario="unit",
            algorithm=algorithm,
            seed=1,
            metrics={name: values[algorithm] for name, values in metrics.items()},
        )
        for algorithm in next(iter(metrics.values()))
    ]
    scenario = _small()
    return BenchmarkReport(scenario=scenario, seeds=[1], runs=runs)


def test_winners_respect_the_direction_of_each_metric():
    report = _handmade(
        {
            "jobs_done": {"fast": 100.0, "slow": 10.0},  # higher is better
            "makespan_s": {"fast": 5.0, "slow": 50.0},  # neutral: nobody wins it
            "refusal_rate": {"fast": 0.4, "slow": 0.1},  # lower is better
        }
    )

    assert report.winners("jobs_done") == ["fast"]
    assert report.winners("refusal_rate") == ["slow"]
    assert report.winners("makespan_s") == []


def test_a_gap_inside_the_tolerance_is_reported_as_a_tie():
    report = _handmade({"jobs_done": {"a": 100.0, "b": 99.5, "c": 80.0}})

    assert sorted(report.winners("jobs_done")) == ["a", "b"]


def test_a_metric_that_is_zero_for_everyone_crowns_nobody():
    """No refusals anywhere is an absence of a difference, not a win for all seven algorithms."""
    report = _handmade({"refusal_rate": {"a": 0.0, "b": 0.0}})

    assert report.winners("refusal_rate") == []


# ------------------------------------------------------------------ rendering


def test_the_table_shows_every_algorithm_and_metric():
    report = _report(("wait", "backoff"), 1)

    table = report.render_table()

    for algorithm in report.algorithms:
        assert algorithm in table
    for name in METRICS:
        assert name in table
    assert "bursty_provider" in table


def test_the_markdown_renders_a_table_a_human_can_read():
    report = _report(("wait", "immediate"), 2)

    markdown = report.render_markdown()

    assert markdown.startswith("### `bursty_provider`")
    assert report.scenario.summary.split(".")[0] in markdown
    assert "| metric |" in markdown
    for endpoint in report.scenario.endpoints:
        assert endpoint.id in markdown
    assert markdown.count("\n|") > len(METRICS), "one markdown row per metric, plus the endpoint table"


def test_the_json_form_is_serialisable_and_carries_the_aggregates():
    report = _report(("wait",), 2)

    payload = json.loads(json.dumps(report.to_dict()))

    assert payload["scenario"] == "bursty_provider"
    assert payload["seeds"] == report.seeds
    assert len(payload["runs"]) == 2
    assert "mean" in payload["aggregates"]["wait"]["jobs_done"]
    assert "jobs_done" in payload["winners"]
    # every run is present with its own metrics and environment counters
    assert set(payload["runs"][0]) == {"scenario", "algorithm", "seed", "metrics", "error_classes", "endpoint_admitted"}


def test_the_default_algorithm_list_is_what_the_framework_ships():
    from pyattacker.algorithm import ALGORITHMS

    assert default_algorithms() == list(ALGORITHMS)
    assert "wait" in default_algorithms() and "quota_aware" in default_algorithms()
