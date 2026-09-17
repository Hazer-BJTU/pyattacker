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


def _handmade(series: dict[str, list[float]], metric: str = "jobs_done") -> BenchmarkReport:
    """A report assembled from explicit per-seed values, so the winner logic can be tested at its edges.

    `series[algorithm]` is that algorithm's value on seeds 1..n: writing the values out is the only
    honest way to test a paired comparison, because what matters is not each algorithm's spread but the
    spread of their difference.
    """
    runs = [
        RunResult(scenario="unit", algorithm=algorithm, seed=index + 1, metrics={metric: value})
        for algorithm, values in series.items()
        for index, value in enumerate(values)
    ]
    scenario = _small()
    seeds = sorted({run.seed for run in runs})
    return BenchmarkReport(scenario=scenario, seeds=seeds, runs=runs)


def _plain(metrics: dict[str, dict[str, float]]) -> BenchmarkReport:
    """One run per algorithm: for the cases where the direction of a metric is all that is tested."""
    runs = [
        RunResult(scenario="unit", algorithm=algorithm, seed=1, metrics={name: values[algorithm] for name, values in metrics.items()})
        for algorithm in next(iter(metrics.values()))
    ]
    return BenchmarkReport(scenario=_small(), seeds=[1], runs=runs)


def _multi(series: dict[str, dict[str, list[float]]]) -> BenchmarkReport:
    """A report from explicit per-seed values for several metrics at once.

    `series[algorithm][metric]` holds that algorithm's values on seeds 1..n, so how much work an
    algorithm completed and how good it looked doing it can be set independently — which is the only way
    to test the completion gate and the winner rule against each other.
    """
    runs = [
        RunResult(
            scenario="unit",
            algorithm=algorithm,
            seed=index + 1,
            metrics={name: values[index] for name, values in metrics.items()},
        )
        for algorithm, metrics in series.items()
        for index in range(len(next(iter(metrics.values()))))
    ]
    return BenchmarkReport(scenario=_small(), seeds=sorted({run.seed for run in runs}), runs=runs)


def test_winners_respect_the_direction_of_each_metric():
    report = _plain(
        {
            "jobs_done": {"fast": 100.0, "slow": 10.0},  # higher is better
            "makespan_s": {"fast": 5.0, "slow": 50.0},  # neutral: nobody wins it
            "refusal_rate": {"fast": 0.4, "slow": 0.1},  # lower is better
        }
    )

    assert report.winners("jobs_done") == ["fast"]
    assert report.winners("refusal_rate") == ["slow"]
    assert report.winners("makespan_s") == []


def test_a_gap_inside_the_paired_spread_is_a_tie():
    """Two algorithms whose difference is noise: the row crowns nobody.

    `a` and `b` both swing wildly from seed to seed, and the swing is *not* shared, so the paired
    difference is as noisy as the values themselves — which is the situation where a benchmark honestly
    has nothing to say.
    """
    report = _handmade({"a": [100.0, 130.0, 100.0], "b": [120.0, 100.0, 130.0]})

    assert sorted(report.winners("jobs_done")) == ["a", "b"]


def test_a_constant_paired_difference_is_a_win_however_noisy_the_absolute_values():
    """The case common random numbers exist for: identical world, different client, constant gap.

    `a` beats `b` by 20 jobs on every seed, while both swing by 30 between seeds. Comparing the two
    absolute means with an independent-samples standard error would call this a tie and throw away the
    strongest property of the design; comparing the paired differences sees a gap with no spread at all.
    """
    report = _handmade({"a": [100.0, 130.0, 160.0], "b": [80.0, 110.0, 140.0]})

    assert report.winners("jobs_done") == ["a"]


def test_a_gap_smaller_than_the_one_percent_floor_is_a_tie_even_when_paired():
    report = _handmade({"a": [100.0, 100.0, 100.0], "b": [99.5, 99.5, 99.5]})

    assert sorted(report.winners("jobs_done")) == ["a", "b"]


def test_a_metric_that_is_zero_for_everyone_crowns_nobody():
    """No refusals anywhere is an absence of a difference, not a win for all seven algorithms."""
    report = _plain({"refusal_rate": {"a": 0.0, "b": 0.0}})

    assert report.winners("refusal_rate") == []


def test_a_conditional_metric_cannot_crown_an_algorithm_that_completed_almost_nothing():
    """The reviewer's 1-of-1000 case: fail-fast must not buy a latency or throughput win.

    `fast` has the best successful-job latency in the table and completed 0.1% of the work; the row has
    to be decided between the algorithms that actually did the work, and `fast` has to be visible as
    excluded rather than silently dropped.
    """
    runs = []
    for algorithm, done, latency in (("fast", 1.0, 10.0), ("steady", 1000.0, 500.0), ("slow", 950.0, 900.0)):
        runs.append(
            RunResult(
                scenario="unit",
                algorithm=algorithm,
                seed=1,
                metrics={"jobs_done": done, "successful_job_latency_p95_ms": latency},
            )
        )
    report = BenchmarkReport(scenario=_small(), seeds=[1], runs=runs)

    assert report.winners("successful_job_latency_p95_ms") == ["steady"]
    # `slow` completed 95% of the best, which clears the 90% floor: it stays in the contest and simply
    # loses to `steady`. Only `fast` is excluded, and it is named so the exclusion is visible.
    assert [(name, round(ratio, 3)) for name, ratio in report.excluded_by_completion("successful_job_latency_p95_ms")] == [
        ("fast", 0.001)
    ]
    # A metric that does not depend on completing anything is not gated.
    assert report.excluded_by_completion("jobs_done") == []


def test_an_unsuited_algorithm_has_no_opinion_recorded_about_it():
    """A scenario that cannot exercise an algorithm must not rank it (see Scenario.unsuited).

    Taking `b` out leaves one contestant, and one contestant is not a comparison, so the row has no
    winner — the point is which algorithm was removed from the ranking, not that `a` collected a star
    for outscoring an entry the scenario had already declared meaningless.
    """
    report = _handmade({"a": [100.0], "b": [1.0]})
    report.unsuited = {"b": "this scenario has one pool"}

    assert "b" not in report.comparable("jobs_done")
    assert report.winners("jobs_done") == []
    assert "b (n/a)" in report.render_table()

    # Add a second algorithm the scenario *can* exercise and the row is a comparison again.
    report.runs.append(RunResult(scenario="unit", algorithm="c", seed=1, metrics={"jobs_done": 50.0}))
    assert report.winners("jobs_done") == ["a"]


def test_the_two_failure_modes_are_reported_but_never_crowned():
    """`jobs_failed` and `jobs_unstarted` close the accounting; they are not achievements.

    Each has the wrong sign available for free: an algorithm can shrink `jobs_failed` by never starting
    a job and `jobs_unstarted` by starting everything and failing it. They stay in the table — the
    totals have to add up — and award nothing.
    """
    report = _plain({"jobs_failed": {"idle": 0.0, "busy": 500.0}, "jobs_unstarted": {"idle": 3000.0, "busy": 0.0}})

    assert METRICS["jobs_failed"].better == "neutral"
    assert METRICS["jobs_unstarted"].better == "neutral"
    assert report.winners("jobs_failed") == []
    assert report.winners("jobs_unstarted") == []
    assert "500.0" in report.render_table() and "3000.0" in report.render_table(), "still reported"


def test_the_completion_baseline_ignores_an_algorithm_the_scenario_cannot_exercise():
    """The gate is a statement about comparable work, so an N/A algorithm must not set its floor.

    `n/a` was run because it was asked for by name and completed 5000 jobs. If it set the baseline,
    every algorithm the scenario is actually about would fall below the 90% floor and the conditional
    row would have no contestant left at all.
    """
    report = _multi(
        {
            "a": {"jobs_done": [100.0], "successful_job_latency_p95_ms": [500.0]},
            "b": {"jobs_done": [95.0], "successful_job_latency_p95_ms": [900.0]},
            "n/a": {"jobs_done": [5000.0], "successful_job_latency_p95_ms": [10.0]},
        }
    )
    report.unsuited = {"n/a": "this scenario has one pool"}

    assert report.comparable("successful_job_latency_p95_ms").keys() == {"a", "b"}
    assert report.excluded_by_completion("successful_job_latency_p95_ms") == []
    assert report.winners("successful_job_latency_p95_ms") == ["a"]


def test_one_eligible_contestant_is_not_crowned():
    """The gate can leave a single algorithm: that is not a win, and the report has to say which one.

    `gaveup` finished 0.1% of the work and has the best latency in the table. Crowning `only` would
    dress up a one-horse race as a result; staying silent would read as "nobody did well".
    """
    report = _multi(
        {
            "only": {"jobs_done": [1000.0], "successful_job_latency_p95_ms": [500.0]},
            "gaveup": {"jobs_done": [1.0], "successful_job_latency_p95_ms": [10.0]},
        }
    )

    assert report.winners("successful_job_latency_p95_ms") == []
    assert report.unrivaled("successful_job_latency_p95_ms") == "only"
    assert report.to_dict()["not_compared"] == {"successful_job_latency_p95_ms": "only"}
    assert "not compared: only `only` was eligible" in report.render_markdown()
    assert "successful_job_latency_p95_ms (only)" in report.render_table()


def test_a_lone_algorithm_in_a_report_is_not_crowned():
    """A star means "beat the others"; a one-algorithm run is a diagnostic, not a comparison."""
    report = _multi({"a": {"jobs_done": [100.0, 120.0], "throughput_rps": [1.0, 1.1]}})

    assert report.winners("jobs_done") == []
    assert report.winners("throughput_rps") == []
    # Nothing was removed by the gate either, so there is no exclusion for the report to explain.
    assert report.unrivaled("throughput_rps") is None
    assert report.to_dict()["not_compared"] == {}


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
    assert default_algorithms(_small()) == [
        name for name in ALGORITHMS if name not in {unsuited for unsuited, _ in _small().unsuited}
    ]


def test_the_report_measures_its_own_wall_time():
    """`wall_s` on the report used to be a field nobody assigned, so every report claimed 0.0s."""
    report = _report(("wait",), 1)

    assert report.wall_s > 0.0
    assert f"wall {report.wall_s:.1f}s" in report.render_table()
    assert f"wall {report.wall_s:.1f}s" in report.render_markdown()


def test_cost_and_diagnostic_metrics_crown_nobody():
    """`wall_s` is what the sweep cost and `endpoint_spread` describes a heterogeneous fleet: neither
    is evidence about algorithm quality, so neither gets a winner."""
    report = _report(("wait", "immediate"), 1)

    assert METRICS["wall_s"].better == "neutral"
    assert METRICS["endpoint_spread"].better == "neutral"
    assert report.winners("wall_s") == []
    assert report.winners("endpoint_spread") == []
    assert report.to_dict()["winners"]["wall_s"] == []


def test_the_json_carries_the_exclusions_the_gate_made():
    report = _report(("wait", "immediate"), 1)

    payload = report.to_dict()

    assert set(payload["excluded_by_completion"]) == {
        name for name, metric in METRICS.items() if metric.gated_by_completion
    }
    assert payload["unsuited"] == {}  # nothing unsuited was asked for here
