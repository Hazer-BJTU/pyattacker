"""What a benchmark run measures, and how runs are combined.

Two deliberate choices:

* **No composite score.** Every metric is reported on its own, with its direction, and the report
  names the best algorithm per metric. Folding "throughput", "p99 latency" and "how much you
  annoyed the provider" into one weighted number would hide the weights — which are the actual
  opinion — inside an arithmetic result that looks objective.
* **Every metric is declared once.** `METRICS` is the single source for the harness's output keys,
  the report's columns, the JSON field names and the documentation, and a test asserts the harness
  emits nothing undeclared. A metric that exists only in the code is a metric nobody can interpret.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass

__all__ = ["METRICS", "Metric", "aggregate", "percentile"]


@dataclass(frozen=True)
class Metric:
    """One measured quantity: what it is, in what unit, and which way is better."""

    name: str
    unit: str
    better: str  # "higher" | "lower" | "neutral"
    description: str
    #: True when the metric is only defined for work that *completed*, so an algorithm that gave up on
    #: most of the workload cannot be crowned on it (see `BenchmarkReport.winners`). Latency percentiles
    #: and throughput are the obvious cases: an algorithm that abandons 99.7% of its jobs has very few
    #: latencies to be slow at, and a small makespan to divide by.
    gated_by_completion: bool = False


def _m(name: str, unit: str, better: str, description: str, *, gated: bool = False) -> tuple[str, Metric]:
    return name, Metric(name=name, unit=unit, better=better, description=description, gated_by_completion=gated)


METRICS: dict[str, Metric] = dict(
    [
        _m("jobs_done", "jobs", "higher", "Jobs that finished successfully before the horizon."),
        _m(
            "jobs_failed",
            "jobs",
            "neutral",
            "Jobs that gave up: a step exhausted its retry budget. Reported because the accounting has to "
            "close (`done + failed + unstarted = jobs`), not awarded: an algorithm can shrink this number "
            "by never starting the jobs it would have failed.",
        ),
        _m(
            "jobs_unstarted",
            "jobs",
            "neutral",
            "Jobs the workers never began because the horizon arrived first. Diagnostic for the same "
            "reason: an algorithm can shrink it by starting and failing everything immediately.",
        ),
        _m(
            "makespan_s",
            "s",
            "neutral",
            "Simulated seconds from the start until the last worker stopped: the horizon, plus whatever "
            "step was still in flight when it arrived. Read it next to `jobs_done`, since finishing early "
            "by failing fast is not an achievement.",
        ),
        _m(
            "throughput_rps",
            "jobs/s",
            "higher",
            "Completed jobs per simulated second of the run's *own* makespan: what came out, over the time "
            "it took. Conditional on purpose — with a fixed denominator this would be `jobs_done` rescaled "
            "by a constant, and without the completion gate an algorithm could manufacture throughput by "
            "abandoning the queue and finishing early.",
            gated=True,
        ),
        _m("requests", "requests", "neutral", "Requests sent, including refusals and retries."),
        _m(
            "attempts_per_completed_job",
            "attempts",
            "lower",
            "Step attempts the run spent per completed job — what finishing cost. The zero-retry baseline "
            "is `steps_per_job`, not 1.0: a three-step job costs three attempts even when nothing fails, "
            "and attempts spent on jobs that later gave up are in the numerator but not the denominator.",
            gated=True,
        ),
        _m(
            "attempt_inflation",
            "ratio",
            "lower",
            "Step attempts per *attempted* step (1.0 = every step that was attempted succeeded on its "
            "first try). Numerator and denominator cover the same steps, so this is retry pressure "
            "without the volume. Conditional, because which steps get attempted is the algorithm's "
            "choice: one that only takes the uncontended ones has an easier sample of steps.",
            gated=True,
        ),
        _m(
            "failed_attempt_rate",
            "ratio",
            "lower",
            "Failed step attempts as a fraction of all step attempts. Includes attempts the retry policy "
            "then abandoned, which `retry_rate` excludes.",
        ),
        _m(
            "retry_rate",
            "ratio",
            "lower",
            "Retries the policy actually scheduled, as a fraction of all step attempts. "
            "`failed_attempt_rate` minus this is the share of failures that were given up on.",
            gated=True,
        ),
        _m("refusal_rate", "ratio", "lower", "Requests refused by the provider (429) per request sent."),
        _m("error_rate", "ratio", "lower", "Requests that failed with an error per request sent."),
        _m(
            "successful_job_latency_p50_ms",
            "ms",
            "lower",
            "Median time of a job that *succeeded*, retries included. Conditional by name: jobs that "
            "gave up have no completion time, so the value describes the survivors.",
            gated=True,
        ),
        _m(
            "successful_job_latency_p95_ms",
            "ms",
            "lower",
            "95th percentile job time among successful jobs: the tail a user notices.",
            gated=True,
        ),
        _m(
            "successful_job_latency_p99_ms",
            "ms",
            "lower",
            "99th percentile job time among successful jobs.",
            gated=True,
        ),
        _m(
            "acquire_wait_p50_ms",
            "ms",
            "lower",
            "Median time a step spent waiting for a lease. Measured over acquisitions that succeeded, "
            "so an algorithm that abandons the queue instead of waiting is measured on the ones it did queue for.",
            gated=True,
        ),
        _m(
            "acquire_wait_p99_ms",
            "ms",
            "lower",
            "99th percentile lease wait: what a saturated pool costs a caller that waits.",
            gated=True,
        ),
        _m("request_latency_p50_ms", "ms", "neutral", "Median served request latency (a property of the world)."),
        _m("utilization", "ratio", "higher", "Served work-seconds over offered capacity-seconds."),
        _m(
            "endpoint_spread",
            "ratio",
            "neutral",
            "Relative spread of admitted requests across endpoints (0 = perfectly even). Diagnostic: the "
            "endpoints are deliberately unlike each other, so piling traffic onto the dependable one is "
            "good practice that *raises* this number. A fairness metric would have to compare each "
            "endpoint's admitted share against the share it was offered.",
        ),
        _m("leases_active_at_end", "leases", "lower", "Leases still held when the run ended; must be zero."),
        _m("wall_s", "s", "neutral", "Real seconds the harness spent simulating: cost, never quality."),
    ]
)


def percentile(values: list[float], q: float) -> float:
    """Linear-interpolated percentile (the same definition `statistics.quantiles` uses, without its bins)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * max(0.0, min(1.0, q))
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def aggregate(runs: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    """Combine several seeds' metric dicts into mean/min/max/stdev per metric.

    The spread is the honest part: a benchmark that reports one number per algorithm invites the
    reader to treat a 2% gap as a fact. ``stdev`` is the sample standard deviation (0.0 for one run).
    """
    if not runs:
        return {}
    names = [name for name in METRICS if any(name in run for run in runs)]
    combined: dict[str, dict[str, float]] = {}
    for name in names:
        values = [float(run[name]) for run in runs if name in run]
        combined[name] = {
            "mean": statistics.fmean(values),
            "min": min(values),
            "max": max(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        }
    return combined
