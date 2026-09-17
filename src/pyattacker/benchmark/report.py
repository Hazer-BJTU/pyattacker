"""Running a comparison and rendering the result.

The report's job is to make an honest table easy to read and a dishonest one impossible to write by
accident: every metric is shown with its unit and direction, the spread across seeds is shown next to
the mean, and every row names its winner — including the rows where the honest answer is "several
algorithms tie" or "this metric does not separate them".
"""

from __future__ import annotations

import math
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..algorithm import ALGORITHMS
from ..errors import ConfigError
from .harness import Harness, RunResult
from .metrics import METRICS, aggregate
from .scenario import Scenario

__all__ = ["BenchmarkReport", "default_algorithms", "run_benchmark"]

# Two means within this relative distance are reported as a tie rather than as a winner: at three
# seeds, a 1% gap is not evidence of anything.
TIE_TOLERANCE = 0.01

# The second, larger guard: a row is only a win if the gap survives the seed-to-seed noise. The
# threshold is this many standard errors of the *paired* difference (every algorithm runs every seed,
# so the runs are paired by construction and the covariance between them is exactly what common random
# numbers buys). A heuristic, deliberately: with three seeds the Student-t critical value for 95% is
# 4.3, and pretending 2.0 means "95% significant" would be a stronger claim than the experiment
# supports. One seed has no spread to test against, which is why a single-seed report crowns the top
# scorer and the spread column is what warns the reader.
SIGNIFICANCE_K = 2.0

# An algorithm has to complete this fraction of the best algorithm's jobs before any *quality* metric is
# allowed to crown it — not only the ones whose value is undefined without completions, but every row
# whose denominator the client partly controls. See Metric.requires_comparable_completion.
COMPLETION_FLOOR = 0.9


def default_algorithms(scenario: Scenario | None = None) -> list[str]:
    """The algorithms the framework ships, minus the ones a scenario declares itself unsuited for.

    A scenario says which of its algorithms it cannot exercise (`Scenario.unsuited`): a single-pool
    world has nothing for `failover` to fail over to, and the pool's default selection is already
    least-busy-first, so `least_busy` is the same code path as `wait` there. Ranking them anyway
    produces a number that looks comparable and is not, which is worse than saying "not run".
    """
    names = list(ALGORITHMS)
    if scenario is None:
        return names
    unsuited = {name for name, _ in scenario.unsuited}
    return [name for name in names if name not in unsuited]


@dataclass
class BenchmarkReport:
    """Every run of every algorithm, plus the aggregates the table shows."""

    scenario: Scenario
    seeds: list[int]
    runs: list[RunResult]
    wall_s: float = 0.0
    #: Algorithms that were run although the scenario cannot exercise them, with the reason.
    unsuited: dict[str, str] = field(default_factory=dict)
    _aggregates: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)

    # ------------------------------------------------------------------ aggregation
    @property
    def algorithms(self) -> list[str]:
        seen: list[str] = []
        for run in self.runs:
            if run.algorithm not in seen:
                seen.append(run.algorithm)
        return seen

    def aggregates(self, algorithm: str) -> dict[str, dict[str, float]]:
        if not self._aggregates:
            for name in self.algorithms:
                self._aggregates[name] = aggregate([run.metrics for run in self.runs if run.algorithm == name])
        return self._aggregates[algorithm]

    def mean(self, algorithm: str, metric: str) -> float:
        return self.aggregates(algorithm).get(metric, {}).get("mean", 0.0)

    def stdev(self, algorithm: str, metric: str) -> float:
        return self.aggregates(algorithm).get(metric, {}).get("stdev", 0.0)

    def series(self, metric: str) -> dict[str, dict[int, float]]:
        """Per-algorithm values indexed by seed, which is what makes a paired comparison possible."""
        series: dict[str, dict[int, float]] = {}
        for run in self.runs:
            if metric in run.metrics:
                series.setdefault(run.algorithm, {})[run.seed] = run.metrics[metric]
        return series

    def excluded_by_completion(self, metric: str) -> list[tuple[str, float]]:
        """Algorithms the completion gate kept out of a quality row, with their completion ratio.

        Returned for the report to print: an algorithm that would have won a latency row by finishing
        0.3% of the workload should be visible as *excluded*, not silently dropped and not crowned.
        """
        if not METRICS[metric].requires_comparable_completion:
            return []
        # The baseline is the best *comparable* completion. An algorithm the scenario cannot exercise,
        # run only because it was asked for by name, must not set the threshold that excludes the
        # algorithms the scenario is actually about.
        suited = [name for name in self.series(metric) if name not in self.unsuited]
        best_done = max((self.mean(name, "jobs_done") for name in suited), default=0.0)
        if best_done <= 0:
            return []
        excluded = []
        for name in suited:
            ratio = self.mean(name, "jobs_done") / best_done
            if ratio < COMPLETION_FLOOR:
                excluded.append((name, ratio))
        return sorted(excluded, key=lambda item: -item[1])

    def comparable(self, metric: str) -> dict[str, dict[int, float]]:
        """The runs this metric may be compared on: suited algorithms that clear the completion gate."""
        series = self.series(metric)
        eligible = {name: values for name, values in series.items() if name not in self.unsuited}
        if METRICS[metric].requires_comparable_completion:
            gated = {name for name, _ in self.excluded_by_completion(metric)}
            eligible = {name: values for name, values in eligible.items() if name not in gated}
        return eligible

    def unrivaled(self, metric: str) -> str | None:
        """The one algorithm a gated metric is left with, when the gate removed every other contestant.

        `winners` refuses to crown it, so the report has to name it: "only this one was eligible" is a
        statement about the field, and silence would read as "nobody did well". Restricted to gated
        metrics on purpose, and to rows that started with more than one contestant — this explains a row
        the gate emptied, not a report that only ever contained one algorithm.
        """
        if not METRICS[metric].requires_comparable_completion:
            return None
        if len(self.series(metric)) < 2:
            return None
        eligible = self.comparable(metric)
        return next(iter(eligible)) if len(eligible) == 1 else None

    def winners(self, metric: str) -> list[str]:
        """The algorithms that can claim this metric: the best mean, and anyone within noise of it.

        Two guards against reading a table too hard. A relative `TIE_TOLERANCE` (1%) covers the case of
        a metric with tiny variance; the seed-to-seed spread covers the far more common case where the
        variance is the whole story — a 1.3% edge on a metric that moves 4% between seeds is not a
        result, and a report that crowns it is lying politely.

        Each challenger is compared against the leader with `SIGNIFICANCE_K` standard errors of their
        *paired per-seed difference* — `stdev(challenger - leader) / sqrt(n)` over the seeds both ran —
        which is what the common-random-numbers design buys: the seed-to-seed noise the two algorithms
        share cancels, leaving the difference. Comparing against the leader rather than against the
        growing group keeps the rule one line long and its meaning obvious.
        """
        info = METRICS[metric]
        direction = info.better
        if direction == "neutral":
            return []
        eligible = self.comparable(metric)
        # A star is a comparative statement. One contestant is not a comparison, whether the others
        # were excluded by the completion gate or never ran.
        if len(eligible) < 2:
            return []
        means = {name: statistics.fmean(values.values()) for name, values in eligible.items()}
        best = max(means.values()) if direction == "higher" else min(means.values())
        # A metric that is zero everywhere (no refusals, say) separates nothing; calling the zeros
        # "winners" would dress up the absence of a difference as a result.
        if best == 0 and all(value == 0 for value in means.values()):
            return []
        scale = abs(best) if abs(best) > 1e-12 else 1.0
        leader = max(means, key=lambda name: means[name]) if direction == "higher" else min(means, key=lambda name: means[name])
        leader_series = eligible[leader]
        winners = []
        for name, values in eligible.items():
            gap = abs(means[name] - best)
            common = sorted(set(values) & set(leader_series))
            # Paired by seed: the spread that matters is the spread of the *difference*, which is small
            # exactly when common random numbers worked.
            spread = statistics.stdev([values[seed] - leader_series[seed] for seed in common]) if len(common) > 1 else 0.0
            standard_error = spread / math.sqrt(len(common)) if common else 0.0
            if gap <= max(TIE_TOLERANCE * scale, SIGNIFICANCE_K * standard_error):
                winners.append(name)
        return sorted(winners, key=lambda name: (-means[name] if direction == "higher" else means[name]))

    # ------------------------------------------------------------------ serialisation
    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario.name,
            "summary": self.scenario.summary,
            "seeds": self.seeds,
            "jobs": self.scenario.jobs,
            "concurrency": self.scenario.concurrency,
            "wall_s": round(self.wall_s, 3),
            "runs": [run.as_dict() for run in self.runs],
            "aggregates": {
                name: {metric: {k: round(v, 6) for k, v in stats.items()} for metric, stats in self.aggregates(name).items()}
                for name in self.algorithms
            },
            "winners": {metric: self.winners(metric) for metric in METRICS},
            "excluded_by_completion": {
                metric: [
                    {"algorithm": name, "completion_ratio": round(ratio, 6)}
                    for name, ratio in self.excluded_by_completion(metric)
                ]
                for metric in METRICS
                if METRICS[metric].requires_comparable_completion
            },
            "unsuited": dict(self.unsuited),
            # Rows where the completion gate left a single contestant: no star, and the reason on record.
            "not_compared": {metric: name for metric in METRICS if (name := self.unrivaled(metric)) is not None},
        }

    # ------------------------------------------------------------------ rendering
    def render_table(self, *, metrics: list[str] | None = None) -> str:
        """A terminal table: one row per metric, one column per algorithm, best mean marked with `*`."""
        names = metrics or [name for name in METRICS if any(name in run.metrics for run in self.runs)]
        algorithms = self.algorithms
        label_width = max(len(METRICS[name].name) + len(METRICS[name].unit) + 4 for name in names)
        header = "metric".ljust(label_width) + "".join(
            (f"{name} (n/a)" if name in self.unsuited else name).center(16) for name in algorithms
        )
        lines = [header, "-" * len(header)]
        for metric in names:
            info = METRICS[metric]
            best = set(self.winners(metric))
            cells = []
            for algorithm in algorithms:
                mean = self.mean(algorithm, metric)
                mark = "*" if algorithm in best else " "
                if info.unit == "s" or info.unit == "ms":
                    cells.append(f"{mean:>11.2f}{mark}   ")
                elif info.unit in ("jobs", "requests", "attempts", "leases"):
                    cells.append(f"{mean:>11.1f}{mark}   ")
                else:
                    cells.append(f"{mean:>11.4f}{mark}   ")
            lines.append(f"{info.name} [{info.unit}]".ljust(label_width) + "".join(cells))
        lines.append("")
        lines.append(
            f"scenario {self.scenario.name} | {len(self.seeds)} seed(s) {self.seeds} | "
            f"{self.scenario.jobs} jobs, {self.scenario.steps_per_job} steps, {self.scenario.calls_per_step} calls each | "
            f"wall {self.wall_s:.1f}s"
        )
        lines.append(
            "* best mean; anyone whose gap is inside the 1% tolerance or two standard errors of the "
            "paired difference shares the mark. Rows without a mark separate nobody."
        )
        excluded = [
            (metric, name, ratio)
            for metric in names
            for name, ratio in self.excluded_by_completion(metric)
        ]
        if excluded:
            detail = ", ".join(f"{name} on {metric} ({ratio:.1%} of the best completion)" for metric, name, ratio in excluded[:4])
            lines.append(
                "quality rows only consider algorithms that completed at least "
                f"{COMPLETION_FLOOR:.0%} of the best completion; excluded: {detail}"
            )
        sole = {metric: name for metric in names if (name := self.unrivaled(metric)) is not None}
        if sole:
            detail = ", ".join(f"{metric} ({name})" for metric, name in sole.items())
            lines.append(f"no winner where a single algorithm was eligible to be compared: {detail}")
        if self.unsuited:
            detail = "; ".join(f"{name} ({reason})" for name, reason in sorted(self.unsuited.items()))
            lines.append(f"not applicable in {self.scenario.name}: {detail}")
        return "\n".join(lines)

    def render_markdown(self, *, metrics: list[str] | None = None) -> str:
        """The same table as markdown, plus the environment's own numbers — what goes into the docs."""
        names = metrics or [name for name in METRICS if any(name in run.metrics for run in self.runs)]
        algorithms = self.algorithms
        lines = [
            f"### `{self.scenario.name}`",
            "",
            self.scenario.summary,
            "",
            f"Seeds: {', '.join(str(seed) for seed in self.seeds)} · "
            f"{self.scenario.jobs} jobs x {self.scenario.steps_per_job} steps x {self.scenario.calls_per_step} calls, "
            f"{self.scenario.concurrency} workers · horizon {self.scenario.horizon_s:.0f}s simulated · "
            f"wall {self.wall_s:.1f}s",
            "",
            "| metric | " + " | ".join(f"`{name}`" for name in algorithms) + " | best |",
            "|---" * (len(algorithms) + 2) + "|",
        ]
        for metric in names:
            info = METRICS[metric]
            best = self.winners(metric)
            cells = []
            for algorithm in algorithms:
                mean = self.mean(algorithm, metric)
                spread = self.stdev(algorithm, metric)
                rendered = f"{mean:,.2f}" if info.unit in ("s", "ms") else f"{mean:,.4f}"
                if spread > 0:
                    rendered = f"{rendered} ±{spread:,.2f}" if info.unit in ("s", "ms") else f"{rendered} ±{spread:,.4f}"
                cells.append(rendered)
            verdict = ", ".join(f"`{name}`" for name in best) if best else "—"
            gated = self.excluded_by_completion(metric)
            sole = self.unrivaled(metric)
            if sole is not None:
                verdict = f"not compared: only `{sole}` was eligible"
            elif gated:
                verdict += " (excluded: " + ", ".join(f"`{name}` {ratio:.1%}" for name, ratio in gated) + ")"
            lines.append(f"| {info.name} ({info.unit}) | " + " | ".join(cells) + f" | {verdict} |")
        lines.append("")
        lines.append("Per-endpoint admissions (last run of each algorithm):")
        lines.append("")
        lines.append("| algorithm | " + " | ".join(f"`{endpoint.id}`" for endpoint in self.scenario.endpoints) + " |")
        lines.append("|---" * (len(self.scenario.endpoints) + 1) + "|")
        for algorithm in algorithms:
            last = next((run for run in reversed(self.runs) if run.algorithm == algorithm), None)
            if last is None:
                continue
            cells = [str(last.endpoint_admitted.get(endpoint.id, 0)) for endpoint in self.scenario.endpoints]
            lines.append(f"| `{algorithm}` | " + " | ".join(cells) + " |")
        if self.unsuited:
            lines.append("")
            lines.append(
                "Not applicable in this scenario, and marked N/A above: "
                + "; ".join(f"`{name}` — {reason}" for name, reason in sorted(self.unsuited.items()))
                + "."
            )
        return "\n".join(lines)


def run_benchmark(
    scenario: Scenario,
    algorithms: list[str] | None = None,
    *,
    seeds: int = 3,
    wall_budget: float = 600.0,
    on_run: Callable[[RunResult], None] | None = None,
    clock_factory: Callable[[], object] | None = None,
) -> BenchmarkReport:
    """Run every algorithm over the same scenario, one seed at a time, and collect the results.

    The seed list is derived from the scenario's own seed, so a report is reproducible from its name
    and a seed count. `wall_budget` applies per run, not to the whole sweep: a single run that cannot
    finish raises `BenchmarkTimeout` with the numbers it did reach. `clock_factory` exists so the same
    scenario can be replayed on the reference (real-time) clock and compared — see `ScaledClock`.

    The budget arguments are validated rather than clamped: a sweep that ran a different experiment than
    the one it was asked for would report the wrong numbers under the right heading.
    """
    if seeds < 1:
        raise ConfigError(f"seeds must be at least 1, got {seeds}: a sweep needs a seed to be reproducible")
    names = algorithms or default_algorithms(scenario)
    seed_list = [scenario.seed + offset for offset in range(seeds)]
    runs: list[RunResult] = []
    started = time.monotonic()
    for name in names:
        for seed in seed_list:
            result = Harness(
                scenario, name, seed=seed, wall_budget=wall_budget, clock=clock_factory() if clock_factory else None
            ).run()
            runs.append(result)
            if on_run is not None:
                on_run(result)
    return BenchmarkReport(
        scenario=scenario,
        seeds=seed_list,
        runs=runs,
        wall_s=time.monotonic() - started,
        # Asked for explicitly, run anyway, marked N/A in the table.
        unsuited={name: reason for name, reason in scenario.unsuited if name in names},
    )
