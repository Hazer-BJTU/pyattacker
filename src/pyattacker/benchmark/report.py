"""Running a comparison and rendering the result.

The report's job is to make an honest table easy to read and a dishonest one impossible to write by
accident: every metric is shown with its unit and direction, the spread across seeds is shown next to
the mean, and every row names its winner — including the rows where the honest answer is "several
algorithms tie" or "this metric does not separate them".
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..algorithm import ALGORITHMS
from .harness import Harness, RunResult
from .metrics import METRICS, aggregate
from .scenario import Scenario

__all__ = ["BenchmarkReport", "default_algorithms", "run_benchmark"]

# Two means within this relative distance are reported as a tie rather than as a winner: at three
# seeds, a 1% gap is not evidence of anything.
TIE_TOLERANCE = 0.01

# The second, larger guard: a row is only a win if the gap survives the seed-to-seed noise. The
# threshold is this many standard errors of the difference between the two means (about 95% for a
# normal difference), computed from the seeds actually run. One seed has no spread to test against,
# which is exactly why a single-seed report crowns the top scorer and warns the reader.
SIGNIFICANCE_K = 2.0


def default_algorithms() -> list[str]:
    """The algorithms the framework ships, in registration order.

    `failover` is included even though a single-pool scenario is not what it is for (it is at its
    best carrying a request across pools); leaving it out would be a quiet editorial choice, and the
    report says so instead.
    """
    return list(ALGORITHMS)


@dataclass
class BenchmarkReport:
    """Every run of every algorithm, plus the aggregates the table shows."""

    scenario: Scenario
    seeds: list[int]
    runs: list[RunResult]
    wall_s: float = 0.0
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

    def winners(self, metric: str) -> list[str]:
        """The algorithms that can claim this metric: the best mean, and anyone within noise of it.

        Two guards against reading a table too hard. A relative `TIE_TOLERANCE` (1%) covers the case of
        a metric with tiny variance; the seed-to-seed spread covers the far more common case where the
        variance is the whole story — a 1.3% edge on a metric that moves 4% between seeds is not a
        result, and a report that crowns it is lying politely.

        Each challenger is compared against the leader with `SIGNIFICANCE_K` standard errors of their
        difference (sample `stdev` over the seeds, so `sd_best^2/n + sd_other^2/n` under the hood).
        Comparing against the leader rather than against the growing group keeps the rule one line
        long and its meaning obvious.
        """
        direction = METRICS[metric].better
        if direction == "neutral":
            return []
        means = {name: self.mean(name, metric) for name in self.algorithms}
        if not means:
            return []
        best = max(means.values()) if direction == "higher" else min(means.values())
        # A metric that is zero everywhere (no refusals, say) separates nothing; calling the zeros
        # "winners" would dress up the absence of a difference as a result.
        if best == 0 and all(value == 0 for value in means.values()):
            return []
        scale = abs(best) if abs(best) > 1e-12 else 1.0
        if direction == "higher":
            leader = max(means, key=lambda name: means[name])
        else:
            leader = min(means, key=lambda name: means[name])
        leader_sd = self.stdev(leader, metric)
        seeds = max(1, len(self.seeds))
        winners = [
            name
            for name, value in means.items()
            if abs(value - best)
            <= max(
                TIE_TOLERANCE * scale,
                SIGNIFICANCE_K * math.sqrt((leader_sd**2 + self.stdev(name, metric) ** 2) / seeds),
            )
        ]
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
        }

    # ------------------------------------------------------------------ rendering
    def render_table(self, *, metrics: list[str] | None = None) -> str:
        """A terminal table: one row per metric, one column per algorithm, best mean marked with `*`."""
        names = metrics or [name for name in METRICS if any(name in run.metrics for run in self.runs)]
        algorithms = self.algorithms
        label_width = max(len(METRICS[name].name) + len(METRICS[name].unit) + 4 for name in names)
        header = "metric".ljust(label_width) + "".join(name.center(16) for name in algorithms)
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
            "difference shares the mark. Rows without a mark separate nobody."
        )
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
    """
    names = algorithms or default_algorithms()
    seed_list = [scenario.seed + offset for offset in range(max(1, seeds))]
    runs: list[RunResult] = []
    for name in names:
        for seed in seed_list:
            result = Harness(
                scenario, name, seed=seed, wall_budget=wall_budget, clock=clock_factory() if clock_factory else None
            ).run()
            runs.append(result)
            if on_run is not None:
                on_run(result)
    return BenchmarkReport(scenario=scenario, seeds=seed_list, runs=runs)
