"""Benchmark acquire algorithms against simulated providers.

The kernel answers "run this work reliably"; this package answers a different question: *which
strategy should that work use?* It is a simulation, not a micro-benchmark — the world is a set of
written-down assumptions (a capacity cycle, a token bucket that tightens under pressure, latency with
a tail, failures and storms, several endpoints of different character), the client is a closed loop
of workers running the real `Pool` and the real algorithm, and time is simulated so a ten-minute
scenario costs seconds.

```python
from pyattacker.benchmark import get_scenario, run_benchmark

report = run_benchmark(get_scenario("bursty_provider"), ["wait", "backoff", "least_busy"], seeds=3)
print(report.render_table())
```

See `docs/benchmark.md` for what the base scenario assumes, what each metric means, and how to read
the table without over-reading it.
"""

from __future__ import annotations

from .clock import ScaledClock, VirtualClock
from .harness import BenchmarkError, BenchmarkStalled, BenchmarkTimeout, Harness, RunResult
from .metrics import METRICS, Metric, aggregate, percentile
from .provider import EndpointStats, ProviderError, ProviderStats, SimulatedProvider
from .report import BenchmarkReport, default_algorithms, run_benchmark
from .scenario import (
    SCENARIOS,
    EndpointProfile,
    FailureProfile,
    LatencyProfile,
    LoadCycle,
    RateLimitProfile,
    Scenario,
    get_scenario,
)

__all__ = [
    "METRICS",
    "SCENARIOS",
    "BenchmarkError",
    "BenchmarkReport",
    "BenchmarkStalled",
    "BenchmarkTimeout",
    "EndpointProfile",
    "EndpointStats",
    "FailureProfile",
    "Harness",
    "LatencyProfile",
    "LoadCycle",
    "Metric",
    "ProviderError",
    "ProviderStats",
    "RateLimitProfile",
    "RunResult",
    "ScaledClock",
    "Scenario",
    "SimulatedProvider",
    "VirtualClock",
    "aggregate",
    "default_algorithms",
    "get_scenario",
    "percentile",
    "run_benchmark",
]
