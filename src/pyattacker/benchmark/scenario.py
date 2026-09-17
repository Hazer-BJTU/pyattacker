"""Scenarios: the assumptions a benchmark run makes about the world.

A scenario is not a workload — it is the *environment* the workload runs in. Everything a provider
might do to you is written down here as data, so that a comparison between two acquire algorithms is
a comparison between two algorithms and not between two moods of the test author.

The abstraction, and what each assumption stands for
---------------------------------------------------
Each field below exists because it changes what a *good* strategy looks like; a scenario that varies
only latency would rank the algorithms the same way a spreadsheet would.

* **A capacity cycle** (`LoadCycle`) — the provider is busier at some times than others, so the
  capacity you can actually use is a function of time. A strategy that treats capacity as static
  either wastes the peaks or hammers the troughs. Real version: the vendor's own load, your
  account's fair-share slice, an upstream region failing over and taking your capacity with it.
* **A token bucket with bite** (`RateLimitProfile`) — the harder you push, the more requests come
  back refused, and the refills are slower than the refusals. Real version: `429` with `Retry-After`,
  which is the single most common way a well-written client still fails.
* **Latency that is not a number** (`LatencyProfile`) — a log-normal body plus a slow tail, so
  "in-flight count" and "recently fastest" are both noisy signals. Real version: queueing behind
  other tenants, GC pauses, a model that streams slowly for long prompts.
* **Failures, including correlated ones** (`FailureProfile`) — independent blips, plus storms where
  one endpoint is bad for a while. Real version: a region's networking, a provider incident.
* **Several endpoints with different characters** (`EndpointProfile`) — a fast flaky one, a slow
  steady one, a tightly metered one. Real version: two vendors plus a burst-limited trial key. A
  single strategy cannot be best at all of it, which is the point: the benchmark should be able to
  say "no winner" rather than manufacture one.

What every assumption has in common, and why it makes the comparison objective: it evolves on its
own clock, it is a pure function of simulated time (the cycle) or of the client's *observed* request
history (the bucket), it never consults which algorithm is running, and the randomness is drawn per
`(endpoint, request ordinal)` from a per-endpoint stream. Two algorithms therefore meet the same
world, and the same algorithm meets the same world twice.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace

from ..errors import ConfigError
from ..task import Retrying

__all__ = [
    "EndpointProfile",
    "FailureProfile",
    "LatencyProfile",
    "LoadCycle",
    "RateLimitProfile",
    "Scenario",
    "SCENARIOS",
    "get_scenario",
]


@dataclass(frozen=True)
class LoadCycle:
    """The provider's own busy/free rhythm, as a capacity multiplier between ``trough`` and ``peak``."""

    period_s: float = 120.0
    trough: float = 0.35
    peak: float = 1.0
    phase_s: float = 0.0

    def factor(self, t: float) -> float:
        """Multiplier at simulated time ``t``. Starts at the trough, so a run begins under pressure."""
        if self.period_s <= 0:
            return self.peak
        middle, amplitude = self._middle_amplitude()
        return middle - amplitude * math.cos(2.0 * math.pi * (t - self.phase_s) / self.period_s)

    def integral(self, start: float, end: float) -> float:
        """``∫ factor`` over ``[start, end]`` — capacity-seconds, analytically.

        The utilisation metric needs "how much capacity was on offer", and it must not depend on
        *when the client happened to send requests*: a step-wise sum over request arrivals would
        differ slightly between two algorithms (a left-Riemann sum over a different partition is a
        different number), which would quietly penalise whichever one sampled the cycle differently.
        Closed form, so both see the same offer.
        """
        if end <= start:
            return 0.0
        if self.period_s <= 0:
            return self.peak * (end - start)
        middle, amplitude = self._middle_amplitude()
        omega = 2.0 * math.pi / self.period_s
        return middle * (end - start) - amplitude / omega * (
            math.sin(omega * (end - self.phase_s)) - math.sin(omega * (start - self.phase_s))
        )

    def _middle_amplitude(self) -> tuple[float, float]:
        return (self.peak + self.trough) / 2.0, (self.peak - self.trough) / 2.0


@dataclass(frozen=True)
class LatencyProfile:
    """Response times: a log-normal body (``median_s``, ``sigma``) plus a heavier tail."""

    median_s: float = 0.4
    sigma: float = 0.55
    tail_rate: float = 0.02
    tail_factor: float = 6.0

    def sample(self, rng) -> float:
        """One draw, in seconds. Uses the caller's RNG so the stream is reproducible per endpoint."""
        value = self.median_s * math.exp(self.sigma * rng.gauss(0.0, 1.0)) if self.sigma > 0 else self.median_s
        if self.tail_rate > 0 and rng.random() < self.tail_rate:
            value *= self.tail_factor
        return max(0.0, value)


@dataclass(frozen=True)
class FailureProfile:
    """Independent errors, plus storms: windows in which one endpoint is mostly broken."""

    error_rate: float = 0.01
    storm_rate: float = 0.05
    storm_check_s: float = 10.0
    storm_duration_s: float = 30.0
    storm_error_rate: float = 0.5

    def error_probability(self, *, in_storm: bool) -> float:
        return self.storm_error_rate if in_storm else self.error_rate


@dataclass(frozen=True)
class RateLimitProfile:
    """A token bucket, and the provider's reaction to being pushed.

    ``per_window``/``window_s`` set the refill rate; ``burst`` allows short spikes above it. Every
    refusal shrinks the effective allowance by ``tightening`` (down to ``tightening_floor``), and the
    allowance recovers by ``recovery_per_window`` of the nominal rate each window — so a client that
    spreads its load gets its capacity back, and one that keeps hammering keeps losing it.
    """

    per_window: int = 300
    window_s: float = 60.0
    burst: float = 1.5
    retry_after_s: float = 1.0
    tightening: float = 0.5
    tightening_floor: float = 0.25
    recovery_per_window: float = 1.0

    @property
    def refill_per_s(self) -> float:
        return self.per_window / self.window_s if self.window_s > 0 else float(self.per_window)

    @property
    def burst_size(self) -> float:
        return max(1.0, self.per_window * self.burst)


@dataclass(frozen=True)
class EndpointProfile:
    """One endpoint's character: how much it takes, how fast it answers, how it fails, what it charges."""

    id: str
    capacity: int
    latency: LatencyProfile
    failures: FailureProfile = field(default_factory=FailureProfile)
    rate_limit: RateLimitProfile | None = None
    quota_units: int | None = None  # declared quota for the `quota_aware` algorithm, if any
    weight: float = 1.0  # share of the provider-wide cycle this endpoint feels

    def effective_capacity(self, factor: float) -> int:
        """Concurrent requests the endpoint accepts at a given point of the capacity cycle."""
        return max(1, round(self.capacity * self.weight * factor))


@dataclass(frozen=True)
class Scenario:
    """All assumptions for one benchmark run, plus its budget."""

    name: str
    summary: str
    endpoints: tuple[EndpointProfile, ...]
    cycle: LoadCycle = field(default_factory=LoadCycle)
    horizon_s: float = 600.0  # hard limit on simulated time
    jobs: int = 3000  # jobs to complete; workers stop admitting new ones at the horizon
    concurrency: int = 32  # closed-loop workers, i.e. requests that can be in flight
    # A job is a short pipeline, not one call: `steps_per_job` sequential steps, each with its own
    # attempt budget and task context, each leasing `calls_per_step` times in a row. Sequential steps
    # with a shared context are what give `sticky` an affinity to keep and `least_busy` a choice to
    # make; with one call per job, four of the seven algorithms would be indistinguishable.
    steps_per_job: int = 3
    calls_per_step: int = 2
    seed: int = 20260917
    # The client's own retry policy is an assumption too: it is the same for every algorithm, and a
    # stingy budget would turn the whole comparison into "who ran out of attempts first".
    retry: Retrying = field(
        default_factory=lambda: Retrying(max_attempts=5, base=0.5, factor=2.0, cap=8.0, jitter="full")
    )

    def with_overrides(self, **changes) -> "Scenario":
        """A copy with a few fields replaced — for sweeps and for tests that need a small world."""
        return replace(self, **changes)


def _base_scenario() -> Scenario:
    return Scenario(
        name="bursty_provider",
        summary=(
            "One vendor reached through three endpoints: a fast flaky one, a slow steady one with the "
            "largest capacity, and a tightly metered one. Capacity swings on a 2-minute cycle, the "
            "metered endpoint throttles hard when pushed, latency has a slow tail, and each endpoint "
            "has occasional failure storms."
        ),
        endpoints=(
            # Fast, but it is the one that goes bad: short latencies, three times the error rate of
            # the others, and it is where a storm hurts most.
            EndpointProfile(
                id="fast-flaky",
                capacity=6,
                latency=LatencyProfile(median_s=0.25, sigma=0.5, tail_rate=0.02, tail_factor=5.0),
                failures=FailureProfile(error_rate=0.03, storm_rate=0.08, storm_error_rate=0.6),
                rate_limit=RateLimitProfile(per_window=1200, window_s=60.0, burst=2.0),
                quota_units=1200,
            ),
            # Slow and dependable, and the endpoint most exposed to the cycle: when the provider is
            # busy, this is where the capacity disappears first.
            EndpointProfile(
                id="slow-steady",
                capacity=12,
                latency=LatencyProfile(median_s=1.2, sigma=0.3, tail_rate=0.01, tail_factor=4.0),
                failures=FailureProfile(error_rate=0.004, storm_rate=0.02),
                rate_limit=RateLimitProfile(per_window=2400, window_s=60.0, burst=1.5),
                quota_units=2400,
                weight=1.2,
            ),
            # Cheap and quick, but metered: below the bucket's rate it is the best endpoint in the
            # pool, above it every extra request is a 429 that also costs future allowance.
            EndpointProfile(
                id="metered",
                capacity=4,
                latency=LatencyProfile(median_s=0.5, sigma=0.6, tail_rate=0.03, tail_factor=6.0),
                failures=FailureProfile(error_rate=0.01),
                rate_limit=RateLimitProfile(
                    per_window=180, window_s=60.0, burst=1.4, retry_after_s=1.5, tightening=0.35
                ),
                quota_units=180,
            ),
        ),
        cycle=LoadCycle(period_s=120.0, trough=0.35, peak=1.0),
        horizon_s=600.0,
        jobs=3000,
        concurrency=12,
        steps_per_job=3,
        calls_per_step=2,
    )


SCENARIOS: dict[str, Scenario] = {_base_scenario().name: _base_scenario()}


def get_scenario(name: str) -> Scenario:
    """Look up a scenario by name. A bad name is a `ConfigError`, like every other user-supplied one."""
    try:
        return SCENARIOS[name]
    except KeyError:
        raise ConfigError(f"unknown scenario: {name!r} (available: {sorted(SCENARIOS)})") from None
