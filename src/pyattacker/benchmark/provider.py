"""The simulated provider: the world as a client experiences it.

One request in, one outcome out. The provider knows everything about itself — the phase of its
capacity cycle, how many requests are in flight, what is left in its token bucket, whether it is in
a failure storm — and it tells the client none of it. The only way to learn anything here is to send
a request and read the answer, which is the whole point: an algorithm that could read the schedule
would be tuned to the schedule, not to reality.

Two further properties make comparisons fair rather than merely black-box:

* **The world's mood is a function of time, not of the caller.** The capacity cycle is arithmetic;
  storm windows are drawn per `(endpoint, window index)`. Neither depends on how many requests the
  algorithm has sent or where it sent them.
* **Each request's draws are indexed by its ordinal at that endpoint**, not by wall order across the
  run: request *j* to an endpoint always sees the same latency and the same failure roll, whichever
  algorithm produced it (a per-request `random.Random(f"{seed}:{endpoint}:{j}")`). Two algorithms
  therefore meet the same world even though they visit it in a different order. This is the
  variance-reduction trick the simulation literature calls common random numbers, and it is what
  makes a 3% difference between two algorithms meaningful instead of noise.

Errors are raised the way a vendor SDK raises them — a status code, sometimes a `Retry-After` — and
deliberately *not* as the framework's own `RetryableError`. `errors.error_class_of` decides what is
worth retrying, so the benchmark exercises the real classification path rather than a hand-labelled
shortcut.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from .scenario import EndpointProfile, Scenario

__all__ = ["EndpointStats", "ProviderError", "ProviderStats", "SimulatedProvider"]


class ProviderError(Exception):
    """An error a real provider would put on the wire: a status code, and maybe a Retry-After.

    ``status`` is what `error_class_of` classifies (429 -> rate_limit, 503 -> upstream, ...), and
    ``retry_after`` is what `retry_after_of` picks up, so a scenario exercises exactly the same
    handling a vendor SDK's exception would.
    """

    def __init__(self, status: int, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


@dataclass
class EndpointStats:
    """What the environment did, counted. Read only after the run (see `SimulatedProvider.stats`)."""

    requests: int = 0  # attempts aimed at this endpoint
    admitted: int = 0
    refused_capacity: int = 0
    refused_quota: int = 0
    failed: int = 0
    busy_s: float = 0.0  # sum of the latencies actually served
    offered_capacity_s: float = 0.0  # integral of the capacity that was on offer over time
    tokens_used: float = 0.0
    peak_in_flight: int = 0

    @property
    def refusals(self) -> int:
        return self.refused_capacity + self.refused_quota


@dataclass
class ProviderStats:
    """The whole environment's counters, plus the derived numbers the report quotes."""

    endpoints: dict[str, EndpointStats] = field(default_factory=dict)
    elapsed_s: float = 0.0

    @property
    def requests(self) -> int:
        return sum(endpoint.requests for endpoint in self.endpoints.values())

    @property
    def admitted(self) -> int:
        return sum(endpoint.admitted for endpoint in self.endpoints.values())

    @property
    def refusals(self) -> int:
        return sum(endpoint.refusals for endpoint in self.endpoints.values())

    @property
    def failed(self) -> int:
        return sum(endpoint.failed for endpoint in self.endpoints.values())

    @property
    def busy_s(self) -> float:
        return sum(endpoint.busy_s for endpoint in self.endpoints.values())

    @property
    def offered_capacity_s(self) -> float:
        return sum(endpoint.offered_capacity_s for endpoint in self.endpoints.values())

    @property
    def utilization(self) -> float:
        """Served work-seconds over offered capacity-seconds — how much of the offer was used."""
        offered = self.offered_capacity_s
        return (self.busy_s / offered) if offered > 0 else 0.0


class SimulatedProvider:
    """The environment: capacity that ebbs and flows, a token bucket, latency, and failures."""

    def __init__(self, scenario: Scenario, clock, *, seed: int | None = None) -> None:
        self.scenario = scenario
        self.clock = clock
        self.seed = scenario.seed if seed is None else seed
        self.stats = ProviderStats(endpoints={endpoint.id: EndpointStats() for endpoint in scenario.endpoints})
        self._profiles: dict[str, EndpointProfile] = {endpoint.id: endpoint for endpoint in scenario.endpoints}
        # Per-endpoint mutable state. Private by convention and by construction: nothing hands this
        # object to the pool, the algorithm or the task context.
        self._in_flight: dict[str, int] = {endpoint.id: 0 for endpoint in scenario.endpoints}
        self._tokens: dict[str, float] = {}
        self._allowance: dict[str, float] = {}
        self._refilled_at: dict[str, float] = {}
        self._storm_until: dict[str, float] = {}
        self._storm_checked_at: dict[str, float] = {}
        self._integrated_at: dict[str, float] = {}
        for endpoint in scenario.endpoints:
            bucket = endpoint.rate_limit
            self._tokens[endpoint.id] = bucket.burst_size if bucket else math.inf
            self._allowance[endpoint.id] = 1.0
            self._refilled_at[endpoint.id] = 0.0
            self._storm_until[endpoint.id] = 0.0
            self._storm_checked_at[endpoint.id] = 0.0
            self._integrated_at[endpoint.id] = 0.0

    # ------------------------------------------------------------------ the one public door
    def capacity_at(self, endpoint_id: str, t: float) -> int:
        """Concurrent requests this endpoint accepts at time ``t``.

        Public for the *report* (utilisation needs to know what was on offer). Never called by the
        harness while a run is in progress, and never handed to an algorithm — that would turn the
        benchmark into a game of reading the answer key.
        """
        profile = self._profiles[endpoint_id]
        return profile.effective_capacity(self.scenario.cycle.factor(t))

    async def perform(self, endpoint_id: str) -> dict[str, float]:
        """Serve one request, or refuse it, or fail it. Raises exactly what a client would catch."""
        profile = self._profiles[endpoint_id]
        endpoint = self.stats.endpoints[endpoint_id]
        now = self.clock.now()
        self._integrate(endpoint_id, now)
        endpoint.requests += 1
        ordinal = endpoint.requests

        refusal = self._refusal(endpoint_id, profile, now)
        if refusal is not None:
            reason, retry_after = refusal
            if reason == "capacity":
                endpoint.refused_capacity += 1
            else:
                endpoint.refused_quota += 1
            self._tighten(endpoint_id, profile)
            raise ProviderError(429, f"{reason} limit reached at {now:.3f}s", retry_after=retry_after)

        # Admitted. The draws are indexed by ordinal, so request j sees the same world for every
        # algorithm that gets that far.
        rng = random.Random(f"{self.seed}:{endpoint_id}:{ordinal}")
        endpoint.admitted += 1
        self._in_flight[endpoint_id] += 1
        endpoint.peak_in_flight = max(endpoint.peak_in_flight, self._in_flight[endpoint_id])
        latency = profile.latency.sample(rng)
        failure = self._failure(endpoint_id, profile, now, rng)
        tokens = self._consume(endpoint_id, profile)
        endpoint.busy_s += latency
        endpoint.tokens_used += tokens
        try:
            await self.clock.sleep(latency)
        finally:
            self._in_flight[endpoint_id] -= 1
        if failure is not None:
            endpoint.failed += 1
            raise failure
        return {"tokens": tokens, "latency_s": latency}

    def close(self) -> ProviderStats:
        """Fold the tail of the capacity integral in and return the counters (call once, at the end)."""
        now = self.clock.now()
        for endpoint_id in self._profiles:
            self._integrate(endpoint_id, now)
        self.stats.elapsed_s = now
        return self.stats

    # ------------------------------------------------------------------ the world's own dynamics
    def _refusal(self, endpoint_id: str, profile: EndpointProfile, now: float) -> tuple[str, float] | None:
        """Capacity first, then the bucket: a provider that is full says so before it meters you."""
        capacity = self.capacity_at(endpoint_id, now)
        if self._in_flight[endpoint_id] >= capacity:
            # A full endpoint answers fast and asks you to come back shortly: it is busy, not
            # broken, so the delay is short and has nothing to do with the bucket.
            return "capacity", 0.25
        bucket = profile.rate_limit
        if bucket is not None:
            self._refill(endpoint_id, bucket, now)
            if self._tokens[endpoint_id] < 1.0:
                return "quota", bucket.retry_after_s
        return None

    def _refill(self, endpoint_id: str, bucket, now: float) -> None:
        elapsed = now - self._refilled_at[endpoint_id]
        if elapsed <= 0:
            return
        windows = elapsed / bucket.window_s if bucket.window_s > 0 else 0.0
        self._allowance[endpoint_id] = min(1.0, self._allowance[endpoint_id] + bucket.recovery_per_window * windows)
        rate = bucket.refill_per_s * self._allowance[endpoint_id]
        self._tokens[endpoint_id] = min(bucket.burst_size, self._tokens[endpoint_id] + elapsed * rate)
        self._refilled_at[endpoint_id] = now

    def _tighten(self, endpoint_id: str, profile: EndpointProfile) -> None:
        """A refusal costs future allowance: this is the "stricter when pushed" assumption."""
        bucket = profile.rate_limit
        if bucket is None:
            return
        self._allowance[endpoint_id] = max(
            bucket.tightening_floor, self._allowance[endpoint_id] * bucket.tightening
        )

    def _consume(self, endpoint_id: str, profile: EndpointProfile) -> float:
        bucket = profile.rate_limit
        if bucket is None:
            return 0.0
        self._tokens[endpoint_id] = max(0.0, self._tokens[endpoint_id] - 1.0)
        return 1.0

    def _failure(self, endpoint_id: str, profile: EndpointProfile, now: float, rng: random.Random):
        """Independent blips and storms. Returns the exception to raise, or None."""
        in_storm = now < self._storm_until[endpoint_id]
        if not in_storm and now >= self._storm_checked_at[endpoint_id]:
            # Storms are decided per time window from a time-indexed stream: the endpoint's mood is
            # a property of the world, not of how many requests arrived.
            window = int(now // max(profile.failures.storm_check_s, 1e-9))
            weather = random.Random(f"{self.seed}:{endpoint_id}:storm:{window}")
            self._storm_checked_at[endpoint_id] = now + profile.failures.storm_check_s
            if weather.random() < profile.failures.storm_rate:
                self._storm_until[endpoint_id] = now + profile.failures.storm_duration_s
                in_storm = True
        if rng.random() >= profile.failures.error_probability(in_storm=in_storm):
            return None
        roll = rng.random()
        if roll < 0.5:
            return ProviderError(503, "upstream unavailable" + (" (storm)" if in_storm else ""))
        if roll < 0.8:
            return ConnectionError("connection reset by peer")
        return TimeoutError("read timed out")

    def _integrate(self, endpoint_id: str, now: float) -> None:
        """Capacity-seconds on offer since the last event, for the utilisation metric.

        Uses the closed-form integral of the cycle rather than ``capacity_at(previous) * elapsed``:
        the offer is then a property of the interval, not of when this client happened to send
        requests, which is what keeps the utilisation metric comparable across algorithms.
        """
        previous = self._integrated_at[endpoint_id]
        if now > previous:
            profile = self._profiles[endpoint_id]
            offered = profile.capacity * profile.weight * self.scenario.cycle.integral(previous, now)
            self.stats.endpoints[endpoint_id].offered_capacity_s += offered
            self._integrated_at[endpoint_id] = now
