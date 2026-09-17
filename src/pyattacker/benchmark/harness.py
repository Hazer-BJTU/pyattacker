"""The closed-loop client: C workers, one pool, one algorithm under test.

Why not the Runner: the benchmark asks "which acquire algorithm copes best with this world", and the
Runner's job — scheduling, checkpointing, storing — is the same for every algorithm and would only
add noise (and real-time waits: a heartbeat timer and several `asyncio.wait_for` calls that a
simulated clock cannot control). So the harness drives the same pieces a real task does — a `Pool`,
a `TaskContext`, `ctx.acquire_lease(...)`, `lease.report(...)`, `Retrying` — and nothing else.

A job is a small pipeline, not a single call: `steps_per_job` sequential steps, each with its own
attempt budget and its own task context, and each step leasing `calls_per_step` times in a row. That
shape is what gives the algorithms something to differ about — `sticky` is defined as "prefer the
endpoint this attempt already used", so a one-call job would make it indistinguishable from `wait`,
and `least_busy` only shows its value when several calls in flight compete for the same endpoints.

`ctx.acquire_lease` is used rather than `pool.acquire` so the algorithm sees a real task context
(`rng`, `meta` for `sticky`, `pools` for `failover`, `holds_from` for the deadlock warning) — the
harness bends over backwards to avoid being the thing that is measured.
"""

from __future__ import annotations

import asyncio
import random
import statistics
import time
from dataclasses import dataclass, field
from typing import Any

from ..algorithm import resolve_algorithm
from ..errors import (
    ConfigError,
    FatalError,
    PyAttackerError,
    ResourceUnavailable,
    RetryableError,
    error_class_of,
)
from ..resource import Pool, Resource
from ..task import TaskContext
from .clock import VirtualClock
from .metrics import percentile
from .provider import ProviderError, SimulatedProvider
from .scenario import Scenario

__all__ = ["BenchmarkError", "BenchmarkStalled", "BenchmarkTimeout", "Harness", "RunResult"]


class BenchmarkError(PyAttackerError):
    """Base for the benchmark's refusals to produce a number.

    A `PyAttackerError`, so the CLI reports these as a message and exit code 2 rather than as a
    traceback — they are outcomes of the run, not crashes in it.
    """


class BenchmarkTimeout(BenchmarkError):
    """The wall-clock budget ran out. Never a benchmark *result*: a run that cannot finish says so."""


class BenchmarkStalled(BenchmarkError):
    """Every resource is DEAD or REVOKED, so no amount of waiting can finish the run.

    A property of the scenario rather than of the algorithm or the machine: the framework retires an
    endpoint for good after `dead_after` consecutive failures, and with nothing left to lease neither
    the workers nor the clock can move (no lease to hand out, no timer to advance to). Reported in
    about a second instead of after the whole wall-clock budget.
    """


@dataclass
class RunResult:
    """One algorithm, one scenario, one seed."""

    scenario: str
    algorithm: str
    seed: int
    metrics: dict[str, float]
    error_classes: dict[str, int] = field(default_factory=dict)
    endpoint_admitted: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "algorithm": self.algorithm,
            "seed": self.seed,
            "metrics": {key: round(value, 6) for key, value in self.metrics.items()},
            "error_classes": dict(sorted(self.error_classes.items())),
            "endpoint_admitted": dict(sorted(self.endpoint_admitted.items())),
        }


class Harness:
    """Runs one algorithm against one scenario, and records what happened."""

    def __init__(
        self,
        scenario: Scenario,
        algorithm: str,
        *,
        seed: int | None = None,
        wall_budget: float = 600.0,
        supervise_interval_s: float = 0.05,
        clock: VirtualClock | None = None,
        provider: SimulatedProvider | None = None,
        kind: str = "provider",
    ) -> None:
        self.scenario = scenario
        # Fail here, on the way in, if the algorithm does not exist: discovered mid-run it would look
        # like every job failing for provider reasons, which is exactly the kind of silent
        # misconfiguration a benchmark must not report as a result.
        resolve_algorithm(algorithm)
        self.algorithm = algorithm
        self.seed = scenario.seed if seed is None else seed
        if wall_budget <= 0:
            # A non-positive budget cancels the run before it starts and would then be reported as
            # "stalled", which is a lie about the scenario: it is the budget that is impossible.
            raise ConfigError(f"wall_budget must be positive, got {wall_budget}")
        self.wall_budget = wall_budget
        self.supervise_interval_s = supervise_interval_s
        self.clock = clock or VirtualClock()
        self.provider = provider or SimulatedProvider(scenario, self.clock, seed=self.seed)
        self.pool = Pool(
            "providers",
            [
                Resource.create(
                    kind,
                    id=endpoint.id,
                    capacity=endpoint.capacity,
                    options={"quota": {"tokens": endpoint.quota_units}} if endpoint.quota_units else {},
                    tags={"endpoint": endpoint.id},
                )
                for endpoint in scenario.endpoints
            ],
            algorithm=algorithm,
            clock=self.clock,
            # No caller in this harness holds two leases from this pool at once, so the deadlock
            # heuristic has nothing to warn about — and it would cost a real (not simulated) timer.
            deadlock_warn_s=None,
        )
        self._next_job = 0
        self._jobs_done = 0
        self._jobs_failed = 0
        self._attempts = 0
        self._failed_attempts = 0
        self._retries_scheduled = 0
        self._attempted_steps = 0
        self._job_latencies: list[float] = []
        self._wait_ms: list[float] = []
        self._request_ms: list[float] = []
        self._error_classes: dict[str, int] = {}

    # ------------------------------------------------------------------ entry point
    def run(self) -> RunResult:
        """Run the scenario. Synchronous: the event loop is an implementation detail of the harness."""
        started = time.monotonic()
        asyncio.run(self._simulate())
        wall_s = time.monotonic() - started
        return self._result(wall_s)

    async def _simulate(self) -> None:
        # All workers are spawned before the first await, so the clock never sees a partial fleet.
        workers = [self.clock.spawn(self._worker(index)) for index in range(self.scenario.concurrency)]
        # `work` is one future for "every worker finished", so the race below has exactly two sides: the
        # run finishing, or the supervisor giving up (the only way the supervisor ever completes).
        work = asyncio.ensure_future(asyncio.gather(*workers))
        supervisor = asyncio.ensure_future(self._supervise())
        try:
            # The budget is real time: a scenario is allowed to be slow, it is not allowed to hang.
            await asyncio.wait({work, supervisor}, timeout=self.wall_budget, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (work, supervisor):
                if not task.done():
                    task.cancel()
            # Let the cancellations land, so every lease is released before the metrics are read.
            await asyncio.gather(work, supervisor, return_exceptions=True)

        if supervisor.done() and not supervisor.cancelled() and supervisor.exception() is not None:
            raise supervisor.exception()
        if work.done() and not work.cancelled():
            failure = work.exception()
            if failure is None:
                return  # every worker finished: the normal path
            # `gather` reports a cancelled child by *setting* a CancelledError rather than cancelling
            # itself, so the cleanup above looks like a failure here. It is not one: anything else is a
            # framework bug, and it must never be folded into a metric.
            if not isinstance(failure, asyncio.CancelledError):
                raise failure
        raise self._timeout_error()

    async def _supervise(self) -> None:
        """Real-time safety net: report a world with nothing left to lease instead of waiting it out."""
        while True:
            await asyncio.sleep(self.supervise_interval_s)
            rows = self.pool.snapshot()
            if rows and all(row["state"] in ("dead", "revoked") for row in rows):
                raise BenchmarkStalled(
                    f"every resource in {self.scenario.name} is dead or revoked after "
                    f"{self.clock.now():.1f}s of simulated time ({self._jobs_done} jobs done): the "
                    "scenario's endpoints can be retired for good, and with nothing left to lease neither "
                    "the workers nor the clock can move. Raise dead_after, add an endpoint, or lower the "
                    "failure rates."
                )

    def _timeout_error(self) -> BenchmarkTimeout:
        simulated = self.clock.now()
        if simulated <= 0.0:
            return BenchmarkTimeout(
                f"wall-clock budget of {self.wall_budget:.0f}s exhausted and simulated time never advanced: "
                "the run is stalled, not slow — a worker is waiting for something that cannot happen "
                f"(workers={self.clock.workers}, parked={self.clock.blocked_workers}, timers={self.clock.pending})"
            )
        return BenchmarkTimeout(
            f"wall-clock budget of {self.wall_budget:.0f}s exhausted after {simulated:.1f}s of simulated time; "
            f"{self._jobs_done} jobs done. Lower --jobs/--seeds/--concurrency, or raise the budget."
        )

    # ------------------------------------------------------------------ the workers
    def acquire_stream(self, job: int, step: int, attempt: int) -> random.Random:
        """The randomness the acquire algorithm under test may consume during one attempt.

        Derived from the logical identity of the attempt, never from the worker that happened to claim
        the job: with a per-worker stream, an algorithm that changes request timing changes which worker
        takes which job, which changes how many draws the *previous* job consumed — and the same logical
        attempt then sees different randomness under two algorithms. The Runner does the same thing for
        the same reason (it seeds per pipeline/task/attempt, see `runner.py`).
        """
        return random.Random(f"{self.seed}:acquire:{job}:{step}:{attempt}")

    def retry_stream(self, job: int, step: int, attempt: int) -> random.Random:
        """The randomness the retry policy may consume, in its own stream: separate subsystem, separate draws."""
        return random.Random(f"{self.seed}:retry:{job}:{step}:{attempt}")

    async def _worker(self, worker_id: int) -> None:
        while (job := self._claim_job()) is not None:
            started = self.clock.now()
            succeeded = await self._run_job(job)
            if succeeded:
                self._jobs_done += 1
                self._job_latencies.append((self.clock.now() - started) * 1000.0)
            else:
                self._jobs_failed += 1

    def _claim_job(self) -> int | None:
        """Jobs are handed out one at a time; the horizon stops new ones rather than truncating one."""
        if self._next_job >= self.scenario.jobs or self.clock.now() >= self.scenario.horizon_s:
            return None
        self._next_job += 1
        return self._next_job - 1

    async def _run_job(self, job: int) -> bool:
        for step in range(self.scenario.steps_per_job):
            if not await self._run_step(job, step):
                return False
        return True

    async def _run_step(self, job: int, step: int) -> bool:
        """One step of a job: attempts, retries, and the retry policy the framework itself applies."""
        policy = self.scenario.retry
        started = self.clock.now()
        attempt = 0
        # One distinct step is being attempted, however many attempts it ends up costing: the
        # denominator of `attempt_inflation`, which is what makes 1.0 mean "no retries".
        self._attempted_steps += 1
        while True:
            attempt += 1
            self._attempts += 1
            context = self._context(job, step, attempt)
            try:
                await self._call_sequence(context)
            except (
                ProviderError,
                ConnectionError,
                TimeoutError,
                RetryableError,
                FatalError,
                ResourceUnavailable,
            ) as exc:
                # `ResourceUnavailable` is in this list on purpose: the algorithm declining to wait is
                # an ordinary failed attempt, and the *policy* decides what it is worth — with the
                # scenario's default `Retrying` it is classified `unknown` and not retried, but a
                # scenario that sets `retry_unknown=True` or `on=(ResourceUnavailable,)` gets the
                # framework's semantics instead of a hidden special case. Anything not listed here (a
                # config error, a framework bug) propagates: turning it into a metric would hide it
                # behind a plausible-looking number.
                self._failed_attempts += 1
                error_class = error_class_of(exc)
                self._error_classes[error_class] = self._error_classes.get(error_class, 0) + 1
                decision = policy.decide(
                    exc,
                    attempts_used=attempt,
                    rng=self.retry_stream(job, step, attempt),
                    elapsed=self.clock.now() - started,
                )
                if not decision["retry"]:
                    return False
                # A retry that is actually going to happen: the counter behind `retry_rate`, as opposed
                # to `failed_attempt_rate`, which counts every failed attempt including abandoned ones.
                self._retries_scheduled += 1
                # The retry backoff is simulated time, like every other wait: a policy that backs off
                # further is not penalised in wall-clock terms, it is penalised in makespan.
                await self.clock.sleep(decision["delay_s"])
            else:
                return True

    async def _call_sequence(self, context: TaskContext) -> None:
        """`calls_per_step` leases, one after another, sharing the task context (which is what sticky reads)."""
        for _ in range(self.scenario.calls_per_step):
            waited_from = self.clock.now()
            # Marked as blocked around the acquire: the clock may advance while a worker waits for a
            # lease, because only a release (which itself needs simulated time) can end that wait.
            with self.clock.blocked():
                lease = await context.acquire_lease(self.pool, algorithm=self.algorithm)
            self._wait_ms.append((self.clock.now() - waited_from) * 1000.0)
            try:
                outcome = await self.provider.perform(lease.resource.id)
            except BaseException as exc:  # reported, then re-raised for the retry policy to judge
                lease.report(ok=False, error=exc)
                raise
            else:
                lease.report(
                    ok=True,
                    latency_ms=outcome["latency_s"] * 1000.0,
                    usage={"tokens": outcome["tokens"]},
                )
                self._request_ms.append(outcome["latency_s"] * 1000.0)
            finally:
                lease.release_now()

    def _context(self, job: int, step: int, attempt: int) -> TaskContext:
        return TaskContext(
            run_id=f"bench-{self.seed}",
            pipeline_id=f"{self.scenario.name}-{job}",
            pipeline_key=f"{self.scenario.name}-{job}",
            pipeline_name=self.scenario.name,
            task_name=f"step{step}",
            seq=step,
            attempt=attempt,
            clock=self.clock,
            pools={"providers": self.pool},
            rng=self.acquire_stream(job, step, attempt),
            default_pool="providers",
        )

    # ------------------------------------------------------------------ what happened
    def _result(self, wall_s: float) -> RunResult:
        provider = self.provider.close()
        pool = self.pool.stats()
        makespan = self.clock.now()
        done = self._jobs_done
        requests = provider.requests
        admitted = [provider.endpoints[endpoint.id].admitted for endpoint in self.scenario.endpoints]
        mean_admitted = statistics.fmean(admitted) if admitted else 0.0
        spread = (
            statistics.stdev(admitted) / mean_admitted
            if len(admitted) > 1 and mean_admitted > 0
            else 0.0
        )
        metrics = {
            "jobs_done": float(done),
            "jobs_failed": float(self._jobs_failed),
            "jobs_unstarted": float(max(0, self.scenario.jobs - self._next_job)),
            "makespan_s": makespan,
            # Its own makespan, not the budget: see METRICS["throughput_rps"]. The completion gate is
            # what keeps the honest denominator from rewarding a run that gave up on most of its work.
            "throughput_rps": (done / makespan) if makespan > 0 else 0.0,
            "requests": float(requests),
            "attempts_per_completed_job": (self._attempts / done) if done else 0.0,
            "attempt_inflation": (self._attempts / self._attempted_steps) if self._attempted_steps else 0.0,
            "failed_attempt_rate": (self._failed_attempts / self._attempts) if self._attempts else 0.0,
            "retry_rate": (self._retries_scheduled / self._attempts) if self._attempts else 0.0,
            "refusal_rate": (provider.refusals / requests) if requests else 0.0,
            "error_rate": (provider.failed / requests) if requests else 0.0,
            "successful_job_latency_p50_ms": percentile(self._job_latencies, 0.50),
            "successful_job_latency_p95_ms": percentile(self._job_latencies, 0.95),
            "successful_job_latency_p99_ms": percentile(self._job_latencies, 0.99),
            "acquire_wait_p50_ms": percentile(self._wait_ms, 0.50),
            "acquire_wait_p99_ms": percentile(self._wait_ms, 0.99),
            "request_latency_p50_ms": percentile(self._request_ms, 0.50),
            "utilization": provider.utilization,
            "endpoint_spread": spread,
            "leases_active_at_end": float(pool.active),
            "wall_s": wall_s,
        }
        return RunResult(
            scenario=self.scenario.name,
            algorithm=self.algorithm,
            seed=self.seed,
            metrics=metrics,
            error_classes=dict(self._error_classes),
            endpoint_admitted={endpoint.id: provider.endpoints[endpoint.id].admitted for endpoint in self.scenario.endpoints},
        )
