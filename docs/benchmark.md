# Benchmarking Acquire Algorithms

`pyattacker.benchmark` simulates an API provider and runs the library's real acquire algorithms
against it. It answers one question: **which acquire algorithm should this workload use?** A scenario
is the input, a metrics vector the output, and a result is reproducible from a scenario name, a seed
and a seed count. The package is stdlib-only and opens no sockets.

## 1. What this is, and what it is not

It is not a micro-benchmark: nothing here measures this machine, and `wall_s` is a *cost* column, never
a quality signal. It is a simulation, so the numbers are a property of the assumptions written down in
the scenario: change an assumption and the ranking can change, because the base scenario's token bucket
punishes a client that hammers it. Hence two rules: no composite score (section 6), and a scenario may
report "no winner" (the base scenario is deliberately three endpoints of different character).

## 2. How a run works

`scenario -> simulated provider -> closed-loop workers -> metrics`.

* **Scenario** (`scenario.py`) — assumptions as frozen dataclasses: cycle, latency, failures, limits,
  endpoints, job shape, retry policy, budget.
* **Provider** (`provider.py`) — `perform(endpoint_id)` serves one request, refuses it with a 429, or
  fails it with a vendor-shaped error, knowing the world's state and telling the client none of it.
* **Workers** (`harness.py`) — `concurrency` tasks claiming jobs of `steps_per_job` sequential steps,
  each step leasing `calls_per_step` times via `ctx.acquire_lease(pool, algorithm=...)`. `Pool`,
  `TaskContext`, the algorithms, `Retrying` and `errors.error_class_of` are the real ones.

**The `Runner` is deliberately not used.** Its scheduling, checkpointing and storing are identical for
every algorithm, and it contains real-time waits a simulated clock cannot control (a heartbeat
`asyncio.sleep`, several `asyncio.wait_for` calls). Driving it would either hang a virtual-clock run or
force the run onto the real clock, which section 5 exists to avoid. `deadlock_warn_s=None` is passed
for the same reason: the deadlock heuristic costs a real timer.

## 3. The base scenario, `bursty_provider`

One vendor reached through three endpoints; 3000 jobs x 3 steps x 2 calls, 12 workers, a 600s horizon,
and one retry policy for every algorithm (`Retrying(max_attempts=5, base=0.5, factor=2.0, cap=8.0,
jitter="full")`). `sticky` and `least_busy` have something to do only because a job is a pipeline, which
the scenario tests assert (`tests/test_benchmark_scenario.py::test_the_base_scenario_covers_the_problems_it_claims_to`).

| Assumption (field) | Real-world problem it stands for | What it discriminates between |
|---|---|---|
| Capacity cycle: `LoadCycle(period_s=120, trough=0.35, peak=1.0)`, starting at the trough; capacity is `max(1, round(capacity x weight x factor))` | The vendor's own load, your fair-share slice, a region failing over and taking your capacity with it | Strategies that treat capacity as static (`immediate`, and `quota_aware`'s fixed declared quota) against strategies that retry and re-queue. It makes *when* you ask as important as *where* |
| Token bucket that tightens when pushed: refill `per_window/window_s`, `burst` allows spikes; every refusal multiplies the allowance by `tightening` (floor `tightening_floor`), recovered by `recovery_per_window` per window | `429` with `Retry-After`, the most common way a well-written client still fails | Clients that spread load and get capacity back against those that keep hammering and keep losing it. `metered` tightens by 0.35 and retries after 1.5s; the others tighten by 0.5 |
| Latency that is not a number: log-normal body (`median_s`, `sigma`) plus a slow tail (`tail_rate`, `tail_factor`); the endpoints use 0.25s/0.5, 1.2s/0.3 and 0.5s/0.6, tails of 2%, 1%, 3% | Queueing behind other tenants, GC pauses, a model that streams slowly for long prompts | Whether "in-flight count" and "recently fastest" are usable signals at all, and separates the p50 story from the p95/p99 story |
| Failures including correlated ones: `FailureProfile` (defaults `error_rate=0.01`, `storm_rate=0.05`, `storm_duration_s=30`, `storm_error_rate=0.5`, each endpoint overriding); a storm is decided per `(seed, endpoint, time window)` | Independent blips plus a provider incident, or a region's networking going bad for a while | How fast an algorithm notices that one endpoint is currently bad, and how much retry budget it burns learning that. Errors are `ProviderError(503)`, `ConnectionError`, `TimeoutError` and 429, so the framework's own classification runs |
| Several endpoints of different character: `fast-flaky` (capacity 6, 0.25s, error 0.03, storm 0.08 at 0.6), `slow-steady` (capacity 12 x weight 1.2, 1.2s, error 0.004, storm 0.02), `metered` (capacity 4, 0.5s, quota 180) | Two vendors plus a burst-limited trial key; the cheap fast one is the one that goes bad | Place-versus-time tradeoffs. No single strategy is best at all three, which is why the report must be able to return several winners or none |
| Declared quotas: `quota_units` per endpoint (1200/2400/180), consumed through `lease.report(usage={"tokens": n})` | A plan with a visible allowance | `quota_aware`'s ranking signal. It is declared metadata, not the provider's refusal behaviour — section 8 |

## 4. Why the comparison is objective

| Rule | Where it lives | Test that enforces it |
|---|---|---|
| The world's mood is a function of time, not of the caller: capacity is arithmetic in `t`, and storm windows are drawn per `(endpoint, window index)`, never from traffic | `provider.py` (`capacity_at`, `_failure`) | `tests/test_benchmark_provider.py::test_a_storm_is_a_property_of_time_not_of_traffic` |
| Common random numbers: latency and failure rolls come from `random.Random(f"{seed}:{endpoint}:{ordinal}")`, indexed by the request's ordinal *at that endpoint*, so two algorithms meet the same world even though they visit it in a different order | `provider.py` (`perform`) | `tests/test_benchmark_provider.py::test_each_request_sees_the_world_by_its_ordinal_whatever_the_timing`; `tests/test_benchmark_harness.py::test_two_algorithms_meet_the_same_world` compares every ordinal both reached and refuses to pass on fewer than 20 shared requests |
| Nothing hands the algorithm a reference to the environment: the harness builds a `Pool` and a `TaskContext` and nothing else, `SimulatedProvider` is private to it, and `capacity_at()` is public only so the metrics can integrate the capacity on offer | `harness.py` (`_context`, `_call_sequence`) | `tests/test_benchmark_harness.py::test_the_algorithm_is_handed_the_pool_but_never_the_environment` walks everything a spy algorithm is given and fails if the scenario or the provider is reachable |
| The same seed reproduces the numbers exactly (every metric except `wall_s`) | `scenario.seed` -> `Harness.seed` -> per-run streams | `tests/test_benchmark_harness.py::test_the_same_seed_and_scenario_reproduce_the_same_numbers` |
| No sockets, and nothing in the package can reach the network | stdlib-only imports | `tests/test_benchmark_harness.py::test_the_benchmark_opens_no_network_connections`; `::test_the_benchmark_package_imports_nothing_that_can_reach_the_network` |
| Errors are the ones a vendor SDK raises, so `errors.error_class_of` classifies rather than a hand-written label | `provider.py` (`ProviderError`) | `tests/test_benchmark_provider.py::test_failures_look_like_provider_errors_the_framework_can_classify` |
| Utilisation's "capacity on offer" does not depend on when the client sent requests: the cycle is integrated in closed form, not summed over arrivals | `scenario.py` (`LoadCycle.integral`) | `tests/test_benchmark_scenario.py::test_the_closed_form_integral_matches_a_numeric_one` |

## 5. Time

Provider time is simulated because the interesting scenarios span minutes (a capacity cycle, a storm, a
quota window) and a comparison costing real minutes per algorithm would not be run. `VirtualClock` keeps
a timer heap and moves the whole simulation to the next deadline; `sleep()` is a real await on a future,
so the caller blocks and the driver decides when the deadline arrives.

Time advances only when all three hold: (1) a timer is pending; (2) every registered worker is parked,
either inside `sleep()` or inside `blocked()`, the marker around an await that only a release or time
can end (a lease acquire); and (3) the event loop has run 16 consecutive probe callbacks with no
observable activity. Condition 2 alone is not enough: a coroutine can sit inside an await that is
already resolvable — a broadcast that fired while it was queued, a lease just released — and is runnable
with no time passing. Condition 3 closes that window: the probe re-arms with `loop.call_soon`, which
appends to the tail of the ready queue, so pending continuations run before the next probe tick. The
test suite's `FakeClock` was unusable because it advances time *inside* the sleeper (`t += seconds`):
32 workers sleeping 1s at once would land at 32 simulated seconds.

Hand-computed timelines pin the rule down (`tests/test_benchmark_clock.py`): 32 concurrent 1s sleeps
leave the clock at 1.0, not 32; timers fire in deadline order while a worker still working holds time
back; pure yields never move time and never hang.

**Differential check against the real clock.** `ScaledClock(speedup=10.0)` compresses real time instead,
so running the same scenario on both is how a bug in the advance rule would show up as a wrong number
rather than as a hang. On `bursty_provider` with `jobs=120, concurrency=6, horizon_s=90.0`, `wait`, seed
3:

| metric | `VirtualClock` | `ScaledClock(speedup=10.0)` |
|---|---|---|
| `jobs_done` | 120.0 | 120.0 |
| `makespan_s` | 81.819 | 82.293 |
| `refusal_rate` | 0.000 | 0.000 |
| `error_rate` | 0.028 | 0.028 |
| `utilization` | 0.308 | 0.311 |
| `throughput_rps` | 1.467 | 1.458 |

Agreement is within 0.6% on makespan and 1% on utilisation, for 8.4s of wall clock for the pair. This
confirms time-keeping and throughput, not the refusal path: in this small world demand never saturates
capacity and both clocks report zero refusals.

**Known limitation.** `Wait(timeout=...)` and `AcquireTimeout` are enforced by the event loop's real
clock: `Wait` blocks on `pool.wait_slot`, which awaits an `asyncio.Event` under
`asyncio.wait_for(..., remaining)`. A virtual-clock run therefore cannot see a simulated acquisition
timeout, so scenarios avoid acquisition timeouts — the harness acquires with no `timeout` — and bound
the run with `horizon_s`, which stops new jobs and never truncates one in flight.

## 6. Metrics

`METRICS` is the single source for the harness's output keys, the report's columns, the JSON field names
and `bench --list`; a test asserts the harness emits exactly that set
(`tests/test_benchmark_report.py::test_the_harness_emits_exactly_the_declared_metrics`).

| Metric | Unit | Better | Meaning |
|---|---|---|---|
| `jobs_done` | jobs | higher | Jobs that finished successfully before the horizon |
| `jobs_failed` | jobs | lower | Jobs that gave up: a step exhausted its retry budget |
| `jobs_unstarted` | jobs | lower | Jobs the workers never began because the horizon arrived first |
| `makespan_s` | s | neutral | Simulated seconds until the last job finished or gave up; read it next to `jobs_done` |
| `throughput_rps` | jobs/s | higher | Completed jobs per simulated second of makespan |
| `requests` | requests | neutral | Requests sent, including refusals and retries |
| `attempts_per_job` | attempts | lower | Step attempts per completed job (1.0 = no retries) |
| `retry_rate` | ratio | lower | Failed step attempts as a fraction of all step attempts |
| `refusal_rate` | ratio | lower | Requests refused by the provider (429) per request sent |
| `error_rate` | ratio | lower | Requests that failed with an error per request sent |
| `job_latency_p50_ms` | ms | lower | Median wall time of a job, retries included |
| `job_latency_p95_ms` | ms | lower | 95th percentile job time: the tail a user notices |
| `job_latency_p99_ms` | ms | lower | 99th percentile job time |
| `acquire_wait_p50_ms` | ms | lower | Median time a step spent waiting for a lease |
| `acquire_wait_p99_ms` | ms | lower | 99th percentile lease wait: what a saturated pool costs |
| `request_latency_p50_ms` | ms | neutral | Median served request latency (a property of the world) |
| `utilization` | ratio | higher | Served work-seconds over offered capacity-seconds |
| `endpoint_spread` | ratio | lower | Relative spread of admitted requests across endpoints (0 = even) |
| `leases_active_at_end` | leases | lower | Leases still held when the run ended; must be zero |
| `wall_s` | s | lower | Real seconds the harness spent simulating (cost, not quality) |

**A vector, not a composite.** Folding throughput, p99 latency and "how much you annoyed the provider"
into one weighted number would hide the weights — which are the actual opinion — inside an arithmetic
result that looks objective. The report names the best algorithm per metric instead, so the reader sees
that the winner of one row is often the loser of another.

**Reading the table.** A `*` marks every algorithm that can claim the row: the best mean, plus anyone
whose gap is not distinguishable from noise. Two guards, whichever is wider:

* `TIE_TOLERANCE = 0.01` in relative terms — at three seeds, a 1% gap is not evidence of anything; and
* `SIGNIFICANCE_K = 2.0` standard errors of the difference between the two means, estimated from the
  seeds actually run (`sd_leader^2/n + sd_challenger^2/n`, with n the number of seeds).

The second guard is what stops the report from crowning a 1.3% win on a metric that moves 4% between
seeds. Each challenger is compared against the leader rather than against the growing group, which keeps
the rule one line long and its meaning obvious. With a single seed there is no spread to test against, so
the top scorer is crowned — one seed is not a comparison, and the `±` column is what says so. A row is
unmarked when its direction is `neutral` (`makespan_s`, `requests`, `request_latency_p50_ms`) or when
every algorithm scores zero on it
(`tests/test_benchmark_report.py::test_a_metric_that_is_zero_for_everyone_crowns_nobody`).

**Seeds** —
`run_benchmark` derives its seed list from `scenario.seed` (`seed + 0 .. seeds - 1`), runs each algorithm
on each seed, and reports the mean with `min`, `max` and sample `stdev` from `aggregate()`; the markdown
form prints `mean ±stdev`. At three seeds a 1% gap is not evidence of anything, which is why the spread
is printed next to every mean and feeds the significance guard above.

## 7. How to run it

`uv run pyattacker bench [options]`. The full flag table is
[`docs/cli.md`](cli.md#bench--compare-the-acquire-algorithms-in-simulation); `pyattacker bench --list`
prints the same surface with the scenarios, the algorithms and every metric, and is the thing to run
when the two disagree.

The worked example whose numbers section 8 quotes is `uv run pyattacker bench --markdown
/tmp/bench-full.md`: 7 algorithms x 3 seeds = 21 runs of 3000 jobs x 3 steps x 2 calls on 12 workers,
in **15-20 seconds of wall clock** on a laptop-class machine (measured runs land at 15.2s, 17.7s and
18.2s; the spread is the machine, not the simulation, which is deterministic). A fast check of two
algorithms on a smaller world (`--algorithms wait,immediate --seeds 1 --jobs 200 --concurrency 4
--horizon 60`) costs about 0.13s.

The Python API is the same machinery:

```python
from pyattacker.benchmark import Harness, get_scenario, run_benchmark

scenario = get_scenario("bursty_provider")
report = run_benchmark(scenario, ["wait", "least_busy"], seeds=3)
report.render_table()      # terminal table, `*` on each row's best mean
report.render_markdown()   # markdown, plus per-endpoint admissions
report.to_dict()           # JSON-serialisable, aggregates and winners included
one = Harness(scenario.with_overrides(jobs=120, concurrency=6, horizon_s=90.0), "wait", seed=3).run()
one.metrics["throughput_rps"], one.error_classes, one.endpoint_admitted
```

**Writing your own scenario.** The dataclasses are the API: `Scenario`, `EndpointProfile`,
`LatencyProfile`, `FailureProfile`, `RateLimitProfile`, `LoadCycle`. `with_overrides(**changes)`
returns a copy and leaves `SCENARIOS` alone, and a scenario can be built by hand
(`tests/test_benchmark_scenario.py::test_an_overridden_scenario_is_a_copy_and_leaves_the_registry_alone`,
`::test_a_scenario_can_be_built_by_hand`). Give it a `name`, a one-line `summary` (the markdown renderer
prints it) and endpoints. A scenario with several endpoints is still **one** `Pool` in the harness, so
it cannot yet exercise `failover` across vendors (section 9).

**Staying inside the budget.** The defaults are nowhere near the 10-minute limit: the whole 21-run sweep
measured 15.2s, and per-run cost ranged from 0.06s (`immediate`) to 1.09s (`failover`). The knobs are
`--jobs` and `--seeds` (they scale the work directly), then `--horizon`, then `--concurrency` (which
changes the world, not just the cost); `--wall-budget` is the guard, not the tuning knob.

**`BenchmarkTimeout`.** `Harness.run` wraps the simulation in `asyncio.wait_for(..., wall_budget)` and
raises rather than returning partial metrics — a run that cannot finish says so. If simulated time
advanced, the message reports the seconds and jobs done and suggests a cheaper run; if it never advanced
it says the run is "stalled, not slow" (workers parked, timers pending, something waiting for an event
that cannot happen), with the worker, parked and pending counts. Both are covered by
`tests/test_benchmark_harness.py::test_the_wall_budget_is_real_time_and_reported_as_a_failure_not_a_result`
and `::test_a_stalled_simulation_says_so_instead_of_timing_out_silently`.

## 8. What the current data says

The 3-seed means from the sweep above (seeds 20260917, 20260918, 20260919); `--markdown` writes the full
table, with `min`/`max`/`stdev` and per-endpoint admissions.

| Metric | `immediate` | `wait` | `backoff` | `least_busy` | `failover` | `sticky` | `quota_aware` |
|---|---|---|---|---|---|---|---|
| `jobs_done` | 7.67 | 1,375.67 | 1,303.00 | 1,375.67 | 1,375.67 | 1,394.00 | 811.00 |
| `throughput_rps` | 0.8759 | 2.2745 | 2.1436 | 2.2745 | 2.2745 | 2.2954 | 1.3370 |
| `refusal_rate` | 0.3262 | 0.2534 | 0.3053 | 0.2534 | 0.2534 | 0.2538 | 0.4387 |
| `error_rate` | 0.0220 | 0.0257 | 0.0254 | 0.0257 | 0.0257 | 0.0232 | 0.0167 |
| `job_latency_p95_ms` | 8,499.78 | 9,428.68 | 9,451.19 | 9,428.68 | 9,428.68 | 9,197.98 | 10,189.92 |
| `utilization` | 0.4825 | 0.6402 | 0.5753 | 0.6402 | 0.6402 | 0.6360 | 0.6058 |

**`immediate` is not a contender in a world with contention.** It completed 7.67 jobs of 3,000 (0.26%)
and failed 2,992.33, with `attempts_per_job` 408.38 against `wait`'s 5.63. It has the shortest
`makespan_s` (8.99s, a neutral row with no winner) and it is the sole winner of five marked rows —
`jobs_unstarted` (0.00), `wall_s` (0.06s), `job_latency_p99_ms` (8,894ms), `acquire_wait_p99_ms` (0.00)
and `retry_rate` (0.01) — every one of those "wins" being the same fact: it fails fast instead of
queueing, so it finishes by giving up. `retry_rate` deserves its own note: it abandons a step with
`ResourceUnavailable`, which the harness records as a strategy outcome rather than a failed attempt, so
its 2,992 abandoned jobs never appear as retries at all. A benchmark that only reported retry rate
would rank `immediate` first.

**`wait`, `least_busy` and `failover` produce identical numbers** — not similar, identical, on every
metric and every seed. This is a fact about the framework that the benchmark surfaced, not a harness
artifact:

* `LeastBusy.acquire` tries `pool.try_acquire(...)` and falls back to `Wait()`. `try_acquire` is
  `select(None)`, and `select` with no `key` already ranks "least-used first, round-robin on ties"
  (`src/pyattacker/resource.py`, the sort plus the `self._rr` rotation), so it is the same code path as
  `Wait` on one pool.
* `Failover` with `pools=()` uses `[pool.name]` — one pool. It tries `Immediate()` there, then defers
  to `Wait()` on the same pool, which begins with the same `try_acquire`.

So `least_busy` is currently an alias for the default on a single pool, and `failover` is untested in
its intended shape: it is designed for several pools, and a multi-vendor scenario is the next one.

**`quota_aware` does measurably worse here**: 811.00 jobs done against 1,375.67 for `wait` (-41%),
`throughput_rps` 1.3370 (-41%), `refusal_rate` 0.4387 (+73% relative), `retry_rate` 0.6524,
`attempts_per_job` 9.76. The mechanism is visible in the admissions: it ranks by the *declared* quota
ratio (`QuotaAware.score` reads `resource.options["quota"]["tokens"]`), so it avoids the metered endpoint
(337 admissions against `wait`'s 1,374, on seed 20260917) and overloads `slow-steady` (4,032 against
3,108, at 1.2s median — hence `job_latency_p50_ms` 6,886.05 against 3,534.91). Declared quota is not the
refusals it will meet:
the provider refuses on live in-flight capacity and on its own bucket with tightening, which the
declared number does not reflect. It still wins `error_rate` (0.0167) by sending less traffic to
`fast-flaky` (error rate 0.03) — a different mix, not a better client.

**`sticky` and the plain strategies are close, and the report says so.** `sticky` completed 1,394.00
jobs against `wait`'s 1,375.67 — +1.3%, against a seed-to-seed stdev of ±67.73 on its own mean. Under
the significance rule the two share every row they lead, so the honest verdict is "no winner", not
"`sticky` is 1.3% better". Its `endpoint_spread` (0.5592 against 0.5668) is likewise a tie. Making
`sticky` distinguishable needs a world where reuse actually pays (section 9).

**Several metrics did not separate the algorithms at all.** `acquire_wait_p50_ms` is 0.00 for all seven,
and `acquire_wait_p99_ms` is 0.00 for everything except the strategies that never queue (`immediate`;
the rest sit between 205.91 and 545.58). The client rarely waits for a *lease*, because `Pool.select`
almost always finds a free slot; what stops a request is the provider's refusal, met while the lease is
already held. `makespan_s`, `requests` and `request_latency_p50_ms` are declared `neutral`, so they are
printed without a winner by construction, and `leases_active_at_end` is 0.0 everywhere, where the
zero-everywhere rule crowns nobody.

## 9. Limitations and next steps

* **Single pool.** `Harness` builds one `Pool` and puts it in `TaskContext.pools` under one name, so
  `failover` cannot be measured in its intended shape and `least_busy` cannot be distinguished from the
  default ranking. A multi-vendor scenario needs several pools, not just several endpoints.
* **`sticky`'s affinity spans only the calls inside one step.** The `TaskContext` is created per
  attempt, so the affinity recorded by the first call lives only until the second — two calls of six.
* **Utilisation integrates a continuous cycle** (`capacity x weight x integral of factor`) while the
  provider enforces the integer `max(1, round(capacity x weight x factor))`: a smooth approximation of a
  step function, comparable between algorithms but not exact.
* **The real-clock mode is slow and CPU-inflated by design.** At `speedup=20`, real CPU time ages the
  simulation 20x; it exists to keep `VirtualClock` honest, not to produce the numbers.
* **One scenario ships.** `SCENARIOS` has a single entry, so today "the ranking" means "the ranking in
  this one world".

Candidate scenarios, in rough order of how much they would add: multi-vendor failover (two pools, one
degrading, to give `failover` something to do); one dead endpoint (capacity pinned low, or storms at
1.0) to see which strategies notice and which keep aiming at it; a thundering herd after an outage
(every worker retries at once when an endpoint returns, where `backoff`'s jitter should earn its keep);
and a per-endpoint cache that pays out for reuse — the world `sticky` was written for.
