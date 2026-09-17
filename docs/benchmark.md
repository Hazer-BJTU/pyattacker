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
| The world's mood is a function of time, not of the caller: capacity is arithmetic in `t`, and `storm_until(endpoint, t)` is a pure function of `(seed, endpoint, t)` — a storm runs from its time window's start, so the weather does not depend on when requests arrive | `provider.py` (`capacity_at`, `storm_until`, `_storm_window`) | `tests/test_benchmark_provider.py::test_storm_state_does_not_depend_on_request_cadence` compares two *different* traffic schedules at the same timestamps, and `::test_the_storm_interval_is_anchored_to_its_window` pins the anchoring |
| Common random numbers: latency and failure rolls come from `random.Random(f"{seed}:{endpoint}:{ordinal}")`, indexed by the request's ordinal *at that endpoint*. Two algorithms therefore share the same **exogenous** randomness — the same dice and the same weather — though not the same realized provider state, which diverges because the bucket and the in-flight count react to what each of them did | `provider.py` (`perform`) | `tests/test_benchmark_provider.py::test_each_request_sees_the_world_by_its_ordinal_whatever_the_timing`; `tests/test_benchmark_harness.py::test_two_algorithms_see_the_same_exogenous_draws` compares every ordinal both reached and refuses to pass on fewer than 20 shared requests |
| Client-side randomness follows the logical identity, not the worker: the acquire algorithm and the retry policy each draw from `Random(f"{seed}:{{acquire,retry}}:{job}:{step}:{attempt}")` | `harness.py` (`acquire_stream`, `retry_stream`) | `tests/test_benchmark_harness.py::test_client_randomness_follows_the_logical_identity_not_the_worker` |
| A scenario declares the algorithms it cannot exercise, and they are not ranked on numbers that cannot mean anything | `scenario.py` (`Scenario.unsuited`), `report.py` (`default_algorithms`, `winners`) | `tests/test_benchmark_harness.py::test_a_scenario_declares_which_algorithms_it_cannot_exercise`, `::test_an_algorithm_the_scenario_cannot_exercise_is_marked_not_ranked` |
| A wait that only a cooldown can end is a real timer on the pool's clock, so simulated time reaches it and the waiters are woken — and the *earliest* pending deadline owns that timer | `resource.py` (`_ensure_cooldown_notifier`, `_cooldown_deadline`) | `tests/test_lease_safety.py::test_a_wait_that_only_a_cooldown_can_end_actually_ends` and `::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it` (kernel regression tests: both time out or wake late without the fix), `tests/test_benchmark_harness.py::test_a_wait_that_only_a_cooldown_can_end_advances_simulated_time` |
| An algorithm that does a fraction of the work cannot be crowned on *any* quality row — not the latency and throughput rows where its value is undefined without completions, and not the rates whose denominators it partly controls; the excluded ones are named in the table and the JSON, the baseline ignores algorithms the scenario declares unsuited, and a gated row with a single eligible contestant has no winner at all | `report.py` (`COMPLETION_FLOOR`, `excluded_by_completion`, `comparable`, `unrivaled`, `winners`), `metrics.py` (`Metric.requires_comparable_completion`) | `tests/test_benchmark_report.py::test_every_quality_row_requires_comparable_completion`, `::test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row` (parametrised over all thirteen gated rows), `::test_a_quality_metric_cannot_crown_an_algorithm_that_completed_almost_nothing`, `::test_the_completion_baseline_ignores_an_algorithm_the_scenario_cannot_exercise`, `::test_one_eligible_contestant_is_not_crowned` |
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
| `makespan_s` | 80.253 | 81.916 |
| `refusal_rate` | 0.000 | 0.000 |
| `error_rate` | 0.029 | 0.029 |
| `utilization` | 0.316 | 0.310 |
| `throughput_rps` | 1.495 | 1.465 |

Agreement is within 2% on makespan and 2% on utilisation, for 8.3s of wall clock for the pair.
`throughput_rps` is `jobs_done / makespan_s` on each clock, so the two agreeing on throughput is the
same statement as their agreeing on makespan — not a third, independent measurement. This confirms
time-keeping and throughput, not the refusal path: in this small world demand never saturates capacity
and both clocks report zero refusals.

**A wait the clock could otherwise miss.** A degraded resource recovers *lazily* — the cooldown expires
when somebody asks `state_at(now)` — and that expiry emits no event of its own. A pool whose every
resource is cooling down and which has no lease in flight therefore has nothing left to broadcast: the
waiters park on the broadcast, and the deadline they are actually waiting for passes unnoticed. Under a
simulated clock that is a hang (reproduced: `timers=0`, twelve waiters parked, simulated time frozen);
in real time it is a starvation bug that resolves only when unrelated activity happens to notify the
pool. `Pool` now arms one task per pool, only while somebody is waiting, that sleeps on the pool's clock
until the earliest *future* cooldown on a DEGRADED slot and then broadcasts. Three details matter and all
three were bugs first: a slot whose cooldown has already expired must not arm a zero-delay timer (that is
a livelock that advances no simulated time at all); DEAD or REVOKED slots must be ignored (they never
come back, and their stale `blocked_until` would keep re-arming); and the armed *deadline*, not just the
armed task, has to be tracked, because a cooldown that starts later can still end earlier — B breaking at
t=5 with a 5s cooldown while A's 30s notifier is already sleeping. Keeping the old timer wakes the waiters
at t=30 instead of t=10, twenty simulated seconds after the resource they were waiting for came back
(`tests/test_lease_safety.py::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it` pins both
numbers with `VirtualClock`).

**A world with nothing left to lease.** If every endpoint ends up DEAD or REVOKED — the framework retires
one for good after `dead_after` consecutive failures — no lease can ever be handed out again, so the
workers park and the clock has nothing to advance to. A real-time supervisor notices within 50ms and
raises `BenchmarkStalled` with the numbers, instead of burning the wall-clock budget: it is a property of
the scenario (an endpoint set that can die permanently), not of the algorithm or the machine.

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
| `jobs_done` | jobs | higher | Jobs completed successfully among the work admitted before the horizon cutoff. No new job is admitted after it, but one already in flight finishes — which is why a 600s run can have a makespan slightly above 600s |
| `jobs_failed` | jobs | neutral | Jobs that gave up: a step exhausted its retry budget. Diagnostic: an algorithm can shrink it by never starting the jobs it would have failed |
| `jobs_unstarted` | jobs | neutral | Jobs the workers never began because the horizon arrived first. Diagnostic: an algorithm can shrink it by starting everything and failing it |
| `makespan_s` | s | neutral | Simulated seconds until the last worker stopped (the horizon, plus a step still in flight); read it next to `jobs_done` |
| `throughput_rps` | jobs/s | higher * | Completed jobs per simulated second of the run's *own* makespan — the observed rate, not the budget's capacity |
| `requests` | requests | neutral | Requests sent, including refusals and retries |
| `attempts_per_completed_job` | attempts | lower * | Step attempts spent per completed job. The zero-retry baseline is `steps_per_job`, not 1.0, and attempts spent on jobs that later failed are in the numerator only |
| `attempt_inflation` | ratio | lower * | Step attempts per *attempted* step (1.0 = every attempted step succeeded on its first try): retry pressure without the volume |
| `failed_attempt_rate` | ratio | lower * | Failed step attempts as a fraction of all step attempts, including the ones the policy then abandoned |
| `retry_rate` | ratio | lower * | Retries the policy actually scheduled, per step attempt. `failed_attempt_rate` minus this is the share of failures given up on |
| `refusal_rate` | ratio | lower * | Requests refused by the provider (429) per request sent |
| `error_rate` | ratio | lower * | Requests that failed with an error per request sent |
| `successful_job_latency_p50_ms` | ms | lower * | Median time of a job that *succeeded*, retries included; the name is the qualifier |
| `successful_job_latency_p95_ms` | ms | lower * | 95th percentile job time among successful jobs: the tail a user notices |
| `successful_job_latency_p99_ms` | ms | lower * | 99th percentile job time among successful jobs |
| `acquire_wait_p50_ms` | ms | lower * | Median time a step spent waiting for a lease (over acquisitions that succeeded) |
| `acquire_wait_p99_ms` | ms | lower * | 99th percentile lease wait: what a saturated pool costs a caller that waits |
| `request_latency_p50_ms` | ms | neutral | Median served request latency (a property of the world) |
| `utilization` | ratio | higher * | Served work-seconds over offered capacity-seconds, integrated over the run's own makespan |
| `endpoint_spread` | ratio | neutral | Spread of admitted requests across endpoints; diagnostic, because the endpoints are deliberately unlike each other |
| `leases_active_at_end` | leases | lower | Leases still held when the run ended; must be zero. The one directional counter that is not a quality claim |
| `wall_s` | s | neutral | Real seconds the harness spent simulating: cost, never quality |

`*` marks a row that **requires comparable completion** (`Metric.requires_comparable_completion`). The
rule is not "the metric is undefined without completions" — that was the first version of it, and it was
too narrow. It is:

> A directional quality metric may only produce a comparative winner when the algorithms completed a
> comparable amount of useful work. `jobs_done` is the comparison of how much work got done, and the
> correctness/diagnostic counters make no quality claim, so those are the only ungated rows.

The reason is that the client controls the denominator of almost every rate here. `refusal_rate` is
`refusals / requests_sent`, and an algorithm that abandons contention before asking sends no request to
be refused; `error_rate` and `failed_attempt_rate` are the same shape; `retry_rate` is low for an
algorithm that never gets far enough to meet a retryable failure; and `utilization` integrates offered
capacity over a run whose *length* the client chose, so a run that stops after eight seconds never gives
the capacity it left unused a chance to be used. An algorithm that does 0.2% of the work should not win
any of those rows, and `test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row` checks
that on every one of them by handing the do-nothing algorithm the numerically best value.

Excluded algorithms are named, with their completion ratio, in the `best` column and in
`excluded_by_completion` in the JSON. Without the gate the first version of this table crowned a strategy
that finished 7.7 of 3000 jobs on two latency rows and on throughput, because failing fast is quick and
leaves few slow jobs behind. The gate is also what makes the honest throughput denominator
(`jobs_done / makespan_s`) safe, and it is applied to the rates for the reason above: an algorithm that
attempts three easy steps and abandons the queue reads 1.0 on `attempt_inflation`, 0.0 on `retry_rate`
and a flattering `refusal_rate` — all vacuous, all excluded.

`attempts_per_completed_job` and `attempt_inflation` answer different questions and the difference is the
point: the first is what finishing cost (it grows when attempts are wasted on jobs that later fail), the
second is how often an attempt had to be repeated (it does not grow with volume, so it compares retry
pressure across algorithms that did different amounts of work). A benchmark whose retry policy were part
of the comparison would want the second; one asking "what does a completed job cost me" wants the first.
`failed_attempt_rate` and `retry_rate` split the other way: the first counts every failed attempt, the
second only the failures that led to another attempt, so their difference is the share of failures the
policy judged hopeless.

**A vector, not a composite.** Folding throughput, p99 latency and "how much you annoyed the provider"
into one weighted number would hide the weights — which are the actual opinion — inside an arithmetic
result that looks objective. The report names the best algorithm per metric instead, so the reader sees
that the winner of one row is often the loser of another.

**Reading the table.** A `*` marks every algorithm that can claim the row: the best mean, plus anyone
whose gap is not distinguishable from noise. Two guards, whichever is wider:

* `TIE_TOLERANCE = 0.01` in relative terms — at three seeds, a 1% gap is not evidence of anything; and
* `SIGNIFICANCE_K = 2.0` standard errors of the **paired per-seed difference** `d_i = metric(challenger,
  seed_i) - metric(leader, seed_i)`, estimated as `stdev(d) / sqrt(n)` over the seeds both algorithms ran.

The second guard is what stops the report from crowning a 1.3% win on a metric that moves 4% between
seeds, and it is **paired** because every algorithm runs every seed: the seed-to-seed noise the two
algorithms share cancels, so what is tested is the difference, which is small exactly when common random
numbers worked. The independent-samples form `sd_leader^2/n + sd_challenger^2/n` would throw away the
strongest property of the design — and it did, until the review pointed it out. Each challenger is
compared against the leader rather than against the growing group, which keeps the rule one line long and
its meaning obvious. `SIGNIFICANCE_K = 2.0` is a **heuristic**, deliberately not described as 95%: with
three seeds the Student-t critical value is 4.3, and claiming significance at 2.0 would assert more than
the experiment supports.

Three ways a row ends up unmarked, besides the direction being `neutral` (`makespan_s`, `jobs_failed`,
`jobs_unstarted`, `requests`, `request_latency_p50_ms`, `wall_s`, `endpoint_spread`):

* **fewer than two eligible contestants.** A star is a comparative statement, and one algorithm is not a
  comparison — either because only one ran, or because the completion gate left only one. In the second
  case the report names the survivor (`not_compared` in the JSON, `only X was eligible` in the markdown),
  since silence there would read as "nobody did well";
* **every algorithm scores zero** on it (`tests/test_benchmark_report.py::test_a_metric_that_is_zero_for_everyone_crowns_nobody`);
* it was asked for by name although the scenario declares it unsuited, which leaves N/A in the column and
  the algorithm out of the ranking entirely.

Together with the gate, that gives the winner rule one sentence: *a quality row is decided among the
algorithms that did comparable work, and only when at least two of them could be compared at all.*

With a single seed there is no spread to test against, so the best mean wins among the algorithms that
ran — one *seed* is a weak comparison, which is what the missing `±` says. One *algorithm* is a different
matter, and is never crowned.

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
/tmp/bench-full.md`: the five algorithms this scenario is suited to, x 3 seeds = 15 runs of 3000 jobs x 3
steps x 2 calls on 12 workers, in **13 seconds of wall clock** on a laptop-class machine (the spread
between runs is the machine; the simulation itself is deterministic). `--algorithms` with all seven costs
about half again as much (20.2s measured) and marks the two unsuited columns N/A. A fast check of two
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

**Staying inside the budget.** The defaults are nowhere near the 10-minute limit: the whole 15-run sweep
measured 13.0s (20.2s with all seven algorithms), and per-run cost ranged from 0.09s (`immediate`) to
1.37s (`quota_aware`, which is the only one that varies much between seeds). The knobs are `--jobs` and
`--seeds` (they scale the work directly), then `--horizon`, then `--concurrency` (which changes the world,
not just the cost); `--wall-budget` is the guard, not the tuning knob.

**`BenchmarkTimeout`.** `Harness.run` starts the workers and a supervisor and waits for whichever finishes
first under a real-time `asyncio.wait(..., timeout=wall_budget)`, then cancels both and raises rather than
returning partial metrics — a run that cannot finish says so. If simulated time advanced, the message
reports the seconds and jobs done and suggests a cheaper run; if it never advanced it says the run is
"stalled, not slow" (workers parked, timers pending, something waiting for an event that cannot happen),
with the worker, parked and pending counts. The supervisor is what turns the other hopeless case — every
endpoint permanently DEAD or REVOKED, so no lease can ever be handed out again — into `BenchmarkStalled`
within 50ms of wall clock instead of a budget-shaped timeout. All three are covered by
`tests/test_benchmark_harness.py::test_the_wall_budget_is_real_time_and_reported_as_a_failure_not_a_result`,
`::test_a_stalled_simulation_says_so_instead_of_timing_out_silently` and
`::test_a_world_with_no_lease_left_is_reported_in_a_second_not_a_budget`.

## 8. What the current data says

The 3-seed means from the sweep above (seeds 20260917, 20260918, 20260919), after the review fixes:
`--markdown` writes the full table with `min`/`max`/`stdev` and per-endpoint admissions, and `--json`
carries the winners and the exclusions. In the `best` column, `—` means the row separates nobody and `*`
marks a row that requires comparable completion.

| Metric | `immediate` | `wait` | `backoff` | `sticky` | `quota_aware` | best |
|---|---|---|---|---|---|---|
| `jobs_done` | 7.00 | 1,348.67 | 1,361.67 | 1,404.00 | 843.67 | `sticky`, `backoff` |
| `jobs_failed` | 2,993.00 | 547.00 | 508.00 | 545.67 | 1,088.67 | — |
| `jobs_unstarted` | 0.00 | 1,104.33 | 1,130.33 | 1,050.33 | 1,067.67 | — |
| `makespan_s` | 8.25 | 606.45 | 606.57 | 606.05 | 610.83 | — |
| `throughput_rps` | 0.8910 | 2.2241 | 2.2449 | 2.3167 | 1.3809 | `sticky`, `backoff` * |
| `attempts_per_completed_job` | 459.65 | 5.69 | 5.57 | 5.55 | 10.80 | `sticky`, `backoff` * |
| `attempt_inflation` | 1.0077 | 1.6160 | 1.5947 | 1.5899 | 2.3396 | `sticky`, `backoff` * |
| `failed_attempt_rate` | 0.9930 | 0.4516 | 0.4393 | 0.4401 | 0.6856 | `backoff`, `sticky`, `wait` * |
| `retry_rate` | 0.0077 | 0.3803 | 0.3725 | 0.3700 | 0.5705 | `sticky`, `backoff` * |
| `refusal_rate` | 0.3387 | 0.2600 | 0.2484 | 0.2573 | 0.4760 | `backoff`, `sticky` * |
| `error_rate` | 0.0191 | 0.0244 | 0.0254 | 0.0223 | 0.0218 | `wait`, `sticky` * |
| `successful_job_latency_p95_ms` | 7,755.55 | 9,603.35 | 9,456.53 | 9,288.31 | 9,910.48 | `sticky`, `wait` * |
| `utilization` | 0.4928 | 0.6313 | 0.6384 | 0.6351 | 0.5026 | `wait`, `backoff`, `sticky` * |
| `endpoint_spread` | 0.6538 | 0.5707 | 0.5640 | 0.5759 | 0.9368 | — |

`*` marks the rows of this table that require comparable completion: `immediate` (0.5% of the best
completion) and `quota_aware` (60.1%) are excluded from every one of them, and named as excluded in both
the table and the JSON. There are thirteen such rows in total — the nine shown, plus
`successful_job_latency_p50_ms`, `successful_job_latency_p99_ms` and both `acquire_wait_*` percentiles,
which are left out of this table for width and are all in `--markdown`'s. The only directional rows *not*
marked are `jobs_done` (the comparison itself) and `leases_active_at_end` (a correctness counter).

**`immediate` is measured, and then excluded.** It completed 7.00 of 3,000 jobs (0.23%) and failed
2,993.00. `jobs_unstarted` (0.00) is no longer a win for it — the accounting row is a diagnostic now, and
"starts everything and finishes nothing" is not a quality. It is the *raw* leader on
`successful_job_latency_p95_ms` (7,755.55ms) and `successful_job_latency_p99_ms` (8,152.07ms), which is
exactly the fail-fast artefact the completion gate blocks: with 0.5% of the best completion it is excluded
from every quality row, and the table says so by name. Its `failed_attempt_rate` (0.9930) is the worst
in the table — almost every request it was allowed to make failed — while its `retry_rate` (0.0077) is the
lowest, because between the refusals and its own refusal to queue it barely schedules any. That inversion
is why the two metrics are separate and why the second one is gated: "scheduled few retries" reads as
virtue only in a run that attempted almost nothing. The same reasoning is what removed it from the
`refusal_rate` and `error_rate` rows in this revision: sending 71.7 requests instead of 12,246 means there
is almost nothing in those denominators to be counted against, so a low rate there is a statement about
how little it asked for.

**`backoff` and `sticky` lead; `wait` is close behind.** The paired test puts them together on `jobs_done`
(1,361.67 and 1,404.00 against `wait`'s 1,348.67, with per-seed spreads of 23-77 jobs), on throughput, on
`successful_job_latency_p50_ms` (3,522ms and 3,368ms against 3,574ms) and on `acquire_wait_p99_ms`.
`sticky` alone leads `successful_job_latency_p99_ms` (11,668.53ms against 12,501.36ms for `wait`). The gaps
are small and the honest summary is "these three are hard to separate in this world", not "sticky is best
by 1.3%": the paired differences are tighter than the absolute spreads, which is what the seeds are for,
but they are still single-digit percentages on a 10-minute simulated run.

**Retry pressure separates them more than latency does.** `attempt_inflation` is 1.59-1.62 for the three
front-runners against 2.34 for `quota_aware`: the leader's typical step takes about one and a half
attempts, `quota_aware`'s more than two and a third. `wait` is inside the `failed_attempt_rate` tie even
though its mean is the highest of the three (0.4516 against 0.4393 for `backoff`), and it retries the most
of the three (0.3803 against 0.3725): the paired differences are what put it in the tie, and the tie is the
honest statement about how little three seeds can see. `attempts_per_completed_job` is 5.55-5.69 for the
three, where a run of this scenario's three steps that never had to repeat one would read 3.00 — the
repetitions are a large share of what a completed job costs.

**`quota_aware` loses, and the reason is visible in the mechanism.** 843.67 jobs against `wait`'s
1,348.67 (-37%), `refusal_rate` 0.4760 against 0.2600, `endpoint_spread` 0.9368 (the most lopsided of the
five). It ranks by the *declared* quota (`QuotaAware.score` reads `resource.options["quota"]["tokens"]`),
which is not the refusals it will actually meet: the provider refuses on live in-flight capacity and on its
own bucket with tightening, neither of which the declared number reflects. In the last run of each
algorithm (seed 20260919) it sent 2,136 of 6,434 admitted requests to `fast-flaky` (the endpoint with the
highest error rate and storm rate) and only 306 to `metered`, while `wait` sent 4,618 and 1,300
respectively. Its `error_rate` (0.0218) is competitive with the others even though it provoked twice the
refusals: less traffic to the flaky endpoint is a different mix, not a better client.

**Several rows crown nobody, and that is the answer.** `makespan_s`, `requests`, `jobs_failed`,
`jobs_unstarted`, `request_latency_p50_ms`, `wall_s` and `endpoint_spread` are declared neutral — the
middle two because their direction can be won by doing less, and `wall_s`/`endpoint_spread` because they
are a cost and a description of a deliberately heterogeneous fleet (sending more traffic to the dependable
endpoint *raises* the spread). `acquire_wait_p50_ms` is 0.00 for every candidate that completed the work
(the client rarely waits for a *lease*: what stops a request in this scenario is the provider's refusal,
met while the lease is already held), and after the completion gate it has no winner at all.
`leases_active_at_end` is 0.0 everywhere, where the zero-everywhere rule crowns nobody. `error_rate` is a
two-way tie (`wait`, `sticky`) among the four algorithms that did comparable work: the independent failure
rates are simply not what separates these algorithms here.

**What is not in the table.** `failover` and `least_busy` are declared unsuited to this scenario and are
not run by default: one pool gives `failover` nothing to fail over to, and the pool's default selection is
already least-busy-first, so `least_busy` is the same code path as `wait` here. Running them explicitly
marks both columns N/A rather than printing numbers a reader would compare. That is a fact the benchmark
surfaced about the framework, and it belongs in the open — see the multi-vendor scenario in section 9.

## 9. Limitations and next steps

* **Single pool.** `Harness` builds one `Pool` and puts it in `TaskContext.pools` under one name. That is
  why `failover` and `least_busy` are declared unsuited (section 8) rather than ranked: a multi-vendor
  scenario needs several pools, not just several endpoints, and it is the obvious next scenario.
* **`sticky`'s affinity spans only the calls inside one step.** The `TaskContext` is created per attempt,
  so the affinity recorded by the first call is available to the second and then discarded — two calls out
  of six per job. A world where reuse pays (a per-endpoint cache) would show what it is worth.
* **Utilisation integrates a continuous cycle** (`capacity x weight x integral of factor`) while the
  provider enforces the integer `max(1, round(capacity x weight x factor))`: a smooth approximation of a
  step function, comparable between algorithms but not exact.
* **The completion gate is a blunt instrument.** 90% of the best completion is a threshold, not a
  statistical statement, and it is applied to the *mean* completion over seeds. A completion-aware latency
  metric (restricted mean time to completion, say) would need no threshold at all, and is the natural next
  step if latency rows turn out to matter.
* **Two seeds is not a sample.** The paired test is honest about the noise it can see, but with three seeds
  it can only detect large effects; `--seeds` is the knob, and the `±` column is the warning.
* **The real-clock mode is slow and CPU-inflated by design.** At `speedup=20`, real CPU time ages the
  simulation 20x; it exists to keep `VirtualClock` honest, not to produce the numbers.
* **One scenario ships.** `SCENARIOS` has a single entry, so today "the ranking" means "the ranking in this
  one world" — the same warning as section 1, from the other direction.

Candidate scenarios, in rough order of how much they would add: **multi-vendor failover** (two pools, one
degrading, which is the only way to measure `failover` and to separate `least_busy` from the default);
one dead endpoint (capacity pinned low, or storms at 1.0) to see which strategies notice and which keep
aiming at it; a thundering herd after an outage (every worker retries at once when an endpoint returns,
where `backoff`'s jitter should earn its keep); and a per-endpoint cache that pays out for reuse — the
world `sticky` was written for.
