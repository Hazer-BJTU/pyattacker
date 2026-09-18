# pyattacker Design Document

> Version: 0.1.0 (M0–M4 complete, M5 advanced control flow in progress — see "Implemented / Left for later" in Section 9)
> In one sentence: **an async task orchestration framework centered on the artifact, using the pipeline as the unit of completion, and the resource pool as the only shared surface.**
> It does not touch the network, does not do reduction, and does not do DAG scheduling — it is only responsible for "running tens of thousands of mutually independent pipelines to completion, reliably, recoverably, and observably".

---

## 1. Scope and Boundaries

| What the framework handles | What the framework does not handle |
|---|---|
| Task orchestration, concurrency control, resource leasing and reclaiming | HTTP requests, authentication, SSE parsing (the openai/anthropic protocols are your own business) |
| Artifact persistence (persist-on-produce) and task-level checkpoints | Semantic reduction (accuracy / F1 / pass@k — any cross-pipeline aggregation) |
| Failure classification, retry decisions, backoff policies, circuit-breaking | Dataset download and cleaning (it only accepts an iterable stream of seeds) |
| Structured logs, run manifests, live monitoring, export | Multi-turn agent state machines, DAG dependency scheduling, service-ification / gateway proxying |
| Resource pool publish/subscribe, health and quota accounting | Distributed scheduling (cross-process shards are a later increment) |

**The precise boundary of "no reduction"**: the framework does compute *operational statistics* (pipeline counts,
success rate, latency distribution, error classification, pool utilization), but it will never perform
*semantic aggregation* over artifacts. If you want accuracy, there are two paths, and neither requires the
framework to step in:

1. Export the artifacts (`export` / `report.export_jsonl`) and compute it yourself outside;
2. Write a **sink pipeline**: use one task to publish the results onto some resource/bus, where subscribers
   consume them — reduction becomes something you build yourself out of framework primitives.

---

## 2. Core Invariants

These six are the foundation of the design; no change may break them:

1. **Zero semantic coupling between pipelines.** The only shared surface is the resource pool (including bus
   signals). Concurrency is therefore trivial: one coroutine per pipeline, with no global dependency graph.
2. **Task = a unary `(Artifact) -> Artifact` function**, chained linearly, with no branching and no joining.
   When you need fan-out/loops/batching, the task does its own `asyncio.gather` internally. A task may
   instead **hand off** (§4.8, advanced, opt-in): the chain is still a chain of unary tasks with no joins,
   and the task is literally still `(Artifact) -> Artifact | Handoff`; what changes is the *traversal order*
   along declared forward edges, not the topology or the artifact contract.
3. **Artifacts are persisted as soon as they are produced**, so the **checkpoint granularity = task**, not pipeline.
4. **Failure is an exception**, and no state machine is introduced. Retry has two orthogonal boundaries: the
   task level (replay with the same input artifact) and the pipeline level (a full rerun).
5. **Resources can only be used through a lease, and they must be returned**; a task never holds a resource
   when it ends (see §4.2).
6. The persistence layer writes only **facts** (artifact / attempt / event), never semantic metrics.

---

## 3. Conceptual Model

```
        seed (one row of the dataset)
              │
              ▼
   ┌──── pipeline (unit of completion and recovery; semantically independent of each other) ────┐
   │                                                                                            │
   │   task A ──artifact──▶ task B ──artifact──▶ task C ──artifact──▶ task D ──▶ final artifact │
   │   (fetch)                  (async request)      (async evaluate) (compute metrics)         │
   │                             │                     │                                        │
   └─────────────────────────────┼─────────────────────┼────────────────────────────────────────┘
                                 │ acquire/lease       │
                                 ▼                     ▼
                        ┌───────── resource pool (the only shared surface) ──────────┐
                        │  resource#1  resource#2  resource#3 …                      │
                        │  state: ready/degraded/dead/revoked  health/quota stats    │
                        │  event stream: published/leased/degraded/recovered/revoked…│
                        └────────────────────────────────────────────────────────────┘
                                 ▲                     │
                                 │ publish/subscribe   │ algorithm decides "how to wait, how long, which pool to switch to"
```

| Concept | Definition | Code |
|---|---|---|
| **artifact** | The persisted state of a task. Content-addressed (blake2b), persisted as soon as it is produced; `seq=-1` is the pipeline's seed input | `artifact.py` |
| **task** | A unary `(artifact) -> artifact` function; may carry `resource` / `algorithm` / `retry` / `timeout_s` | `task.py` |
| **pipeline** | A linear chain of tasks, the unit of **completion** and **recovery**; `map(seeds)` expands it into mutually independent instances | `pipeline.py` |
| **resource** | A class of leasable external capability (one endpoint / one key / one local worker) | `resource.py` |
| **pool** | A set of resources + a default acquisition algorithm + a log of state events | `resource.py` |
| **lease** | One lease; `lease.client` is the usable object built by the factory; it must be returned | `resource.py` |
| **algorithm** | The policy for "how to get a resource out of the pool" (the pull side) | `algorithm.py` |
| **bus** | A lightweight cross-pipeline signal bus (the push side) | `resource.py` |

### 3.1 Pipeline Identity Is Content-Addressed

```
pipeline_key = blake2b(canonical_json({
    spec_digest,          # v2 task-chain fingerprint: config/parameters/children/policies + source digest
    seed_digest,          # digest of the seed contents (that row of the dataset)
    repeat,               # which sample of pass@k this is
}))
```

Three direct consequences:

* **Idempotent**: rerunning the same sample does not produce a second pipeline, and `resume` simply skips the
  ones that already succeeded.
* **Reproducible**: `spec_digest` includes each task's **source digest**, so changing task code is the same as
  swapping the pipeline, and old checkpoints are not incorrectly reused (`include_code=False` turns this off).
* **pass@k for free**: `template.map(seeds, repeats=3)` expands into three independent pipelines in one go,
  sharing the same seed digest.

The `control` block of a handoff-enabled pipeline (§4.8) is part of this fingerprint **only when it is
present**: a control-free pipeline digests exactly the task-fingerprint list it always did, so this feature
invalidates no stored digest, checkpoint or shard assignment (a `v3:` migration would).

The spec fingerprint is versioned as `v2:`. Explicit keys retain their supplied identity, but
stored spec/seed digests must match before skip or restore. Legacy stores remain readable;
default IDs change and explicit legacy keys conflict. Closures, globals, endpoint options and
pool defaults require declared task config/version. See [resume identity](reference.md#resume-identity)
for migration and external side-effect idempotency; checkpoints do not guarantee exactly-once calls.

---

## 4. Key Mechanisms

### 4.1 Task-Level Checkpoint and Recovery

After each task succeeds, three things happen in order:

1. `store.put_artifact(...)` — the artifact is persisted (content-addressed + deduplicated);
2. `store.record_task(...)` — the task's final state along with duration, error, and leases used;
3. the checkpoint cursor advances — `store.upsert_pipeline(record.n_tasks_done = seq + 1)` for an
   intermediate task, or the terminal `store.finish_pipeline(..., n_tasks_done = n_tasks)` for the last one.

For the last task the order is **finality, then terminal state**: `mark_final` runs before `finish_pipeline`,
and the cursor advance travels *with* `finish_pipeline` rather than being written on its own. Both orderings
matter, and each covers a torn write the other cannot repair: a crash after the terminal write but before
`mark_final` would leave a permanently `succeeded` pipeline (skipped forever) whose final artifact was never
marked, whereas a crash before the terminal write only costs a re-run of the final task — the documented
at-least-once boundary.

The recovery algorithm (`Runner._open_pipeline` / `Runner._drive`):

```
resume(spec):
    rec = store.get_pipeline(spec.pipeline_id)
    if rec exists and (rec.spec_digest != spec.spec_digest or rec.seed_digest != spec.seed_digest):
        → PipelineIdentityConflict; preserve existing pipeline and checkpoint
    if rec.state == succeeded and not retry_succeeded:  → skip (counted as skipped)
    if rec.state in (failed, interrupted) and spec declares control and the newest active handoff row is an END:
        → mark_final(entry artifact) + settle succeeded   # before the linear rule below, which cannot
          decide it: an early END left no artifact at n_tasks - 1; an unusable entry instead rewinds
          the cursor to 0 and emits checkpoint_missing (§4.8.4)
    if rec.state in (failed, interrupted) and rec.n_tasks_done >= n_tasks:
        if rec.n_tasks_done > n_tasks:    # not a state the Runner can create
            → record CorruptCheckpoint, emit pipeline.corrupt_cursor, leave the cursor as evidence
        elif artifact(n_tasks - 1) is available and decodes:
            → mark_final first when the artifact is not already final (idempotent either way, and nothing
              has been destroyed at this point), then settle the terminal row in one write — state=succeeded,
              cursor=n_tasks, run_id=current, failure fields cleared — and emit pipeline.terminal_repaired
              carrying the previous state/error/run. Nothing is rerun.
        else:
            → start = 0   # payload dropped (checkpoint_missing) or undecodable (checkpoint_unusable);
                          # the ordinary restart rule below applies
    start = 0
    if rec.state in (failed, interrupted) and rec.n_tasks_done > 0:
        h = newest handoff row for pid with handoff_id > rec.handoff_floor
        if h is not None and h.to_seq is not None and h.to_seq >= rec.n_tasks_done:
            entry = store.get_artifact(pid, h.entry_seq)      # the ledger names the entry state (§4.8.3)
            if entry.available: start = h.to_seq; prev_value = decode(entry)
            else:               start = 0   # the entry payload is gone → restart from the seed, loudly
        else:
            prev = store.get_artifact(pid, rec.n_tasks_done - 1)
            if prev.available:  start = rec.n_tasks_done; prev_value = decode(prev)
            else:               start = 0   # journal=summary stores no payload → the whole pipeline must be rerun (an event is left behind)
    if start == 0 and spec declares control:
        rec.handoff_floor = newest ledger ID (or 0); persist with the reset cursor before tasks
    for seq in range(start, n_tasks):  ...
```

The handoff branch is consulted first and is deliberately narrow: only the **newest active** ledger row (`handoff_id > handoff_floor`) can be
pending, because forward-only handoffs have strictly increasing targets, so a row whose `to_seq` is below the
cursor has been consumed by later forward progress and the ordinary artifact rule applies. A consumed row
therefore costs nothing, and a pending one resumes at the target without re-running the source task.

A failure in **either** step of that finalization — the finality mark or the terminal settle — is contained
by the recovery path: the row is left exactly as it was (original failure, cursor, owning run), and
`pipeline.terminal_repair_failed` records the attempt with its phase, so a later run still reports the
original cause instead of the repair's own error. Letting such an exception escape into the worker's
generic internal-error path would rewrite the row with that error and destroy the provenance the repair
exists to preserve, which is why the whole finalization is one controlled operation. The same reasoning is
why the terminal transition (state, cursor, owning run, failure fields) is a single store write when the
store offers `settle_pipeline`, and why `mark_final` is skipped when the artifact is already final — and
documented as idempotent for the crash case where that cannot be observed.

Because that row keeps its original owner, the failure cannot appear in the repairing run's run-scoped
statistics; it is counted in `RunReport.repair_failures`, which the CLI exit code and the summary both use,
so a failed repair can never be reported as a clean run. The two-write fallback for stores without
`settle_pipeline` classifies its steps separately: a failed terminal transition is a failed repair (above),
while a failed metadata cleanup is not — the row is already durably `succeeded`, and the stale failure text
is that store's documented degraded guarantee, reported as `pipeline.terminal_cleanup_failed`.

The key payoff: **if task C fails, only task C needs to be rerun, and task B's request is not re-sent**; and
because the seed artifact is persisted too, **recovery does not depend on the original dataset file**.

### 4.2 ★ Lease Safety Contract

Acquiring/releasing resources inside a task is **the most critical point of interaction between your code and
the framework**. The contract is as follows:

| Scenario | Guarantee | Implementation site |
|---|---|---|
| Finishing `async with ctx.acquire(...)` normally | returned synchronously on exit | `_LeaseGuard.__aexit__` |
| An exception raised inside the block | same as above — `__aexit__` still runs and does not swallow the exception | same as above |
| acquire→use→release inside a loop | returned immediately on every iteration, so concurrency is genuinely yielded (no accumulating occupancy) | how you write it + the same mechanism |
| `CancelledError` (external cancellation / timeout) | returned synchronously in `finally` | `Runner._execute_task` |
| Escape hatch `await ctx.acquire_lease()` with a forgotten return | **force-reclaimed** when the task ends, recording a `lease.leaked` event + counter | `TaskContext.reclaim_now` |
| Still holding a lease after the task returns | impossible: reclaim comes first, persistence second, and both are synchronous | same as above |

The root reason this holds: **reclaim is a pure synchronous function**.
`reclaim_now()` performs no `await`, and neither does `lease.release_now()`, so
`CancelledError` / an `asyncio.wait_for` timeout / any exception **cannot interrupt** it.
All pool state allocation and release executes synchronously; under single-threaded asyncio there is no
"checked-then-preempted" window, and therefore no need for locks.

Three accompanying design choices:

* **Resource health is reported explicitly by the task** (`lease.report(ok=False)`); the framework does not
  guess on your behalf whether "this failure is the resource's fault". The benefit is that a healthy endpoint
  is not circuit-broken because of one JSON parsing error.
* **Leaks are observable**: a `lease.leaked` event + `PoolStats.leaked_total` + `RunReport.leases_leaked`;
  when you need strict mode, use `strict_leases=True`, and a leak immediately fails that task (`LeaseLeakError`).
* **Suspected deadlocks raise a warning**: when a task, while already holding a resource from this pool,
  requests another resource from the same pool and the pool has no free capacity, waiting longer than
  `deadlock_warn_s` (5s by default) emits an `acquire.suspected_deadlock` event.

### 4.3 Resource Pool

**State machine** (`state_at` advances lazily, no background task):

```
READY ──consecutive failures ≥ degrade_after──▶ DEGRADED(blocked_until = now+cooldown_s) ──cooldown expires──▶ READY
   │
   └──consecutive failures ≥ dead_after──▶ DEAD        (report(ok=True) can pull DEGRADED/DEAD back to READY)
REVOKED ◀── explicit revoke / revoked from within a task
```

Note: cooldown expiry does **not reset** `consecutive_failures` (it is cleared only on success).
Otherwise, when "degrade threshold < dead threshold" and the pool holds a single resource, every cooldown
would wipe the counter and `DEAD` would never be reached.

A resource whose `factory` raised follows the same state machine (`degrade_after`/`dead_after`/cooldown),
and cooldown expiry *does* clear the resource's stored client error, so the factory is invoked again on
the next lease attempt — DEGRADED is a genuine second chance for a transient factory failure, not merely
a delay before DEAD.

**Publish/subscribe** uses two channels with different semantics and different implementations:

| Channel | API | Purpose |
|---|---|---|
| pull (acquisition) | `async with ctx.acquire(**selector) as lease:` | wait + lease; the selector supports `id`/`kind`/`tags`/`options` and dotted paths (`"a.b"`) |
| push (subscribe) | `ctx.subscribe(pool, ["resource.published"])`, `ctx.bus.subscribe(topic)` | receive resource publish/retire/degrade/recover signals |
| push (publish) | `ctx.publish_resource(pool, resource)`, `ctx.revoke_resource(...)` | a task that discovers a new endpoint at runtime injects it, and other pipelines can lease it immediately |

Every state change in a pool emits one `ResourceEvent`, which feeds **four** consumers at once: waiters
(wakeup), subscribers, the `events` table (structured log), and the monitoring snapshot. One fact, four views.

### 4.4 Algorithm and Retry Are Two Orthogonal Axes

This is the most easily confused part, so the implementation deliberately keeps them apart:

| | algorithm | retry |
|---|---|---|
| Timing | **Before** doing the work: how to wait for/select a resource | **After** the work fails: whether to try again |
| Input | Pool capacity/health/quota | Exception type + error classification |
| Built-in | `immediate` (fail if unavailable) / `wait` (default) / `backoff` (exponential backoff + full jitter) / `least_busy` (pick the least-busy) / `failover` (switch pools in order) / `sticky` (stay on the resource this pipeline already used) / `quota_aware` (prefer the most remaining quota) | `Retrying(max_attempts, on, base, factor, cap, jitter, max_total_s)` |
| Where it is declared | `@task(algorithm="backoff")` or `pool.algorithm` | `@task(retry={"max_attempts": 4, "on": ["RetryableError"]})` |

`failover` tries every listed pool immediately, in order; if none has capacity, its `fallback`
(`Wait()` by default) parks on `pools[0]` only — it does not loop the fallback across the whole
list. Read the list as "try these, then settle on the primary", not "wait on whichever frees up
first".

**Failure classification** (`errors.error_class_of`, a pure function, unit-testable):
408/504 in a `status`/`status_code` attribute → `timeout`, 425/429 → `rate_limit`, 5xx → `upstream`, 4xx → `fatal`;
`TimeoutError` → `timeout`; `ConnectionError` → `connection`; `ValueError/TypeError/...` → `invalid`; everything
else → `unknown`. You can take over directly by raising
`RetryableError(msg, error_class=..., retry_after=...)` / `FatalError`, or register your own classifier.

**No retry by default** (`max_attempts=1`), preserving the simple "a failure is a failure" semantics; turn it
on explicitly when you need it.
**Every retry decision is persisted** (`attempts.decision_json`): `{retry, reason, delay_s, error_class, attempt, max_attempts, retry_after}`,
with `reason ∈ {ok, retryable, attempts_exhausted, policy_declined, total_budget}`. `delay_s` is always present
(`0.0` when there is nothing to delay), so code reading the schema never has to guard against a missing key.
So "why did it retry 5 times / why did it give up" can be queried straight out of the database instead of being
guessed at by digging through logs.

### 4.5 Delayed Continuations and Batched Facts (M2)

Two throughput mechanisms that do not change the execution model, only how it is scheduled and persisted:

**Retry backoff parks the pipeline instead of sleeping in a worker.** `Runner._execute_task` runs exactly one
attempt and hands the retry decision back to `_drive`; when the policy wants another try, the pipeline state
goes into a `DelayQueue` (`scheduler.py`) and the worker immediately picks up other work. A single pump task
moves parked states back into the work queue once their timer expires. Consequences:

* `concurrency` finally means what it says: attempts in flight, not pipelines sitting out a 30-second backoff;
* the run is not finished when the workers go idle — `_wait_for_completion` waits for parked pipelines too,
  so a backoff is never mistaken for an interruption;
* on shutdown the pump is cancelled first, every worker gets a sentinel, and whatever is still parked is
  recorded as `pipeline.deferred_interrupted` — resumable, never silently dropped.

Timers go through the injectable `Clock`, and `interruptible_sleep` races `clock.sleep` against a wakeup
event, which gives both behaviours needed: with a real clock a newly pushed, sooner timer cuts a long wait
short, and with a fake clock the pump advances virtual time instantly (so tests stay deterministic).

**Append-only facts are batched.** `WriteBehindStore` buffers attempts and events and flushes them in batches
(size threshold, time interval, any read API, the run heartbeat, the end of the run). State writes —
`pipelines`, `tasks`, `artifacts` — always go straight through, because a checkpoint that is not durable yet
is not a checkpoint. A `SIGKILL` can therefore lose the last batch of history while every checkpoint stays
intact; `--no-write-behind` trades throughput for an immediate commit per attempt.

### 4.6 Monitoring: Cares About Traffic and Blocking, Not About Metrics

```python
snapshot = runner.stats()          # live in-process snapshot
# or cross-process: pyattacker watch runs.db   ← a read-only connection to the same SQLite file (WAL: one writer, many readers)
```

The panel shows: pipeline state distribution, p95/max latency, per-task counts, and for each pool its
`active/capacity`, `ready/degraded/dead`, `waiting`, throughput and leak counters, and recent errors.
It is also usable programmatically: `monitor.render_snapshot(stats)` / `monitor.watch(store)`.

### 4.7 Completion Is Counted, Worker Lifetime Is Supervised

A run finishes when `pipelines_done` reaches `pipelines_admitted`. That invariant only holds if every admitted
pipeline eventually reaches one of the worker's own terminal paths — and a `BaseException` that is not
`CancelledError` (a custom subclass raised by a store hook, for instance) reaches none of them: it escapes the
worker loop, the worker task ends, and the counter completion waits on can never advance. The run used to wait
forever, silently: no error, no exit code, no terminal row, no event.

Worker lifetime is therefore observed separately from pipeline accounting, through a done-callback on every
worker task. It also sees a death *after* the handler chain (queue or worker housekeeping), and turns it into
one run-level fault:

* **stop first, account never.** The run is hard-stopped (`stop("worker_crashed")`, no new admissions) and
  `pipelines_done` is deliberately *not* incremented to satisfy the counter invariant: the run is not
  completing normally, and the stop is what releases the waiter.
* **the pipeline is terminalized.** A `running` row that no live worker owns is the silent state this path
  exists to remove, so the in-flight pipeline is recorded `failed` — or `interrupted` when the run was already
  winding down — with the escaping exception on the row. A pipeline that was already terminal when the worker
  died keeps the state it earned: history is not rewritten. **The persisted row decides, not the runner's
  in-memory record** — a store is not required to write back into the `PipelineRecord` it was handed, and the
  item can be a state that came back through the retry queue while a terminal write it already made is
  durable, so trusting memory would rewrite a `succeeded` row (or an ordinary failure with its own
  provenance) as the crash. If the row does not exist yet it is created, the same way the internal-error path
  does it.
* **the work queue is released.** A full queue is only ever drained by workers, so admission and the shutdown
  sentinels hand items over through an abort-aware put that gives up as soon as a worker dies. Without it the
  *producer* would park on the dead worker's queue and the run would hang one step earlier.
* **the fault is explicit.** `runner.worker_crashed` names the pipeline, the exception and its traceback, and
  `run_async` raises `WorkerCrashed` — original exception as `__cause__` — after the run record has been
  closed as `interrupted`. If the store cannot record the crash either, the existing `StoreUnavailable` fatal
  path is taken instead of retrying a broken store.

`KeyboardInterrupt` and `SystemExit` are the boundary: asyncio re-raises them out of the task so the loop
stops, which means no done-callback can run at all. A narrow guard inside the worker records the row and the
event before they continue on their way, but the loop still tears down: the caller sees its own interrupt and
the run record is not closed. A `BaseException` raised *by a task* is not involved in any of this — the task's
own handling contains it, exactly like an ordinary exception. Cancellation is untouched too: a cancelled
worker still marks its pipeline `interrupted` and re-raises, and supervision does not turn a deliberate cancel
into a crash.

### 4.8 Advanced: Handoffs —— Declared Forward Jumps (Opt-In, Experimental)

The forward-only contract below remains the v1 path. Backward-enabled declarations use the visit-aware
contract in §4.8.8 and [the backward guide](backward.md); forward-only traversal still uses its existing
ledger/cursor recovery without new identity or budget requirements.

Everything above describes an ordinary pipeline: a chain walked one task at a time, each task returning the
artifact the next one consumes. This section describes the one feature that changes the *traversal* of that
chain: a step that can tell the rest of the chain no longer needs to run — the answer is good enough, the
sample is out of scope, a cached result exists — can **skip ahead** instead of running stations it does not
need, or hiding the branch inside one task, or raising (which would record the pipeline as failed, which is a
lie). The feature is deliberately fenced off from the rest:

* **opt-in** — without a `control` declaration a pipeline behaves exactly as before, down to a byte-identical
  `spec_digest` (see §3.1). Nothing in this section applies to a pipeline that does not ask for it;
* **advanced tier** — not because it is hard to call, but because it changes the execution model. It is
  documented under its own heading, released as a minor, and marked *experimental until 1.0*: the guarantees
  below are the stable part, while the spelling (`Handoff`, `control`) may still change;
* **forward-only in this model** — a handoff may only skip *ahead*: `control.edges` and `Handoff.to()` never
  acquire implicit backward semantics. The motivating case for the capability is the opposite direction (a
  validator sends a bad model output **back** to the generator for another sample). §4.8.7 specifies that
  model and §4.8.8 implements it as a *separately declared* opt-in tier, so the forward contract described
  in this section is unchanged rather than extended.

#### 4.8.1 The transport is a return value, not a control-flow exception

A task hands off by **returning** a framework-owned directive instead of a value:

```python
from pyattacker import Handoff

@task("judge")
async def judge(value: Verdict, ctx: TaskContext) -> Handoff | Report:
    if value.good_enough:
        return Handoff.end(value.as_report(), reason="already good enough")          # finish here
    if not value.needs_metrics:
        return Handoff.to("report", value.as_report(), reason="metrics not needed")  # skip ahead
    return await write_report(value)                                                 # ordinary success
```

`Handoff.to(target, value=UNSET, *, reason="")` names a destination — a task name, a task's seq, or `"end"`;
`Handoff.end(value=UNSET, *, reason="")` finishes the pipeline with that value as its final artifact.
`UNSET` (the sentinel the task layer uses for "not given") means "the target enters with the artifact *this*
task received"; an explicit value becomes a payload artifact of its own. `None` is a legitimate payload, so
only `UNSET` means "reuse".

Because the directive is a **return value**, it is not a failure and there is no new failure path:

* the retry policy is never consulted, so `retry.on=(Exception,)` and `retry_unknown=True` cannot turn a
  handoff into a retry, and no `decision.reason` value is added;
* no exception class is involved, so a task-side `except Exception:` or a `try/finally` cannot swallow or
  cancel the transfer — a control transfer buried in an exception is exactly what this design avoids;
* leases are released on the way out exactly as on success: `async with ctx.acquire(...)` has already
  returned them, and `finally: ctx.reclaim_now()` still runs. `strict_leases=True` plus a leaked lease
  stays a task failure, and the handoff is not honoured;
* cancellation and `timeout_s` are unchanged: a cancelled or timed-out attempt never reaches the return.

The honest cost, accepted deliberately: a handoff is initiated where the task can `return`, so a helper deep
in the call stack must hand the directive back up. That is the intended trade — an explicit, reviewable
handoff beats an invisible control transfer — and it keeps the task's signature honest
(`-> Handoff | Report`).

#### 4.8.2 A handoff is a durable state transition, not a control-flow trick

The runner intercepts the directive inside the attempt, before it is ever encoded as an artifact, and turns
it into one recorded hop. Five facts land together (see §4.8.4):

| Fact | Where | Meaning |
|---|---|---|
| the source task row | `tasks.state` | `handed_off` — it ended cleanly and produced no artifact |
| the attempt row | `attempts.outcome` | `handed_off`, with an empty `decision` (there was no decision) |
| the entry artifact | `artifacts` | the target's input: either the artifact the source received, or a new payload |
| the ledger row | `handoffs` | from/to, the entry reference, whether it was reused, and the author's reason |
| the cursor | `pipelines.n_tasks_done` | the target's position (`n_tasks` for `END`) |

The ledger is what makes "why does this pipeline's task list skip stations" answerable straight from the
store, and it is the **source of truth for recovery**. The `pipeline.handoff` event is the audit trail: a hard
kill can lose the event while the ledger stays authoritative, which is the one asymmetry to remember.

#### 4.8.3 The entry state always has a durable reference, at its own address

`Handoff.to(target)` with no value records `state.artifact.id` — the artifact this task received — so the
ledger's `entry_artifact_id` is never null. An explicit value is encoded by the project's `CodecRegistry` and
written as an ordinary artifact whose `seq` is allocated **inside the commit** as `n_tasks + k` (`k` = the
handoffs already recorded for that pipeline). The documented artifact order therefore becomes:

```
seed (-1)  →  chain (0 … n-1)  →  handoff payloads (>= n)
```

Two consequences matter. A payload can never overwrite a task slot (the trap a naive "write it into slot
`t-1`" design falls into), and it is recorded under the name of the task that handed it off. And
`Artifact.seq` is no longer globally synonymous with a task position: it is a task position for the chain,
`-1` for the seed, and a payload address at or above `n_tasks` once control flow is enabled. Readers that
only ever see the chain keep their existing semantics.

#### 4.8.4 The commit, and the resume rule it enables

A handoff is a checkpoint, so its write ordering is part of the contract. The store gains one **optional**
capability, `commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)` — the
`resources()`/`settle_pipeline` precedent — which performs the whole transition **atomically**: finalize the
source task, insert the handed-off attempt, persist the payload (allocating its address), append the ledger
row, and move the cursor while keeping the pipeline `running`. For `END` it also marks the entry artifact
final and settles the pipeline `succeeded` in that same commit.

Atomicity is a *requirement* of the capability rather than a bonus, because there is deliberately no second
recovery protocol: a store that cannot commit as one unit simply does not expose the method, and opening a
pipeline that declares `control` on such a store fails fast with a `ConfigError` naming the capability. A
silently non-durable handoff is the one outcome this rule exists to prevent. (`WriteBehindStore` forwards the
capability, flushing buffered attempts and events first, and writes the current handed-off attempt through the
commit rather than through its buffer.)

Recovery then has exactly one new branch, and it is consulted **before** the linear terminal rules:

```
newest ledger row h for the pipeline with handoff_id > rec.handoff_floor
if rec.state in (failed, interrupted) and h is not None:
    if h.to_seq is None:                 # END that did not finish writing
        entry = artifact(h.entry_seq)
        if usable: mark_final(entry); settle succeeded       # never re-run the source task
        else:      cursor = 0; checkpoint_missing            # the ordinary restart-from-zero rule
    elif h.to_seq >= rec.n_tasks_done:   # the commit landed, the target did not finish
        entry = artifact(h.entry_seq)
        if usable: start at h.to_seq with decode(entry)
        else:      cursor = 0; checkpoint_missing
    else:                                # consumed by later forward progress
        the ordinary artifact(cursor - 1) rule
```

On every restart from the seed, persist `rec.handoff_floor = newest ledger ID` with the reset cursor
through `reset_pipeline(record)` before executing tasks. The reset atomically removes current task rows
and chain artifacts (seq 0..n-1), clears prior final flags, and writes the cursor/watermark together.
Seed and high-band payload artifacts, attempts, events and handoffs remain as history. This excludes handoffs from an abandoned execution without deleting history;
the watermark stays unchanged on a resumed target. SQLite migrates the column with a zero default and
read-only readers tolerate its absence. A failed SQLite handoff transaction rolls back before any later
event or cleanup write can commit. Completion selects one final artifact, clearing older final flags.

Only the newest active row can be pending, because forward-only handoffs have strictly increasing targets — a
`to_seq` below the cursor has necessarily been passed. The `END` branch has to be consulted first because an
early `END` left no artifact at `n_tasks - 1` at all, so the linear terminal repair cannot even decide the
case.

#### 4.8.5 The cursor is a position, and traversal still terminates structurally

`n_tasks_done` keeps its field and its "seq to run next" meaning. For a control-enabled pipeline it is a
**position**, not a count of executed tasks: skipped slots never ran, so their task rows do not exist and
`n_tasks_done == n_tasks_total` no longer implies "every task ran". No new progress field is introduced, and
no surface may render `n_tasks_done / n_tasks_total` as a completion percentage for such a pipeline
(`report`/`watch`/`/pipelines` expose the handoff count next to it for exactly that reason).

Termination is still structural, not budgetary: a handoff may only target a strictly later position, so the
cursor strictly increases and the chain is walked at most once. That is what keeps this version free of loop
budgets — and why the v2 backward case cannot be added without one (§4.8.7).

#### 4.8.6 Declaring edges, and what validation does (and does not) check

```python
pipeline("qa", retrieve | ask | judge | report,
         control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})
```

Edges are **declared, not derived**: a returned `Handoff` along an undeclared edge is a fatal error (never a
silent jump, never a retry), and every declared edge is resolved and range-checked when the pipeline is built:

* the source and the destination must exist, and a repeated task name must be disambiguated by its numeric
  seq (the error says which seqs matched);
* a destination must be strictly later than its source (forward-only);
* `"end"` is a valid destination, except from the last task, where it has no effect and is refused;
* an `edges` block has no unknown keys — `mode` is deliberately absent, because there is exactly one forward
  mode. Backward operations are separate keys (`rewind`, `retry_all`, `max_handoffs`), validated by the same
  entry point and specified in §4.8.8.

Validation is **structural only**. The handoff payload is an arbitrary argument, not the source's normal
return type, so `source.returns -> target.accepts` is deliberately not checked: it would reject valid
handoffs (a judge handing `value.more_queries()` to an `ask` step) and accept invalid ones.

The one annotation rule this adds: a `Handoff` member in an annotation is an escape. On the `returns` side
`-> Handoff | Report` chains as `Report`, and `-> Handoff` alone chains with anything, because such a task
produces no artifact on that path. The escape applies only to produced/return annotations; the accepted
side remains unchanged because the runner never passes a directive as an artifact. The declarative layer accepts the same block as `pipeline.control`, with
field paths (`pipeline.control.edges['judge'][0]`), validated through the same entry point, so `validate` and
`run` refuse the same configs with exit 2.

The resolved control plan defensively copies task names and target sequences, then freezes the mapping.
Runtime topology cannot be changed after validation or diverge from the computed `spec_digest`. An
unencodable explicit handoff payload raises `FatalError`, so an aggressive retry policy cannot replay
a task that returned that invalid directive.

#### 4.8.7 What is deliberately not in this version

The motivating case for the whole capability is the **opposite** direction, and writing it down is part of
committing to it:

```
ask(temperature=0.2) ─▶ validate ─▶ (invalid) ⇢ revoke to ask(temperature=0.7) ─▶ validate ─▶ …
```

Model evaluation is sampling, not function application: a structured-output step regularly produces a
structurally invalid result, and the validator can tell. The pipeline should then *go back* to the generator
and try again, possibly with different parameters carried in the artifact. A task-internal loop cannot express
that well — it collapses generation, validation and the intermediate steps into one record, one lease history,
one retry policy and one timeout, which makes "how many regenerations did this sample need" invisible exactly
where it is the measurement. A revoke handoff would make each regeneration a real visit of the generation step
with its own attempt records, and keep the whole thing crash-resumable, so a run over thousands of samples
still resumes mid-sample instead of restarting it. It is the reason several record decisions here are shaped
the way they are, and it is the reason the model below is specified now rather than discovered later:

| Not included | Why, and what would be needed |
|---|---|
| Backward / revoke handoffs | The motivating case for the capability: a validator sends work **back** to the generator. It needs a visit model — `(seq, visit)` identity on tasks and attempts, a durable per-seq counter advanced in the same commit as the entry record, visit-aware RNG (`ctx.seed` is currently `digest(pipeline_id\|seq\|attempt)`, so a revisit would see identical randomness), a loop budget (termination is no longer structural), and progress reporting that does not present a visit count as completion. The record decisions here — the ledger, the entry-artifact address, the atomic commit, the position cursor — were chosen so that model can be added without changing them, and §4.8.8 adds it as a separately declared opt-in tier. |
| Declared DAGs, joins, fan-in | The chain stays a chain. A handoff is a scheduling statement about one pipeline, not a graph edge. |
| Cross-pipeline handoffs | Pipelines stay semantically independent; the only shared surface is still the resource pool. |
| Runtime-invented targets | Edges are declared, so a typed or misspelled target fails loudly instead of silently reshaping the pipeline. |
| Handoffs from `fanout` branches | A group is one step in the record (`fanout` runs its children inside one task), so a control transfer cannot be attributed to one of N concurrent branches. A returned directive fails the group with a clear `FatalError` instead of travelling inside a collected payload. |
| Payload type checking | See §4.8.6: it has no sound definition without a declared payload contract of its own. |

#### 4.8.8 Advanced v2: rewind, retry-all and optional payload history

Implemented as a separate opt-in capability: `control.rewind` declares strictly earlier destinations,
`control.retry_all` declares sources, and `control.max_handoffs` bounds traversal. Rewind requires explicit
author-selected entry state; retry-all decodes the original seed captured at binding. History-bearing
payloads are optional and never drive scheduling. The [backward guide](backward.md) describes the interface.

A persisted traversal record owns per-seq counters, effective slot-to-visit mappings and pending entry,
including its exact input occurrence. Every fresh entry allocates a visit; resume reuses the pending visit.
Success commits output/task/attempt/effective mapping/cursor together. A control transition also commits
suffix invalidation, budget consumption and target entry allocation. Cursor comparisons and historic
completion rows cannot decide whether a backward transition is consumed. Resume at seq 0 is a real entry.

Visits preserve task/artifact identities for visit 0, qualify subsequent IDs, and participate in RNG.
Rewind/retry-all retain historic records. Budget count survives resume and missing-payload fallback.
Completion and finality use exact occurrence identity. Backward requeue releases the worker.
Snapshot history is application-managed JSON state with a versioned codec, not the execution ledger.

Two rules keep the model honest at its edges. **Ownership:** recovering a backward pipeline continues an
exact durable visit, so a `running` row is never taken over implicitly — `resume=True` is the operator's
claim that the previous owner is gone, and without it the row is skipped and left untouched (the forward path
keeps its older restart-from-zero rule, which is why this is not in the generic open path).
**Discarding state is explicit:** `fresh_restart=True` is the only switch that drops a checkpoint or a
traversal. It restarts from the bound seed, resets the control budget and invalidates the previous ledger
watermark, while append-only history and (for a backward pipeline) the visit counters and occurrences
survive — so historical occurrences stay addressable, and a store
whose traversal was lost has its counters rebuilt from its own rows. `retry_succeeded` stays an eligibility
switch ("also admit succeeded pipelines") and no longer implies discarding anything.

The store records how far its on-disk model has come (`store/visits.py`): `base` until the first revisit is
committed, then `visits-v1`, written in the same transaction as the occurrence that justifies it. An unknown
level is refused on open instead of interpreted, and a SQLite store at `visits-v1` arms a writer guard that
refuses writes from any connection which has not declared visit-lineage awareness — the marker exists so a
lineage-unaware writer fails loudly rather than mutating the wrong occurrence. The migration stays additive
for forward-only work, which never leaves `base`.

---

## 5. Data Model (SQLite, WAL + `synchronous=NORMAL`)

Three layers of facts with non-overlapping responsibilities:

| Table | What it is | Semantics |
|---|---|---|
| `runs` | One run | heartbeat, state, config snapshot, version, host, seed |
| `pipelines` | **Current state** | one SQL query selects the pending work on resume; `n_tasks_done` is the checkpoint cursor |
| `tasks` | The current state of each task | overwritten in place; records `attempts_used`, duration, error, leases used |
| `attempts` | **Append-only history** | one row per attempt, numbered continuously across resumes, never overwritten |
| `artifacts` | The state carrier | content-addressed + `payload BLOB`; `is_final` marks the final product. `seq` is a task position for the chain, `-1` for the seed, and a **handoff payload address at or above `n_tasks`** on a control-enabled pipeline (§4.8.3) |
| `events` | **Structured log** | `scope ∈ run/pipeline/task/pool/resource`; the complete story of one pipeline = a query by `pipeline_id` |
| `handoffs` | **Control-flow history** (advanced, opt-in) | append-only: one row per task-initiated jump, naming the from/to positions and the durable entry artifact; the authoritative record recovery resumes from (§4.8) |
| `resources` | The pool's final state | resource specs (keys masked) + health statistics |

**The complete record of one pipeline**:

```sql
SELECT * FROM pipelines WHERE pipeline_id = ?;                  -- state and checkpoint
SELECT * FROM tasks     WHERE pipeline_id = ? ORDER BY seq;     -- final state of each task
SELECT * FROM attempts  WHERE pipeline_id = ? ORDER BY seq, attempt_no;  -- full history and retry decisions
SELECT * FROM artifacts WHERE pipeline_id = ? ORDER BY seq;     -- intermediate states, payloads, the final product
SELECT * FROM handoffs  WHERE pipeline_id = ? ORDER BY handoff_id;-- control-flow history: where it jumped, and why
SELECT * FROM events    WHERE pipeline_id = ? ORDER BY event_id;-- structured log
```

The `journal` mode: `full` stores the artifact payload (**the precondition for recovery**); `summary` keeps only
digests and metadata (saves space, at the cost that intermediate artifacts cannot be reused, so resume can only
rerun the whole pipeline, and it leaves a `pipeline.checkpoint_missing` event behind).

**Write policy**: all store methods are synchronous — this keeps critical writes such as "record a running row
before the attempt starts" from being interrupted by cancellation. Batched writes / write-behind merging is a
later optimization and does not affect the interface.

**Read policy**: the list queries above may materialize their result — a report wants a list. A whole-kind
read that must stay bounded (an export of a large store) goes through the optional paged-iteration
extension instead: keyset batches of `ITER_BATCH_SIZE` rows, ordered by a key that ends in a unique column
so a batch boundary can neither drop nor duplicate a row; a nested `pipelines` row materializes one
pipeline, which is the documented memory unit. Where the key is monotonic (`event_id`, `attempt_id`) the
iterator is bounded by the high-water mark taken when it starts, so an export of a live store cannot chase
a moving tail; `pipelines`/`tasks`/`artifacts` have no monotonic key and are documented as a best-effort
traversal of the live store. `Store` is unchanged, so a third-party store that implements only the list
API stays complete, just not bounded in memory — see
[the store reference](reference.md#paged-reads-and-third-party-stores).

---

## 6. Two Ways to Use It

### 6.1 SDK (the primary form)

```python
from pyattacker import Pool, Resource, RetryableError, Retrying, Runner, pipeline, task

class Client:                      # your own network code, the framework does not touch it
    def __init__(self, options): self.options = options
    async def chat(self, prompt):  ...

@task("fetch")
def fetch(row: dict) -> dict:
    return {"q": row["q"]}

@task("ask", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, on=(RetryableError, TimeoutError), base=0.5, cap=30.0),
      timeout_s=60)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:   # returned on exit; returned on exception too
        text = await lease.client.chat(row["q"])
        lease.report(ok=True, usage={"tokens": 128})
        return {"a": text}

@task("judge", resource="judges")
async def judge(row, ctx): ...

@task("metrics")                    # synchronous pure-computation task
def metrics(row) -> dict: ...

pool = Pool("apis", [Resource.create("llm", capacity=4,
                                     options={"base_url": ..., "api_key": "${OPENAI_KEY}", "model": "gpt-4o"},
                                     factory=lambda res: Client(res.options))
                     for _ in range(8)],
            algorithm="backoff")

template = pipeline("qa_eval", fetch | ask | judge | metrics, tags={"bench": "mmlu"})

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(dataset_rows, repeats=3))   # a generator, streaming, memory O(concurrency)
    print(report.summary())
    report.export_jsonl("runs/qa.jsonl")
```

Recovery: `runner.run(template.map(dataset_rows), resume=True)` — already successful pipelines are skipped, and
failed ones continue from **the first task that produced no artifact**.

A complete worked example — data preparation, a two-turn model call, three judges, a user-written
reduction, in both a grouped and a per-judge pipeline shape, with the checkpoint cost measured
rather than asserted — lives in `examples/llm_eval/`.

### 6.2 Declarative (simple tasks)

The declarative form only describes **composition and resources**; the logic still lives in Python
(`use: myproj.tasks:ask_model`). The document may be YAML, TOML or JSON; the suffix picks the parser, and
only the YAML one is an optional dependency (§8.13).

```yaml
run:   { store: runs/demo.db, concurrency: 8, journal: full, label: demo }
pools:
  apis:
    kind: llm
    algorithm: backoff
    resources:
      - { id: api-1, capacity: 4, options: { model: gpt-4o, api_key: "${OPENAI_KEY}" } }
pipeline:
  name: qa_eval
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: pyattacker.tasks:simulate_llm
      resource: apis
      algorithm: backoff
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }   # factory arguments
      # a bare `on` is a YAML 1.1 boolean key: the retry key must be quoted
      retry: { max_attempts: 3, base: 0.2, "on": [RetryableError, TimeoutError] }
source: { kind: jsonl, path: data.jsonl, limit: 100, key_field: id, repeats: 1 }
```

```bash
pyattacker validate -c pyattacker.yaml     # validate and print the effective config
pyattacker run      -c pyattacker.yaml --progress
pyattacker resume   -c pyattacker.yaml     # = run --resume
pyattacker watch    runs/demo.db           # open another process to watch it live
pyattacker report   runs/demo.db --errors 20
pyattacker export   runs/demo.db out.jsonl
pyattacker demo                            # verify the installation with zero configuration
```

Exit codes: `0` all succeeded / `1` some failed / `2` config error / `130` interrupted.

### 6.3 Sharding, Merging and Export (M3)

The kernel is single-loop and SQLite takes one writer, so horizontal scale means **processes with their own
stores**, joined afterwards. Shard assignment is a pure function of the content-addressed `pipeline_key`, so the
same dataset always splits the same way and `--resume` lands every pipeline back in the shard that owns it.

```bash
# convenience: spawn N children locally, wait, then print the merged view
pyattacker run -c qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db
# -> runs/qa.shard0of4.db … runs/qa.shard3of4.db

# or drive the shards yourself (a cluster, a scheduler, N terminals)
pyattacker run -c qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
pyattacker run -c qa_eval.yaml --shard 1/4 --store runs/qa.shard1.db

# resume is per shard: rerun the exact same command with --resume (or --shards N)
pyattacker resume -c qa_eval.yaml --shards 4 --store runs/qa.db

# one coherent answer out of N files
pyattacker report runs/qa.shard*of4.db
pyattacker export runs/qa.shard*of4.db runs/all.jsonl
pyattacker export runs/qa.shard*of4.db runs/tasks.csv --rows tasks --format csv
```

`--shard I/N` takes an explicit `--store` verbatim; without it, `run.store` gets a `.shardIofN` suffix so two
children can never fight over one file. Each child also gets `PYATACKER_SHARD=I/N` in its environment, so a task
can record its own provenance.

**Row shapes** (`--rows`) for whatever consumes the results: `pipelines` (nested, the default), `tasks`,
`attempts` (with the retry `decision`), `events`, `artifacts`. **Formats** (`--format`): `jsonl`, `json`, `csv`.
CSV takes its header from the first `header_rows` rows and folds anything introduced later into an `extra`
column, which keeps memory flat without silently dropping fields. Every kind is exported in full — `events`
used to stop at the newest 100 000 rows — and read in bounded batches; see
[the export reference](reference.md#export).

### 6.4 Plugins, Backends and the Monitoring Endpoint (M4)

**Plugins** are plain `importlib.metadata` entry points — no registry file, no import-time magic:

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"          # a TaskSpec, or a factory returning one
[project.entry-points."pyattacker.algorithms"]
my_algo  = "my_pkg.algo:MyAlgorithm"
[project.entry-points."pyattacker.codecs"]
my_codec = "my_pkg.codec:MyCodec"
[project.entry-points."pyattacker.stores"]
s3       = "my_pkg.s3:open_store"           # keyed by URI scheme
```

Then `use: my_judge`, `algorithm: my_algo` and `store: "s3://bucket/runs.db"` simply work.
Three rules keep this from becoming a liability:

* **built-ins resolve first**, so a plugin can never shadow `echo` or `wait`;
* **a broken plugin is recorded, not raised** — `pyattacker plugins` lists what loaded and what
  failed, and the healthy plugins keep working. Nothing a plugin does may escape into
  `Runner.__init__`, which is where codec plugins are installed;
* **a codec plugin that claims a payload wins over an earlier registration**, so a specialised
  codec is not dead code behind the JSON catch-all. An explicit `for_types=` mapping still beats
  the scan, because naming the type is a stronger statement than "I can encode this".

A complete example lives in `examples/plugin_package/`.

**Artifact backends** decide where payload bytes live. `inline` (default) keeps them in the store;
`file:///data/blobs` spills anything above a threshold into content-addressed files; `null` keeps the
digest and drops the bytes. The store keeps `digest`/`size`/`codec` in its own row plus an opaque
`blob_ref`, and hydrates `payload` back on read — so an artifact is still one object to everything
upstream, and content addressing means identical payloads collapse into one file and a shared
backend can safely serve many runs.

```toml
[run]
artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

**Monitoring endpoint**: `pyattacker serve runs/qa.db` starts a zero-dependency, read-only HTTP view
(fresh read-only connection per request, so it can run beside a live run). `/stats`, `/events`,
`/pipelines`, `/resources`, `/errors` are JSON; `/` is a small auto-refreshing dashboard. It binds
to loopback and has no authentication — it exposes your payloads, so treat it as a debug view.

**Fan-out**: `fanout(a, b, ...)` runs several tasks on the *same* input concurrently **inside one
task**, which is how a genuinely branching step is expressed without turning pipelines into a DAG.
The tradeoff is explicit: retry granularity becomes the group, and the group adopts the most
forgiving child policy. Because the Runner only ever sees the group spec, `resource`, `algorithm`
and `timeout_s` are lifted onto it from the children — but only when every child agrees on them,
since one group cannot mean two different policies. First-class `Parallel`/`Gather` nodes remain
deliberately out of scope — the unary task model is what keeps the kernel (and its recovery story)
small.

---

## 7. Module Structure

```
src/pyattacker/
  artifact.py     Artifact / Codec / content addressing      (no internal dependencies)
  errors.py       exception hierarchy + pure error-classification functions
  task.py         @task / TaskSpec / TaskContext / lease tracking and force-reclaim
  pipeline.py     Chain composition / artifact type chaining checks / pipeline_key / map(repeats)
  handoff.py      the Handoff directive + the resolved, validated control-edge plan (M5, advanced)
  resource.py     Resource / Pool / Lease / Bus / state machine and event stream
  algorithm.py    acquisition algorithms (immediate/wait/backoff/least_busy/failover)
  runner.py       pipeline coroutine scheduling / task-level checkpoint / retry / graceful shutdown / stats
  scheduler.py    delayed continuations: park a pipeline instead of sleeping in a worker
  store/          base (records + protocol) / memory / sqlite / writebehind (batched append-only facts)
  monitor.py      snapshot rendering + watch
  shard.py        deterministic shard assignment + per-shard store paths (M3)
  merge.py        join N shard stores: de-duplicate by pipeline_id, recompute statistics (M3)
  export.py       row shapes (pipelines/tasks/attempts/events/artifacts) and formats (jsonl/json/csv) (M3)
  declarative.py  YAML/TOML → pools + pipeline + source (including ${ENV} expansion)
  plugins.py      entry-point discovery for tasks/algorithms/codecs/stores (M4)
  backends.py     where artifact payloads live: inline / content-addressed files / null (M4)
  server.py       read-only HTTP view of a store: /stats, /events, /pipelines (M4)
  cli.py          run/resume/report/watch/export/serve/plugins/validate/demo/bench (+ --shard / --shards)
  __main__.py     `python -m pyattacker`, used by the shard children
  benchmark/      clock (simulated time) / scenario (assumptions as data) / provider (the black-box
                  world) / harness (closed-loop client) / metrics / report — drives pools, not runs
  tasks/          built-in utility tasks: mock.* / fanout / shell.run / file.write_jsonl / jsonl_source
```

The dependency direction is strictly one-way: `errors → artifact → task → handoff → pipeline → resource/algorithm → store → runner → cli`,
and `resource` does not depend on `algorithm` in reverse (algorithms are injected through `pool.acquire`).
`shard`/`merge`/`export` sit beside the kernel: they read stores and pipeline streams, and nothing in the kernel
depends on them. `benchmark/` sits beside it too, one level lower: it drives `Pool` and the algorithms directly
and never enters a run, which is what keeps its simulated clock exact (see §8.14).

---

## 8. Known Tradeoffs and Limitations (Deliberate, Written Down Up Front)

1. **Stop conditions are best-effort**: `stop_after_failures` is evaluated at admission time and at every
   pipeline completion, so pipelines already admitted (roughly `2 × concurrency`) still run. With a large
   retry backoff, parking means many pipelines can be *in progress* at once, so more of them may be admitted
   before the budget is spent. The budget stops the bleeding, it does not rewind time.
2. **Write-behind can lose the tail**: attempts and events are batched (size- or interval-triggered, plus a
   flush on every heartbeat and at the end of a run), so a `SIGKILL` can lose the last unsent batch. Every
   checkpoint and artifact is written synchronously, which is why a resumed run still only re-executes what
   was genuinely unfinished. Use `--no-write-behind` when you would rather pay a commit per attempt.
3. **`journal=summary` sacrifices recovery granularity**: with no payloads stored there are no intermediate
   artifacts to reuse, so the whole pipeline must be rerun. If you want recovery, you must use `full`.
4. **Resource health relies on explicit reporting**: only `lease.report(ok=False)` counts as a failure; the
   framework does not guess.
5. **`quota_aware` is a preference, not a hard limit**: when every candidate is out of quota the best of them
   is still handed out, because refusing to work is worse than overspending. For a hard stop, raise from the
   task once its own budget is gone.
6. **v1 only supports asyncio tasks**: wrap blocking code yourself inside the task with
   `await asyncio.to_thread(...)` (one line of code, in exchange for a pool that needs no locks and carries no
   thread-safety burden).
7. **One writer per database**: scaling out means more processes, each with its own store (`--shard i/N`),
   never more writers on one file — SQLite allows a single writer (WAL keeps one writer plus many readers, which
   is what lets `watch`/`report` run beside a live run).
8. **Shard balance is statistical, not exact**: assignment is `hash(pipeline_key) % N`, because the alternative
   (round-robin over the stream) would move every pipeline whenever the dataset changes and would break resume.
   With 4 shards and 20 pipelines, expect the split to look like 6/14 sometimes; it evens out at scale.
9. **A merged report is a union, not a sum**: the same pipeline can exist in two shards after a shard-count
   change, so `merge_reports` de-duplicates by `pipeline_id` (best state wins, latest finish breaks ties) and
   *recomputes* statistics from the merged rows. It reports how many rows it folded so the number is never
   hidden.
10. **A pipeline is a linear chain**: the kernel is implemented in terms of "nodes + dependency edges", so
    adding `Parallel/Gather` is just syntactic sugar, but it is deliberately not exposed. Use `fanout(...)`
    inside a task instead: branches stay one step in the record, at the cost of group-level retry granularity.
    The one qualification is the opt-in handoff of §4.8: it changes the *traversal* of the chain
    along declared edges, never its topology — there is still no join and no second entry point.
11. **A `null` backend costs you recovery granularity**: dropping payloads means intermediate artifacts
    cannot be reused, so `resume` reruns the whole pipeline — the same tradeoff as `journal=summary`.
    A missing blob file behaves the same way, on purpose: `available` goes false and the work is redone.
12. **The HTTP endpoint is unauthenticated and loopback-only by default.** It is a debug view over your
    run's payloads, not a service. Put it behind your own proxy if you need one, and think before binding
    it to a public interface.
13. **YAML is an extra, not a dependency**: `dependencies` is empty, and the declarative layer reads JSON and
    TOML with the standard library, so `pip install pyattacker` pulls in nothing. A `.yaml`/`.yml` config needs
    `pip install "pyattacker[yaml]"`, and without it the loader raises a `ConfigError` naming the extra and the
    file at the moment that file is read — the check is per file, from its suffix, never at import time. The
    cost is one extra install step for the readers who want YAML; the benefit is that everyone else, including
    every SDK-only user, pays nothing for a parser they never call.
14. **The benchmark is a simulation, and its numbers are a property of its assumptions.** It compares
    acquisition algorithms in a written-down world (capacity cycle, token bucket, latency tail, storms,
    three endpoints) so that "which algorithm is better here" becomes a question with an answer and a
    seed. It is not a measurement of anyone's provider, and changing an assumption can change the
    ranking; two of the seven built-ins cannot be exercised in this world at all — `failover` has nothing
    to fail over to with one pool, and `least_busy` is the pool's default selection under another name —
    which the scenario declares (and marks N/A) rather than hides behind a plausible-looking number. See
    `docs/benchmark.md`.
15. **Subprocess trees are only guaranteed on POSIX.** `shell_run` terminates and reaps the process it
    started on every exit path — normal exit, `timeout_s`, cancellation, any other exception — and on POSIX
    it signals the child's whole process group, so a shell pipeline or an argv program's descendants go with
    it. Windows has no process-group signalling in the standard library (`os.killpg` does not exist and
    `asyncio` cannot send `CTRL_BREAK_EVENT` to a child's group), so only the direct child is terminated
    there and a descendant may outlive the task. The offline tests verify the descendant guarantee on POSIX
    and skip those two cases elsewhere with that reason stated.
16. **Handoffs are opt-in and fenced off** (§4.8). A pipeline that declares `control` trades
    one guarantee for another: its cursor becomes a *position* rather than a progress count (skipped slots
    have no task rows, and `n_tasks_done / n_tasks_total` is not a completion percentage for such a
    pipeline), and its recovery depends on the `handoffs` ledger being readable — which is why a store
    without the atomic `commit_handoff` capability is refused up front instead of being downgraded to a
    non-durable jump. The feature is marked experimental until 1.0: the guarantees above are the stable
    part, the spelling may still change. The forward-only path has no revisits; joins and cross-pipeline transfers are not
    included; a pipeline that is mostly handoffs is a sign the problem wants a graph engine, which this is
    not.

---

## 9. Testing Strategy and Current Status

**Zero network, zero external services** — everything runs on the built-in mock tasks. The core assertions:

* `tests/test_lease_safety.py` — every clause of the §4.2 contract: exception/cancellation/timeout/escape-hatch
  leak/loop acquire, plus "`active == 0` in the pool after the run ends".
* `tests/test_runner.py` — ★ recovery: everything fails in round one → round two with `resume=True` →
  **the earlier tasks' call counts do not increase**, only the failed tasks rerun, and round three is entirely
  `skipped`; the behavior when `journal=summary` makes a checkpoint unusable; retry decision fields; the
  concurrency upper bound; shutdown conditions; export.
* `tests/test_pipeline.py` — construction-time artifact type chaining validation, source digest affecting
  identity, `map(repeats)` and explicit keys.
* `tests/test_scheduler.py` — the M2 infrastructure: `DelayQueue` ordering, a sooner timer cutting a longer
  wait short, cancellation never losing a parked item, `WriteBehindStore` buffering/flush triggers and the
  rule that state writes are never buffered.
* `tests/test_algorithms.py` — resource acquisition strategies (`sticky` affinity, `quota_aware` ranking,
  `least_busy`, `failover`) and pool wait-time metrics.
* `tests/test_m2.py` — the M2 claims end to end through `Runner`: with `concurrency=1` another pipeline
  completes while one is parked for a retry (asserted from the event order, not from timing), a stopped run
  records parked pipelines as resumable, and a finished run leaves no buffered facts behind.
* `tests/test_shard.py` — partitioning is total (every pipeline in exactly one shard), deterministic and
  content-addressed/stable across processes, `parse_shard`/`shard_index`/`shard_env` validation, and
  `--shards N` spawning real child processes with a retrying resource-pool pipeline merged cleanly across them.
* `tests/test_export.py` — every row shape and every format, including the CSV `extra` column for keys that
  appear after `header_rows`, `merge_reports` folding duplicate `pipeline_id`s by best-state/latest-finish, and
  the CLI's sharded paths: `run --shard i/N` with explicit stores, `--shards N`, JSON summaries, merged
  `report`/`export` over shard stores, and the `ConfigError` when a shard has nowhere to write.
* `tests/test_plugins.py` — discovery and resolution with an injected entry-point provider (no installation
  needed): built-ins win, a raising plugin is recorded instead of propagated, `use:`/`algorithm:`/store-scheme
  resolution all reach plugins.
* `tests/test_server.py` — the HTTP endpoint over real loopback requests: JSON shapes, limits, 404s, and that
  a run started while the server is up shows up in `/stats`.
* `tests/test_backends.py` — spilling above a threshold, hydration on read, content-addressed de-duplication,
  `journal=summary` keeping nothing anywhere, and **resume through a spilled checkpoint**.
* `tests/test_packaging.py` — the version in `pyproject.toml` matches the running package, no accidental
  dependencies, every module imports, and every promised name is exported.
* `tests/test_optional_yaml.py` — the optional extra as a user meets it (§8.13): the SDK runs with PyYAML
  absent, JSON/TOML configs load, a `.yaml`/`.yml` file raises a `ConfigError` naming the file and the extra,
  a *broken* PyYAML surfaces its own error rather than that hint, and the CLI turns the missing-extra case into
  exit code 2. `sys.modules["yaml"] = None` simulates the absence, so all of this runs on every ordinary test
  run rather than only in the job that has no PyYAML.
* `tests/test_benchmark_*.py` — the simulated benchmark, tested at the level of its claims rather than its
  output: hand-computed timelines for the virtual clock (32 concurrent 1 s sleeps cost one second, a
  runnable worker is never skipped past), the provider's own dynamics (a full endpoint refuses with a 429,
  a refusal costs future allowance, storms are a function of time not of traffic), what makes the
  comparison fair (two algorithms are served identical draws for the same `(endpoint, ordinal)`; nothing
  hands the algorithm a reference to the environment; the same seed reproduces the numbers exactly), and
  that the whole thing opens no network connections. The virtual clock is additionally checked against a
  compressed real-time clock, which is correct by construction and too slow to use.
* **The YAML-dependent tests are marked `requires_yaml`** instead of being guarded with `importorskip()`, so
  the development suite *fails* when the extra goes missing rather than silently skipping a third of itself.
  CI therefore runs the suite twice: with the extra (everything), and against a bare `pip install` of the
  package with `-m "not requires_yaml"`. Which side is the guarantee is the point — the positive case is
  tested normally, the negative case explicitly. The release workflow closes the loop on the built wheel, in
  both directions: without the extra (PyYAML absent, JSON works, YAML asks for the extra) and with it (PyYAML
  resolves from the published metadata and a YAML config validates).
* `tests/test_artifact.py` / `test_store.py` / `test_declarative.py` / `test_cli.py` — codecs,
  store semantics and consistency between the two stores, config parsing, CLI end to end.
* `tests/test_errors.py` — `error_class_of`/`is_retryable_class`/`retry_after_of` as pure functions: every
  `_STATUS_RULES` bracket, the `FatalError`/`TimeoutError`/`ConnectionError` branches, explicit `.error_class`
  precedence, and extracting a server-suggested `retry_after` from both a direct attribute and response headers.
* `tests/test_handoff.py` — the advanced feature end to end: a control-free run is provably untouched
  (a literal `spec_digest` and no new rows), a forward handoff skips stations and a returned directive along
  an undeclared edge is fatal without being retried, `END` finalizes its entry artifact, a **real SIGKILLed
  process** resumes at the target without re-running the source task, a consumed ledger row falls back to the
  ordinary artifact rule, a lost entry payload restarts from zero, the commit is atomic and the handed-off
  attempt never passes through the write-behind buffer, a store without the capability is refused (while a
  control-free pipeline on that same store keeps working), and validation, `fanout` rejection, leases,
  timeouts and every observability surface are pinned.
* `tests/test_monitor.py` — the `watch` terminal renderer: progress-bar clamping/rounding, run-scoped vs.
  store-wide snapshots, pool bars, and the leaked-leases/stopping indicators.
* `tests/test_tasks.py` — `shell_run`: string vs. argv form; string commands reject `{value}` interpolation
  outright, while argv commands pass the substituted value as one literal argument via
  `create_subprocess_exec`, without implicit shell interpretation. (If the argv form's own command explicitly
  invokes a shell or another interpreter, e.g. `["sh", "-c", ...]`, that interpreter's input-safety semantics
  are the caller's responsibility — the guarantee here is "no *implicit* shell", not "safe with any program".)
  The other built-in mock tasks are exercised incidentally wherever other test files need a stand-in task,
  rather than in a dedicated file.
* `tests/test_subprocess_lifecycle.py` — `shell_run`'s process lifetime as the OS sees it. Cancellation,
  a `timeout_s` expiry, a `Runner` stop and a cancellation aimed at the cleanup itself all leave the child
  killed *and reaped* (`os.kill(pid, 0)` must fail, which catches both "still running" and "killed but not
  waited for"), while a child that exited on its own is left alone and keeps its result. One test cancels
  the caller *while the process is still being created*: it deliberately widens that window by holding the
  wrapped `loop.subprocess_exec` until the test releases it, because the child already exists there while no
  frame holds a handle — a race no test can be trusted to hit on purpose (the stock 3.11/3.12 implementation
  only happens to close the transport when its own internal wait is cancelled, which is not a guarantee
  `shell_run` may lean on). The descendant tests cancel a string command whose shell is waiting on a real
  pipeline, and an argv program that spawned a child of its own, then assert every PID is gone — they fail
  if cleanup only kills the direct child. Children signal readiness by writing their own PID (no fixed sleep
  anywhere), and the `tracked_pids` fixture kills every PID a test saw even when the test fails, so a red
  test cannot leave a live process behind. The two descendant tests are skipped off POSIX with the platform
  reason (see §8.15); the kill and liveness probes follow the platform's own semantics, and only the POSIX
  branches are exercised by CI.
* `tests/test_tutorial.py` — every code block in `docs/tutorial.md` marked as a complete program
  (`# tutorial/<name>.py`) is extracted and actually run, so the tutorial cannot silently rot out of sync
  with the real API.

All time-related logic (backoff, circuit-break cooldown) goes through an injectable `Clock`, and tests use
`tests/helpers.py::FakeClock` to turn time into a controllable variable, making them both deterministic and fast.
The suite finishes in a few seconds — run `uv run pytest` to see the current count (this document intentionally
does not hardcode it, since a specific number goes stale every time a test is added or removed) — so there is
no excuse for not running it.
`ruff check` is clean under the configuration in `pyproject.toml`, where every ignored rule carries a
reason — a lint exception should be an argument, not an accident.

**Implemented (M0 – M4, i.e. everything planned for 0.1.0)**: the full kernel for the five concepts, in-memory/SQLite stores, task-level
checkpoint and recovery, retry and error classification, the resource pool state machine and publish/subscribe,
7 acquisition algorithms, the declarative layer, the CLI, the built-in mock utility tasks, delayed continuations
(backoff without holding a worker), write-behind batching of attempts/events, per-resource targeted wakeups,
pool wait-time metrics, deterministic sharding with merged reports, five row shapes in three export formats,
entry-point plugins, external artifact backends, a fan-out helper, and a read-only HTTP monitoring endpoint.

**Left for later (post-0.1.0)**: a distributed scheduler, Parquet export, blob garbage collection
(`FileBackend` is content-addressed, so orphan blobs are safe but never removed), and first-class
`Parallel`/`Gather` nodes — the last one only if the unary task model proves too limiting in practice.

---

## 10. Milestones

| | Goal | Completion criterion |
|---|---|---|
| **M0 Skeleton** ✅ | five-concept kernel + in-memory store + linear execution + built-in mocks + CLI | `pyattacker demo` runs end to end |
| **M1 Persistence and recovery** ✅ | all SQLite tables, task-level checkpoint, resume, structured events, SIGINT | resume after SIGKILL without re-sending earlier tasks |
| **M2 Smarter resources and retries** ✅ | write-behind, backoff that yields the worker, per-resource targeted wakeups, quota-aware algorithms, finer `acquire` metrics | backoff is observable when the pool is saturated, and can be replayed from `events` |
| **M3 Scale and ergonomics** ✅ | `--shard i/N` + `--shards N`, merged reports, shard utilities, multi-shape/multi-format export | multiple processes run the same dataset |
| **M4 Ecosystem** ✅ | entry-point plugins, external artifact backends, fan-out helper, HTTP monitoring endpoint, 0.1.0 packaging | third parties can publish task packages |
| **M5 Advanced control flow (opt-in)** 🚧 | declared forward handoffs (`Handoff`, `control=`, the `handoffs` ledger, atomic `commit_handoff`, ledger-first recovery) | a handoff is a durable checkpoint: a killed process resumes at the target with the entry state, and a control-free pipeline is provably unchanged |

---

## 11. Non-Goals (written into the README to prevent scope creep)

* No HTTP client / provider SDK adapter layer (you write the tasks yourself; this is deliberate design, not a missing feature)
* No DAG / multi-turn agent orchestration (pipelines stay linear; fan-out is implemented inside a task, and
  the one exception is the opt-in handoff of §4.8 — no declared graph, no joins, no
  cross-pipeline orchestration)
* No semantic reduction (accuracy / pass@k / any cross-pipeline aggregation)
* No service-ification / gateway / proxy
* No dataset store (it only accepts an iterable stream of seeds + one `jsonl_source` utility)
* No distributed scheduling (`--shard` is the multi-process ceiling)
