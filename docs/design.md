# pyattacker Design Document

> Version: 0.0.1 (the skeleton is in place — see "Implemented / Not implemented" in Section 9)
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
   When you need fan-out/loops/batching, the task does its own `asyncio.gather` internally.
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
    spec_digest,          # task-chain fingerprint: each task's name/target/args + source digest
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

---

## 4. Key Mechanisms

### 4.1 Task-Level Checkpoint and Recovery

After each task succeeds, three things happen in order:

1. `store.put_artifact(...)` — the artifact is persisted (content-addressed + deduplicated);
2. `store.record_task(...)` — the task's final state along with duration, error, and leases used;
3. `store.upsert_pipeline(record.n_tasks_done = seq + 1)` — **advances the checkpoint cursor**.

The recovery algorithm (`Runner._execute_pipeline`):

```
resume(spec):
    rec = store.get_pipeline(spec.pipeline_id)
    if rec.state == succeeded and not retry_succeeded:  → skip (counted as skipped)
    start = 0
    if rec.state in (failed, interrupted) and rec.n_tasks_done > 0:
        prev = store.get_artifact(pid, rec.n_tasks_done - 1)
        if prev.available:  start = rec.n_tasks_done; prev_value = decode(prev)
        else:               start = 0   # journal=summary stores no payload → the whole pipeline must be rerun (an event is left behind)
    for seq in range(start, n_tasks):  ...
```

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

**Failure classification** (`errors.error_class_of`, a pure function, unit-testable):
408/504 in a `status`/`status_code` attribute → `timeout`, 425/429 → `rate_limit`, 5xx → `upstream`, 4xx → `fatal`;
`TimeoutError` → `timeout`; `ConnectionError` → `connection`; `ValueError/TypeError/...` → `invalid`; everything
else → `unknown`. You can take over directly by raising
`RetryableError(msg, error_class=..., retry_after=...)` / `FatalError`, or register your own classifier.

**No retry by default** (`max_attempts=1`), preserving the simple "a failure is a failure" semantics; turn it
on explicitly when you need it.
**Every retry decision is persisted** (`attempts.decision_json`): `{retry, reason, delay_s, error_class, attempt, max_attempts, retry_after}`,
with `reason ∈ {ok, retryable, attempts_exhausted, policy_declined, total_budget}`.
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

---

## 5. Data Model (SQLite, WAL + `synchronous=NORMAL`)

Three layers of facts with non-overlapping responsibilities:

| Table | What it is | Semantics |
|---|---|---|
| `runs` | One run | heartbeat, state, config snapshot, version, host, seed |
| `pipelines` | **Current state** | one SQL query selects the pending work on resume; `n_tasks_done` is the checkpoint cursor |
| `tasks` | The current state of each task | overwritten in place; records `attempts_used`, duration, error, leases used |
| `attempts` | **Append-only history** | one row per attempt, numbered continuously across resumes, never overwritten |
| `artifacts` | The state carrier | content-addressed + `payload BLOB`; `is_final` marks the final product |
| `events` | **Structured log** | `scope ∈ run/pipeline/task/pool/resource`; the complete story of one pipeline = a query by `pipeline_id` |
| `resources` | The pool's final state | resource specs (keys masked) + health statistics |

**The complete record of one pipeline**:

```sql
SELECT * FROM pipelines WHERE pipeline_id = ?;                  -- state and checkpoint
SELECT * FROM tasks     WHERE pipeline_id = ? ORDER BY seq;     -- final state of each task
SELECT * FROM attempts  WHERE pipeline_id = ? ORDER BY seq, attempt_no;  -- full history and retry decisions
SELECT * FROM artifacts WHERE pipeline_id = ? ORDER BY seq;     -- intermediate states and the final product
SELECT * FROM events    WHERE pipeline_id = ? ORDER BY event_id;-- structured log
```

The `journal` mode: `full` stores the artifact payload (**the precondition for recovery**); `summary` keeps only
digests and metadata (saves space, at the cost that intermediate artifacts cannot be reused, so resume can only
rerun the whole pipeline, and it leaves a `pipeline.checkpoint_missing` event behind).

**Write policy**: all store methods are synchronous — this keeps critical writes such as "record a running row
before the attempt starts" from being interrupted by cancellation. Batched writes / write-behind merging is a
later optimization and does not affect the interface.

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

### 6.2 Declarative (simple tasks)

The declarative form only describes **composition and resources**; the logic still lives in Python
(`use: myproj.tasks:ask_model`):

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
      retry: { max_attempts: 3, base: 0.2, on: [RetryableError, TimeoutError] }
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
column, which keeps memory flat without silently dropping fields.

---

## 7. Module Structure

```
src/pyattacker/
  artifact.py     Artifact / Codec / content addressing      (no internal dependencies)
  errors.py       exception hierarchy + pure error-classification functions
  task.py         @task / TaskSpec / TaskContext / lease tracking and force-reclaim
  pipeline.py     Chain composition / artifact type chaining checks / pipeline_key / map(repeats)
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
  cli.py          run/resume/report/watch/export/validate/demo (+ --shard / --shards)
  __main__.py     `python -m pyattacker`, used by the shard children
  tasks/          built-in utility tasks: mock.* / shell.run / file.write_jsonl / jsonl_source
```

The dependency direction is strictly one-way: `errors → artifact → task → pipeline → resource/algorithm → store → runner → cli`,
and `resource` does not depend on `algorithm` in reverse (algorithms are injected through `pool.acquire`).
`shard`/`merge`/`export` sit beside the kernel: they read stores and pipeline streams, and nothing in the kernel
depends on them.

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
    adding `Parallel/Gather` is just syntactic sugar, but v1 does not expose it.

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
* `tests/test_shard.py` — partitioning is total (every pipeline in exactly one shard), deterministic, and the
  CLI paths agree: `--shard i/N` with explicit stores, `--shards N` spawning children, JSON summaries,
  merged `report`/`export`, and the `ConfigError` when a shard has nowhere to write.
* `tests/test_export.py` — every row shape and every format, including the CSV `extra` column for keys that
  appear after `header_rows`, and `merge_reports` folding duplicate `pipeline_id`s by best-state/latest-finish.
* `tests/test_artifact.py` / `test_store.py` / `test_declarative.py` / `test_cli.py` — codecs,
  store semantics and consistency between the two stores, config parsing, CLI end to end.

All time-related logic (backoff, circuit-break cooldown) goes through an injectable `Clock`, and tests use
`tests/helpers.py::FakeClock` to turn time into a controllable variable, making them both deterministic and fast.
The suite is 140 tests and finishes in about a second, so there is no excuse for not running it.

**Implemented (M0 + M1 + M2 + M3)**: the full kernel for the five concepts, in-memory/SQLite stores, task-level
checkpoint and recovery, retry and error classification, the resource pool state machine and publish/subscribe,
7 acquisition algorithms, the declarative layer, the CLI, the built-in mock utility tasks, delayed continuations
(backoff without holding a worker), write-behind batching of attempts/events, per-resource targeted wakeups,
pool wait-time metrics, deterministic sharding with merged reports, and five row shapes in three export formats.

**Not implemented (M4)**: an HTTP monitoring endpoint, entry-point plugins, `Parallel`/`Gather` syntactic
sugar, attachments (external backends for large artifacts).

---

## 10. Milestones

| | Goal | Completion criterion |
|---|---|---|
| **M0 Skeleton** ✅ | five-concept kernel + in-memory store + linear execution + built-in mocks + CLI | `pyattacker demo` runs end to end |
| **M1 Persistence and recovery** ✅ | all SQLite tables, task-level checkpoint, resume, structured events, SIGINT | resume after SIGKILL without re-sending earlier tasks |
| **M2 Smarter resources and retries** ✅ | write-behind, backoff that yields the worker, per-resource targeted wakeups, quota-aware algorithms, finer `acquire` metrics | backoff is observable when the pool is saturated, and can be replayed from `events` |
| **M3 Scale and ergonomics** ✅ | `--shard i/N` + `--shards N`, merged reports, shard utilities, multi-shape/multi-format export | multiple processes run the same dataset |
| **M4 Ecosystem** | entry-point plugins, HTTP monitoring endpoint, optional extras, PyPI 0.1.0 | third parties can publish task packages |

---

## 11. Non-Goals (written into the README to prevent scope creep)

* No HTTP client / provider SDK adapter layer (you write the tasks yourself; this is deliberate design, not a missing feature)
* No DAG / multi-turn agent orchestration (pipelines stay linear; fan-out is implemented inside a task)
* No semantic reduction (accuracy / pass@k / any cross-pipeline aggregation)
* No service-ification / gateway / proxy
* No dataset store (it only accepts an iterable stream of seeds + one `jsonl_source` utility)
* No distributed scheduling (`--shard` is the multi-process ceiling)
