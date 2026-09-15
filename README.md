# pyattacker

> An async task orchestration framework centered on the **artifact**, using the **pipeline** as the unit of completion, and the **resource pool** as the only shared surface.
> Few dependencies (the core is only the standard library + PyYAML), resumable, observable, built for "tens of thousands of mutually independent tasks".

When you benchmark LLMs / agents, some things always get reimplemented: managing multiple endpoint configs,
batched concurrent requests, the "which rows already ran" resume logic, retry and backoff, and recording the
details of every request for later review. pyattacker extracts these concerns into a kernel that
**does not touch the network** — you write the tasks, and it handles the rest.

```bash
uv sync
uv run pyattacker demo            # run once with zero configuration to verify the installation
uv run pytest                     # the whole suite, zero network, about a second
```

## Core Model

| Concept | Meaning | In one line |
|---|---|---|
| **artifact** | the persisted state of a task | content-addressed, **persisted as soon as it is produced** → checkpoint granularity = task |
| **task** | the smallest unit of scheduling | a unary `(artifact) -> artifact` function, sync or async |
| **pipeline** | the unit of completion | `fetch \| ask \| judge \| metrics` chained linearly, semantically independent of each other |
| **resource** | a leasable external capability | one endpoint / one key; once pooled, it can be published and subscribed to concurrency-safely |
| **algorithm** | the policy for acquiring resources | `wait`, `backoff`, `least_busy`, `failover`, `sticky`, `quota_aware`, `immediate` — orthogonal to "retry on failure" |

## 30-Second Quickstart (SDK)

```python
from pyattacker import Pool, Resource, Retrying, Runner, pipeline, task

@task("ask", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, base=0.5, cap=30.0), timeout_s=60)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:     # returned on exit; returned on exception too
        text = await lease.client.chat(row["q"])         # lease.client is built by resource.factory
        lease.report(ok=True, usage={"tokens": 128})     # report health/quota back to the pool
        return {"q": row["q"], "a": text}

pool = Pool("apis",
            [Resource.create("llm", capacity=4, options={"model": "gpt-4o"},
                             factory=lambda res: MyClient(res.options)) for _ in range(8)],
            algorithm="backoff")

template = pipeline("qa", ask)

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(dataset_rows))   # a generator, streaming, memory O(concurrency)
    print(report.summary())
    report.export_jsonl("runs/qa.jsonl")
```

Want pass@k? `template.map(rows, repeats=3)` — the same seed expands into three independent pipelines.

## Lease Safety: The Framework's Hardest Guarantee

Resource leasing is the most critical point of interaction between your code and the framework, so:

```python
async with ctx.acquire(resource) as lease:   # ← the only recommended form
    for chunk in chunks:                      # repeatedly acquiring/releasing inside a loop is fine too
        ...
```

* An exception, a cancellation, a `timeout_s` timeout — **always returned**;
* Forgot to release through the escape hatch `await ctx.acquire_lease()`? It is **force-reclaimed** when the
  task ends, and a `lease.leaked` event is recorded;
* Reclaim is a **pure synchronous function**, and `CancelledError` cannot interrupt it — this is why "a task
  never holds a resource after it ends" holds;
* When you need something stricter, `strict_leases=True`: a leak immediately fails that task (`LeaseLeakError`).

## Scheduling That Keeps Its Promises

* **A retry backoff does not hold a worker.** When the policy wants another attempt, the pipeline is parked in
  a delay queue and the worker immediately picks up other work. `concurrency` therefore means *attempts in
  flight*, not *pipelines sitting out a 30-second backoff*.
* **A parked pipeline is never mistaken for a dead one.** The end of a run waits for parked pipelines too; a
  stop records them as `interrupted` with their checkpoints intact, so `resume` picks them up.
* **Pool wakeups are targeted.** Releasing one resource wakes only the waiters whose selector can use it,
  instead of every blocked pipeline.
* **History is batched, checkpoints are not.** Attempts and events are written in batches (size, interval,
  heartbeat, and end-of-run), while artifacts and checkpoints are always committed immediately. `SIGKILL` can
  cost you the last batch of history, never a checkpoint. `--no-write-behind` opts out.
* **Waiting is measured.** Pool stats report `waits_total`, `wait_ms_avg`, `p50`, `p95` and `max`, and a wait
  beyond `slow_wait_ms` emits an `acquire.slow_wait` event you can alert on.

## Running It Across Processes (Sharding)

SQLite takes one writer and the kernel is a single event loop, so scale means **processes with their own
stores**, joined afterwards. A pipeline's shard comes from its content-addressed key, so the same dataset always
splits the same way and `--resume` puts every pipeline back where it was:

```bash
# convenience: N children, then a merged report
uv run pyattacker run -c examples/qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db
# -> runs/qa.shard0of4.db … runs/qa.shard3of4.db

# or drive each shard yourself (cluster, scheduler, four terminals)
uv run pyattacker run -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
uv run pyattacker resume -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db

# one coherent answer out of N files (de-duplicated, statistics recomputed)
uv run pyattacker report runs/qa.shard*of4.db
uv run pyattacker export runs/qa.shard*of4.db runs/all.jsonl
uv run pyattacker export runs/qa.shard*of4.db runs/tasks.csv --rows tasks --format csv
```

`--rows` picks the shape: `pipelines` (nested, default), `tasks`, `attempts` (including each retry `decision`),
`events`, `artifacts`. `--format` picks `jsonl`, `json` or `csv`.

## Semantic Recovery

Every successful task persists its artifact and advances the checkpoint. On resume:

```python
runner.run(template.map(rows), resume=True)   # or pyattacker resume -c config.yaml
```

* Already successful pipelines → skipped outright;
* Failed pipelines → continue from **the first task that produced no artifact**: **if task C died, only task C
  reruns, and task B's request is not re-sent**;
* The seed artifact is persisted too → recovery **does not depend on the original dataset file**;
* Changed a task's source code (`spec_digest` includes source digests) → treated as a new pipeline, so old
  results are not incorrectly reused.

## Declarative (Simple Tasks)

```yaml
run:   { store: runs/demo.db, concurrency: 8, label: demo }
pools:
  apis:
    kind: llm
    algorithm: backoff
    resources: [ { id: api-1, capacity: 4, options: { model: gpt-4o } } ]
pipeline:
  name: qa
  tasks:
    - { use: pyattacker.tasks:echo }
    - { use: pyattacker.tasks:simulate_llm, resource: apis, algorithm: backoff,
        retry: { max_attempts: 3, on: [RetryableError, TimeoutError] } }
source: { kind: range, n: 100 }
```

```bash
uv run pyattacker validate -c examples/qa_eval.yaml
uv run pyattacker run      -c examples/qa_eval.yaml --progress
uv run pyattacker watch    runs/demo.db          # open another process to monitor it live
uv run pyattacker report   runs/demo.db --errors 20
uv run pyattacker export   runs/demo.db out.jsonl
```

Exit codes: `0` all succeeded / `1` some failed / `2` config error / `130` interrupted.

## Records and Monitoring

The complete story of one pipeline = five tables queried by `pipeline_id`: state and checkpoint, the final
state of each task, the full attempt history (including **every retry decision**
`{retry, reason, delay_s, error_class}`), intermediate and final artifacts, and the structured event stream.
`pyattacker report/watch` consumes these facts directly.

## Out of Scope

Network requests (you write the openai/anthropic protocols yourself), **semantic reduction** (accuracy / pass@k
and other cross-pipeline aggregation), DAG orchestration, service-ification, distributed scheduling. See
sections 1 and 11 of [`docs/design.md`](docs/design.md).

## Development

```bash
uv sync                      # create the venv + install dependencies (the only core dependency is pyyaml)
uv run pytest                # 107 tests, all zero-network, finishing in < 1s
uv run pyattacker demo       # end-to-end smoke test
```

Design document: [`docs/design.md`](docs/design.md) (conceptual model, the six core invariants, data model,
known tradeoffs, milestones).
