<p align="center">
  <img src="https://raw.githubusercontent.com/Hazer-BJTU/pyattacker/ff819b1710d26eadf7c2e89465044975aef8eb8e/assets/logo/title.png" width="600" alt="pyattacker">
</p>

[![PyPI](https://img.shields.io/pypi/v/pyattacker)](https://pypi.org/project/pyattacker/)
[![CI](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml/badge.svg)](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/pyattacker)](https://pypi.org/project/pyattacker/)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE)

> Run tens of thousands of independent tasks to completion — resumably, observably, and without
> reimplementing endpoint pools, retries and "which rows already ran" for the fifth time.

## The situation this is for

Picture a long workflow test, a model benchmark, or any experiment made of many independent items.
Three hours in, one network request fails. The process dies — and you have no idea *which stage*
each item actually reached. Rerunning from scratch means paying for every request that already
succeeded, so you start writing a `results.jsonl` and a "skip what's already in there" check.

Then the provider starts rate-limiting you. A single API key serialises your parallel requests into
a queue, so you add a second key, a third, and now you need to decide which request goes where, what
happens when one endpoint starts returning 429s, and whether a failure means *retry* or *give up*.
Somewhere in there, `asyncio.Semaphore` stops being enough and you are writing a scheduler.

pyattacker is that scheduler, extracted and made boring:

* **a failure costs you one task, not the run** — every task's output is persisted the moment it is
  produced, so `resume` continues from durable task checkpoints; external side effects still need
  idempotency when a crash occurs before checkpoint persistence;
* **endpoints are a pool, not a global variable** — capacity, health, and quota per endpoint, leased
  through `async with`, with seven policies for choosing which one to use and how to wait;
* **the record is queryable, not a log file** — every attempt, every retry decision (`{retry, reason,
  delay_s, error_class}`), every intermediate artifact, in SQLite you can `SELECT` from while the run
  is still going.

It **does not touch the network**: you write the openai/anthropic calls, it handles everything around
them. The base install has no dependencies at all — the only third-party code in the project is a YAML
parser, it is optional, and only a `.yaml`/`.yml` config needs it.

**New here?** The [tutorial](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/tutorial.md) goes from a five-line program to a sharded, resumable
model evaluation. Every snippet in it is executed by the test suite.

## Install

```bash
uv add pyattacker            # or: pip install pyattacker
uv add "pyattacker[yaml]"    # only for .yaml/.yml configs; JSON and TOML need no extra
```

From a clone:

```bash
git clone https://github.com/Hazer-BJTU/pyattacker && cd pyattacker
uv sync
uv run pyattacker demo       # zero-config smoke test: 50 simulated pipelines, retries, a report
```

Requires Python 3.11+. `import pyattacker`, the CLI and every config format except YAML work with no
third-party package installed; a `.yaml`/`.yml` config without the extra is a config error (exit code 2)
that names the extra to install.

## Core Model

Five concepts, and that is the whole vocabulary:

| Concept | Meaning | In one line |
|---|---|---|
| **artifact** | the persisted state of a task | content-addressed, **persisted as soon as it is produced** → checkpoint granularity = task |
| **task** | the smallest unit of scheduling | a unary `(artifact) -> artifact` function, sync or async — or `-> artifact \| Handoff`, to skip ahead ([advanced](#advanced-handoffs-opt-in)) |
| **pipeline** | the unit of completion | `fetch \| ask \| judge \| metrics` chained linearly, semantically independent of each other |
| **resource** | a leasable external capability | one endpoint / one key; once pooled, it can be published and subscribed to concurrency-safely |
| **algorithm** | the policy for acquiring resources | `wait`, `backoff`, `least_busy`, `failover`, `sticky`, `quota_aware`, `immediate` — orthogonal to "retry on failure" |

## 30-Second Quickstart (SDK)

The program below is complete: it defines every name it uses, runs offline, and is executed by the test
suite on every commit. Only the built-in simulation is involved — no API key, no network.

```python
# example/readme_quickstart.py
"""A first run: one custom task, a pool of two simulated endpoints, four pipelines, one report."""
from pyattacker import Pool, Resource, Retrying, Runner, pipeline, task


@task("fetch", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, base=0.5, cap=30.0), timeout_s=60)
async def fetch(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:                     # returned on exit; returned on exception too
        await ctx.clock.sleep(0.001)                       # stand-in for the network call
        lease.report(ok=True, usage={"tokens": 128})       # report health/quota back to the pool
        return {"q": row["q"], "a": f"answer from {lease.resource.id}"}


def dataset_rows():
    """Your dataset. A generator works too: `map` streams it, so memory stays O(concurrency)."""
    return [{"q": f"question-{i}"} for i in range(4)]


pool = Pool("apis",
            [Resource.create("llm", id=f"api-{i}", capacity=4) for i in range(1, 3)],
            algorithm="backoff")

template = pipeline("qa", fetch)

with Runner(store="runs/qa.db", pools=[pool], concurrency=8) as runner:
    report = runner.run(template.map(dataset_rows()))
    print(report.summary())
    report.export_jsonl("runs/qa.jsonl")
```

`ctx.acquire()` falls back to the task's declared `resource`, and `lease.report(ok=...)` feeds the pool's
health and quota accounting.

Where the real call goes — this part is yours, and it comes after the framework is already working:

```python
@task("ask", resource="apis", retry={"max_attempts": 3}, timeout_s=60)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:
        text = await lease.client.chat(row["q"])         # lease.client is built by resource.factory
        return {"q": row["q"], "a": text}

pool = Pool("apis",
            [Resource.create("llm", capacity=4, options={"model": "gpt-4o"},
                             factory=lambda res: MyClient(res.options)) for _ in range(8)],
            algorithm="backoff")
```

`MyClient` is your client class. `factory` is called once per resource, lazily, on the first lease, and
whatever it returns is `lease.client` — pyattacker never opens a socket itself.

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

## Semantic Recovery

Every successful task persists its artifact and advances the checkpoint. On resume:

```python
runner.run(template.map(rows), resume=True)   # or pyattacker resume -c config.yaml
```

* Already successful pipelines with matching task/input identity → skipped outright;
* Failed pipelines → continue from **the first task that produced no artifact**: **if task C died, only task C
  reruns when task B's checkpoint is durable**;
* The seed artifact is persisted too → recovery **does not depend on the original dataset file**;
* A pipeline that **handed off** ([advanced](#advanced-handoffs-opt-in)) resumes at the station it jumped to,
  with the entry state the ledger recorded — the task that handed off is not re-run;
* Changed a task's source code (`spec_digest` includes source digests) → treated as a new pipeline, so old
  results are not incorrectly reused. Factory parameters, fanout children, retry/algorithm policies and
  explicit task `config`/`version` are included too. Explicit keys reject changed definitions or inputs.

**Upgrading an existing store:** fingerprint v2 changes default IDs and shard assignment; legacy work may
rerun and old explicit keys conflict. Finish old runs with the old package, then use a new store. See the
[resume identity and idempotency reference](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/reference.md#resume-identity)
for migration guidance, dynamic functions and external configuration.

## Advanced: Handoffs (Opt-In)

**Advanced tier: opt-in, changes the execution model, not needed for ordinary pipelines, experimental until
1.0.** A task can decide that the rest of the chain no longer needs to run, and *say so* instead of inventing a
failure or hiding the branch inside one step. It **returns** a directive — `Handoff.to(target, value)` to
continue at a declared later station, `Handoff.end(value)` to finish the pipeline right there:

```python
# example/readme_handoff.py
"""A gate that skips the stations it does not need, and records why."""

from pyattacker import Handoff, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"q": seed["q"], "confidence": seed["confidence"]}


@task("ask")
async def ask(row: dict, ctx) -> dict:
    await ctx.clock.sleep(0.001)                       # <- your HTTP call
    return {**row, "answer": f"answer-for:{row['q']}"}


@task("judge")
def judge(row: dict) -> Handoff | dict:
    if row["confidence"] >= 0.9:
        return Handoff.end({**row, "verdict": "confident"}, reason="already good enough")
    if row["confidence"] >= 0.5:
        return Handoff.to("report", {**row, "verdict": "ok"}, reason="metrics not needed")
    return {**row, "verdict": "needs-metrics"}


@task("metrics")
def metrics(row: dict) -> dict:
    return {**row, "score": round(row["confidence"] * 10, 1)}


@task("report")
def report(row: dict) -> dict:
    return {**row, "reported": True}


# Edges are declared, never derived: judge may continue at report, or end the pipeline.
template = pipeline("qa", prepare | ask | judge | metrics | report,
                    control={"edges": {"judge": ["report", "end"]}})

with Runner(store=":memory:", concurrency=4) as runner:
    report_obj = runner.run(template.map([
        {"q": "2+2", "confidence": 0.95},                        # judge ends the pipeline here
        {"q": "17*23", "confidence": 0.6},                       # judge skips metrics, continues at report
        {"q": "prove sqrt(2) is irrational", "confidence": 0.2},  # no handoff: the whole chain runs
    ]))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        ran = [t.name for t in store.tasks(record.pipeline_id)]
        hops = store.handoffs(pipeline_id=record.pipeline_id)
        print(f"{record.state}  ran={ran}  skipped={len(template.tasks) - len(ran)}  handoffs={len(hops)}")
```

* **Declared forward edges.** A `Handoff` along an undeclared edge, or from a pipeline with no
  `control` block, is a fatal configuration error — never retried, never a silent jump. Destinations must be
  strictly later than their source; `end` from the last task is refused because it would do nothing.
* **A handoff is a disposition, not a failure.** It is a return value, so the retry policy never sees it and
  a task-side `except Exception:` cannot swallow it; leases are released exactly as on success, and a
  cancelled or timed-out attempt never reaches the return.
* **It is a durable checkpoint.** The jump is committed atomically (source task, attempt, entry artifact,
  ledger row and cursor together), so a killed process resumes **at the target** with the recorded entry
  state and does not re-run the source task. On a control-enabled pipeline `n_tasks_done` is a *position*,
  not a progress count: skipped stations have no task rows. `report`/`watch` count commits in the selected
  scope (one run when filtered, all history otherwise); `/pipelines.handoffs` counts active execution records and `handoffs_historical` counts all ledger
  records. A resume can use an earlier run's active handoff while recording no new handoffs. Pipeline
  exports include ledger identity and the watermark to distinguish the scopes.
* **Opt-in and inert.** Without the `control` block nothing changes — not one row, not one counter, and not a
  byte of `spec_digest`.
* **Backward traversal is separately declared.** `Handoff.rewind(target, value)` sends author-selected
  state to an earlier task; `Handoff.retry_all()` restarts from the original bound seed. Declare
  `control.rewind` / `control.retry_all` and a finite `control.max_handoffs`. Optional `HistoryArtifact`
  payloads provide explicit snapshots and restoration; ordinary dictionaries remain author-controlled.
  Visits and exact artifact occurrences retain history and make recovery safe. See the
  [backward traversal guide](docs/backward.md) for APIs, budgets and recovery boundaries.

The API is one class ([`Handoff`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/reference.md#advanced-handoffs-opt-in)),
one declaration (`control={"edges": {...}}`) and one optional store capability; a custom store that cannot
commit a handoff atomically is refused up front instead of writing a jump that would not survive a crash.
Walkthrough: [tutorial step 15](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/tutorial.md#step-15--advanced-skipping-stations-handoffs).
Model and rules: [design §4.8](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/design.md#48-advanced-handoffs--declared-forward-jumps-opt-in-experimental).

When a pipeline restarts from the seed, previous task rows and chain artifacts are reset atomically with the cursor/watermark;
previous handoffs stay in its history but no longer act as
checkpoints. A later resume follows only the current execution's handoffs, and completion selects one
final artifact. Custom stores supporting handoffs must persist the durable `handoff_floor` watermark
alongside the cursor and implement atomic `reset_pipeline(record)`; see the [recovery contract](docs/reference.md#tables-and-readers).

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

`--rows` picks the shape: `pipelines` (nested, default — its tasks, artifacts and handoffs included), `tasks`,
`attempts` (including each retry `decision`), `events`, `artifacts`. `--format` picks `jsonl`, `json` or `csv`.

## Declarative (Simple Tasks)

The YAML describes **composition and resources**; the logic stays in Python (`use: my_pkg.tasks:ask`).
Reading a `.yaml`/`.yml` file is the only part of pyattacker that needs a third-party library, so it lives
in the `yaml` extra; the same config written as JSON or TOML is read with the standard library. The loader
decides per file, from the suffix, and says which extra to install when the parser is missing.

The block below is a complete config — every `use:` target is a built-in, so it validates as it stands.
Save it as `qa.yaml`; `examples/qa_eval.yaml` is the same shape with more comments.

```yaml
# example/readme_qa_eval.yaml
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
    - use: pyattacker.tasks:simulate_llm            # a factory: `kwargs` are its arguments
      resource: apis
      algorithm: backoff
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }
      retry: { max_attempts: 3, "on": [RetryableError, TimeoutError] }
source: { kind: range, n: 100 }
```

A bare `on:` is a YAML 1.1 boolean key, not the retry field: retry keys must be quoted. The loader
detects the mistake and says so rather than silently ignoring the policy.

```bash
uv run pyattacker validate -c qa.yaml            # parse, check, print the effective config
uv run pyattacker run      -c qa.yaml --progress
uv run pyattacker watch    runs/demo.db          # open another process to monitor it live
uv run pyattacker report   runs/demo.db --errors 20
uv run pyattacker export   runs/demo.db out.jsonl
uv run pyattacker serve    runs/demo.db          # HTTP dashboard + JSON endpoints
uv run pyattacker plugins                        # installed plugins
```

Exit codes: `0` all succeeded / `1` some failed / `2` config error / `130` interrupted.
Every flag of every subcommand: [`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/cli.md).

## Records and Monitoring

The complete story of one pipeline = six tables queried by `pipeline_id`: state and checkpoint, the final
state of each task, the full attempt history (including **every retry decision**
`{retry, reason, delay_s, error_class}`), intermediate and final artifacts, the control-flow ledger
(`handoffs`, empty for an ordinary pipeline), and the structured event stream.
`pyattacker report/watch` consumes these facts directly.

## Extending It

**Plugins** are ordinary `importlib.metadata` entry points — install a package and its names become
usable from any config:

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"        # a TaskSpec, or a factory returning one
[project.entry-points."pyattacker.algorithms"]
my_algo  = "my_pkg.algo:MyAlgorithm"
[project.entry-points."pyattacker.stores"]
s3       = "my_pkg.s3:open_store"         # keyed by URI scheme: store = "s3://bucket/runs.db"
```

```bash
uv run pyattacker plugins                 # what is installed, and what failed to load
```

Built-ins resolve first (a plugin cannot shadow `echo`), and a plugin that raises on import is
recorded rather than fatal. A complete worked example: [`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.md).

**Large payloads** can live outside the database:

```bash
uv run pyattacker run -c examples/qa_eval.yaml --artifact-backend file:///data/blobs
# or, in the config:  artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

Files are content-addressed, written atomically, and hydrated back on read — so a resumed run
reuses spilled checkpoints transparently. `null` keeps digests and drops bytes; `inline` (default)
keeps everything in the store.

**A zero-dependency monitoring endpoint**:

```bash
uv run pyattacker serve runs/qa.db        # http://127.0.0.1:8787
# /  dashboard   /stats  /events  /pipelines  /resources  /errors   (JSON)
```

It opens a fresh read-only connection per request, so it runs happily beside a live run. It has **no
authentication** and binds to loopback: it exposes your payloads, so treat it as a debug view and do not
put it on a public interface without your own proxy in front.

**Branching inside a step** — `fanout(a, b)` runs several tasks on the same input concurrently and
returns `{task_name: value}`. Retry granularity becomes the group, which is the honest price of not
turning pipelines into a DAG. The group is the only spec the Runner sees, so `resource`, `algorithm` and
`timeout_s` are inherited from the children when all of them agree (a `timeout_s` then bounds the whole
group).

## Benchmarking the Algorithms

Which acquisition algorithm should a workload use? The pool ships seven, and the honest answer depends
on the provider — so there is a simulation for it. A scenario states the assumptions (a capacity cycle,
a token bucket that tightens under pressure, latency with a slow tail, independent failures and
correlated storms, three endpoints of different character), the client is a closed loop of workers
driving the real `Pool` and the real algorithm, and time is simulated, so a ten-minute scenario costs
seconds.

```bash
uv run pyattacker bench                       # the scenario's 5 algorithms x 3 seeds, about 13 seconds
uv run pyattacker bench --list                # the scenarios, the algorithms, and what each metric means
uv run pyattacker bench --algorithms wait,backoff --seeds 5 --json runs/bench.json
```

It is a black box on purpose: the provider never exposes its state to the algorithm, its mood is a
function of time rather than of who is asking, and each request's draws are indexed by its ordinal — so
two algorithms meet the same *exogenous* randomness and the same weather, and the comparison is between
algorithms rather than between moods. (Their realized provider state still diverges, because the bucket
and the in-flight count react to what each of them did — that divergence is the measurement.)
It reports a vector of metrics (throughput, tail latency, retries, refusals provoked, capacity
utilisation, fairness across endpoints) instead of one weighted score, and it names the winner per metric
— including when the winner is "nobody", when there was only one algorithm left to compare, and never for
an algorithm that completed a fraction of the work: a strategy that abandons the queue cannot win a rate
by keeping its denominator small.

[`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/benchmark.md) has the assumptions, the metrics, the current numbers, and
what they do not say.

## Documentation

| Document | What is in it |
|---|---|
| [`docs/tutorial.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/tutorial.md) | fourteen runnable steps, from "one task" to a sharded evaluation; each one executed by the test suite |
| [`docs/reference.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/reference.md) | every public class and function: signatures, parameters, examples |
| [`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/cli.md) | every subcommand, every flag, exit codes, config reference |
| [`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/design.md) | conceptual model, the six invariants, the lease contract, data model, tradeoffs |
| [`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/benchmark.md) | the algorithm benchmark: what the scenarios assume, what the metrics mean, how to read the table |
| [`CHANGELOG.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/CHANGELOG.md) | what changed, release by release |
| [`docs/releasing.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/releasing.md) | for maintainers: how a release is cut and published |

## Examples

| Example | What it shows |
|---|---|
| [`examples/quickstart.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/quickstart.py) | the SDK in 60 lines: a custom client factory, retries, resume |
| [`examples/llm_eval/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/llm_eval/README.md) | a complete evaluation — prepare → 2-turn model call → 3 judges → reduce, in **two pipeline shapes**, with the checkpoint-granularity tradeoff *measured* (grouped re-sent 2 judge requests that had already succeeded; split re-sent 0) |
| [`examples/sharded.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/sharded.py) | one dataset across N stores, then a merged report |
| [`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.md) | a real installable plugin: tasks, an algorithm, a codec |
| [`examples/qa_eval.yaml`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/qa_eval.yaml) | the declarative path, end to end |

```bash
uv run python examples/quickstart.py
uv run python -m examples.llm_eval.demo
uv run python examples/sharded.py
uv run pyattacker run -c examples/qa_eval.yaml --limit 40
```

## Out of Scope

These are design decisions, not missing features:

* **Network requests** — you write the openai/anthropic protocols yourself. The kernel never opens a socket.
* **Semantic reduction** — accuracy, pass@k, F1 and any cross-pipeline aggregation. Export the artifacts and
  compute it outside, or write a sink pipeline out of the primitives.
* **DAG orchestration** — a pipeline is a linear chain; branch inside a task with `fanout`. The one
  qualification is the opt-in [handoff](#advanced-handoffs-opt-in): it changes the traversal of
  the chain along declared edges, never its topology (no joins, no second entry point, no cross-pipeline
  jumps).
* **A serving gateway** — the only HTTP surface is the read-only debug endpoint above.
* **Distributed scheduling** — scale out with `--shard`; multi-process is the ceiling.

Sections 1 and 11 of [`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/design.md) state the boundary precisely, and section 8 lists
every known tradeoff with its reason.

## Status

**Unreleased — advanced control flow (opt-in).** Tasks can now
[hand off](#advanced-handoffs-opt-in): return a `Handoff` to skip declared stations or finish the pipeline
early, recorded in a durable ledger that recovery resumes from. It is opt-in and inert — a pipeline without a
`control` block writes no new rows and keeps a byte-identical `spec_digest` — and marked experimental until
1.0. Backward traversal now adds declared rewind and retry-all with visits and optional payload history.

**0.2.0 — a benchmark, stricter identity, and three correctness fixes.** New: `pyattacker bench`, a
simulated provider world that compares the acquire algorithms on a vector of metrics instead of a weighted
score ([`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/benchmark.md));
`Retrying.decide`, the retry decision as a method on the policy; and paged whole-kind reads
(`pyattacker.store.iter_*`, backed by the optional `PagedStore` extension) so exporting a large store no
longer materialises it. 0.1.x itself had implemented everything planned for M0–M4: the kernel, persistence
and task-level recovery, retries and error classification, the resource pool with 7 acquisition algorithms,
delayed continuations and write-behind batching, sharding and merged reports, five export shapes in three
formats, entry-point plugins, external artifact backends, the fan-out helper, and the HTTP monitoring
endpoint.

Three changes are worth reading before upgrading. **PyYAML is no longer a dependency** — the base install
has none at all, so a `.yaml`/`.yml` config needs `pip install "pyattacker[yaml]"`. **`with_overrides`
distinguishes "not given" from `None`**: an omitted keyword keeps the value, an explicit `None` now clears
`resource`/`algorithm`/`timeout_s`/`version`, and the new `UNSET` sentinel means "not given" when a caller
forwards a dict. **One shared validation entry** now backs `validate` and every `run` mode, so a config it
accepts is one that can run: unknown fields, wrong types, bad pool references, malformed artifact backends
and typos fail with exit 2 and a field path before anything starts. Export `limit` also means one thing per
row kind now, and `None` means complete.

Fixed since 0.1.1: `shell_run` leaked its child when the task was cancelled and killed without reaping on
timeout — every exit path, process creation included, now kills and reaps, and on POSIX it signals the whole
process group; exporting events silently stopped at the newest 100 000 rows; the declarative `run:` block
dropped `artifact_backend`, write-behind and the batch knobs, `--no-write-behind` was never applied in a
single process, and `--artifact-backend` never reached shard children; resume now rejects a pipeline key
whose task or seed digest changed instead of quietly reusing a stale result; and the benchmark collected a
list of its own fixes (see the changelog). The API is young: it follows semantic versioning from here, but
expect refinement before 1.0.

Left for later: a distributed scheduler, Parquet export, blob garbage collection, and first-class
`Parallel`/`Gather` nodes.

## Development

```bash
uv sync                      # create the venv + install the dev group (which includes the optional yaml extra)
uv run pytest                # the whole suite: zero network, a few seconds
uv run ruff check            # lint (configuration lives in pyproject.toml, with reasons for each exception)
uv run pyattacker demo       # end-to-end smoke test
uv run pyattacker bench      # compare the acquire algorithms in simulation (about 13 seconds)
uv build                     # sdist + wheel
```

Tests are offline and deterministic (time goes through an injectable `Clock`). The tutorial's code blocks are
extracted and executed by `tests/test_tutorial.py`, so documentation that rots fails CI.

## License

MIT — see [LICENSE](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE).

