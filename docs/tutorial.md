# Tutorial: from one task to a resumable model evaluation

This is the systematic, step-by-step guide to pyattacker. It starts from a five-line program and ends with
a small but realistic model evaluation that retries, survives a provider outage, resumes from a checkpoint,
and can be split across processes.

Who it is for: you are running LLM / agent evaluations in Python, you already know `asyncio`, and what you
want is *not* another agent framework — it is the boring part (endpoint pools, concurrency, retries,
"which rows already ran", a record of every request).

How to read it:

* Every step is a **complete program**. Run it, read the output, then read the notes under it.
* Code blocks that begin with a `# tutorial/step_NN_....py` comment are extracted from this file and
  executed by [`tests/test_tutorial.py`](../tests/test_tutorial.py) on every test run, so the code you see
  is the code that runs. Save a block as the file named in that comment and `python` it directly.
* Nothing here touches the network. Where a real program would do `await self.http.post(...)`, the examples
  do `await asyncio.sleep(...)`; the comment `# <- your HTTP call` marks the exact spot.
* Reference material lives elsewhere: [`docs/design.md`](design.md) for the model and the tradeoffs,
  [`README.md`](../README.md) for the compact API tour, [`examples/`](../examples) for full programs.

## Look up by feature, not just by step

The steps below are meant to be read in order the first time. Once you already know the basics and just
want "how does X work", use this table instead of scanning headings:

| Feature | This tutorial | `design.md` | Source |
|---|---|---|---|
| Core concepts (artifact/task/pipeline/runner) | [Step 1](#step-1--one-seed-one-task-one-run) | [§3](design.md#3-conceptual-model) | `pipeline.py`, `task.py`, `runner.py` |
| `map`/streaming, pass@k (`repeats=`) | [Step 2](#step-2--many-inputs-at-once) | [§3](design.md#3-conceptual-model) | `pipeline.py` |
| Pipeline composition, artifact-type chaining | [Step 3](#step-3--chaining-tasks) | [§3](design.md#3-conceptual-model) | `pipeline.py` |
| Checkpoint/artifact, content addressing, `spec_digest` | [Step 4](#step-4--the-checkpoint-is-the-artifact) | [§4.1](design.md#41-task-level-checkpoint-and-recovery) | `artifact.py`, `runner.py` |
| Resource pools, basics | [Step 5](#step-5--endpoints-as-a-pool) | [§4.3](design.md#43-resource-pool) | `resource.py` |
| Resource pool state machine (READY/DEGRADED/DEAD/REVOKED) | *no tutorial step yet — only appears as bare config in Step 11* | [§4.3](design.md#43-resource-pool) | `resource.py` |
| Publish/subscribe bus (`ctx.subscribe`/`ctx.publish_resource`) | *no tutorial step yet* | [§4.3](design.md#43-resource-pool) | `resource.py` (`Bus`, `Pool.subscribe`) |
| Acquire algorithms (wait/backoff/least\_busy/failover/sticky/quota\_aware) | [Step 6](#step-6--choosing-how-to-wait) | [§4.4](design.md#44-algorithm-and-retry-are-two-orthogonal-axes) | `algorithm.py` |
| Lease safety contract | [Step 7](#step-7--the-lease-contract) | [§4.2](design.md#42--lease-safety-contract) | `task.py`, `resource.py` |
| Error classification, retry/backoff policy | [Step 8](#step-8--failures-classify-retry-record) | [§4.4](design.md#44-algorithm-and-retry-are-two-orthogonal-axes) | `errors.py`, `algorithm.py` |
| Resume / recovery | [Step 9](#step-9--resume-what-reruns-and-what-does-not) | [§4.1](design.md#41-task-level-checkpoint-and-recovery) | `runner.py` |
| Scheduler internals (`DelayQueue`, write-behind batching) | *mentioned only in passing (end of Step 8, troubleshooting notes) — no dedicated step* | [§4.5](design.md#45-delayed-continuations-and-batched-facts-m2) | `scheduler.py`, `store/writebehind.py` |
| Data model / store tables, export | [Step 10](#step-10--reading-the-record) | [§5](design.md#5-data-model-sqlite-wal--synchronousnormal) | `store/`, `export.py` |
| Declarative config | [Step 11](#step-11--the-declarative-path-and-the-cli) | [§6.2](design.md#62-declarative-simple-tasks) | `declarative.py` |
| CLI reference (`validate`/`run`/`resume`/`report`/`watch`/`export`/`serve`/`plugins`/`demo`) | scattered across [Step 0](#step-0--install-and-sanity-check), [Step 11](#step-11--the-declarative-path-and-the-cli), [Step 12](#step-12--sharding-and-merging) — no single summary table yet | — | `cli.py` |
| Sharding and merging | [Step 12](#step-12--sharding-and-merging) | [§6.3](design.md#63-sharding-merging-and-export-m3) | `shard.py`, `merge.py` |
| Monitoring / HTTP endpoint | *one sentence at the end of Step 14 — no runnable example* | [§4.6](design.md#46-monitoring-cares-about-traffic-and-blocking-not-about-metrics), [§6.4](design.md#64-plugins-backends-and-the-monitoring-endpoint-m4) | `server.py`, `monitor.py` |
| Custom types / codecs, artifact backends, plugins | [Step 14](#step-14--your-own-types-blobs-plugins) | [§6.4](design.md#64-plugins-backends-and-the-monitoring-endpoint-m4) | `artifact.py`, `backends.py`, `plugins.py` |
| Fan-out / branching, integration-scale example | [Step 13](#step-13--capstone-a-small-model-evaluation) | — | `tasks/__init__.py` (`fanout`) |

Rows marked "no tutorial step yet" are real gaps in this tutorial, not omissions from this table.
The `design.md`/source columns are the actual reference for those until a step is written.

---

## Step 0 — install and sanity-check

```bash
uv sync                    # or: python -m venv .venv && .venv/bin/pip install -e .
uv run pyattacker demo     # zero-config smoke test: 50 simulated pipelines, retries, a report
uv run pytest -q           # the whole suite, offline, a few seconds
```

`pyattacker demo` is the fastest way to see the shape of a run:

```text
run ... status=completed  wall=1.2s
  pipelines: total=50 succeeded=50
  ...
```

Requirements: Python 3.11+ and nothing else — pyattacker's only dependency is PyYAML.

---

## Step 1 — one seed, one task, one run

```python
# tutorial/step_01_smallest.py
"""Step 1 - the smallest useful program: one seed, one task, one persisted result."""

from pyattacker import Runner, pipeline, task


@task("double")
def double(seed: dict) -> dict:
    """A task is a unary function: one artifact in, one artifact out."""
    return {"n": seed["n"], "doubled": seed["n"] * 2}


template = pipeline("doubling", double)

with Runner(store="runs/step01.db", concurrency=2, label="step-1") as runner:
    report = runner.run(template.map([{"n": 21}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    print(f"pipeline {row['pipeline_id'][:12]} state={row['state']} "
          f"tasks={row['n_tasks_done']}/{row['n_tasks_total']}")
    print("final artifact:", row["artifacts"][-1]["payload"])
```

```text
run run-... status=completed  wall=0.00s
  pipelines: total=1 succeeded=1
  pipeline latency ms: p50=0.487 p95=0.487 max=0.487
  attempts: total=1
  tasks: double=1
pipeline b3265e1db9b8 state=succeeded tasks=1/1
final artifact: {'doubled': 42, 'n': 21}
```

Five concepts are already in play, and they are the whole framework:

| Concept | In this program | What it means |
|---|---|---|
| **artifact** | `{"n": 21, "doubled": 42}` | the persisted state of one task, content-addressed, written as soon as it is produced |
| **task** | `double` | a unary function `(artifact) -> artifact`; sync or async, no framework base class |
| **pipeline** | `pipeline("doubling", double)` | a linear chain of tasks; the unit of *completion* and *resume* |
| **seed** | `[{"n": 21}]` | one dataset row; `template.map(seeds)` turns each row into an independent pipeline |
| **runner** | `Runner(store=..., concurrency=2)` | the scheduler: owns the store, the pools, and the worker slots |

Notes worth knowing early:

* A task takes **one** positional parameter, or **two** (`value, ctx`) when it needs the framework context.
  Three parameters is a configuration error, not a runtime surprise — wrap extra state in a closure.
* `Runner` is a context manager and closing it closes the store. Reading a **file-backed** store after
  the `with` block has ended will fail; read inside, or reopen the file later (Step 10). An in-memory
  store is unaffected.
* `store=":memory:"` is the default and is perfect for tests; a path gives you a durable SQLite file.

---

## Step 2 — many inputs at once

The pipeline template is reusable: `map()` turns a stream of seeds into pipelines that are semantically
completely independent of each other.

```python
# tutorial/step_02_map.py
"""Step 2 - one template, many inputs: map() turns a stream of seeds into independent pipelines."""

import asyncio
import time

from pyattacker import Runner, pipeline, task


def dataset(n: int):
    """Any iterable works; a generator keeps memory flat no matter how large the dataset is."""
    for i in range(n):
        yield {"qid": f"q{i:02d}", "value": i}


@task("square")
async def square(row: dict) -> dict:
    await asyncio.sleep(0.02)  # <- in real life: one HTTP request
    return {"qid": row["qid"], "squared": row["value"] ** 2}


template = pipeline("squares", square)

with Runner(store="runs/step02.db", concurrency=4, label="step-2") as runner:
    started = time.perf_counter()
    report = runner.run(template.map(dataset(12)))
    elapsed = time.perf_counter() - started
    print(report.summary())
    print(f"12 pipelines x 20ms of work, concurrency=4 -> {elapsed:.2f}s wall clock")

    # pass@k: one seed expands into k independent pipelines, each with its own checkpoint.
    for spec in template.map([{"qid": "q00", "value": 0}], repeats=3):
        print(f"  repeat={spec.repeat} key={spec.key[:12]}")
```

```text
run run-... status=completed  wall=0.07s
  pipelines: total=12 succeeded=12
  pipeline latency ms: p50=20.734 p95=21.868 max=22.085
  attempts: total=12
  tasks: square=12
12 pipelines x 20ms of work, concurrency=4 -> 0.07s wall clock
  repeat=0 key=b8c5e6d11da5
  repeat=1 key=418c035f6b74
  repeat=2 key=055e9cb3f519
```

* `map()` takes **any iterable**, including a generator, and yields `PipelineSpec` objects lazily. The
  producer/worker queue keeps memory at O(`concurrency`), so a 10-million-row dataset costs the same as
  a 10-row one.
* `concurrency=4` means *four attempts in flight*, not four pipelines alive: a pipeline waiting out a
  retry backoff holds no worker slot (Step 8).
* 12 × 20 ms of work at concurrency 4 takes ~0.07 s, not 0.24 s — parallelism comes from `asyncio`
  tasks inside the runner, so your task body must actually await something to overlap.
* `repeats=3` gives you **pass@k / self-consistency sampling** for free: one seed, three pipelines, three
  independent checkpoints. `key_of=` lets you supply your own stable ids (e.g. your dataset's primary key)
  instead of the content-addressed default.
* `spec.key` (== `pipeline_id`) is derived from *the task chain + the seed content + the repeat index*.
  Re-running the same dataset therefore produces the same ids — that is what makes resume and sharding
  possible.

---

## Step 3 — chaining tasks

`a | b | c` builds a pipeline. Each task's artifact is the next task's input, and the chain is validated
**when you build it**, not halfway through a run.

```python
# tutorial/step_03_chain.py
"""Step 3 - chaining: prepare | answer | judge. Each task's artifact is the next task's input."""

from pyattacker import PipelineBuildError, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"question": seed["q"], "context": ["doc-1", "doc-2"]}


@task("answer")
def answer(row: dict) -> dict:
    return {**row, "answer": f"answer to {row['question']!r}"}


@task("judge")
def judge(row: dict) -> dict:
    return {"question": row["question"], "answer": row["answer"], "score": len(row["answer"]) % 5}


@task("expects_int")
def expects_int(value: int) -> int:
    return value


# The chain is validated where it is built, not in the middle of a run:
try:
    pipeline("broken", prepare | expects_int)
except PipelineBuildError as exc:
    print("rejected at construction time:", exc)

template = pipeline("rag", prepare | answer | judge, tags={"example": "step-3"})

with Runner(store="runs/step03.db", concurrency=2, label="step-3") as runner:
    report = runner.run(template.map([{"q": "why is the sky blue?"}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    print("tasks:")
    for item in row["tasks"]:
        print(f"  seq={item['seq']} {item['name']:<8} {item['state']:<9} "
              f"attempts={item['attempts_used']} duration_ms={round(item['duration_ms'], 3)}")
    print("artifacts (the seed, then one per task):")
    for item in row["artifacts"]:
        print(f"  seq={item['seq']:>2} {item['task']:<10} digest={item['digest'][:12]} "
              f"payload={item['payload']!r}")
    print("final artifact:", row["artifacts"][-1]["payload"])
```

```text
rejected at construction time: artifact types do not chain: task 'prepare' produces dict, but task 'expects_int' requires int
run run-... status=completed  wall=0.00s
  pipelines: total=1 succeeded=1
  ...
tasks:
  seq=0 prepare  succeeded attempts=1 duration_ms=0.016
  seq=1 answer   succeeded attempts=1 duration_ms=0.005
  seq=2 judge    succeeded attempts=1 duration_ms=0.004
artifacts (the seed, then one per task):
  seq=-1 __seed__   digest=ce5d39232b24 payload={'q': 'why is the sky blue?'}
  seq= 0 prepare    digest=0ad2b84efc93 payload={'context': ['doc-1', 'doc-2'], 'question': 'why is the sky blue?'}
  seq= 1 answer     digest=c6fbc3132821 payload={'answer': "answer to 'why is the sky blue?'", 'context': [...], 'question': '...'}
  seq= 2 judge      digest=ffc919adcb7e payload={'answer': "answer to 'why is the sky blue?'", 'question': '...', 'score': 2}
final artifact: {'answer': "answer to 'why is the sky blue?'", 'question': 'why is the sky blue?', 'score': 2}
```

* Type checking uses the **annotations**: a task returning `Question` chains into a task taking `Question`
  (subclasses are accepted), `Any` or no annotation is permissive, and a bare container accepts its
  parameterised form (`dict` ← `dict[str, Any]`). Mismatches raise `PipelineBuildError` immediately.
* `seq` is the task's position in the chain. `seq=-1` is the seed — the dataset row itself is a stored
  artifact, which matters for resume (Step 9).
* The final artifact is simply the last task's output; `is_final=True` marks it, and `export` can hand it
  to you per pipeline.
* A pipeline is deliberately **linear**. If a step genuinely branches — three judges, k samples, several
  metrics — keep the branch inside one task with `fanout(...)` (Step 13). Turning pipelines into a DAG is
  out of scope, and that is what keeps completion and resume this simple.

---

## Step 4 — the checkpoint is the artifact

This is the property the whole design exists for: **every successful task persists its artifact
immediately**, so the checkpoint granularity is the task.

```python
# tutorial/step_04_checkpoint.py
"""Step 4 - what a checkpoint is: one content-addressed artifact per task, durable immediately."""

from dataclasses import dataclass

from pyattacker import Runner, pipeline, task


@dataclass
class Answer:
    qid: str
    text: str


@task("fetch")
def fetch(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": seed["question"]}


@task("answer")
def answer(row: dict) -> Answer:
    """An annotated type is registered automatically, so a restored checkpoint is an Answer again."""
    return Answer(qid=row["qid"], text=f"answer to {row['question']!r}")


@task("score")
def score(ans: Answer) -> dict:
    return {"qid": ans.qid, "score": len(ans.text) % 5}


template = pipeline("qa", fetch | answer | score, tags={"stage": "tutorial"})
rows = [{"qid": "q1", "question": "why?"}, {"qid": "q2", "question": "how?"}]

with Runner(store="runs/step04.db", concurrency=2) as runner:
    first = runner.run(template.map(rows))
    print("first run: ", first.stats["pipelines"]["by_state"])

    # Same seeds + same task source -> the same content-addressed keys -> nothing runs again.
    again = runner.run(template.map(rows))
    print("second run: skipped =", again.skipped,
          "(a skipped pipeline is not rewritten, so it still belongs to the first run)")

    row = next(iter(runner.store.export_rows()))
    print(f"\npipeline {row['pipeline_id'][:12]} tags={row['tags']}")
    for item in row["artifacts"]:
        print(f"  seq={item['seq']:>2} {item['task']:<10} {item['type']:<7} codec={item['codec']:<5} "
              f"digest={item['digest'][:10]} final={item['is_final']} payload={item['payload']!r}")

    checkpoint = runner.store.get_artifact(row["pipeline_id"], 1)
    restored = runner.registry.load(checkpoint.encoded())
    print("checkpoint at seq=1 restores as:", type(restored).__name__, restored)


@task("score")
def score_changed(ans: Answer) -> dict:
    """Same name, different body: the source digest changes, so this is a *different* pipeline."""
    return {"qid": ans.qid, "score": len(ans.text) % 7}


changed = pipeline("qa", fetch | answer | score_changed, tags={"stage": "tutorial"})
print("\nchanged task source -> new pipeline:", template.spec_digest != changed.spec_digest)
print("old spec_digest:", template.spec_digest[:16], "new spec_digest:", changed.spec_digest[:16])
```

```text
first run:  {'succeeded': 2}
second run: skipped = 2 (a skipped pipeline is not rewritten, so it still belongs to the first run)

pipeline 46122647cecb tags={'stage': 'tutorial'}
  seq=-1 __seed__   dict    codec=json  digest=354ba03eb6 final=False payload={'qid': 'q1', 'question': 'why?'}
  seq= 0 fetch      dict    codec=json  digest=354ba03eb6 final=False payload={'qid': 'q1', 'question': 'why?'}
  seq= 1 answer     Answer  codec=json  digest=a14ab1a820 final=False payload={'qid': 'q1', 'text': "answer to 'why?'"}
  seq= 2 score      dict    codec=json  digest=29d44d6ed5 final=True payload={'qid': 'q1', 'score': 1}
checkpoint at seq=1 restores as: Answer Answer(qid='q1', text="answer to 'why?'")

changed task source -> new pipeline: True
old spec_digest: 5808fe46061bad55 new spec_digest: 01b60826ed0465ea
```

Four things just happened, in order, for every successful task: the artifact bytes are written, the task
row is finalised, `n_tasks_done` advances, and only then does the next task start.

* **Content addressing.** `seq=-1` and `seq=0` share a digest here because `fetch` returns the seed
  unchanged — identical payloads are identical bytes. Digests are `blake2b`, and the artifact's identity
  is `(pipeline_id, seq)`.
* **Dataclasses round-trip.** Annotating the return type (`-> Answer`) registers the class, so a restored
  checkpoint is an `Answer`, not a `dict`. For your own binary types, register a codec (Step 14).
* **Change the task, get a new pipeline.** The pipeline key includes a digest of each task's *source code*,
  plus its name, resource and `timeout_s`, plus five fields of its retry policy (`max_attempts`, `base`,
  `factor`, `cap`, `jitter` — the parts that change how long a step takes, not `on` / `retry_unknown` /
  `max_total_s`). Editing a task body therefore abandons the old
  checkpoints instead of silently reusing results produced by different code. Pass `include_code=False` on
  `pipeline(...)` if you deliberately want code changes to reuse them.
* **Re-running the same seeds is a no-op.** The second run reports `skipped=2` and total=0: a skipped
  pipeline is not rewritten, so its row still belongs to the run that actually did the work. Nothing was
  recomputed and no artifact was touched.

---

## Step 5 — endpoints as a pool

A `Resource` is one concrete capability (an endpoint, a key, a local worker). A `Pool` is a group of them
plus a policy for waiting. Tasks get one through `ctx.acquire(...)`, and the pool is the **only** shared
surface in the framework.

```python
# tutorial/step_05_pool.py
"""Step 5 - endpoints as a pool: a factory makes a client, capacity bounds concurrency, selectors route."""

import asyncio

from pyattacker import Pool, Resource, Runner, pipeline, task


class Client:
    """The shape your real provider client should have: built from resource.options, owned by the pool."""

    def __init__(self, options: dict) -> None:
        self.model = options["model"]

    async def chat(self, prompt: str) -> str:
        await asyncio.sleep(0.01)  # <- your HTTP call
        return f"[{self.model}] {prompt}"


def build_client(resource: Resource) -> Client:
    return Client(resource.options)


@task("ask", resource="apis", timeout_s=5)
async def ask(row: dict, ctx) -> dict:
    model = "gpt-4o" if row["hard"] else "gpt-4o-mini"
    async with ctx.acquire(model=model) as lease:  # only a matching resource is handed out
        text = await lease.client.chat(row["question"])
        lease.report(ok=True, latency_ms=10, usage={"tokens": len(text)})
        return {"question": row["question"], "answer": text, "served_by": lease.resource.id}


pool = Pool(
    "apis",
    [
        Resource.create("llm", id="api-a", capacity=2, options={"model": "gpt-4o"}, factory=build_client),
        Resource.create("llm", id="api-b", capacity=2, options={"model": "gpt-4o"}, factory=build_client),
        Resource.create("llm", id="api-c", capacity=4, options={"model": "gpt-4o-mini"}, factory=build_client),
    ],
)

rows = [{"question": f"q{i}", "hard": i % 2 == 0} for i in range(6)]

with Runner(store="runs/step05.db", pools=[pool], concurrency=8) as runner:
    report = runner.run(pipeline("qa", ask).map(rows))
    print(report.summary())

    print("resource state (capacity is per resource, so this pool can hand out 8 leases at once):")
    for slot in pool.snapshot():
        print(f"  {slot['id']:<6} state={slot['state']:<7} leases={slot['leases']} "
              f"active_now={slot['active']}/{slot['capacity']}")

    stats = pool.stats()
    print(f"pool totals: leases={stats.leases_total} ok={stats.ok_total} "
          f"waiting={stats.waiting} utilization={stats.utilization}")
    print("usage reported through lease.report():", stats.usage)
```

```text
run run-... status=completed  wall=0.02s
  pipelines: total=6 succeeded=6
  ...

resource state (capacity is per resource, so this pool can hand out 8 leases at once):
  api-a  state=ready   leases=2 active_now=0/2
  api-b  state=ready   leases=1 active_now=0/2
  api-c  state=ready   leases=3 active_now=0/4
pool totals: leases=6 ok=6 waiting=0 utilization=0.0
usage reported through lease.report(): {'tokens': 81.0}
```

* `capacity` belongs to the **resource**, not the pool: `api-a` and `api-b` allow 2 concurrent leases
  each, `api-c` allows 4, so this pool can have 8 requests in flight. Total pool capacity is the number
  you compare against `concurrency`.
* `factory=` is called **once per resource**, lazily, the first time that resource is leased, and every
  lease then shares the same client object — that is where you put your connection pool / SDK client.
  If the factory raises, the resource is treated as unusable: the lease is refused with
  `ResourceUnavailable` (never a lease whose `client` is `None`), the reason is recorded as a
  `resource.factory_failed` event, and repeated refusals eventually mark the resource `dead`.
* The selector (`ctx.acquire(model="gpt-4o-mini")`) matches against `options`, `tags`, `id` and `kind`,
  and supports dot paths into nested options (`"quota.tokens"`). Two models cannot be served by one
  resource, so the selector is how you route to the right client.
* `lease.report(...)` is how the pool learns: `ok=False` feeds circuit-breaking, `latency_ms` maintains an
  EMA, and `usage={"tokens": n}` accumulates quota you can rank on (Step 6). All of it shows up in
  `pool.stats()` and in the run's resource table.
* `pool.snapshot()` is a per-resource view, `pool.stats()` the aggregate. Both are safe to call while a
  run is in flight, which is what the HTTP monitor does (Step 14).

> **Gotcha:** with the default `wait` algorithm, a selector that matches **no** resource blocks forever.
> Either make sure every selector has a resource, or pass `timeout=` / use `algorithm="immediate"` so you
> get an error instead of a hang.

---

## Step 6 — choosing how to wait

Acquiring a resource is policy; using it is your code. The algorithm is set per task or per pool.

```python
# tutorial/step_06_algorithms.py
"""Step 6 - how a task waits for a resource is a policy, orthogonal to what the task does."""

import asyncio

from pyattacker import Pool, Resource, Runner, pipeline, task


def make_pool(name: str, algorithm: str, *, resources: int = 1, capacity: int = 1,
              options: dict | None = None) -> Pool:
    return Pool(
        name,
        [
            Resource.create("llm", id=f"{name}-{i}", capacity=capacity,
                            options=dict(options or {"model": "m"}))
            for i in range(resources)
        ],
        algorithm=algorithm,
    )


def experiment(algorithm: str, rows: list, *, resources: int = 1, capacity: int = 1,
               concurrency: int = 4):
    """One task body, one pool, four acquire policies."""

    @task("ask", resource=f"p-{algorithm}", algorithm=algorithm)
    async def ask(row: dict, ctx) -> dict:
        async with ctx.acquire() as lease:
            await asyncio.sleep(0.02)  # the request
            return {"i": row["i"], "resource": lease.resource.id}

    pool = Pool(f"p-{algorithm}", [
        Resource.create("llm", id=f"p-{algorithm}-{i}", capacity=capacity, options={"model": "m"})
        for i in range(resources)
    ], algorithm=algorithm)
    with Runner(store=f"runs/step06_{algorithm}.db", pools=[pool], concurrency=concurrency) as runner:
        report = runner.run(pipeline(f"t-{algorithm}", ask).map(rows))
        error_types = sorted({e["error_type"] for e in runner.store.errors()})
    return report, pool, error_types


rows4 = [{"i": i} for i in range(4)]

# immediate: fail fast when the pool is saturated (note: ResourceUnavailable is classified "unknown",
# so the default policy does not retry it - add retry_unknown=True if you want capacity retries)
report, _, error_types = experiment("immediate", rows4)
print("immediate :", report.stats["pipelines"]["by_state"], "-> errors:", error_types)

# wait (the default): queue up until a slot frees
report, _, _ = experiment("wait", rows4)
print("wait      :", report.stats["pipelines"]["by_state"], "(4 x 20ms of work on a single slot)")

# least_busy: spread over the resources instead of piling onto the first free one
report, pool, _ = experiment("least_busy", [{"i": i} for i in range(6)],
                             resources=2, concurrency=2)
print("least_busy:", report.stats["pipelines"]["by_state"],
      "leases per resource:", {s["id"]: s["leases"] for s in pool.snapshot()})

# quota_aware: prefer the resource with the most quota left (declared in options, consumed via report)
pool = make_pool("p-quota", "quota_aware", resources=2)
pool.resources()[0].options["quota"] = {"tokens": 1000}
pool.resources()[1].options["quota"] = {"tokens": 100_000}


@task("budgeted", resource="p-quota", algorithm="quota_aware")
async def budgeted(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=True, usage={"tokens": 1000})
        return {"i": row["i"], "resource": lease.resource.id}


with Runner(store="runs/step06_quota.db", pools=[pool], concurrency=1) as runner:
    runner.run(pipeline("t-quota", budgeted).map([{"i": i} for i in range(3)]))
    picked = [row["artifacts"][-1]["payload"]["resource"] for row in runner.store.export_rows()]
print("quota_aware:", picked, "(roomiest first, then the top-up, then the exhausted one again)")

# sticky: a pipeline keeps the resource it already used (prompt caches, warm connections)
pool = make_pool("p-sticky", "sticky", resources=2)


@task("two_calls", resource="p-sticky", algorithm="sticky")
async def two_calls(row: dict, ctx) -> dict:
    async with ctx.acquire() as first:
        first_id = first.resource.id
    async with ctx.acquire() as second:
        return {"first": first_id, "second": second.resource.id, "same": first_id == second.resource.id}


with Runner(store="runs/step06_sticky.db", pools=[pool], concurrency=1) as runner:
    runner.run(pipeline("t-sticky", two_calls).map([{"i": 0}]))
    payload = next(iter(runner.store.export_rows()))["artifacts"][-1]["payload"]
print("sticky    :", payload)
```

```text
immediate : {'failed': 3, 'succeeded': 1} -> errors: ['ResourceUnavailable']
wait      : {'succeeded': 4} (4 x 20ms of work on a single slot)
least_busy: {'succeeded': 6} leases per resource: {'p-least_busy-0': 3, 'p-least_busy-1': 3}
quota_aware: ['p-quota-1', 'p-quota-0', 'p-quota-1'] (roomiest first, then the top-up, then the exhausted one again)
sticky    : {'first': 'p-sticky-0', 'same': True, 'second': 'p-sticky-0'}
```

| Algorithm | Behaviour when nothing is free | Use it when |
|---|---|---|
| `wait` *(default)* | queues until a slot frees or `timeout=` expires | steady-state work, you want throughput over latency |
| `backoff` | waits with exponential backoff + jitter (`base`, `factor`, `cap`, `max_wait`) | a saturated provider, so releases do not cause a thundering herd |
| `least_busy` | picks the lowest load ratio, falls back to waiting | several endpoints of unequal capacity |
| `failover` | tries a list of pools in order, then falls back | primary/backup providers, different keys per tier |
| `sticky` | prefers the resource this attempt already used | prompt/prefix caches, warm connections, sticky sessions |
| `quota_aware` | ranks by remaining *ratio*, then absolute headroom | budget-limited endpoints, `options={"quota": {"tokens": N}}` |
| `immediate` | raises `ResourceUnavailable` right away | you would rather shed load than queue |

Two details the output makes visible:

* `immediate` failed 3 of 4 pipelines. `ResourceUnavailable` is classified as `unknown`, and the default
  retry policy does not retry unknown errors — that is intentional, so a capacity mistake is loud. Set
  `Retrying(max_attempts=3, retry_unknown=True)` if you want capacity retries.
* `quota_aware` picked `p-quota-1` first (both resources were fresh, so the larger absolute headroom won
  the tie-break), then `p-quota-0` (still untouched, therefore the better ratio), then back to
  `p-quota-1` once the 1 000-token resource had been consumed. Quota is a *preference*, never a hard
  stop: refusing to work is worse than overspending.

Acquire-time backoff (`backoff`) and retry-time backoff (`Retrying`) are **different knobs**: the first
decides how long to wait for a slot, the second how long to wait after a failure.

---

## Step 7 — the lease contract

Acquiring and releasing resources is the most critical interaction between your code and the framework, so
this is where the guarantees are hardest.

```python
# tutorial/step_07_lease_safety.py
"""Step 7 - the lease contract: every exit path returns the resource, or the framework reclaims it and says so."""

import asyncio

from pyattacker import Pool, Resource, Retrying, Runner, pipeline, task


def make_pool() -> Pool:
    return Pool("apis", [Resource.create("llm", id="api-1", capacity=1)], algorithm="wait")


def event_kinds(store) -> list[str]:
    return [e.kind for e in store.events(limit=100) if e.kind.startswith(("resource.", "lease."))]


# 1) an exception inside the block
@task("raises_while_holding", resource="apis", retry=Retrying(max_attempts=1))
async def raises_while_holding(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=False, error="boom")
        raise RuntimeError("the request blew up")  # __aexit__ still runs and still returns the lease


pool = make_pool()
with Runner(store="runs/step07a.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("raises", raises_while_holding).map([{"i": 0}]))
    print("1) exception:", report.stats["pipelines"]["by_state"],
          "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active, "ready =", pool.stats().ready)


# 2) the escape hatch, with the release forgotten
@task("forgets_to_release", resource="apis")
async def forgets_to_release(row: dict, ctx) -> dict:
    lease = await ctx.acquire_lease()  # tracked by ctx, but not returned here
    return {"held": lease.resource.id}


pool = make_pool()
with Runner(store="runs/step07b.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("leaky", forgets_to_release).map([{"i": 0}]))
    print("\n2) forgotten release:", report.stats["pipelines"]["by_state"],
          "| leases_leaked =", report.leases_leaked, "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active,
      "(the task still succeeded - a leak is reported, not hidden)")


# 3) the same leak, with strict_leases on
pool = make_pool()
with Runner(store="runs/step07c.db", pools=[pool], concurrency=1, strict_leases=True) as runner:
    report = runner.run(pipeline("leaky", forgets_to_release).map([{"i": 0}]))
    print("\n3) strict_leases:", report.stats["pipelines"]["by_state"],
          "| error:", sorted({e["error_type"] for e in runner.store.errors()}),
          "| leases_leaked =", report.leases_leaked)


# 4) cancellation by timeout_s
@task("too_slow", resource="apis", timeout_s=0.01)
async def too_slow(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=True)
        await asyncio.sleep(5)  # cancelled by timeout_s; the lease is returned on the way out
        return {"never": "reached"}


pool = make_pool()
with Runner(store="runs/step07d.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("slow", too_slow).map([{"i": 0}]))
    attempt = runner.store.attempts()[0]
    print("\n4) timeout_s:", report.stats["pipelines"]["by_state"],
          "| error_class =", attempt.error_class, "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active,
      "(cancellation cannot strand a resource)")
```

```text
1) exception: {'failed': 1} | events: ['resource.leased', 'resource.released']
   pool afterwards: active = 0 ready = 1

2) forgotten release: {'succeeded': 1} | leases_leaked = 1 | events: ['resource.leased', 'lease.leaked', 'resource.leaked']
   pool afterwards: active = 0 (the task still succeeded - a leak is reported, not hidden)

3) strict_leases: {'failed': 1} | error: ['LeaseLeakError'] | leases_leaked = 1

4) timeout_s: {'failed': 1} | error_class = timeout | events: ['resource.leased', 'resource.released']
   pool afterwards: active = 0 (cancellation cannot strand a resource)
```

| Scenario | Guarantee |
|---|---|
| `async with ctx.acquire(...)` exits normally | returned synchronously on exit |
| an exception is raised inside the block | returned anyway; `__aexit__` runs and does not swallow the exception |
| acquire → use → release inside a loop | returned on every iteration, so concurrency is genuinely yielded |
| `timeout_s` expires, the run is cancelled, Ctrl-C | returned in `finally`, synchronously |
| `await ctx.acquire_lease()` and you forget the release | force-reclaimed when the task ends, `lease.leaked` + `resource.leaked` recorded |
| still holding a lease after the task returns | impossible — reclamation runs before the task row is written |

The reason it holds: **reclaim is a pure synchronous function**. `ctx.reclaim_now()` and
`lease.release_now()` contain no `await`, so `CancelledError`, an `asyncio` timeout or any exception
cannot interrupt them. Pool state transitions are synchronous too, so under a single-threaded event loop
there is no "checked-then-preempted" window and no lock is needed.

Practical consequences:

* Use `async with ctx.acquire(...) as lease:` and hold the lease only around the request. Long holds are
  legal but they are what starves the pool.
* A forgotten release is a *bug you are told about*, not a silent capacity loss: `report.leases_leaked`,
  the two events, and `strict_leases=True` when you want it to fail the task instead.
* Holding a resource from a pool while asking the same pool for a second one is a deadlock shape; the pool
  emits `acquire.suspected_deadlock` (after `deadlock_warn_s`, default 5 s) instead of hanging silently.
  Acquire both up front, or use two pools.

---

## Step 8 — failures: classify, retry, record

Failures are ordinary Python exceptions. The framework classifies them with one pure function and then asks
your policy what to do — and every attempt, including every *decision*, lands in the record.

```python
# tutorial/step_08_retry.py
"""Step 8 - failure handling: classify once, retry by policy, keep the decision in the record."""

from pyattacker import (
    FatalError,
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    error_class_of,
    pipeline,
    task,
)


class HttpError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


print("classification is one pure function over any exception:")
for exc in (HttpError(429), HttpError(503), HttpError(404), TimeoutError(),
            ConnectionError("reset"), ValueError("bad json"), Exception("?")):
    print(f"  {type(exc).__name__:<16}{str(exc):<12} -> {error_class_of(exc)}")

CALLS = {"n": 0}


@task("ask", resource="apis", retry=Retrying(max_attempts=4, base=0.01, cap=0.05))
async def ask(row: dict, ctx) -> dict:
    CALLS["n"] += 1
    async with ctx.acquire() as lease:
        if ctx.attempt <= 2:
            lease.report(ok=False, error="429")
            # A retryable error carries its own class, and retry_after is a server-suggested delay.
            raise RetryableError("rate limited", error_class="rate_limit", retry_after=0.01)
        lease.report(ok=True)
        return {"i": row["i"], "attempts": ctx.attempt}


@task("always_fatal", resource="apis", retry=Retrying(max_attempts=5))
def always_fatal(row: dict) -> dict:
    raise FatalError("the request itself is invalid; another attempt cannot help")


fatal_specs = list(pipeline("fatal", always_fatal).map([{"i": 9}]))

with Runner(store="runs/step08.db", pools=[Pool("apis", [Resource.create("llm", id="api-1", capacity=4)])],
            concurrency=2) as runner:
    report = runner.run(pipeline("retrying", ask).map([{"i": 1}]))
    print("\nretried pipeline:", report.stats["pipelines"]["by_state"],
          "| requests made:", CALLS["n"])
    for attempt in runner.store.attempts():
        print(f"  attempt {attempt.attempt_no} {attempt.outcome:<9} class={attempt.error_class or '-':<11} "
              f"delay_s={attempt.retry_delay_s} why={attempt.decision.get('reason')}")

    fatal = runner.run(iter(fatal_specs))
    attempts = runner.store.attempts(pipeline_id=fatal_specs[0].pipeline_id)
    print("fatal pipeline:  ", fatal.stats["pipelines"]["by_state"],
          f"| attempts used: {len(attempts)} of max_attempts=5 ->", attempts[0].error_class)
```

```text
classification is one pure function over any exception:
  HttpError       HTTP 429     -> rate_limit
  HttpError       HTTP 503     -> upstream
  HttpError       HTTP 404     -> fatal
  TimeoutError                 -> timeout
  ConnectionError reset        -> connection
  ValueError      bad json     -> invalid
  Exception       ?            -> unknown

retried pipeline: {'succeeded': 1} | requests made: 3
  attempt 1 failed    class=rate_limit  delay_s=0.01 why=retryable
  attempt 2 failed    class=rate_limit  delay_s=0.01 why=retryable
  attempt 3 succeeded class=-           delay_s=None why=ok
fatal pipeline:   {'failed': 1} | attempts used: 1 of max_attempts=5 -> fatal
```

Classes: `retryable`, `rate_limit`, `timeout`, `connection`, `upstream` (retried by default),
`invalid`, `fatal`, `cancelled`, `unknown` (not retried by default). `error_class_of()` recognises
`TimeoutError`, `ConnectionError`, an `error_class` attribute, and HTTP status codes on
`status` / `status_code` / `http_status` / `code` (falling back to `exc.response.status_code`;
408/504 → timeout, 425/429 → rate_limit, 500/502/503/505/507/529 → upstream, any other 4xx → fatal),
which covers the exception types of the usual
SDKs without importing them.

`Retrying` fields:

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `1` | total attempts, **so the default is "no retries"** — failures are not hidden |
| `on` | `()` | extra exception types that count as retryable (your own classes) |
| `retry_classified` | `True` | retry the classes above |
| `retry_unknown` | `False` | also retry `unknown` (e.g. `ResourceUnavailable`) |
| `base`, `factor`, `cap` | `0.5`, `2.0`, `30.0` | exponential backoff bounds |
| `jitter` | `"full"` | `none` / `full` / `equal` — full jitter is the safe default for many clients |
| `max_total_s` | `None` | give up once attempts + delays exceed this budget |

Ways to steer it:

* `raise RetryableError("rate limited", error_class="rate_limit", retry_after=0.01)` — carry the class, and
  honour a server-suggested delay. `retry_after` is also read from a `Retry-After` header if the exception
  exposes one.
* `raise FatalError(...)` — never retried, whatever `max_attempts` says (see the second pipeline above).
* `with_retry(ask, max_attempts=5)` — adjust one task's policy inside an existing chain.
* Retry backoff does **not** hold a worker: the pipeline is parked in a delay queue and the worker moves on,
  so `concurrency` stays honest. The parked pipeline is still counted as unfinished — a run ending during a
  backoff records it as `interrupted` with its checkpoint intact.

---

## Step 9 — resume: what reruns, and what does not

A run is resumable because the store already contains everything the remaining work needs.

```python
# tutorial/step_09_resume.py
"""Step 9 - resume: a failed pipeline continues at the first task that produced no artifact."""

from pyattacker import RetryableError, Retrying, Runner, pipeline, task

STATE = {"judge_works": False}
SENT = {"ask": 0, "judge": 0}


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"qid": seed["qid"], "prompt": f"explain {seed['qid']}"}


@task("ask", retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
def ask(row: dict) -> dict:
    SENT["ask"] += 1  # <- in real life this is the request you do not want to pay for twice
    return {**row, "answer": "42"}


@task("judge", retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
def judge(row: dict) -> dict:
    SENT["judge"] += 1
    if not STATE["judge_works"]:
        raise RetryableError("the judge model is down", error_class="upstream")
    return {**row, "score": 1}


template = pipeline("eval", prepare | ask | judge)
rows = [{"qid": f"q{i}"} for i in range(3)]
specs = list(template.map(rows))

with Runner(store="runs/step09.db", concurrency=2) as runner:
    first = runner.run(iter(specs))
    print("round 1:", first.stats["pipelines"]["by_state"], "| requests sent:", SENT,
          "| attempts recorded:", len(runner.store.attempts()))
    print("  failed at:", sorted({e["failed_task"] for e in runner.store.errors()}))

    STATE["judge_works"] = True
    second = runner.run(iter(template.map(rows)), resume=True)
    print("round 2:", second.stats["pipelines"]["by_state"], "| requests sent:", SENT,
          "| skipped:", second.skipped)
    print("  -> ask was not re-sent; only the failed task ran again")

    row = next(iter(runner.store.export_rows()))
    print(f"\n  checkpoint: {row['n_tasks_done']}/{row['n_tasks_total']} tasks done, state={row['state']}")
    for attempt in runner.store.attempts(pipeline_id=specs[0].pipeline_id):
        print(f"    {attempt.task_name:<8} attempt {attempt.attempt_no} {attempt.outcome:<9} "
              f"run={attempt.run_id[4:]}")

    seed_artifact = runner.store.get_artifact(specs[0].pipeline_id, -1)
    print("  the seed is a stored artifact too: seq=-1 ->", seed_artifact.type_name,
          seed_artifact.digest[:10])
```

```text
round 1: {'failed': 3} | requests sent: {'ask': 3, 'judge': 6} | attempts recorded: 12
  failed at: ['judge']
round 2: {'succeeded': 3} | requests sent: {'ask': 3, 'judge': 9} | skipped: 0
  -> ask was not re-sent; only the failed task ran again

  checkpoint: 3/3 tasks done, state=succeeded
    prepare  attempt 1 succeeded run=20260915-213904-b464d3
    ask      attempt 1 succeeded run=20260915-213904-b464d3
    judge    attempt 1 failed    run=20260915-213904-b464d3
    judge    attempt 2 failed    run=20260915-213904-b464d3
    judge    attempt 1 succeeded run=20260915-213904-548d63
  the seed is a stored artifact too: seq=-1 -> dict d6125621c2
```

The recovery rules, in order:

1. The pipeline already `succeeded` → skip it entirely (`skipped` counter, no task rows rewritten). Unless
   you pass `retry_succeeded=True` / `--retry-succeeded`, which re-runs it.
2. The pipeline is `failed` or `interrupted` with `n_tasks_done > 0` → load the artifact at
   `n_tasks_done - 1` and continue at the next `seq`. **This is why `ask` sent nothing in round 2**: the
   judge is the first task with no artifact, so only the judge ran.
3. The artifact row is gone, or its payload was not kept (`journal=summary`) → the pipeline restarts
   from `seq=0` and records `pipeline.checkpoint_missing`.
4. The artifact is there but cannot be decoded (a removed codec, a dataclass that changed shape) → the
   same restart, recorded as `pipeline.checkpoint_unusable` so the two causes stay distinguishable.
5. Otherwise the pipeline is new, and `Runner` stores the seed artifact before the first task.

Two consequences worth internalising:

* `skipped=0` in round 2 is correct — nothing had succeeded, three pipelines were *failed* and got
  resumed. `skipped` counts rule 1, not "work that was not repeated".
* The dataset stream's only job at resume time is to *identify* the pipelines (the key comes from the seed
  content); the resumed task's input comes from the store, never from re-reading the row. A run that was
  interrupted mid-pipeline therefore does not depend on the dataset file still being around, only on the
  same seeds being enumerated.
* Every attempt keeps its `run_id`, so the record shows exactly which run did which work — attempt 1 of
  `judge` belongs to the first run, attempt 1 of the resumed `judge` to the second.
* Events to alert on: `pipeline.resumed`, `pipeline.skipped`, `pipeline.checkpoint_missing`,
  `pipeline.checkpoint_unusable`, `pipeline.deferred_interrupted`.

---

## Step 10 — reading the record

Everything the framework knows lives in a handful of tables. Five of them are keyed by `pipeline_id`,
are the ones you will actually query, and are the five exportable row kinds (`--rows`); `runs` (keyed by
`run_id`) and `resources` (keyed by pool + resource id) have Python readers but no export path.

```python
# tutorial/step_10_records.py
"""Step 10 - reading the record: five tables, five row kinds, three formats."""

from contextlib import closing

from pyattacker import ROW_KINDS, Runner, export_store, iter_rows, open_store, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"i": seed["i"], "value": seed["i"] * 2}


@task("check")
def check(row: dict) -> dict:
    if row["i"] % 2:  # deliberately fail the odd rows so the error surface has something to show
        raise ValueError("odd rows are not supported")
    return {"i": row["i"], "ok": True}


STORE = "runs/step10.db"
with Runner(store=STORE, concurrency=2, label="step-10") as runner:
    report = runner.run(pipeline("checked", prepare | check).map([{"i": i} for i in range(4)]))

    print(report.summary())
    print("\nto_dict() gives you the machine-readable version of the same facts:")
    print(" ", {k: v for k, v in report.to_dict().items() if k != "pipelines"})

    print("\nlive counters (safe to call while a run is in flight):")
    live = runner.stats()
    print(" ", {k: live[k] for k in ("in_flight_pipelines", "delayed_pipelines", "buffered")})
    print("  counters:", dict(live["counters"]))

    print("\nerrors():")
    for item in runner.store.errors():
        print(f"  {item['name']}/{item['failed_task']}: {item['error_type']}: {item['error_message']}")

    row = next(item for item in runner.store.export_rows() if item["state"] == "failed")
    print(f"\none failed pipeline {row['pipeline_id'][:12]}:")
    for item in row["tasks"]:
        print(f"  seq={item['seq']} {item['name']:<8} {item['state']:<9} "
              f"attempts={item['attempts_used']} error={item['error_class']}")
    for attempt in runner.store.attempts(pipeline_id=row["pipeline_id"]):
        print(f"  attempt {attempt.attempt_no} of {attempt.task_name}: outcome={attempt.outcome} "
              f"decision={attempt.decision.get('reason')} leases={len(attempt.leases)} "
              f"duration_ms={attempt.duration_ms}")
    print("  events:", [e.kind for e in runner.store.events(pipeline_id=row["pipeline_id"])][:8])

    print("\nrow kinds:")
    for kind in ROW_KINDS:
        print(f"  {kind:<10} {sum(1 for _ in iter_rows(runner.store, kind=kind))} rows")

    count = export_store(runner.store, "runs/step10_attempts.csv", kind="attempts", fmt="csv")
    print(f"\nexported {count} attempt rows as CSV")

# The store is a plain SQLite file: reopen it from anywhere, any process.
with closing(open_store(STORE)) as reopened:
    counts = reopened.stats()
    print("reopened:", counts["pipelines"]["total"], "pipelines,",
          counts["attempts_total"], "attempts,", counts["events_total"], "events")
```

```text
run run-... status=completed  wall=0.00s
  pipelines: total=4 failed=2 succeeded=2
  attempts: total=8
  tasks: check=4 prepare=4
  errors (2):
    - checked/check: ValueError: odd rows are not supported

to_dict() gives you the machine-readable version of the same facts:
  {'run_id': 'run-...', 'status': 'completed', 'duration_ms': 3.973, 'skipped': 0, 'leases_leaked': 0,
   'stop_reason': None, 'tasks': {'by_name': {'check': 4, 'prepare': 4}, ...}, 'attempts_total': 8, 'events_total': 13}

live counters (safe to call while a run is in flight):
  {'in_flight_pipelines': 0, 'delayed_pipelines': 0, 'buffered': {'pending': 0, 'flushes': 5, ...}}
  counters: {'pipelines_admitted': 4, 'pipelines_succeeded': 2, 'pipelines_done': 4, 'pipelines_failed': 2}

errors():
  checked/check: ValueError: odd rows are not supported

one failed pipeline f41b0b0541a2:
  seq=0 prepare  succeeded attempts=1 error=None
  seq=1 check    failed    attempts=1 error=invalid
  attempt 1 of prepare: outcome=succeeded decision=ok leases=0 duration_ms=0.004
  attempt 1 of check: outcome=failed decision=attempts_exhausted leases=0 duration_ms=0.004
  events: ['task.succeeded', 'task.failed', 'pipeline.failed']

row kinds:
  pipelines  4 rows
  tasks      8 rows
  attempts   8 rows
  events     13 rows
  artifacts  10 rows

exported 8 attempt rows as CSV
reopened: 4 pipelines, 8 attempts, 13 events
```

| Table | One row per | Contains | Reader |
|---|---|---|---|
| `runs` | run | label, status, heartbeat, config snapshot, version, host | `store.get_run(id)`, `store.stats()` |
| `pipelines` | pipeline | state, checkpoint (`n_tasks_done/…`), key, seed/spec digests, tags, failure, resume chain | `store.pipelines(...)`, `store.export_rows()` |
| `tasks` | task of a pipeline | final state, attempts used, duration, artifact ids, error class | `store.tasks(...)`, `iter_rows(store, kind="tasks")` |
| `attempts` | attempt | outcome, error class/type/traceback, **retry decision**, leases used, duration | `store.attempts(...)`, `kind="attempts"` |
| `events` | event | the structured stream: every task/pipeline/resource transition | `store.events(...)`, `kind="events"` |
| `artifacts` | artifact | payload, digest, codec, type, `blob_ref` | `store.artifacts(pid)`, `kind="artifacts"` |

* `report.summary()` is for humans, `report.to_dict()` for dashboards, `runner.stats()` for a live view
  (it is safe to call mid-run), and `store.errors()` for the failure list.
* `attempts` is the table that answers "why did this take 40 seconds": it has each attempt's duration, its
  lease log, and the decision (`retry`, `reason`, `delay_s`, `error_class`).
* Export shapes: `--rows pipelines|tasks|attempts|events|artifacts` × `--format jsonl|json|csv`. `csv`
  flattens nested values into compact JSON, so it opens cleanly in a spreadsheet.
* Writes are batched for the append-only tables (attempts, events) and synchronous for state (artifacts,
  checkpoints): `SIGKILL` can cost you the last batch of history, never a checkpoint. `--no-write-behind`
  commits every event immediately.

---

## Step 11 — the declarative path and the CLI

Composition and resources can live in YAML; the logic stays in Python. The declarative layer does exactly
three things: pick tasks, chain them, configure pools.

```python
# tutorial/step_11_declarative.py
"""Step 11 - the declarative path: composition and resources in YAML, logic still in Python."""

import json
from pathlib import Path

from pyattacker import Runner, load_spec

CONFIG = """
run:      { store: runs/step11.db, concurrency: 4, label: declarative }
pools:
  apis:
    kind: llm
    algorithm: backoff        # back off instead of failing when every slot is busy
    capacity: 2               # pool-level default, overridable per resource
    degrade_after: 3          # 3 consecutive failures -> circuit-break for cooldown_s
    cooldown_s: 10
    resources:
      - { id: api-1, options: { model: gpt-4o, base_url: "https://api-a.example/v1",
                                api_key: "${OPENAI_KEY:-sk-demo}" } }
      - { id: api-2, options: { model: gpt-4o } }
pipeline:
  name: qa_eval
  tags: { bench: tutorial }
  tasks:
    - use: pyattacker.tasks:echo                 # replace with "your.pkg.tasks:fetch"
    - use: pyattacker.tasks:simulate_llm         # replace with "your.pkg.tasks:ask_model"
      resource: apis
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.0, tokens: 64, model: gpt-4o }
      retry: { max_attempts: 3, base: 0.01, cap: 0.05, "on": [RetryableError, TimeoutError] }
source: { kind: range, n: 8 }
"""

Path("qa.yaml").write_text(CONFIG, encoding="utf-8")
spec = load_spec("qa.yaml")  # <- builds the pools + the pipeline template, imports nothing of yours yet

print(json.dumps(spec.describe(), indent=2))
print("unresolved ${ENV} references:", spec.unresolved_env or "none")

with Runner(pools=spec.pools, **spec.run) as runner:
    report = runner.run(spec.pipelines())
    print(report.summary())
```

```text
{
  "config": "qa.yaml",
  "pipeline": {"name": "qa_eval", "tasks": ["mock.echo", "mock.llm"],
               "tags": {"bench": "tutorial"}, "spec_digest": "ad271b5b5b00fcb2..."},
  "pools": {"apis": {"kind": "llm", "resources": 2, "capacity": 4, "algorithm": "backoff"}},
  "run": {"store": "runs/step11.db", "concurrency": 4, "label": "declarative"},
  "source": {"kind": "range", "n": 8},
  "unresolved_env": []
}
unresolved ${ENV} references: none
run run-... status=completed  wall=0.01s
  pipelines: total=8 succeeded=8
  tasks: mock.echo=8 mock.llm=8
```

The same config from the command line:

```bash
uv run pyattacker validate -c qa.yaml            # parse + summarise, run nothing (exit 2 on a config error)
uv run pyattacker run      -c qa.yaml --progress # run; --limit N for a slice
uv run pyattacker resume   -c qa.yaml            # same as run --resume
uv run pyattacker report   runs/step11.db --errors 20
uv run pyattacker watch    runs/step11.db        # live view from a second process
uv run pyattacker export   runs/step11.db out.jsonl --rows attempts --format csv
uv run pyattacker serve    runs/step11.db        # read-only HTTP debug endpoint (Step 14)
uv run pyattacker plugins                        # entry points, and which ones failed to load
```

Config sections:

| Section | Keys |
|---|---|
| `run` | `store`, `concurrency`, `journal` (`full` keeps payloads, `summary` keeps only digests), `label`, `strict_leases`, `stop_after_failures`, `stop_after_s`, `retry_succeeded`, `heartbeat_s`, `grace_s`, `stale_after_s`, `notes` |
| `pools.<name>` | `kind`, `algorithm`, `capacity` (default for its resources), `degrade_after`, `dead_after`, `cooldown_s`, `deadlock_warn_s`, `resources: [{id, kind, capacity, options, tags}]` |
| `pipeline` | `name`, `tags`, `include_code`, and `tasks: [{use, name, resource, algorithm, timeout_s, retry, args, kwargs}]` |
| `source` | `kind: range` (`n`) or `kind: jsonl` (`path`, `limit`), plus `repeats`, `key_field` |

Details that save time:

* `use:` is resolved by shape, not by one global list: a name containing a colon is
  `module:attribute` and is imported directly; a bare name is looked up in the built-ins first and then
  in the installed plugins, so a plugin can never shadow `echo`. `pyattacker.tasks:simulate_llm` and
  `your_pkg.tasks:ask_model` are both valid (no plugin needed); a factory (a callable returning a
  `TaskSpec`) is called with `args`/`kwargs`.
* `${VAR}` and `${VAR:-default}` are expanded in every string *value* of the file (keys are left alone);
  unresolved names are reported by `validate` and by `describe()["unresolved_env"]`.
* **YAML 1.1 pitfall:** a bare `on:` key parses as boolean `true`. Write `"on": [RetryableError, TimeoutError]`
  — the loader detects the mistake and says so.
* Exit codes: `0` all pipelines succeeded, `1` some failed, `2` config error, `130` interrupted.

---

## Step 12 — sharding and merging

SQLite takes one writer and the kernel is a single event loop, so scaling out means **processes with their
own stores**, joined afterwards. A pipeline's shard comes from its content-addressed key, so the same
dataset always splits the same way.

```python
# tutorial/step_12_shards.py
"""Step 12 - scale out: deterministic shards with their own stores, merged into one answer."""

from pyattacker import (
    Runner,
    merge_reports,
    pipeline,
    shard_index,
    shard_specs,
    shard_store_path,
    task,
)


@task("ask")
def ask(seed: dict) -> dict:
    return {"i": seed["i"], "answer": seed["i"] * 3}


template = pipeline("eval", ask, tags={"example": "step-12"})
specs = list(template.map([{"i": i} for i in range(12)]))
SHARDS = 3
BASE = "runs/step12.db"

paths = []
for index in range(SHARDS):
    mine = list(shard_specs(specs, index, SHARDS))  # blake2b(pipeline_key) % SHARDS == index
    path = shard_store_path(BASE, index, SHARDS)  # runs/step12.shard0of3.db ...
    with Runner(store=path, concurrency=2, label=f"shard-{index}") as runner:
        report = runner.run(iter(mine))
    paths.append(path)
    print(f"shard {index}: {len(mine):>2} pipelines -> {report.stats['pipelines']['by_state']} -> {path}")

merged = merge_reports(paths)
print("\n" + merged.summary())
print("pipeline rows:", len(merged.rows), "| duplicates dropped:", merged.duplicates,
      "| sources:", len(merged.sources))

# Merging is idempotent: a duplicated store is de-duplicated by pipeline_id, best state wins.
again = merge_reports([*paths, paths[0]])
print("with one store counted twice:", len(again.rows), "rows,", again.duplicates, "duplicates dropped")

merged.export("runs/step12-all.jsonl")
with open("runs/step12-all.jsonl", encoding="utf-8") as handle:
    print("merged export rows:", sum(1 for _ in handle))

# Sharding is content-addressed, not round-robin: re-running the same seeds replays the same split.
print("shard of the first pipeline:", shard_index(specs[0].pipeline_id, SHARDS), "of", SHARDS)
```

```text
shard 0:  6 pipelines -> {'succeeded': 6} -> runs/step12.shard0of3.db
shard 1:  2 pipelines -> {'succeeded': 2} -> runs/step12.shard1of3.db
shard 2:  4 pipelines -> {'succeeded': 4} -> runs/step12.shard2of3.db

merged 12 pipelines from 3 store(s)
  succeeded=12  attempts=12 events=27
  tasks: ask=12
pipeline rows: 12 | duplicates dropped: 0 | sources: 3
with one store counted twice: 12 rows, 6 duplicates dropped
merged export rows: 12
shard of the first pipeline: 2 of 3
```

```bash
# N children, one per shard, then a merged report (results land in runs/qa.shard0of4.db …)
uv run pyattacker run -c examples/qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db

# or drive each shard yourself — cluster, job scheduler, four terminals
uv run pyattacker run    -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
uv run pyattacker resume -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db

# one coherent answer out of N files
uv run pyattacker report runs/qa.shard*of4.db
uv run pyattacker export runs/qa.shard*of4.db runs/all.jsonl
```

* `shard_index(key, N) = int(blake2b(key, digest_size=16).hexdigest()[:16], 16) % N` — a real hash, not
  Python's `hash()` (which is salted per process), so it is stable across runs and machines. `--shard 2/4
  --resume` therefore puts every pipeline back where it was, and shard sizes are *roughly* equal (6/2/4
  above is normal for 12 pipelines).
* Merging de-duplicates by `pipeline_id` and keeps the best state (succeeded > failed > interrupted, then
  the latest finish), then recomputes the statistics from the merged rows — so a partially-overlapping
  re-run, or a store accidentally counted twice, still produces one answer.
* Merge is a library call too: `merge_reports([...])` returns a `MergedReport` with `.summary()`,
  `.stats()`, `.errors()` and `.export(path, fmt=..., kind=...)`.

---

## Step 13 — capstone: a small model evaluation

This is the shape of a real evaluation: prepare → a two-turn model call → three judges → reduce, with two
model pools, retries, a provider outage, and a resume whose cost we actually measure.

```python
# tutorial/step_13_capstone.py
"""Step 13 - capstone: a small model evaluation, with the price of its checkpoint granularity measured."""

import asyncio

from pyattacker import (
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    fanout,
    pipeline,
    task,
)


class RequestLog:
    """Counts what actually left the process - the number this whole exercise is about."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def note(self, model: str) -> None:
        self.sent.append(model)

    def counts(self, since: int = 0) -> dict[str, int]:
        out: dict[str, int] = {}
        for model in self.sent[since:]:
            out[model] = out.get(model, 0) + 1
        return out


LOG = RequestLog()
DOWN: set[str] = {"judge-b"}  # the provider having a bad day; cleared before the resume round


class Backend:
    """Your client. The framework never opens a socket; this class is where the network lives."""

    def __init__(self, log: RequestLog, model: str) -> None:
        self.log = log
        self.model = model

    async def chat(self, prompt: str) -> str:
        self.log.note(self.model)
        await asyncio.sleep(0.002)  # <- the HTTP request
        if self.model in DOWN:
            raise RetryableError(f"{self.model} is unavailable", error_class="upstream")
        return f"[{self.model}] {prompt[:40]}"


def backend_factory(resource: Resource) -> Backend:
    return Backend(LOG, resource.options["model"])


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": seed["question"]}


@task("ask", resource="models", timeout_s=10)
async def ask(row: dict, ctx) -> dict:
    """One task = one step of the pipeline, but it may make as many requests as it needs."""
    messages = [f"Q: {row['question']}"]
    async with ctx.acquire(model="eval-model") as lease:
        for _ in range(2):  # a two-turn conversation, inside a single checkpoint
            messages.append(await lease.client.chat(" | ".join(messages)))
        lease.report(ok=True, usage={"tokens": 64})
    return {**row, "transcript": messages}


def judge(model: str):
    @task(f"judge.{model}", resource="judges", algorithm="least_busy",
          retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
    async def judge_with(row: dict, ctx) -> dict:
        async with ctx.acquire(model=model) as lease:
            verdict = await lease.client.chat(row["transcript"][-1])
            lease.report(ok=True, usage={"tokens": 8})
            return {**row, "judge": model, "verdict": verdict}

    return judge_with


judges = fanout(judge("judge-a"), judge("judge-b"), judge("judge-c"), name="judges")


@task("reduce")
def reduce_scores(row: dict) -> dict:
    """Aggregation across pipelines stays yours to write - the framework stores it, it does not judge it."""
    branches = list(row.values())
    return {
        "qid": branches[0]["qid"],
        "verdicts": {b["judge"]: b["verdict"] for b in branches},
        "n_models": len(branches),
    }


def make_pools() -> list[Pool]:
    return [
        Pool("models", [Resource.create("llm", id="m-1", capacity=4,
                                        options={"model": "eval-model"}, factory=backend_factory)]),
        Pool("judges", [Resource.create("llm", id=f"j-{name}", capacity=1,
                                        options={"model": name}, factory=backend_factory)
                        for name in ("judge-a", "judge-b", "judge-c")]),
    ]


template = pipeline("eval", prepare | ask | judges | reduce_scores, tags={"bench": "tutorial"})
rows = [{"qid": "q1", "question": "2 + 2 = ?"}, {"qid": "q2", "question": "3 + 4 = ?"}]

with Runner(store="runs/step13.db", pools=make_pools(), concurrency=4) as runner:
    first = runner.run(template.map(rows))
    print("round 1:", first.stats["pipelines"]["by_state"],
          "| requests:", LOG.counts(),
          "| failed at:", sorted({e["failed_task"] for e in runner.store.errors()}))

    DOWN.clear()
    mark = len(LOG.sent)
    second = runner.run(template.map(rows), resume=True)
    print("round 2:", second.stats["pipelines"]["by_state"],
          "| requests:", LOG.counts(mark), "| skipped:", second.skipped)
    print("total requests:", len(LOG.sent))

    row = next(iter(runner.store.export_rows()))
    print("\nfinal artifact:", row["artifacts"][-1]["payload"])

# The resume was honest but not free: judge-a and judge-c were re-sent even though their verdicts
# were already on disk. Putting the three judges in three *tasks* (C1 | C2 | C3) makes the
# checkpoint finer: the same failure would resume at C2, and re-send nothing that succeeded.
# examples/llm_eval/ measures both shapes side by side.
print("\nthe grouped shape re-sent verdicts that were already persisted:",
      sum(LOG.counts(mark).values()) - 2, "of", len(LOG.sent) - mark, "requests in round 2")
```

```text
round 1: {'failed': 2} | requests: {'eval-model': 4, 'judge-a': 4, 'judge-b': 4, 'judge-c': 4} | failed at: ['judges']
round 2: {'succeeded': 2} | requests: {'judge-a': 2, 'judge-b': 2, 'judge-c': 2} | skipped: 0
total requests: 22

final artifact: {'n_models': 3, 'qid': 'q1',
                 'verdicts': {'judge-a': '...', 'judge-b': '...', 'judge-c': '...'}}

the grouped shape re-sent verdicts that were already persisted: 4 of 6 requests in round 2
```

What each piece demonstrates:

* **`ask` makes two requests inside one task.** The task model is unary, so a multi-turn conversation, a
  tool loop or a "retry the parse until it validates" loop all belong *inside* one task. The checkpoint
  grain then matches your mental model: one step, one artifact.
* **`fanout(...)` keeps the branch inside a task.** Three judges run concurrently on the same input and
  return `{task_name: value}`. Children share the parent's context, so their leases and events stay in one
  coherent record. The Runner only ever sees the *group* spec, so the fan-out lifts what the children agree
  on (`resource`, `algorithm`, `timeout_s`, and the most forgiving retry policy) onto it — here that is the
  `judges` pool and `least_busy`, and it is why the declaration has to be identical on all three children.
* **Pools per role.** `models` has one resource with capacity 4; `judges` has one resource per model with
  capacity 1, so the three branches never queue behind each other.
* **The outage is data.** `judge-b`'s `RetryableError(error_class="upstream")` is classified, retried by
  the group's policy, and ends as a recorded failure with the failed task named — not as a stack trace in
  a log file.
* **The resume is honest about its cost.** Round 2 re-ran the *whole* judges group, so `judge-a` and
  `judge-c` were sent again even though their verdicts were already on disk. Four of the six requests in
  round 2 were repeats. That is the price of putting three requests in one checkpoint.

The alternative shape — C1 → C2 → C3 as three tasks instead of one fan-out — makes the checkpoint finer:
the same failure resumes at C2, re-sends nothing that succeeded, and costs two extra pipeline steps.
[`examples/llm_eval/`](../examples/llm_eval/README.md) implements both shapes over the same tasks and
measures them: with one judge endpoint down, the grouped shape re-sent 2 judge requests that had already
succeeded, and the split shape re-sent 0. The rule of thumb that falls out of it:

> If a request is expensive or slow, give it its own task. If a step is a fan-out of cheap calls, one task
> and one checkpoint is the better trade.

---

## Step 14 — your own types, blobs, plugins

Three extension points cover almost everything: codecs for your types, artifact backends for where payloads
live, and entry-point plugins for making your code addressable from a config file.

```python
# tutorial/step_14_extending.py
"""Step 14 - your own types and your own blobs: a codec, a size comparison, and an artifact backend."""

import json
import struct
from pathlib import Path

from pyattacker import CodecRegistry, FileBackend, Runner, list_plugins, pipeline, task


class Embedding:
    """A type the built-in JSON codec would store as a fat list of floats."""

    def __init__(self, values: list[float]) -> None:
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    def __repr__(self) -> str:
        return f"Embedding({len(self.values)} floats)"


class EmbeddingCodec:
    """Four methods: a name, a claim on a type, and an encode/decode pair."""

    name = "f32"

    def can_encode(self, obj: object) -> bool:
        return isinstance(obj, Embedding)

    def dumps(self, obj: Embedding) -> bytes:
        return struct.pack(f"<{len(obj.values)}f", *obj.values)

    def loads(self, data: bytes) -> Embedding:
        return Embedding(list(struct.unpack(f"<{len(data) // 4}f", data)))


# A private registry: the same object must be given to the pipeline (for seeds) and to the Runner.
registry = CodecRegistry()
registry.register(EmbeddingCodec(), for_types=(Embedding,))


@task("embed")
def embed(seed: dict) -> Embedding:
    return Embedding([float(i) for i in range(seed["dim"])])


@task("norm")
def norm(vec: Embedding) -> dict:
    return {"dim": len(vec), "norm": round(sum(v * v for v in vec.values) ** 0.5, 3)}


template = pipeline("embedding", embed | norm, registry=registry)

with Runner(store="runs/step14.db", pools=[], concurrency=2, registry=registry,
            # Spill payloads of any size to content-addressed files instead of the database.
            artifact_backend=FileBackend(root="runs/blobs", min_bytes=0)) as runner:
    report = runner.run(template.map([{"dim": 384}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    for item in row["artifacts"]:
        shown = str(item["payload"])[:36]
        print(f"  seq={item['seq']:>2} {item['task']:<10} type={item['type']:<10} codec={item['codec']:<5} "
              f"payload={shown + '...' if len(shown) == 36 else shown}")

    pipeline_id = row["pipeline_id"]
    vector = runner.store.get_artifact(pipeline_id, 0)
    print("built-in json would need", len(json.dumps([float(i) for i in range(384)])), "bytes;",
          "the f32 codec used", vector.size, "bytes")
    print("the store kept only a reference:", f"blobs/{vector.digest[:2]}/{vector.digest[2:]}",
          "- the payload is not in the database")
    print("restored from the blob:", type(registry.load(vector.encoded())).__name__,
          registry.load(vector.encoded()))

files = [p for p in sorted(Path("runs/blobs").rglob("*")) if p.is_file()]
print("blobs on disk:", len(files), "files,", sorted(p.stat().st_size for p in files), "bytes")
print("installed plugins:", len(list_plugins()),
      "(entry points; see examples/plugin_package for a complete one)")
```

```text
run run-... status=completed  wall=0.01s
  pipelines: total=1 succeeded=1
  ...
  seq=-1 __seed__   type=dict       codec=json  payload={'dim': 384}
  seq= 0 embed      type=Embedding  codec=f32   payload=AAAAAAAAgD8AAABAAABAQAAAgEAAAKBAAADA...
  seq= 1 norm       type=dict       codec=json  payload={'dim': 384, 'norm': 4335.978}
built-in json would need 2578 bytes; the f32 codec used 1536 bytes
the store kept only a reference: blobs/15/0b9b3a521bb02c0f88eab2716c9d61 - the payload is not in the database
restored from the blob: Embedding Embedding(384 floats)
blobs on disk: 3 files, [11, 27, 1536] bytes
```

* **Codecs** need four members: `name`, `can_encode(obj)`, `dumps(obj) -> bytes`, `loads(bytes) -> obj`.
  Register with `registry.register(codec, for_types=(MyType,))`, and pass the *same* registry to
  `pipeline(..., registry=...)` (seeds) and to `Runner(registry=...)` (artifacts). Later registrations win,
  so a specialised codec always beats the built-in JSON catch-all.
* **Artifact backends** decide where payloads live: `None`/`"inline"` (in the database), `"null"` (drop
  bytes, keep digests), or `"file:///data/blobs"` / `{"kind": "file", "root": ..., "min_bytes": 262144}`.
  Files are content-addressed, written atomically and re-hydrated on read, so a resumed run reuses spilled
  checkpoints transparently. In the example `min_bytes=0` spills everything, which is why the artifact row
  above shows a reference instead of bytes.
* **Plugins** are ordinary `importlib.metadata` entry points — install a package and its names become
  usable from any config: `pyattacker.tasks`, `pyattacker.algorithms`, `pyattacker.codecs`,
  `pyattacker.stores` (keyed by URI scheme, so `store = "s3://bucket/runs.db"` works). Built-ins resolve
  first, and a plugin that raises on import is *recorded* rather than fatal —
  `pyattacker plugins` shows both. A complete worked package is
  [`examples/plugin_package/`](../examples/plugin_package/README.md).
* **Monitoring**: `pyattacker serve runs/qa.db` is a zero-dependency read-only HTTP view (`/`,
  `/stats`, `/events`, `/pipelines`, `/resources`, `/errors`) that opens a fresh connection per request,
  so it runs happily beside a live run. It binds to loopback and has no authentication — treat it as a
  debug view, not a dashboard.

---

## Cheat sheet

| I want to… | Do this |
|---|---|
| run one task over a dataset | `Runner(store=..., pools=[...]).run(template.map(rows))` |
| k samples per row | `template.map(rows, repeats=k)` |
| stable ids from my dataset | `template.map(rows, key_of=lambda r: r["qid"])` |
| test without touching disk | `Runner(store=":memory:")` |
| see what happened | `report.summary()`, `report.to_dict()`, `store.errors()` |
| see why it was slow | `store.attempts(pipeline_id=...)` → `duration_ms`, `decision`, `leases` |
| resume after a crash | `runner.run(specs, resume=True)` or `pyattacker resume -c cfg.yaml` |
| re-run results I do not trust | `retry_succeeded=True` / `--retry-succeeded` |
| bound the blast radius | `stop_after_failures=N`, `stop_after_s=T`, `--limit N` |
| cap concurrency per endpoint | `Resource.create(..., capacity=N)` |
| fail fast instead of queueing | `algorithm="immediate"` + `Retrying(retry_unknown=True)` |
| survive a 429 | `raise RetryableError(..., error_class="rate_limit", retry_after=...)` |
| store big payloads out of the DB | `--artifact-backend file:///data/blobs` |
| use four processes | `--shards 4 --jobs 4`, then `report`/`export` over the shard files |
| branch inside a step | `fanout(task_a, task_b)` |
| make my code usable from YAML | no plugin: `use: my_pkg.tasks:my_task`; with an entry point in `pyattacker.tasks`: `use: my_task` |

The six invariants everything else is built on — pipelines share nothing but the resource pools; a task is
a unary `(artifact) -> artifact` with no branching or joining; artifacts are persisted as soon as they are
produced; failure is an exception, not a state machine; a resource is only usable through a lease that must
be returned; the store writes facts, never metrics — are stated and justified in
[`docs/design.md`](design.md) §2.

## Troubleshooting

**"My task takes three arguments."** Tasks are unary: `(value)` or `(value, ctx)`. Put extra state in a
factory closure (`def make_task(model): @task(...) async def t(value, ctx): ...; return t`) — this is the
same pattern Step 6 uses.

**"Resume re-ran the whole pipeline."** The store was almost certainly written with `journal: summary`,
which keeps digests but no payloads, so the checkpoint cannot be decoded. Look for a
`pipeline.checkpoint_missing` event. Use `journal: full` (the default).

**"Nothing ran, there are no rows."** Every pipeline was skipped because it had already succeeded — see
`report.skipped`. That is the intended behaviour, including for a second identical run.

**"A pipeline failed with `unknown`."** The exception was not classifiable (for example
`ResourceUnavailable`, or a bare `Exception`). Either map it — `raise RetryableError(...)` — or set
`retry_unknown=True` if you really want to retry anything.

**"It hangs with no output."** A selector matches no resource and the algorithm is `wait`; or a task holds
a lease from a pool while asking the same pool for another (look for an `acquire.suspected_deadlock`
event); or an `await` in your own code never returns. `timeout_s=` on the task and `timeout=` on the
acquire turn both into errors.

**"My resource never comes back."** It does — check `leases_leaked` and the `lease.leaked` event: you used
`await ctx.acquire_lease()` without releasing. Use `async with`, and set `strict_leases=True` in CI so this
fails loudly.

**"The store file is busy / one process is not enough."** SQLite allows one writer. Shard into several
processes with their own stores (Step 12) instead of pointing several runs at one file.

**"Where did my artifact bytes go?"** If `journal: summary` or a `null` backend is set, only digests are
kept. Otherwise check `blob_ref`: the payload may be in a file backend, which is transparent on read.

**"A run took 30 s in a retry backoff and I lost concurrency."** You did not: the pipeline is parked in the
delay queue and the worker took other work. `runner.stats()["delayed_pipelines"]` shows how many are
parked right now.

## Where to look next

Looking for a specific feature rather than a whole document? See
[Look up by feature, not just by step](#look-up-by-feature-not-just-by-step) near the top.

| Resource | What is in it |
|---|---|
| [`docs/design.md`](design.md) | conceptual model, six invariants, lease contract, data model, tradeoffs, milestones |
| [`README.md`](../README.md) | the compact tour: scheduling guarantees, sharding, plugins, out of scope |
| [`examples/quickstart.py`](../examples/quickstart.py) | the SDK in 60 lines, with a resume round |
| [`examples/llm_eval/`](../examples/llm_eval/README.md) | the full evaluation, two pipeline shapes, measured checkpoint granularity |
| [`examples/sharded.py`](../examples/sharded.py) | one dataset across N stores, then a merged report |
| [`examples/plugin_package/`](../examples/plugin_package/README.md) | an installable plugin: tasks, an algorithm, a codec |
| [`examples/qa_eval.yaml`](../examples/qa_eval.yaml) | the declarative path, end to end |
