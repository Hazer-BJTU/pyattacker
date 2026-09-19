# Tutorial: from one task to a resumable model evaluation

**English** | [简体中文](zh-CN/tutorial.md)

A step-by-step guide to using pyattacker, from a five-line program to a resumable, sharded model evaluation.

**Who it is for:** you run LLM or agent evaluations in Python, you know `asyncio`, and you want the
infrastructure part — endpoint pools, concurrency, retries, "which rows already ran", a record of every
request.

**How to read it:**

* Every step is a complete program. Run it, read the output, then read the notes.
* Code blocks starting with `# tutorial/step_NN_....py` are extracted from this file and executed by
  [`tests/test_tutorial.py`](../tests/test_tutorial.py) on every test run. Save one as the file named in the
  comment and run it directly.
* Nothing here touches the network. Where a real program would `await self.http.post(...)`, these examples
  `await asyncio.sleep(...)` — the comment `# <- your HTTP call` marks the spot.

**Other documents:** [`docs/reference.md`](reference.md) documents every class and function;
[`docs/cli.md`](cli.md) covers the command line; [`docs/design.md`](design.md) explains why the framework
is shaped this way; [`examples/`](../examples) holds complete programs.

## Find what you need

Read the steps in order the first time. Afterwards, use this table.

| I want to… | Step | Reference |
|---|---|---|
| write my first task and run it | [Step 1](#step-1--one-seed-one-task-one-run) | [Tasks](reference.md#tasks) |
| run a whole dataset, or k samples per row | [Step 2](#step-2--many-inputs-at-once) | [`map`](reference.md#map) |
| chain several steps together | [Step 3](#step-3--chaining-tasks) | [Pipelines](reference.md#pipelines) |
| understand what gets saved, and when | [Step 4](#step-4--the-checkpoint-is-the-artifact) | [Artifacts](reference.md#artifacts-and-codecs) |
| spread load over several endpoints or keys | [Step 5](#step-5--endpoints-as-a-pool) | [Resources](reference.md#resources) |
| choose how to wait when everything is busy | [Step 6](#step-6--choosing-how-to-wait) | [Algorithms](reference.md#acquire-algorithms) |
| use a resource safely | [Step 7](#step-7--the-lease-contract) | [`Lease`](reference.md#lease) |
| retry failures, and see why something gave up | [Step 8](#step-8--failures-classify-retry-record) | [`Retrying`](reference.md#retrying), [Errors](reference.md#errors) |
| pick up after a crash without re-paying for work | [Step 9](#step-9--resume-what-reruns-and-what-does-not) | [`Runner`](reference.md#runner) |
| query the record, export results | [Step 10](#step-10--reading-the-record) | [Stores](reference.md#stores), [Export](reference.md#export) |
| drive it from YAML and the CLI | [Step 11](#step-11--the-declarative-path-and-the-cli) | [`docs/cli.md`](cli.md) |
| use more than one process | [Step 12](#step-12--sharding-and-merging) | [Sharding](reference.md#sharding-and-merging) |
| branch inside a step | [Step 13](#step-13--capstone-a-small-model-evaluation) | [`fanout`](reference.md#fanout) |
| store custom types or large payloads, ship a plugin | [Step 14](#step-14--your-own-types-blobs-plugins) | [Codecs](reference.md#codecregistry), [Backends](reference.md#artifact-backends), [Plugins](reference.md#plugins) |
| monitor a run in progress | [Step 14](#step-14--your-own-types-blobs-plugins) | [Monitoring](reference.md#monitoring) |
| skip the rest of a chain from inside a task (advanced) | [Step 15](#step-15--advanced-skipping-stations-handoffs) | [Handoffs](reference.md#advanced-handoffs-opt-in) |
| send work back to an earlier station (advanced) | [Step 16](#step-16--advanced-regenerating-with-rewind-and-retry-all) | [Backward traversal](reference.md#advanced-backward-traversal-rewind-retry-all-visits) |
| snapshot and restore state inside a payload (advanced) | [Step 17](#step-17--advanced-payloads-that-carry-their-own-history) | [`HistoryArtifact`](reference.md#historyartifact) |

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

Requirements: Python 3.11+ and nothing else — the base install has no dependencies. Step 11 reads a YAML
config, the one thing that needs the optional extra (`uv add "pyattacker[yaml]"`); every other step installs
nothing.

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

Five concepts, and they are the whole vocabulary:

| Concept | In this program | What it is |
|---|---|---|
| **artifact** | `{"n": 21, "doubled": 42}` | the persisted output of one task, written as soon as it is produced |
| **task** | `double` | a unary function `(artifact) -> artifact`; sync or async, no base class |
| **pipeline** | `pipeline("doubling", double)` | a linear chain of tasks; the unit of completion and resume |
| **seed** | `[{"n": 21}]` | one dataset row; `template.map(seeds)` makes one pipeline per row |
| **runner** | `Runner(store=..., concurrency=2)` | the scheduler: owns the store, the pools and the worker slots |

Three things to know now:

* A task takes **one** parameter, or **two** (`value, ctx`) when it needs the framework context. Anything else
  raises `ConfigError` at decoration time. Put extra state in a closure.
* `Runner` is a context manager, and closing it closes the store. Read a **file-backed** store inside the
  `with` block, or reopen the file later (Step 10). In-memory stores are unaffected.
* `store=":memory:"` is the default and is what you want in tests; a path gives you a durable SQLite file.

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

* `map()` takes **any iterable**, including a generator, and yields lazily. Memory stays at
  O(`concurrency`), so a 10-million-row dataset costs the same as a 10-row one.
* `concurrency=4` means four **attempts in flight**, not four pipelines alive — a pipeline waiting out a retry
  backoff holds no worker slot (Step 8).
* Your task body must actually `await` something for work to overlap. 12 × 20 ms at concurrency 4 finishes in
  ~0.07 s; a task that blocks the event loop would take 0.24 s.
* `repeats=3` is pass@k and self-consistency sampling: one seed, three pipelines, three independent
  checkpoints. Use `key_of=lambda row: row["qid"]` to supply your own ids instead of the content-addressed
  default.
* `spec.key` (the same value as `pipeline_id`) comes from the task chain plus the seed content plus the repeat
  index, so re-running the same dataset produces the same ids. That is what makes resume and sharding work.

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

* Chain checking uses the **annotations**: a task returning `Question` chains into a task taking `Question`
  (subclasses are fine), `Any` or no annotation is permissive, and a bare container accepts its parameterised
  form (`dict` ← `dict[str, Any]`). A mismatch raises `PipelineBuildError` when you build the pipeline.
* `seq` is a task's position in the chain. `seq=-1` is the seed: the dataset row is itself a stored artifact,
  which is what lets a resumed run work without the original file (Step 9).
* The final artifact is the last task's output, marked `is_final=True`.
* A pipeline is **linear**. When a step genuinely branches — three judges, k samples, several metrics — keep
  the branch inside one task with `fanout(...)` (Step 13).

---

## Step 4 — the checkpoint is the artifact

Every successful task persists its artifact immediately, so the checkpoint granularity is the task.

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

For every successful task, four things happen in order: the artifact bytes are written, the task row is
finalised, `n_tasks_done` advances, and only then does the next task start.

* **Content addressing.** `seq=-1` and `seq=0` share a digest here because `fetch` returns the seed unchanged
  — identical payloads are identical bytes, so they cost storage once. An artifact's identity is
  `(pipeline_id, seq)`.
* **Dataclasses round-trip.** Annotating the return type (`-> Answer`) registers the class, so a restored
  checkpoint is an `Answer`, not a `dict`. For binary types, register a codec (Step 14).
* **Change a task's code, get a new pipeline.** The pipeline key includes a digest of each task's source, so
  editing a task body abandons the old checkpoints rather than reusing results produced by different code.
  Pass `include_code=False` to `pipeline(...)` when you want body-only changes to keep reusing them;
  factory parameters, child tasks and declared policies still affect identity. Explicit keys reject
  task/input mismatches. See [resume identity](reference.md#resume-identity) for v2 store upgrades
  and idempotency when external work succeeds before a checkpoint becomes durable.
* **Re-running the same seeds is a no-op.** The second run reports `skipped=2`. A skipped pipeline is not
  rewritten, so its row still belongs to the run that did the work.

---

## Step 5 — endpoints as a pool

A `Resource` is one concrete capability (an endpoint, a key, a local worker). A `Pool` is a group of them plus
a policy for waiting. Tasks get one through `ctx.acquire(...)`.

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

* `capacity` belongs to the **resource**, not the pool: `api-a` and `api-b` allow 2 concurrent leases each,
  `api-c` allows 4, so this pool can serve 8 requests at once. Compare that total against `concurrency` —
  more workers than capacity means workers queueing on the pool.
* `factory=` is called **once per resource**, lazily, on first lease; every lease of that resource then shares
  the client object. That is where your SDK client or connection pool goes. If the factory raises, the lease
  is refused with `ResourceUnavailable` (never a lease whose `client` is `None`), a `resource.factory_failed`
  event is recorded, and repeated failures eventually mark the resource `dead`.
* The selector (`ctx.acquire(model="gpt-4o-mini")`) matches on `options`, `tags`, `id` and `kind`, including
  dot paths into nested options (`"quota.tokens"`). This is how you route to the right client.
* `lease.report(...)` is how the pool learns anything: `ok=False` feeds circuit-breaking, `latency_ms`
  maintains an EMA, `usage={"tokens": n}` accumulates quota you can rank on (Step 6).
* `pool.snapshot()` is the per-resource view, `pool.stats()` the aggregate. Both are safe to call during a run.

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

Two things the output makes visible:

* `immediate` failed 3 of 4 pipelines. `ResourceUnavailable` classifies as `unknown`, and the default retry
  policy does not retry `unknown`. Set `Retrying(max_attempts=3, retry_unknown=True)` if you want capacity
  retries.
* `quota_aware` picked `p-quota-1` first (both fresh, so the larger absolute headroom won the tie-break), then
  `p-quota-0` (untouched, so the better ratio), then `p-quota-1` again once the 1 000-token resource was
  spent. Quota is a preference, not a hard stop — for a hard limit, track the budget in your task and raise.

Acquire-time backoff (`backoff`) and retry-time backoff (`Retrying`) are **different knobs**: the first
decides how long to wait for a slot, the second how long to wait after a failure.

---

## Step 7 — the lease contract

`async with ctx.acquire(...)` returns the resource on every exit path. This step demonstrates each of them.

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

What to do with that:

* Use `async with ctx.acquire(...) as lease:` and hold the lease only around the request. Long holds are legal,
  and they are what starves a pool.
* A forgotten release is reported, not hidden: check `report.leases_leaked`, watch for the `lease.leaked`
  event, and set `strict_leases=True` in CI so it fails the task instead.
* Holding a resource from a pool while asking the same pool for a second one is a deadlock shape. The pool
  emits `acquire.suspected_deadlock` after `deadlock_warn_s` (default 5 s) rather than hanging silently.
  Acquire both up front, or use two pools.

---

## Step 8 — failures: classify, retry, record

Failures are ordinary Python exceptions. The framework classifies them, asks your policy what to do, and
records every attempt and every decision.

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

Retried by default: `retryable`, `rate_limit`, `timeout`, `connection`, `upstream`. Not retried by default:
`invalid`, `fatal`, `cancelled`, `unknown`. Classification reads `TimeoutError`, `ConnectionError`, an
`error_class` attribute, and HTTP status codes on `status` / `status_code` / `http_status` / `code` (falling
back to `exc.response.status_code`) — which covers the usual provider SDKs without importing them. The full
table is in [the reference](reference.md#error-classes).

The `Retrying` fields you will reach for most:

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `1` | total attempts — **the default is no retries** |
| `on` | `()` | extra exception types to treat as retryable |
| `retry_unknown` | `False` | also retry `unknown`, e.g. `ResourceUnavailable` |
| `base`, `factor`, `cap` | `0.5`, `2.0`, `30.0` | exponential backoff bounds |
| `max_total_s` | `None` | give up once attempts plus delays exceed this budget |

Ways to steer it:

* `raise RetryableError("rate limited", error_class="rate_limit", retry_after=0.01)` — carry the class and
  honour a server-suggested delay. `retry_after` is also read from a `Retry-After` header when the exception
  exposes one.
* `raise FatalError(...)` — never retried, whatever `max_attempts` says.
* `with_retry(ask, max_attempts=5)` — reuse a task under a different policy.

A retry backoff does not hold a worker: the pipeline is parked and the worker takes other work, so
`concurrency` stays honest. A run ending during a backoff records the parked pipeline as `interrupted` with its
checkpoint intact, so `resume` picks it up.

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

The rules, in the order they are applied:

1. The pipeline already `succeeded` → skipped entirely. Pass `retry_succeeded=True` / `--retry-succeeded` to
   re-run it anyway (it then starts from the seed, since a finished pipeline has no checkpoint left to
   continue). To *discard* the checkpoint of a pipeline that has not succeeded, use `fresh_restart=True` /
   `--fresh-restart` — the same switch resets a spent backward-traversal budget. Append-only history
   (attempts, events, handoffs) survives; a backward pipeline additionally keeps its visit occurrences
   and counters, while a forward pipeline reuses its task/artifact addresses by design.
2. The pipeline is `failed` or `interrupted` with `n_tasks_done > 0` → load the artifact at `n_tasks_done - 1`
   and continue at the next `seq`. **This is why `ask` sent nothing in round 2**: the judge was the first task
   with no artifact, so only the judge ran.
3. The pipeline is `failed` or `interrupted` and its cursor already reached the end
   (`n_tasks_done == n_tasks_total`) → every task is checkpointed, so the pipeline is repaired to `succeeded`
   **without re-running anything**: the last artifact is verified (present, payload kept, decodable), marked
   final, and `pipeline.terminal_repaired` carries the state, error and run the row was carrying. This is the
   shape a store failure or a kill during the final write leaves behind. If the artifact cannot be verified,
   rules 5 and 6 apply instead; if the repair itself fails — marking the artifact final or settling the row —
   the row is left untouched (original failure included) and `pipeline.terminal_repair_failed` records the
   attempt, so the next run still reports the original cause.
4. The cursor is *past* the end (`n_tasks_done > n_tasks_total`) → not a state the Runner can create: the row
   is recorded as `CorruptCheckpoint` and reported as `pipeline.corrupt_cursor`, never promoted to success,
   and the stored cursor is left in place as the evidence.
5. The artifact row is gone, or its payload was not kept (`journal=summary`, `null` backend) → the pipeline
   restarts from `seq=0` and records `pipeline.checkpoint_missing` (this also covers rule 3 when the terminal
   artifact's payload is gone).
6. The artifact is there but cannot be decoded (a removed codec, a changed dataclass, a corrupted payload) →
   the same restart, recorded as `pipeline.checkpoint_unusable` so the two causes stay distinguishable (this
   also covers rule 3 when the terminal artifact is present but undecodable).
7. Otherwise the pipeline is new, and the seed artifact is stored before the first task runs.

Three things to note:

* `skipped=0` in round 2 is correct: nothing had succeeded, three pipelines were *failed* and got resumed.
  `skipped` counts rule 1 only, not "work that was not repeated".
* At resume time the dataset stream only *identifies* pipelines — the resumed task's input comes from the
  store. A run interrupted mid-pipeline does not need the dataset file to still exist, only the same seeds to
  be enumerated.
* Every attempt keeps its `run_id`, so the record shows which run did which work.

Events worth alerting on: `pipeline.resumed`, `pipeline.skipped`, `pipeline.restarted`,
`pipeline.checkpoint_missing`, `pipeline.checkpoint_unusable`, `pipeline.deferred_interrupted`,
`pipeline.terminal_repaired`, `pipeline.terminal_repair_failed`, `pipeline.terminal_cleanup_failed`,
`pipeline.corrupt_cursor`.

Two run-level events deserve the same treatment. `runner.internal_error` is a framework-level surprise
recorded against one pipeline while the run carries on. `runner.worker_crashed` is heavier: a worker died
outside its own handlers (a `BaseException` that is not a cancellation, raised by a store hook for example),
so the run stops, the pipeline that worker was holding is recorded `failed` — or `interrupted` if the run was
already stopping — and `run()` raises `WorkerCrashed` with the original exception as its cause. If this event
shows up, the run did not finish on its own terms: read the pipeline row, fix the cause, and rerun with
`--resume`.

---

## Step 10 — reading the record

Everything the framework knows lives in a handful of tables. Five are keyed by `pipeline_id` and are the five
exportable row kinds (`--rows`); `runs` and `resources` have Python readers but no export path.

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

* `report.summary()` is for humans, `report.to_dict()` for dashboards, `runner.stats()` for a live view (safe
  mid-run), `store.errors()` for the failure list.
* `attempts` is the table that answers "why did this take 40 seconds": each attempt's duration, its lease log,
  and its decision (`retry`, `reason`, `delay_s`, `error_class`).
* Export shapes: `--rows pipelines|tasks|attempts|events|artifacts` × `--format jsonl|json|csv`. CSV flattens
  nested values into compact JSON, so it opens cleanly in a spreadsheet.
* Append-only tables (attempts, events) are written in batches; state (artifacts, checkpoints) is written
  synchronously. `SIGKILL` can cost the last batch of history, never a checkpoint. `--no-write-behind` commits
  everything immediately.

---

## Step 11 — the declarative path and the CLI

Composition and resources can live in YAML; the logic stays in Python. The declarative layer does exactly
three things: pick tasks, chain them, configure pools.

This is the first step that needs something beyond the standard library, so it is where the extra comes in:
`uv add "pyattacker[yaml]"` (or `pip install "pyattacker[yaml]"`). Only the YAML *parser* is optional — the
layer itself, and the same config written as `.json` or `.toml`, work without it. The parser is chosen from
the file suffix, so a `.yaml` file on a machine without the extra is a config error that names the extra
rather than an import traceback.

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
| `run` | `store`, `concurrency`, `journal` (`full` keeps payloads, `summary` keeps only digests), `label`, `strict_leases`, `stop_after_failures`, `stop_after_s`, `retry_succeeded`, `fresh_restart`, `heartbeat_s`, `grace_s`, `stale_after_s`, `notes` |
| `pools.<name>` | `kind`, `algorithm`, `capacity` (default for its resources), `degrade_after`, `dead_after`, `cooldown_s`, `deadlock_warn_s`, `resources: [{id, kind, capacity, options, tags}]` |
| `pipeline` | `name`, `tags`, `include_code`, and `tasks: [{use, name, resource, algorithm, timeout_s, retry, args, kwargs}]` |
| `source` | `kind: range` (`n`) or `kind: jsonl` (`path`, `limit`), plus `repeats`, `key_field` |

Details that save time:

* `use:` is resolved by shape: a name containing a colon is `module:attribute` and is imported directly; a bare
  name is looked up in the built-ins first and then in installed plugins, so a plugin cannot shadow `echo`.
  `pyattacker.tasks:simulate_llm` and `your_pkg.tasks:ask_model` are both valid with no plugin. A factory (a
  callable returning a `TaskSpec`) is called with `args`/`kwargs`.
* `${VAR}` and `${VAR:-default}` are expanded in every string *value* (keys are left alone). Unresolved names
  are reported by `validate` and by `describe()["unresolved_env"]`; `--strict-env` makes them an error.
* **YAML 1.1 pitfall:** a bare `on:` key parses as boolean `true`. Write
  `"on": [RetryableError, TimeoutError]` — the loader detects the mistake and says so.
* Exit codes: `0` all succeeded, `1` some failed, `2` config error, `130` interrupted.

Every flag of every command is in [`docs/cli.md`](cli.md).

---

## Step 12 — sharding and merging

Scaling out means several processes with their own stores, joined afterwards. A pipeline's shard comes from its
content-addressed key, so the same dataset always splits the same way.

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
print("  attempts:", merged.stats()["attempts_total"], "->", again.stats()["attempts_total"],
      "| source_events:", merged.stats()["source_events_total"], "->",
      again.stats()["source_events_total"], "(raw, so it follows the sources)")

merged.export("runs/step12-all.jsonl")
with open("runs/step12-all.jsonl", encoding="utf-8") as handle:
    print("merged export rows:", sum(1 for _ in handle))

# Sharding is content-addressed, not round-robin: re-running the same seeds replays the same split.
print("shard of the first pipeline:", shard_index(specs[0].pipeline_id, SHARDS), "of", SHARDS)
```

```text
shard 0:  6 pipelines -> {'succeeded': 6} -> runs/step12.shard0of3.db
shard 1:  5 pipelines -> {'succeeded': 5} -> runs/step12.shard1of3.db
shard 2:  1 pipelines -> {'succeeded': 1} -> runs/step12.shard2of3.db

merged 12 pipelines from 3 store(s)
  succeeded=12  attempts=12 source_events=27
  tasks: ask=12
pipeline rows: 12 | duplicates dropped: 0 | sources: 3
with one store counted twice: 12 rows, 6 duplicates dropped
  attempts: 12 -> 12 | source_events: 27 -> 40 (raw, so it follows the sources)
merged export rows: 12
shard of the first pipeline: 1 of 3
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

* `shard_index(key, N)` hashes the key with `blake2b` rather than Python's `hash()` (which is salted per
  process), so the split is stable across runs and machines. `--shard 2/4 --resume` therefore puts every
  pipeline back where it was. Sizes are only *roughly* equal — 6/5/1 for twelve pipelines is not a bug.
* Merging de-duplicates by `pipeline_id`, keeps the best state (succeeded > failed > interrupted, then the
  latest finish), and recomputes the workload counters — `attempts_total`, `handoffs_total` — from the
  surviving rows. A partially-overlapping re-run, or a store counted twice, still produces one answer. The
  event log is the deliberate exception: an event is not part of a pipeline row, so `source_events_total`
  is a raw total over the sources you passed, and it is named that way rather than pretending otherwise.
* `merge_reports([...])` returns a `MergedReport` with `.summary()`, `.stats()`, `.errors()` and
  `.export(path, fmt=..., kind=...)`.

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

What each piece does:

* **`ask` makes two requests inside one task.** A multi-turn conversation, a tool loop, or a "retry the parse
  until it validates" loop all belong inside one task. One step, one artifact.
* **`fanout(...)` keeps the branch inside a task.** Three judges run concurrently on the same input and return
  `{task_name: value}`. Children share the parent's context, so their leases and events land in one record. The
  Runner only sees the group spec, so `resource`, `algorithm` and `timeout_s` are lifted from the children only
  when all of them agree — which is why the three judges declare the same pool and algorithm.
* **Pools per role.** `models` has one resource with capacity 4; `judges` has one resource per model with
  capacity 1, so the three branches never queue behind each other.
* **The outage becomes data.** `judge-b`'s `RetryableError(error_class="upstream")` is classified, retried by
  the group's policy, and recorded as a failure with the failing task named.
* **The resume shows its cost.** Round 2 re-ran the whole judges group, so `judge-a` and `judge-c` were sent
  again even though their verdicts were on disk — four of six requests were repeats.

That last point is the choice you have to make when a step branches:

> If a request is expensive or slow, give it its own task. If a step fans out over cheap calls, one task and
> one checkpoint is the better trade.

Three judges as three tasks (`C1 | C2 | C3`) makes the checkpoint finer: the same failure resumes at `C2` and
re-sends nothing that succeeded, at the cost of two extra pipeline steps.
[`examples/llm_eval/`](../examples/llm_eval/README.md) implements both shapes over the same tasks and measures
them — with one judge endpoint down, the grouped shape re-sent 2 already-successful requests and the split
shape re-sent 0.

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
  Register with `registry.register(codec, for_types=(MyType,))` and pass the *same* registry to
  `pipeline(..., registry=...)` for seeds and to `Runner(registry=...)` for artifacts. A codec that claims a
  payload beats the built-in JSON catch-all.
* **Artifact backends** decide where payload bytes live: `None`/`"inline"` keeps them in the database, `"null"`
  drops them and keeps digests, `"file:///data/blobs"` (or
  `{"kind": "file", "root": ..., "min_bytes": 262144}`) spills to content-addressed files that are hydrated
  back on read. `min_bytes=0` above spills everything, which is why the artifact row shows a reference instead
  of bytes.
* **Plugins** are `importlib.metadata` entry points in four groups: `pyattacker.tasks`,
  `pyattacker.algorithms`, `pyattacker.codecs`, and `pyattacker.stores` (keyed by URI scheme, so
  `store = "s3://bucket/runs.db"` works). Built-ins resolve first, and a plugin that raises on import is
  recorded rather than fatal — `pyattacker plugins` shows both. A complete worked package is
  [`examples/plugin_package/`](../examples/plugin_package/README.md).

**Monitoring a run in progress.** `pyattacker watch runs/qa.db` gives you a terminal view from a second
process, and `pyattacker serve runs/qa.db` an HTTP dashboard plus JSON at `/stats`, `/metrics`, `/events`, `/pipelines`,
`/resources` and `/errors`. Both open read-only connections, so they are safe beside a live run. The HTTP
endpoint has no authentication and serves your payloads — keep it on loopback.
For live experiment accuracy, run [`examples/live_metrics.py`](../examples/live_metrics.py). Its
completion callback decodes final artifacts, deduplicates by pipeline ID, and reports the current
accuracy through `runner.report_metric()`; the dashboard displays the value as it changes.
* **Monitoring**: `pyattacker serve runs/qa.db` is a zero-dependency read-only HTTP view (`/`,
  `/stats`, `/metrics`, `/events`, `/pipelines`, `/resources`, `/errors`) that opens a fresh connection per request,
  so it runs happily beside a live run. It binds to loopback and has no authentication — treat it as a
  debug view, not a dashboard.

---

## Step 15 — advanced: skipping stations (handoffs)

**This step is the one advanced feature in the framework: it is opt-in, it changes the execution model, and
it is marked experimental until 1.0.** Everything before this step works without it, and a pipeline that does
not declare it behaves exactly as it did before the feature existed. Read this step when you have a step that
decides *the rest of the chain no longer needs to run*.

The situation: `judge` can tell that an answer is already good enough, or that the `metrics` step is
unnecessary for this row. The old options were to run the remaining tasks anyway, to fold everything into one
task with `fanout` (losing per-step records), or to raise — which records the pipeline as **failed**, which is
a lie. A **handoff** says what actually happened: this row skipped stations 3–5 and continued at station 6, or
finished right here.

```python
# tutorial/step_15_handoff.py
"""Step 15 (advanced) - a task hands off: skip stations, or finish the pipeline, and nothing lies about it."""

import json
from contextlib import closing

from pyattacker import Handoff, Runner, open_store, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"q": seed["q"], "confidence": seed["confidence"]}


@task("ask")
async def ask(row: dict, ctx) -> dict:
    # <- your HTTP call: the model answers, and reports how sure it is
    await ctx.clock.sleep(0.001)
    return {**row, "answer": f"answer-for:{row['q']}"}


@task("judge")
def judge(row: dict) -> Handoff | dict:
    """Three outcomes: finish here, skip metrics, or carry on down the chain."""
    if row["confidence"] >= 0.9:
        # already good enough: end the pipeline with this as its final artifact
        return Handoff.end({"q": row["q"], "answer": row["answer"], "verdict": "confident"},
                           reason="already good enough")
    if row["confidence"] >= 0.5:
        # not worth the metrics call, but the report is still wanted: continue at "report"
        return Handoff.to("report", {"q": row["q"], "answer": row["answer"], "verdict": "ok"},
                          reason="metrics not needed")
    return {**row, "verdict": "needs-metrics"}


@task("metrics")
def metrics(row: dict) -> dict:
    return {**row, "score": round(row["confidence"] * 10, 1)}


@task("report")
def report(row: dict) -> dict:
    return {**row, "reported": True}


# The edges are declared, not derived: "report" is what judge may jump to, and END finishes the pipeline.
template = pipeline(
    "qa",
    prepare | ask | judge | metrics | report,
    control={"edges": {"judge": ["report", "end"]}},
)

rows = [
    {"q": "capital of France", "confidence": 0.95},   # judge ends the pipeline (metrics and report skipped)
    {"q": "17 * 23", "confidence": 0.6},              # judge skips metrics, continues at report
    {"q": "prove sqrt(2) is irrational", "confidence": 0.2},  # judge hands nothing off: the full chain runs
]

with Runner(store="runs/handoff.db", concurrency=4) as runner:
    report_obj = runner.run(template.map(rows))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        ran = [t.name for t in store.tasks(record.pipeline_id)]
        skipped = [t.name for t in template.tasks if t.name not in ran]
        print(f"\n{record.pipeline_id[:8]}  state={record.state}  ran={ran}  skipped={skipped}")
        for hop in store.handoffs(pipeline_id=record.pipeline_id):
            where = "END" if hop.to_seq is None else hop.to_task
            print(f"   jumped: {hop.from_task} -> {where}  ({hop.reason})  entry={hop.entry_artifact_id}")

# The record is a plain store: reopen it whenever, and read the same ledger back.
with closing(open_store("runs/handoff.db")) as reopened:
    print("\nhandoffs recorded:", reopened.stats()["handoffs_total"])
    row = next(iter(reopened.export_rows()))
    print("first exported pipeline's ledger:", json.dumps(row["handoffs"], ensure_ascii=False))
```

The three rows take three different paths, and the record says so without any of them having failed:

```text
ec5fc1e5  state=succeeded  ran=['prepare', 'ask', 'judge']  skipped=['metrics', 'report']
   jumped: judge -> END  (already good enough)  entry=ec5fc1e5dff6a260512e579a276d11e0:5
cdce72ca  state=succeeded  ran=['prepare', 'ask', 'judge', 'report']  skipped=['metrics']
   jumped: judge -> report  (metrics not needed)  entry=cdce72cae311e870d87d85e9dbcaa280:5
260cf635  state=succeeded  ran=['prepare', 'ask', 'judge', 'metrics', 'report']  skipped=[]
```

What is worth knowing before you use it:

* **It is a return value, not an exception.** The retry policy never sees it, a task-side
  `except Exception:` cannot swallow it, and `async with ctx.acquire(...)` has already returned its leases
  on the way out. A cancelled or timed-out attempt never reaches the return, so nothing is half-transferred.
* **The edges are declared, so a mistake is loud.** Returning a `Handoff` from a pipeline with no `control`
  block, or along an edge that was not declared *from that task*, is a `FatalError` — never retried, never a
  silent jump. Destinations must be strictly later than their source (the `edges` operation is forward-only), a name
  that appears twice in the chain must be given as a seq, and `end` from the last task is refused because it
  would do nothing.
* **A handoff is a checkpoint, so resume continues at the target.** If the process dies after the jump, the
  next `resume=True` run starts at the destination with the recorded entry state and does **not** re-run the
  source task. The `handoffs` row is what makes that possible: it names the destination and the entry
  artifact, which is either the artifact the source task received (`Handoff.to(target)` with no value) or a
  new payload stored at its own address above the chain (`seq >= n_tasks`).
* **`n_tasks_done` becomes a position.** The skipped slots have no task rows, so on a control-enabled
  pipeline `n_tasks_done / n_tasks_total` is not a completion percentage — `store.handoffs()` and
  `stats()["handoffs_total"]` are how you see what actually happened. `report`, `watch`, `/pipelines` and
  every export show the handoff count next to it.
* **Do not reach for it first.** A handoff is a *scheduling statement* about the current pipeline: "this row
  should continue over there". Conditions still belong in task code, branching inside a step is still
  `fanout`, and iterating a dataset is still `map`. A pipeline that is mostly handoffs is a sign the problem
  wants a graph engine, which this is not.
* **Advanced tier.** It is opt-in, it changes the execution model, and it is experimental until 1.0: the
  guarantees above are stable, the spelling may still change. Separately declared backward operations
  use visits and finite budgets; see [Step 16](#step-16--advanced-regenerating-with-rewind-and-retry-all)
  and [reference → advanced: backward traversal](reference.md#advanced-backward-traversal-rewind-retry-all-visits).
  Both built-in stores can commit a handoff; a custom store that cannot is refused up front with a
  `ConfigError` rather than writing a jump that would not survive a crash.

A whole-pipeline restart (an explicit `fresh_restart=True`, a pipeline that already succeeded under
`retry_succeeded=True`, or a lost entry payload) keeps previous handoffs, attempts and events as history, but
atomically clears current task/chain artifact state
with the durable ledger watermark before restarting at seq 0. A later failure
therefore cannot resume an old jump or recover an old END result. Subsequent target resumes preserve the
current watermark, so repeated interruptions still use the active handoff. Completion leaves one final
artifact. See the [store recovery contract](reference.md#tables-and-readers) when implementing a backend.

---

## Step 16 — advanced: regenerating with rewind and retry-all

**Like Step 15, this is an advanced, opt-in feature, experimental until 1.0.** It changes the traversal of
the chain: a validator can send the work *back* to an earlier station instead of failing the row or looping
inside one task. Read it when a step decides that an earlier step should run again with different state.

The situation: `validate` rejects a structured answer, and the fix is another generation with the same
prompt plus the error feedback. Folding that loop into one task would collapse generation and validation
into one record — "how many regenerations did this row need" would be invisible exactly where it is the
measurement. A **rewind** keeps both generations as separate visits with their own task, attempt and
artifact rows.

```python
# tutorial/step_16_backward.py
"""Step 16 (advanced) - a validator rewinds to the generator; state is what the author chose."""

from pyattacker import Handoff, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"prompt": seed["prompt"], "temperature": 0.2}


@task("generate")
def generate(state: dict, ctx) -> dict:
    # <- your model call: a revisit is a genuinely new sample
    return {**state, "answer": f"sample-{ctx.visit}", "valid": ctx.visit >= 1}


@task("validate")
def validate(row: dict, ctx) -> Handoff | dict:
    if not row["valid"]:
        # Explicit state: the author decides what the generator gets, the framework guesses nothing.
        return Handoff.rewind(
            "generate",
            {"prompt": row["prompt"], "temperature": 0.7, "feedback": "answer did not parse"},
            reason="invalid structured output",
        )
    return {**row, "validated": True}


@task("report")
def report(row: dict, ctx) -> dict:
    # ctx.visit counts entries into *this* station, and report ran once — so it is 0 here. How many
    # regenerations the row needed is the generate station's counter, printed from the store below.
    return {"answer": row["answer"], "temperature": row["temperature"]}


# `rewind` is declared from validate to the strictly earlier generate; max_handoffs is required and is
# the loop budget: transfer N+1 fails before it can invalidate anything.
template = pipeline(
    "regenerate",
    prepare | generate | validate | report,
    control={"rewind": {"validate": ["generate"]}, "max_handoffs": 3},
)

with Runner(store="runs/backward.db", max_handoffs=10) as runner:   # 10 is a ceiling, not a floor
    report_obj = runner.run(template.map([{"prompt": "Return JSON with one field"}]))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        print(f"\n{record.pipeline_id[:8]}  state={record.state}  position={record.n_tasks_done}")
        for row in store.tasks(record.pipeline_id):
            print(f"   seq={row.seq} visit={row.visit} {row.name:9s} {row.state}")
        for hop in store.handoffs(pipeline_id=record.pipeline_id):
            print(f"   {hop.operation}: {hop.from_task} -> {hop.to_task}  (visit {hop.from_visit} -> {hop.to_visit})")
    state = store.visit_state(record.pipeline_id)
    print("\nbudget consumed:", state["handoffs"], " visits per station:", state["counters"])
    print("effective outputs:", {s: store.get_artifact(record.pipeline_id, s).visit for s in range(4)})
    # Every occurrence is still there by exact id, including the superseded first generation.
    print("first generation kept:", store.get_artifact_by_id(f"{record.pipeline_id}:1").payload)
```

The row is generated twice, and the record shows both visits instead of hiding the retry:

```text
succeeded  position=4
   seq=0 visit=0 prepare   succeeded
   seq=1 visit=0 generate  succeeded
   seq=1 visit=1 generate  succeeded
   seq=2 visit=0 validate  handed_off
   seq=2 visit=1 validate  succeeded
   seq=3 visit=0 report    succeeded
   rewind: validate -> generate  (visit 0 -> 1)

budget consumed: 1  visits per station: {'0': 0, '1': 1, '2': 1, '3': 0}
effective outputs: {0: 0, 1: 1, 2: 1, 3: 0}
```

What is worth knowing before you use it:

* **You choose the state; the framework does not roll back dictionaries.** `Handoff.rewind(target, value)`
  requires an explicit value (`None` is a real value), and the target must be a declared, strictly earlier
  task. Results before the target stay effective; the target and everything after it become historical and
  run again with new visits.
* **`Handoff.retry_all()` restarts from the original bound seed**, freshly decoded from the bytes captured
  at binding — mutating `spec.seed` or a task's input afterwards does not change it. Use
  `Handoff.rewind(0, chosen_state)` when the restart should begin with different state.
* **Loops are bounded, so termination is not structural.** `control.max_handoffs` is required for a
  backward plan, and `RunConfig.max_handoffs` (default 1000, `run.max_handoffs` in a config) can lower it:
  the effective limit is the minimum. `END` never consumes a transfer. The failing transition is refused
  *before* anything is invalidated, so the record still describes exactly what committed.
* **Getting out of a spent budget, and out of a `running` row, is explicit.** `resume=True` keeps the
  consumed budget and continues the current traversal, so a pipeline that failed *because* it ran out of
  transfers needs `fresh_restart=True` / `--fresh-restart`: a new budget lifecycle, a restart from the bound
  seed, and visit counters plus audit rows preserved. And a row that still says `running` — the shape a hard
  kill leaves — is only claimed with `resume=True`; without it the run skips it instead of forking a
  traversal another run may still own.
* **A crash cannot lose the lineage.** Entry, visit allocation, effective slots and the pending input commit
  atomically, so a resume continues the same visit with its chosen payload rather than replaying the whole
  pipeline. What the framework does not give you is exactly-once external side effects — include `ctx.visit`
  in an idempotency key when each regeneration should be a new external operation.
* **Visit 0 is unchanged.** A pipeline that never rewinds keeps `pipeline_id:seq` ids, the same random
  stream and the same `spec_digest`, so adding the declaration is what opts a pipeline into the new
  addresses — not upgrading the package.
* **Ordinary dictionaries stay ordinary.** `HistoryArtifact` is an optional payload base class for
  snapshot/restore bookkeeping; nothing in the runner reads it to decide where to go next.

The full interface — the visit/occurrence model, the budget lifecycle, the store capability and the
`HistoryArtifact` codec — is in [reference → advanced: backward traversal](reference.md#advanced-backward-traversal-rewind-retry-all-visits),
and the optional payload history gets its own step next.

---

## Step 17 — advanced: payloads that carry their own history

**Also advanced, also opt-in, and it changes nothing about scheduling.** Steps 15 and 16 decide *where* the
pipeline goes; this one is the optional companion to a rewind, for when the state you send back should carry
named checkpoints of its own.

The situation: `validate` wants to send `generate` back to the state as it was *before* the sample that
failed — not to a dictionary the author rebuilt by hand. Building that dictionary is the common case and it
is what Step 16 does; there is nothing wrong with it. But when the payload *is* the application's state
machine — it has stages, earlier stages are worth keeping, and "regenerate from stage 2 while stage 3 stays
on the record" is the operation you want — `HistoryArtifact` is an optional payload base class that carries
detached snapshots of application state plus a versioned codec for them.

```python
# tutorial/step_17_history.py
"""Step 17 (advanced) - optional payload history: named checkpoints carried inside the payload."""

from pyattacker import CodecRegistry, Handoff, HistoryArtifact, Runner, pipeline, task


class DraftState(HistoryArtifact):
    """Application state with its own snapshots; the runner stores it and never reads it."""


@task("prepare")
def prepare(seed: dict) -> DraftState:
    state = DraftState({"prompt": seed["prompt"], "temperature": 0.2})
    return state.checkpoint("prepared")


@task("generate")
def generate(state: DraftState, ctx) -> DraftState:
    # <- your model call: a revisit is a genuinely new sample, so the label names the visit
    draft = state.with_state({**state.state, "answer": f"sample-{ctx.visit}", "valid": ctx.visit >= 1})
    return draft.checkpoint(f"sample-{ctx.visit}", metadata={"temperature": draft.state["temperature"]})


@task("validate")
def validate(state: DraftState, ctx) -> Handoff | DraftState:
    if not state.state["valid"]:
        # Go back to the snapshot taken before this sample, then choose what the generator sees next.
        base = state.restore("prepared")
        return Handoff.rewind(
            "generate",
            base.with_state({**base.state, "temperature": 0.7}),
            reason="answer did not parse",
        )
    return state


@task("report")
def report(state: DraftState, ctx) -> dict:
    # The decoded payload still carries every snapshot, in order, plus the one that is selected.
    return {
        "answer": state.state["answer"],
        "labels": [row["label"] for row in state.history],
        "selected": state.selected,
    }


registry = CodecRegistry()
registry.register_type(DraftState)   # decoding restores DraftState, not a bare HistoryArtifact

# The same registry goes to the template (for seeds) and to the Runner (for checkpoints) - Step 14's rule.
template = pipeline(
    "draft",
    prepare | generate | validate | report,
    registry=registry,
    control={"rewind": {"validate": ["generate"]}, "max_handoffs": 3},
)

with Runner(store=":memory:", registry=registry) as runner:
    report_obj = runner.run(template.map([{"prompt": "Return JSON with one field"}]))
    print(report_obj.summary())
    store = runner.store
    record = next(iter(store.pipelines()))
    for row in store.tasks(record.pipeline_id):
        print(f"   seq={row.seq} visit={row.visit} {row.name:9s} {row.state}")
    validate_output = runner.registry.load(store.get_artifact(record.pipeline_id, 2).encoded())
    print("\nfinal artifact:", runner.registry.load(store.get_artifact(record.pipeline_id, 3).encoded()))
    print("snapshots:", [row["label"] for row in validate_output.history])
    print("selected:", validate_output.selected)
    pruned = validate_output.prune("sample-0")   # explicit, and never the selected snapshot
    print("after pruning sample-0:", [row["label"] for row in pruned.history])
    try:
        pruned.prune("prepared")
    except ValueError as exc:
        print("prune refused:", exc)
```

The traversal is Step 16's; what is new is that the payload itself remembers where it has been:

```text
   seq=0 visit=0 prepare   succeeded
   seq=1 visit=0 generate  succeeded
   seq=1 visit=1 generate  succeeded
   seq=2 visit=0 validate  handed_off
   seq=2 visit=1 validate  succeeded
   seq=3 visit=0 report    succeeded

final artifact: {'answer': 'sample-1', 'labels': ['prepared', 'sample-0', 'sample-1'], 'selected': 'snapshot:0'}
snapshots: ['prepared', 'sample-0', 'sample-1']
selected: snapshot:0
after pruning sample-0: ['prepared', 'sample-1']
prune refused: cannot prune the selected snapshot
```

What is worth knowing before you use it:

* **It is a payload, not a record.** `HistoryArtifact` is a decoded application value; it is *not* the
  persisted `Artifact` row, and snapshot history never replaces the framework's execution ledger, task or
  visit records. Nothing in the runner reads it to decide where to go next — the `Handoff.rewind` you
  return is still the only thing that moves the pipeline. A plain dictionary stays a plain dictionary:
  there is no automatic snapshot on task entry or completion.
* **Snapshots are detached, so nested edits cannot rewrite the past.** `state`, `history` and
  `snapshot(...)` all return deep copies; `checkpoint(label, *, metadata=None)` appends a snapshot with a
  stable id (`snapshot:0`, `snapshot:1`, …) and a unique label, and `snapshot:` is reserved for those ids.
  `with_state(value)` replaces the current state *without* appending a snapshot, `restore(id_or_label)`
  replaces it *and* records `selected`, keeping the whole history so the later stages stay inspectable,
  and `prune(*selectors)` is the explicit way to drop snapshots — it raises `ValueError` rather than
  pruning the selected one, and ids are not reused.
* **Persisting is the commit's job, not `checkpoint()`'s.** Calling `checkpoint()` inside a task does not
  touch the store. The runner persists the payload — history included — when it commits the task's output
  or its control transition, exactly like any other artifact. A crash before that commit loses the
  in-memory snapshot, and the codec cannot help with that.
* **The codec needs the class.** State and metadata must be JSON serializable, and the versioned
  `history-v1` codec restores snapshots *and* the registered subclass: give the same `CodecRegistry` to
  `pipeline(..., registry=...)` and `Runner(..., registry=...)` (Step 14) and register the subclass with
  `register_type` — an unregistered subclass fails decoding explicitly instead of coming back as a
  `HistoryArtifact`. Subclasses inherit the base constructor, so application fields live in `state`;
  custom constructors and extra attributes are outside this interface.
* **History grows with the payload.** Every snapshot is kept in full, so a long-lived value can grow with
  the number and size of them; prune on purpose. For "send this row back" the usual shape is one snapshot
  per visit plus the states you want to return to, which is what the program above does.

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
| start a pipeline over from the seed, keeping its audit rows | `fresh_restart=True` / `--fresh-restart` |
| bound the blast radius | `stop_after_failures=N`, `stop_after_s=T`, `--limit N` |
| cap concurrency per endpoint | `Resource.create(..., capacity=N)` |
| fail fast instead of queueing | `algorithm="immediate"` + `Retrying(retry_unknown=True)` |
| survive a 429 | `raise RetryableError(..., error_class="rate_limit", retry_after=...)` |
| store big payloads out of the DB | `--artifact-backend file:///data/blobs` |
| use four processes | `--shards 4 --jobs 4`, then `report`/`export` over the shard files |
| branch inside a step | `fanout(task_a, task_b)` |
| skip ahead / finish early, on the record | `return Handoff.to("report", v)` / `Handoff.end(v)` on a pipeline declared with `control={"edges": {...}}` |
| send a station back to an earlier one (advanced) | `return Handoff.rewind("generate", chosen_state)` on a pipeline declared with `control={"rewind": {...}, "max_handoffs": N}` |
| restart the whole pipeline from its seed (advanced) | `return Handoff.retry_all()` on a pipeline declared with `control={"retry_all": [...], "max_handoffs": N}` |
| keep named state checkpoints inside a payload (advanced) | `class S(HistoryArtifact)`, then `s.checkpoint("label")` / `.restore(...)` / `.prune(...)`, registered in both registries |
| make my code usable from YAML | no plugin: `use: my_pkg.tasks:my_task`; with an entry point in `pyattacker.tasks`: `use: my_task` |

Every class and function, with signatures and parameter tables: [`docs/reference.md`](reference.md).

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
acquire turn both become errors.

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

**"A backward pipeline fails at build time, or dies with a spent budget."** `control.max_handoffs` is
required — and must be a positive integer, so `0`, `"3"`, `3.0` and `True` are refused — as soon as
`rewind` or `retry_all` is declared. It is consumed by every nonterminal transfer and survives a resume,
so a pipeline that failed *because* the budget ran out needs `fresh_restart=True` / `--fresh-restart`:
`resume=True` alone replays the same fatal error. A row still marked `running` after a hard kill is only
claimed with `resume=True`; without it the run skips it instead of forking a traversal.

**"`Handoff.rewind` raises a `FatalError`."** The target must be a declared, strictly earlier task, named by
a unique task name or by its seq. Self-rewind, `end`, and a destination that the pipeline did not declare
are authoring mistakes, so they are fatal and never retried; `retry_all` likewise needs its source in
`control.retry_all` and accepts no value.

## Where to look next

Looking for a specific feature rather than a whole document? See [Find what you need](#find-what-you-need)
near the top.

| Resource | What is in it |
|---|---|
| [`docs/reference.md`](reference.md) | every public class and function: signatures, parameters, examples |
| [`docs/cli.md`](cli.md) | every subcommand and flag, exit codes, the config file reference |
| [`docs/design.md`](design.md) | conceptual model, six invariants, lease contract, data model, tradeoffs |
| [`README.md`](../README.md) | the compact tour: scheduling guarantees, sharding, plugins, out of scope |
| [`examples/quickstart.py`](../examples/quickstart.py) | the SDK in 60 lines, with a resume round |
| [`examples/llm_eval/`](../examples/llm_eval/README.md) | the full evaluation, two pipeline shapes, measured checkpoint granularity |
| [`examples/sharded.py`](../examples/sharded.py) | one dataset across N stores, then a merged report |
| [`examples/plugin_package/`](../examples/plugin_package/README.md) | an installable plugin: tasks, an algorithm, a codec |
| [`examples/qa_eval.yaml`](../examples/qa_eval.yaml) | the declarative path, end to end |
