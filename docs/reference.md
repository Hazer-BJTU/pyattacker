# API Reference

**English** | [简体中文](zh-CN/reference.md)

Every public name in `pyattacker`, with its signature, its parameters, and a usage example.

**Looking for something else?** [`docs/tutorial.md`](tutorial.md) teaches the workflow step by step;
[`docs/cli.md`](cli.md) documents the command line; [`docs/design.md`](design.md) explains why the model is
shaped this way.

## Conventions used here

* Everything in the [Contents](#contents) table is importable from the top-level package:
  `from pyattacker import Runner`. A handful of supporting types are *returned* by those APIs without being
  exported themselves (`PoolStats`, `DeclarativeSpec`, `TaskRecord`, `RunRecord`, the `Store` protocol); this
  page names the submodule when that is the case, and you normally never import them by hand.
* Signatures are written as they appear in the source. `*` in a signature means every parameter after it is
  keyword-only.
* **Defaults matter here.** Two in particular surprise people: `Retrying(max_attempts=1)` means *no retries*
  unless you ask, and `Runner(store=":memory:")` means nothing is persisted unless you give it a path.
* **Some examples are complete programs.** A block whose first line is `# reference/<name>.py` is written out
  and executed by [`tests/test_docs_examples.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/tests/test_docs_examples.py) on every test run — the
  README and the CLI document share that contract. Blocks without a marker are fragments on purpose.

## Contents

| Section | Names |
|---|---|
| [Tasks](#tasks) | `task`, `build_task_spec`, `TaskSpec`, `Retrying`, `TaskContext`, `with_retry` |
| [Pipelines](#pipelines) | `pipeline`, `PipelineTemplate`, `PipelineSpec`, `Chain`, `compute_spec_digest` |
| [Handoffs](#handoffs-opt-in) | `Handoff`, the `control=` declaration, `HandoffRecord` |
| [Backward traversal](#backward-traversal-rewind-retry-all-visits) | `Handoff.rewind`, `Handoff.retry_all`, the backward `control` keys, visits, budgets, recovery |
| [Running](#running) | `Runner`, `RunConfig`, `RunReport`, worker liveness |
| [Resources](#resources) | `Resource`, `Pool`, `Lease`, `PoolStats`, `Bus`, `ResourceState`, `ResourceEvent` |
| [Acquire algorithms](#acquire-algorithms) | `Wait`, `Backoff`, `LeastBusy`, `Failover`, `Sticky`, `QuotaAware`, `Immediate`, `resolve_algorithm` |
| [Errors](#errors) | the exception hierarchy, `error_class_of` |
| [Artifacts and codecs](#artifacts-and-codecs) | `Artifact`, `Codec`, `CodecRegistry`, `HistoryArtifact`, `JsonCodec`, `BytesCodec`, `Encoded`, `canonical_json`, `digest_of` |
| [Stores](#stores) | `open_store`, `SqliteStore`, `MemoryStore`, record types |
| [Artifact backends](#artifact-backends) | `InlineBackend`, `FileBackend`, `NullBackend`, `resolve_backend` |
| [Sharding and merging](#sharding-and-merging) | `shard_index`, `in_shard`, `shard_specs`, `shard_store_path`, `parse_shard`, `merge_reports`, `MergedReport` |
| [Export](#export) | `iter_rows`, `export_store`, `export_stores`, `ROW_KINDS`, `FORMATS` |
| [Declarative configs](#declarative-configs) | `load_spec`, `DeclarativeSpec` |
| [Plugins](#plugins) | `PLUGINS`, `PluginRegistry`, `list_plugins` |
| [Built-in tasks](#built-in-tasks) | `echo`, `fanout`, `flaky`, `delay`, `boom`, `leaky`, `simulate_llm`, `shell_run`, `write_jsonl`, `jsonl_source`, `seed_factory` |
| [Monitoring](#monitoring) | `StatsServer` |

---

## Tasks

A task is a unary function: one value in, one value out. It may be sync or async, and it takes either
`(value)` or `(value, ctx)`. Any other signature is a `ConfigError` raised at decoration time.

### `task`

```python
@task(name=None, *, resource=None, algorithm=None, retry=None, timeout_s=None, config=None, version=None) -> TaskSpec
```

Turns a function into a `TaskSpec`. Usable bare (`@task`), with a name (`@task("ask")`), or with options.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | the function's name | the task's name in records and events |
| `resource` | `str` | `None` | default pool for `ctx.acquire()` calls that do not name one |
| `algorithm` | `str` or algorithm | `None` | default acquire policy; falls back to the pool's |
| `retry` | `Retrying` or `dict` | no retries | policy applied when an attempt raises |
| `timeout_s` | `float` | `None` | wall-clock limit for one attempt; **async tasks only** |
| `config` | JSON mapping | `{}` | declared behavior, snapshotted into the resume fingerprint |
| `version` | `str` | `None` | explicit revision for external behavior or dynamic code |

```python
from pyattacker import Retrying, task

@task("prepare")                                   # sync, no context
def prepare(seed: dict) -> dict:
    return {"q": seed["question"]}

@task("ask", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, base=0.5, cap=30.0), timeout_s=60)
async def ask(row: dict, ctx) -> dict:             # async, with context
    async with ctx.acquire(model="gpt-4o") as lease:
        return {"a": await lease.client.chat(row["q"])}
```

`timeout_s` on a sync task is accepted but cannot fire — a sync function holds the event loop, so there is
no point at which the framework could cancel it. Put blocking work behind `await asyncio.to_thread(...)`.

A task needing more than the value and the context takes it from a closure:

```python
def make_judge(model: str, threshold: float):
    @task(f"judge.{model}", resource="judges", config={"model": model, "threshold": threshold})
    async def judge(row: dict, ctx) -> dict:
        async with ctx.acquire(model=model) as lease:
            return {**row, "pass": await lease.client.score(row) >= threshold}
    return judge

pipeline("eval", prepare | make_judge("gpt-4o", 0.8))
```

### `build_task_spec`

```python
build_task_spec(fn, *, name=None, resource=None, algorithm=None, retry=None,
                timeout_s=None, registry=None, config=None, version=None,
                children=(), parameters=None) -> TaskSpec
```

The function `@task` is built on. Call it directly when the target is not known at decoration time — which
is what the declarative layer does with `use:`. Passing an existing `TaskSpec` with overrides returns a new
spec rather than mutating it.

### `TaskSpec`

Immutable description of a task. You rarely construct one; you receive them from `@task` and pass them to
`pipeline(...)`.

| Attribute | Meaning |
|---|---|
| `name` | task name as it appears in records |
| `fn` | the wrapped callable |
| `resource`, `algorithm`, `retry`, `timeout_s` | the declared policies |
| `accepts`, `returns` | type hints, used for the build-time chain check |
| `takes_ctx` | whether `fn` accepts `(value, ctx)` |
| `module`, `qualname`, `code_digest` | identify this version of the code; folded into the pipeline digest |
| `config`, `version` | explicitly declared behavior and revision |
| `parameters`, `children` | factory parameters and nested specs; recorded separately from user config |

| Method | Returns |
|---|---|
| `is_async` | whether `fn` is a coroutine function |
| `fingerprint(*, include_code=True)` | the dict that feeds `spec_digest`; includes nested specs |
| `runtime_algorithm()` | a runtime algorithm derived from the captured identity configuration |
| `with_overrides(**kwargs)` | a new spec with the given fields replaced |

`with_overrides` has three distinct cases, and they are user-visible:

* a keyword that is **not passed** keeps the current value;
* an explicit `None` **clears** a field that supports being empty — `resource`, `algorithm`,
  `timeout_s`, `version`. This is how a factory's own `resource=` or `algorithm=` is removed;
* `UNSET` (exported as `pyattacker.UNSET`) means "not passed" even when the key is present, so a
  builder that always emits the same set of keys can forward its dict without clearing everything the
  caller did not mention. The declarative loader does exactly that.

`None` for a field that cannot be empty (`name`, `fn`, `retry`, `children`, `config`, `parameters`)
is a `ConfigError` rather than a silent no-op. Clearing `config` means `config={}`; clearing `children`
means `children=()`.

Two `TaskSpec`s compose with `|` into a `Chain`. `spec | other` validates nothing on its own; the check
happens in `pipeline(...)`.

### `Retrying`

```python
Retrying(max_attempts=1, on=(), retry_classified=True, retry_unknown=False,
         base=0.5, factor=2.0, cap=30.0, jitter="full", max_total_s=None)
```

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `1` | total attempts including the first — **the default is no retries** |
| `on` | `()` | extra exception types to treat as retryable |
| `retry_classified` | `True` | retry the retryable error classes (`rate_limit`, `timeout`, `connection`, `upstream`, `retryable`) |
| `retry_unknown` | `False` | also retry `unknown` — needed for `ResourceUnavailable` |
| `base`, `factor`, `cap` | `0.5`, `2.0`, `30.0` | delay is `min(cap, base * factor**(attempt-1))`, then jittered |
| `jitter` | `"full"` | `"none"`, `"full"`, or `"equal"` |
| `max_total_s` | `None` | give up once elapsed time plus the next delay would exceed this |

```python
Retrying(max_attempts=5, base=0.5, cap=30.0)                    # typical API client
Retrying(max_attempts=3, on=(MyProviderError,))                 # add your own exception type
Retrying(max_attempts=3, retry_unknown=True)                    # retry capacity shortages too
Retrying(max_attempts=10, max_total_s=120.0)                    # bounded by time, not just count
```

A `dict` is accepted anywhere a `Retrying` is, which is what makes the YAML form work:
`retry={"max_attempts": 3, "on": ["RetryableError"]}`.

| Method | Returns |
|---|---|
| `should_retry(exc, error_class=None)` | whether this exception is retryable under this policy |
| `delay_for(attempt, rng, retry_after=None)` | the delay in seconds; `retry_after` wins when the server suggested one |

### `TaskContext`

The `ctx` a two-parameter task receives. Constructed per attempt by the `Runner`; never by you.

| Attribute | Meaning |
|---|---|
| `pipeline_id`, `run_id`, `task_name`, `seq` | identity of the work in progress |
| `attempt` | 1-based attempt number — `if ctx.attempt > 1:` is how you detect a retry |
| `bus` | the run's `Bus` |
| `store` | the run's store, for a task that needs to read history |
| `meta` | free-form dict, per attempt |

#### `ctx.acquire`

```python
ctx.acquire(pool=None, *, algorithm=None, timeout=None, where=None, **selector)
```

Returns an async context manager yielding a `Lease`. **This is the recommended way to use a resource** —
the lease is returned on every exit path, including exceptions, cancellation and timeout.

| Parameter | Meaning |
|---|---|
| `pool` | pool name or object; defaults to the task's `resource=` |
| `algorithm` | override the acquire policy for this one call |
| `timeout` | raise `AcquireTimeout` if no resource arrives in time |
| `where` | `Callable[[Resource], bool]` for a predicate the selector cannot express |
| `**selector` | match on `id`, `kind`, `tags`, and `options`, including dot paths |

```python
async with ctx.acquire(model="gpt-4o") as lease:                     # by option
    ...
async with ctx.acquire("judges", id="judge-a", timeout=30) as lease: # explicit pool, bounded wait
    ...
async with ctx.acquire(where=lambda r: r.options["ctx_len"] >= 32000) as lease:
    ...
```

A selector matching **no** resource waits forever under the default `wait` algorithm. Pass `timeout=` or use
`algorithm="immediate"` if you would rather have an error than a hang.

| Other method | Purpose |
|---|---|
| `await ctx.acquire_lease(...)` | escape hatch returning a bare `Lease`; you must release it. A forgotten release is force-reclaimed at task end and recorded as `lease.leaked` |
| `ctx.publish_resource(pool, resource)` | add a resource at runtime; other pipelines can lease it immediately |
| `ctx.revoke_resource(pool, resource_id, reason="")` | withdraw one |
| `ctx.subscribe(pool, events=None)` | async iterator over pool events |
| `ctx.held_leases()` | leases this attempt currently holds |
| `ctx.reclaim_now()` | force-release everything held; synchronous, uninterruptible |
| `ctx.emit(kind, **data)` | write your own event into the run's event stream |
| `ctx.report_metric(name, value, *, label="", display="number")` | publish a latest value scoped to this pipeline |

```python
@task("adaptive", resource="apis")
async def adaptive(row: dict, ctx) -> dict:
    if ctx.attempt > 1:
        ctx.emit("my.retrying", attempt=ctx.attempt, reason="previous attempt timed out")
    async with ctx.acquire() as lease:
        return {"a": await lease.client.chat(row["q"])}
```

### `with_retry`

```python
with_retry(spec, **retry) -> TaskSpec
```

A copy of one task with its retry policy changed — for reusing a task under a different policy without
redefining it.

```python
from pyattacker import with_retry

careful = with_retry(ask, max_attempts=6, cap=60.0)
pipeline("qa", prepare | careful | judge)
```

Note that this changes the pipeline's `spec_digest`, and therefore its identity: pipelines built with
`careful` do not share checkpoints with pipelines built with `ask`.

---

## Pipelines

A pipeline is a linear chain of tasks and the unit of completion and resume. It is a *template* until you
give it seeds; `map()` turns each seed into an independent `PipelineSpec`.

### `pipeline`

```python
pipeline(name, *tasks_or_chain, tags=None, include_code=True, registry=None, control=None) -> PipelineTemplate
```

| Parameter | Meaning |
|---|---|
| `name` | pipeline name, recorded on every row |
| `*tasks_or_chain` | one chain (`a \| b \| c`), or several `TaskSpec`s as separate arguments |
| `tags` | free-form dict stored with each pipeline, for filtering later |
| `include_code` | when `True` (default), each task's source digest is part of the pipeline's identity |
| `registry` | a custom `CodecRegistry` for non-JSON artifact types |
| `control` | which task may hand off where (see [handoffs](#handoffs-opt-in)); `None` (the default) keeps the pipeline an ordinary linear chain |

```python
from pyattacker import pipeline

template = pipeline("qa", prepare | ask | judge, tags={"bench": "mmlu"})
template = pipeline("qa", prepare, ask, judge)          # equivalent
template = pipeline("qa", ask)                          # a single task is a valid pipeline
```

The chain is validated **here**, not mid-run: if `prepare` returns `dict` and the next task requires `int`,
this raises `PipelineBuildError` immediately. Checking uses annotations — subclasses are accepted, `Any` or
a missing annotation is permissive, and a bare container accepts its parameterised form. A `Handoff` member
in a `returns` annotation is an escape: `-> Handoff | Report` is checked as `Report`, and `-> Handoff` alone
chains with anything (a task on that path produces no artifact at all).

`include_code=False` is for the case where you deliberately want a task's body to change without abandoning
existing checkpoints. The default is the safe direction: edited code means a new pipeline.

### `PipelineTemplate`

| Attribute / method | Returns |
|---|---|
| `name`, `tags`, `tasks`, `spec_digest` | the declaration |
| `n_tasks` | how many tasks in the chain |
| `task_names` | their names in order |
| `describe()` | a JSON-ready summary (what `validate` prints), including the resolved `control` block when there is one |
| `control` | the resolved edge plan, or `None` |
| `bind(seed, *, key=None, repeat=0)` | one `PipelineSpec` from one seed |
| `map(seeds, *, repeats=1, key_of=None)` | a lazy iterator of `PipelineSpec` |

#### `map`

```python
map(seeds, *, repeats=1, key_of=None) -> Iterator[PipelineSpec]
```

| Parameter | Meaning |
|---|---|
| `seeds` | **any iterable**, including a generator — it is consumed lazily |
| `repeats` | k independent pipelines per seed: pass@k and self-consistency sampling |
| `key_of` | `Callable[[seed], str]` supplying your own stable ids instead of content-addressed ones |

```python
runner.run(template.map(rows))                              # one pipeline per row
runner.run(template.map(rows, repeats=5))                   # pass@5
runner.run(template.map(rows, key_of=lambda r: r["qid"]))   # ids from your dataset's primary key

def stream():                                                # streams seeds; max_admitted bounds unfinished pipelines
    with open("dataset.jsonl") as fh:
        for line in fh:
            yield json.loads(line)

runner.run(template.map(stream()))
```

With `repeats>1` and `key_of`, keys become `f"{key}#{repeat}"`.

### `PipelineSpec`

One pipeline waiting to run: the seed plus the chain. `spec.pipeline_id` (also `spec.key`) is the
content-addressed identity that makes resume and sharding work.

| Attribute | Meaning |
|---|---|
| `pipeline_id` / `key` | `blake2b` of the spec digest, the seed digest and the repeat index |
| `seed` | the dataset row |
| `repeat` | which sample of pass@k this is |
| `name`, `tasks`, `n_tasks`, `tags` | inherited from the template |
| `control` | the resolved edge plan, or `None` |

### `Chain`

What `a | b | c` produces. `chain.tasks` is the tuple of specs. You only need the type when writing code that
composes pipelines programmatically:

```python
from pyattacker import Chain

steps = prepare | ask
if with_judging:
    steps = steps | judge
template = pipeline("qa", steps)
```

### `compute_spec_digest`

```python
compute_spec_digest(tasks, *, include_code=True, control=None) -> str
```

The task-chain fingerprint (`v2:` followed by a 32-character hex digest). Useful for checking whether a code change would invalidate existing checkpoints
before you run anything:

```python
from pyattacker import compute_spec_digest

if compute_spec_digest(new_chain.tasks) != stored_digest:
    print("this chain will start fresh pipelines, not resume the old ones")
```

The digest covers each task's name, resource, `timeout_s`, source digest, and the five retry fields that
change how long a step takes (`max_attempts`, `base`, `factor`, `cap`, `jitter`). It deliberately excludes
`on`, `retry_unknown` and `max_total_s`.

A `control` block is folded in **only when it is present**, so this feature changed no existing digest: a
control-free pipeline keeps the exact identity (and therefore the pipeline ids, checkpoints and shard
assignment) it had before handoffs existed. Declared edges are digested in their resolved form — seq
destinations, sorted — so spelling a target as a name or as a seq is the same pipeline.

---

<a id="advanced-handoffs-opt-in"></a>

## Handoffs (Opt-In)

**Handoffs, forward jumps and rewinds are supported control-flow features, enabled explicitly through `control`.**

A task may *skip ahead* by returning a framework-owned directive instead of a value; the pipeline
continues at a declared later position (or finishes on the spot) and the framework records the jump durably.
A pipeline that declares nothing here is completely unaffected — see the design document
[§4.8](design.md#48-handoffs--declared-forward-jumps-opt-in) for the full reasoning.
Backward traversal — `Handoff.rewind`, `Handoff.retry_all` and optional payload history — is a *separately
declared* part of the same feature: see [backward traversal](#backward-traversal-rewind-retry-all-visits).

### `Handoff`

```python
Handoff.to(target, value=UNSET, *, reason="") -> Handoff
Handoff.end(value=UNSET, *, reason="") -> Handoff
```

| Field / method | Meaning |
|---|---|
| `target` | a task name, a task's seq, or `None` for `END` |
| `value` | the target's entry state. `UNSET` (the default) means "reuse the artifact this task received"; `None` is a real payload |
| `reason` | free-form string, recorded in the ledger and the `pipeline.handoff` event |
| `is_end` | whether this directive finishes the pipeline |
| `reuses_input` | whether the target enters with this task's own input artifact |

```python
from pyattacker import Handoff, TaskContext, task

@task("judge")
async def judge(value: Verdict, ctx: TaskContext) -> Handoff | Report:
    if value.good_enough:
        return Handoff.end(value.as_report(), reason="already good enough")
    if not value.needs_metrics:
        return Handoff.to("report", value.as_report(), reason="metrics not needed")
    return await write_report(value)

template = pipeline("qa", retrieve | ask | judge | report,
                    control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})
```

Rules that are worth knowing before you use it:

* **A handoff is a return, never a failure.** The retry policy is not consulted (no `decision` reason is
  added, and `retry.on=(Exception,)` / `retry_unknown=True` cannot turn it into a retry), `async with
  ctx.acquire(...)` has already released its leases, and a cancelled or timed-out attempt never reaches the
  return. Under `strict_leases=True` a leaked lease fails the task, and the handoff is not honoured.
* **Edges are declared, not derived.** Returning a `Handoff` with no `control` block, or along an edge that
  was not declared from *that* task, is a `FatalError` (never retried, never a silent jump).
* **Forward only.** A destination must be strictly later than its source. `"end"` is a valid destination
  except from the last task, where it has no effect and is refused.
* **Repeated task names need a seq.** "ask" appearing twice is refused as ambiguous; target seq `2` instead.
* **The payload is not type-checked.** A handoff argument is an arbitrary value, not the source's normal
  return type, and it is encoded with the pipeline's `CodecRegistry` like any artifact.
* **A handoff never comes from one of N branches.** `fanout` rejects a directive returned by a branch,
  because a group is one step in the record and a control transfer cannot be attributed to one of several
  concurrent branches.
* **The cursor becomes a position.** On a control-enabled pipeline `n_tasks_done` is where execution is, not
  how many tasks ran, and skipped slots have no task rows. Do not render it as a completion percentage; the
  handoff count is exposed next to it instead.
* **Public API.** `Handoff`, `control` and the guarantees above follow the project's versioning policy.
  Backward traversal is *separately declared* rather than part of this
  forward model — see [backward traversal](#backward-traversal-rewind-retry-all-visits).

### What a hop records

| Where | What it says |
|---|---|
| `tasks.state = "handed_off"` | the source task ended cleanly and produced no artifact |
| `attempts.outcome = "handed_off"` | the attempt that jumped, with an empty `decision` |
| `artifacts` | the entry state: the reused artifact, or a new payload at `seq = n_tasks + k` |
| `handoffs` | the ledger row: from/to, `entry_artifact_id`, `entry_reused`, `reason` |
| `pipeline.handoff` event | the audit trail (a hard kill can lose it; the ledger is authoritative) |

`HandoffRecord` (exported from `pyattacker`) is that ledger row: `handoff_id`, `pipeline_id`, `run_id`,
`from_seq`, `from_task`, `to_seq`/`to_task` (`None` for `END`), `entry_seq`, `entry_artifact_id`,
`entry_reused`, `reason`, `ts`.

---

<a id="advanced-backward-traversal-rewind-retry-all-visits"></a>

## Backward traversal (rewind, retry-all, visits)

**Rewind and retry-all are supported control-flow features, enabled explicitly through `control`.** Backward operations
are declared separately from the forward model above, so a pipeline that declares none keeps its identity,
its visit-0 artifact addresses, its random stream and its `spec_digest`. Read this section when a station
decides that an **earlier** station must run again with state the author chooses — regenerate after a failed
validation, retry a step with different parameters — and both runs have to stay on the record instead of
collapsing into one task. The [tutorial](tutorial.md#step-16--regenerating-with-rewind-and-retry-all)
builds up to it in two steps, the second one covering
[payload history](tutorial.md#step-17--payloads-that-carry-their-own-history).

### `Handoff.rewind` and `Handoff.retry_all`

```python
Handoff.rewind(target, value, *, reason="") -> Handoff   # explicit state is required; None is a value
Handoff.retry_all(*, reason="") -> Handoff               # restart at seq 0 from the original bound seed
```

| Field / method | Meaning |
|---|---|
| `target` | a task name or a task's seq, always **strictly earlier** than the source |
| `value` | the target's entry state. `rewind` requires it; `retry_all` takes none |
| `reason` | free-form string, recorded in the handoff ledger and the `pipeline.handoff` event |
| `operation` | `"rewind"` or `"retry_all"`: what the ledger row says this transfer was |

Both are returned from a task exactly like `Handoff.to` and `Handoff.end`, and the forward rules carry
over: a handoff is a return value and never a failure (the retry policy is not consulted, leases are
already released, a cancelled or timed-out attempt never reaches the return), a directive can never escape
from a `fanout` branch, and an undeclared or malformed one is a `FatalError` rather than a silent jump.
What differs:

* **You choose the state; the framework rolls nothing back.** `rewind` requires an explicit value — `None`
  is a real value, not "reuse the input" — and the destination must be a declared, strictly earlier task,
  named by a unique task name or by its seq. Self-rewind and `end` as a rewind target are refused, a name
  that appears twice in the chain must be given as a seq, and results **before** the target stay effective
  while the target and everything after it become historical and run again with their own visits.
* **Forward stays forward.** `Handoff.to()` keeps using `control.edges` and never acquires implicit
  backward semantics; the forward declaration is optional for a backward-only pipeline.
* **`retry_all` replays the bound seed.** It restarts at seq 0 from the seed **freshly decoded from the
  bytes captured at binding**, so mutating a task's input or `spec.seed` afterwards does not change what
  runs. It takes no replacement value: to restart with *different* state, use `Handoff.rewind(0,
  chosen_state)` from a later task. It may be declared on the first task, including a single-task pipeline,
  and it does not create another mapped row or repeat, reset resources or start another CLI run — it clears
  the effective task results while preserving visits, artifacts, attempts and the consumed control budget.
* **Exception retry is a different mechanism.** `Retrying` retries an attempt inside the same visit;
  `rewind` and `retry_all` are returned control directives and never invoke the failure retry policy.
  Authoring mistakes and budget exhaustion are fatal failures that policy cannot retry.

```python
# reference/backward_rewind.py
"""Backward traversal in one program: a validator sends the work back to the generator."""

from pyattacker import Handoff, Runner, pipeline, task


@task("generate")
def generate(state: dict, ctx) -> dict:
    # <- your model call; a revisit is a genuinely new sample
    return {**state, "sample": ctx.visit, "ok": ctx.visit >= 1}


@task("validate")
def validate(row: dict, ctx) -> Handoff | dict:
    if not row["ok"]:
        # Explicit state: the author decides what the generator receives next.
        return Handoff.rewind("generate", {"prompt": row["prompt"], "feedback": "retry warmer"},
                              reason="invalid sample")
    return row


template = pipeline(
    "rewind",
    generate | validate,
    control={
        "rewind": {"validate": ["generate"]},   # a strictly earlier destination, declared
        "max_handoffs": 3,                      # required: the loop budget for this pipeline
    },
)

with Runner(store=":memory:", max_handoffs=10) as runner:   # the runtime ceiling; the lower limit wins
    report = runner.run(template.map([{"prompt": "Return JSON"}]))
    store = runner.store
    record = next(iter(store.pipelines()))
    print(report.stats["pipelines"]["by_state"], "position:", record.n_tasks_done)
    print([(row.seq, row.visit, row.name, row.state) for row in store.tasks(record.pipeline_id)])
    print("budget consumed:", store.visit_state(record.pipeline_id)["handoffs"])
```

### The `control` block

| Key | Shape | Meaning |
|---|---|---|
| `edges` | `{source: [later targets]}` | forward jumps (`Handoff.to` / `Handoff.end`); optional when only backward operations are declared |
| `rewind` | `{source: [strictly earlier targets]}` | enables `Handoff.rewind` from each source |
| `retry_all` | `[sources]` | enables `Handoff.retry_all` from each source |
| `max_handoffs` | positive integer | **required** once `rewind` or `retry_all` is present: this pipeline's finite control budget |

`max_handoffs` is validated like every other declaration: `3.0`, `True`, `"3"` and `0` are all refused, and
a `max_handoffs` with no backward operation is refused too (`control: max_handoffs requires backward
operations`). `RunConfig.max_handoffs` (default 1000, spelled `run.max_handoffs` in a config) is the
**runtime ceiling**: the effective limit is `min(control.max_handoffs, run.max_handoffs)`, so a run can
lower a pipeline's budget and can never raise it. A fresh start resets it. Names and seqs resolve exactly
as they do for `edges` (an exact task name wins over a numeric string, a repeated name must be given as a
seq), and every problem is reported as a configuration field path — see
[CLI → backward control declarations](cli.md#backward-control-declarations).

```python
# reference/backward_retry_all.py
"""Retry-all restarts the pipeline from the seed the run was bound to."""

from pyattacker import Handoff, Runner, pipeline, task

entries = []


@task("prepare")
def prepare(seed: dict, ctx) -> dict:
    entries.append(ctx.visit)   # a fresh entry after retry-all, not a retry of a failed attempt
    return {"prompt": seed["prompt"], "prepared": True}


@task("check")
def check(row: dict, ctx) -> Handoff | dict:
    if ctx.visit == 0:
        return Handoff.retry_all(reason="new preparation")   # source declared in control.retry_all
    return row


template = pipeline("retry-all", prepare | check,
                    control={"retry_all": ["check"], "max_handoffs": 2})

with Runner(store=":memory:") as runner:
    report = runner.run(template.map([{"prompt": "one row"}]))
    store = runner.store
    record = next(iter(store.pipelines()))
    print(report.stats["pipelines"]["by_state"])
    print("prepare entries:", entries)
    print("transfers:", store.visit_state(record.pipeline_id)["handoffs"])
```

### Visits and artifact occurrences

`ctx.visit` starts at 0 for **each station** and increases on every fresh entry into that station — an
ordinary successor entry after a rewind counts too — while `ctx.attempt` numbers attempts *within one
visit*. Together they are what keeps a regeneration visible instead of hidden.

| Where | What it says |
|---|---|
| `pipeline_id:seq` | the artifact id of visit 0; a revisit uses `pipeline_id:seq#visit` |
| `TaskRecord.visit`, `AttemptRecord.visit`, `Artifact.visit` | which occurrence a row is |
| `store.get_artifact(pipeline_id, seq)` | the **effective** output of that station, at its current visit |
| `store.get_artifact_by_id(artifact_id)` | one **exact historical** occurrence, superseded ones included |
| `store.visit_state(pipeline_id)` | cursor, pending entry, effective slots, per-seq counters, consumed handoffs |
| `PipelineRecord.n_tasks_done` | a *position* in a backward pipeline, not a completion count |

Visits participate in the derived randomness: the RNG and `ctx.seed` are byte-identical to the old
derivation for visit 0 and include the visit on revisits, so a regeneration samples differently. They do
not make external side effects exactly-once — include `ctx.visit` in an idempotency key when each
regeneration should be a new external operation (see [External side effects](#external-side-effects)).

A pending entry references its exact input, and a resume keeps its visit and continues the consumed
attempt numbering: an attempt number is reserved before task code runs, so a hard kill can leave a gap in
completed attempt rows but can never reuse a number. Framework recovery is at least once for uncommitted
work.

### Budgets and termination

Backward traversal is no longer structurally finite, which is why the budget is explicit and mandatory.
Every nonterminal transfer in a backward-enabled pipeline counts, including forward `edges` transfers, and
**N permits exactly N transfers**: the N+1-th fails *before* publishing a transition or invalidating
results, so the record still describes exactly what committed. `END` can finish at the limit without
consuming another transfer.

The consumed count survives a resume, a retry-all and the automatic missing-payload seed fallback; only an
explicit fresh start begins a new budget lifecycle. Visits and audit rows survive that restart too, so
historical occurrences stay addressable.

### Recovery and ownership

What opening a stored row does depends on the row, and the rules are meant to be read, not inferred:

| stored row | what this run does |
| --- | --- |
| `failed`, `interrupted` | ordinary checkpoint recovery: the exact durable visit — visit number, pending entry, consumed attempt numbering — continues |
| `running`, `resume=True` | the operator's claim that the previous owner is gone. `interrupt_stale` reclaims heartbeat-stale rows first; the exact durable visit then continues |
| `running`, no `resume` | **skipped**, never taken over: a durable pending visit can be continued after a crash, so a second writer would fork one traversal. `pipeline.skipped` carries `reason="owned_by_another_run"` plus the owner's run id |
| rows durable, traversal gone | refused: `corrupt visit checkpoint: missing traversal state`. `fresh_restart=True` is the documented way to discard it and start over |
| `succeeded` | skipped, unless `retry_succeeded=True`; a restart then runs from the bound seed with a fresh budget |

`fresh_restart=True` is the one switch that discards durable progress: it clears the effective traversal
and any pending entry (settling every task row it leaves in flight as `interrupted`), starts again from the
immutable bound seed, resets the control budget and invalidates the previous ledger — while visit counters
and visit-qualified audit rows stay, so historical occurrences remain addressable, and a store whose
traversal was lost has its counters rebuilt from those rows. It applies to forward pipelines too, where it
means "ignore the checkpoint, run the chain again": there the append-only history survives as well, but
task and chain-artifact addresses are reused by design rather than kept as separate occurrences (the
[store recovery contract](#tables-and-readers) spells that difference out). Combine it with
`retry_succeeded=True` to restart a pipeline that already succeeded. A fresh start emits
`pipeline.restarted` (with the discarded cursor), not `pipeline.checkpoint_missing`: nothing was lost.

A missing or unavailable pending payload emits `pipeline.checkpoint_missing` and establishes a seed replay,
preserving budget and counters. The first execution of a pipeline never takes that path — its input is the
bound seed it already holds, so a `journal="summary"` store (whose written seed payload is intentionally
dropped) does not report a checkpoint failure it never had. Summary journals and `null` backends can run
loops in-process, but they cannot resume their missing payloads. A pending rewind to seq 0 uses its chosen
payload rather than triggering a seed reset. Backward transfers requeue through the timer pump, releasing
the worker for other pipelines.

### Inspecting a backward run

Pipeline exports add a `control` traversal record only for backward-enabled pipelines. It contains
`cursor`, `pending`, effective `active` slots, durable `counters`, `handoffs` consumed, `version`, the exact
current `input` and the `terminal` reference. Nested task and artifact rows include ids, visits and
`active` markers, and individual task/attempt/artifact exports include `visit` (attempts also carry their
task-run id). The existing closed set of top-level export row kinds is unchanged. The HTTP `/pipelines`
view includes traversal state and `cursor_kind="position"` for backward-enabled rows; a forward-only row
gets neither.

A report is scoped to the run it covers and counts that run's repeated visits and attempts, so after a
resume it shows the new run's workload while the store and the export retain every earlier row. Those
totals are workload, not completion percentages.

### Store capability

The optional store methods behind this section — `visit_state`, `reset_visits`, `commit_entry`,
`commit_visit_attempt`, `commit_visit_success`, `commit_control_transition`, `repair_visit_terminal` and
`get_artifact_by_id` — are specified under
[Stores → optional store capabilities](#optional-store-capabilities). They sit outside the base `Store`
protocol, so a third-party backend keeps working for ordinary pipelines; opening a backward-enabled
pipeline on a store that fails the probe is refused with a `ConfigError` up front instead of being
downgraded to a non-durable loop.

The same feature owns the store's **feature level** and the compatibility rules that come with it: a store
stays at `base` until the first revisit is committed, a build that opens an unknown level refuses the store
outright, and a SQLite store at `visits-v1` arms a writer guard against lineage-unaware writers. See
[Stores → store compatibility and backups](#store-compatibility-and-backups).

---

## Running

### `Runner`

```python
Runner(*, store=":memory:", pools=(), concurrency=16, clock=None, bus=None,
       registry=None, config=None, **config_overrides)
```

The scheduler. It owns the store, the pools and the worker slots. Any `RunConfig` field can be passed
directly as a keyword argument.

```python
from pyattacker import Runner

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(rows))
    print(report.summary())
```

Use it as a context manager. Closing it closes the store, so read a **file-backed** store's rows *inside*
the `with` block, or reopen the file afterwards with `open_store`.

| Method | Purpose |
|---|---|
| `run(specs, *, resume=False, **overrides)` | run to completion, returns a `RunReport`. Wraps `run_async` in `asyncio.run` |
| `await run_async(specs, *, resume=False, **overrides)` | same, inside an existing event loop |
| `stats()` | live snapshot; safe to call mid-run |
| `report_metric(name, value, *, label="", display="number", pipeline_id=None)` | publish an application-defined latest value |
| `stop(reason="user")` | ask the run to stop gracefully: stop admitting, drain what is in flight |
| `stopping` | whether a stop is in progress |
| `run_id` | the current run's id |
| `add_pool(pool)` / `pool(name)` | register or fetch a pool |
| `close()` | close the store (the context manager does this) |

```python
report = runner.run(template.map(rows), resume=True)              # resume
report = runner.run(template.map(rows), concurrency=8)            # override for one call

live = runner.stats()                                             # while running
print(live["in_flight_pipelines"], live["delayed_pipelines"])      # in flight vs parked in backoff
```

`run()` reads a `PipelineSpec` iterator only while admission capacity is available.
`max_admitted=None` (default) resolves to `4 * concurrency`; a positive integer sets
an explicit limit on queued, executing and delayed pipelines **together**. Retries
retain their admission and can return to workers while new input is paused. Lower
limits reduce memory held by unfinished work, but can leave workers idle during
backoff; a limit below concurrency also limits useful parallelism. Suite members
share one budget per Runner; shard processes each have their own budget.

This bounds the number of unfinished scheduler states, not total bytes or process
RSS. Memory also includes each retained seed/current value, source-side buffering,
worker references, write-behind buffers, caches and reporting queries. The default
`MemoryStore` retains accumulated records and artifact payloads as the run grows;
use a file-backed store and a streaming source for large datasets. A pre-built input
list or unbounded task payload remains outside the admission guarantee.

`runner.stats()` exposes `admitted_pipelines` (admitted minus terminal) and the
effective `max_admitted`; the run's persisted config records that effective limit.
Stop, cancellation and run budgets also interrupt admission waits. As before,
synchronous input iteration and blocking task code must return control before the
async scheduler can respond.


### `RunConfig`

Everything that shapes one run. Pass a `RunConfig`, or pass its fields as keyword arguments to `Runner`.

| Field | Default | Meaning |
|---|---|---|
| `store` | `":memory:"` | `":memory:"`, a SQLite path, a plugin URI, or an open store instance |
| `journal` | `"full"` | `"full"` keeps artifact payloads (**required for task-level resume**); `"summary"` keeps only metadata |
| `concurrency` | `16` | max attempts in flight; a pipeline parked in a retry backoff does not hold a slot |
| `max_admitted` | `None` | queued + executing + delayed pipeline limit; auto = `4 * concurrency`, otherwise a positive integer |
| `label` | `""` | a label recorded on the run |
| `run_id` | `None` | explicit run id; default is timestamp + digest |
| `resume` | `False` | mark pipelines abandoned by dead runs as resumable before scheduling |
| `retry_succeeded` | `False` | re-run pipelines already marked succeeded. Eligibility only: it never discards the checkpoint or traversal of a pipeline that has not succeeded |
| `fresh_restart` | `False` | start admitted pipelines over from the bound seed: discard checkpoint/traversal, reset the control budget. Append-only history survives; a backward pipeline additionally keeps its visit occurrences and counters |
| `heartbeat_s` | `5.0` | how often the run's heartbeat is written |
| `grace_s` | `5.0` | how long a graceful shutdown waits before cancelling workers |
| `stale_after_s` | `30.0` | a running pipeline from a run whose heartbeat is older than this is considered abandoned |
| `strict_leases` | `False` | a leaked lease fails the task (`LeaseLeakError`) instead of being reclaimed quietly |
| `stop_after_failures` | `None` | stop admitting work after N failures (best-effort) |
| `stop_after_s` | `None` | stop admitting work after this much wall-clock time |
| `handle_signals` | `True` | install SIGINT/SIGTERM handlers calling `stop()` |
| `write_behind` | `None` | batch append-only facts; `None` means on for file-backed stores |
| `write_batch` | `128` | batch size when write-behind is active |
| `flush_interval` | `1.0` | seconds between flushes |
| `artifact_backend` | `None` | where payloads live: `None`/`"inline"`, `"file:///path"`, `"null"`, or a spec dict |
| `max_handoffs` | `1000` | runtime ceiling on nonterminal control transfers in backward-enabled pipelines; the effective limit is `min(control.max_handoffs, this)` (see [backward traversal](#backward-traversal-rewind-retry-all-visits)) |
| `notes`, `meta` | `""`, `{}` | free-form, recorded on the run |

```python
from pyattacker import RunConfig, Runner

config = RunConfig(store="runs/qa.db", concurrency=64, journal="full",
                   strict_leases=True, stop_after_failures=50)

with Runner(config=config, pools=[pool]) as runner:
    report = runner.run(template.map(rows))
```

`stop_after_failures` is evaluated at admission and at each completion, so pipelines already admitted still
finish — it stops the bleeding rather than rewinding time.

### `RunReport`

What `run()` returns.

| Attribute / method | Meaning |
|---|---|
| `run_id`, `status`, `duration_ms` | run identity and outcome |
| `stats` | the full statistics dict, including `stats["pipelines"]["by_state"]` |
| `skipped` | how many pipelines were skipped because they had already succeeded, or (backward traversal) because a `running` row belongs to another run and `resume` did not claim it |
| `leases_leaked` | how many leases had to be force-reclaimed |
| `repair_failures` | pipelines this run could not settle out of a torn terminal state; run-local, so it is the only place such a failure is visible (the row keeps its original owner) |
| `stop_reason` | why the run stopped early, if it did |
| `summary()` | a human-readable multi-line report |
| `to_dict()` | the same facts, machine-readable |
| `export_jsonl(path, *, scope="store", run_id=None)` | write pipeline rows, returns the count |

```python
report = runner.run(template.map(rows))

print(report.summary())                                  # for a human
metrics = report.to_dict()                               # for a dashboard
print(report.stats["pipelines"]["by_state"])             # {'succeeded': 98, 'failed': 2}

failed = report.stats["pipelines"]["by_state"].get("failed", 0) + report.repair_failures
if report.status != "completed" or failed:
    report.export_jsonl("runs/failures.jsonl")
    raise SystemExit(1)                                  # fail your CI job
```

### Worker liveness

Completion is counted, so a run needs every admitted pipeline to reach a terminal state. A worker that dies
outside its own handlers — a `BaseException` that is not `CancelledError`, raised by the framework's own code
or by something it calls, such as a third-party store hook — can no longer reach one, and the run would
otherwise wait forever. Instead:

* the run is hard-stopped (`stop_reason == "worker_crashed"`), so peer workers are cut off too and nothing
  new is admitted;
* the pipeline the dead worker was holding is recorded `failed` — or `interrupted` if the run was already
  stopping — with the escaping exception on the row; a pipeline that was already terminal keeps its state,
  which is read from the store rather than from the runner's in-memory record (a store need not write back
  into the record it was handed, so the persisted row is the one that counts);
* a `runner.worker_crashed` event carries the pipeline, the exception and its traceback;
* `run_async` raises `WorkerCrashed` (after the run record is closed as `interrupted`), with the original
  exception as `__cause__`. `run()` propagates it and the CLI exits 2 with a named error, so a crashed worker
  cannot look like a completed run.

```python
from pyattacker import WorkerCrashed

with Runner(store="runs/qa.db", pools=[pool]) as runner:
    try:
        report = runner.run(template.map(rows))
    except WorkerCrashed as exc:
        print(f"lost {exc.pipeline_id}: {exc.__cause__!r}")   # the run's record is already durable
        raise
```

A `BaseException` raised *by a task* never reaches this path (the task's own handling contains it like an
ordinary exception), and cancellation keeps its semantics: a cancelled worker records `interrupted` and
re-raises. `KeyboardInterrupt` and `SystemExit` are the one boundary: asyncio stops the loop before any
supervisor can run, so the worker records the row and the event itself and the interrupt still propagates
unchanged — the caller sees its own interrupt, and only that run record is left unfinishable.

---

## Resources

A `Resource` is one concrete external capability — an endpoint, an API key, a local worker. A `Pool` is a
group of them plus a default acquire policy. Pools are the **only** state shared between pipelines.

### `Resource`

```python
Resource.create(kind="generic", *, id=None, options=None, tags=None, capacity=1,
                factory=None, degrade_after=3, dead_after=8, cooldown_s=30.0, **meta) -> Resource
```

| Parameter | Default | Meaning |
|---|---|---|
| `kind` | `"generic"` | a category you choose (`"llm"`, `"gpu"`, …); selectable |
| `id` | generated | stable identifier, recorded on every lease |
| `options` | `{}` | the config your factory reads: base url, key, model. **Selectable, including dot paths** |
| `tags` | `{}` | extra selectable labels |
| `capacity` | `1` | concurrent leases **this one resource** allows |
| `factory` | `None` | `Callable[[Resource], client]`, called once per resource, lazily, on first lease |
| `degrade_after` | `3` | consecutive failures before the resource is circuit-broken |
| `dead_after` | `8` | consecutive failures before it is marked dead |
| `cooldown_s` | `30.0` | how long a degraded resource stays out of rotation |

```python
from pyattacker import Resource

def build_client(res: Resource):
    return OpenAI(base_url=res.options["base_url"], api_key=res.options["api_key"])

resources = [
    Resource.create("llm", id=f"key-{i}", capacity=8, factory=build_client,
                    options={"base_url": "https://api.example/v1", "api_key": key,
                             "model": "gpt-4o", "quota": {"tokens": 2_000_000}},
                    tags={"tier": "prod"})
    for i, key in enumerate(api_keys)
]
```

`capacity` is per resource, so total pool capacity is the sum. Compare *that* against `concurrency`: more
workers than capacity means workers waiting on the pool.

The factory is where your client lives, built once and shared by every lease of that resource. If it raises,
the lease is refused with `ResourceUnavailable` (never a lease whose `client` is `None`), a
`resource.factory_failed` event is recorded, and repeated failures move the resource through the same
degrade/dead path. A cooldown clears the stored error, so a transient factory failure gets a genuine retry.

| Attribute / method | Meaning |
|---|---|
| `id`, `kind`, `options`, `tags`, `capacity`, `meta` | as constructed |
| `spec()` | a JSON-ready description with secrets masked |
| `lookup(key)` | `(found, value)` for a selector key, including dot paths |

### `Pool`

```python
Pool(name, resources=(), *, kind=None, algorithm=None, bus=None, clock=None,
     deadlock_warn_s=5.0, on_event=None)
```

| Parameter | Default | Meaning |
|---|---|---|
| `name` | — | how tasks refer to it (`resource="apis"`) |
| `resources` | `()` | the resources it starts with |
| `kind` | `None` | a default kind for resources added later |
| `algorithm` | `Wait()` | the default acquire policy for tasks that do not override it |
| `deadlock_warn_s` | `5.0` | emit `acquire.suspected_deadlock` when a task holding one of this pool's resources waits this long for another |
| `on_event` | `None` | a callback for every pool event |

```python
from pyattacker import Pool

pool = Pool("apis", resources, algorithm="backoff")
judges = Pool("judges", [Resource.create("llm", id=f"j-{n}", capacity=1, options={"model": n})
                         for n in ("judge-a", "judge-b", "judge-c")],
              algorithm="least_busy")

with Runner(store="runs/qa.db", pools=[pool, judges], concurrency=32) as runner:
    ...
```

| Method | Purpose |
|---|---|
| `add(resource)` | add one at runtime; waiters whose selector matches are woken |
| `revoke(resource_id, reason="")` | withdraw one |
| `resources()` | the current list |
| `stats(**selector)` | aggregate `PoolStats` |
| `snapshot()` | per-resource dicts: state, leases, active, capacity |
| `subscribe(events=None)` | async iterator of pool events |

```python
print(pool.stats().utilization, pool.stats().waiting)
for slot in pool.snapshot():
    print(slot["id"], slot["state"], f"{slot['active']}/{slot['capacity']}")
```

Both are safe to call during a run — that is what `watch` and `serve` do.

Adding endpoints while a run is in flight works, and waiting pipelines pick them up:

```python
@task("discover")
async def discover(row: dict, ctx) -> dict:
    for endpoint in await find_new_endpoints():
        ctx.publish_resource("apis", Resource.create("llm", capacity=4, options=endpoint))
    return row
```

### `Lease`

What `ctx.acquire(...)` yields. One lease is one use of one resource.

| Attribute | Meaning |
|---|---|
| `client` | the object your factory built — this is what you call |
| `resource` | the `Resource` behind it |
| `options` | shortcut for `lease.resource.options` |
| `held_ms` | how long this lease has been held |
| `task_name` | the task holding it |

#### `lease.report`

```python
lease.report(*, ok=True, latency_ms=None, usage=None, error=None) -> None
```

**How the pool learns anything.** The framework does not guess whether a failure was the endpoint's fault, so
a healthy endpoint is never circuit-broken because your JSON parser choked.

| Parameter | Effect |
|---|---|
| `ok=False` | counts a consecutive failure, feeding `degrade_after` / `dead_after` |
| `ok=True` | clears the failure counter — a recovery, and it can revive a degraded or dead resource |
| `latency_ms` | maintains an EMA per resource |
| `usage` | accumulates quota counters, which `quota_aware` ranks on |
| `error` | recorded for diagnosis |

```python
async with ctx.acquire(model="gpt-4o") as lease:
    try:
        started = time.perf_counter()
        response = await lease.client.chat(row["q"])
    except ProviderOverloaded as exc:
        lease.report(ok=False, error=exc)          # the endpoint's fault: count it
        raise RetryableError("overloaded", error_class="upstream") from exc
    except json.JSONDecodeError:
        lease.report(ok=True)                       # our parsing bug, not the endpoint's
        raise
    lease.report(ok=True, latency_ms=(time.perf_counter() - started) * 1000,
                 usage={"tokens": response.usage.total_tokens})
    return {"a": response.text}
```

| Other method | Purpose |
|---|---|
| `degrade(reason="")` | take this resource out of rotation now — e.g. you know its quota is gone |
| `release_now()` | synchronous release; the escape hatch's counterpart, not needed with `async with` |

### `PoolStats`

Returned by `pool.stats()` (defined in `pyattacker.resource`, not exported at top level).

| Field | Meaning |
|---|---|
| `active`, `capacity`, `utilization` | current occupancy |
| `ready`, `degraded`, `dead` | resource counts by state |
| `waiting` | how many acquirers are queued right now |
| `leases_total`, `ok_total`, `failed_total` | throughput |
| `total`, `revoked` | resource counts |
| `leaked_total` | leases that had to be force-reclaimed |
| `waits_total`, `wait_ms_avg`, `wait_ms_p50`, `wait_ms_p95`, `wait_ms_max` | how long acquiring takes |
| `usage` | accumulated `lease.report(usage=...)` counters |

`wait_ms_p95` climbing is the signal that your pool is the bottleneck rather than the provider.

### `Bus`

A lightweight cross-pipeline signal bus, for the push side of coordination.

```python
count = ctx.bus.publish("found_answer", qid=row["qid"])      # returns subscriber count

async for message in ctx.bus.subscribe("found_answer"):      # "*" for everything
    ...
```

### `ResourceState`

`READY`, `DEGRADED`, `DEAD`, `REVOKED`. A string enum, so `str(state)` reads `"ResourceState.READY"` in logs
while the wire format uses `.value`.

Transitions: `READY` → `DEGRADED` after `degrade_after` consecutive failures, for `cooldown_s`; → `DEAD`
after `dead_after`. A successful `report(ok=True)` pulls a degraded or dead resource back to `READY`. A
cooldown expiring does *not* reset the failure counter — only success does.

### `ResourceEvent`

One state change in a pool, fed simultaneously to waiters, subscribers, the `events` table and the monitoring
snapshot. Fields: `kind`, `pool`, `resource_id`, `data`, `ts`, plus `as_dict()`.

---

## Acquire algorithms

An algorithm decides **how to get a resource out of a pool**. It is a separate axis from retry, which decides
what to do **after work fails**. Set one per pool (`Pool(..., algorithm=...)`), per task
(`@task(algorithm=...)`), or per call (`ctx.acquire(algorithm=...)`).

Every algorithm is available by name, so `algorithm="backoff"` and `algorithm=Backoff(cap=60)` are both
valid; the string form is what YAML configs use.

| Algorithm | When nothing is free | Use it for |
|---|---|---|
| `Wait()` **(default)** | queues until a slot frees or `timeout` expires | steady-state throughput |
| `Backoff()` | waits with exponential backoff + jitter | a saturated provider; avoids a thundering herd on release |
| `LeastBusy()` | picks the lowest load ratio, then falls back | several endpoints of unequal capacity |
| `Failover()` | tries pools in order, then falls back | primary/backup providers, tiered keys |
| `Sticky()` | prefers the resource this pipeline already used | prompt caches, warm connections |
| `QuotaAware()` | ranks by remaining quota | budget-limited endpoints |
| `Immediate()` | raises `ResourceUnavailable` at once | shedding load instead of queueing |

```python
Wait(timeout=None)
Backoff(base=0.2, factor=2.0, cap=10.0, jitter="full", max_wait=None)
LeastBusy(fallback=None)                 # fallback defaults to Wait()
Failover(pools=(), fallback=None)
Sticky(fallback=None)
QuotaAware(metric="tokens", reserve=0.05)
Immediate()
```

```python
from pyattacker import Backoff, Failover, Pool, QuotaAware, task

# Back off on a busy pool instead of piling up.
pool = Pool("apis", resources, algorithm=Backoff(base=0.5, cap=60.0))

# Spend the big-quota key first; declare quota in options, consume it via lease.report(usage=...).
budgeted = Pool("apis", resources, algorithm=QuotaAware(metric="tokens", reserve=0.05))

# Primary, then backup, then wait on the primary.
@task("ask", resource="primary", algorithm=Failover(pools=["primary", "backup", "spot"]))
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        return {"a": await lease.client.chat(row["q"])}
```

Three behaviours worth knowing before you rely on them:

* **`Failover` does not loop.** It tries every listed pool once, in order; if none has capacity, its
  `fallback` (`Wait()` by default) parks on `pools[0]` only. Read it as "try these, then settle on the
  primary".
* **`QuotaAware` is a preference, not a limit.** When every candidate is exhausted, the best of them is still
  handed out — refusing to work is worse than overspending. For a hard stop, track the budget in your task
  and raise.
* **`Immediate` raises `ResourceUnavailable`, which classifies as `unknown`**, and the default retry policy
  does not retry `unknown`. That is deliberate — a capacity mistake should be loud. Pair it with
  `Retrying(retry_unknown=True)` if you want capacity retries.

Acquire-time backoff and retry-time backoff are different knobs: `Backoff` decides how long to wait for a
*slot*, `Retrying` how long to wait after a *failure*.

### `resolve_algorithm`

```python
resolve_algorithm(spec) -> AcquireAlgorithm
```

Turns a name, a dict or an instance into an algorithm. Built-ins resolve before plugins, so a plugin cannot
shadow `wait`.

```python
resolve_algorithm("backoff")
resolve_algorithm({"name": "backoff", "cap": 60.0})
```

To write your own, implement the `AcquireAlgorithm` protocol (`pyattacker.algorithm`) and register it under the
`pyattacker.algorithms` entry-point group — see [Plugins](#plugins) and `examples/plugin_package/`.

---

## Errors

Failure is an ordinary exception; there is no state machine to satisfy. The framework classifies whatever you
raise and asks your retry policy what to do.

### Hierarchy

```
PyAttackerError
├── ConfigError              a config or declaration mistake (CLI exit code 2)
│   └── PipelineIdentityConflict  stored key has a different task or seed digest
├── PipelineBuildError       the task chain does not type-check
├── PluginError              a plugin failed to load or resolve
├── ArtifactCodecError       a payload could not be encoded or decoded
├── ResourceError
│   ├── ResourceUnavailable  no resource could be leased
│   │   └── AcquireTimeout   ... within the timeout
│   ├── PoolNotFound         no pool by that name
│   └── LeaseLeakError       a lease outlived its task, under strict_leases=True
├── RetryableError           you are declaring this failure retryable
├── FatalError               you are declaring this failure final
├── BudgetExceeded           a run budget was spent
├── RunInterrupted           the run was stopped
├── CorruptCheckpoint        a stored checkpoint contradicts the pipeline definition
├── StoreUnavailable         the store became untrustworthy
└── WorkerCrashed            a worker died outside its own handlers
```

### Error classes

`error_class_of(exc)` maps any exception to one of these strings:

| Class | Retried by default | Recognised from |
|---|---|---|
| `rate_limit` | yes | status 425/429 |
| `timeout` | yes | `TimeoutError`, status 408/504 |
| `connection` | yes | `ConnectionError` |
| `upstream` | yes | status 500/502/503/505/507/529 |
| `retryable` | yes | `RetryableError` without an explicit class |
| `invalid` | no | `ValueError`, `TypeError`, … |
| `fatal` | no | `FatalError`, any other 4xx |
| `cancelled` | no | cancellation |
| `unknown` | no | anything else, including `ResourceUnavailable` |

Status codes are read from `status`, `status_code`, `http_status` or `code`, falling back to
`exc.response.status_code` — which covers the usual provider SDKs without importing them.

```python
from pyattacker import error_class_of

error_class_of(HttpError(429))        # 'rate_limit'
error_class_of(TimeoutError())        # 'timeout'
error_class_of(ValueError("bad"))     # 'invalid'
```

### Steering the decision yourself

```python
from pyattacker import FatalError, RetryableError

# Retryable, with the class and the server's suggested delay.
raise RetryableError("rate limited", error_class="rate_limit", retry_after=response.headers["retry-after"])

# Never retried, whatever max_attempts says.
raise FatalError("the prompt exceeds the model's context window")
```

An `error_class` attribute on your own exception class works too, and `retry_after` is also read from a
`Retry-After` header when the exception exposes one.

Every decision is persisted, so "why did this retry five times / why did it stop" is a query, not an
archaeology exercise:

```python
for attempt in store.attempts(pipeline_id=pid):
    print(attempt.attempt_no, attempt.error_class, attempt.decision)
    # {'retry': True, 'reason': 'retryable', 'delay_s': 0.7, 'error_class': 'rate_limit', ...}
```

`reason` is one of `ok`, `retryable`, `attempts_exhausted`, `policy_declined`, `total_budget`.

---

## Artifacts and codecs

An artifact is a task's persisted output. It is written the moment the task succeeds, which is what makes the
checkpoint granularity a task rather than a pipeline.

### `Artifact`

| Field | Meaning |
|---|---|
| `pipeline_id`, `seq` | its identity; `seq=-1` is the pipeline's seed. On a control-enabled pipeline a handoff payload lives at `seq >= n_tasks` (see [handoffs](#handoffs-opt-in)), so `seq` is a task position only for the chain. In a [backward-enabled](#backward-traversal-rewind-retry-all-visits) pipeline the first occurrence keeps `pipeline_id:seq` and a revisit qualifies the id as `pipeline_id:seq#visit` |
| `task_name` | which task produced it |
| `type_name`, `codec` | how to restore it |
| `digest`, `size` | `blake2b` of the payload, and its length |
| `payload` | the encoded bytes, or `None` when they live in a backend or were dropped |
| `blob_ref` | where the bytes live when they are not inline |
| `is_final` | whether this is the pipeline's final output |
| `available` | whether the payload can actually be read back — **this is what resume checks** |
| `encoded()` | an `Encoded` triple ready for `registry.load(...)` |

```python
artifact = store.get_artifact(pipeline_id, 1)
if artifact.available:
    value = runner.registry.load(artifact.encoded())
```

Identical payloads produce identical digests, so a task returning its input unchanged costs no extra storage.

### `CodecRegistry`

Decides how values become bytes. JSON handles dicts, lists, scalars, and dataclasses; annotate a task's
return type and that class is registered automatically, so a restored checkpoint comes back as your type
rather than a dict.

| Method | Purpose |
|---|---|
| `register(codec, *, for_types=(), name=None)` | add a codec, optionally bound to specific types |
| `register_type(cls)` | register a dataclass explicitly (usable as a decorator) |
| `codec_for(obj)` | which codec would handle this value |
| `dump(obj)` / `load(encoded)` | encode / decode |
| `type_name_of(obj)` | the recorded type name |

```python
import numpy as np
from pyattacker import CodecRegistry, Runner, pipeline

class NumpyCodec:
    name = "npy"
    def can_encode(self, obj): return isinstance(obj, np.ndarray)
    def dumps(self, obj):
        buf = io.BytesIO(); np.save(buf, obj); return buf.getvalue()
    def loads(self, data): return np.load(io.BytesIO(data), allow_pickle=False)

registry = CodecRegistry()
registry.register(NumpyCodec(), for_types=(np.ndarray,))

template = pipeline("embed", embed_task, registry=registry)
with Runner(store="runs/embed.db", registry=registry) as runner:   # the same registry in both places
    runner.run(template.map(rows))
```

The registry must be given to **both** the template (for seeds) and the `Runner` (for checkpoints). A codec
that claims a payload takes precedence over an earlier registration, so a specialised codec is not dead code
behind the JSON catch-all; an explicit `for_types=` beats the scan.

Built-in codecs: `JsonCodec` (the default) and `BytesCodec` (raw `bytes`/`bytearray`). Distribute your own
under the `pyattacker.codecs` entry-point group and it installs itself.

### `HistoryArtifact`

An optional base class for a payload that carries its own named snapshots of **application** state. It is a
decoded payload, not a subclass of the persisted `Artifact` record, and nothing in the runner reads it to
decide where execution goes next: it exists so a [rewind](#backward-traversal-rewind-retry-all-visits)
can send back a state the author chose with the states it passed through still inspectable. Ordinary
dictionaries stay ordinary dictionaries — there is no automatic snapshot on task entry or completion. The
tutorial builds one in [Step 17](tutorial.md#step-17--payloads-that-carry-their-own-history).

```python
HistoryArtifact(state, *, history=None, selected=None, next_snapshot=0)
```

| Member | Meaning |
|---|---|
| `state` | the current application state, as a detached deep copy |
| `history` | every snapshot, oldest first, as detached records: `{id, label, state, metadata}` |
| `selected` | the id of the snapshot `restore` selected last, or `None` |
| `checkpoint(label, *, metadata=None)` | a new value with a detached snapshot appended; labels are unique and `snapshot:` is reserved for the stable ids (`snapshot:0`, …) |
| `with_state(value)` | a new value with the current state replaced, appending no snapshot |
| `snapshot(id_or_label)` | one detached snapshot record; an unknown or ambiguous selector raises `KeyError` |
| `restore(id_or_label)` | a new value whose state is that snapshot **and** whose `selected` names it; the whole history is retained, so later stages stay inspectable |
| `prune(*selectors)` | a new value without those snapshots; raises `ValueError` when a selector names the selected snapshot, and ids are never reused |

```python
# reference/history_artifact.py
"""HistoryArtifact: named, detached snapshots inside an application payload."""

from pyattacker import CodecRegistry, HistoryArtifact


class GenerationState(HistoryArtifact):
    pass


registry = CodecRegistry()
registry.register_type(GenerationState)   # the codec restores the subclass, not a bare HistoryArtifact

state = GenerationState({"prompt": "Return JSON", "temperature": 0.2})
state = state.checkpoint("before-generation")
state = state.with_state({**state.state, "answer": "invalid"}).checkpoint("after-generation")
restored = state.restore("before-generation")   # selected, with the later snapshot still in history
next_state = restored.with_state({**restored.state, "temperature": 0.7})

print("labels:", [row["label"] for row in next_state.history])
print("selected:", next_state.selected)
print("round trip equal:", registry.load(registry.dump(next_state)).state == next_state.state)
try:
    next_state.prune("before-generation")   # the selected snapshot cannot be pruned
except ValueError as exc:
    print("prune refused:", exc)
```

The rules such a payload lives under:

* **Nested mutable values never alias the retained snapshots.** `state`, `history` and `snapshot(...)` all
  hand back detached copies, so editing the current state cannot rewrite the past.
* **State and metadata must be JSON serializable.** Encoding rejects clients, leases and other runtime
  objects.
* **The versioned `history-v1` codec preserves the snapshots and the registered subclass type.** Subclasses
  inherit the base constructor (application fields live in `state`); a custom constructor or extra-attribute
  serialization is outside this interface, and an unregistered subclass fails decoding explicitly rather
  than coming back as a plain `HistoryArtifact`.
* **Persisting is the commit's job, not `checkpoint()`'s.** Calling `checkpoint()` inside a task does not
  touch the store; the runner persists the payload with the task's output or its control-transition commit,
  so a crash before that commit can lose the in-memory snapshot.
* **History is self-contained and grows** with the number and size of the snapshots — prune on purpose. It
  does not replace the framework's execution ledger, its task rows or its visit records.

### Helpers

| Function | Purpose |
|---|---|
| `canonical_json(obj)` | deterministic JSON: sorted keys, no insignificant whitespace |
| `digest_of(data)` | the `blake2b` digest used for content addressing |
| `Encoded` | a frozen `(type_name, codec, data)` triple |

---

## Stores

A store holds every fact the framework records. SQLite is the default; the in-memory store has identical
semantics and is what you want in tests.

### `open_store`

```python
open_store(spec, *, journal="full", write_behind=None, batch_size=128,
           flush_interval=1.0, clock=None, backend=None) -> Store
```

```python
from contextlib import closing
from pyattacker import open_store

with closing(open_store("runs/qa.db")) as store:      # reopen a finished run, any process
    print(store.stats()["pipelines"])
    for row in store.errors(limit=20):
        print(row["name"], row["failed_task"], row["error_type"])
```

`spec` can be `":memory:"`, a path, a plugin URI (`"s3://bucket/runs.db"`), or an already-open store.
Reading a live store while a run writes to it is supported — WAL allows one writer and many readers.

### Fact batches and failure recovery

`pyattacker.store.FactBatchStore` is an optional capability, separate from the required `Store`
protocol: `write_facts(attempts, events)` atomically commits both sequences and assigns their IDs
only after success. On exception no batch rows may be committed and input IDs must be unchanged.
The capability owns its transaction; SQLite rejects a call inside an existing transaction without
committing or rolling back the caller's work. An empty batch performs no transaction.

`WriteBehindStore` uses this capability when available. SQLite implements it, including the SQLite
stores wrapped by either Suite layout. A failed atomic batch stays buffered and can be flushed again.
In a split Suite, a successful child batch stays committed if a later child's batch fails; retry only
writes the remaining batches. Eviction flushes before removing a child from the connection cache,
so a failed flush leaves that child and its pending batch available for retry. Checkpoint, artifact,
and task-state writes still bypass the buffer; handoff/visit transactions keep their existing boundaries.

A legacy store without `write_facts` is supported through individual `record_attempt`/`emit_event`
calls. Each normally returned call removes that confirmed record from the buffer. If a call raises,
the original exception propagates and the failing record plus its unattempted suffix stay buffered.
Because the failing call might have committed before raising, this wrapper then refuses further
fact writes and flush-dependent reads/flushes with `StoreUnavailable`, chaining the original error.
`buffer_stats()["flush_blocked"]` reports this condition; `pending` includes the uncertain record.
Reconcile persisted records with the original submitted facts before constructing another wrapper;
do not blindly replay the pending suffix. There is no automatic exactly-once guarantee for an
ambiguous legacy write. Custom stores that can guarantee atomic rollback should implement the optional
capability instead. SQLite subclasses that customize fact writes must also customize `write_facts`
or disable it with `write_facts = None` to keep using their individual write overrides.

Closing always releases the underlying store even when flush raises; the error remains visible.
Suite close attempts every child, the catalog and the ownership lock, and propagates the first error.
After a failed close, reopen/reconcile as needed; do not retry a flush on the closed connection.

### Tables and readers

| Table | One row per | Read with |
|---|---|---|
| `runs` | run | `store.get_run(id)` |
| `pipelines` | pipeline: state, checkpoint cursor, digests, tags | `store.pipelines(...)`, `store.export_rows()` |
| `tasks` | task: final state, attempts used, duration, error | `store.tasks(...)` |
| `attempts` | attempt: outcome (`succeeded`/`failed`/`timeout`/`cancelled`/`handed_off`), error class, **retry decision**, leases, duration | `store.attempts(...)` |
| `artifacts` | artifact | `store.artifacts(pid)`, `store.get_artifact(pid, seq)` |
| `results` | final result, status, error and experiment identity per pipeline | pipeline order |
| `handoffs` | handoff: from/to position, entry artifact, whether it was reused, reason | `store.handoffs(...)` (**optional capability**), nested in `export_rows()` |
| `events` | structured event | `store.events(...)` |
| `resources` | resource: spec (secrets masked) and health stats | included in `stats()` |

| Method | Returns |
|---|---|
| `stats(run_id=None)` | counts, state distribution, latency percentiles; `handoffs_total` counts recorded jumps (0 for ordinary runs) |
| `errors(*, run_id=None, limit=20)` | failures with task name, error type and message |
| `export_rows(*, run_id=None)` | nested pipeline rows: tasks, artifacts and handoffs included; each row carries `attempts_total`, which `merge_reports` counts from |
| `attempts(*, pipeline_id=None, ...)` | attempt history |
| `events(*, pipeline_id=None, run_id=None, kind=None, limit=...)` | the event stream, optionally filtered by event kind |
| `count_events(*, kind=None, run_id=None, pipeline_id=None)` | exact count of matching events, via aggregate query (no materialization) |
| `close()` | close the connection |

```python
# "Why did this pipeline take 40 seconds?"
for attempt in store.attempts(pipeline_id=pid):
    print(f"{attempt.task_name} #{attempt.attempt_no} {attempt.outcome} "
          f"{attempt.duration_ms}ms class={attempt.error_class} {attempt.decision}")

# The full story of one pipeline.
print([event.kind for event in store.events(pipeline_id=pid)])
```

Record types with typed fields: `PipelineRecord`, `AttemptRecord`, `EventRecord` and `HandoffRecord` are
exported; `TaskRecord`, `RunRecord` and the `Store` protocol live in `pyattacker.store.base`. `SqliteStore` and
`MemoryStore` are the implementations, and `open_store` is how you get one.

`journal="summary"` keeps digests and metadata but no payloads. It saves space and costs you task-level
resume — with no artifact to restore, a resumed pipeline starts over and records
`pipeline.checkpoint_missing`.

The stored cursor is also repaired rather than trusted blindly. A `failed`/`interrupted` pipeline whose
`n_tasks_done` already equals `n_tasks_total` is a torn finalization (a store failure or a kill during the
final write): every task is checkpointed, so the next run verifies the last artifact exactly as an ordinary
resume does — present, payload kept, decodable — then marks it final and settles the terminal row (state,
cursor, owning run and cleared failure fields, in one write where the store offers the optional
`settle_pipeline` capability), records `pipeline.terminal_repaired` with the state/error/run the row was
carrying, and finishes as `succeeded` without re-running anything. Two cases do **not** repair, and they are
not the same thing: a *past-the-end* cursor is not a state the Runner can create, so it is reported as
`CorruptCheckpoint` (`pipeline.corrupt_cursor`), never promoted to success, with the stored value left in
place as the evidence; and a terminal artifact that is missing, payload-less or undecodable falls back to the
ordinary restart-from-zero rule (`pipeline.checkpoint_missing` / `pipeline.checkpoint_unusable`). A repair
that fails in **either** step of its finalization — the finality mark or the terminal settle — leaves the
row untouched, original failure and owning run included, and records `pipeline.terminal_repair_failed` with
the phase, so a later attempt still reports the original cause. Because such a row never belongs to the
repairing run, that failure can never show up in the run-scoped `stats`; it is counted in
`RunReport.repair_failures`, which is what makes the CLI exit `1` instead of reporting a clean run.

A **post-hoc** `pyattacker report <store>` now surfaces failed terminal repair attempts: it queries
the event log for `pipeline.terminal_repair_failed` events associated with the pipelines in the report
scope, and prints a `Terminal repair failures: N pipeline(s)` section listing each affected pipeline
and its failure phase. The count is de-duplicated by `pipeline_id`, so the same pipeline appearing in
multiple stores does not double-count. The live `run` exit code remains the authoritative signal for the
attempt it just made; the post-hoc report is a historical view.
A control-enabled pipeline adds one branch, consulted **before** those rules
([handoffs](#handoffs-opt-in)): if the newest active ledger row's target is at or ahead of the cursor, the
run resumes *at that target* with the recorded entry artifact and never re-runs the source task, and a
durable `END` row is settled from its own entry artifact. A row whose target is behind the cursor has been
consumed by later forward progress, so the ordinary `artifact(cursor - 1)` rule applies instead. If the
entry payload is gone (`journal=summary`, a null backend, a deleted blob) the pipeline restarts from the seed
with `pipeline.checkpoint_missing`, exactly like a lost linear checkpoint.

A restart from seq 0 (an explicit `fresh_restart=True`, a pipeline that already succeeded under
`retry_succeeded=True`, or an unusable checkpoint) durably advances
`PipelineRecord.handoff_floor` to the latest ledger ID before task execution. Rows at or below that
watermark remain in the append-only history and export, but cannot drive recovery for the new execution.
For control-enabled pipelines, `reset_pipeline(record)` commits that cursor/watermark together with
deleting previous current task rows and chain artifacts (`0 <= seq < n_tasks`). Seed and high-band
payload artifacts and append-only attempts/events/handoffs remain; retained artifacts have final flags
cleared. Thus `tasks()` and chain artifact exports describe the current execution, including skipped
stations having no rows. Historical reused-entry addresses may reference deleted/replaced chain slots;
the ledger preserves provenance, not immutable snapshots of those slots.

`fresh_restart=True` is the only switch that discards a checkpoint on purpose, for backward and forward
pipelines alike: the pipeline runs again from its bound seed, `resume`/repair rules are skipped for that
open, and the open records `pipeline.restarted` (with the cursor it discarded) instead of
`pipeline.checkpoint_missing`, because nothing was lost. Everything append-only survives — attempts, events,
handoffs and, for a backward pipeline, the visit counters, so historical occurrences stay addressable. A
checkpoint that the framework can no longer use is therefore never a dead end, and neither is a spent
backward-traversal budget; see [recovery and ownership](#recovery-and-ownership). By contrast
`retry_succeeded=True` only widens *which* pipelines are eligible to run again — it never discards the state
of one that has not succeeded.

The watermark survives subsequent resumes and process restarts; filtering by the current `run_id` would
incorrectly discard a valid handoff after a second interrupted resume. Custom stores offering handoffs
must persist this field on pipeline reads and writes and provide the atomic reset capability. Writable SQLite opens migrate older databases with
a default of zero; read-only tools treat a missing column as zero without migrating.

Successful completion selects exactly one `is_final` artifact per pipeline, clearing old final flags
from earlier executions. Historical payloads and ledger rows remain available for inspection.

`mark_final` is therefore contractually idempotent, and it is skipped outright when the artifact is already
final.

A store without the `settle_pipeline` capability does the same repair in two writes, and the two steps are
classified differently on purpose: a failed **terminal transition** is a failed repair (retryable, as
above), while a failed **metadata cleanup** is not — the row is already durably `succeeded`, so the pipeline
is repaired and only its failure text is stale. That second case emits `pipeline.terminal_cleanup_failed`
and is not counted as a failure, because a succeeded row is skipped forever and the cleanup can never be
retried. Stale failure metadata on a succeeded row is the documented degraded guarantee of such a store; the
built-in backends settle the row in one write and never see it.

### Paged reads and third-party stores

The list methods above are the **required** interface, and they may materialize their result — that is
what makes `merge_reports` and a small report easy to write. A whole-kind read that must stay bounded
in memory (an export of a large store) goes through the `iter_*` helpers instead:

| Helper | Yields, in this order |
|---|---|
| `iter_pipelines(store, *, run_id=None, state=None)` | `PipelineRecord`, `created_at` then `pipeline_id` |
| `iter_tasks(store, pipeline_id=None, *, run_id=None)` | `TaskRecord`, `pipeline_id`, `seq`, then `task_run_id` |
| `iter_attempts(store, *, run_id=None, pipeline_id=None)` | `AttemptRecord`, `attempt_id` (write order) |
| `iter_events(store, *, pipeline_id=None, run_id=None, kind=None)` | `EventRecord`, `event_id` (write order, oldest first), optionally filtered by event kind |
| `iter_artifacts(store, *, pipeline_id)` | `Artifact` of one pipeline, `seq` then `artifact_id` |

Every one of those orders **ends in a unique key**, and that is not decoration: `tasks` is keyed by
`task_run_id` and `artifacts` by `artifact_id`, so `(pipeline_id, seq)` and `seq` are not unique by
contract. A page cursor is a strict `>`, so paging on the non-unique prefix alone would silently drop
every row that ties with the last row of a page. `pipeline_id` (primary key), `event_id` and
`attempt_id` (monotonic counters) are unique on their own.

```python
from pyattacker.store import iter_events

for event in iter_events(store, run_id=run_id):   # one batch in memory, not the whole log
    ...
```

`PagedStore` is the **optional** extension that makes those reads batched: a store implements
`iter_pipelines` / `iter_tasks` / `iter_attempts` / `iter_events` / `iter_artifacts` with the signatures
above, reads at most `ITER_BATCH_SIZE` (1000) rows per query, and yields in the documented order.
`SqliteStore` implements all five with keyset pagination (`WHERE <key> > <last row of the batch>
ORDER BY <key> LIMIT 1000`), so no query returns more than a batch and no read cursor stays open while
a row is being processed. `MemoryStore` walks its live containers; for it, bounded memory is inherent.

**Reading a store that is still being written.** Each page is its own statement — there is no
long-lived read transaction and no point-in-time snapshot of the whole store. What an iterator
guarantees depends on whether its key is monotonic:

* `events` and `attempts` are **bounded by a high-water mark** (`MAX(event_id)` / `MAX(attempt_id)`
  among the matching rows, taken when the first page is read). Rows appended after that are not part
  of that traversal, so a long export cannot chase a moving tail; a new iterator sees them. Both
  kinds are append-only, so the mark is a true snapshot of the key range.
* `pipelines`, `tasks` and `artifacts` are a **best-effort traversal**: no monotonic key exists to
  bound (`created_at` is caller-supplied, `task_run_id`/`artifact_id` are not ordered by time), so a
  row inserted ahead of the cursor can appear in the export and one inserted behind it cannot. An
  export of a live store is "everything that existed and was reachable while I walked", not a
  snapshot; re-export a finished store when you need reproducibility.

The compatibility rule for a store that does not implement the extension — the third-party store
plugin layer is public API, and existing plugins were written against the list methods:

* each `iter_*` helper uses the store's native paged method **when it exists**, and otherwise
  **delegates to the list API** (`iter_pipelines` → `pipelines()`, `iter_tasks` → `tasks()`,
  `iter_attempts` → `attempts()`, `iter_artifacts` → `artifacts()`, and `iter_events` → `events()`
  with the largest limit it can express, because that list API's own `limit` means "the most recent
  N" and cannot say "everything");
* the fallback is correct but materializes the kind, so a third-party store gets complete exports
  with the memory profile of its list API. Implementing the five methods is what upgrades it;
* `Store` remains the only protocol `open_store()` checks, so adding the extension breaks nothing.
  `WriteBehindStore` implements it and flushes before every paged read, like its other read views.

### Optional store capabilities

**`commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)`**,
**`handoffs(*, pipeline_id=None, run_id=None, limit=None)`** and **`reset_pipeline(record)`** are the optional capability behind the
handoff feature, in the same "not part of the protocol" spirit as `resources()`. The commit is **one atomic
write**: finalize the source task as `handed_off`, insert the handed-off attempt, persist the payload
allocating its address above the chain, append the ledger row and move the cursor — and for `END` also mark
the entry artifact final and settle the pipeline `succeeded`. Atomicity is a requirement of the capability,
not a bonus: there is deliberately no second recovery protocol, so a store that cannot do it as one unit must
not expose the method, and opening a pipeline that declares `control` on such a store fails fast with a
`ConfigError` naming the commit/reset capability rather than writing a non-durable jump. `handoffs()` reads the ledger
oldest first, and `limit` keeps the newest N (oldest first), like `events`/`attempts`. `supports_handoff(store)`
is the probe: it unwraps `WriteBehindStore`, which forwards the capability with a flush of buffered
attempts/events first, and writes the handed-off attempt through the commit rather than through its buffer.
Failures must roll back database writes before any later event or cleanup write. An external blob backend
may retain an unreferenced blob after failure; it must not make a partially committed checkpoint visible.
Handoff-capable stores must also preserve `PipelineRecord.handoff_floor` across writes and reads (see the
restart rule above).
Both built-in backends implement it; a third-party store that does not simply cannot run control-enabled
pipelines, while ordinary pipelines on it are untouched.

**`reset_pipeline(record)`** must atomically delete previous current task rows and chain artifact slots,
clear remaining artifact final flags, and persist the reset pipeline row including cursor and watermark.
Preserve the seed, high-band payloads and append-only history. It runs only on control-enabled seed starts;
resuming at a target leaves the current state intact. `WriteBehindStore` flushes before reset. A store
missing this method cannot advertise handoff support; ordinary control-free pipelines remain supported.

**`settle_pipeline(pipeline_id, *, state, n_tasks_done, run_id)`** is the other optional capability, in the
same "not part of the protocol" spirit as `resources()`: one write that moves a row to its terminal state,
rebinds the owning run and clears the failure fields together. `Runner._settle_succeeded` uses it when the
terminal-cursor repair finishes a pipeline (see § Stores above), because a torn write there could otherwise
lose the original failure before the row was settled. `SqliteStore` and `MemoryStore` implement it, and
`WriteBehindStore` passes it through because it is a state write, not a batched fact. A store without it still
works: the Runner falls back to `finish_pipeline` (state and cursor, atomically) followed by a cleanup
`upsert_pipeline`, whose worst case is a settled row that still carries the old failure text.

**`visit_state(pipeline_id)`**, **`reset_visits(record, seed, *, fresh_budget=False)`**,
**`commit_entry(task)`**, **`commit_visit_attempt(pipeline, task)`**,
**`commit_visit_success(pipeline, task, attempt, artifact, *, final)`**,
**`commit_control_transition(record, *, pipeline, task, attempt, payload, entry_id, target_task, limit)`**,
**`repair_visit_terminal(record)`** and **`get_artifact_by_id(artifact_id)`** are the optional capability
behind [backward traversal](#backward-traversal-rewind-retry-all-visits), again outside the protocol.
`store/visits.py`'s `VisitStore` holds
the shared transition semantics and both built-in backends derive from it, so a backend supplies one atomic
write boundary plus its low-level row writes. Each operation is one commit: entry allocation advances the
per-seq visit counter and records the pending input, ordinary success writes the output occurrence and the
effective slot together, and a control transition additionally invalidates the active suffix, consumes one
budget unit and allocates the target entry. A control transfer commits its source visit and attempt, the
entry occurrence, the ledger row, the control count and the allocated target entry together; a forward
transfer inside a backward-enabled pipeline consumes budget without moving the cursor backwards. SQLite
serializes these capability transactions with `BEGIN IMMEDIATE`, and `MemoryStore` restores its state on a
failed capability write: a blob write that failed cannot publish its reference, while a rolled-back database
transaction can leave an unreferenced blob. `supports_visits(store)` is the probe; it unwraps
`WriteBehindStore` (which flushes before delegating these operations synchronously) and requires the visit
methods, `feature_level()` **plus** `commit_handoff`, `reset_pipeline` and `handoffs`, because a backward
transition lands its ledger row and source task through that same commit. `feature_level()` is required
rather than optional: the compatibility rule below is part of the capability, not an extra. A store that
fails the probe is refused with a `ConfigError` when a backward-enabled pipeline is opened, never downgraded
to a non-durable loop.

### Store compatibility and backups

SQLite upgrades older stores additively: visit columns default to 0, a traversal-state table appears and a
`store_meta` feature level is recorded. Forward-only work never leaves `base`. The level becomes `visits-v1`
inside the *same transaction* that allocates the first second occurrence for a station, and it never goes
back down — audit rows are not deleted, so neither is the fact that they exist.

That is also the moment a lineage-unaware writer stops being able to interpret the store, so a SQLite store
at `visits-v1` arms a **writer guard**: `INSERT`/`UPDATE`/`DELETE` on `pipelines`, `tasks` and `artifacts`
from a connection that has not declared visit-lineage awareness fail loudly (`no such function:
pyattacker_store_requires_visits_aware_writer`). Raw and legacy reads are not blocked — the guard protects
state, not access — but interpretation of revisit-aware lineage by a build that does not understand it is
unsupported: such a reader cannot resolve which occurrence is effective, so its output describes the rows,
not the execution. What the guard guarantees is the destructive half: a writer released before this feature
cannot silently mutate the wrong occurrence, because it fails on its first write. A build that opens a level
it does not know refuses the store outright (`StoreFeatureUnsupported`, read-only included) rather than
reporting a lineage it cannot see.

Practical consequences:

* **Backups.** For a live database use SQLite's own backup API (`sqlite3 <store> ".backup <copy>"`, or
  `Connection.backup()`), which is safe while a writer is running; copying an active database file together
  with its `-wal`/`-shm` side files is only reliable when no writer is active. A SQL dump restores fine as
  well — `sqlite3 <store> .dump | sqlite3 <copy>` writes table data before creating the guard triggers, so an
  unaware connection can replay it, and the copy inherits the guard and the feature level with it.
* Writing to a guarded store from the `sqlite3` shell needs the guard function registered on that connection
  (or the triggers dropped); both are outside the supported interface.
* The only supported way back to `base` is a migration performed by a build that understands the level.
* Simultaneous runners executing the same logical pipeline are not a supported scheduling mode; independent
  shard rows remain independent (a `running` row is still never taken over without `resume`, see
  [recovery and ownership](#recovery-and-ownership)).

---

## Artifact backends

A backend decides where payload *bytes* live. The store keeps the digest, size and codec either way, so an
artifact stays one object to everything above it.

| Backend | Behaviour |
|---|---|
| `InlineBackend()` **(default)** | payloads live in the store |
| `FileBackend(root=..., min_bytes=262144)` | anything at or above `min_bytes` is written to a content-addressed file, hydrated back on read |
| `NullBackend()` | keeps digests, drops bytes |

```python
from pyattacker import FileBackend, Runner

with Runner(store="runs/qa.db", artifact_backend=FileBackend(root="/data/blobs")) as runner:
    ...

# Equivalent, and what the CLI and YAML accept:
Runner(store="runs/qa.db", artifact_backend="file:///data/blobs")
Runner(store="runs/qa.db", artifact_backend={"kind": "file", "root": "/data/blobs",
                                            "min_bytes": 262144})
```

Files are content-addressed and written atomically, so identical payloads collapse into one file and a shared
root can serve many runs safely. Resume through a spilled checkpoint is transparent.

`NullBackend` costs you recovery granularity, exactly like `journal="summary"`: no payload means no
checkpoint to resume from. A missing blob file behaves the same way on purpose — `available` goes false and
the work is redone rather than silently skipped.

`resolve_backend(spec)` turns a string or dict into a backend. `FileBackend.stats()` reports what it has
written. Distribute your own under `pyattacker.stores` if it needs its own URI scheme.

---

## Sharding and merging

One store takes one writer, so scaling out means several processes with their own stores, joined afterwards.
Shard assignment is a pure function of the content-addressed pipeline key, so the same dataset always splits
the same way and a resumed pipeline lands back in the shard that owns it.

| Function | Purpose |
|---|---|
| `shard_index(key, count)` | which shard a pipeline key belongs to |
| `in_shard(key, index, count)` | whether it belongs to this one |
| `shard_specs(specs, index, count, *, key="pipeline_id")` | filter a spec stream down to one shard |
| `shard_store_path(base, index, count)` | `runs/qa.db` → `runs/qa.shard0of4.db` |
| `parse_shard(text)` | `"0/4"` → `(0, 4)`, with validation |

```python
from pyattacker import merge_reports, shard_specs, shard_store_path

specs = list(template.map(rows))
paths = []
for index in range(4):
    mine = list(shard_specs(specs, index, 4))
    path = shard_store_path("runs/qa.db", index, 4)
    with Runner(store=path, pools=make_pools(), concurrency=16) as runner:
        runner.run(iter(mine))
    paths.append(path)

merged = merge_reports(paths)
print(merged.summary())
merged.export("runs/all.jsonl")
```

Assignment is `blake2b(key) % count`, not Python's salted `hash()`, so it is stable across processes and
machines. Balance is statistical: 12 pipelines across 3 shards splitting 6/2/4 is normal, and it evens out at
scale. In practice you usually let the CLI do the spawning — `--shards 4 --jobs 4` — and use these functions
when you are driving a cluster yourself.

### `merge_reports`

```python
merge_reports(paths) -> MergedReport
```

De-duplicates by `pipeline_id` (best state wins, latest finish breaks ties), then **recomputes** the
workload counters from the surviving rows: `pipelines`, `tasks`, `attempts_total` and `handoffs_total`
describe the union, so merging is idempotent — a store counted twice, or one pipeline present in two
shards after a shard-count change, cannot inflate them.

`source_events_total` is the deliberate exception, and is named for it: events do not hang off a pipeline
row, so nothing in the merged rows says which of two copies owns an event. It is a raw total over the
sources you passed, duplicates included.

Reading the counters off the rows strengthens what a custom store's `export_rows()` has to provide, so it is
worth being precise about the two fields:

* **`attempts_total` is required.** It is a core part of a pipeline row (`PipelineRecord.attempts_total`);
  a row without it raises a `ConfigError` naming the row and its source, rather than silently counting `0`.
* **The nested `handoffs` ledger is optional**, because the ledger itself is an optional capability. A row
  that does not nest it is read through the store's own `handoffs(pipeline_id=...)` when the store has one —
  the same capability the nesting comes from — and a store with neither has no jumps, which is `0` and true.

| `MergedReport` member | Meaning |
|---|---|
| `rows` | the merged pipeline rows |
| `duplicates` | how many rows were folded away |
| `sources` | which stores contributed |
| `attempts_total`, `handoffs_total` | recomputed from the surviving rows, so de-duplicated |
| `source_events_total` | raw event-log rows across the given sources, **not** de-duplicated |
| `events_total` | deprecated alias of `source_events_total`, on the object only (removed at 1.0) |
| `stats()` | the same counters, plus the recomputed pipeline and task statistics |
| `summary()` | human-readable |
| `errors(limit=20)` | failures across all shards |
| `export(path, *, fmt="jsonl", kind="pipelines")` | write the merged view |

---

## Resume identity

A task fingerprint records its name/target, declared resource and timeout, every retry field
(including exception module/qualified names), task algorithm configuration, `config`, `version`,
and factory `parameters`. `fanout` also records its ordered child fingerprints and `on_error`.
Built-in factories record their behavior arguments automatically. User `config` is separate from
factory parameters, so a declarative override cannot erase a built-in's behavior identity.

`config` and `parameters` accept only null, bool, int, finite float, string, lists, and objects
with string keys; they are snapshotted when the spec is built. Tuples, non-string object keys,
Python subclasses of JSON primitives and cycles are rejected rather than coerced. Arbitrary closures, clients, globals, imported helpers, endpoint options, and
pool-default algorithms are **not** inspected. Declare behavior from those sources explicitly:

```python
from pyattacker import task

@task("ask", config={"model": "model-a", "temperature": 0.2}, version="prompt-v2")
async def ask(row, ctx):
    ...  # use the same declared model, temperature and prompt revision in your client call
```

Built-in task algorithms capture their normalized configuration at TaskSpec construction.
The Runner builds fresh runtime instances from that same snapshot, so later mutation of a supplied
algorithm instance or mapping cannot change recorded behavior. Nested fallbacks are snapshotted,
and implicit Wait fallbacks normalize to an explicit `Wait()` for Sticky, LeastBusy, Failover and
QuotaAware. Failover's tuple/list pool-name sequences intentionally normalize to a JSON list.

A custom task algorithm must provide `fingerprint()` returning the same strict JSON domain, or
the task must supply `version=` and bump it when algorithm behavior changes. Hook results are
captured with the spec and checked before task execution and each acquisition, including custom
fallbacks; drift fails before that algorithm runs. Custom hooks must describe configuration,
not changing counters, and their configuration must stay stable during an acquisition.
Version-only custom algorithms must support an independent `deepcopy`; the definition is copied
at construction and detached runtime copies are produced for each task. Built-in strings,
config mappings and equivalent instances normalize to the same fingerprint. Secrets and runtime clients should never be placed in identity config.

`pipeline(..., include_code=False)` removes source digests recursively, including fanout children.
It retains factory parameters, config, version and policies. Source-inspection failure falls back
to module/qualified name, so dynamically defined functions need an explicit version to distinguish
implementations with the same name. Changes to imported helpers also need config/version updates.

Default IDs hash the spec digest, seed digest and repeat index. An unchanged definition/input
retains its ID and shard; changed behavior produces a new ID and may move to another shard.
Explicit `bind(key=...)`, `map(key_of=...)` and declarative `source.key_field` retain their supplied
IDs, but the Runner checks the stored spec and seed digest **before** skipping or restoring.
A mismatch raises `PipelineIdentityConflict` (`ConfigError`, CLI exit 2), interrupts the new run,
and leaves the conflicting pipeline's stored definition, result and checkpoint untouched.
Already-open in-flight pipelines are finalized as interrupted immediately, with their durable
checkpoint cursors preserved; monitoring does not need a later resume to repair their state.
Use a new key or store for changed work. `retry_succeeded=True` is not a conflict override.

### Upgrading existing stores

The new spec digest is prefixed `v2:`; all default pipeline IDs change from the old fingerprint
format. Old records remain readable/exportable, but are not automatically migrated or reused:
the old fingerprint omitted information needed to verify equivalence. Opening a store whose
oldest pipeline has a legacy digest emits a warning and `run.legacy_identity` event before tasks
start. Default IDs rerun work; explicit old keys conflict. Finish expensive old runs with the old
package, then start a new store for v2. Do not rewrite legacy digests to bypass verification.
For sharded runs, this identity change also changes shard assignment; keep the old shard stores
and old package together when completing an old run.

### External side effects

Recovery skips tasks whose checkpoint cursor and artifact are durable. It does not guarantee
exactly-once requests or file writes: a provider may complete a request before the process crashes
or fails while writing the checkpoint, and that task can run again. Journal summary, null/missing
payloads and unusable checkpoints can also require replay of earlier tasks.

If your provider supports idempotency keys, derive one from the stable pipeline and task identity,
for example `f"{ctx.pipeline_id}:{ctx.seq}"`, and reuse it across retries rather than including the
attempt number. In a backward-enabled pipeline, include `ctx.visit` when each regeneration should be a
new operation, for example `f"{ctx.pipeline_id}:{ctx.seq}#{ctx.visit}"`. A handoff payload is *not* a task
identity, so never key external work off an
artifact address. Respect the provider's retention window and API contract. For file sinks, upsert
by the same identity or write one atomically replaced file per pipeline/task; a plain append-only
`write_jsonl` task can create duplicate lines on replay. Resource lease safety does not make those
external side effects idempotent.

---

## Export

Six row shapes, three formats. Rows stream out of the store in bounded batches; the merged report
(N stores into one coherent answer) is the one place that has to hold rows in memory.

| Name | Value |
|---|---|
| `ROW_KINDS` | `("pipelines", "tasks", "attempts", "events", "artifacts", "results")` |
| `FORMATS` | `("jsonl", "json", "csv")` |

| Function | Purpose |
|---|---|
| `iter_rows(store, *, kind="pipelines", run_id=None, limit=None)` | stream rows as dicts |
| `export_store(store, path, *, kind="pipelines", fmt="jsonl", run_id=None, limit=None)` | write one store, returns the row count |
| `export_stores(paths, path, *, kind=..., fmt=..., limit=...)` | write several shard stores as one concatenated file |

One row per kind, in the order `limit` truncates:

| `kind` | One row per | Order |
|---|---|---|
| `pipelines` (default) | pipeline, nested — tasks, artifacts and handoffs included | `created_at`, then `pipeline_id` |
| `tasks` | task: final state, duration, error, leases used | `pipeline_id`, `seq`, then `task_run_id` |
| `attempts` | attempt, including each retry `decision` | `attempt_id` (write order) |
| `events` | structured event | `event_id` (write order, oldest first) |
| `artifacts` | artifact, intermediate ones included | pipeline order, then `seq`, then `artifact_id` |
| `results` | pipeline result: key, repeat, state, final payload and error; failed/interrupted rows included | `created_at`, then `pipeline_id` |

`limit` means the same thing for every kind — it counts rows of that kind, including `artifacts`,
where it used to count pipelines:

* `None` (the default): the **complete** history, nothing is truncated;
* `0`: no rows;
* `N > 0`: the first N rows in the order above;
* a negative value: `ConfigError`.

(`export_stores` applies the limit per store, so each store contributes at most its first N rows.)

Every order ends in a key that is unique, so a read that crosses a batch boundary can neither drop nor
duplicate a row — including when many pipelines share one `created_at`, or when several tasks or
artifacts share a `(pipeline_id, seq)` / `seq`, which the schema does not forbid.

```python
from pyattacker import export_store, iter_rows

# Compute your own metric — the framework stores facts and leaves semantics to you.
correct = sum(1 for row in iter_rows(store, kind="pipelines")
              if row["state"] == "succeeded" and row["artifacts"][-1]["payload"]["correct"])
print(f"accuracy: {correct / total:.1%}")

# Retry analysis in a spreadsheet.
export_store(store, "attempts.csv", kind="attempts", fmt="csv")
```

`pipelines` rows are nested — tasks, artifacts and handoffs included — which is why it is the default. The
`handoffs` list includes `handoff_id` and committing `run_id`; the pipeline row includes
`handoff_floor`. IDs above the floor belong to the active execution history; IDs at or below it are
historical records excluded from recovery. `store.handoffs(pipeline_id=...)` includes all records.
`stats(run_id)["handoffs_total"]` counts only commits by that run, so a resume can use an active handoff
from a previous run while reporting zero new handoffs. The
`handoffs` list is `[]` for an ordinary pipeline and has one object per recorded jump otherwise (see
[handoffs](#handoffs-opt-in)); it is nested rather than a row kind of its own, so `ROW_KINDS` is
unchanged. CSV takes its
header from the first rows and folds later keys into an `extra` column, so memory stays flat and no field is
dropped silently.

**Exporting a live store.** An export is not a transaction: it reads one page at a time, so what it
guarantees depends on the kind (the details are in [stores](#paged-reads-and-third-party-stores)).
`events` and `attempts` are bounded by the high-water mark of their monotonic key taken when the
export starts — rows written afterwards are not included, and a fresh export sees them.
`pipelines`, `tasks` and `artifacts` are a best-effort traversal: a row written ahead of the cursor
can appear, one written behind it cannot. Export a finished store when you need a reproducible file.

**Where streaming holds, and where it does not.** `jsonl` and `json` write row by row, and `csv` buffers
only the `header_rows` prefix it needs for the header. On the store side, `tasks`/`attempts`/`events` are
read through the paged helpers in batches of `ITER_BATCH_SIZE` (1000) rows, and `artifacts` pages the
pipelines and then streams each one's artifacts, so the memory unit is **one pipeline**, not the store. A
`pipelines` row is itself nested, so exporting that kind materializes one pipeline's tasks and artifacts at
a time. `merge_reports` is the deliberate exception: de-duplicating by `pipeline_id` needs the winning row
of every pipeline, so the merged rows are held in memory (the one counter that is not read off those rows —
`source_events_total` — comes from each source's aggregate query instead of materializing the log).

---

## Declarative configs

`load_spec` reads a YAML/TOML/JSON file into pools plus a pipeline template plus a seed source, returning a
`DeclarativeSpec` (defined in `pyattacker.declarative`). The file describes **composition and resources**; your
logic stays in Python behind `use:`.

The parser is chosen from the file's suffix, and only the YAML one is optional: `.json` and `.toml` are read
with the standard library, while `.yaml`/`.yml` needs the `yaml` extra (`pip install "pyattacker[yaml]"`).
Without it, `load_spec` raises a `ConfigError` that names both the file and the extra — the check happens
when that file is read, so a process that only ever sees JSON/TOML never needs PyYAML installed.

```python
from pyattacker import Runner, load_spec

spec = load_spec("qa.yaml")                 # raises ConfigError on a bad file
print(spec.describe())                       # what `pyattacker validate` prints

with Runner(pools=spec.pools, **spec.run) as runner:
    report = runner.run(spec.pipelines(limit=100))
```

`load_spec` is the shared validation entry: `run`, `validate` and every `--shards` child go through it, and
a config it refuses is exit code `2` before a store exists. It checks unknown fields, field types, numeric
ranges, pool references (including the `resource` a `use:` factory declares itself), algorithm names and
parameters, and the `source:` declaration — always naming the field path. It is a declaration check only:
no dataset is opened and no artifact backend is constructed. [`docs/cli.md`](cli.md#validate--check-a-config-without-running)
lists what is covered.

A field the config does not mention keeps whatever the `use:` target declares; an explicit `field: null`
clears `resource`, `algorithm`, `timeout_s` or `version`. The rule and its SDK spelling
([`TaskSpec.with_overrides`](#taskspec)) are the same one.

| `DeclarativeSpec` member | Meaning |
|---|---|
| `template`, `pools`, `run`, `source` | the parsed sections |
| `unresolved_env` | `${VAR}` references that resolved to nothing |
| `seeds()` | the seed iterable |
| `pipelines(*, limit=None)` | the `PipelineSpec` stream |
| `describe()` | JSON-ready summary |

`load_spec(path, strict_env=True)` raises instead of warning on an unset `${VAR}` — worth using in CI, where
a silently empty API key is worse than a failed job.

Two details worth knowing:

* `use:` is resolved by shape. A name with a colon (`my_pkg.tasks:ask`) is imported directly; a bare name
  (`echo`) is looked up in the built-ins first, then in plugins — so a plugin can never shadow a built-in. A
  callable returning a `TaskSpec` is treated as a factory and called with `args`/`kwargs`.
* **YAML 1.1 parses a bare `on:` key as boolean `true`.** Write `"on": [RetryableError]` in a retry block.
  The loader detects the mistake and tells you.

The full file format is in [`docs/cli.md`](cli.md#config-file-reference).

---

## Plugins

Plugins are ordinary `importlib.metadata` entry points. Install a package, and its names work in any config.

| Group | Provides |
|---|---|
| `pyattacker.tasks` | a `TaskSpec`, or a factory returning one → `use: my_task` |
| `pyattacker.algorithms` | an acquire algorithm → `algorithm: my_algo` |
| `pyattacker.codecs` | a codec, installed on the first `Runner` |
| `pyattacker.stores` | a store factory keyed by URI scheme → `store: "s3://bucket/runs.db"` |

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"
[project.entry-points."pyattacker.algorithms"]
my_algo = "my_pkg.algo:MyAlgorithm"
```

```python
from pyattacker import PLUGINS, list_plugins

for item in list_plugins():
    print(item["group"], item["name"], item.get("error", "ok"))

print(PLUGINS.errors())        # {name: reason} for everything that failed to load
```

Built-ins resolve first, and a plugin that raises on import is recorded rather than propagated — so a broken
plugin cannot take a run down, and it cannot fail silently either. `pyattacker plugins` prints the same
information. `PluginRegistry` is the type; `PLUGINS` is the process-wide instance. A complete worked package
is in [`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.md).

---

## Built-in tasks

Three of these are for production use (`fanout`, `shell_run`, `write_jsonl`); the rest simulate work so you can
exercise the machinery without a network. In configs they carry `mock.*` names: `use: pyattacker.tasks:flaky`.

### `fanout`

```python
fanout(*specs, name=None, on_error="raise", retry=None) -> TaskSpec
```

Runs several tasks on the **same** input concurrently, inside one task, and returns `{task_name: value}`. This
is how a genuinely branching step is expressed without turning the pipeline into a DAG.

```python
from pyattacker import fanout, pipeline

judges = fanout(judge("judge-a"), judge("judge-b"), judge("judge-c"), name="judges")

@task("reduce")
def reduce_scores(row: dict) -> dict:
    branches = list(row.values())
    return {"qid": branches[0]["qid"],
            "verdicts": {b["judge"]: b["verdict"] for b in branches}}

template = pipeline("eval", prepare | ask | judges | reduce_scores)
```

Three consequences, all deliberate:

* **Retry granularity is the group.** One failing branch retries the whole fan-out; the children's individual
  policies do not apply branch by branch. The group adopts the most forgiving child policy unless you pass
  `retry=`.
* **Branches share the parent's context**, so their leases and events are recorded under the fan-out task —
  one complete record per step.
* **The Runner only sees the group spec**, so `resource`, `algorithm` and `timeout_s` are lifted from the
  children only when every child agrees. That is why the three judges above must declare the same pool and
  algorithm.

The tradeoff: three requests in one checkpoint means a failure re-sends all three on resume. If a request is
expensive, give it its own task instead — `examples/llm_eval/` measures both shapes.

### `shell_run`

```python
shell_run(command, *, timeout_s=60.0, check=True, name=None) -> TaskSpec
```

Runs a subprocess and returns its stdout/stderr.

```python
# The argv form: no shell, and "{value}" arrives as one literal argument.
shell_run(["python", "postprocess.py", "--input", "{value}"])
```

**Use the argv form when the command needs the artifact.** It runs through `create_subprocess_exec`, so the
substituted value reaches the child as a single literal argument whatever characters it contains. The string
form rejects `{value}` outright rather than interpolating into a shell command line. The substitution is a
plain substring swap, not `str.format()`, so other braces (a `jq` filter, a dict literal) are left alone.

The guarantee is "no *implicit* shell", not "safe with any program": if your argv itself invokes an
interpreter (`["sh", "-c", ...]`), that interpreter's input handling is yours to reason about.

**Process lifetime.** The task owns the process it starts, and every exit path runs the same cleanup —
process creation included. The OS child exists before `create_subprocess_exec` /
`create_subprocess_shell` has returned a handle, so a cancellation that lands in that window is not
allowed to abandon it: the task keeps waiting for the handle, disposes of the child, and only then
lets the cancellation continue. A normal exit is left alone (its result is returned as before, and
`check=False` still reports a non-zero `returncode` instead of raising). If `timeout_s` expires, the
coroutine is cancelled (a `Runner` stop, a group task timeout, an outer `asyncio` cancellation) or any
other exception escapes, a child that is still running is killed with `SIGKILL` and then **reaped**
before that exception continues to the caller: cancellation still arrives as `CancelledError` and a
timeout as `TimeoutError`, but no process is left running behind them. There is no graceful `SIGTERM`
window — cleanup does not wait for a child to finish. The wait for the OS to report the exit is bounded
(5 s), which only matters for a process the OS never reports as exited, and nothing cleanup itself runs
into (a reader left in a bad state by a cancelled `communicate()`, a failed signal) is allowed to
replace the caller's exception: cleanup is best effort, the caller's error type is not.

**Descendants.** On POSIX, each child is started in its own session (`start_new_session=True`), so
cleanup signals the whole process group rather than one PID. For a string command that covers every
part of a pipeline or a subshell; for the argv form it covers the program and whatever that program
spawned. There is no "kill only the direct child" mode, and no cgroup/pidfd machinery beyond the
process group. On Windows there is no group signalling in the standard library (`os.killpg` does not
exist, and `asyncio` cannot send `CTRL_BREAK_EVENT` to a child's group), so only the direct child is
terminated there and a descendant of a shell command may outlive the task — a documented limitation,
verified on POSIX only. Because the child leads its own session on POSIX, it does not receive
terminal-generated signals such as Ctrl-C; the task's own cancellation is what stops it. Cleanup acts
only while the child itself is still unreaped: a command that already exited normally is not followed,
so a process it deliberately left behind (a shell's `&`, a daemon) is not killed — and once the child
has been reaped its PID, which is also the process-group id, may have been reused, so signalling that
group would not be safe anyway.

### `write_jsonl`

```python
write_jsonl(path, *, mode="a", name=None) -> TaskSpec
```

Appends each artifact to a JSONL file as a final step. One export mechanism among several — `export_store`
and `report.export_jsonl` are usually the better fit.

### Simulation tasks

| Task | Signature | Behaviour |
|---|---|---|
| `echo` | `echo` (a spec, not a factory) | returns the value unchanged |
| `flaky` | `flaky(fail_times=2, *, error="retryable", message=..., retry=None)` | fails N times, then succeeds — for exercising retry |
| `delay` | `delay(seconds=1.0)` | waits, for watching concurrency |
| `boom` | `boom(message="boom", *, error="retryable")` | always fails; `error` is `retryable`, `fatal` or `invalid` |
| `leaky` | `leaky(*, pool=None)` | leaks a lease on purpose, to demonstrate forced reclaim |
| `simulate_llm` | `simulate_llm(*, latency_ms=5.0, fail_rate=0.0, error="rate_limit", tokens=32, resource=None, **selector)` | acquire → wait → report → return: the workhorse for demonstrating pools and backoff |

```python
from pyattacker import Runner, pipeline, simulate_llm

# A pool exercised at a 30% failure rate, with no network anywhere.
template = pipeline("smoke", simulate_llm(resource="apis", fail_rate=0.3, latency_ms=20))
```

Swap `ctx.clock.sleep` for an HTTP call and `simulate_llm` becomes a real task — it is written as a worked
example of the shape.

### Seed helpers

| Function | Purpose |
|---|---|
| `jsonl_source(path, *, limit=None)` | stream a JSONL file as seeds, one line per pipeline |
| `seed_factory(kind="range", *, n=10, path=None, limit=None)` | what a config's `source:` block resolves through: `range` or `jsonl` |

```python
from pyattacker import jsonl_source

runner.run(template.map(jsonl_source("dataset.jsonl", limit=500)))
```

---

## Monitoring

### Application-reported metrics

An application can publish its latest experiment values while a run is active. Pyattacker stores
and displays the values; the application computes them. For a runnable accuracy example, see
[`examples/live_metrics.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/live_metrics.py).

```python
runner.report_metric("evaluated", completed, label="Evaluated")
runner.report_metric("accuracy", correct / completed, label="Accuracy", display="percent")
# Inside a task, ctx.report_metric("phase", "scoring", display="text")
```

`Runner(..., on_pipeline_finished=callback)` calls `callback(runner, record, artifact)` after a pipeline's
terminal state is stored. `artifact` is its final `Artifact` for success and `None` for failure or
interruption. The passed `runner` provides `report_metric()` and can decode a retained artifact with
`runner.registry.load(artifact.encoded())`. The callback
runs in the scheduler thread and should finish quickly. Exceptions in it are recorded as
`monitor.callback_failed` and do not change the pipeline outcome. A process crash may miss or replay
the callback, so applications should deduplicate by pipeline ID and rebuild from stored final
artifacts when needed. Resumed runs have a new run ID and their reported values have a separate scope.

`report_metric(name, value, *, label="", display="number", pipeline_id=None)` accepts a string,
boolean, or finite number. `display` is `number`, `percent` (a numeric fraction, displayed as a
percentage), or `text` (a string). Repeating the same name in the same run and scope replaces the
previous value. `ctx.report_metric(...)` uses the current pipeline as its scope. Reports are
synchronous state writes, including when event write-behind is enabled. The feature is optional for
third-party stores: reporting on a store without it raises `StoreFeatureUnsupported`.

`/metrics?run_id=...` returns `{"run_id": ..., "rows": [{"run_id", "pipeline_id", "name",
"value", "label", "display", "updated_at"}, ...]}`. Add `pipeline_id=...` to read a pipeline's
reported values; `/pipelines` also includes a `reported_metrics` array in each row. With no run ID,
`read_snapshot()`, `watch`, and every `StatsServer` endpoint select the latest started run, including
runs that reported only pipeline-scoped values. The HTML dashboard resolves that run through `/stats`
and passes its ID to `/metrics`, `/pipelines`, and `/events` for a consistent refresh. Use
`?run_id=all` (or `--run-id all` for `watch`/`serve`) for aggregate operational data; that view
does not show application metrics from an arbitrarily chosen run. The HTML dashboard shows
run-level values as cards; the terminal `watch` view shows them too. The HTTP server remains read-only,
and metric writes use the active Runner's store connection.

### `runner.stats()`

The in-process live snapshot. Safe to call mid-run.

```python
live = runner.stats()
live["in_flight_pipelines"]      # attempts actually running
live["delayed_pipelines"]        # parked in a retry backoff, holding no worker
live["counters"]                 # admitted / succeeded / failed / done
live["pools"]                    # per-pool occupancy, health and wait percentiles
```

### `StatsServer`

```python
StatsServer(store, *, host="127.0.0.1", port=8787, run_id=None, errors=10)
```

A dependency-free read-only HTTP view. It opens a fresh read-only connection per request, so it runs beside a
live run.

```python
from pyattacker import StatsServer

with StatsServer("runs/qa.db", port=8787) as server:
    print(server.url)            # http://127.0.0.1:8787
    server.wait()                # or just let your program continue
```

| Endpoint | Returns |
|---|---|
| `/` | a small auto-refreshing dashboard |
| `/stats`, `/metrics`, `/events`, `/pipelines`, `/resources`, `/errors` | JSON |

`/stats` carries `handoffs_total` (commits by the selected run, or all commits with `run_id=all`), and each `/pipelines` row carries a
`handoffs` count of active-execution records, `handoffs_historical` count of all records and
`handoff_floor`, next to `n_tasks_done`/`n_tasks_total` — on a control-enabled pipeline those two are a
**position** in the chain, not a count of tasks that ran, so a non-zero `handoffs` is what says "this
pipeline skipped stations".

**There is no artifact endpoint, and the endpoint has no authentication.** What the JSON views expose is what
the run *recorded*: `/events` returns each event's `data` verbatim, and `/pipelines`, `/errors` and `/stats`
(via `recent_errors`) return the stored `error_message`. A task that logs a row, or raises with one in the
message, publishes it there — so treat this as a debug view over your own data, and note that artifact payloads
themselves are reachable only through the store or `pyattacker export`. It binds to loopback for that reason.
Put your own proxy in front before exposing it anywhere else. `pyattacker serve` is the same thing from the
command line.

`pyattacker.monitor.render_snapshot(snapshot)` and `read_snapshot(store)` are the terminal renderer behind
`pyattacker watch`, if you want to embed the same view. Import them from the submodule:
`from pyattacker.monitor import render_snapshot, read_snapshot`.

---

## Where to look next

| Document | What is in it |
|---|---|
| [`docs/tutorial.md`](tutorial.md) | the guided path: seventeen runnable steps |
| [`docs/cli.md`](cli.md) | commands, flags, exit codes, config file format |
| [`docs/design.md`](design.md) | the model, the invariants, and the tradeoffs behind these APIs |
| [`examples/`](https://github.com/Hazer-BJTU/pyattacker/tree/main/examples) | complete programs, including a measured comparison of pipeline shapes |

## Suite API

`ExperimentSpec(id, factory, definition_digest, pool_aliases={}, label="", config_path="", ignored_run_fields=[], limit=None)` describes one replayable input factory. `SuiteSpec(id, experiments, output_root, layout="combined", pools=[], run={}, path="", unresolved_env=[])` composes members. Use `suite.runner(on_pipeline_finished=callback, **run_overrides)` and `runner.run(suite.pipelines(runner.store, experiments=None, limit=None))`. `SuiteStore(root, read_only=True, experiment=None, max_open=8)` opens either layout. Both layouts require this store capability. `TaskContext` exposes `suite_id`, `experiment_id`, `local_key`, `output_dir`; `PipelineRecord` also preserves `repeat`. `Runner.report_metric` accepts `experiment_id` for member metrics. Defaults written as empty containers here are dataclass factories, not shared mutable arguments.

[Full guide and examples](suites.md).
