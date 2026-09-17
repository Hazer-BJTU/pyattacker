# API Reference

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

## Contents

| Section | Names |
|---|---|
| [Tasks](#tasks) | `task`, `build_task_spec`, `TaskSpec`, `Retrying`, `TaskContext`, `with_retry` |
| [Pipelines](#pipelines) | `pipeline`, `PipelineTemplate`, `PipelineSpec`, `Chain`, `compute_spec_digest` |
| [Running](#running) | `Runner`, `RunConfig`, `RunReport` |
| [Resources](#resources) | `Resource`, `Pool`, `Lease`, `PoolStats`, `Bus`, `ResourceState`, `ResourceEvent` |
| [Acquire algorithms](#acquire-algorithms) | `Wait`, `Backoff`, `LeastBusy`, `Failover`, `Sticky`, `QuotaAware`, `Immediate`, `resolve_algorithm` |
| [Errors](#errors) | the exception hierarchy, `error_class_of` |
| [Artifacts and codecs](#artifacts-and-codecs) | `Artifact`, `Codec`, `CodecRegistry`, `JsonCodec`, `BytesCodec`, `Encoded`, `canonical_json`, `digest_of` |
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
pipeline(name, *tasks_or_chain, tags=None, include_code=True, registry=None) -> PipelineTemplate
```

| Parameter | Meaning |
|---|---|
| `name` | pipeline name, recorded on every row |
| `*tasks_or_chain` | one chain (`a \| b \| c`), or several `TaskSpec`s as separate arguments |
| `tags` | free-form dict stored with each pipeline, for filtering later |
| `include_code` | when `True` (default), each task's source digest is part of the pipeline's identity |
| `registry` | a custom `CodecRegistry` for non-JSON artifact types |

```python
from pyattacker import pipeline

template = pipeline("qa", prepare | ask | judge, tags={"bench": "mmlu"})
template = pipeline("qa", prepare, ask, judge)          # equivalent
template = pipeline("qa", ask)                          # a single task is a valid pipeline
```

The chain is validated **here**, not mid-run: if `prepare` returns `dict` and the next task requires `int`,
this raises `PipelineBuildError` immediately. Checking uses annotations — subclasses are accepted, `Any` or
a missing annotation is permissive, and a bare container accepts its parameterised form.

`include_code=False` is for the case where you deliberately want a task's body to change without abandoning
existing checkpoints. The default is the safe direction: edited code means a new pipeline.

### `PipelineTemplate`

| Attribute / method | Returns |
|---|---|
| `name`, `tags`, `tasks`, `spec_digest` | the declaration |
| `n_tasks` | how many tasks in the chain |
| `task_names` | their names in order |
| `describe()` | a JSON-ready summary (what `validate` prints) |
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

def stream():                                                # memory stays O(concurrency)
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
compute_spec_digest(tasks, *, include_code=True) -> str
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

`run()` accepts any iterator of `PipelineSpec`, so a generator keeps memory flat regardless of dataset size.

### `RunConfig`

Everything that shapes one run. Pass a `RunConfig`, or pass its fields as keyword arguments to `Runner`.

| Field | Default | Meaning |
|---|---|---|
| `store` | `":memory:"` | `":memory:"`, a SQLite path, a plugin URI, or an open store instance |
| `journal` | `"full"` | `"full"` keeps artifact payloads (**required for task-level resume**); `"summary"` keeps only metadata |
| `concurrency` | `16` | max attempts in flight; a pipeline parked in a retry backoff does not hold a slot |
| `label` | `""` | a label recorded on the run |
| `run_id` | `None` | explicit run id; default is timestamp + digest |
| `resume` | `False` | mark pipelines abandoned by dead runs as resumable before scheduling |
| `retry_succeeded` | `False` | re-run pipelines already marked succeeded |
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
| `skipped` | how many pipelines were skipped because they had already succeeded |
| `leases_leaked` | how many leases had to be force-reclaimed |
| `stop_reason` | why the run stopped early, if it did |
| `summary()` | a human-readable multi-line report |
| `to_dict()` | the same facts, machine-readable |
| `export_jsonl(path, *, scope="store", run_id=None)` | write pipeline rows, returns the count |

```python
report = runner.run(template.map(rows))

print(report.summary())                                  # for a human
metrics = report.to_dict()                               # for a dashboard
print(report.stats["pipelines"]["by_state"])             # {'succeeded': 98, 'failed': 2}

if report.status != "completed" or report.stats["pipelines"]["by_state"].get("failed"):
    report.export_jsonl("runs/failures.jsonl")
    raise SystemExit(1)                                  # fail your CI job
```

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
└── StoreUnavailable         the store became untrustworthy
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
| `pipeline_id`, `seq` | its identity; `seq=-1` is the pipeline's seed |
| `task_name` | which task produced it |
| `type_name`, `codec` | how to restore it |
| `digest`, `size` | `blake2b` of the payload, and its length |
| `payload` | the encoded bytes, or `None` when they live in a backend or were dropped |
| `blob_ref` | where the bytes live when they are not inline |
| `is_final` | whether this is the pipeline's final output |
| `available` | whether the payload can actually be read back — **this is what resume checks** |
| `encoded()` | an `Encoded` pair ready for `registry.load(...)` |

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

### Tables and readers

| Table | One row per | Read with |
|---|---|---|
| `runs` | run | `store.get_run(id)` |
| `pipelines` | pipeline: state, checkpoint cursor, digests, tags | `store.pipelines(...)`, `store.export_rows()` |
| `tasks` | task: final state, attempts used, duration, error | `store.tasks(...)` |
| `attempts` | attempt: outcome, error class, **retry decision**, leases, duration | `store.attempts(...)` |
| `artifacts` | artifact | `store.artifacts(pid)`, `store.get_artifact(pid, seq)` |
| `events` | structured event | `store.events(...)` |
| `resources` | resource: spec (secrets masked) and health stats | included in `stats()` |

| Method | Returns |
|---|---|
| `stats(run_id=None)` | counts, state distribution, latency percentiles |
| `errors(*, run_id=None, limit=20)` | failures with task name, error type and message |
| `export_rows(*, run_id=None)` | nested pipeline rows: tasks and artifacts included |
| `attempts(*, pipeline_id=None, ...)` | attempt history |
| `events(*, pipeline_id=None, limit=...)` | the event stream |
| `close()` | close the connection |

```python
# "Why did this pipeline take 40 seconds?"
for attempt in store.attempts(pipeline_id=pid):
    print(f"{attempt.task_name} #{attempt.attempt_no} {attempt.outcome} "
          f"{attempt.duration_ms}ms class={attempt.error_class} {attempt.decision}")

# The full story of one pipeline.
print([event.kind for event in store.events(pipeline_id=pid)])
```

Record types with typed fields: `PipelineRecord`, `AttemptRecord` and `EventRecord` are exported;
`TaskRecord`, `RunRecord` and the `Store` protocol live in `pyattacker.store.base`. `SqliteStore` and
`MemoryStore` are the implementations, and `open_store` is how you get one.

`journal="summary"` keeps digests and metadata but no payloads. It saves space and costs you task-level
resume — with no artifact to restore, a resumed pipeline starts over and records
`pipeline.checkpoint_missing`.

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

De-duplicates by `pipeline_id` (best state wins, latest finish breaks ties), then **recomputes** statistics
from the merged rows. Merging is idempotent, so a store counted twice does not inflate anything.

| `MergedReport` member | Meaning |
|---|---|
| `rows` | the merged pipeline rows |
| `duplicates` | how many rows were folded away |
| `sources` | which stores contributed |
| `stats()` | recomputed statistics |
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
attempt number. Respect the provider's retention window and API contract. For file sinks, upsert
by the same identity or write one atomically replaced file per pipeline/task; a plain append-only
`write_jsonl` task can create duplicate lines on replay. Resource lease safety does not make those
external side effects idempotent.

---

## Export

Five row shapes, three formats, streaming throughout.

| Name | Value |
|---|---|
| `ROW_KINDS` | `("pipelines", "tasks", "attempts", "events", "artifacts")` |
| `FORMATS` | `("jsonl", "json", "csv")` |

| Function | Purpose |
|---|---|
| `iter_rows(store, *, kind="pipelines", run_id=None)` | stream rows as dicts |
| `export_store(store, path, *, kind="pipelines", fmt="jsonl", run_id=None)` | write one store, returns the row count |
| `export_stores(paths, path, *, kind=..., fmt=...)` | write several shard stores as one merged file |

```python
from pyattacker import export_store, iter_rows

# Compute your own metric — the framework stores facts and leaves semantics to you.
correct = sum(1 for row in iter_rows(store, kind="pipelines")
              if row["state"] == "succeeded" and row["artifacts"][-1]["payload"]["correct"])
print(f"accuracy: {correct / total:.1%}")

# Retry analysis in a spreadsheet.
export_store(store, "attempts.csv", kind="attempts", fmt="csv")
```

`pipelines` rows are nested — tasks and artifacts included — which is why it is the default. CSV takes its
header from the first rows and folds later keys into an `extra` column, so memory stays flat and no field is
dropped silently.

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
is in [`examples/plugin_package/`](../examples/plugin_package/README.md).

---

## Built-in tasks

Two of these are for production use (`fanout`, `shell_run`, `write_jsonl`); the rest simulate work so you can
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

**Process lifetime.** The task owns the process it starts, and every exit path runs the same cleanup.
A normal exit is left alone (its result is returned as before, and `check=False` still reports a
non-zero `returncode` instead of raising). If `timeout_s` expires, the coroutine is cancelled (a
`Runner` stop, a group task timeout, an outer `asyncio` cancellation) or any other exception escapes,
a child that is still running is killed with `SIGKILL` and then **reaped** before that exception
continues to the caller: cancellation still arrives as `CancelledError` and a timeout as
`TimeoutError`, but no process is left running behind them. There is no graceful `SIGTERM` window —
cleanup does not wait for a child to finish. The wait for the OS to report the exit is bounded (5 s),
which only matters for a process the OS never reports as exited, and nothing cleanup itself runs into
(a reader left in a bad state by a cancelled `communicate()`, a failed signal) is allowed to replace
the caller's exception: cleanup is best effort, the caller's error type is not.

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
| `/stats`, `/events`, `/pipelines`, `/resources`, `/errors` | JSON |

**It has no authentication and it serves your artifact payloads.** It binds to loopback for that reason. Put
your own proxy in front before exposing it anywhere else. `pyattacker serve` is the same thing from the
command line.

`pyattacker.monitor.render_snapshot(snapshot)` and `read_snapshot(store)` are the terminal renderer behind
`pyattacker watch`, if you want to embed the same view. Import them from the submodule:
`from pyattacker.monitor import render_snapshot, read_snapshot`.

---

## Where to look next

| Document | What is in it |
|---|---|
| [`docs/tutorial.md`](tutorial.md) | the guided path: fourteen runnable steps |
| [`docs/cli.md`](cli.md) | commands, flags, exit codes, config file format |
| [`docs/design.md`](design.md) | the model, the invariants, and the tradeoffs behind these APIs |
| [`examples/`](../examples) | complete programs, including a measured comparison of pipeline shapes |

