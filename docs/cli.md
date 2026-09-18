# CLI Reference

**English** | [简体中文](zh-CN/cli.md)

```
pyattacker {run,resume,report,watch,export,serve,plugins,validate,demo}
```

Everything here is also reachable as `python -m pyattacker ...`. The commands split into three groups:
**run work** (`run`, `resume`, `demo`), **read results** (`report`, `watch`, `export`, `serve`), and
**check the setup** (`validate`, `plugins`).

## Exit codes

| Code | Meaning |
|---|---|
| `0` | every pipeline succeeded (or was skipped as already done) |
| `1` | the run finished but some pipelines failed, or this run could not repair one (`repair_failures`) |
| `2` | configuration error — nothing ran; also a run that ended on a named framework error (`WorkerCrashed`, `StoreUnavailable`) |
| `130` | interrupted (SIGINT); parked and in-flight pipelines are recorded as resumable |

`2` means "fix the config" (or "read the error"); `1` means "read the report". A `130` run is always safe to
`resume`.

The `1` case covers two different things, and the report says which: a pipeline that reached a failed
terminal state during this run (counted in `pipelines.by_state`), and a pipeline this run could not settle
out of a torn terminal state — `repair_failures` in the report (and in `--summary-format json`), because its
row deliberately keeps the original failure and the run that produced it.

A `2` on `run` is not always a config mistake: a worker that dies outside its own handlers raises
`WorkerCrashed` (printed as `WorkerCrashed: worker for pipeline … died with …`), after the pipeline it was
holding has been recorded as failed and the run record closed. The run's own record survives the error, so
`pyattacker report <store>` still shows what happened; fix the cause and rerun with `--resume`.

---

## `run` — execute a declarative config

```bash
pyattacker run -c config.yaml [options]
```

| Flag | Effect |
|---|---|
| `-c, --config PATH` | the config file: yaml, toml or json. The suffix picks the parser; yaml needs the optional extra |
| `--limit N` | run only the first N pipelines (a smoke test over a real dataset) |
| `--store PATH` | override `run.store`. Used verbatim with `--shard`; otherwise a shard suffix is appended |
| `--concurrency N` | override `run.concurrency` — attempts in flight, not pipelines alive |
| `--journal {full,summary}` | `full` (default) stores artifact payloads, which is what makes resume work at task granularity; `summary` keeps digests only |
| `--label TEXT` | a label recorded on the run, for telling runs apart later |
| `--resume` | skip finished pipelines, restart failed ones at their checkpoint |
| `--retry-succeeded` | with `--resume`, rerun even the pipelines that succeeded. It only widens *which* pipelines are eligible; it never discards an unfinished one's checkpoint |
| `--fresh-restart` | start admitted pipelines over from the seed: discard checkpoints/traversal, reset the control budget. Append-only history (attempts/events/handoffs) survives; a backward pipeline additionally keeps its visit occurrences and counters |
| `--strict-leases` | a leaked lease fails its task (`LeaseLeakError`) instead of being reclaimed quietly. Worth turning on in CI |
| `--stop-after-failures N` | stop admitting work once N pipelines have failed (best-effort: already-admitted pipelines still finish) |
| `--no-signals` | do not install SIGINT/SIGTERM handlers |
| `--artifact-backend SPEC` | where payload bytes live: `inline` (default), `null`, `file:///path`, or a JSON spec |
| `--progress` | print live progress from a second connection to the same store |
| `--shard I/N` | run only shard `I` of `N`. Each shard writes its own store file |
| `--shards N` | spawn N children locally, one per shard, then print a merged report |
| `--jobs N` | how many shard children run at once (with `--shards`) |
| `--summary-format {text,json}` | `json` prints one summary object per run |
| `--no-write-behind` | commit every attempt and event immediately instead of batching them |
| `--strict-env` | exit `2` on an unset `${VAR}` in the config instead of warning |

`--shard` and `--shards` are alternatives: the first is one process doing its share (you orchestrate the
rest), the second is the convenience path where pyattacker orchestrates locally. Shard assignment is a pure
function of the content-addressed pipeline key, so the same dataset always splits the same way and `--resume`
lands every pipeline back in the shard that owns it. Each child also gets `PYATACKER_SHARD=I/N` in its
environment.

## `resume` — `run --resume`

Identical flags to `run`. Rerun the exact command you used originally, with `resume` in place of `run`:

```bash
pyattacker run    -c qa.yaml --shards 4 --store runs/qa.db
pyattacker resume -c qa.yaml --shards 4 --store runs/qa.db
```

What actually reruns: nothing for a pipeline that succeeded; for a failed or interrupted one, the first task
that produced no artifact and everything after it. A task whose artifact is on disk is never re-executed, so
requests that already cost money are not re-sent. Two things defeat this and both leave a
`pipeline.checkpoint_missing` event behind: `journal: summary` and the `null` artifact backend, neither of
which keeps the payload a checkpoint needs.

`--resume` is also what claims a pipeline whose row still says `running` — the shape a hard kill leaves. A
backward-enabled pipeline is never taken over without it: the run skips that row (`pipeline.skipped` with
`reason="owned_by_another_run"`) rather than forking a traversal another run may still own. A restart that
should *discard* a checkpoint instead of resuming it is `--fresh-restart` (`fresh_restart=True`), which also
resets a spent handoff budget; see [backward traversal](backward.md#recovery-and-ownership).

## `demo` — verify the install with zero config

```bash
pyattacker demo [--store PATH] [--pipelines N] [--concurrency N] [--fail-rate F] [--export PATH]
```

Simulated tasks, no network, no config file. `--fail-rate` (e.g. `0.3`) makes failures happen so you can watch
retries and read the resulting decision records. Useful as a CI smoke test.

---

## `bench` — compare the acquire algorithms in simulation

```bash
pyattacker bench [--scenario NAME] [--list] [--algorithms A,B] [--seeds N]
                 [--jobs N] [--concurrency N] [--horizon S] [--wall-budget S]
                 [--clock {virtual,real}] [--speedup F] [--json PATH] [--markdown PATH] [--quiet]
```

No network and no provider: a scenario is a written-down world (capacity cycle, token bucket, latency
tail, failure storms, three endpoints of different character), the client is a closed loop of workers
driving the real `Pool` and the real algorithm, and time is simulated. `docs/benchmark.md` explains the
assumptions, the metrics and how to read the table; this table is the flag surface.

| Flag | Effect |
|---|---|
| `--scenario NAME` | which simulated world to run in; default `bursty_provider` |
| `--list` | print the scenarios, the algorithms and every metric with its unit and direction, then exit 0 |
| `--algorithms A,B` | comma-separated subset; default is every algorithm the scenario can exercise — asking for one it declares unsuited runs it anyway with an N/A column (e.g. `failover`, which a single-pool scenario cannot show at its best) |
| `--seeds N` | how many seeds to average over, as `scenario.seed + 0 .. N-1` (default 3; at least 1) |
| `--jobs N` | override the scenario's job count (at least 1) |
| `--concurrency N` | override the worker count — the client's in-flight count, so an assumption as well as a cost knob (at least 1) |
| `--horizon S` | override the simulated-time horizon, which stops new jobs rather than truncating one (positive) |
| `--wall-budget S` | real seconds any single run may take (default 600, positive); a run that cannot finish raises rather than returning partial metrics |
| `--clock {virtual,real}` | `virtual` (default) is simulated time that costs nothing; `real` replays the scenario in compressed real time to validate the simulator, and is much slower |
| `--speedup F` | compression factor for `--clock real` (default 10, positive) |
| `--json PATH` | write the full report as JSON (`-` for stdout) |
| `--markdown PATH` | write the report as a markdown table, including per-endpoint admissions |
| `--quiet` | no progress on stderr |

The table goes to stdout and progress to stderr, so `pyattacker bench --json - --quiet | jq .` composes.
Exit codes follow the rest of the CLI: `0` for a completed sweep, `2` for an unknown scenario or
algorithm. A benchmark that cannot finish is a `2` as well — never a partial table. A budget outside its
range is refused the same way (`--seeds 0` says "at least 1") rather than clamped: a sweep that silently
ran a different experiment than the one it announced would report the wrong numbers under the right
heading.

---

## `report` — statistics and failures

```bash
pyattacker report STORE [STORE ...] [--run-id ID] [--errors N] [--json] [--artifact-backend SPEC]
```

Several stores are merged into one coherent view: de-duplicated by `pipeline_id` (best state wins, latest
finish breaks ties) with statistics recomputed from the merged rows, and it tells you how many rows it folded.
`--errors N` prints the first N failures with their error class. `--json` gives you the same numbers as an
object. A run that recorded handoffs says so in its summary line (`... attempts: total=12 handoffs=2`), and
`--rows pipelines` on `export` carries the ledger itself (see
[advanced: handoffs](reference.md#advanced-handoffs-opt-in)).

## `watch` — live monitoring

```bash
pyattacker watch STORE [--run-id ID] [--interval S] [--iterations N] [--no-clear]
```

A read-only connection to the same SQLite file, so it runs beside a live run (WAL allows one writer and many
readers). Shows the pipeline state distribution, latency percentiles, per-pool `active/capacity`,
`ready/degraded/dead`, how many pipelines are waiting or parked, and recent errors. A run with handoffs also
shows `handoffs=N` (commits in the selected scope, all history without a run filter; a resume may reuse earlier active handoffs) on its attempts line — on a control-enabled pipeline the cursor is a position, not a
progress count, so that number is what explains a short task list. `--iterations N` makes it
exit on its own, which is what you want in a script.

## `export` — records out

```bash
pyattacker export STORE [STORE ...] OUTPUT [--rows SHAPE] [--format FMT] [--run-id ID]
```

| `--rows` | One row per |
|---|---|
| `pipelines` (default) | pipeline, nested — tasks and final artifact included |
| `tasks` | task: final state, duration, error, leases used |
| `attempts` | attempt, including each retry `decision` |
| `events` | structured event |
| `artifacts` | artifact, intermediate ones included |

`--format` is `jsonl` (default), `json` or `csv`. CSV takes its header from the first rows and folds later
keys into an `extra` column, so memory stays flat and no field is silently dropped.

Every kind is exported in full: a store with more than 100 000 events used to lose everything older than
the newest 100 000 from `--rows events`. Rows come out oldest-first for `events`/`attempts` and in
pipeline/`seq` order for `tasks`/`artifacts`, read from the store in bounded batches — [the export
reference](reference.md#export) has the exact per-kind order, the `limit` rule, the memory notes and what
an export of a store that is still being written does and does not guarantee.

## `serve` — read-only HTTP view

```bash
pyattacker serve STORE [--host HOST] [--port PORT] [--run-id ID]
```

`/` is a small auto-refreshing dashboard; `/stats`, `/events`, `/pipelines`, `/resources`, `/errors` are JSON.
A fresh read-only connection per request means it is safe beside a live run.

**It has no authentication and exposes your artifact payloads.** It binds to loopback for that reason. Put
your own proxy in front before binding it anywhere else.

---

## `validate` — check a config without running

```bash
pyattacker validate -c config.yaml [--strict-env]
```

Parses the config, resolves every `use:` target and pool, checks the whole document and prints the
effective configuration. Exit code `2` with the reason if anything is wrong. `--strict-env` promotes an
unset `${VAR}` from a warning to an error — worth it in CI, where a silently empty API key is worse than a
failed job.

`run` and `resume` go through the same check before creating a store, so a config that `validate` refuses
is a `2` there too, and no task starts. What is checked, with the field path in every message:

| Area | Examples |
|---|---|
| unknown fields | `run.concurency` (with a "did you mean `run.concurrency`?"), `pools.apis.capcity` |
| field types | `run.concurrency: "8"`, `pipeline.tasks[0].use: 5` |
| numeric ranges | `run.concurrency: 0`, `run.heartbeat_s` at or below 0, `pools.apis.capacity: 0` |
| pool references | `pipeline.resource`, `pipeline.tasks[0].resource`, and the `resource` a `use:` factory declares itself |
| algorithms | `algorithm: nosuchalgorithm`, `algorithm: {name: backoff, bse: 1}` |
| artifact backend | `artifact_backend: {kind: file}` with no `root`, an unknown `kind`, a JSON-string spec that does not parse, or a `min_bytes` that is not a non-negative integer |
| sections | `pipeline:` (and every other section) must be a mapping — a scalar or a list is a config error, not a traceback |
| source | `source.kind` must be `range`/`jsonl`; `jsonl` requires `source.path`; `source.repeats` at least 1 |
| retry | `"on"` names, and the numeric/boolean fields of a retry block |

It is a *declaration* check only: it does not open `source.path`, walk the dataset, build an
`artifact_backend` (which would create its directory), or call a task. It does, however, check that an
`artifact_backend` is one `resolve_backend` could construct — required fields such as a file backend's
`root` included — so `validate` and `run` accept and refuse exactly the same specs. `${VAR}` values are
expanded as described below.

## `plugins` — what is installed

```bash
pyattacker plugins [--json]
```

Lists discovered entry points for `pyattacker.tasks`, `pyattacker.algorithms`, `pyattacker.codecs` and
`pyattacker.stores`, **including the ones that failed to load and why**. A broken plugin is recorded, never
raised, so it can never take a run down — but it also never fails silently.

---

## Config file reference

The example below is YAML, but every command that takes `-c` accepts the same document as `.json` or
`.toml`; the loader picks the parser from the suffix. Only the YAML parser is an optional dependency
(`pip install "pyattacker[yaml]"`) — and a missing one is reported as a config error naming the extra,
exit code 2, before anything runs. The block is complete (every `use:` target is a built-in) and is passed
through `validate` by the test suite.

```yaml
# example/cli_config_reference.yaml
run:
  store: runs/demo.db          # path, ":memory:", or a plugin scheme like s3://bucket/runs.db
  concurrency: 8
  journal: full                # full | summary
  label: demo
  artifact_backend: { kind: file, root: /data/blobs, min_bytes: 262144 }

pools:
  apis:
    kind: llm
    algorithm: backoff         # wait | backoff | least_busy | failover | sticky | quota_aware | immediate
    resources:
      - id: api-1
        capacity: 4            # concurrent leases this endpoint allows
        options: { model: gpt-4o, api_key: "${OPENAI_KEY:-sk-demo}" }

pipeline:
  name: qa_eval
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: pyattacker.tasks:simulate_llm   # or your own task: my_pkg.tasks:ask_model
      resource: apis
      algorithm: backoff
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }   # factory arguments
      # YAML 1.1 parses a bare `on` as boolean true, so the retry key must be quoted
      retry: { max_attempts: 3, base: 0.2, "on": [RetryableError, TimeoutError] }

source: { kind: jsonl, path: data.jsonl, limit: 100, key_field: id, repeats: 1 }
```

The `run:` block accepts exactly the fields the CLI maps onto `RunConfig`: `store`, `journal`,
`concurrency`, `label`, `heartbeat_s`, `grace_s`, `stale_after_s`, `strict_leases`,
`stop_after_failures`, `stop_after_s`, `max_handoffs`, `retry_succeeded`, `fresh_restart`, `seed`, `notes`,
`write_behind`,
`write_batch`, `flush_interval`, `artifact_backend` and `meta`. Anything else is a config error, not a
quietly ignored line. Precedence is explicit: a flag on the command line wins over the `run:` block,
which wins over the built-in default.

`artifact_backend` takes the same values as `--artifact-backend`: `"inline"` (the default, payloads stay
in the store), `"null"` (keep the digest, drop the bytes), `"file:///data/blobs"`, or the mapping form
above. In YAML, quote it: a bare `artifact_backend: null` is the null *value*, which means `inline` (the
same as omitting the field), while `"null"` is the backend that drops payload bytes. A resumed run needs
the payloads a checkpoint is made of, so `journal: summary` and the null backend both cost you task-level
resume.

Task entries also accept `config: {model: model-a}` and `version: "prompt-v2"` for behavior that
cannot be inferred from source. These join the recovery fingerprint; factory parameters remain
separate. With `source.key_field`, changing task identity or seed contents under an existing key
raises a config error (exit 2) and preserves the old result. Use a new key/store for changed work.
See [resume identity](reference.md#resume-identity) before upgrading an existing store.

A task entry is an **override over the resolved `use:` target**, and the two cases are distinct: a field
the entry does not mention keeps whatever the target declares (its own `resource`, `algorithm`,
`timeout_s`, `retry`, ...), while an explicit `field: null` clears a field that supports being empty
(`resource`, `algorithm`, `timeout_s`, `version`). That is the same rule as
[`TaskSpec.with_overrides`](reference.md#taskspec) in the SDK.

`${VAR}` is expanded from the environment (see `--strict-env`). `source.kind` is `jsonl` or `range`;
`repeats: k` is pass@k — k independent pipelines per seed. CLI flags override the `run:` block.

### `pipeline.control` — advanced, opt-in handoffs

A config may also declare which task is allowed to **hand off** (skip ahead) by returning a
[`Handoff`](reference.md#advanced-handoffs-opt-in), so the same pipeline can be expressed declaratively:

```yaml
pipeline:
  name: qa_eval
  control:
    edges:
      judge: [report, end]   # judge may continue at report, or finish the pipeline
      ask: [report]
  tasks:
    - { use: pyattacker.tasks:echo, name: fetch }
    - { use: my_pkg.tasks:ask_model, name: ask, resource: apis }
    - { use: my_pkg.tasks:judge, name: judge }
    - { use: my_pkg.tasks:report, name: report }
```

A destination is a task name, a task's numeric seq, or `end`, and it must be strictly later than its source
(the `edges` operation is forward-only). A name that appears twice in the chain must be given as a seq. Declaring
`end` from the *last* task is refused, because it would have no effect. Every problem is reported as a field
path — `pipeline.control.edges['judge'][0]: destination 'fetch' (seq 0) is not later than the source
'judge' (seq 2); v1 handoffs are forward-only` — under `validate` (exit 2) as well as `run`, because both go
through the same validation entry. Within an `edges` block there is no other key and no `mode` in this
version; backward traversal declares its own `rewind` / `retry_all` / `max_handoffs` keys instead (see
[Advanced backward control declarations](#advanced-backward-control-declarations)).

Numeric source keys work across YAML, JSON and TOML: JSON/TOML spell seq 0 as the key `"0"`
(e.g. `"edges": {"0": [2]}`). An exact task name takes precedence over a numeric string. Numeric
destinations remain integers. Declaring the same source twice through a name and a seq is an error,
rather than silently replacing one list. Effective configuration uses numeric seqs for repeated or
reserved (`end`) names instead of inventing a `name#N` syntax.

The feature is **advanced**: it changes the execution model, so it is opt-in, marked experimental until 1.0,
and needs a store that can commit a handoff atomically (both built-in backends can). Pipelines without the
block are unaffected in every respect. See
[reference → advanced: handoffs](reference.md#advanced-handoffs-opt-in) for the API and
[design §4.8](design.md#48-advanced-handoffs--declared-forward-jumps-opt-in-experimental) for the model.

The declarative layer describes **composition and resources only**; the logic stays in Python behind `use:`.
Anything it cannot express is a reason to use the SDK, not a reason to add YAML — see
[`docs/tutorial.md`](tutorial.md) step 11 for where the line falls.

### Advanced backward control declarations

`pipeline.control` also accepts `rewind: {source: [earlier_targets]}`, `retry_all: [sources]` and
required positive `max_handoffs` for backward traversal. `edges` is optional for backward-only plans;
existing forward-only declarations stay unchanged. `run.max_handoffs` sets the runtime ceiling (default
1000). Validation shares Python's name/seq resolution and reports configuration field paths. Rewind payloads
are chosen by task code, not config. See [the backward guide](backward.md) for working Python examples,
state-history interfaces, budget lifecycle and missing-payload recovery.

## See also

* [`docs/tutorial.md`](tutorial.md) — the guided path, with runnable programs
* [`docs/reference.md`](reference.md) — every class and function the SDK exposes
* [`docs/design.md`](design.md) — why the CLI has these commands and no others
* [`README.md`](../README.md) — the compact tour
