# CLI Reference

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
| `1` | the run finished but some pipelines failed |
| `2` | configuration error — nothing ran |
| `130` | interrupted (SIGINT); parked and in-flight pipelines are recorded as resumable |

`2` means "fix the config"; `1` means "read the report". A `130` run is always safe to `resume`.

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
| `--retry-succeeded` | with `--resume`, rerun even the pipelines that succeeded |
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

## `demo` — verify the install with zero config

```bash
pyattacker demo [--store PATH] [--pipelines N] [--concurrency N] [--fail-rate F] [--export PATH]
```

Simulated tasks, no network, no config file. `--fail-rate` (e.g. `0.3`) makes failures happen so you can watch
retries and read the resulting decision records. Useful as a CI smoke test.

---

## `report` — statistics and failures

```bash
pyattacker report STORE [STORE ...] [--run-id ID] [--errors N] [--json] [--artifact-backend SPEC]
```

Several stores are merged into one coherent view: de-duplicated by `pipeline_id` (best state wins, latest
finish breaks ties) with statistics recomputed from the merged rows, and it tells you how many rows it folded.
`--errors N` prints the first N failures with their error class. `--json` gives you the same numbers as an
object.

## `watch` — live monitoring

```bash
pyattacker watch STORE [--run-id ID] [--interval S] [--iterations N] [--no-clear]
```

A read-only connection to the same SQLite file, so it runs beside a live run (WAL allows one writer and many
readers). Shows the pipeline state distribution, latency percentiles, per-pool `active/capacity`,
`ready/degraded/dead`, how many pipelines are waiting or parked, and recent errors. `--iterations N` makes it
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

Parses the config, resolves every `use:` target and pool, and prints the effective configuration. Exits `2`
with the reason if anything is wrong. `--strict-env` promotes an unset `${VAR}` from a warning to an error —
worth it in CI, where a silently empty API key is worse than a failed job.

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
exit code 2, before anything runs.

```yaml
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
        options: { model: gpt-4o, api_key: "${OPENAI_KEY}" }

pipeline:
  name: qa_eval
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: my_pkg.tasks:ask_model     # or a plugin name registered in pyattacker.tasks
      resource: apis
      algorithm: backoff
      timeout_s: 30
      retry: { max_attempts: 3, base: 0.2, on: [RetryableError, TimeoutError] }

source: { kind: jsonl, path: data.jsonl, limit: 100, key_field: id, repeats: 1 }
```

Task entries also accept `config: {model: model-a}` and `version: "prompt-v2"` for behavior that
cannot be inferred from source. These join the recovery fingerprint; factory parameters remain
separate. With `source.key_field`, changing task identity or seed contents under an existing key
raises a config error (exit 2) and preserves the old result. Use a new key/store for changed work.
See [resume identity](reference.md#resume-identity) before upgrading an existing store.

`${VAR}` is expanded from the environment (see `--strict-env`). `source.kind` is `jsonl` or `range`;
`repeats: k` is pass@k — k independent pipelines per seed. CLI flags override the `run:` block.

The declarative layer describes **composition and resources only**; the logic stays in Python behind `use:`.
Anything it cannot express is a reason to use the SDK, not a reason to add YAML — see
[`docs/tutorial.md`](tutorial.md) step 11 for where the line falls.

## See also

* [`docs/tutorial.md`](tutorial.md) — the guided path, with runnable programs
* [`docs/reference.md`](reference.md) — every class and function the SDK exposes
* [`docs/design.md`](design.md) — why the CLI has these commands and no others
* [`README.md`](../README.md) — the compact tour
