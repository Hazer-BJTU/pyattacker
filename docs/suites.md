# Experiment suites

**English** | [简体中文](zh-CN/suites.md)

A Suite combines independent experiments under one Runner. They share the global worker
limit and, when explicitly bound, the **same Pool instance**: capacity, health, cooldown and
quota state are shared. Each experiment retains its own pipeline identities, checkpoints,
results and application metrics. There are no dependencies between experiments.

## Compose existing configurations

The runnable example reuses the same standalone experiment twice, deliberately demonstrating
that identical samples are independent in different members:

```bash
uv run pyattacker validate -c examples/suites/suite.json
uv run pyattacker run -c examples/suites/suite.json
uv run pyattacker resume -c examples/suites/suite.json --experiment baseline
uv run pyattacker report runs/suite-demo
uv run pyattacker watch runs/suite-demo --experiment candidate
uv run pyattacker serve runs/suite-demo
uv run pyattacker export runs/suite-demo runs/all.jsonl --rows results
uv run pyattacker export runs/suite-demo runs/exported --rows results --by-experiment
```

The complete configuration is [examples/suites/suite.json](../examples/suites/suite.json).
The member is [experiment.json](../examples/suites/experiment.json), which still works with
ordinary `run -c`. JSON and TOML need no extra dependency; YAML requires the `yaml` extra.

```json
{
  "suite": {"id": "comparison"},
  "run": {"concurrency": 16},
  "output": {"root": "runs/comparison", "layout": "by_experiment"},
  "pools": {
    "shared": {"resources": [{"id": "api-1", "capacity": 4}]}
  },
  "experiments": {
    "baseline": {"config": "baseline.json", "pool_bindings": {"apis": "shared"}},
    "candidate": {"config": "candidate.json", "pool_bindings": {"apis": "shared"}}
  }
}
```

`experiments.ID.config` is relative to the **Suite configuration file**. Paths *inside the
referenced configuration* retain standalone semantics: `source.path` remains relative to the
process working directory. Arbitrary task arguments are never rewritten and the process never
changes directory. `output.root` is relative to the Suite file; CLI `--output-root` is relative
to the working directory. This distinction lets existing member files work unchanged.

The Suite's `run` block owns execution settings. Member `run` blocks are validated but not
applied: `validate` lists their `ignored_run_fields`, the effective Suite run overrides and
pool mappings. CLI execution flags override the Suite. `run.store` and `--store` are rejected
in Suite mode; there is only one storage entry point, `output.root`.

`pool_bindings` maps each experiment's local name to a top-level shared pool. It applies both
to task declarations and to explicit calls such as `ctx.acquire("apis")`, publishing and
subscribing. A bound local pool definition is replaced by the shared definition. Unbound
local pools receive an internal `experiment:ID:NAME` name and remain independent, even if
another member also calls its pool `apis`. No implicit same-name pool merging occurs.

Member entries also accept `label` (display only) and `limit` (maximum expanded pipelines for
that member, including repeats). CLI `--limit` limits total expanded submissions. A limit
never claims the source was exhausted. Repeat `--experiment ID` to select several members;
unselected members retain their existing state. Nested suites and parameter-matrix expansion
are not supported.

## Identity, admission and recovery

`suite_id` and `experiment_id` are stable identifiers; `run_id` identifies one invocation.
IDs use 1–80 ASCII letters, digits, hyphens or underscores, start with a letter or digit, and
exclude reserved device names. Member IDs must be case-insensitively unique.

Suite pipeline IDs include a digest of `(suite_id, experiment_id)` and a digest of the
original local key. `key == pipeline_id` remains true; `local_key` preserves the original
key separately and exports retain `repeat`. For referenced configs with `source.key_field`,
the local key is the sample field (with the existing repeat suffix), without the template
name. Renaming a display label or template, reordering members, and adding other members do
not change existing IDs. Legacy pipelines outside Suite mode retain their old IDs.

Each member has a persisted definition digest covering its task fingerprint, source
configuration, effective pool declarations/bindings and member limit. Pool declarations are
hashed before environment expansion, so rotating `${API_KEY}` does not change identity.
Changes to literal pool settings or bindings still change the digest. If an environment change
alters experiment semantics (rather than credentials), use a new member ID or output root.
Changing the definition under an existing ID is rejected. The digest is not a snapshot of
all source file bytes; inputs must remain replayable and stable. Existing spec/seed identity
checks still protect explicit keys against changed work. SDK users supply their own digest
and must include configuration/input semantics not already covered by the task fingerprint.

Input factories are consumed round-robin with bounded admission. This improves submission
fairness; it does not reserve equal execution time or pool capacity. Unknown-length sources
stay unknown. Persisted `source_exhausted`, source errors, limits and per-run membership
separate “submitted work finished” from “the whole experiment finished.” The displayed member
state is reconstructed from source facts and pipeline checkpoints; stored status summaries
are not recovery checkpoints.

A member's source exception marks that member failed and lets other members continue.
Ordinary task failures also leave independent members running. Storage/worker failures stop
the Suite. CLI exit codes remain `0` for success, `1` for task/source failures, `2` for invalid
configuration and `130` for interruption. A deliberately limited run may exit `0` with an
`incomplete` member; inspect `source_exhausted` rather than treating exit status as proof of
complete source consumption.

Recovery skips successes and resumes incomplete pipelines at task checkpoints, including
handoff/backward-visit state. It requires the original configuration and replayable sources
for unsubmitted inputs; persisted seeds do not recreate an unread input tail. Suite output
allows one active writer, with independent read-only monitors. Suite `--shard`/`--shards` are
rejected because separate processes cannot share the same live pool quotas; ordinary
single-experiment sharding remains supported.

## Output layouts

`combined` is the default. `by_experiment` is optional and separates full state, not just
export files:

```text
combined/                       by_experiment/
  manifest.json                   manifest.json
  state.db                        suite.db
  artifacts/                      experiments/
                                    baseline/
                                      state.db
                                      artifacts/
                                    candidate/
                                      state.db
                                      artifacts/
```

`artifacts/` is created only when using the layout-owned file backend (`run.artifact_backend:
"file"` or `--artifact-backend file`). The default remains inline payloads in SQLite. In Suite
mode the exact shorthand `file` means a file backend rooted in each layout's `artifacts/`;
explicit paths, file URIs and backend mappings retain their ordinary external-backend meaning.
Layout-owned file references are relative and travel with the output root. Reopening inherits
the manifest backend; an explicit specification must match it exactly.
Changing backends in place is rejected before opening the database. External backends
keep their own path/locator contract and are not moved or copied automatically.

`manifest.json` contains the layout version, stable Suite identity, catalog locator and an
optional backend specification. It never copies expanded member configs or task secrets.
The catalog stores member definition digests and per-run membership. A task's state, artifacts,
reset and visit transactions all stay in one data database. There is no cross-database atomic
checkpoint or second authoritative checkpoint in the catalog. At most eight child connections
are cached by default; eviction flushes pending history. Selective runs only write to selected
member databases, including when reopening an evicted connection; reading other members does
not create run records or migrate their databases. Missing child databases are errors,
never silently recreated during recovery.

Results are generated on explicit export. `--by-experiment` treats its output argument as a
directory and writes `OUTPUT/ID/ROWS.FORMAT` (for example `results.jsonl` or `attempts.jsonl`);
it works for either storage layout. Each
single-store export atomically replaces its destination after successful writing. A split
export is atomic per output file, not across the whole directory.

Both layouts require `SuiteStore` and its experiment state/membership capabilities. Arbitrary
custom Store plugins are not supported for Suite execution; they remain supported by ordinary
Runner usage. A root cannot change Suite identity or layout in place. Online layout conversion
is not implemented.

## Results, monitoring and metrics

Opening a Suite directory with `report`, `watch`, `serve` or `export` selects the **cumulative
Suite view** by default, including successful items skipped during recovery. `--experiment ID`
selects one member. `--run-id` selects durable outcomes and source facts for that invocation:
a failure in run A stays failed after run B succeeds. Successful checkpoints skipped during
resume are counted as `skipped`, with no execution start time or attempts. Execution durations
start within that invocation, excluding time between runs. Task outcomes also retain their
run identity; attempts/events are scoped to the invocation that produced them.

Run outcomes update in the same SQLite transaction as checkpoints, including handoff/visit
transitions. A process crash can still lose buffered attempt/event details, but committed
outcomes and attempt counters survive. On resume, the exclusive writer lock also permits
marking previous catalog runs still labeled `running` as `interrupted`, without modifying
unselected member databases. Their end and heartbeat timestamps record when recovery detected
the interruption (the exact crash time is unknown), so historical elapsed time stops growing.

Payloads remain mutable checkpoints: historical exports
omit artifacts/results once another run owns the checkpoint, rather than returning newer data.
Use the cumulative view for the latest available successful results.

The dashboard lists members and links to their detail view. JSON endpoints accept
`?experiment=ID`, combined with `run_id`; `/experiments` returns member/source status and
metrics. Per-database event/attempt IDs remain local; use Suite/member/pipeline provenance
when consuming multi-database audit logs. Shared resource events have no fabricated member
identity. Resource snapshots describe the shared pools rather than attributing their activity
to an individual member.

`--rows results` provides one lightweight row per pipeline: key/local key, repeat, Suite/member
identity, state, final result, artifact availability and error fields. Failed/interrupted
pipelines remain in the export. Existing `pipelines`, `tasks`, `attempts`, `events` and
`artifacts` audit exports also retain member identity when applicable. Export iteration is
bounded; split exports visit members in ID order and use each database's normal row order.
Live exports retain the existing best-effort, non-snapshot semantics.

`Runner.report_metric(..., experiment_id="baseline")` writes a member-scoped latest value.
`TaskContext.report_metric()` retains its pipeline scope. Completion callbacks receive a
`PipelineRecord` with `suite_id`, `experiment_id`, `local_key` and `repeat`, allowing applications
to maintain separate accumulators. Rebuild those accumulators from persisted final results
on recovery: previously successful skips do not invoke completion callbacks. Metrics are
run-scoped; applications must republish rebuilt values for a new run. The framework never
averages member accuracy values or invents a Suite-level business metric.

## Python API and task output

```python
from pyattacker import ExperimentSpec, SuiteSpec, pipeline, task

@task("double")
def double(value, ctx):
    # ctx.suite_id, ctx.experiment_id, ctx.local_key and ctx.output_dir
    # identify the owning experiment without changing the input value.
    return value * 2

template = pipeline("numbers", double)
suite = SuiteSpec(
    id="numbers",
    experiments=[
        ExperimentSpec("first", lambda: template.map(range(3)), definition_digest="numbers-v1"),
        ExperimentSpec("second", lambda: template.map(range(3)), definition_digest="numbers-v1"),
    ],
    output_root="runs/numbers",
    layout="by_experiment",
)
with suite.runner(concurrency=4, handle_signals=False) as runner:
    report = runner.run(suite.pipelines(runner.store), resume=True)
    print(report.summary())
```

`ExperimentSpec.factory` must create a fresh iterable of ordinary PipelineSpec instances on
every invocation. Its `pool_aliases` maps local names to actual names in `SuiteSpec.pools`.
`SuiteSpec.runner()` owns its SuiteStore; use the Runner context manager to close it.
`SuiteSpec.pipelines(store, experiments=[...], limit=N)` selects members and streams their
namespaced pipelines. `SuiteStore(root, read_only=True, experiment=ID)` provides the same
read interface as CLI directory reads; close it after use.

`ctx.output_dir` is the Suite root in combined mode and the member directory in split mode.
Use it explicitly for application-owned files. Existing `write_jsonl(path=...)` and arbitrary
Python file writes are not intercepted or redirected. Applications remain responsible for
file naming, concurrent appends and idempotency across retries. Framework checkpoints and
exported final results do not depend on those application files.
