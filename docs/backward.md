# Advanced: backward traversal and payload history

**English** | [简体中文](zh-CN/backward.md)

Backward traversal is opt-in and experimental until 1.0. It keeps the same pipeline identity,
resource pools and dataset row, but permits declared revisits. Ordinary pipelines and forward-only
control declarations keep their previous IDs, digests and behavior.

## Rewind with an ordinary dictionary

A rewind changes the next station and its input. You choose the state; the framework does not roll back
keys, merge old dictionaries or guess what the earlier task accepts.

```python
from pyattacker import Handoff, Runner, pipeline, task

@task("generate")
def generate(value, ctx):
    return {**value, "answer": "bad" if ctx.visit == 0 else "good"}

@task("validate")
def validate(value, ctx):
    if value["answer"] == "bad":
        return Handoff.rewind("generate", {
            "prompt": value["prompt"],
            "feedback": "Use the requested schema",
        }, reason="invalid structured output")
    return value

template = pipeline("regenerate", generate | validate, control={
    "rewind": {"validate": ["generate"]},
    "retry_all": ["validate"],
    "max_handoffs": 3,
})
with Runner(store=":memory:", max_handoffs=10) as runner:
    report = runner.run(template.map([{"prompt": "Return JSON"}]))
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
```

`Handoff.rewind(target, value, reason="")` requires an explicit value; `None` is a real value.
The target must be a declared strictly earlier task, identified by unique name or numeric seq.
Self-rewind and `end` rewind targets are rejected. Results before the target remain effective;
results from the target onward become historical. The whole suffix executes again, with its own visits.

Existing `Handoff.to()` stays strictly forward and uses `control.edges`; it never acquires implicit
backward semantics. The forward declaration is optional for a backward-only pipeline. No directive
can escape from a `fanout` branch.

## Retry the whole pipeline

`return Handoff.retry_all(reason="new preparation")` requires that source in `control.retry_all`.
It restarts at seq 0 from the **original bound seed**, freshly decoded from bytes captured at binding.
Nested mutation of a task input or `spec.seed` does not alter that bound seed. It accepts no replacement
value: use `Handoff.rewind(0, chosen_state)` from a later task to restart with different state.

Retry-all does not create another mapped row/repeat, reset resources or start another CLI run.
It clears effective task results, while preserving visits, artifacts, attempts and the consumed
control budget. It can be declared on the first task, including a single-task pipeline.

Exception retry is separate: `Retrying` retries an attempt within the same visit; rewind and retry-all
are returned control directives. They do not invoke the failure retry policy. Authoring mistakes and
control-budget exhaustion are fatal failures that cannot be retried by that policy.

## Optional history-bearing application state

`HistoryArtifact` is a decoded application payload, not the persisted `Artifact` record. Dictionaries
remain ordinary dictionaries; there is no automatic snapshot on task entry or completion.

```python
from pyattacker import CodecRegistry, HistoryArtifact

class GenerationState(HistoryArtifact):
    pass

registry = CodecRegistry()
registry.register_type(GenerationState)
state = GenerationState({"prompt": "Return JSON", "temperature": 0.2})
state = state.checkpoint("before-generation")
state = state.with_state({**state.state, "answer": "invalid"}).checkpoint("after-generation")
restored = state.restore("before-generation")
next_state = restored.with_state({**restored.state, "temperature": 0.7})
assert len(next_state.history) == 2  # restoration retains later history
assert registry.load(registry.dump(next_state)).state == next_state.state
```

Pass the registry to both `pipeline(..., registry=registry)` and `Runner(..., registry=registry)`.
Return `Handoff.rewind("generate", next_state)` to schedule the selected state.

- `checkpoint(label, *, metadata=None)` returns a new value with a detached snapshot of application state.
  Labels are unique; `snapshot:` is reserved for stable IDs such as `snapshot:0`.
- `with_state(value)` replaces current state without appending a snapshot.
- `snapshot(id_or_label)` returns a detached snapshot record; `history` returns detached records.
- `restore(id_or_label)` replaces current state, retains the whole history, and exposes `selected`.
- `prune(*selectors)` explicitly removes snapshots and **raises** `ValueError` when a selector names the
  selected snapshot. IDs are not reused.

Nested mutable values never alias retained snapshots through these interfaces. Application state and
metadata must be JSON serializable; encoding rejects clients, leases and other runtime objects.
The versioned `history-v1` codec preserves snapshots and registered subclass type. Subclasses inherit
the base constructor; use `state` for application fields. Custom subclass constructor/extra attribute
serialization is outside this initial interface. Unregistered subclasses fail decoding explicitly.

History is self-contained and can grow with the number and size of snapshots; prune explicitly when
appropriate. Calling `checkpoint()` inside a task does **not** immediately persist it. The runner persists
history with the task's output or control-transition commit. A crash before that commit can lose the
in-memory snapshot. Snapshot history does not replace the framework's execution ledger or visit records.

## Visits, recovery and finite budgets

`ctx.visit` starts at 0 for each station and increases on each fresh entry, including ordinary successor
entries after a rewind. IDs keep `pipeline_id:seq` for visit 0 and use `pipeline_id:seq#visit` thereafter.
Artifacts carry producing visit and retain immutable occurrences with exact IDs. Use
`store.get_artifact_by_id(id)` for an exact historical occurrence;
`store.get_artifact(pipeline_id, seq)` selects the effective output for that station.
`ctx.attempt` numbers attempts within one visit. The RNG/`ctx.seed` derivation is byte-identical for
visit 0 and includes visit on revisits. Include visit and attempt in external idempotency keys when you
want a new external operation for each regeneration/attempt.

A pending entry references its exact input. Resume keeps its visit and continues consumed attempt
numbering. An attempt number is reserved before task code runs; a hard kill may leave a gap in completed
attempt rows, but cannot reuse its number. Framework recovery is at least once for uncommitted work;
it cannot guarantee exactly-once external side effects.

Both built-in stores provide atomic `reset_visits`, `commit_entry`, `commit_visit_attempt`,
`commit_visit_success`, `commit_control_transition` and `repair_visit_terminal`, plus `visit_state`,
`get_artifact_by_id` and `feature_level()`. `supports_visits` probes exactly that set *plus* the v1 ledger
capability (`commit_handoff`, `reset_pipeline`, `handoffs`), because a control transition lands its ledger row
and source task row through it; a store missing any of them is refused with a `ConfigError` when a
backward-enabled pipeline is opened, never downgraded to a non-durable loop. `feature_level()` is part of the
probe on purpose: a store that cannot declare how far its on-disk model has come cannot honour the
[compatibility rule](#store-compatibility) either. Write-behind flushes before delegating these operations
synchronously.
A control transfer commits its source visit/attempt, entry occurrence, ledger, control count and allocated
target entry together, and additionally invalidates the active suffix for `rewind`/`retry_all` (a forward
transfer in such a pipeline consumes budget without moving the cursor backwards).
Ordinary success commits its output, completed visit, effective mapping and next cursor together. SQLite
serializes capability transactions with `BEGIN IMMEDIATE`;
MemoryStore restores its state on failed capability writes. A failed blob write cannot publish its reference;
a rolled-back database transaction can leave an unreferenced blob.

Require a positive integer `control.max_handoffs` when declaring backward operations (`3.0`, `True`, `"3"`
and `0` are all rejected). The runtime ceiling
`RunConfig.max_handoffs` (default 1000, also accepted as `run.max_handoffs` in config) can lower that cap.
The minimum is effective. Every nonterminal handoff in such a pipeline counts, including forward transfers;
`END` can finish at the limit without consuming another transfer. Budget N permits exactly N transfers.
The next fails before publishing a transition or invalidating results. Resume, retry-all and automatic
missing-payload seed fallback retain the count. Only an explicit fresh start (`fresh_restart=True`, or
`retry_succeeded=True` on a pipeline that already succeeded) begins a new budget lifecycle; visit counters
and audit records survive it. That is the way out of a spent budget: a pipeline that failed *because* it
consumed its budget has no progress left to resume, so without it every later run replays the same fatal
error. Plain `resume=True` keeps the consumed budget and continues the current traversal, and
`retry_succeeded=True` on its own never restarts an unfinished traversal — it only admits pipelines that
already succeeded (see *Recovery and ownership* below).

### Recovery and ownership

What an open does depends on the stored row, and the rules are meant to be explicit rather than emergent:

| stored row | what this run does |
| --- | --- |
| `failed`, `interrupted` | ordinary checkpoint recovery: the exact durable visit — visit number, pending entry, consumed attempt numbering — continues |
| `running`, `resume=True` | the operator's claim that the previous owner is gone. `interrupt_stale` reclaims heartbeat-stale rows first; the exact durable visit then continues |
| `running`, no `resume` | **skipped**, never taken over. A durable pending visit can be continued after a crash, so a second writer would fork one traversal. The row is left exactly as it is, and `pipeline.skipped` carries `reason="owned_by_another_run"` plus the owner's run id |
| rows durable, traversal gone | refused: `corrupt visit checkpoint: missing traversal state`. `fresh_restart=True` is the documented way to discard it and start over |
| `succeeded` | skipped, unless `retry_succeeded=True`; a restart then runs from the bound seed with a fresh budget |

`fresh_restart=True` is the one switch that discards durable progress: it clears the effective traversal and
any pending entry (settling every task row it leaves in flight as `interrupted`), starts again from the
immutable bound seed, resets the control budget and invalidates the previous ledger — while visit counters
and visit-qualified audit rows (tasks, attempts, artifacts, visits) are kept, so historical occurrences stay
addressable, and a store whose traversal was lost has its counters rebuilt from those rows. It applies to
forward pipelines too, where it means "ignore the checkpoint, run the chain again": there the append-only
history survives as well, but task and chain-artifact addresses are reused by design rather than kept as
separate occurrences (the [reference](reference.md#tables-and-readers) spells that difference out). Combine it with
`retry_succeeded=True` to restart a pipeline that already succeeded. A fresh start emits
`pipeline.restarted` (with the discarded cursor), not `pipeline.checkpoint_missing`: nothing was lost.

A missing/unavailable pending payload emits `pipeline.checkpoint_missing` and establishes a seed replay,
preserving budget and counters. The first execution of a pipeline never takes that path: its input is the
bound seed it already holds, so a `journal="summary"` store (whose written seed payload is intentionally
dropped) does not report a checkpoint failure it never had. Summary journals and null backends can run loops
in-process, but cannot resume their missing payloads. A pending rewind to seq 0 uses its chosen payload
rather than triggering a seed reset. Backward transfers requeue through the timer pump to release the worker
for other pipelines.

## Inspecting execution

Pipeline exports add a `control` traversal record only for backward-enabled pipelines. It contains
`cursor`, `pending`, effective `active` slots, durable `counters`, `handoffs` consumed, `version`,
exact current `input` and `terminal` reference. Nested task/artifact rows include IDs, visits and
`active` markers. Individual task/attempt/artifact exports include visit; attempts include task-run ID.
The existing closed top-level export row kinds are unchanged. The HTTP `/pipelines` view includes
traversal state and `cursor_kind="position"` for backward-enabled rows (a forward-only row gets neither).
A report is scoped to the run it covers and counts that run's repeated visits and attempts, so after a
resume it shows the new run's workload while the store and the export retain every earlier row. Those
totals are workload, not completion percentages.

### Store compatibility

SQLite upgrades older stores with visit columns defaulting to 0, a traversal-state table and a
`store_meta` feature level. This is an additive upgrade for forward-only work: a store stays at level `base`
until the first revisit is written.

The level becomes `visits-v1` inside the *same transaction* that allocates the first second occurrence for a
station, and it never goes back down — audit rows are not deleted, so neither is the fact that they exist.
That is also the moment a lineage-unaware writer stops being able to interpret the store, so a SQLite store
arms a **writer guard**: `INSERT`/`UPDATE`/`DELETE` on `pipelines`, `tasks` and `artifacts` from a connection
that has not declared visit-lineage awareness fail loudly (`no such function:
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
* Writing to a guarded store from `sqlite3` needs the guard function registered on that connection (or the
  triggers dropped) — both are outside the supported interface;
* the only supported way back to `base` is a migration performed by a build that understands the level.

Simultaneous runners executing the same logical pipeline are not a supported scheduling mode; independent
shard rows remain independent (a `running` row is still never taken over without `resume`, see above).
See [#51](https://github.com/Hazer-BJTU/pyattacker/issues/51) for the design and review checklist.
