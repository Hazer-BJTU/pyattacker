"""Advanced backward traversal: user state, exact visits, atomic recovery and bounded loops."""

from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest

from pyattacker import (
    CodecRegistry,
    FatalError,
    Handoff,
    HistoryArtifact,
    PipelineBuildError,
    Pool,
    Resource,
    Runner,
    pipeline,
    task,
)
from pyattacker.artifact import Encoded
from pyattacker.errors import ArtifactCodecError
from pyattacker.export import iter_rows
from pyattacker.store import MemoryStore, SqliteStore, WriteBehindStore
from pyattacker.store.visits import supports_visits


@pytest.fixture(params=["memory", "sqlite", "writebehind"])
def store(request, tmp_path):
    inner = MemoryStore() if request.param == "memory" else SqliteStore(str(tmp_path / "visits.db"))
    result = WriteBehindStore(inner, batch_size=1000) if request.param == "writebehind" else inner
    yield result
    result.close()


def run(store, spec, **options):
    return Runner(store=store, handle_signals=False, **options).run([spec])


def test_rewind_preserves_prefix_and_revisits_every_successor(store):
    seen = []

    @task("prepare")
    def prepare(value, ctx):
        seen.append((ctx.seq, ctx.visit, value))
        return {"prompt": value}

    @task("generate")
    def generate(value, ctx):
        seen.append((ctx.seq, ctx.visit, value.copy()))
        return {**value, "output": ctx.visit}

    @task("validate")
    def validate(value, ctx):
        seen.append((ctx.seq, ctx.visit, value.copy()))
        if ctx.visit == 0:
            return Handoff.rewind("generate", {"prompt": "selected", "feedback": "bad JSON"})
        return value

    @task("report")
    def report(value, ctx):
        seen.append((ctx.seq, ctx.visit, value.copy()))
        return value

    template = pipeline(
        "qa",
        prepare | generate | validate | report,
        control={"rewind": {"validate": ["generate"]}, "max_handoffs": 2},
    )
    spec = template.bind("original")
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert [(s, v) for s, v, _ in seen] == [(0, 0), (1, 0), (2, 0), (1, 1), (2, 1), (3, 0)]
    assert seen[3][2] == {"prompt": "selected", "feedback": "bad JSON"}
    assert store.get_artifact(spec.pipeline_id, 1).visit == 1
    assert store.get_artifact_by_id(f"{spec.pipeline_id}:1").visit == 0
    state = store.visit_state(spec.pipeline_id)
    assert state["active"]["0"]["visit"] == 0
    assert state["active"]["1"]["visit"] == 1
    assert len(store.artifacts(spec.pipeline_id)) == 7
    assert len([a for a in store.artifacts(spec.pipeline_id) if a.is_final]) == 1
    assert [(a.seq, a.visit, a.attempt_no) for a in store.attempts()] == [
        (0, 0, 1),
        (1, 0, 1),
        (2, 0, 1),
        (1, 1, 1),
        (2, 1, 1),
        (3, 0, 1),
    ]
    rows = list(iter_rows(store))
    assert rows[0]["control"]["handoffs"] == 1
    assert sum(t["active"] for t in rows[0]["tasks"]) == 4
    assert list(iter_rows(store, kind="attempts"))[-1]["visit"] == 0


def test_retry_all_uses_bound_seed_after_nested_mutation(store):
    seen = []

    @task("first")
    def first(value, ctx):
        seen.append((ctx.visit, ctx.seed, value["nested"]["x"]))
        value["nested"]["x"] = 99
        return value

    @task("gate")
    def gate(value, ctx):
        if ctx.visit == 0:
            return Handoff.retry_all(reason="new sample")
        return value

    spec = pipeline("retry", first | gate, control={"retry_all": ["gate"], "max_handoffs": 1}).bind(
        {"nested": {"x": 1}}
    )
    spec.seed["nested"]["x"] = 55  # immutable bound bytes, not this mutable object, define retry-all
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert [v for _, _, v in seen] == [1, 1]
    assert seen[0][1] != seen[1][1]
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1
    assert len(store.tasks()) == 4


def test_end_at_control_limit_and_exact_reused_occurrence(store):
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", value) if ctx.visit == 0 else Handoff.end(reason="done")

    @task("c")
    def c(value, ctx):
        pytest.fail("END should skip this task")

    spec = pipeline(
        "end", a | b | c, control={"edges": {"b": ["end"]}, "rewind": {"b": ["a"]}, "max_handoffs": 1}
    ).bind({"x": 1})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    ledger = store.handoffs(pipeline_id=spec.pipeline_id)
    assert ledger[-1].entry_artifact_id == f"{spec.pipeline_id}:0#1"
    assert [a.id for a in store.artifacts(spec.pipeline_id) if a.is_final] == [ledger[-1].entry_artifact_id]


def test_budget_is_durable_and_cannot_be_reset_by_resume(store):
    @task("loop")
    def loop(value, ctx):
        return Handoff.retry_all()

    spec = pipeline("loop", loop, control={"retry_all": ["loop"], "max_handoffs": 5}).bind({})
    assert run(store, spec, max_handoffs=2).stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 2
    assert len(store.handoffs()) == 2
    assert "consumed 2, allowed 2" in store.get_pipeline(spec.pipeline_id).error_message
    assert run(store, spec, max_handoffs=1, resume=True).stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 2
    assert [(t.visit, t.attempts_used) for t in store.tasks()] == [(0, 1), (1, 1), (2, 2)]
    assert store.attempts()[-1].outcome == "failed"


def test_explicit_fresh_restart_resets_an_exhausted_budget(store):
    """A failed loop must not be unresumable: `fresh_restart=True` starts a new budget lifecycle.

    Without the reset, a pipeline that failed *because* it spent its budget replays the same fatal
    error on every later open -- there is no durable checkpoint left that can make progress -- and
    the operator has no documented way back short of abandoning the store. The escape hatch is the
    explicit `fresh_restart`, and *only* that: `retry_succeeded` is an eligibility switch for
    succeeded pipelines, not a licence to replay an unfinished traversal from its seed.
    """
    visits = []

    @task("a")
    def a(value, ctx):
        visits.append(("a", ctx.visit))
        return value

    @task("b")
    def b(value, ctx):
        visits.append(("b", ctx.visit))
        # Spend the whole budget on the first pass, then overrun it: the first execution fails, and
        # only a fresh execution (a new budget plus a new visit) can reach the ordinary return.
        if ctx.visit in (0, 1):
            return Handoff.rewind("a", {"x": ctx.visit})
        return value

    spec = pipeline("escape", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"x": -1})
    first = run(store, spec)
    assert first.stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1
    assert "control budget exhausted" in store.get_pipeline(spec.pipeline_id).error_message

    # Plain resume deliberately keeps the consumed budget, so it still cannot progress.
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1

    # `retry_succeeded` must not become a second, implicit meaning: the durable traversal (and its
    # spent budget) is exactly what it must keep. The pipeline still resumes at its durable cursor
    # -- seq 1, where the exhausted loop stopped -- and fails there on the same spent budget.
    visits.clear()
    assert run(store, spec, retry_succeeded=True).stats["pipelines"]["by_state"] == {"failed": 1}
    assert visits == [("b", 1)]
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}

    visits.clear()
    fresh = run(store, spec, fresh_restart=True)
    assert fresh.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert visits == [("a", 2), ("b", 2)]
    # Counters and audit records survive the reset; only the budget starts a new lifecycle.
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 2, "1": 2}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 0
    assert len(store.handoffs(pipeline_id=spec.pipeline_id)) == 1
    assert any(
        e.kind == "pipeline.restarted" and e.data == {
            "reason": "fresh_restart", "via": "visits", "discarded_cursor": 1,
            "budget_reset": True, "counters_preserved": True,
            "visit_occurrences_preserved": True, "append_only_history_preserved": True,
        }
        for e in store.events(run_id=fresh.run_id)
    )


def test_retry_succeeded_does_not_replay_an_unfinished_backward_pipeline(store):
    """`resume=True + retry_succeeded=True` must not turn a recoverable visit into a seed replay.

    This is the operator's everyday combination ("finish what is unfinished, redo what succeeded").
    A *failed* backward pipeline still owns a durable pending entry, so the only correct behaviour
    is to continue that exact visit: replaying the seed would repeat external side effects the
    checkpoint was about to continue, and would do it silently.
    """
    seen = []
    failures = {"on": True}

    @task("a")
    def a(value, ctx):
        seen.append(("a", ctx.visit))
        if failures["on"] and ctx.visit == 1:
            raise RuntimeError("flaky endpoint")
        return value

    @task("b")
    def b(value, ctx):
        seen.append(("b", ctx.visit))
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("unfinished", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 2}).bind({"seed": True})
    first = run(store, spec)
    assert first.stats["pipelines"]["by_state"] == {"failed": 1}
    # The rewind is durable: the pending entry of visit 1 is exactly what recovery must continue.
    state = store.visit_state(spec.pipeline_id)
    assert state["counters"] == {"0": 1, "1": 0}
    assert state["pending"]["visit"] == 1 and state["cursor"] == 0

    failures["on"] = False
    seen.clear()
    second = run(store, spec, resume=True, retry_succeeded=True)
    assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert seen == [("a", 1), ("b", 1)]  # no replay of visit 0, no seed reset
    assert [e.kind for e in store.events(run_id=second.run_id) if e.kind in
            ("pipeline.checkpoint_missing", "pipeline.restarted")] == []
    # The consumed budget and the visit counters are preserved, not reset.
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}


def test_running_backward_pipeline_is_skipped_without_resume(store):
    """A `running` row is someone else's traversal: skipped, not taken over, unless `resume` claims it.

    The durable shape here is the one a hard kill leaves behind -- a `running` pipeline row plus a
    committed pending visit -- so both halves of the contract are exercised: refusing to take over,
    and continuing the exact visit once the operator says the previous owner is gone.
    """
    seen = []
    failures = {"on": True}

    @task("a")
    def a(value, ctx):
        seen.append(("a", ctx.visit))
        if failures["on"] and ctx.visit == 1:
            raise RuntimeError("flaky endpoint")
        return value

    @task("b")
    def b(value, ctx):
        seen.append(("b", ctx.visit))
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("owned", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 2}).bind({"seed": True})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    # Simulate the killed owner: the row still says running, owned by a run that is not ours.
    row = store.get_pipeline(spec.pipeline_id)
    store.upsert_pipeline(dataclasses.replace(row, state="running", run_id="run-killed", finished_at=None))

    before = store.visit_state(spec.pipeline_id)
    seen.clear()
    skipped = run(store, spec, retry_succeeded=True, fresh_restart=True)
    assert skipped.stats["pipelines"]["by_state"] == {}  # nothing ran, nothing failed
    assert skipped.skipped == 1
    assert seen == []
    assert store.visit_state(spec.pipeline_id) == before
    assert store.get_pipeline(spec.pipeline_id).state == "running"
    assert store.get_pipeline(spec.pipeline_id).run_id == "run-killed"
    assert any(
        e.kind == "pipeline.skipped" and e.data["reason"] == "owned_by_another_run"
        and e.data["owner_run_id"] == "run-killed"
        for e in store.events(run_id=skipped.run_id)
    )

    failures["on"] = False
    resumed = run(store, spec, resume=True)
    assert resumed.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert seen == [("a", 1), ("b", 1)]  # the exact durable visit, not a replay
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}
    assert store.get_pipeline(spec.pipeline_id).resume_of == "run-killed"


def test_fresh_restart_settles_a_pending_task_it_abandons(store):
    """The occurrence a discarded traversal was waiting on must not stay `running` forever.

    A hard kill leaves exactly this shape: a committed pending visit whose task row is still in
    flight. Resume reuses that occurrence, but a fresh restart throws it away — so the row has to be
    settled as abandoned. It stays in history; nothing points at it any more.
    """
    failures = {"on": True}

    @task("a")
    def a(value, ctx):
        if failures["on"] and ctx.visit == 1:
            raise RuntimeError("killed mid-visit")
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("abandon", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    pending = store.visit_state(spec.pipeline_id)["pending"]
    assert (pending["seq"], pending["visit"]) == (0, 1)

    flush = getattr(store, "flush", None)
    if callable(flush):  # a write-behind wrapper must not keep the flip in its buffer
        flush()
    inner = getattr(store, "inner", store)
    # Simulate the kill: the pending occurrence's task row was left in flight.
    if isinstance(inner, MemoryStore):
        row = inner._tasks[pending["task_run_id"]]
        inner._tasks[pending["task_run_id"]] = dataclasses.replace(row, state="running")
    else:
        inner._conn.execute(
            "UPDATE tasks SET state='running' WHERE task_run_id=?", (pending["task_run_id"],)
        )
        inner._conn.commit()
    running = next(t for t in store.tasks(pipeline_id=spec.pipeline_id)
                   if t.task_run_id == pending["task_run_id"])
    assert running.state == "running"

    failures["on"] = False
    assert run(store, spec, resume=True, fresh_restart=True).stats["pipelines"]["by_state"] == {
        "succeeded": 1
    }
    abandoned = next(t for t in store.tasks(pipeline_id=spec.pipeline_id)
                     if t.task_run_id == pending["task_run_id"])
    assert abandoned.state == "interrupted"  # settled as abandoned, still in history
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 2, "1": 1}


def test_fresh_restart_settles_a_running_task_when_the_traversal_is_gone(store):
    """The abandoned-row rule cannot depend on the traversal naming the occurrence.

    If the traversal record itself is lost, `fresh_restart` has no pending entry to point at — but a
    task row still marked `running` is abandoned either way, and leaving it that way would make the
    store describe work that nobody owns and nothing can settle.
    """
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("orphan", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    # Damage the store the way the escape hatch exists to recover from, then leave an in-flight row
    # behind (the shape a kill during a revisit produces).
    store.finish_pipeline(spec.pipeline_id, "interrupted", n_tasks_done=0)
    flush = getattr(store, "flush", None)
    if callable(flush):
        flush()
    inner = getattr(store, "inner", store)
    orphan = next(t for t in store.tasks(pipeline_id=spec.pipeline_id) if t.visit == 1)
    if isinstance(inner, MemoryStore):
        inner._visits.pop(spec.pipeline_id)
        inner._tasks[orphan.task_run_id] = dataclasses.replace(orphan, state="running")
    else:
        inner._conn.execute("DELETE FROM visit_state WHERE pipeline_id=?", (spec.pipeline_id,))
        inner._conn.execute(
            "UPDATE tasks SET state='running' WHERE task_run_id=?", (orphan.task_run_id,)
        )
        inner._conn.commit()

    assert run(store, spec, resume=True, fresh_restart=True).stats["pipelines"]["by_state"] == {
        "succeeded": 1
    }
    settled = next(t for t in store.tasks(pipeline_id=spec.pipeline_id)
                   if t.task_run_id == orphan.task_run_id)
    assert settled.state == "interrupted"
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 2, "1": 2}


def test_fresh_restart_recovers_a_traversal_the_framework_cannot_open(store):
    """The escape hatch also covers a corrupted checkpoint, which otherwise has no way out.

    With durable rows but no traversal, every later open -- `resume=True` included -- refuses with
    "corrupt visit checkpoint: missing traversal state". `fresh_restart=True` is the documented,
    explicit way to discard that state and start over instead of abandoning the pipeline id.
    """
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("lost", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    # A terminal row is not the corruption case: only an unfinished one reaches the missing-traversal
    # check, exactly like a store damaged between the pipeline write and the traversal write.
    store.finish_pipeline(spec.pipeline_id, "interrupted", n_tasks_done=0)
    inner = getattr(store, "inner", store)
    if isinstance(inner, MemoryStore):
        inner._visits.pop(spec.pipeline_id)
    else:
        inner._conn.execute("DELETE FROM visit_state WHERE pipeline_id=?", (spec.pipeline_id,))
        inner._conn.commit()

    broken = run(store, spec, resume=True, retry_succeeded=True)
    assert broken.stats["pipelines"]["by_state"] == {"failed": 1}
    assert "missing traversal state" in store.get_pipeline(spec.pipeline_id).error_message

    restarted = run(store, spec, resume=True, fresh_restart=True)
    assert restarted.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 2, "1": 2}


def test_fresh_restart_applies_to_forward_pipelines(store):
    """`fresh_restart` is not a backward-only flag: a forward checkpoint is discarded the same way.

    The point of the option is "start this pipeline over", and that has to mean something for every
    pipeline a run admits; a flag that silently did nothing for a forward chain would be worse than
    no flag at all.
    """
    seen = []
    failures = {"on": True}

    @task("first")
    def first(value, ctx):
        seen.append("first")
        return value

    @task("second")
    def second(value, ctx):
        seen.append("second")
        if failures["on"]:
            raise RuntimeError("endpoint down")
        return value

    spec = pipeline("forward", first | second).bind(1)
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    assert seen == ["first", "second"]
    assert store.get_pipeline(spec.pipeline_id).n_tasks_done == 1

    # Without the flag, the good checkpoint is resumed: only the second task runs again.
    seen.clear()
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"failed": 1}
    assert seen == ["second"]

    seen.clear()
    restarted = run(store, spec, resume=True, fresh_restart=True)
    assert restarted.stats["pipelines"]["by_state"] == {"failed": 1}
    assert seen == ["first", "second"]  # the checkpoint was discarded, not resumed
    assert any(
        e.kind == "pipeline.restarted" and e.data["reason"] == "fresh_restart"
        and e.data["via"] == "forward" and e.data["discarded_cursor"] == 1
        for e in store.events(run_id=restarted.run_id)
    )
    assert not any(
        e.kind == "pipeline.checkpoint_missing" for e in store.events(run_id=restarted.run_id)
    )

    failures["on"] = False
    assert run(store, spec, resume=True, fresh_restart=True).stats["pipelines"]["by_state"] == {
        "succeeded": 1
    }
    # Attempts are append-only in both traversal modes, so a restart never erases the audit trail of
    # the execution it replaced: two attempts for the first run, then one, two and two more.
    assert len(store.attempts(pipeline_id=spec.pipeline_id)) == 7


@pytest.mark.parametrize("sources", [["b", 1], ["b", "1"], [1, "b"]])
def test_retry_all_rejects_duplicate_source_aliases(sources):
    """Aliases that resolve to one station are a declaration error, exactly as they are for `rewind`."""

    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return value

    with pytest.raises(PipelineBuildError, match="duplicate source"):
        pipeline("dup", a | b, control={"retry_all": sources, "max_handoffs": 1})
    # The valid spellings keep working, including a bare numeric token.
    assert pipeline("ok", a | b, control={"retry_all": ["b", 0], "max_handoffs": 1})
    assert pipeline("ok", a | b, control={"retry_all": [1], "max_handoffs": 1})


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize("journal", ["full", "summary"])
def test_fresh_run_never_reports_a_missing_checkpoint(kind, journal, tmp_path):
    """A brand-new run has nothing to recover, so it must not borrow the recovery event.

    With `journal="summary"` the store drops the payload of the seed it has just written. Reading
    that occurrence back made every *first* execution of a backward pipeline emit
    `pipeline.checkpoint_missing` and reset the traversal it had only just created -- a checkpoint
    failure that never happened. The genuine missing-payload fallback (a resumed run) keeps its
    event, which `test_missing_entry_fallback_preserves_budget_and_counters` covers.
    """
    store = MemoryStore(journal=journal) if kind == "memory" else SqliteStore(
        str(tmp_path / "fresh.db"), journal=journal
    )
    seen = []

    @task("a")
    def a(value, ctx):
        seen.append(("a", ctx.visit))
        return value

    @task("b")
    def b(value, ctx):
        seen.append(("b", ctx.visit))
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("fresh", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    try:
        report = run(store, spec)
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert seen == [("a", 0), ("b", 0), ("a", 1), ("b", 1)]
        assert [e.kind for e in store.events(run_id=report.run_id)
                if e.kind == "pipeline.checkpoint_missing"] == []
    finally:
        store.close()


def test_revisit_store_marks_its_level_and_refuses_an_unaware_writer(tmp_path):
    """Downgrade protection: once a revisit exists, a lineage-unaware writer cannot write.

    The marker is durable and part of the schema, so it also stops a writer that was released before
    the marker existed: a connection that never declared visit-lineage awareness cannot prepare its
    own `INSERT`/`UPDATE`/`DELETE` against a seq-keyed table. Reads stay available.
    """
    import sqlite3

    path = str(tmp_path / "downgrade.db")
    store = SqliteStore(path)
    assert store.feature_level() == "base"

    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("guarded", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    try:
        assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert store.feature_level() == "visits-v1"
        triggers = {
            row[0] for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        assert triggers == {
            f"pyattacker_writer_guard_{table}_{event}"
            for table in ("pipelines", "tasks", "artifacts")
            for event in ("insert", "update", "delete")
        }
    finally:
        store.close()

    # A pyattacker that understands visit lineage reopens the store and keeps writing normally.
    reopened = SqliteStore(path)
    try:
        assert reopened.feature_level() == "visits-v1"
        assert reopened.upsert_pipeline(reopened.get_pipeline(spec.pipeline_id)) is None
    finally:
        reopened.close()

    # An older writer -- any connection that has not declared itself -- is refused, loudly.
    legacy = sqlite3.connect(path)
    try:
        legacy.execute("SELECT 1 FROM pipelines").fetchone()  # reads are untouched
        with pytest.raises(sqlite3.OperationalError, match="pyattacker_store_requires_visits_aware_writer"):
            legacy.execute("UPDATE pipelines SET state='running' WHERE pipeline_id=?", (spec.pipeline_id,))
        with pytest.raises(sqlite3.OperationalError, match="pyattacker_store_requires_visits_aware_writer"):
            legacy.execute("DELETE FROM artifacts WHERE pipeline_id=?", (spec.pipeline_id,))
        with pytest.raises(sqlite3.OperationalError, match="pyattacker_store_requires_visits_aware_writer"):
            legacy.execute(
                "INSERT OR REPLACE INTO tasks (task_run_id, pipeline_id, run_id, name, seq, state) "
                "VALUES ('x','y','z','t',0,'running')"
            )
    finally:
        legacy.close()


def test_store_level_is_irreversible_and_a_newer_level_refuses_to_open(tmp_path):
    """The marker never goes back down, and an unknown (newer) level is refused on open."""
    import sqlite3

    from pyattacker.errors import StoreFeatureUnsupported

    path = str(tmp_path / "level.db")
    store = SqliteStore(path)

    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("level", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    try:
        run(store, spec)
        assert store.feature_level() == "visits-v1"
        # An explicit restart discards traversal state but not the level: audit rows are still there,
        # so the store must keep refusing writers that cannot tell the occurrences apart.
        run(store, spec, fresh_restart=True)
        assert store.feature_level() == "visits-v1"
    finally:
        store.close()

    with sqlite3.connect(path) as raw:
        raw.execute("UPDATE store_meta SET value='visits-v2' WHERE key='feature_level'")
    for read_only in (False, True):
        with pytest.raises(StoreFeatureUnsupported, match="visits-v2"):
            SqliteStore(path, read_only=read_only)
    # ...and the same guard is reachable through the public opener.
    from pyattacker.store import open_store

    with pytest.raises(StoreFeatureUnsupported):
        open_store(path)


def test_unknown_level_is_refused_before_anything_is_migrated(tmp_path):
    """A store this build cannot interpret is not touched at all — not even by the additive migration.

    The refusal has to happen before `executescript(SCHEMA)`/`_migrate()`: otherwise opening a
    future store with an older build would "repair" a schema whose semantics it cannot see and leave
    a half-upgraded database behind the error. The fixture is deliberately an *old-style* schema, so
    every migration step the current build would normally add is observable afterwards.
    """
    import sqlite3

    from pyattacker.errors import StoreFeatureUnsupported

    path = str(tmp_path / "future.db")
    raw = sqlite3.connect(path)
    try:
        raw.executescript(
            """
            CREATE TABLE pipelines (
                pipeline_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, name TEXT NOT NULL, key TEXT NOT NULL,
                state TEXT NOT NULL, tags_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL,
                started_at REAL, finished_at REAL, n_tasks_total INTEGER NOT NULL DEFAULT 0,
                n_tasks_done INTEGER NOT NULL DEFAULT 0, attempts_total INTEGER NOT NULL DEFAULT 0,
                failed_task TEXT, error_type TEXT, error_message TEXT, traceback TEXT,
                seed_digest TEXT NOT NULL DEFAULT '', spec_digest TEXT NOT NULL DEFAULT '', resume_of TEXT
            );
            CREATE TABLE artifacts (
                artifact_id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, task_name TEXT NOT NULL,
                seq INTEGER NOT NULL, type_name TEXT NOT NULL, codec TEXT NOT NULL, digest TEXT NOT NULL,
                size INTEGER NOT NULL, payload BLOB, created_at REAL NOT NULL,
                is_final INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO store_meta VALUES ('feature_level', 'visits-v2');
            INSERT INTO pipelines (pipeline_id, run_id, name, key, state, created_at)
                VALUES ('p', 'r', 'n', 'k', 'running', 1.0);
            """
        )
        raw.commit()
    finally:
        raw.close()

    for read_only in (False, True):
        with pytest.raises(StoreFeatureUnsupported, match="visits-v2"):
            SqliteStore(path, read_only=read_only)

    check = sqlite3.connect(path)
    try:
        tables = {row[0] for row in check.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "visit_state" not in tables  # the traversal table was not created
        assert "visit" not in {row[1] for row in check.execute("PRAGMA table_info(artifacts)")}
        assert "handoff_floor" not in {row[1] for row in check.execute("PRAGMA table_info(pipelines)")}
        assert check.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0] == 0
        assert check.execute(
            "SELECT value FROM store_meta WHERE key='feature_level'"
        ).fetchone()[0] == "visits-v2"
        assert check.execute("SELECT count(*) FROM pipelines").fetchone()[0] == 1
    finally:
        check.close()


def test_forward_only_store_stays_writable_by_an_unaware_writer(tmp_path):
    """The version upgrade is additive until the first revisit: no marker, no guard, no refusal.

    This is the other half of the downgrade contract — a store that never allocated a revisit is
    still an ordinary v1 store, so an older binary keeps working on it and the migration costs
    nothing for forward-only work.
    """
    import sqlite3

    path = str(tmp_path / "plain.db")
    store = SqliteStore(path)

    @task("only")
    def only(value, ctx):
        return value

    spec = pipeline("plain", only).bind(1)
    try:
        assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert store.feature_level() == "base"
        triggers = store._conn.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0]
        assert triggers == 0
    finally:
        store.close()

    legacy = sqlite3.connect(path)
    try:
        legacy.execute("UPDATE pipelines SET state='running' WHERE pipeline_id=?", (spec.pipeline_id,))
        legacy.execute(
            "INSERT OR REPLACE INTO artifacts (artifact_id,pipeline_id,task_name,seq,type_name,codec,"
            "digest,size,payload,created_at,is_final,blob_ref) "
            "VALUES ('x','y','t',0,'dict','json','d',2,'{}',1.0,0,NULL)"
        )
        legacy.commit()
    finally:
        legacy.close()


def test_resumed_visit_run_stats_match_across_stores(store):
    """`attempts_total` counts attempt rows for the run, in both backends.

    A resumed pending entry is rebound to the new run while keeping the attempts it already consumed
    (`store/visits.py`'s `commit_entry`), so summing `TaskRecord.attempts_used` over the run's task rows
    double-counts the earlier run's attempt. MemoryStore used to do exactly that while SqliteStore
    counted `attempts` rows, which made the same history report two different totals.
    """
    fail = {"on": True}

    @task("t")
    def t(value, ctx):
        if fail["on"]:
            raise RuntimeError("dead endpoint")
        return value

    spec = pipeline("stats", t, control={"retry_all": ["t"], "max_handoffs": 3}).bind({})
    first = run(store, spec)
    assert first.stats["pipelines"]["by_state"] == {"failed": 1}
    fail["on"] = False
    second = run(store, spec, resume=True)
    assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}

    stats = store.stats(second.run_id)
    attempt_rows = store.attempts(run_id=second.run_id)
    assert stats["attempts_total"] == len(attempt_rows)
    # The visit kept its consumed attempt numbering even though the row is owned by the new run.
    assert [t.attempts_used for t in store.tasks(run_id=second.run_id)] == [2]


@pytest.mark.parametrize(
    "bad_open",
    [
        {"max_handoffs": 0},          # invalid runtime ceiling, checked before the row exists
        {},                           # placeholder replaced below by the missing-pool variant
    ],
    ids=["bad-ceiling", "missing-pool"],
)
def test_failed_backward_open_does_not_poison_the_pipeline(store, bad_open):
    """A backward pipeline that failed *before its first transaction* must stay runnable.

    `_open_backward_pipeline` used to require a traversal record for any existing row, so an open that
    failed at a configuration check wrote `state="failed"` with no traversal and every later run --
    `resume=True` and `retry_succeeded=True` included -- died with "missing traversal state". There was
    no durable progress to protect, so the row has to be treated as fresh instead of corrupt.
    """
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"x": 1}) if ctx.visit == 0 else value

    spec = pipeline("poison", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"x": 0})
    if bad_open == {}:
        # A pool the pipeline references but the runner does not provide: the same pre-transaction
        # failure shape as an invalid ceiling.
        @task("b", resource="api")
        def b(value, ctx):  # shadows the declaration above on purpose: this is the resource-bound one
            return Handoff.rewind("a", {"x": 1}) if ctx.visit == 0 else value

        spec = pipeline("poison", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"x": 0})
        assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
        # Still a real failure while the pool is missing -- and still not reported as corruption.
        assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
        assert "unknown resource pool" in store.get_pipeline(spec.pipeline_id).error_message
        # Registering the pool is enough to recover: no re-keying, no new store.
        assert run(store, spec, pools=[Pool("api", [Resource("api-1", capacity=1)])]).stats[
            "pipelines"
        ]["by_state"] == {"succeeded": 1}
        return

    assert run(store, spec, max_handoffs=0).stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id) is None
    assert store.get_pipeline(spec.pipeline_id).n_tasks_done == 0

    # Nothing durable was ever written, so the next open starts a real traversal instead of
    # reporting corruption, and the loop runs to completion.
    assert run(store, spec, max_handoffs=1).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1


def test_memory_store_finality_and_reset_cover_visit_occurrences(store):
    """`mark_final`/`reset_pipeline` must treat `_occurrences` exactly like the artifact slots.

    SqliteStore keeps one table, so both operations see visit occurrences automatically; MemoryStore
    keeps `_artifacts` and `_occurrences` apart, and a mismatch there would make `artifacts()` answer
    differently per backend (two final artifacts, or removed occurrences that survive a reset).
    """
    @task("a")
    def a(value, ctx):
        return {"v": ctx.visit}

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"v": 9}) if ctx.visit == 0 else value

    spec = pipeline("divergence", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({})
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert len([art for art in store.artifacts(spec.pipeline_id) if art.is_final]) == 1

    store.mark_final(spec.pipeline_id, -1)  # the seed slot: exactly one final, never two
    assert [art.id for art in store.artifacts(spec.pipeline_id) if art.is_final] == [
        f"{spec.pipeline_id}:-1"
    ]

    record = store.get_pipeline(spec.pipeline_id)
    store.reset_pipeline(record)
    remaining = {(art.seq, art.is_final) for art in store.artifacts(spec.pipeline_id)}
    assert remaining == {(-1, False), (record.n_tasks_total, False)}


@pytest.mark.parametrize("operation", ["rewind", "retry_all"])
def test_resume_committed_transfer_to_zero_reuses_target_visit(store, monkeypatch, operation):
    seen = []

    @task("a")
    def a(value, ctx):
        seen.append(("a", ctx.visit, value.copy()))
        return value

    @task("b")
    def b(value, ctx):
        seen.append(("b", ctx.visit, value.copy()))
        if ctx.visit == 0:
            return Handoff.rewind("a", {"selected": True}) if operation == "rewind" else Handoff.retry_all()
        return value

    spec = pipeline(
        "crash", a | b, control={"rewind": {"b": ["a"]}, "retry_all": ["b"], "max_handoffs": 2}
    ).bind({"original": True})
    inner = getattr(store, "inner", store)
    original = inner.commit_control_transition

    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash after committed transfer")

    monkeypatch.setattr(inner, "commit_control_transition", crash)
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    before = store.visit_state(spec.pipeline_id)
    assert before["pending"]["seq"] == 0
    assert before["pending"]["visit"] == 1
    monkeypatch.setattr(inner, "commit_control_transition", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert [x[:2] for x in seen] == [("a", 0), ("b", 0), ("a", 1), ("b", 1)]
    assert seen[2][2] == ({"selected": True} if operation == "rewind" else {"original": True})
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}
    assert len(store.handoffs()) == 1


def test_resume_after_ordinary_success_does_not_allocate_or_execute_it_again(store, monkeypatch):
    calls = []

    @task("a")
    def a(value, ctx):
        calls.append(("a", ctx.visit))
        return value

    @task("b")
    def b(value, ctx):
        calls.append(("b", ctx.visit))
        return value

    spec = pipeline("success-crash", a | b, control={"retry_all": ["b"], "max_handoffs": 1}).bind(1)
    inner = getattr(store, "inner", store)
    original = inner.commit_visit_success

    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash after ordinary success")

    monkeypatch.setattr(inner, "commit_visit_success", crash)
    run(store, spec)
    monkeypatch.setattr(inner, "commit_visit_success", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert calls == [("a", 0), ("b", 0)]
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 0, "1": 0}


def test_control_transaction_rolls_back_all_facts_on_failure(store, monkeypatch):
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("rollback", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({})
    inner = getattr(store, "inner", store)
    original = inner._visit_save

    def fail(pid, state):
        if state["handoffs"]:
            raise RuntimeError("injected transaction failure")
        return original(pid, state)

    monkeypatch.setattr(inner, "_visit_save", fail)
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    state = store.visit_state(spec.pipeline_id)
    assert state["handoffs"] == 0
    assert state["active"]["0"]["visit"] == 0
    assert state["pending"]["seq"] == 1
    assert store.handoffs() == []
    assert len(store.artifacts(spec.pipeline_id)) == 2
    assert all(a.outcome != "handed_off" for a in store.attempts())
    # The compatibility marker is written in that same transaction, so it has to roll back with it:
    # a level (or a guard) that outlived the rollback would let the next revisit commit an occurrence
    # into a store that still advertises `base`.
    assert store.feature_level() == "base"
    if not isinstance(inner, MemoryStore):
        assert inner._conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger'"
        ).fetchone()[0] == 0
    monkeypatch.setattr(inner, "_visit_save", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}
    # Only the successful revisit may enter the level, and it must also arm the guard.
    assert store.feature_level() == "visits-v1"
    if not isinstance(inner, MemoryStore):
        triggers = {
            row[0] for row in inner._conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
        }
        assert triggers == {
            f"pyattacker_writer_guard_{table}_{event}"
            for table in ("pipelines", "tasks", "artifacts")
            for event in ("insert", "update", "delete")
        }


def test_terminal_repair_and_explicit_fresh_execution_keep_history(store):
    @task("a")
    def a(value, ctx):
        return value

    spec = pipeline("fresh", a, control={"retry_all": ["a"], "max_handoffs": 1}).bind(1)
    run(store, spec)
    store.finish_pipeline(spec.pipeline_id, "interrupted", n_tasks_done=1)
    repaired = run(store, spec, resume=True)
    assert repaired.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert len(store.tasks()) == 1
    assert run(store, spec, retry_succeeded=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert len(store.tasks()) == 2
    assert [a.visit for a in store.artifacts(spec.pipeline_id) if a.is_final] == [1]


@pytest.mark.parametrize("payload", [None, {"data": [1, 2]}])
def test_explicit_rewind_payload_is_not_inferred(store, payload):
    received = []

    @task("a")
    def a(value, ctx):
        received.append(value)
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", payload) if ctx.visit == 0 else value

    spec = pipeline("none", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind("seed")
    assert run(store, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert received == ["seed", payload]


@pytest.mark.parametrize(
    "control",
    [
        {"rewind": {"b": ["a"]}},
        {"rewind": {"b": ["b"]}, "max_handoffs": 1},
        {"rewind": {"a": ["b"]}, "max_handoffs": 1},
        {"rewind": {"b": ["end"]}, "max_handoffs": 1},
        {"retry_all": [True], "max_handoffs": 1},
        {"retry_all": ["b"], "max_handoffs": True},
        {"retry_all": [], "max_handoffs": 1},
        {"rewind": {}, "max_handoffs": 1},
        {"max_handoffs": 1},
        {"retry_all": ["b"], "max_handoffs": 1, "unknown": True},
    ],
)
def test_bad_declarations_fail_at_build_time(control):
    @task("a")
    def a(v):
        return v

    @task("b")
    def b(v):
        return v

    with pytest.raises(PipelineBuildError, match="control"):
        pipeline("bad", a | b, control=control)


def test_directive_validation_and_plan_canonical_identity():
    with pytest.raises(FatalError, match="explicit value"):
        Handoff.rewind("a")
    with pytest.raises(TypeError):
        Handoff.retry_all(value={})

    @task("a")
    def a(v):
        return v

    @task("b")
    def b(v):
        return v

    named = pipeline(
        "canonical", a | b, control={"rewind": {"b": ["a"]}, "retry_all": ["b"], "max_handoffs": 2}
    )
    numeric = pipeline(
        "canonical", a | b, control={"rewind": {"1": [0]}, "retry_all": [1], "max_handoffs": 2}
    )
    assert named.spec_digest == numeric.spec_digest
    assert named.describe()["control"] == numeric.describe()["control"]


def test_missing_capability_is_not_hidden_by_wrapper():
    class PlainStore:
        pass

    assert not supports_visits(PlainStore())
    plain = MemoryStore()
    plain.commit_entry = None
    wrapped = WriteBehindStore(plain)
    assert not supports_visits(wrapped)
    # The compatibility contract counts as part of the capability: a store that cannot declare how
    # far its on-disk model has come cannot honour the downgrade rule either.
    class NoLevelStore(MemoryStore):
        feature_level = None  # type: ignore[assignment]

    assert not supports_visits(NoLevelStore())
    assert supports_visits(MemoryStore())


class GenerationState(HistoryArtifact):
    pass


def test_history_is_detached_and_keeps_future_snapshots_on_restore():
    original = {"nested": [1]}
    state = GenerationState(original).checkpoint("before", metadata={"valid": True})
    original["nested"].append(2)
    state = state.with_state({"nested": [3]}).checkpoint("after")
    restored = state.restore("before")
    restored.state["nested"].append(4)
    restored.history[0]["state"]["nested"].append(5)
    assert restored.state == {"nested": [1]}
    assert len(restored.history) == 2
    assert isinstance(restored, GenerationState)
    assert state.state == {"nested": [3]}
    with pytest.raises(ValueError, match="duplicate"):
        restored.checkpoint("before")
    with pytest.raises(ValueError, match="selected"):
        restored.prune("before")
    assert len(restored.prune("after").history) == 1
    with pytest.raises(KeyError):
        restored.restore("missing")
    with pytest.raises(ValueError, match="reserved"):
        restored.checkpoint("snapshot:0")


def test_history_codec_roundtrip_and_invalid_envelopes():
    registry = CodecRegistry()
    registry.register_type(GenerationState)
    state = (
        GenerationState({"x": [1]})
        .checkpoint("before")
        .with_state(None)
        .checkpoint("after")
        .restore("before")
    )
    encoded = registry.dump(state)
    restored = registry.load(encoded)
    assert isinstance(restored, GenerationState)
    assert restored.state == state.state
    assert restored.history == state.history
    assert restored.selected == state.selected
    assert registry.dump(restored).digest == encoded.digest
    with pytest.raises(ArtifactCodecError, match="unregistered"):
        CodecRegistry().load(encoded)
    raw = json.loads(encoded.data)
    raw["version"] = 2
    with pytest.raises(ArtifactCodecError, match="version"):
        registry.load(dataclasses.replace(encoded, data=json.dumps(raw).encode()))
    with pytest.raises(ArtifactCodecError, match="JSON"):
        registry.dump(HistoryArtifact({"client": object()}))
    with pytest.raises(ArtifactCodecError):
        registry.load(Encoded("HistoryArtifact", "history-v1", "", 0, b"{}"))


def test_history_payload_rewind_and_retry_all(store):
    registry = CodecRegistry()
    registry.register_type(GenerationState)
    seen = []

    @task("a")
    def a(value, ctx):
        assert isinstance(value, GenerationState)
        seen.append((ctx.visit, value.state, len(value.history)))
        return value.with_state({"x": ctx.visit + 1}).checkpoint(f"visit-{ctx.visit}")

    @task("b")
    def b(value, ctx):
        if ctx.visit == 0:
            return Handoff.rewind("a", value.restore("seed"))
        if ctx.visit == 1:
            return Handoff.retry_all()
        return value

    spec = pipeline(
        "history",
        a | b,
        registry=registry,
        control={"rewind": {"b": ["a"]}, "retry_all": ["b"], "max_handoffs": 2},
    ).bind(GenerationState({"x": 0}).checkpoint("seed"))
    assert run(store, spec, registry=registry).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert seen == [(0, {"x": 0}, 1), (1, {"x": 0}, 2), (2, {"x": 0}, 1)]
    final = store.get_artifact(store.pipelines()[0].pipeline_id, 1)
    assert isinstance(registry.load(final.encoded()), GenerationState)
    assert list(iter_rows(store, kind="artifacts"))[-1]["payload"]["version"] == 1


def test_backward_requeue_allows_other_pipeline_to_run(store):
    seen = []

    @task("a")
    async def a(value, ctx):
        seen.append((value, ctx.visit))
        await asyncio.sleep(0)
        return Handoff.retry_all() if value == "loop" and ctx.visit < 3 else value

    template = pipeline("fair", a, control={"retry_all": ["a"], "max_handoffs": 3})
    report = Runner(store=store, concurrency=1, handle_signals=False).run(template.map(["loop", "other"]))
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 2}
    assert seen.index(("other", 0)) < seen.index(("loop", 3))


def test_sqlite_reopen_resumes_exact_occurrence_and_seed(tmp_path, monkeypatch):
    seen = []

    @task("a")
    def a(value, ctx):
        seen.append((ctx.visit, ctx.attempt, value.copy()))
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("reopen", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
    path = str(tmp_path / "reopen.db")
    first = SqliteStore(path)
    original = first.commit_control_transition

    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash after commit")

    monkeypatch.setattr(first, "commit_control_transition", crash)
    run(first, spec)
    first.close()
    second = SqliteStore(path)
    try:
        assert run(second, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert seen == [(0, 1, {"seed": True}), (1, 1, {"selected": True})]
        assert second.get_artifact_by_id(f"{spec.pipeline_id}:0").payload is not None
        assert second.get_artifact(spec.pipeline_id, 0).id.endswith("#1")
    finally:
        second.close()


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize("journal", ["full", "summary"])
def test_backend_and_summary_traversal(kind, journal, tmp_path):
    from pyattacker.backends import FileBackend

    backend = FileBackend(str(tmp_path / "blobs"), min_bytes=0)
    result = (
        MemoryStore(journal=journal, backend=backend)
        if kind == "memory"
        else SqliteStore(str(tmp_path / "backend.db"), journal=journal, backend=backend)
    )
    seen = []

    @task("a")
    def a(value, ctx):
        seen.append(value.copy())
        return value

    @task("b")
    def b(value, ctx):
        if ctx.visit == 0:
            return Handoff.rewind("a", {"selected": True})
        if ctx.visit == 1:
            return Handoff.retry_all()
        return value

    spec = pipeline(
        "backend", a | b, control={"rewind": {"b": ["a"]}, "retry_all": ["b"], "max_handoffs": 2}
    ).bind({"seed": True})
    try:
        assert run(result, spec).stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert seen == [{"seed": True}, {"selected": True}, {"seed": True}]
        assert result.visit_state(spec.pipeline_id)["handoffs"] == 2
        assert all(a.available == (journal == "full") for a in result.artifacts(spec.pipeline_id))
        assert sum(a.is_final for a in result.artifacts(spec.pipeline_id)) == 1
    finally:
        result.close()


def test_missing_entry_fallback_preserves_budget_and_counters(store, monkeypatch):
    @task("a")
    def a(value, ctx):
        return value

    @task("b")
    def b(value, ctx):
        return Handoff.rewind("a", {"selected": True}) if ctx.visit == 0 else value

    spec = pipeline("missing", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({})
    inner = getattr(store, "inner", store)
    original = inner.commit_control_transition

    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("after transfer")

    monkeypatch.setattr(inner, "commit_control_transition", crash)
    run(store, spec)
    entry_id = inner.visit_state(spec.pipeline_id)["input"]
    original_read = inner.get_artifact_by_id

    def missing(artifact_id):
        return None if artifact_id == entry_id else original_read(artifact_id)

    monkeypatch.setattr(inner, "get_artifact_by_id", missing)
    monkeypatch.setattr(inner, "commit_control_transition", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.visit_state(spec.pipeline_id)["handoffs"] == 1
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 2, "1": 1}
    assert any(e.kind == "pipeline.checkpoint_missing" and e.data["budget_preserved"] for e in store.events())


def test_ordinary_success_transaction_rolls_back_output_and_active_mapping(store, monkeypatch):
    @task("a")
    def a(value, ctx):
        return value

    spec = pipeline("ordinary-rollback", a, control={"retry_all": ["a"], "max_handoffs": 1}).bind(1)
    inner = getattr(store, "inner", store)
    original = inner._visit_save

    def fail(pid, state):
        if state["terminal"] is not None:
            raise RuntimeError("fail atomic success")
        return original(pid, state)

    monkeypatch.setattr(inner, "_visit_save", fail)
    assert run(store, spec).stats["pipelines"]["by_state"] == {"failed": 1}
    assert store.visit_state(spec.pipeline_id)["active"] == {}
    assert len(store.artifacts(spec.pipeline_id)) == 1
    assert store.attempts() == []
    monkeypatch.setattr(inner, "_visit_save", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.tasks()[0].visit == 0
    assert store.tasks()[0].attempts_used == 2


def test_backward_declarative_validation(tmp_path):
    from pyattacker import ConfigError, load_spec

    raw = {
        "pipeline": {
            "name": "declarative",
            "tasks": [{"use": "echo", "name": "a"}, {"use": "echo", "name": "b"}],
            "control": {"rewind": {"b": ["a"]}, "retry_all": ["b"], "max_handoffs": 2},
        },
        "run": {"max_handoffs": 1},
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw))
    loaded = load_spec(str(path))
    assert loaded.template.control.backward_enabled
    assert loaded.run["max_handoffs"] == 1
    raw["run"]["max_handoffs"] = True
    path.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match=r"run\.max_handoffs"):
        load_spec(str(path))


def test_sigkill_mid_rewound_visit_resumes_same_visit_and_consumes_attempt(tmp_path):
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path

    script = tmp_path / "application.py"
    database = tmp_path / "kill.db"
    marker = tmp_path / "entered"
    script.write_text("""import asyncio, sys
from pathlib import Path
from pyattacker import task, pipeline, Handoff, Runner

@task("a")
async def a(value, ctx):
    if ctx.visit == 1 and sys.argv[2] == "crash":
        Path(sys.argv[3]).write_text("entered")
        await asyncio.sleep(60)
    return value

@task("b")
def b(value, ctx):
    if ctx.visit == 0:
        return Handoff.rewind("a", {"selected": True})
    return value

spec = pipeline("kill", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind({"seed": True})
# The recovery contract for a `running` row is explicit: only `resume=True` claims the traversal
# back from the run that was killed (see `test_running_backward_pipeline_is_skipped_without_resume`).
report = Runner(store=sys.argv[1], handle_signals=False, write_behind=True,
                resume=sys.argv[2] == "resume").run([spec])
assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}, report.stats
""")
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    process = subprocess.Popen(
        [sys.executable, str(script), str(database), "crash", str(marker)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and time.monotonic() < deadline and process.poll() is None:
            time.sleep(0.01)
        assert marker.exists(), "child did not reach rewound visit"
    finally:
        process.kill()
        process.communicate(timeout=5)
    resumed = subprocess.run(
        [sys.executable, str(script), str(database), "resume", str(marker)],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert resumed.returncode == 0, resumed.stderr
    store = SqliteStore(str(database), read_only=True)
    try:
        pid = store.pipelines()[0].pipeline_id
        assert store.visit_state(pid)["handoffs"] == 1
        assert store.visit_state(pid)["counters"] == {"0": 1, "1": 1}
        assert [(a.seq, a.visit, a.attempt_no) for a in store.attempts()] == [
            (0, 0, 1),
            (1, 0, 1),
            (0, 1, 2),
            (1, 1, 1),
        ]
        assert store.get_artifact(pid, 0).payload == b'{"selected":true}'
    finally:
        store.close()


@pytest.mark.parametrize(
    "declaration",
    [
        {"rewind": {"b": ["a"]}, "max_handoffs": 1},
        {"retry_all": ["b"], "max_handoffs": 1},
    ],
)
def test_backward_describe_is_reusable(declaration):
    from pyattacker.handoff import build_control

    plan = build_control(declaration, ["a", "b"])
    assert build_control(plan.describe(), ["a", "b"]).fingerprint() == plan.fingerprint()


@pytest.mark.parametrize("strict", [False, True])
def test_backward_transfer_obeys_lease_reclamation(store, strict):
    pool = Pool("api", [Resource("api-1", capacity=1)])

    @task("a", resource="api")
    async def a(value, ctx):
        async with ctx.acquire():
            return value

    @task("b", resource="api")
    async def b(value, ctx):
        if ctx.visit == 0:
            await ctx.acquire_lease()  # intentionally leak to verify strict mode preempts transfer
            return Handoff.rewind("a", value)
        return value

    spec = pipeline("leases", a | b, control={"rewind": {"b": ["a"]}, "max_handoffs": 1}).bind(1)
    report = run(store, spec, pools=[pool], strict_leases=strict)
    assert report.stats["pipelines"]["by_state"] == ({"failed": 1} if strict else {"succeeded": 1})
    assert pool.stats().active == 0
    assert len(store.handoffs()) == (0 if strict else 1)
    assert store.attempts()[1].leases[0]["released"]
