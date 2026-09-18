"""Forward handoffs (``docs/design.md`` §4.8) —— the advanced, opt-in control-flow feature.

What this file proves, in the order the design promises it:

* **opt-in and inert**: without a ``control`` block nothing changes at all — no ledger row, no counter,
  not a byte of ``spec_digest`` (a literal digest is pinned below);
* **a forward handoff skips stations**: the skipped task functions never run, their task rows do not
  exist, the pipeline still ends ``succeeded``, and the record explains the gap;
* **``END`` finishes the pipeline** with its entry artifact as the final output (a new payload, or the
  artifact this task received), cursor at ``n_tasks``, ``n_tasks_done`` a *position* rather than a count;
* **durability**: the commit is one atomic store operation (task row + handed-off attempt + entry
  artifact + ledger row + cursor), a killed process resumes *at the target* with the entry state and
  never re-runs the source task, and a store that cannot commit atomically is refused up front;
* **the payload has its own address**: handoff payloads live at ``seq >= n_tasks`` so they can never
  overwrite a task slot, and a lost payload falls back to the documented restart-from-zero rule;
* **structural validation only**: undeclared, backward, ambiguous and last-task-``END`` targets are
  refused with a field path (and ``validate`` agrees with ``run``), a directive along an undeclared edge
  is fatal rather than a silent jump, and ``fanout`` never lets one escape its group.

The tests drive real runs (or a real SIGKILLed process) rather than hand-written rows — except where
they must fabricate the durable state a *non-atomic* store could leave behind, which those tests say.
"""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from pyattacker import (
    Handoff,
    MemoryStore,
    Pool,
    Resource,
    Runner,
    SqliteStore,
    TaskSpec,
    build_task_spec,
    echo,
    fanout,
    load_spec,
    pipeline,
    simulate_llm,
    task,
)
from pyattacker.cli import main as cli_main
from pyattacker.errors import ConfigError, FatalError, PipelineBuildError
from pyattacker.export import iter_rows
from pyattacker.handoff import ControlPlan, build_control
from pyattacker.merge import merge_reports
from pyattacker.monitor import read_snapshot, render_snapshot
from pyattacker.server import StatsServer
from pyattacker.store.base import HandoffRecord
from pyattacker.store.writebehind import WriteBehindStore

CALLS: dict[str, int] = {}
SEED = {"row": 1}


def _note(name: str) -> None:
    CALLS[name] = CALLS.get(name, 0) + 1


@pytest.fixture(autouse=True)
def _reset_calls():
    CALLS.clear()


@contextmanager
def _runner(store: Any, **cfg: Any):
    """A Runner for one test, closed afterwards — the store stays usable while the block runs."""
    runner = Runner(store=store, concurrency=2, handle_signals=False, **cfg)
    try:
        yield runner
    finally:
        runner.close()


# --------------------------------------------------------------------------- tasks
@task("ho.a")
def ho_a(value, ctx):
    _note("a")
    return {"seed": value}


@task("ho.b")
def ho_b(value, ctx):
    _note("b")
    return {**value, "b": True}


@task("ho.c")
def ho_c(value, ctx):
    _note("c")
    return {**value, "c": True}


@task("ho.d")
def ho_d(value, ctx):
    _note("d")
    return {**value, "d": True}


@task("ho.e")
def ho_e(value, ctx):
    _note("e")
    return {**value, "e": True}


def _judge(target: str, *, name: str = "ho.judge", reuse: bool = False) -> TaskSpec:
    """A task that hands off to ``target``: with a new payload, or with no value at all.

    Built through ``build_task_spec`` so the declared target is part of the task's identity, and so two
    variants are two distinct specs rather than one name with two behaviours.
    """

    def _impl(value: Any, ctx: Any) -> Handoff:
        _note("judge")
        if reuse:
            return Handoff.to(target, reason="nothing to add")
        return Handoff.to(target, {**value, "jumped": True}, reason="metrics not needed")

    _impl.__name__ = f"judge_to_{target}_{'reuse' if reuse else 'payload'}"
    return build_task_spec(_impl, name=name, parameters={"target": target, "reuse": reuse})


def _gate(*, name: str = "ho.gate", reuse: bool = False) -> TaskSpec:
    """A task that ends the pipeline: with a new payload, or with the artifact it received."""

    def _impl(value: Any, ctx: Any) -> Handoff:
        _note("gate")
        if reuse:
            return Handoff.end(reason="the input is already the answer")
        return Handoff.end({**value, "ended": True}, reason="already good enough")

    _impl.__name__ = f"gate_{'reuse' if reuse else 'payload'}"
    return build_task_spec(_impl, name=name, parameters={"reuse": reuse})


def _rogue(target: str, *, name: str = "ho.rogue") -> TaskSpec:
    """A task that hands off somewhere the pipeline did not declare."""

    def _impl(value: Any, ctx: Any) -> Handoff:
        _note("rogue")
        return Handoff.to(target, value, reason="undeclared")

    _impl.__name__ = f"rogue_to_{target}"
    return build_task_spec(_impl, name=name, parameters={"target": target})


# a | b | judge | c | d : the hop skips ho.c and continues at ho.d
SKIP_TEMPLATE = pipeline(
    "ho-skip", ho_a | ho_b | _judge("ho.d") | ho_c | ho_d, control={"edges": {"ho.judge": ["ho.d", "end"]}}
)
# a | gate | c | d : END after the gate, so nothing after it runs at all
END_TEMPLATE = pipeline("ho-end", ho_a | _gate() | ho_c | ho_d, control={"edges": {"ho.gate": ["end"]}})
# a | judge(reuse) | c | d : the target enters with the artifact the judge received
REUSE_TEMPLATE = pipeline(
    "ho-reuse", ho_a | _judge("ho.d", reuse=True) | ho_c | ho_d, control={"edges": {"ho.judge": ["ho.d"]}}
)
END_REUSE_TEMPLATE = pipeline(
    "ho-end-reuse", ho_a | _gate(reuse=True) | ho_c, control={"edges": {"ho.gate": ["end"]}}
)
PLAIN_TEMPLATE = pipeline("ho-plain", ho_a | ho_b | ho_d)


def _skipped(store: Any, pipeline_id: str, n_tasks: int) -> list[int]:
    """The seqs of a chain that have no task row —— the stations a handoff skipped."""
    ran = {record.seq for record in store.tasks(pipeline_id)}
    return [seq for seq in range(n_tasks) if seq not in ran]


def _kinds(store: Any, pipeline_id: str) -> list[str]:
    return [event.kind for event in store.events(pipeline_id=pipeline_id, limit=500)]


# ============================================================== opt-in and inertness
def test_a_control_free_pipeline_digests_exactly_as_before():
    """The pinned literal is the compatibility regression, not a snapshot of today's code.

    ``include_code=False`` keeps it stable across edits to the built-in task's source, so it can only
    fail if the *shape* of the digest payload changes — which is exactly the promise: no ``v3:`` bump,
    no invalidated store, and a control block folded in only when there is one.
    """
    plain = pipeline("ho-digest", echo | simulate_llm(latency_ms=0), include_code=False)
    assert plain.spec_digest == "v2:4a437b72348308d332f9ed3c9cfa2aa6"

    controlled = pipeline(
        "ho-digest",
        echo | simulate_llm(latency_ms=0),
        include_code=False,
        control={"edges": {"mock.echo": ["mock.llm"]}},
    )
    assert controlled.spec_digest == "v2:f3f54045c8763c50982650930d0bca22"
    assert controlled.control is not None


def test_a_control_free_run_writes_no_handoff_rows(tmp_path):
    with _runner(str(tmp_path / "plain.db")) as runner:
        report = runner.run(PLAIN_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert CALLS == {"a": 1, "b": 1, "d": 1}
        assert store.handoffs() == []
        assert store.stats(report.run_id)["handoffs_total"] == 0
        assert [record.outcome for record in store.attempts(pipeline_id=pid)] == 3 * ["succeeded"]
        assert next(iter(store.export_rows()))["handoffs"] == []
        succeeded = [e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.succeeded"]
        assert succeeded and "handed_off" not in succeeded[0].data


# ==================================================================== forward handoff
def test_a_forward_handoff_skips_the_stations_in_between(tmp_path):
    with _runner(str(tmp_path / "skip.db")) as runner:
        report = runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        row = store.get_pipeline(pid)

        # the skipped task function never ran, and its slot has no row at all
        assert CALLS == {"a": 1, "b": 1, "judge": 1, "d": 1}
        assert _skipped(store, pid, SKIP_TEMPLATE.n_tasks) == [3]  # ho.c
        assert [t.name for t in store.tasks(pid)] == ["ho.a", "ho.b", "ho.judge", "ho.d"]

        # the pipeline still succeeded, and the cursor ends where the position rule says
        assert (row.state, row.n_tasks_done, row.n_tasks_total) == ("succeeded", 5, 5)
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}

        # the source task is finalized as handed off, in the same record as the jump
        source = next(t for t in store.tasks(pid) if t.seq == 2)
        assert (source.state, source.output_artifact_id) == ("handed_off", None)
        attempt = next(a for a in store.attempts(pipeline_id=pid) if a.seq == 2)
        assert attempt.outcome == "handed_off"
        assert attempt.decision == {}  # a handoff is not a retry decision
        assert attempt.error_class is None


def test_the_ledger_records_the_hop_and_its_entry_state(tmp_path):
    with _runner(str(tmp_path / "ledger.db")) as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        ledger = store.handoffs(pipeline_id=pid)
        assert len(ledger) == 1
        hop = ledger[0]
        assert (hop.from_task, hop.from_seq, hop.to_task, hop.to_seq) == ("ho.judge", 2, "ho.d", 4)
        assert hop.reason == "metrics not needed"
        assert hop.entry_reused is False
        # the payload lives above the chain: seed (-1) -> chain (0..4) -> payloads (>= 5)
        assert hop.entry_seq == SKIP_TEMPLATE.n_tasks
        assert hop.entry_artifact_id == f"{pid}:{hop.entry_seq}"
        payload = store.get_artifact(pid, hop.entry_seq)
        assert payload is not None and payload.task_name == "ho.judge"
        assert json.loads(payload.payload) == {"seed": SEED, "b": True, "jumped": True}

        # ... and the target really received it
        received = next(t for t in store.tasks(pid) if t.seq == 4)
        assert received.input_artifact_id == hop.entry_artifact_id

        event = next(e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.handoff")
        assert event.data["task"] == "ho.judge" and event.data["to_task"] == "ho.d"
        assert event.data["handoff_id"] == hop.handoff_id
        assert event.task_run_id == f"{pid}:2"


def test_a_handoff_without_a_value_reuses_the_input_artifact(tmp_path):
    with _runner(str(tmp_path / "reuse.db")) as runner:
        runner.run(REUSE_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        hop = store.handoffs(pipeline_id=pid)[0]
        assert hop.entry_reused is True
        # the judge sits at seq 1, so its input is ho.a's artifact at seq 0
        assert (hop.entry_seq, hop.entry_artifact_id) == (0, f"{pid}:0")
        assert store.get_artifact(pid, hop.entry_seq).task_name == "ho.a"
        # seed + ho.a's artifact + the target's own: a reused entry creates no payload artifact
        assert [a.seq for a in store.artifacts(pid)] == [-1, 0, 3]
        received = next(t for t in store.tasks(pid) if t.seq == 3)
        assert received.input_artifact_id == f"{pid}:0"
        assert CALLS["d"] == 1


def test_each_payload_gets_its_own_address_above_the_chain(tmp_path):
    """Two hops into one pipeline row: the payload band is allocated per commit, never reused."""
    db = str(tmp_path / "band.db")
    with _runner(db) as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        pid = runner.store.pipelines()[-1].pipeline_id
        first = runner.store.handoffs(pipeline_id=pid)[0]
    # retry_succeeded re-runs the same pipeline row, so the second hop lands in the same ledger
    with _runner(db, retry_succeeded=True) as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        ledger = runner.store.handoffs(pipeline_id=pid)
    assert [hop.entry_seq for hop in ledger] == [first.entry_seq, first.entry_seq + 1]
    assert ledger[0].entry_artifact_id != ledger[1].entry_artifact_id
    assert all(hop.entry_seq >= SKIP_TEMPLATE.n_tasks for hop in ledger)
    # the chain's own artifacts were overwritten in place by the rerun, never by a payload
    reopened = SqliteStore(db)
    try:
        assert [a.seq for a in reopened.artifacts(pid) if a.seq < SKIP_TEMPLATE.n_tasks] == [-1, 0, 1, 4]
    finally:
        reopened.close()


# ============================================================================== END
def test_end_finishes_the_pipeline_with_the_payload_as_final_artifact(tmp_path):
    with _runner(str(tmp_path / "end.db")) as runner:
        report = runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        row = store.get_pipeline(pid)
        assert (row.state, row.n_tasks_done, row.n_tasks_total) == ("succeeded", 4, 4)
        assert CALLS == {"a": 1, "gate": 1}  # ho.c and ho.d never ran
        assert _skipped(store, pid, END_TEMPLATE.n_tasks) == [2, 3]

        hop = store.handoffs(pipeline_id=pid)[0]
        assert (hop.to_seq, hop.to_task) == (None, None)
        final = store.get_artifact(pid, hop.entry_seq)
        assert final.is_final is True and final.seq >= END_TEMPLATE.n_tasks
        assert json.loads(final.payload) == {"seed": SEED, "ended": True}

        succeeded = next(e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.succeeded")
        assert succeeded.data["handed_off"] is True
        assert report.stats["handoffs_total"] == 1


def test_end_with_no_value_marks_the_input_artifact_final(tmp_path):
    with _runner(str(tmp_path / "end-reuse.db")) as runner:
        runner.run(END_REUSE_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        hop = store.handoffs(pipeline_id=pid)[0]
        assert (hop.entry_reused, hop.entry_seq) == (True, 0)  # the gate's input: ho.a's artifact
        assert store.get_artifact(pid, 0).is_final is True
        assert store.get_pipeline(pid).state == "succeeded"
        assert CALLS == {"a": 1, "gate": 1}


def test_the_cursor_of_a_control_enabled_pipeline_is_a_position(tmp_path):
    """`n_tasks_done == n_tasks_total` here does **not** mean "every task ran"."""
    with _runner(str(tmp_path / "position.db")) as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        row = store.get_pipeline(pid)
        assert row.n_tasks_done == row.n_tasks_total == 5
        assert len(store.tasks(pid)) == 4
        assert len(store.handoffs(pipeline_id=pid)) == 1
        # the hop's target is behind the final cursor: later stations did run after the jump
        hop = store.handoffs(pipeline_id=pid)[0]
        assert hop.to_seq is not None and row.n_tasks_done > hop.to_seq


# ============================================================================ recovery
def test_resume_continues_at_the_target_without_rerunning_the_source(tmp_path):
    db = str(tmp_path / "resume.db")

    def _doomed(value: Any, ctx: Any) -> Any:
        _note("doomed")
        if CALLS.get("doomed") == 1:
            raise FatalError("the target dies on its first visit")
        return {**value, "d": True}

    doomed = build_task_spec(_doomed, name="ho.doomed")
    template = pipeline(
        "ho-resume", ho_a | _judge("ho.doomed") | ho_c | doomed, control={"edges": {"ho.judge": ["ho.doomed"]}}
    )
    with _runner(db) as runner:
        report = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert CALLS == {"a": 1, "judge": 1, "doomed": 1}
        assert store.get_pipeline(pid).n_tasks_done == 3

    with _runner(db, resume=True) as runner:
        second = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}
        # the source task and every station before it were not revisited
        assert CALLS == {"a": 1, "judge": 1, "doomed": 2}
        assert store.get_pipeline(pid).state == "succeeded"
        resumed = next(e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.resumed")
        assert resumed.data["from_seq"] == 3
        assert resumed.data["via"] == "handoff"
        assert resumed.data["handoff"]["from_task"] == "ho.judge"


def test_a_consumed_handoff_falls_back_to_the_ordinary_cursor_rule(tmp_path):
    """Once later stations have run, the newest ledger row is behind the cursor and the artifact rule wins."""
    db = str(tmp_path / "consumed.db")

    def _fail_end(value: Any, ctx: Any) -> Any:
        _note("end")
        if CALLS.get("end") == 1:
            raise FatalError("the last station dies")
        return {**value, "end": True}

    fail_end = build_task_spec(_fail_end, name="ho.fail_end")
    template = pipeline(
        "ho-consumed",
        ho_a | _judge("ho.d") | ho_c | ho_d | ho_e | fail_end,
        control={"edges": {"ho.judge": ["ho.d"]}},
    )
    with _runner(db) as runner:
        runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert CALLS == {"a": 1, "judge": 1, "d": 1, "e": 1, "end": 1}
        assert store.get_pipeline(pid).n_tasks_done == 5  # past the hop's target (3)
        hop = store.handoffs(pipeline_id=pid)[0]
        assert hop.to_seq == 3 and hop.to_seq < store.get_pipeline(pid).n_tasks_done

    with _runner(db, resume=True) as runner:
        report = runner.run(template.map([SEED]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        # artifact(cursor - 1) = ho.e's output: only the failed last station is revisited
        assert CALLS == {"a": 1, "judge": 1, "d": 1, "e": 1, "end": 2}


def test_an_end_ledger_row_that_did_not_finish_writing_is_repaired(tmp_path):
    """A non-atomic store can leave "ledger row without the terminal settle"; the repair goes by ledger.

    With an atomic commit this state is unreachable, so it is *fabricated* the way such a store would
    leave it (the ledger row is durable, the row is back to ``failed``). The repair must then use the
    row's entry artifact: the linear ``artifact(n_tasks - 1)`` rule cannot even decide the case, because
    an early END left no artifact in the last slots.
    """
    db = str(tmp_path / "end-repair.db")
    with _runner(db) as runner:
        runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        hop = store.handoffs(pipeline_id=pid)[0]
        store.finish_pipeline(pid, "failed", n_tasks_done=END_TEMPLATE.n_tasks)
        assert store.get_pipeline(pid).state == "failed"
    CALLS.clear()

    with _runner(db, resume=True) as runner:
        report = runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        row = store.get_pipeline(pid)
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert (row.state, row.n_tasks_done) == ("succeeded", 4)
        assert CALLS == {}  # nothing was re-run at all
        assert store.get_artifact(pid, hop.entry_seq).is_final is True
        repaired = next(e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.terminal_repaired")
        assert repaired.data["handoff_id"] == hop.handoff_id
        succeeded = [e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.succeeded"]
        assert succeeded[-1].data["handed_off"] is True


def test_a_cursor_past_the_end_is_still_corruption_even_with_an_end_ledger_row(tmp_path):
    """The corruption bound is not papered over by a durable END row: the two rules stay separate."""
    db = str(tmp_path / "corrupt.db")
    with _runner(db) as runner:
        runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        # not a state the Runner can create: the cursor is beyond the chain *and* an END row is durable
        record = store.get_pipeline(pid)
        record.n_tasks_done = 99
        store.upsert_pipeline(record)
        store.finish_pipeline(pid, "failed", error=ValueError("injected"))
    CALLS.clear()

    with _runner(db, resume=True) as runner:
        report = runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert store.get_pipeline(pid).error_type == "CorruptCheckpoint"
        assert store.get_pipeline(pid).n_tasks_done == 99  # the evidence is left in place
        assert CALLS == {}
        kinds = _kinds(store, pid)
        assert "pipeline.corrupt_cursor" in kinds
        assert "pipeline.terminal_repaired" not in kinds


def test_a_lost_forward_entry_payload_restarts_from_zero(tmp_path):
    """``journal=summary`` keeps the ledger row but not the bytes: the restart-from-zero rule applies."""
    db = str(tmp_path / "lost.db")
    with _runner(db, journal="summary") as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        # poison the row back to a resumable failure, the shape a kill would leave
        store.finish_pipeline(pid, "interrupted", n_tasks_done=4)
        assert store.get_artifact(pid, SKIP_TEMPLATE.n_tasks).available is False
    CALLS.clear()

    with _runner(db, journal="summary", resume=True) as runner:
        second = runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert CALLS == {"a": 1, "b": 1, "judge": 1, "d": 1}  # everything ran again, from the seed
        missing = [e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.checkpoint_missing"]
        assert missing and missing[-1].data["to_seq"] == 4


def test_a_lost_end_entry_payload_restarts_from_zero(tmp_path):
    db = str(tmp_path / "lost-end.db")
    with _runner(db, journal="summary") as runner:
        runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        store.finish_pipeline(pid, "interrupted", n_tasks_done=END_TEMPLATE.n_tasks)
    CALLS.clear()

    with _runner(db, journal="summary", resume=True) as runner:
        second = runner.run(END_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert CALLS == {"a": 1, "gate": 1}
        assert _kinds(store, pid).count("pipeline.checkpoint_missing") == 1


KILL_MODULE = '''
from pathlib import Path
from pyattacker import Handoff, build_task_spec, pipeline

HERE = Path(__file__).parent
CALLS = HERE / "calls.txt"


def note(name):
    with CALLS.open("a", encoding="utf-8") as handle:
        handle.write(name + "\\n")


def _a(value, ctx):
    note("a")
    return {"seed": value}


def _judge(value, ctx):
    note("judge")
    return Handoff.to("k.d", {**value, "jumped": True}, reason="skip c")


def _c(value, ctx):
    note("c")
    return value


async def _d(value, ctx):
    note("d")
    release = HERE / "release-d"
    while not release.exists():
        await ctx.clock.sleep(0.02)
    return {**value, "d": True}


TEMPLATE = pipeline(
    "killed",
    build_task_spec(_a, name="k.a")
    | build_task_spec(_judge, name="k.judge")
    | build_task_spec(_c, name="k.c")
    | build_task_spec(_d, name="k.d"),
    control={"edges": {"k.judge": ["k.d"]}},
)
'''

KILL_SCRIPT = '''
import sys
sys.path.insert(0, {here!r})
import killmod
from pyattacker import Runner

runner = Runner(store={db!r}, concurrency=2, handle_signals=False, heartbeat_s=0.05, stale_after_s=0.05)
runner.run(killmod.TEMPLATE.map([{{"row": 1}}]))
'''


@pytest.mark.skipif(os.name != "posix", reason="SIGKILL-based test: POSIX only")
def test_a_killed_process_resumes_at_the_target(tmp_path):
    """The end-to-end durability claim: a real process is SIGKILLed after the commit.

    The child hands off, opens the target and blocks there. The parent waits until both facts are
    durable, kills the process, then resumes in-process: execution must continue *at the target* with
    the recorded entry state, and neither the source task nor the stations before it may run again.
    """
    src = str(Path(__file__).resolve().parents[1] / "src")
    db = str(tmp_path / "killed.db")
    (tmp_path / "killmod.py").write_text(KILL_MODULE, encoding="utf-8")
    script = tmp_path / "killed_pipeline.py"
    script.write_text(KILL_SCRIPT.format(here=str(tmp_path), db=db), encoding="utf-8")
    calls = tmp_path / "calls.txt"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([src, os.environ.get("PYTHONPATH", "")])}
    child = subprocess.Popen(
        [sys.executable, str(script)], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.time() + 90
        hop = None
        rows: list[Any] = []
        while time.time() < deadline:
            if child.poll() is not None:
                out, err = child.communicate()
                pytest.fail(f"the child exited early ({child.returncode}):\n{out}\n{err}")
            if os.path.exists(db):
                try:
                    probe = SqliteStore(db, read_only=True)
                except Exception:  # the file exists but is not a database yet
                    probe = None
                if probe is not None:
                    try:
                        rows = probe.handoffs()
                        target = [t for t in probe.tasks() if t.seq == 3]
                    except sqlite3.OperationalError:  # the writer has not created the schema yet
                        rows, target = [], []
                    finally:
                        probe.close()
                    if rows and target:
                        hop = rows[0]
                        break
            time.sleep(0.05)
        else:
            pytest.fail("the child never committed the handoff and opened the target")
        assert (hop.from_task, hop.from_seq, hop.to_task, hop.to_seq) == ("k.judge", 1, "k.d", 3)
        assert calls.read_text(encoding="utf-8").split() == ["a", "judge", "d"]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=30)
    finally:
        if child.poll() is None:  # pragma: no cover - defensive
            child.kill()
            child.wait(timeout=30)

    # Reclaim the abandoned row the way a real `--resume` does, but deterministically: `stale_after_s=0`
    # says "a heartbeat that is not from *this* run is stale", so the test does not race the child's
    # 50ms heartbeat loop (the staleness clock itself is covered by the resume tests).
    probe = SqliteStore(db)
    try:
        assert probe.interrupt_stale(stale_after_s=0.0) == 1
        assert probe.get_pipeline(rows[0].pipeline_id).state == "interrupted"
    finally:
        probe.close()

    # Resume the killed run.
    (tmp_path / "release-d").write_text("go", encoding="utf-8")
    spec = importlib.util.spec_from_file_location("killmod", tmp_path / "killmod.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["killmod"] = module
    spec.loader.exec_module(module)

    with _runner(db, resume=True, stale_after_s=0.01) as runner:
        report = runner.run(module.TEMPLATE.map([{"row": 1}]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.status == "completed"
        assert store.get_pipeline(pid).state == "succeeded"
        # the kill happened after the hop: the source task and the skipped station never ran again
        assert calls.read_text(encoding="utf-8").split() == ["a", "judge", "d", "d"]
        assert [t.seq for t in store.tasks(pid)] == [0, 1, 3]
        assert store.get_pipeline(pid).n_tasks_done == 4
        final = [a for a in store.artifacts(pid) if a.is_final]
        assert len(final) == 1 and json.loads(final[0].payload)["d"] is True


# ====================================================================== store contract
def _make_store(kind: str, tmp_path: Path) -> Any:
    return MemoryStore() if kind == "memory" else SqliteStore(str(tmp_path / "backend.db"))


@pytest.fixture(params=["sqlite", "memory"])
def store(request: pytest.FixtureRequest, tmp_path: Path):
    backend = _make_store(request.param, tmp_path)
    try:
        yield backend
    finally:
        backend.close()


def test_handoffs_read_back_in_order_on_both_backends(store):
    """The ledger is a real read API: oldest first, `limit` keeps the newest N (the events rule)."""
    with _runner(store) as runner:
        report = runner.run(SKIP_TEMPLATE.map([SEED]))
        pid = runner.store.pipelines()[-1].pipeline_id
        assert [hop.from_task for hop in store.handoffs()] == ["ho.judge"]
        assert [hop.from_task for hop in store.handoffs(pipeline_id=pid, limit=1)] == ["ho.judge"]
        assert store.handoffs(pipeline_id="nope") == []
        assert store.handoffs(run_id=report.run_id)[0].pipeline_id == pid
        assert store.handoffs(run_id="other") == []
        # the exported row shape is the same on both backends (test_store.py pins the outer key set)
        row = next(row for row in store.export_rows() if row["pipeline_id"] == pid)
        assert set(row["handoffs"][0]) == {
            "from_task", "from_seq", "to_task", "to_seq", "reason",
            "entry_artifact_id", "entry_seq", "entry_reused", "ts",
        }


def test_a_store_without_the_capability_is_refused_up_front(tmp_path):
    """Fail fast: no silent downgrade to a non-durable handoff."""

    class NoHandoffs(SqliteStore):
        commit_handoff = None  # type: ignore[assignment]
        handoffs = None  # type: ignore[assignment]

    store = NoHandoffs(str(tmp_path / "legacy.db"))
    try:
        with _runner(store) as runner:
            report = runner.run(SKIP_TEMPLATE.map([SEED]))
            pid = runner.store.pipelines()[-1].pipeline_id
            row = runner.store.get_pipeline(pid)
            assert report.stats["pipelines"]["by_state"] == {"failed": 1}
            assert row.error_type == "ConfigError"
            assert "commit_handoff" in (row.error_message or "")
            assert CALLS == {}  # refused before any task ran
            failed = next(e for e in store.events(pipeline_id=pid) if e.kind == "pipeline.failed")
            assert failed.data["phase"] == "control_check"
    finally:
        store.close()


class ToggleStore(SqliteStore):
    """A store whose handoff capability can be switched off between runs (a backend downgrade)."""

    def disable_handoffs(self) -> None:
        self.commit_handoff = None  # type: ignore[method-assign]  # an instance attribute shadows it
        self.handoffs = None  # type: ignore[method-assign]


def test_a_store_created_before_the_feature_stays_readable(tmp_path):
    """A reader must be able to open a pre-0.3.0 store: `report`/`watch`/`serve` never migrate a file.

    The old database has no `handoffs` table at all, and a read-only connection never runs the schema, so
    the ledger reads have to answer "nothing was ever recorded" instead of raising.
    """
    db = str(tmp_path / "legacy.db")
    with _runner(db) as runner:
        report = runner.run(PLAIN_TEMPLATE.map([SEED]))
        pid = runner.store.pipelines()[-1].pipeline_id
        assert report.stats["handoffs_total"] == 0
    raw = sqlite3.connect(db)
    raw.execute("DROP TABLE handoffs")
    raw.commit()
    raw.close()

    reopened = SqliteStore(db, read_only=True)
    try:
        assert reopened.handoffs() == []
        assert reopened.handoffs(pipeline_id=pid) == []
        assert reopened.stats()["handoffs_total"] == 0
        row = next(row for row in reopened.export_rows() if row["pipeline_id"] == pid)
        assert row["handoffs"] == []
    finally:
        reopened.close()


def test_a_capability_less_store_keeps_working_for_ordinary_pipelines(tmp_path):
    """The capability is optional: a third-party store without it is untouched by this feature."""
    store = ToggleStore(str(tmp_path / "legacy.db"))
    store.disable_handoffs()
    runner = Runner(store=store, concurrency=2, handle_signals=False)
    try:
        report = runner.run(PLAIN_TEMPLATE.map([SEED]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        pid = store.pipelines()[-1].pipeline_id
        # a failed row then resumes through the ordinary checkpoint rule, with no ledger read at all
        store.finish_pipeline(pid, "failed", n_tasks_done=2)
        CALLS.clear()
        report = runner.run(PLAIN_TEMPLATE.map([SEED]), resume=True)
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert CALLS == {"d": 1}  # resumed from ho.b's artifact; only the last station reran
        assert "runner.internal_error" not in _kinds(store, pid)
    finally:
        runner.close()


def test_a_downgraded_store_reports_a_config_error_not_a_crash(tmp_path):
    """A pipeline that *is* control-enabled and resumable still gets the clear refusal, not an AttributeError."""
    db = str(tmp_path / "downgrade.db")
    store = ToggleStore(db)
    runner = Runner(store=store, concurrency=2, handle_signals=False)
    try:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        pid = store.pipelines()[-1].pipeline_id
        store.finish_pipeline(pid, "failed", n_tasks_done=3)
        store.disable_handoffs()
        report = runner.run(SKIP_TEMPLATE.map([SEED]), resume=True)
        row = store.get_pipeline(pid)
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert row.error_type == "ConfigError"
        assert "commit_handoff" in (row.error_message or "")
        assert "runner.internal_error" not in _kinds(store, pid)
    finally:
        runner.close()


class ProbeWriteBehind(WriteBehindStore):
    """Records what the inner store can see at the moment of a handoff commit."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.commits: list[tuple[int, int, int]] = []

    def commit_handoff(self, record: HandoffRecord, **kwargs: Any) -> Any:
        before = len(self.inner.attempts(pipeline_id=record.pipeline_id))
        buffered = self.pending
        result = super().commit_handoff(record, **kwargs)
        self.commits.append((before, len(self.inner.attempts(pipeline_id=record.pipeline_id)), buffered))
        return result


def test_write_behind_flushes_and_commits_the_handed_off_attempt(tmp_path):
    """The handed-off attempt is written by the commit itself, never left in the batch buffer.

    With a large batch and no flush interval the attempts *before* the handoff are still buffered when
    the commit runs, which is what makes the observation meaningful: the flush has to happen inside the
    commit, and the current attempt must not join the buffer afterwards.
    """
    db = str(tmp_path / "wb.db")
    inner = SqliteStore(db)
    store = ProbeWriteBehind(inner, batch_size=1000, flush_interval=0.0)
    runner = Runner(store=store, concurrency=2, handle_signals=False)
    try:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        pid = runner.store.pipelines()[-1].pipeline_id
        assert store.commits, "the run never committed a handoff"
        before, after, buffered = store.commits[0]
        assert before == 0, "the earlier attempts should still have been buffered at commit time"
        assert buffered == 4, "2 attempts + 2 events (attempts and events share the buffer)"
        assert after == 3, "the flush plus the handed-off attempt are durable right after the commit"
        assert store.pending == 0
        assert [a.outcome for a in inner.attempts(pipeline_id=pid)][2] == "handed_off"
    finally:
        runner.close()


# ======================================================================= validation
BAD_CONTROLS = [
    ({"edges": {"ho.d": ["ho.a"]}}, "forward-only"),
    ({"edges": {"nope": ["ho.d"]}}, "unknown source task"),
    ({"edges": {"ho.a": ["nope"]}}, "unknown destination task"),
    ({"edges": {"ho.a": ["ho.d", 9]}}, "no task at seq 9"),
    ({"edges": {"ho.a": []}}, "has no destinations"),
    ({"edges": {"ho.a": "ho.d"}}, "destinations must be a list"),
    ({"edges": {}}, "is empty"),
    ({"edges": {"ho.a": ["ho.d"]}, "mode": "handoff"}, "unknown field"),
    ({}, "missing required field"),
    ("handoff", "must be a mapping"),
    ({"edges": {"ho.d": ["end"]}}, "has no effect"),
    ({"edges": {"end": ["ho.d"]}}, "a destination, not a task"),
]


@pytest.mark.parametrize("control, fragment", BAD_CONTROLS)
def test_a_bad_control_declaration_names_its_field_path(control, fragment):
    with pytest.raises(PipelineBuildError) as caught:
        pipeline("ho-bad", ho_a | ho_b | ho_c | ho_d, control=control)
    assert "control" in str(caught.value)
    assert fragment in str(caught.value)


def test_a_backward_edge_is_refused_before_the_pipeline_exists():
    with pytest.raises(PipelineBuildError, match="forward-only"):
        pipeline("ho-back", ho_a | ho_b | ho_c, control={"edges": {"ho.c": ["ho.a"]}})


def test_an_ambiguous_task_name_must_be_given_as_a_seq():
    with pytest.raises(PipelineBuildError) as caught:
        pipeline("ho-ambiguous", ho_a | ho_c | ho_c | ho_d, control={"edges": {"ho.a": ["ho.c"]}})
    assert "appears at seqs [1, 2]" in str(caught.value)
    # the numeric seq disambiguates, and both spellings resolve to the same canonical plan
    assert build_control({"edges": {"ho.a": [2]}}, ["ho.a", "ho.c", "ho.c"]).edges == {0: (2,)}
    assert build_control({"edges": {0: [2, 1]}}, ["ho.a", "ho.c", "ho.c"]).edges == {0: (1, 2)}


def test_a_plan_is_resolved_not_declaration_shaped():
    plan = build_control(
        {"edges": {"ho.c": ["ho.d", "end"], "ho.a": [3]}}, ["ho.a", "ho.b", "ho.c", "ho.d"]
    )
    assert plan.fingerprint() == {"edges": {"0": [3], "2": [3, "end"]}}
    assert plan.describe() == {"edges": {"ho.a": ["ho.d"], "ho.c": ["ho.d", "end"]}}
    assert plan.allows(2, "ho.d") == 3
    assert plan.allows(2, "end") is None
    with pytest.raises(FatalError, match="declares no edge from it"):
        plan.allows(1, "ho.d")  # ho.b declares no edge at all
    with pytest.raises(FatalError, match="is the last task"):
        ControlPlan(task_names=("x", "y"), edges={0: (1,)}).allows(1, None)
    with pytest.raises(FatalError, match="unknown destination task"):
        plan.allows(2, "ho.nope")


def test_validate_and_run_agree_on_a_control_config(tmp_path):
    good = {
        "pipeline": {
            "name": "qa",
            "control": {"edges": {"judge": ["report", "end"]}},
            "tasks": [
                {"use": "pyattacker.tasks:echo", "name": "ask"},
                {"use": "pyattacker.tasks:echo", "name": "judge"},
                {"use": "pyattacker.tasks:echo", "name": "report"},
            ],
        },
        "source": {"kind": "range", "n": 1},
    }
    good_path = tmp_path / "good.json"
    good_path.write_text(json.dumps(good), encoding="utf-8")
    assert load_spec(good_path).template.control is not None
    assert load_spec(good_path).describe()["pipeline"]["control"] == {"edges": {"judge": ["report", "end"]}}
    assert cli_main(["validate", "-c", str(good_path)]) == 0

    bad = json.loads(json.dumps(good))
    bad["pipeline"]["control"] = {"edges": {"judge": ["ask"]}}
    bad_path = tmp_path / "bad.json"
    bad_path.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ConfigError) as caught:
        load_spec(bad_path)
    assert "pipeline.control.edges['judge']" in str(caught.value)
    assert cli_main(["validate", "-c", str(bad_path)]) == 2

    unknown = json.loads(json.dumps(good))
    unknown["pipeline"]["control"] = {"edges": {"judge": ["report"]}, "mode": "handoff"}
    unknown_path = tmp_path / "unknown.json"
    unknown_path.write_text(json.dumps(unknown), encoding="utf-8")
    assert cli_main(["validate", "-c", str(unknown_path)]) == 2


def test_handoff_annotations_chain_through_the_next_task():
    @task("ann.a")
    def ann_a(value) -> int:
        return 1

    @task("ann.maybe")
    def ann_maybe(value: int) -> Handoff | str:
        return "x"

    @task("ann.needs_str")
    def ann_needs_str(value: str) -> str:
        return value

    @task("ann.gives_list")
    def ann_gives_list(value: int) -> Handoff | list:
        return []

    @task("ann.only")
    def ann_only(value: int) -> Handoff:
        return Handoff.end(value)

    assert pipeline("ann-ok", ann_a | ann_maybe | ann_needs_str).n_tasks == 3
    assert pipeline("ann-bare", ann_a | ann_only | ann_needs_str).n_tasks == 3
    with pytest.raises(PipelineBuildError):
        pipeline("ann-bad", ann_a | ann_gives_list | ann_needs_str)


# ==================================================================== runtime misuse
def test_an_undeclared_edge_is_fatal_and_never_retried(tmp_path):
    rogue = replace(_rogue("ho.d"), retry=replace(_rogue("ho.d").retry, max_attempts=5, retry_unknown=True))
    template = pipeline("ho-rogue", ho_a | rogue | ho_d, control={"edges": {"ho.a": ["ho.d"]}})
    with _runner(str(tmp_path / "rogue.db")) as runner:
        report = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert CALLS == {"a": 1, "rogue": 1}  # retry_unknown=True changed nothing
        row = store.get_pipeline(pid)
        assert row.error_type == "FatalError"
        assert "declares no edge from it" in (row.error_message or "")
        attempts = [a for a in store.attempts(pipeline_id=pid) if a.task_name == "ho.rogue"]
        assert [a.outcome for a in attempts] == ["failed"]
        assert attempts[0].error_class == "fatal"
        assert store.handoffs() == []


def test_a_handoff_without_a_control_block_is_a_configuration_error(tmp_path):
    template = pipeline("ho-nocontrol", ho_a | _rogue("ho.d") | ho_d)
    with _runner(str(tmp_path / "nocontrol.db")) as runner:
        runner.run(template.map([SEED]))
        store = runner.store
        row = store.get_pipeline(store.pipelines()[-1].pipeline_id)
        assert row.error_type == "FatalError"
        assert "declares no control block" in (row.error_message or "")
        assert CALLS == {"a": 1, "rogue": 1}


def test_a_timed_out_attempt_never_reaches_the_handoff(tmp_path):
    """Cancellation and timeout are unchanged: the directive is a return, so a killed attempt has none."""

    async def _sleeper(value: Any, ctx: Any) -> Handoff:
        _note("sleeper")
        await ctx.clock.sleep(1.0)
        return Handoff.end(value, reason="too late")

    sleeper = replace(build_task_spec(_sleeper, name="ho.sleeper"), timeout_s=0.01)
    template = pipeline("ho-timeout", ho_a | sleeper | ho_c, control={"edges": {"ho.sleeper": ["end"]}})
    with _runner(str(tmp_path / "timeout.db")) as runner:
        report = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert store.handoffs(pipeline_id=pid) == []
        assert store.attempts(pipeline_id=pid)[-1].outcome == "timeout"
        assert "pipeline.handoff" not in _kinds(store, pid)


def test_strict_leases_turn_a_leak_into_a_failure_not_a_handoff(tmp_path):
    pool = Pool("ho-pool", [Resource.create("k", id="ho-1", capacity=1)])

    async def _leaky(value: Any, ctx: Any) -> Handoff:
        await ctx.acquire_lease("ho-pool")  # deliberately not returned
        return Handoff.end(value, reason="leaked")

    leaky = replace(build_task_spec(_leaky, name="ho.leaky"), resource="ho-pool")
    template = pipeline("ho-leak", ho_a | leaky | ho_c, control={"edges": {"ho.leaky": ["end"]}})
    with _runner(":memory:", pools=[pool], strict_leases=True) as runner:
        report = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert store.get_pipeline(pid).error_type == "LeaseLeakError"
        assert store.handoffs() == []
        assert pool.stats().active == 0  # reclaim is unchanged, and it happens before the record


def test_a_handoff_leaves_no_lease_held(tmp_path):
    pool = Pool("ho-pool", [Resource.create("k", id="ho-1", capacity=1)])

    async def _polite(value: Any, ctx: Any) -> Handoff:
        async with ctx.acquire("ho-pool") as lease:
            lease.report(ok=True)
        return Handoff.end(value, reason="all done")

    polite = replace(build_task_spec(_polite, name="ho.polite"), resource="ho-pool")
    template = pipeline("ho-lease", ho_a | polite | ho_c, control={"edges": {"ho.polite": ["end"]}})
    with _runner(":memory:", pools=[pool], strict_leases=True) as runner:
        report = runner.run(template.map([SEED]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert report.leases_leaked == 0
        assert pool.stats().active == 0


@pytest.mark.parametrize("mode", ["raise", "collect"])
def test_fanout_rejects_a_branch_that_hands_off(tmp_path, mode):
    """A directive can never come from one of N concurrent branches, and never enters a payload."""

    def _wants_end(value: Any, ctx: Any) -> Handoff:
        return Handoff.end(value, reason="branch thinks it is done")

    branch = build_task_spec(_wants_end, name="ho.wants_end")
    group = fanout(branch, ho_d, name="ho.group", on_error=mode)
    template = pipeline("ho-fanout", ho_a | group | ho_c, control={"edges": {"ho.a": ["ho.c"]}})
    with _runner(str(tmp_path / f"fanout-{mode}.db")) as runner:
        report = runner.run(template.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert store.get_pipeline(pid).error_type == "FatalError"
        assert "returned a Handoff" in (store.get_pipeline(pid).error_message or "")
        assert store.handoffs() == []
        assert "fanout.handoff_rejected" in _kinds(store, pid)


# ========================================================================= surfaces
def test_the_record_exposes_the_handoff_everywhere(tmp_path):
    db = str(tmp_path / "surface.db")
    with _runner(db) as runner:
        report = runner.run(SKIP_TEMPLATE.map([SEED]))
        store = runner.store
        pid = store.pipelines()[-1].pipeline_id

        # the run report counts them, and both summaries say so
        assert report.stats["handoffs_total"] == 1
        assert "handoffs=1" in report.summary()
        assert "handoffs=1" in render_snapshot(read_snapshot(store, report.run_id))

        # the pipeline row carries the ledger, and the other kinds are unaffected
        row = next(row for row in iter_rows(store, kind="pipelines") if row["pipeline_id"] == pid)
        assert [hop["from_task"] for hop in row["handoffs"]] == ["ho.judge"]
        assert row["handoffs"][0]["to_task"] == "ho.d"
        assert row["handoffs"][0]["entry_reused"] is False
        for kind in ("tasks", "attempts", "events", "artifacts"):
            assert list(iter_rows(store, kind=kind)), kind

        # a merged report recomputes the counter from the merged rows' owner stats
        merged = merge_reports([store])
        assert merged.stats()["handoffs_total"] == 1
        assert "handoffs=1" in merged.summary()

        # and the HTTP view names the count next to the cursor it explains
        server = StatsServer(store, port=0)
        try:
            _, body = server.payload("/pipelines", {})
            entry = next(item for item in body["rows"] if item["pipeline_id"] == pid)
            assert entry["handoffs"] == 1
            assert (entry["n_tasks_done"], entry["n_tasks_total"]) == (5, 5)
        finally:
            server.stop()


def test_the_ledger_survives_a_reopened_store(tmp_path):
    db = str(tmp_path / "reopen.db")
    with _runner(db) as runner:
        runner.run(SKIP_TEMPLATE.map([SEED]))
        pid = runner.store.pipelines()[-1].pipeline_id
        hop = runner.store.handoffs(pipeline_id=pid)[0]

    reopened = SqliteStore(db)
    try:
        again = reopened.handoffs(pipeline_id=pid)[0]
        assert (again.handoff_id, again.from_seq, again.to_seq) == (hop.handoff_id, 2, 4)
        assert again.entry_artifact_id == hop.entry_artifact_id
        assert reopened.get_artifact(pid, again.entry_seq).available
        # the skipped slot holds nothing at all: a payload never lands in a task slot
        assert reopened.get_artifact(pid, 3) is None
    finally:
        reopened.close()
