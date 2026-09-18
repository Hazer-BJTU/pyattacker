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
    monkeypatch.setattr(inner, "_visit_save", original)
    assert run(store, spec, resume=True).stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert store.visit_state(spec.pipeline_id)["counters"] == {"0": 1, "1": 1}


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
report = Runner(store=sys.argv[1], handle_signals=False, write_behind=True).run([spec])
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


def test_backward_guide_examples_execute():
    import re
    from pathlib import Path

    guide = Path(__file__).resolve().parents[1] / "docs" / "backward.md"
    for snippet in re.findall(r"```python\n(.*?)```", guide.read_text(), re.DOTALL):
        exec(compile(snippet, str(guide), "exec"), {})


@pytest.mark.parametrize("strict", [False, True])
def test_backward_transfer_obeys_lease_reclamation(store, strict):
    from pyattacker import Pool, Resource

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
