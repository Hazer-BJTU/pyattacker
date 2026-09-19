"""Application-reported metrics are durable latest values, independent of task outcomes."""

from __future__ import annotations

import json
import urllib.request

import pytest

from pyattacker import Handoff, Runner, SqliteStore, pipeline, task
from pyattacker.errors import ConfigError, StoreFeatureUnsupported
from pyattacker.reported_metrics import read_reported_metrics, report_metric
from pyattacker.server import StatsServer


@task("reported.answer")
def answer(seed, ctx):
    ctx.report_metric("phase", "scored", display="text")
    return {"correct": seed["correct"]}


def test_live_accuracy_is_application_owned_and_visible_through_read_only_server(tmp_path):
    db = str(tmp_path / "metrics.db")
    scores: dict[str, bool] = {}
    def finished(runner, record, artifact):
        assert runner.store.get_pipeline(record.pipeline_id).state == record.state
        if artifact is not None:
            assert artifact.is_final
            scores[record.pipeline_id] = runner.registry.load(artifact.encoded())["correct"]
        else:
            scores.pop(record.pipeline_id, None)
        runner.report_metric("evaluated", len(scores))
        runner.report_metric("accuracy", sum(scores.values()) / len(scores), display="percent")

    with Runner(store=db, on_pipeline_finished=finished, handle_signals=False) as runner, StatsServer(db, port=0) as server:
        spec = pipeline("reported-eval", answer)
        first = runner.run(spec.map([{"correct": True}, {"correct": False}]))
        run_id = first.run_id
        status, body = server.payload("/metrics", {"run_id": [run_id]})
        assert status == 200
        assert {row["name"]: row["value"] for row in body["rows"]} == {
            "accuracy": 0.5, "evaluated": 2,
        }
        with urllib.request.urlopen(f"{server.url}/metrics?run_id={run_id}") as response:
            assert json.load(response)["rows"][0]["name"] == "accuracy"
        assert runner.stats()["reported_metrics"]
        pipeline_id = next(iter(scores))
        scoped = server.payload("/metrics", {"run_id": [run_id], "pipeline_id": [pipeline_id]})[1]
        assert [(r["name"], r["value"]) for r in scoped["rows"]] == [("phase", "scored")]
        selected = server.payload("/pipelines", {"run_id": [run_id]})[1]["rows"]
        assert all(row["reported_metrics"][0]["value"] == "scored" for row in selected)

        runner.config.retry_succeeded = True
        second = runner.run(spec.map([{"correct": True}, {"correct": False}]))
        assert second.run_id != run_id
        assert len(scores) == 2  # application deduplicates by pipeline ID
        assert server.payload("/metrics", {"run_id": [run_id]})[1]["rows"][0]["value"] == 0.5

    readonly = SqliteStore(db, read_only=True)
    try:
        assert {r.name: r.value for r in read_reported_metrics(readonly, run_id=run_id)} == {
            "accuracy": 0.5, "evaluated": 2,
        }
    finally:
        readonly.close()


def test_metric_validation_and_optional_store_capability():
    class LegacyStore:
        pass

    with pytest.raises(StoreFeatureUnsupported):
        report_metric(LegacyStore(), "run", "x", 1)
    assert read_reported_metrics(LegacyStore(), run_id="run") == []
    for value in (float("nan"), float("inf"), [], None):
        with pytest.raises(ConfigError):
            report_metric(LegacyStore(), "run", "x", value)
    with pytest.raises(ConfigError):
        report_metric(LegacyStore(), "run", "x", "bad", display="percent")


def test_callback_failure_does_not_change_pipeline_outcome():
    def broken(runner, record, artifact):
        raise ValueError("monitor unavailable")

    with Runner(on_pipeline_finished=broken, handle_signals=False) as runner:
        report = runner.run(pipeline("callback-failure", answer).map([{"correct": True}]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert runner.stats()["counters"]["monitor_callback_errors"] == 1


def test_completion_observer_sees_failure_and_terminal_handoff():
    seen = []

    @task("reported.fail")
    def fail(seed):
        raise ValueError("bad row")

    @task("reported.gate")
    def gate(seed):
        return Handoff.end({"correct": True})

    @task("reported.unused")
    def unused(seed):
        raise AssertionError("handoff should skip this")

    with Runner(on_pipeline_finished=lambda runner, record, artifact: seen.append((record.state, artifact)),
                handle_signals=False) as runner:
        runner.run(pipeline("reported-fail", fail).map([{}]))
        runner.run(pipeline("reported-end", gate | unused,
                            control={"edges": {"reported.gate": ["end"]}}).map([{}]))
        assert [state for state, _ in seen] == ["failed", "succeeded"]
        assert seen[0][1] is None
        assert seen[1][1].is_final
