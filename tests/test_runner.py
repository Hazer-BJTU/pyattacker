"""Runner behaviour: task-level checkpoints, resume semantics, retry decisions, error classification, concurrency and shutdown.

The centrepiece here is the ★ resume test: it proves that artifacts already produced are
reused and that earlier tasks are not re-sent.
"""

from __future__ import annotations

import asyncio
import json

from helpers import run

from pyattacker import (
    FatalError,
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    pipeline,
    task,
)
from pyattacker.tasks import flaky

# ------------------------------------------------------------------ counters
CALLS: dict[str, int] = {"fetch": 0, "ask": 0}
BEHAVIOR: dict[str, bool] = {"ask_fails": True}


@task("r.fetch")
def r_fetch(seed, ctx):
    CALLS["fetch"] += 1
    return {"q": seed["q"]}


@task("r.ask", retry=Retrying(max_attempts=1))
def r_ask(row, ctx):
    CALLS["ask"] += 1
    if BEHAVIOR["ask_fails"]:
        raise RetryableError("model unavailable", error_class="upstream")
    return {"a": row["q"].upper()}


TEMPLATE = pipeline("resume-demo", r_fetch | r_ask)

LIVE: dict[str, int] = {"now": 0, "peak": 0}


@task("c.slow")
async def c_slow(seed, ctx):
    LIVE["now"] += 1
    LIVE["peak"] = max(LIVE["peak"], LIVE["now"])
    await asyncio.sleep(0.005)
    LIVE["now"] -= 1
    return seed


@task("f.always", retry=Retrying(max_attempts=3, base=0.001, cap=0.002))
def f_always(seed, ctx):
    raise RetryableError("always fails", error_class="upstream")


@task("f.fatal", retry=Retrying(max_attempts=5, base=0.001))
def f_fatal(seed, ctx):
    raise FatalError("bad request")


def _seeds(n: int = 3):
    return [{"q": f"q{i}"} for i in range(n)]


# ------------------------------------------------------------------ basics
def test_happy_path_records_everything():
    runner = Runner(store=":memory:", concurrency=3, handle_signals=False)
    report = runner.run(pipeline("ok", flaky(0)).map(_seeds(4)))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 4}
    assert report.stats["attempts_total"] == 4
    assert report.leases_leaked == 0

    rows = list(runner.store.export_rows())
    assert len(rows) == 4
    assert all(row["state"] == "succeeded" for row in rows)
    assert all(row["tasks"][0]["attempts_used"] == 1 for row in rows)
    # task-level checkpoint + final artifact marker (entry 0 is the seed artifact, seq=-1)
    assert all(row["n_tasks_done"] == row["n_tasks_total"] == 1 for row in rows)
    assert all(sum(1 for a in row["artifacts"] if a["is_final"]) == 1 for row in rows)
    assert all(row["artifacts"][-1]["is_final"] is True for row in rows)
    assert all(row["artifacts"][-1]["payload"]["attempts"] == 1 for row in rows)
    assert all(a["task"] == "__seed__" for row in rows for a in row["artifacts"][:1])


def test_retry_reexecutes_and_records_decision():
    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("retry", flaky(2)).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    attempts = runner.store.attempts()
    assert [a.outcome for a in attempts] == ["failed", "failed", "succeeded"]
    assert [a.attempt_no for a in attempts] == [1, 2, 3]
    assert attempts[0].decision["retry"] is True
    assert attempts[0].decision["error_class"] == "retryable"
    assert attempts[1].decision["retry"] is True
    assert attempts[2].decision["retry"] is False
    assert attempts[2].decision["reason"] == "ok"
    assert attempts[0].retry_delay_s is not None

    # the pipeline-level counter (distinct from the per-task attempts_used) must reflect
    # every attempt actually made, not just the exhausted-attempts failure path
    pid = next(iter(runner.store.pipelines())).pipeline_id
    assert runner.store.get_pipeline(pid).attempts_total == 3


def test_attempts_total_accumulates_across_tasks():
    runner = Runner(store=":memory:", handle_signals=False)
    spec = pipeline(
        "multi",
        r_fetch | flaky(1),
    )
    runner.run(spec.map([{"q": "hi"}]))
    pid = next(iter(runner.store.pipelines())).pipeline_id
    # 1 attempt for r_fetch + 2 attempts (1 failure, 1 success) for the flaky task
    assert runner.store.get_pipeline(pid).attempts_total == 3


def test_attempts_exhausted_marks_pipeline_failed():
    runner = Runner(store=":memory:", handle_signals=False)
    spec = pipeline("boom", flaky(5, error="timeout").with_overrides(
        retry=Retrying(max_attempts=2, base=0.001)
    ))
    report = runner.run(spec.map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    attempts = runner.store.attempts()
    assert len(attempts) == 2
    assert attempts[-1].decision["reason"] == "attempts_exhausted"
    assert attempts[-1].decision["delay_s"] == 0.0  # always present, per the documented schema
    assert attempts[-1].error_class == "timeout"
    row = next(iter(runner.store.export_rows()))
    assert row["error_type"] == "TimeoutError"
    assert row["tasks"][0]["state"] == "failed"

    pid = next(iter(runner.store.pipelines())).pipeline_id
    assert runner.store.get_pipeline(pid).attempts_total == 2


def test_finalize_pools_persists_a_redacted_resource_spec():
    pool = Pool("apis", [Resource.create("llm", id="api-1", options={"api_key": "sk-secret123"})])
    runner = Runner(store=":memory:", pools=[pool], handle_signals=False)
    runner.run(pipeline("noop", r_fetch).map([{"q": "hi"}]))

    rows = runner.store.resources(pool="apis")
    assert len(rows) == 1
    assert rows[0]["spec"]["id"] == "api-1"
    # a real spec is persisted (not the {} placeholder) and secrets are redacted
    assert rows[0]["spec"]["options"]["api_key"] == "***t123"


def test_fatal_error_is_not_retried():
    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("fatal", f_fatal).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    assert len(runner.store.attempts()) == 1
    assert runner.store.attempts()[0].decision["reason"] == "policy_declined"
    assert runner.store.attempts()[0].decision["delay_s"] == 0.0  # always present, per the documented schema
    assert runner.store.attempts()[0].error_class == "fatal"


def test_total_budget_exhaustion_still_reports_delay_s():
    runner = Runner(store=":memory:", handle_signals=False)
    spec = pipeline("budget", flaky(5, error="timeout").with_overrides(
        retry=Retrying(max_attempts=5, base=1.0, jitter="none", max_total_s=0.0)
    ))
    report = runner.run(spec.map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    attempts = runner.store.attempts()
    assert len(attempts) == 1  # the very first failure already exceeds a zero total budget
    assert attempts[0].decision["reason"] == "total_budget"
    assert attempts[0].decision["delay_s"] == 0.0  # always present, per the documented schema


# ------------------------------------------------------------- ★ resume semantics
def test_resume_continues_from_task_checkpoint(tmp_path):
    CALLS.update(fetch=0, ask=0)
    BEHAVIOR["ask_fails"] = True
    db = str(tmp_path / "runs.db")
    seeds = _seeds(3)
    specs = list(TEMPLATE.map(seeds))
    runner = Runner(store=db, concurrency=2, handle_signals=False)
    try:
        first = runner.run(TEMPLATE.map(seeds))
        assert first.stats["pipelines"]["by_state"] == {"failed": 3}
        assert CALLS == {"fetch": 3, "ask": 3}
        record = runner.store.get_pipeline(specs[0].pipeline_id)
        assert record.state == "failed"
        assert record.n_tasks_done == 1  # ★ the first task's output is already checkpointed
        assert record.failed_task == "r.ask"

        BEHAVIOR["ask_fails"] = False
        second = runner.run(TEMPLATE.map(seeds), resume=True)

        assert second.stats["pipelines"]["by_state"] == {"succeeded": 3}
        assert CALLS["fetch"] == 3  # ★ fetch was not re-run (its artifact was reused)
        assert CALLS["ask"] == 6  # only the failed task was re-run
        resumed = [e for e in runner.store.events(limit=500) if e.kind == "pipeline.resumed"]
        assert resumed and resumed[0].data["from_seq"] == 1
        assert runner.store.get_pipeline(specs[0].pipeline_id).state == "succeeded"

        # Third run: everything is already done → skipped outright
        third = runner.run(TEMPLATE.map(seeds), resume=True)
        assert third.skipped == 3
        assert CALLS == {"fetch": 3, "ask": 6}
    finally:
        runner.close()


def test_resume_reuses_seed_artifact_without_dataset(tmp_path):
    CALLS.update(fetch=0, ask=0)
    BEHAVIOR["ask_fails"] = True
    db = str(tmp_path / "runs.db")
    runner = Runner(store=db, handle_signals=False)
    try:
        specs = list(TEMPLATE.map(_seeds(2)))
        runner.run(TEMPLATE.map(_seeds(2)))
        # Resume does not depend on the original dataset: here the seeds are deliberately the
        # same, but map runs again
        BEHAVIOR["ask_fails"] = False
        runner.run(TEMPLATE.map([{"q": "q0"}, {"q": "q1"}]), resume=True)
        assert CALLS["fetch"] == 2  # the seed artifact is reused, so fetch still ran only once
        assert len(specs) == 2
    finally:
        runner.close()


def test_summary_journal_cannot_reuse_checkpoint(tmp_path):
    CALLS.update(fetch=0, ask=0)
    BEHAVIOR["ask_fails"] = True
    db = str(tmp_path / "runs.db")
    runner = Runner(store=db, journal="summary", handle_signals=False)
    try:
        runner.run(TEMPLATE.map(_seeds(2)))
        assert runner.store.get_artifact(
            runner.store.pipelines()[0].pipeline_id, 0
        ).payload is None  # summary mode does not store payloads

        BEHAVIOR["ask_fails"] = False
        report = runner.run(TEMPLATE.map(_seeds(2)), resume=True)
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 2}
        assert CALLS["fetch"] == 4  # ★ intermediate artifacts cannot be reused → the whole chain re-runs
        kinds = [e.kind for e in runner.store.events(limit=500)]
        assert "pipeline.checkpoint_missing" in kinds
    finally:
        runner.close()


# --------------------------------------------------------------- scheduling behaviour
def test_concurrency_is_bounded_and_actually_parallel():
    LIVE.update(now=0, peak=0)
    runner = Runner(store=":memory:", concurrency=4, handle_signals=False)
    report = runner.run(pipeline("c", c_slow).map(_seeds(20)))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 20}
    assert LIVE["peak"] <= 4
    assert LIVE["peak"] >= 2  # genuinely concurrent, not serial
    assert LIVE["now"] == 0


def test_stop_after_failures_interrupts_run():
    """The failure budget must stop admission promptly instead of running the whole dataset.

    Uses one attempt per pipeline (no retries, no parking) so the admissions that happen
    before the budget is spent are bounded by the queue size, not by timing.
    """
    instant_fail = f_always.with_overrides(retry=Retrying(max_attempts=1))
    runner = Runner(store=":memory:", concurrency=1, stop_after_failures=1, handle_signals=False)
    report = runner.run(pipeline("f", instant_fail).map(_seeds(200)))

    assert report.status == "interrupted"
    assert report.stop_reason == "stop_after_failures"
    assert report.stats["pipelines"]["total"] < 10  # 200 seeds were available; only a handful were admitted
    assert report.stats["pipelines"]["by_state"]["failed"] >= 1


def test_unknown_pool_fails_fast_without_running_task():
    from pyattacker import task as task_decorator

    @task_decorator("needs.pool", resource="missing-pool")
    def needs_pool(seed, ctx):
        raise AssertionError("must not be executed")

    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("np", needs_pool).map([{"i": 0}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    row = next(iter(runner.store.export_rows()))
    assert row["error_type"] == "ConfigError"
    assert len(runner.store.attempts()) == 0  # not even an attempt should exist


# --------------------------------------------------------------- internal errors
def test_worker_records_internal_error_and_keeps_the_run_going():
    """A framework-level surprise (not a task exception) must be recorded, not swallow the worker.

    Regression: ``_worker``'s except-block read ``state`` to fill in ``pipeline_id`` on the
    ``runner.internal_error`` event, but when the very first call inside the try (opening the
    pipeline) is what raises, ``state`` was never assigned — the ``getattr(state, ...)`` then
    raised ``UnboundLocalError`` *inside the exception handler itself*. That second exception
    escaped ``_worker`` entirely; ``asyncio.gather(..., return_exceptions=True)`` swallowed it
    silently, the worker task simply died, and with ``concurrency=1`` the run hung forever
    waiting for a queue nothing was draining anymore.
    """

    async def _case():
        runner = Runner(store=":memory:", concurrency=1, handle_signals=False)
        calls = {"n": 0}
        real_open = runner._open_pipeline

        def flaky_open(spec, run_id):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("framework surprise")
            return real_open(spec, run_id)

        runner._open_pipeline = flaky_open
        spec = pipeline("internal-error", flaky(0))
        report = await asyncio.wait_for(
            runner.run_async(spec.map([{"i": 0}, {"i": 1}])), timeout=5.0
        )

        # the second pipeline still ran to completion: one worker dying does not take the run down
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        events = runner.store.events(limit=50)
        internal = [e for e in events if e.kind == "runner.internal_error"]
        assert len(internal) == 1
        assert internal[0].pipeline_id is None  # the pipeline never even got a state object
        assert internal[0].data["error"] == "RuntimeError: framework surprise"
        assert "Traceback" in internal[0].data["traceback"]

    run(_case())


def test_internal_error_after_state_exists_still_reports_its_pipeline_id():
    """Once ``state`` is bound, the event should carry the real ``pipeline_id`` (not None)."""

    async def _case():
        runner = Runner(store=":memory:", concurrency=1, handle_signals=False)

        async def boom_drive(state):
            raise RuntimeError("drive exploded")

        runner._drive = boom_drive
        spec = pipeline("internal-error-2", flaky(0))
        report = await asyncio.wait_for(runner.run_async(spec.map([{"i": 0}])), timeout=5.0)

        assert report.stats["pipelines"]["by_state"] == {"running": 1}  # opened, but never reached a terminal state
        events = runner.store.events(limit=50)
        internal = next(e for e in events if e.kind == "runner.internal_error")
        assert internal.pipeline_id is not None
        assert internal.data["error"] == "RuntimeError: drive exploded"

    run(_case())


def test_events_form_a_structured_per_pipeline_log():
    runner = Runner(store=":memory:", handle_signals=False)
    spec = next(iter(pipeline("log", flaky(1)).map([{"i": 0}])))
    runner.run([spec])

    events = runner.store.events(pipeline_id=spec.pipeline_id, limit=100)
    kinds = [e.kind for e in events]
    assert "pipeline.succeeded" in kinds
    assert "task.failed" in kinds
    assert "task.retry_scheduled" in kinds
    assert "task.succeeded" in kinds
    assert all(e.pipeline_id == spec.pipeline_id for e in events)
    failed = next(e for e in events if e.kind == "task.failed")
    assert failed.data["decision"]["retry"] is True
    assert failed.task_run_id == f"{spec.pipeline_id}:0"


def test_report_export_jsonl(tmp_path):
    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("e", flaky(0)).map(_seeds(3)))
    out = tmp_path / "out.jsonl"
    count = report.export_jsonl(str(out))

    assert count == 3
    lines = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 3
    assert {line["name"] for line in lines} == {"e"}
    assert all(line["state"] == "succeeded" for line in lines)
    assert all(line["artifacts"][-1]["payload"]["attempts"] == 1 for line in lines)
    assert all(line["tasks"][0]["name"] == "mock.flaky" for line in lines)

    run_scoped = tmp_path / "run.jsonl"
    assert report.export_jsonl(str(run_scoped), scope="run") == 3


def test_stats_snapshot_is_available_before_and_after_run():
    runner = Runner(store=":memory:", handle_signals=False)
    empty = runner.stats()
    assert empty["pipelines"]["total"] == 0
    assert empty["in_flight_pipelines"] == 0

    runner.run(pipeline("s", flaky(0)).map(_seeds(2)))
    after = runner.stats()
    assert after["pipelines"]["total"] == 2
    assert after["elapsed_s"] is not None


def test_cli_run_persists_to_configured_store_and_resume_skips(tmp_path, capsys):
    """Regression: run.store must actually be used (it used to be filtered out by a whitelist, so runs were written to the in-memory store and resume always re-ran everything)."""
    from pyattacker.cli import main

    db = tmp_path / "runs" / "cli.db"
    cfg = tmp_path / "pyattacker.yaml"
    cfg.write_text(
        f"""
run: {{store: {db}, concurrency: 4, label: cli-demo}}
pipeline:
  name: cli_demo
  tasks:
    - {{use: pyattacker.tasks:flaky, kwargs: {{fail_times: 0}}}}
source: {{kind: range, n: 5}}
""",
        encoding="utf-8",
    )

    assert main(["run", "-c", str(cfg)]) == 0
    first = capsys.readouterr().out
    assert db.exists() and db.stat().st_size > 0
    assert "succeeded=5" in first
    assert "attempts: total=5" in first

    assert main(["resume", "-c", str(cfg)]) == 0
    second = capsys.readouterr().out
    assert "skipped=5" in second
    assert "attempts: total=0" in second

    assert main(["report", str(db)]) == 0
    report = capsys.readouterr().out
    assert "succeeded=5" in report

    out = tmp_path / "out.jsonl"
    assert main(["export", str(db), str(out)]) == 0
    assert len(out.read_text(encoding="utf-8").strip().splitlines()) == 5
    assert main(["report", str(tmp_path / "missing.db")]) == 2
