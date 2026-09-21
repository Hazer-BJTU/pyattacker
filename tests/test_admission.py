"""Retry backpressure bounds source consumption without starving continuations."""

import asyncio
import json

import pytest

from pyattacker import Retrying, RunConfig, Runner, pipeline, task
from pyattacker.cli import main
from pyattacker.declarative import _validate_run
from pyattacker.errors import ConfigError, RetryableError
from pyattacker.tasks import echo


async def until(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), 3)


@pytest.mark.parametrize("limit", [None, 1, 7])
@pytest.mark.parametrize("ending", ["stop", "cancel", "budget"])
def test_long_backoff_bounds_source_and_can_stop(tmp_path, limit, ending):
    async def case():
        @task("park", retry=Retrying(max_attempts=2, base=0))
        def park(seed):
            raise RetryableError("wait", retry_after=60)

        consumed = []
        template = pipeline("bounded", park)
        with Runner(
            store=str(tmp_path / "facts.db"),
            concurrency=1,
            max_admitted=limit,
            grace_s=0,
            handle_signals=False,
        ) as runner:
            cap = limit or 4

            def source():
                for i in range(10000):
                    consumed.append(i)
                    yield template.bind(i)

            running = asyncio.create_task(runner.run_async(source()))
            try:
                await until(lambda: len(runner._delays) == cap)
                await asyncio.sleep(0.01)
                assert len(consumed) == cap
                snapshot = runner.stats()
                assert snapshot["admitted_pipelines"] == snapshot["max_admitted"] == cap
                if ending == "cancel":
                    running.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(running, 3)
                else:
                    if ending == "stop":
                        runner.stop()
                    else:
                        # Expire only once capacity is full, independent of disk speed.
                        runner.config.stop_after_s = 0.001
                    report = await asyncio.wait_for(running, 3)
                    assert report.status == "interrupted"
                    assert report.stop_reason == ("user" if ending == "stop" else "stop_after_s")
                assert len(consumed) == cap
                assert not runner._delays
                assert all(row.state == "interrupted" for row in runner.store.pipelines())
            finally:
                if not running.done():
                    running.cancel()
                await asyncio.gather(running, return_exceptions=True)

    asyncio.run(case())


@pytest.mark.parametrize("limit", [1, 3, 8])
def test_retries_complete_at_capacity_and_runner_can_be_reused(limit):
    @task("recover", retry=Retrying(max_attempts=2, base=0.001, cap=0.001))
    def recover(seed, ctx):
        if ctx.attempt == 1:
            raise RetryableError("retry")
        return seed

    template = pipeline("recover", recover)
    with Runner(concurrency=2, max_admitted=limit, handle_signals=False) as runner:

        def source():
            for i in range(50):
                assert runner._counters["pipelines_admitted"] - runner._counters["pipelines_done"] < limit
                yield template.bind(i)

        first = asyncio.run(asyncio.wait_for(runner.run_async(source()), 5))
        assert first.stats["pipelines"]["by_state"] == {"succeeded": 50}
        assert first.stats["attempts_total"] == 100
        assert runner.store.get_run(first.run_id).config["max_admitted"] == limit
        second = runner.run(source())
        assert second.skipped == 50
        assert runner.stats()["admitted_pipelines"] == 0


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2"])
def test_invalid_admission_limit(value):
    with pytest.raises(ConfigError, match="max_admitted"):
        RunConfig(max_admitted=value)
    with pytest.raises(ConfigError, match="max_admitted"):
        _validate_run({"max_admitted": value})


def test_cli_admission_override_is_persisted(tmp_path):
    path = tmp_path / "run.json"
    db = tmp_path / "run.db"
    path.write_text(
        json.dumps(
            {
                "pipeline": {"name": "p", "tasks": [{"use": "echo"}]},
                "source": {"kind": "range", "n": 2},
                "run": {"store": str(db), "max_admitted": 8},
            }
        )
    )
    assert main(["run", "-c", str(path), "--max-admitted", "1", "--no-signals"]) == 0
    from pyattacker import SqliteStore

    store = SqliteStore(str(db), read_only=True)
    try:
        assert store.get_run(store.pipelines()[0].run_id).config["max_admitted"] == 1
    finally:
        store.close()


def test_failure_budget_while_admission_is_full():
    @task("fail", retry=Retrying(max_attempts=1))
    def fail(seed):
        raise ValueError("failed")

    with Runner(concurrency=1, max_admitted=1, stop_after_failures=1, handle_signals=False) as runner:
        report = runner.run(pipeline("f", fail).map(range(100)))
        assert report.stop_reason == "stop_after_failures"
        assert runner._counters["pipelines_admitted"] == 1


def test_source_error_after_capacity_is_freed():
    def source():
        yield pipeline("first", echo).bind(1)
        raise ValueError("source failed")

    with Runner(max_admitted=1, handle_signals=False) as runner:
        report = runner.run(source())
        assert report.stop_reason == "producer_error"
        assert report.stats["producer_error"] == "ValueError: source failed"


def test_worker_crash_releases_admission_waiter():
    from pyattacker.errors import WorkerCrashed

    class WorkerDeath(BaseException):
        pass

    async def case():
        with Runner(concurrency=1, max_admitted=1, handle_signals=False) as runner:

            async def die(state):
                await asyncio.sleep(0)
                raise WorkerDeath("worker died")

            runner._drive = die
            with pytest.raises(WorkerCrashed):
                await asyncio.wait_for(runner.run_async(pipeline("die", echo).map(range(10))), 3)

    asyncio.run(case())


@pytest.mark.parametrize("layout", ["combined", "by_experiment"])
def test_suite_shares_one_admission_budget(tmp_path, layout):
    from pyattacker import ExperimentSpec, SuiteSpec

    async def case():
        consumed = []

        @task("suite-park", retry=Retrying(max_attempts=2, base=0))
        def park(seed):
            raise RetryableError("wait", retry_after=60)

        template = pipeline("park", park)

        def source():
            for i in range(100):
                consumed.append(i)
                yield template.bind(i)

        suite = SuiteSpec(
            "budget",
            [ExperimentSpec(eid, source, "v1") for eid in ("a", "b")],
            str(tmp_path),
            layout,
            run={"max_admitted": 3},
        )
        with suite.runner(concurrency=1, grace_s=0, handle_signals=False) as runner:
            running = asyncio.create_task(runner.run_async(suite.pipelines(runner.store)))
            try:
                await until(lambda: len(runner._delays) == 3)
                assert len(consumed) == 3
                runner.stop()
                report = await asyncio.wait_for(running, 3)
                assert report.stats["pipelines"]["total"] == 3
                assert all(not e["source_exhausted"] for e in report.stats["experiments"])
            finally:
                if not running.done():
                    running.cancel()
                await asyncio.gather(running, return_exceptions=True)

    asyncio.run(case())


def test_shards_forward_admission_override():
    from argparse import Namespace

    from pyattacker.cli import _child_argv

    args = Namespace(config="c.json", max_admitted=7)
    argv = _child_argv(args, 0, 2, "/tmp/shard.db", resume=False)
    assert argv[argv.index("--max-admitted") + 1] == "7"
