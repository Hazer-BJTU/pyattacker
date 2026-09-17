"""Recovery must reuse only the same declared behavior and input, across persistent runs."""
from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import subprocess
import sys

import pytest

from pyattacker import (
    Backoff,
    ConfigError,
    Failover,
    LeastBusy,
    PipelineIdentityConflict,
    Pool,
    QuotaAware,
    Resource,
    Retrying,
    Runner,
    Sticky,
    Wait,
    pipeline,
    task,
)
from pyattacker.cli import main
from pyattacker.declarative import load_spec
from pyattacker.shard import shard_index
from pyattacker.tasks import boom, delay, echo, fanout, flaky, shell_run, simulate_llm, write_jsonl


@pytest.mark.parametrize('before,after', [
    (delay(0), delay(10)), (boom('a'), boom('b')),
    (flaky(0), flaky(0, error='fatal')),
    (simulate_llm(tokens=1), simulate_llm(tokens=2)),
    (shell_run(['echo', 'a']), shell_run(['echo', 'b'])),
    (write_jsonl('a.jsonl'), write_jsonl('b.jsonl')),
    (fanout(echo), fanout(delay(0))),
    (fanout(echo), fanout(echo, on_error='collect')),
])
def test_factory_behavior_changes_default_identity(before, after):
    assert pipeline('x', before).bind(1).pipeline_id != pipeline('x', after).bind(1).pipeline_id


def test_changed_factory_parameters_do_not_skip_results():
    with Runner(handle_signals=False) as runner:
        first = pipeline('x', flaky(0, name='same'))
        changed = pipeline('x', flaky(0, name='same', error='fatal'))
        runner.run(first.map([1]))
        report = runner.run(changed.map([1]), resume=True)
        assert report.skipped == 0
        assert report.stats['pipelines']['by_state'] == {'succeeded': 1}
        assert runner.run(changed.map([1]), resume=True).skipped == 1


@pytest.mark.parametrize('policy', [
    Retrying(on=(ValueError,)), Retrying(retry_classified=False),
    Retrying(retry_unknown=True), Retrying(max_total_s=5),
])
def test_retry_decisions_change_identity(policy):
    assert pipeline('x', echo).spec_digest != pipeline('x', dataclasses.replace(echo, retry=policy)).spec_digest


def test_algorithm_configuration_and_representation():
    def spec(algorithm):
        return pipeline('x', dataclasses.replace(echo, algorithm=algorithm)).spec_digest
    assert spec('backoff') == spec(Backoff())
    assert spec({'name': 'backoff', 'base': 2}) == spec(Backoff(base=2))
    assert spec(Backoff()) != spec(Backoff(base=2))
    assert spec({'name': 'sticky', 'fallback': 'wait'}) != spec({'name': 'sticky', 'fallback': 'immediate'})


def test_config_snapshot_version_and_recursive_include_code():
    config = {'model': {'name': 'a'}}
    wrapped = task(config=config, version='1')(echo.fn)
    identity = pipeline('x', wrapped).spec_digest
    config['model']['name'] = 'b'
    assert pipeline('x', wrapped).spec_digest == identity
    assert pipeline('x', task(config=config, version='1')(echo.fn)).spec_digest != identity
    assert pipeline('x', dataclasses.replace(wrapped, version='2')).spec_digest != identity
    child = dataclasses.replace(echo, code_digest='changed')
    assert pipeline('x', fanout(echo)).spec_digest != pipeline('x', fanout(child)).spec_digest
    assert pipeline('x', fanout(echo), include_code=False).spec_digest == pipeline('x', fanout(child), include_code=False).spec_digest
    assert pipeline('x', fanout(echo), include_code=False).spec_digest != pipeline('x', fanout(delay(0)), include_code=False).spec_digest


@pytest.mark.parametrize('config', [
    {'client': object()}, {'value': float('nan')}, {1: "x"},
    {"value": (1, 2)}, {"nested": [{False: "x"}]},
])
def test_identity_rejects_runtime_objects_and_nonfinite_values(config):
    with pytest.raises(ConfigError, match='finite JSON'):
        task(config=config)(echo.fn)


class CustomAlgorithm:
    name = 'custom'

    async def acquire(self, pool, **kwargs):
        return pool.try_acquire()


def test_custom_algorithm_requires_declared_identity():
    with pytest.raises(ConfigError, match='fingerprint'):
        pipeline('x', dataclasses.replace(echo, algorithm=CustomAlgorithm()))
    a = dataclasses.replace(echo, algorithm=CustomAlgorithm(), version='1')
    b = dataclasses.replace(a, version='2')
    assert pipeline('x', a).spec_digest != pipeline('x', b).spec_digest


def test_identity_and_shards_stable_across_processes():
    code = "from pyattacker import pipeline,fanout; from pyattacker.tasks import delay,echo; print(pipeline('x',fanout(delay(0),echo)).bind({'a':1}).pipeline_id)"
    expected = pipeline('x', fanout(delay(0), echo)).bind({'a': 1}).pipeline_id
    for hash_seed in ('1', '2'):
        actual = subprocess.check_output([sys.executable, '-c', code], text=True, env={**os.environ, 'PYTHONHASHSEED': hash_seed}).strip()
        assert actual == expected
        assert shard_index(actual, 4) == shard_index(expected, 4)
    repeated = list(pipeline('x', echo).map([1], repeats=3))
    assert len({s.pipeline_id for s in repeated}) == 3


@pytest.mark.parametrize('state', ['succeeded', 'failed', 'interrupted'])
@pytest.mark.parametrize('change', ['seed', 'code'])
@pytest.mark.parametrize('storage', ['memory', 'sqlite'])
def test_explicit_key_conflict_preserves_history(tmp_path, state, change, storage):
    store = ':memory:' if storage == 'memory' else str(tmp_path / 'run.db')
    original = pipeline('x', echo).bind(1, key='row')
    changed = pipeline('x', dataclasses.replace(echo, code_digest='changed') if change == 'code' else echo).bind(2 if change == 'seed' else 1, key='row')
    with Runner(store=store, handle_signals=False, retry_succeeded=True) as runner:
        runner.run([original])
        runner.store.finish_pipeline('row', state, n_tasks_done=1)
        before = dataclasses.asdict(runner.store.get_pipeline('row'))
        artifacts = [dataclasses.asdict(a) for a in runner.store.artifacts('row')]
        with pytest.raises(PipelineIdentityConflict, match='row'):
            runner.run([changed], resume=True)
        assert dataclasses.asdict(runner.store.get_pipeline('row')) == before
        assert [dataclasses.asdict(a) for a in runner.store.artifacts('row')] == artifacts
        assert len(runner.store.attempts(pipeline_id='row')) == 1
        assert runner.store.get_run(runner.run_id).status == 'interrupted'
        assert any(e.kind == 'pipeline.identity_conflict' for e in runner.store.events())


def test_same_explicit_key_resumes_checkpoint_and_skips_success(tmp_path):
    calls = []
    fail = True

    @task('prepare')
    def prepare(value):
        calls.append('prepare')
        return value + 1

    @task('finish')
    def finish(value):
        calls.append('finish')
        if fail:
            raise ValueError('temporarily fails')
        return value + 1

    spec = pipeline('x', prepare, finish).bind(1, key='row')
    with Runner(store=str(tmp_path / 'run.db'), handle_signals=False) as runner:
        runner.run([spec])
    fail = False
    with Runner(store=str(tmp_path / 'run.db'), handle_signals=False) as runner:
        assert runner.run([spec], resume=True).stats['pipelines']['by_state'] == {'succeeded': 1}
        assert runner.run([spec], resume=True).skipped == 1
        assert calls == ['prepare', 'finish', 'finish']
        assert runner.store.get_artifact('row', 1).payload == b'3'


def test_legacy_store_warns_and_rejects_explicit_reuse():
    spec = pipeline('x', echo).bind(1, key='row')
    with Runner(handle_signals=False) as runner:
        runner.run([spec])
        row = runner.store.get_pipeline('row')
        row.spec_digest = 'legacy-digest'
        runner.store.upsert_pipeline(row)
        with pytest.warns(UserWarning, match='legacy task fingerprints'), pytest.raises(PipelineIdentityConflict):
            runner.run([spec], resume=True)
        assert runner.store.get_pipeline('row').spec_digest == 'legacy-digest'
        with pytest.warns(UserWarning, match='legacy task fingerprints'):
            assert runner.run(pipeline('x', echo).map([1]), resume=True).skipped == 0


def test_declarative_key_field_and_version(tmp_path, capsys):
    data = tmp_path / 'data.jsonl'
    data.write_text('{"id":1,"value":"a"}\n')
    config = tmp_path / 'run.json'
    raw = {'run': {'store': str(tmp_path / 'run.db')}, 'pipeline': {'name': 'x', 'tasks': [{'use': 'echo', 'version': '1', 'config': {'model': 'a'}}]}, 'source': {'kind': 'jsonl', 'path': str(data), 'key_field': 'id'}}
    config.write_text(json.dumps(raw))
    assert load_spec(config).template.tasks[0].version == '1'
    assert main(['run', '-c', str(config), '--no-signals']) == 0
    data.write_text('{"id":1,"value":"b"}\n')
    assert main(['resume', '-c', str(config), '--no-signals']) == 2
    assert 'conflicts' in capsys.readouterr().err


def test_custom_algorithm_hook_and_factory_config_override(tmp_path):
    class Configured(CustomAlgorithm):
        def __init__(self, mode):
            self.mode = mode

        def fingerprint(self):
            return {"mode": self.mode}

    a = dataclasses.replace(echo, algorithm=Configured("a"))
    b = dataclasses.replace(echo, algorithm=Configured("b"))
    assert pipeline("x", a).spec_digest != pipeline("x", b).spec_digest
    config = tmp_path / "run.json"
    def load(seconds):
        config.write_text(json.dumps({"pipeline": {"tasks": [{"use": "delay", "kwargs": {"seconds": seconds}, "config": {"model": "a"}}]}}))
        return load_spec(config).template.spec_digest
    assert load(0) != load(1)


@pytest.mark.parametrize("storage", ["memory", "sqlite"])
def test_identity_conflict_cancels_inflight_and_does_not_start_queued_work(tmp_path, storage):
    async def scenario():
        calls = []
        started = asyncio.Event()
        cancelled = asyncio.Event()

        @task("slow")
        async def slow(value):
            calls.append(value)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        store = ":memory:" if storage == "memory" else str(tmp_path / "run.db")
        with Runner(store=store, handle_signals=False, concurrency=2, grace_s=60) as runner:
            runner.run([pipeline("x", echo).bind(1, key="row")])
            conflict = pipeline("x", echo).bind(2, key="row")
            queued = pipeline("x", echo, slow)
            def specs():
                yield queued.bind(0)
                yield conflict
                yield from queued.map(range(1, 100))
            with pytest.raises(PipelineIdentityConflict):
                await asyncio.wait_for(runner.run_async(specs(), resume=True), timeout=2)
            assert started.is_set()
            assert cancelled.is_set()
            assert calls == [0]
            assert runner.store.get_pipeline("row").state == "succeeded"
            interrupted = runner.store.get_pipeline(queued.bind(0).pipeline_id)
            assert interrupted.state == "interrupted"
            assert interrupted.n_tasks_done == 1
            assert runner.store.get_artifact(interrupted.pipeline_id, 0).payload == b"0"
            assert not runner.store.pipelines(run_id=runner.run_id, state="running")
            tasks = runner.store.tasks(interrupted.pipeline_id)
            assert [record.state for record in tasks] == ["succeeded", "interrupted"]
    asyncio.run(scenario())


@pytest.mark.parametrize("mapping", [False, True])
def test_runner_uses_algorithm_snapshot_after_caller_mutation(mapping):
    algorithm = {"name": "backoff", "base": 1.0} if mapping else Backoff(base=1.0)

    @task("probe", resource="apis", algorithm=algorithm)
    async def probe(value, ctx):
        async with ctx.acquire():
            observed = ctx.default_algorithm.base
        ctx.default_algorithm.base = 200  # a runtime instance cannot change the next task's definition
        return observed

    template = pipeline("x", probe)
    original_fingerprint = probe.fingerprint()
    if mapping:
        algorithm["base"] = 100
    else:
        algorithm.base = 100
    assert probe.fingerprint() == original_fingerprint
    assert pipeline("x", probe).spec_digest == template.spec_digest
    pool = Pool("apis", [Resource.create("llm")])
    with Runner(pools=[pool], handle_signals=False) as runner:
        runner.run(template.map([1, 2]))
        outputs = [runner.store.get_artifact(s.pipeline_id, 0).payload for s in template.map([1, 2])]
        assert outputs == [b"1.0", b"1.0"]


@pytest.mark.parametrize("factory", [Sticky, LeastBusy, Failover, QuotaAware])
def test_implicit_wait_fallback_is_canonical(factory):
    implicit = dataclasses.replace(echo, algorithm=factory())
    explicit = dataclasses.replace(echo, algorithm=factory(fallback=Wait()))
    assert pipeline("x", implicit).spec_digest == pipeline("x", explicit).spec_digest
    changed = dataclasses.replace(echo, algorithm=factory(fallback=Wait(timeout=1)))
    assert pipeline("x", implicit).spec_digest != pipeline("x", changed).spec_digest


def test_nested_algorithm_snapshot_is_not_changed_by_caller():
    fallback = Backoff(base=1)
    algorithm = Sticky(fallback=fallback)
    spec = dataclasses.replace(echo, algorithm=algorithm)
    identity = spec.fingerprint()
    fallback.base = 100
    algorithm.fallback = Wait()
    assert spec.fingerprint() == identity
    assert spec.runtime_algorithm().fallback.base == 1


class FingerprintedAlgorithm(CustomAlgorithm):
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def fingerprint(self):
        return {"value": self.value}

    async def acquire(self, pool, **kwargs):
        self.calls += 1
        return await super().acquire(pool, **kwargs)


@pytest.mark.parametrize("value", [(1, 2), {1: "x"}, float("inf")])
def test_custom_algorithm_fingerprint_rejects_coercions(value):
    with pytest.raises(ConfigError, match="finite JSON"):
        dataclasses.replace(echo, algorithm=FingerprintedAlgorithm(value))


def test_custom_algorithm_drift_is_rejected_before_task_execution():
    algorithm = FingerprintedAlgorithm(1)
    called = []

    @task("probe", algorithm=algorithm)
    def probe(value):
        called.append(value)
        return value

    template = pipeline("x", probe)
    identity = probe.fingerprint()
    algorithm.value = 2
    assert probe.fingerprint() == identity
    with Runner(handle_signals=False) as runner:
        report = runner.run(template.map([1]))
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        row = runner.store.get_pipeline(template.bind(1).pipeline_id)
        assert "fingerprint changed" in row.error_message
        assert not called


def test_custom_algorithm_drift_is_rechecked_at_acquisition():
    algorithm = FingerprintedAlgorithm(1)

    @task("probe", algorithm=Sticky(fallback=algorithm), resource="apis")
    async def probe(value, ctx):
        algorithm.value = 2  # after _begin_task, before the algorithm is used
        async with ctx.acquire():
            return value

    pool = Pool("apis", [Resource.create("llm")])
    with Runner(pools=[pool], handle_signals=False) as runner:
        report = runner.run(pipeline("x", probe).map([1]))
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        assert algorithm.calls == 0
        assert pool.stats().active == 0


def test_version_only_custom_algorithm_is_detached_from_source():
    algorithm = CustomAlgorithm()
    algorithm.settings = {"mode": "a"}
    spec = dataclasses.replace(echo, algorithm=algorithm, version="1")
    algorithm.settings["mode"] = "b"
    runtime = spec.runtime_algorithm()
    assert runtime.settings == {"mode": "a"}
    runtime.settings["mode"] = "c"
    assert spec.runtime_algorithm().settings == {"mode": "a"}


def test_json_identity_accepts_shared_subtrees_but_rejects_cycles():
    values = [None, True, 1, 1.5, "x", {"value": []}]
    task(config={"a": values, "b": values})(echo.fn)
    values.append(values)
    with pytest.raises(ConfigError, match="cyclic"):
        task(config={"a": values})(echo.fn)


def test_failover_pool_sequences_normalize_intentionally():
    a = dataclasses.replace(echo, algorithm=Failover(pools=("primary", "backup")))
    b = dataclasses.replace(echo, algorithm=Failover(pools=["primary", "backup"]))
    assert a.fingerprint() == b.fingerprint()
    assert a.runtime_algorithm().pools == ["primary", "backup"]


def test_uncopyable_version_only_custom_algorithm_is_rejected():
    class Uncopyable(CustomAlgorithm):
        def __deepcopy__(self, memo):
            return self
    with pytest.raises(ConfigError, match="separate instance"):
        dataclasses.replace(echo, algorithm=Uncopyable(), version="1")
