"""Declarative config layer tests: YAML → pools + pipeline template + source.

Coverage
* load_spec correctly builds pools (kind/capacity/resources/algorithm), the pipeline template, run, source
* ``${VAR}`` and ``${VAR:-default}`` environment variable expansion (monkeypatch.setenv / delenv);
  unset variables are recorded in unresolved_env; strict_env=True raises ConfigError
* ``use: flaky`` built-in shorthand and ``use: module:attr`` fully qualified names (temporary module + syspath_prepend)
* resolution of exception names in retry.on: "RetryableError" (pyattacker.errors) / "TimeoutError" (builtin); unknown names raise
* missing pipeline section / empty pipeline.tasks / task without the use field → ConfigError
* describe() output structure (config/pipeline/pools/run/source/unresolved_env)
* the count of spec.pipelines(limit=N), the content-addressed key, and the dedupe semantics of repeats/key_field
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest

from pyattacker.algorithm import Backoff, Wait
from pyattacker.declarative import DeclarativeSpec, expand_env, load_spec, resolve_target
from pyattacker.errors import ConfigError, RetryableError
from pyattacker.tasks import boom, flaky

# --------------------------------------------------------------------- helpers


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def _pools(spec: DeclarativeSpec) -> dict[str, Any]:
    return {pool.name: pool for pool in spec.pools}


FULL_CONFIG = """
pools:
  apis:
    kind: llm
    capacity: 3
    algorithm: backoff
    resources:
      - id: api-1
        options: {model: gpt-4o}
      - id: api-2
        capacity: 5
        tags: {region: eu}
  judge:
    algorithm: wait
    resources:
      - id: judge-1
pipeline:
  name: qa
  resource: apis
  tags: {suite: decl}
  tasks:
    - use: echo
    - use: echo
run:
  concurrency: 7
  label: decl-run
  journal: summary
source:
  kind: range
  n: 4
"""


# ------------------------------------------------------------------ basic construction


@pytest.mark.requires_yaml
def test_load_spec_builds_pools_pipeline_run_and_source(tmp_path):
    cfg = _write(tmp_path, "full.yaml", FULL_CONFIG)
    spec = load_spec(cfg)

    assert isinstance(spec, DeclarativeSpec)
    assert spec.path == str(cfg)
    assert spec.unresolved_env == []

    pools = _pools(spec)
    assert sorted(pools) == ["apis", "judge"]

    apis = pools["apis"]
    assert apis.kind == "llm"
    assert len(apis) == 2
    assert [r.id for r in apis.resources()] == ["api-1", "api-2"]
    assert [r.capacity for r in apis.resources()] == [3, 5]  # resource-level capacity overrides the pool default
    assert [r.kind for r in apis.resources()] == ["llm", "llm"]  # inherits the pool kind
    assert apis.resources()[0].options == {"model": "gpt-4o", "kind": "llm"}
    assert apis.resources()[1].tags == {"region": "eu"}
    assert isinstance(apis.default_algorithm, Backoff)
    assert apis.default_algorithm.name == "backoff"

    judge = pools["judge"]
    assert judge.kind is None
    assert [r.id for r in judge.resources()] == ["judge-1"]
    assert judge.resources()[0].kind == "judge"  # without a kind it falls back to the pool name
    assert isinstance(judge.default_algorithm, Wait)

    assert spec.template.name == "qa"
    assert spec.template.task_names() == ["mock.echo", "mock.echo"]
    assert spec.template.tags == {"suite": "decl"}
    assert spec.template.n_tasks == 2
    assert spec.template.tasks[0].resource == "apis"  # pipeline.resource is handed to every task as its default pool
    assert spec.template.tasks[1].resource == "apis"

    assert spec.run == {"concurrency": 7, "label": "decl-run", "journal": "summary"}
    assert spec.source == {"kind": "range", "n": 4}
    assert [s.seed for s in spec.pipelines()] == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]


@pytest.mark.requires_yaml
def test_describe_output_structure(tmp_path):
    cfg = _write(tmp_path, "full.yaml", FULL_CONFIG)
    spec = load_spec(cfg)

    described = spec.describe()
    assert set(described) == {"config", "pipeline", "pools", "run", "source", "unresolved_env"}
    assert described["config"] == str(cfg)
    assert described["pipeline"] == {
        "name": "qa",
        "tasks": ["mock.echo", "mock.echo"],
        "tags": {"suite": "decl"},
        "spec_digest": spec.template.spec_digest,
    }
    assert len(described["pipeline"]["spec_digest"]) == 32  # hex of blake2b(digest_size=16)
    assert described["pools"] == {
        "apis": {"kind": "llm", "resources": 2, "capacity": 8, "algorithm": "backoff"},
        "judge": {"kind": None, "resources": 1, "capacity": 1, "algorithm": "wait"},
    }
    assert described["run"] == {"concurrency": 7, "label": "decl-run", "journal": "summary"}
    assert described["source"] == {"kind": "range", "n": 4}
    assert described["unresolved_env"] == []


# ------------------------------------------------------------- environment expansion


@pytest.mark.requires_yaml
def test_env_expansion_with_and_without_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("DECL_API_ID", "api-9")
    monkeypatch.setenv("DECL_API_KEY", "sk-secret")
    monkeypatch.delenv("DECL_MODEL", raising=False)
    monkeypatch.delenv("DECL_UNSET", raising=False)
    monkeypatch.delenv("DECL_LABEL", raising=False)

    cfg = _write(
        tmp_path,
        "env.yaml",
        """
        pools:
          apis:
            resources:
              - id: ${DECL_API_ID}
                options:
                  api_key: ${DECL_API_KEY}
                  model: ${DECL_MODEL:-gpt-4o-mini}
                  note: ${DECL_UNSET}
        pipeline:
          name: qa
          tasks:
            - use: echo
        run:
          label: ${DECL_LABEL:-label-x}
        """,
    )
    spec = load_spec(cfg)

    resource = spec.pools[0].resources()[0]
    assert resource.id == "api-9"
    assert resource.options["api_key"] == "sk-secret"
    assert resource.options["model"] == "gpt-4o-mini"  # unset → use the default value
    assert resource.options["note"] == "${DECL_UNSET}"  # unset and no default → kept verbatim
    assert spec.run["label"] == "label-x"
    assert spec.unresolved_env == ["DECL_UNSET"]


def test_expand_env_recurses_and_reports_unresolved(monkeypatch):
    monkeypatch.setenv("DECL_X", "1")
    monkeypatch.delenv("DECL_Y", raising=False)
    unresolved: list[str] = []

    expanded = expand_env({"a": ["${DECL_X}", "${DECL_Y:-2}"], "b": {"c": "${DECL_Y}"}}, unresolved=unresolved)

    assert expanded == {"a": ["1", "2"], "b": {"c": "${DECL_Y}"}}
    assert unresolved == ["DECL_Y"]


def test_expand_env_strict_raises_config_error(monkeypatch):
    monkeypatch.delenv("DECL_MISSING", raising=False)
    with pytest.raises(ConfigError, match="DECL_MISSING"):
        expand_env({"v": "${DECL_MISSING}"}, strict=True)


@pytest.mark.requires_yaml
def test_load_spec_strict_env_raises_and_resolve_target_errors(tmp_path, monkeypatch):
    monkeypatch.delenv("DECL_DEFINITELY_UNSET", raising=False)
    cfg = _write(
        tmp_path,
        "strict.yaml",
        """
        pipeline:
          name: qa
          tasks:
            - use: echo
        run:
          label: ${DECL_DEFINITELY_UNSET}
        """,
    )
    spec = load_spec(cfg)
    assert spec.run["label"] == "${DECL_DEFINITELY_UNSET}"
    assert spec.unresolved_env == ["DECL_DEFINITELY_UNSET"]
    with pytest.raises(ConfigError, match="DECL_DEFINITELY_UNSET"):
        load_spec(cfg, strict_env=True)

    assert resolve_target("flaky") is flaky  # built-in shorthand
    assert resolve_target("pyattacker.tasks:boom") is boom  # fully qualified name
    with pytest.raises(ConfigError, match="nope"):
        resolve_target("nope")
    with pytest.raises(ConfigError, match="no_such_module_for_decl_test"):
        resolve_target("no_such_module_for_decl_test:thing")


# ------------------------------------------------------------------ use resolution


@pytest.mark.requires_yaml
def test_use_builtin_shorthand_and_qualified_module_target(tmp_path, monkeypatch):
    module = tmp_path / "decl_custom_tasks.py"
    module.write_text(
        textwrap.dedent(
            '''
            """Temporary module: verifies the ``use: module:attr`` import path."""
            from pyattacker import task


            @task("custom.upper")
            def upper(value, ctx):
                return str(value).upper()
            '''
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    cfg = _write(
        tmp_path,
        "use.yaml",
        """
        pipeline:
          name: mixed
          tasks:
            - use: flaky
              kwargs: {fail_times: 1}
              retry: {max_attempts: 3, base: 0.01}
            - use: decl_custom_tasks:upper
        """,
    )
    spec = load_spec(cfg)
    first, second = spec.template.tasks

    assert first.name == "mock.flaky"  # built-in shorthand: calling the factory yields a TaskSpec
    assert first.retry.max_attempts == 3
    assert first.retry.base == 0.01

    # fully qualified name: import the module and get the attribute; the TaskSpec carries the name declared by @task
    assert second.name == "custom.upper"
    assert second.module == "decl_custom_tasks"
    assert second.qualname.endswith("upper")
    assert spec.template.task_names() == ["mock.flaky", "custom.upper"]


@pytest.mark.requires_yaml
def test_task_entry_name_override_is_honored(tmp_path):
    """A task entry's ``name:`` should override the name carried by the use target."""
    cfg = _write(
        tmp_path,
        "rename.yaml",
        """
        pipeline:
          name: rename
          tasks:
            - use: echo
              name: fetch
        """,
    )
    names = load_spec(cfg).template.task_names()
    assert names == ["fetch"]  # the name in the config overrides the name carried by the use target


@pytest.mark.requires_yaml
def test_use_factory_args_dict_is_passed_as_keywords(tmp_path):
    """``args: {fail_times: 1}`` should be equivalent to ``flaky(fail_times=1)``."""
    cfg = _write(
        tmp_path,
        "use_args.yaml",
        """
        pipeline:
          name: args
          tasks:
            - use: flaky
              args: {fail_times: 1}
        """,
    )
    spec = load_spec(cfg)
    task = spec.template.tasks[0]
    assert task.name == "mock.flaky"
    assert task.retry.max_attempts == 2  # default retry budget of flaky(fail_times=1) = 1 + 1


@pytest.mark.requires_yaml
def test_use_unknown_target_raises_config_error(tmp_path):
    cfg = _write(
        tmp_path,
        "bad_use.yaml",
        """
        pipeline:
          name: qa
          tasks:
            - use: not_a_builtin_and_not_a_module
        """,
    )
    with pytest.raises(ConfigError, match="not_a_builtin_and_not_a_module"):
        load_spec(cfg)


# ---------------------------------------------------------------- retry.on


@pytest.mark.requires_yaml
def test_retry_on_resolves_exception_names(tmp_path):
    cfg = _write(
        tmp_path,
        "retry.yaml",
        """
        pipeline:
          name: r
          tasks:
            - use: echo
              name: a
              retry:
                max_attempts: 4
                base: 0.25
                "on": [RetryableError, TimeoutError]
            - use: echo
              name: b
              retry: {max_attempts: 2, "on": TimeoutError}
        """,
    )
    spec = load_spec(cfg)
    first, second = spec.template.tasks

    assert first.retry.max_attempts == 4
    assert first.retry.base == 0.25
    assert first.retry.on == (RetryableError, TimeoutError)
    # a single string is also normalized into a 1-tuple
    assert second.retry.on == (TimeoutError,)
    assert second.retry.max_attempts == 2


@pytest.mark.requires_yaml
def test_retry_bare_on_is_yaml_boolean_and_raises_with_hint(tmp_path):
    """YAML 1.1 parses a bare ``on`` as boolean true — it must raise with an actionable hint."""
    cfg = _write(
        tmp_path,
        "retry_bare_on.yaml",
        """
        pipeline:
          name: r
          tasks:
            - use: echo
              retry:
                max_attempts: 2
                on: [RetryableError]
        """,
    )
    with pytest.raises(ConfigError) as excinfo:
        load_spec(cfg)
    message = str(excinfo.value)
    assert "YAML" in message
    assert '"on"' in message


@pytest.mark.requires_yaml
def test_retry_on_unknown_exception_name_raises(tmp_path):
    cfg = _write(
        tmp_path,
        "retry_bad.yaml",
        """
        pipeline:
          name: r
          tasks:
            - use: echo
              retry:
                max_attempts: 2
                "on": [NoSuchErrorAnywhere]
        """,
    )
    with pytest.raises(ConfigError, match="NoSuchErrorAnywhere"):
        load_spec(cfg)


# ------------------------------------------------------------- config error paths


@pytest.mark.requires_yaml
def test_missing_pipeline_section_raises_config_error(tmp_path):
    cfg = _write(tmp_path, "no_pipeline.yaml", "pools: {}\nrun: {label: x}\n")
    with pytest.raises(ConfigError, match="pipeline"):
        load_spec(cfg)


@pytest.mark.requires_yaml
def test_empty_pipeline_tasks_raises_config_error(tmp_path):
    cfg = _write(tmp_path, "empty.yaml", "pipeline:\n  name: qa\n  tasks: []\n")
    with pytest.raises(ConfigError, match="tasks"):
        load_spec(cfg)


@pytest.mark.requires_yaml
def test_task_without_use_field_raises_config_error(tmp_path):
    cfg = _write(
        tmp_path,
        "no_use.yaml",
        """
        pipeline:
          name: qa
          tasks:
            - name: oops
        """,
    )
    with pytest.raises(ConfigError, match="use"):
        load_spec(cfg)


def test_missing_config_file_raises_config_error(tmp_path):
    with pytest.raises(ConfigError, match="does not exist"):
        load_spec(tmp_path / "nope.yaml")


# --------------------------------------------------- pipelines(limit) and deduplication


@pytest.mark.requires_yaml
def test_pipelines_limit_counts_yielded_specs(tmp_path):
    cfg = _write(
        tmp_path,
        "seeds.yaml",
        """
        pipeline:
          name: qa
          tasks:
            - use: echo
        source:
          kind: range
          n: 5
        """,
    )
    spec = load_spec(cfg)

    assert [s.seed for s in spec.pipelines(limit=3)] == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert len(list(spec.pipelines(limit=0))) == 0
    assert len(list(spec.pipelines(limit=99))) == 5

    all_specs = list(spec.pipelines())
    assert len(all_specs) == 5
    assert len({s.pipeline_id for s in all_specs}) == 5  # content addressing: one key per seed
    assert all(s.key == s.pipeline_id for s in all_specs)
    assert all(s.name == "qa" and s.n_tasks == 1 for s in all_specs)


@pytest.mark.requires_yaml
def test_pipelines_repeats_and_key_field_dedupe_semantics(tmp_path):
    seeds = tmp_path / "seeds.jsonl"
    seeds.write_text('{"i": 1}\n{"i": 1}\n{"i": 2}\n', encoding="utf-8")
    cfg = _write(
        tmp_path,
        "repeats.yaml",
        f"""
        pipeline:
          name: qa
          tasks:
            - use: echo
        source:
          kind: jsonl
          path: "{seeds}"
          key_field: i
          repeats: 2
        """,
    )
    spec = load_spec(cfg)
    specs = list(spec.pipelines())

    assert len(specs) == 6  # 3 lines x repeats=2
    assert [s.key for s in specs] == [
        "qa:1#0",
        "qa:1#1",
        "qa:1#0",
        "qa:1#1",
        "qa:2#0",
        "qa:2#1",
    ]
    assert [s.repeat for s in specs] == [0, 1, 0, 1, 0, 1]
    # content-addressed dedupe semantics: a repeated seed (i=1 appears twice) yields the same pipeline_id,
    # the iterator does not deduplicate (the count is still 6), but the runner/store treat it as one pipeline.
    assert specs[0].pipeline_id == specs[2].pipeline_id
    assert specs[1].pipeline_id == specs[3].pipeline_id
    assert specs[0].pipeline_id != specs[1].pipeline_id
    assert len({s.pipeline_id for s in specs}) == 4  # qa:1#0 / qa:1#1 / qa:2#0 / qa:2#1

    assert [s.seed for s in spec.pipelines(limit=2)] == [{"i": 1}, {"i": 1}]
    assert [s.key for s in spec.pipelines(limit=2)] == ["qa:1#0", "qa:1#1"]
