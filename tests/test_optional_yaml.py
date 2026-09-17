"""PyYAML is an extra, not a dependency.

The base install declares no dependencies at all, and the declarative layer needs nothing beyond the
standard library unless the config file is YAML. That has three consequences worth testing, and they
are tested here rather than in the YAML tests themselves:

* the SDK and the other two config formats run with no third-party library present;
* a ``.yaml``/``.yml`` config fails with a message that names the extra and the command to install it,
  instead of a bare ``ModuleNotFoundError`` from deep inside the loader;
* the decision is made per file, from its suffix — the same process can read a JSON config and refuse
  a YAML one.

Absence is simulated by putting ``None`` into ``sys.modules``, which is how CPython records "this
module may not be imported" (``import yaml`` then raises ``ModuleNotFoundError`` named ``yaml``), and a
*broken* PyYAML is simulated by shadowing it with a module whose own import fails. That keeps these
tests meaningful in both environments: the developer one, which has the extra, and the CI job that
installs the bare package.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from pyattacker import Runner, echo, load_spec, pipeline
from pyattacker.cli import main
from pyattacker.errors import ConfigError

YAML_CONFIG = """
pipeline:
  name: no-extra
  tasks:
    - {use: echo}
"""

JSON_CONFIG = '{"pipeline": {"name": "no-extra", "tasks": [{"use": "echo"}]}}'

TOML_CONFIG = """
[pipeline]
name = "no-extra"

[[pipeline.tasks]]
use = "echo"
"""


def _without_pyyaml(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate a machine that installed pyattacker without the `yaml` extra."""
    monkeypatch.setitem(sys.modules, "yaml", None)


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


# ------------------------------------------------------------------ the SDK itself


def test_the_sdk_runs_without_the_extra(monkeypatch):
    """Nothing in the kernel touches PyYAML, so a program with no config file runs bare."""
    _without_pyyaml(monkeypatch)

    runner = Runner(store=":memory:", concurrency=2, handle_signals=False)
    report = runner.run(pipeline("no-extra", echo).map([{"i": 1}, {"i": 2}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 2}


# ------------------------------------------------------------------ per-file dispatch


@pytest.mark.parametrize("name", ["spec.json", "spec.toml"])
def test_json_and_toml_configs_need_no_extra(tmp_path, monkeypatch, name):
    """The suffix decides, so the two standard-library formats keep working with PyYAML absent."""
    _without_pyyaml(monkeypatch)
    text = JSON_CONFIG if name.endswith(".json") else TOML_CONFIG

    spec = load_spec(_write(tmp_path, name, text))

    assert spec.template.name == "no-extra"
    assert spec.template.task_names() == ["mock.echo"]


@pytest.mark.parametrize("name", ["spec.yaml", "spec.yml"])
def test_yaml_config_without_the_extra_names_the_install_command(tmp_path, monkeypatch, name):
    _without_pyyaml(monkeypatch)
    cfg = _write(tmp_path, name, YAML_CONFIG)

    with pytest.raises(ConfigError) as excinfo:
        load_spec(cfg)

    message = str(excinfo.value)
    assert str(cfg) in message, "the error must say which file pulled the dependency in"
    assert "pyattacker[yaml]" in message, "the error must name the extra to install"
    assert "JSON and TOML" in message, "and must mention that there is a way out that needs nothing"


def test_a_broken_pyyaml_is_not_reported_as_missing(tmp_path, monkeypatch):
    """PyYAML installed but broken (one of its own imports fails) is not a missing extra.

    The hint is only correct when the `yaml` module itself is absent, so the loader checks the name on
    the exception: a user who already has the extra needs the real error, which names the module that
    is actually missing, not advice to install what they already installed.
    """
    (tmp_path / "yaml.py").write_text("import pyattacker_no_such_internal_module\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "yaml", raising=False)  # a cached real PyYAML would win instead

    cfg = _write(tmp_path, "spec.yaml", YAML_CONFIG)

    with pytest.raises(ModuleNotFoundError) as excinfo:
        load_spec(cfg)
    assert excinfo.value.name == "pyattacker_no_such_internal_module"


def test_one_process_reads_json_and_refuses_yaml(tmp_path, monkeypatch):
    """The check is per file, not per process: the loader never assumes the whole run is YAML."""
    _without_pyyaml(monkeypatch)
    json_cfg = _write(tmp_path, "spec.json", JSON_CONFIG)
    yaml_cfg = _write(tmp_path, "spec.yaml", YAML_CONFIG)

    assert load_spec(json_cfg).template.name == "no-extra"
    with pytest.raises(ConfigError):
        load_spec(yaml_cfg)


# ------------------------------------------------------------------ the CLI


def test_cli_reports_the_missing_extra_as_a_config_error(tmp_path, monkeypatch, capsys):
    """Exit code 2 and the install hint on stderr — the same shape as every other config error."""
    _without_pyyaml(monkeypatch)
    cfg = _write(tmp_path, "spec.yaml", YAML_CONFIG)

    assert main(["validate", "-c", str(cfg)]) == 2

    err = capsys.readouterr().err
    assert err.startswith("Config error:")
    assert "pyattacker[yaml]" in err


def test_the_cli_validates_json_and_toml_configs_without_the_extra(tmp_path, monkeypatch, capsys):
    """The shared validation entry is standard-library code: adding it to the `validate`/`run` path
    must not make either of them want PyYAML. The same config is checked (and refused) either way."""
    _without_pyyaml(monkeypatch)
    good = _write(tmp_path, "good.json", JSON_CONFIG)

    assert main(["validate", "-c", str(good)]) == 0
    capsys.readouterr()

    bad_json = _write(
        tmp_path,
        "bad.json",
        '{"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "run": {"concurency": 4}}',
    )
    bad_toml = _write(
        tmp_path,
        "bad.toml",
        """
        [pipeline]
        name = "x"

        [[pipeline.tasks]]
        use = "echo"

        [run]
        concurency = 4
        """,
    )

    for cfg in (bad_json, bad_toml):
        assert main(["validate", "-c", str(cfg)]) == 2
        err = capsys.readouterr().err
        assert "Config error" in err
        assert "concurency" in err
