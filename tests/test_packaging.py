"""Packaging: the metadata a release has to get right.

Cheap checks, but they catch the failure mode where the version in `pyproject.toml` and the one
the running code reports drift apart, or where a module is added to the package but never shipped.
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import pyattacker

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def project() -> dict:
    with open(ROOT / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)["project"]


def test_version_matches_between_package_and_metadata(project):
    assert pyattacker.__version__ == project["version"]
    assert pyattacker.__version__.count(".") == 2


def test_declared_dependencies_are_intentional(project):
    # The kernel is standard library only, and so is the declarative layer for JSON/TOML configs, so
    # the base install declares nothing. PyYAML is optional: only a .yaml/.yml config needs it, and
    # the loader raises a ConfigError naming the extra when it is absent. Anything in `dependencies`
    # would be paid for by every user of the SDK, including those who never write a config file.
    assert project["dependencies"] == []
    assert project["optional-dependencies"]["yaml"] == ["pyyaml>=6.0"]
    assert project["requires-python"] == ">=3.11"


def test_no_module_imports_yaml_at_import_time():
    """`import pyattacker` must not need the extra; reading a .yaml file is the only thing that may."""
    import re

    package_root = Path(pyattacker.__file__).parent
    offenders = sorted(
        path.name
        for path in package_root.rglob("*.py")
        # Column 0 only: the one legitimate import lives inside a function, and an indented import is
        # exactly what keeps the extra optional.
        if re.search(r"^(import yaml|from yaml)", path.read_text(encoding="utf-8"), re.MULTILINE)
    )
    assert offenders == [], f"these modules import yaml eagerly: {offenders}"


def test_console_script_and_module_entry_point(project):
    assert project["scripts"]["pyattacker"] == "pyattacker.cli:main"

    result = subprocess.run(
        [sys.executable, "-m", "pyattacker", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert pyattacker.__version__ in result.stdout


def test_every_source_module_is_importable():
    """A module that is not importable is a module that will not be in the wheel."""
    import importlib

    package_root = Path(pyattacker.__file__).parent
    modules = sorted(
        path.relative_to(package_root).with_suffix("").as_posix().replace("/", ".")
        for path in package_root.rglob("*.py")
        if "__pycache__" not in path.parts
    )
    assert len(modules) >= 20
    for name in modules:
        if name.endswith(".__main__"):
            continue
        importlib.import_module(f"pyattacker.{name}")


def test_public_api_is_exported():
    """Names the docs and examples promise must exist at the top level."""
    promised = [
        "Runner",
        "pipeline",
        "task",
        "Pool",
        "Resource",
        "Retrying",
        "Artifact",
        "Codec",
        "build_task_spec",
        "fanout",
        "merge_reports",
        "shard_specs",
        "iter_rows",
        "FileBackend",
        "PluginRegistry",
        "StatsServer",
        "load_spec",
    ]
    missing = [name for name in promised if not hasattr(pyattacker, name)]
    assert missing == []
    assert set(promised).issubset(set(pyattacker.__all__))
