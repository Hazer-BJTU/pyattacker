"""``TaskSpec.with_overrides`` —— the override contract, and the sentinel that makes it usable.

The rule the rest of the framework now relies on:

* a keyword that is **not passed** keeps the current value;
* an explicit ``None`` **clears** a field that supports being empty
  (``resource``, ``algorithm``, ``timeout_s``, ``version``);
* ``UNSET`` means "not passed" even when the key is present, so a builder that always produces
  the same keys (the declarative loader) can forward its dict without wiping everything the
  configuration did not mention;
* ``None`` for a field that cannot be empty is a config error, not a silent no-op — a spec with
  no name or no retry policy would fail much later, somewhere less obvious.

Before this contract existed, every ``None`` was dropped, so there was no way to clear a
resource or an algorithm that a factory had declared.
"""

from __future__ import annotations

import pytest

from pyattacker import UNSET, task
from pyattacker.errors import ConfigError


@task(
    "defaults.task",
    resource="apis",
    algorithm="wait",
    timeout_s=5.0,
    version="v1",
    config={"model": "model-a"},
)
def defaults_task(value, ctx):
    return value


def test_omitted_keywords_keep_every_field():
    renamed = defaults_task.with_overrides(name="renamed")

    assert renamed.name == "renamed"
    assert renamed.resource == "apis"
    assert renamed.algorithm == "wait"
    assert renamed.timeout_s == 5.0
    assert renamed.version == "v1"
    assert dict(renamed.config) == {"model": "model-a"}


def test_explicit_none_clears_the_supported_fields():
    cleared = defaults_task.with_overrides(resource=None, algorithm=None, timeout_s=None, version=None)

    assert cleared.resource is None
    assert cleared.algorithm is None
    assert cleared.runtime_algorithm() is None
    assert cleared.timeout_s is None
    assert cleared.version is None
    # ... and the fields that were not mentioned are untouched by the clearing
    assert cleared.name == "defaults.task"
    assert dict(cleared.config) == {"model": "model-a"}


def test_unset_is_treated_as_not_passed():
    """The declarative layer builds one dict with every key; UNSET is how a key says "leave it"."""
    kept = defaults_task.with_overrides(name=UNSET, resource=UNSET, algorithm=UNSET, timeout_s=UNSET)

    assert kept == defaults_task  # frozen dataclass equality compares the declared fields


def test_clearing_a_field_that_cannot_be_empty_is_a_config_error():
    with pytest.raises(ConfigError, match="cannot be cleared"):
        defaults_task.with_overrides(name=None)
    with pytest.raises(ConfigError, match="cannot be cleared"):
        defaults_task.with_overrides(retry=None)
    with pytest.raises(ConfigError, match="cannot be cleared"):
        defaults_task.with_overrides(children=None)


def test_overrides_applied_to_a_factory_spec_do_not_wipe_its_own_retry():
    """Both forwarding paths must change only what they were actually given, or a factory's retry
    policy (``flaky(fail_times=1)``: two attempts) would be silently reset to the default."""
    from pyattacker import build_task_spec
    from pyattacker.tasks import flaky

    spec = flaky(fail_times=1)
    original = spec.retry
    assert original.max_attempts == 2

    renamed = spec.with_overrides(name="renamed")

    assert renamed.name == "renamed"
    assert renamed.retry == original

    rebuilt = build_task_spec(spec, name="rebuilt")

    assert rebuilt.name == "rebuilt"
    assert rebuilt.retry == original
