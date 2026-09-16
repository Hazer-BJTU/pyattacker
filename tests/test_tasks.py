"""Built-in utility tasks (pyattacker.tasks), currently just `shell_run`.

The centerpiece here is the injection regression for issue #5: a value coming out of an
upstream task (model/judge output in the typical use case) must never be able to break out of
its position and run as a separate shell command. `shlex.quote()` on a single substitution
turns out not to be enough — it is only safe when `{value}` lands as a whole, unquoted shell
token, and shell_run has no way to guarantee where in a user-written template it lands (inside
`'...'`, inside `"..."`, inside `$(...)`, ...). So string commands reject `{value}` outright, and
only the argv form (no shell involved at all) may reference it — via a literal `"{value}"`
substring replacement, not `str.format()`, so unrelated braces (a jq filter, a Python dict
literal) in another argv element are left alone.
"""

from __future__ import annotations

import json

import pytest

from pyattacker import Runner, pipeline
from pyattacker.errors import ConfigError
from pyattacker.tasks import shell_run

# a value that would break out of naive interpolation if not handled safely
_DANGEROUS = "foo'; touch {marker}; echo 'done"


def _run_one(spec, seed):
    runner = Runner(store=":memory:", handle_signals=False)
    try:
        report = runner.run(pipeline("shell", spec).map([seed]))
        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        row = next(iter(runner.store.export_rows()))
        return row["artifacts"][-1]["payload"]
    finally:
        runner.close()


def test_string_command_runs_and_captures_output():
    result = _run_one(shell_run("echo hello"), {"i": 0})
    assert result["returncode"] == 0
    assert result["stdout"].strip() == "hello"


def test_argv_command_runs_and_captures_output():
    result = _run_one(shell_run(["echo", "hello"]), {"i": 0})
    assert result["returncode"] == 0
    assert result["stdout"].strip() == "hello"


@pytest.mark.parametrize(
    "template",
    [
        "echo {value}",
        "echo '{value}'",
        'echo "{value}"',
        "python -c 'print({value})'",
    ],
)
def test_string_command_rejects_value_interpolation_in_any_quoting_context(template):
    """No placement is safe: quoting only protects an unquoted standalone token, and shell_run
    cannot know or enforce where in the caller's template the substitution lands."""
    with pytest.raises(ConfigError, match="may not interpolate"):
        shell_run(template)


def test_string_command_with_literal_braces_that_are_not_the_placeholder_is_unaffected():
    result = _run_one(shell_run("echo {not_value}"), {"i": 0})
    assert result["stdout"].strip() == "{not_value}"


def test_argv_command_treats_untrusted_value_as_one_literal_argument(tmp_path):
    marker = tmp_path / "pwned_argv"
    dangerous = _DANGEROUS.format(marker=marker)
    result = _run_one(shell_run(["echo", "{value}"]), dangerous)

    assert not marker.exists()
    assert result["returncode"] == 0
    assert result["stdout"].strip() == json.dumps(dangerous)


def test_argv_command_is_safe_even_when_the_value_contains_command_substitution(tmp_path):
    marker = tmp_path / "pwned_subst"
    dangerous = f"foo$(touch {marker})bar"
    result = _run_one(shell_run(["echo", "{value}"]), dangerous)

    assert not marker.exists()
    assert result["stdout"].strip() == json.dumps(dangerous)


def test_argv_substitution_is_a_literal_swap_not_str_format():
    """Another argv element's own braces (a jq filter, a Python dict literal) must be left
    alone — only the exact substring "{value}" is a placeholder, never str.format() syntax."""
    result = _run_one(
        shell_run(["python3", "-c", "import sys; print(sys.argv[1], sys.argv[2])", '{"a": 1}', "{value}"]),
        "hi",
    )
    assert result["stdout"].strip() == '{"a": 1} "hi"'


def test_check_raises_a_retryable_error_on_nonzero_exit():
    runner = Runner(store=":memory:", handle_signals=False)
    try:
        report = runner.run(pipeline("shell-fail", shell_run("exit 1")).map([{"i": 0}]))
        assert report.stats["pipelines"]["by_state"] == {"failed": 1}
        row = next(iter(runner.store.export_rows()))
        assert row["error_type"] == "RetryableError"
    finally:
        runner.close()


def test_check_false_returns_the_failed_result_instead_of_raising():
    result = _run_one(shell_run("exit 1", check=False), {"i": 0})
    assert result["returncode"] == 1
