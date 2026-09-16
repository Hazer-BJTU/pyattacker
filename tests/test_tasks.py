"""Built-in utility tasks (pyattacker.tasks), currently just `shell_run`.

The centerpiece here is the injection regression for issue #5: a value coming out of an
upstream task (model/judge output in the typical use case) must never be able to break out of
its position and run as a separate shell command, in either the string-template form or the
argv form.
"""

from __future__ import annotations

import json

from pyattacker import Runner, pipeline
from pyattacker.tasks import shell_run

# a value that would break out of naive `f"...{value}..."` interpolation if not handled safely
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


def test_string_command_quotes_untrusted_value_instead_of_letting_it_break_out(tmp_path):
    marker = tmp_path / "pwned_str"
    dangerous = _DANGEROUS.format(marker=marker)
    result = _run_one(shell_run("echo {value}"), dangerous)

    assert not marker.exists(), "the ';' must not have started a second command"
    assert result["returncode"] == 0
    # the value round-trips through the shell as one literal argument (JSON-encoded)
    assert result["stdout"].strip() == json.dumps(dangerous)


def test_argv_command_treats_untrusted_value_as_one_literal_argument(tmp_path):
    marker = tmp_path / "pwned_argv"
    dangerous = _DANGEROUS.format(marker=marker)
    result = _run_one(shell_run(["echo", "{value}"]), dangerous)

    assert not marker.exists()
    assert result["returncode"] == 0
    assert result["stdout"].strip() == json.dumps(dangerous)


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
