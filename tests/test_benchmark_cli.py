"""`pyattacker bench`: the CLI surface of the benchmark, and the guarantees it inherits.

The benchmark is simulation-only, so these tests can afford to actually run it: a scenario trimmed to
a few dozen jobs takes milliseconds, and running the real command is what proves `--json`/`--markdown`
write what a reader expects.
"""

from __future__ import annotations

import json

from pyattacker.cli import main

SMALL = ["--jobs", "12", "--concurrency", "2", "--seeds", "1"]


def test_list_prints_scenarios_algorithms_and_metrics(capsys):
    assert main(["bench", "--list"]) == 0

    out = capsys.readouterr().out
    assert "bursty_provider" in out
    assert "quota_aware" in out
    assert "refusal_rate" in out
    assert "jobs/s" in out, "metrics are listed with their unit"


def test_a_small_run_prints_a_table(capsys):
    assert main(["bench", *SMALL, "--algorithms", "wait,immediate", "--quiet"]) == 0

    out = capsys.readouterr().out
    assert "wait" in out and "immediate" in out
    assert "jobs_done [jobs]" in out
    assert "best mean" in out


def test_progress_goes_to_stderr_and_the_table_to_stdout(capsys):
    assert main(["bench", *SMALL, "--algorithms", "wait"]) == 0

    captured = capsys.readouterr()
    assert "benchmarking 1 algorithms" in captured.err
    assert "jobs/s" in captured.err, "progress lines carry the headline numbers"
    assert "jobs_done [jobs]" in captured.out
    assert "benchmarking" not in captured.out, "the table is the only thing on stdout"


def test_json_output_is_a_report_a_program_can_read(tmp_path, capsys):
    target = tmp_path / "nested" / "report.json"

    assert main(["bench", *SMALL, "--algorithms", "wait", "--json", str(target), "--quiet"]) == 0

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["scenario"] == "bursty_provider"
    assert payload["seeds"] == [20260917]
    assert [run["algorithm"] for run in payload["runs"]] == ["wait"]
    assert "mean" in payload["aggregates"]["wait"]["jobs_done"]
    assert payload["winners"]["jobs_done"] == ["wait"], "one algorithm is trivially the winner"


def test_json_to_stdout_replaces_the_table(capsys):
    assert main(["bench", *SMALL, "--algorithms", "wait", "--json", "-", "--quiet"]) == 0

    out = capsys.readouterr().out
    assert json.loads(out)["scenario"] == "bursty_provider"
    assert "jobs_done [jobs]" not in out


def test_markdown_output_is_written(tmp_path):
    target = tmp_path / "results.md"

    assert main(["bench", *SMALL, "--algorithms", "wait,backoff", "--markdown", str(target), "--quiet"]) == 0

    text = target.read_text(encoding="utf-8")
    assert text.startswith("### `bursty_provider`")
    assert "| metric | `wait` | `backoff` | best |" in text
    assert "| `fast-flaky` |" in text or "Per-endpoint admissions" in text
    # the markdown table's separator has to match its header, or the table renders as text
    header = next(line for line in text.splitlines() if line.startswith("| metric |"))
    separator = next(line for line in text.splitlines() if line.startswith("|---"))
    assert header.count("|") == separator.count("|")


def test_overrides_reach_the_scenario(capsys):
    assert main(["bench", "--jobs", "5", "--concurrency", "1", "--horizon", "20", "--seeds", "1", "--algorithms", "wait", "--quiet"]) == 0

    out = capsys.readouterr().out
    assert "5 jobs, 3 steps, 2 calls each" in out
    assert "horizon 600s" not in out, "the override has to be visible in the summary line"


def test_an_unknown_scenario_is_a_config_error(capsys):
    assert main(["bench", "--scenario", "nope", "--quiet"]) == 2

    assert "unknown scenario" in capsys.readouterr().err


def test_an_unknown_algorithm_is_a_config_error(capsys):
    assert main(["bench", *SMALL, "--algorithms", "nope"]) == 2

    assert "unknown acquire algorithm" in capsys.readouterr().err


def test_the_reference_clock_mode_runs_the_same_scenario():
    assert main(["bench", "--jobs", "8", "--concurrency", "2", "--seeds", "1", "--algorithms", "wait", "--clock", "real", "--speedup", "50", "--quiet"]) == 0
