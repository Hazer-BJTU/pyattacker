"""CLI entry tests: call ``pyattacker.cli.main(argv)`` directly, no subprocess.

Coverage
* ``demo --store <tmp>/d.db --pipelines 10 --fail-rate 0.2 --export <tmp>/out.jsonl``
  → exit code 0, the db is written to disk, the export has exactly 10 valid JSON lines with the right structure
* ``report <db>`` / ``report --json`` → exit code 0 (the db must already exist)
* ``export <db> <out>`` → exit code 0, the file is parseable, the number of lines equals the number of pipelines
* ``validate -c <cfg>`` → exit code 0, stdout is the describe() JSON
* config errors (missing file / missing pipeline section / task without use) → exit code 2
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

from pyattacker.cli import main

# demo is deliberately given a failure probability: by the exit code convention demo is always 0 (it is only a demo),
# but both terminal states must be able to appear/be tolerated among the 10 exported records.
DEMO_PIPELINES = 10
DEMO_FAIL_RATE = 0.2

VALID_CONFIG = """
pools:
  apis:
    kind: llm
    capacity: 2
    algorithm: backoff
    resources:
      - id: api-1
        options: {model: gpt-4o}
pipeline:
  name: cli-check
  resource: apis
  tasks:
    - use: echo
run:
  concurrency: 2
  label: cli-run
"""


def _run_demo(
    tmp_path: Path,
    *,
    pipelines: int = DEMO_PIPELINES,
    fail_rate: float = DEMO_FAIL_RATE,
    export: Path | None = None,
    db_name: str = "demo.db",
) -> tuple[int, Path]:
    db = tmp_path / db_name
    argv = [
        "demo",
        "--store",
        str(db),
        "--pipelines",
        str(pipelines),
        "--fail-rate",
        str(fail_rate),
    ]
    if export is not None:
        argv += ["--export", str(export)]
    return main(argv), db


def _write_config(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


# ---------------------------------------------------------------------- demo


def test_demo_returns_zero_exports_ten_json_lines_and_creates_db(tmp_path):
    out = tmp_path / "out.jsonl"
    rc, db = _run_demo(tmp_path, export=out)

    assert rc == 0
    assert db.exists()
    assert db.stat().st_size > 0

    assert out.exists()
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == DEMO_PIPELINES

    rows = [json.loads(line) for line in lines]  # every line is valid JSON
    assert all(isinstance(row, dict) for row in rows)
    assert len({row["pipeline_id"] for row in rows}) == DEMO_PIPELINES
    assert {row["name"] for row in rows} == {"demo.qa"}
    assert all(row["run_id"].startswith("run-") for row in rows)
    assert {row["state"] for row in rows} <= {"succeeded", "failed"}
    assert all(1 <= len(row["tasks"]) <= 4 for row in rows)  # a failure aborts the remaining tasks early
    assert all(row["artifacts"] for row in rows)

    # journal=full: the exported artifact payloads are already decoded into JSON objects
    assert all(any(a["payload"] is not None for a in row["artifacts"]) for row in rows)

    succeeded = [row for row in rows if row["state"] == "succeeded"]
    assert succeeded  # fail-rate 0.2 will not make all 10 fail
    for row in succeeded:
        assert len(row["tasks"]) == 4
        assert row["n_tasks_done"] == 4
        assert row["artifacts"][-1]["is_final"] is True
    for row in rows:
        if row["state"] == "failed":
            assert row["error_type"] == "RetryableError"  # the demo only simulates retryable failures
            assert row["error_message"]
            assert row["n_tasks_done"] < 4


# -------------------------------------------------------------------- report


def test_report_returns_zero_for_existing_db(tmp_path):
    rc, db = _run_demo(tmp_path, pipelines=6, fail_rate=0.0, db_name="report.db")
    assert rc == 0
    assert db.exists()

    assert main(["report", str(db)]) == 0
    assert main(["report", str(db), "--json", "--errors", "3"]) == 0


# -------------------------------------------------------------------- export


def test_export_returns_zero_and_writes_parseable_jsonl(tmp_path):
    rc, db = _run_demo(tmp_path, pipelines=6, fail_rate=0.0, db_name="export.db")
    assert rc == 0
    assert db.exists()

    out = tmp_path / "exported.jsonl"
    assert main(["export", str(db), str(out)]) == 0
    assert out.exists()

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 6
    assert {row["name"] for row in rows} == {"demo.qa"}
    assert {row["state"] for row in rows} == {"succeeded"}  # fail-rate=0
    assert all(row["n_tasks_done"] == 4 for row in rows)


# ------------------------------------------------------------------ validate


def test_validate_returns_zero_and_prints_describe_json(tmp_path, capsys):
    cfg = _write_config(tmp_path, "spec.yaml", VALID_CONFIG)

    assert main(["validate", "-c", str(cfg)]) == 0

    captured = capsys.readouterr()
    described = json.loads(captured.out)
    assert described["config"] == str(cfg)
    assert described["pipeline"]["name"] == "cli-check"
    assert described["pipeline"]["tasks"] == ["mock.echo"]
    assert described["pools"]["apis"] == {
        "kind": "llm",
        "resources": 1,
        "capacity": 2,
        "algorithm": "backoff",
    }
    assert described["run"] == {"concurrency": 2, "label": "cli-run"}
    assert described["unresolved_env"] == []
    assert "Warning" not in captured.err


# ------------------------------------------------ unresolved ${ENV} outside validate

RUN_CONFIG_WITH_MISSING_ENV = """
pipeline:
  name: env-check
  tasks:
    - use: echo
run:
  concurrency: 1
  label: "${CLI_TEST_DEFINITELY_UNSET}"
"""


def test_run_warns_about_unresolved_env_instead_of_ignoring_it(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("CLI_TEST_DEFINITELY_UNSET", raising=False)
    cfg = _write_config(tmp_path, "spec.yaml", RUN_CONFIG_WITH_MISSING_ENV)
    db = tmp_path / "run.db"

    rc = main(["run", "-c", str(cfg), "--store", str(db)])

    assert rc == 0
    assert "Warning: unresolved environment variables ['CLI_TEST_DEFINITELY_UNSET']" in capsys.readouterr().err


def test_run_strict_env_fails_fast_on_a_missing_variable(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("CLI_TEST_DEFINITELY_UNSET", raising=False)
    cfg = _write_config(tmp_path, "spec.yaml", RUN_CONFIG_WITH_MISSING_ENV)
    db = tmp_path / "run.db"

    rc = main(["run", "-c", str(cfg), "--store", str(db), "--strict-env"])

    assert rc == 2
    assert "CLI_TEST_DEFINITELY_UNSET" in capsys.readouterr().err
    assert not db.exists()  # failed during load_spec, before the run ever started


def test_run_resume_shares_the_same_env_warning_path(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("CLI_TEST_DEFINITELY_UNSET", raising=False)
    cfg = _write_config(tmp_path, "spec.yaml", RUN_CONFIG_WITH_MISSING_ENV)
    db = tmp_path / "run.db"
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0
    capsys.readouterr()

    rc = main(["resume", "-c", str(cfg), "--store", str(db)])

    assert rc == 0
    assert "Warning: unresolved environment variables ['CLI_TEST_DEFINITELY_UNSET']" in capsys.readouterr().err


# ------------------------------------------------------------- config errors → 2


def test_config_errors_return_exit_code_two(tmp_path, capsys):
    missing = tmp_path / "missing.yaml"
    assert main(["validate", "-c", str(missing)]) == 2
    assert "Config error" in capsys.readouterr().err

    no_pipeline = _write_config(tmp_path, "no_pipeline.yaml", "run: {label: x}\n")
    assert main(["validate", "-c", str(no_pipeline)]) == 2
    assert "pipeline" in capsys.readouterr().err

    no_use = _write_config(
        tmp_path,
        "no_use.yaml",
        """
        pipeline:
          name: x
          tasks:
            - name: oops
        """,
    )
    assert main(["run", "-c", str(no_use)]) == 2  # it fails during load_spec, so it never actually starts
    assert "use" in capsys.readouterr().err
