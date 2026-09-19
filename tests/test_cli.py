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
import sqlite3
import textwrap
from pathlib import Path
from typing import ClassVar

import pytest

from pyattacker import SqliteStore
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


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
def test_run_warns_about_unresolved_env_instead_of_ignoring_it(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("CLI_TEST_DEFINITELY_UNSET", raising=False)
    cfg = _write_config(tmp_path, "spec.yaml", RUN_CONFIG_WITH_MISSING_ENV)
    db = tmp_path / "run.db"

    rc = main(["run", "-c", str(cfg), "--store", str(db)])

    assert rc == 0
    assert "Warning: unresolved environment variables ['CLI_TEST_DEFINITELY_UNSET']" in capsys.readouterr().err


@pytest.mark.requires_yaml
def test_run_strict_env_fails_fast_on_a_missing_variable(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("CLI_TEST_DEFINITELY_UNSET", raising=False)
    cfg = _write_config(tmp_path, "spec.yaml", RUN_CONFIG_WITH_MISSING_ENV)
    db = tmp_path / "run.db"

    rc = main(["run", "-c", str(cfg), "--store", str(db), "--strict-env"])

    assert rc == 2
    assert "CLI_TEST_DEFINITELY_UNSET" in capsys.readouterr().err
    assert not db.exists()  # failed during load_spec, before the run ever started


@pytest.mark.requires_yaml
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


@pytest.mark.requires_yaml
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


# ---------------------------------------------------------------------- watch


def test_watch_reads_an_existing_store_a_bounded_number_of_times(tmp_path, capsys):
    rc, db = _run_demo(tmp_path, pipelines=4, fail_rate=0.0, db_name="watch.db")
    assert rc == 0
    capsys.readouterr()  # discard the demo run's own summary() output

    rc = main(["watch", str(db), "--interval", "0", "--iterations", "2", "--no-clear"])

    assert rc == 0
    out = capsys.readouterr().out
    assert out.count("succeeded=4") == 2  # one render per iteration
    assert "in-flight=0" in out


def test_watch_missing_store_returns_config_error(tmp_path, capsys):
    missing = tmp_path / "nope.db"

    rc = main(["watch", str(missing), "--iterations", "1"])

    assert rc == 2
    assert "Config error" in capsys.readouterr().err


# ---------------------------------------------------------------------- plugins


def test_plugins_reports_none_installed_by_default(capsys):
    rc = main(["plugins"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "no plugins installed" in out
    assert "pyattacker.tasks" in out


def test_plugins_json_output_is_parseable(capsys):
    rc = main(["plugins", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"plugins": [], "errors": {}}


# ------------------------------------------------------------------------ serve


class _FakeStatsServer:
    """Records the wiring _cmd_serve does, without ever binding a real socket."""

    instances: ClassVar[list["_FakeStatsServer"]] = []

    def __init__(self, store, *, host="127.0.0.1", port=8787, run_id=None, errors=10):
        self.store_spec = store
        self.host = host
        self.port = port
        self.run_id = run_id
        self.errors = errors
        self.calls: list[str] = []
        self.url = f"http://{host}:{port}"
        type(self).instances.append(self)

    def start(self):
        self.calls.append("start")
        return self

    def wait(self):
        self.calls.append("wait")

    def stop(self):
        self.calls.append("stop")


def test_serve_wires_argparse_options_into_the_stats_server(tmp_path, capsys, monkeypatch):
    """Through the real CLI entry point: verifies _cmd_serve's own wiring (host/port/run_id ->
    StatsServer, and the start/wait/stop lifecycle), not just that StatsServer itself works.
    """
    rc, db = _run_demo(tmp_path, pipelines=3, fail_rate=0.0, db_name="serve.db")
    assert rc == 0

    _FakeStatsServer.instances = []
    monkeypatch.setattr("pyattacker.cli.StatsServer", _FakeStatsServer)

    rc = main(
        [
            "serve",
            str(db),
            "--host",
            "127.0.0.1",
            "--port",
            "12345",
            "--run-id",
            "some-run",
        ]
    )

    assert rc == 0
    assert len(_FakeStatsServer.instances) == 1
    fake = _FakeStatsServer.instances[0]
    assert fake.store_spec == str(db)
    assert fake.host == "127.0.0.1"
    assert fake.port == 12345
    assert fake.run_id == "some-run"
    assert fake.calls == ["start", "wait", "stop"]

    out = capsys.readouterr().out
    assert f"serving {db} at {fake.url}" in out
    assert f"{fake.url}/stats" in out


def test_serve_binds_the_requested_port_and_stops_cleanly(tmp_path, capsys):
    rc, db = _run_demo(tmp_path, pipelines=3, fail_rate=0.0, db_name="serve.db")
    assert rc == 0

    from pyattacker.server import StatsServer

    # a real StatsServer, exercised the same way (start/fetch/stop), independent of _cmd_serve's own wiring
    server = StatsServer(str(db), host="127.0.0.1", port=0).start()
    try:
        assert server.port > 0
        import urllib.request

        with urllib.request.urlopen(f"{server.url}/healthz", timeout=5.0) as response:
            assert response.status == 200
    finally:
        server.stop()


def test_serve_missing_store_returns_config_error(tmp_path, capsys):
    missing = tmp_path / "nope.db"

    rc = main(["serve", str(missing), "--port", "0"])

    assert rc == 2
    assert "Config error" in capsys.readouterr().err


# --------------------------------------------------------------------- --progress


@pytest.mark.requires_yaml
def test_run_with_progress_flag_completes_without_error(tmp_path, capsys):
    """``--progress`` opens a second read-only connection to the same store from a background
    thread while the run is in flight; it must not raise and must not stop the run from finishing.
    """
    cfg = _write_config(tmp_path, "spec.yaml", VALID_CONFIG)
    db = tmp_path / "progress.db"

    rc = main(["run", "-c", str(cfg), "--store", str(db), "--progress"])

    assert rc == 0
    assert db.exists()
    err = capsys.readouterr().err
    assert "Traceback" not in err


# ------------------------------- run config mapping, CLI overrides and preflight validation
#
# The three modes of "run" (one process, one manual shard, auto-sharded children) all have to end
# up with the same backend / write-behind configuration, and the only honest place to check that is
# the persisted run record: `runs.config_json` is what the issue measured and what an operator
# reads back afterwards.

def _write_json_config(tmp_path: Path, name: str, config: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def _shard_config(**run: object) -> dict:
    """A complete JSON config: a declared pool, one pipeline, six seeds to split over two shards."""
    return {
        "pools": {"apis": {"capacity": 1, "resources": [{"id": "api-1"}]}},
        "pipeline": {"name": "backend-check", "resource": "apis", "tasks": [{"use": "echo"}]},
        "run": {"concurrency": 1, **run},
        "source": {"kind": "range", "n": 6},
    }


def _run_manifest(db: Path) -> list[dict]:
    """Every persisted run record's config, read from ``runs.config_json`` with the stdlib."""
    import sqlite3

    connection = sqlite3.connect(str(db))
    try:
        rows = connection.execute("SELECT config_json FROM runs").fetchall()
    finally:
        connection.close()
    return [json.loads(row[0]) for row in rows]


BACKEND_CONFIG = """
pools:
  apis:
    capacity: 1
    resources:
      - id: api-1
pipeline:
  name: backend-check
  resource: apis
  tasks:
    - use: echo
run:
  concurrency: 1
  artifact_backend: "null"
  write_batch: 7
  flush_interval: 0.25
"""

BACKEND_FORM_CONFIG = """
pools:
  apis:
    capacity: 1
    resources:
      - id: api-1
pipeline:
  name: backend-form
  resource: apis
  tasks:
    - use: echo
run:
  concurrency: 1
  artifact_backend: {value}
"""


@pytest.mark.requires_yaml
@pytest.mark.parametrize(
    ("value", "expected"),
    [("null", "inline"), ('"null"', "null"), ("inline", "inline")],
    ids=["bare-null-is-inline", "quoted-null-is-the-null-backend", "inline"],
)
def test_artifact_backend_forms_mean_what_they_say(tmp_path, value, expected):
    """Pin the spelling: in YAML a bare ``null`` is the null *value*, which
    ``resolve_backend(None)`` documents as the same thing as ``inline``; the *string* ``"null"`` is
    what selects the hash-only null backend. The bug was that the field never reached RunConfig at
    all, so every spelling was recorded as ``inline``.
    """
    cfg = _write_config(tmp_path, "form.yaml", BACKEND_FORM_CONFIG.format(value=value))
    db = tmp_path / "form.db"

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0

    (manifest,) = _run_manifest(db)
    assert manifest["artifact_backend"] == expected


def test_mapping_and_json_backend_specs_are_accepted_without_touching_the_filesystem(tmp_path):
    """The accepted side of the backend invariant, and the one place `validate` could grow a side
    effect: a mapping spec (and its JSON-string spelling) must validate, the real `run` must
    construct it, and `validate` itself must not create the directory the backend owns."""
    forms = [
        ({"kind": "file", "root": str(tmp_path / "mapping")}, "file", tmp_path / "mapping"),
        ({"kind": "file", "root": str(tmp_path / "json"), "min_bytes": 1}, "file", tmp_path / "json"),
        ({"kind": "null"}, "null", None),
        (json.dumps({"kind": "file", "root": str(tmp_path / "string")}), "file", tmp_path / "string"),
    ]
    for index, (backend, expected, root) in enumerate(forms):
        cfg = _write_json_config(tmp_path, f"backend-ok-{index}.json", _shard_config(artifact_backend=backend))
        db = tmp_path / f"backend-ok-{index}.db"

        assert main(["validate", "-c", str(cfg)]) == 0
        if root is not None:
            assert not Path(root).exists(), "validate must not construct an artifact backend"

        assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0
        if root is not None:
            assert Path(root).is_dir(), "the run must have constructed the configured backend"
        (manifest,) = _run_manifest(db)
        assert manifest["artifact_backend"] == expected


# Every malformed backend declaration is asserted through *both* `validate` and `run`: the two share
# the check, so a spec `validate` accepts has to be constructible by `resolve_backend`, or the
# precheck sends the operator on with a config that dies during store setup.
MALFORMED_BACKENDS = [
    ({"kind": "file"}, "backend requires ['root']", "file-no-root"),
    ({"kind": "file", "root": 5}, "run.artifact_backend.root", "file-root-not-a-string"),
    ({"kind": "file", "root": ""}, "run.artifact_backend.root", "file-root-empty"),
    ({"kind": "file", "root": "/data", "min_bytes": "big"}, "run.artifact_backend.min_bytes", "min-bytes-string"),
    ({"kind": "file", "root": "/data", "min_bytes": -1}, "run.artifact_backend.min_bytes", "min-bytes-negative"),
    ({"kind": "file", "root": "/data", "bse": 1}, "unknown field(s) 'bse'", "unknown-field"),
    ({"kind": "nosuch", "root": "/data"}, "unknown artifact backend", "unknown-kind"),
    ("{not json", "is not valid JSON", "json-string-unparseable"),
    ('{"kind": "file"}', "backend requires ['root']", "json-string-no-root"),
    ('{"kind": "nosuch", "root": "/data"}', "unknown artifact backend", "json-string-unknown-kind"),
]


@pytest.mark.parametrize(
    ("backend", "fragment"),
    [(backend, fragment) for backend, fragment, _ in MALFORMED_BACKENDS],
    ids=[name for _, _, name in MALFORMED_BACKENDS],
)
def test_malformed_artifact_backends_are_refused_by_validate_and_by_run(tmp_path, capsys, backend, fragment):
    cfg = _write_json_config(tmp_path, "backend-bad.json", _shard_config(artifact_backend=backend))
    db = tmp_path / "backend-bad.db"
    capsys.readouterr()  # discard earlier output

    assert main(["validate", "-c", str(cfg)]) == 2
    assert fragment in capsys.readouterr().err

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 2
    assert fragment in capsys.readouterr().err
    assert not db.exists()  # refused before a store file exists


# `pipeline:` used to be dereferenced before it was checked, so a non-mapping section escaped as an
# AttributeError from `pipe_cfg.get("tasks")` -- exit 1 with a traceback from both commands.
MALFORMED_PIPELINES = [
    ("hello", "pipeline", "scalar"),
    ([1, 2], "pipeline", "list"),
    (5, "pipeline", "number"),
    ([], "pipeline", "empty-list"),
    ({}, "pipeline", "empty-mapping"),
    ({"tasks": "nope"}, "pipeline.tasks", "tasks-not-a-list"),
]


@pytest.mark.parametrize(
    ("pipeline", "fragment"),
    [(pipeline, fragment) for pipeline, fragment, _ in MALFORMED_PIPELINES],
    ids=[name for _, _, name in MALFORMED_PIPELINES],
)
def test_a_malformed_pipeline_section_is_a_config_error_not_a_traceback(tmp_path, capsys, pipeline, fragment):
    cfg = _write_json_config(tmp_path, "pipeline-shape.json", {"pipeline": pipeline})
    db = tmp_path / "pipeline-shape.db"
    capsys.readouterr()

    assert main(["validate", "-c", str(cfg)]) == 2
    err = capsys.readouterr().err
    assert fragment in err
    assert "Traceback" not in err

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 2
    err = capsys.readouterr().err
    assert fragment in err
    assert "Traceback" not in err
    assert not db.exists()


@pytest.mark.requires_yaml
def test_config_backend_and_no_write_behind_reach_the_run_record(tmp_path):
    """The issue's first symptom, verbatim: a config that asks for the null backend plus
    ``--no-write-behind`` used to be recorded as ``artifact_backend: inline`` and
    ``write_behind: true`` — the CLI filtered the config field out and never applied the flag.
    """
    cfg = _write_config(tmp_path, "backend.yaml", BACKEND_CONFIG)
    db = tmp_path / "backend.db"

    assert main(["run", "-c", str(cfg), "--store", str(db), "--no-write-behind"]) == 0

    (manifest,) = _run_manifest(db)
    assert manifest["artifact_backend"] == "null"
    assert manifest["write_behind"] is False


@pytest.mark.requires_yaml
def test_config_batch_write_knobs_take_effect_without_a_flag(tmp_path):
    """``write_behind``/``write_batch``/``flush_interval`` are config fields like any other: with no
    flag at all they must reach the RunConfig the store is opened with."""
    cfg = _write_config(tmp_path, "backend.yaml", BACKEND_CONFIG)
    db = tmp_path / "batched.db"

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0

    (manifest,) = _run_manifest(db)
    assert manifest["artifact_backend"] == "null"
    assert manifest["write_behind"] is True
    assert manifest["write_batch"] == 7
    assert manifest["flush_interval"] == 0.25


def test_manual_shard_applies_the_config_backend_and_the_no_write_behind_flag(tmp_path):
    cfg = _write_json_config(tmp_path, "manual.json", _shard_config(artifact_backend="null"))
    db = tmp_path / "manual.db"

    rc = main(["run", "-c", str(cfg), "--shard", "1/2", "--store", str(db), "--no-write-behind"])

    assert rc == 0
    (manifest,) = _run_manifest(db)
    assert manifest["artifact_backend"] == "null"
    assert manifest["write_behind"] is False


def test_auto_shards_pass_the_backend_and_write_behind_to_every_child(tmp_path, capsys):
    """The issue's second symptom: ``run --shards 2 --artifact-backend null`` spawned children
    whose command line did not carry the flag, so every child recorded the default inline backend.
    """
    cfg = _write_json_config(tmp_path, "auto.json", _shard_config())
    base = tmp_path / "auto.db"

    rc = main(
        [
            "run",
            "-c",
            str(cfg),
            "--shards",
            "2",
            "--jobs",
            "2",
            "--store",
            str(base),
            "--artifact-backend",
            "null",
            "--no-write-behind",
        ]
    )

    assert rc == 0
    for index in range(2):
        manifests = _run_manifest(tmp_path / f"auto.shard{index}of2.db")
        assert manifests, f"shard {index} left no run record"
        assert all(manifest["artifact_backend"] == "null" for manifest in manifests)
        assert all(manifest["write_behind"] is False for manifest in manifests)
    assert "shard 0/2" in capsys.readouterr().out


def test_cli_flags_override_the_config_run_block(tmp_path):
    cfg = _write_json_config(
        tmp_path,
        "precedence.json",
        _shard_config(concurrency=2, artifact_backend="inline", write_behind=True),
    )
    db = tmp_path / "precedence.db"

    rc = main(
        [
            "run",
            "-c",
            str(cfg),
            "--store",
            str(db),
            "--concurrency",
            "3",
            "--artifact-backend",
            "null",
            "--no-write-behind",
        ]
    )

    assert rc == 0
    (manifest,) = _run_manifest(db)
    assert manifest["concurrency"] == 3  # the flag, not run.concurrency: 2
    assert manifest["artifact_backend"] == "null"  # the flag, not run.artifact_backend: inline
    assert manifest["write_behind"] is False  # the flag, not run.write_behind: true


# Every entry is (config, the field path the error must name). A config that cannot run is a `2`
# before anything is created -- no Runner, no store file, no shard child.
INVALID_CONFIGS = [
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "run": {"concurency": 4}},
        "unknown field(s) 'concurency'",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "run": {"concurrency": 0}},
        "run.concurrency",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "run": {"heartbeat_s": -1}},
        "run.heartbeat_s",
    ),
    (
        {"pipeline": {"name": "x", "resource": "nosuchpool", "tasks": [{"use": "echo"}]}},
        "pipeline.resource",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo", "resource": "nosuchpool"}]}},
        "pipeline.tasks[0].resource",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo", "tieout": 5}]}},
        "pipeline.tasks[0]",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo", "retry": {"max_attempts": 0}}]}},
        "pipeline.tasks[0].retry.max_attempts",
    ),
    (
        {"pools": {"apis": {"capcity": 2}}, "pipeline": {"name": "x", "tasks": [{"use": "echo"}]}},
        "pools.apis",
    ),
    (
        {
            "pools": {"apis": {"algorithm": "nosuchalgorithm"}},
            "pipeline": {"name": "x", "resource": "apis", "tasks": [{"use": "echo"}]},
        },
        "pools.apis.algorithm",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "source": {"kind": "nosuchkind"}},
        "source.kind",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "source": {"kind": "jsonl"}},
        "source.path",
    ),
    (
        {"pipeline": {"name": "x", "tasks": [{"use": "echo"}]}, "source": {"kind": "jsonl", "path": "d.jsonl", "limit": None}},
        "source.limit",
    ),
    (
        {"pools": {"apis": {"capacity": None}}, "pipeline": {"name": "x", "tasks": [{"use": "echo"}]}},
        "pools.apis.capacity",
    ),
    (
        {"pipeline": {"name": "x", "include_code": None, "tasks": [{"use": "echo"}]}},
        "pipeline.include_code",
    ),
]


@pytest.mark.parametrize(("config", "field_path"), INVALID_CONFIGS, ids=[item[1] for item in INVALID_CONFIGS])
def test_invalid_configs_are_refused_before_anything_runs(tmp_path, capsys, config, field_path):
    """`validate` and `run` share one validation entry: the same config is a `2` on both, the error
    names the field path, and no store file (in any of the three modes) is ever created."""
    cfg = _write_json_config(tmp_path, "invalid.json", config)
    db = tmp_path / "invalid.db"

    assert main(["validate", "-c", str(cfg)]) == 2
    err = capsys.readouterr().err
    assert field_path in err, err
    assert "Config error" in err

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 2
    assert field_path in capsys.readouterr().err
    assert not db.exists()

    assert main(["run", "-c", str(cfg), "--shards", "2", "--store", str(db)]) == 2
    assert not db.exists()
    assert not (tmp_path / "invalid.shard0of2.db").exists()



# ------------------------------------------------- terminal-repair failure visibility
# JSON on purpose: this test must run in the bare install too, where the `yaml` extra is absent.
REPAIR_CONFIG = json.dumps(
    {
        "pools": {
            "apis": {
                "kind": "llm",
                "capacity": 2,
                "algorithm": "backoff",
                "resources": [{"id": "api-1", "options": {"model": "gpt-4o"}}],
            }
        },
        "pipeline": {"name": "cli-repair", "resource": "apis", "tasks": [{"use": "echo"}]},
        "run": {"concurrency": 2, "label": "cli-repair"},
    }
)


class _FailingSettleStore(SqliteStore):
    """A store whose terminal settle keeps failing, like a disk that stays full."""

    def settle_pipeline(self, pipeline_id, *, state, n_tasks_done, run_id):  # type: ignore[override]
        raise RuntimeError("injected store failure while settling the terminal row")


def _failing_open_store(spec, **kwargs):
    return _FailingSettleStore(
        str(spec), journal=kwargs.get("journal", "full"), backend=kwargs.get("backend")
    )


def _poison_terminal_row(db: Path) -> str:
    """Leave the `failed` + cursor == n_tasks row a failed final write creates, owned by an old run."""
    store = SqliteStore(str(db))
    pid = store.pipelines()[0].pipeline_id
    total = store.get_pipeline(pid).n_tasks_total
    store.close()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE pipelines SET state='failed', n_tasks_done=?, run_id='run-original', "
            "error_type='RuntimeError', error_message='original torn finalization', failed_task='mock.echo' "
            "WHERE pipeline_id=?",
            (total, pid),
        )
    return pid


def test_a_failed_terminal_repair_is_visible_to_the_caller_and_exits_non_zero(
    tmp_path, monkeypatch, capsys
):
    """The row keeps its original failure, but the run must not be able to look successful."""
    db = tmp_path / "repair.db"
    cfg = _write_config(tmp_path, "repair.json", REPAIR_CONFIG)
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0
    pid = _poison_terminal_row(db)

    monkeypatch.setattr("pyattacker.runner.open_store", _failing_open_store)
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 1

    out = capsys.readouterr().out
    assert "repair failures: 1" in out
    store = SqliteStore(str(db))
    row = store.get_pipeline(pid)
    # storage provenance is untouched: the row still describes the original failure and owning run
    assert (row.state, row.error_type, row.failed_task, row.run_id) == (
        "failed",
        "RuntimeError",
        "mock.echo",
        "run-original",
    )
    assert row.error_message == "original torn finalization"
    assert "pipeline.terminal_repair_failed" in [e.kind for e in store.events(pipeline_id=pid, limit=50)]
    store.close()

    # and the machine-readable summary carries the run-local count too
    assert main(["run", "-c", str(cfg), "--store", str(db), "--summary-format", "json"]) == 1
    # the JSON summary is the last line of stdout (the run may have warned before it)
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["repair_failures"] == 1
    assert payload["pipelines"]["by_state"] == {}


def test_posthoc_report_shows_repair_failures(
    tmp_path, monkeypatch, capsys
):
    """Issue #46: a post-hoc `report` must surface failed repair attempts from the event log."""
    db = tmp_path / "repair.db"
    cfg = _write_config(tmp_path, "repair.json", REPAIR_CONFIG)
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 0
    pid = _poison_terminal_row(db)

    # Inject a failing repair attempt
    monkeypatch.setattr("pyattacker.runner.open_store", _failing_open_store)
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 1

    # The run-local counter worked, but now we close and re-open the store
    # (post-hoc view) and check that stats() and events(kind=...) expose the failure.
    store = SqliteStore(str(db))
    stats = store.stats()
    assert stats["repair_failures"] == 1

    # events(kind=...) filters correctly
    repair_events = store.events(kind="pipeline.terminal_repair_failed")
    assert len(repair_events) == 1
    assert repair_events[0].pipeline_id == pid

    # Other kinds are excluded
    all_events = store.events()
    assert len(all_events) > len(repair_events)

    store.close()

    # Now run `pyattacker report` on the store and check it shows the repair failure
    out = capsys.readouterr().out  # clear previous output
    assert main(["report", str(db)]) == 0
    out = capsys.readouterr().out
    assert "Terminal repair failures: 1" in out


# ---------------------------------------------------------- worker liveness (#44)
class _WorkerDied(BaseException):
    """A direct ``BaseException``: nothing in the worker's own handler chain can contain it."""


class _CrashingFinalStore(SqliteStore):
    """A store whose final-write hook dies, like a broken third-party backend."""

    def mark_final(self, pipeline_id, seq):  # type: ignore[override]
        raise _WorkerDied("store hook died mid-pipeline")


def _crashing_open_store(spec, **kwargs):
    return _CrashingFinalStore(
        str(spec), journal=kwargs.get("journal", "full"), backend=kwargs.get("backend")
    )


def test_a_worker_crash_exits_non_zero_with_a_named_error(tmp_path, monkeypatch, capsys):
    """Before worker supervision existed this shape hung forever: no error, no exit code, no record.

    The CLI now gets a named run-level error, and the pipeline that died keeps a terminal row.
    """
    db = tmp_path / "crash.db"
    cfg = _write_config(tmp_path, "crash.json", REPAIR_CONFIG)

    monkeypatch.setattr("pyattacker.runner.open_store", _crashing_open_store)
    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 2

    err = capsys.readouterr().err
    assert "WorkerCrashed" in err
    assert "_WorkerDied: store hook died mid-pipeline" in err

    store = SqliteStore(str(db))
    rows = store.pipelines()
    assert [row.state for row in rows] == ["failed"]
    assert rows[0].error_type == "WorkerCrashed"
    assert "runner.worker_crashed" in [e.kind for e in store.events(limit=50)]
    store.close()


# ------------------------------------------------- fresh restart (#52 review item 1)
def _fresh_restart_config(**run: object) -> dict:
    """A forward pipeline whose second task always fails, so the checkpoint is observable.

    ``boom`` is built with a fatal error, so it fails once per run without retrying: the run fails
    after checkpointing ``echo``, a later ``--resume`` continues at the second task, and
    ``--fresh-restart`` must re-run both.
    """
    return {
        "pipeline": {
            "name": "restart-check",
            "tasks": [{"use": "echo"}, {"use": "boom", "args": {"message": "down", "error": "fatal"}}],
        },
        "run": {"concurrency": 1, **run},
        "source": {"kind": "range", "n": 1},
    }


def _attempts_per_task(db: Path) -> dict[str, int]:
    """Attempt rows in the store, per task name — run-independent, so a resume is observable."""
    store = SqliteStore(str(db))
    try:
        counts: dict[str, int] = {}
        for attempt in store.attempts():
            counts[attempt.task_name] = counts.get(attempt.task_name, 0) + 1
        return counts
    finally:
        store.close()


def test_fresh_restart_flag_discards_a_forward_checkpoint(tmp_path, capsys):
    """`--fresh-restart` must reach RunConfig and change what a resume does.

    The same store is resumed twice: plain `--resume` keeps the checkpoint (the first task is not
    attempted again), `--resume --fresh-restart` starts over (both tasks run again).
    """
    cfg = _write_json_config(tmp_path, "restart.json", _fresh_restart_config())
    db = tmp_path / "restart.db"

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 1
    capsys.readouterr()
    assert _attempts_per_task(db) == {"mock.echo": 1, "mock.boom": 1}

    assert main(["resume", "-c", str(cfg), "--store", str(db)]) == 1
    capsys.readouterr()
    assert _attempts_per_task(db) == {"mock.echo": 1, "mock.boom": 2}  # checkpoint reused

    assert main(["resume", "-c", str(cfg), "--store", str(db), "--fresh-restart"]) == 1
    capsys.readouterr()
    assert _attempts_per_task(db) == {"mock.echo": 2, "mock.boom": 3}  # ran from the seed again

    store = SqliteStore(str(db))
    try:
        kinds = [e.kind for e in store.events(limit=50)]
        assert "pipeline.restarted" in kinds
        assert "pipeline.checkpoint_missing" not in kinds
    finally:
        store.close()


def test_fresh_restart_is_accepted_in_the_config_run_block(tmp_path, capsys):
    """The config surface is the same set as the flags (`RUN_FIELDS`), including the new switch."""
    cfg = _write_json_config(tmp_path, "restart-config.json", _fresh_restart_config(fresh_restart=True))
    db = tmp_path / "restart-config.db"

    assert main(["run", "-c", str(cfg), "--store", str(db)]) == 1
    capsys.readouterr()
    # The `run:` block applies on every run, so the resume restarts instead of resuming.
    assert main(["resume", "-c", str(cfg), "--store", str(db)]) == 1
    capsys.readouterr()
    assert _attempts_per_task(db) == {"mock.echo": 2, "mock.boom": 2}

    bad = _write_json_config(tmp_path, "restart-bad.json", _fresh_restart_config(fresh_restart="yes"))
    assert main(["validate", "-c", str(bad)]) == 2


def test_shard_children_inherit_fresh_restart():
    """A restart flag that does not travel to the shard children would restart only the parent's shard."""
    from pyattacker.cli import _child_argv

    class _Args:
        config = "cfg.json"
        fresh_restart = True
        retry_succeeded = False
        strict_leases = False
        no_write_behind = False
        no_signals = False
        strict_env = False
        artifact_backend = None
        journal = None
        label = None
        concurrency = None
        stop_after_failures = None

    argv = _child_argv(_Args(), 1, 2, "/tmp/shard.db", resume=True)
    assert "--fresh-restart" in argv

    _Args.fresh_restart = False
    assert "--fresh-restart" not in _child_argv(_Args(), 1, 2, "/tmp/shard.db", resume=True)
