"""SQLite Suite catalog and bounded routing to experiment stores.

Checkpoints always commit within one data database. The catalog contains identity,
source state and admission history, never a second copy of a pipeline checkpoint.
"""

from __future__ import annotations

import dataclasses
import heapq
import json
import os
import tempfile
import time
from collections import Counter, OrderedDict
from collections.abc import Iterator, Sequence
from itertools import islice
from operator import itemgetter
from pathlib import Path
from typing import Any

from ..artifact import SEED_SEQ, SEED_TASK, Artifact
from ..backends import FileBackend
from ..errors import ConfigError
from ..reported_metrics import ReportedMetric, report_metric
from ..suite import namespace, validate_id
from .base import PipelineRecord, _validate_limit
from .sqlite import SqliteStore, _to_event, _to_handoff, _to_pipeline, _to_task
from .writebehind import WriteBehindStore

# Namespace length comes from the identity function, not a duplicated digest width.
# This exact expression is shared by indexes and predicates for combined layouts.
_PREFIX_SQL = f"substr(pipeline_id,1,{len(namespace('', ''))})"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS suite_experiments (
 experiment_id TEXT PRIMARY KEY, definition_digest TEXT NOT NULL, label TEXT NOT NULL,
 prefix TEXT UNIQUE NOT NULL, source_exhausted INTEGER NOT NULL DEFAULT 0,
 source_error TEXT, limited INTEGER NOT NULL DEFAULT 0, last_run_id TEXT, state TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS suite_experiment_runs (
 run_id TEXT NOT NULL, experiment_id TEXT NOT NULL, definition_digest TEXT NOT NULL,
 source_exhausted INTEGER NOT NULL DEFAULT 0, source_error TEXT,
 limited INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending', PRIMARY KEY (run_id, experiment_id)
);
CREATE TABLE IF NOT EXISTS suite_admissions (
 run_id TEXT NOT NULL, pipeline_id TEXT NOT NULL, experiment_id TEXT NOT NULL,
 local_key TEXT NOT NULL, repeat INTEGER NOT NULL,
 PRIMARY KEY (run_id, pipeline_id)
);
CREATE INDEX IF NOT EXISTS suite_admissions_pipeline ON suite_admissions(pipeline_id);
CREATE TABLE IF NOT EXISTS suite_identity (suite_id TEXT NOT NULL, experiment_id TEXT);
"""


def atomic_json(path: Path, value: Any) -> None:
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".manifest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class _RelativeFileBackend(FileBackend):
    """Layout-owned blobs can travel with their Suite directory."""

    def __init__(self, root: Path, *, read_only: bool = False) -> None:
        self.root = str(root.resolve())
        self.min_bytes = 0
        self.name = "file"
        if not read_only:
            root.mkdir(parents=True, exist_ok=True)

    def put(self, artifact: Any) -> str:
        super().put(artifact)
        return str(Path(artifact.digest[:2]) / artifact.digest[2:])


def _backend_spec(backend: Any) -> Any:
    if isinstance(backend, FileBackend):
        return {"kind": "file", "root": backend.root, "min_bytes": backend.min_bytes}
    if backend is None or isinstance(backend, (str, dict)):
        return backend
    raise ConfigError("Suite artifact backend must be a serializable built-in specification")


def _install_outcomes(store: SqliteStore) -> None:
    """Snapshot run outcomes in the SAME transaction as each checkpoint mutation.

    Triggers cover linear, handoff and visit writes, including terminal repair.
    The checkpoint keeps cumulative counters; the outcome keeps invocation counters.
    """
    conn = store._conn
    conn.execute("CREATE TABLE IF NOT EXISTS suite_pipeline_runs AS SELECT * FROM pipelines WHERE 0")
    columns = [row[1] for row in conn.execute("PRAGMA table_info(pipelines)")]
    if not any(
        row[1] == "baseline_attempts" for row in conn.execute("PRAGMA table_info(suite_pipeline_runs)")
    ):
        conn.execute(
            "ALTER TABLE suite_pipeline_runs ADD COLUMN baseline_attempts INTEGER NOT NULL DEFAULT 0"
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS suite_outcome_key ON suite_pipeline_runs(run_id,pipeline_id)"
    )
    now = "((julianday('now') - 2440587.5) * 86400.0)"
    special = {
        "created_at": "created_at",
        "started_at": f"CASE WHEN NEW.state='pending' THEN started_at ELSE COALESCE(started_at,NEW.finished_at,{now}) END",
        "finished_at": "CASE WHEN NEW.state IN ('pending','running') THEN NULL ELSE MAX(COALESCE(started_at,NEW.finished_at),NEW.finished_at) END",
        "attempts_total": "CASE WHEN NEW.attempts_total<OLD.attempts_total THEN 0 ELSE MAX(0,NEW.attempts_total-baseline_attempts) END",
    }
    assignments = ",".join(f'"{c}"={special.get(c, "NEW." + c)}' for c in columns)
    assignments += ",baseline_attempts=CASE WHEN NEW.attempts_total<OLD.attempts_total THEN NEW.attempts_total ELSE baseline_attempts END"
    conn.execute(f"""CREATE TRIGGER IF NOT EXISTS suite_outcome_update AFTER UPDATE ON pipelines BEGIN
        UPDATE suite_pipeline_runs SET {assignments}
        WHERE run_id=NEW.run_id AND pipeline_id=NEW.pipeline_id AND state IN ('pending','running');
        END""")
    conn.execute("CREATE TABLE IF NOT EXISTS suite_task_runs AS SELECT * FROM tasks WHERE 0")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS suite_task_outcome_key ON suite_task_runs(run_id,task_run_id)"
    )
    task_columns = [row[1] for row in conn.execute("PRAGMA table_info(tasks)")]
    names = ",".join(task_columns)
    values = ",".join("NEW." + c for c in task_columns)
    for operation in ("INSERT", "UPDATE"):
        conn.execute(f"""CREATE TRIGGER IF NOT EXISTS suite_task_{operation.lower()} AFTER {operation} ON tasks BEGIN
            INSERT OR REPLACE INTO suite_task_runs ({names}) VALUES ({values}); END""")
    # Suite recent-history views sort by event time, not insertion order. Keep
    # these indexes Suite-only so ordinary stores do not pay their write cost.
    for table, key in (("events", "event_id"), ("handoffs", "handoff_id")):
        for suffix, columns in (
            ("time", f"ts,{key}"),
            ("run_time", f"run_id,ts,{key}"),
            ("member_time", f"{_PREFIX_SQL},ts,{key}"),
            ("member_run_time", f"{_PREFIX_SQL},run_id,ts,{key}"),
        ):
            conn.execute(f"CREATE INDEX IF NOT EXISTS suite_{table}_{suffix} ON {table}({columns})")
    conn.execute("CREATE INDEX IF NOT EXISTS suite_events_pipeline_time ON events(pipeline_id,ts,event_id)")
    conn.commit()


class SuiteStore:
    """Both layouts use this Store extension; arbitrary custom stores are not supported.

    At most ``max_open`` experiment connections are held, in addition to the catalog.
    Readers open the same catalog read-only and never migrate it.
    """

    suite_store = True

    @classmethod
    def create(cls, suite: Any, **kwargs: Any) -> SuiteStore:
        backend_spec = _backend_spec(kwargs.get("backend"))
        root = Path(suite.output_root)
        manifest = root / "manifest.json"
        expected = {
            "version": 1,
            "suite_id": suite.id,
            "layout": suite.layout,
            "catalog": "state.db" if suite.layout == "combined" else "suite.db",
        }
        if manifest.exists():
            old = json.loads(manifest.read_text())
            if any(old.get(key) != value for key, value in expected.items()):
                raise ConfigError("existing output root has a different Suite identity or layout")
        else:
            if root.exists() and any(root.iterdir()):
                raise ConfigError("output.root is nonempty without a Suite manifest")
            root.mkdir(parents=True, exist_ok=True)
            # Publish only after creating the catalog. A failed initial creation is
            # explicit on reopening rather than silently adopting unrelated files.
            db = SqliteStore(str(root / expected["catalog"]))
            db._conn.executescript(_SCHEMA)
            db._conn.execute("INSERT INTO suite_identity VALUES (?,NULL)", (suite.id,))
            db._conn.commit()
            db.close()
            if backend_spec is not None:
                expected["artifact_backend"] = backend_spec
            atomic_json(manifest, expected)
        store = cls(str(root), **kwargs)
        try:
            store.register(suite)
        except BaseException:
            store.close()
            raise
        return store

    def __init__(
        self,
        root: str,
        *,
        read_only: bool = False,
        experiment: str | None = None,
        journal: str = "full",
        backend: Any = None,
        write_behind: bool | None = None,
        batch_size: int = 128,
        flush_interval: float = 1.0,
        max_open: int = 8,
    ) -> None:
        self.root = Path(root).resolve()
        self.path = str(self.root)
        self.read_only = read_only
        self.journal = journal
        self.experiment = experiment
        self.max_open = max(1, max_open)
        self._children: OrderedDict[str, Any] = OrderedDict()
        self._run = None
        self._selected: list[str] | None = None
        self._writer_guard = None
        self._batch = write_behind is not False
        self.batch_size = batch_size if self._batch else 0
        self.flush_interval = flush_interval
        self._backend = backend
        try:
            self.manifest = json.loads((self.root / "manifest.json").read_text())
        except (OSError, ValueError) as exc:
            raise ConfigError(f"cannot read Suite manifest at {root}: {exc}") from exc
        persisted_backend = self.manifest.get("artifact_backend")
        if backend is not None and _backend_spec(backend) != persisted_backend:
            raise ConfigError("artifact backend conflicts with Suite manifest; use a new output root")
        self._backend = persisted_backend
        self.suite_id = validate_id(self.manifest.get("suite_id"), "manifest.suite_id")
        self.layout = self.manifest.get("layout")
        expected = "state.db" if self.layout == "combined" else "suite.db"
        if (
            self.manifest.get("version") != 1
            or self.layout not in ("combined", "by_experiment")
            or self.manifest.get("catalog") != expected
        ):
            raise ConfigError("unsupported or invalid Suite manifest")
        catalog_path = self.root / expected
        if not catalog_path.is_file():
            raise ConfigError(f"missing Suite catalog: {catalog_path}")
        self.catalog = SqliteStore(
            str(catalog_path), read_only=read_only, journal=journal, backend=self._backend_for(self.root)
        )
        self.backend = self.catalog.backend
        self.data = (
            WriteBehindStore(self.catalog, batch_size=batch_size, flush_interval=flush_interval)
            if self._batch and not read_only
            else self.catalog
        )
        try:
            if not read_only:
                self._writer_guard = open(self.root / ".writer.lock", "a+b")  # noqa: SIM115 (owned until close)
                try:
                    if os.name == "nt":
                        import msvcrt

                        self._writer_guard.seek(0)
                        self._writer_guard.write(b"0")
                        self._writer_guard.flush()
                        self._writer_guard.seek(0)
                        msvcrt.locking(self._writer_guard.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(self._writer_guard.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise ConfigError("Suite output already has an active writer") from exc
            identity = self.catalog._conn.execute(
                "SELECT suite_id,experiment_id FROM suite_identity"
            ).fetchall()
            if len(identity) != 1 or tuple(identity[0]) != (self.suite_id, None):
                raise ConfigError("Suite catalog identity does not match manifest")
            if not read_only:
                _install_outcomes(self.catalog)
            self._reload()
            if experiment is not None and experiment not in self._members:
                raise ConfigError(f"unknown experiment: {experiment}")
            # Never create a replacement for a missing child while resuming.
            if self.layout == "by_experiment":
                for eid in self._members:
                    if not self._child_path(eid).is_file():
                        raise ConfigError(f"missing experiment database: {self._child_path(eid)}")
        except BaseException:
            self.close()
            raise

    def _backend_for(self, directory: Path, *, read_only: bool | None = None) -> Any:
        if self._backend == "file":
            return _RelativeFileBackend(
                directory / "artifacts", read_only=self.read_only if read_only is None else read_only
            )
        return self._backend

    def _reload(self) -> None:
        self._members = {
            row["experiment_id"]: dict(row)
            for row in self.catalog._conn.execute("SELECT * FROM suite_experiments ORDER BY experiment_id")
        }
        self._prefixes = {row["prefix"]: eid for eid, row in self._members.items()}

    def _child_path(self, eid: str) -> Path:
        return self.root / "experiments" / eid / "state.db"

    def output_dir(self, eid: str) -> Path:
        return self.root / "experiments" / eid if self.layout == "by_experiment" else self.root

    def _child(self, eid: str) -> Any:
        if self.layout == "combined":
            return self.data
        if eid not in self._members:
            raise ConfigError(f"unknown experiment {eid}")
        if eid in self._children:
            self._children.move_to_end(eid)
            return self._children[eid]
        if len(self._children) >= self.max_open:
            oldest = next(iter(self._children))
            old = self._children[oldest]
            # Keep a failed batch reachable (and its connection open) for retry.
            # close() releases the handle even on failure, so flush before eviction.
            if hasattr(old, "flush"):
                old.flush()
            old.close()
            del self._children[oldest]
        path = self._child_path(eid)
        if not path.is_file():
            raise ConfigError(f"missing experiment database: {path}")
        read_only = self.read_only or (self._selected is not None and eid not in self._selected)
        child = SqliteStore(
            str(path),
            read_only=read_only,
            journal=self.journal,
            backend=self._backend_for(path.parent, read_only=read_only),
        )
        if not read_only:
            _install_outcomes(child)
        identity = child._conn.execute("SELECT suite_id,experiment_id FROM suite_identity").fetchall()
        if len(identity) != 1 or tuple(identity[0]) != (self.suite_id, eid):
            child.close()
            raise ConfigError(f"experiment database identity conflict: {eid}")
        if not read_only and self._run is not None:
            current = self.catalog.get_run(self._run.run_id)
            child.start_run(current or self._run)
        handle = (
            WriteBehindStore(child, batch_size=self.batch_size, flush_interval=self.flush_interval)
            if self._batch and not read_only
            else child
        )
        self._children[eid] = handle
        return handle

    def _eid(self, pid: str) -> str:
        # Constant-size namespace prefix; local hashes are opaque and never parsed.
        prefix = pid.rsplit("-", 1)[0] + "-"
        eid = self._prefixes.get(prefix)
        if eid is None or (self.experiment is not None and eid != self.experiment):
            raise ConfigError(f"pipeline does not belong to selected Suite: {pid}")
        return eid

    def register(self, suite: Any) -> None:
        # Validate every member before mutating any existing definition.
        old_ids = {eid.casefold(): eid for eid in self._members}
        for exp in suite.experiments:
            old = self._members.get(exp.id)
            if exp.id.casefold() in old_ids and old_ids[exp.id.casefold()] != exp.id:
                raise ConfigError(f"experiment directory collision: {exp.id}")
            if old and old["definition_digest"] != exp.definition_digest:
                raise ConfigError(
                    f"experiment {exp.id}: definition changed; use a new experiment ID or output root"
                )
        for exp in suite.experiments:
            if self.layout == "by_experiment" and exp.id not in self._members:
                path = self._child_path(exp.id)
                path.parent.mkdir(parents=True, exist_ok=True)
                child = SqliteStore(str(path))
                child._conn.executescript(_SCHEMA)
                identity = child._conn.execute("SELECT * FROM suite_identity").fetchone()
                if identity and (identity["suite_id"], identity["experiment_id"]) != (suite.id, exp.id):
                    child.close()
                    raise ConfigError(f"experiment database identity conflict: {exp.id}")
                if identity is None:
                    child._conn.execute("INSERT INTO suite_identity VALUES (?,?)", (suite.id, exp.id))
                child._conn.commit()
                child.close()
            self.catalog._conn.execute(
                "INSERT INTO suite_experiments (experiment_id,definition_digest,label,prefix) VALUES (?,?,?,?) "
                "ON CONFLICT(experiment_id) DO UPDATE SET label=excluded.label",
                (exp.id, exp.definition_digest, exp.label, namespace(suite.id, exp.id)),
            )
        self.catalog._conn.commit()
        self._reload()

    def select_experiments(self, suite_id: str, selected: Sequence[str]) -> None:
        if suite_id != self.suite_id or set(selected) - self._members.keys():
            raise ConfigError("experiment selection does not match this Suite store")
        # Selection may change when a Runner is reused; reopen with the proper access mode.
        for child in self._children.values():
            child.close()
        self._children.clear()
        self._selected = list(selected)

    def prepare(self, suite: Any, selected: Sequence[Any]) -> None:
        if self._run is None or suite.id != self.suite_id:
            raise ConfigError("Suite pipeline stream must run in its own Suite runner")
        for exp in selected:
            self.catalog._conn.execute(
                "INSERT INTO suite_experiment_runs (run_id,experiment_id,definition_digest,state) VALUES (?,?,?,'running')",
                (self._run.run_id, exp.id, exp.definition_digest),
            )
            self.catalog._conn.execute(
                "UPDATE suite_experiments SET source_exhausted=0,source_error=NULL,limited=0,state='running',last_run_id=? "
                "WHERE experiment_id=?",
                (self._run.run_id, exp.id),
            )
        self.catalog._conn.commit()

    def source_state(
        self, eid: str, *, exhausted: bool, error: str | None = None, limited: bool = False
    ) -> None:
        self.catalog._conn.execute(
            "UPDATE suite_experiment_runs SET source_exhausted=?,source_error=?,limited=? WHERE run_id=? AND experiment_id=?",
            (int(exhausted), error, int(limited), self._run.run_id, eid),
        )
        self.catalog._conn.execute(
            "UPDATE suite_experiments SET source_exhausted=?,source_error=?,limited=? WHERE experiment_id=?",
            (int(exhausted), error, int(limited), eid),
        )
        self.catalog._conn.commit()

    def admit(self, spec: Any, rid: str) -> None:
        eid = self._eid(spec.pipeline_id)
        self.catalog._conn.execute(
            "INSERT OR IGNORE INTO suite_admissions VALUES (?,?,?,?,?)",
            (rid, spec.pipeline_id, eid, spec.local_key, spec.repeat),
        )
        self.catalog._conn.commit()
        # Identity lives with each child as well, independently of top-level summaries.
        child = self._child(eid)
        inner = getattr(child, "inner", child)
        existing = child.get_pipeline(spec.pipeline_id)
        with inner._visit_atomic(spec.pipeline_id):
            if existing is None:
                record = PipelineRecord(
                    spec.pipeline_id,
                    rid,
                    spec.name,
                    spec.key,
                    tags=dict(spec.template.tags),
                    n_tasks_total=spec.n_tasks,
                    seed_digest=spec.seed_digest,
                    spec_digest=spec.spec_digest,
                    suite_id=spec.suite_id,
                    experiment_id=eid,
                    local_key=spec.local_key,
                    repeat=spec.repeat,
                )
                encoded = spec.seed_encoded or spec.template.registry.dump(spec.seed)
                seed = Artifact(
                    Artifact.build_id(spec.pipeline_id, SEED_SEQ),
                    spec.pipeline_id,
                    SEED_TASK,
                    SEED_SEQ,
                    encoded.type_name,
                    encoded.codec,
                    encoded.digest,
                    encoded.size,
                    encoded.data,
                    time.time(),
                )
                # Admission is itself durable. A killed process may have queued work
                # that no worker opened yet; it must not disappear from progress.
                inner._write_pipeline(record)
                inner._write_artifact(seed)
            # Initialize before the worker starts. Never copy a previous run's terminal
            # state, timing or errors into this invocation's pending admission.
            columns = [row[1] for row in inner._conn.execute("PRAGMA table_info(pipelines)")]
            values = {c: c for c in columns}
            values.update(
                run_id="?",
                state="'pending'",
                created_at="?",
                started_at="NULL",
                finished_at="NULL",
                attempts_total="0",
                error_type="NULL",
                error_message="NULL",
                traceback="NULL",
                failed_task="NULL",
            )
            inner._conn.execute(
                f"INSERT OR IGNORE INTO suite_pipeline_runs ({','.join(columns)},baseline_attempts) "
                f"SELECT {','.join(values[c] for c in columns)},attempts_total FROM pipelines WHERE pipeline_id=?",
                (rid, time.time(), spec.pipeline_id),
            )
            if self.layout == "by_experiment":
                inner._conn.execute(
                    "INSERT OR IGNORE INTO suite_admissions VALUES (?,?,?,?,?)",
                    (rid, spec.pipeline_id, eid, spec.local_key, spec.repeat),
                )

    def _enrich(self, record: PipelineRecord) -> PipelineRecord:
        eid = self._eid(record.pipeline_id)
        row = self.catalog._conn.execute(
            "SELECT local_key,repeat FROM suite_admissions WHERE pipeline_id=? LIMIT 1", (record.pipeline_id,)
        ).fetchone()
        return dataclasses.replace(
            record,
            suite_id=self.suite_id,
            experiment_id=eid,
            local_key=row["local_key"] if row else record.local_key,
            repeat=row["repeat"] if row else record.repeat,
        )

    # Required protocol members are real methods: Python 3.12 runtime protocols
    # inspect attributes statically and intentionally do not invoke __getattr__.
    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        return self._child(self._eid(pipeline_id)).get_pipeline(pipeline_id)

    def upsert_pipeline(self, record: PipelineRecord) -> None:
        self._child(self._eid(record.pipeline_id)).upsert_pipeline(self._enrich(record))

    def finish_pipeline(self, pipeline_id: str, state: str, **kwargs: Any) -> None:
        self._child(self._eid(pipeline_id)).finish_pipeline(pipeline_id, state, **kwargs)

    def put_artifact(self, artifact: Artifact) -> Artifact:
        return self._child(self._eid(artifact.pipeline_id)).put_artifact(artifact)

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        self._child(self._eid(pipeline_id)).mark_final(pipeline_id, seq)

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None:
        return self._child(self._eid(pipeline_id)).get_artifact(pipeline_id, seq)

    def artifacts(self, pipeline_id: str) -> list[Artifact]:
        return self._child(self._eid(pipeline_id)).artifacts(pipeline_id)

    def iter_artifacts(self, *, pipeline_id: str) -> Iterator[Artifact]:
        return self._child(self._eid(pipeline_id)).iter_artifacts(pipeline_id=pipeline_id)

    def record_task(self, record: Any) -> None:
        self._child(self._eid(record.pipeline_id)).record_task(record)

    def record_attempt(self, record: Any) -> Any:
        return self._child(self._eid(record.pipeline_id)).record_attempt(record)

    # Point writes delegate a complete atomic operation to exactly one database.
    _RECORD = frozenset(
        {
            "reset_pipeline",
            "repair_visit_terminal",
            "reset_visits",
            "commit_entry",
            "commit_visit_attempt",
            "commit_visit_success",
            "commit_control_transition",
            "commit_handoff",
        }
    )
    _POINT = frozenset(
        {
            "settle_pipeline",
            "visit_state",
        }
    )

    def __getattr__(self, name: str) -> Any:
        if name in self._RECORD:

            def write(record: Any, *args: Any, **kwargs: Any) -> Any:
                if isinstance(record, PipelineRecord):
                    record = self._enrich(record)
                return getattr(self._child(self._eid(record.pipeline_id)), name)(record, *args, **kwargs)

            return write
        if name in self._POINT:

            def point(pipeline_id: str, *args: Any, **kwargs: Any) -> Any:
                method = getattr(self._child(self._eid(pipeline_id)), name)
                return method(pipeline_id, *args, **kwargs)

            return point
        raise AttributeError(name)

    def feature_level(self) -> str:
        return (
            "visits-v1"
            if any(store.feature_level() == "visits-v1" for _, store in self._stores())
            else "base"
        )

    def get_artifact_by_id(self, artifact_id: str) -> Any:
        # Artifact IDs retain the pipeline prefix, including visit-qualified IDs.
        return self._child(self._eid(artifact_id.split(":", 1)[0])).get_artifact_by_id(artifact_id)

    def start_run(self, run: Any) -> Any:
        self._run = run
        return self.catalog.start_run(run)

    def get_run(self, run_id: str) -> Any:
        return self.catalog.get_run(run_id)

    def latest_run_id(self) -> str | None:
        return self.catalog.latest_run_id()

    def heartbeat(self, run_id: str, ts: float | None = None) -> None:
        self.catalog.heartbeat(run_id, ts)
        for eid, child in self._children.items():
            if self._selected is None or eid in self._selected:
                child.heartbeat(run_id, ts)

    def finish_run(self, run_id: str, status: str, ended_at: float | None = None) -> None:
        self.flush()
        self.catalog.finish_run(run_id, status, ended_at)
        for row in self.experiments(run_id):
            self.catalog._conn.execute(
                "UPDATE suite_experiment_runs SET state=? WHERE run_id=? AND experiment_id=?",
                (row["state"], run_id, row["experiment_id"]),
            )
            self.catalog._conn.execute(
                "UPDATE suite_experiments SET state=? WHERE experiment_id=?",
                (row["state"], row["experiment_id"]),
            )
        self.catalog._conn.commit()
        # Includes evicted children, whose run rows would otherwise stay running.
        if self.layout == "by_experiment":
            for eid in self._selected if self._selected is not None else self._members:
                self._child(eid).finish_run(run_id, status, ended_at)

    def interrupt_stale(self, *, stale_after_s: float = 30, keep_run_id: str | None = None) -> int:
        # Exclusive ownership proves earlier writers are gone, even if their last
        # heartbeat looks fresh. Unselected experiments are not touched.
        total = 0
        for eid in self._selected if self._selected is not None else self._members:
            child = self._child(eid)
            if hasattr(child, "flush"):
                child.flush()
            inner = getattr(child, "inner", child)
            with inner._visit_atomic(""):
                for table in ("pipelines", "tasks"):
                    cursor = inner._conn.execute(
                        f"UPDATE {table} SET state='interrupted' WHERE state='running' "
                        "AND pipeline_id LIKE ? AND run_id<>?",
                        (self._members[eid]["prefix"] + "%", keep_run_id or ""),
                    )
                    if table == "pipelines":
                        total += cursor.rowcount
        # The writer lock proves that all prior Suite invocations have stopped,
        # including ones with only pending/completed work or unselected members.
        # Finalize their catalog records without opening any additional children.
        # The actual crash time is unknown: record when recovery detected it.
        detected_at = time.time()
        with self.catalog._conn:
            self.catalog._conn.execute(
                "UPDATE runs SET status='interrupted',ended_at=?,heartbeat_at=? "
                "WHERE status='running' AND run_id<>?",
                (detected_at, detected_at, keep_run_id or ""),
            )
        return total

    def emit_event(self, event: Any) -> None:
        store = self._child(self._eid(event.pipeline_id)) if event.pipeline_id else self.data
        if event.kind == "pipeline.skipped":
            inner = getattr(store, "inner", store)
            inner._conn.execute(
                "UPDATE suite_pipeline_runs SET state='skipped',finished_at=?,attempts_total=0 "
                "WHERE run_id=? AND pipeline_id=? AND state='pending'",
                (event.ts, event.run_id, event.pipeline_id),
            )
            inner._conn.commit()
        store.emit_event(event)

    def upsert_resource(self, *args: Any, **kwargs: Any) -> None:
        self.catalog.upsert_resource(*args, **kwargs)

    def resources(self, pool: str | None = None) -> list[Any]:
        return self.catalog.resources(pool)

    def upsert_reported_metric(self, row: Any) -> None:
        store = self._child(self._eid(row.pipeline_id)) if row.pipeline_id else self.data
        store.upsert_reported_metric(row)

    def report_experiment_metric(
        self, rid: str, eid: str, name: str, value: Any, **kwargs: Any
    ) -> ReportedMetric:
        if eid not in self._members:
            raise ConfigError(f"unknown experiment {eid}")
        # Reserved scope keys cannot collide with namespaced real pipeline IDs.
        row = report_metric(self.catalog, rid, name, value, pipeline_id=f"experiment:{eid}", **kwargs)
        return dataclasses.replace(row, pipeline_id=None, experiment_id=eid)

    def reported_metrics(self, *, run_id: str | None = None, pipeline_id: str | None = None) -> list[Any]:
        if pipeline_id is not None:
            return self._child(self._eid(pipeline_id)).reported_metrics(
                run_id=run_id, pipeline_id=pipeline_id
            )
        if self.experiment:
            return [
                dataclasses.replace(row, pipeline_id=None, experiment_id=self.experiment)
                for row in self.catalog.reported_metrics(
                    run_id=run_id, pipeline_id=f"experiment:{self.experiment}"
                )
            ]
        return self.catalog.reported_metrics(run_id=run_id)

    def _stores(self, *, include_catalog: bool = False) -> Iterator[tuple[str | None, Any]]:
        self.flush()
        self._reload()
        if self.layout == "combined":
            yield None, self.data
        else:
            if include_catalog and self.experiment is None:
                yield None, self.data
            for eid in self._members:
                if self.experiment is None or self.experiment == eid:
                    yield eid, self._child(eid)

    def _selected_pid(self, pid: str | None, run_id: str | None = None) -> bool:
        if pid is None:
            return self.experiment is None
        if self.experiment is not None and not pid.startswith(self._members[self.experiment]["prefix"]):
            return False
        if run_id is not None:
            return (
                self.catalog._conn.execute(
                    "SELECT 1 FROM suite_admissions WHERE run_id=? AND pipeline_id=?", (run_id, pid)
                ).fetchone()
                is not None
            )
        return True

    def _pipeline_query(self, inner, run_id):
        table = "pipelines" if run_id is None else "suite_pipeline_runs"
        if not self._has_table(inner, table):
            return None
        # Current checkpoints are cumulative; only historical rows filter run_id.
        where, args = self._filters(run_id=run_id)
        if run_id is not None:
            where.append(
                "EXISTS (SELECT 1 FROM suite_admissions AS a "
                f"WHERE a.run_id=? AND a.pipeline_id={table}.pipeline_id)"
            )
            args.append(run_id)
        return table, where, args

    def iter_pipelines(self, *, run_id: str | None = None, state: str | None = None) -> Iterator[Any]:
        for _, store in self._stores():
            inner = getattr(store, "inner", store)
            query = self._pipeline_query(inner, run_id)
            if query is None:
                continue
            table, where, args = query
            if state is not None:
                where.append("state=?")
                args.append(state)
            yield from inner._iter_keyset(
                table, where, args, columns=("created_at", "pipeline_id"), mapper=_to_pipeline
            )

    def pipelines(self, *, limit: int | None = None, **kwargs: Any) -> list[Any]:
        return list(islice(self.iter_pipelines(**kwargs), limit))

    def _iter_facts(
        self, row_kind: str, *, pipeline_id: str | None = None, run_id: str | None = None, **kwargs: Any
    ) -> Iterator[Any]:
        if pipeline_id:
            yield from getattr(self._child(self._eid(pipeline_id)), "iter_" + row_kind)(
                pipeline_id=pipeline_id, run_id=run_id, **kwargs
            )
            return
        for _, store in self._stores(include_catalog=row_kind == "events"):
            for row in getattr(store, "iter_" + row_kind)(run_id=run_id, **kwargs):
                if self._selected_pid(row.pipeline_id):
                    yield row

    def _query_stores(self, pipeline_id: str | None = None, *, include_catalog: bool = False):
        if pipeline_id is not None:
            self.flush()
            yield self._child(self._eid(pipeline_id))
        else:
            for _, store in self._stores(include_catalog=include_catalog):
                yield store

    def _filters(self, *, pipeline_id=None, run_id=None, kind=None):
        where, args = [], []
        if pipeline_id is not None:
            where.append("pipeline_id=?")
            args.append(pipeline_id)
        elif self.experiment is not None:
            where.append(f"{_PREFIX_SQL}=?")
            args.append(self._members[self.experiment]["prefix"])
        if run_id is not None:
            where.append("run_id=?")
            args.append(run_id)
        if kind is not None:
            where.append("kind=?")
            args.append(kind)
        return where, args

    @staticmethod
    def _where(filters):
        return " WHERE " + " AND ".join(filters) if filters else ""

    def iter_tasks(self, pipeline_id: str | None = None, *, run_id: str | None = None) -> Iterator[Any]:
        table = "tasks" if run_id is None else "suite_task_runs"
        for store in self._query_stores(pipeline_id):
            inner = getattr(store, "inner", store)
            if not self._has_table(inner, table):
                continue
            where, args = self._filters(pipeline_id=pipeline_id, run_id=run_id)
            yield from inner._iter_keyset(
                table, where, args, columns=("pipeline_id", "seq", "task_run_id"), mapper=_to_task
            )

    @staticmethod
    def _has_table(inner, table):
        return (
            inner._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            is not None
        )

    def iter_run_artifacts(self, *, pipeline_id: str, run_id: str) -> Iterator[Artifact]:
        # Payloads are checkpoints, not versioned history. Never label a later
        # run's payload as an earlier run's result.
        current = self.get_pipeline(pipeline_id)
        if current is not None and current.run_id == run_id:
            yield from self.iter_artifacts(pipeline_id=pipeline_id)

    def iter_attempts(self, **kwargs: Any) -> Iterator[Any]:
        return self._iter_facts("attempts", **kwargs)

    def iter_events(self, **kwargs: Any) -> Iterator[Any]:
        return self._iter_facts("events", **kwargs)

    def tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None, limit: int | None = None
    ) -> list[Any]:
        _validate_limit(limit)
        if limit == 0:
            return []
        table = "tasks" if run_id is None else "suite_task_runs"
        rows = self._query_rows(
            table,
            "pipeline_id,seq,visit,task_run_id",
            pipeline_id=pipeline_id,
            run_id=run_id,
            limit=limit,
        )
        key = itemgetter("pipeline_id", "seq", "visit", "task_run_id")
        selected = sorted(rows, key=key) if limit is None else heapq.nsmallest(limit, rows, key=key)
        return [_to_task(row) for row in selected]

    def attempts(self, *, limit: int | None = None, **kwargs: Any) -> list[Any]:
        return list(islice(self.iter_attempts(**kwargs), limit))

    def _query_rows(self, table, order, *, pipeline_id=None, run_id=None, kind=None, limit=None):
        # Consume each connection before opening the next: the bounded cache may
        # evict it. Only N candidates per database cross into Python when limited.
        for store in self._query_stores(pipeline_id, include_catalog=table == "events"):
            inner = getattr(store, "inner", store)
            if not self._has_table(inner, table):
                continue
            where, args = self._filters(pipeline_id=pipeline_id, run_id=run_id, kind=kind)
            sql = f"SELECT * FROM {table}{self._where(where)} ORDER BY {order}"
            if limit is not None:
                sql += " LIMIT ?"
                args.append(limit)
            yield from inner._conn.execute(sql, args)

    def events(
        self,
        *,
        pipeline_id: str | None = None,
        run_id: str | None = None,
        kind: str | None = None,
        limit: int | None = 200,
    ) -> list[Any]:
        _validate_limit(limit)
        if limit == 0:
            return []
        rows = self._query_rows(
            "events",
            "ts DESC,event_id DESC",
            pipeline_id=pipeline_id,
            run_id=run_id,
            kind=kind,
            limit=limit,
        )
        # IDs are local to each database. Ties keep database traversal order,
        # matching the previous stable nlargest selection.
        key = itemgetter("ts", "event_id")
        selected = sorted(rows, key=key) if limit is None else heapq.nlargest(limit, rows, key=key)
        return [_to_event(row) for row in sorted(selected, key=key)]

    def _count(self, table, *, pipeline_id=None, run_id=None, kind=None):
        total = 0
        for store in self._query_stores(pipeline_id, include_catalog=table == "events"):
            inner = getattr(store, "inner", store)
            if self._has_table(inner, table):
                where, args = self._filters(pipeline_id=pipeline_id, run_id=run_id, kind=kind)
                total += inner._conn.execute(
                    f"SELECT COUNT(*) FROM {table}{self._where(where)}", args
                ).fetchone()[0]
        return total

    def count_events(self, *, pipeline_id=None, run_id=None, kind=None) -> int:
        return self._count("events", pipeline_id=pipeline_id, run_id=run_id, kind=kind)

    def handoffs(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int | None = None
    ) -> list[Any]:
        _validate_limit(limit)
        if limit == 0:
            return []
        if pipeline_id is not None:
            # Recovery asks for the latest committed transition by ID, not wall
            # time. Preserve that point-query contract when clocks move backwards.
            return self._child(self._eid(pipeline_id)).handoffs(
                pipeline_id=pipeline_id, run_id=run_id, limit=limit
            )
        rows = self._query_rows("handoffs", "ts DESC,handoff_id DESC", run_id=run_id, limit=limit)
        key = itemgetter("ts", "handoff_id")
        if limit is None:
            return [_to_handoff(row) for row in sorted(rows, key=key)]
        # The previous sorted(...)[-N:] chose the later database on exact ties.
        selected = heapq.nlargest(limit, enumerate(rows), key=lambda pair: (*key(pair[1]), pair[0]))
        selected.sort(key=lambda pair: (*key(pair[1]), pair[0]))
        return [_to_handoff(row) for _, row in selected]

    def export_rows(self, *, run_id: str | None = None) -> Iterator[dict[str, Any]]:
        for _, store in self._stores():
            for row in store.export_rows():
                if self._selected_pid(row["pipeline_id"], run_id):
                    if run_id is not None:
                        inner = getattr(store, "inner", store)
                        saved = inner._conn.execute(
                            "SELECT * FROM suite_pipeline_runs WHERE run_id=? AND pipeline_id=?",
                            (run_id, row["pipeline_id"]),
                        ).fetchone()
                        if saved is None:
                            continue
                        record = _to_pipeline(saved)
                        current_run = row["run_id"]
                        row.update({k: v for k, v in dataclasses.asdict(record).items() if k in row})
                        row["duration_ms"] = (
                            (record.finished_at - record.started_at) * 1000
                            if record.finished_at is not None and record.started_at is not None
                            else None
                        )
                        row["tasks"] = [
                            dataclasses.asdict(t) for t in self.iter_tasks(record.pipeline_id, run_id=run_id)
                        ]
                        row["handoffs"] = [
                            dataclasses.asdict(h)
                            for h in self.handoffs(pipeline_id=record.pipeline_id, run_id=run_id)
                        ]
                        if current_run != run_id or record.state == "skipped":
                            row["artifacts"] = []
                            row.pop("control", None)
                    yield row

    def errors(self, *, run_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        import heapq

        records = heapq.nlargest(
            limit, self.iter_pipelines(run_id=run_id, state="failed"), key=lambda r: r.finished_at or 0
        )
        return [dataclasses.asdict(row) for row in records]

    def _pipeline_aggregates(self, run_id, *, with_durations=False):
        counts: dict[str, Counter] = {}
        attempts_total = 0
        durations = []
        for _, store in self._stores():
            inner = getattr(store, "inner", store)
            query = self._pipeline_query(inner, run_id)
            if query is None:
                continue
            table, where, args = query
            clause = self._where(where)
            for row in inner._conn.execute(
                f"SELECT experiment_id,state,COUNT(*) AS n,SUM(attempts_total) AS attempts "
                f"FROM {table}{clause} GROUP BY experiment_id,state",
                args,
            ):
                counts.setdefault(row["experiment_id"], Counter())[row["state"]] += row["n"]
                attempts_total += row["attempts"] or 0
            if with_durations:
                time_where = [*where, "finished_at IS NOT NULL", "started_at IS NOT NULL"]
                durations.extend(
                    row[0] * 1000
                    for row in inner._conn.execute(
                        f"SELECT finished_at-started_at FROM {table}{self._where(time_where)}", args
                    )
                )
        return counts, attempts_total, durations

    def experiments(self, run_id: str | None = None) -> list[dict[str, Any]]:
        counts, _, _ = self._pipeline_aggregates(run_id)
        return self._experiments(run_id, counts)

    def _experiments(self, run_id, counts):
        table = "suite_experiments" if run_id is None else "suite_experiment_runs"
        where, args = (" WHERE run_id=?", [run_id]) if run_id else ("", [])
        rows = [dict(row) for row in self.catalog._conn.execute(f"SELECT * FROM {table}{where}", args)]
        for row in rows:
            eid = row["experiment_id"]
            by_state = dict(counts.get(eid, {}))
            row.update(
                label=self._members[eid]["label"],
                pipelines={"total": sum(by_state.values()), "by_state": by_state},
            )
            row["source_exhausted"] = bool(row["source_exhausted"])
            row["state"] = (
                "failed"
                if row["source_error"] or by_state.get("failed")
                else "completed"
                if row["source_exhausted"]
                and not any(by_state.get(s) for s in ("pending", "running", "interrupted", "canceled"))
                else "pending"
                if not row.get("last_run_id", run_id)
                else "incomplete"
            )
            rid = run_id or row.get("last_run_id")
            if row["state"] == "incomplete" and rid:
                run = self.get_run(rid)
                if run and run.status == "running":
                    row["state"] = "running"
            row["reported_metrics"] = [
                dataclasses.asdict(dataclasses.replace(m, pipeline_id=None, experiment_id=eid))
                for m in self.catalog.reported_metrics(run_id=rid, pipeline_id=f"experiment:{eid}")
            ]
            row.pop("prefix", None)
        return [row for row in rows if self.experiment is None or row["experiment_id"] == self.experiment]

    def stats(self, run_id: str | None = None) -> dict[str, Any]:
        counts, invocation_attempts, durations = self._pipeline_aggregates(run_id, with_durations=True)
        states: Counter = Counter()
        for by_state in counts.values():
            states.update(by_state)
        durations.sort()

        def pct(p: float) -> float | None:
            return round(durations[int(p * (len(durations) - 1))], 3) if durations else None

        tasks: Counter = Counter()
        attempts: Counter = Counter()
        totals = Counter()
        for eid, store in self._stores(include_catalog=True):
            inner = getattr(store, "inner", store)
            where, args = self._filters(run_id=run_id)
            clause = self._where(where)
            tables = (
                ("events",)
                if self.layout == "by_experiment" and eid is None
                else ("events", "attempts", "handoffs")
            )
            for table in tables:
                if self._has_table(inner, table):
                    totals[table] += inner._conn.execute(
                        f"SELECT COUNT(*) FROM {table}{clause}", args
                    ).fetchone()[0]
            if tables == ("events",):
                continue
            if run_id is None:
                for row in inner._conn.execute(
                    f"SELECT name,COUNT(*) AS n,SUM(attempts_used) AS attempts FROM tasks{clause} GROUP BY name",
                    args,
                ):
                    tasks[row["name"]] += row["n"]
                    attempts[row["name"]] += row["attempts"] or 0
            else:
                for row in inner._conn.execute(
                    f"SELECT task_name,COUNT(*) AS n FROM attempts{clause} GROUP BY task_name", args
                ):
                    attempts[row["task_name"]] += row["n"]
                # Match the old first-attempt-per-slot counting, including its name.
                for row in inner._conn.execute(
                    "SELECT first.task_name,COUNT(*) AS n FROM attempts AS first JOIN "
                    f"(SELECT MIN(attempt_id) AS first_id FROM attempts{clause} "
                    "GROUP BY pipeline_id,task_run_id) AS slots ON first.attempt_id=slots.first_id "
                    "GROUP BY first.task_name",
                    args,
                ):
                    tasks[row["task_name"]] += row["n"]
        experiments = self._experiments(run_id, counts)
        return {
            "suite_id": self.suite_id,
            "scope": "run" if run_id else "suite",
            "experiments": experiments,
            "pipelines": {
                "total": sum(states.values()),
                "by_state": dict(states),
                "duration_ms": {"p50": pct(0.5), "p95": pct(0.95), "max": pct(1)},
            },
            "tasks": {"by_name": dict(tasks), "attempts_by_name": dict(attempts)},
            "attempts_total": invocation_attempts if run_id is not None else totals["attempts"],
            "events_total": totals["events"],
            "handoffs_total": totals["handoffs"],
            "source_errors": sum(bool(row["source_error"]) for row in experiments),
        }

    def flush(self) -> int:
        total = self.data.flush() if hasattr(self.data, "flush") else 0
        for child in self._children.values():
            if hasattr(child, "flush"):
                total += child.flush()
        return total

    def close(self) -> None:
        error = None
        # A failed child flush must neither hide its error nor prevent the other
        # children/catalog/ownership lock from being closed.
        for handle in [*self._children.values(), self.data, self._writer_guard]:
            if handle is None:
                continue
            try:
                handle.close()
            except BaseException as exc:
                if error is None:
                    error = exc
        self._children.clear()
        self._writer_guard = None
        if error is not None:
            raise error
