"""SQLite store —— the default persistence backend.

* WAL + ``synchronous=NORMAL``, a single writer connection, many reads within the process.
* Each table does exactly one thing: ``pipelines`` is state, ``tasks``/``attempts`` are history,
  ``artifacts`` is the state carrier, ``events`` is the structured log.
* Every SQL detail can be queried directly; no extra logging system is needed.
"""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import json
import os
import sqlite3
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from ..artifact import Artifact, Encoded
from ..errors import PyAttackerError
from .base import (
    ITER_BATCH_SIZE,
    AttemptRecord,
    EventRecord,
    HandoffRecord,
    PipelineRecord,
    RunRecord,
    TaskRecord,
    handoff_row,
)
from .visits import (
    FEATURE_BASE,
    FEATURE_VISITS,
    VisitStore,
    check_feature_level,
)

__all__ = ["SqliteStore"]

WRITER_GUARD = "pyattacker_store_requires_visits_aware_writer"
"""Name of the SQL function a writer must have registered to touch a revisit-aware store.

A trigger cannot consult per-connection state directly (SQLite refuses a trigger that references
``temp``), so the declaration is inverted: the guard triggers call this function, and a connection
that never registered it cannot even *prepare* its own ``INSERT``/``UPDATE``/``DELETE``. That is
what makes the marker below work against a lineage-unaware writer that has no idea the marker
exists — see :meth:`SqliteStore._install_writer_guard`.
"""

GUARDED_TABLES = ("pipelines", "tasks", "artifacts")
"""Seq-keyed tables whose rows a lineage-unaware writer would mutate as if occurrence identity were
``(pipeline_id, seq)``. Append-only ledgers (``attempts``, ``handoffs``, ``events``) are not
guarded: writing a row there is not itself destructive, and no such write is reachable without
first touching one of these three."""

META_TABLE = "store_meta"
"""Key/value table holding the durable feature level (``store/visits.py``)."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    label TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'running',
    started_at REAL NOT NULL,
    ended_at REAL,
    heartbeat_at REAL,
    spec_digest TEXT NOT NULL DEFAULT '',
    code_version TEXT NOT NULL DEFAULT '',
    python TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    config_json TEXT NOT NULL DEFAULT '{}',
    notes TEXT NOT NULL DEFAULT '',
    resume_of TEXT
);

CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    key TEXT NOT NULL,
    state TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    n_tasks_total INTEGER NOT NULL DEFAULT 0,
    n_tasks_done INTEGER NOT NULL DEFAULT 0,
    attempts_total INTEGER NOT NULL DEFAULT 0,
    failed_task TEXT,
    error_type TEXT,
    error_message TEXT,
    traceback TEXT,
    seed_digest TEXT NOT NULL DEFAULT '',
    spec_digest TEXT NOT NULL DEFAULT '',
    resume_of TEXT,
    handoff_floor INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_pipelines_run ON pipelines(run_id, state);
CREATE INDEX IF NOT EXISTS idx_pipelines_state ON pipelines(state);
-- the keyset pagination order used by iter_pipelines/export_rows (created_at, then pipeline_id)
CREATE INDEX IF NOT EXISTS idx_pipelines_created ON pipelines(created_at, pipeline_id);

CREATE TABLE IF NOT EXISTS tasks (
    task_run_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    attempts_used INTEGER NOT NULL DEFAULT 0,
    started_at REAL,
    ended_at REAL,
    duration_ms REAL,
    input_artifact_id TEXT,
    output_artifact_id TEXT,
    error_class TEXT,
    error_type TEXT,
    error_message TEXT,
    traceback TEXT,
    leases_json TEXT NOT NULL DEFAULT '[]',
    metrics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_tasks_pipeline ON tasks(pipeline_id, seq);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    task_run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    seq INTEGER NOT NULL,
    attempt_no INTEGER NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    duration_ms REAL,
    outcome TEXT NOT NULL,
    error_class TEXT,
    error_type TEXT,
    error_message TEXT,
    traceback TEXT,
    retry_delay_s REAL,
    decision_json TEXT NOT NULL DEFAULT '{}',
    leases_json TEXT NOT NULL DEFAULT '[]',
    metrics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_attempts_pipeline ON attempts(pipeline_id, seq, attempt_no);
CREATE INDEX IF NOT EXISTS idx_attempts_run ON attempts(run_id);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type_name TEXT NOT NULL,
    codec TEXT NOT NULL,
    digest TEXT NOT NULL,
    size INTEGER NOT NULL,
    payload BLOB,
    created_at REAL NOT NULL,
    is_final INTEGER NOT NULL DEFAULT 0,
    blob_ref TEXT
);
CREATE INDEX IF NOT EXISTS idx_artifacts_pipeline ON artifacts(pipeline_id, seq);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    scope TEXT NOT NULL,
    kind TEXT NOT NULL,
    run_id TEXT,
    pipeline_id TEXT,
    task_run_id TEXT,
    pool TEXT,
    resource_id TEXT,
    data_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_pipeline ON events(pipeline_id, event_id);
CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, event_id);

-- Advanced control flow (docs/design.md §4.8): one append-only row per task-initiated handoff, the
-- pipeline's control-flow history and the durable state recovery resumes at. `to_seq`/`to_task` are
-- NULL for END; `entry_seq` is the entry artifact's position (the lookup key), `entry_artifact_id` the
-- recorded reference, never null. A new *table* needs no `_migrate` step: CREATE TABLE IF NOT EXISTS
-- in this script is the whole upgrade for an existing store.
CREATE TABLE IF NOT EXISTS handoffs (
    handoff_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    from_seq INTEGER NOT NULL,
    from_task TEXT NOT NULL,
    to_seq INTEGER,
    to_task TEXT,
    entry_seq INTEGER NOT NULL,
    entry_artifact_id TEXT NOT NULL,
    entry_reused INTEGER NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_handoffs_pipeline ON handoffs(pipeline_id, handoff_id);
CREATE INDEX IF NOT EXISTS idx_handoffs_run ON handoffs(run_id, handoff_id);

CREATE TABLE IF NOT EXISTS resources (
    pool TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    spec_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL,
    stats_json TEXT NOT NULL DEFAULT '{}',
    published_by TEXT NOT NULL DEFAULT '',
    published_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (pool, resource_id)
);
"""


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


class SqliteStore(VisitStore):
    """The default, file-backed :class:`~pyattacker.store.base.Store` implementation.

    Invariants: WAL + ``synchronous=NORMAL`` and a single writer connection (see module
    docstring); state writes (pipelines/tasks) are synchronous so a crash never leaves a gap
    between "the scheduler thinks this happened" and "the row says so" — only append-only facts
    (attempts/events) are ever eligible for batching, and only when wrapped in
    ``WriteBehindStore``.

    Collaborators: normally not constructed directly — ``open_store(path)`` builds one and, for
    a file-backed store, wraps it in ``store/writebehind.py``'s ``WriteBehindStore`` unless
    write-behind was explicitly disabled.
    """

    def __init__(
        self,
        path: str,
        *,
        journal: str = "full",
        read_only: bool = False,
        backend: Any = None,
    ) -> None:
        self.path = path
        self.journal = journal
        self.read_only = read_only
        from ..backends import resolve_backend

        self.backend = resolve_backend(backend)
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        if read_only:
            # read-only mode must use an absolute-path file: URI, otherwise sqlite treats a relative path as a host
            uri = Path(path).resolve().as_uri() + "?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
        else:
            self._conn = sqlite3.connect(path, check_same_thread=False, timeout=10.0)
        self._conn.row_factory = sqlite3.Row
        # Declared on every connection this build opens, before any statement runs: the writer guard
        # triggers installed on a revisit-aware store call it, so this is what distinguishes "a
        # pyattacker that understands visit lineage" from "a writer that does not".
        self._conn.create_function(WRITER_GUARD, 0, lambda: 1, deterministic=True)
        try:
            # Connection-local, and needed before the first query so a locked database still waits
            # instead of failing immediately.
            self._conn.execute("PRAGMA busy_timeout=10000")
            # The feature level is read and enforced *before* anything else runs. A store this build
            # does not understand must not be touched at all -- not even by the additive migration,
            # which would otherwise "repair" a schema whose semantics this build cannot see (and
            # leave a half-upgraded database behind the refusal). `_read_feature_level` treats a
            # missing `store_meta` as `base`, so an old store still passes and then gets migrated.
            check_feature_level(
                self._read_feature_level(), store=path or ":memory:", read_only=read_only
            )
            if not read_only:
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA synchronous=NORMAL")
                self._conn.executescript(SCHEMA)
                with self._visit_atomic(""):
                    self._migrate()
        except BaseException:
            # A store this build refuses to open must not leave a connection (and its file handles)
            # behind, since the caller only sees the exception.
            self._conn.close()
            raise

    def visit_state(self, pipeline_id):
        exists = self._conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='visit_state'").fetchone()
        if not exists:
            return None
        row = self._conn.execute("SELECT state_json FROM visit_state WHERE pipeline_id=?", (pipeline_id,)).fetchone()
        return json.loads(row[0]) if row else None

    @contextlib.contextmanager
    def _visit_atomic(self, pipeline_id):
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def _visit_save(self, pipeline_id, state):
        self._conn.execute("INSERT INTO visit_state VALUES (?,?) ON CONFLICT(pipeline_id) DO UPDATE SET state_json=excluded.state_json",
                           (pipeline_id, _dumps(state)))

    def _visit_pipeline(self, record):
        self._write_pipeline(record)

    def _visit_task(self, record):
        self._write_task(record)

    def _visit_attempt(self, record):
        record.attempt_id = self._write_attempt(record)

    def _visit_artifact(self, artifact):
        return _hydrate(self._write_artifact(artifact), self.backend)

    def _visit_handoff(self, record, task, attempt, payload, cursor, final):
        stored = self._commit_handoff(record, task=task, attempt=attempt, payload=payload, cursor=cursor, final=final)
        if final:
            self._visit_final(record.pipeline_id, record.entry_artifact_id)
        return stored

    def _visit_final(self, pipeline_id, artifact_id):
        self._conn.execute("UPDATE artifacts SET is_final=(artifact_id=?) WHERE pipeline_id=?",
                           (artifact_id or "", pipeline_id))

    def get_artifact_by_id(self, artifact_id):
        row = self._conn.execute("SELECT * FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
        return _hydrate(_to_artifact(row), self.backend) if row else None

    def _migrate(self) -> None:
        """Add columns that older stores do not have.

        ``CREATE TABLE IF NOT EXISTS`` silently skips existing tables, so a schema addition needs
        an explicit upgrade step; without it, resuming an old store would fail at the first insert.
        """
        self._conn.execute("CREATE TABLE IF NOT EXISTS visit_state (pipeline_id TEXT PRIMARY KEY, state_json TEXT NOT NULL)")
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {META_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        for table, additions in {"artifacts": {"visit": "INTEGER NOT NULL DEFAULT 0"},
                                 "tasks": {"visit": "INTEGER NOT NULL DEFAULT 0"},
                                 "attempts": {"visit": "INTEGER NOT NULL DEFAULT 0"},
                                 "handoffs": {"operation": "TEXT NOT NULL DEFAULT 'forward'",
                                              "from_visit": "INTEGER NOT NULL DEFAULT 0",
                                              "to_visit": "INTEGER", "transition_version": "INTEGER"}}.items():
            present = {row["name"] for row in self._conn.execute(f"PRAGMA table_info({table})")}
            for column, declaration in additions.items():
                if column not in present:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(artifacts)")}
        if "blob_ref" not in columns:
            self._conn.execute("ALTER TABLE artifacts ADD COLUMN blob_ref TEXT")

        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(pipelines)")}
        if "handoff_floor" not in columns:
            self._conn.execute("ALTER TABLE pipelines ADD COLUMN handoff_floor INTEGER NOT NULL DEFAULT 0")

    # ------------------------------------------------- compatibility marker
    def _read_feature_level(self) -> str:
        """The store's durable feature level, or :data:`FEATURE_BASE` when it was never marked."""
        if not self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (META_TABLE,)
        ).fetchone():
            return FEATURE_BASE  # a store old enough to predate the marker
        row = self._conn.execute(f"SELECT value FROM {META_TABLE} WHERE key='feature_level'").fetchone()
        return row[0] if row else FEATURE_BASE

    def feature_level(self) -> str:
        return self._read_feature_level()

    def _visit_observed_counters(self, pipeline_id: str, n_tasks_total: int) -> dict[str, int]:
        """Rebuild visit counters from the durable rows when the traversal record is gone.

        Both tables are consulted because allocation writes the task row first (with its visit
        already assigned) and the artifact afterwards: a task killed in between must still keep its
        visit reserved, or the next allocation would reuse an ID a task row already carries. The
        chain bound keeps handoff payloads (addressed at ``n_tasks_total + k``) out of the result.
        """
        rows = self._conn.execute(
            "SELECT seq, MAX(visit) AS visit FROM ("
            "  SELECT seq, visit FROM artifacts WHERE pipeline_id=? AND seq>=0 AND seq<?"
            "  UNION ALL"
            "  SELECT seq, visit FROM tasks WHERE pipeline_id=? AND seq>=0 AND seq<?"
            ") GROUP BY seq",
            (pipeline_id, n_tasks_total, pipeline_id, n_tasks_total),
        )
        return {str(row["seq"]): row["visit"] for row in rows}

    def _visit_abandon_running_tasks(self, pipeline_id: str) -> None:
        """Move every in-flight task row of a discarded traversal to ``interrupted`` (``visits.py``).

        Only ``running`` rows are touched: a row that already finished, failed or was settled by an
        earlier abandonment keeps the state that describes what actually happened to it.
        """
        self._conn.execute(
            "UPDATE tasks SET state='interrupted' WHERE pipeline_id=? AND state='running'",
            (pipeline_id,),
        )

    def _visit_mark_revisit(self) -> None:
        """Enter :data:`FEATURE_VISITS`: mark the level and arm the writer guard.

        Both land inside the caller's ``BEGIN IMMEDIATE`` transaction, together with the traversal
        write that allocated the revisit, so a store can never contain a second occurrence without
        the marker (or the marker without the occurrence). Deliberately **not** cached in Python
        state: the marker is written in the same transaction that can still roll back, and a cache
        surviving that rollback would let the next revisit skip the marking and commit an occurrence
        into a store that still advertises ``base`` and carries no guard. Both statements are
        idempotent, so re-running them on every revisit is the cheap, always-correct option.
        """
        self._conn.execute(
            f"INSERT INTO {META_TABLE} (key, value) VALUES ('feature_level', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (FEATURE_VISITS,),
        )
        self._install_writer_guard()

    def _install_writer_guard(self) -> None:
        """Refuse writes from a connection that has not declared visit-lineage awareness.

        The triggers are created here — only once the store actually holds revisit state — and are
        part of the durable schema, so they also apply to a writer that predates them. They call
        :data:`WRITER_GUARD`, which every connection opened by a visit-aware build registers. A
        lineage-unaware writer fails loudly on its first ``INSERT``/``UPDATE``/``DELETE`` against a
        seq-keyed table (``no such function: ...`` or the ``RAISE`` below) instead of silently
        operating on the wrong artifact occurrence. The guard protects *state*, not access: raw or
        legacy reads are not blocked, but interpreting revisit-aware lineage with a build that does
        not understand it is unsupported — such a reader cannot tell which occurrence is effective.
        An explicit migration is the only supported way back down a feature level.
        """
        for table in GUARDED_TABLES:
            for event in ("INSERT", "UPDATE", "DELETE"):
                self._conn.execute(
                    f"CREATE TRIGGER IF NOT EXISTS pyattacker_writer_guard_{table}_{event.lower()} "
                    f"BEFORE {event} ON {table} "
                    f"WHEN {WRITER_GUARD}() IS NOT 1 "
                    "BEGIN SELECT RAISE(ABORT, 'store contains revisit-aware state (visits-v1); "
                    "this writer does not understand visit lineage - upgrade pyattacker'); END"
                )

    # ------------------------------------------------------------------ runs
    def start_run(self, run: RunRecord) -> RunRecord:
        run.heartbeat_at = run.started_at
        self._conn.execute(
            "INSERT OR REPLACE INTO runs (run_id,label,status,started_at,ended_at,heartbeat_at,"
            "spec_digest,code_version,python,host,config_json,notes,resume_of) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                run.run_id, run.label, run.status, run.started_at, run.ended_at, run.heartbeat_at,
                run.spec_digest, run.code_version, run.python, run.host,
                _dumps(run.config), run.notes, run.resume_of,
            ),
        )
        self._conn.commit()
        return run

    def heartbeat(self, run_id: str, ts: float | None = None) -> None:
        self._conn.execute("UPDATE runs SET heartbeat_at=? WHERE run_id=?", (ts or time.time(), run_id))
        self._conn.commit()

    def finish_run(self, run_id: str, status: str, ended_at: float | None = None) -> None:
        self._conn.execute(
            "UPDATE runs SET status=?, ended_at=?, heartbeat_at=? WHERE run_id=?",
            (status, ended_at or time.time(), ended_at or time.time(), run_id),
        )
        self._conn.commit()

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return _to_run(row) if row else None

    # ------------------------------------------------------------- pipelines
    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        row = self._conn.execute(
            "SELECT * FROM pipelines WHERE pipeline_id=?", (pipeline_id,)
        ).fetchone()
        return _to_pipeline(row) if row else None

    def upsert_pipeline(self, record: PipelineRecord) -> None:
        self._write_pipeline(record)
        self._conn.commit()

    def reset_pipeline(self, record: PipelineRecord) -> None:
        """Reset current state and persist the new ledger boundary in one transaction."""
        with self._conn:
            self._conn.execute("DELETE FROM tasks WHERE pipeline_id=?", (record.pipeline_id,))
            self._conn.execute(
                "DELETE FROM artifacts WHERE pipeline_id=? AND seq>=0 AND seq<?",
                (record.pipeline_id, record.n_tasks_total),
            )
            self._conn.execute("UPDATE artifacts SET is_final=0 WHERE pipeline_id=?", (record.pipeline_id,))
            self._write_pipeline(record)

    def _write_pipeline(self, record: PipelineRecord) -> None:
        self._conn.execute(
            "INSERT INTO pipelines (pipeline_id,run_id,name,key,state,tags_json,created_at,started_at,"
            "finished_at,n_tasks_total,n_tasks_done,attempts_total,failed_task,error_type,error_message,"
            "traceback,seed_digest,spec_digest,resume_of,handoff_floor) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pipeline_id) DO UPDATE SET run_id=excluded.run_id, state=excluded.state, "
            "started_at=excluded.started_at, finished_at=excluded.finished_at, "
            "n_tasks_total=excluded.n_tasks_total, n_tasks_done=excluded.n_tasks_done, "
            "attempts_total=excluded.attempts_total, failed_task=excluded.failed_task, "
            "error_type=excluded.error_type, error_message=excluded.error_message, "
            "traceback=excluded.traceback, resume_of=excluded.resume_of, handoff_floor=excluded.handoff_floor",
            (
                record.pipeline_id, record.run_id, record.name, record.key, record.state,
                _dumps(record.tags), record.created_at, record.started_at, record.finished_at,
                record.n_tasks_total, record.n_tasks_done, record.attempts_total, record.failed_task,
                record.error_type, record.error_message, record.traceback, record.seed_digest,
                record.spec_digest, record.resume_of, record.handoff_floor,
            ),
        )

    def finish_pipeline(
        self,
        pipeline_id: str,
        state: str,
        *,
        n_tasks_done: int | None = None,
        error: BaseException | None = None,
        failed_task: str | None = None,
        traceback: str | None = None,
    ) -> None:
        fields = ["state=?", "finished_at=?"]
        values: list[Any] = [state, time.time()]
        if n_tasks_done is not None:
            fields.append("n_tasks_done=?")
            values.append(n_tasks_done)
        if error is not None:
            fields += ["error_type=?", "error_message=?", "traceback=?", "failed_task=?"]
            values += [type(error).__name__, str(error)[:2000], traceback, failed_task]
        values.append(pipeline_id)
        self._conn.execute(f"UPDATE pipelines SET {', '.join(fields)} WHERE pipeline_id=?", values)
        self._conn.commit()

    def settle_pipeline(
        self, pipeline_id: str, *, state: str, n_tasks_done: int, run_id: str
    ) -> None:
        """Terminal settle in one write: state, cursor, owning run and cleared failure fields together.

        The optional capability the terminal-cursor repair uses (``store/base.py``): because a crash
        before it leaves the original failure metadata untouched and a crash after it leaves a fully
        settled row, there is no durable moment in which the row has lost its failure but is not yet
        terminal.
        """
        self._conn.execute(
            "UPDATE pipelines SET state=?, n_tasks_done=?, run_id=?, finished_at=?, error_type=NULL, "
            "error_message=NULL, traceback=NULL, failed_task=NULL WHERE pipeline_id=?",
            (state, n_tasks_done, run_id, time.time(), pipeline_id),
        )
        self._conn.commit()

    def interrupt_stale(self, *, stale_after_s: float = 30.0, keep_run_id: str | None = None) -> int:
        cutoff = time.time() - stale_after_s
        sql = (
            "UPDATE pipelines SET state='interrupted' WHERE state='running' AND run_id IN "
            "(SELECT run_id FROM runs WHERE status<>'running' OR heartbeat_at IS NULL OR heartbeat_at < ?)"
        )
        args: list[Any] = [cutoff]
        if keep_run_id is not None:
            sql += " AND run_id<>?"
            args.append(keep_run_id)
        count = self._conn.execute(sql, args).rowcount or 0
        self._conn.execute(
            "UPDATE tasks SET state='interrupted' WHERE state='running' AND run_id IN "
            "(SELECT run_id FROM runs WHERE status<>'running')"
        )
        self._conn.commit()
        return count

    # ------------------------------------------------------------- artifacts
    def _write_artifact(self, artifact: Artifact) -> Artifact:
        """The artifact INSERT without the commit, so several facts can land in one transaction."""
        stored = _persist_form(artifact, self.journal, self.backend)
        self._conn.execute(
            "INSERT OR REPLACE INTO artifacts (artifact_id,pipeline_id,task_name,seq,type_name,codec,"
            "digest,size,payload,created_at,is_final,blob_ref,visit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stored.id, stored.pipeline_id, stored.task_name, stored.seq, stored.type_name,
                stored.codec, stored.digest, stored.size, stored.payload, stored.created_at,
                1 if stored.is_final else 0, stored.blob_ref, stored.visit,
            ),
        )
        return stored

    def put_artifact(self, artifact: Artifact) -> Artifact:
        stored = self._write_artifact(artifact)
        self._conn.commit()
        return stored

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None:
        state = self.visit_state(pipeline_id)
        pipeline = self.get_pipeline(pipeline_id) if state is not None else None
        if state is not None and 0 <= seq < pipeline.n_tasks_total:
            active = state["active"].get(str(seq))
            return self.get_artifact_by_id(active["output"]) if active and active["output"] else None
        row = self._conn.execute(
            "SELECT * FROM artifacts WHERE pipeline_id=? AND seq=?", (pipeline_id, seq)
        ).fetchone()
        return _hydrate(_to_artifact(row), self.backend) if row else None

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        self._conn.execute(
            "UPDATE artifacts SET is_final=(seq=?) WHERE pipeline_id=?", (seq, pipeline_id)
        )
        self._conn.commit()

    def artifacts(self, pipeline_id: str) -> list[Artifact]:
        rows = self._conn.execute(
            "SELECT * FROM artifacts WHERE pipeline_id=? ORDER BY seq,created_at,artifact_id", (pipeline_id,)
        ).fetchall()
        return [_hydrate(_to_artifact(r), self.backend) for r in rows]

    # ----------------------------------------------------------------- tasks
    def _write_task(self, record: TaskRecord) -> None:
        """The task upsert without the commit, so several facts can land in one transaction."""
        self._conn.execute(
            "INSERT OR REPLACE INTO tasks (task_run_id,pipeline_id,run_id,name,seq,state,attempts_used,"
            "started_at,ended_at,duration_ms,input_artifact_id,output_artifact_id,error_class,error_type,"
            "error_message,traceback,leases_json,metrics_json,visit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record.task_run_id, record.pipeline_id, record.run_id, record.name, record.seq,
                record.state, record.attempts_used, record.started_at, record.ended_at,
                record.duration_ms, record.input_artifact_id, record.output_artifact_id,
                record.error_class, record.error_type, record.error_message, record.traceback,
                _dumps(record.leases), _dumps(record.metrics), record.visit,
            ),
        )

    def record_task(self, record: TaskRecord) -> None:
        self._write_task(record)
        self._conn.commit()

    def _write_attempt(self, record: AttemptRecord) -> int:
        """The attempt INSERT without the commit; returns the assigned ``attempt_id``."""
        cur = self._conn.execute(
            "INSERT INTO attempts (run_id,pipeline_id,task_run_id,task_name,seq,attempt_no,started_at,"
            "ended_at,duration_ms,outcome,error_class,error_type,error_message,traceback,retry_delay_s,"
            "decision_json,leases_json,metrics_json,visit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record.run_id, record.pipeline_id, record.task_run_id, record.task_name, record.seq,
                record.attempt_no, record.started_at, record.ended_at, record.duration_ms, record.outcome,
                record.error_class, record.error_type, record.error_message, record.traceback,
                record.retry_delay_s, _dumps(record.decision), _dumps(record.leases),
                _dumps(record.metrics), record.visit,
            ),
        )
        return int(cur.lastrowid)

    def record_attempt(self, record: AttemptRecord) -> AttemptRecord:
        record.attempt_id = self._write_attempt(record)
        self._conn.commit()
        return record

    # -------------------------------------------------------------- handoffs
    def _has_handoffs(self) -> bool:
        """Whether this database has the ``handoffs`` table yet.

        A **reader** can open a store that was created before this feature existed — ``report``, ``watch``
        and ``serve`` all open read-only, and a read-only connection never runs the schema. Asking
        ``sqlite_master`` first is what keeps such a store readable instead of failing with "no such
        table: handoffs"; the lookup is not cached, because a live writer may create the table at any time.
        """
        return (
            self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='handoffs'"
            ).fetchone()
            is not None
        )

    def commit_handoff(
        self,
        record: HandoffRecord,
        *,
        task: TaskRecord,
        attempt: AttemptRecord,
        payload: Encoded | None = None,
        cursor: int,
        final: bool = False,
    ) -> Artifact | None:
        """Atomically commit a handoff; any database failure rolls back the transition."""
        # Roll back every database write if any part fails. Later event/cleanup writes
        # must never commit a partial transition left on this connection.
        with self._conn:
            return self._commit_handoff(
                record, task=task, attempt=attempt, payload=payload, cursor=cursor, final=final
            )

    def _commit_handoff(
        self,
        record: HandoffRecord,
        *,
        task: TaskRecord,
        attempt: AttemptRecord,
        payload: Encoded | None = None,
        cursor: int,
        final: bool = False,
    ) -> Artifact | None:
        """One transaction: task row + handed-off attempt + payload artifact + ledger row + cursor.

        See the capability contract in ``store/base.py``. The payload's ``seq`` is allocated here as
        ``n_tasks_total + k`` (``k`` = the handoffs already recorded for this pipeline), so a payload can
        never land in a task slot and every payload gets its own address; the documented artifact order
        is therefore ``seed (-1) → chain (0 … n-1) → handoff payloads (≥ n)``.
        """
        row = self._conn.execute(
            "SELECT n_tasks_total FROM pipelines WHERE pipeline_id=?", (record.pipeline_id,)
        ).fetchone()
        if row is None:
            raise PyAttackerError(
                f"commit_handoff: no pipeline row for {record.pipeline_id!r}; the handoff has no pipeline "
                "to advance (a handoff is only ever committed while its pipeline is open)"
            )
        stored: Artifact | None = None
        if payload is not None:
            recorded = self._conn.execute(
                "SELECT COUNT(*) FROM handoffs WHERE pipeline_id=?", (record.pipeline_id,)
            ).fetchone()[0]
            seq = int(row["n_tasks_total"]) + int(recorded)
            stored = self._write_artifact(
                Artifact(
                    id=Artifact.build_id(record.pipeline_id, seq),
                    pipeline_id=record.pipeline_id,
                    task_name=task.name,
                    seq=seq,
                    type_name=payload.type_name,
                    codec=payload.codec,
                    digest=payload.digest,
                    size=payload.size,
                    payload=payload.data,
                    created_at=time.time(),
                    is_final=final,
                    visit=task.visit,
                )
            )
            record.entry_seq = seq
        if record.entry_seq is None:
            raise PyAttackerError(
                "commit_handoff: entry_seq is required when a handoff reuses an artifact (only a payload "
                "handoff lets the store allocate the entry address)"
            )
        record.entry_artifact_id = stored.id if stored is not None else (record.entry_artifact_id or Artifact.build_id(record.pipeline_id, record.entry_seq))
        self._write_task(task)
        attempt.attempt_id = self._write_attempt(attempt)
        cur = self._conn.execute(
            "INSERT INTO handoffs (pipeline_id,run_id,from_seq,from_task,to_seq,to_task,entry_seq,"
            "entry_artifact_id,entry_reused,reason,ts,operation,from_visit,to_visit,transition_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record.pipeline_id, record.run_id, record.from_seq, record.from_task, record.to_seq,
                record.to_task, record.entry_seq, record.entry_artifact_id,
                1 if record.entry_reused else 0, record.reason, record.ts, record.operation, record.from_visit,
                record.to_visit, record.transition_version,
            ),
        )
        record.handoff_id = int(cur.lastrowid)
        if final:
            # END: finality and the terminal row travel with the ledger row. Clearing the failure fields
            # mirrors settle_pipeline — a resumed pipeline that finishes here must not keep the previous
            # run's error text on a row that is now `succeeded`.
            self._conn.execute(
                "UPDATE artifacts SET is_final=(seq=?) WHERE pipeline_id=?",
                (record.entry_seq, record.pipeline_id),
            )
            self._conn.execute(
                "UPDATE pipelines SET state='succeeded', n_tasks_done=?, run_id=?, finished_at=?, "
                "error_type=NULL, error_message=NULL, traceback=NULL, failed_task=NULL "
                "WHERE pipeline_id=?",
                (cursor, record.run_id, time.time(), record.pipeline_id),
            )
        else:
            self._conn.execute(
                "UPDATE pipelines SET state='running', n_tasks_done=?, run_id=? WHERE pipeline_id=?",
                (cursor, record.run_id, record.pipeline_id),
            )
        return stored

    def handoffs(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int | None = None
    ) -> list[HandoffRecord]:
        """The ledger, oldest first; ``limit`` keeps the newest N, oldest first (the ``events`` rule).

        An empty list on a store that predates the feature: it has no ledger, which is the truth, and
        its pipelines cannot have handed off.
        """
        if not self._has_handoffs():
            return []
        sql = "SELECT * FROM handoffs WHERE 1=1"
        args: list[Any] = []
        if pipeline_id:
            sql += " AND pipeline_id=?"
            args.append(pipeline_id)
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        sql += " ORDER BY handoff_id DESC"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [_to_handoff(r) for r in reversed(rows)]

    # ---------------------------------------------------------------- events
    def emit_event(self, event: EventRecord) -> None:
        cur = self._conn.execute(
            "INSERT INTO events (ts,scope,kind,run_id,pipeline_id,task_run_id,pool,resource_id,data_json) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                event.ts, event.scope, event.kind, event.run_id, event.pipeline_id, event.task_run_id,
                event.pool, event.resource_id, _dumps(event.data),
            ),
        )
        self._conn.commit()
        event.event_id = cur.lastrowid

    def upsert_resource(
        self,
        pool: str,
        resource_id: str,
        kind: str,
        spec: Mapping[str, Any],
        state: str,
        stats: Mapping[str, Any],
        *,
        published_by: str = "",
    ) -> None:
        now = time.time()
        self._conn.execute(
            "INSERT INTO resources (pool,resource_id,kind,spec_json,state,stats_json,published_by,"
            "published_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pool,resource_id) DO UPDATE SET state=excluded.state, stats_json=excluded.stats_json,"
            " updated_at=excluded.updated_at",
            (pool, resource_id, kind, _dumps(spec), state, _dumps(stats), published_by, now, now),
        )
        self._conn.commit()

    def resources(self, pool: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM resources WHERE 1=1"
        args: list[Any] = []
        if pool:
            sql += " AND pool=?"
            args.append(pool)
        sql += " ORDER BY pool, resource_id"
        return [
            {
                "pool": row["pool"],
                "resource_id": row["resource_id"],
                "kind": row["kind"],
                "spec": json.loads(row["spec_json"]),
                "state": row["state"],
                "stats": json.loads(row["stats_json"]),
                "published_by": row["published_by"],
                "updated_at": row["updated_at"],
            }
            for row in self._conn.execute(sql, args).fetchall()
        ]

    # ----------------------------------------------------------- query views
    # Batched whole-kind reads (the optional PagedStore extension). Each query is a keyset page:
    # `WHERE <key> > (last row of the previous page) ORDER BY <key> LIMIT ITER_BATCH_SIZE`. Keyset
    # rather than one long-lived cursor so no read statement stays open while the caller processes
    # a row, and rather than `fetchall` so the Python-side working set is one batch, not the table.
    def _iter_keyset(
        self,
        table: str,
        where: list[str],
        args: list[Any],
        *,
        columns: tuple[str, ...],
        mapper: Any,
        bound: str | None = None,
    ) -> Iterator[Any]:
        """Page through one table with a keyset predicate, one bounded batch per query.

        ``columns`` must end in a **unique** key. The tables are keyed by ``task_run_id`` /
        ``artifact_id`` while the natural order is by ``(pipeline_id, seq)`` / ``seq``, and those
        prefixes are not unique; a strict ``>`` cursor over a non-unique prefix silently skips every
        row that ties with the last row of a page.

        ``bound`` names a monotonic column (``event_id`` / ``attempt_id``). The high-water mark among
        the matching rows is captured before the first page and every page is restricted to it, so
        rows appended while the caller consumes the iterator are never exported. Without it, the
        traversal is best-effort over a live table: rows appended ahead of the cursor can appear.
        """
        filters = list(where)
        params = list(args)
        if bound is not None:
            clause = (" WHERE " + " AND ".join(filters)) if filters else ""
            mark = self._conn.execute(
                f"SELECT MAX({bound}) FROM {table}{clause}", params
            ).fetchone()[0]
            if mark is None:
                return  # nothing matched when the iterator started
            filters.append(f"{bound}<=?")
            params.append(mark)

        predicate = _keyset_predicate(columns)
        order = ", ".join(columns)
        cursor: tuple[Any, ...] | None = None
        while True:
            sql = f"SELECT * FROM {table}"
            page_params = list(params)
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            if cursor is not None:
                sql += (" AND " if filters else " WHERE ") + predicate
                page_params += _keyset_params(cursor)
            sql += f" ORDER BY {order} LIMIT ?"
            page_params.append(ITER_BATCH_SIZE)
            rows = self._conn.execute(sql, page_params).fetchall()
            if not rows:
                return
            for row in rows:
                yield mapper(row)
            if len(rows) < ITER_BATCH_SIZE:
                return
            cursor = tuple(rows[-1][column] for column in columns)

    def iter_pipelines(
        self, *, run_id: str | None = None, state: str | None = None
    ) -> Iterator[PipelineRecord]:
        """``created_at`` then ``pipeline_id`` — total, because ``pipeline_id`` is the primary key
        and the upsert never rewrites ``created_at``."""
        where: list[str] = []
        args: list[Any] = []
        if run_id:
            where.append("run_id=?")
            args.append(run_id)
        if state:
            where.append("state=?")
            args.append(state)
        yield from self._iter_keyset(
            "pipelines", where, args,
            columns=("created_at", "pipeline_id"), mapper=_to_pipeline,
        )

    def iter_tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None
    ) -> Iterator[TaskRecord]:
        """``pipeline_id``, ``seq``, then ``task_run_id`` — the last one makes the cursor unique."""
        where: list[str] = []
        args: list[Any] = []
        if pipeline_id:
            where.append("pipeline_id=?")
            args.append(pipeline_id)
        if run_id:
            where.append("run_id=?")
            args.append(run_id)
        yield from self._iter_keyset(
            "tasks", where, args,
            columns=("pipeline_id", "seq", "task_run_id"), mapper=_to_task,
        )

    def iter_attempts(
        self, *, run_id: str | None = None, pipeline_id: str | None = None
    ) -> Iterator[AttemptRecord]:
        """``attempt_id`` (total, monotonic); bounded by the mark taken when iteration starts."""
        where: list[str] = []
        args: list[Any] = []
        if run_id:
            where.append("run_id=?")
            args.append(run_id)
        if pipeline_id:
            where.append("pipeline_id=?")
            args.append(pipeline_id)
        yield from self._iter_keyset(
            "attempts", where, args,
            columns=("attempt_id",), mapper=_to_attempt, bound="attempt_id",
        )

    def iter_events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None,
        kind: str | None = None
    ) -> Iterator[EventRecord]:
        """``event_id`` (total, monotonic); bounded by the mark taken when iteration starts."""
        where: list[str] = []
        args: list[Any] = []
        if pipeline_id:
            where.append("pipeline_id=?")
            args.append(pipeline_id)
        if run_id:
            where.append("run_id=?")
            args.append(run_id)
        if kind:
            where.append("kind=?")
            args.append(kind)
        yield from self._iter_keyset(
            "events", where, args,
            columns=("event_id",), mapper=_to_event, bound="event_id",
        )

    def iter_artifacts(self, *, pipeline_id: str) -> Iterator[Artifact]:
        """``seq`` then ``artifact_id`` — the last one makes the cursor unique within a pipeline."""
        yield from self._iter_keyset(
            "artifacts", ["pipeline_id=?"], [pipeline_id],
            columns=("seq", "artifact_id"),
            mapper=lambda row: _hydrate(_to_artifact(row), self.backend),
        )

    def pipelines(
        self, *, run_id: str | None = None, state: str | None = None, limit: int | None = None
    ) -> list[PipelineRecord]:
        sql = "SELECT * FROM pipelines WHERE 1=1"
        args: list[Any] = []
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        if state:
            sql += " AND state=?"
            args.append(state)
        sql += " ORDER BY created_at"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        return [_to_pipeline(r) for r in self._conn.execute(sql, args).fetchall()]

    def tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None, limit: int | None = None
    ) -> list[TaskRecord]:
        sql = "SELECT * FROM tasks WHERE 1=1"
        args: list[Any] = []
        if pipeline_id:
            sql += " AND pipeline_id=?"
            args.append(pipeline_id)
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        sql += " ORDER BY pipeline_id, seq, visit, task_run_id"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        return [_to_task(r) for r in self._conn.execute(sql, args).fetchall()]

    def attempts(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        limit: int | None = None,
    ) -> list[AttemptRecord]:
        sql = "SELECT * FROM attempts WHERE 1=1"
        args: list[Any] = []
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        if pipeline_id:
            sql += " AND pipeline_id=?"
            args.append(pipeline_id)
        sql += " ORDER BY attempt_id"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        return [_to_attempt(r) for r in self._conn.execute(sql, args).fetchall()]

    def events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None,
        kind: str | None = None, limit: int = 200
    ) -> list[EventRecord]:
        sql = "SELECT * FROM events WHERE 1=1"
        args: list[Any] = []
        if pipeline_id:
            sql += " AND pipeline_id=?"
            args.append(pipeline_id)
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        if kind:
            sql += " AND kind=?"
            args.append(kind)
        sql += " ORDER BY event_id DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [_to_event(r) for r in reversed(rows)]

    def stats(self, run_id: str | None = None) -> dict[str, Any]:
        where, args = ("WHERE run_id=?", [run_id]) if run_id else ("", [])
        by_state = {
            row["state"]: row["n"]
            for row in self._conn.execute(
                f"SELECT state, COUNT(*) AS n FROM pipelines {where} GROUP BY state", args
            )
        }
        total = sum(by_state.values())
        durations = [
            r["d"] * 1000.0
            for r in self._conn.execute(
                f"SELECT (finished_at - started_at) AS d FROM pipelines {where} "
                f"{'AND' if where else 'WHERE'} finished_at IS NOT NULL AND started_at IS NOT NULL "
                "ORDER BY d",
                args,
            )
        ]
        task_where, task_args = ("WHERE run_id=?", [run_id]) if run_id else ("", [])

        def _pct(p: float) -> float | None:
            if not durations:
                return None
            idx = min(len(durations) - 1, int(p * (len(durations) - 1)))
            return round(durations[idx], 3)

        return {
            "pipelines": {
                "total": total,
                "by_state": by_state,
                "duration_ms": {"p50": _pct(0.5), "p95": _pct(0.95), "max": _pct(1.0)},
            },
            "tasks": {
                "by_name": {
                    r["name"]: r["n"]
                    for r in self._conn.execute(
                        f"SELECT name, COUNT(*) AS n FROM tasks {task_where} GROUP BY name", task_args
                    )
                },
                "attempts_by_name": {
                    r["name"]: r["n"]
                    for r in self._conn.execute(
                        f"SELECT name, SUM(attempts_used) AS n FROM tasks {task_where} GROUP BY name",
                        task_args,
                    )
                },
            },
            "attempts_total": self._conn.execute(
                f"SELECT COUNT(*) AS n FROM attempts {task_where}", task_args
            ).fetchone()["n"],
            # Handoffs are counted for every run, not only control-enabled ones: a run with none
            # reports 0, which keeps the stats shape stable for readers — including a store created
            # before this feature existed, which has no ledger table at all.
            "handoffs_total": (
                self._conn.execute(
                    f"SELECT COUNT(*) AS n FROM handoffs {task_where}", task_args
                ).fetchone()["n"]
                if self._has_handoffs()
                else 0
            ),
            "events_total": self._conn.execute(
                f"SELECT COUNT(*) AS n FROM events {task_where}", task_args
            ).fetchone()["n"],
            # Count failed terminal repair attempts from the event log, without materializing it.
            # This is a per-run observability metric: the live run still uses RunReport.repair_failures.
            "repair_failures": self._conn.execute(
                f"SELECT COUNT(*) AS n FROM events {task_where} "
                f"{'AND' if task_where else 'WHERE'} kind='pipeline.terminal_repair_failed'",
                task_args,
            ).fetchone()["n"],
        }

    def errors(self, *, run_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        sql = (
            "SELECT pipeline_id,name,failed_task,error_type,error_message,finished_at FROM pipelines "
            "WHERE state='failed'"
        )
        args: list[Any] = []
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        sql += " ORDER BY finished_at DESC LIMIT ?"
        args.append(limit)
        return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def export_rows(self, *, run_id: str | None = None) -> Iterator[dict[str, Any]]:
        # Paged over pipelines: the nested part (tasks + artifacts) is materialized per pipeline,
        # which is the documented memory unit, not per store.
        for record in self.iter_pipelines(run_id=run_id):
            tasks = self.tasks(record.pipeline_id)
            arts = self.artifacts(record.pipeline_id)
            row = {
                "pipeline_id": record.pipeline_id,
                "key": record.key,
                "name": record.name,
                "run_id": record.run_id,
                "state": record.state,
                "tags": record.tags,
                "n_tasks_done": record.n_tasks_done,
                "n_tasks_total": record.n_tasks_total,
                "attempts_total": record.attempts_total,
                "handoff_floor": record.handoff_floor,
                "started_at": record.started_at,
                "finished_at": record.finished_at,
                "duration_ms": (
                    round((record.finished_at - record.started_at) * 1000.0, 3)
                    if record.started_at and record.finished_at
                    else None
                ),
                "failed_task": record.failed_task,
                "error_type": record.error_type,
                "error_message": record.error_message,
                "tasks": [
                    {
                        "name": t.name,
                        "seq": t.seq,
                        "state": t.state,
                        "attempts_used": t.attempts_used,
                        "duration_ms": t.duration_ms,
                        "error_class": t.error_class,
                        "error_type": t.error_type,
                        "error_message": t.error_message,
                        "output_artifact_id": t.output_artifact_id,
                    }
                    for t in tasks
                ],
                "artifacts": [
                    {
                        "task": a.task_name,
                        "seq": a.seq,
                        "type": a.type_name,
                        "codec": a.codec,
                        "digest": a.digest,
                        "is_final": a.is_final,
                        "payload": _decode_payload(a),
                    }
                    for a in arts
                ],
                # Empty for an ordinary pipeline, and always present: one row shape for every pipeline.
                "handoffs": [handoff_row(h) for h in self.handoffs(pipeline_id=record.pipeline_id)],
            }

            traversal = self.visit_state(record.pipeline_id)
            if traversal is not None:
                row["control"] = traversal
                for item, task in zip(row["tasks"], self.tasks(record.pipeline_id), strict=True):
                    item.update(task_run_id=task.task_run_id, visit=task.visit,
                                active=traversal["active"].get(str(task.seq), {}).get("task_run_id") == task.task_run_id,
                                input_artifact_id=task.input_artifact_id)
                for item, artifact in zip(row["artifacts"], self.artifacts(record.pipeline_id), strict=True):
                    item.update(artifact_id=artifact.id, visit=artifact.visit,
                                active=artifact.id == traversal["terminal"] or any(
                                    slot["output"] == artifact.id for slot in traversal["active"].values()))
            yield row

    def close(self) -> None:
        with contextlib.suppress(Exception):  # pragma: no cover - defensive
            self._conn.close()


# ------------------------------------------------------------ row mapping
def _keyset_predicate(columns: tuple[str, ...]) -> str:
    """``(a, b) > (?, ?)`` written out column by column.

    SQLite only learned row values in 3.15, and the spelled-out form is what lets the query use an
    index on ``(a, b)`` for both the range and the order.
    """
    terms = []
    for index, column in enumerate(columns):
        equalities = [f"{earlier}=?" for earlier in columns[:index]]
        terms.append("(" + " AND ".join([*equalities, f"{column}>?"]) + ")")
    return "(" + " OR ".join(terms) + ")"


def _keyset_params(cursor: tuple[Any, ...]) -> list[Any]:
    """The parameters :func:`_keyset_predicate` expects for one cursor row."""
    params: list[Any] = []
    for index in range(len(cursor)):
        params.extend(cursor[:index])
        params.append(cursor[index])
    return params


def _to_run(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"], label=row["label"], status=row["status"], started_at=row["started_at"],
        ended_at=row["ended_at"], heartbeat_at=row["heartbeat_at"], spec_digest=row["spec_digest"],
        code_version=row["code_version"], python=row["python"], host=row["host"],
        config=json.loads(row["config_json"]), notes=row["notes"], resume_of=row["resume_of"],
    )


def _to_pipeline(row: sqlite3.Row) -> PipelineRecord:
    return PipelineRecord(
        pipeline_id=row["pipeline_id"], run_id=row["run_id"], name=row["name"], key=row["key"],
        state=row["state"], tags=json.loads(row["tags_json"]), created_at=row["created_at"],
        started_at=row["started_at"], finished_at=row["finished_at"],
        n_tasks_total=row["n_tasks_total"], n_tasks_done=row["n_tasks_done"],
        attempts_total=row["attempts_total"], failed_task=row["failed_task"],
        error_type=row["error_type"], error_message=row["error_message"], traceback=row["traceback"],
        seed_digest=row["seed_digest"], spec_digest=row["spec_digest"], resume_of=row["resume_of"],
        handoff_floor=row["handoff_floor"] if "handoff_floor" in row.keys() else 0,  # noqa: SIM118 (sqlite Row membership checks values)
    )


def _to_task(row: sqlite3.Row) -> TaskRecord:
    return TaskRecord(
        task_run_id=row["task_run_id"], pipeline_id=row["pipeline_id"], run_id=row["run_id"],
        name=row["name"], seq=row["seq"], state=row["state"], attempts_used=row["attempts_used"],
        started_at=row["started_at"], ended_at=row["ended_at"], duration_ms=row["duration_ms"],
        input_artifact_id=row["input_artifact_id"], output_artifact_id=row["output_artifact_id"],
        error_class=row["error_class"], error_type=row["error_type"],
        error_message=row["error_message"], traceback=row["traceback"],
        leases=json.loads(row["leases_json"]), metrics=json.loads(row["metrics_json"]),
        visit=row["visit"] if "visit" in row.keys() else 0,  # noqa: SIM118
    )


def _to_attempt(row: sqlite3.Row) -> AttemptRecord:
    return AttemptRecord(
        attempt_id=row["attempt_id"], run_id=row["run_id"], pipeline_id=row["pipeline_id"],
        task_run_id=row["task_run_id"], task_name=row["task_name"], seq=row["seq"],
        attempt_no=row["attempt_no"], started_at=row["started_at"], ended_at=row["ended_at"],
        duration_ms=row["duration_ms"], outcome=row["outcome"], error_class=row["error_class"],
        error_type=row["error_type"], error_message=row["error_message"], traceback=row["traceback"],
        retry_delay_s=row["retry_delay_s"], decision=json.loads(row["decision_json"]),
        leases=json.loads(row["leases_json"]), metrics=json.loads(row["metrics_json"]),
        visit=row["visit"] if "visit" in row.keys() else 0,  # noqa: SIM118
    )


def _to_event(row: sqlite3.Row) -> EventRecord:
    return EventRecord(
        ts=row["ts"], kind=row["kind"], scope=row["scope"], run_id=row["run_id"],
        pipeline_id=row["pipeline_id"], task_run_id=row["task_run_id"], pool=row["pool"],
        resource_id=row["resource_id"], data=json.loads(row["data_json"]), event_id=row["event_id"],
    )


def _to_handoff(row: sqlite3.Row) -> HandoffRecord:
    return HandoffRecord(
        handoff_id=row["handoff_id"], pipeline_id=row["pipeline_id"], run_id=row["run_id"],
        from_seq=row["from_seq"], from_task=row["from_task"], to_seq=row["to_seq"],
        to_task=row["to_task"], entry_seq=row["entry_seq"],
        entry_artifact_id=row["entry_artifact_id"], entry_reused=bool(row["entry_reused"]),
        reason=row["reason"], ts=row["ts"],
        operation=row["operation"] if "operation" in row.keys() else "forward",  # noqa: SIM118
        from_visit=row["from_visit"] if "from_visit" in row.keys() else 0,  # noqa: SIM118
        to_visit=row["to_visit"] if "to_visit" in row.keys() else None,  # noqa: SIM118
        transition_version=row["transition_version"] if "transition_version" in row.keys() else None,  # noqa: SIM118
    )


def _to_artifact(row: sqlite3.Row) -> Artifact:
    payload = row["payload"]
    return Artifact(
        id=row["artifact_id"], pipeline_id=row["pipeline_id"], task_name=row["task_name"],
        seq=row["seq"], type_name=row["type_name"], codec=row["codec"], digest=row["digest"],
        size=row["size"], payload=bytes(payload) if payload is not None else None,
        created_at=row["created_at"], is_final=bool(row["is_final"]),
        # `in row` would test *values* (sqlite3.Row iterates values), so keys() is the only way
        # to ask about a column name on an older row shape.
        visit=row["visit"] if "visit" in row.keys() else 0,  # noqa: SIM118
        blob_ref=row["blob_ref"] if "blob_ref" in row.keys() else None,  # noqa: SIM118 (values vs keys)
    )


def _persist_form(artifact: Artifact, journal: str, backend: Any) -> Artifact:
    """Decide what actually lands in the database: inline bytes, a blob reference, or neither."""
    if journal != "full":
        # `journal` is the authority on payload retention: summary/hash-only keeps no bytes
        # anywhere, not even in the backend.
        if artifact.payload is None:
            return artifact
        return dataclasses.replace(artifact, payload=None, blob_ref=None)
    if artifact.payload is not None and backend.wants(artifact):
        return dataclasses.replace(artifact, payload=None, blob_ref=backend.put(artifact))
    return artifact


def _hydrate(artifact: Artifact, backend: Any) -> Artifact:
    """Fill `payload` back in from the backend so callers always see a complete artifact."""
    if artifact.payload is None and artifact.blob_ref:
        data = backend.get(artifact.blob_ref)
        if data is not None:
            return dataclasses.replace(artifact, payload=data)
    return artifact


def _decode_payload(artifact: Artifact) -> Any:
    if artifact.payload is None:
        return None
    if artifact.codec in ("json", "history-v1"):
        try:
            return json.loads(artifact.payload.decode("utf-8"))
        except Exception:  # pragma: no cover - defensive
            return base64.b64encode(artifact.payload).decode("ascii")
    return base64.b64encode(artifact.payload).decode("ascii")
