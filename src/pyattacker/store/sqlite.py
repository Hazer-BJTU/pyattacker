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

from ..artifact import Artifact
from .base import AttemptRecord, EventRecord, PipelineRecord, RunRecord, TaskRecord

__all__ = ["SqliteStore"]

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
    resume_of TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipelines_run ON pipelines(run_id, state);
CREATE INDEX IF NOT EXISTS idx_pipelines_state ON pipelines(state);

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


class SqliteStore:
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
        if not read_only:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=10000")
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()

    def _migrate(self) -> None:
        """Add columns that older stores do not have.

        ``CREATE TABLE IF NOT EXISTS`` silently skips existing tables, so a schema addition needs
        an explicit upgrade step; without it, resuming an old store would fail at the first insert.
        """
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(artifacts)")}
        if "blob_ref" not in columns:
            self._conn.execute("ALTER TABLE artifacts ADD COLUMN blob_ref TEXT")

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
        self._conn.execute(
            "INSERT INTO pipelines (pipeline_id,run_id,name,key,state,tags_json,created_at,started_at,"
            "finished_at,n_tasks_total,n_tasks_done,attempts_total,failed_task,error_type,error_message,"
            "traceback,seed_digest,spec_digest,resume_of) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pipeline_id) DO UPDATE SET run_id=excluded.run_id, state=excluded.state, "
            "started_at=excluded.started_at, finished_at=excluded.finished_at, "
            "n_tasks_total=excluded.n_tasks_total, n_tasks_done=excluded.n_tasks_done, "
            "attempts_total=excluded.attempts_total, failed_task=excluded.failed_task, "
            "error_type=excluded.error_type, error_message=excluded.error_message, "
            "traceback=excluded.traceback, resume_of=excluded.resume_of",
            (
                record.pipeline_id, record.run_id, record.name, record.key, record.state,
                _dumps(record.tags), record.created_at, record.started_at, record.finished_at,
                record.n_tasks_total, record.n_tasks_done, record.attempts_total, record.failed_task,
                record.error_type, record.error_message, record.traceback, record.seed_digest,
                record.spec_digest, record.resume_of,
            ),
        )
        self._conn.commit()

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

    def bump_attempts(self, pipeline_id: str, delta: int = 1) -> None:
        self._conn.execute(
            "UPDATE pipelines SET attempts_total = attempts_total + ? WHERE pipeline_id=?",
            (delta, pipeline_id),
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
    def put_artifact(self, artifact: Artifact) -> Artifact:
        stored = _persist_form(artifact, self.journal, self.backend)
        self._conn.execute(
            "INSERT OR REPLACE INTO artifacts (artifact_id,pipeline_id,task_name,seq,type_name,codec,"
            "digest,size,payload,created_at,is_final,blob_ref) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stored.id, stored.pipeline_id, stored.task_name, stored.seq, stored.type_name,
                stored.codec, stored.digest, stored.size, stored.payload, stored.created_at,
                1 if stored.is_final else 0, stored.blob_ref,
            ),
        )
        self._conn.commit()
        return stored

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None:
        row = self._conn.execute(
            "SELECT * FROM artifacts WHERE pipeline_id=? AND seq=?", (pipeline_id, seq)
        ).fetchone()
        return _hydrate(_to_artifact(row), self.backend) if row else None

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        self._conn.execute(
            "UPDATE artifacts SET is_final=1 WHERE pipeline_id=? AND seq=?", (pipeline_id, seq)
        )
        self._conn.commit()

    def artifacts(self, pipeline_id: str) -> list[Artifact]:
        rows = self._conn.execute(
            "SELECT * FROM artifacts WHERE pipeline_id=? ORDER BY seq", (pipeline_id,)
        ).fetchall()
        return [_hydrate(_to_artifact(r), self.backend) for r in rows]

    # ----------------------------------------------------------------- tasks
    def record_task(self, record: TaskRecord) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO tasks (task_run_id,pipeline_id,run_id,name,seq,state,attempts_used,"
            "started_at,ended_at,duration_ms,input_artifact_id,output_artifact_id,error_class,error_type,"
            "error_message,traceback,leases_json,metrics_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record.task_run_id, record.pipeline_id, record.run_id, record.name, record.seq,
                record.state, record.attempts_used, record.started_at, record.ended_at,
                record.duration_ms, record.input_artifact_id, record.output_artifact_id,
                record.error_class, record.error_type, record.error_message, record.traceback,
                _dumps(record.leases), _dumps(record.metrics),
            ),
        )
        self._conn.commit()

    def record_attempt(self, record: AttemptRecord) -> AttemptRecord:
        cur = self._conn.execute(
            "INSERT INTO attempts (run_id,pipeline_id,task_run_id,task_name,seq,attempt_no,started_at,"
            "ended_at,duration_ms,outcome,error_class,error_type,error_message,traceback,retry_delay_s,"
            "decision_json,leases_json,metrics_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record.run_id, record.pipeline_id, record.task_run_id, record.task_name, record.seq,
                record.attempt_no, record.started_at, record.ended_at, record.duration_ms, record.outcome,
                record.error_class, record.error_type, record.error_message, record.traceback,
                record.retry_delay_s, _dumps(record.decision), _dumps(record.leases),
                _dumps(record.metrics),
            ),
        )
        self._conn.commit()
        record.attempt_id = cur.lastrowid
        return record

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

    # ----------------------------------------------------------- query views
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
        sql += " ORDER BY pipeline_id, seq"
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
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int = 200
    ) -> list[EventRecord]:
        sql = "SELECT * FROM events WHERE 1=1"
        args: list[Any] = []
        if pipeline_id:
            sql += " AND pipeline_id=?"
            args.append(pipeline_id)
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
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
            "events_total": self._conn.execute(
                f"SELECT COUNT(*) AS n FROM events {task_where}", task_args
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
        for record in self.pipelines(run_id=run_id):
            tasks = self.tasks(record.pipeline_id)
            arts = self.artifacts(record.pipeline_id)
            yield {
                "pipeline_id": record.pipeline_id,
                "key": record.key,
                "name": record.name,
                "run_id": record.run_id,
                "state": record.state,
                "tags": record.tags,
                "n_tasks_done": record.n_tasks_done,
                "n_tasks_total": record.n_tasks_total,
                "attempts_total": record.attempts_total,
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
            }

    def close(self) -> None:
        with contextlib.suppress(Exception):  # pragma: no cover - defensive
            self._conn.close()


# ------------------------------------------------------------ row mapping
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
    )


def _to_event(row: sqlite3.Row) -> EventRecord:
    return EventRecord(
        ts=row["ts"], kind=row["kind"], scope=row["scope"], run_id=row["run_id"],
        pipeline_id=row["pipeline_id"], task_run_id=row["task_run_id"], pool=row["pool"],
        resource_id=row["resource_id"], data=json.loads(row["data_json"]), event_id=row["event_id"],
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
    if artifact.codec == "json":
        try:
            return json.loads(artifact.payload.decode("utf-8"))
        except Exception:  # pragma: no cover - defensive
            return base64.b64encode(artifact.payload).decode("ascii")
    return base64.b64encode(artifact.payload).decode("ascii")
