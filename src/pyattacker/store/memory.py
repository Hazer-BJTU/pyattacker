"""In-memory store —— for tests, dry runs and "nothing hits disk" scenarios. Semantics match SqliteStore.

Note: internal containers always use an underscore prefix so they do not collide with the
method names in the :class:`~pyattacker.store.base.Store` protocol (``pipelines`` / ``events`` / ``artifacts`` / ``tasks``).
"""

from __future__ import annotations

import base64
import dataclasses
import json
import time
from collections.abc import Iterator, Mapping
from typing import Any

from ..artifact import Artifact, Encoded
from ..errors import PyAttackerError
from .base import (
    AttemptRecord,
    EventRecord,
    HandoffRecord,
    PipelineRecord,
    RunRecord,
    TaskRecord,
    handoff_row,
)

__all__ = ["MemoryStore"]


class MemoryStore:
    """Pure in-memory :class:`~pyattacker.store.base.Store` implementation — for tests, dry runs, and "nothing hits disk" scenarios.

    Invariants: satisfies the same semantics as :class:`~pyattacker.store.sqlite.SqliteStore`
    (state writes are synchronous, journal modes behave the same way), but every read/write is a
    plain dict/list operation — no serialization round-trip, no durability across process restarts.

    Collaborators: constructed by ``open_store(":memory:")``; interchangeable with
    ``SqliteStore`` anywhere a ``Store`` is expected.
    """

    def __init__(self, *, journal: str = "full", backend: Any = None) -> None:
        self.journal = journal
        from ..backends import resolve_backend

        self.backend = resolve_backend(backend)
        self._runs: dict[str, RunRecord] = {}
        self._pipelines: dict[str, PipelineRecord] = {}
        self._artifacts: dict[tuple[str, int], Artifact] = {}
        self._tasks: dict[str, TaskRecord] = {}
        self._resources: dict[tuple[str, str], dict[str, Any]] = {}
        self._attempts: list[AttemptRecord] = []
        self._handoffs: list[HandoffRecord] = []
        self._events: list[EventRecord] = []
        self._event_id = 0

    # ------------------------------------------------------------------ runs
    def start_run(self, run: RunRecord) -> RunRecord:
        run.heartbeat_at = run.started_at
        self._runs[run.run_id] = run
        return run

    def heartbeat(self, run_id: str, ts: float | None = None) -> None:
        run = self._runs.get(run_id)
        if run is not None:
            run.heartbeat_at = ts or time.time()

    def finish_run(self, run_id: str, status: str, ended_at: float | None = None) -> None:
        run = self._runs.get(run_id)
        if run is not None:
            run.status = status
            run.ended_at = ended_at or time.time()

    def get_run(self, run_id: str) -> RunRecord | None:
        return self._runs.get(run_id)

    # ------------------------------------------------------------- pipelines
    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        return self._pipelines.get(pipeline_id)

    def upsert_pipeline(self, record: PipelineRecord) -> None:
        self._pipelines[record.pipeline_id] = record

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
        record = self._pipelines.get(pipeline_id)
        if record is None:
            return
        record.state = state
        record.finished_at = time.time()
        if n_tasks_done is not None:
            record.n_tasks_done = n_tasks_done
        if error is not None:
            record.error_type = type(error).__name__
            record.error_message = str(error)[:2000]
            record.traceback = traceback
            record.failed_task = failed_task

    def settle_pipeline(
        self, pipeline_id: str, *, state: str, n_tasks_done: int, run_id: str
    ) -> None:
        """Terminal settle in one step (see ``store/base.py``): no torn intermediate row."""
        record = self._pipelines.get(pipeline_id)
        if record is None:
            return
        record.state = state
        record.n_tasks_done = n_tasks_done
        record.run_id = run_id
        record.finished_at = time.time()
        record.error_type = record.error_message = record.traceback = record.failed_task = None

    def interrupt_stale(self, *, stale_after_s: float = 30.0, keep_run_id: str | None = None) -> int:
        now = time.time()
        count = 0
        for record in self._pipelines.values():
            if record.state != "running" or record.run_id == keep_run_id:
                continue
            run = self._runs.get(record.run_id)
            stale = (
                run is None
                or run.status != "running"
                or (run.heartbeat_at is not None and now - run.heartbeat_at > stale_after_s)
            )
            if stale:
                record.state = "interrupted"
                count += 1
        for task in self._tasks.values():
            if task.state == "running":
                run = self._runs.get(task.run_id)
                if run is None or run.status != "running":
                    task.state = "interrupted"
        return count

    # ------------------------------------------------------------- artifacts
    def put_artifact(self, artifact: Artifact) -> Artifact:
        stored = _persist_form(artifact, self.journal, self.backend)
        self._artifacts[(stored.pipeline_id, stored.seq)] = stored
        return stored

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None:
        artifact = self._artifacts.get((pipeline_id, seq))
        return None if artifact is None else _hydrate(artifact, self.backend)

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        for key, artifact in list(self._artifacts.items()):
            if key[0] == pipeline_id:
                self._artifacts[key] = dataclasses.replace(artifact, is_final=key[1] == seq)

    def artifacts(self, pipeline_id: str) -> list[Artifact]:
        items = [a for (pid, _), a in self._artifacts.items() if pid == pipeline_id]
        return [_hydrate(a, self.backend) for a in sorted(items, key=lambda a: a.seq)]

    # ----------------------------------------------------------------- tasks
    def record_task(self, record: TaskRecord) -> None:
        self._tasks[record.task_run_id] = record

    def tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None, limit: int | None = None
    ) -> list[TaskRecord]:
        items = [
            t
            for t in self._tasks.values()
            if (pipeline_id is None or t.pipeline_id == pipeline_id) and (run_id is None or t.run_id == run_id)
        ]
        items.sort(key=lambda t: (t.pipeline_id, t.seq))
        return items[:limit] if limit else items

    def record_attempt(self, record: AttemptRecord) -> AttemptRecord:
        record.attempt_id = len(self._attempts) + 1
        self._attempts.append(record)
        return record

    # -------------------------------------------------------------- handoffs
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
        """One step, no torn state to reason about (see the capability contract in ``store/base.py``)."""
        pipeline = self._pipelines.get(record.pipeline_id)
        if pipeline is None:
            raise PyAttackerError(
                f"commit_handoff: no pipeline row for {record.pipeline_id!r}; the handoff has no pipeline "
                "to advance (a handoff is only ever committed while its pipeline is open)"
            )
        stored: Artifact | None = None
        if payload is not None:
            recorded = sum(1 for row in self._handoffs if row.pipeline_id == record.pipeline_id)
            seq = pipeline.n_tasks_total + recorded
            stored = self.put_artifact(
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
                )
            )
            record.entry_seq = seq
        if record.entry_seq is None:
            raise PyAttackerError(
                "commit_handoff: entry_seq is required when a handoff reuses an artifact (only a payload "
                "handoff lets the store allocate the entry address)"
            )
        record.entry_artifact_id = Artifact.build_id(record.pipeline_id, record.entry_seq)
        self.record_task(task)
        attempt.attempt_id = len(self._attempts) + 1
        self._attempts.append(attempt)
        record.handoff_id = len(self._handoffs) + 1
        self._handoffs.append(record)
        pipeline.n_tasks_done = cursor
        pipeline.run_id = record.run_id
        if final:
            self.mark_final(record.pipeline_id, record.entry_seq)
            pipeline.state = "succeeded"
            pipeline.finished_at = time.time()
            pipeline.error_type = pipeline.error_message = pipeline.traceback = pipeline.failed_task = None
        else:
            pipeline.state = "running"
        return stored

    def handoffs(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int | None = None
    ) -> list[HandoffRecord]:
        """The ledger, oldest first; ``limit`` keeps the newest N, oldest first (the ``events`` rule)."""
        items = [
            row
            for row in self._handoffs
            if (pipeline_id is None or row.pipeline_id == pipeline_id)
            and (run_id is None or row.run_id == run_id)
        ]
        return items[-limit:] if limit else items

    def attempts(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        limit: int | None = None,
    ) -> list[AttemptRecord]:
        items = [
            a
            for a in self._attempts
            if (run_id is None or a.run_id == run_id) and (pipeline_id is None or a.pipeline_id == pipeline_id)
        ]
        return items[-limit:] if limit else items

    # ---------------------------------------------------------------- events
    def emit_event(self, event: EventRecord) -> None:
        self._event_id += 1
        event.event_id = self._event_id
        self._events.append(event)

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
        self._resources[(pool, resource_id)] = {
            "pool": pool,
            "resource_id": resource_id,
            "kind": kind,
            "spec": dict(spec),
            "state": state,
            "stats": dict(stats),
            "published_by": published_by,
            "updated_at": time.time(),
        }

    def resources(self, pool: str | None = None) -> list[dict[str, Any]]:
        return [
            dict(item)
            for (p, _), item in self._resources.items()
            if pool is None or p == pool
        ]

    # ----------------------------------------------------------- query views
    # Batched whole-kind reads (the optional PagedStore extension). A memory store already holds
    # every row, so "bounded memory" is inherent; these yield the live records (no copies, and an
    # artifact's payload is hydrated on demand) while keeping the order and the live-store semantics
    # identical to SqliteStore's.
    def iter_pipelines(
        self, *, run_id: str | None = None, state: str | None = None
    ) -> Iterator[PipelineRecord]:
        """``created_at`` then ``pipeline_id`` — total, because ``pipeline_id`` is the key and the
        upsert never rewrites ``created_at``."""
        items = [
            p
            for p in self._pipelines.values()
            if (run_id is None or p.run_id == run_id) and (state is None or p.state == state)
        ]
        items.sort(key=lambda p: (p.created_at, p.pipeline_id))
        yield from items

    def iter_tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None
    ) -> Iterator[TaskRecord]:
        """``pipeline_id``, ``seq``, then ``task_run_id`` — the last one makes the cursor unique."""
        items = [
            t
            for t in self._tasks.values()
            if (pipeline_id is None or t.pipeline_id == pipeline_id) and (run_id is None or t.run_id == run_id)
        ]
        items.sort(key=lambda t: (t.pipeline_id, t.seq, t.task_run_id))
        yield from items

    def iter_attempts(
        self, *, run_id: str | None = None, pipeline_id: str | None = None
    ) -> Iterator[AttemptRecord]:
        """``attempt_id`` (total, monotonic); bounded by the mark taken when iteration starts."""
        mark = max(
            (
                record.attempt_id
                for record in self._attempts
                if record.attempt_id is not None
                and (run_id is None or record.run_id == run_id)
                and (pipeline_id is None or record.pipeline_id == pipeline_id)
            ),
            default=None,
        )
        if mark is None:
            return  # nothing matched when the iterator started
        for record in self._attempts:
            if record.attempt_id is not None and record.attempt_id > mark:
                return  # appended after the mark: bounded, like the SQLite side
            if (run_id is None or record.run_id == run_id) and (
                pipeline_id is None or record.pipeline_id == pipeline_id
            ):
                yield record

    def iter_events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None
    ) -> Iterator[EventRecord]:
        """``event_id`` (total, monotonic); bounded by the mark taken when iteration starts."""
        mark = max(
            (
                event.event_id
                for event in self._events
                if event.event_id is not None
                and (pipeline_id is None or event.pipeline_id == pipeline_id)
                and (run_id is None or event.run_id == run_id)
            ),
            default=None,
        )
        if mark is None:
            return  # nothing matched when the iterator started
        for event in self._events:
            if event.event_id is not None and event.event_id > mark:
                return  # appended after the mark: bounded, like the SQLite side
            if (pipeline_id is None or event.pipeline_id == pipeline_id) and (
                run_id is None or event.run_id == run_id
            ):
                yield event

    def iter_artifacts(self, *, pipeline_id: str) -> Iterator[Artifact]:
        """``seq`` then ``artifact_id`` — the last one makes the cursor unique within a pipeline."""
        items = [a for (pid, _), a in self._artifacts.items() if pid == pipeline_id]
        for artifact in sorted(items, key=lambda a: (a.seq, a.id)):
            yield _hydrate(artifact, self.backend)

    def pipelines(
        self, *, run_id: str | None = None, state: str | None = None, limit: int | None = None
    ) -> list[PipelineRecord]:
        items = [
            p
            for p in self._pipelines.values()
            if (run_id is None or p.run_id == run_id) and (state is None or p.state == state)
        ]
        items.sort(key=lambda p: p.created_at)
        return items[:limit] if limit else items

    def events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int = 200
    ) -> list[EventRecord]:
        items = [
            e
            for e in self._events
            if (pipeline_id is None or e.pipeline_id == pipeline_id)
            and (run_id is None or e.run_id == run_id)
        ]
        return items[-limit:]

    def all_events(self) -> list[EventRecord]:
        return list(self._events)

    def stats(self, run_id: str | None = None) -> dict[str, Any]:
        pipes = self.pipelines(run_id=run_id)
        by_state: dict[str, int] = {}
        task_counts: dict[str, int] = {}
        attempts_by_task: dict[str, int] = {}
        durations: list[float] = []
        for p in pipes:
            by_state[p.state] = by_state.get(p.state, 0) + 1
            if p.started_at and p.finished_at:
                durations.append((p.finished_at - p.started_at) * 1000.0)
        relevant = [t for t in self._tasks.values() if run_id is None or t.run_id == run_id]
        for t in relevant:
            task_counts[t.name] = task_counts.get(t.name, 0) + 1
            attempts_by_task[t.name] = attempts_by_task.get(t.name, 0) + t.attempts_used
        durations.sort()

        def pct(p: float) -> float | None:
            if not durations:
                return None
            idx = min(len(durations) - 1, int(p * (len(durations) - 1)))
            return round(durations[idx], 3)

        return {
            "pipelines": {
                "total": len(pipes),
                "by_state": by_state,
                "duration_ms": {"p50": pct(0.5), "p95": pct(0.95), "max": pct(1.0)},
            },
            "tasks": {"by_name": task_counts, "attempts_by_name": attempts_by_task},
            "attempts_total": sum(t.attempts_used for t in relevant),
            "handoffs_total": sum(
                1 for h in self._handoffs if run_id is None or h.run_id == run_id
            ),
            "events_total": sum(
                1 for e in self._events if run_id is None or e.run_id == run_id
            ),
        }

    def errors(self, *, run_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        out = [
            {
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "failed_task": p.failed_task,
                "error_type": p.error_type,
                "error_message": p.error_message,
                "when": p.finished_at,
            }
            for p in self.pipelines(run_id=run_id)
            if p.state == "failed"
        ]
        return out[-limit:]

    def export_rows(self, *, run_id: str | None = None) -> Iterator[dict[str, Any]]:
        for p in self.iter_pipelines(run_id=run_id):
            yield {
                "pipeline_id": p.pipeline_id,
                "key": p.key,
                "name": p.name,
                "run_id": p.run_id,
                "state": p.state,
                "tags": p.tags,
                "n_tasks_done": p.n_tasks_done,
                "n_tasks_total": p.n_tasks_total,
                "attempts_total": p.attempts_total,
                "started_at": p.started_at,
                "finished_at": p.finished_at,
                "duration_ms": (
                    round((p.finished_at - p.started_at) * 1000.0, 3)
                    if p.started_at and p.finished_at
                    else None
                ),
                "failed_task": p.failed_task,
                "error_type": p.error_type,
                "error_message": p.error_message,
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
                    for t in self.tasks(p.pipeline_id)
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
                    for a in self.artifacts(p.pipeline_id)
                ],
                # Empty for an ordinary pipeline, and always present: one row shape for every pipeline.
                "handoffs": [handoff_row(h) for h in self.handoffs(pipeline_id=p.pipeline_id)],
            }

    def close(self) -> None:
        return None


def _persist_form(artifact: Artifact, journal: str, backend: Any) -> Artifact:
    """Decide what actually lands in the store: inline bytes, a blob reference, or neither."""
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
