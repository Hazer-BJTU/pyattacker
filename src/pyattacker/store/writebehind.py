"""Write-behind wrapper: batch the append-only facts, keep state writes synchronous.

Why only *append-only* facts (attempts + events)? Because the runner reads state back:
a pipeline checkpoint and an artifact must be durable and visible the moment they are
written — that is the whole point of task-level checkpoints. Attempts and events are
history: nothing inside a run reads them back, so they can be batched.

Flush triggers (no background thread, no timer task — fully deterministic):

* batch size reached,
* ``flush_interval`` elapsed since the last flush (checked whenever something is buffered),
* any read API is called (so a monitor in the same process always sees fresh numbers),
* the runner calls :meth:`flush` from its heartbeat loop and once at the end of a run.

Failure model: a hard crash (SIGKILL) can lose at most the last unsent batch, while
every checkpoint stays intact — a resumed run re-executes only the tasks that were
genuinely unfinished.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from typing import Any

from ..artifact import Artifact, Encoded
from ..errors import ConfigError, StoreFeatureUnsupported
from .base import (
    AttemptRecord,
    EventRecord,
    HandoffRecord,
    PipelineRecord,
    RunRecord,
    Store,
    TaskRecord,
)
from .base import iter_artifacts as _iter_artifacts
from .base import iter_attempts as _iter_attempts
from .base import iter_events as _iter_events
from .base import iter_pipelines as _iter_pipelines
from .base import iter_tasks as _iter_tasks

__all__ = ["WriteBehindStore", "wrap_write_behind"]


class WriteBehindStore:
    """Wrap any :class:`Store`, buffering attempts/events and flushing them in batches."""

    def __init__(
        self,
        inner: Store,
        *,
        batch_size: int = 128,
        flush_interval: float = 1.0,
        clock: Any = None,
    ) -> None:
        self.inner = inner
        self.journal = inner.journal
        self.batch_size = max(1, int(batch_size))
        self.flush_interval = max(0.0, float(flush_interval))
        self._clock = clock or _RealClock()
        self._attempts: list[AttemptRecord] = []
        self._events: list[EventRecord] = []
        self._last_flush = self._clock.now()
        self.flushes = 0
        self.buffered_attempts = 0
        self.buffered_events = 0

    # ------------------------------------------------------------------ buffering
    @property
    def pending(self) -> int:
        return len(self._attempts) + len(self._events)

    def flush(self) -> int:
        """Write every buffered fact to the inner store. Returns how many rows were written."""
        if not self._attempts and not self._events:
            self._last_flush = self._clock.now()
            return 0
        written = 0
        # attempts first: they are the more valuable record if the process dies mid-flush
        for attempt in self._attempts:
            self.inner.record_attempt(attempt)
            written += 1
        self._attempts.clear()
        for event in self._events:
            self.inner.emit_event(event)
            written += 1
        self._events.clear()
        self.flushes += 1
        self._last_flush = self._clock.now()
        return written

    def _maybe_flush(self) -> None:
        if self.pending >= self.batch_size:
            self.flush()
            return
        if self.flush_interval and (self._clock.now() - self._last_flush) >= self.flush_interval:
            self.flush()

    def buffer_stats(self) -> dict[str, Any]:
        return {
            "pending": self.pending,
            "flushes": self.flushes,
            "buffered_attempts": self.buffered_attempts,
            "buffered_events": self.buffered_events,
            "batch_size": self.batch_size,
            "flush_interval": self.flush_interval,
        }

    # ------------------------------------------------------- append-only writes
    def record_attempt(self, record: AttemptRecord) -> AttemptRecord:
        """Buffered. ``attempt_id`` is assigned by the inner store when the batch is flushed."""
        self._attempts.append(record)
        self.buffered_attempts += 1
        self._maybe_flush()
        return record

    def emit_event(self, event: EventRecord) -> None:
        """Buffered. ``event_id`` is assigned by the inner store when the batch is flushed."""
        self._events.append(event)
        self.buffered_events += 1
        self._maybe_flush()

    def upsert_reported_metric(self, row: Any) -> None:
        writer = getattr(self.inner, "upsert_reported_metric", None)
        if not callable(writer):
            raise StoreFeatureUnsupported("store does not support reported metrics")
        writer(row)

    def reported_metrics(
        self, *, run_id: str | None = None, pipeline_id: str | None = None
    ) -> list[Any]:
        reader = getattr(self.inner, "reported_metrics", None)
        return reader(run_id=run_id, pipeline_id=pipeline_id) if callable(reader) else []

    # -------------------------------------------------------------- runs (sync)
    def start_run(self, run: RunRecord) -> RunRecord:
        return self.inner.start_run(run)

    def heartbeat(self, run_id: str, ts: float | None = None) -> None:
        self.inner.heartbeat(run_id, ts)

    def finish_run(self, run_id: str, status: str, ended_at: float | None = None) -> None:
        self.flush()  # never end a run with facts still in the buffer
        self.inner.finish_run(run_id, status, ended_at)

    def get_run(self, run_id: str) -> RunRecord | None:
        self.flush()
        return self.inner.get_run(run_id)

    def latest_run_id(self) -> str | None:
        reader = getattr(self.inner, "latest_run_id", None)
        return reader() if callable(reader) else None

    # ----------------------------------------------------- pipelines (synchronous)
    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        self.flush()
        return self.inner.get_pipeline(pipeline_id)

    def upsert_pipeline(self, record: PipelineRecord) -> None:
        self.inner.upsert_pipeline(record)

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
        self.inner.finish_pipeline(
            pipeline_id,
            state,
            n_tasks_done=n_tasks_done,
            error=error,
            failed_task=failed_task,
            traceback=traceback,
        )

    def interrupt_stale(self, *, stale_after_s: float = 30.0, keep_run_id: str | None = None) -> int:
        self.flush()
        return self.inner.interrupt_stale(stale_after_s=stale_after_s, keep_run_id=keep_run_id)

    # ---------------------------------------------------- artifacts (synchronous)
    def put_artifact(self, artifact: Artifact) -> Artifact:
        return self.inner.put_artifact(artifact)

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        self.inner.mark_final(pipeline_id, seq)

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None:
        self.flush()
        return self.inner.get_artifact(pipeline_id, seq)

    def artifacts(self, pipeline_id: str) -> list[Artifact]:
        self.flush()
        return self.inner.artifacts(pipeline_id)

    # -------------------------------------------------------- tasks (synchronous)
    def record_task(self, record: TaskRecord) -> None:
        self.inner.record_task(record)

    def tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None, limit: int | None = None
    ) -> list[TaskRecord]:
        self.flush()
        return self.inner.tasks(pipeline_id, run_id=run_id, limit=limit)

    def attempts(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        limit: int | None = None,
    ) -> list[AttemptRecord]:
        self.flush()
        return self.inner.attempts(run_id=run_id, pipeline_id=pipeline_id, limit=limit)

    # ------------------------------------------------- handoffs (advanced, passthrough)
    # Both methods are forwarded *and* defined explicitly, because a forwarded call would skip the
    # flush: the commit must never land on top of buffered facts (a crash would then lose the attempt
    # trail of a handoff that is durably committed), and a read must see what is buffered. The
    # handed-off attempt itself travels through the commit — never through the buffered
    # ``record_attempt`` path. Whether the capability exists at all is decided by the store underneath;
    # ``supports_handoff`` unwraps this wrapper before probing.
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
        self.flush()
        inner = getattr(self.inner, "commit_handoff", None)
        if inner is None:
            raise ConfigError(
                f"{type(self.inner).__name__} cannot commit handoffs: it does not provide the optional "
                "commit_handoff capability (see store/base.py)"
            )
        return inner(record, task=task, attempt=attempt, payload=payload, cursor=cursor, final=final)

    def handoffs(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int | None = None
    ) -> list[HandoffRecord]:
        self.flush()
        inner = getattr(self.inner, "handoffs", None)
        if inner is None:
            raise ConfigError(
                f"{type(self.inner).__name__} cannot read handoffs: it does not provide the optional "
                "handoffs capability (see store/base.py)"
            )
        return inner(pipeline_id=pipeline_id, run_id=run_id, limit=limit)

    # ----------------------------------------------------------------- resources
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
        self.inner.upsert_resource(
            pool, resource_id, kind, spec, state, stats, published_by=published_by
        )

    def resources(self, pool: str | None = None) -> list[dict[str, Any]]:
        self.flush()
        return self.inner.resources(pool=pool)

    # ---------------------------------------------------------------- read views
    # The paged iterators are delegated explicitly, not through `__getattr__`: forwarding them to
    # the inner store would skip the flush, so an export could silently miss the buffered
    # attempts/events that the list APIs always flush first.
    def iter_pipelines(
        self, *, run_id: str | None = None, state: str | None = None
    ) -> Iterator[PipelineRecord]:
        self.flush()
        return _iter_pipelines(self.inner, run_id=run_id, state=state)

    def iter_tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None
    ) -> Iterator[TaskRecord]:
        self.flush()
        return _iter_tasks(self.inner, pipeline_id=pipeline_id, run_id=run_id)

    def iter_attempts(
        self, *, run_id: str | None = None, pipeline_id: str | None = None
    ) -> Iterator[AttemptRecord]:
        self.flush()
        return _iter_attempts(self.inner, run_id=run_id, pipeline_id=pipeline_id)

    def iter_events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None,
        kind: str | None = None
    ) -> Iterator[EventRecord]:
        self.flush()
        return _iter_events(self.inner, pipeline_id=pipeline_id, run_id=run_id, kind=kind)

    def iter_artifacts(self, *, pipeline_id: str) -> Iterator[Artifact]:
        self.flush()
        return _iter_artifacts(self.inner, pipeline_id=pipeline_id)

    def pipelines(
        self, *, run_id: str | None = None, state: str | None = None, limit: int | None = None
    ) -> list[PipelineRecord]:
        self.flush()
        return self.inner.pipelines(run_id=run_id, state=state, limit=limit)

    def events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None,
        kind: str | None = None, limit: int = 200
    ) -> list[EventRecord]:
        self.flush()
        # When kind is None, don't forward it to legacy inner stores that don't accept the kwarg.
        if kind is None:
            return self.inner.events(pipeline_id=pipeline_id, run_id=run_id, limit=limit)
        return self.inner.events(pipeline_id=pipeline_id, run_id=run_id, kind=kind, limit=limit)

    def count_events(
        self, *, kind: str | None = None, run_id: str | None = None,
        pipeline_id: str | None = None,
    ) -> int:
        self.flush()
        native = getattr(self.inner, "count_events", None)
        if callable(native):
            return int(native(kind=kind, run_id=run_id, pipeline_id=pipeline_id))
        # Fallback: use the compatibility-aware iterator helper so legacy inner stores
        # that don't accept `kind` don't crash.
        from .base import iter_events
        return sum(
            1 for _ in iter_events(self.inner, kind=kind, run_id=run_id, pipeline_id=pipeline_id)
        )

    def stats(self, run_id: str | None = None) -> dict[str, Any]:
        self.flush()
        return self.inner.stats(run_id)

    def errors(self, *, run_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        self.flush()
        return self.inner.errors(run_id=run_id, limit=limit)

    def export_rows(self, *, run_id: str | None = None) -> Iterator[dict[str, Any]]:
        self.flush()
        return self.inner.export_rows(run_id=run_id)

    def close(self) -> None:
        self.flush()
        self.inner.close()

    def reset_pipeline(self, record: PipelineRecord) -> None:
        self.flush()
        self.inner.reset_pipeline(record)

    def repair_visit_terminal(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.repair_visit_terminal(*args, **kwargs)

    def reset_visits(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.reset_visits(*args, **kwargs)

    def commit_entry(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.commit_entry(*args, **kwargs)

    def commit_visit_attempt(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.commit_visit_attempt(*args, **kwargs)

    def commit_visit_success(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.commit_visit_success(*args, **kwargs)

    def commit_control_transition(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.commit_control_transition(*args, **kwargs)

    def visit_state(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.visit_state(*args, **kwargs)

    def get_artifact_by_id(self, *args: Any, **kwargs: Any) -> Any:
        self.flush()
        return self.inner.get_artifact_by_id(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Anything not delegated explicitly (store-specific extras) passes through."""
        return getattr(self.inner, name)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<WriteBehindStore inner={self.inner!r} pending={self.pending}>"


class _RealClock:
    __slots__ = ()

    @staticmethod
    def now() -> float:
        return time.monotonic()


def wrap_write_behind(
    store: Store, *, batch_size: int = 128, flush_interval: float = 1.0, clock: Any = None
) -> WriteBehindStore:
    """Idempotent helper: wrapping a :class:`WriteBehindStore` again returns it unchanged."""
    if isinstance(store, WriteBehindStore):
        return store
    return WriteBehindStore(store, batch_size=batch_size, flush_interval=flush_interval, clock=clock)
