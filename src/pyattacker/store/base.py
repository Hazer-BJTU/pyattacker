"""Store-layer interface and record definitions.

Conventions:
* **All store methods are synchronous** (no awaits inside). That is safe under a single event loop,
  and it keeps critical writes such as persisting a running record before an attempt starts from being interrupted by cancellation.
  Batched writes / write-behind is a later optimization and does not affect the interface.
* Facts live on three levels: ``pipelines`` (state) / ``tasks``+``attempts`` (history) / ``artifacts`` (state carriers).
* `journal` modes: ``full`` keeps the artifact payload (**the precondition for resume**);
  ``summary`` keeps only the summary and metadata, in which case intermediate artifacts cannot be reused and resume can only re-run whole pipelines.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..artifact import Artifact

__all__ = [
    "RunRecord",
    "PipelineRecord",
    "TaskRecord",
    "AttemptRecord",
    "EventRecord",
    "Store",
    "PIPELINE_STATES",
    "TASK_STATES",
    "open_store",
]

PIPELINE_STATES = ("pending", "running", "succeeded", "failed", "interrupted", "canceled")
TASK_STATES = ("pending", "running", "succeeded", "failed", "interrupted", "canceled")


def _now() -> float:
    return time.time()


@dataclass
class RunRecord:
    run_id: str
    label: str = ""
    status: str = "running"
    started_at: float = field(default_factory=_now)
    ended_at: float | None = None
    heartbeat_at: float | None = None
    spec_digest: str = ""
    code_version: str = ""
    python: str = ""
    host: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    resume_of: str | None = None


@dataclass
class PipelineRecord:
    pipeline_id: str
    run_id: str
    name: str
    key: str
    state: str = "pending"
    tags: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=_now)
    started_at: float | None = None
    finished_at: float | None = None
    n_tasks_total: int = 0
    n_tasks_done: int = 0
    error_type: str | None = None
    error_message: str | None = None
    traceback: str | None = None
    failed_task: str | None = None
    seed_digest: str = ""
    spec_digest: str = ""
    resume_of: str | None = None
    attempts_total: int = 0


@dataclass
class TaskRecord:
    task_run_id: str
    pipeline_id: str
    run_id: str
    name: str
    seq: int
    state: str = "pending"
    attempts_used: int = 0
    started_at: float | None = None
    ended_at: float | None = None
    duration_ms: float | None = None
    input_artifact_id: str | None = None
    output_artifact_id: str | None = None
    error_class: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    traceback: str | None = None
    leases: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class AttemptRecord:
    pipeline_id: str
    run_id: str
    task_run_id: str
    task_name: str
    seq: int
    attempt_no: int
    started_at: float
    ended_at: float | None = None
    duration_ms: float | None = None
    outcome: str = "running"  # succeeded | failed | timeout | cancelled
    error_class: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    traceback: str | None = None
    retry_delay_s: float | None = None
    decision: dict[str, Any] = field(default_factory=dict)
    leases: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    attempt_id: int | None = None


@dataclass
class EventRecord:
    ts: float
    kind: str
    scope: str = "pipeline"  # run | pipeline | task | pool | resource
    run_id: str | None = None
    pipeline_id: str | None = None
    task_run_id: str | None = None
    pool: str | None = None
    resource_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    event_id: int | None = None


@runtime_checkable
class Store(Protocol):
    journal: str

    def start_run(self, run: RunRecord) -> RunRecord: ...

    def heartbeat(self, run_id: str, ts: float | None = None) -> None: ...

    def finish_run(self, run_id: str, status: str, ended_at: float | None = None) -> None: ...

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None: ...

    def upsert_pipeline(self, record: PipelineRecord) -> None: ...

    def finish_pipeline(
        self,
        pipeline_id: str,
        state: str,
        *,
        n_tasks_done: int | None = None,
        error: BaseException | None = None,
        failed_task: str | None = None,
        traceback: str | None = None,
    ) -> None: ...

    def interrupt_stale(self, *, stale_after_s: float = 30.0, keep_run_id: str | None = None) -> int:
        """Mark running pipelines whose owning run is dead or whose heartbeat timed out as interrupted; returns the number of rows."""
        ...

    def put_artifact(self, artifact: Artifact) -> Artifact: ...

    def mark_final(self, pipeline_id: str, seq: int) -> None:
        """Mark the last artifact of a pipeline as the final output."""
        ...

    def get_artifact(self, pipeline_id: str, seq: int) -> Artifact | None: ...

    def artifacts(self, pipeline_id: str) -> list[Artifact]: ...

    def record_task(self, record: TaskRecord) -> None: ...

    def record_attempt(self, record: AttemptRecord) -> AttemptRecord: ...

    def attempts(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        limit: int | None = None,
    ) -> list[AttemptRecord]:
        """Attempt history, oldest first (the append-only half of the record)."""
        ...

    def emit_event(self, event: EventRecord) -> None: ...

    def upsert_resource(self, pool: str, resource_id: str, kind: str, spec: Mapping[str, Any],
                        state: str, stats: Mapping[str, Any], *, published_by: str = "") -> None: ...

    def resources(self, pool: str | None = None) -> list[dict[str, Any]]: ...

    def pipelines(
        self, *, run_id: str | None = None, state: str | None = None, limit: int | None = None
    ) -> list[PipelineRecord]: ...

    def events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None, limit: int = 200
    ) -> list[EventRecord]: ...

    def stats(self, run_id: str | None = None) -> dict[str, Any]: ...

    def errors(self, *, run_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]: ...

    def export_rows(self, *, run_id: str | None = None) -> Iterator[dict[str, Any]]: ...

    def close(self) -> None: ...


def open_store(
    spec: Any,
    *,
    journal: str = "full",
    write_behind: bool | None = None,
    batch_size: int = 128,
    flush_interval: float = 1.0,
    clock: Any = None,
    backend: Any = None,
) -> Store:
    """``open_store("runs.db")`` / ``open_store(":memory:")`` / ``open_store(store_instance)``.

    ``write_behind`` batches the append-only facts (attempts + events) while keeping state
    writes synchronous. ``None`` means "auto": on for file-backed stores, where the batch
    saves a commit per attempt, and off for in-memory stores, where it buys nothing.
    """
    if isinstance(spec, Store):
        return spec

    from ..plugins import PLUGINS
    from .writebehind import WriteBehindStore

    factory = PLUGINS.store_factory(str(spec))
    if factory is not None:
        # A store plugin owns its URI scheme (s3://, gs://, ...) and its own durability story;
        # the framework only asks it to satisfy the Store protocol.
        store: Store = factory(str(spec), journal=journal)
        file_backed = True
    elif spec is None or spec == ":memory:" or spec == "memory":
        from .memory import MemoryStore

        store = MemoryStore(journal=journal, backend=backend)
        file_backed = False
    else:
        from .sqlite import SqliteStore

        store = SqliteStore(str(spec), journal=journal, backend=backend)
        file_backed = True

    # An explicit True always wraps (predictable), while "auto" only wraps when batching can
    # actually buy something: a file store pays a commit per attempt, memory pays nothing.
    if write_behind or (write_behind is None and file_backed):
        store = WriteBehindStore(
            store, batch_size=batch_size, flush_interval=flush_interval, clock=clock
        )
    return store
