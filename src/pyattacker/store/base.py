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
    """One call to ``Runner.run``/``run_async`` —— the top level of the store's data model.

    Attributes:
        run_id: Unique id for this run; the scope every stats/report query filters by.
        status: ``"running"`` while in progress, then ``"completed"``/``"interrupted"``.
        heartbeat_at: Updated periodically while running; other runners sharing the same store use
            a stale heartbeat to detect an abandoned run (see ``interrupt_stale``).
        spec_digest: Digest of the run's ``meta`` config, for distinguishing otherwise-identical runs.
        code_version: The ``pyattacker`` package version that produced this run.
        resume_of: run_id this run resumed from, when this run was started with ``resume=True``
            and picked up an existing run's state; ``None`` for a fresh run.
    """

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
    """The durable state of one pipeline —— the row that makes resume possible.

    Invariants: ``n_tasks_done`` is the durable checkpoint cursor and must never be silently
    rewound; a pipeline resumed with ``state in ("failed", "interrupted")`` and ``n_tasks_done > 0``
    is expected to load artifact ``n_tasks_done - 1`` and continue from there (see
    ``Runner._open_pipeline``).

    Attributes:
        pipeline_id: Content-addressed id (task-chain fingerprint + seed + repeat index); stable
            across resumes/re-runs of the same logical pipeline.
        run_id: The run currently "owning" this row — rebound on every resume to whichever run is
            touching it now, since stats/reports are scoped by run_id.
        key: The pipeline's dedup key (usually equal to ``pipeline_id`` unless an explicit
            ``key_of`` was supplied to ``template.map``).
        n_tasks_total / n_tasks_done: Total tasks in the chain / tasks completed so far — the
            resume cursor described above.
        seed_digest: Digest of the encoded seed value, for detecting a changed seed.
        spec_digest: Digest of the task chain's fingerprint (see ``pipeline.compute_spec_digest``);
            differs from ``seed_digest`` in scope — this changes when the *code* changes, not the data.
        resume_of: The run_id this pipeline was last resumed from, when it was; ``None`` otherwise.
        attempts_total: Cumulative attempts across every task in this pipeline (not just the
            current one), persisted synchronously so a crash mid-backoff does not lose the count.
    """

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
    """The current state of one task-slot within a pipeline (seq N of the chain), across however many attempts it has taken.

    Collaborators: one :class:`TaskRecord` accumulates across all of a task's
    :class:`AttemptRecord` rows; it holds the *latest* attempt's outcome, while ``AttemptRecord``
    keeps the full append-only history.

    Attributes:
        task_run_id: ``f"{pipeline_id}:{seq}"`` — this task-slot's identity within its pipeline.
        seq: Position in the pipeline's task chain (0-indexed).
        attempts_used: How many attempts this task-slot has consumed so far.
        input_artifact_id / output_artifact_id: The artifact this attempt consumed / produced
            (``output_artifact_id`` is ``None`` until an attempt succeeds).
        leases: Snapshot of leases used by the most recent attempt (see ``TaskContext.lease_log``).
    """

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
    """One attempt of one task —— the append-only history that ``TaskRecord`` summarizes.

    Attributes:
        seq: The task-slot's position in the pipeline (same meaning as ``TaskRecord.seq``).
        attempt_no: 1-indexed attempt count for this task-slot.
        outcome: ``"succeeded" | "failed" | "timeout" | "cancelled"`` (``"running"`` only
            transiently, never persisted as a final row).
        retry_delay_s: The backoff chosen after this attempt, if it failed and another was scheduled.
        decision: The full retry-decision payload (``{retry, reason, delay_s, error_class,
            attempt, max_attempts, retry_after}``, see ``docs/design.md`` §4.4) — this is what
            makes "why did it retry N times" answerable straight from the store.
        leases: Every lease used during this attempt, including ones released mid-attempt (see
            ``TaskContext.lease_log``); a superset of what ``TaskRecord.leases`` keeps.
        attempt_id: Assigned by the store on insert; ``None`` until then.
    """

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
    """One structured log entry —— the shared shape of everything emitted via ``Runner._emit``/``Pool._emit``.

    Attributes:
        scope: What this event is about (``"run" | "pipeline" | "task" | "pool" | "resource"``);
            decides which of the id fields below are meaningful for this row.
        kind: Dotted event name (e.g. ``"pipeline.failed"``, ``"resource.degraded"``) — the
            primary thing to filter/group by.
        data: Free-form event-specific payload; shape depends on ``kind``, not otherwise validated.
        event_id: Assigned by the store on insert; ``None`` until then.
    """

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

    # ``resources()`` is deliberately *not* part of this protocol: ``Store`` is
    # ``@runtime_checkable``, and ``open_store()`` uses ``isinstance(spec, Store)`` to recognize an
    # already-open store passed in directly. Requiring a new method here would make any
    # pre-existing custom ``Store`` that hasn't added it stop satisfying the protocol, and
    # ``open_store`` would then mistreat the live object as an unresolved path/URI spec. All
    # built-in backends (``MemoryStore``, ``SqliteStore``, ``WriteBehindStore``) implement it;
    # callers that want it should check with ``getattr(store, "resources", None)``.

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
