"""Store-layer interface and record definitions.

Conventions:
* **All store methods are synchronous** (no awaits inside). That is safe under a single event loop,
  and it keeps critical writes such as persisting a running record before an attempt starts from being interrupted by cancellation.
  Batched writes / write-behind is a later optimization and does not affect the interface.
* Facts live on three levels: ``pipelines`` (state) / ``tasks``+``attempts`` (history) / ``artifacts`` (state carriers).
* `journal` modes: ``full`` keeps the artifact payload (**the precondition for resume**);
  ``summary`` keeps only the summary and metadata, in which case intermediate artifacts cannot be reused and resume can only re-run whole pipelines.
* The list queries (``pipelines``/``tasks``/``attempts``/``events``/``artifacts``) are the required
  interface and may materialize their result. Whole-kind reads that must stay bounded in memory go
  through the ``iter_*`` helpers, which use the optional :class:`PagedStore` extension when the
  store provides it and otherwise fall back to the list API — see ``docs/reference.md``.
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
    "PagedStore",
    "PIPELINE_STATES",
    "TASK_STATES",
    "ITER_BATCH_SIZE",
    "iter_pipelines",
    "iter_tasks",
    "iter_attempts",
    "iter_events",
    "iter_artifacts",
    "open_store",
]

PIPELINE_STATES = ("pending", "running", "succeeded", "failed", "interrupted", "canceled")
TASK_STATES = ("pending", "running", "succeeded", "failed", "interrupted", "canceled")

# Rows a paged store may hold in Python at once. One batch is a fixed, small working set, which is
# what keeps an export's memory independent of the table's row count.
ITER_BATCH_SIZE = 1000

# The list API's ``events(limit=...)`` means "the most recent N" and defaults to 200, so a
# list-only store can only be asked for its whole log with the largest limit an int can express.
_LIST_LIMIT_ALL = 2**63 - 1


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
        resume_of: Reserved for run-level provenance; not currently set by ``Runner.run_async``
            (which always constructs a fresh ``RunRecord`` without it). The actual "resumed from"
            provenance tracked today lives per-pipeline, on ``PipelineRecord.resume_of``.
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
        pipeline_id / key: The pipeline's stable logical identity; ``PipelineTemplate.bind``
            always assigns the same value to both, so ``key == pipeline_id`` always holds. That
            value is content-addressed (task-chain fingerprint + seed + repeat index) by default,
            or caller-defined when an explicit ``key_of`` was supplied to ``template.map`` — in
            which case ``pipeline_id`` is no longer content-addressed either.
        run_id: The run currently "owning" this row — rebound on every resume to whichever run is
            touching it now, since stats/reports are scoped by run_id.
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
    """The required store interface: writes plus point/list queries.

    The list queries may materialize their result, which is fine for a report but not for an
    export of a large store. :class:`PagedStore` is the *optional* extension that adds bounded
    whole-kind iteration; the ``iter_*`` helpers below use it when available and fall back to the
    list API when it is not, so every store written against this protocol keeps working.
    """

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


@runtime_checkable
class PagedStore(Protocol):
    """Optional extension to :class:`Store`: batched, bounded-memory iteration over a whole kind.

    Nothing in the framework *requires* it. ``Store`` stays the only protocol ``open_store()``
    checks, so a third-party store written against the list API keeps working unchanged — the
    ``iter_*`` helpers below prefer a native paged method when the store has one and otherwise
    delegate to the list API. Implement the methods here when a whole-kind read must not
    materialize the table (``SqliteStore`` does):

    * yield in the documented order (see :func:`pyattacker.export.iter_rows`) and **end that order in
      a unique key**. The tables are keyed by ``task_run_id`` / ``artifact_id`` while the natural
      order is by ``(pipeline_id, seq)`` / ``seq``, and paging with a strict ``>`` cursor over a
      non-unique prefix skips every row that ties with the last row of a page;
    * read at most ``ITER_BATCH_SIZE`` rows per query;
    * keep the Python-side working set at one batch — for the *nested* per-pipeline data, one
      pipeline is the documented unit (see ``docs/reference.md`` § Stores);
    * where the order key is monotonic (``event_id`` / ``attempt_id``), capture its high-water mark
      before the first page and bound every page to it, so an export of a live store cannot chase
      rows appended after it started. Where it is not (``pipelines`` / ``tasks`` / ``artifacts``),
      the traversal is best-effort over the live table and must be documented as such.
    """

    def iter_pipelines(
        self, *, run_id: str | None = None, state: str | None = None
    ) -> Iterator[PipelineRecord]: ...

    def iter_tasks(
        self, pipeline_id: str | None = None, *, run_id: str | None = None
    ) -> Iterator[TaskRecord]: ...

    def iter_attempts(
        self, *, run_id: str | None = None, pipeline_id: str | None = None
    ) -> Iterator[AttemptRecord]: ...

    def iter_events(
        self, *, pipeline_id: str | None = None, run_id: str | None = None
    ) -> Iterator[EventRecord]: ...

    def iter_artifacts(self, *, pipeline_id: str) -> Iterator[Artifact]: ...


def iter_pipelines(
    store: Store, *, run_id: str | None = None, state: str | None = None
) -> Iterator[PipelineRecord]:
    """Stream pipelines oldest first: ``created_at``, ties broken by ``pipeline_id``.

    The tie-break is what makes a paged read safe: ``created_at`` alone is not unique (a fast
    scheduler writes many pipelines inside one clock tick), and paging on a non-unique key can
    skip or repeat a row. ``pipeline_id`` is the primary key and the upsert never rewrites
    ``created_at``, so this cursor is total and stable.

    Live-store semantics: best-effort traversal. A pipeline inserted ahead of the cursor while the
    iterator runs can appear; one inserted behind it cannot.
    """
    native = getattr(store, "iter_pipelines", None)
    if callable(native):
        yield from native(run_id=run_id, state=state)
    else:
        # Compatibility fallback: the list API. It materializes the kind — correctness first,
        # bounded memory only where the store implements the paged extension.
        yield from store.pipelines(run_id=run_id, state=state)


def iter_tasks(
    store: Store, pipeline_id: str | None = None, *, run_id: str | None = None
) -> Iterator[TaskRecord]:
    """Stream tasks ordered by ``pipeline_id``, ``seq``, then ``task_run_id``.

    ``(pipeline_id, seq)`` alone is not unique — the table is keyed by ``task_run_id`` — so the id
    is part of the cursor; without it a page boundary inside a tie drops the rest of the tie.
    Live-store semantics: best-effort traversal (see :func:`iter_pipelines`); the fallback for a
    store without the extension is the list API.
    """
    native = getattr(store, "iter_tasks", None)
    if callable(native):
        yield from native(pipeline_id=pipeline_id, run_id=run_id)
    else:
        yield from store.tasks(pipeline_id=pipeline_id, run_id=run_id)


def iter_attempts(
    store: Store, *, run_id: str | None = None, pipeline_id: str | None = None
) -> Iterator[AttemptRecord]:
    """Stream attempts in insertion order (``attempt_id``), oldest first.

    ``attempt_id`` is monotonic, so the iterator is bounded by the high-water mark taken when its
    first page is read: attempts recorded after that are not part of this traversal (per iterator,
    not permanent — a new iterator sees them).
    """
    native = getattr(store, "iter_attempts", None)
    if callable(native):
        yield from native(run_id=run_id, pipeline_id=pipeline_id)
    else:
        yield from store.attempts(run_id=run_id, pipeline_id=pipeline_id)


def iter_events(
    store: Store, *, pipeline_id: str | None = None, run_id: str | None = None
) -> Iterator[EventRecord]:
    """Stream events in insertion order (``event_id``), oldest first.

    ``event_id`` is the store's own monotonic counter, so this order is total and paging on it
    cannot drop or duplicate a row. The iterator is bound to the high-water mark taken when its
    first page is read: events emitted after that are not part of this traversal (per iterator, not
    permanent — a new iterator sees them).
    """
    native = getattr(store, "iter_events", None)
    if callable(native):
        yield from native(pipeline_id=pipeline_id, run_id=run_id)
    else:
        # The list API's own `limit` means "the most recent N" (default 200) and cannot express
        # "everything", so the fallback asks for the largest limit it can represent. Both built-in
        # stores return the selected events oldest first, which is the order the export documents.
        yield from store.events(pipeline_id=pipeline_id, run_id=run_id, limit=_LIST_LIMIT_ALL)


def iter_artifacts(store: Store, *, pipeline_id: str) -> Iterator[Artifact]:
    """Stream one pipeline's artifacts ordered by ``seq``, then ``artifact_id``.

    ``seq`` alone is not unique — the table is keyed by ``artifact_id`` — so the id is part of the
    cursor. Live-store semantics: best-effort traversal (see :func:`iter_pipelines`).
    """
    native = getattr(store, "iter_artifacts", None)
    if callable(native):
        yield from native(pipeline_id=pipeline_id)
    else:
        yield from store.artifacts(pipeline_id)


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
