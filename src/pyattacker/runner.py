"""Runner —— the scheduler for pipeline coroutines.

Key points:

* **One coroutine per pipeline**; pipelines are fully decoupled, so no global
  dependency graph is needed.
* **Task-level checkpoint**: the artifact of every successful task is persisted
  immediately and ``n_tasks_done`` is updated. Resume continues from "the first
  task that produced no artifact"; earlier requests are never re-sent.
* **Where the lease guarantee lands**: every attempt — whether it succeeds,
  fails, times out, or is cancelled — calls ``ctx.reclaim_now()`` synchronously
  in ``finally``; before the task record is written there is one more fallback check.
* **Retry backoff does not hold a worker**: a pipeline waiting out its backoff is parked in a delay
  queue, so ``concurrency`` counts attempts in flight rather than pipelines sitting idle.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import inspect
import json
import os
import random
import signal
import socket
import sys
import threading
import time
import traceback as tb_mod
import warnings
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .algorithm import AcquireAlgorithm
from .artifact import (
    DEFAULT_REGISTRY,
    SEED_SEQ,
    SEED_TASK,
    Artifact,
    CodecRegistry,
    digest_of,
)
from .errors import (
    ConfigError,
    CorruptCheckpoint,
    LeaseLeakError,
    PipelineIdentityConflict,
    PyAttackerError,
    StoreUnavailable,
    WorkerCrashed,
    error_class_of,
)
from .pipeline import PipelineSpec
from .resource import Bus, Pool, ResourceEvent
from .scheduler import DelayQueue
from .store import (
    AttemptRecord,
    EventRecord,
    PipelineRecord,
    RunRecord,
    Store,
    TaskRecord,
    open_store,
)
from .task import TaskContext, TaskSpec

__all__ = ["Runner", "RunConfig", "RunReport"]


def _clock_now() -> float:
    return time.monotonic()


class _RealClock:
    __slots__ = ()

    @staticmethod
    def now() -> float:
        return time.monotonic()

    @staticmethod
    async def sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclass
class RunConfig:
    """Everything that shapes one call to :meth:`Runner.run`/``run_async``.

    Invariants:
    * Not mutated by ``Runner``: overrides go through ``dataclasses.replace``, producing a new
      instance rather than changing the one passed in. That said, this is a plain mutable
      dataclass — nothing stops a caller from mutating a shared instance themselves, so treat it
      as owned by the ``Runner`` once passed in.

    Attributes:
        store: Where state lives — ``":memory:"``, a sqlite path, a store plugin URI, or an
            already-open :class:`~pyattacker.store.base.Store` instance.
        journal: ``"full"`` keeps artifact payloads (required for resume); ``"summary"`` keeps
            only metadata, so a resumed pipeline must re-run from the beginning.
        concurrency: Max attempts in flight at once — not max pipelines in flight, since a
            pipeline waiting out a retry backoff is parked and does not occupy a worker slot.
        run_id: Explicit run id; default is a timestamp+digest string.
        resume: When true, ``run_async`` calls ``interrupt_stale`` before scheduling, so pipelines
            abandoned by a dead run become resumable. It does *not* gate checkpoint restoration
            itself: ``_open_pipeline`` restores an existing ``failed``/``interrupted`` pipeline's
            checkpoint unconditionally, based on the stored record alone.
        retry_succeeded: When true, re-run pipelines already marked ``"succeeded"`` instead of
            skipping them (for re-evaluation passes over the same store).
        heartbeat_s: How often the run's heartbeat is written; drives ``stale_after_s`` staleness
            detection for other runners sharing the same store.
        grace_s: How long a graceful shutdown waits for in-flight workers before cancelling them.
        stale_after_s: A running pipeline whose owning run's heartbeat is older than this (and
            that run is not this one) is considered abandoned and gets marked interrupted.
        strict_leases: When true, a task ending while still holding a lease is a task failure
            (``LeaseLeakError``) instead of a force-reclaimed-and-recorded event.
        stop_after_failures / stop_after_s: Optional run-level budgets; once hit, the run stops
            admitting new pipelines and drains in-flight ones.
        handle_signals: Install SIGINT/SIGTERM handlers that call ``Runner.stop`` (main thread only).
        seed: Currently unused by the scheduler — the per-attempt RNG is derived purely from
            ``pipeline_id``/``seq``/``attempts_used`` (see ``Runner._execute_task``), not from
            this field. Reserved for a future run-level seed mix-in.
        write_behind: ``None`` (auto) batches append-only facts (attempts + events) for file-backed
            stores only; state writes (pipelines/tasks) are always synchronous. See
            ``store/writebehind.py`` for the failure model of a batched write.
        write_batch / flush_interval: Batching knobs when write-behind is active.
        artifact_backend: Where artifact payloads live — ``None``/``"inline"`` keeps them in the
            store; a file backend spills large ones to disk (see ``backends.py``).
        meta: Free-form metadata recorded on the run (digested into ``spec_digest``).
    """

    store: Any = ":memory:"
    journal: str = "full"  # full | summary
    concurrency: int = 16
    label: str = ""
    run_id: str | None = None
    resume: bool = False
    retry_succeeded: bool = False
    heartbeat_s: float = 5.0
    grace_s: float = 5.0
    stale_after_s: float = 30.0
    strict_leases: bool = False
    stop_after_failures: int | None = None
    stop_after_s: float | None = None
    handle_signals: bool = True
    seed: int = 0
    notes: str = ""
    # Append-only facts (attempts + events) are batched; checkpoints/artifacts stay synchronous.
    # None = auto (on for file-backed stores). See store/writebehind.py for the failure model.
    write_behind: bool | None = None
    write_batch: int = 128
    flush_interval: float = 1.0
    # Where artifact payloads live: None/"inline" keeps them in the store; "file:///data/blobs"
    # (or {"kind": "file", "root": ..., "min_bytes": ...}) spills large ones to disk.
    artifact_backend: Any = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunReport:
    """The result of one completed (or interrupted) run —— returned by :meth:`Runner.run`.

    Collaborators: ``store`` (kept so :meth:`summary`/:meth:`export_jsonl` can read back error
    detail and full pipeline rows after the run has already finished) and
    :meth:`~pyattacker.store.base.Store.stats`, whose return value populates ``stats`` verbatim.

    Attributes:
        run_id: The id this run was recorded under.
        status: ``"completed"`` or ``"interrupted"`` (a stop condition or a SIGINT/SIGTERM signal).
            An outer ``asyncio.CancelledError`` (the caller cancelling the ``run_async`` task
            itself) is re-raised instead — that path never returns a ``RunReport`` at all.
        duration_ms: Wall-clock duration of the run loop (not counting store teardown).
        stats: The store's aggregate view for this run — pipeline/task counts by state, latency
            percentiles, etc. (see ``Store.stats``); this is what :meth:`to_dict` flattens.
        skipped: Pipelines skipped because they were already ``"succeeded"`` (and
            ``retry_succeeded`` was false).
        leases_leaked: Leases force-reclaimed because a task ended while still holding them.
        stop_reason: Why the run stopped early (``"stop_after_failures"``, ``"signal"``, ...);
            ``None`` when every admitted pipeline simply ran to completion.
        store: The store this run used; kept for post-run introspection, not part of the report's
            own data (two reports can legitimately share one store, e.g. across a resume).
        repair_failures: Pipelines this run could not settle out of a torn terminal state, whose row is
            deliberately still owned by the earlier run that created it (see
            ``Runner._settle_terminal_cursor``). Such a row can never appear in ``stats``, which is scoped
            by ``run_id``, so this run-local counter is what keeps the failure visible to callers — and to
            the CLI's exit code. Appended after the pre-existing fields so positional construction by
            callers keeps its meaning.
    """

    run_id: str
    status: str
    started_at: float
    ended_at: float
    duration_ms: float
    stats: dict[str, Any]
    skipped: int = 0
    leases_leaked: int = 0
    stop_reason: str | None = None
    store: Any = None
    # Appended after every pre-existing public field on purpose: adding it in the middle would silently
    # reinterpret positional construction by callers (`skipped`, `leases_leaked`, then this).
    repair_failures: int = 0

    # ------------------------------------------------------------------ views
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "duration_ms": round(self.duration_ms, 3),
            "skipped": self.skipped,
            "leases_leaked": self.leases_leaked,
            "repair_failures": self.repair_failures,
            "stop_reason": self.stop_reason,
            **self.stats,
        }

    def summary(self) -> str:
        pipes = self.stats.get("pipelines", {})
        by_state = pipes.get("by_state", {})
        durations = pipes.get("duration_ms", {})
        tasks = self.stats.get("tasks", {})
        lines = [
            f"run {self.run_id}  status={self.status}  wall={self.duration_ms / 1000:.2f}s"
            + (f"  stop={self.stop_reason}" if self.stop_reason else ""),
            f"  pipelines: total={pipes.get('total', 0)} "
            + " ".join(f"{k}={v}" for k, v in sorted(by_state.items()))
            + (f" skipped={self.skipped}" if self.skipped else ""),
            f"  pipeline latency ms: p50={durations.get('p50')} p95={durations.get('p95')} max={durations.get('max')}",
            f"  attempts: total={self.stats.get('attempts_total', 0)}"
            + (f"  leases_leaked={self.leases_leaked}" if self.leases_leaked else ""),
        ]
        by_name = tasks.get("by_name", {})
        if by_name:
            lines.append("  tasks: " + " ".join(f"{k}={v}" for k, v in sorted(by_name.items())))
        if self.repair_failures:
            lines.append(
                f"  repair failures: {self.repair_failures} pipeline(s) could not be settled out of a "
                "torn terminal state; their rows keep the original failure and owning run"
            )
        failed = by_state.get("failed", 0)
        if failed and self.store is not None:
            lines.append(f"  errors ({failed}):")
            for item in self.store.errors(run_id=self.run_id, limit=5):
                lines.append(
                    f"    - {item.get('name')}/{item.get('failed_task')}: "
                    f"{item.get('error_type')}: {str(item.get('error_message'))[:120]}"
                )
        return "\n".join(lines)

    def export_jsonl(self, path: str, *, scope: str = "store", run_id: str | None = None) -> int:
        """Export full pipeline records (including artifact payloads) as JSONL; returns the line count.

        ``scope="store"`` (the default) exports the current state of **all**
        pipelines in the store —— on a resume run only the touched pipelines are
        attached to the new run_id, so the "full view" is the union that evaluation wants.
        ``scope="run"`` exports only those touched by this run.
        """
        if self.store is None:
            raise PyAttackerError("report has no store bound; cannot export")
        rid = self.run_id if scope == "run" else run_id
        count = 0
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            for row in self.store.export_rows(run_id=rid):
                handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                count += 1
        return count


@dataclass
class _Outcome:
    value: Any = None
    artifact: Artifact | None = None
    error: BaseException | None = None
    attempts: int = 0
    error_class: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class _TaskResult:
    """Result of a single attempt.

    ``retry_after`` is not ``None`` when the attempt failed but the policy wants another
    try: the caller then parks the pipeline instead of sleeping in the worker.
    """

    outcome: _Outcome | None = None
    retry_after: float | None = None
    attempts: int = 0

    @property
    def retrying(self) -> bool:
        return self.retry_after is not None


@dataclass
class _RunState:
    """Everything needed to continue one pipeline between attempts.

    A worker holds this while an attempt runs; while the pipeline backs off, the state
    lives in the :class:`DelayQueue` instead. That is what keeps ``concurrency`` honest:
    it counts attempts in flight, not pipelines sitting out a 30-second backoff.
    """

    spec: PipelineSpec
    record: PipelineRecord
    run_id: str
    seq: int
    value: Any
    artifact: Artifact | None
    task_spec: TaskSpec
    task_record: TaskRecord
    default_algorithm: AcquireAlgorithm | None = None
    attempts_used: int = 0
    task_started: float = 0.0
    start_index: int = 0
    retry_after: float | None = None

    @property
    def pipeline_id(self) -> str:
        return self.spec.pipeline_id


class Runner:
    """Scheduler. Usage::

        runner = Runner(store="runs.db", concurrency=16, pools=[pool])
        report = runner.run(pipeline("qa", fetch | ask | judge).map(dataset))
    """

    def __init__(
        self,
        *,
        store: Any = ":memory:",
        pools: Sequence[Pool] = (),
        concurrency: int = 16,
        clock: Any = None,
        bus: Bus | None = None,
        registry: CodecRegistry | None = None,
        config: RunConfig | None = None,
        **config_overrides: Any,
    ) -> None:
        if config is not None and config_overrides:
            config = dataclasses.replace(config, **config_overrides)
        elif config is None:
            config = RunConfig(store=store, concurrency=concurrency, **config_overrides)
        elif store != ":memory:":
            config = dataclasses.replace(config, store=store)
        self.config = config
        self.clock = clock or _RealClock()
        self.bus = bus or Bus(clock=self.clock)
        self.registry = registry or DEFAULT_REGISTRY
        # Codec plugins register lazily, on the first Runner: importing pyattacker must not go
        # looking at every installed distribution.
        from .plugins import PLUGINS

        PLUGINS.install_codecs(self.registry)
        self.pools: dict[str, Pool] = {}
        for pool in pools:
            self.add_pool(pool)
        self.store: Store = open_store(
            config.store,
            journal=config.journal,
            write_behind=config.write_behind,
            batch_size=config.write_batch,
            flush_interval=config.flush_interval,
            clock=self.clock,
            backend=config.artifact_backend,
        )
        self._stopping = threading.Event()
        self._stop_reason: str | None = None
        self._counters: Counter[str] = Counter()
        self._run_id: str | None = None
        self._last_traceback: str | None = None
        # Re-created per run: an asyncio primitive binds to the loop it first waits on, and a
        # Runner may legitimately be used across several loops (asyncio.run per call).
        self._delays: DelayQueue[_RunState] = DelayQueue(clock=self.clock, name="retry")
        self._all_done: asyncio.Event | None = None
        self._hard_stop = False  # set when the caller cancelled us: do not wait for in-flight work
        self._fatal_error: BaseException | None = None  # set when the store itself becomes untrustworthy
        self._worker_crash: WorkerCrashed | None = None  # set when a worker died outside its own handlers
        # Which item each worker is holding right now, so a worker that dies can be told which
        # pipeline it left behind. Rebuilt per run; entries are removed by the done-callback.
        self._inflight: dict[asyncio.Task[Any], Any] = {}
        # Workers whose death has already been handled: the done-callback and the worker's own
        # KeyboardInterrupt/SystemExit guard can both observe the same one.
        self._crashed_workers: set[asyncio.Task[Any]] = set()
        # Resolved when a worker dies: nothing may keep blocking on the work queue after that,
        # because the queue is only ever drained by workers. See _hand_over.
        self._abort: asyncio.Future[None] | None = None
        self._live: dict[str, Any] = {"running": 0, "started_at": None}

    # ------------------------------------------------------------- pool wiring
    def add_pool(self, pool: Pool) -> Pool:
        pool.clock = self.clock
        if pool.bus is None:
            pool.bus = self.bus
        previous = pool.on_event

        def _sink(event: ResourceEvent) -> None:
            if previous is not None:
                with contextlib.suppress(Exception):  # another sink's failure is not ours
                    previous(event)
            self._on_resource_event(event)

        pool.on_event = _sink
        self.pools[pool.name] = pool
        return pool

    def pool(self, name: str) -> Pool:
        try:
            return self.pools[name]
        except KeyError as exc:
            from .errors import PoolNotFound

            raise PoolNotFound(f"unknown resource pool: {name!r} (registered: {sorted(self.pools)})") from exc

    def _on_resource_event(self, event: ResourceEvent) -> None:
        self.store.emit_event(
            EventRecord(
                ts=time.time(),
                kind=event.kind,
                scope="resource",
                run_id=self._run_id,
                pool=event.pool,
                resource_id=event.resource_id,
                data=dict(event.data),
            )
        )

    # ------------------------------------------------------------------ control
    def stop(self, reason: str = "user") -> None:
        """Request a graceful shutdown: accept no new pipelines, and let in-flight pipelines finish."""
        if not self._stopping.is_set():
            self._stop_reason = reason
            self._stopping.set()
            if self._all_done is not None:
                self._all_done.set()  # wake a run that is waiting for its pipelines to drain

    @property
    def stopping(self) -> bool:
        return self._stopping.is_set()

    @property
    def run_id(self) -> str | None:
        return self._run_id

    # ------------------------------------------------------------------ entry point
    def run(
        self,
        pipelines: Iterable[PipelineSpec],
        *,
        resume: bool | None = None,
        run_id: str | None = None,
    ) -> RunReport:
        """Synchronous entry point. When already inside an event loop (e.g. Jupyter), it transparently switches to a background thread."""
        kwargs = {"resume": resume, "run_id": run_id}
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run_async(pipelines, **kwargs))
        box: dict[str, Any] = {}

        def _target() -> None:
            try:
                box["report"] = asyncio.run(self.run_async(pipelines, **kwargs))
            except BaseException as exc:  # pragma: no cover - propagate in-thread exceptions back out
                box["error"] = exc

        thread = threading.Thread(target=_target, name="pyattacker-runner")
        thread.start()
        thread.join()
        if "error" in box:
            raise box["error"]
        return box["report"]

    async def run_async(
        self,
        pipelines: Iterable[PipelineSpec],
        *,
        resume: bool | None = None,
        run_id: str | None = None,
    ) -> RunReport:
        cfg = self.config
        if resume is None:
            resume = cfg.resume
        self._stopping.clear()
        self._stop_reason = None
        self._counters.clear()
        started_wall = time.time()
        started = self.clock.now()
        self._delays = DelayQueue(clock=self.clock, name="retry")
        self._all_done = asyncio.Event()
        self._hard_stop = False
        self._fatal_error = None
        self._worker_crash = None
        self._inflight = {}
        self._crashed_workers = set()
        self._abort = asyncio.get_running_loop().create_future()
        self._identity_error: PipelineIdentityConflict | None = None
        for pool in self.pools.values():
            pool.reset_waiters()
        self._live["started_at"] = started
        rid = run_id or cfg.run_id or f"run-{time.strftime('%Y%m%d-%H%M%S')}-{digest_of(str(started_wall))[:6]}"
        self._run_id = rid
        write_batch = int(getattr(self.store, "batch_size", 0))
        run_config = {
            "concurrency": cfg.concurrency,
            "journal": cfg.journal,
            "write_behind": bool(write_batch),
            "artifact_backend": getattr(getattr(self.store, "backend", None), "name", None),
        }
        if write_batch:
            # Recorded only when batching is actually on: it is what makes a configured
            # write_batch/flush_interval verifiable from the run record itself, instead of only
            # from the store object the Runner owns.
            run_config["write_batch"] = write_batch
            run_config["flush_interval"] = getattr(self.store, "flush_interval", None)
        run_config.update(cfg.meta)
        self.store.start_run(
            RunRecord(
                run_id=rid,
                label=cfg.label,
                status="running",
                started_at=started_wall,
                spec_digest=digest_of(json.dumps(cfg.meta, sort_keys=True, default=str)),
                code_version=_code_version(),
                python=sys.version.split()[0],
                host=socket.gethostname(),
                config=run_config,
                notes=cfg.notes,
            )
        )
        existing = self.store.pipelines(limit=1)
        if existing and not existing[0].spec_digest.startswith("v2:"):
            message = (
                "store contains legacy task fingerprints; v2 default pipeline IDs will rerun work, "
                "and explicit keys with old fingerprints will conflict. Use a new store or retain "
                "the old package to finish the old run; see docs/reference.md#resume-identity."
            )
            warnings.warn(message, UserWarning, stacklevel=2)
            self._emit("run.legacy_identity", scope="run", data={"message": message})
        if resume:
            interrupted = self.store.interrupt_stale(
                stale_after_s=cfg.stale_after_s, keep_run_id=rid
            )
            if interrupted:
                self._emit("run.interrupted_pipelines", data={"count": interrupted})

        heartbeat = asyncio.create_task(self._heartbeat_loop(rid))
        restore_signals = self._install_signals() if cfg.handle_signals else None
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(2, cfg.concurrency * 2))
        workers = [
            asyncio.create_task(self._worker(queue, rid)) for _ in range(max(1, cfg.concurrency))
        ]
        # Worker *lifetime* is supervised, not just counted: a worker that dies outside its own
        # handlers can no longer advance the counters that completion is based on, so the run has
        # to observe the death instead of waiting for a condition that can never become true.
        for worker in workers:
            worker.add_done_callback(functools.partial(self._on_worker_done, run_id=rid))
        # One pump moves parked pipelines back into the work queue once their backoff is over.
        pump = asyncio.create_task(self._delays.pump(queue), name="pyattacker-timers")
        producer_error: BaseException | None = None
        try:
            for spec in pipelines:
                if self._stopping.is_set():
                    break
                if cfg.stop_after_s is not None and (self.clock.now() - started) > cfg.stop_after_s:
                    self.stop("stop_after_s")
                    break
                if (
                    cfg.stop_after_failures is not None
                    and self._counters["pipelines_failed"] >= cfg.stop_after_failures
                ):
                    self.stop("stop_after_failures")
                    break
                self._counters["pipelines_admitted"] += 1
                if not await self._hand_over(queue, spec):
                    # A worker died before this pipeline could be handed over. It was never queued,
                    # so it is un-admitted again rather than counted as work the run abandoned --
                    # and admitting must stop here, because the crash handler owns the wind-down.
                    self._counters["pipelines_admitted"] -= 1
                    break
                # Yield after every admission: without it a fast producer can fill the queue
                # without ever letting a worker run, which delays every stop condition
                # (failures, wall-clock budget) and makes Ctrl-C feel unresponsive.
                await asyncio.sleep(0)
        except asyncio.CancelledError as exc:  # outer cancellation: wind down, then re-raise
            self._hard_stop = True
            self.stop("cancelled")
            producer_error = exc
        except BaseException as exc:  # the generator itself raised
            self.stop("producer_error")
            producer_error = exc
        finally:
            try:
                if not self._stopping.is_set():
                    try:
                        await self._wait_for_completion()
                    except asyncio.CancelledError as exc:
                        # Swallow it here so the wind-down still runs; it is re-raised at the end.
                        self._hard_stop = True
                        self.stop("cancelled")
                        producer_error = producer_error or exc
                await self._shutdown(queue, workers, pump)
            finally:
                heartbeat.cancel()
                if restore_signals is not None:
                    restore_signals()
                self._finalize_pools()

        if self._fatal_error is not None:
            # The store itself failed while recovering from an earlier framework surprise -- its
            # durability guarantees are no longer trustworthy, so skip the normal finalization
            # (finish_run/emit/stats all talk to that same store) and fail loudly and immediately
            # instead of returning a report that might be built on an inconsistent read.
            raise self._fatal_error
        ended = self.clock.now()
        status = "interrupted" if self._stopping.is_set() else "completed"
        self.store.finish_run(rid, status, ended_at=time.time())  # flushes first (write-behind)
        self._emit(
            "run.finished",
            scope="run",
            data={"status": status, "duration_ms": (ended - started) * 1000.0},
        )
        stats = self.store.stats(rid)  # read APIs flush, so the event above is durable too
        report = RunReport(
            run_id=rid,
            status=status,
            started_at=started_wall,
            ended_at=time.time(),
            duration_ms=(ended - started) * 1000.0,
            stats=stats,
            skipped=self._counters["skipped"],
            leases_leaked=self._counters["leases_leaked"],
            repair_failures=self._counters["repair_failures"],
            stop_reason=self._stop_reason,
            store=self.store,
        )
        self._flush_store()
        if self._identity_error is not None:
            raise self._identity_error
        if self._worker_crash is not None:
            # A worker died outside its own handlers: the run-level fault wins over the ordinary
            # return value. Everything the caller needs to investigate is already durable -- the
            # pipeline has a terminal row, ``runner.worker_crashed`` carries the traceback, and the
            # run record above is finished as ``interrupted`` -- so the record survives the raise,
            # while a returned report would describe a run that lost a worker as merely stopped.
            raise self._worker_crash
        if isinstance(producer_error, asyncio.CancelledError):
            raise producer_error
        if producer_error is not None:
            report.stats["producer_error"] = f"{type(producer_error).__name__}: {producer_error}"
        return report

    def _check_all_done(self) -> None:
        """Re-check stop conditions and signal completion once every admitted pipeline is terminal.

        Stop conditions must be evaluated here and not only in the producer loop: with a fast
        producer and slow pipelines, every spec can be admitted before the failure budget is
        spent, and the run would never stop.
        """
        cfg = self.config
        if (
            cfg.stop_after_failures is not None
            and self._counters["pipelines_failed"] >= cfg.stop_after_failures
        ):
            self.stop("stop_after_failures")
        if self._all_done is None:
            return
        if self._counters["pipelines_done"] >= self._counters["pipelines_admitted"]:
            self._all_done.set()

    async def _hand_over(self, queue: "asyncio.Queue[Any]", item: Any) -> bool:
        """Put an item on the work queue, but stop waiting for room once a worker has died.

        ``queue.put`` blocks while the queue is full, and a full queue is only ever drained by
        workers — so waiting on it can outlive the worker that made waiting necessary. That is the
        same liveness hole as the dead worker itself, one step removed: with every worker gone and
        a full queue, the producer would park in ``put`` forever and the run would never reach the
        wind-down that raises :class:`~pyattacker.errors.WorkerCrashed`.

        Returns ``False`` when the abort won the race; the item was not queued then. The caller
        decides what that means (stop admitting; fall back to a hard shutdown when delivering
        shutdown sentinels).
        """
        assert self._abort is not None, "_hand_over is only valid while a run is in flight"
        if not queue.full():
            queue.put_nowait(item)  # fast path: room already available, no task and no second await
            return True
        putter = asyncio.create_task(queue.put(item))
        try:
            await asyncio.wait({putter, self._abort}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # A pending ``put`` is cancelled *before* it can enqueue: asyncio.Queue.put() removes its
            # putter future and re-raises without calling put_nowait(), so the item is not delivered
            # twice or half-delivered.
            if not putter.done():
                putter.cancel()
            await asyncio.gather(putter, return_exceptions=True)
        return not putter.cancelled()

    async def _wait_for_completion(self) -> None:
        """Wait for pipelines that are parked in the delay queue (retry backoffs).

        Without this, a run would "finish" the moment its workers went idle, and every
        pipeline sitting out a backoff would be recorded as interrupted. When a stop was
        requested we deliberately skip the wait: the stop is the whole point.

        That early return is also what releases a run whose worker died: completion is otherwise a
        counter condition that a dead worker can no longer satisfy, so the crash handler stops the
        run instead of trying to make ``pipelines_done`` reach ``pipelines_admitted``.
        """
        cfg = self.config
        while True:
            if self._stopping.is_set():
                return
            if self._counters["pipelines_done"] >= self._counters["pipelines_admitted"]:
                return
            if cfg.stop_after_s is not None and (self.clock.now() - (self._live["started_at"] or 0)) > cfg.stop_after_s:
                self.stop("stop_after_s")
                return
            assert self._all_done is not None
            self._all_done.clear()
            try:
                await asyncio.wait_for(self._all_done.wait(), timeout=0.5)
            except TimeoutError:
                continue

    async def _shutdown(
        self,
        queue: "asyncio.Queue[Any]",
        workers: Sequence[asyncio.Task[Any]],
        pump: asyncio.Task[Any],
    ) -> None:
        """Stop the timer pump, drain the workers, then account for anything still deferred.

        Two flavours: a *graceful* stop (stop condition or a finished producer) lets in-flight
        pipelines finish and cancels stragglers only after ``grace_s``; a *hard* stop (the
        caller cancelled us) cancels the workers straight away.

        Order matters: the pump stops *before* the sentinels are queued, otherwise it could
        push a state in behind them and that state would never be picked up again.
        """
        try:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)
            hard = self._hard_stop
            if not hard:
                # Delivering the sentinels is itself a blocking hand-over, so it uses the same
                # abort-aware path as admission: a worker that dies while it is being drained would
                # otherwise leave the run parked on a full queue that nothing can consume anymore.
                for _ in workers:
                    if not await self._hand_over(queue, None):
                        hard = True
                        break
            try:
                if hard:
                    # The caller cancelled us, or a worker died while draining: a worker stuck in a
                    # 30-minute task must not keep the process alive, so cancel them rather than
                    # waiting for a drain that may no longer be able to complete.
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                else:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*workers, return_exceptions=True),
                            timeout=max(0.0, self.config.grace_s),
                        )
                    except TimeoutError:  # grace expired: stop waiting for stragglers
                        for worker in workers:
                            worker.cancel()
                        await asyncio.gather(*workers, return_exceptions=True)
            except asyncio.CancelledError:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                raise
        finally:
            self._mark_deferred_interrupted()
            self._flush_store()

    def _mark_deferred_interrupted(self) -> int:
        """Mark pipelines still parked in the delay queue when the run stopped.

        Their checkpoints are already durable, so a later ``resume=True`` continues exactly
        where they stopped. Nothing is dropped silently: each one is recorded and counted.
        """
        leftover = self._delays.drain()
        for state in leftover:
            # attempts_total is already durable: _execute_task persists state.record at the
            # start of every attempt, before a pipeline is ever parked for a retry backoff.
            self.store.finish_pipeline(
                state.pipeline_id, "interrupted", n_tasks_done=state.record.n_tasks_done
            )
            self._emit(
                "pipeline.deferred_interrupted",
                pipeline_id=state.pipeline_id,
                data={
                    "task": state.task_spec.name,
                    "seq": state.seq,
                    "attempts": state.attempts_used,
                },
            )
        if leftover:
            self._counters["pipelines_deferred"] += len(leftover)
            self._counters["pipelines_done"] += len(leftover)
            self._check_all_done()
        return len(leftover)

    async def _worker(self, queue: "asyncio.Queue[Any]", run_id: str) -> None:
        worker = asyncio.current_task()
        try:
            await self._worker_loop(queue, run_id, worker)
        except (KeyboardInterrupt, SystemExit) as exc:
            # asyncio deliberately re-raises these two out of the task, which stops the loop before
            # any done-callback can run -- so the primary observation point never sees them. Handling
            # them here is what still gets a terminal row and an event written; the exception is then
            # re-raised unchanged, because "KeyboardInterrupt stops the process" is not ours to
            # change. Every other BaseException is left to the done-callback, which sees it too;
            # _crash_worker is idempotent per worker, so the two observation points cannot double up.
            item = self._inflight.pop(worker, None) if worker is not None else None
            self._crash_worker(exc, item, run_id=run_id, worker=worker)
            raise

    async def _worker_loop(
        self, queue: "asyncio.Queue[Any]", run_id: str, worker: "asyncio.Task[Any] | None"
    ) -> None:
        while True:
            item = await queue.get()
            if worker is not None:
                # Published before anything below can raise. This is what lets supervision name the
                # pipeline a dead worker left behind, and it is deliberately not cleared on the way
                # out: the done-callback that reads it runs only after this coroutine is gone.
                self._inflight[worker] = item
            state = None
            try:
                if item is None:
                    return
                if self._identity_error is not None:
                    continue  # stop queued work before opening or overwriting any other records
                self._live["running"] += 1
                try:
                    # The queue carries two shapes: fresh PipelineSpec objects from the producer,
                    # and _RunState objects coming back from the delay pump after a backoff.
                    state = item if isinstance(item, _RunState) else self._open_pipeline(item, run_id)
                    if state is not None:
                        await self._drive(state)
                finally:
                    self._live["running"] -= 1
            except asyncio.CancelledError:
                if state is not None:
                    try:
                        self.store.finish_pipeline(
                            state.pipeline_id, "interrupted", n_tasks_done=state.record.n_tasks_done
                        )
                    except Exception as exc:
                        self._fatal_error = StoreUnavailable(
                            f"could not persist cancellation for pipeline {state.pipeline_id!r}: {exc}"
                        )
                        self._hard_stop = True
                        self.stop("store_unavailable")
                        raise
                    self._counters["pipelines_done"] += 1
                    self._check_all_done()
                raise
            except PipelineIdentityConflict as exc:
                # Do not send conflicts through internal-error recovery: that would overwrite
                # the historical row and its checkpoint with a failure from the new definition.
                self._identity_error = exc
                self._hard_stop = True
                self.stop("identity_conflict")
                self._counters["pipelines_done"] += 1
                self._emit(
                    "pipeline.identity_conflict", pipeline_id=item.pipeline_id, data={"error": str(exc)}
                )
            except Exception as exc:  # framework-level surprise: record it, don't take down the run
                self._counters["pipelines_failed"] += 1
                self._counters["pipelines_done"] += 1
                tb = tb_mod.format_exc()
                try:
                    pipeline_id = self._finish_pipeline_after_internal_error(item, state, exc, run_id, tb)
                except Exception as recovery_exc:
                    # The recovery path itself talks to the store (get_pipeline/upsert_pipeline/
                    # finish_pipeline); if *that* fails too, the run's durability guarantees can
                    # no longer be trusted. The original bug this whole path exists to prevent
                    # was "an exception raised while handling an exception kills this worker
                    # silently, and asyncio.gather(..., return_exceptions=True) at shutdown never
                    # tells anyone" -- swallowing recovery_exc here would just reintroduce that
                    # one level deeper. So this is deliberately fatal and explicit: stop the run
                    # hard (do not wait for other in-flight work) and let run_async re-raise a
                    # clear StoreUnavailable instead of hanging or silently losing this worker.
                    self._fatal_error = StoreUnavailable(
                        f"could not persist internal-error recovery for a pipeline: "
                        f"{type(recovery_exc).__name__}: {recovery_exc}"
                    )
                    self._hard_stop = True
                    self.stop("store_unavailable")
                    self._check_all_done()
                    raise
                self._check_all_done()
                self._emit(
                    "runner.internal_error",
                    pipeline_id=pipeline_id,
                    data={"error": f"{type(exc).__name__}: {exc}", "traceback": tb},
                )
            finally:
                queue.task_done()

    # -------------------------------------------------------- worker supervision
    def _on_worker_done(self, worker: asyncio.Task[Any], *, run_id: str) -> None:
        """Observe worker lifetime: a worker that died outside its own handlers is a run-level fault.

        Completion is counted (``pipelines_done`` against ``pipelines_admitted``), which assumes
        every admitted pipeline reaches one of the worker loop's own terminal paths. A
        ``BaseException`` that is not ``CancelledError`` reaches none of them: the task ends, the
        counter can never advance, and ``_wait_for_completion`` would keep waiting for a condition
        that has become impossible — a hang with no error, no exit code and no record. This callback
        is the missing observation point, and because it is attached to the task rather than woven
        into the handler chain it also sees a death *after* that chain (queue or worker housekeeping)
        and one from a coroutine that never got that far.

        It must never raise: a done-callback runs in the event loop's callback context, where an
        exception is only logged — and the run would hang anyway.
        """
        item = self._inflight.pop(worker, None)
        if worker.cancelled():
            return  # a cancelled worker already recorded its own pipeline inside the worker loop
        exc = worker.exception()
        if exc is None:
            return  # an ordinary end: the sentinel path, nothing to supervise
        try:
            self._crash_worker(exc, item, run_id=run_id, worker=worker)
        except BaseException:  # pragma: no cover - defence in depth: a callback must not raise
            # _crash_worker stops the run before it touches the store, so anything reaching here went
            # wrong before that point. Release the run anyway: a callback that raises is a run that
            # can hang waiting on a counter nobody advances, and asyncio logs what happened.
            self._hard_stop = True
            self.stop("worker_crashed")

    def _crash_worker(
        self,
        exc: BaseException,
        item: Any,
        *,
        run_id: str,
        worker: "asyncio.Task[Any] | None" = None,
    ) -> None:
        """Stop the run for a worker that died, and leave a record of why.

        Order matters: stopping comes first (the run must stop even if recording fails), and the
        in-flight pipeline is terminalized before the event, so an event that says "this pipeline
        crashed" is never published ahead of the row it describes.
        """
        if worker is not None:
            if worker in self._crashed_workers:
                return  # already handled: the two observation points can both see the same death
            self._crashed_workers.add(worker)
        crashing_pipeline = self._stopping.is_set()  # a crash while already winding down
        tb = "".join(tb_mod.format_exception(exc))
        pipeline_id = getattr(item, "pipeline_id", None)
        crash = WorkerCrashed(
            (
                f"worker for pipeline {pipeline_id!r} died with {type(exc).__name__}: {exc}"
                if pipeline_id is not None
                else f"a worker died with {type(exc).__name__}: {exc}"
            ),
            pipeline_id=pipeline_id,
        )
        crash.__cause__ = exc
        # The same shape as the other fatal paths: stop admitting (the abort also releases anything
        # parked on the full work queue), do not wait for in-flight work, and deliberately do *not*
        # manufacture ``pipelines_done`` to satisfy the counter invariant -- the run is no longer
        # completing normally, and ``stop`` is what releases the waiter.
        self._hard_stop = True
        if self._abort is not None and not self._abort.done():
            self._abort.set_result(None)
        self.stop("worker_crashed")
        # Recorded before the store is touched again, so nothing that happens below can stop the run
        # from raising: the exception is what tells the caller a worker died. First crash wins -- it
        # is the one that stopped the run, and later ones are usually its consequences (each still
        # gets its own terminal row and event below).
        self._worker_crash = self._worker_crash or crash
        if item is not None:
            try:
                pipeline_id = self._terminalize_crashed_pipeline(
                    item, crash, tb, run_id=run_id, interrupted=crashing_pipeline
                )
            except BaseException as store_exc:
                # Recording the crash needs the store -- and *this* store is the prime suspect, so a
                # second failure here is expected rather than exotic. If it happens, the run's
                # durability guarantees are gone: take the existing fatal path (raise
                # StoreUnavailable and skip the normal finalization) instead of retrying a broken
                # store, and keep the crash as the cause so the fault that started this is not hidden
                # behind the store's own error.
                fatal = StoreUnavailable(
                    f"a worker died with {type(exc).__name__} and the crash could not be persisted: "
                    f"{type(store_exc).__name__}: {store_exc}"
                )
                fatal.__cause__ = exc
                self._fatal_error = fatal
        # The exception raised by run_async carries the type, the message and the traceback, so a
        # store that cannot take this one event does not make the crash silent -- which is also why
        # a BaseException escaping here is swallowed: this is already the last line of defence.
        with contextlib.suppress(BaseException):
            self._emit(
                "runner.worker_crashed",
                pipeline_id=pipeline_id,
                data={
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": tb,
                    "during_shutdown": crashing_pipeline,
                },
            )
        # Pushed out immediately rather than at the next flush point: KeyboardInterrupt/SystemExit
        # are about to tear the loop down, and anything still batched would go with it unless the
        # embedding happens to give the run one more teardown pass. ``_flush_store`` swallows only
        # ``Exception``, so it is suppressed again here: a done-callback that raises is a
        # done-callback that can leave the run waiting on a counter nobody will advance.
        with contextlib.suppress(BaseException):
            self._flush_store()

    def _terminalize_crashed_pipeline(
        self, item: Any, error: WorkerCrashed, tb: str, *, run_id: str, interrupted: bool
    ) -> str | None:
        """Give the pipeline a dead worker was holding a terminal row.

        Nothing else will ever move it out of ``running``: the worker that owned it is gone, and
        every reader (the report, the CLI exit code, a later ``resume``) reads the persisted row.
        A pipeline that was *already* terminal when the worker died -- the death happened in queue
        housekeeping, after the pipeline had finished -- keeps its own state, because history is
        not rewritten to blame a pipeline that actually succeeded.
        """
        state = item if isinstance(item, _RunState) else None
        pipeline_id = getattr(item, "pipeline_id", None)
        if pipeline_id is None:
            return None  # nothing identifiable: the worker died between items
        # The *persisted* row is authoritative, never the in-memory state. ``finish_pipeline`` is not
        # required to write back into the caller's ``PipelineRecord`` (SQLite updates the row only),
        # and the item can be an ``_RunState`` that came back through the delay queue while a terminal
        # write it already made is durable -- the crash lands after that write, for example in the
        # success event or in queue housekeeping. Trusting ``state.record`` would then rewrite a
        # ``succeeded`` row (or an ordinary failure with its own provenance) as this crash.
        record = self.store.get_pipeline(pipeline_id)
        if record is not None and record.state in ("succeeded", "failed", "interrupted"):
            return pipeline_id
        if record is None:
            # No durable row: use what the worker still holds, or create one from the spec (mirroring
            # _finish_pipeline_after_internal_error). A crash before the row was ever written must not
            # make the pipeline vanish from the report.
            record = state.record if state is not None else None
            if record is None:
                spec = item if isinstance(item, PipelineSpec) else None
                if spec is None:  # pragma: no cover - defensive: nothing to create a row from
                    return pipeline_id
                record = PipelineRecord(
                    pipeline_id=pipeline_id,
                    run_id=run_id,
                    name=spec.name,
                    key=spec.key,
                    tags=dict(spec.template.tags),
                    n_tasks_total=spec.n_tasks,
                    seed_digest=spec.seed_digest,
                    spec_digest=spec.spec_digest,
                    state="running",
                    started_at=time.time(),
                )
            self.store.upsert_pipeline(record)
        # A death during an ordinary stop is recorded as interrupted, matching the run it belongs
        # to: the pipeline did not finish, and the run was already winding down without it.
        # Otherwise the worker's death is a failure of that pipeline, exactly like the
        # internal-error path.
        self.store.finish_pipeline(
            pipeline_id,
            "interrupted" if interrupted else "failed",
            n_tasks_done=record.n_tasks_done,
            error=error,
            failed_task=state.task_spec.name if state is not None else None,
            traceback=tb,
        )
        return pipeline_id

    def _finish_pipeline_after_internal_error(
        self, item: Any, state: "_RunState | None", exc: Exception, run_id: str, tb: str
    ) -> str | None:
        """Give a framework-level surprise the same terminal representation a task failure gets.

        The worker's counters (``pipelines_failed``/``pipelines_done``) already treat this
        pipeline as terminally failed; the persisted row must agree, or the final report (and
        the CLI's exit code, which reads the persisted failed-count) can disagree with the
        scheduler and silently call the run a success. Two shapes reach here:
        ``state`` already exists (``_drive`` raised mid-flight — its row is durable, just still
        ``"running"``), or it does not (``_open_pipeline`` itself raised, possibly before ever
        calling ``upsert_pipeline``) — in which case a row is created first so the pipeline does
        not simply vanish from the report.

        Unlike ``_terminalize_crashed_pipeline``, this path deliberately rewrites the row *without*
        checking whether it is already terminal: the run continues and returns a report afterwards,
        so this row is the only trace the caller has that the run hit a framework-level fault. A
        supervisor-observed death is different — the run stops and raises, so the exception carries
        the fault and an already-earned terminal row must not be overwritten.
        """
        if state is not None:
            self.store.finish_pipeline(
                state.pipeline_id,
                "failed",
                n_tasks_done=state.record.n_tasks_done,
                error=exc,
                failed_task=state.task_spec.name,
                traceback=tb,
            )
            return state.pipeline_id
        if not isinstance(item, PipelineSpec):
            return None  # nothing identifiable to attach the failure to
        # `state is None` only means _open_pipeline() never *returned* a _RunState -- it may
        # already have restored an existing checkpoint (record.n_tasks_done) before raising,
        # possibly before it ever reassigns record.run_id to the *current* run. Preserve the
        # checkpoint either way (never rewind n_tasks_done back to 0 for an existing row), but
        # always rebind run_id to this run: the final report is scoped by run_id (self.store
        # .stats(run_id)), so a failure left attached to a stale run_id would silently vanish
        # from the report of the run that actually encountered it.
        record = self.store.get_pipeline(item.pipeline_id)
        if record is None:
            record = PipelineRecord(
                pipeline_id=item.pipeline_id,
                run_id=run_id,
                name=item.name,
                key=item.key,
                tags=dict(item.template.tags),
                n_tasks_total=item.n_tasks,
                seed_digest=item.seed_digest,
                spec_digest=item.spec_digest,
                state="running",
                started_at=time.time(),
            )
        else:
            record.run_id = run_id
        self.store.upsert_pipeline(record)
        self.store.finish_pipeline(item.pipeline_id, "failed", error=exc, traceback=tb)
        return item.pipeline_id

    # ------------------------------------------------------- pipeline execution
    def _open_pipeline(self, spec: PipelineSpec, run_id: str) -> _RunState | None:
        """Resolve the checkpoint, register the pipeline row, and return its opening state.

        Returns ``None`` when there is nothing to run: the pipeline already succeeded, a configuration
        problem was recorded as a pipeline failure, or a torn finalization was settled by
        :meth:`_settle_terminal_cursor`.
        """
        cfg = self.config
        record = self.store.get_pipeline(spec.pipeline_id)
        if record is not None:
            changed = []
            if record.spec_digest != spec.spec_digest:
                changed.append("task definition (spec_digest)")
            if record.seed_digest != spec.seed_digest:
                changed.append("input (seed_digest)")
            if changed:
                raise PipelineIdentityConflict(
                    f"pipeline key {spec.key!r} conflicts with stored {' and '.join(changed)}; "
                    "the existing result/checkpoint is unchanged. Use a new key or store for "
                    "changed work; --retry-succeeded does not override identity conflicts."
                )
        if record is not None and record.state == "succeeded" and not cfg.retry_succeeded:
            self._counters["skipped"] += 1
            self._counters["pipelines_done"] += 1
            self._check_all_done()
            self._emit("pipeline.skipped", pipeline_id=spec.pipeline_id, data={"reason": "succeeded"})
            return None

        # A cursor at (or past) the end of the chain must never reach the resume rule below, which
        # would index spec.tasks[cursor] and raise IndexError -- forever, on every later run. Handle
        # the two shapes explicitly before the ordinary checkpoint branch.
        if (
            record is not None
            and record.state in ("failed", "interrupted")
            and record.n_tasks_done >= spec.n_tasks
            and self._settle_terminal_cursor(spec, record, run_id)
        ):
            return None
            # (_settle_terminal_cursor returns False only when the terminal artifact was unusable, in
            # which case it rewound the row and the ordinary restart-from-zero rule below applies.)

        start_index = 0
        value: Any = spec.seed
        artifact: Artifact | None = None
        if record is not None and record.state in ("failed", "interrupted") and record.n_tasks_done > 0:
            candidate = self.store.get_artifact(spec.pipeline_id, record.n_tasks_done - 1)
            if candidate is not None and candidate.available:
                start_index = record.n_tasks_done
                artifact = candidate
                try:
                    value = self.registry.load(candidate.encoded())
                except Exception as exc:
                    self._emit(
                        "pipeline.checkpoint_unusable",
                        pipeline_id=spec.pipeline_id,
                        data={"error": str(exc)},
                    )
                    start_index, artifact, value = 0, None, spec.seed
            else:
                self._emit(
                    "pipeline.checkpoint_missing",
                    pipeline_id=spec.pipeline_id,
                    data={"reason": "journal did not persist the artifact payload; rerunning the whole pipeline"},
                )

        resumed_from = record.run_id if record is not None and start_index > 0 else None
        record = record or PipelineRecord(
            pipeline_id=spec.pipeline_id,
            run_id=run_id,
            name=spec.name,
            key=spec.key,
            tags=dict(spec.template.tags),
            n_tasks_total=spec.n_tasks,
            seed_digest=spec.seed_digest,
            spec_digest=spec.spec_digest,
        )
        record.run_id = run_id
        record.state = "running"
        record.started_at = record.started_at or time.time()
        record.finished_at = None
        record.n_tasks_total = spec.n_tasks
        record.n_tasks_done = start_index
        record.resume_of = resumed_from
        record.error_type = record.error_message = record.traceback = record.failed_task = None
        self.store.upsert_pipeline(record)
        problem = self._pool_problem(spec)
        if problem is not None:
            self._counters["pipelines_failed"] += 1
            self._counters["pipelines_done"] += 1
            self.store.finish_pipeline(
                spec.pipeline_id, "failed", n_tasks_done=start_index, error=problem
            )
            self._check_all_done()
            self._emit(
                "pipeline.failed",
                pipeline_id=spec.pipeline_id,
                data={"error": str(problem), "phase": "resource_check"},
            )
            return None
        if start_index:
            self._emit(
                "pipeline.resumed",
                pipeline_id=spec.pipeline_id,
                data={"from_seq": start_index, "resume_of": resumed_from},
            )

        seed_artifact = self.store.get_artifact(spec.pipeline_id, SEED_SEQ)
        if seed_artifact is None or not seed_artifact.available:
            seed_artifact = self._store_artifact(
                spec, SEED_TASK, SEED_SEQ, spec.seed, is_final=False
            )
        if start_index == 0:
            artifact = seed_artifact
            value = spec.seed

        state = _RunState(
            spec=spec,
            record=record,
            run_id=run_id,
            seq=start_index,
            value=value,
            artifact=artifact,
            task_spec=spec.tasks[start_index],
            task_record=None,  # type: ignore[arg-type]  (set by _begin_task below)
            start_index=start_index,
        )
        self._begin_task(state)
        return state

    def _settle_terminal_cursor(
        self, spec: PipelineSpec, record: PipelineRecord, run_id: str
    ) -> bool:
        """Settle a failed/interrupted row whose cursor already reached (or passed) the end.

        Two shapes arrive here, and they are not the same thing:

        * ``n_tasks_done == n_tasks`` — the torn finalization the success path can create: every task is
          checkpointed, but the terminal write did not land (a store error, or a process killed between
          the two writes). The artifacts are the truth, so the pipeline is repaired to ``succeeded``
          rather than run again — and rather than indexing ``spec.tasks[n_tasks]``, which used to raise
          ``IndexError`` into ``runner.internal_error`` on every later run. The terminal artifact has to
          pass the same checks an ordinary resumed checkpoint passes: present, payload available, and
          **decodable**.
        * ``n_tasks_done > n_tasks`` — not a state the Runner can create. The row is reported as corrupt
          and is never promoted to success; the cursor is deliberately left untouched as the evidence.

        A repair that fails — in ``mark_final`` or in the terminal settle itself — must never replace the
        failure that created the torn state: the row is left exactly as it was (state, cursor, owning run
        and failure fields included) and ``pipeline.terminal_repair_failed`` records the attempt, so a
        later repair still reports the original provenance. ``mark_final`` is skipped when the artifact is
        already final, and the terminal transition happens after it, in one write when the store offers
        ``settle_pipeline``.

        Returns True when the pipeline was settled here (nothing left to run). When the terminal artifact
        cannot serve as a checkpoint the row is rewound in memory and False is returned, so the ordinary
        restart-from-zero rule applies with its usual event.
        """
        previous = {
            "previous_state": record.state,
            "previous_error_type": record.error_type,
            "previous_error_message": record.error_message,
            "previous_failed_task": record.failed_task,
            "previous_run_id": record.run_id,
            "n_tasks_done": record.n_tasks_done,
            "n_tasks_total": spec.n_tasks,
        }
        if record.n_tasks_done > spec.n_tasks:
            error = CorruptCheckpoint(
                f"stored cursor n_tasks_done={record.n_tasks_done} exceeds the {spec.n_tasks} "
                f"task(s) of pipeline {spec.name!r}; the row is left for inspection"
            )
            self._counters["pipelines_failed"] += 1
            self._counters["pipelines_done"] += 1
            # Rebind the owning run so the corruption shows up in *this* run's report and exit code,
            # but keep the cursor: the corrupt value is the evidence. No n_tasks_done=... on purpose.
            record.run_id = run_id
            self.store.upsert_pipeline(record)
            self.store.finish_pipeline(spec.pipeline_id, "failed", error=error)
            self._check_all_done()
            self._emit("pipeline.corrupt_cursor", pipeline_id=spec.pipeline_id, data=previous)
            self._emit(
                "pipeline.failed",
                pipeline_id=spec.pipeline_id,
                data={"error": str(error), "phase": "corrupt_cursor"},
            )
            return True

        last = self.store.get_artifact(spec.pipeline_id, spec.n_tasks - 1)
        if last is None or not last.available:
            self._emit(
                "pipeline.checkpoint_missing",
                pipeline_id=spec.pipeline_id,
                data={
                    "reason": "the terminal artifact is gone or its payload was not kept; "
                    "rerunning the whole pipeline",
                    **previous,
                },
            )
            record.n_tasks_done = 0  # the ordinary branch below restarts from the seed
            return False
        try:
            self.registry.load(last.encoded())
        except Exception as exc:
            # `available` only means the bytes are there. A payload that no longer decodes is not a
            # checkpoint: marking it final would hand consumers a value they cannot restore, so the
            # pipeline restarts instead, with the cause kept distinct from a dropped payload.
            self._emit(
                "pipeline.checkpoint_unusable",
                pipeline_id=spec.pipeline_id,
                data={"error": str(exc), **previous},
            )
            record.n_tasks_done = 0
            return False

        # The whole finalization is one controlled recovery operation. A failure in *any* step of it has
        # to stay here: the pipeline row is the only place the original failure is recorded, and letting
        # the exception escape into the worker's internal-error path would rewrite that row with the
        # repair's own error — destroying exactly what the repair exists to preserve. The row is left
        # untouched (state, cursor, owning run and failure fields, all of them), and the failed attempt is
        # recorded as an event instead. `mark_final` is skipped when the artifact is already final, and is
        # documented as idempotent for the crash case where that cannot be observed.
        phase = "mark_final"
        try:
            if not last.is_final:
                self.store.mark_final(spec.pipeline_id, last.seq)
            phase = "settle"
            self._settle_succeeded(spec, run_id)
        except Exception as exc:
            # `pipelines_failed` keeps the run-level budgets honest; `repair_failures` is what makes the
            # failure visible at the API/CLI boundary, because this row stays owned by the earlier run and
            # therefore never appears in this run's run-scoped stats.
            self._counters["pipelines_failed"] += 1
            self._counters["repair_failures"] += 1
            self._counters["pipelines_done"] += 1
            self._check_all_done()
            self._emit(
                "pipeline.terminal_repair_failed",
                pipeline_id=spec.pipeline_id,
                data={**previous, "error": f"{type(exc).__name__}: {exc}", "phase": phase},
            )
            return True

        self._counters["pipelines_succeeded"] += 1
        self._counters["pipelines_done"] += 1
        self._check_all_done()
        self._emit(
            "pipeline.terminal_repaired",
            pipeline_id=spec.pipeline_id,
            data={**previous, "artifact": last.id, "artifact_seq": last.seq},
        )
        self._emit(
            "pipeline.succeeded",
            pipeline_id=spec.pipeline_id,
            data={"tasks": spec.n_tasks, "resumed_from": None, "repaired": True},
        )
        return True

    def _settle_succeeded(self, spec: PipelineSpec, run_id: str) -> None:
        """Make a repaired terminal state durable: one write when the store can, two ordered ones when not.

        ``settle_pipeline`` is the optional capability documented in ``store/base.py``. Without it the
        fallback writes the terminal transition first and the metadata cleanup second, and the two steps are
        classified differently on purpose, because they leave the row in different states:

        * a **terminal transition** failure propagates to the caller, which records a repair failure: the
          row is still the repairable one and a later run can try again;
        * a **cleanup** failure does not: the row is already durably ``succeeded``, so the pipeline *is*
          repaired and only its failure metadata is stale. Because a succeeded row is skipped forever, no
          later run can retry that cleanup — which is exactly why it must not be counted as a failed
          repair. It is reported as ``pipeline.terminal_cleanup_failed`` instead, and the stale fields are
          the documented degraded guarantee of a store without ``settle_pipeline``.
        """
        settle = getattr(self.store, "settle_pipeline", None)
        if callable(settle):
            settle(spec.pipeline_id, state="succeeded", n_tasks_done=spec.n_tasks, run_id=run_id)
            return
        self.store.finish_pipeline(spec.pipeline_id, "succeeded", n_tasks_done=spec.n_tasks)
        try:
            record = self.store.get_pipeline(spec.pipeline_id)
            if record is not None:
                record.run_id = run_id
                record.error_type = record.error_message = record.traceback = record.failed_task = None
                self.store.upsert_pipeline(record)
        except Exception as exc:
            self._emit(
                "pipeline.terminal_cleanup_failed",
                pipeline_id=spec.pipeline_id,
                data={
                    "error": f"{type(exc).__name__}: {exc}",
                    "note": "the pipeline is durably succeeded; only its failure metadata could not be "
                    "cleared (a store without settle_pipeline does this in a second write)",
                },
            )

    def _begin_task(self, state: _RunState) -> None:
        """Open the state's current task: resolve its algorithm and record a ``running`` row.

        The row is written *before* the first attempt, so a crash mid-attempt stays visible
        instead of leaving an unexplained gap.
        """
        state.task_spec = state.spec.tasks[state.seq]
        state.default_algorithm = state.task_spec.runtime_algorithm()
        state.attempts_used = 0
        state.task_started = self.clock.now()
        state.retry_after = None
        state.task_record = TaskRecord(
            task_run_id=f"{state.pipeline_id}:{state.seq}",
            pipeline_id=state.pipeline_id,
            run_id=state.run_id,
            name=state.task_spec.name,
            seq=state.seq,
            state="running",
            started_at=time.time(),
            input_artifact_id=state.artifact.id if state.artifact else None,
        )
        self.store.record_task(state.task_record)

    async def _drive(self, state: _RunState) -> None:
        """Run one pipeline until it finishes, fails, or parks itself for a retry backoff."""
        while True:
            result = await self._execute_task(state)
            state.attempts_used = result.attempts
            if result.retrying:
                state.retry_after = result.retry_after
                # Park the pipeline: the timer pump will put it back once the backoff is over,
                # so the worker slot is returned to the pool for the whole waiting period.
                self._delays.push(state, result.retry_after or 0.0)
                self._emit(
                    "pipeline.deferred",
                    pipeline_id=state.pipeline_id,
                    data={
                        "task": state.task_spec.name,
                        "seq": state.seq,
                        "attempt": state.attempts_used,
                        "delay_s": round(result.retry_after or 0.0, 4),
                        "in_flight": max(0, self._live["running"] - 1),
                    },
                )
                return

            outcome = result.outcome or _Outcome(
                error=PyAttackerError("attempt finished without a result")
            )
            if not outcome.ok:
                self._counters["pipelines_failed"] += 1
                self._counters["pipelines_done"] += 1
                # attempts_total is already durable: _execute_task persists state.record at the
                # start of every attempt, including the one that just failed.
                self.store.finish_pipeline(
                    state.pipeline_id,
                    "failed",
                    n_tasks_done=state.seq,
                    error=outcome.error,
                    failed_task=state.task_spec.name,
                    traceback=self._last_traceback,
                )
                self._check_all_done()
                self._emit(
                    "pipeline.failed",
                    pipeline_id=state.pipeline_id,
                    data={
                        "task": state.task_spec.name,
                        "seq": state.seq,
                        "error_class": outcome.error_class,
                        "error": f"{type(outcome.error).__name__}: {outcome.error}",
                    },
                )
                return

            # ★ task-level checkpoint: the artifact is already durable, now advance the cursor
            state.record.n_tasks_done = state.seq + 1
            if state.seq + 1 >= state.spec.n_tasks:
                # Finality first, terminal state second. The reverse order has a window in which a
                # crash leaves a permanently `succeeded` row whose final artifact was never marked --
                # and because a succeeded row is skipped forever, nothing would ever repair it. This
                # order's worst case is the documented at-least-once boundary: the final task runs
                # again. The cursor advance travels with finish_pipeline, so the row is never durably
                # `running` with cursor == n_tasks either.
                if outcome.artifact is not None:
                    self.store.mark_final(state.pipeline_id, outcome.artifact.seq)
                self.store.finish_pipeline(
                    state.pipeline_id, "succeeded", n_tasks_done=state.spec.n_tasks
                )
                self._counters["pipelines_succeeded"] += 1
                self._counters["pipelines_done"] += 1
                self._check_all_done()
                self._emit(
                    "pipeline.succeeded",
                    pipeline_id=state.pipeline_id,
                    data={"tasks": state.spec.n_tasks, "resumed_from": state.start_index},
                )
                return
            # Intermediate checkpoint: written on its own so a crash mid-pipeline keeps the cursor.
            self.store.upsert_pipeline(state.record)
            state.value = outcome.value
            state.artifact = outcome.artifact
            state.seq += 1
            self._begin_task(state)

    def _pool_problem(self, spec: PipelineSpec) -> ConfigError | None:
        """Problems that should be blocked at construction time: a task declares an unknown resource pool."""
        for task_spec in spec.tasks:
            if task_spec.resource and task_spec.resource not in self.pools:
                return ConfigError(
                    f"task {task_spec.name!r} declares unknown resource pool {task_spec.resource!r}"
                    f" (registered: {sorted(self.pools)})"
                )
        return None

    async def _execute_task(self, state: _RunState) -> _TaskResult:
        """Execute exactly **one** attempt of the state's current task.

        Never sleeps and never blocks on a retry decision: when the policy wants another
        attempt, the delay is handed back to :meth:`_drive`, which parks the pipeline.
        """
        spec = state.spec
        seq = state.seq
        task_spec = state.task_spec
        record = state.task_record
        task_run_id = record.task_run_id
        retry = task_spec.retry
        state.attempts_used += 1
        attempts_used = state.attempts_used
        record.attempts_used = attempts_used
        state.record.attempts_total += 1
        # Persisted immediately, not just at the next checkpoint: a crash while this pipeline
        # sits in the delay queue waiting out a retry backoff must not lose the attempt count
        # that already durably happened. Pipeline state writes are synchronous by convention
        # (see store/base.py), so one extra write per attempt is the correct trade, not batched.
        self.store.upsert_pipeline(state.record)
        # Deterministic seed, not the shared `random` module: re-running the same pipeline/seq/attempt
        # (e.g. replaying a resumed run against the same checkpoint) must reproduce the same backoff
        # jitter and the same ctx.seed, or two "identical" runs would silently diverge. Do not switch
        # this to random.Random() without a good reason.
        rng = random.Random(int(digest_of(f"{spec.pipeline_id}|{seq}|{attempts_used}")[:16], 16))
        ctx = TaskContext(
            run_id=state.run_id,
            pipeline_id=spec.pipeline_id,
            pipeline_key=spec.key,
            pipeline_name=spec.name,
            task_name=task_spec.name,
            seq=seq,
            attempt=attempts_used,
            clock=self.clock,
            pools=self.pools,
            bus=self.bus,
            rng=rng,
            registry=self.registry,
            seed=rng.randrange(1 << 31),
            default_pool=task_spec.resource,
            default_algorithm=state.default_algorithm,
            emit=lambda kind, data, _pid=spec.pipeline_id, _tid=task_run_id: self._emit(
                kind, scope="task", pipeline_id=_pid, task_run_id=_tid, data=dict(data)
            ),
            meta={"tags": spec.template.tags},
        )
        attempt_started = self.clock.now()
        error: BaseException | None = None
        cancelled = False
        out_value: Any = None
        leaked = 0
        try:
            produced = task_spec(state.value, ctx)
            if inspect.isawaitable(produced):
                if task_spec.timeout_s:
                    out_value = await asyncio.wait_for(produced, task_spec.timeout_s)
                else:
                    out_value = await produced
            else:  # synchronous, pure-computation task
                out_value = produced
        except asyncio.CancelledError as exc:
            cancelled = True
            error = exc
        except BaseException as exc:
            error = exc
        finally:
            # ★ reclamation guarantee: synchronous, cannot be interrupted by cancellation
            leaked = ctx.reclaim_now()
            if leaked:
                self._counters["leases_leaked"] += leaked

        attempt_ms = (self.clock.now() - attempt_started) * 1000.0
        if cancelled:
            self._record_attempt(
                spec, task_spec, task_run_id, attempts_used, attempt_started, attempt_ms,
                "cancelled", error, ctx.lease_log(), {},
            )
            record.state = "interrupted"
            record.ended_at = time.time()
            self.store.record_task(record)
            raise error  # let the cancellation keep propagating upward

        if error is None and leaked and self.config.strict_leases:
            error = LeaseLeakError(
                f"task {task_spec.name!r} ended while still holding {leaked} leases (strict_leases=True)"
            )

        if error is None:
            artifact = self._store_artifact(spec, task_spec.name, seq, out_value)
            record.state = "succeeded"
            record.ended_at = time.time()
            record.duration_ms = attempt_ms
            record.output_artifact_id = artifact.id
            record.error_class = record.error_type = record.error_message = None
            self.store.record_task(record)
            self._record_attempt(
                spec, task_spec, task_run_id, attempts_used, attempt_started, attempt_ms,
                "succeeded", None, ctx.lease_log(), {"decision": {"retry": False, "reason": "ok"}},
            )
            self._emit(
                "task.succeeded",
                pipeline_id=spec.pipeline_id,
                task_run_id=task_run_id,
                data={"task": task_spec.name, "seq": seq, "attempt": attempts_used,
                      "duration_ms": round(attempt_ms, 3), "artifact": artifact.id,
                      "digest": artifact.digest},
            )
            return _TaskResult(
                outcome=_Outcome(value=out_value, artifact=artifact, attempts=attempts_used),
                attempts=attempts_used,
            )

        # ---------------- failure: decide whether to retry ----------------
        # The policy itself lives on Retrying (task.py): the benchmark harness asks it the same
        # question outside a run, and one implementation is one thing to keep correct.
        elapsed = self.clock.now() - state.task_started
        decision = retry.decide(error, attempts_used=attempts_used, rng=rng, elapsed=elapsed)
        decision["leaked_leases"] = leaked
        delay = decision["delay_s"]
        error_class = decision["error_class"]
        self._last_traceback = "".join(tb_mod.format_exception(type(error), error, error.__traceback__))
        self._record_attempt(
            spec, task_spec, task_run_id, attempts_used, attempt_started, attempt_ms,
            "timeout" if error_class == "timeout" else "failed", error, ctx.lease_log(),
            {"decision": decision},
        )
        self._emit(
            "task.failed",
            pipeline_id=spec.pipeline_id,
            task_run_id=task_run_id,
            data={
                "task": task_spec.name,
                "seq": seq,
                "attempt": attempts_used,
                "error_class": error_class,
                "error": f"{type(error).__name__}: {error}",
                "decision": decision,
            },
        )
        if not decision["retry"]:
            record.state = "failed"
            record.ended_at = time.time()
            record.duration_ms = (self.clock.now() - state.task_started) * 1000.0
            record.error_class = error_class
            record.error_type = type(error).__name__
            record.error_message = str(error)[:2000]
            record.traceback = self._last_traceback
            self.store.record_task(record)
            return _TaskResult(
                outcome=_Outcome(error=error, attempts=attempts_used, error_class=error_class),
                attempts=attempts_used,
            )
        # Keep the in-progress row visible while the pipeline is parked.
        record.state = "running"
        self.store.record_task(record)
        self._emit(
            "task.retry_scheduled",
            pipeline_id=spec.pipeline_id,
            task_run_id=task_run_id,
            data={"task": task_spec.name, "attempt": attempts_used, "delay_s": round(delay, 4)},
        )
        return _TaskResult(retry_after=delay, attempts=attempts_used)

    # ------------------------------------------------------------------ helpers
    def _record_attempt(
        self,
        spec: PipelineSpec,
        task_spec: TaskSpec,
        task_run_id: str,
        attempt_no: int,
        started: float,
        duration_ms: float,
        outcome: str,
        error: BaseException | None,
        leases: Sequence[Any],
        extra: Mapping[str, Any],
    ) -> AttemptRecord:
        decision = dict(extra.get("decision", {}))
        record = AttemptRecord(
            pipeline_id=spec.pipeline_id,
            run_id=self._run_id or "",
            task_run_id=task_run_id,
            task_name=task_spec.name,
            seq=int(task_run_id.rsplit(":", 1)[1]),
            attempt_no=attempt_no,
            started_at=time.time() - duration_ms / 1000.0,
            ended_at=time.time(),
            duration_ms=round(duration_ms, 3),
            outcome=outcome,
            error_class=error_class_of(error) if error is not None else None,
            error_type=type(error).__name__ if error is not None else None,
            error_message=str(error)[:2000] if error is not None else None,
            traceback=(
                "".join(tb_mod.format_exception(type(error), error, error.__traceback__))
                if error is not None
                else None
            ),
            retry_delay_s=decision.get("delay_s"),
            decision=decision,
            leases=[_lease_entry(lease) for lease in leases],
        )
        return self.store.record_attempt(record)

    def _store_artifact(
        self, spec: PipelineSpec, task_name: str, seq: int, value: Any, *, is_final: bool = False
    ) -> Artifact:
        encoded = self.registry.dump(value)
        artifact = Artifact(
            id=Artifact.build_id(spec.pipeline_id, seq),
            pipeline_id=spec.pipeline_id,
            task_name=task_name,
            seq=seq,
            type_name=encoded.type_name,
            codec=encoded.codec,
            digest=encoded.digest,
            size=encoded.size,
            payload=encoded.data,
            created_at=time.time(),
            is_final=is_final,
        )
        return self.store.put_artifact(artifact)

    def _emit(
        self,
        kind: str,
        *,
        scope: str = "pipeline",
        pipeline_id: str | None = None,
        task_run_id: str | None = None,
        pool: str | None = None,
        resource_id: str | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        self.store.emit_event(
            EventRecord(
                ts=time.time(),
                kind=kind,
                scope=scope,
                run_id=self._run_id,
                pipeline_id=pipeline_id,
                task_run_id=task_run_id,
                pool=pool,
                resource_id=resource_id,
                data=dict(data or {}),
            )
        )

    async def _heartbeat_loop(self, run_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(self.config.heartbeat_s)
                self.store.heartbeat(run_id)
                self._flush_store()  # bound how much a hard crash can lose
        except asyncio.CancelledError:
            return

    def _flush_store(self) -> int:
        """Push buffered facts to disk if the store batches them (no-op otherwise)."""
        flush = getattr(self.store, "flush", None)
        if flush is None:
            return 0
        try:
            return int(flush())
        except Exception:  # pragma: no cover - a flush failure must not kill the run
            return 0

    def _finalize_pools(self) -> None:
        for pool in self.pools.values():
            resources = {r.id: r for r in pool.resources()}
            for slot in pool.snapshot():
                resource = resources.get(slot["id"])
                self.store.upsert_resource(
                    pool.name,
                    slot["id"],
                    slot["kind"],
                    resource.spec() if resource is not None else {},
                    slot["state"],
                    slot,
                )

    def _install_signals(self):
        try:
            loop = asyncio.get_running_loop()
            if not hasattr(loop, "add_signal_handler"):  # some loops (e.g. Windows) cannot
                return None
        except RuntimeError:  # pragma: no cover - no running loop
            return None
        if threading.current_thread() is not threading.main_thread():  # pragma: no cover
            return None

        def _handler() -> None:
            self.stop("signal")

        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, _handler)
        except (ValueError, NotImplementedError):  # pragma: no cover
            return None

        def _restore() -> None:
            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.remove_signal_handler(sig)
            except Exception:  # pragma: no cover
                pass

        return _restore

    # ------------------------------------------------------------------ monitoring
    def stats(self) -> dict[str, Any]:
        """Live snapshot: safe to call at any point during a run (for progress bars / monitoring dashboards)."""
        rows = self.store.stats(self._run_id) if self._run_id else {"pipelines": {"total": 0, "by_state": {}}}
        elapsed = (
            self.clock.now() - self._live["started_at"] if self._live.get("started_at") else None
        )
        return {
            "run_id": self._run_id,
            "stopping": self.stopping,
            "in_flight_pipelines": self._live["running"],
            "counters": dict(self._counters),
            "elapsed_s": round(elapsed, 3) if elapsed else None,
            "pools": {name: dataclasses.asdict(pool.stats()) for name, pool in self.pools.items()},
            "delayed_pipelines": len(self._delays),
            "buffered": self.store.buffer_stats() if hasattr(self.store, "buffer_stats") else None,
            **rows,
        }

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> "Runner":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def _code_version() -> str:
    try:
        from . import __version__

        return __version__
    except Exception:  # pragma: no cover
        return "unknown"


def _lease_entry(lease: Any) -> dict[str, Any]:
    """Normalize a lease record: accepts both a Lease object and a dict produced by ctx.lease_log()."""
    if isinstance(lease, Mapping):
        return dict(lease)
    return {
        "pool": lease.pool.name,
        "resource": lease.resource.id,
        "kind": lease.resource.kind,
        "held_ms": round(lease.held_ms, 3),
        "released": lease.released,
    }
