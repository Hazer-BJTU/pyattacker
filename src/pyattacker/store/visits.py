"""Optional visit-aware store capability for backward-enabled pipelines.

The JSON traversal record owns counters, effective slots and the pending entry. Backends
supply one atomic write boundary and low-level writes; it never relies on seq ordering.

The module also owns the store's **feature level**, the durable compatibility marker that tells a
lineage-unaware writer to stay away (see :func:`check_feature_level`).
"""

from __future__ import annotations

import copy
import dataclasses
import time
from typing import Any

from ..artifact import Artifact, Encoded
from ..errors import FatalError, PyAttackerError, StoreFeatureUnsupported
from .base import AttemptRecord, HandoffRecord, PipelineRecord, TaskRecord

FEATURE_BASE = "base"
"""No visit-qualified occurrence has ever been written: the store is an ordinary v1 store."""

FEATURE_VISITS = "visits-v1"
"""A second immutable occurrence for some station exists, or did at some point.

Irreversible on purpose: audit rows are never deleted, so a store that has reached this level stays
here even after the revisit that set it finished.
"""

FEATURE_LEVELS = (FEATURE_BASE, FEATURE_VISITS)
"""Every level this build understands, oldest first. A store above the last entry is refused."""

CURRENT_FEATURE_LEVEL = FEATURE_LEVELS[-1]
"""The level this build writes: what a persistent backend marks when it commits a revisit."""


def check_feature_level(level: str, *, store: str, read_only: bool = False) -> None:
    """Refuse a store whose durable feature level this build does not understand.

    The check runs when a store is opened, before any read or write. An unknown level means the
    store was written by a newer pyattacker whose lineage model this build cannot interpret:
    operating on it would mean selecting artifact occurrences by ``seq`` alone, which reads the
    wrong payload and can delete the wrong rows. ``read_only`` is accepted so the message can name
    the mode, but the level is refused there too — a lineage-unaware reader reports a lineage it
    does not have.
    """
    if level in FEATURE_LEVELS:
        return
    mode = "read-only" if read_only else "write"
    raise StoreFeatureUnsupported(
        f"store {store} is at feature level {level!r}, which this build does not understand "
        f"(it knows {', '.join(FEATURE_LEVELS)}); refusing {mode} access. Upgrade pyattacker to open "
        "this store; back it up with SQLite's own backup API (sqlite3 .backup) or a file copy while "
        "no writer is active (see docs/reference.md, store compatibility and backups)"
    )


class VisitStore:
    """Shared transition semantics, implemented by MemoryStore and SqliteStore.

    Backend hooks are private; third-party stores can implement the public capability directly.
    An input/output occurrence is always referenced by exact ID, independently of task seq.

    The capability is deliberately more than the visit methods alone: a backward control transition
    also lands its ledger row, source task and attempt through the v1 ``commit_handoff``
    (see :meth:`commit_control_transition`), so a store that exposes the visit methods without that
    commit would accept a rewind and then fail halfway through it.
    """

    def feature_level(self) -> str:
        """The durable feature level this store has reached (:data:`FEATURE_BASE` when unmarked).

        Read by a later opener to decide whether it may touch the store at all; see
        :func:`check_feature_level`. A backend that keeps no durable state (``MemoryStore``) still
        answers, so callers and tests do not have to special-case it.
        """
        return FEATURE_BASE

    def _visit_mark_revisit(self) -> None:
        """Backend hook: record durably that a revisit-qualified occurrence now exists.

        Called inside the transaction that allocates the first second occurrence for a station —
        the exact moment a lineage-unaware writer stops being able to tell the occurrences apart —
        so the marker can never be separated from the state it describes. A backend without durable
        state has nothing to protect and keeps the default no-op.
        """

    def _visit_abandon_running_tasks(self, pipeline_id: str) -> None:
        """Backend hook: settle every task row of this pipeline that is still in flight.

        ``reset_visits`` discards a traversal, and a hard kill can leave the occurrence it was waiting on
        ``running`` with nothing left that will ever move it. The traversal record names that occurrence
        exactly, but a damaged store may not have the record at all — so the invariant is expressed on the
        rows instead: during an open (before this run begins its own task) no task of this pipeline can
        legitimately be in flight, and any row that says otherwise is abandoned. Backends that keep task
        rows move such rows to ``interrupted``, the state abandonment already uses; a backend without
        durable rows keeps the default no-op.
        """

    def _visit_observed_counters(self, pipeline_id: str, n_tasks_total: int) -> dict[str, int]:
        """The highest visit allocated per ``seq``, read back from durable rows.

        Used only when the traversal record itself is missing (damage, or the explicit restart that
        recovers from it), where it reconstructs the counters the lost record owned. Backends that
        keep no durable rows return the empty mapping and keep the default.

        Only real station slots (``0 <= seq < n_tasks_total``) count: handoff payloads are durable
        artifacts too, but they are addressed beyond the chain (``n_tasks_total + k``), so treating
        them as stations would invent counters for seqs that do not exist.
        """
        return {}

    def reset_visits(
        self, record: PipelineRecord, seed: Artifact, *, fresh_budget: bool = False
    ) -> PipelineRecord:
        with self._visit_atomic(record.pipeline_id):
            # The traversal is about to be replaced, so nothing it still owns can be "in flight":
            # every task row of this pipeline that says `running` is abandoned, and nothing else will
            # ever move it (a hard kill leaves exactly that shape). Done before the new traversal is
            # written, and keyed by the pipeline rather than by the record's pending entry, so a
            # damaged store with no traversal left is covered by the same rule.
            self._visit_abandon_running_tasks(record.pipeline_id)
            old = self.visit_state(record.pipeline_id)
            # Visit counters are preserved across a reset -- that is what keeps every historical
            # occurrence addressable -- and when the traversal itself is gone they are rebuilt from
            # the durable rows instead of restarting at 0. Starting over at 0 would re-allocate
            # occurrence IDs that earlier rows still carry, so the audit trail would silently lose
            # the artifacts it points at.
            counters = (old or {}).get("counters")
            if counters is None:
                counters = self._visit_observed_counters(record.pipeline_id, record.n_tasks_total)
            state = {
                "version": (old or {}).get("version", 0) + 1,
                "counters": counters,
                "active": {},
                "pending": None,
                "input": seed.id,
                "cursor": 0,
                "terminal": None,
                "handoffs": 0 if fresh_budget else (old or {}).get("handoffs", 0),
            }
            self._visit_final(record.pipeline_id, None)
            latest = self.handoffs(pipeline_id=record.pipeline_id, limit=1)
            record = dataclasses.replace(
                record, n_tasks_done=0, handoff_floor=latest[-1].handoff_id if latest else 0
            )
            self._visit_pipeline(record)
            self._visit_save(record.pipeline_id, state)
            return record

    def repair_visit_terminal(self, record: PipelineRecord) -> None:
        with self._visit_atomic(record.pipeline_id):
            state = self._require_visits(record.pipeline_id)
            if state["terminal"] is None or self.get_artifact_by_id(state["terminal"]) is None:
                raise PyAttackerError("corrupt visit checkpoint: terminal occurrence is missing")
            self._visit_final(record.pipeline_id, state["terminal"])
            self._visit_pipeline(
                dataclasses.replace(
                    record, state="succeeded", n_tasks_done=record.n_tasks_total, finished_at=time.time()
                )
            )

    def _allocate_entry(self, pipeline_id: str, state: dict[str, Any], task: TaskRecord) -> TaskRecord:
        key = str(task.seq)
        visit = state["counters"].get(key, -1) + 1
        state["counters"][key] = visit
        if visit > 0:
            # The first revisit is the moment this store stops being interpretable as a v1 store:
            # one station now owns two immutable occurrences, and a writer that selects by `seq`
            # alone reads or mutates the wrong one. Marked here, inside the caller's transaction.
            self._visit_mark_revisit()
        task = dataclasses.replace(
            task, visit=visit, task_run_id=f"{pipeline_id}:{task.seq}" + (f"#{visit}" if visit else "")
        )
        state["pending"] = {
            "seq": task.seq,
            "visit": visit,
            "task_run_id": task.task_run_id,
            "input": task.input_artifact_id,
        }
        state["cursor"] = task.seq
        state["input"] = task.input_artifact_id
        self._visit_task(task)
        return task

    def commit_entry(self, task: TaskRecord) -> TaskRecord:
        with self._visit_atomic(task.pipeline_id):
            state = self._require_visits(task.pipeline_id)
            if (
                state["terminal"] is not None
                or state["cursor"] != task.seq
                or state["input"] != task.input_artifact_id
            ):
                raise PyAttackerError("corrupt visit checkpoint: entry does not match durable cursor/input")
            if state["pending"] is not None:
                pending = state["pending"]
                if pending["seq"] != task.seq or pending["input"] != task.input_artifact_id:
                    raise PyAttackerError("corrupt visit checkpoint: pending entry mismatch")
                rows = self.tasks(task.pipeline_id)
                existing = next((r for r in rows if r.task_run_id == pending["task_run_id"]), None)
                if existing is None:
                    raise PyAttackerError("corrupt visit checkpoint: pending task is missing")
                # Keep the committed visit and consumed attempt numbering, rebind its owning run.
                task = dataclasses.replace(
                    existing,
                    run_id=task.run_id,
                    state="running",
                    error_class=None,
                    error_type=None,
                    error_message=None,
                    traceback=None,
                )
                self._visit_task(task)
            else:
                task = self._allocate_entry(task.pipeline_id, state, task)
            self._visit_save(task.pipeline_id, state)
            return task

    def commit_visit_attempt(self, pipeline: PipelineRecord, task: TaskRecord) -> None:
        """Reserve attempt numbering before executing task code (including interrupted attempts)."""
        with self._visit_atomic(task.pipeline_id):
            self._check_source(self._require_visits(task.pipeline_id), task)
            self._visit_task(task)
            self._visit_pipeline(pipeline)

    def _require_visits(self, pipeline_id: str) -> dict[str, Any]:
        state = self.visit_state(pipeline_id)
        if state is None:
            raise PyAttackerError("corrupt visit checkpoint: traversal state is missing")
        return copy.deepcopy(state)

    @staticmethod
    def _check_source(state: dict[str, Any], task: TaskRecord) -> None:
        pending = state["pending"]
        if pending is None or pending["task_run_id"] != task.task_run_id or pending["visit"] != task.visit:
            raise PyAttackerError("stale control transition: active source entry changed")

    def commit_visit_success(
        self,
        pipeline: PipelineRecord,
        task: TaskRecord,
        attempt: AttemptRecord,
        artifact: Artifact,
        *,
        final: bool,
    ) -> Artifact:
        with self._visit_atomic(task.pipeline_id):
            state = self._require_visits(task.pipeline_id)
            self._check_source(state, task)
            stored = self._visit_artifact(artifact)
            self._visit_task(task)
            self._visit_attempt(attempt)
            state["active"][str(task.seq)] = {
                "task_run_id": task.task_run_id,
                "visit": task.visit,
                "output": artifact.id,
            }
            state["pending"] = None
            state["cursor"] = task.seq + 1
            state["input"] = artifact.id
            pipeline = dataclasses.replace(pipeline, n_tasks_done=task.seq + 1)
            if final:
                state["terminal"] = artifact.id
                pipeline.state = "succeeded"
                pipeline.finished_at = time.time()
                self._visit_final(task.pipeline_id, artifact.id)
            self._visit_pipeline(pipeline)
            self._visit_save(task.pipeline_id, state)
            return stored

    def commit_control_transition(
        self,
        record: HandoffRecord,
        *,
        pipeline: PipelineRecord,
        task: TaskRecord,
        attempt: AttemptRecord,
        payload: Encoded | None,
        entry_id: str | None,
        target_task: TaskRecord | None,
        limit: int,
    ) -> Artifact:
        with self._visit_atomic(record.pipeline_id):
            state = self._require_visits(record.pipeline_id)
            self._check_source(state, task)
            final = record.to_seq is None
            if not final and state["handoffs"] >= limit:
                raise FatalError(f"control budget exhausted: consumed {state['handoffs']}, allowed {limit}")
            if payload is None:
                entry = self.get_artifact_by_id(entry_id or "")
                if entry is None:
                    raise PyAttackerError("control entry artifact is missing")
                record.entry_seq = entry.seq
                record.entry_artifact_id = entry.id
            record.transition_version = state["version"] + 1
            record.to_visit = None if final else state["counters"].get(str(record.to_seq), -1) + 1
            stored = self._visit_handoff(
                record, task, attempt, payload, pipeline.n_tasks_total if final else record.to_seq, final
            )
            entry = stored if payload is not None else entry
            if entry is None:
                raise PyAttackerError("control payload was not persisted")
            state["version"] += 1
            state["pending"] = None
            if not final:
                state["handoffs"] += 1
            if record.operation in ("rewind", "retry_all"):
                state["active"] = {k: v for k, v in state["active"].items() if int(k) < record.to_seq}
                self._visit_final(record.pipeline_id, None)
            else:
                state["active"][str(task.seq)] = {
                    "task_run_id": task.task_run_id,
                    "visit": task.visit,
                    "output": None,
                }
            state["cursor"] = pipeline.n_tasks_total if final else record.to_seq
            state["input"] = entry.id
            state["terminal"] = entry.id if final else None
            if target_task is not None:
                self._allocate_entry(
                    record.pipeline_id, state, dataclasses.replace(target_task, input_artifact_id=entry.id)
                )
            self._visit_save(record.pipeline_id, state)
            return entry


def supports_visits(store: Any) -> bool:
    from .writebehind import WriteBehindStore

    while isinstance(store, WriteBehindStore):
        store = store.inner
    return all(
        callable(getattr(store, name, None))
        for name in (
            "visit_state",
            "reset_visits",
            "commit_entry",
            "commit_visit_attempt",
            "commit_visit_success",
            "commit_control_transition",
            "get_artifact_by_id",
            "reset_pipeline",
            "commit_handoff",
            "repair_visit_terminal",
            "handoffs",
            # The compatibility contract is part of the capability, not an extra: a store that cannot
            # declare how far its on-disk model has come cannot honour the downgrade rule the visit
            # model depends on, so it is refused here rather than trusted and checked later.
            "feature_level",
        )
    )
