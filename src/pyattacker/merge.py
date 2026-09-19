"""Merge: one coherent answer out of N shard databases.

Sharding means several processes each wrote their own store. Joining them cannot be a matter of
concatenating rows, because:

* the same ``pipeline_id`` can exist in more than one shard — you changed the shard count, or
  reran with ``--resume`` after moving files. Counting it twice would inflate every rate in the
  report, so rows are de-duplicated by ``pipeline_id``;
* the copies are not equivalent: one shard may hold a ``succeeded`` row while another holds the
  ``failed`` one from an earlier attempt. The rule is: **prefer the best state, break ties by the
  latest finish time**, and report how many duplicates were folded so the number is never hidden.

Statistics are recomputed from the merged rows rather than summed per store, for the same reason:
``attempts_total`` and ``handoffs_total`` are read off the surviving rows, so folding a duplicate — or
passing the same store twice — cannot inflate them. The one count that cannot be re-derived is the event
log: events are not part of a pipeline row, so nothing in the merged rows says which of two copies owns
them. That one keeps its own scope, as ``source_events_total``: a raw total over the given sources,
deliberately **not** de-duplicated (issue #59).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import ConfigError
from .export import write_rows
from .store.base import Store, handoff_row

__all__ = ["MergedReport", "merge_reports", "STATE_RANK"]

# How good a pipeline state is; used to pick a winner when the same pipeline exists twice.
STATE_RANK = {"succeeded": 4, "failed": 3, "interrupted": 2, "pending": 1, "canceled": 0}


@dataclass
class MergedReport:
    rows: list[dict[str, Any]]
    sources: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)
    duplicates: int = 0
    # De-duplicated: recomputed from the rows that survived folding, never summed per store.
    attempts_total: int = 0
    handoffs_total: int = 0
    # Raw: the append-only event log, summed over the given sources and **not** de-duplicated. An event
    # cannot be attributed to one of two copies of a pipeline, so this keeps its own scope instead of
    # pretending to be a de-duplicated workload count.
    source_events_total: int = 0
    repair_failures: int = 0

    @property
    def events_total(self) -> int:
        """Deprecated alias of :attr:`source_events_total`, removed at 1.0.

        The old name is deliberately kept **off** ``stats()``: the point of the rename (issue #59) was that a
        report cannot carry the de-duplicated counters and the raw event log under names that do not say which
        is which. On the object it costs nothing and spares a caller a rename.
        """
        return self.source_events_total

    # ----------------------------------------------------------------- views
    def stats(self) -> dict[str, Any]:
        by_state: dict[str, int] = {}
        task_counts: dict[str, int] = {}
        durations: list[float] = []
        for row in self.rows:
            state = row.get("state", "unknown")
            by_state[state] = by_state.get(state, 0) + 1
            for task in row.get("tasks") or []:
                name = task.get("name", "?")
                task_counts[name] = task_counts.get(name, 0) + 1
            if row.get("duration_ms") is not None:
                durations.append(float(row["duration_ms"]))
        durations.sort()

        def pct(q: float) -> float | None:
            if not durations:
                return None
            index = min(len(durations) - 1, int(q * (len(durations) - 1)))
            return round(durations[index], 3)

        return {
            "sources": len(self.sources),
            "pipelines": {
                "total": len(self.rows),
                "by_state": by_state,
                "duration_ms": {"p50": pct(0.5), "p95": pct(0.95), "max": pct(1.0)},
            },
            "tasks": {"by_name": task_counts},
            "attempts_total": self.attempts_total,
            "handoffs_total": self.handoffs_total,
            "source_events_total": self.source_events_total,
            "repair_failures": self.repair_failures,
            "duplicates_folded": self.duplicates,
        }

    def summary(self) -> str:
        stats = self.stats()
        pipes = stats["pipelines"]
        by_state = pipes["by_state"]
        durations = pipes["duration_ms"]
        lines = [
            f"merged {len(self.rows)} pipelines from {len(self.sources)} store(s)"
            + (f"  (folded {self.duplicates} duplicate rows)" if self.duplicates else ""),
            "  " + " ".join(f"{k}={v}" for k, v in sorted(by_state.items()))
            + f"  attempts={stats['attempts_total']} source_events={stats['source_events_total']}"
            + (f"  handoffs={stats['handoffs_total']}" if stats["handoffs_total"] else "")
            + (f"  repair_failures={stats['repair_failures']}" if stats["repair_failures"] else ""),
            f"  pipeline latency ms: p50={durations['p50']} p95={durations['p95']} max={durations['max']}",
        ]
        tasks = stats["tasks"]["by_name"]
        if tasks:
            lines.append("  tasks: " + " ".join(f"{k}={v}" for k, v in sorted(tasks.items())))
        failed = [row for row in self.rows if row.get("state") == "failed"]
        if failed:
            lines.append(f"  errors ({len(failed)}):")
            for row in failed[:5]:
                lines.append(
                    f"    - {row.get('name')}/{row.get('failed_task') or '?'}: "
                    f"{row.get('error_type')}: {str(row.get('error_message'))[:110]}"
                )
        for path in self.sources:
            lines.append(f"  source: {path}")
        return "\n".join(lines)

    def errors(self, limit: int = 20) -> list[dict[str, Any]]:
        out = [
            {
                "pipeline_id": row.get("pipeline_id"),
                "name": row.get("name"),
                "failed_task": row.get("failed_task"),
                "error_type": row.get("error_type"),
                "error_message": row.get("error_message"),
            }
            for row in self.rows
            if row.get("state") == "failed"
        ]
        return out[:limit]

    def export(self, path: str | None, *, fmt: str = "jsonl", kind: str = "pipelines") -> int:
        if kind != "pipelines":
            raise ValueError("a merged report only carries pipeline rows; export per store for other kinds")
        return write_rows(self.rows, path, fmt=fmt, title=f"merged from {len(self.sources)} stores")


def _countable_row(store: Any, row: Mapping[str, Any], source: str) -> Mapping[str, Any]:
    """Make one exported pipeline row countable, or say precisely why it is not.

    De-duplicating first and counting from the surviving rows means two fields have to be readable off a row
    (issue #59). They are not equally negotiable:

    * ``attempts_total`` is a core part of a pipeline row — ``PipelineRecord.attempts_total``, which both
      built-in stores export. A row without it cannot be counted, and quietly reporting ``0`` would be a
      wrong number that looks like a real one, so this fails loudly and names the row and its source.
    * the ``handoffs`` ledger is an **optional** capability: a store that predates handoffs has no ledger and
      genuinely has no jumps. A row that does not nest the ledger is read through the store's own
      ``handoffs()`` when it has one (the same capability the nesting comes from, serialized by the same
      :func:`handoff_row`), and only a store with neither counts as zero.
    """
    if "attempts_total" not in row:
        raise ConfigError(
            f"merge_reports cannot count pipeline row {row.get('pipeline_id')!r} from {source!r}: it has no "
            "'attempts_total'. A merged report recomputes its counters from the surviving rows instead of "
            "summing store.stats() per source, so export_rows() must carry the field — both built-in stores "
            "do. See docs/reference.md, 'Tables and readers'."
        )
    if "handoffs" in row:
        return row
    ledger = getattr(store, "handoffs", None)
    if not callable(ledger):
        return row  # no ledger capability: no ledger rows, which is the truth
    return {**row, "handoffs": [handoff_row(hop) for hop in ledger(pipeline_id=row["pipeline_id"])]}


def _winner(current: Mapping[str, Any], candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    current_rank = STATE_RANK.get(str(current.get("state")), -1)
    candidate_rank = STATE_RANK.get(str(candidate.get("state")), -1)
    if candidate_rank != current_rank:
        return candidate if candidate_rank > current_rank else current
    current_end = current.get("finished_at") or 0.0
    candidate_end = candidate.get("finished_at") or 0.0
    return candidate if candidate_end > current_end else current


def merge_reports(
    sources: Sequence[Store | str],
    *,
    run_id: str | None = None,
    open_store: Any = None,
) -> MergedReport:
    """Merge pipeline rows from several stores. ``sources`` may be stores or SQLite paths."""
    if open_store is None:  # imported lazily so this module stays usable without SQLite
        from .store.base import count_events as _count
        from .store.base import open_store as _open

        open_store = _open
        count_ev = _count
    else:
        from .store.base import count_events as _count
        count_ev = _count

    rows_by_id: dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    source_events_total = 0
    # Track which pipelines have at least one failed terminal repair.
    # We collect this during the store loop, before stores are closed.
    repair_failed_pids: set[str] = set()
    paths: list[str] = []
    run_ids: list[str] = []

    for source in sources:
        store = source if hasattr(source, "export_rows") else open_store(str(source))
        path = getattr(store, "path", None) or getattr(getattr(store, "inner", None), "path", None)
        paths.append(str(path or source))
        try:
            # The event log is the one count that cannot be re-derived after folding, because events do
            # not hang off a pipeline row: sum the aggregate per source and call it what it is. Never do
            # this for attempts/handoffs — those come back off the surviving rows below (issue #59).
            counts = store.stats(run_id)
            source_events_total += int(counts.get("events_total") or 0)
            for row in store.export_rows(run_id=run_id):
                key = row["pipeline_id"]
                run_ids.append(row["run_id"])
                row = _countable_row(store, row, paths[-1])
                if key in rows_by_id:
                    duplicates += 1
                    rows_by_id[key] = _winner(rows_by_id[key], row)
                else:
                    rows_by_id[key] = row
                # Collect repair-failed pipeline ids from this store.
                # (A pipeline may appear in multiple stores if they were copied.)
                if key not in repair_failed_pids and count_ev(
                    store, kind="pipeline.terminal_repair_failed", pipeline_id=key
                ) > 0:
                    repair_failed_pids.add(key)
        finally:
            if not hasattr(source, "export_rows"):
                store.close()

    ordered = sorted(rows_by_id.values(), key=lambda r: (r.get("name") or "", r.get("started_at") or 0.0))
    # Recomputed from the surviving rows, never summed per store: this is what makes a merge idempotent,
    # for the same store passed twice and for a pipeline that genuinely exists in two shards after a
    # shard-count change. Both shapes are read straight off the documented pipeline row.
    attempts_total = sum(int(row.get("attempts_total") or 0) for row in ordered)
    handoffs_total = sum(
        1
        for row in ordered
        for hop in (row.get("handoffs") or ())
        # The ledger is nested whole (it outlives the run that wrote it), so honour the report's scope
        # the same way `store.stats(run_id)` does instead of counting every historical jump.
        if run_id is None or hop.get("run_id") == run_id
    )
    return MergedReport(
        rows=ordered,
        sources=paths,
        run_ids=sorted(set(run_ids)),
        duplicates=duplicates,
        source_events_total=source_events_total,
        attempts_total=attempts_total,
        handoffs_total=handoffs_total,
        repair_failures=len(repair_failed_pids),
    )
