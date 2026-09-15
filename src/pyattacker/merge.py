"""Merge: one coherent answer out of N shard databases.

Sharding means several processes each wrote their own store. Joining them cannot be a matter of
concatenating rows, because:

* the same ``pipeline_id`` can exist in more than one shard — you changed the shard count, or
  reran with ``--resume`` after moving files. Counting it twice would inflate every rate in the
  report, so rows are de-duplicated by ``pipeline_id``;
* the copies are not equivalent: one shard may hold a ``succeeded`` row while another holds the
  ``failed`` one from an earlier attempt. The rule is: **prefer the best state, break ties by the
  latest finish time**, and report how many duplicates were folded so the number is never hidden.

Statistics are recomputed from the merged rows rather than summed per store, for the same reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .export import write_rows
from .store.base import Store

__all__ = ["MergedReport", "merge_reports", "STATE_RANK"]

# How good a pipeline state is; used to pick a winner when the same pipeline exists twice.
STATE_RANK = {"succeeded": 4, "failed": 3, "interrupted": 2, "pending": 1, "canceled": 0}


@dataclass
class MergedReport:
    rows: list[dict[str, Any]]
    sources: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)
    duplicates: int = 0
    events_total: int = 0
    attempts_total: int = 0

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
            "events_total": self.events_total,
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
            + f"  attempts={stats['attempts_total']} events={stats['events_total']}",
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
        from .store.base import open_store as _open

        open_store = _open
    rows_by_id: dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    events_total = 0
    attempts_total = 0
    paths: list[str] = []
    run_ids: list[str] = []
    for source in sources:
        store = source if hasattr(source, "export_rows") else open_store(str(source))
        path = getattr(store, "path", None) or getattr(getattr(store, "inner", None), "path", None)
        paths.append(str(path or source))
        try:
            # Counts come from the aggregate query, not from materializing the log.
            counts = store.stats(run_id)
            events_total += int(counts.get("events_total") or 0)
            attempts_total += int(counts.get("attempts_total") or 0)
            for row in store.export_rows(run_id=run_id):
                key = row["pipeline_id"]
                run_ids.append(row["run_id"])
                if key in rows_by_id:
                    duplicates += 1
                    rows_by_id[key] = _winner(rows_by_id[key], row)
                else:
                    rows_by_id[key] = row
        finally:
            if not hasattr(source, "export_rows"):
                store.close()

    ordered = sorted(rows_by_id.values(), key=lambda r: (r.get("name") or "", r.get("started_at") or 0.0))
    return MergedReport(
        rows=ordered,
        sources=paths,
        run_ids=sorted(set(run_ids)),
        duplicates=duplicates,
        events_total=events_total,
        attempts_total=attempts_total,
    )
