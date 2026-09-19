"""Monitoring operational state and values explicitly reported by applications.

* :func:`render_snapshot` renders Runner.stats() or store.stats() into a text panel.
* :func:`watch` periodically reads the store (read-only connection), so it **works across processes** ——
  the process running tasks writes to the store, and any terminal can open `pyattacker watch runs.db` to see live state.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from typing import Any

from .reported_metrics import read_reported_metrics

__all__ = ["render_snapshot", "watch", "read_snapshot"]

_BAR = "█"
_EMPTY = "·"


def _bar(value: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return _EMPTY * width
    filled = max(0, min(width, round(width * value / total)))
    return _BAR * filled + _EMPTY * (width - filled)


def render_snapshot(snapshot: Mapping[str, Any], *, width: int = 20) -> str:
    pipes = snapshot.get("pipelines", {})
    by_state = pipes.get("by_state", {})
    total = pipes.get("total", 0)
    status = snapshot.get("run_status")
    tail = "  [stopping]" if snapshot.get("stopping") else (f"  [status={status}]" if status else "")
    lines = [
        f"run={snapshot.get('run_id')}  elapsed={snapshot.get('elapsed_s')}s"
        f"  in-flight={snapshot.get('in_flight_pipelines', 0)}" + tail,
        f"pipelines {_bar(total - by_state.get('running', 0) - by_state.get('pending', 0), total, width)} "
        f"{total}  " + " ".join(f"{k}={v}" for k, v in sorted(by_state.items())),
    ]
    durations = pipes.get("duration_ms", {})
    if any(durations.values()):
        lines.append(
            f"latency ms  p50={durations.get('p50')} p95={durations.get('p95')} max={durations.get('max')}"
        )
    tasks = snapshot.get("tasks", {}).get("by_name", {})
    if tasks:
        lines.append("tasks       " + " ".join(f"{k}={v}" for k, v in sorted(tasks.items())))
    metrics = snapshot.get("reported_metrics", [])
    if metrics:
        lines.append("experiment  " + " ".join(
            f"{row['label'] or row['name']}="
            + (f"{row['value']:.1%}" if row['display'] == 'percent' else str(row['value']))
            for row in metrics
        ))
    pools = snapshot.get("pools", {})
    for name, stats in pools.items():
        if not isinstance(stats, dict):
            continue
        lines.append(
            f"pool {name:<10} {_bar(stats.get('active', 0), max(1, stats.get('capacity', 1)), width)} "
            f"active={stats.get('active')}/{stats.get('capacity')} ready={stats.get('ready')} "
            f"degraded={stats.get('degraded')} dead={stats.get('dead')} waiting={stats.get('waiting')}"
        )
    if snapshot.get("attempts_total") is not None:
        lines.append(
            f"attempts    total={snapshot.get('attempts_total')} events={snapshot.get('events_total')}"
            + (f" handoffs={snapshot['handoffs_total']}" if snapshot.get("handoffs_total") else "")
        )
    leaked = snapshot.get("leases_leaked") or snapshot.get("counters", {}).get("leases_leaked")
    if leaked:
        lines.append(f"!! leaked leases: {leaked} (forcibly reclaimed, but it means a task did not return its resource)")
    errors = snapshot.get("recent_errors") or []
    for item in errors[:3]:
        lines.append(
            f"  ! {item.get('name')}/{item.get('failed_task')}: {item.get('error_type')}: "
            f"{str(item.get('error_message'))[:80]}"
        )
    return "\n".join(lines)


def read_snapshot(store: Any, run_id: str | None = None, *, errors: int = 3) -> dict[str, Any]:
    """Read a snapshot from any store (including a read-only connection)."""
    data = store.stats(run_id)
    snapshot = dict(data)
    snapshot["run_id"] = run_id
    snapshot["recent_errors"] = store.errors(run_id=run_id, limit=errors)
    snapshot["reported_metrics"] = [
        {"name": row.name, "value": row.value, "label": row.label, "display": row.display}
        for row in read_reported_metrics(store, run_id=run_id)
    ]
    run = store.get_run(run_id) if run_id else None
    if run is not None:
        snapshot["elapsed_s"] = round((run.ended_at or time.time()) - run.started_at, 2)
        snapshot["run_status"] = run.status
    return snapshot


async def watch(
    store: Any,
    *,
    run_id: str | None = None,
    interval: float = 1.0,
    printer: Callable[[str], None] = print,
    clear: bool = True,
    iterations: int | None = None,
    stop: Callable[[], bool] | None = None,
) -> None:
    """Refresh the panel periodically. With ``clear=True`` it repaints using ANSI clear-screen."""
    count = 0
    while True:
        if stop is not None and stop():
            return
        snapshot = read_snapshot(store, run_id)
        text = render_snapshot(snapshot)
        if clear:
            printer("\033[2J\033[H" + text)
        else:
            printer(text)
        count += 1
        if iterations is not None and count >= iterations:
            return
        await asyncio.sleep(interval)
