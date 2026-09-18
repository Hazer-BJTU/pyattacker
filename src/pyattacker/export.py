"""Export: turn a store into analysis-ready rows, in more than one shape.

The kernel records facts; this module decides how to lay them out for whoever consumes them:

* ``pipelines`` (default) — one row per pipeline with its tasks, artifacts and handoffs nested;
* ``tasks`` / ``attempts`` — one row per unit, so retries and error classes are directly
  groupable (this is what you want in pandas);
* ``events`` — the structured log;
* ``artifacts`` — the persisted state of each step.

Every kind is exported in full unless an explicit ``limit`` says otherwise, and rows stream out of
the store in bounded batches — see :func:`iter_rows` for the order and limit rule.

Formats: ``jsonl`` (default, streaming), ``json`` (a single array) and ``csv`` (flat; nested
values become compact JSON strings). CSV needs a stable header, so the header is taken from the
first ``header_rows`` rows and anything introduced later is folded into an ``extra`` column —
bounded memory, no surprises.
"""

from __future__ import annotations

import csv
import dataclasses
import json
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from itertools import islice
from typing import Any

from .errors import ConfigError
from .store.base import (
    Store,
    iter_artifacts,
    iter_attempts,
    iter_events,
    iter_pipelines,
    iter_tasks,
)

__all__ = ["ROW_KINDS", "FORMATS", "iter_rows", "flatten", "write_rows", "export_store"]

ROW_KINDS = ("pipelines", "tasks", "attempts", "events", "artifacts")
FORMATS = ("jsonl", "json", "csv")


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _jsonable(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _task_row(record: Any) -> dict[str, Any]:
    return {
        "task_run_id": record.task_run_id,
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "name": record.name,
        "seq": record.seq,
        "visit": getattr(record, "visit", 0),
        "state": record.state,
        "attempts_used": record.attempts_used,
        "started_at": record.started_at,
        "ended_at": record.ended_at,
        "duration_ms": record.duration_ms,
        "input_artifact_id": record.input_artifact_id,
        "output_artifact_id": record.output_artifact_id,
        "error_class": record.error_class,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "leases": _jsonable(record.leases),
        "metrics": _jsonable(record.metrics),
    }


def _attempt_row(record: Any) -> dict[str, Any]:
    return {
        "attempt_id": record.attempt_id,
        "task_run_id": record.task_run_id,
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "task_name": record.task_name,
        "seq": record.seq,
        "visit": getattr(record, "visit", 0),
        "attempt_no": record.attempt_no,
        "outcome": record.outcome,
        "started_at": record.started_at,
        "ended_at": record.ended_at,
        "duration_ms": record.duration_ms,
        "error_class": record.error_class,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "retry_delay_s": record.retry_delay_s,
        # the retry decision is the part you actually want to group by when tuning a policy
        "decision": _jsonable(record.decision),
        "leases": _jsonable(record.leases),
    }


def _event_row(record: Any) -> dict[str, Any]:
    return {
        "event_id": record.event_id,
        "ts": record.ts,
        "scope": record.scope,
        "kind": record.kind,
        "run_id": record.run_id,
        "pipeline_id": record.pipeline_id,
        "task_run_id": record.task_run_id,
        "pool": record.pool,
        "resource_id": record.resource_id,
        "data": _jsonable(record.data),
    }


def _artifact_row(record: Any) -> dict[str, Any]:
    return {
        "artifact_id": record.id,
        "pipeline_id": record.pipeline_id,
        "task_name": record.task_name,
        "seq": record.seq,
        "visit": getattr(record, "visit", 0),
        "type_name": record.type_name,
        "codec": record.codec,
        "digest": record.digest,
        "size": record.size,
        "is_final": record.is_final,
        "created_at": record.created_at,
        "available": record.available,
        "payload": _decode(record),
    }


def _decode(artifact: Any) -> Any:
    if artifact.payload is None:
        return None
    if artifact.codec in ("json", "history-v1"):
        try:
            return json.loads(artifact.payload.decode("utf-8"))
        except Exception:  # pragma: no cover - defensive
            import base64

            return base64.b64encode(artifact.payload).decode("ascii")
    import base64

    return base64.b64encode(artifact.payload).decode("ascii")


def iter_rows(
    store: Store, *, kind: str = "pipelines", run_id: str | None = None, limit: int | None = None
) -> Iterator[dict[str, Any]]:
    """Yield export rows of one kind. ``run_id=None`` means "everything in this store".

    Order — the front of it is what ``limit`` truncates, and every kind ends its order in a key that
    is unique, so a paged read can neither drop nor duplicate a row:

    * ``pipelines`` — ``created_at``, then ``pipeline_id`` (the primary key);
    * ``tasks`` — ``pipeline_id``, then ``seq``, then ``task_run_id``;
    * ``attempts`` — ``attempt_id`` (insertion order, oldest first);
    * ``events`` — ``event_id`` (insertion order, oldest first);
    * ``artifacts`` — pipeline order (as above), then ``seq``, then ``artifact_id``.

    ``tasks`` and ``artifacts`` need that last component: the tables are keyed by ``task_run_id`` /
    ``artifact_id``, so ``(pipeline_id, seq)`` and ``seq`` are not unique by contract and a cursor
    over them alone would skip rows that tie across a page boundary.

    ``limit`` counts rows of the requested kind — including ``artifacts``, where it used to count
    pipelines — and means the same thing for every kind: ``None`` (the default) exports the complete
    history, ``0`` exports nothing, a positive N exports the first N rows in the order above, and a
    negative value raises :class:`~pyattacker.errors.ConfigError`.

    Live stores: ``events`` and ``attempts`` are bounded by the high-water mark of their monotonic
    keys, taken when the export starts, so rows appended while it runs are not exported and the
    iterator cannot chase a moving tail. ``pipelines``, ``tasks`` and ``artifacts`` have no
    monotonic key, so they are a **best-effort traversal** of the live store: a row inserted ahead
    of the cursor can appear, one inserted behind it cannot. Nothing here is a long-lived read
    transaction or a point-in-time snapshot of the whole store.

    Rows stream out of the store: no kind is materialized whole. With ``kind="pipelines"`` a single
    row nests that pipeline's tasks and artifacts, so one pipeline is the memory unit; the other
    kinds are read in bounded batches where the store implements the optional paged-iteration
    extension (``SqliteStore`` does, see :class:`~pyattacker.store.base.PagedStore`).
    """
    if kind not in ROW_KINDS:
        raise ConfigError(f"unknown row kind {kind!r}; available: {list(ROW_KINDS)}")
    if limit is not None and limit < 0:
        raise ConfigError(f"limit must be >= 0, got {limit}")

    rows: Iterator[dict[str, Any]]
    if kind == "pipelines":
        rows = store.export_rows(run_id=run_id)
    elif kind == "tasks":
        rows = (_task_row(record) for record in iter_tasks(store, run_id=run_id))
    elif kind == "attempts":
        rows = (_attempt_row(record) for record in iter_attempts(store, run_id=run_id))
    elif kind == "events":
        rows = (_event_row(record) for record in iter_events(store, run_id=run_id))
    else:
        rows = _artifact_rows(store, run_id=run_id)
    yield from rows if limit is None else islice(rows, limit)


def _artifact_rows(store: Store, *, run_id: str | None = None) -> Iterator[dict[str, Any]]:
    """Artifacts across pipelines: page the pipelines, then stream each one's artifacts.

    The old shape collected every pipeline id up front; paging the pipelines instead keeps the
    memory unit at "one pipeline's artifacts" and can neither duplicate nor drop a row, because
    :func:`~pyattacker.store.base.iter_pipelines` orders by ``(created_at, pipeline_id)``.
    """
    for pipeline in iter_pipelines(store, run_id=run_id):
        for artifact in iter_artifacts(store, pipeline_id=pipeline.pipeline_id):
            yield _artifact_row(artifact)


def flatten(value: Any) -> Any:
    """Make a value CSV-safe: nested structures become compact JSON, ``None`` becomes empty."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def write_rows(
    rows: Iterable[Mapping[str, Any]],
    path: str | os.PathLike[str] | None,
    *,
    fmt: str = "jsonl",
    header_rows: int = 1000,
    title: str | None = None,
) -> int:
    """Write rows to ``path`` (or return them via stdout when ``path`` is ``None``).

    Returns the number of rows written. ``json`` needs the row count up front for a valid array?
    No — it streams with separators, which keeps memory flat for large exports.
    """
    fmt = fmt.lower()
    if fmt not in FORMATS:
        raise ConfigError(f"unknown format {fmt!r}; available: {list(FORMATS)}")
    # Not a context manager on purpose: the same handle is written from three branches below and
    # closed once in `finally`.
    handle = None
    close = False
    if path is not None:
        text = str(path)
        os.makedirs(os.path.dirname(os.path.abspath(text)) or ".", exist_ok=True)
        handle = open(text, "w", encoding="utf-8", newline="")  # noqa: SIM115 (closed in `finally`)
        close = True
    else:  # pragma: no cover - the CLI always passes a path
        import sys

        handle = sys.stdout
    count = 0
    try:
        if fmt == "jsonl":
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                count += 1
            return count

        if fmt == "json":
            handle.write(f'{{"title": {json.dumps(title or "", ensure_ascii=False)}, "rows": [')
            first = True
            for row in rows:
                if not first:
                    handle.write(",\n")
                handle.write(json.dumps(row, ensure_ascii=False, default=str))
                first = False
                count += 1
            handle.write("]}\n")
            return count

        # csv: build the header from a bounded prefix, fold late keys into `extra`
        header: list[str] = []
        seen: set[str] = set()
        buffered: list[Mapping[str, Any]] = []
        iterator = iter(rows)
        for row in iterator:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    header.append(key)
            buffered.append(row)
            if len(buffered) >= max(1, header_rows):
                break
        writer = csv.DictWriter(handle, fieldnames=[*header, "extra"], extrasaction="ignore")
        writer.writeheader()
        for row in buffered:
            writer.writerow(_csv_row(row, seen))
            count += 1
        for row in iterator:
            writer.writerow(_csv_row(row, seen))
            count += 1
        return count
    finally:
        if close:
            handle.close()


def _csv_row(row: Mapping[str, Any], known: set[str]) -> dict[str, Any]:
    out = {key: flatten(value) for key, value in row.items() if key in known}
    extra = {key: value for key, value in row.items() if key not in known}
    out["extra"] = json.dumps(extra, ensure_ascii=False, default=str) if extra else ""
    return out


def export_store(
    store: Store,
    path: str | None,
    *,
    kind: str = "pipelines",
    fmt: str = "jsonl",
    run_id: str | None = None,
    limit: int | None = None,
) -> int:
    rows = iter_rows(store, kind=kind, run_id=run_id, limit=limit)
    return write_rows(rows, path, fmt=fmt)


def export_stores(
    stores: Sequence[Store],
    path: str | None,
    *,
    kind: str = "pipelines",
    fmt: str = "jsonl",
    run_id: str | None = None,
    limit: int | None = None,
) -> int:
    """Export several stores into one file (concatenated; see :mod:`pyattacker.merge` to de-duplicate).

    ``limit`` is applied per store, so each store contributes at most its first N rows of ``kind``.
    """

    def _chain() -> Iterator[dict[str, Any]]:
        for store in stores:
            yield from iter_rows(store, kind=kind, run_id=run_id, limit=limit)

    return write_rows(_chain(), path, fmt=fmt)
