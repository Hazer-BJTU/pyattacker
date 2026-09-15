"""Export: turn a store into analysis-ready rows, in more than one shape.

The kernel records facts; this module decides how to lay them out for whoever consumes them:

* ``pipelines`` (default) — one row per pipeline with its tasks and artifacts nested;
* ``tasks`` / ``attempts`` — one row per unit, so retries and error classes are directly
  groupable (this is what you want in pandas);
* ``events`` — the structured log;
* ``artifacts`` — the persisted state of each step.

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
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .errors import ConfigError
from .store.base import Store

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
        "pipeline_id": record.pipeline_id,
        "run_id": record.run_id,
        "task_name": record.task_name,
        "seq": record.seq,
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
    if artifact.codec == "json":
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
    """Yield export rows of one kind. ``run_id=None`` means "everything in this store"."""
    if kind not in ROW_KINDS:
        raise ConfigError(f"unknown row kind {kind!r}; available: {list(ROW_KINDS)}")
    if kind == "pipelines":
        yield from store.export_rows(run_id=run_id)
        return
    if kind == "tasks":
        records: Iterable[Any] = store.tasks(run_id=run_id, limit=limit)
        mapper = _task_row
    elif kind == "attempts":
        records = store.attempts(run_id=run_id, limit=limit)
        mapper = _attempt_row
    elif kind == "events":
        records = store.events(run_id=run_id, limit=limit or 100000)
        mapper = _event_row
    else:
        if run_id is None:
            pipeline_ids = [p.pipeline_id for p in store.pipelines(limit=limit)]
        else:
            pipeline_ids = [p.pipeline_id for p in store.pipelines(run_id=run_id, limit=limit)]
        for pipeline_id in pipeline_ids:
            for artifact in store.artifacts(pipeline_id):
                yield _artifact_row(artifact)
        return
    for record in records:
        yield mapper(record)


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
    handle = None
    close = False
    if path is not None:
        text = str(path)
        os.makedirs(os.path.dirname(os.path.abspath(text)) or ".", exist_ok=True)
        handle = open(text, "w", encoding="utf-8", newline="")
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
            handle.write('{"title": %s, "rows": [' % json.dumps(title or "", ensure_ascii=False))
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
        writer = csv.DictWriter(handle, fieldnames=header + ["extra"], extrasaction="ignore")
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
    """Export several stores into one file (concatenated; see :mod:`pyattacker.merge` to de-duplicate)."""

    def _chain() -> Iterator[dict[str, Any]]:
        for store in stores:
            yield from iter_rows(store, kind=kind, run_id=run_id, limit=limit)

    return write_rows(_chain(), path, fmt=fmt)
