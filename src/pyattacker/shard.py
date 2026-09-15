"""Sharding: run one dataset across N independent processes.

Why shards instead of threads: SQLite allows exactly one writer per database, and the kernel
is deliberately single-event-loop. So the unit of horizontal scaling is a **process with its
own store**, not a bigger pool of coroutines.

The contract that makes this work:

* shard assignment is a pure function of the pipeline key (content-addressed), so the same
  dataset and template always split the same way — rerunning with ``--resume`` lands every
  pipeline back in the shard that owns it;
* shards never talk to each other, so no locks, no coordination, no failure coupling;
* results are joined afterwards by :mod:`pyattacker.merge`, which de-duplicates by
  ``pipeline_id`` (changing the shard count moves pipelines between files; merging must not
  double-count them).
"""

from __future__ import annotations

import os
from typing import Any, Iterable, Iterator, TypeVar

from .artifact import digest_of
from .errors import ConfigError

__all__ = [
    "parse_shard",
    "shard_index",
    "in_shard",
    "shard_specs",
    "shard_store_path",
    "describe_shard",
]

T = TypeVar("T")


def parse_shard(text: str | tuple[int, int] | None) -> tuple[int, int] | None:
    """Parse ``"1/4"`` (or pass through a ``(1, 4)`` tuple). Returns ``None`` for no sharding."""
    if text is None:
        return None
    if isinstance(text, tuple):
        index, count = text
    else:
        raw = str(text).strip()
        if "/" not in raw:
            raise ConfigError(f"shard must look like 'index/count', got {text!r}")
        head, _, tail = raw.partition("/")
        try:
            index, count = int(head), int(tail)
        except ValueError as exc:
            raise ConfigError(f"shard must look like 'index/count', got {text!r}") from exc
    if count < 1:
        raise ConfigError(f"shard count must be >= 1, got {count}")
    if not (0 <= index < count):
        raise ConfigError(f"shard index must be in [0, {count}), got {index}")
    return index, count


def shard_index(key: str, count: int) -> int:
    """Stable shard for a pipeline key.

    Hashes the key first so that both content-addressed ids and user-supplied ``key_of``
    strings spread uniformly.
    """
    if count < 1:
        raise ConfigError(f"shard count must be >= 1, got {count}")
    return int(digest_of(key)[:16], 16) % count


def in_shard(key: str, index: int, count: int) -> bool:
    return shard_index(key, count) == index


def shard_specs(specs: Iterable[T], index: int, count: int, *, key: str = "pipeline_id") -> Iterator[T]:
    """Filter a pipeline stream down to this shard. ``index``/``count`` come from :func:`parse_shard`."""
    if count <= 1:
        yield from specs
        return
    for spec in specs:
        if in_shard(getattr(spec, key), index, count):
            yield spec


def shard_store_path(base: str, index: int, count: int) -> str:
    """``runs/qa.db`` + ``1/4`` → ``runs/qa.shard1of4.db`` (one writer per file)."""
    if count <= 1:
        return base
    if base in (":memory:", "memory"):
        return base
    root, ext = os.path.splitext(base)
    return f"{root}.shard{index}of{count}{ext or '.db'}"


def describe_shard(index: int, count: int) -> str:
    return f"{index}/{count}"


def shard_paths(base: str, count: int) -> list[str]:
    """Every shard's store path, in index order (used by `run --shards N` and merged reports)."""
    return [shard_store_path(base, index, count) for index in range(count)]


def shard_env(index: int, count: int) -> dict[str, Any]:
    """Environment hints for child processes so a task can record its own provenance."""
    return {"PYATACKER_SHARD": describe_shard(index, count)}
