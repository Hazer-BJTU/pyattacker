"""Test helpers: a fake clock, pool construction, and running coroutines synchronously."""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Sequence

from pyattacker import Pool, Resource


class FakeClock:
    """A clock whose time is controllable and whose sleep returns instantly.

    Swap it in for the Runner/Pool clock and the backoff, circuit-break and rate-limit
    tests become both deterministic and very fast.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += max(0.0, seconds)
        await asyncio.sleep(0)  # yield control, preserving real concurrency semantics


def make_pool(
    name: str = "apis",
    *,
    count: int = 1,
    capacity: int = 1,
    kind: str = "llm",
    algorithm: Any = "wait",
    **resource_kwargs: Any,
) -> Pool:
    resources = [
        Resource.create(kind, id=f"{name}-{i}", capacity=capacity, **resource_kwargs)
        for i in range(1, count + 1)
    ]
    return Pool(name, resources, algorithm=algorithm)


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Tests all use asyncio.run uniformly, to avoid pulling in pytest-asyncio."""
    return asyncio.run(coro)
