"""An acquisition algorithm published under the ``pyattacker.algorithms`` entry-point group."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyattacker import Lease, Wait


@dataclass
class LeastLoaded:
    """Pick the candidate with the fewest leases *and* the lowest reported usage.

    A deliberately simple example of the one method the framework needs: ``name`` plus
    ``acquire(pool, ctx=..., where=..., timeout=..., selector=...) -> Lease``. Anything the pool
    can rank, this can rank too — see ``Pool.select``.
    """

    name: str = "least_loaded"
    fallback: Any = field(default=None)

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        def score(resource: Any, stats: Any) -> float:
            busy = stats.active / max(1, resource.capacity)
            used = sum(float(v) for v in stats.usage.values() if isinstance(v, (int, float)))
            return -(busy + used / 1_000_000.0)  # "highest score wins"

        lease = pool.select(score, where=where, ctx=ctx, **(selector or {}))
        if lease is not None:
            return lease
        return await (self.fallback or Wait()).acquire(
            pool, ctx=ctx, where=where, timeout=timeout, selector=selector
        )
