"""Algorithm —— the strategy for "how to get a resource out of a pool".

How it differs from retry (two orthogonal axes; do not conflate them):

* **algorithm**: before doing the work, how to wait for/select an available
  resource (wait, back off, switch pools, pick the least-busy one).
* **retry**: after the work fails, whether/when to run it again (exception-driven).

The single exit for every algorithm is :meth:`Pool.acquire`; they only compose
three primitives: ``pool.try_acquire()`` (synchronous attempt),
``pool.wait_slot()`` (wait for a broadcast), and ``clock.sleep()`` (backoff).
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .errors import AcquireTimeout, PoolNotFound, ResourceUnavailable
from .resource import Lease, Pool

__all__ = [
    "AcquireAlgorithm",
    "Immediate",
    "Wait",
    "Backoff",
    "LeastBusy",
    "Failover",
    "Sticky",
    "QuotaAware",
    "resolve_algorithm",
    "ALGORITHMS",
]


@runtime_checkable
class AcquireAlgorithm(Protocol):
    name: str

    async def acquire(
        self,
        pool: Pool,
        *,
        ctx: Any = None,
        where: Callable[[Any], bool] | None = None,
        timeout: float | None = None,
        selector: dict[str, Any] | None = None,
    ) -> Lease: ...


def _describe(pool: Pool, selector: dict[str, Any] | None, where: Any) -> str:
    stats = pool.stats(**{k: v for k, v in (selector or {}).items() if k in ("kind", "id")})
    return (
        f"pool={pool.name!r} selector={selector or {}} where={'yes' if where else 'no'} "
        f"resources={stats.total} ready={stats.ready} degraded={stats.degraded} "
        f"dead={stats.dead} active={stats.active}/{stats.capacity} waiting={stats.waiting}"
    )


@dataclass
class Immediate:
    """Fail immediately when nothing is available (fast-fail on capacity shortage)."""

    name: str = "immediate"

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        lease = pool.try_acquire(where=where, ctx=ctx, **(selector or {}))
        if lease is None:
            raise ResourceUnavailable(f"no resource available: {_describe(pool, selector, where)}")
        return lease


@dataclass
class Wait:
    """FIFO wait (the default algorithm), optionally with a timeout."""

    name: str = "wait"
    timeout: float | None = None

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        deadline_timeout = timeout if timeout is not None else self.timeout
        started = pool.clock.now()
        while True:
            lease = pool.try_acquire(where=where, ctx=ctx, **(selector or {}))
            if lease is not None:
                # Backfill the real waiting time: try_acquire only ever sees 0, and time spent
                # blocked here is exactly what pool stats should report.
                pool.note_wait(lease, (pool.clock.now() - started) * 1000.0, selector=selector)
                return lease
            remaining = None
            if deadline_timeout is not None:
                remaining = deadline_timeout - (pool.clock.now() - started)
                if remaining <= 0:
                    raise AcquireTimeout(f"timed out waiting for a resource ({deadline_timeout}s): {_describe(pool, selector, where)}")
            got = await pool.wait_slot(remaining, ctx=ctx, where=where, selector=selector)
            if not got and deadline_timeout is not None:
                raise AcquireTimeout(f"timed out waiting for a resource ({deadline_timeout}s): {_describe(pool, selector, where)}")


@dataclass
class Backoff:
    """Backoff-style acquire: when the pool is saturated / a resource is in circuit-break cooldown, retry after exponential backoff.

    This is the default remedy for the "API is saturated" scenario.
    """

    name: str = "backoff"
    base: float = 0.2
    factor: float = 2.0
    cap: float = 10.0
    jitter: str = "full"
    max_wait: float | None = None

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        limit = timeout if timeout is not None else self.max_wait
        started = pool.clock.now()
        rng = getattr(ctx, "rng", None) or random.Random()
        attempt = 0
        while True:
            lease = pool.try_acquire(where=where, ctx=ctx, **(selector or {}))
            if lease is not None:
                pool.note_wait(lease, (pool.clock.now() - started) * 1000.0, selector=selector)
                return lease
            attempt += 1
            raw = min(self.cap, self.base * self.factor ** (attempt - 1))
            if self.jitter == "full":
                delay = rng.uniform(0, raw)
            elif self.jitter == "equal":
                delay = raw / 2 + rng.uniform(0, raw / 2)
            else:
                delay = raw
            elapsed = pool.clock.now() - started
            if limit is not None and elapsed + delay >= limit:
                raise AcquireTimeout(f"timed out waiting for a resource with backoff ({limit}s): {_describe(pool, selector, where)}")
            if ctx is not None:
                ctx.emit(
                    "acquire.backoff",
                    pool=pool.name,
                    attempt=attempt,
                    delay_s=round(delay, 4),
                    selector=dict(selector or {}),
                )
            await pool.clock.sleep(delay)


@dataclass
class LeastBusy:
    """Pick the resource with the lowest load ratio; if none is available, fall back to ``fallback`` (Wait by default)."""

    name: str = "least_busy"
    fallback: AcquireAlgorithm | None = None

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        def pick(resource: Any) -> bool:
            return where is None or bool(where(resource))

        lease = pool.try_acquire(where=pick, ctx=ctx, **(selector or {}))
        if lease is not None:
            return lease
        fallback = self.fallback or Wait()
        return await fallback.acquire(pool, ctx=ctx, where=where, timeout=timeout, selector=selector)


@dataclass
class Failover:
    """Fail over across multiple pools in order; when all are unavailable, defer to ``fallback``."""

    name: str = "failover"
    pools: Sequence[str] = ()
    fallback: AcquireAlgorithm | None = None

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        names = list(self.pools) or [pool.name]
        available = getattr(ctx, "pools", {}) or {}
        reasons: dict[str, str] = {}
        last_error: Exception | None = None
        for name in names:
            target = available.get(name)
            if target is None:
                reasons[name] = "unknown resource pool"
                last_error = last_error or PoolNotFound(f"unknown resource pool: {name}")
                continue
            try:
                return await Immediate().acquire(
                    target, ctx=ctx, where=where, timeout=timeout, selector=selector
                )
            except ResourceUnavailable as exc:
                reasons[name] = str(exc).split(":")[0]
                last_error = exc
        fallback = self.fallback or Wait()
        try:
            target = available.get(names[0], pool)
            return await fallback.acquire(
                target, ctx=ctx, where=where, timeout=timeout, selector=selector
            )
        except (ResourceUnavailable, AcquireTimeout) as exc:
            # Report every pool's reason: hiding them behind the last error makes failover
            # misconfiguration (a typo'd pool name) look like plain capacity shortage.
            reasons[names[0]] = f"{reasons.get(names[0], 'no resource available')} (fallback: {exc})"
            detail = "; ".join(f"{name}: {reason}" for name, reason in reasons.items())
            error = ResourceUnavailable(f"no pool available: {names} -> {detail}")
            raise error from (last_error or exc)


def _affinity(ctx: Any, pool_name: str) -> str | None:
    if ctx is None:
        return None
    return (ctx.meta.get("sticky") or {}).get(pool_name)


def _remember_affinity(ctx: Any, pool_name: str, resource_id: str) -> None:
    if ctx is None:
        return
    ctx.meta.setdefault("sticky", {})[pool_name] = resource_id


@dataclass
class Sticky:
    """Prefer the resource this pipeline already used.

    Useful when the provider gives you something for free by staying on one endpoint:
    prompt/prefix caches, warm connections, sticky sessions. The affinity lives on the task
    context, so it survives retries and later tasks of the same pipeline but never leaks
    between pipelines.
    """

    name: str = "sticky"
    fallback: AcquireAlgorithm | None = None

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        preferred = _affinity(ctx, pool.name)
        if preferred is not None:
            lease = pool.select(
                None, where=lambda r, _pref=preferred: r.id == _pref, ctx=ctx, **(selector or {})
            )
            if lease is not None:
                return lease
        inner = self.fallback or Wait()
        lease = await inner.acquire(pool, ctx=ctx, where=where, timeout=timeout, selector=selector)
        _remember_affinity(ctx, pool.name, lease.resource.id)
        return lease


@dataclass
class QuotaAware:
    """Pick the resource with the most remaining quota.

    Quota is declared on the resource (``options={"quota": {"tokens": 1_000_000}}``) and
    consumed through ``lease.report(usage={"tokens": n})``. This is a *preference*, not a hard
    limit: when every candidate is exhausted the best of them is still used, because refusing
    to work is worse than overspending. For a hard stop, have the task raise once its own
    budget is gone.
    """

    name: str = "quota_aware"
    metric: str = "tokens"
    reserve: float = 0.05
    fallback: AcquireAlgorithm | None = None

    def score(self, resource: Any, stats: Any) -> tuple[float, float]:
        """Rank by *remaining ratio*, then by *absolute remaining* as the tie-break.

        Ratio first so a nearly-exhausted resource is avoided even when its quota is huge;
        absolute second so that between two equally-fresh resources the roomier one wins.
        Unknown quota ranks below anything metered, and anything inside the reserve ranks
        below everything else while staying rankable.
        """
        quota = (resource.options.get("quota") or {}).get(self.metric)
        if not quota:
            return (-100.0, 0.0)  # unknown quota ranks below anything with a known one
        quota_f = float(quota)
        remaining = quota_f - float(stats.usage.get(self.metric, 0.0))
        ratio = remaining / quota_f
        if ratio <= self.reserve:
            ratio -= 10.0  # inside the reserve: usable, but only as a last resort
        return (ratio, remaining)

    async def acquire(self, pool, *, ctx=None, where=None, timeout=None, selector=None) -> Lease:
        lease = pool.select(self.score, where=where, ctx=ctx, **(selector or {}))
        if lease is not None:
            return lease
        inner = self.fallback or Wait()
        return await inner.acquire(pool, ctx=ctx, where=where, timeout=timeout, selector=selector)


ALGORITHMS: dict[str, type] = {
    "immediate": Immediate,
    "wait": Wait,
    "backoff": Backoff,
    "least_busy": LeastBusy,
    "failover": Failover,
    "sticky": Sticky,
    "quota_aware": QuotaAware,
}


def resolve_algorithm(spec: Any) -> AcquireAlgorithm:
    """Normalize ``"backoff"`` / ``{"name": "backoff", "base": 1}`` / an instance into an algorithm object."""
    if spec is None:
        return Wait()
    if isinstance(spec, str):
        if spec in ALGORITHMS:
            return ALGORITHMS[spec]()
        from .plugins import PLUGINS

        plugin = PLUGINS.algorithm(spec)
        if plugin is not None:
            return plugin
        raise PoolNotFound(
            f"unknown acquire algorithm: {spec!r}, choices: {sorted(ALGORITHMS)} "
            f"+ installed plugins {PLUGINS.names('algorithms')}"
        )
    if isinstance(spec, dict):
        params = dict(spec)
        name = params.pop("name", "wait")
        cls = ALGORITHMS.get(name)
        if cls is None:
            from .plugins import PLUGINS

            plugin = PLUGINS.algorithm(name)
            if plugin is not None:
                if params:
                    raise PoolNotFound(
                        f"algorithm plugin {name!r} does not take parameters: {sorted(params)}"
                    )
                return plugin
            raise PoolNotFound(f"unknown acquire algorithm: {name!r}, choices: {sorted(ALGORITHMS)}")
        if "fallback" in params and isinstance(params["fallback"], (str, dict)):
            params["fallback"] = resolve_algorithm(params["fallback"])
        return cls(**params)
    if isinstance(spec, AcquireAlgorithm):
        return spec
    raise PoolNotFound(f"cannot resolve acquire algorithm: {spec!r}")
