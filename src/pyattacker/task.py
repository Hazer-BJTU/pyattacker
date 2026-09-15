"""Task —— the smallest unit of scheduling: one artifact in, one artifact out.

This is also where the **lease safety contract** lives (the single most important guarantee of this framework):

* The one recommended way to acquire: ``async with ctx.acquire(...) as lease:``
  —— ``__aexit__`` returns the lease synchronously, so it always runs even if the task raises internally.
* "acquire→use→release" inside a loop is allowed and recommended: return the lease as soon as each
  iteration ends, which hands the concurrency back to others instead of letting one task hog a resource long-term.
* Leases obtained through the escape hatch ``await ctx.acquire_lease(...)`` are **tracked by ctx just the same**:
  when the task finishes (success, failure, cancellation and timeout all included) the Runner calls
  :meth:`TaskContext.reclaim_now` to force-reclaim them synchronously, and records a ``lease.leaked`` event.
* ``reclaim_now`` is a purely synchronous function, so it **cannot be interrupted by CancelledError**;
  that is the root reason "a finished task never holds a resource" holds.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import random
import typing
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from .artifact import DEFAULT_REGISTRY, CodecRegistry, digest_of
from .errors import (
    ConfigError,
    FatalError,
    RetryableError,
    error_class_of,
    is_retryable_class,
)
from .resource import Lease, Pool, Resource

__all__ = ["Retrying", "TaskSpec", "task", "TaskContext"]


@dataclass(frozen=True)
class Retrying:
    """Retry policy after a failure. Defaults to ``max_attempts=1`` —— no retries, a failure is a failure."""

    max_attempts: int = 1
    on: tuple[type[BaseException], ...] = ()
    retry_classified: bool = True
    retry_unknown: bool = False
    base: float = 0.5
    factor: float = 2.0
    cap: float = 30.0
    jitter: str = "full"  # none | full | equal
    max_total_s: float | None = None

    def should_retry(self, exc: BaseException, error_class: str | None = None) -> bool:
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit)):
            return False
        if isinstance(exc, FatalError):
            return False
        if isinstance(exc, RetryableError):
            return True
        if self.on and isinstance(exc, self.on):
            return True
        klass = error_class or error_class_of(exc)
        if self.retry_classified and is_retryable_class(klass):
            return True
        return bool(self.retry_unknown and klass == "unknown")

    def delay_for(self, attempt: int, rng: random.Random, retry_after: float | None = None) -> float:
        """Backoff duration after the given attempt fails (``attempt`` starts at 1)."""
        if retry_after is not None:
            return max(0.0, retry_after)
        raw = min(self.cap, self.base * self.factor ** max(0, attempt - 1))
        if self.jitter == "full":
            return rng.uniform(0, raw)
        if self.jitter == "equal":
            return raw / 2 + rng.uniform(0, raw / 2)
        return raw


@dataclass(frozen=True)
class TaskSpec:
    """Static description of a task. All runtime state lives in :class:`TaskContext`."""

    name: str
    fn: Callable[..., Any]
    resource: str | None = None
    algorithm: Any = None
    retry: Retrying = field(default_factory=Retrying)
    timeout_s: float | None = None
    accepts: Any = Any
    returns: Any = Any
    takes_ctx: bool = False
    module: str = ""
    qualname: str = ""
    code_digest: str = ""

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    def fingerprint(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "target": f"{self.module}:{self.qualname}",
            "code": self.code_digest,
            "resource": self.resource,
            "timeout_s": self.timeout_s,
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "base": self.retry.base,
                "factor": self.retry.factor,
                "cap": self.retry.cap,
                "jitter": self.retry.jitter,
            },
        }

    def with_overrides(self, **kwargs: Any) -> "TaskSpec":
        return replace(self, **{k: v for k, v in kwargs.items() if v is not None})

    def __or__(self, other: Any) -> Any:
        from .pipeline import Chain

        left = Chain((self,))
        return left | other

    def __call__(self, value: Any, ctx: "TaskContext") -> Any:
        return self.fn(value, ctx) if self.takes_ctx else self.fn(value)


def _code_digest(fn: Callable[..., Any]) -> str:
    try:
        return digest_of(inspect.getsource(fn))
    except (OSError, TypeError):  # REPL / dynamically defined function
        return digest_of(f"{getattr(fn, '__module__', '')}:{getattr(fn, '__qualname__', '')}")


def build_task_spec(
    fn: Callable[..., Any],
    *,
    name: str | None = None,
    resource: str | None = None,
    algorithm: Any = None,
    retry: Retrying | Mapping[str, Any] | None = None,
    timeout_s: float | None = None,
    registry: CodecRegistry | None = None,
) -> TaskSpec:
    """Wrap a plain function into a :class:`TaskSpec` (the internal implementation of ``@task``)."""
    if isinstance(fn, TaskSpec):  # double decoration / declarative override
        return fn if not any((name, resource, algorithm, retry, timeout_s)) else fn.with_overrides(
            name=name, resource=resource, algorithm=algorithm, timeout_s=timeout_s,
            retry=_as_retrying(retry) if retry is not None else None,
        )
    if not callable(fn):
        raise ConfigError(f"task must be callable, got {fn!r}")

    sig = inspect.signature(fn)
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(p.kind is p.VAR_POSITIONAL for p in sig.parameters.values())
    if len(positional) == 1 or (has_varargs and len(positional) <= 1):
        takes_ctx = False
    elif len(positional) == 2:
        takes_ctx = True
    else:
        raise ConfigError(
            f"task {getattr(fn, '__name__', fn)!r} must have the signature (artifact) or (artifact, ctx), "
            f"but has {len(positional)} positional parameters"
        )

    try:
        hints = typing.get_type_hints(fn)
    except Exception:  # don't blow up when an annotation references an unresolvable name
        hints = {}
    accepts = hints.get(positional[0].name, Any) if positional else Any
    returns = hints.get("return", Any)

    reg = registry or DEFAULT_REGISTRY
    for tp in (accepts, returns):
        if isinstance(tp, type) and tp.__module__ != "builtins":
            with contextlib.suppress(Exception):  # an unregisterable annotation is not fatal
                reg.register_type(tp)

    return TaskSpec(
        name=name or getattr(fn, "__name__", "task"),
        fn=fn,
        resource=resource,
        algorithm=algorithm,
        retry=_as_retrying(retry),
        timeout_s=timeout_s,
        accepts=accepts,
        returns=returns,
        takes_ctx=takes_ctx,
        module=getattr(fn, "__module__", "") or "",
        qualname=getattr(fn, "__qualname__", "") or "",
        code_digest=_code_digest(fn),
    )


def _as_retrying(spec: Retrying | Mapping[str, Any] | None) -> Retrying:
    if spec is None:
        return Retrying()
    if isinstance(spec, Retrying):
        return spec
    params = dict(spec)
    on = params.get("on")
    if isinstance(on, (list, tuple)):
        params["on"] = tuple(on)
    return Retrying(**params)


def task(
    name: str | Callable[..., Any] | None = None,
    *,
    resource: str | None = None,
    algorithm: Any = None,
    retry: Retrying | Mapping[str, Any] | None = None,
    timeout_s: float | None = None,
) -> Any:
    """Mark a function as a task. Supports ``@task`` / ``@task("name")`` / ``@task(name=..., retry=...)``."""

    def decorate(fn: Callable[..., Any]) -> TaskSpec:
        return build_task_spec(
            fn,
            name=name if isinstance(name, str) else None,
            resource=resource,
            algorithm=algorithm,
            retry=retry,
            timeout_s=timeout_s,
        )

    if callable(name):
        return decorate(name)
    return decorate


class _LeaseGuard:
    """The value returned by ``ctx.acquire(...)``: usable with either ``async with`` or ``await``.

    * ``async with guard as lease`` —— the recommended form, returning the lease **synchronously** on exit.
    * ``await guard`` —— the escape hatch; the lease is still tracked by ctx and force-reclaimed when the task ends.
    """

    __slots__ = ("algorithm", "ctx", "lease", "pool", "selector", "timeout", "where")

    def __init__(
        self,
        ctx: "TaskContext",
        pool: Pool | None,
        algorithm: Any,
        timeout: float | None,
        where: Callable[[Resource], bool] | None,
        selector: dict[str, Any],
    ) -> None:
        self.ctx = ctx
        self.pool = pool
        self.algorithm = algorithm
        self.timeout = timeout
        self.where = where
        self.selector = selector
        self.lease: Lease | None = None

    async def _do(self) -> Lease:
        if self.lease is not None:
            return self.lease
        self.lease = await self.ctx.acquire_lease(
            pool=self.pool,
            algorithm=self.algorithm,
            timeout=self.timeout,
            where=self.where,
            **self.selector,
        )
        return self.lease

    def __await__(self) -> Any:
        return self._do().__await__()

    async def __aenter__(self) -> Lease:
        return await self._do()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if self.lease is not None:
            self.lease.release_now()  # synchronous, idempotent, uninterruptible
        return False


class TaskContext:
    """The only entry point through which a running task interacts with the framework."""

    __slots__ = (
        "_emit_cb",
        "_history",
        "_leases",
        "attempt",
        "bus",
        "clock",
        "default_algorithm",
        "default_pool",
        "meta",
        "pipeline_id",
        "pipeline_key",
        "pipeline_name",
        "pools",
        "registry",
        "rng",
        "run_id",
        "seed",
        "seq",
        "task_name",
    )

    def __init__(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        pipeline_key: str,
        pipeline_name: str,
        task_name: str,
        seq: int,
        attempt: int,
        clock: Any,
        pools: Mapping[str, Pool],
        bus: Any = None,
        rng: random.Random | None = None,
        registry: CodecRegistry | None = None,
        seed: int | None = None,
        default_pool: str | None = None,
        default_algorithm: Any = None,
        emit: Callable[[str, Mapping[str, Any]], None] | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self.run_id = run_id
        self.pipeline_id = pipeline_id
        self.pipeline_key = pipeline_key
        self.pipeline_name = pipeline_name
        self.task_name = task_name
        self.seq = seq
        self.attempt = attempt
        self.clock = clock
        self.bus = bus
        self.pools = pools
        self.rng = rng or random.Random()
        self.registry = registry or DEFAULT_REGISTRY
        self.seed = seed
        self.meta = dict(meta or {})
        self.default_pool = default_pool
        self.default_algorithm = default_algorithm
        self._leases: list[Lease] = []
        self._history: list[Lease] = []
        self._emit_cb = emit

    # ------------------------------------------------- resource acquisition
    def acquire(
        self,
        pool: str | Pool | None = None,
        *,
        algorithm: Any = None,
        timeout: float | None = None,
        where: Callable[[Resource], bool] | None = None,
        **selector: Any,
    ) -> _LeaseGuard:
        """Acquire a resource. Recommended: ``async with ctx.acquire(model="gpt-4o") as lease:``."""
        target = self._resolve_pool(pool)
        return _LeaseGuard(self, target, algorithm, timeout, where, selector)

    async def acquire_lease(
        self,
        pool: str | Pool | None = None,
        *,
        algorithm: Any = None,
        timeout: float | None = None,
        where: Callable[[Resource], bool] | None = None,
        **selector: Any,
    ) -> Lease:
        """Escape hatch: get the lease directly (still tracked; force-reclaimed when the task ends if not returned)."""
        target = self._resolve_pool(pool)
        algo = algorithm if algorithm is not None else self.default_algorithm
        lease = await target.acquire(ctx=self, algorithm=algo, timeout=timeout, where=where, **selector)
        self._leases.append(lease)
        self._history.append(lease)
        return lease

    def _resolve_pool(self, pool: str | Pool | None) -> Pool:
        if isinstance(pool, Pool):
            return pool
        name = pool or self.default_pool
        if name is None:
            raise ConfigError(
                f"task {self.task_name!r} has no resource pool specified; declare it in @task(resource=...), "
                f"or call ctx.acquire(pool=...)"
            )
        target = self.pools.get(name)
        if target is None:
            from .errors import PoolNotFound

            raise PoolNotFound(f"unregistered resource pool: {name!r} (registered: {sorted(self.pools)})")
        return target

    def publish_resource(self, pool: str | Pool, resource: Resource, **kwargs: Any) -> Resource:
        """Publish a new resource into the pool so other pipelines can lease it right away."""
        target = self._resolve_pool(pool)
        published = target.add(resource, published_by=f"{self.pipeline_id}/{self.task_name}", **kwargs)
        self.emit("resource.published_by_task", pool=target.name, resource_id=published.id)
        return published

    def revoke_resource(self, pool: str | Pool, resource_id: str, *, reason: str = "") -> bool:
        target = self._resolve_pool(pool)
        return target.revoke(resource_id, reason=reason)

    def subscribe(self, pool: str | Pool, events: Sequence[str] | None = None) -> AsyncIterator[Any]:
        """Subscribe to a pool's resource events (published/retired/degraded/recovered…)."""
        return self._resolve_pool(pool).subscribe(events)

    # ------------------------------------------------------- lease tracking
    @property
    def held_leases(self) -> tuple[Lease, ...]:
        return tuple(self._leases)

    def holds_from(self, pool: Pool) -> bool:
        return any(not lease.released and lease.pool is pool for lease in self._leases)

    def held_resource_ids(self, pool: Pool) -> set[str]:
        return {
            lease.slot.resource.id for lease in self._leases if not lease.released and lease.pool is pool
        }

    def _untrack(self, lease: Lease) -> None:
        with contextlib.suppress(ValueError):
            self._leases.remove(lease)

    def lease_log(self) -> list[dict[str, Any]]:
        """**All** leases used by this attempt (including returned ones), for detailed logging."""
        return [
            {
                "pool": lease.pool.name,
                "resource": lease.resource.id,
                "kind": lease.resource.kind,
                "held_ms": round(lease.held_ms, 3),
                "released": lease.released,
            }
            for lease in self._history
        ]

    def reclaim_now(self) -> int:
        """Force-reclaim every outstanding lease synchronously; returns how many were reclaimed.

        This is the backstop behind "a finished task never holds a resource": because it is synchronous,
        neither ``CancelledError`` / timeout / exception can stop it from running to completion.
        """
        reclaimed = 0
        for lease in list(self._leases):
            if lease.released:
                self._untrack(lease)
                continue
            self.emit(
                "lease.leaked",
                pool=lease.pool.name,
                resource_id=lease.resource.id,
                held_ms=round(lease.held_ms, 3),
                hint="lease still held when the task finished; force-reclaimed; use async with ctx.acquire(...)",
            )
            lease.pool._release(lease, leaked=True)  # synchronous return
            self._untrack(lease)
            reclaimed += 1
        return reclaimed

    # --------------------------------------------------------- observability
    def emit(self, kind: str, **data: Any) -> None:
        if self._emit_cb is None:
            return
        payload = dict(data)
        payload.setdefault("task", self.task_name)
        payload.setdefault("seq", self.seq)
        payload.setdefault("attempt", self.attempt)
        with contextlib.suppress(Exception):  # a failed event record must not affect scheduling
            self._emit_cb(kind, payload)
