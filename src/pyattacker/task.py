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
import copy
import inspect
import json
import math
import random
import typing
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from typing import Any

from .algorithm import backoff_delay
from .artifact import DEFAULT_REGISTRY, CodecRegistry, digest_of
from .errors import (
    ConfigError,
    FatalError,
    RetryableError,
    error_class_of,
    is_retryable_class,
    retry_after_of,
)
from .resource import Lease, Pool, Resource

__all__ = ["UNSET", "Retrying", "TaskSpec", "task", "TaskContext"]


class _Unset:
    """Marker type of :data:`UNSET`; only the singleton is ever meant to be used."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Any = _Unset()
"""Sentinel for "no override was given", as opposed to an explicit ``None``.

:meth:`TaskSpec.with_overrides` uses it so that a caller forwarding a dict built from an optional
configuration (the declarative layer does exactly that) can leave a field alone by putting
``UNSET`` in it, while a real ``None`` still means "clear this field".
"""

# TaskSpec fields for which None is a meaningful value, so an override may set them back to it.
_CLEARABLE_FIELDS = frozenset({"resource", "algorithm", "timeout_s", "version"})


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
        return backoff_delay(attempt, rng, base=self.base, factor=self.factor, cap=self.cap, jitter=self.jitter)

    def decide(
        self,
        exc: BaseException,
        *,
        attempts_used: int,
        rng: random.Random,
        elapsed: float,
    ) -> dict[str, Any]:
        """The full retry decision for one failed attempt, as the record schema documents it.

        Answers "retry or not, and after how long" from the exception alone plus the attempt budget
        spent so far (``elapsed`` is the wall-clock time this task has been running, which is what
        ``max_total_s`` bounds). The result is the ``decision`` object stored on every attempt
        record (``docs/design.md`` §4.4) — ``retry``/``reason``/``delay_s``/``error_class``/
        ``max_attempts``/``attempt``/``retry_after``.

        This lives here rather than in the Runner because the Runner is not the only caller: the
        benchmark harness drives the same policy outside a run, and a second copy of these rules
        would be a second thing to keep in step (see ``backoff_delay`` for the same reasoning).
        """
        error_class = error_class_of(exc)
        retry_after = retry_after_of(exc)
        decision: dict[str, Any] = {
            "retry": False,
            "error_class": error_class,
            "max_attempts": self.max_attempts,
            "attempt": attempts_used,
            "retry_after": retry_after,
        }
        delay = 0.0
        if attempts_used >= self.max_attempts:
            decision["reason"] = "attempts_exhausted"
        elif not self.should_retry(exc, error_class):
            decision["reason"] = "policy_declined"
        else:
            delay = self.delay_for(attempts_used, rng, retry_after)
            if self.max_total_s is not None and elapsed + delay > self.max_total_s:
                decision["reason"] = "total_budget"
                delay = 0.0
            else:
                decision["retry"] = True
                decision["reason"] = "retryable"
        # Always present, per the documented schema — 0.0 when there is no retry to delay, not a
        # missing key that turns "why did it give up" queries into a KeyError.
        decision["delay_s"] = round(delay, 4)
        return decision


@dataclass(frozen=True)
class TaskSpec:
    """Static description of a task. All runtime state lives in :class:`TaskContext`.

    Invariants: immutable; ``@task``/``build_task_spec`` build one once at decoration time, and
    ``with_overrides`` returns a new instance rather than mutating in place (needed because a
    ``TaskSpec`` is reused as-is across every pipeline built from the same template).

    Collaborators: :func:`~pyattacker.pipeline.pipeline`/:class:`~pyattacker.pipeline.Chain`
    validate adjacent specs' ``accepts``/``returns`` before assembling a pipeline;
    :class:`~pyattacker.runner.Runner` calls ``__call__`` once per attempt with the current value
    and (if ``takes_ctx``) a fresh :class:`TaskContext`.

    Attributes:
        config / version: Declared JSON behavior and explicit revision. Config is snapshotted
            at construction; arbitrary closures and external endpoint settings are not inspected.
        parameters / children: Factory behavior arguments and nested specs, kept separately from
            user config so config overrides cannot erase factory identity.
        fn: The wrapped callable; ``(value)`` or ``(value, ctx)`` depending on ``takes_ctx``.
        resource: Default pool name this task acquires from (``ctx.acquire()`` with no ``pool=``
            falls back to this); ``None`` means the task must always name its pool explicitly.
        algorithm: Supplied default acquire policy. Built-ins execute from a captured configuration,
            unaffected by later mutation; custom fingerprint hooks are checked before use.
        retry: Retry policy applied when an attempt of this task raises.
        timeout_s: Wall-clock timeout for one attempt (only meaningful for an async ``fn``).
        accepts / returns: Type hints inferred from ``fn``'s signature; used only for the adjacent-task
            compatibility check at pipeline-build time — not enforced at runtime.
        takes_ctx: Whether ``fn`` takes a second ``(value, ctx)`` parameter; inferred from ``fn``'s
            positional-parameter count when the spec is built.
        module / qualname / code_digest: Normally identify this exact version of the task's code
            (folded into the pipeline's ``spec_digest`` so that changing a task's source
            invalidates old checkpoints instead of silently reusing them — see ``pipeline.py``
            module docstring). Two things weaken that guarantee: ``pipeline(..., include_code=
            False)`` drops ``code_digest`` from the fingerprint entirely, and when ``fn``'s
            source cannot be inspected (e.g. dynamically defined), ``code_digest`` falls back to
            digesting ``module:qualname``, which does not distinguish different implementations
            behind the same name.
    """

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
    config: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)
    parameters: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)
    version: str | None = None
    children: tuple["TaskSpec", ...] = field(default=(), repr=False)
    _config_json: str = field(default="{}", init=False, repr=False)
    _parameters_json: str = field(default="{}", init=False, repr=False)
    _algorithm_snapshot: "_AlgorithmSnapshot | None" = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.version is not None and not isinstance(self.version, str):
            raise ConfigError("task version must be a string")
        if not isinstance(self.config, Mapping) or not isinstance(self.parameters, Mapping):
            raise ConfigError("task config and parameters must be JSON mappings")
        object.__setattr__(self, "_config_json", _identity_json(dict(self.config)))
        object.__setattr__(self, "_parameters_json", _identity_json(dict(self.parameters)))
        object.__setattr__(self, "_algorithm_snapshot", _snapshot_algorithm(self.algorithm, version=self.version))

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    def fingerprint(self, *, include_code: bool = True) -> dict[str, Any]:
        """Stable declared behavior; never inspect closures or serialize runtime clients."""
        result = {
            "name": self.name,
            "target": f"{self.module}:{self.qualname}",
            "config": json.loads(self._config_json),
            "parameters": json.loads(self._parameters_json),
            "version": self.version,
            "children": [child.fingerprint(include_code=include_code) for child in self.children],
            "resource": self.resource,
            "algorithm": self._algorithm_snapshot.identity() if self._algorithm_snapshot is not None else None,
            "timeout_s": self.timeout_s,
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "on": [f"{exc.__module__}:{exc.__qualname__}" for exc in self.retry.on],
                "retry_classified": self.retry.retry_classified,
                "retry_unknown": self.retry.retry_unknown,
                "max_total_s": self.retry.max_total_s,
                "base": self.retry.base,
                "factor": self.retry.factor,
                "cap": self.retry.cap,
                "jitter": self.retry.jitter,
            },
        }
        if include_code:
            result["code"] = self.code_digest
        return result

    def runtime_algorithm(self) -> Any:
        """Resolve a fresh runtime algorithm from the same snapshot used by fingerprint()."""
        return self._algorithm_snapshot.runtime() if self._algorithm_snapshot is not None else None

    def with_overrides(self, **kwargs: Any) -> "TaskSpec":
        """Return a copy of this spec with the given fields replaced.

        Override semantics: an **omitted** keyword keeps the current value, and an explicit
        ``None`` **clears** a field that supports being empty — ``resource``, ``algorithm``,
        ``timeout_s`` and ``version``. ``UNSET`` (the sentinel re-exported by this module) is
        treated as "not provided", which is what lets a caller forward a dict whose keys come
        from an optional config without clearing everything it did not mention.

        Passing ``None`` for a field that cannot be empty (``name``, ``fn``, ``retry``,
        ``children``, ``config``, ``parameters`` and the derived identity fields) is a
        :class:`~pyattacker.errors.ConfigError` rather than a silent no-op, because it could only
        ever produce a broken spec.
        """
        changes = {key: value for key, value in kwargs.items() if value is not UNSET}
        cleared = sorted(key for key, value in changes.items() if value is None and key not in _CLEARABLE_FIELDS)
        if cleared:
            raise ConfigError(
                f"task field(s) {cleared} cannot be cleared with None; "
                f"only {sorted(_CLEARABLE_FIELDS)} support it"
            )
        return replace(self, **changes)

    def __or__(self, other: Any) -> Any:
        from .pipeline import Chain

        left = Chain((self,))
        return left | other

    def __call__(self, value: Any, ctx: "TaskContext") -> Any:
        return self.fn(value, ctx) if self.takes_ctx else self.fn(value)


def _identity_json(value: Any) -> str:
    def validate(node: Any, ancestors: set[int]) -> None:
        if type(node) in (type(None), bool, int, str):
            return
        if type(node) is float and math.isfinite(node):
            return
        if type(node) not in (dict, list):
            raise ValueError(f"unsupported identity value: {type(node).__name__}")
        if id(node) in ancestors:
            raise ValueError("cyclic identity value")
        ancestors.add(id(node))
        try:
            if type(node) is dict:
                if any(type(key) is not str for key in node):
                    raise ValueError("identity object keys must be strings")
                values = node.values()
            else:
                values = node
            for item in values:
                validate(item, ancestors)
        finally:
            ancestors.remove(id(node))

    try:
        validate(value, set())
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ConfigError(f"task identity must contain only finite JSON values: {exc}") from exc


@dataclass(frozen=True)
class _AlgorithmSnapshot:
    data: str
    factory: Any = None
    fallback: "_AlgorithmSnapshot | None" = None
    custom: Any = None
    checked: bool = False

    def identity(self) -> Any:
        return json.loads(self.data)

    def validate_custom(self) -> None:
        expected = self.identity()
        target = f"{type(self.custom).__module__}:{type(self.custom).__qualname__}"
        actual = _identity_json({"target": target, "config": self.custom.fingerprint()})
        if actual != self.data:
            raise ConfigError(
                f"custom algorithm {expected['target']} fingerprint changed after task construction; "
                "build a new TaskSpec/pipeline for changed behavior"
            )

    def runtime(self) -> Any:
        if self.factory is not None:
            params = self.identity()["params"]
            if self.fallback is not None:
                params["fallback"] = self.fallback.runtime()
            return self.factory(**params)
        if self.checked:
            self.validate_custom()
            return _CheckedAlgorithm(self)
        return _copy_custom_algorithm(self.custom)


@dataclass
class _CheckedAlgorithm:
    snapshot: _AlgorithmSnapshot

    @property
    def name(self) -> str:
        return self.snapshot.custom.name

    async def acquire(self, pool: Pool, **kwargs: Any) -> Lease:
        # Check again at acquisition: another coroutine may have changed the source object
        # since the attempt was opened, including while waiting between acquisitions.
        self.snapshot.validate_custom()
        return await self.snapshot.custom.acquire(pool, **kwargs)


def _copy_custom_algorithm(algorithm: Any) -> Any:
    try:
        cloned = copy.deepcopy(algorithm)
    except Exception as exc:
        raise ConfigError("versioned custom algorithm must support deepcopy, or provide fingerprint()") from exc
    if cloned is algorithm:
        raise ConfigError("versioned custom algorithm deepcopy must return a separate instance")
    return cloned


def _snapshot_algorithm(spec: Any, *, version: str | None = None) -> _AlgorithmSnapshot | None:
    if spec is None:
        # Pool defaults and endpoint options are external: version them through task config/version.
        return None
    from .algorithm import ALGORITHMS, Wait, resolve_algorithm

    algorithm = resolve_algorithm(spec)
    target = f"{type(algorithm).__module__}:{type(algorithm).__qualname__}"
    if type(algorithm) in ALGORITHMS.values():
        params = {}
        fallback = None
        for item in fields(algorithm):
            value = getattr(algorithm, item.name)
            if item.name == "fallback":
                fallback = _snapshot_algorithm(value if value is not None else Wait(), version=version)
                params[item.name] = fallback.identity()
            elif item.name == "pools":
                # Failover accepts a Sequence of names; tuple/list represent the same behavior.
                params[item.name] = list(value)
            else:
                params[item.name] = value
        return _AlgorithmSnapshot(
            _identity_json({"target": target, "params": params}),
            factory=type(algorithm), fallback=fallback,
        )
    hook = getattr(algorithm, "fingerprint", None)
    if callable(hook):
        return _AlgorithmSnapshot(
            _identity_json({"target": target, "config": hook()}), custom=algorithm, checked=True,
        )
    if version is not None:
        return _AlgorithmSnapshot(
            _identity_json({"target": target, "version": version}),
            custom=_copy_custom_algorithm(algorithm),
        )
    raise ConfigError(
        f"custom algorithm {target} must provide fingerprint() with JSON values or use task(version=...)"
    )


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
    config: Mapping[str, Any] | None = None,
    version: str | None = None,
    children: tuple[TaskSpec, ...] = (),
    parameters: Mapping[str, Any] | None = None,
) -> TaskSpec:
    """Wrap a plain function into a :class:`TaskSpec` (the internal implementation of ``@task``)."""
    if isinstance(fn, TaskSpec):  # double decoration / declarative override
        if not (any((name, resource, algorithm, retry, timeout_s, children))
                or config is not None or version is not None or parameters is not None):
            return fn
        # Only the arguments that were actually provided become overrides: with_overrides now
        # treats an explicit None as "clear", so forwarding its own defaults would wipe fields
        # the caller never mentioned (a factory's own resource/retry, for example).
        changes: dict[str, Any] = {}
        if name is not None:
            changes["name"] = name
        if resource is not None:
            changes["resource"] = resource
        if algorithm is not None:
            changes["algorithm"] = algorithm
        if retry is not None:
            changes["retry"] = _as_retrying(retry)
        if timeout_s is not None:
            changes["timeout_s"] = timeout_s
        if children:
            changes["children"] = children
        if config is not None:
            changes["config"] = config
        if version is not None:
            changes["version"] = version
        if parameters is not None:
            changes["parameters"] = parameters
        return fn.with_overrides(**changes)
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
        config=config if config is not None else {},
        version=version,
        children=children,
        parameters=parameters if parameters is not None else {},
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
    config: Mapping[str, Any] | None = None,
    version: str | None = None,
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
            config=config,
            version=version,
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
        # Deliberately not `True`: the lease safety contract (docs/design.md §4.2) promises that a
        # failure inside the `with` block still propagates. Swallowing it here would silently turn
        # every task exception into a successful-looking task.
        return False


class TaskContext:
    """The only entry point through which a running task interacts with the framework."""

    __slots__ = (
        "_emit_cb",
        "_history",
        "_leases",
        "_report_metric_cb",
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
        "visit",
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
        visit: int = 0,
        clock: Any,
        pools: Mapping[str, Pool],
        bus: Any = None,
        rng: random.Random | None = None,
        registry: CodecRegistry | None = None,
        seed: int | None = None,
        default_pool: str | None = None,
        default_algorithm: Any = None,
        emit: Callable[[str, Mapping[str, Any]], None] | None = None,
        report_metric: Callable[..., Any] | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self.run_id = run_id
        self.pipeline_id = pipeline_id
        self.pipeline_key = pipeline_key
        self.pipeline_name = pipeline_name
        self.task_name = task_name
        self.seq = seq
        self.attempt = attempt
        self.visit = visit
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
        self._report_metric_cb = report_metric

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
        payload.setdefault("visit", self.visit)
        with contextlib.suppress(Exception):  # a failed event record must not affect scheduling
            self._emit_cb(kind, payload)

    def report_metric(
        self, name: str, value: str | int | float | bool, *, label: str = "",
        display: str = "number"
    ) -> Any:
        """Persist a latest-value report scoped to this pipeline."""
        if self._report_metric_cb is None:
            raise ConfigError("this task context has no metric reporter")
        return self._report_metric_cb(name, value, label=label, display=display)
