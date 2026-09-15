"""Resource / Pool / Lease / Bus —— the framework's single shared surface.

Core invariants (must be preserved):

1. **Every change to pool state is synchronous**. No ``await`` takes part in
   allocation decisions, so under single-threaded asyncio no lock is needed and
   no "check-then-preempted" window exists.
2. **``Lease.release_now()`` is synchronous, idempotent, and cannot be
   interrupted by cancellation**. Task failure/cancellation/timeout never
   prevents a resource from being returned.
3. A lease can be returned only once; returning it again is a harmless no-op.
4. Every pool state change emits a :class:`ResourceEvent`, which feeds all of:
   waiters, subscribers, the database event table, and monitoring snapshots.

Publish/subscribe and acquire are two separate channels:
* **pull**: ``await pool.acquire(...)`` —— wait + lease.
* **push**: ``pool.subscribe()`` / :class:`Bus` —— receive resource publish, retire, and degrade signals.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Callable, Iterable, Mapping, Sequence

from .errors import AcquireTimeout, PoolNotFound, ResourceUnavailable

__all__ = [
    "ResourceState",
    "ResourceStats",
    "Resource",
    "Lease",
    "Pool",
    "PoolStats",
    "ResourceEvent",
    "Bus",
]

_ids = itertools.count(1)


class ResourceState(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"  # temporary circuit-break: unavailable until blocked_until
    DEAD = "dead"  # too many consecutive failures; needs manual/external recovery
    REVOKED = "revoked"  # explicitly withdrawn


@dataclass
class ResourceStats:
    active: int = 0
    leases: int = 0
    ok: int = 0
    failed: int = 0
    leaked: int = 0
    waits: int = 0
    wait_ms_total: float = 0.0
    consecutive_failures: int = 0
    degraded_count: int = 0
    latency_ms_ema: float | None = None
    usage: dict[str, float] = field(default_factory=dict)
    last_used_at: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "active": self.active,
            "leases": self.leases,
            "ok": self.ok,
            "failed": self.failed,
            "leaked": self.leaked,
            "waits": self.waits,
            "consecutive_failures": self.consecutive_failures,
            "degraded_count": self.degraded_count,
            "latency_ms_ema": self.latency_ms_ema,
            "usage": dict(self.usage),
        }


@dataclass
class Resource:
    """One concrete resource in a pool (an endpoint / a key / a local worker).

    ``capacity`` is the number of concurrent leases allowed for a **single
    resource instance**. ``factory`` is optional and turns options into a
    genuinely usable client object (sync or async).
    """

    id: str
    kind: str = "generic"
    options: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, Any] = field(default_factory=dict)
    capacity: int = 1
    factory: Callable[[Resource], Any] | None = None
    degrade_after: int = 3
    dead_after: int = 8
    cooldown_s: float = 30.0
    meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        kind: str = "generic",
        *,
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
        tags: Mapping[str, Any] | None = None,
        capacity: int = 1,
        factory: Callable[[Resource], Any] | None = None,
        degrade_after: int = 3,
        dead_after: int = 8,
        cooldown_s: float = 30.0,
        **meta: Any,
    ) -> "Resource":
        return cls(
            id=id or f"{kind}-{next(_ids)}",
            kind=kind,
            options=dict(options or {}),
            tags=dict(tags or {}),
            capacity=max(1, int(capacity)),
            factory=factory,
            degrade_after=degrade_after,
            dead_after=dead_after,
            cooldown_s=cooldown_s,
            meta=dict(meta),
        )

    def spec(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "options": _redact(self.options),
            "tags": dict(self.tags),
            "capacity": self.capacity,
            "degrade_after": self.degrade_after,
            "dead_after": self.dead_after,
            "cooldown_s": self.cooldown_s,
        }

    def lookup(self, key: str) -> tuple[bool, Any]:
        if key == "id":
            return True, self.id
        if key == "kind":
            return True, self.kind
        if key in self.tags:
            return True, self.tags[key]
        if key in self.options:
            return True, self.options[key]
        if "." in key:  # dot paths into options are supported: options={a:{b:1}} → "a.b"
            node: Any = self.options
            for part in key.split("."):
                if not isinstance(node, Mapping) or part not in node:
                    return False, None
                node = node[part]
            return True, node
        return False, None


_SECRET_HINTS = ("key", "token", "secret", "password", "authorization")


def _redact(options: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in options.items():
        if any(h in k.lower() for h in _SECRET_HINTS) and isinstance(v, str) and v:
            out[k] = f"***{v[-4:]}" if len(v) > 4 else "***"
        elif isinstance(v, Mapping):
            out[k] = _redact(v)
        else:
            out[k] = v
    return out


class _Waiter:
    """One blocked acquisition: its own event plus the selector it is waiting for.

    Private. Exists so a release wakes only the waiters that could use that resource.
    """

    __slots__ = ("event", "selector", "where")

    def __init__(self, *, selector: Mapping[str, Any], where: Callable[[Resource], bool] | None) -> None:
        self.event = asyncio.Event()
        self.selector = dict(selector)
        self.where = where

    def matches(self, resource: Resource) -> bool:
        if self.where is not None and not self.where(resource):
            return False
        for key, want in self.selector.items():
            found, got = resource.lookup(key)
            if not found or got != want:
                return False
        return True


class _Slot:
    """Mutable state internal to a pool. Not public API."""

    __slots__ = ("resource", "stats", "state", "blocked_until", "client", "client_error", "published_at")

    def __init__(self, resource: Resource, published_at: float) -> None:
        self.resource = resource
        self.stats = ResourceStats()
        self.state = ResourceState.READY
        self.blocked_until = 0.0
        self.client: Any = None
        self.client_error: str | None = None
        self.published_at = published_at

    def state_at(self, now: float) -> ResourceState:
        """Lazy state advance: once the cooldown elapses, DEGRADED returns to READY.

        Note: ``consecutive_failures`` is **not reset** here —— consecutive
        failures accumulate across cooldown windows; otherwise, when the degrade
        threshold is below the dead threshold, DEAD could never be reached (each
        cooldown would wipe the count out). The count is cleared only by
        ``report(ok=True)``.
        """
        if self.state is ResourceState.DEGRADED and now >= self.blocked_until:
            self.state = ResourceState.READY
        return self.state

    def available(self, now: float) -> bool:
        return self.state_at(now) is ResourceState.READY and self.stats.active < self.resource.capacity

    def free(self, now: float) -> int:
        if self.state_at(now) is not ResourceState.READY:
            return 0
        return max(0, self.resource.capacity - self.stats.active)


@dataclass(frozen=True)
class ResourceEvent:
    ts: float
    pool: str
    kind: str
    resource_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "pool": self.pool,
            "kind": self.kind,
            "resource_id": self.resource_id,
            "data": self.data,
        }


class Pool:
    """Resource pool: a group of like resources + a default acquire algorithm + a state event stream."""

    def __init__(
        self,
        name: str,
        resources: Iterable[Resource] = (),
        *,
        kind: str | None = None,
        algorithm: Any = None,
        bus: "Bus | None" = None,
        clock: Any = None,
        deadlock_warn_s: float | None = 5.0,
        on_event: Callable[[ResourceEvent], None] | None = None,
    ) -> None:
        self.name = name
        self.kind = kind
        self.bus = bus
        self.clock = clock or _RealClock()
        self.deadlock_warn_s = deadlock_warn_s
        self.on_event = on_event
        self._slots: dict[str, _Slot] = {}
        self._order: list[str] = []
        self._wake: asyncio.Event | None = None
        self._loop: Any = None
        self._waiters: list[_Waiter] = []
        self._rr = 0
        self._wait_samples: deque[float] = deque(maxlen=512)
        self._wait_total_ms = 0.0
        self._wait_max_ms = 0.0
        self.slow_wait_ms = 1000.0
        self._subscribers: list[tuple[asyncio.Queue, frozenset[str] | None]] = []
        self.default_algorithm = algorithm
        self.waiting = 0
        for res in resources:
            self.add(res, emit=False)

    # ---------------------------------------------------------------- membership
    def add(self, resource: Resource, *, emit: bool = True, published_by: str | None = None) -> Resource:
        if resource.id not in self._slots:
            self._slots[resource.id] = _Slot(resource, self.clock.now())
            self._order.append(resource.id)
        else:  # re-publishing the same id = reset its state
            slot = self._slots[resource.id]
            slot.resource = resource
            slot.state = ResourceState.READY
            slot.blocked_until = 0.0
        if emit:
            self._emit(
                "resource.published",
                resource_id=resource.id,
                data={"kind": resource.kind, "capacity": resource.capacity, "by": published_by},
            )
        self._notify()
        return resource

    publish = add

    def revoke(self, resource_id: str, *, reason: str = "", emit: bool = True) -> bool:
        slot = self._slots.get(resource_id)
        if slot is None:
            return False
        slot.state = ResourceState.REVOKED
        if emit:
            self._emit("resource.revoked", resource_id=resource_id, data={"reason": reason})
        self._notify()
        return True

    def resources(self) -> list[Resource]:
        return [self._slots[i].resource for i in self._order if i in self._slots]

    def __len__(self) -> int:
        return len(self._slots)

    # ------------------------------------------------------------------ acquire
    def _candidates(
        self,
        selector: Mapping[str, Any] | None,
        where: Callable[[Resource], bool] | None,
        now: float,
        *,
        only_available: bool,
    ) -> list[_Slot]:
        out: list[_Slot] = []
        for rid in self._order:
            slot = self._slots[rid]
            if slot.state_at(now) is ResourceState.REVOKED:
                continue
            if where is not None and not where(slot.resource):
                continue
            if selector:
                ok = True
                for key, want in selector.items():
                    found, got = slot.resource.lookup(key)
                    if not found or got != want:
                        ok = False
                        break
                if not ok:
                    continue
            if only_available and not slot.available(now):
                continue
            out.append(slot)
        return out

    def select(
        self,
        key: Callable[[Resource, ResourceStats], Any] | None = None,
        *,
        where: Callable[[Resource], bool] | None = None,
        ctx: Any = None,
        **selector: Any,
    ) -> "Lease | None":
        """Try to acquire a lease synchronously, letting the caller rank the candidates.

        ``key(resource, stats)`` returns a score (any orderable value — a tuple works, which is
        how `quota_aware` breaks ties by absolute headroom); the highest available score wins. Ties are
        broken least-busy-first with round-robin rotation, so traffic does not pile onto one
        resource. Returns ``None`` if nothing is available; never blocks.

        This is the single allocation point: :meth:`try_acquire` is ``select(None)``, and the
        quota-aware / least-busy / sticky algorithms are all just different ``key`` functions.
        """
        now = self.clock.now()
        candidates = self._candidates(selector, where, now, only_available=True)
        if not candidates:
            return None
        if key is not None:
            scored = [(key(slot.resource, slot.stats), slot) for slot in candidates]
            best = max(score for score, _ in scored)
            candidates = [slot for score, slot in scored if score == best]
        # least-used first, round-robin on ties
        candidates.sort(key=lambda s: s.stats.active / max(1, s.resource.capacity))
        best_ratio = candidates[0].stats.active / max(1, candidates[0].resource.capacity)
        head = [s for s in candidates if s.stats.active / max(1, s.resource.capacity) == best_ratio]
        slot = head[self._rr % len(head)]
        self._rr += 1
        return self._lease(slot, ctx=ctx, waited_ms=0.0)

    def try_acquire(
        self,
        *,
        where: Callable[[Resource], bool] | None = None,
        ctx: Any = None,
        **selector: Any,
    ) -> "Lease | None":
        """Try to acquire a lease synchronously. Returns ``None`` if nothing is available; never blocks."""
        return self.select(None, where=where, ctx=ctx, **selector)

    def note_wait(self, lease: "Lease", waited_ms: float, *, selector: Mapping[str, Any] | None = None) -> None:
        """Record how long a caller waited before this lease was handed out.

        Algorithms call this once they finally acquire something, so waiting time shows up in
        pool stats instead of being invisible.
        """
        if waited_ms <= 0:
            return
        lease.slot.stats.waits += 1
        lease.slot.stats.wait_ms_total += waited_ms
        self._wait_samples.append(waited_ms)
        self._wait_total_ms += waited_ms
        self._wait_max_ms = max(self._wait_max_ms, waited_ms)
        if waited_ms > self.slow_wait_ms:
            self._emit(
                "acquire.slow_wait",
                resource_id=lease.resource.id,
                data={"waited_ms": round(waited_ms, 3), "selector": dict(selector or {})},
            )

    def _lease(self, slot: _Slot, *, ctx: Any, waited_ms: float) -> "Lease":
        slot.stats.active += 1
        slot.stats.leases += 1
        slot.stats.waits += 1 if waited_ms > 0 else 0
        slot.stats.wait_ms_total += waited_ms
        if slot.client is None and slot.resource.factory is not None and slot.client_error is None:
            try:
                slot.client = slot.resource.factory(slot.resource)
            except Exception as exc:  # factory failed → resource unusable; let the caller retry / switch pools
                slot.client_error = f"{type(exc).__name__}: {exc}"
                slot.stats.active -= 1
                slot.stats.failed += 1
                slot.stats.consecutive_failures += 1
                if slot.stats.consecutive_failures >= slot.resource.dead_after:
                    slot.state = ResourceState.DEAD
                self._emit(
                    "resource.factory_failed",
                    resource_id=slot.resource.id,
                    data={"error": slot.client_error},
                )
                raise ResourceUnavailable(
                    f"factory failed for resource {slot.resource.id}: {slot.client_error}"
                ) from exc
        lease = Lease(pool=self, slot=slot, ctx=ctx, acquired_at=self.clock.now())
        self._emit(
            "resource.leased",
            resource_id=slot.resource.id,
            data={"active": slot.stats.active, "capacity": slot.resource.capacity, "waited_ms": round(waited_ms, 3)},
        )
        return lease

    async def acquire(
        self,
        *,
        ctx: Any = None,
        algorithm: Any = None,
        timeout: float | None = None,
        where: Callable[[Resource], bool] | None = None,
        **selector: Any,
    ) -> "Lease":
        algo = algorithm or self.default_algorithm
        if algo is None:
            from .algorithm import Wait

            algo = Wait()
        if isinstance(algo, str):
            from .algorithm import resolve_algorithm

            algo = resolve_algorithm(algo)
        return await algo.acquire(self, ctx=ctx, where=where, timeout=timeout, selector=selector)

    def reset_waiters(self) -> None:
        """Drop every waiter so the next one registers on the current event loop.

        A Runner may be used across several event loops (``asyncio.run`` per call), and an
        asyncio primitive binds to the loop it first waited on.
        """
        self._waiters.clear()
        self._wake = None
        self._loop = None

    def _notify(self, slot: _Slot | None = None) -> None:
        """Wake the waiters that could actually use what just changed.

        Broadcasting to everyone is correct but O(waiters) per release; with thousands of
        blocked pipelines that is the dominant cost. When we know which slot changed we only
        wake waiters whose selector matches it.
        """
        if not self._waiters:
            return
        if slot is None:
            for waiter in self._waiters:
                waiter.event.set()
            return
        for waiter in self._waiters:
            if waiter.matches(slot.resource):
                waiter.event.set()

    async def wait_slot(
        self,
        timeout: float | None = None,
        *,
        ctx: Any = None,
        where: Callable[[Resource], bool] | None = None,
        selector: Mapping[str, Any] | None = None,
    ) -> bool:
        """Wait for the "at least one resource available" broadcast. Returns whether one appeared before the timeout (acquiring it is not guaranteed)."""
        waiter = _Waiter(selector=dict(selector or {}), where=where)
        self._waiters.append(waiter)
        started = self.clock.now()
        deadline = None if timeout is None else started + timeout
        warn_task: asyncio.Task[None] | None = None
        try:
            if (
                ctx is not None
                and self.deadlock_warn_s
                and ctx.holds_from(self)
                and not self._candidates(selector, where, self.clock.now(), only_available=True)
            ):
                warn_task = asyncio.create_task(self._warn_possible_deadlock(ctx, selector, where))
            self.waiting += 1
            while True:
                now = self.clock.now()
                if self._candidates(selector, where, now, only_available=True):
                    return True
                remaining = None if deadline is None else max(0.0, deadline - now)
                if remaining is not None and remaining <= 0:
                    return False
                # Clearing right before the await is safe: there is no await point between
                # the clear and the wait, so a notification cannot slip through the gap.
                waiter.event.clear()
                try:
                    await asyncio.wait_for(waiter.event.wait(), remaining)
                except TimeoutError:
                    return False
        finally:
            self.waiting -= 1
            if waiter in self._waiters:
                self._waiters.remove(waiter)
            if warn_task is not None:
                warn_task.cancel()

    async def _warn_possible_deadlock(
        self,
        ctx: Any,
        selector: Mapping[str, Any] | None,
        where: Callable[[Resource], bool] | None,
    ) -> None:
        await asyncio.sleep(self.deadlock_warn_s or 0)
        held = ctx.held_resource_ids(self)
        self._emit(
            "acquire.suspected_deadlock",
            resource_id=None,
            data={
                "task": getattr(ctx, "task_name", None),
                "pipeline_id": getattr(ctx, "pipeline_id", None),
                "held": sorted(held),
                "selector": dict(selector or {}),
                "hint": "task requested another resource from this pool while holding one, and the pool has no free capacity",
            },
        )

    # ------------------------------------------------------------------ release
    def _release(self, lease: "Lease", *, leaked: bool = False) -> bool:
        slot = lease.slot
        if lease.released:
            return False
        lease.released = True
        if slot.stats.active > 0:
            slot.stats.active -= 1
        if leaked:
            slot.stats.leaked += 1
        self._emit(
            "resource.leaked" if leaked else "resource.released",
            resource_id=slot.resource.id,
            data={"active": slot.stats.active, "held_ms": round(lease.held_ms, 3), "task": lease.task_name},
        )
        self._notify(slot)  # only waiters that can use *this* resource need to wake up
        return True

    def _report(
        self,
        lease: "Lease",
        *,
        ok: bool,
        latency_ms: float | None,
        usage: Mapping[str, float] | None,
        error: Any,
    ) -> None:
        slot = lease.slot
        slot.stats.last_used_at = self.clock.now()
        if usage:
            for k, v in usage.items():
                try:
                    slot.stats.usage[k] = slot.stats.usage.get(k, 0.0) + float(v)
                except (TypeError, ValueError):
                    continue
        if latency_ms is not None:
            prev = slot.stats.latency_ms_ema
            slot.stats.latency_ms_ema = latency_ms if prev is None else 0.7 * prev + 0.3 * latency_ms
        if ok:
            slot.stats.ok += 1
            slot.stats.consecutive_failures = 0
            if slot.state is not ResourceState.READY:
                slot.state = ResourceState.READY
                slot.blocked_until = 0.0
                self._emit("resource.recovered", resource_id=slot.resource.id)
                self._notify(slot)
            return
        slot.stats.failed += 1
        slot.stats.consecutive_failures += 1
        if slot.stats.consecutive_failures >= slot.resource.dead_after:
            if slot.state is not ResourceState.DEAD:
                slot.state = ResourceState.DEAD
                self._emit(
                    "resource.dead",
                    resource_id=slot.resource.id,
                    data={"consecutive_failures": slot.stats.consecutive_failures, "error": _short(error)},
                )
                self._notify()
        elif slot.stats.consecutive_failures >= slot.resource.degrade_after:
            slot.state = ResourceState.DEGRADED
            slot.blocked_until = self.clock.now() + slot.resource.cooldown_s
            slot.stats.degraded_count += 1
            self._emit(
                "resource.degraded",
                resource_id=slot.resource.id,
                data={
                    "consecutive_failures": slot.stats.consecutive_failures,
                    "cooldown_s": slot.resource.cooldown_s,
                    "error": _short(error),
                },
            )
            self._notify()

    def _degrade(self, lease: "Lease", reason: str) -> None:
        slot = lease.slot
        slot.state = ResourceState.DEGRADED
        slot.blocked_until = self.clock.now() + slot.resource.cooldown_s
        slot.stats.degraded_count += 1
        self._emit("resource.degraded", resource_id=slot.resource.id, data={"reason": reason})
        self._notify()

    # ------------------------------------------------------------ publish/subscribe
    def subscribe(
        self, events: Sequence[str] | None = None, *, maxsize: int = 1024
    ) -> AsyncIterator[ResourceEvent]:
        """Subscribe to this pool's resource events. ``events`` is a list of event-name prefixes/full names; None means all."""
        queue: asyncio.Queue[ResourceEvent] = asyncio.Queue(maxsize=maxsize)
        pattern = None if events is None else frozenset(events)
        entry = (queue, pattern)
        self._subscribers.append(entry)

        async def _gen() -> AsyncIterator[ResourceEvent]:
            try:
                while True:
                    yield await queue.get()
            finally:
                if entry in self._subscribers:
                    self._subscribers.remove(entry)

        return _gen()

    def _emit(self, kind: str, *, resource_id: str | None, data: Mapping[str, Any] | None = None) -> None:
        event = ResourceEvent(
            ts=self.clock.now(), pool=self.name, kind=kind, resource_id=resource_id, data=dict(data or {})
        )
        for queue, pattern in list(self._subscribers):
            if pattern is None or kind in pattern:
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:  # subscriber too slow: drop the oldest entry
                    try:
                        queue.get_nowait()
                        queue.put_nowait(event)
                    except Exception:
                        pass
        if self.bus is not None:
            self.bus.publish("resource", **event.as_dict())
        if self.on_event is not None:
            try:
                self.on_event(event)
            except Exception:  # a failed event record must not affect scheduling
                pass

    # ------------------------------------------------------------------ observability
    def stats(self, *, where: Callable[[Resource], bool] | None = None, **selector: Any) -> "PoolStats":
        now = self.clock.now()
        slots = self._candidates(selector or None, where, now, only_available=False)
        ready = degraded = dead = revoked = active = capacity = 0
        usage: dict[str, float] = {}
        for slot in slots:
            state = slot.state_at(now)
            active += slot.stats.active
            capacity += slot.resource.capacity
            if state is ResourceState.READY:
                ready += 1
            elif state is ResourceState.DEGRADED:
                degraded += 1
            elif state is ResourceState.DEAD:
                dead += 1
            else:
                revoked += 1
            for k, v in slot.stats.usage.items():
                usage[k] = usage.get(k, 0.0) + v
        return PoolStats(
            name=self.name,
            kind=self.kind,
            total=len(slots),
            ready=ready,
            degraded=degraded,
            dead=dead,
            revoked=revoked,
            active=active,
            capacity=capacity,
            waiting=self.waiting,
            leases_total=sum(s.stats.leases for s in slots),
            ok_total=sum(s.stats.ok for s in slots),
            failed_total=sum(s.stats.failed for s in slots),
            leaked_total=sum(s.stats.leaked for s in slots),
            usage=usage,
            waits_total=sum(s.stats.waits for s in slots),
            wait_ms_avg=round(self._wait_total_ms / len(self._wait_samples), 3) if self._wait_samples else None,
            wait_ms_p50=_percentile(self._wait_samples, 0.5),
            wait_ms_p95=_percentile(self._wait_samples, 0.95),
            wait_ms_max=round(self._wait_max_ms, 3) if self._wait_max_ms else None,
        )

    def snapshot(self) -> list[dict[str, Any]]:
        now = self.clock.now()
        return [
            {
                "id": s.resource.id,
                "kind": s.resource.kind,
                "state": s.state_at(now).value,
                "active": s.stats.active,
                "capacity": s.resource.capacity,
                "consecutive_failures": s.stats.consecutive_failures,
                "leases": s.stats.leases,
                "failed": s.stats.failed,
                "latency_ms_ema": s.stats.latency_ms_ema,
                "blocked_until": s.blocked_until or None,
            }
            for s in (self._slots[i] for i in self._order)
        ]


@dataclass
class PoolStats:
    name: str
    kind: str | None
    total: int
    ready: int
    degraded: int
    dead: int
    revoked: int
    active: int
    capacity: int
    waiting: int
    leases_total: int
    ok_total: int
    failed_total: int
    leaked_total: int
    usage: dict[str, float] = field(default_factory=dict)
    waits_total: int = 0
    wait_ms_avg: float | None = None
    wait_ms_p50: float | None = None
    wait_ms_p95: float | None = None
    wait_ms_max: float | None = None

    @property
    def utilization(self) -> float:
        return round(self.active / self.capacity, 4) if self.capacity else 0.0


class Lease:
    """A single resource lease. **Must be returned**; prefer ``async with ctx.acquire(...)``."""

    __slots__ = ("pool", "slot", "ctx", "acquired_at", "released")

    def __init__(self, *, pool: Pool, slot: _Slot, ctx: Any, acquired_at: float) -> None:
        self.pool = pool
        self.slot = slot
        self.ctx = ctx
        self.acquired_at = acquired_at
        self.released = False

    # -------------------------------------------------------------- read-only properties
    @property
    def resource(self) -> Resource:
        return self.slot.resource

    @property
    def client(self) -> Any:
        """The usable object produced by the factory (e.g. an HTTP client); None when there is no factory."""
        return self.slot.client

    @property
    def options(self) -> dict[str, Any]:
        return self.slot.resource.options

    @property
    def held_ms(self) -> float:
        return (self.pool.clock.now() - self.acquired_at) * 1000.0

    @property
    def task_name(self) -> str | None:
        return getattr(self.ctx, "task_name", None)

    # ---------------------------------------------------------------- feedback
    def report(
        self,
        *,
        ok: bool = True,
        latency_ms: float | None = None,
        usage: Mapping[str, float] | None = None,
        error: Any = None,
    ) -> None:
        """Report the outcome of this use; drives the resource's health/quota stats. (Synchronous, cannot be interrupted.)"""
        self.pool._report(self, ok=ok, latency_ms=latency_ms, usage=usage, error=error)

    def degrade(self, reason: str = "") -> None:
        """Proactively circuit-break this resource temporarily (e.g. you judge its quota to be exhausted)."""
        self.pool._degrade(self, reason)

    # ---------------------------------------------------------------- release
    def release_now(self) -> bool:
        """Synchronous, idempotent release. Returns True only if this call actually returned it."""
        if self.released:
            return False
        if self.ctx is not None:
            # Duck-typed contexts (tests, custom drivers) may not implement tracking.
            untrack = getattr(self.ctx, "_untrack", None)
            if untrack is not None:
                untrack(self)
        return self.pool._release(self)

    async def release(self) -> None:
        """``await lease.release()`` is equivalent to :meth:`release_now`; provided only for API symmetry."""
        self.release_now()

    def __repr__(self) -> str:  # pragma: no cover - for debugging convenience
        state = "released" if self.released else "held"
        return f"<Lease {self.slot.resource.id} {state} {self.held_ms:.0f}ms>"


class Bus:
    """A minimal publish/subscribe bus for lightweight cross-pipeline signal synchronization."""

    def __init__(self, clock: Any = None) -> None:
        self.clock = clock or _RealClock()
        self._topics: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}

    def publish(self, topic: str, **data: Any) -> int:
        payload = {"ts": self.clock.now(), "topic": topic, **data}
        delivered = 0
        for name, queues in list(self._topics.items()):
            if name != topic and name != "*":
                continue
            for queue in list(queues):
                try:
                    queue.put_nowait(payload)
                    delivered += 1
                except asyncio.QueueFull:
                    pass
        return delivered

    def subscribe(self, topic: str = "*", *, maxsize: int = 1024) -> AsyncIterator[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self._topics.setdefault(topic, []).append(queue)

        async def _gen() -> AsyncIterator[dict[str, Any]]:
            try:
                while True:
                    yield await queue.get()
            finally:
                if queue in self._topics.get(topic, []):
                    self._topics[topic].remove(queue)

        return _gen()


class _RealClock:
    __slots__ = ()

    @staticmethod
    def now() -> float:
        return time.monotonic()

    @staticmethod
    async def sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)


def _percentile(samples: "deque[float]", q: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    index = min(len(ordered) - 1, int(q * (len(ordered) - 1)))
    return round(ordered[index], 3)


def _short(value: Any) -> str:
    text = "" if value is None else f"{type(value).__name__}: {value}"
    return text[:300]
