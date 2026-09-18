"""Handoff —— **advanced, opt-in**: a task may *skip ahead* by returning a directive instead of a value.

Ordinary pipelines never touch this module: without a ``control`` declaration on the pipeline, a task
that returns a :class:`Handoff` is a configuration error and nothing else changes (see
``pipeline.pipeline`` and ``docs/design.md`` §4.8).

The two moving parts:

* :class:`Handoff` —— the value a task returns instead of its artifact. Because it is a return, it is
  never a failure: the retry policy is not consulted, no exception class is involved, and a task-side
  ``except Exception:`` cannot swallow the control transfer. The runner intercepts the directive before
  it is ever encoded as an artifact.
* :class:`ControlPlan` —— the resolved form of the pipeline's ``control={"edges": {...}}`` declaration.
  Edges are **declared, not derived**: a handoff along an undeclared edge is a fatal error instead of a
  silent jump, and every declared edge is range-checked against the chain when the pipeline is built.

Invariants (v1):
* forward only —— a destination is always strictly later than its source, so traversal stays acyclic and
  terminates structurally;
* no joins, no cross-pipeline transfer, no target invented at runtime;
* the entry state always has a durable reference: ``Handoff.to(target)`` with no value reuses the
  artifact this task received, an explicit value becomes a payload artifact of its own.
"""

from __future__ import annotations

import difflib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .errors import FatalError, PipelineBuildError, PyAttackerError
from .task import UNSET

__all__ = ["END", "Handoff", "ControlPlan", "build_control"]

END = "end"
"""The destination spelling for "finish the pipeline here" —— ``Handoff.end()`` is its runtime form."""

CONTROL_KEYS = frozenset({"edges"})
"""Keys a ``control=`` block may set. v1 has exactly one mode, so ``mode`` is deliberately absent."""


@dataclass(frozen=True, slots=True)
class Handoff:
    """A task's directive: "this pipeline continues over there, with this entry state".

    Built through :meth:`to` / :meth:`end` rather than by hand, and returned from a task like any other
    value::

        @task("judge")
        async def judge(value: Verdict, ctx: TaskContext) -> Handoff | Report:
            if value.good_enough:
                return Handoff.end(value.as_report(), reason="already good enough")
            if not value.needs_metrics:
                return Handoff.to("report", value.as_report(), reason="metrics not needed")
            return await write_report(value)

    Attributes:
        target: A task name, a task's seq, or ``None`` for ``END``.
        value: The target's entry state; :data:`~pyattacker.task.UNSET` means "reuse the artifact this
            task received" (an explicit ``None`` is a real payload, not "no value").
        reason: Free-form explanation, recorded in the handoff ledger and the ``pipeline.handoff`` event.
    """

    target: str | int | None
    value: Any = UNSET
    reason: str = ""
    operation: str = "forward"

    def __post_init__(self) -> None:
        if self.operation not in ("forward", "rewind", "retry_all"):
            raise FatalError(f"unknown handoff operation {self.operation!r}")
        if self.operation == "rewind" and (self.value is UNSET or self.target is None):
            raise FatalError("rewind requires a task target and an explicit value")
        if self.operation == "retry_all" and (self.value is not UNSET or self.target != 0):
            raise FatalError("retry_all accepts no replacement value and targets seq 0")
        # FatalError, not a build error: the directive is normally *built inside the task*, and an
        # authoring mistake must never be retried (`FatalError` is outside every retry policy).
        if self.target is not None and not isinstance(self.target, (str, int)):
            raise FatalError(
                f"a Handoff target must be a task name, a seq or END, got {type(self.target).__name__}"
            )
        if isinstance(self.target, bool):  # bool is an int subclass; `Handoff.to(True)` is always a typo
            raise FatalError("a Handoff target must be a task name or a seq, not a bool")
        if isinstance(self.target, str) and not self.target:
            raise FatalError("a Handoff target name must not be empty")
        if isinstance(self.target, int) and self.target < 0:
            raise FatalError(f"a Handoff target seq must be >= 0, got {self.target}")
        if not isinstance(self.reason, str):
            raise FatalError(f"a Handoff reason must be a string, got {type(self.reason).__name__}")

    @classmethod
    def to(cls, target: str | int, value: Any = UNSET, *, reason: str = "") -> "Handoff":
        """Continue at ``target`` (a task name, a seq, or ``"end"``) with ``value`` as its entry state."""
        if target == END:
            return cls(None, value, reason)
        return cls(target, value, reason)

    @classmethod
    def end(cls, value: Any = UNSET, *, reason: str = "") -> "Handoff":
        """Finish the pipeline successfully here, with ``value`` as the final artifact."""
        return cls(None, value, reason)

    @classmethod
    def rewind(cls, target: str | int, value: Any = UNSET, *, reason: str = "") -> "Handoff":
        """Re-enter an earlier task with author-selected state (an explicit value is required)."""
        return cls(target, value, reason, "rewind")

    @classmethod
    def retry_all(cls, *, reason: str = "") -> "Handoff":
        """Restart this pipeline from its original bound seed."""
        return cls(0, UNSET, reason, "retry_all")

    @property
    def is_end(self) -> bool:
        return self.target is None

    @property
    def reuses_input(self) -> bool:
        """Whether the target enters with the artifact this task received, rather than a new payload."""
        return self.value is UNSET

    def __repr__(self) -> str:  # the payload is omitted on purpose: it can be arbitrarily large
        if self.operation == "retry_all":
            return f"Handoff.retry_all(reason={self.reason!r})"
        if self.operation == "rewind":
            return f"Handoff.rewind({self.target!r}, value=<payload>, reason={self.reason!r})"
        head = "Handoff.end(" if self.target is None else f"Handoff.to({self.target!r}, "
        parts = [] if self.value is UNSET else ["value=<payload>"]
        parts.append(f"reason={self.reason!r}")
        return head + ", ".join(parts) + ")"


@dataclass(frozen=True)
class ControlPlan:
    """The resolved, validated form of a pipeline's ``control=`` declaration.

    Resolution happens once, at pipeline-build time: names become seqs, "end" becomes ``None``, and every
    edge is range-checked (both ends exist, the destination is strictly later). The plan is then the
    single source of truth at runtime — a returned :class:`Handoff` is matched against ``edges``, so an
    undeclared jump fails loudly instead of silently rewriting the pipeline's shape.

    Attributes:
        task_names: The chain's task names, by seq — used for resolution, error messages and rendering.
        edges: ``{from_seq: (destination seqs…)}`` with ``None`` for ``END``; both keys and destinations
            are sorted, so the plan (and therefore ``spec_digest``) does not depend on declaration order.
    """

    task_names: tuple[str, ...] = ()
    edges: Mapping[int, tuple[int | None, ...]] = field(default_factory=dict)

    rewind_edges: Mapping[int, tuple[int, ...]] = field(default_factory=dict)
    retry_all_sources: tuple[int, ...] = ()
    max_handoffs: int | None = None

    @property
    def backward_enabled(self) -> bool:
        return bool(self.rewind_edges or self.retry_all_sources)

    def __post_init__(self) -> None:
        object.__setattr__(self, "rewind_edges", MappingProxyType({k: tuple(v) for k, v in self.rewind_edges.items()}))
        object.__setattr__(self, "retry_all_sources", tuple(self.retry_all_sources))
        if self.backward_enabled:
            if isinstance(self.max_handoffs, bool) or not isinstance(self.max_handoffs, int) or self.max_handoffs <= 0:
                raise PipelineBuildError("control.max_handoffs: a positive finite integer is required")
            for source, targets in self.rewind_edges.items():
                if isinstance(source, bool) or not isinstance(source, int) or not 0 <= source < len(self.task_names):
                    raise PipelineBuildError("control.rewind: invalid source")
                if not targets or any(isinstance(t, bool) or not isinstance(t, int) or not 0 <= t < source for t in targets):
                    raise PipelineBuildError("control.rewind: targets must be strictly earlier tasks")
            if any(isinstance(t, bool) or not isinstance(t, int) or not 0 <= t < len(self.task_names) for t in self.retry_all_sources):
                raise PipelineBuildError("control.retry_all: invalid source")
        object.__setattr__(self, "task_names", tuple(self.task_names))
        object.__setattr__(self, "edges", MappingProxyType({
            source: tuple(targets) for source, targets in self.edges.items()
        }))
        for from_seq, targets in self.edges.items():
            if isinstance(from_seq, bool) or not isinstance(from_seq, int):
                raise PipelineBuildError("control: source seq must be an integer")
            if not 0 <= from_seq < len(self.task_names):
                raise PipelineBuildError(f"control: no task at seq {from_seq}")
            for target in targets:
                if target is None and from_seq == len(self.task_names) - 1:
                    raise PipelineBuildError("control: end from the last task has no effect")
                if target is not None and (isinstance(target, bool) or not isinstance(target, int)
                                           or not from_seq < target < len(self.task_names)):
                    raise PipelineBuildError(
                        f"control: handoffs are forward-only, but {from_seq} -> {target} is not"
                    )

    # -------------------------------------------------------------- resolution
    def targets(self, from_seq: int) -> tuple[int | None, ...]:
        """The destinations declared for the task at ``from_seq`` (empty when it declares none)."""
        return tuple(self.edges.get(from_seq, ()))

    def allows(self, from_seq: int, target: str | int | None, operation: str = "forward") -> int | None:
        """Resolve one runtime target against the declared edges, or raise :class:`FatalError`.

        The error is fatal by design: an undeclared edge is an authoring mistake, so it must not be
        retried, silently ignored, or turned into an ordinary value.
        """
        destination = self.resolve_target(target, where=f"task {self.task_names[from_seq]!r}")
        if operation == "retry_all":
            if from_seq not in self.retry_all_sources:
                raise FatalError(f"task {self.task_names[from_seq]!r} declares no retry_all permission")
            return 0
        if operation == "rewind":
            if destination not in self.rewind_edges.get(from_seq, ()):
                raise FatalError(f"task {self.task_names[from_seq]!r} returned an undeclared rewind to {target!r}")
            return destination

        if destination is None and from_seq == len(self.task_names) - 1:
            raise FatalError(
                f"task {self.task_names[from_seq]!r} is the last task, so Handoff.end() has no effect; "
                "return the value instead"
            )
        declared = self.edges.get(from_seq, ())
        if destination not in declared:
            detail = (
                "the pipeline declares only [" + ", ".join(self._render(dest) for dest in declared) + "] for it"
                if declared
                else "the pipeline declares no edge from it"
            )
            raise FatalError(
                f"task {self.task_names[from_seq]!r} (seq {from_seq}) handed off to "
                f"{self._render(destination)}, but {detail}; add the edge to control={{'edges': {{...}}}}"
            )
        return destination

    def resolve_target(self, target: str | int | None, *, where: str) -> int | None:
        """Turn a name / seq / ``"end"`` / ``None`` into a destination, or raise :class:`FatalError`.

        ``None`` is accepted alongside ``"end"`` because that is the runtime form of
        :meth:`Handoff.end` — the declaration spells it ``"end"``, a returned directive carries ``None``.
        """
        if target is None or target == END:
            return None
        if isinstance(target, bool) or not isinstance(target, (str, int)):
            raise FatalError(f"{where} handed off to {target!r}, which is not a task name, a seq or END")
        if isinstance(target, int):
            if not 0 <= target < len(self.task_names):
                raise FatalError(
                    f"{where} handed off to seq {target}, but the pipeline has "
                    f"{len(self.task_names)} task(s) (seq 0..{len(self.task_names) - 1})"
                )
            return target
        return _lookup_name(target, self.task_names, where=where, kind="destination", error=FatalError)

    # ------------------------------------------------------------------ views
    def fingerprint(self) -> dict[str, Any]:
        """The canonical, digestable form: resolved seqs, so the spelling of a declaration is irrelevant."""
        result = {
            "edges": {
                str(from_seq): [("end" if target is None else target) for target in targets]
                for from_seq, targets in sorted(self.edges.items())
            }
        }
        if self.backward_enabled:
            result.update(rewind={str(k): list(v) for k, v in sorted(self.rewind_edges.items())},
                          retry_all=list(sorted(self.retry_all_sources)), max_handoffs=self.max_handoffs)
        return result

    def describe(self) -> dict[str, Any]:
        """A reusable declaration, with numeric seqs for ambiguous or reserved names."""

        def token(seq: int) -> str | int:
            name = self.task_names[seq]
            return seq if name == END or self.task_names.count(name) > 1 else name

        result = {
            "edges": {
                token(from_seq): [END if target is None else token(target) for target in targets]
                for from_seq, targets in sorted(self.edges.items())
            }
        }
        if self.backward_enabled:
            if not self.edges:
                result.pop("edges")
            if self.rewind_edges:
                result["rewind"] = {token(k): [token(t) for t in v] for k, v in sorted(self.rewind_edges.items())}
            if self.retry_all_sources:
                result["retry_all"] = [token(s) for s in sorted(self.retry_all_sources)]
            result["max_handoffs"] = self.max_handoffs
        return result

    def _render(self, target: int | None) -> str:
        if target is None:
            return END
        name = self.task_names[target]
        # A repeated task name needs its seq to be unambiguous; a unique one reads better bare.
        return f"{name}#{target}" if self.task_names.count(name) > 1 else name


def _lookup_name(
    name: str,
    task_names: Sequence[str],
    *,
    where: str,
    kind: str,
    error: type[PyAttackerError] = PipelineBuildError,
) -> int:
    """Resolve a task name to its seq; an ambiguous name is an error, not a guess.

    ``error`` is the class to raise: :class:`~pyattacker.errors.PipelineBuildError` while a pipeline is
    being built (where the declarative layer turns it into a field-path config error), and
    :class:`~pyattacker.errors.FatalError` for the same mistake made at runtime.
    """
    matches = [index for index, candidate in enumerate(task_names) if candidate == name]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        known = sorted(set(task_names))
        close = difflib.get_close_matches(name, known, n=1)
        hint = f" (did you mean {close[0]!r}?)" if close else ""
        raise error(
            f"{where} names unknown {kind} task {name!r}{hint}; tasks: {[(i, n) for i, n in enumerate(task_names)]}"
        )
    raise error(
        f"{where} names {kind} task {name!r}, which appears at seqs {matches} — "
        "use the numeric seq to say which one"
    )


def _build_forward(raw: Any, task_names: Sequence[str]) -> ControlPlan:
    """Resolve and structurally validate a ``control=`` declaration.

    The single validation entry for both worlds: ``pipeline(control=...)`` calls it directly, and the
    declarative layer calls it so a config file reports the same problems with a field path. Every
    message therefore starts with a path relative to the block (``control.edges['judge'][0]: …``), which
    the declarative layer prefixes with ``pipeline.``.

    Structural checks only: a handoff payload is an arbitrary argument, not the source's normal return
    type, so ``source.returns -> target.accepts`` is deliberately *not* checked — it would reject valid
    handoffs and accept invalid ones.
    """
    if not isinstance(raw, Mapping):
        raise PipelineBuildError(
            f"control: must be a mapping like {{'edges': {{'judge': ['report', 'end']}}}}, "
            f"got {type(raw).__name__}"
        )
    unknown = sorted(str(key) for key in raw if key not in CONTROL_KEYS)
    if unknown:
        note = (
            "; v1 has exactly one mode, so 'edges' is the only key"
            if "mode" in unknown
            else f"; available: {sorted(CONTROL_KEYS)}"
        )
        raise PipelineBuildError(f"control: unknown field(s) {unknown}{note}")
    if "edges" not in raw:
        raise PipelineBuildError("control: missing required field 'edges'")
    raw_edges = raw["edges"]
    if not isinstance(raw_edges, Mapping):
        raise PipelineBuildError(
            f"control.edges: must be a mapping of task -> [destinations], got {type(raw_edges).__name__}"
        )
    if not raw_edges:
        raise PipelineBuildError("control.edges: is empty; declare at least one edge or drop the control block")

    names = tuple(task_names)
    edges: dict[int, tuple[int | None, ...]] = {}
    for token, destinations in raw_edges.items():
        path = f"control.edges[{token!r}]"
        if token == END:
            raise PipelineBuildError(f"{path}: 'end' is a destination, not a task")
        if isinstance(token, bool) or not isinstance(token, (str, int)):
            raise PipelineBuildError(
                f"{path}: an edge source must be a task name or a seq, got {type(token).__name__}"
            )
        # JSON/TOML object keys are strings. Accept canonical seq keys when they
        # do not name an actual task, so numeric disambiguation works in every format.
        source_seq = token
        if isinstance(token, str) and token not in names and token.isascii() and token.isdecimal():
            source_seq = int(token) if str(int(token)) == token else token
        if isinstance(source_seq, int):
            if not 0 <= source_seq < len(names):
                raise PipelineBuildError(
                    f"{path}: no task at seq {source_seq}; the chain has {len(names)} task(s) "
                    f"(seq 0..{len(names) - 1})"
                )
            from_seq = source_seq
        else:
            from_seq = _lookup_name(token, names, where=path, kind="source")
        if from_seq in edges:
            raise PipelineBuildError(f"{path}: duplicate source for seq {from_seq}; use one name or seq entry")
        if not isinstance(destinations, (list, tuple)):
            raise PipelineBuildError(
                f"{path}: destinations must be a list, got {type(destinations).__name__}"
            )
        if not destinations:
            raise PipelineBuildError(f"{path}: has no destinations; drop the entry instead")

        resolved: set[int | None] = set()
        for index, destination in enumerate(destinations):
            target = _build_target(
                destination, names, from_seq=from_seq, path=f"{path}[{index}]", source=names[from_seq]
            )
            resolved.add(target)
        edges[from_seq] = tuple(sorted(resolved, key=lambda value: (value is None, value)))

    return ControlPlan(task_names=names, edges=edges)


def _build_target(destination: Any, names: Sequence[str], *, from_seq: int, path: str, source: str) -> int | None:
    """Validate one declared destination of one declared edge."""
    if destination == END:
        if from_seq == len(names) - 1:
            raise PipelineBuildError(
                f"{path}: 'end' from the last task {source!r} has no effect (it is already the end); "
                "drop the edge"
            )
        return None
    if isinstance(destination, bool) or not isinstance(destination, (str, int)):
        raise PipelineBuildError(
            f"{path}: a destination must be a task name, a seq or 'end', got {type(destination).__name__}"
        )
    if isinstance(destination, int):
        if not 0 <= destination < len(names):
            raise PipelineBuildError(
                f"{path}: no task at seq {destination}; the chain has {len(names)} task(s) "
                f"(seq 0..{len(names) - 1})"
            )
        target: int = destination
    else:
        target = _lookup_name(destination, names, where=path, kind="destination")
    if target <= from_seq:
        raise PipelineBuildError(
            f"{path}: destination {names[target]!r} (seq {target}) is not later than the source "
            f"{source!r} (seq {from_seq}); v1 handoffs are forward-only"
        )
    return target


def build_control(raw: Any, task_names: Sequence[str]) -> ControlPlan:
    """Build the shared forward/backward declaration with canonical resolved identities."""
    if not isinstance(raw, Mapping) or not any(k in raw for k in ("rewind", "retry_all", "max_handoffs")):
        return _build_forward(raw, task_names)
    unknown = set(raw) - {"edges", "rewind", "retry_all", "max_handoffs"}
    if unknown:
        # Same shape as `_build_forward`'s message: a typo in a backward declaration deserves the same
        # "available:" hint as a typo in a forward one, and `mode` stays called out because it is the
        # field people reach for expecting the two directions to be selectable through it.
        note = (
            "; v1 has exactly one forward mode, so 'edges' is the only forward key"
            if "mode" in unknown
            else "; available: ['edges', 'rewind', 'retry_all', 'max_handoffs']"
        )
        raise PipelineBuildError(f"control: unknown field(s) {sorted(map(str, unknown))}{note}")
    names = tuple(task_names)
    forward = _build_forward({"edges": raw["edges"]}, names) if "edges" in raw else ControlPlan(names)

    def resolve(token: Any, path: str, *, source: bool = False) -> int:
        if source and isinstance(token, str) and token not in names and token.isascii() and token.isdecimal() and str(int(token)) == token:
            token = int(token)
        if isinstance(token, bool) or not isinstance(token, (str, int)):
            raise PipelineBuildError(f"{path}: expected a task name or seq")
        if isinstance(token, str):
            if token == END:
                raise PipelineBuildError(f"{path}: end is not a backward task target")
            return _lookup_name(token, names, where=path, kind="task")
        if not 0 <= token < len(names):
            raise PipelineBuildError(f"{path}: unknown task seq {token}")
        return token

    rewinds = raw.get("rewind", {})
    if not isinstance(rewinds, Mapping) or ("rewind" in raw and not rewinds):
        raise PipelineBuildError("control.rewind: expected a nonempty mapping")
    edges: dict[int, tuple[int, ...]] = {}
    for source, targets in rewinds.items():
        path = f"control.rewind[{source!r}]"
        seq = resolve(source, path, source=True)
        if seq in edges:
            raise PipelineBuildError(f"{path}: duplicate source")
        if not isinstance(targets, (list, tuple)) or not targets:
            raise PipelineBuildError(f"{path}: expected a nonempty destination list")
        values = tuple(sorted({resolve(t, f"{path}[{i}]") for i, t in enumerate(targets)}))
        if any(t >= seq for t in values):
            raise PipelineBuildError(f"{path}: rewind destinations must be strictly earlier than source")
        edges[seq] = values
    sources = raw.get("retry_all", [])
    if not isinstance(sources, (list, tuple)) or ("retry_all" in raw and not sources):
        raise PipelineBuildError("control.retry_all: expected a nonempty source list")
    # Resolved first, deduplicated second — never the other way round. A set comprehension would
    # silently collapse aliases that name the same station (`["b", 1]`, or the numeric-string form
    # of an int), which is the same declaration error `rewind` already rejects for its sources.
    resolved: list[int] = []
    seen: set[int] = set()
    for index, token in enumerate(sources):
        path = f"control.retry_all[{index}]"
        seq = resolve(token, path, source=True)
        if seq in seen:
            raise PipelineBuildError(f"{path}: duplicate source {token!r} (already declared as seq {seq})")
        seen.add(seq)
        resolved.append(seq)
    retry = tuple(sorted(resolved))
    if not edges and not retry:
        raise PipelineBuildError("control: max_handoffs requires backward operations")
    return ControlPlan(names, forward.edges, edges, retry, raw.get("max_handoffs"))
