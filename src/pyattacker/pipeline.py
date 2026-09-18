"""Pipeline —— a linearly chained sequence of tasks; the unit of **completion** and the unit of **resume**.

* ``a | b | c`` builds a chain; construction time validates that adjacent tasks' artifact types chain.
* ``template.map(seeds)`` expands dataset rows into pipelines that are **semantically fully independent**.
* ``pipeline_key`` is determined by (task-chain fingerprint + seed content + repeat index) —— content-addressed,
  hence naturally idempotent: re-running the same sample never produces a second pipeline, and resume skips it.
* The task-chain fingerprint includes each task's **source digest** by default: changing task code is the same
  as swapping the pipeline, so old checkpoints are never reused by mistake (disable with ``include_code=False``).
"""

from __future__ import annotations

import functools
import inspect
import itertools
import operator
import typing
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .artifact import DEFAULT_REGISTRY, CodecRegistry, canonical_json, digest_of
from .errors import PipelineBuildError
from .handoff import ControlPlan, Handoff, build_control
from .task import TaskSpec

__all__ = ["Chain", "PipelineTemplate", "PipelineSpec", "pipeline"]


@dataclass(frozen=True)
class Chain:
    """An ordered, not-yet-validated sequence of tasks —— what ``a | b | c`` builds before :func:`pipeline` runs it.

    Invariants: immutable; ``|`` always returns a new ``Chain`` rather than mutating either side,
    so an intermediate chain can be reused as a building block for several pipelines.

    Collaborators: :func:`pipeline` is what turns a finished ``Chain`` into a validated
    :class:`PipelineTemplate` (checking that adjacent tasks' artifact types actually chain).
    """

    tasks: tuple[TaskSpec, ...]

    def __or__(self, other: Any) -> "Chain":
        if isinstance(other, TaskSpec):
            return Chain((*self.tasks, other))
        if isinstance(other, Chain):
            return Chain((*self.tasks, *other.tasks))
        raise PipelineBuildError(f"pipelines can only be composed from TaskSpec or Chain, got {type(other).__name__}")

    def __len__(self) -> int:
        return len(self.tasks)

    def __iter__(self) -> Iterator[TaskSpec]:
        return iter(self.tasks)

    def __getitem__(self, index: int) -> TaskSpec:
        return self.tasks[index]


def _without_handoff(annotation: Any) -> Any:
    """Drop a ``Handoff`` member from an annotation, because a directive is never an artifact value.

    A task that may hand off declares it in its return type (``-> Handoff | Report``). The remaining
    members still have to chain into the next task; an annotation of exactly ``Handoff`` chains with
    anything, because such a task produces no artifact at all on that path. The accepted side is
    treated the same way for symmetry — a value passed across an edge is never a ``Handoff``.
    """
    if annotation is Handoff:
        return Any
    members = typing.get_args(annotation)
    if not members or Handoff not in members:
        return annotation
    remaining = tuple(member for member in members if member is not Handoff)
    if not remaining:  # pragma: no cover - defensive (Handoff alone is handled above)
        return Any
    if len(remaining) == 1:
        return remaining[0]
    # The union is built from a runtime tuple, so `X | Y` is spelled as a fold rather than statically.
    return functools.reduce(operator.or_, remaining)


def _compatible(produced: Any, accepted: Any) -> bool:
    produced = _without_handoff(produced)
    accepted = _without_handoff(accepted)
    if produced is Any or accepted is Any:
        return True
    if produced is inspect.Parameter.empty or accepted is inspect.Parameter.empty:
        return True
    if produced == accepted:
        return True
    origin_p, origin_a = typing.get_origin(produced), typing.get_origin(accepted)
    if origin_p is not None or origin_a is not None:
        # One side parameterised, the other bare (`dict[str, Any]` -> `dict`): the bare form is
        # simply less specific, so ordinary subclass rules decide. Two parameterised forms stay
        # strict (`list[int]` -> `list[str]` is still rejected), because comparing element types
        # via str() is the only thing that keeps that check meaningful without evaluating them.
        if origin_p is not None and origin_a is None and isinstance(accepted, type):
            try:
                return issubclass(origin_p, accepted)
            except TypeError:  # pragma: no cover - defensive
                return False
        if origin_a is not None and origin_p is None and isinstance(produced, type):
            try:
                return issubclass(origin_a, produced)
            except TypeError:  # pragma: no cover - defensive
                return False
        return str(produced) == str(accepted)
    try:
        if isinstance(produced, type) and isinstance(accepted, type):
            return issubclass(produced, accepted)
    except TypeError:  # pragma: no cover - defensive
        return False
    return False


def _validate_chain(tasks: Sequence[TaskSpec]) -> None:
    if not tasks:
        raise PipelineBuildError("pipeline cannot be empty")
    for left, right in itertools.pairwise(tasks):
        if not _compatible(left.returns, right.accepts):
            raise PipelineBuildError(
                f"artifact types do not chain: task {left.name!r} produces "
                f"{getattr(left.returns, '__name__', left.returns)}, "
                f"but task {right.name!r} requires {getattr(right.accepts, '__name__', right.accepts)}"
            )


@dataclass(frozen=True)
class PipelineTemplate:
    """Static definition of a pipeline (without seed data)."""

    name: str
    tasks: tuple[TaskSpec, ...]
    tags: dict[str, Any] = field(default_factory=dict)
    spec_digest: str = ""
    registry: CodecRegistry = field(default=DEFAULT_REGISTRY, compare=False, repr=False)
    # The resolved ``control=`` declaration (**advanced**), or None for an ordinary linear pipeline. Kept
    # after ``registry`` so positional construction keeps its meaning, and excluded from comparison like
    # the registry: identity lives in ``spec_digest``, which folds the plan in when there is one.
    control: ControlPlan | None = field(default=None, compare=False)

    # ------------------------------------------------------- construction
    @property
    def chain(self) -> Chain:
        return Chain(self.tasks)

    @property
    def n_tasks(self) -> int:
        return len(self.tasks)

    def task_names(self) -> list[str]:
        return [t.name for t in self.tasks]

    def describe(self) -> dict[str, Any]:
        described = {
            "name": self.name,
            "tasks": self.task_names(),
            "tags": dict(self.tags),
            "spec_digest": self.spec_digest,
        }
        if self.control is not None:
            described["control"] = self.control.describe()
        return described

    # ------------------------------------------------------------ binding
    def bind(self, seed: Any, *, key: str | None = None, repeat: int = 0) -> "PipelineSpec":
        encoded = self.registry.dump(seed)
        if key is None:
            key = digest_of(
                canonical_json({"spec": self.spec_digest, "seed": encoded.digest, "repeat": repeat})
            )
        return PipelineSpec(
            template=self,
            pipeline_id=key,
            key=key,
            seed=seed,
            seed_digest=encoded.digest,
            repeat=repeat,
            spec_digest=self.spec_digest,
        )

    def map(
        self,
        seeds: Iterable[Any],
        *,
        repeats: int = 1,
        key_of: Callable[[Any], str] | None = None,
    ) -> Iterator["PipelineSpec"]:
        """Expand seeds into a stream of pipelines. ``repeats>1`` serves pass@k / self-consistency sampling."""
        repeats = max(1, int(repeats))
        for seed in seeds:
            base_key = key_of(seed) if key_of is not None else None
            for repeat in range(repeats):
                explicit = None
                if base_key is not None:
                    explicit = base_key if repeats == 1 else f"{base_key}#{repeat}"
                yield self.bind(seed, key=explicit, repeat=repeat)


@dataclass(frozen=True)
class PipelineSpec:
    """A schedulable pipeline instance bound to a seed."""

    template: PipelineTemplate
    pipeline_id: str
    key: str
    seed: Any
    seed_digest: str
    repeat: int = 0
    spec_digest: str = ""

    @property
    def name(self) -> str:
        return self.template.name

    @property
    def tasks(self) -> tuple[TaskSpec, ...]:
        return self.template.tasks

    @property
    def n_tasks(self) -> int:
        return len(self.template.tasks)

    @property
    def control(self) -> ControlPlan | None:
        """The pipeline's resolved control declaration, or ``None`` when it is an ordinary chain."""
        return self.template.control

    def __repr__(self) -> str:  # pragma: no cover - debugging
        return f"<PipelineSpec {self.name} id={self.pipeline_id[:12]} tasks={self.n_tasks}>"


def compute_spec_digest(
    tasks: Sequence[TaskSpec], *, include_code: bool = True, control: ControlPlan | None = None
) -> str:
    """The pipeline's identity: the task fingerprints, plus the control block **only when there is one**.

    That "only when" is a compatibility contract, not an optimisation: folding an always-present key in
    would change every stored ``spec_digest``, and with it every default pipeline id, checkpoint and
    shard assignment — the migration the ``v2:`` bump already paid for once. A control-free pipeline
    therefore digests exactly what it digested before this feature existed.
    """
    payload: list[Any] = [t.fingerprint(include_code=include_code) for t in tasks]
    if control is not None:
        payload.append({"control": control.fingerprint()})
    return "v2:" + digest_of(canonical_json(payload))


def pipeline(
    name: str,
    *tasks_or_chain: Any,
    tags: Mapping[str, Any] | None = None,
    include_code: bool = True,
    registry: CodecRegistry | None = None,
    control: Mapping[str, Any] | None = None,
) -> PipelineTemplate:
    """Declare a pipeline: ``pipeline("qa", fetch | ask | judge | metrics)``.

    ``control`` (**advanced**, opt-in) declares which task may hand off where::

        pipeline("qa", retrieve | ask | judge | report,
                 control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})

    A destination is a task name, a task's seq or ``"end"``, and it must be strictly later than its
    source (v1 is forward-only). Without the block nothing changes: a returned ``Handoff`` is then a
    configuration error, not a silent jump. See ``docs/design.md`` §4.8.
    """
    if not tasks_or_chain:
        raise PipelineBuildError("pipeline needs at least one task")
    if len(tasks_or_chain) == 1:
        head = tasks_or_chain[0]
        if isinstance(head, Chain):
            items = head.tasks
        elif isinstance(head, TaskSpec):
            items = (head,)
        else:
            raise PipelineBuildError(f"pipeline accepts only tasks or a chain, got {type(head).__name__}")
    else:
        items = tuple(tasks_or_chain)
        for item in items:
            if not isinstance(item, TaskSpec):
                raise PipelineBuildError(
                    f"pipeline members must be tasks, got {type(item).__name__}; the multi-argument form cannot mix in a chain"
                )
    _validate_chain(items)
    plan = build_control(control, [t.name for t in items]) if control is not None else None
    return PipelineTemplate(
        name=name,
        tasks=tuple(items),
        tags=dict(tags or {}),
        spec_digest=compute_spec_digest(items, include_code=include_code, control=plan),
        registry=registry or DEFAULT_REGISTRY,
        control=plan,
    )


def with_retry(spec: TaskSpec, **retry: Any) -> TaskSpec:
    """Convenience: override one task's retry parameters in place within a chain, e.g. ``with_retry(ask, max_attempts=4)``."""
    from dataclasses import replace as _replace

    return _replace(spec, retry=_replace(spec.retry, **retry))
