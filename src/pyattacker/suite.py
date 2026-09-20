"""Compose independent experiments without creating independent schedulers."""

from __future__ import annotations

import dataclasses
import re
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .artifact import canonical_json, digest_of
from .errors import ConfigError
from .pipeline import PipelineSpec


def validate_id(value: Any, path: str) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value)
        or value.lower()
        in {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(10)), *(f"lpt{i}" for i in range(10))}
    ):
        raise ConfigError(f"{path}: use 1-80 ASCII letters, digits, '-' or '_'; reserved names are forbidden")
    return value


def namespace(suite_id: str, experiment_id: str) -> str:
    return "suite-" + digest_of(canonical_json([suite_id, experiment_id])) + "-"


class _SuiteStream:
    """Bind invocation selection before stale-owner recovery, then stream inputs."""

    def __init__(self, suite: Any, store: Any, selected: Sequence[Any], limit: int | None) -> None:
        self.suite, self.store, self.selected = suite, store, selected
        self.iterator = suite._pipelines(store, selected, limit)

    def prepare_run(self, store: Any) -> None:
        if store is not self.store or not getattr(store, "suite_store", False):
            raise ConfigError("Suite pipelines require their owning SuiteStore in both layouts")
        store.select_experiments(self.suite.id, [exp.id for exp in self.selected])

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> PipelineSpec:
        return next(self.iterator)

    def close(self) -> None:
        self.iterator.close()


@dataclass
class ExperimentSpec:
    """A replayable pipeline factory and its local-to-Runner pool mapping.

    ``definition_digest`` must cover application configuration and input identity;
    it must not contain secrets. The config loader computes it automatically.
    """

    id: str
    factory: Callable[[], Iterable[PipelineSpec]]
    definition_digest: str
    pool_aliases: dict[str, str] = field(default_factory=dict)
    label: str = ""
    config_path: str = ""
    ignored_run_fields: list[str] = field(default_factory=list)
    limit: int | None = None

    def __post_init__(self) -> None:
        validate_id(self.id, "experiment.id")
        if (
            not isinstance(self.definition_digest, str)
            or not self.definition_digest
            or not callable(self.factory)
        ):
            raise ConfigError("experiment requires a replayable factory and definition_digest")
        if self.limit is not None and (type(self.limit) is not int or self.limit < 0):
            raise ConfigError("experiment.limit must be a non-negative integer")


@dataclass
class SuiteSpec:
    id: str
    experiments: Sequence[ExperimentSpec]
    output_root: str
    layout: str = "combined"
    pools: list[Any] = field(default_factory=list)
    run: dict[str, Any] = field(default_factory=dict)
    path: str = ""
    unresolved_env: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        validate_id(self.id, "suite.id")
        if self.layout not in ("combined", "by_experiment"):
            raise ConfigError("output.layout must be combined or by_experiment")
        if not self.output_root:
            raise ConfigError("output.root is required")
        ids = [exp.id.casefold() for exp in self.experiments]
        if not ids or len(set(ids)) != len(ids):
            raise ConfigError("experiments must be nonempty with case-insensitively unique IDs")
        names = [pool.name for pool in self.pools]
        if len(set(names)) != len(names):
            raise ConfigError("Suite pool names must be unique")
        for exp in self.experiments:
            if set(exp.pool_aliases.values()) - set(names):
                raise ConfigError(f"experiment {exp.id}: unregistered pool binding")
        if "store" in self.run:
            raise ConfigError("Suite uses output.root; run.store is not allowed")

    def select(self, experiments: Sequence[str] | None = None) -> list[ExperimentSpec]:
        wanted = set(experiments) if experiments is not None else {exp.id for exp in self.experiments}
        unknown = wanted - {exp.id for exp in self.experiments}
        if unknown or not wanted:
            raise ConfigError(f"unknown or empty experiment selection: {sorted(unknown)}")
        return [exp for exp in self.experiments if exp.id in wanted]

    def describe(self) -> dict[str, Any]:
        return {
            "config": self.path,
            "suite_id": self.id,
            "output": {"root": self.output_root, "layout": self.layout},
            "run": dict(self.run),
            "unresolved_env": sorted(set(self.unresolved_env)),
            "experiments": [
                {
                    "id": e.id,
                    "label": e.label,
                    "config": e.config_path,
                    "definition_digest": e.definition_digest,
                    "pool_bindings": e.pool_aliases,
                    "ignored_run_fields": e.ignored_run_fields,
                    "limit": e.limit,
                }
                for e in self.experiments
            ],
        }

    def pipelines(
        self, store: Any, *, experiments: Sequence[str] | None = None, limit: int | None = None
    ) -> Iterator[PipelineSpec]:
        """Round-robin admission; a broken source fails only its own experiment."""
        if limit is not None and (type(limit) is not int or limit < 0):
            raise ConfigError("limit must be a non-negative integer")
        selected = self.select(experiments)
        if not getattr(store, "suite_store", False):
            raise ConfigError("Suite requires SuiteStore in both output layouts")
        return _SuiteStream(self, store, selected, limit)

    def _pipelines(
        self, store: Any, selected: Sequence[ExperimentSpec], limit: int | None
    ) -> Iterator[PipelineSpec]:
        store.prepare(self, selected)

        def source(exp: ExperimentSpec) -> Iterator[PipelineSpec]:
            yield from exp.factory()

        streams = deque((exp, source(exp), 0) for exp in selected)
        count = 0
        try:
            while streams and (limit is None or count < limit):
                exp, stream, seen = streams.popleft()
                if exp.limit is not None and seen >= exp.limit:
                    stream.close()
                    store.source_state(exp.id, exhausted=False, limited=True)
                    continue
                try:
                    spec = next(stream)
                    if not isinstance(spec, PipelineSpec) or spec.suite_id is not None:
                        raise ConfigError("experiment factory must yield ordinary PipelineSpec instances")
                except StopIteration:
                    store.source_state(exp.id, exhausted=True)
                    continue
                except Exception as exc:
                    store.source_state(exp.id, exhausted=False, error=f"{type(exc).__name__}: {exc}")
                    continue
                local_key = spec.key
                pid = namespace(self.id, exp.id) + digest_of(canonical_json(local_key))
                streams.append((exp, stream, seen + 1))
                count += 1
                yield dataclasses.replace(
                    spec,
                    pipeline_id=pid,
                    key=pid,
                    suite_id=self.id,
                    experiment_id=exp.id,
                    local_key=local_key,
                    pool_aliases=dict(exp.pool_aliases),
                    output_dir=str(store.output_dir(exp.id)),
                )
        finally:
            for exp, stream, _ in streams:
                stream.close()
                store.source_state(exp.id, exhausted=False, limited=limit is not None and count >= limit)

    def runner(self, *, on_pipeline_finished: Any = None, **overrides: Any) -> Any:
        """Create one Runner and its owned SuiteStore. Use it as a context manager."""
        from .runner import RunConfig, Runner
        from .store.suite import SuiteStore

        if "store" in overrides:
            raise ConfigError("Suite requires its output.root store in both layouts")
        config = RunConfig(**(self.run | overrides))
        store = SuiteStore.create(
            self,
            journal=config.journal,
            backend=config.artifact_backend,
            write_behind=config.write_behind,
            batch_size=config.write_batch,
            flush_interval=config.flush_interval,
        )
        config.store = store
        return Runner(config=config, pools=self.pools, on_pipeline_finished=on_pipeline_finished)


def load_suite(path: Path, raw: Mapping[str, Any], *, strict_env: bool = False) -> SuiteSpec:
    from .declarative import (
        _build_pool,
        _load_raw,
        _mapping,
        _unknown,
        _validate_pools,
        _validate_run,
        expand_env,
        load_spec,
    )

    _unknown("config", raw, {"suite", "experiments", "run", "pools", "output"})
    cfg = _mapping(raw.get("suite"), "suite")
    _unknown("suite", cfg, {"id"})
    sid = validate_id(cfg.get("id"), "suite.id")
    output = _mapping(raw.get("output"), "output")
    _unknown("output", output, {"root", "layout"})
    if not isinstance(output.get("root"), str) or not output["root"]:
        raise ConfigError("output.root: expected a nonempty directory path")
    _validate_run(raw.get("run"))
    run = dict(raw.get("run") or {})
    if "store" in run:
        raise ConfigError("Suite uses output.root; run.store is not allowed")
    _validate_pools(raw.get("pools"))
    shared_raw = dict(raw.get("pools") or {})
    pools = [_build_pool(name, spec or {}) for name, spec in shared_raw.items()]
    entries = _mapping(raw.get("experiments"), "experiments")
    experiments = []
    unresolved: list[str] = []
    for eid, entry in entries.items():
        validate_id(eid, "experiments key")
        entry = _mapping(entry, f"experiments.{eid}")
        _unknown(f"experiments.{eid}", entry, {"config", "pool_bindings", "label", "limit"})
        filename = entry.get("config")
        if not isinstance(filename, str) or not filename:
            raise ConfigError(f"experiments.{eid}.config: expected a file path")
        member_path = path.parent / filename
        if not member_path.is_file():
            raise ConfigError(f"experiments.{eid}.config: file does not exist: {member_path}")
        member_raw = expand_env(_load_raw(member_path), strict=strict_env, unresolved=unresolved)
        if "suite" in member_raw or "experiments" in member_raw:
            raise ConfigError(f"experiments.{eid}: nested suites are not supported")
        aliases = _mapping(entry.get("pool_bindings", {}), f"experiments.{eid}.pool_bindings")
        local_raw = dict(_mapping(member_raw.get("pools") or {}, f"experiments.{eid}.pools"))
        for local, target in aliases.items():
            if not isinstance(local, str) or not isinstance(target, str) or target not in shared_raw:
                raise ConfigError(f"experiments.{eid}.pool_bindings: unknown shared pool {target!r}")
            local_raw[local] = shared_raw[target]
        # Validate/build the member against the effective pool declarations, without
        # changing its source paths or the task's local resource names.
        effective = dict(member_raw, pools=local_raw)
        member = load_spec(member_path, strict_env=strict_env, _raw=effective)
        unresolved.extend(member.unresolved_env)
        mapping = {}
        for pool in member.pools:
            if pool.name in aliases:
                mapping[pool.name] = aliases[pool.name]
            else:
                local = pool.name
                pool.name = f"experiment:{eid}:{local}"
                if pool.name in shared_raw:
                    raise ConfigError(f"experiments.{eid}: pool namespace collision")
                mapping[local] = pool.name
                pools.append(pool)
        # Hash effective semantics, never persist expanded secrets. Names/tags are
        # descriptive; local source keys deliberately exclude the template name.
        definition = {
            "spec": member.template.spec_digest,
            "source": member.source,
            "pools": local_raw,
            "bindings": dict(aliases),
            "limit": entry.get("limit"),
        }

        def factory(member=member):
            key_field = member.source.get("key_field")
            seeds = (
                ({"i": i} for i in range(member.source.get("n", 10)))
                if member.source.get("kind", "range") == "range"
                else member.seeds()
            )
            yield from member.template.map(
                seeds,
                repeats=member.source.get("repeats", 1),
                key_of=(lambda seed: str(seed[key_field])) if key_field else None,
            )

        label = entry.get("label", eid)
        if not isinstance(label, str):
            raise ConfigError(f"experiments.{eid}.label must be a string")
        experiments.append(
            ExperimentSpec(
                eid,
                factory,
                digest_of(canonical_json(definition)),
                mapping,
                label,
                str(member_path),
                sorted(member.run),
                entry.get("limit"),
            )
        )
    return SuiteSpec(
        sid,
        experiments,
        str((path.parent / output["root"]).resolve()),
        output.get("layout", "combined"),
        pools,
        run,
        str(path),
        unresolved,
    )
