"""Declarative layer —— describe "composition and resources" with YAML/TOML, not logic.

A clear-eyed admission: the declarative layer can never escape Python (``use: myproj.tasks:ask_model`` still imports your code),
so this layer only does three things: **pick tasks, chain pipelines, configure resource pools**.

YAML is an optional extra (``pyyaml``), needed only to read a ``.yaml``/``.yml`` config: the base install
carries no dependencies at all, and a missing PyYAML surfaces as a ``ConfigError`` naming the extra.
TOML/JSON go through the standard library ``tomllib``/``json`` and need nothing.
"""

from __future__ import annotations

import builtins
import importlib
import json
import os
import re
import sys
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import errors as _errors
from .algorithm import resolve_algorithm
from .errors import ConfigError
from .pipeline import PipelineSpec, PipelineTemplate, pipeline
from .resource import Pool, Resource
from .task import Retrying, TaskSpec, build_task_spec
from .tasks import BUILTIN_TASKS, seed_factory

__all__ = ["DeclarativeSpec", "load_spec", "expand_env", "resolve_target"]

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def expand_env(value: Any, *, strict: bool = False, unresolved: list[str] | None = None) -> Any:
    """Recursively expand ``${VAR}`` / ``${VAR:-default}``. With ``strict=True`` a missing variable is an error."""
    if isinstance(value, str):
        def _sub(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            if unresolved is not None:
                unresolved.append(name)
            if strict:
                raise ConfigError(f"environment variable not set: {name}")
            return match.group(0)

        return _ENV_RE.sub(_sub, value)
    if isinstance(value, Mapping):
        return {k: expand_env(v, strict=strict, unresolved=unresolved) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v, strict=strict, unresolved=unresolved) for v in value]
    return value


def _load_raw(path: Path) -> dict[str, Any]:
    """Read the config file, dispatching on its suffix. Nothing here is imported at module scope."""
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ModuleNotFoundError as exc:
            # Only "the extra is not installed" earns the hint, so the check is narrow: `import yaml`
            # can also fail because PyYAML is installed but something inside it is missing, and in
            # that case the real ModuleNotFoundError names the module that is actually absent —
            # telling that user to install an extra they already have would send them the wrong way.
            if exc.name != "yaml":
                raise
            # The one place PyYAML is needed, and it is an extra rather than a dependency: a machine
            # that reads JSON/TOML configs, or no config at all, installs pyattacker and nothing else.
            # Say so in the error, because the traceback a bare `import yaml` gives names neither the
            # extra nor the file that pulled it in.
            raise ConfigError(
                f"{path} is a YAML config, which needs the optional 'yaml' extra: "
                'pip install "pyattacker[yaml]" (or: uv add "pyattacker[yaml]"). '
                "JSON and TOML configs need no extra."
            ) from exc

        data = yaml.safe_load(text)
    elif suffix == ".json":
        data = json.loads(text)
    elif suffix in (".toml", ".tml"):
        import tomllib

        data = tomllib.loads(text)
    else:
        raise ConfigError(f"unsupported config format: {path.suffix} (supported: .yaml/.yml/.json/.toml)")
    if not isinstance(data, dict):
        raise ConfigError(f"config file top level must be a mapping: {path}")
    return data


def _resolve_exc(name: str) -> type[BaseException]:
    if hasattr(_errors, name):
        candidate = getattr(_errors, name)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            return candidate
    candidate = getattr(builtins, name, None)
    if isinstance(candidate, type) and issubclass(candidate, BaseException):
        return candidate
    for module_name in ("asyncio",):
        module = importlib.import_module(module_name)
        candidate = getattr(module, name, None)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            return candidate
    raise ConfigError(f"cannot resolve exception name: {name!r} (use a pyattacker.errors name or a builtin exception name)")


def resolve_target(use: str) -> Any:
    """Resolve a ``use:`` value.

    Order: built-in shorthand (``"flaky"``) -> installed plugin (the ``pyattacker.tasks`` entry
    point group) -> explicit ``"pkg.mod:attr"``. Built-ins win, so a plugin can never shadow one.
    """
    if ":" in use:
        module_name, _, attr = use.partition(":")
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise ConfigError(f"cannot import module {module_name!r}: {exc}") from exc
        target = getattr(module, attr, None)
        if target is None:
            raise ConfigError(f"module {module_name!r} does not contain {attr!r}")
        return target
    if use in BUILTIN_TASKS:
        return BUILTIN_TASKS[use]
    from .plugins import PLUGINS

    plugin = PLUGINS.task(use)
    if plugin is not None:
        return plugin
    raise ConfigError(
        f"cannot resolve use: {use!r} (built-ins: {sorted(BUILTIN_TASKS)}; "
        f"installed task plugins: {PLUGINS.names('tasks')}; or write 'module:attribute')"
    )


_RETRY_FIELDS = set(Retrying.__dataclass_fields__)


def _build_retrying(raw: Any) -> Retrying:
    """Turn the declarative retry section into a Retrying, and translate the YAML pitfalls into plain language."""
    if raw is None:
        return Retrying()
    if isinstance(raw, Retrying):
        return raw
    params = dict(raw)
    if True in params:  # YAML 1.1 parses bare on/off/yes/no as booleans
        params["on"] = params.pop(True)
        raise ConfigError(
            'retry "on" was parsed by YAML as boolean true (a YAML 1.1 pitfall): '
            'write it as "on": [RetryableError, TimeoutError] (with quotes)'
        )
    unknown = set(params) - _RETRY_FIELDS
    if unknown:
        raise ConfigError(f"retry section has unknown fields {sorted(unknown)}; available: {sorted(_RETRY_FIELDS)}")
    on = params.get("on")
    if isinstance(on, str):
        on = [on]
    if on:
        params["on"] = tuple(_resolve_exc(name) for name in on)
    return Retrying(**params)


def _build_task(entry: Mapping[str, Any], *, default_pool: str | None = None) -> TaskSpec:
    if "use" not in entry:
        raise ConfigError(f"task declaration is missing the use field: {entry}")
    target = resolve_target(str(entry["use"]))
    raw_args = entry.get("args") or []
    kwargs = dict(entry.get("kwargs") or {})
    if isinstance(raw_args, Mapping):
        # tolerate keyword arguments written directly under args: args: {fail_times: 1}
        kwargs = {**dict(raw_args), **kwargs}
        raw_args = []
    overrides = {
        "name": entry.get("name"),
        "resource": entry.get("resource", default_pool),
        "algorithm": entry.get("algorithm"),
        "timeout_s": entry.get("timeout_s"),
    }
    if entry.get("retry") is not None:
        overrides["retry"] = _build_retrying(entry["retry"])
    if isinstance(target, TaskSpec):
        return target.with_overrides(**overrides)
    if callable(target):
        produced = target(*raw_args, **kwargs) if (raw_args or kwargs) else target
        if isinstance(produced, TaskSpec):
            return produced.with_overrides(**overrides)
        if callable(produced):
            return build_task_spec(produced, **overrides)
    raise ConfigError(f"use={entry['use']!r} is neither a TaskSpec nor a callable")


def _build_pool(name: str, spec: Mapping[str, Any]) -> Pool:
    default_capacity = int(spec.get("capacity", 1))
    default_kind = spec.get("kind")
    resources: list[Resource] = []
    for index, item in enumerate(spec.get("resources") or []):
        item = dict(item)
        kind = item.get("kind") or default_kind or name
        options = dict(item.get("options") or {})
        options.setdefault("kind", kind)
        resources.append(
            Resource.create(
                kind,
                id=item.get("id") or f"{name}-{index + 1}",
                options=options,
                tags=dict(item.get("tags") or {}),
                capacity=int(item.get("capacity", default_capacity)),
                degrade_after=int(item.get("degrade_after", spec.get("degrade_after", 3))),
                dead_after=int(item.get("dead_after", spec.get("dead_after", 8))),
                cooldown_s=float(item.get("cooldown_s", spec.get("cooldown_s", 30.0))),
            )
        )
    algorithm = spec.get("algorithm")
    return Pool(
        name,
        resources,
        kind=default_kind,
        algorithm=resolve_algorithm(algorithm) if algorithm is not None else None,
        deadlock_warn_s=spec.get("deadlock_warn_s", 5.0),
    )


@dataclass
class DeclarativeSpec:
    """A fully-resolved config file: pools + pipeline template + run parameters, ready to hand to a :class:`~pyattacker.runner.Runner`.

    Collaborators: produced by :func:`load_spec`; :meth:`pipelines` is what a CLI ``run`` command
    actually iterates to get :class:`~pyattacker.pipeline.PipelineSpec` instances.

    Attributes:
        path: Source config file path, kept for error messages and :meth:`describe`.
        template: The built, validated :class:`~pyattacker.pipeline.PipelineTemplate`.
        pools: Resource pools declared under ``pools:``, ready to pass to ``Runner(pools=...)``.
        run: The raw ``run:`` section (concurrency, store, etc.) — not validated here; the CLI
            maps it onto :class:`~pyattacker.runner.RunConfig` fields.
        source: The raw ``source:`` section describing how to generate seeds (``kind``/``repeats``/
            ``key_field`` plus factory-specific keys); consumed by :meth:`seeds`/:meth:`pipelines`.
        raw: The config with ``${VAR}`` references expanded where resolvable; in non-strict mode
            an unresolved reference is left as the literal ``${VAR}`` placeholder rather than
            raising (see :func:`expand_env`) — this is *not* guaranteed to be fully expanded.
            Kept for :meth:`describe` and debugging.
        unresolved_env: Names of ``${VAR}`` references that had no default and no environment
            value (only populated when ``load_spec`` was *not* called with ``strict_env=True``,
            since strict mode raises instead of collecting them).
    """

    path: str
    template: PipelineTemplate
    pools: list[Pool] = field(default_factory=list)
    run: dict[str, Any] = field(default_factory=dict)
    source: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)
    unresolved_env: list[str] = field(default_factory=list)

    # ------------------------------------------------------------- execution
    def seeds(self) -> Iterable[Any]:
        spec = dict(self.source)
        kind = spec.pop("kind", "range")
        spec.pop("repeats", None)
        spec.pop("key_field", None)
        return seed_factory(kind, **spec)

    def pipelines(self, *, limit: int | None = None) -> Iterator[PipelineSpec]:
        repeats = int(self.source.get("repeats", 1))
        key_field = self.source.get("key_field")
        key_of = None
        if key_field:

            def explicit_key(seed: Any) -> str:
                return f"{self.template.name}:{seed[key_field]}"

            key_of = explicit_key

        for count, spec in enumerate(
            self.template.map(self.seeds(), repeats=repeats, key_of=key_of), start=1
        ):
            if limit is not None and count > limit:
                return
            yield spec

    def describe(self) -> dict[str, Any]:
        return {
            "config": self.path,
            "pipeline": self.template.describe(),
            "pools": {
                pool.name: {
                    "kind": pool.kind,
                    "resources": len(pool),
                    "capacity": sum(r.capacity for r in pool.resources()),
                    "algorithm": getattr(pool.default_algorithm, "name", None),
                }
                for pool in self.pools
            },
            "run": dict(self.run),
            "source": dict(self.source),
            "unresolved_env": sorted(set(self.unresolved_env)),
        }


def load_spec(path: str | Path, *, strict_env: bool = False) -> DeclarativeSpec:
    """Read a declarative config and build pools + the pipeline template + run parameters."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"config file does not exist: {path}")
    raw = _load_raw(path)
    unresolved: list[str] = []
    raw = expand_env(raw, strict=strict_env, unresolved=unresolved)

    pools = [_build_pool(name, spec or {}) for name, spec in (raw.get("pools") or {}).items()]
    pipe_cfg = raw.get("pipeline")
    if not pipe_cfg:
        raise ConfigError("config is missing the pipeline section")
    entries = pipe_cfg.get("tasks")
    if not entries:
        raise ConfigError("pipeline.tasks must not be empty")
    tasks = tuple(_build_task(entry, default_pool=pipe_cfg.get("resource")) for entry in entries)
    template = pipeline(
        pipe_cfg.get("name") or path.stem,
        *tasks,
        tags=dict(pipe_cfg.get("tags") or {}),
        include_code=bool(pipe_cfg.get("include_code", True)),
    )
    source = dict(raw.get("source") or {"kind": "range", "n": 1})
    return DeclarativeSpec(
        path=str(path),
        template=template,
        pools=pools,
        run=dict(raw.get("run") or {}),
        source=source,
        raw=raw,
        unresolved_env=unresolved,
    )


def main_guard() -> None:  # pragma: no cover - manual debugging only
    spec = load_spec(sys.argv[1])
    print(json.dumps(spec.describe(), indent=2, ensure_ascii=False))
