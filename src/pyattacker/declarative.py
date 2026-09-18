"""Declarative layer —— describe "composition and resources" with YAML/TOML, not logic.

A clear-eyed admission: the declarative layer can never escape Python (``use: myproj.tasks:ask_model`` still imports your code),
so this layer only does three things: **pick tasks, chain pipelines, configure resource pools**.

YAML is an optional extra (``pyyaml``), needed only to read a ``.yaml``/``.yml`` config: the base install
carries no dependencies at all, and a missing PyYAML surfaces as a ``ConfigError`` naming the extra.
TOML/JSON go through the standard library ``tomllib``/``json`` and need nothing.
"""

from __future__ import annotations

import builtins
import difflib
import importlib
import json
import os
import re
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any

from . import errors as _errors
from .algorithm import ALGORITHMS, resolve_algorithm
from .backends import BACKENDS
from .errors import ConfigError, PipelineBuildError, PyAttackerError
from .handoff import build_control
from .pipeline import PipelineSpec, PipelineTemplate, pipeline
from .resource import Pool, Resource
from .task import UNSET, Retrying, TaskSpec, build_task_spec
from .tasks import BUILTIN_TASKS, seed_factory

__all__ = ["DeclarativeSpec", "RUN_FIELDS", "load_spec", "expand_env", "resolve_target"]

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


# ------------------------------------------------- config validation / normalization
#
# One shared entry, `load_spec`, used by both `validate` and a real `run`: a config that cannot be
# run is rejected before a Runner (or a shard child) is created. Every message names the field path
# (`run.concurency`, `pipeline.tasks[1].resource`) and a close match when there is one, because a
# typo should not need a diff against the schema to find.
#
# The check is declarative-only: it never opens a dataset, never builds an artifact backend and
# never calls a task. Its job is exactly what `validate` advertises — declarations and required
# fields — not "would this dataset run cleanly".

RUN_FIELDS = frozenset({
    "store",
    "journal",
    "concurrency",
    "label",
    "heartbeat_s",
    "grace_s",
    "stale_after_s",
    "strict_leases",
    "stop_after_failures",
    "stop_after_s",
    "retry_succeeded",
    "seed",
    "notes",
    "write_behind",
    "write_batch",
    "flush_interval",
    "artifact_backend",
    "meta",
})
"""Keys a config's ``run:`` block may set — the same set the CLI maps onto ``RunConfig``.

``run_id``, ``resume`` and ``handle_signals`` are deliberately absent: they describe the invocation
(``--resume``, which process) rather than the config file, and the CLI owns them.
"""

_PIPELINE_FIELDS = frozenset({"name", "resource", "tags", "include_code", "tasks", "control"})
_TASK_FIELDS = frozenset(
    {"use", "args", "kwargs", "name", "resource", "algorithm", "timeout_s", "config", "version", "retry"}
)
_POOL_FIELDS = frozenset(
    {"kind", "capacity", "resources", "algorithm", "degrade_after", "dead_after", "cooldown_s", "deadlock_warn_s"}
)
_RESOURCE_FIELDS = frozenset({"id", "kind", "options", "tags", "capacity", "degrade_after", "dead_after", "cooldown_s"})
# Per-kind allowed keys, mirroring `seed_factory` plus the two keys `DeclarativeSpec` consumes itself.
_SOURCE_FIELDS = {
    "range": frozenset({"kind", "n", "repeats", "key_field"}),
    "jsonl": frozenset({"kind", "path", "limit", "repeats", "key_field"}),
}


def _fail(path: str, message: str) -> None:
    raise ConfigError(f"{path}: {message}")


def _unknown(path: str, section: Mapping[Any, Any], allowed: Iterable[str]) -> None:
    """Reject keys the schema does not know, naming each one and its closest known neighbour."""
    known = sorted(allowed)
    unknown = [key for key in section if key not in known]
    if not unknown:
        return
    hints = [
        f"{path}.{match}"
        for key in unknown
        if (match := next(iter(difflib.get_close_matches(str(key), known, n=1)), None))
    ]
    rendered = ", ".join(repr(key) for key in unknown)
    suffix = f" (did you mean {', '.join(hints)}?)" if hints else ""
    _fail(path, f"unknown field(s) {rendered}{suffix}; available: {known}")


def _mapping(value: Any, path: str) -> Mapping[Any, Any]:
    if not isinstance(value, Mapping):
        _fail(path, f"must be a mapping, got {type(value).__name__}")
    return value


def _text(value: Any, path: str) -> None:
    if not isinstance(value, str):
        _fail(path, f"must be a string, got {value!r}")


def _flag(value: Any, path: str) -> None:
    if not isinstance(value, bool):
        _fail(path, f"must be true or false, got {value!r}")


def _range(value: Any, path: str, minimum: float | None, *, exclusive: bool) -> None:
    if minimum is None:
        return
    too_small = value <= minimum if exclusive else value < minimum
    if too_small:
        _fail(path, f"must be {'greater than' if exclusive else 'at least'} {minimum}, got {value!r}")


def _integer(value: Any, path: str, *, minimum: int | None = None, exclusive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(path, f"must be an integer, got {value!r}")
    _range(value, path, minimum, exclusive=exclusive)


def _number(value: Any, path: str, *, minimum: float | None = None, exclusive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, f"must be a number, got {value!r}")
    _range(value, path, minimum, exclusive=exclusive)


def _backend_param(name: str, value: Any, path: str, item: Any) -> None:
    """Type-check one backend parameter against the dataclass field it would land on."""
    if isinstance(item.default, bool):
        _flag(value, path)
    elif isinstance(item.default, int):
        _integer(value, path, minimum=0)
    elif isinstance(item.default, float):
        _number(value, path, minimum=0)
    elif isinstance(item.default, str) or item.type in ("str", str):
        _text(value, path)
        if name == "root" and not value.strip():
            _fail(path, "must not be empty")
    # An unrecognised parameter shape is left to `resolve_backend`, the code that will actually
    # build the backend; the invariant here is "accepted implies constructible".


def _backend(value: Any, path: str) -> None:
    """The shape of an ``artifact_backend``, checked without constructing one.

    ``resolve_backend`` is deliberately not called here: constructing a ``FileBackend`` creates its
    root directory, and `validate` must not touch the filesystem. The check therefore mirrors what
    ``resolve_backend`` accepts — the JSON-string form, the known backend kinds, the fields each
    backend takes and the ones it *requires* — so that anything this accepts can also be constructed
    for real, barring environmental failures such as an unwritable path.
    """
    if value is None:
        return
    if isinstance(value, str):
        raw = value.strip()
        if not raw.startswith("{"):
            # ""/"inline"/"none" -> inline, "null" -> the null backend, "file://..." or a bare path
            # -> a file backend rooted there. All of those are constructible by definition.
            return
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            _fail(path, f"is not valid JSON: {exc}")
        if not isinstance(parsed, Mapping):
            _fail(path, f"must describe a mapping, got {type(parsed).__name__}")
        _backend(parsed, path)
        return
    body = _mapping(value, path)
    params = dict(body)
    kind = params.pop("kind", params.pop("name", "file"))
    factory = BACKENDS.get(kind)
    if factory is None:
        _fail(f"{path}.kind", f"unknown artifact backend {kind!r}; available: {sorted(BACKENDS)}")
    declared = {item.name: item for item in fields(factory)}
    _unknown(path, params, declared)
    missing = sorted(
        name
        for name, item in declared.items()
        if item.default is MISSING and item.default_factory is MISSING and name not in params
    )
    if missing:
        _fail(path, f"{kind!r} backend requires {missing}")
    for name, param in params.items():
        _backend_param(name, param, f"{path}.{name}", declared[name])


def _algorithm_param(name: str, value: Any, path: str) -> None:
    """Type-check one algorithm parameter against the dataclass field it would land on."""
    if name == "fallback":  # nested algorithm, written as a name or a mapping
        _algorithm(value, path)
    elif name == "pools":  # Failover: a sequence of pool names
        if not isinstance(value, (list, tuple)) or not all(isinstance(item, str) for item in value):
            _fail(path, f"must be a list of pool names, got {value!r}")
    elif name in ("timeout", "max_wait"):  # optional deadline
        if value is not None:
            _number(value, path, minimum=0, exclusive=True)
    elif name == "jitter":
        _text(value, path)
        if value not in ("none", "full", "equal"):
            _fail(path, f"must be 'none', 'full' or 'equal', got {value!r}")
    elif name == "metric":
        _text(value, path)
    else:
        _number(value, path, minimum=0)


def _algorithm(value: Any, path: str) -> None:
    """Unknown names, unknown parameters and plugin algorithms given parameters are config errors."""
    if value is None:
        return
    if isinstance(value, Mapping):
        params = dict(value)
        name = params.pop("name", "wait")
        if isinstance(name, str) and (cls := ALGORITHMS.get(name)) is not None:
            _unknown(path, params, (item.name for item in fields(cls)))
            for key, param in params.items():
                _algorithm_param(key, param, f"{path}.{key}")
    try:
        resolve_algorithm(value)
    except (PyAttackerError, TypeError, ValueError) as exc:
        _fail(path, str(exc))


def _validate_run(raw: Any) -> None:
    if raw is None:
        return
    section = _mapping(raw, "run")
    _unknown("run", section, RUN_FIELDS)
    for key, value in section.items():
        path = f"run.{key}"
        if key in ("store", "label", "notes"):
            _text(value, path)
        elif key == "journal":
            _text(value, path)
            if value not in ("full", "summary"):
                _fail(path, f"must be 'full' or 'summary', got {value!r}")
        elif key == "concurrency":
            _integer(value, path, minimum=1)
        elif key in ("heartbeat_s", "stale_after_s", "stop_after_s", "flush_interval"):
            _number(value, path, minimum=0, exclusive=True)
        elif key == "grace_s":
            _number(value, path, minimum=0)
        elif key in ("stop_after_failures", "write_batch"):
            _integer(value, path, minimum=1)
        elif key in ("strict_leases", "retry_succeeded"):
            _flag(value, path)
        elif key == "write_behind":
            if value is not None:  # null = auto (on for a file-backed store)
                _flag(value, path)
        elif key == "seed":
            _integer(value, path)
        elif key == "meta":
            _mapping(value, path)
        elif key == "artifact_backend":
            _backend(value, path)


def _validate_resource(raw: Any, path: str) -> None:
    section = _mapping(raw, path)
    _unknown(path, section, _RESOURCE_FIELDS)
    for key in ("id", "kind"):
        if section.get(key) is not None:
            _text(section[key], f"{path}.{key}")
    for key in ("options", "tags"):
        if section.get(key) is not None:
            _mapping(section[key], f"{path}.{key}")
    # Presence, not non-None value: these are read with int()/float() downstream, so an explicit
    # `capacity:` with no value would crash the builder rather than be a config error.
    if "capacity" in section:
        _integer(section["capacity"], f"{path}.capacity", minimum=1)
    for key in ("degrade_after", "dead_after"):
        if key in section:
            _integer(section[key], f"{path}.{key}", minimum=1)
    if "cooldown_s" in section:
        _number(section["cooldown_s"], f"{path}.cooldown_s", minimum=0)


def _validate_pools(raw: Any) -> None:
    if raw is None:
        return
    for name, spec in _mapping(raw, "pools").items():
        path = f"pools.{name}"
        if not isinstance(name, str):
            _fail("pools", f"pool names must be strings, got {name!r}")
        if spec is None:  # `apis:` with no body declares an empty pool, as it always has
            continue
        section = _mapping(spec, path)
        _unknown(path, section, _POOL_FIELDS)
        if section.get("kind") is not None:
            _text(section["kind"], f"{path}.kind")
        if "capacity" in section:
            _integer(section["capacity"], f"{path}.capacity", minimum=1)
        for key in ("degrade_after", "dead_after"):
            if key in section:
                _integer(section[key], f"{path}.{key}", minimum=1)
        if "cooldown_s" in section:
            _number(section["cooldown_s"], f"{path}.cooldown_s", minimum=0)
        if "deadlock_warn_s" in section:
            _number(section["deadlock_warn_s"], f"{path}.deadlock_warn_s", minimum=0, exclusive=True)
        if "algorithm" in section:
            _algorithm(section["algorithm"], f"{path}.algorithm")
        resources = section.get("resources")
        if resources is not None:
            if not isinstance(resources, list):
                _fail(f"{path}.resources", f"must be a list, got {type(resources).__name__}")
            for index, item in enumerate(resources):
                _validate_resource(item, f"{path}.resources[{index}]")


def _validate_retry(raw: Any, path: str) -> None:
    body = _mapping(raw, path)
    if True in body:  # YAML 1.1 parsed a bare `on` as boolean true: keep the actionable hint
        _build_retrying(raw)
    _unknown(path, body, _RETRY_FIELDS)
    if "max_attempts" in body:
        _integer(body["max_attempts"], f"{path}.max_attempts", minimum=1)
    for key in ("base", "factor", "cap"):
        if key in body:
            _number(body[key], f"{path}.{key}", minimum=0)
    if body.get("max_total_s") is not None:
        _number(body["max_total_s"], f"{path}.max_total_s", minimum=0)
    if "jitter" in body:
        _text(body["jitter"], f"{path}.jitter")
        if body["jitter"] not in ("none", "full", "equal"):
            _fail(f"{path}.jitter", f"must be 'none', 'full' or 'equal', got {body['jitter']!r}")
    for key in ("retry_classified", "retry_unknown"):
        if key in body:
            _flag(body[key], f"{path}.{key}")
    on = body.get("on")
    if on is not None:
        names = [on] if isinstance(on, str) else on
        if not isinstance(names, (list, tuple)):
            _fail(f"{path}.on", f"must be an exception name or a list of them, got {on!r}")
        for name in names:
            _text(name, f"{path}.on")
    try:
        # Resolves `on` names as well, so an exception that does not exist is refused here rather
        # than while the config is being built.
        _build_retrying(raw)
    except ConfigError as exc:
        _fail(path, str(exc))


def _validate_task(raw: Any, path: str) -> None:
    section = _mapping(raw, path)
    _unknown(path, section, _TASK_FIELDS)
    if "use" not in section:
        _fail(path, "task declaration is missing the use field")
    _text(section["use"], f"{path}.use")
    for key in ("name", "resource"):
        if section.get(key) is not None:
            _text(section[key], f"{path}.{key}")
    if section.get("timeout_s") is not None:
        _number(section["timeout_s"], f"{path}.timeout_s", minimum=0, exclusive=True)
    if section.get("version") is not None:
        _text(section["version"], f"{path}.version")
    if section.get("config") is not None:
        _mapping(section["config"], f"{path}.config")
    if section.get("kwargs") is not None:
        _mapping(section["kwargs"], f"{path}.kwargs")
    if section.get("args") is not None and not isinstance(section["args"], (list, tuple, Mapping)):
        _fail(f"{path}.args", f"must be a list or a mapping, got {type(section['args']).__name__}")
    if "algorithm" in section:
        _algorithm(section["algorithm"], f"{path}.algorithm")
    if section.get("retry") is not None:
        _validate_retry(section["retry"], f"{path}.retry")


def _validate_pipeline(raw: Any, pool_names: Sequence[str]) -> None:
    section = _mapping(raw, "pipeline")
    _unknown("pipeline", section, _PIPELINE_FIELDS)
    for key in ("name", "resource"):
        if section.get(key) is not None:
            _text(section[key], f"pipeline.{key}")
    if section.get("tags") is not None:
        _mapping(section["tags"], "pipeline.tags")
    if "include_code" in section:
        # `include_code: null` would silently mean False (dropping source digests from the
        # fingerprint), so only an explicit boolean is accepted here.
        _flag(section["include_code"], "pipeline.include_code")
    if section.get("resource") is not None and section["resource"] not in pool_names:
        _fail("pipeline.resource", f"unknown resource pool {section['resource']!r} (declared: {sorted(pool_names)})")
    entries = section.get("tasks")
    if not isinstance(entries, list):
        _fail("pipeline.tasks", f"must be a list of task entries, got {type(entries).__name__}")
    for index, entry in enumerate(entries):
        _validate_task(entry, f"pipeline.tasks[{index}]")


def _validate_source(raw: Any) -> None:
    if raw is None:
        return
    section = _mapping(raw, "source")
    kind = section.get("kind", "range")
    if not isinstance(kind, str):
        _fail("source.kind", f"must be a string, got {kind!r}")
    if kind not in _SOURCE_FIELDS:
        _fail("source.kind", f"unknown source kind {kind!r}; available: {sorted(_SOURCE_FIELDS)}")
    _unknown("source", section, _SOURCE_FIELDS[kind])
    # Presence, not non-None value: these reach range()/int() downstream, where an explicit
    # `n:` with no value would be a TypeError rather than a config error.
    for key in ("n", "limit"):
        if key in section:
            _integer(section[key], f"source.{key}", minimum=0)
    if "repeats" in section:
        _integer(section["repeats"], "source.repeats", minimum=1)
    if "key_field" in section:
        _text(section["key_field"], "source.key_field")
    if kind == "jsonl" and section.get("path") is None:
        _fail("source.path", "is required when source.kind is 'jsonl'")
    if section.get("path") is not None:
        _text(section["path"], "source.path")


def _check_task_pool(task: TaskSpec, path: str, pool_names: Sequence[str]) -> None:
    if task.resource is not None and task.resource not in pool_names:
        _fail(f"{path}.resource", f"unknown resource pool {task.resource!r} (declared: {sorted(pool_names)})")
    for index, child in enumerate(task.children):
        _check_task_pool(child, f"{path}.children[{index}]", pool_names)


def _validate_pool_references(tasks: Sequence[TaskSpec], pool_names: Sequence[str]) -> None:
    """Every pool a task *declares* must exist, including the one inherited from the factory.

    Runtime has always refused an unknown pool (``PoolNotFound``); doing it here turns a
    per-pipeline failure into a config error, which is the difference between "exit 1, read the
    report" and "exit 2, fix the name".
    """
    for index, task in enumerate(tasks):
        _check_task_pool(task, f"pipeline.tasks[{index}]", pool_names)


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
    # UNSET, not None: a field the config does not mention must keep whatever the resolved
    # TaskSpec already declares (a factory's own resource, algorithm, timeout or retry), while an
    # explicit `resource: null` is a real override that clears it.
    overrides: dict[str, Any] = {
        "name": entry.get("name", UNSET),
        "resource": entry.get("resource", default_pool if default_pool is not None else UNSET),
        "algorithm": entry.get("algorithm", UNSET),
        "timeout_s": entry.get("timeout_s", UNSET),
        "config": entry.get("config", UNSET),
        "version": entry.get("version", UNSET),
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
            # build_task_spec builds a fresh spec, so "not provided" is its None default.
            return build_task_spec(produced, **{k: v for k, v in overrides.items() if v is not UNSET})
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
        pools: Resource pools declared under ``pools:``, validated by :func:`load_spec` and ready to
            pass to ``Runner(pools=...)``.
        run: The ``run:`` section (concurrency, store, ...), validated by :func:`load_spec` against
            the fields a config may set (see :data:`RUN_FIELDS`); the CLI maps it onto
            :class:`~pyattacker.runner.RunConfig`.
        source: The ``source:`` section describing how to generate seeds (``kind``/``repeats``/
            ``key_field`` plus factory-specific keys); validated by :func:`load_spec` (kind,
            required fields, numeric ranges) and consumed by :meth:`seeds`/:meth:`pipelines`.
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
    """Read a declarative config, validate it, and build pools + the pipeline template + run parameters.

    Validation is the shared entry point for ``validate`` and ``run``: unknown fields, field types,
    numeric ranges, pool references, algorithm configuration and source declarations are all checked
    here, so a config that cannot run fails before a Runner (or a shard child) is created.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"config file does not exist: {path}")
    raw = _load_raw(path)
    unresolved: list[str] = []
    raw = expand_env(raw, strict=strict_env, unresolved=unresolved)

    _validate_run(raw.get("run"))
    _validate_pools(raw.get("pools"))
    _validate_source(raw.get("source"))
    pools = [_build_pool(name, spec or {}) for name, spec in (raw.get("pools") or {}).items()]
    pipe_cfg = raw.get("pipeline")
    if not pipe_cfg:
        raise ConfigError("config is missing the pipeline section")
    # Normalise the section *before* reading from it: a scalar or a list here used to reach
    # `pipe_cfg.get("tasks")` and escape as an AttributeError instead of a config error.
    pipe_cfg = _mapping(pipe_cfg, "pipeline")
    entries = pipe_cfg.get("tasks")
    if not entries:
        raise ConfigError("pipeline.tasks must not be empty")
    _validate_pipeline(pipe_cfg, [pool.name for pool in pools])
    tasks = tuple(_build_task(entry, default_pool=pipe_cfg.get("resource")) for entry in entries)
    _validate_pool_references(tasks, [pool.name for pool in pools])
    control = pipe_cfg.get("control")
    if control is not None:
        # The shared validation entry, so `validate` and `run` refuse exactly the same declarations —
        # and so a config error names the field path (`pipeline.control.edges[...]`) instead of the
        # SDK's relative one. `pipeline()` below builds the same plan again from the same mapping.
        try:
            build_control(control, [task.name for task in tasks])
        except PipelineBuildError as exc:
            raise ConfigError(f"pipeline.{exc}") from exc
    template = pipeline(
        pipe_cfg.get("name") or path.stem,
        *tasks,
        tags=dict(pipe_cfg.get("tags") or {}),
        include_code=bool(pipe_cfg.get("include_code", True)),
        control=control,
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
