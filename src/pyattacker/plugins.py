"""Plugins: let a third-party package publish tasks, algorithms, codecs and stores.

Discovery uses the standard ``importlib.metadata`` entry points — no bespoke registry file, no
import-time magic. A package declares::

    [project.entry-points."pyattacker.tasks"]
    my_judge = "my_pkg.tasks:my_judge"          # a TaskSpec, or a factory returning one

    [project.entry-points."pyattacker.algorithms"]
    my_algo = "my_pkg.algo:MyAlgorithm"         # a class with the AcquireAlgorithm shape

    [project.entry-points."pyattacker.codecs"]
    my_codec = "my_pkg.codec:MyCodec"           # a Codec instance or a zero-arg class

    [project.entry-points."pyattacker.stores"]
    s3 = "my_pkg.s3:open_store"                 # called with the store spec string

and then ``use: my_judge`` works in a declarative config, ``algorithm: my_algo`` works in a
task declaration, and ``store = "s3://bucket/runs.db"`` works in a run config.

Two rules keep plugins from becoming a liability:

* **A broken plugin never takes the kernel down.** Loading is lazy and every failure is recorded
  in :meth:`PluginRegistry.errors` (surfaced by ``pyattacker plugins``) rather than raised.
* **Resolution order is explicit**: built-ins first, then entry points, then ``module:attr``.
  A plugin therefore cannot shadow ``echo`` or ``wait`` by accident.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .errors import PluginError

__all__ = [
    "GROUPS",
    "PluginError",
    "EntryPointLike",
    "PluginRegistry",
    "PLUGINS",
    "list_plugins",
]

# entry-point group -> what it provides
GROUPS: dict[str, str] = {
    "tasks": "pyattacker.tasks",
    "algorithms": "pyattacker.algorithms",
    "codecs": "pyattacker.codecs",
    "stores": "pyattacker.stores",
}


@runtime_checkable
class EntryPointLike(Protocol):
    """The subset of ``importlib.metadata.EntryPoint`` this module needs (fakeable in tests)."""

    name: str
    value: str

    def load(self) -> Any: ...


def _default_provider(group: str) -> Mapping[str, EntryPointLike]:
    from importlib.metadata import entry_points

    discovered = entry_points()
    selected = (
        discovered.select(group=group)
        if hasattr(discovered, "select")
        else discovered.get(group, [])  # pragma: no cover - Python < 3.10 shape
    )
    return {entry.name: entry for entry in selected}


@dataclass
class _LoadResult:
    value: Any = None
    error: str | None = None
    origin: str = ""


class PluginRegistry:
    """Lazy, cached, failure-tolerant view over the installed entry points."""

    def __init__(self, provider: Callable[[str], Mapping[str, EntryPointLike]] | None = None) -> None:
        self._provider = provider or _default_provider
        self._entries: dict[str, dict[str, EntryPointLike]] = {}
        self._loaded: dict[tuple[str, str], _LoadResult] = {}
        self._errors: dict[str, str] = {}  # "group:name" -> message

    # ------------------------------------------------------------ discovery
    def entry_points(self, group: str) -> dict[str, EntryPointLike]:
        """Every entry point in one group (``"tasks"``, ``"algorithms"``, ...)."""
        if group not in GROUPS:
            raise PluginError(f"unknown plugin group {group!r}; available: {sorted(GROUPS)}")
        if group not in self._entries:
            try:
                self._entries[group] = dict(self._provider(GROUPS[group]))
            except Exception as exc:  # a broken installation must not break discovery
                self._errors[f"{group}:*"] = f"{type(exc).__name__}: {exc}"
                self._entries[group] = {}
        return self._entries[group]

    def names(self, group: str) -> list[str]:
        return sorted(self.entry_points(group))

    def load(self, group: str, name: str) -> Any | None:
        """Load one plugin, returning ``None`` (and recording why) if it cannot be loaded."""
        key = (group, name)
        if key in self._loaded:
            return self._loaded[key].value
        entry = self.entry_points(group).get(name)
        if entry is None:
            return None
        try:
            value = entry.load()
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            self._loaded[key] = _LoadResult(None, message, getattr(entry, "value", ""))
            self._errors[f"{group}:{name}"] = message
            return None
        self._loaded[key] = _LoadResult(value, None, getattr(entry, "value", ""))
        return value

    def load_all(self, group: str) -> dict[str, Any]:
        """Load every plugin in a group, skipping (and recording) the broken ones."""
        out: dict[str, Any] = {}
        for name in self.names(group):
            value = self.load(group, name)
            if value is not None:
                out[name] = value
        return out

    # ------------------------------------------------------------- resolution
    def task(self, name: str) -> Any | None:
        return self.load("tasks", name)

    def algorithm(self, name: str) -> Any | None:
        value = self.load("algorithms", name)
        if value is None:
            return None
        if isinstance(value, type):
            try:
                return value()
            except TypeError as exc:
                self._errors[f"algorithms:{name}"] = f"cannot instantiate: {exc}"
                return None
        if callable(value) and not hasattr(value, "acquire"):
            try:
                return value()
            except TypeError as exc:
                self._errors[f"algorithms:{name}"] = f"cannot build: {exc}"
                return None
        return value

    def codec(self, name: str) -> Any | None:
        """Load a codec plugin, normalizing a zero-argument class or factory into an instance.

        Never raises: a plugin that cannot be built is recorded like any other load failure. This
        matters more than it looks — ``install_codecs`` runs during ``Runner.__init__``, so an
        exception here would take down the kernel rather than one plugin.
        """
        value = self.load("codecs", name)
        if value is None:
            return None
        # A class is callable *and* has the methods as attributes, so instance-ness cannot be
        # probed with hasattr alone: a class must always be constructed first.
        if isinstance(value, type) or (callable(value) and not hasattr(value, "dumps")):
            try:
                produced = value()
            except Exception as exc:
                self._errors[f"codecs:{name}"] = f"cannot build codec: {exc}"
                return None
        else:
            produced = value
        if not hasattr(produced, "dumps"):
            self._errors[f"codecs:{name}"] = f"{type(produced).__name__} is not a codec"
            return None
        return produced

    def store_factory(self, spec: str) -> Callable[..., Any] | None:
        """A store plugin is keyed by URI scheme: ``s3://bucket/key`` looks up ``s3``."""
        if "://" not in spec:
            return None
        scheme = spec.split("://", 1)[0]
        return self.load("stores", scheme)

    # ------------------------------------------------------------ diagnostics
    @property
    def errors(self) -> dict[str, str]:
        return dict(self._errors)

    def describe(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for group in GROUPS:
            for name, entry in sorted(self.entry_points(group).items()):
                value = self.load(group, name)
                rows.append(
                    {
                        "group": group,
                        "name": name,
                        "target": getattr(entry, "value", ""),
                        "ok": value is not None,
                        "error": self._errors.get(f"{group}:{name}"),
                    }
                )
        return rows

    def refresh(self) -> None:
        """Forget everything that was discovered (used by tests and by ``--reload``)."""
        self._entries.clear()
        self._loaded.clear()
        self._errors.clear()

    def install_codecs(self, registry: Any) -> list[str]:
        """Register every codec plugin into a :class:`~pyattacker.artifact.CodecRegistry`.

        Goes through :meth:`codec` rather than :meth:`load_all` so that a plugin may publish either
        an instance *or* a zero-argument class — a registry stores instances, and handing it a class
        would fail much later, on the first payload of that type.
        """
        installed: list[str] = []
        for name in self.names("codecs"):
            try:
                codec = self.codec(name)  # normalizes classes/factories, records failures
                if codec is None:
                    continue
                registry.register(codec, name=name)
                installed.append(name)
            except Exception as exc:
                # Belt and braces: this runs while a Runner is being constructed, so *nothing*
                # a plugin does may escape into the caller.
                self._errors[f"codecs:{name}"] = f"{type(exc).__name__}: {exc}"
        return installed


PLUGINS = PluginRegistry()


def list_plugins(registry: PluginRegistry | None = None) -> list[dict[str, Any]]:
    """Everything installed right now, for ``pyattacker plugins``."""
    return (registry or PLUGINS).describe()
