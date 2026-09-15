"""Example pyattacker plugin package.

Everything here is ordinary library code; the only unusual part is the entry-point declarations
in ``pyproject.toml``, which is what makes the names resolvable from a config file.

A store plugin would look the same, with ``[project.entry-points."pyattacker.stores"]`` keyed by a
URI scheme, and a factory ``open_store(spec: str, *, journal: str = "full") -> Store``.
"""

__version__ = "0.1.0"
