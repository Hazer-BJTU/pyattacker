"""Artifact backends: where payload bytes live.

By default a payload lives inline in the store (a SQLite BLOB), which is right for the common
case — a model response is a few kilobytes. It stops being right when an artifact is a rendered
image, an audio clip or a multi-megabyte transcript: the database doubles in size, every backup
copies it, and `export` has to materialize it.

A backend answers one question — *should these bytes go somewhere else?* — and then owns them:

* :class:`InlineBackend` (default): never spills, behaviour is exactly as before;
* :class:`FileBackend`: content-addressed files under a root directory, written atomically;
* :class:`NullBackend`: keeps the digest and drops the bytes (hash-only journals).

The contract with the store is deliberately small: the store still records `digest`/`size`/`codec`
in its own row, plus an opaque `blob_ref`. On read the store fills `payload` back in from the
backend, so an artifact stays a single object to everything upstream.

**Content addressing is what makes this safe**: the file name is the digest, so identical payloads
collapse into one file, a partially written file can never be mistaken for a complete one, and a
backend can be shared by many runs and many stores.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .artifact import Artifact
from .errors import ConfigError, PyAttackerError

__all__ = [
    "ArtifactBackend",
    "InlineBackend",
    "FileBackend",
    "NullBackend",
    "resolve_backend",
    "BACKENDS",
]

DEFAULT_MIN_BYTES = 256 * 1024


@runtime_checkable
class ArtifactBackend(Protocol):
    """Where artifact payloads are kept."""

    name: str

    def wants(self, artifact: Artifact) -> bool:
        """Should this artifact's payload be stored outside the database?"""
        ...

    def put(self, artifact: Artifact) -> str:
        """Store the payload, returning an opaque reference the store will keep."""
        ...

    def get(self, ref: str) -> bytes | None:
        """Fetch a payload by reference (``None`` when it is gone)."""
        ...

    def delete(self, ref: str) -> bool:
        """Best-effort removal. Never called by the kernel; provided for operators."""
        ...


@dataclass
class InlineBackend:
    """Keep everything in the store. The default, and the previous behaviour exactly."""

    name: str = "inline"

    def wants(self, artifact: Artifact) -> bool:
        return False

    def put(self, artifact: Artifact) -> str:  # pragma: no cover - never called
        raise PyAttackerError("InlineBackend never spills payloads")

    def get(self, ref: str) -> bytes | None:
        return None

    def delete(self, ref: str) -> bool:
        return False


@dataclass
class FileBackend:
    """Content-addressed files under ``root``: ``<root>/ab/cdef…``.

    Writes go to a temporary file in the same directory and are then ``os.replace``-d into place,
    so a crash mid-write cannot leave a truncated blob that looks valid.
    """

    root: str
    min_bytes: int = DEFAULT_MIN_BYTES
    name: str = "file"

    def __post_init__(self) -> None:
        self.root = os.path.abspath(os.path.expanduser(str(self.root)))
        os.makedirs(self.root, exist_ok=True)

    def wants(self, artifact: Artifact) -> bool:
        return artifact.payload is not None and artifact.size >= self.min_bytes

    def path_for(self, digest: str) -> str:
        return os.path.join(self.root, digest[:2], digest[2:])

    def put(self, artifact: Artifact) -> str:
        if artifact.payload is None:
            raise PyAttackerError(f"artifact {artifact.id} has no payload to spill")
        target = self.path_for(artifact.digest)
        if not os.path.exists(target):
            os.makedirs(os.path.dirname(target), exist_ok=True)
            # Not a context manager on purpose: the file must outlive the block so it can be
            # fsync-ed and then atomically renamed into place.
            handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
                dir=os.path.dirname(target), prefix=".tmp-", delete=False
            )
            try:
                handle.write(artifact.payload)
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()
                os.replace(handle.name, target)
            except BaseException:
                handle.close()
                with contextlib.suppress(OSError):  # pragma: no cover - best effort
                    os.unlink(handle.name)
                raise
        return f"file://{target}"

    def resolve(self, ref: str) -> str:
        """Turn a reference into a path: ``file:///abs/path`` or a path relative to ``root``."""
        path = ref[len("file://") :] if ref.startswith("file://") else ref
        if not os.path.isabs(path):
            path = os.path.join(self.root, path)
        return path

    def get(self, ref: str) -> bytes | None:
        try:
            with open(self.resolve(ref), "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def delete(self, ref: str) -> bool:
        try:
            os.unlink(self.resolve(ref))
            return True
        except OSError:
            return False

    def stats(self) -> dict[str, Any]:
        """How much the backend holds (for operators; walks the tree, so not for hot paths)."""
        files = 0
        total = 0
        for dirpath, _, names in os.walk(self.root):
            for name in names:
                if name.startswith(".tmp-"):
                    continue
                files += 1
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:  # pragma: no cover
                    continue
        return {"root": self.root, "files": files, "bytes": total}


@dataclass
class NullBackend:
    """Keep the digest, drop the bytes: useful when you only care *that* an artifact existed."""

    name: str = "null"
    min_bytes: int = 0
    _seen: int = 0

    def wants(self, artifact: Artifact) -> bool:
        return artifact.payload is not None

    def put(self, artifact: Artifact) -> str:
        self._seen += 1
        return f"null:{artifact.digest}:{artifact.size}"

    def get(self, ref: str) -> bytes | None:
        return None

    def delete(self, ref: str) -> bool:
        return True


BACKENDS: dict[str, Any] = {"inline": InlineBackend, "file": FileBackend, "null": NullBackend}


def resolve_backend(spec: Any) -> ArtifactBackend:
    """``None``/``"inline"``/``"file:///data/blobs"``/``{"kind": "file", "root": ...}``/an instance."""
    if spec is None:
        return InlineBackend()
    if isinstance(spec, ArtifactBackend):
        return spec
    if isinstance(spec, str):
        raw = spec.strip()
        if raw in ("", "inline", "none"):
            return InlineBackend()
        if raw == "null":
            return NullBackend()
        if raw.startswith("file://"):
            return FileBackend(root=raw[len("file://") :])
        if raw.startswith("{"):
            try:
                return resolve_backend(json.loads(raw))
            except json.JSONDecodeError as exc:
                raise ConfigError(f"artifact backend is not valid JSON: {exc}") from exc
        # a bare path is treated as a file backend root, which is what people mean by "--blobs /data"
        return FileBackend(root=raw)
    if isinstance(spec, dict):
        params = dict(spec)
        kind = str(params.pop("kind", params.pop("name", "file")))
        factory = BACKENDS.get(kind)
        if factory is None:
            raise ConfigError(f"unknown artifact backend {kind!r}; available: {sorted(BACKENDS)}")
        try:
            return factory(**params)
        except TypeError as exc:
            raise ConfigError(f"cannot configure {kind!r} backend: {exc}") from exc
    raise ConfigError(f"cannot resolve artifact backend from {spec!r}")
