"""Artifact —— the persistent carrier of a task's state.

Design notes:
* Artifacts are **content-addressed** (blake2b digest), which gives deduplication and integrity checking for free.
* An artifact is persisted as soon as it is produced —— that is the task-level checkpoint and the entire source of resume capability.
* Encoding goes through a pluggable codec, JSON by default (supports dataclass / primitives / Enum / bytes).
* An artifact with ``seq == SEED_SEQ`` is the seed input of the whole pipeline (one row of the dataset).
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Protocol, runtime_checkable

from .errors import ArtifactCodecError

__all__ = [
    "SEED_SEQ",
    "SEED_TASK",
    "Encoded",
    "Artifact",
    "Codec",
    "JsonCodec",
    "BytesCodec",
    "CodecRegistry",
    "DEFAULT_REGISTRY",
    "canonical_json",
    "digest_of",
]

SEED_SEQ = -1
SEED_TASK = "__seed__"


def _json_default(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return sorted(obj, key=repr)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(obj)).decode("ascii")}
    to_json = getattr(obj, "__json__", None)
    if callable(to_json):
        return to_json()
    raise ArtifactCodecError(
        f"artifact payload is not JSON-encodable: {type(obj).__name__}; "
        f"register a codec for it (CodecRegistry.register)"
    )


def canonical_json(obj: Any) -> str:
    """Stable serialization: sorted keys, no extra whitespace. Used for digest computation, not for display."""
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=_json_default
    )


def digest_of(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.blake2b(data, digest_size=16).hexdigest()


@dataclass(frozen=True, slots=True)
class Encoded:
    """The result of one encoding pass. ``data`` is always bytes, ready to be written straight into a BLOB."""

    type_name: str
    codec: str
    data: bytes
    digest: str
    size: int


@runtime_checkable
class Codec(Protocol):
    name: str

    def can_encode(self, obj: Any) -> bool: ...

    def dumps(self, obj: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class JsonCodec:
    name = "json"

    def can_encode(self, obj: Any) -> bool:
        try:
            canonical_json(obj)
        except ArtifactCodecError:
            return False
        return True

    def dumps(self, obj: Any) -> bytes:
        return canonical_json(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class BytesCodec:
    """Raw byte payloads (images, audio, log chunks, ...)."""

    name = "bytes"

    def can_encode(self, obj: Any) -> bool:
        return isinstance(obj, (bytes, bytearray, memoryview))

    def dumps(self, obj: Any) -> bytes:
        return bytes(obj)

    def loads(self, data: bytes) -> Any:
        return data


class CodecRegistry:
    """Registry mapping types → codecs; also responsible for restoring JSON payloads back into user types."""

    def __init__(self) -> None:
        self._codecs: dict[str, Codec] = {"json": JsonCodec(), "bytes": BytesCodec()}
        self._by_type: dict[type, str] = {
            bytes: "bytes",
            bytearray: "bytes",
            memoryview: "bytes",
        }
        self._rebuild: dict[str, type] = {}

    def register(self, codec: Codec, *, for_types: Iterable[type] = (), name: str | None = None) -> None:
        codec_name = name or codec.name
        self._codecs[codec_name] = codec
        for tp in for_types:
            self._by_type[tp] = codec_name

    def register_type(self, cls: type) -> type:
        """Register a (dataclass) type; on restore the object is rebuilt as ``cls(**payload)``."""
        self._rebuild[cls.__name__] = cls
        return cls

    def type_name_of(self, obj: Any) -> str:
        return type(obj).__name__

    def codec_for(self, obj: Any) -> Codec:
        codec_name = self._by_type.get(type(obj))
        if codec_name is not None:
            return self._codecs[codec_name]
        for base in type(obj).__mro__[1:]:
            codec_name = self._by_type.get(base)
            if codec_name is not None:
                return self._codecs[codec_name]
        for codec in self._codecs.values():
            if codec.can_encode(obj):
                return codec
        raise ArtifactCodecError(f"no codec available for {type(obj).__name__}")

    def dump(self, obj: Any) -> Encoded:
        codec = self.codec_for(obj)
        data = codec.dumps(obj)
        return Encoded(
            type_name=self.type_name_of(obj),
            codec=codec.name,
            data=data,
            digest=digest_of(data),
            size=len(data),
        )

    def load_raw(self, encoded: Encoded) -> Any:
        codec = self._codecs.get(encoded.codec)
        if codec is None:
            raise ArtifactCodecError(f"unknown codec: {encoded.codec}")
        return codec.loads(encoded.data)

    def load(self, encoded: Encoded) -> Any:
        """Decode + best-effort restore of the user type (a registered dataclass)."""
        value = self.load_raw(encoded)
        cls = self._rebuild.get(encoded.type_name)
        if cls is None:
            return value
        if dataclasses.is_dataclass(cls) and isinstance(value, dict):
            try:
                return cls(**value)
            except TypeError as exc:  # on field mismatch fall back to the raw dict, staying diagnosable
                raise ArtifactCodecError(
                    f"cannot restore artifact as {cls.__name__}: {exc}"
                ) from exc
        if isinstance(cls, type) and not isinstance(value, cls):
            try:
                return cls(value)
            except Exception:  # pragma: no cover - best effort
                return value
        return value


DEFAULT_REGISTRY = CodecRegistry()


@dataclass(frozen=True, slots=True)
class Artifact:
    """The persisted state produced by a task."""

    id: str
    pipeline_id: str
    task_name: str
    seq: int
    type_name: str
    codec: str
    digest: str
    size: int
    payload: bytes | None
    created_at: float
    is_final: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        """Whether the payload is stored alongside the record (False when journal=summary)."""
        return self.payload is not None

    def encoded(self) -> Encoded:
        if self.payload is None:
            raise ArtifactCodecError(f"artifact {self.id} has no stored payload (a limitation of journal mode)")
        return Encoded(
            type_name=self.type_name,
            codec=self.codec,
            data=self.payload,
            digest=self.digest,
            size=self.size,
        )

    @staticmethod
    def build_id(pipeline_id: str, seq: int) -> str:
        return f"{pipeline_id}:{seq}"
