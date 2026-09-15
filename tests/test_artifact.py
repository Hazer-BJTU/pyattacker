"""The Artifact layer: stable serialization, content addressing, codec registration and type restoration."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

import pytest
from helpers import run  # noqa: F401  (single shared test entry point, kept for consistency)

from pyattacker import Artifact, CodecRegistry, canonical_json, digest_of
from pyattacker.errors import ArtifactCodecError


@dataclass
class Row:
    qid: str
    question: str


def test_canonical_json_is_key_order_independent():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    # A non-ASCII (CJK) payload must be preserved verbatim rather than \u-escaped.
    assert canonical_json({"a": "\u4e2d\u6587"}) == '{"a":"\u4e2d\u6587"}'


def test_canonical_json_rejects_unencodable():
    class Weird:
        pass

    with pytest.raises(ArtifactCodecError):
        canonical_json({"x": Weird()})


def test_digest_is_content_addressed():
    assert digest_of("abc") == digest_of(b"abc")
    assert digest_of("abc") != digest_of("abd")
    assert len(digest_of("abc")) == 32


def test_registry_dumps_dataclass_and_restores_type():
    reg = CodecRegistry()
    reg.register_type(Row)
    encoded = reg.dump(Row(qid="q1", question="2+2=?"))
    assert encoded.type_name == "Row"
    assert encoded.codec == "json"
    assert encoded.size == len(encoded.data)
    assert encoded.digest == digest_of(encoded.data)

    restored = reg.load(encoded)
    assert isinstance(restored, Row)
    assert restored == Row(qid="q1", question="2+2=?")


def test_registry_plain_types_roundtrip():
    reg = CodecRegistry()
    assert reg.load(reg.dump({"a": [1, 2, 3]})) == {"a": [1, 2, 3]}
    assert reg.load(reg.dump("text")) == "text"
    assert reg.load(reg.dump([1, "x", None])) == [1, "x", None]


def test_bytes_use_dedicated_codec():
    reg = CodecRegistry()
    encoded = reg.dump(b"\x00\x01binary")
    assert encoded.codec == "bytes"
    assert encoded.type_name == "bytes"
    assert reg.load(encoded) == b"\x00\x01binary"


def test_artifact_encoded_requires_payload():
    artifact = Artifact(
        id="p:0",
        pipeline_id="p",
        task_name="t",
        seq=0,
        type_name="dict",
        codec="json",
        digest="d",
        size=0,
        payload=None,
        created_at=0.0,
    )
    assert artifact.available is False
    with pytest.raises(ArtifactCodecError):
        artifact.encoded()

    full = dataclasses.replace(artifact, payload=b"{}")
    assert full.available is True
    assert full.encoded().data == b"{}"
