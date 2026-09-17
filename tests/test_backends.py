"""Artifact backends: where payload bytes live, and how the stores spill/hydrate around them.

Coverage
* ``resolve_backend``: defaults/inline/none, null, ``file://`` URIs, bare paths, dict and JSON
  specs, instance pass-through, and every ``ConfigError`` path
* ``FileBackend``: ``wants`` vs ``min_bytes``, digest-derived paths, atomic content-addressed
  writes (no ``.tmp-`` leftovers, identical payloads collapse), ``get``/``delete`` ref forms,
  ``stats`` counts; plus the trivial ``InlineBackend`` / ``NullBackend`` contracts
* store integration, run against **both** ``SqliteStore`` and ``MemoryStore``: spill + hydrated
  ``get_artifact``, ``journal="summary"`` keeping nothing anywhere, and a deleted blob making the
  artifact unavailable again
* resume across a *spilled* checkpoint: the first task is not re-executed because the checkpoint
  was hydrated from the backend
* runner/store wiring: ``open_store(path, backend=...)`` and ``Runner(artifact_backend="file://...")``,
  including the backend name recorded in the run manifest
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from pyattacker import (
    Artifact,
    FileBackend,
    InlineBackend,
    MemoryStore,
    NullBackend,
    Retrying,
    Runner,
    SqliteStore,
    open_store,
    pipeline,
    task,
)
from pyattacker.artifact import DEFAULT_REGISTRY, digest_of
from pyattacker.backends import DEFAULT_MIN_BYTES, ArtifactBackend, resolve_backend
from pyattacker.errors import ConfigError, PyAttackerError, RetryableError

# --------------------------------------------------------------------- helpers
_VALUE = {"answer": 42, "text": "x" * 200}


def _make_store(kind: str, tmp_path: Any, *, journal: str = "full", backend: Any = None, db_name: str = "store.db"):
    if kind == "memory":
        return MemoryStore(journal=journal, backend=backend)
    return SqliteStore(str(tmp_path / db_name), journal=journal, backend=backend)


def _artifact(value: Any = _VALUE, *, seq: int = 0, pipeline_id: str = "p1", task_name: str = "produce") -> Artifact:
    """A realistic artifact: metadata and payload agree, as the codec produces them."""
    encoded = DEFAULT_REGISTRY.dump(value)
    return Artifact(
        id=Artifact.build_id(pipeline_id, seq),
        pipeline_id=pipeline_id,
        task_name=task_name,
        seq=seq,
        type_name=encoded.type_name,
        codec=encoded.codec,
        digest=encoded.digest,
        size=encoded.size,
        payload=encoded.data,
        created_at=1000.0,
    )


def _bytes_artifact(payload: bytes | None, *, artifact_id: str = "p1:0") -> Artifact:
    """A raw-payload artifact with a digest that matches it (used for size-boundary tests)."""
    data = payload if payload is not None else b""
    return Artifact(
        id=artifact_id,
        pipeline_id="p1",
        task_name="produce",
        seq=0,
        type_name="bytes",
        codec="bytes",
        digest=digest_of(data),
        size=len(data),
        payload=payload,
        created_at=1000.0,
    )


def _raw_artifact(store: Any, pipeline_id: str, seq: int) -> tuple[bytes | None, str | None]:
    """The *persisted* (payload, blob_ref) — i.e. before hydration. Both stores keep it plainly."""
    if isinstance(store, MemoryStore):
        stored = store._artifacts[(pipeline_id, seq)]
        return stored.payload, stored.blob_ref
    row = store._conn.execute(
        "SELECT payload, blob_ref FROM artifacts WHERE pipeline_id=? AND seq=?", (pipeline_id, seq)
    ).fetchone()
    return row["payload"], row["blob_ref"]


@pytest.fixture(params=["sqlite", "memory"])
def spilling_store(request, tmp_path):
    """The same fake asserts run against both stores, each with its own spilling backend."""
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    store = _make_store(request.param, tmp_path, backend=backend)
    try:
        yield store, backend
    finally:
        store.close()


# ------------------------------------------------------------------ resolve_backend
def test_resolve_backend_defaults_to_inline():
    for spec in (None, "", "   ", "inline", "none"):
        backend = resolve_backend(spec)
        assert type(backend) is InlineBackend
        assert backend.name == "inline"
    assert resolve_backend({"kind": "inline"}).name == "inline"


def test_resolve_backend_null_keeps_no_bytes():
    backend = resolve_backend("null")
    assert type(backend) is NullBackend
    assert (backend.name, backend.min_bytes) == ("null", 0)
    assert resolve_backend({"kind": "null"}).name == "null"


def test_resolve_backend_file_uri_and_bare_path(tmp_path):
    from_uri = resolve_backend(f"file://{tmp_path}/blobs")
    assert type(from_uri) is FileBackend
    assert from_uri.root == str(tmp_path / "blobs")
    assert from_uri.min_bytes == DEFAULT_MIN_BYTES == 256 * 1024
    assert os.path.isdir(from_uri.root)  # the root is created on construction

    bare = resolve_backend(str(tmp_path / "bare"))
    assert type(bare) is FileBackend
    assert bare.root == str(tmp_path / "bare")
    assert os.path.isdir(bare.root)


def test_resolve_backend_dict_and_json_specs(tmp_path):
    root = tmp_path / "dict-blobs"
    configured = resolve_backend({"kind": "file", "root": str(root), "min_bytes": 7})
    assert type(configured) is FileBackend
    assert (configured.root, configured.min_bytes) == (str(root), 7)

    by_name = resolve_backend({"name": "file", "root": str(root), "min_bytes": 3})
    assert type(by_name) is FileBackend and by_name.min_bytes == 3

    from_json = resolve_backend(json.dumps({"kind": "file", "root": str(root), "min_bytes": 1}))
    assert type(from_json) is FileBackend and from_json.min_bytes == 1
    assert type(resolve_backend('{"kind": "null"}')) is NullBackend


def test_resolve_backend_passes_instances_through(tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=11)
    assert resolve_backend(backend) is backend
    inline = InlineBackend()
    assert resolve_backend(inline) is inline
    assert isinstance(backend, ArtifactBackend)  # the protocol is runtime-checkable


def test_resolve_backend_rejects_bad_specs():
    for spec in (123, 1.5, [], ["file"], object()):
        with pytest.raises(ConfigError, match="cannot resolve artifact backend"):
            resolve_backend(spec)
    with pytest.raises(ConfigError, match="unknown artifact backend 's3'"):
        resolve_backend({"kind": "s3"})
    with pytest.raises(ConfigError, match="cannot configure 'file' backend"):
        resolve_backend({"kind": "file", "bogus": 1})
    with pytest.raises(ConfigError, match="cannot configure 'file' backend"):
        resolve_backend({})  # file needs a root
    with pytest.raises(ConfigError, match="not valid JSON"):
        resolve_backend("{not json")


# --------------------------------------------------------------- backend primitives
def test_file_backend_wants_only_when_size_reaches_min_bytes(tmp_path):
    assert FileBackend(root=str(tmp_path / "b"), min_bytes=5).wants(_bytes_artifact(b"12345")) is True
    assert FileBackend(root=str(tmp_path / "b"), min_bytes=6).wants(_bytes_artifact(b"12345")) is False
    assert FileBackend(root=str(tmp_path / "b"), min_bytes=1).wants(_bytes_artifact(None)) is False
    assert InlineBackend().wants(_bytes_artifact(b"12345")) is False

    null = NullBackend()
    assert null.wants(_bytes_artifact(b"12345")) is True
    assert null.wants(_bytes_artifact(None)) is False
    ref = null.put(_bytes_artifact(b"12345"))
    assert ref == f"null:{digest_of(b'12345')}:5"
    assert null.get(ref) is None
    assert null.delete(ref) is True

    inline = InlineBackend()
    assert inline.get("file:///anything") is None
    assert inline.delete("file:///anything") is False
    with pytest.raises(PyAttackerError, match="never spills payloads"):
        inline.put(_bytes_artifact(b"12345"))


def test_file_backend_path_for_is_derived_from_the_digest(tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"))
    digest = digest_of(b"some bytes")
    assert backend.path_for(digest) == os.path.join(backend.root, digest[:2], digest[2:])
    assert backend.path_for("abcdef") == os.path.join(backend.root, "ab", "cdef")


def test_file_backend_put_is_atomic_and_content_addressed(tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    first = _bytes_artifact(b"same bytes", artifact_id="p1:0")
    second = _bytes_artifact(b"same bytes", artifact_id="p2:0")
    target = backend.path_for(first.digest)

    ref = backend.put(first)
    assert ref == f"file://{target}"
    assert backend.put(second) == ref  # content addressing: identical payloads collapse to one file

    with open(target, "rb") as handle:
        assert handle.read() == b"same bytes"
    assert backend.stats() == {"root": backend.root, "files": 1, "bytes": len(b"same bytes")}
    leftovers = [name for _, _, names in os.walk(backend.root) for name in names if name.startswith(".tmp-")]
    assert leftovers == []  # the temp file was replaced into place, not left behind
    assert os.listdir(os.path.dirname(target)) == [os.path.basename(target)]

    with pytest.raises(PyAttackerError, match="no payload to spill"):
        backend.put(_bytes_artifact(None))


def test_file_backend_get_accepts_uri_absolute_and_relative_refs(tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    artifact = _bytes_artifact(b"hello blob")
    ref = backend.put(artifact)
    relative = os.path.join(artifact.digest[:2], artifact.digest[2:])

    assert backend.get(ref) == b"hello blob"
    assert backend.get(backend.path_for(artifact.digest)) == b"hello blob"
    assert backend.get(relative) == b"hello blob"  # a relative ref resolves inside root
    assert backend.get(f"file://{tmp_path}/missing") is None
    assert backend.get("does/not/exist") is None

    assert backend.delete(ref) is True
    assert backend.get(ref) is None
    assert backend.delete(ref) is False  # already gone, best effort


def test_file_backend_stats_ignores_leftover_temp_files(tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    assert backend.stats() == {"root": backend.root, "files": 0, "bytes": 0}

    backend.put(_bytes_artifact(b"a" * 10, artifact_id="p1:0"))
    backend.put(_bytes_artifact(b"b" * 25, artifact_id="p1:1"))
    assert backend.stats() == {"root": backend.root, "files": 2, "bytes": 35}

    shard = os.path.join(backend.root, "ab")
    os.makedirs(shard, exist_ok=True)
    with open(os.path.join(shard, ".tmp-crashed"), "wb") as handle:
        handle.write(b"partial write")
    assert backend.stats() == {"root": backend.root, "files": 2, "bytes": 35}


# ------------------------------------------------------------- store integration
def test_file_backend_spills_and_get_artifact_hydrates(spilling_store):
    store, backend = spilling_store
    assert store.backend is backend
    original = _artifact({"answer": 42, "text": "x" * 200})

    stored = store.put_artifact(original)

    # persisted form: metadata + a reference, no bytes in the store row itself
    assert stored.payload is None
    assert stored.blob_ref == f"file://{backend.path_for(original.digest)}"
    assert stored.digest == original.digest
    assert stored.size == original.size
    assert os.path.isfile(backend.path_for(original.digest))
    assert backend.get(stored.blob_ref) == original.payload
    assert _raw_artifact(store, "p1", 0) == (None, stored.blob_ref)

    # read form: hydrated back into a single complete artifact
    hydrated = store.get_artifact("p1", 0)
    assert hydrated.payload == original.payload
    assert hydrated.available is True
    assert (hydrated.codec, hydrated.digest, hydrated.size) == (original.codec, original.digest, original.size)
    assert DEFAULT_REGISTRY.load(hydrated.encoded()) == {"answer": 42, "text": "x" * 200}
    assert store.artifacts("p1")[0].payload == original.payload


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_small_payloads_stay_inline_when_min_bytes_is_not_reached(kind, tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1_000_000)
    store = _make_store(kind, tmp_path, backend=backend, db_name="inline.db")
    try:
        original = _artifact({"small": True})
        stored = store.put_artifact(original)
        assert stored.payload == original.payload
        assert stored.blob_ref is None
        assert backend.stats() == {"root": backend.root, "files": 0, "bytes": 0}
        assert store.get_artifact("p1", 0).payload == original.payload
    finally:
        store.close()


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_summary_journal_keeps_nothing_even_with_a_backend(kind, tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    store = _make_store(kind, tmp_path, journal="summary", backend=backend, db_name="summary.db")
    try:
        original = _artifact({"secret": "bytes"})
        stored = store.put_artifact(original)

        assert stored.payload is None
        assert stored.blob_ref is None  # not in the store ...
        assert os.listdir(backend.root) == []  # ... and not on disk either
        assert backend.stats() == {"root": backend.root, "files": 0, "bytes": 0}

        read_back = store.get_artifact("p1", 0)
        assert read_back.payload is None
        assert read_back.available is False
        assert (read_back.digest, read_back.size) == (original.digest, original.size)
    finally:
        store.close()


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_deleting_the_blob_makes_the_artifact_unavailable_again(kind, tmp_path):
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    store = _make_store(kind, tmp_path, backend=backend, db_name="deleted.db")
    try:
        original = _artifact({"payload": "delete me"})
        stored = store.put_artifact(original)
        assert store.get_artifact("p1", 0).available is True
        blob = backend.path_for(original.digest)
        assert os.path.isfile(blob)

        assert backend.delete(stored.blob_ref) is True
        assert not os.path.exists(blob)

        gone = store.get_artifact("p1", 0)
        assert gone.payload is None
        assert gone.available is False  # exactly the signal resume keys off
        assert gone.blob_ref == stored.blob_ref  # the ref is still on record; only the bytes are gone
        assert backend.get(stored.blob_ref) is None
    finally:
        store.close()


# ----------------------------------------------------- resume through a spilled checkpoint
RESUME_CALLS: dict[str, int] = {"fetch": 0, "ask": 0}
RESUME_STATE: dict[str, bool] = {"ask_fails": True}


@task("blob.fetch")
def blob_fetch(seed: Any, ctx: Any) -> dict[str, Any]:
    RESUME_CALLS["fetch"] += 1
    return {"q": seed["q"], "step": "fetch"}


@task("blob.ask", retry=Retrying(max_attempts=1))
def blob_ask(row: Any, ctx: Any) -> dict[str, Any]:
    RESUME_CALLS["ask"] += 1
    if RESUME_STATE["ask_fails"]:
        raise RetryableError("model unavailable", error_class="upstream")
    return {"a": row["q"].upper()}


BLOB_TEMPLATE = pipeline("blob-resume", blob_fetch | blob_ask)


@pytest.mark.parametrize("kind", ["sqlite", "memory"])
def test_resume_reuses_a_spilled_checkpoint_without_re_executing_the_first_task(kind, tmp_path):
    RESUME_CALLS.update(fetch=0, ask=0)
    RESUME_STATE["ask_fails"] = True
    backend = FileBackend(root=str(tmp_path / "blobs"), min_bytes=1)
    if kind == "memory":
        runner = Runner(store=MemoryStore(backend=backend), handle_signals=False)
    else:
        runner = Runner(store=str(tmp_path / "resume.db"), artifact_backend=backend, handle_signals=False)
    try:
        seeds = [{"q": "q0"}]
        specs = list(BLOB_TEMPLATE.map(seeds))
        pipeline_id = specs[0].pipeline_id

        first = runner.run(BLOB_TEMPLATE.map(seeds))
        assert first.stats["pipelines"]["by_state"] == {"failed": 1}
        assert RESUME_CALLS == {"fetch": 1, "ask": 1}
        assert runner.store.get_pipeline(pipeline_id).n_tasks_done == 1

        # the checkpoint really is on the backend: nothing in the row, bytes in the blob file
        raw_payload, blob_ref = _raw_artifact(runner.store, pipeline_id, 0)
        assert raw_payload is None
        checkpoint_digest = _checkpoint_digest(runner, pipeline_id)
        assert blob_ref == f"file://{backend.path_for(checkpoint_digest)}"
        assert os.path.isfile(backend.path_for(checkpoint_digest))

        checkpoint = runner.store.get_artifact(pipeline_id, 0)
        assert checkpoint.available is True
        assert DEFAULT_REGISTRY.load(checkpoint.encoded()) == {"q": "q0", "step": "fetch"}

        RESUME_STATE["ask_fails"] = False
        second = runner.run(BLOB_TEMPLATE.map(seeds), resume=True)

        assert second.stats["pipelines"]["by_state"] == {"succeeded": 1}
        assert RESUME_CALLS["fetch"] == 1  # ★ hydrated from the backend: the first task was not re-executed
        assert RESUME_CALLS["ask"] == 2  # only the failed task ran again
        assert runner.store.get_pipeline(pipeline_id).state == "succeeded"
        resumed = [e for e in runner.store.events(limit=200) if e.kind == "pipeline.resumed"]
        assert [(e.pipeline_id, e.data["from_seq"]) for e in resumed] == [(pipeline_id, 1)]
    finally:
        runner.close()


def _checkpoint_digest(runner: Runner, pipeline_id: str) -> str:
    return runner.store.get_artifact(pipeline_id, 0).digest


# ------------------------------------------------------------------ runner wiring
@task("blob.big")
def blob_big(value: Any, ctx: Any) -> dict[str, Any]:
    return {"blob": "z" * 300_000, "keep": value}


def test_open_store_accepts_a_backend_spec_and_spills(tmp_path):
    blobs = tmp_path / "open-blobs"
    store = open_store(str(tmp_path / "open.db"), backend={"kind": "file", "root": str(blobs), "min_bytes": 1})
    try:
        assert store.backend.name == "file"
        assert (store.backend.root, store.backend.min_bytes) == (str(blobs), 1)
        assert store.journal == "full"

        original = _artifact({"payload": "spill me"})
        stored = store.put_artifact(original)
        assert stored.payload is None
        assert stored.blob_ref == f"file://{store.backend.path_for(original.digest)}"
        assert os.path.isfile(store.backend.path_for(original.digest))
        assert store.get_artifact("p1", 0).payload == original.payload
    finally:
        store.close()


def test_runner_artifact_backend_uri_spills_and_records_the_backend_name(tmp_path):
    blobs = tmp_path / "runner-blobs"
    db = str(tmp_path / "wired.db")
    runner = Runner(store=db, concurrency=4, artifact_backend=f"file://{blobs}", handle_signals=False)
    try:
        specs = list(pipeline("wired", blob_big).map([{"i": 1}]))
        report = runner.run(pipeline("wired", blob_big).map([{"i": 1}]))
        pipeline_id = specs[0].pipeline_id

        assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
        raw_payload, blob_ref = _raw_artifact(runner.store, pipeline_id, 0)
        assert raw_payload is None
        assert blob_ref is not None and blob_ref.startswith("file://")
        assert os.path.isfile(blob_ref[len("file://") :])

        hydrated = runner.store.get_artifact(pipeline_id, 0)
        assert hydrated.available is True
        assert DEFAULT_REGISTRY.load(hydrated.encoded()) == {"blob": "z" * 300_000, "keep": {"i": 1}}
    finally:
        runner.close()

    store = SqliteStore(db)
    try:
        run = store.get_run(report.run_id)
        assert run.status == "completed"
        # The manifest records the effective write-behind mode and, when it is on, the batch knobs
        # that mode is using (which is what makes a configured write_batch/flush_interval checkable
        # from the run record rather than only from the store object).
        assert run.config == {
            "concurrency": 4,
            "journal": "full",
            "write_behind": True,
            "write_batch": 128,
            "flush_interval": 1.0,
            "artifact_backend": "file",
        }
    finally:
        store.close()
