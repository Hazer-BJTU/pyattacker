"""Atomic fact batches, legacy failure boundaries, and Suite connection ownership."""

from __future__ import annotations

import sqlite3

import pytest

from pyattacker import Artifact, ExperimentSpec, MemoryStore, SuiteSpec, SuiteStore
from pyattacker.errors import StoreUnavailable
from pyattacker.store import FactBatchStore, SqliteStore, WriteBehindStore
from pyattacker.store.base import AttemptRecord, EventRecord, PipelineRecord
from pyattacker.suite import namespace


def attempt(n, pid="p"):
    return AttemptRecord(pid, "run", pid + ":0", "task", 0, n, 0.0, outcome="failed")


def buffer(inner):
    return WriteBehindStore(inner, batch_size=1000, flush_interval=0)


def facts(store, pid="p", count=3):
    attempts = [attempt(n, pid) for n in range(count)]
    events = [EventRecord(ts=n, kind=str(n), pipeline_id=pid) for n in range(count)]
    for row in attempts:
        store.record_attempt(row)
    for row in events:
        store.emit_event(row)
    return attempts, events


def test_sqlite_batch_uses_one_commit_and_keeps_checkpoints_synchronous(tmp_path):
    path = str(tmp_path / "facts.db")
    inner = SqliteStore(path)
    store = buffer(inner)
    try:
        assert isinstance(inner, FactBatchStore)
        attempts, events = facts(store, count=64)
        store.upsert_pipeline(PipelineRecord("p", "run", "pipe", "p"))
        store.put_artifact(Artifact("p:0", "p", "task", 0, "dict", "json", "d", 2, b"{}", 0))
        reader = SqliteStore(path, read_only=True)
        try:
            assert reader.get_pipeline("p") is not None
            assert reader.get_artifact("p", 0).payload == b"{}"
            assert reader.attempts() == reader.events() == []
            statements = []
            inner._conn.set_trace_callback(statements.append)
            assert store.flush() == 128
            assert statements.count("COMMIT") == 1
            assert len(reader.attempts()) == len(reader.events()) == 64
            assert [r.attempt_id for r in attempts] == list(range(1, 65))
            assert [r.event_id for r in events] == list(range(1, 65))
            statements.clear()
            assert store.flush() == 0
            assert "COMMIT" not in statements
        finally:
            reader.close()
    finally:
        store.close()


@pytest.mark.parametrize("kind", ["attempt", "event"])
@pytest.mark.parametrize("position", [0, 1, 2])
def test_sqlite_failed_batch_rolls_back_rows_and_ids_then_retries(tmp_path, monkeypatch, kind, position):
    inner = SqliteStore(str(tmp_path / "facts.db"))
    store = buffer(inner)
    attempts, events = facts(store)
    method = "_write_" + kind
    original = getattr(inner, method)
    calls = 0
    error = OSError("injected after INSERT")

    def fail(row):
        nonlocal calls
        value = original(row)
        calls += 1
        if calls == position + 1:
            raise error
        return value

    try:
        with monkeypatch.context() as patch:
            patch.setattr(inner, method, fail)
            with pytest.raises(OSError) as caught:
                store.flush()
            assert caught.value is error
        assert not inner._conn.in_transaction
        assert inner.attempts() == inner.events() == []
        assert all(row.attempt_id is None for row in attempts)
        assert all(row.event_id is None for row in events)
        assert store.pending == 6
        assert store.flushes == 0
        assert not store.buffer_stats()["flush_blocked"]
        assert store.flush() == 6
        assert [r.attempt_no for r in inner.attempts()] == [0, 1, 2]
        assert [r.kind for r in inner.events()] == ["0", "1", "2"]
        assert store.pending == 0
        assert store.flushes == 1
    finally:
        store.close()


def test_commit_failure_rolls_back_and_preserves_batch(tmp_path):
    inner = SqliteStore(str(tmp_path / "facts.db"))
    store = buffer(inner)
    attempts, events = facts(store)
    conn = inner._conn

    class FailedCommit:
        def __getattr__(self, name):
            return getattr(conn, name)

        def commit(self):
            raise sqlite3.OperationalError("injected commit failure")

    try:
        inner._conn = FailedCommit()
        with pytest.raises(sqlite3.OperationalError, match="commit failure"):
            store.flush()
        inner._conn = conn
        assert not conn.in_transaction
        assert inner.attempts() == inner.events() == []
        assert all(r.attempt_id is None for r in attempts)
        assert all(r.event_id is None for r in events)
        assert store.flush() == 6
    finally:
        inner._conn = conn
        store.close()


def test_batch_rejects_existing_transaction_without_rolling_it_back(tmp_path):
    inner = SqliteStore(str(tmp_path / "facts.db"))
    try:
        inner._conn.execute("BEGIN")
        inner._write_event(EventRecord(ts=0, kind="caller's pending write"))
        with pytest.raises(sqlite3.OperationalError, match="transaction"):
            inner.write_facts([attempt(1)], [])
        assert inner._conn.in_transaction
        assert len(inner.events()) == 1
        assert inner.attempts() == []
        inner._conn.rollback()
        assert inner.events() == []
    finally:
        inner.close()


@pytest.mark.parametrize("kind", ["attempt", "event"])
@pytest.mark.parametrize("position", [0, 1, 2])
@pytest.mark.parametrize("committed", [False, True])
def test_legacy_failure_acknowledges_prefix_but_never_replays_unknown_write(
    monkeypatch, kind, position, committed
):
    inner = MemoryStore()
    assert not isinstance(inner, FactBatchStore)
    store = buffer(inner)
    facts(store)
    method = "record_attempt" if kind == "attempt" else "emit_event"
    original = getattr(inner, method)
    calls = 0
    error = OSError("legacy write outcome unknown")

    def fail(row):
        nonlocal calls
        calls += 1
        if calls == position + 1:
            if committed:
                original(row)
            raise error
        return original(row)

    monkeypatch.setattr(inner, method, fail)
    with pytest.raises(OSError) as caught:
        store.flush()
    assert caught.value is error
    confirmed = position + (3 if kind == "event" else 0)
    assert store.pending == 6 - confirmed
    assert store.flushes == 0
    assert store.buffer_stats()["flush_blocked"]
    snapshot = (len(inner.attempts()), len(inner.events()))
    assert sum(snapshot) == confirmed + int(committed)
    monkeypatch.setattr(inner, method, original)
    for action in (store.flush, store.events, lambda: store.record_attempt(attempt(9)),
                   lambda: store.emit_event(EventRecord(ts=9, kind="new"))):
        with pytest.raises(StoreUnavailable, match="unknown commit outcome") as blocked:
            action()
        assert blocked.value.__cause__ is error
    assert (len(inner.attempts()), len(inner.events())) == snapshot
    assert store.pending == 6 - confirmed
    closed = []
    monkeypatch.setattr(inner, "close", lambda: closed.append(True))
    with pytest.raises(StoreUnavailable):
        store.close()
    assert closed == [True]


def make_suite(tmp_path, layout, **kwargs):
    suite = SuiteSpec("batch", [
        ExperimentSpec(eid, lambda: iter(()), "v1") for eid in ("a", "b")
    ], str(tmp_path / "suite"), layout=layout)
    return SuiteStore.create(suite, batch_size=1000, flush_interval=0, **kwargs)


@pytest.mark.parametrize("layout", ["combined", "by_experiment"])
def test_suite_batches_are_atomic_per_database_and_retry_does_not_replay_committed_stores(
    tmp_path, monkeypatch, layout
):
    store = make_suite(tmp_path, layout)
    try:
        for eid in ("a", "b"):
            facts(store, namespace("batch", eid) + "sample")
        store.emit_event(EventRecord(ts=1, kind="catalog"))
        failing = store._child("b").inner
        original = failing._write_event

        def fail(row):
            original(row)
            raise OSError("child batch failed")

        with monkeypatch.context() as patch:
            patch.setattr(failing, "_write_event", fail)
            with pytest.raises(OSError, match="child batch failed"):
                store.flush()
        if layout == "by_experiment":
            assert len(store._child("a").inner.attempts()) == 3
            assert len(store.catalog.events()) == 1
        assert failing.attempts() == failing.events() == []
        store.flush()
        assert len(store.attempts()) == 6
        assert len(store.events()) == 7
    finally:
        store.close()


def test_failed_eviction_keeps_batch_and_connection_available_for_retry(tmp_path, monkeypatch):
    store = make_suite(tmp_path, "by_experiment", max_open=1)
    try:
        facts(store, namespace("batch", "a") + "sample")
        first = store._child("a")
        with monkeypatch.context() as patch:
            def fail(*args):
                raise OSError("cannot flush")
            patch.setattr(first.inner, "write_facts", fail)
            with pytest.raises(OSError, match="cannot flush"):
                store._child("b")
        assert list(store._children) == ["a"]
        assert first.pending == 6
        assert first.inner.attempts() == []
        store._child("b")
        assert list(store._children) == ["b"]
        assert len(store._child("a").attempts()) == 3
        assert len(store._child("a").events()) == 3
    finally:
        store.close()


def test_suite_close_releases_every_connection_even_when_one_flush_fails(tmp_path, monkeypatch):
    store = make_suite(tmp_path, "by_experiment")
    children = [store._child(eid) for eid in ("a", "b")]
    for child in children:
        facts(child)
    error = OSError("first child's flush failed")

    def fail(*args):
        raise error

    monkeypatch.setattr(children[0].inner, "write_facts", fail)
    with pytest.raises(OSError) as caught:
        store.close()
    assert caught.value is error
    for inner in [children[0].inner, children[1].inner, store.catalog]:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            inner._conn.execute("SELECT 1")
    assert store._writer_guard is None
    reader = SqliteStore(str(store._child_path("b")), read_only=True)
    try:
        assert len(reader.attempts()) == len(reader.events()) == 3
    finally:
        reader.close()
