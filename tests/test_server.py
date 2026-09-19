"""The read-only HTTP monitoring endpoint (:class:`pyattacker.StatsServer`).

Every test starts the server on ``port=0`` (the OS picks a free port, the real one lands in
``server.port`` / ``server.url``) and fetches with ``urllib.request``; the store was seeded by a
real (small) run first, so the JSON assertions are about concrete values, not just shapes.

Coverage
* ``/`` (HTML, also ``/index.html``), ``/healthz``, ``/stats``, ``/metrics``, ``/events?limit=N``,
  ``/pipelines?state=&limit=``, ``/resources``, ``/errors`` and the 404 path
* limit handling: honoured, defaulted (50) for a bad value, clamped to >= 1
* run scoping: the configured ``run_id`` and a ``?run_id=`` override
* a live run is visible immediately (a fresh read-only connection per request — no cached snapshot)
* ``stop()`` releases the port (a second server binds the same explicit port), ``start()`` after
  ``stop()`` works, and ``wait()`` returns once the server is stopped
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any

import pytest

from pyattacker import MemoryStore, Runner, SqliteStore, boom, echo, pipeline
from pyattacker.server import StatsServer
from pyattacker.tasks import delay

_PATHS = ["/", "/stats", "/metrics", "/events", "/pipelines", "/resources", "/errors", "/healthz"]

# Seed layout (two runs, three pipelines, eight events):
#   run A: 2 successful "srv-ok" pipelines (task.succeeded + pipeline.succeeded each) + run.finished
#   run B: 1 failed "srv-bad" pipeline (task.failed + pipeline.failed) + run.finished
_SEEDED_EVENTS = 8


def _seed_store(db: str) -> tuple[str, str]:
    """Run two small pipelines into ``db``; returns ``(run_ok, run_bad)``."""
    runner = Runner(store=db, concurrency=2, handle_signals=False)
    try:
        ok = runner.run(pipeline("srv-ok", echo).map([{"i": 1}, {"i": 2}]))
        bad = runner.run(pipeline("srv-bad", boom("upstream exploded")).map([{"i": 3}]))
        return ok.run_id, bad.run_id
    finally:
        runner.close()


def _fetch(server: StatsServer, path: str, *, timeout: float = 5.0) -> tuple[int, str, bytes]:
    with urllib.request.urlopen(f"{server.url}{path}", timeout=timeout) as response:
        return response.status, response.headers.get("Content-Type", ""), response.read()


def _fetch_json(server: StatsServer, path: str) -> tuple[int, Any]:
    status, content_type, raw = _fetch(server, path)
    assert content_type == "application/json; charset=utf-8"
    return status, json.loads(raw.decode("utf-8"))


@pytest.fixture
def seeded(tmp_path):
    db = str(tmp_path / "server.db")
    run_ok, run_bad = _seed_store(db)
    return db, run_ok, run_bad


# ------------------------------------------------------------------ endpoint contract
def test_endpoints_serve_html_and_concrete_json(seeded):
    db, _run_ok, _run_bad = seeded
    with StatsServer(db, port=0) as server:
        assert server.port > 0
        assert server.url == f"http://127.0.0.1:{server.port}"

        status, content_type, raw = _fetch(server, "/")
        assert status == 200
        assert content_type == "text/html; charset=utf-8"
        html = raw.decode("utf-8")
        assert html.startswith("<!doctype html>")
        assert "<title>pyattacker</title>" in html
        assert "fetch(statsPath)" in html  # the page resolves one run, then reuses its scope
        assert "fetch('metrics?' + scope)" in html
        assert "fetch('pipelines?limit=50&' + scope)" in html
        assert "fetch('events?limit=40&' + scope)" in html
        assert _fetch(server, "/index.html")[2] == raw  # documented alias

        assert _fetch_json(server, "/healthz") == (200, {"ok": True})
        assert _fetch_json(server, "/health") == (200, {"ok": True})

        status, stats = _fetch_json(server, "/stats?run_id=all")
        assert status == 200
        assert stats["run_id"] is None  # not scoped: the whole store
        assert "run_status" not in stats  # only present when a run_id is known
        assert stats["pipelines"]["total"] == 3
        assert stats["pipelines"]["by_state"] == {"succeeded": 2, "failed": 1}
        assert set(stats["pipelines"]["duration_ms"]) == {"p50", "p95", "max"}
        assert stats["pipelines"]["duration_ms"]["p50"] > 0
        assert stats["tasks"]["by_name"] == {"mock.echo": 2, "mock.boom": 1}
        assert stats["attempts_total"] == 3
        assert stats["events_total"] == _SEEDED_EVENTS
        assert "buffered" not in stats  # the server strips runner-only fields
        assert len(stats["recent_errors"]) == 1
        assert stats["recent_errors"][0]["name"] == "srv-bad"
        assert stats["recent_errors"][0]["error_type"] == "RetryableError"
        assert stats["recent_errors"][0]["error_message"] == "upstream exploded"

        status, failed = _fetch_json(server, "/pipelines?state=failed&limit=1")
        assert status == 200
        assert failed["limit"] == 1
        assert len(failed["rows"]) == 1
        row = failed["rows"][0]
        assert row["name"] == "srv-bad"
        assert row["state"] == "failed"
        assert row["failed_task"] == "mock.boom"
        assert row["error_type"] == "RetryableError"
        assert row["error_message"] == "upstream exploded"
        assert (row["n_tasks_done"], row["n_tasks_total"], row["attempts_total"]) == (0, 1, 1)
        assert row["run_id"] == _run_bad
        assert row["finished_at"] >= row["started_at"]

        status, succeeded = _fetch_json(server, "/pipelines?run_id=all&state=succeeded")
        assert status == 200
        assert succeeded["limit"] == 50  # the default when the query omits it
        assert [r["name"] for r in succeeded["rows"]] == ["srv-ok", "srv-ok"]
        assert {r["run_id"] for r in succeeded["rows"]} == {_run_ok}

        status, errors = _fetch_json(server, "/errors")
        assert status == 200
        assert [e["failed_task"] for e in errors["rows"]] == ["mock.boom"]
        assert errors["rows"][0]["pipeline_id"] == row["pipeline_id"]
        assert set(errors["rows"][0]) == {
            "pipeline_id",
            "name",
            "failed_task",
            "error_type",
            "error_message",
            "finished_at",
        }

        status, resources = _fetch_json(server, "/resources")
        assert status == 200
        assert resources == {"rows": []}

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            _fetch(server, "/nope")
        assert excinfo.value.code == 404
        assert json.loads(excinfo.value.read().decode("utf-8")) == {
            "error": "unknown path '/nope'",
            "paths": _PATHS,
        }
        assert server.payload("/also-missing", {}) == (
            404,
            {"error": "unknown path '/also-missing'", "paths": _PATHS},
        )
        assert server.requests == 11  # every fetch above plus the direct payload() call


def test_stats_can_be_scoped_to_a_run_and_the_query_overrides_it(seeded):
    db, run_ok, run_bad = seeded
    with StatsServer(db, port=0, run_id=run_bad) as server:
        status, stats = _fetch_json(server, "/stats")
        assert status == 200
        assert stats["run_id"] == run_bad
        assert stats["run_status"] == "completed"
        assert stats["elapsed_s"] >= 0
        assert stats["pipelines"]["total"] == 1
        assert stats["pipelines"]["by_state"] == {"failed": 1}
        assert stats["tasks"]["by_name"] == {"mock.boom": 1}
        assert stats["attempts_total"] == 1
        assert stats["events_total"] == 3
        assert [e["name"] for e in stats["recent_errors"]] == ["srv-bad"]

        # an explicit ?run_id= wins over the one the server was configured with
        status, other = _fetch_json(server, f"/stats?run_id={run_ok}")
        assert status == 200
        assert other["run_id"] == run_ok
        assert other["pipelines"]["total"] == 2
        assert other["pipelines"]["by_state"] == {"succeeded": 2}
        assert other["recent_errors"] == []

        # ... and the same override works on /pipelines
        status, rows = _fetch_json(server, f"/pipelines?run_id={run_ok}&state=succeeded")
        assert [r["name"] for r in rows["rows"]] == ["srv-ok", "srv-ok"]
        assert rows["limit"] == 50


def test_events_limit_is_honoured_defaulted_and_clamped(seeded):
    db, _run_ok, _run_bad = seeded
    store = SqliteStore(db)
    try:
        expected = [e.kind for e in store.events(limit=2)]
        assert expected == ["pipeline.failed", "run.finished"]  # run B is the most recent run
    finally:
        store.close()

    with StatsServer(db, port=0) as server:
        status, one = _fetch_json(server, "/events?limit=1")
        assert status == 200
        assert one["limit"] == 1
        assert [r["kind"] for r in one["rows"]] == ["run.finished"]
        assert one["rows"][0]["event_id"] == _SEEDED_EVENTS
        assert one["rows"][0]["scope"] == "run"
        assert one["rows"][0]["pipeline_id"] is None

        status, two = _fetch_json(server, "/events?limit=2")
        assert two["limit"] == 2
        assert [r["kind"] for r in two["rows"]] == expected  # the most recent N, oldest first
        assert [r["event_id"] for r in two["rows"]] == [_SEEDED_EVENTS - 1, _SEEDED_EVENTS]
        assert set(two["rows"][0]) == {
            "event_id",
            "ts",
            "kind",
            "scope",
            "pipeline_id",
            "pool",
            "data",
        }

        status, defaulted = _fetch_json(server, "/events")
        assert status == 200
        assert defaulted["limit"] == 50
        assert len(defaulted["rows"]) == 3  # the latest run owns three events

        over = _fetch_json(server, "/events?limit=10000")[1]
        assert (over["limit"], len(over["rows"])) == (10000, 3)

        bad = _fetch_json(server, "/events?limit=not-a-number")[1]
        assert (bad["limit"], len(bad["rows"])) == (50, 3)

        clamped = _fetch_json(server, "/events?limit=0")[1]
        assert (clamped["limit"], len(clamped["rows"])) == (1, 1)


def test_resources_endpoint_works_through_a_file_backed_store_path(tmp_path):
    """End-to-end regression for issue #3: ``StatsServer(<sqlite path>)`` is exactly how the
    CLI's ``serve`` command starts the server — a raw path string, never a live store object.
    The old implementation only ever worked for the live-object case (see the test above),
    so this is the path that must be exercised directly.
    """
    from pyattacker import Pool, Resource

    db = str(tmp_path / "resources.db")
    pool = Pool("apis", [Resource.create("llm", id="api-1", options={"api_key": "sk-secret123"}, capacity=2)])
    runner = Runner(store=db, pools=[pool], handle_signals=False)
    try:
        runner.run(pipeline("srv-res", echo).map([{"i": 1}]))
    finally:
        runner.close()

    with StatsServer(db, port=0) as server:  # a raw path string, like the CLI passes
        status, body = _fetch_json(server, "/resources")
        assert status == 200
        assert "note" not in body
        assert len(body["rows"]) == 1
        row = body["rows"][0]
        assert row["pool"] == "apis"
        assert row["resource_id"] == "api-1"
        assert row["kind"] == "llm"
        assert row["state"] == "ready"
        assert row["spec"]["options"]["api_key"] == "***t123"  # redacted, not the raw secret
        assert row["stats"]["capacity"] == 2

        scoped = _fetch_json(server, "/resources?pool=apis")[1]
        assert len(scoped["rows"]) == 1
        assert _fetch_json(server, "/resources?pool=workers")[1] == {"rows": []}


def test_resources_endpoint_degrades_for_a_store_without_resources():
    """A custom Store that predates resources() must still satisfy isinstance(_, Store)
    (resources() was deliberately kept out of the protocol for this reason, see store/base.py)
    and the endpoint must degrade instead of raising.
    """

    class _LegacyStore(MemoryStore):
        resources = None  # hide the inherited method: simulates a pre-existing custom Store

    store = _LegacyStore()
    from pyattacker.store.base import Store

    assert isinstance(store, Store)

    with StatsServer(store, port=0) as server:
        status, body = _fetch_json(server, "/resources")
        assert status == 200
        assert body == {"rows": [], "note": "this store backend does not persist resource state"}


def test_resources_endpoint_reads_a_store_object():
    store = MemoryStore()
    store.upsert_resource("apis", "api-1", "llm", {"model": "gpt-4o"}, "ready", {"active": 0})

    with StatsServer(store, port=0) as server:
        status, body = _fetch_json(server, "/resources")
        assert status == 200
        assert "note" not in body
        assert len(body["rows"]) == 1
        assert body["rows"][0]["pool"] == "apis"
        assert body["rows"][0]["resource_id"] == "api-1"
        assert body["rows"][0]["kind"] == "llm"
        assert body["rows"][0]["state"] == "ready"
        assert body["rows"][0]["spec"] == {"model": "gpt-4o"}

        # the other endpoints work against a live store object too
        status, stats = _fetch_json(server, "/stats")
        assert status == 200
        assert stats["pipelines"] == {
            "total": 0,
            "by_state": {},
            "duration_ms": {"p50": None, "p95": None, "max": None},
        }
        assert _fetch_json(server, "/events")[1] == {"rows": [], "limit": 50}


# ------------------------------------------------------------------- live / lifecycle
def test_a_live_run_is_visible_through_a_fresh_connection(tmp_path):
    db = str(tmp_path / "live.db")
    first = Runner(store=db, concurrency=2, handle_signals=False)
    try:
        first.run(pipeline("live-ok", echo).map([{"i": 1}]))
    finally:
        first.close()

    with StatsServer(db, port=0) as server:
        _, before = _fetch_json(server, "/stats")
        assert before["pipelines"]["total"] == 1
        assert before["pipelines"]["by_state"] == {"succeeded": 1}

        # A second run writes to the same store while the server is up. The server holds no
        # connection of its own, so the next request must see it.
        second = Runner(store=db, concurrency=2, handle_signals=False)
        try:
            report = second.run(pipeline("live-ok-2", echo).map([{"i": 2}, {"i": 3}]))
        finally:
            second.close()

        _, after = _fetch_json(server, "/stats")
        assert after["run_id"] == report.run_id
        assert after["pipelines"]["total"] == 2
        assert after["pipelines"]["by_state"] == {"succeeded": 2}

        _, rows = _fetch_json(server, f"/pipelines?run_id={report.run_id}")
        assert [r["name"] for r in rows["rows"]] == ["live-ok-2", "live-ok-2"]
        assert [r["state"] for r in rows["rows"]] == ["succeeded", "succeeded"]


def test_stop_releases_the_port_and_restart_works(tmp_path):
    db = str(tmp_path / "port.db")
    SqliteStore(db).close()

    first = StatsServer(db, port=0).start()
    port = first.port
    assert port > 0
    assert _fetch_json(first, "/healthz") == (200, {"ok": True})
    first.stop()
    assert first.stop() is None  # stop() is idempotent

    # binding the same explicit port immediately afterwards proves the socket was released
    second = StatsServer(db, port=port).start()
    try:
        assert second.port == port
        assert _fetch_json(second, "/healthz") == (200, {"ok": True})
    finally:
        second.stop()

    # ... and the same instance can be started again
    third = first.start()
    assert third is first
    assert first.port > 0
    try:
        assert _fetch_json(first, "/healthz") == (200, {"ok": True})
    finally:
        first.stop()


# --------------------------------------------------------------------- concurrency
def test_concurrent_requests_are_safe_alongside_a_live_writing_run(tmp_path):
    """The documented core use case (design.md §4.6, server.py's own docstring): the server must
    be safely readable *while* a run is actively writing to the same store, and it must survive
    many requests arriving at once (each request opens its own short-lived read-only connection —
    a bug that shared one connection across threads would show up here as corrupted/garbled JSON
    or a crash, not just stale data).
    """
    import concurrent.futures

    db = str(tmp_path / "concurrent.db")
    SqliteStore(db).close()

    runner = Runner(store=db, concurrency=4, handle_signals=False)
    write_thread = threading.Thread(
        target=runner.run,
        args=(pipeline("srv-concurrent", delay(0.05)).map([{"i": i} for i in range(20)]),),
        daemon=True,
    )
    write_thread.start()
    try:
        with StatsServer(db, port=0) as server:
            results: list[tuple[int, Any]] = []
            errors: list[Exception] = []

            def _hit(path: str) -> None:
                try:
                    results.append(_fetch_json(server, path))
                except Exception as exc:  # pragma: no cover - failure path, asserted on below
                    errors.append(exc)

            paths = ["/stats", "/events?limit=20"] * 15
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(_hit, paths))

            assert not errors
            assert len(results) == len(paths)
            totals = [
                status_body[1]["pipelines"]["total"]
                for status_body in results
                if "pipelines" in status_body[1]
            ]
            assert all(status == 200 for status, _ in results)
            assert all(0 <= total <= 20 for total in totals)  # never negative, never more than admitted
            # the run's own writes eventually land: a request issued after join() sees them all
            write_thread.join(timeout=10.0)
            assert not write_thread.is_alive()
            _, final = _fetch_json(server, "/stats")
            assert final["pipelines"]["total"] == 20
            assert final["pipelines"]["by_state"] == {"succeeded": 20}
    finally:
        write_thread.join(timeout=10.0)
        runner.close()


def test_wait_returns_once_the_server_is_stopped(tmp_path):
    db = str(tmp_path / "wait.db")
    SqliteStore(db).close()

    server = StatsServer(db, port=0).start()
    port = server.port
    timer = threading.Timer(0.05, server.stop)
    waiter = threading.Thread(target=server.wait, name="wait-test", daemon=True)
    timer.start()
    waiter.start()
    try:
        waiter.join(timeout=5.0)
        assert not waiter.is_alive()  # wait() returned after stop()
    finally:
        timer.cancel()
        timer.join(timeout=5.0)
        # wait() only joins the serving thread; stop() is what closes the socket, so make sure it
        # has fully returned before rebinding the port.
        server.stop()

    rebound = StatsServer(db, port=port).start()
    try:
        assert rebound.port == port
        assert _fetch_json(rebound, "/healthz") == (200, {"ok": True})
    finally:
        rebound.stop()
