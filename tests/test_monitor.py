"""Monitoring text rendering (`monitor.py`): `_bar`, `render_snapshot`, `read_snapshot`, `watch`.

`watch` itself is a thin polling loop around `read_snapshot`/`render_snapshot`/`printer`; it is
exercised end-to-end here with a real store and a bounded `iterations` count (no sleeping on
wall-clock time, `interval=0`).
"""

from __future__ import annotations

import asyncio

from pyattacker import Runner, boom, echo, pipeline
from pyattacker.monitor import _bar, read_snapshot, render_snapshot, watch


# --------------------------------------------------------------------------- _bar
def test_bar_at_zero_and_full():
    assert _bar(0, 10, width=10) == "·" * 10
    assert _bar(10, 10, width=10) == "█" * 10


def test_bar_partial_rounds_to_the_nearest_cell():
    assert _bar(5, 10, width=10) == "█" * 5 + "·" * 5
    assert _bar(1, 3, width=6) == "██" + "·" * 4  # round(6 * 1/3) == 2


def test_bar_tolerates_a_total_of_zero_or_negative():
    assert _bar(0, 0, width=8) == "·" * 8
    assert _bar(5, -1, width=8) == "·" * 8


def test_bar_over_100_percent_clamps_to_a_full_fixed_width_bar():
    # a value greater than total (e.g. transiently inconsistent by_state counts) must still
    # produce exactly `width` characters, not a longer string -- callers rely on a fixed width.
    assert _bar(15, 10, width=10) == "█" * 10
    assert len(_bar(1000, 1, width=10)) == 10


# --------------------------------------------------------------------- render_snapshot
def _seed_store(db: str) -> tuple[str, str]:
    runner = Runner(store=db, concurrency=2, handle_signals=False)
    try:
        ok = runner.run(pipeline("mon-ok", echo).map([{"i": 1}, {"i": 2}]))
        bad = runner.run(pipeline("mon-bad", boom("upstream exploded")).map([{"i": 3}]))
        return ok.run_id, bad.run_id
    finally:
        runner.close()


def test_render_snapshot_includes_pipeline_bar_and_state_counts(tmp_path):
    db = str(tmp_path / "mon.db")
    _seed_store(db)
    from pyattacker import SqliteStore

    store = SqliteStore(db, read_only=True)
    try:
        snapshot = read_snapshot(store)
        text = render_snapshot(snapshot)
    finally:
        store.close()

    assert "pipelines" in text
    assert "failed=1" in text
    assert "succeeded=2" in text
    assert "█" in text or "·" in text  # the bar rendered something
    assert "tasks" in text
    assert "mock.echo=2" in text
    assert "mock.boom=1" in text
    assert "attempts    total=3" in text
    assert "! mon-bad/mock.boom: RetryableError:" in text  # recent_errors surfaced


def test_render_snapshot_scoped_to_a_run_shows_status_and_elapsed(tmp_path):
    from pyattacker import SqliteStore

    db = str(tmp_path / "mon2.db")
    _run_ok, run_bad = _seed_store(db)

    store = SqliteStore(db, read_only=True)
    try:
        snapshot = read_snapshot(store, run_id=run_bad)
        text = render_snapshot(snapshot)
    finally:
        store.close()

    assert f"run={run_bad}" in text
    assert "status=completed" in text
    assert "failed=1" in text
    assert "succeeded" not in text.split("\n")[1]  # this run only has the failed pipeline


def test_render_snapshot_omits_optional_sections_when_absent():
    # a minimal snapshot (no pools, no leaks, no durations) must not crash or print empty sections
    snapshot = {
        "run_id": None,
        "elapsed_s": 0.0,
        "in_flight_pipelines": 0,
        "pipelines": {"total": 0, "by_state": {}, "duration_ms": {"p50": None, "p95": None, "max": None}},
        "tasks": {"by_name": {}},
        "pools": {},
        "recent_errors": [],
    }
    text = render_snapshot(snapshot)
    assert "latency ms" not in text
    assert "pool " not in text
    assert "leaked leases" not in text
    assert "attempts" not in text


def test_render_snapshot_shows_leaked_leases_and_stopping_flag():
    snapshot = {
        "run_id": "run-x",
        "elapsed_s": 1.0,
        "in_flight_pipelines": 0,
        "stopping": True,
        "pipelines": {"total": 1, "by_state": {"succeeded": 1}, "duration_ms": {"p50": None, "p95": None, "max": None}},
        "tasks": {"by_name": {}},
        "pools": {},
        "leases_leaked": 2,
        "recent_errors": [],
    }
    text = render_snapshot(snapshot)
    assert "[stopping]" in text
    assert "!! leaked leases: 2" in text


def test_render_snapshot_shows_pool_bars():
    snapshot = {
        "run_id": "run-x",
        "elapsed_s": 1.0,
        "in_flight_pipelines": 0,
        "pipelines": {"total": 0, "by_state": {}, "duration_ms": {"p50": None, "p95": None, "max": None}},
        "tasks": {"by_name": {}},
        "pools": {
            "apis": {"active": 1, "capacity": 4, "ready": 3, "degraded": 0, "dead": 0, "waiting": 0},
        },
        "recent_errors": [],
    }
    text = render_snapshot(snapshot)
    assert "pool apis" in text
    assert "active=1/4" in text


# --------------------------------------------------------------------- watch
def test_watch_polls_a_fixed_number_of_times_and_prints_each_snapshot(tmp_path):
    from pyattacker import SqliteStore

    db = str(tmp_path / "watch.db")
    _seed_store(db)
    store = SqliteStore(db, read_only=True)
    printed: list[str] = []
    try:
        asyncio.run(
            watch(store, interval=0, printer=printed.append, clear=False, iterations=3)
        )
    finally:
        store.close()

    assert len(printed) == 3
    assert all("pipelines" in text for text in printed)


def test_watch_clear_prepends_ansi_escape(tmp_path):
    from pyattacker import SqliteStore

    db = str(tmp_path / "watch2.db")
    _seed_store(db)
    store = SqliteStore(db, read_only=True)
    printed: list[str] = []
    try:
        asyncio.run(watch(store, interval=0, printer=printed.append, clear=True, iterations=1))
    finally:
        store.close()

    assert printed[0].startswith("\033[2J\033[H")


def test_watch_stop_callback_ends_the_loop_immediately(tmp_path):
    from pyattacker import SqliteStore

    db = str(tmp_path / "watch3.db")
    _seed_store(db)
    store = SqliteStore(db, read_only=True)
    printed: list[str] = []
    try:
        asyncio.run(
            watch(store, interval=0, printer=printed.append, clear=False, stop=lambda: True)
        )
    finally:
        store.close()

    assert printed == []  # stop() was already true before the first snapshot
