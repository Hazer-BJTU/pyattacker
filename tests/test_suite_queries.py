"""Query contracts and bounded work, independent of the Suite database layout."""

import heapq
from collections import Counter
from contextlib import closing
from operator import attrgetter

import pytest
from test_suite import make_suite, run

from pyattacker import ExperimentSpec, Handoff, MemoryStore, SqliteStore, SuiteStore, pipeline, task
from pyattacker.store import EventRecord, TaskRecord
from pyattacker.store.suite import _PREFIX_SQL
from pyattacker.store.writebehind import WriteBehindStore
from pyattacker.tasks import echo


@pytest.fixture(params=["combined", "by_experiment"])
def suite(request, tmp_path):
    @task("finish")
    def finish(seed):
        return Handoff.end(seed)

    tpl = pipeline("query", finish, echo, control={"edges": {"finish": ["end"]}})
    spec = make_suite(
        tmp_path,
        request.param,
        experiments=[ExperimentSpec(eid, lambda: tpl.map([1, 2]), "v1") for eid in ("a", "b", "c")],
    )
    report = run(spec)
    assert report.stats["handoffs_total"] == 6
    return spec


@pytest.mark.parametrize("backend", ["memory", "sqlite", "buffered", "combined", "by_experiment"])
def test_task_limit_contract(backend, tmp_path):
    if backend in ("memory", "sqlite", "buffered"):
        store = MemoryStore() if backend == "memory" else SqliteStore(str(tmp_path / "db"))
        if backend == "buffered":
            store = WriteBehindStore(store)
        for pid in ("b", "a"):
            for visit in (2, 1):
                store.record_task(TaskRecord(f"{pid}:{visit}", pid, "r", "t", 0, visit=visit))
    else:
        spec = make_suite(tmp_path, backend)
        run(spec)
        store = SuiteStore(spec.output_root, read_only=True, max_open=1)
    try:
        rows = store.tasks(limit=None)
        assert rows == sorted(rows, key=attrgetter("pipeline_id", "seq", "visit", "task_run_id"))
        assert store.tasks(limit=0) == []
        assert store.tasks(run_id="absent", limit=1) == []
        assert store.tasks(limit=1) == rows[:1]
        assert store.tasks(rows[0].pipeline_id, limit=1) == rows[:1]
        assert store.tasks(run_id=rows[0].run_id, limit=1) == rows[:1]
        for invalid in (-1, True, 1.5, "1"):
            with pytest.raises(ValueError, match="limit"):
                store.tasks(limit=invalid)
    finally:
        store.close()


def test_recent_events_filter_before_limit_and_sort_by_time(suite, monkeypatch):
    with closing(SuiteStore(suite.output_root, max_open=1)) as store:
        pids = {p.experiment_id: p.pipeline_id for p in store.pipelines()}
        for eid in ("a", "b", "c"):
            for i, ts in enumerate((100, 300, 200, 50)):
                store.emit_event(
                    EventRecord(ts, "probe", run_id="probe", pipeline_id=pids[eid], data={"i": i})
                )
        store.emit_event(EventRecord(400, "probe", scope="run", run_id="probe"))
    for experiment in (None, "a"):
        with closing(
            SuiteStore(suite.output_root, read_only=True, experiment=experiment, max_open=1)
        ) as store:
            for filters in (
                {"run_id": "probe"},
                {"kind": "probe"},
                {"pipeline_id": pids["a"], "run_id": "probe"},
            ):
                all_rows = list(store.iter_events(**filters))
                key = attrgetter("ts", "event_id")
                assert store.count_events(**filters) == len(all_rows)
                assert store.events(limit=None, **filters) == sorted(all_rows, key=key)
                for limit in (0, 1, 3, 100):
                    expected = sorted(heapq.nlargest(limit, all_rows, key=key), key=key)
                    assert store.events(limit=limit, **filters) == expected
            monkeypatch.setattr(store, "iter_events", lambda **_: pytest.fail("decoded all events"))
            assert len(store.events(run_id="probe", limit=1)) == 1
            assert store.count_events(run_id="probe") == (4 if experiment else 13)


def test_handoff_filter_and_timestamp_order(suite):
    with closing(SuiteStore(suite.output_root, max_open=1)) as store:
        for _, child in store._stores():
            inner = getattr(child, "inner", child)
            # Later inserts have earlier timestamps; IDs and event time disagree.
            inner._conn.execute("UPDATE handoffs SET ts=1000-handoff_id")
            inner._conn.commit()
    for experiment in (None, "a"):
        with closing(
            SuiteStore(suite.output_root, read_only=True, experiment=experiment, max_open=1)
        ) as store:
            rows = store.handoffs()
            assert rows
            assert store.handoffs(limit=0) == []
            for limit in (1, 2, 100):
                assert store.handoffs(limit=limit) == rows[-limit:]
                assert store.handoffs(run_id=rows[0].run_id, limit=limit) == rows[-limit:]
            assert store.handoffs(run_id="absent", limit=1) == []
            for row in store.pipelines():
                handoffs = store.handoffs(pipeline_id=row.pipeline_id)
                assert store.handoffs(pipeline_id=row.pipeline_id, limit=1) == handoffs[-1:]


def test_sql_stats_equal_record_oracle_after_resume(suite, monkeypatch):
    with closing(SuiteStore(suite.output_root, read_only=True)) as store:
        first_run = store.pipelines()[0].run_id
        before = store.stats(first_run)
    second = run(suite, selected=["a"], resume=True)
    with closing(SuiteStore(suite.output_root, read_only=True)) as store:
        assert store.stats(first_run) == before
    for experiment in (None, "a"):
        with closing(
            SuiteStore(suite.output_root, read_only=True, experiment=experiment, max_open=1)
        ) as store:
            expected = {}
            for rid in (None, first_run, second.run_id, "absent"):
                pipelines = list(store.iter_pipelines(run_id=rid))
                attempts = list(store.iter_attempts(run_id=rid))
                names, used = Counter(), Counter()
                if rid is None:
                    for row in store.iter_tasks():
                        names[row.name] += 1
                        used[row.name] += row.attempts_used
                else:
                    seen = set()
                    for row in attempts:
                        used[row.task_name] += 1
                        slot = row.pipeline_id, row.task_run_id
                        if slot not in seen:
                            names[row.task_name] += 1
                            seen.add(slot)
                durations = sorted(
                    (p.finished_at - p.started_at) * 1000
                    for p in pipelines
                    if p.finished_at is not None and p.started_at is not None
                )
                expected[rid] = {
                    "pipelines": {
                        "total": len(pipelines),
                        "by_state": dict(Counter(p.state for p in pipelines)),
                        "duration_ms": {
                            name: round(durations[int(q * (len(durations) - 1))], 3) if durations else None
                            for name, q in (("p50", 0.5), ("p95", 0.95), ("max", 1))
                        },
                    },
                    "tasks": {"by_name": dict(names), "attempts_by_name": dict(used)},
                    "attempts_total": sum(p.attempts_total for p in pipelines) if rid else len(attempts),
                    "events_total": len(list(store.iter_events(run_id=rid))),
                    "handoffs_total": len(store.handoffs(run_id=rid)),
                }

            def forbidden(*args, **kwargs):
                pytest.fail("statistics must not decode fact records")

            for name in ("iter_pipelines", "iter_tasks", "iter_attempts", "iter_events", "handoffs"):
                monkeypatch.setattr(store, name, forbidden)
            for rid, oracle in expected.items():
                actual = store.stats(rid)
                assert {key: actual[key] for key in oracle} == oracle


def test_indexed_limit_work_and_read_only_legacy(suite):
    with closing(SuiteStore(suite.output_root)) as store:
        pid = store.pipelines()[0].pipeline_id
        child = store._child(store._eid(pid))
        inner = getattr(child, "inner", child)
        conn = inner._conn
        conn.executemany(
            "INSERT INTO events(ts,kind,scope,run_id,pipeline_id,data_json) VALUES(?, 'bulk','pipeline','bulk',?,'{}')",
            ((i, pid) for i in range(10000)),
        )
        conn.commit()
        for where, args in (
            ("", []),
            ("WHERE run_id=?", ["bulk"]),
            (f"WHERE {_PREFIX_SQL}=?", [pid[:39]]),
            (f"WHERE {_PREFIX_SQL}=? AND run_id=?", [pid[:39], "bulk"]),
            ("WHERE pipeline_id=?", [pid]),
        ):
            sql = f"SELECT * FROM events {where} ORDER BY ts DESC,event_id DESC LIMIT 1"
            plan = " ".join(str(tuple(row)) for row in conn.execute("EXPLAIN QUERY PLAN " + sql, args))
            assert "suite_events_" in plan and "TEMP B-TREE" not in plan
            steps = []
            conn.set_progress_handler(lambda steps=steps: steps.append(1) or 0, 100)
            try:
                assert conn.execute(sql, args).fetchall()
            finally:
                conn.set_progress_handler(None, 0)
            assert len(steps) < 10
        expected = store.events(limit=3)
        for _, child in store._stores(include_catalog=True):
            inner = getattr(child, "inner", child)
            indexes = inner._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'suite_events_%'"
            ).fetchall()
            for row in indexes:
                inner._conn.execute(f'DROP INDEX "{row[0]}"')
            inner._conn.commit()
    from pathlib import Path

    files = list(Path(suite.output_root).rglob("*.db"))
    assert files
    before = {path: path.read_bytes() for path in files}
    with closing(SuiteStore(suite.output_root, read_only=True, max_open=1)) as store:
        assert store.events(limit=3) == expected
        assert store.count_events(kind="bulk") == 10000
    assert {path: path.read_bytes() for path in files} == before
