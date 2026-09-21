"""Reproduce Suite query costs: PYTHONPATH=src python benchmarks/suite_queries.py.

Uses a temporary combined Suite, synthetic event payloads, and a warm connection.
Times are medians of three calls; baseline times run once. VM steps are sampled
in increments of 100. The baseline is the previous paged Python top-N/count.
"""

import argparse
import heapq
import json
import statistics
import tempfile
import time
from contextlib import closing
from operator import attrgetter

from pyattacker import ExperimentSpec, SuiteSpec, SuiteStore, pipeline
from pyattacker.store.suite import _PREFIX_SQL
from pyattacker.tasks import echo


def measure(conn, operation, repeats=3):
    elapsed = []
    steps = 0

    def progress():
        nonlocal steps
        steps += 100
        return 0

    for _ in range(repeats):
        steps = 0
        conn.set_progress_handler(progress, 100)
        started = time.perf_counter()
        try:
            operation()
        finally:
            elapsed.append((time.perf_counter() - started) * 1000)
            conn.set_progress_handler(None, 0)
    return {"ms": round(statistics.median(elapsed), 3), "vm_steps_approx": steps}


def benchmark(size):
    with tempfile.TemporaryDirectory() as root:
        tpl = pipeline("probe", echo)
        suite = SuiteSpec(
            "query-bench",
            [ExperimentSpec(eid, lambda: tpl.map([1]), "v1") for eid in ("a", "b")],
            root,
            "combined",
        )
        with suite.runner(handle_signals=False) as runner:
            runner.run(suite.pipelines(runner.store))
        with closing(SuiteStore(root)) as store:
            pids = [row.pipeline_id for row in store.pipelines()]
            conn = store.catalog._conn
            started = time.perf_counter()
            conn.executemany(
                "INSERT INTO events(ts,kind,scope,run_id,pipeline_id,data_json) "
                "VALUES(?, 'bench', 'pipeline', 'bench', ?, ?)",
                ((i, pids[i % 2], '{"value":"' + "x" * 128 + '"}') for i in range(size)),
            )
            conn.commit()
            result = {"events": size, "insert_s": round(time.perf_counter() - started, 3)}
            result["query_plans"] = {}
            for scope, clause, args in (
                ("global", "", []),
                ("run", "WHERE run_id=?", ["bench"]),
                ("member", f"WHERE {_PREFIX_SQL}=?", [pids[0][:39]]),
                ("member_run", f"WHERE {_PREFIX_SQL}=? AND run_id=?", [pids[0][:39], "bench"]),
            ):
                result["query_plans"][scope] = [
                    row[3]
                    for row in conn.execute(
                        f"EXPLAIN QUERY PLAN SELECT * FROM events {clause} ORDER BY ts DESC,event_id DESC LIMIT 20",
                        args,
                    )
                ]
            result["events20"] = measure(conn, lambda: store.events(limit=20))
            result["run_events20"] = measure(conn, lambda: store.events(run_id="bench", limit=20))
            result["count"] = measure(conn, store.count_events)
            result["stats"] = measure(conn, store.stats)
            result["old_events20"] = measure(
                conn,
                lambda: heapq.nlargest(20, store.iter_events(), key=attrgetter("ts", "event_id")),
                repeats=1,
            )
            result["old_count"] = measure(conn, lambda: sum(1 for _ in store.iter_events()), repeats=1)
        with closing(SuiteStore(root, read_only=True, experiment="a")) as store:
            result["member_events20"] = measure(store.catalog._conn, lambda: store.events(limit=20))
            result["member_run_events20"] = measure(
                store.catalog._conn, lambda: store.events(run_id="bench", limit=20)
            )
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 100_000, 1_000_000])
    for count in parser.parse_args().sizes:
        print(json.dumps(benchmark(count)), flush=True)
