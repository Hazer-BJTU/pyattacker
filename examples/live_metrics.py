"""Live, application-owned accuracy on the HTML dashboard.

Run ``uv run python examples/live_metrics.py`` and open the printed URL while it runs.
The application reads final artifacts, owns the accumulator, and only reports its
latest values to pyattacker. The database can be reopened to rebuild the accumulator.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from pyattacker import Runner, SqliteStore, StatsServer, pipeline, task
from pyattacker.artifact import DEFAULT_REGISTRY
from pyattacker.store import iter_pipelines


@task("live.judge")
async def judge(seed: dict) -> dict:
    await asyncio.sleep(0.1)
    return {"correct": seed["answer"] == seed["expected"]}


def rebuild(db: str) -> dict[str, bool]:
    """Rebuild application state from durable final artifacts after a restart."""
    results: dict[str, bool] = {}
    store = SqliteStore(db, read_only=True)
    try:
        for record in iter_pipelines(store, state="succeeded"):
            artifact = next((item for item in store.artifacts(record.pipeline_id) if item.is_final), None)
            if artifact is not None and artifact.available:
                results[record.pipeline_id] = bool(DEFAULT_REGISTRY.load(artifact.encoded())["correct"])
    finally:
        store.close()
    return results


def main() -> None:
    db = "runs/live_metrics.db"
    Path(db).parent.mkdir(exist_ok=True)
    results = rebuild(db) if Path(db).exists() else {}
    runner = None

    def completed(record, artifact) -> None:
        if artifact is not None and artifact.available:
            results[record.pipeline_id] = bool(runner.registry.load(artifact.encoded())["correct"])
        elif artifact is None:
            results.pop(record.pipeline_id, None)
        # A failed pipeline does not enter this example's accuracy denominator.
        evaluated = len(results)
        runner.report_metric("evaluated", evaluated, label="Evaluated")
        runner.report_metric("correct", sum(results.values()), label="Correct")
        if evaluated:
            runner.report_metric("accuracy", sum(results.values()) / evaluated,
                                 label="Accuracy", display="percent")

    with Runner(store=db, on_pipeline_finished=completed, retry_succeeded=True,
                handle_signals=False) as active:
        runner = active
        with StatsServer(db, port=0) as server:
            print(f"Monitor: {server.url}")
            rows = ({"answer": i, "expected": i if i % 3 else i + 1} for i in range(100))
            runner.run(pipeline("live-eval", judge).map(rows))


if __name__ == "__main__":
    main()
