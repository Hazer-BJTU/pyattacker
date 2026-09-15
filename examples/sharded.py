"""Sharding from Python: one store per shard, then join the results.

Run it with:
    uv run python examples/sharded.py

In production each shard is its own process (`pyattacker run --shard i/N`), possibly on its own
machine. Here we run them one after another in a single process to keep the example readable —
the stores, the shard assignment and the merge are exactly the same either way.

The point of the example: sharding is *not* a scheduling trick, it is a data-partitioning rule.
`shard_specs` splits the stream by the content-addressed pipeline key, so rerunning with
`resume=True` puts every pipeline back into the store that already owns it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from pyattacker import (
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    SqliteStore,
    export_store,
    merge_reports,
    pipeline,
    shard_specs,
    shard_store_path,
    task,
)

SHARDS = 3
SEEDS = 18
BASE_STORE = "runs/sharded-example.db"

CALLS = {"ask": 0}


class FakeClient:
    """Stands in for your provider client — the framework never touches the network."""

    def __init__(self, options: dict) -> None:
        self.model = options.get("model", "unknown")

    async def chat(self, question: str) -> str:
        await asyncio.sleep(0.002)
        CALLS["ask"] += 1
        if CALLS["ask"] % 7 == 0:  # every 7th call fails once, so retries show up in the record
            raise RetryableError("upstream 503", error_class="upstream")
        return f"[{self.model}] {question}"


def make_client(resource: Resource) -> FakeClient:
    """The factory receives the Resource (called once per resource, shared by all its leases)."""
    return FakeClient(resource.options)


@task("fetch")
def fetch(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": f"{seed['n']} + {seed['n']} = ?"}


@task(
    "ask",
    resource="apis",
    algorithm="backoff",
    retry=Retrying(max_attempts=3, on=(RetryableError,), base=0.005, cap=0.02),
)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        answer = await lease.client.chat(row["question"])
        lease.report(ok=True, usage={"tokens": len(answer)})
        return {"qid": row["qid"], "answer": answer, "resource": lease.resource.id}


@task("score")
def score(row: dict) -> dict:
    return {"qid": row["qid"], "passed": bool(row["answer"]), "answer": row["answer"]}


def make_pool() -> Pool:
    return Pool(
        "apis",
        [
            Resource.create(
                "llm", id=f"api-{i}", capacity=3, options={"model": "gpt-4o"}, factory=make_client
            )
            for i in range(1, 4)
        ],
        algorithm="backoff",
    )


def seeds():
    for i in range(SEEDS):
        yield {"qid": f"q{i:03d}", "n": i}


def main() -> None:
    Path("runs").mkdir(exist_ok=True)
    template = pipeline("sharded_qa", fetch | ask | score, tags={"example": "sharded"})
    paths: list[str] = []

    for index in range(SHARDS):
        store = shard_store_path(BASE_STORE, index, SHARDS)
        paths.append(store)
        # `shard_specs` is the whole partitioning rule: a pure function of the pipeline key.
        specs = shard_specs(template.map(seeds()), index, SHARDS)
        with Runner(store=store, pools=[make_pool()], concurrency=4, label=f"shard{index}") as runner:
            report = runner.run(specs)
        states = report.stats["pipelines"]["by_state"]
        print(f"shard {index}/{SHARDS} -> {store}: " + " ".join(f"{k}={v}" for k, v in states.items()))

    print("\n=== merged view ===")
    merged = merge_reports(paths)
    print(merged.summary())

    print("\n=== per-task rows, CSV (what you would load into pandas) ===")
    store = SqliteStore(paths[0], read_only=True)
    try:
        rows = export_store(store, "runs/sharded-tasks.csv", kind="tasks", fmt="csv")
    finally:
        store.close()
    print(f"wrote {rows} task rows to runs/sharded-tasks.csv")

    print("\n=== merged export ===")
    print(f"wrote {merged.export('runs/sharded-merged.jsonl')} pipelines to runs/sharded-merged.jsonl")
    print("CLI equivalent: uv run pyattacker report " + " ".join(paths))


if __name__ == "__main__":
    main()
