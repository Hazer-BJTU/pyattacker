"""End-to-end example: the SDK shape.

Run it with:
    uv run python examples/quickstart.py

It demonstrates four things:
1. how a resource `factory` turns "an endpoint" into a usable client object
   (here a fake client; in real life put your HTTP code where `await asyncio.sleep` is);
2. acquiring/releasing inside a task, and reporting resource health back to the pool;
3. pipeline = tasks chained linearly; the seed source is a generator, so memory is independent of dataset size;
4. semantic resume: the first round deliberately fails part of the work, the second round only re-runs the failed tasks.

Note: **this framework does not touch the network.** The `await asyncio.sleep(...)` inside FakeClient
is exactly where your HTTP request would go.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path

from pyattacker import (
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    pipeline,
    task,
)

STORE = "runs/quickstart.db"

# Make the first round fail part of the time so retries and resume are visible; flipped to False later.
STATE = {"unreliable": True}


class FakeClient:
    """What your own provider client should look like: build from `resource.options`, own your retry semantics."""

    def __init__(self, options: dict) -> None:
        self.options = options
        self.model = options.get("model", "unknown")

    async def chat(self, prompt: str, seed: int) -> str:
        await asyncio.sleep(0.002)  # <- real life: await self.http.post(...)
        rng = random.Random(seed)
        if STATE["unreliable"] and rng.random() < 0.4:
            raise RetryableError("upstream 503", error_class="upstream")
        return f"[{self.model}] answer to {prompt!r}"


def make_client(resource: Resource) -> FakeClient:
    """The factory: called once per resource, every lease then shares the same client object."""
    return FakeClient(resource.options)


# --------------------------------------------------------------------- tasks
@task("fetch")
def fetch(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": seed["question"]}


@task(
    "ask",
    resource="apis",
    algorithm="backoff",
    retry=Retrying(max_attempts=4, on=(RetryableError, TimeoutError), base=0.01, cap=0.05),
    timeout_s=10,
)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:  # <- released on exit, including on exception
        text = await lease.client.chat(row["question"], ctx.seed)
        # Report health/usage back to the pool so it can degrade bad endpoints and account for quota.
        lease.report(ok=True, latency_ms=2.0, usage={"tokens": len(text)})
        return {"qid": row["qid"], "answer": text, "resource": lease.resource.id}


@task("score")
def score(row: dict) -> dict:
    """Synchronous pure-compute task: produces the per-pipeline verdict that becomes the final artifact."""
    ok = "answer" in row["answer"]
    return {"qid": row["qid"], "passed": ok, "answer": row["answer"]}


def dataset(n: int = 12):
    for i in range(n):
        yield {"qid": f"q{i:03d}", "question": f"{i} + {i} = ?"}


def main() -> None:
    Path("runs").mkdir(exist_ok=True)
    pool = Pool(
        "apis",
        [
            Resource.create(
                "llm",
                id=f"api-{i}",
                capacity=3,
                options={"model": "gpt-4o", "base_url": f"https://api-{i}.example/v1"},
                factory=make_client,
            )
            for i in range(1, 5)
        ],
        algorithm="backoff",
    )
    template = pipeline("qa_eval", fetch | ask | score, tags={"example": "quickstart"})

    with Runner(store=STORE, pools=[pool], concurrency=16, label="quickstart") as runner:
        print("=== Round 1: 40% of requests fail on purpose, watch retries and persistence ===")
        first = runner.run(template.map(dataset()))
        print(first.summary())

        print("\n=== Round 2: resume -- succeeded pipelines are skipped, failed ones continue from their checkpoint ===")
        STATE["unreliable"] = False
        second = runner.run(template.map(dataset()), resume=True)
        print(second.summary())
        print(f"skipped (already succeeded): {second.skipped}")

        print("\n=== Final pool state ===")
        for item in pool.snapshot():
            print(
                f"  {item['id']:<8} {item['state']:<9} active={item['active']}/{item['capacity']} "
                f"leases={item['leases']} failed={item['failed']} ema={item['latency_ms_ema']}"
            )

        print("\n=== Structured log of one pipeline (every fact is queryable by pipeline_id) ===")
        row = next(iter(runner.store.export_rows()))
        print(f"  pipeline {row['pipeline_id'][:12]} state={row['state']} "
              f"tasks_done={row['n_tasks_done']}/{row['n_tasks_total']}")
        for event in runner.store.events(pipeline_id=row["pipeline_id"], limit=10):
            print(f"    {event.kind:<24} {event.data}")

        print(f"\nExported JSONL: {second.export_jsonl('runs/quickstart.jsonl')} pipelines -> runs/quickstart.jsonl")
        print(f"Inspect from the CLI: uv run pyattacker report {STORE}")


if __name__ == "__main__":
    main()
