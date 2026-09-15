"""The evaluation pipeline, in two shapes that produce the same numbers.

Shape 1 — **grouped**: three judge configurations are scored *inside one task*.

    A prepare ─▶ B ask (2 turns) ─▶ C score_all (3 judges, concurrently) ─▶ D reduce

Shape 2 — **split**: the same three scores are three separate tasks.

    A prepare ─▶ B ask (2 turns) ─▶ C1 judge_strict ─▶ C2 judge_balanced ─▶ C3 judge_terse ─▶ D reduce

Both end with the same artifact. The difference is where the framework draws its checkpoint:
in the grouped shape the only restart point is "before all three judges", in the split shape every
judge has its own artifact, so a failure at C3 costs one request instead of three. The demo
measures that difference instead of asserting it.

Task D is the *user's* code on purpose: this framework records facts and never reduces them.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator, Mapping
from typing import Any

from pyattacker import (
    FatalError,
    Pool,
    Resource,
    RetryableError,
    Retrying,
    TaskSpec,
    build_task_spec,
    pipeline,
    task,
)

from .backend import JUDGE_RUBRIC, FailureSwitch, RequestLog, backend_factory

__all__ = [
    "TURNS",
    "PASS_THRESHOLD",
    "JUDGES",
    "MAIN_MODEL",
    "seeds",
    "build_grouped",
    "build_split",
    "make_pools",
]

TURNS = 2
PASS_THRESHOLD = 3.5
MAIN_MODEL = "mock-gpt-large"

#: Three judge configurations that differ in the way real judge fleets do: prompt strictness,
#: sampling temperature and model size.
JUDGES: tuple[dict[str, Any], ...] = (
    {"name": "judge-strict", "key": "strict", "model": "mock-judge-strict", "temperature": 0.0},
    {
        "name": "judge-balanced",
        "key": "balanced",
        "model": "mock-judge-balanced",
        "temperature": 0.3,
    },
    {"name": "judge-terse", "key": "terse", "model": "mock-judge-terse", "temperature": 0.8},
)


# --------------------------------------------------------------------------- pools
def make_pools(
    log: RequestLog,
    *,
    switch: FailureSwitch | None = None,
    latency_ms: float = 8.0,
    failure_rate: float = 0.0,
) -> list[Pool]:
    """One pool for the main model, one for the judge fleet.

    Settings live in each resource's ``options`` so they are visible where they apply; the judge
    pool keeps ``algorithm="least_busy"`` so the three concurrent scoring calls spread across the
    three endpoints instead of queueing behind the first one.
    """
    factory = backend_factory(log, switch=switch)
    main = Pool(
        "main",
        [
            Resource.create(
                "llm",
                id="main-1",
                capacity=4,
                options={"model": MAIN_MODEL, "latency_ms": latency_ms, "failure_rate": failure_rate},
                tags={"role": "main"},
                factory=factory,
            )
        ],
        algorithm="wait",
    )
    judges = Pool(
        "judges",
        [
            Resource.create(
                "llm",
                id=judge["name"],
                capacity=1,
                options={
                    "model": judge["model"],
                    "temperature": judge["temperature"],
                    "latency_ms": latency_ms * 0.6,
                    "failure_rate": failure_rate,
                },
                tags={"role": "judge", "judge": judge["key"]},
                factory=factory,
            )
            for judge in JUDGES
        ],
        algorithm="least_busy",
    )
    return [main, judges]


# --------------------------------------------------------------------------- tasks
@task("prepare")
def prepare(seed: dict) -> dict:
    """A — normalise and check the raw dataset row before spending a single token on it."""
    question = " ".join(str(seed.get("question", "")).split()).strip()
    if not question:
        raise FatalError(f"row {seed.get('qid')!r} has no question")
    if not question.endswith("?"):
        question += "?"
    return {
        "qid": seed["qid"],
        "question": question,
        "expected": seed.get("expected"),
        "checks": {"whitespace_normalised": True, "chars": len(question)},
    }


@task(
    "ask",
    resource="main",
    algorithm="backoff",
    timeout_s=30,
    retry=Retrying(max_attempts=4, on=(RetryableError, TimeoutError), base=0.02, cap=0.2),
)
async def ask(row: dict, ctx: Any) -> dict:
    """B — the main model, called **twice** to simulate a two-turn conversation.

    The loop lives inside the task, and each turn acquires and releases the pool. That is the
    intended pattern: a task may make as many requests as it likes, but it must not hold a lease
    between them, or one slow conversation would occupy a slot in the pool for its whole duration.
    """
    messages: list[dict[str, str]] = [
        {"role": "system", "content": "You are a careful arithmetic tutor. Show your reasoning."},
        {"role": "user", "content": row["question"]},
    ]
    turns: list[str] = []
    for turn in range(1, TURNS + 1):
        if turn > 1:
            messages.append(
                {"role": "user", "content": "Check your work and restate the final answer."}
            )
        async with ctx.acquire() as lease:  # released before the next turn starts
            reply = await lease.client.complete(messages, temperature=0.2)
            lease.report(ok=True, usage={"tokens": len(reply.split())})
        messages.append({"role": "assistant", "content": reply})
        turns.append(reply)
        ctx.emit("ask.turn", turn=turn, chars=len(reply))
    return {**row, "answer": turns[-1], "conversation": messages, "turns": turns}


def _judge_messages(judge: Mapping[str, Any], row: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": f"{JUDGE_RUBRIC} You are {judge['name']}.",
        },
        {
            "role": "user",
            "content": f"Question: {row['question']}\nAnswer: {row['answer']}",
        },
    ]


def _parse_score(raw: str, judge: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the judge's reply. Models return malformed JSON often enough to matter."""
    try:
        payload = json.loads(raw)
        score = int(payload["score"])
    except (ValueError, KeyError, TypeError) as exc:
        raise RetryableError(
            f"{judge['name']} returned unusable output: {raw[:60]!r} ({exc})",
            error_class="invalid_response",
        ) from exc
    if not 1 <= score <= 5:
        raise RetryableError(
            f"{judge['name']} scored {score}, outside 1-5", error_class="invalid_response"
        )
    return {"judge": judge["name"], "score": score, "reason": str(payload.get("reason", ""))[:120]}


@task(
    "score_all",
    resource="judges",
    algorithm="least_busy",
    timeout_s=30,
    retry=Retrying(max_attempts=2, on=(RetryableError, TimeoutError), base=0.02, cap=0.2),
)
async def score_all(row: dict, ctx: Any) -> dict:
    """C — every judge configuration, concurrently, inside a single task.

    Convenient and fast, but the framework can only checkpoint the *group*: if one judge fails, a
    retry re-sends all three, and a resume starts the whole group over. Compare
    :func:`judge_task`, where each judge is its own checkpoint.
    """

    async def score_one(judge: Mapping[str, Any]) -> dict[str, Any]:
        async with ctx.acquire(judge=judge["key"]) as lease:
            raw = await lease.client.complete(
                _judge_messages(judge, row),
                temperature=judge["temperature"],
                response_format="json",
            )
            try:
                score = _parse_score(raw, judge)
            except RetryableError as exc:
                lease.report(ok=False, error=exc)  # a bad reply is a real signal about this endpoint
                raise
            lease.report(ok=True, usage={"tokens": len(raw.split())})
            return score

    # return_exceptions=True on purpose: with plain `gather` the first failure propagates at once
    # and the other two requests are *orphaned* — still in flight, still holding a lease, their
    # results thrown away and their retry duplicated. Waiting for every branch keeps the record
    # honest and the cost of the retry measurable.
    results = await asyncio.gather(*(score_one(judge) for judge in JUDGES), return_exceptions=True)
    failed = [
        (judge["name"], result)
        for judge, result in zip(JUDGES, results, strict=True)
        if isinstance(result, BaseException)
    ]
    if failed:
        ctx.emit(
            "score.partial",
            failed=sorted(name for name, _ in failed),
            completed=len(results) - len(failed),
            note="these scores are discarded; the retry re-sends all three judges",
        )
        raise failed[0][1]
    scores = [item for item in results if isinstance(item, dict)]
    return {**row, "scores": {item["judge"]: item for item in scores}}


def judge_task(judge: Mapping[str, Any]) -> TaskSpec:
    """Build C1/C2/C3: one judge configuration per task, each with its own artifact.

    This is the whole trick of the split shape. Because a task's artifact is persisted the moment
    it is produced, a failure in the *next* judge resumes from here instead of replaying the
    judges that already answered.

    The pipeline is a linear chain, so each task carries the scores it inherited forward — the
    standard accumulator idiom, and the reason the chain stays unary.
    """

    async def _impl(row: dict, ctx: Any) -> dict:
        async with ctx.acquire(judge=judge["key"]) as lease:
            raw = await lease.client.complete(
                _judge_messages(judge, row),
                temperature=judge["temperature"],
                response_format="json",
            )
            try:
                score = _parse_score(raw, judge)
            except RetryableError as exc:
                lease.report(ok=False, error=exc)
                raise
            lease.report(ok=True, usage={"tokens": len(raw.split())})
        return {**row, "scores": {**row.get("scores", {}), score["judge"]: score}}

    _impl.__name__ = judge["key"]
    return build_task_spec(
        _impl,
        name=judge["name"],
        resource="judges",
        algorithm="least_busy",
        timeout_s=30,
        retry=Retrying(max_attempts=2, on=(RetryableError, TimeoutError), base=0.02, cap=0.2),
    )


@task("reduce")
def reduce_scores(row: dict, ctx: Any) -> dict:
    """D — aggregate the three scores. **User code**: the framework never reduces anything.

    It is an ordinary task like any other, so its output is persisted like any other artifact —
    which is exactly how you would build a metric without the framework knowing what a metric is.
    """
    scores = row.get("scores") or {}
    if not scores:
        raise FatalError("no scores to reduce")
    values = [item["score"] for item in scores.values()]
    mean = sum(values) / len(values)
    return {
        "qid": row["qid"],
        "question": row["question"],
        "answer": row["answer"],
        "scores": {name: item["score"] for name, item in sorted(scores.items())},
        "reasons": {name: item["reason"] for name, item in sorted(scores.items())},
        "mean_score": round(mean, 3),
        "passed": mean >= PASS_THRESHOLD,
        "judges": len(values),
    }


# ------------------------------------------------------------------------ pipelines
def build_grouped() -> Any:
    """A ─▶ B ─▶ C(3 judges in one task) ─▶ D."""
    return pipeline(
        "llm_eval_grouped",
        prepare | ask | score_all | reduce_scores,
        tags={"example": "llm_eval", "shape": "grouped"},
    )


def build_split() -> Any:
    """A ─▶ B ─▶ C1 ─▶ C2 ─▶ C3 ─▶ D — the same scores, three checkpoints instead of one."""
    judges = [judge_task(judge) for judge in JUDGES]
    return pipeline(
        "llm_eval_split",
        prepare | ask | judges[0] | judges[1] | judges[2] | reduce_scores,
        tags={"example": "llm_eval", "shape": "split"},
    )


def seeds(count: int = 3) -> Iterator[dict[str, Any]]:
    """A tiny arithmetic dataset — the eval itself is not the point, the plumbing is."""
    problems = [(2, 2), (7, 5), (12, 9), (6, 6), (13, 8)]
    for index in range(count):
        left, right = problems[index % len(problems)]
        yield {
            "qid": f"math-{index:03d}",
            "question": f"what is {left} + {right}",
            "expected": left + right,
        }


def describe(template: Any) -> str:
    return " -> ".join(template.task_names())
