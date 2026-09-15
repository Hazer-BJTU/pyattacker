"""One evaluation, two checkpoint granularities — measured, not asserted.

Run it with::

    uv run python -m examples.llm_eval.demo

The scenario is an ordinary LLM evaluation:

    A  prepare      normalise and validate the dataset row
    B  ask          the main model, called twice inside one task (a two-turn conversation)
    C  score        three judge configurations score the answer
    D  reduce       aggregate the three scores (user code — the framework never reduces)

C exists in two shapes that produce the same numbers:

* **grouped** — one task scores with all three judges concurrently;
* **split** — C1, C2, C3 are separate tasks, so each judge's score is its own artifact.

The demo takes one judge endpoint down for a whole round, then brings it back and resumes both
pipelines. Because the split shape has a checkpoint after every judge, resuming it re-sends only
the judge that failed; the grouped shape has to score all three again. The counters come from the
simulated backend, so the claim is a measurement.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

if __package__ in (None, ""):  # pragma: no cover - guard rail for `python examples/...`
    raise SystemExit("run this as a module: uv run python -m examples.llm_eval.demo")

from pyattacker import Runner, RunReport, SqliteStore

from .backend import FailureSwitch, RequestLog
from .pipelines import JUDGES, build_grouped, build_split, describe, make_pools, seeds

RUNS = Path("runs")
GROUPED_STORE = RUNS / "llm_eval_grouped.db"
SPLIT_STORE = RUNS / "llm_eval_split.db"
JUDGE_MODELS = [judge["model"] for judge in JUDGES]
DOWN = JUDGE_MODELS[-1]  # the terse judge is the one that goes down


def _fresh(path: Path) -> str:
    """Start from an empty store so the numbers below are about this run, not a previous one."""
    RUNS.mkdir(exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)
    return str(path)


def _round(template: Any, store: str, log: RequestLog, switch: FailureSwitch, *, label: str, resume: bool = False) -> RunReport:
    with Runner(
        store=store, pools=make_pools(log, switch=switch), concurrency=8, label=label
    ) as runner:
        return runner.run(template.map(seeds(1)), resume=resume)


def _events(store_path: str, kind: str) -> list[Any]:
    store = SqliteStore(store_path, read_only=True)
    try:
        return [event for event in store.events(limit=500) if event.kind == kind]
    finally:
        store.close()


def _final_rows(store_path: str) -> list[dict[str, Any]]:
    store = SqliteStore(store_path, read_only=True)
    try:
        return list(store.export_rows())
    finally:
        store.close()


def _table(rows: list[list[Any]], header: list[str]) -> str:
    widths = [len(str(cell)) for cell in header]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))
    lines = ["  ".join(str(h).ljust(widths[i]) for i, h in enumerate(header))]
    lines.append("  ".join("-" * width for width in widths))
    lines.extend("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)) for row in rows)
    return "\n".join(lines)


def main() -> int:
    grouped, split = build_grouped(), build_split()
    print("pyattacker example: LLM evaluation with judge scoring\n")
    print(f"  grouped  {describe(grouped)}")
    print(f"  split    {describe(split)}")
    print(
        "\n  A is plain data preparation; D is user code, because this framework records facts\n"
        "  and never reduces them. B calls the main model twice inside one task, acquiring and\n"
        "  releasing the pool per turn so a slow conversation never holds a concurrency slot."
    )

    grouped_store = _fresh(GROUPED_STORE)
    split_store = _fresh(SPLIT_STORE)
    switch = FailureSwitch().fail(DOWN)

    print(f"\n=== round 1: {DOWN} is down for the whole round ===")
    log_g1 = RequestLog()
    report_g1 = _round(grouped, grouped_store, log_g1, switch, label="grouped-round-1")
    print(f"  grouped  pipelines={report_g1.stats['pipelines']['by_state']}  {log_g1.summary()}")

    log_s1 = RequestLog()
    report_s1 = _round(split, split_store, log_s1, switch, label="split-round-1")
    print(f"  split    pipelines={report_s1.stats['pipelines']['by_state']}  {log_s1.summary()}")

    print("\n  The grouped task retried twice and re-sent *all three* judges each time.")
    print("  The split pipeline failed at C3 and stopped; C1 and C2 are already on disk.")

    print("\n=== round 2: the endpoint is back, resume both ===")
    switch.clear()
    log_g2 = RequestLog()
    report_g2 = _round(grouped, grouped_store, log_g2, switch, label="grouped-resume", resume=True)
    log_s2 = RequestLog()
    report_s2 = _round(split, split_store, log_s2, switch, label="split-resume", resume=True)

    for label, report, log in (
        ("grouped", report_g2, log_g2),
        ("split  ", report_s2, log_s2),
    ):
        print(f"  {label}  pipelines={report.stats['pipelines']['by_state']}  {log.summary()}")

    resumed_g = _events(grouped_store, "pipeline.resumed")
    resumed_s = _events(split_store, "pipeline.resumed")
    if resumed_s:
        print(
            f"\n  split resumed at seq={resumed_s[0].data['from_seq']} "
            f"(0=prepare 1=ask 2=C1 3=C2 4=C3 5=reduce) — C1 and C2 were not re-executed."
        )
    if resumed_g:
        print(f"  grouped resumed at seq={resumed_g[0].data['from_seq']} — it has no checkpoint inside C.")

    grouped_requests = log_g1.total + log_g2.total
    split_requests = log_s1.total + log_s2.total
    grouped_wasted = log_g1.wasted_successes(JUDGE_MODELS) + log_g2.wasted_successes(JUDGE_MODELS)
    split_wasted = log_s1.wasted_successes(JUDGE_MODELS) + log_s2.wasted_successes(JUDGE_MODELS)
    minimum = len(JUDGE_MODELS)

    print("\n=== what the two shapes cost for the same three scores ===")
    print(
        _table(
            [
                ["grouped", log_g1.total, log_g2.total, grouped_requests, minimum, grouped_wasted],
                ["split", log_s1.total, log_s2.total, split_requests, minimum, split_wasted],
            ],
            ["shape", "round 1", "round 2", "sent", "needed", "wasted judge calls"],
        )
    )
    print(
        "\n  'sent' counts every provider request (main model + judges) in both rounds.\n"
        "  'wasted' counts judge requests that recomputed a score which had already succeeded.\n"
        f"  With {minimum} judges the grouped shape re-sent {grouped_wasted} of them; the split shape re-sent\n"
        "  none. The gap grows with the number of judges: a coarse checkpoint replays the whole\n"
        "  group on every retry and every resume, a per-judge checkpoint replays exactly one."
    )

    # The example checks its own claim: if a change ever makes the split shape worse, this fails
    # loudly instead of quietly printing a nicer story.
    assert split_wasted == 0, f"split shape wasted {split_wasted} successful calls"
    assert split_requests < grouped_requests, (split_requests, grouped_requests)

    print("\n=== a clean run of the split pipeline (3 pipelines, no failures) ===")
    clean_store = _fresh(RUNS / "llm_eval_clean.db")
    clean_log = RequestLog()
    with Runner(
        store=clean_store, pools=make_pools(clean_log), concurrency=8, label="split-clean"
    ) as clean:
        report = clean.run(split.map(seeds(3)))
        print(f"  pipelines={report.stats['pipelines']['by_state']}  {clean_log.summary()}")
        print(
            f"  wall={report.duration_ms / 1000:.2f}s  attempts={report.stats['attempts_total']}"
            f"  (retries included)"
        )
        for row in clean.store.export_rows():
            final = [a for a in row["artifacts"] if a["is_final"]]
            payload = final[0]["payload"] if final else {}
            print(
                f"  {payload.get('qid')}: scores={payload.get('scores')} "
                f"mean={payload.get('mean_score')} passed={payload.get('passed')}"
            )
        print("\n  inspect it yourself:")
        print(f"    uv run pyattacker report {clean_store}")
        print(f"    uv run pyattacker serve {clean_store}   # then open the printed URL")
        print(f"    uv run pyattacker export {clean_store} runs/llm_eval.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
