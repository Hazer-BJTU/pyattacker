# Example: an LLM evaluation, in two pipeline shapes

A worked example of the thing these frameworks get rewritten for: score model answers with several
judges, keep a usable record of every request, and survive failures without paying for them twice.

```bash
uv run python -m examples.llm_eval.demo     # the guided run (~1s, fully deterministic)
```

## The scenario

```
        seed ─▶ A prepare ─▶ B ask ─▶ C score (3 judges) ─▶ D reduce
```

| Stage | What it does | Why it is interesting |
|---|---|---|
| **A prepare** | normalises whitespace, checks the row is usable | plain task: validates before spending a token, raises `FatalError` on bad input |
| **B ask** | the main model, **called twice inside one task** (a two-turn conversation) | each turn acquires and releases the pool, so a slow conversation never occupies a concurrency slot between turns |
| **C score** | three judge configurations grade the answer | exists in two shapes, see below |
| **D reduce** | averages the three scores, decides pass/fail | **user code**: the framework records facts and never reduces them |

`backend.py` is a separate module holding the simulated provider. It reproduces the properties
that make evaluation hard — latency, occasional failures, malformed JSON, per-endpoint state — and
nothing else. Replies are derived from the request, so an identical run produces identical output,
which is what makes the numbers below a measurement instead of an anecdote.

## The two shapes

**Grouped** — one task, three concurrent judges:

```
A ─▶ B ─▶ C score_all ─▶ D          checkpoint: ○────────○────────○───────○
```

**Split** — one task per judge:

```
A ─▶ B ─▶ C1 strict ─▶ C2 balanced ─▶ C3 terse ─▶ D
                                            checkpoint: ○──○──○──○──○──○──○
```

The pipeline is a linear chain, so each judge carries the scores it inherited forward. That
accumulator idiom is the standard way to express "and also" without turning the pipeline into a
DAG.

## What the demo measures

Round 1 takes `mock-judge-terse` down for the whole round. Round 2 brings it back and resumes both
pipelines:

```
shape    round 1  round 2  sent  needed  wasted judge calls
-------  -------  -------  ----  ------  ------------------
grouped  8        3        11    3       2
split    6        1        7     3       0
```

* **sent** — every provider request in both rounds.
* **wasted judge calls** — judge requests that recomputed a score which had already succeeded.

Two things are visible in the log:

```
split    resumed at seq=4 (0=prepare 1=ask 2=C1 3=C2 4=C3 5=reduce) — C1 and C2 were not re-executed
grouped  resumed at seq=2 — it has no checkpoint inside C
```

**The advantage of the split shape is checkpoint granularity.** A task's artifact is persisted the
moment it is produced, so a failure at C3 resumes *at C3*: the strict and balanced scores are read
back from the store and never re-requested. The grouped task has no interior checkpoint, so any
failure — including a resume from a crash — replays the whole group. With *k* judges the grouped
shape re-sends *k* requests per retry and per resume; the split shape re-sends exactly one.

The cost is honest too: three tasks instead of one means three artifacts, three task rows, and
three `acquire`/`release` pairs instead of one — slightly more bookkeeping, and each judge becomes
individually retryable and individually visible in the record.

**Which to choose:** split when judge calls are expensive or slow, when a single judge is flaky, or
when the evaluation is long enough that a restart is painful. Group when the branches are cheap, or
when you genuinely want all-or-nothing semantics for the step.

## Patterns worth stealing

* **Loop inside a task, release between requests.** `ask` calls the model twice but holds a lease
  only for the duration of each call:
  ```python
  for turn in range(1, TURNS + 1):
      async with ctx.acquire() as lease:      # released before the next turn
          reply = await lease.client.complete(messages)
          lease.report(ok=True, usage={"tokens": len(reply.split())})
  ```
* **Validate model output, then classify the failure.** A judge that returns `{score: 4` is not a
  network problem; `_parse_score` raises `RetryableError(error_class="invalid_response")` so the
  policy retries it, while the original `json.JSONDecodeError` alone would have been classified as
  fatal.
* **Report resource health.** A malformed reply calls `lease.report(ok=False, error=...)`, which is
  what lets the pool degrade an endpoint that keeps misbehaving.
* **Never let a fan-out orphan its requests.** `score_all` uses
  `asyncio.gather(..., return_exceptions=True)` and raises only after every branch finished.
  Plain fail-fast `gather` returns on the first exception while the other requests are still in
  flight — they keep holding leases, and their retry duplicates work nobody will ever read.
* **Cancellation or a crash mid-round leaves a resumable state.** Round 1 above ends with a failed
  pipeline, and round 2 finishes it; nothing needs to be re-created by hand.

## Making it more realistic

```python
# random (but reproducible) failures instead of a scripted switch
pools = make_pools(log, failure_rate=0.05)
# malformed judge output
Resource.create(..., options={"malformed_rate": 0.03})
```

## Looking at the result

```bash
uv run pyattacker report runs/llm_eval_clean.db            # states, latency, errors
uv run pyattacker serve  runs/llm_eval_clean.db            # live HTTP dashboard
uv run pyattacker export runs/llm_eval_clean.db out.jsonl  # pipelines + artifacts
uv run pyattacker export runs/llm_eval_clean.db tasks.csv --rows tasks --format csv
```
