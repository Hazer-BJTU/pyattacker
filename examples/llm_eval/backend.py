"""A stand-in for a chat-completions provider.

Why this is a separate module: pyattacker never touches the network, so a realistic example needs
something that *looks* like a provider without being one. This backend reproduces the properties
that make evaluation hard, and nothing else:

* **latency** — so resource pools and concurrency actually matter;
* **occasional failure** — so retry policy, circuit-breaking and checkpoints matter;
* **random-looking output that is reproducible** — the reply is derived from the request itself, so
  the same run always produces the same text (an evaluation you cannot reproduce is not evidence);
* **scriptable failure** — ``fail_first=N`` makes the first N calls fail, which lets a demo or a
  test be exact instead of probabilistic.

One instance is created per :class:`~pyattacker.Resource` through the pool's ``factory``, exactly
like a real client would be, so per-endpoint counters and state are per resource.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pyattacker import Resource, RetryableError

__all__ = ["RequestLog", "FailureSwitch", "MockBackend", "backend_factory", "JUDGE_RUBRIC"]

JUDGE_RUBRIC = (
    "You are grading a student's answer. Reply with a single JSON object "
    '{"score": <1-5>, "reason": "<one short sentence>"} and nothing else.'
)

_OPENERS = (
    "Working through it step by step:",
    "Let me take that apart:",
    "Short version first, then the reasoning:",
    "Here is how I read the question:",
)
_CLOSERS = (
    "so the final answer is as stated",
    "which checks out against the question",
    "and that is the result I would submit",
    "so I am confident in that value",
)
_REASONS = (
    "the reasoning is correct and complete",
    "the answer is right but the justification is thin",
    "the conclusion is right while one step is hand-waved",
    "partially correct: the method is sound, the arithmetic is not",
    "the final value does not follow from the steps given",
)


@dataclass
class RequestLog:
    """Counts every call this fake provider served, per model.

    The point of the example is a comparison of *how many requests each pipeline shape costs*, so
    the counter is the measuring instrument — it lives outside the backend, shared by every
    endpoint, and the demo prints it.
    """

    calls: Counter[str] = field(default_factory=Counter)
    failures: Counter[str] = field(default_factory=Counter)
    slept_s: float = 0.0

    def note_call(self, model: str, seconds: float) -> None:
        self.calls[model] += 1
        self.slept_s += seconds

    def note_failure(self, model: str) -> None:
        self.failures[model] += 1

    @property
    def total(self) -> int:
        return sum(self.calls.values())

    @property
    def total_failures(self) -> int:
        return sum(self.failures.values())

    def successes(self, model: str) -> int:
        return self.calls.get(model, 0) - self.failures.get(model, 0)

    def wasted_successes(self, models: Sequence[str] | None = None) -> int:
        """Successful calls that recomputed a score which had already been computed.

        For one pipeline that asks each model exactly once, every successful call beyond the first
        is work thrown away — which is precisely what a coarse checkpoint costs.
        """
        names = list(models) if models else sorted(self.calls)
        return sum(max(0, self.successes(name) - 1) for name in names)

    def reset(self) -> None:
        self.calls.clear()
        self.failures.clear()
        self.slept_s = 0.0

    def per_model(self, models: Sequence[str] | None = None) -> dict[str, dict[str, int]]:
        names = list(models) if models else sorted(self.calls)
        return {
            name: {"calls": self.calls.get(name, 0), "failures": self.failures.get(name, 0)}
            for name in names
        }

    def summary(self) -> str:
        parts = [
            f"{name}={self.calls.get(name, 0)}"
            + (f"({self.failures[name]} failed)" if self.failures.get(name) else "")
            for name in sorted(self.calls)
        ]
        return f"{self.total} requests: " + " ".join(parts)


@dataclass
class FailureSwitch:
    """Shared, mutable failure control.

    ``failure_rate`` produces reproducible-but-random failures; this produces *deliberate* ones.
    The demo uses it to take one judge endpoint down for a whole round and bring it back for the
    next, which is what makes the grouped-versus-split comparison exact instead of anecdotal.
    """

    failing: set[str] = field(default_factory=set)

    def fail(self, *models: str) -> "FailureSwitch":
        self.failing.update(models)
        return self

    def clear(self) -> "FailureSwitch":
        self.failing.clear()
        return self

    def is_failing(self, model: str) -> bool:
        return model in self.failing


class MockBackend:
    """One simulated provider endpoint.

    ``complete()`` is the only method a task should need; it mirrors the shape of a real
    chat-completions call closely enough that swapping in ``httpx`` + a provider SDK is a small,
    obvious edit.
    """

    def __init__(
        self,
        model: str,
        *,
        log: RequestLog | None = None,
        options: Mapping[str, Any] | None = None,
        latency_ms: float = 6.0,
        failure_rate: float = 0.0,
        fail_first: int = 0,
        seed: int = 0,
        switch: FailureSwitch | None = None,
    ) -> None:
        self.model = model
        self.options = dict(options or {})
        self.log = log or RequestLog()
        self.switch = switch
        self.latency_ms = float(self.options.get("latency_ms", latency_ms))
        self.failure_rate = float(self.options.get("failure_rate", failure_rate))
        self.fail_first = int(self.options.get("fail_first", fail_first))
        self.seed = seed
        self.calls = 0
        self.malformed_rate = float(self.options.get("malformed_rate", 0.0))

    # ------------------------------------------------------------------ the "API"
    async def complete(
        self,
        messages: Sequence[Mapping[str, str]],
        *,
        temperature: float = 0.7,
        response_format: str | None = None,
        max_tokens: int = 256,
    ) -> str:
        """Simulate one request: wait, maybe fail, then answer.

        The RNG is seeded from the request itself, so failures and wording are reproducible across
        runs without ever being hard-coded.
        """
        self.calls += 1
        prompt = _last_user_message(messages)
        rng = random.Random(f"{self.seed}|{self.model}|{self.calls}|{prompt[:80]}|{temperature}")

        latency_s = (self.latency_ms / 1000.0) * (0.5 + rng.random())
        await asyncio.sleep(latency_s)
        self.log.note_call(self.model, latency_s)

        if self.switch is not None and self.switch.is_failing(self.model):
            self.log.note_failure(self.model)
            raise RetryableError(f"{self.model}: endpoint is down", error_class="upstream")
        if self.calls <= self.fail_first:
            self.log.note_failure(self.model)
            raise RetryableError(
                f"{self.model}: scripted failure on call {self.calls}", error_class="upstream"
            )
        if self.failure_rate and rng.random() < self.failure_rate:
            self.log.note_failure(self.model)
            kind = "rate_limit" if rng.random() < 0.4 else "upstream"
            retry_after = 0.05 if kind == "rate_limit" else None
            raise RetryableError(
                f"{self.model}: simulated {kind}", error_class=kind, retry_after=retry_after
            )

        if response_format == "json":
            if self.malformed_rate and rng.random() < self.malformed_rate:
                return "{score: 4"  # truncated on purpose: the task must validate, not trust
            return json.dumps(
                {"score": rng.randint(2, 5), "reason": rng.choice(_REASONS)}, ensure_ascii=False
            )
        return (
            f"{rng.choice(_OPENERS)} {_reason_about(prompt, rng)} "
            f"{rng.choice(_CLOSERS)}."
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<MockBackend {self.model} calls={self.calls}>"


def _last_user_message(messages: Iterable[Mapping[str, str]]) -> str:
    last = ""
    for message in messages:
        if message.get("role") == "user":
            last = str(message.get("content", ""))
    return last


def _reason_about(prompt: str, rng: random.Random) -> str:
    numbers = [token for token in prompt.replace("?", " ").split() if token.strip("+-*/= ").isdigit()]
    if numbers:
        return f"treating {numbers[0]} as the value to work with, the arithmetic follows directly"
    return "taking the question at face value, the conclusion follows from the stated facts"


def backend_factory(
    log: RequestLog, switch: FailureSwitch | None = None, **defaults: Any
):
    """Build the ``factory=`` callable a pool needs: one backend per resource.

    The factory runs once per resource, so every endpoint gets its own client object — which is
    what makes ``lease.client`` meaningful and what a real HTTP session would also want.
    """

    def _make(resource: Resource) -> MockBackend:
        options = dict(resource.options)
        model = str(options.get("model", resource.id))
        return MockBackend(
            model,
            log=log,
            options=options,
            latency_ms=float(options.get("latency_ms", defaults.get("latency_ms", 6.0))),
            failure_rate=float(options.get("failure_rate", defaults.get("failure_rate", 0.0))),
            fail_first=int(options.get("fail_first", defaults.get("fail_first", 0))),
            seed=int(defaults.get("seed", 0)),
            switch=switch,
        )

    return _make
