"""The retry policy, asked directly: `Retrying.decide`.

The decision used to be inline in the Runner's attempt loop. It moved onto `Retrying` because the
benchmark harness asks the same question outside a run, and these tests pin the answers — the shape
of the record (`docs/design.md` §4.4), which exceptions are retried, and how the budget is spent.
"""

from __future__ import annotations

import random

import pytest

from pyattacker.errors import FatalError, RetryableError
from pyattacker.task import Retrying

DOCUMENTED_KEYS = {"retry", "reason", "delay_s", "error_class", "max_attempts", "attempt", "retry_after"}


def _decide(retry: Retrying, exc: BaseException, *, attempts_used: int = 1, elapsed: float = 0.0, seed: int = 7):
    return retry.decide(exc, attempts_used=attempts_used, rng=random.Random(seed), elapsed=elapsed)


def test_retryable_error_retries_with_the_computed_backoff():
    policy = Retrying(max_attempts=3, base=0.1, factor=2.0, jitter="none")

    decision = _decide(policy, RetryableError("upstream hiccup"))

    assert decision["retry"] is True
    assert decision["reason"] == "retryable"
    assert decision["delay_s"] == 0.1  # base * factor ** (attempt - 1)


def test_the_backoff_grows_with_the_attempt_number():
    policy = Retrying(max_attempts=5, base=0.1, factor=3.0, jitter="none")

    assert _decide(policy, RetryableError("x"), attempts_used=1)["delay_s"] == 0.1
    assert _decide(policy, RetryableError("x"), attempts_used=2)["delay_s"] == 0.3
    assert _decide(policy, RetryableError("x"), attempts_used=3)["delay_s"] == 0.9


def test_a_server_supplied_retry_after_wins_over_the_backoff():
    policy = Retrying(max_attempts=3, base=10.0, jitter="none")

    decision = _decide(policy, RetryableError("slow down", error_class="rate_limit", retry_after=2.5))

    assert decision["delay_s"] == 2.5
    assert decision["retry_after"] == 2.5
    assert decision["error_class"] == "rate_limit"
    assert decision["retry"] is True


def test_attempts_exhausted_stops_retrying_without_a_delay():
    policy = Retrying(max_attempts=2, base=0.1, jitter="none")

    decision = _decide(policy, RetryableError("x"), attempts_used=2)

    assert decision["retry"] is False
    assert decision["reason"] == "attempts_exhausted"
    assert decision["delay_s"] == 0.0


def test_a_fatal_error_is_never_retried_even_with_budget_left():
    policy = Retrying(max_attempts=9, base=0.1, jitter="none")

    decision = _decide(policy, FatalError("bad request"), attempts_used=1)

    assert decision["retry"] is False
    assert decision["reason"] == "policy_declined"
    assert decision["error_class"] == "fatal"


def test_max_total_s_refuses_a_retry_that_would_overrun_the_budget():
    policy = Retrying(max_attempts=9, base=5.0, factor=1.0, jitter="none", max_total_s=6.0)

    decision = _decide(policy, RetryableError("x"), attempts_used=1, elapsed=2.0)

    assert decision["retry"] is False
    assert decision["reason"] == "total_budget"
    assert decision["delay_s"] == 0.0  # the delay is withdrawn, not merely refused


def test_a_budget_that_still_fits_is_honoured():
    policy = Retrying(max_attempts=9, base=1.0, factor=1.0, jitter="none", max_total_s=10.0)

    decision = _decide(policy, RetryableError("x"), attempts_used=1, elapsed=2.0)

    assert decision["retry"] is True
    assert decision["delay_s"] == 1.0


def test_the_decision_record_has_exactly_the_documented_keys():
    """The attempt record's `decision` object is a documented schema; extra keys are a contract change."""
    decision = _decide(Retrying(max_attempts=2), RetryableError("x"))

    assert set(decision) == DOCUMENTED_KEYS


@pytest.mark.parametrize(
    ("exc", "expected_class", "expected_retry"),
    [
        (TimeoutError("too slow"), "timeout", True),
        (ConnectionError("reset"), "connection", True),
        (RetryableError("upstream", error_class="upstream"), "upstream", True),
        (ValueError("bad shape"), "invalid", False),
        (RuntimeError("mystery"), "unknown", False),
    ],
)
def test_classification_decides_whether_a_retry_happens(exc, expected_class, expected_retry):
    """Retry is driven by the error's classification, not by its type name — see errors.error_class_of."""
    decision = _decide(Retrying(max_attempts=3), exc)

    assert decision["error_class"] == expected_class
    assert decision["retry"] is expected_retry
    assert decision["reason"] == ("retryable" if expected_retry else "policy_declined")


def test_retry_unknown_is_opt_in():
    """`unknown` classifications are retried only when the policy asks for it."""
    strict = _decide(Retrying(max_attempts=3, retry_unknown=False), RuntimeError("mystery"))
    permissive = _decide(Retrying(max_attempts=3, retry_unknown=True), RuntimeError("mystery"))

    assert strict["reason"] == "policy_declined"
    assert permissive["reason"] == "retryable"


def test_an_explicit_on_tuple_retries_a_class_that_would_otherwise_be_fatal():
    policy = Retrying(max_attempts=3, on=(ValueError,))

    decision = _decide(policy, ValueError("shape"))

    assert decision["retry"] is True


def test_the_caller_supplied_rng_is_the_only_source_of_randomness():
    """Same seed, same decision — which is what lets a benchmark compare algorithms on one environment."""
    policy = Retrying(max_attempts=3, base=1.0, jitter="full")

    first = _decide(policy, RetryableError("x"), seed=99)
    second = _decide(policy, RetryableError("x"), seed=99)
    other = _decide(policy, RetryableError("x"), seed=100)

    assert first == second
    assert first["delay_s"] != other["delay_s"]
