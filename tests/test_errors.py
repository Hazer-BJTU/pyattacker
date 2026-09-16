"""The failure classification engine (`errors.py`): pure functions, no I/O, no async.

Coverage
* `error_class_of`: every `_STATUS_RULES` bracket, the built-in/stdlib exception fallbacks
  (`FatalError`, `TimeoutError`, `ConnectionError`, `ValueError`/`TypeError`/... -> "invalid"),
  an explicit `.error_class` attribute taking precedence, unknown status codes outside the
  table (2xx/3xx/other 4xx/5xx), and the final "unknown" fallback.
* `is_retryable_class`: exactly the documented retryable set.
* `retry_after_of`: a direct `.retry_after` attribute, a `response.headers` mapping (both
  `retry-after` and `Retry-After` casings), missing/unparsable/negative values.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from pyattacker.errors import (
    ERROR_CLASSES,
    FatalError,
    RetryableError,
    error_class_of,
    is_retryable_class,
    retry_after_of,
)


class _StatusError(Exception):
    def __init__(self, status: int | None = None, *, attr: str = "status") -> None:
        super().__init__(f"status={status}")
        if status is not None:
            setattr(self, attr, status)


class _ResponseError(Exception):
    def __init__(self, status_code: int | None = None, headers: dict | None = None) -> None:
        super().__init__("response error")
        self.response = SimpleNamespace(status_code=status_code, headers=headers)


# --------------------------------------------------------------------- error_class_of
def test_error_class_of_respects_an_explicit_attribute_first():
    exc = RetryableError("nope", error_class="rate_limit")
    assert error_class_of(exc) == "rate_limit"

    # even on an otherwise-fatal-looking exception: explicit wins
    plain = ValueError("bad")
    plain.error_class = "retryable"
    assert error_class_of(plain) == "retryable"


def test_error_class_of_fatal_error_is_fatal():
    assert error_class_of(FatalError("bad request")) == "fatal"


def test_error_class_of_timeout_error_including_asyncio_alias():
    import asyncio

    assert asyncio.TimeoutError is TimeoutError  # builtin alias since 3.11; same branch either way
    assert error_class_of(TimeoutError("slow")) == "timeout"


def test_error_class_of_connection_error():
    assert error_class_of(ConnectionError("refused")) == "connection"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (408, "timeout"),
        (504, "timeout"),
        (425, "rate_limit"),
        (429, "rate_limit"),
        (500, "upstream"),
        (502, "upstream"),
        (503, "upstream"),
        (505, "upstream"),
        (507, "upstream"),
        (529, "upstream"),
        (400, "fatal"),
        (401, "fatal"),
        (403, "fatal"),
        (404, "fatal"),
        (405, "fatal"),
        (409, "fatal"),
        (410, "fatal"),
        (413, "fatal"),
        (415, "fatal"),
        (422, "fatal"),
    ],
)
def test_error_class_of_status_rules_table(status, expected):
    assert error_class_of(_StatusError(status)) == expected


@pytest.mark.parametrize("attr", ["status", "status_code", "http_status", "code"])
def test_error_class_of_reads_status_from_any_of_the_known_attributes(attr):
    assert error_class_of(_StatusError(500, attr=attr)) == "upstream"


def test_error_class_of_reads_status_from_a_response_object():
    assert error_class_of(_ResponseError(503)) == "upstream"
    assert error_class_of(_ResponseError(404)) == "fatal"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (599, "upstream"),  # 5xx outside the explicit table still falls into the 500-600 branch
        (418, "fatal"),  # 4xx outside the explicit table still falls into the 400-500 branch
    ],
)
def test_error_class_of_status_outside_the_table_still_buckets_by_range(status, expected):
    assert error_class_of(_StatusError(status)) == expected


def test_error_class_of_status_outside_any_range_falls_through_to_unknown():
    # a 2xx/3xx "error" (unusual, but the classifier must not crash or misclassify it as fatal)
    assert error_class_of(_StatusError(204)) == "unknown"


@pytest.mark.parametrize("exc_type", [ValueError, TypeError, KeyError, AttributeError, ArithmeticError])
def test_error_class_of_maps_programmer_errors_to_invalid(exc_type):
    assert error_class_of(exc_type("oops")) == "invalid"


def test_error_class_of_unrecognized_exception_is_unknown():
    assert error_class_of(RuntimeError("mystery")) == "unknown"


def test_error_classes_tuple_matches_every_branch_this_module_can_return():
    seen = {
        error_class_of(RetryableError()),
        error_class_of(_StatusError(429)),
        error_class_of(_StatusError(408)),
        error_class_of(ConnectionError()),
        error_class_of(_StatusError(500)),
        error_class_of(ValueError()),
        error_class_of(FatalError()),
        error_class_of(RuntimeError()),
    }
    assert seen <= set(ERROR_CLASSES)


# --------------------------------------------------------------------- is_retryable_class
@pytest.mark.parametrize("klass", ["retryable", "rate_limit", "timeout", "connection", "upstream"])
def test_is_retryable_class_true_for_the_documented_retryable_set(klass):
    assert is_retryable_class(klass) is True


@pytest.mark.parametrize("klass", ["invalid", "fatal", "cancelled", "unknown", "not-a-real-class"])
def test_is_retryable_class_false_otherwise(klass):
    assert is_retryable_class(klass) is False


# --------------------------------------------------------------------- retry_after_of
def test_retry_after_of_reads_a_direct_attribute():
    exc = RetryableError("throttled", retry_after=1.5)
    assert retry_after_of(exc) == 1.5


def test_retry_after_of_reads_response_headers_case_insensitively():
    exc = _ResponseError(429, headers={"retry-after": "2.5"})
    assert retry_after_of(exc) == 2.5

    exc2 = _ResponseError(429, headers={"Retry-After": "3"})
    assert retry_after_of(exc2) == 3.0


def test_retry_after_of_is_none_when_nothing_is_present():
    assert retry_after_of(RuntimeError("no hint")) is None
    assert retry_after_of(_ResponseError(429, headers={})) is None
    assert retry_after_of(_ResponseError(429, headers=None)) is None


def test_retry_after_of_rejects_unparsable_or_negative_values():
    exc = RuntimeError("bad")
    exc.retry_after = "not-a-number"
    assert retry_after_of(exc) is None

    negative = RuntimeError("bad")
    negative.retry_after = -1.0
    assert retry_after_of(negative) is None


def test_retry_after_of_headers_lookup_tolerates_a_broken_get():
    class _AngryHeaders:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("headers backend exploded")

    exc = _ResponseError(429, headers=_AngryHeaders())
    assert retry_after_of(exc) is None
