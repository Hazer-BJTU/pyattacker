"""Exception family —— pyattacker's failure model is built entirely on Python exceptions.

Design notes:
* The framework itself defines only the few exceptions needed for "classification", never a state machine.
* Users may raise any exception directly; raise :class:`RetryableError` / :class:`FatalError` when precise control is needed.
* :func:`error_class_of` is a pure function that maps any exception to a stable string classification;
  that classification is persisted and drives retry decisions and post-hoc statistics.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "PyAttackerError",
    "ConfigError",
    "PipelineBuildError",
    "ArtifactCodecError",
    "PluginError",
    "ResourceError",
    "ResourceUnavailable",
    "AcquireTimeout",
    "PoolNotFound",
    "LeaseLeakError",
    "RetryableError",
    "FatalError",
    "BudgetExceeded",
    "RunInterrupted",
    "StoreUnavailable",
    "ERROR_CLASSES",
    "error_class_of",
    "retry_after_of",
]


class PyAttackerError(Exception):
    """Base class for all framework exceptions."""


class ConfigError(PyAttackerError):
    """Configuration / declarative-file error."""


class PipelineBuildError(PyAttackerError):
    """The pipeline is invalid at construction time (for example, adjacent tasks' artifact types do not chain)."""


class PluginError(PyAttackerError):
    """A plugin entry point is unknown, or could not be loaded.

    Loading failures are normally *recorded* rather than raised (see ``pyattacker.plugins``); this
    is raised for programmer errors such as asking for an unknown plugin group.
    """


class ArtifactCodecError(PyAttackerError):
    """An artifact cannot be encoded/decoded."""


class ResourceError(PyAttackerError):
    """Base class for resource-related errors."""


class ResourceUnavailable(ResourceError):
    """No resource is available in the pool, and the acquire algorithm decided not to wait any longer."""


class AcquireTimeout(ResourceUnavailable):
    """A resource could not be acquired within the given timeout."""


class PoolNotFound(ResourceError):
    """An unregistered resource pool was referenced."""


class LeaseLeakError(ResourceError):
    """A lease was still held when the task finished (only raised when ``strict_leases=True``).

    Normally the framework force-reclaims it and records the fact as a ``lease.leaked`` event,
    without failing the whole pipeline.
    """


class RetryableError(PyAttackerError):
    """Marks an error as "retryable"; may carry a classification and a suggested backoff."""

    def __init__(
        self,
        message: str = "",
        *,
        error_class: str = "retryable",
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.retry_after = retry_after


class FatalError(PyAttackerError):
    """Marks an error as "not retryable" (for example, the request itself is invalid)."""

    def __init__(self, message: str = "", *, error_class: str = "fatal") -> None:
        super().__init__(message)
        self.error_class = error_class


class BudgetExceeded(PyAttackerError):
    """A run budget was exceeded (wall clock / failure count / attempt count)."""


class RunInterrupted(PyAttackerError):
    """The run was interrupted from outside (SIGINT / Runner.stop)."""


class StoreUnavailable(PyAttackerError):
    """The store itself failed while the framework was trying to record a terminal state.

    Raised when even the internal-error recovery path (persisting a pipeline as ``failed``
    after a framework-level surprise) cannot reach the store: at that point the run's own
    durability guarantees can no longer be trusted, so the run stops instead of continuing on
    unrecorded state.
    """


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

ERROR_CLASSES = (
    "retryable",
    "rate_limit",
    "timeout",
    "connection",
    "upstream",
    "invalid",
    "fatal",
    "cancelled",
    "unknown",
)

_RETRYABLE_CLASSES = frozenset({"retryable", "rate_limit", "timeout", "connection", "upstream"})

_STATUS_RULES: tuple[tuple[tuple[int, ...], str], ...] = (
    ((408, 504), "timeout"),
    ((425, 429), "rate_limit"),
    ((500, 502, 503, 505, 507, 529), "upstream"),
    ((400, 401, 403, 404, 405, 409, 410, 413, 415, 422), "fatal"),
)


def _status_of(exc: BaseException) -> int | None:
    for attr in ("status", "status_code", "http_status", "code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None


def error_class_of(exc: BaseException) -> str:
    """Map any exception to a stable classification string. Pure function, unit-testable."""
    explicit = getattr(exc, "error_class", None)
    if isinstance(explicit, str) and explicit:
        return explicit
    if isinstance(exc, FatalError):
        return "fatal"
    if isinstance(exc, TimeoutError):  # includes asyncio.TimeoutError (a builtin alias since 3.11)
        return "timeout"
    if isinstance(exc, ConnectionError):
        return "connection"
    status = _status_of(exc)
    if status is not None:
        for codes, klass in _STATUS_RULES:
            if status in codes:
                return klass
        if 500 <= status < 600:
            return "upstream"
        if 400 <= status < 500:
            return "fatal"
    if isinstance(exc, (ValueError, TypeError, KeyError, AttributeError, ArithmeticError)):
        return "invalid"
    return "unknown"


def is_retryable_class(error_class: str) -> bool:
    """The framework's built-in opinion on which classifications are worth retrying."""
    return error_class in _RETRYABLE_CLASSES


def retry_after_of(exc: BaseException) -> float | None:
    """Extract the server-suggested backoff time (in seconds) from an exception."""
    value: Any = getattr(exc, "retry_after", None)
    if value is None:
        headers = getattr(getattr(exc, "response", None), "headers", None)
        if headers is not None:
            try:
                value = headers.get("retry-after") or headers.get("Retry-After")
            except Exception:  # pragma: no cover - defensive
                value = None
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None
