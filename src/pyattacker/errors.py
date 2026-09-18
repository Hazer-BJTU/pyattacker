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
    "PipelineIdentityConflict",
    "CorruptCheckpoint",
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
    "WorkerCrashed",
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


class PipelineIdentityConflict(ConfigError):
    """An existing pipeline key refers to a different task definition or seed."""


class CorruptCheckpoint(PyAttackerError):
    """A stored checkpoint contradicts the pipeline definition (for example ``n_tasks_done`` exceeds the
    number of tasks in the chain).

    The Runner never raises this out of a worker: it records it on the pipeline row so the corrupt value
    stays visible for inspection instead of being repaired away, promoted to success, or surfacing as a
    framework crash. It is classified ``fatal`` — retrying the same corrupt state cannot help.
    """

    error_class = "fatal"


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


class WorkerCrashed(PyAttackerError):
    """A worker task ended outside its own handlers, so the run could not finish normally.

    The escaping exception — any ``BaseException`` that is not :class:`asyncio.CancelledError`,
    raised by the framework's own code or by something it calls (a store hook, for example) — is
    preserved as ``__cause__``. A ``BaseException`` raised *by a task* never reaches this path: the
    task's own handling contains it, exactly like an ordinary exception.

    Worker *lifetime* is supervised separately from pipeline *accounting*: a worker that died can
    no longer advance either, so the run is stopped hard (no new admissions, no waiting on
    in-flight work), the pipeline the dead worker was holding is given a terminal row, and that
    fact is recorded as a ``runner.worker_crashed`` event before this error is raised. Ordinary
    internal ``Exception`` handling is unaffected: this is for a worker that never reached the
    worker's own error paths at all.
    """

    def __init__(self, message: str = "", *, pipeline_id: str | None = None) -> None:
        super().__init__(message)
        self.pipeline_id = pipeline_id


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

# Each bracket is a framework retry-policy grouping, not a restatement of HTTP semantics — the
# question each answers is "is trying again likely to help", and that is what decides whether it
# lands in _RETRYABLE_CLASSES:
# - 408/504: the request itself may have just been slow -> "timeout", worth another try.
# - 425 ("Too Early", not itself a rate-limit status) / 429 (actual rate limiting): grouped
#   together as "rate_limit" because both mean "retry, just not immediately" from the caller's
#   side, not because 425 and 429 share HTTP semantics. retry_after_of() honors a server-supplied
#   Retry-After delay independently of this classification, so it applies just as much to a
#   retryable 5xx as it does to these.
# - remaining 5xx (504 is already claimed above by "timeout") -> "upstream": the server, not the
#   request, is the problem, so retrying is reasonable regardless of which 5xx it is; splitting
#   them further has not earned its keep.
# - 4xx (validation/auth/not-found/conflict/payload errors) -> "fatal": the request is wrong as
#   sent, and retrying it unchanged would just fail again.
# Adding a new provider-specific status code: pick the bucket by that logic, not by proximity to
# an existing number.
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
