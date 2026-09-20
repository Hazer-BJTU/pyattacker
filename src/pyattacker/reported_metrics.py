"""Application-reported values displayed by the live monitor.

These records carry values supplied by application code; pyattacker never reduces
artifacts or assigns semantic meaning to them.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

from .errors import ConfigError, StoreFeatureUnsupported

__all__ = ["ReportedMetric", "report_metric", "read_reported_metrics"]


@dataclass(frozen=True)
class ReportedMetric:
    run_id: str
    name: str
    value: str | int | float | bool
    label: str = ""
    display: str = "number"
    pipeline_id: str | None = None
    updated_at: float = field(default_factory=time.time)
    experiment_id: str | None = None


def report_metric(
    store: Any,
    run_id: str,
    name: str,
    value: str | int | float | bool,
    *,
    label: str = "",
    display: str = "number",
    pipeline_id: str | None = None,
) -> ReportedMetric:
    """Validate and synchronously upsert one latest-value report."""
    if not run_id or not isinstance(run_id, str):
        raise ConfigError("reported metric requires a run_id")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("reported metric name must be a non-empty string")
    if not isinstance(label, str):
        raise ConfigError("reported metric label must be a string")
    if pipeline_id is not None and (not isinstance(pipeline_id, str) or not pipeline_id):
        raise ConfigError("pipeline_id must be a non-empty string when provided")
    if display not in ("number", "percent", "text"):
        raise ConfigError("reported metric display must be 'number', 'percent', or 'text'")
    if type(value) not in (str, int, float, bool) or (type(value) is float and not math.isfinite(value)):
        raise ConfigError("reported metric value must be a string, boolean, or finite number")
    if display == "percent" and type(value) not in (int, float):
        raise ConfigError("percent display requires a numeric value")
    if display == "text" and type(value) is not str:
        raise ConfigError("text display requires a string value")
    writer = getattr(store, "upsert_reported_metric", None)
    if not callable(writer):
        raise StoreFeatureUnsupported("store does not support reported metrics")
    row = ReportedMetric(run_id, name, value, label, display, pipeline_id)
    writer(row)
    return row


def read_reported_metrics(
    store: Any, *, run_id: str | None = None, pipeline_id: str | None = None
) -> list[ReportedMetric]:
    """Return reports from capable stores, and an empty view from older stores."""
    reader = getattr(store, "reported_metrics", None)
    if not callable(reader):
        return []
    return reader(run_id=run_id, pipeline_id=pipeline_id)
