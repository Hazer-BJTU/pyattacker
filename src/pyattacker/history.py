"""Optional application payload history; snapshots are persisted at task commit boundaries."""

from __future__ import annotations

import copy
import json
from typing import Any

from .errors import ArtifactCodecError

__all__ = ["HistoryArtifact"]


class HistoryArtifact:
    """A copy-on-write payload with explicit, detached application-state snapshots.

    Subclasses inherit this constructor and should register with ``CodecRegistry.register_type``.
    State and history access return detached copies, so nested edits cannot mutate snapshots.
    This is a decoded payload, not a subclass of the persisted ``Artifact`` record.
    """

    def __init__(
        self,
        state: Any,
        *,
        history: list[dict[str, Any]] | None = None,
        selected: str | None = None,
        next_snapshot: int = 0,
    ) -> None:
        self._state = copy.deepcopy(state)
        self._history = copy.deepcopy(history or [])
        self._selected = selected
        self._next_snapshot = next_snapshot

    @property
    def state(self) -> Any:
        return copy.deepcopy(self._state)

    @property
    def history(self) -> tuple[dict[str, Any], ...]:
        return tuple(copy.deepcopy(self._history))

    @property
    def selected(self) -> str | None:
        return self._selected

    def _replace(self, **changes: Any) -> HistoryArtifact:
        result = copy.deepcopy(self)
        for key, value in changes.items():
            setattr(result, key, copy.deepcopy(value))
        return result

    def with_state(self, state: Any) -> HistoryArtifact:
        return self._replace(_state=state)

    def checkpoint(self, label: str, *, metadata: dict[str, Any] | None = None) -> HistoryArtifact:
        if not isinstance(label, str) or not label:
            raise ValueError("snapshot label must be a nonempty string")
        if label.startswith("snapshot:"):
            raise ValueError("snapshot: is reserved for stable snapshot IDs")
        if any(row["label"] == label for row in self._history):
            raise ValueError(f"duplicate snapshot label {label!r}")
        snapshot = {
            "id": f"snapshot:{self._next_snapshot}",
            "label": label,
            "state": self.state,
            "metadata": copy.deepcopy(metadata or {}),
        }
        return self._replace(_history=[*self._history, snapshot], _next_snapshot=self._next_snapshot + 1)

    def snapshot(self, selector: str) -> dict[str, Any]:
        # Labels must not shadow another snapshot's stable ID.
        matches = [row for row in self._history if row["id"] == selector or row["label"] == selector]
        if len(matches) != 1:
            raise KeyError(f"unknown or ambiguous snapshot {selector!r}")
        return copy.deepcopy(matches[0])

    def restore(self, selector: str) -> HistoryArtifact:
        row = self.snapshot(selector)
        return self._replace(_state=row["state"], _selected=row["id"])

    def prune(self, *selectors: str) -> HistoryArtifact:
        ids = {self.snapshot(selector)["id"] for selector in selectors}
        if self._selected in ids:
            raise ValueError("cannot prune the selected snapshot")
        return self._replace(_history=[row for row in self._history if row["id"] not in ids])


class HistoryCodec:
    name = "history-v1"

    def can_encode(self, obj: Any) -> bool:
        return isinstance(obj, HistoryArtifact)

    def dumps(self, obj: HistoryArtifact) -> bytes:
        try:
            return json.dumps(
                {
                    "version": 1,
                    "state": obj.state,
                    "history": obj.history,
                    "selected": obj.selected,
                    "next_snapshot": obj._next_snapshot,
                },
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        except (TypeError, ValueError) as exc:
            raise ArtifactCodecError(f"history state must be JSON serializable: {exc}") from exc

    def loads(self, data: bytes) -> HistoryArtifact:
        try:
            raw = json.loads(data)
            if not isinstance(raw, dict) or raw.get("version") != 1:
                raise ValueError("unsupported history envelope version")
            rows = raw["history"]
            if not isinstance(rows, list) or any(
                not isinstance(row, dict) or set(row) != {"id", "label", "state", "metadata"} for row in rows
            ):
                raise ValueError("invalid snapshot history")
            ids = [row["id"] for row in rows]
            labels = [row["label"] for row in rows]
            if any(not isinstance(x, str) or not x for x in [*ids, *labels]):
                raise ValueError("invalid snapshot selectors")
            if any(label.startswith("snapshot:") for label in labels):
                raise ValueError("snapshot label shadows stable ID")
            if len(set(ids)) != len(ids) or len(set(labels)) != len(labels):
                raise ValueError("duplicate snapshot selector")
            if raw["selected"] is not None and raw["selected"] not in ids:
                raise ValueError("selected snapshot is missing")
            counter = raw["next_snapshot"]
            if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
                raise ValueError("invalid snapshot counter")
            if any(
                identifier != f"snapshot:{int(identifier.removeprefix('snapshot:'))}"
                or int(identifier.removeprefix("snapshot:")) >= counter
                or int(identifier.removeprefix("snapshot:")) < 0
                for identifier in ids
            ):
                raise ValueError("invalid snapshot identity")
            return HistoryArtifact(
                raw["state"], history=rows, selected=raw["selected"], next_snapshot=counter
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactCodecError(f"invalid history envelope: {exc}") from exc
