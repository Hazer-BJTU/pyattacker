"""Tasks published under the ``pyattacker.tasks`` entry-point group."""

from __future__ import annotations

from typing import Any

from pyattacker import TaskSpec, build_task_spec, task


@task("word_count")
def word_count(value: Any, ctx: Any) -> dict[str, Any]:
    """Count words in whatever text the previous step produced."""
    import json

    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    words = text.split()
    return {"words": len(words), "chars": len(text), "preview": text[:40]}


def uppercase_answer(*, field: str = "answer", name: str | None = None) -> TaskSpec:
    """A factory plugin: config passes ``kwargs`` and gets a configured task back.

    Declared in a config as::

        - use: uppercase_answer
          kwargs: { field: text }
    """

    def _impl(value: Any, ctx: Any) -> Any:
        if not isinstance(value, dict) or field not in value:
            raise KeyError(f"expected a mapping with {field!r}, got {type(value).__name__}")
        return {**value, field: str(value[field]).upper()}

    _impl.__name__ = name or "uppercase_answer"
    return build_task_spec(_impl, name=name or "uppercase_answer")
