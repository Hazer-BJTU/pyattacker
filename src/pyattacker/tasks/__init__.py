"""Built-in utility tasks.

Only **network-independent** things live here: mock tasks, dataset reading, subprocesses, file writing.
Real model requests (openai / anthropic protocols) are up to the user —— the framework does not handle networking.
"""

from __future__ import annotations

import asyncio
import json
import random
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from ..errors import FatalError, RetryableError
from ..resource import Resource
from ..task import Retrying, TaskSpec, build_task_spec, task

__all__ = [
    "echo",
    "flaky",
    "delay",
    "boom",
    "leaky",
    "simulate_llm",
    "shell_run",
    "write_jsonl",
    "jsonl_source",
    "seed_factory",
    "BUILTIN_TASKS",
]


# --------------------------------------------------------------------- mock
@task("mock.echo")
def echo(value: Any, ctx: Any) -> Any:
    """Return the value unchanged; used to verify the chain and persistence."""
    return value


def flaky(
    fail_times: int = 2,
    *,
    error: str = "retryable",
    message: str = "mock failure",
    name: str | None = None,
    retry: Retrying | None = None,
) -> TaskSpec:
    """Fail the first ``fail_times`` attempts, then succeed (decided by ctx.attempt, reproducible by construction).

    The default retry budget is exactly enough (``fail_times + 1`` attempts), which makes retry behavior easy to observe.
    """

    def _fail(ctx: Any) -> None:
        if error == "fatal":
            raise FatalError(message)
        if error == "timeout":
            raise TimeoutError(message)
        if error == "rate_limit":
            raise RetryableError(message, error_class="rate_limit", retry_after=0.01)
        if error == "invalid":
            raise ValueError(message)
        raise RetryableError(message)

    def _impl(value: Any, ctx: Any) -> Any:
        if ctx.attempt <= fail_times:
            _fail(ctx)
        return {"value": value, "attempts": ctx.attempt, "resource": None}

    _impl.__name__ = name or f"flaky{fail_times}"
    return build_task_spec(
        _impl,
        name=name or "mock.flaky",
        retry=retry or Retrying(max_attempts=max(1, fail_times + 1), base=0.001, cap=0.01),
    )


def delay(seconds: float = 1.0, *, name: str | None = None) -> TaskSpec:
    """A fixed delay, useful for observing concurrency and throughput."""

    async def _impl(value: Any, ctx: Any) -> Any:
        await ctx.clock.sleep(seconds)
        return value

    _impl.__name__ = name or f"delay{seconds}"
    return build_task_spec(_impl, name=name or "mock.delay")


def boom(message: str = "boom", *, error: str = "retryable", name: str | None = None) -> TaskSpec:
    """Always fails."""

    def _impl(value: Any, ctx: Any) -> Any:
        if error == "fatal":
            raise FatalError(message)
        if error == "invalid":
            raise ValueError(message)
        raise RetryableError(message, error_class="upstream")

    _impl.__name__ = name or "mock.boom"
    return build_task_spec(_impl, name=name or "mock.boom")


def leaky(*, name: str | None = None, pool: str | None = None) -> TaskSpec:
    """Deliberately leaks a lease (acquired through the escape hatch and never returned), to verify the forced-reclaim guarantee."""

    async def _impl(value: Any, ctx: Any) -> Any:
        lease = await ctx.acquire_lease(pool)  # deliberately not released
        return {"leaked": lease.resource.id}

    _impl.__name__ = name or "mock.leaky"
    return build_task_spec(_impl, name=name or "mock.leaky", resource=pool)


def simulate_llm(
    *,
    latency_ms: float = 5.0,
    fail_rate: float = 0.0,
    error: str = "rate_limit",
    tokens: int = 32,
    name: str | None = None,
    resource: str | None = None,
    **selector: Any,
) -> TaskSpec:
    """Simulate one "model request": acquire → wait → report → return.

    This is the workhorse task for demonstrating resource pools and backoff; a real implementation just swaps ``ctx.clock.sleep`` for an HTTP request.
    """

    async def _impl(value: Any, ctx: Any) -> Any:
        rng = random.Random(ctx.seed)
        async with ctx.acquire(resource, **selector) as lease:
            latency = latency_ms * (0.5 + rng.random())
            await ctx.clock.sleep(latency / 1000.0)
            if fail_rate and rng.random() < fail_rate:
                lease.report(ok=False, latency_ms=latency, error=error)
                if error == "timeout":
                    raise TimeoutError("simulated timeout")
                if error == "fatal":
                    raise FatalError("simulated fatal")
                raise RetryableError("simulated failure", error_class=error, retry_after=0.01)
            lease.report(ok=True, latency_ms=latency, usage={"tokens": tokens})
            return {
                "text": f"reply-to:{json.dumps(value, ensure_ascii=False, default=str)[:120]}",
                "resource": lease.resource.id,
                "latency_ms": round(latency, 3),
                "usage": {"tokens": tokens},
            }

    _impl.__name__ = name or "mock.llm"
    return build_task_spec(
        _impl,
        name=name or "mock.llm",
        resource=resource,
        retry=Retrying(max_attempts=3, base=0.01, cap=0.05),
    )


# ---------------------------------------------------------- general helpers
def shell_run(
    command: str | list[str],
    *,
    timeout_s: float | None = 60.0,
    check: bool = True,
    name: str | None = None,
) -> TaskSpec:
    """Run a subprocess (standard library) and return stdout/stderr as the result."""

    async def _impl(value: Any, ctx: Any) -> Any:
        cmd = command.format(value=json.dumps(value, default=str)) if isinstance(command, str) else command
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout_s)
        except TimeoutError:
            proc.kill()
            raise
        result = {
            "cmd": cmd,
            "returncode": proc.returncode,
            "stdout": out.decode("utf-8", "replace")[:10000],
            "stderr": err.decode("utf-8", "replace")[:10000],
        }
        if check and proc.returncode != 0:
            raise RetryableError(
                f"command failed ({proc.returncode}): {result['stderr'][:200]}", error_class="upstream"
            )
        return result

    _impl.__name__ = name or "shell.run"
    return build_task_spec(_impl, name=name or "shell.run")


def write_jsonl(path: str, *, mode: str = "a", name: str | None = None) -> TaskSpec:
    """Append the artifact to a JSONL file (standard library). Scoring is out of scope for the framework; this is just one export mechanism."""

    def _impl(value: Any, ctx: Any) -> Any:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, mode, encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")
        return {"written": path, "bytes": len(json.dumps(value, default=str))}

    _impl.__name__ = name or "file.write_jsonl"
    return build_task_spec(_impl, name=name or "file.write_jsonl")


def jsonl_source(path: str | Path, *, limit: int | None = None) -> Iterator[Any]:
    """Read a JSONL file as a seed stream (each line = one pipeline's input artifact)."""
    with open(path, encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if limit is not None and index >= limit:
                return
            line = line.strip()
            if line:
                yield json.loads(line)


def seed_factory(
    kind: str = "range", *, n: int = 10, path: str | None = None, limit: int | None = None, **_: Any
) -> Iterable[Any]:
    """Resolution of ``source`` in a declarative config."""
    if kind == "range":
        return [{"i": i} for i in range(n)]
    if kind == "jsonl":
        if path is None:
            raise ValueError("jsonl source requires path")
        return jsonl_source(path, limit=limit)
    raise ValueError(f"unknown source kind: {kind}")


BUILTIN_TASKS = {
    "echo": echo,
    "flaky": flaky,
    "delay": delay,
    "boom": boom,
    "leaky": leaky,
    "simulate_llm": simulate_llm,
    "shell_run": shell_run,
    "write_jsonl": write_jsonl,
}
