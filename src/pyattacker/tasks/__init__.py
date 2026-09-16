"""Built-in utility tasks.

Only **network-independent** things live here: mock tasks, dataset reading, subprocesses, file writing.
Real model requests (openai / anthropic protocols) are up to the user —— the framework does not handle networking.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import random
import shlex
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from ..errors import FatalError, RetryableError
from ..task import Retrying, TaskSpec, build_task_spec, task

__all__ = [
    "echo",
    "fanout",
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
def fanout(
    *specs: TaskSpec,
    name: str | None = None,
    on_error: str = "raise",
    retry: Retrying | None = None,
) -> TaskSpec:
    """Run several tasks on the *same* input, concurrently, inside one task.

    The task model is unary and linear on purpose; when a step genuinely branches (three judges,
    k samples, several metrics), this keeps the branch inside one task instead of turning the
    pipeline into a DAG. Three consequences, all deliberate:

    * **Retry granularity is the group.** If one branch fails, the whole fan-out is retried; the
      children's own retry policies are not applied branch by branch. The group therefore adopts
      the most forgiving child policy unless you pass ``retry=``.
    * **Branches share the parent's context**, so their leases and events are recorded under the
      fan-out task. That is what keeps a single, complete record per step.
    * **The group spec is the only one the Runner ever sees** (the children are called as plain
      functions), so ``resource``, ``algorithm`` and ``timeout_s`` are lifted onto it from the
      children — but only when every child agrees on the value. Disagreeing children leave the
      group unset, which means the pool's default is used and the timeout is decided by the group.
      Note the timeout becomes a *group* deadline: it bounds all branches together.

    ``on_error="raise"`` (default) fails the task if any branch fails, like any other exception.
    ``on_error="collect"`` never raises and returns ``{child_name: {"ok": bool, ...}}`` instead.
    """
    if not specs:
        raise ValueError("fanout needs at least one task")
    if on_error not in ("raise", "collect"):
        raise ValueError(f"on_error must be 'raise' or 'collect', got {on_error!r}")

    policy = retry or max((spec.retry for spec in specs), key=lambda r: r.max_attempts)

    async def _impl(value: Any, ctx: Any) -> dict[str, Any]:
        async def _one(spec: TaskSpec) -> Any:
            try:
                produced = spec(value, ctx)
                return await produced if inspect.isawaitable(produced) else produced
            except Exception as exc:  # collected here; the group decides what to do with it
                ctx.emit("fanout.branch_failed", branch=spec.name, error=f"{type(exc).__name__}: {exc}")
                return exc

        results = await asyncio.gather(*(_one(spec) for spec in specs))
        failures = {
            spec.name: result
            for spec, result in zip(specs, results, strict=True)
            if isinstance(result, BaseException)
        }
        ctx.emit(
            "fanout.done",
            branches=len(specs),
            failed=len(failures),
            failed_branches=sorted(failures),
        )
        if on_error == "collect":
            return {
                spec.name: (
                    {"ok": False, "error": f"{type(result).__name__}: {result}"}
                    if isinstance(result, BaseException)
                    else {"ok": True, "value": result}
                )
                for spec, result in zip(specs, results, strict=True)
            }
        if failures:
            first = next(iter(failures.values()))
            raise first  # keep the original class so the retry policy classifies it correctly
        return {spec.name: result for spec, result in zip(specs, results, strict=True)}

    _impl.__name__ = name or "fanout"
    return build_task_spec(
        _impl,
        name=name or "fanout",
        retry=policy,
        **_agreed(specs, "resource"),
        **_agreed(specs, "algorithm"),
        **_agreed(specs, "timeout_s"),
    )


def _agreed(specs: Sequence[TaskSpec], attr: str) -> dict[str, Any]:
    """Lift ``attr`` onto the group spec when every child carries the same non-``None`` value.

    ``fanout`` invokes the child *functions*, so the Runner never sees a child :class:`TaskSpec`:
    anything the children declare has to be re-stated on the group to have any effect. Agreement is
    required because the group can only mean one thing -- with mixed values, doing nothing is more
    honest than silently picking one child's policy.
    """
    values = [getattr(spec, attr) for spec in specs]
    first = values[0]
    if first is None or any(value != first for value in values):
        return {}
    return {attr: first}


def shell_run(
    command: str | list[str],
    *,
    timeout_s: float | None = 60.0,
    check: bool = True,
    name: str | None = None,
) -> TaskSpec:
    """Run a subprocess (standard library) and return stdout/stderr as the result.

    **Prefer the argv form** — ``command=["python", "postprocess.py", "--input", "{value}"]`` —
    which runs via ``create_subprocess_exec`` and never involves a shell: ``{value}`` (the
    upstream artifact, JSON-encoded) is substituted per-argument, so it is passed to the child
    process as one literal argument no matter what characters it contains. This is the safe
    choice whenever ``value`` comes from an untrusted source (model/judge output, external
    data), which is the common case in evaluation pipelines.

    A plain string ``command`` is run through the system shell (``create_subprocess_shell``) for
    when you actually need shell features (pipes, globbing, redirection). ``{value}`` is
    substituted with :func:`shlex.quote` applied, which is safe against shell metacharacters for
    that one substitution — but the rest of a template you write yourself is still your own
    responsibility, and quoting cannot help if the template itself, not just ``{value}``, is
    built from untrusted input.
    """

    async def _impl(value: Any, ctx: Any) -> Any:
        encoded = json.dumps(value, default=str)
        if isinstance(command, str):
            cmd: str | list[str] = command.format(value=shlex.quote(encoded))
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        else:
            cmd = [part.format(value=encoded) for part in command]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
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
    "fanout": fanout,
    "flaky": flaky,
    "delay": delay,
    "boom": boom,
    "leaky": leaky,
    "simulate_llm": simulate_llm,
    "shell_run": shell_run,
    "write_jsonl": write_jsonl,
}
