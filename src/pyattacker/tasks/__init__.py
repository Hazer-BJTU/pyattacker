"""Built-in utility tasks.

Only **network-independent** things live here: mock tasks, dataset reading, subprocesses, file writing.
Real model requests (openai / anthropic protocols) are up to the user —— the framework does not handle networking.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
import signal
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from ..errors import ConfigError, FatalError, RetryableError
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
        parameters={"fail_times": fail_times, "error": error, "message": message},
        retry=retry or Retrying(max_attempts=max(1, fail_times + 1), base=0.001, cap=0.01),
    )


def delay(seconds: float = 1.0, *, name: str | None = None) -> TaskSpec:
    """A fixed delay, useful for observing concurrency and throughput."""

    async def _impl(value: Any, ctx: Any) -> Any:
        await ctx.clock.sleep(seconds)
        return value

    _impl.__name__ = name or f"delay{seconds}"
    return build_task_spec(_impl, name=name or "mock.delay", parameters={"seconds": seconds})


def boom(message: str = "boom", *, error: str = "retryable", name: str | None = None) -> TaskSpec:
    """Always fails."""

    def _impl(value: Any, ctx: Any) -> Any:
        if error == "fatal":
            raise FatalError(message)
        if error == "invalid":
            raise ValueError(message)
        raise RetryableError(message, error_class="upstream")

    _impl.__name__ = name or "mock.boom"
    return build_task_spec(_impl, name=name or "mock.boom", parameters={"message": message, "error": error})


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
        parameters={
            "latency_ms": latency_ms, "fail_rate": fail_rate, "error": error,
            "tokens": tokens, "resource": resource, "selector": selector,
        },
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
        parameters={"on_error": on_error},
        children=tuple(specs),
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


# ------------------------------------------------------- subprocess lifecycle
# `shell_run` owns the process it starts, so every exit path -- normal exit, timeout, coroutine
# cancellation, any other exception -- runs the same cleanup: if the child is still running it is
# SIGKILLed and then *reaped* before the original exception propagates. The waits are bounded so a
# pathological child cannot hold a cancellation hostage; SIGKILL is not catchable, so the bound only
# matters if the OS never reports the exit at all.
#
# Every step below is best effort and swallows its own failures (`Exception`, plus `CancelledError`,
# which is not an `Exception`): the exception the caller is about to see must be the one it gets, so
# neither a pipe a cancelled `communicate()` left in a bad state nor a failed kill/reap may replace a
# `CancelledError` with a `ConnectionResetError`. Only a `BaseException` that is *not* cancellation --
# `KeyboardInterrupt`, `SystemExit` -- is allowed through.
_POSIX = os.name == "posix"
_CLEANUP_TIMEOUT_S = 5.0
# POSIX: start each child in its own session, so that it leads its own process group and one
# `killpg` reaches a whole tree -- a shell pipeline, or whatever an argv program spawned. Windows has
# no equivalent in the standard library (`start_new_session` is ignored there and `os.killpg` does
# not exist), so only the direct child is terminated; see docs/reference.md for the stated guarantee.
_SPAWN_KWARGS: dict[str, Any] = {"start_new_session": True} if _POSIX else {}


def _signal_process(proc: asyncio.subprocess.Process) -> None:
    """SIGKILL a still-running child -- and, on POSIX, everything in its process group.

    Nothing raised here escapes. The process may have exited between the caller's ``returncode``
    check and this signal (``ProcessLookupError``): that race is exactly what the cleanup has to
    tolerate, and a failure to signal is not something to escalate -- a *new* exception raised from
    cleanup would replace the one the caller is about to see.
    """
    try:
        if _POSIX:
            # The child is a session leader (``start_new_session``), so its group id is its pid.
            os.killpg(proc.pid, signal.SIGKILL)
        else:  # pragma: no cover - Windows: no process-group signalling in the standard library
            proc.kill()
    except Exception:
        return  # already gone, or not signalable: the reap wait below still runs


async def _drain_pipes(proc: asyncio.subprocess.Process, deadline: float) -> None:
    """Read to EOF so the pipe transports disconnect and ``Process.wait()`` can complete.

    ``wait()`` is resolved by the transport only once every pipe has seen EOF, and a cancelled
    ``communicate()`` can leave a reader paused above its flow-control limit with data still
    buffered. The child (POSIX: its whole group) has just been killed, so EOF is close; the deadline
    covers the remaining case of a write end held open by a process that escaped the group.
    """
    streams = [stream for stream in (proc.stdout, proc.stderr) if stream is not None]
    remaining = deadline - asyncio.get_running_loop().time()
    if not streams or remaining <= 0:
        return
    try:
        await asyncio.wait_for(asyncio.gather(*(stream.read() for stream in streams)), remaining)
    except (TimeoutError, asyncio.CancelledError):
        return  # best effort: the reap below is what the lifecycle guarantee rests on
    except Exception:
        return  # a reader left in a bad state by the cancelled communicate() must not mask the caller's error


async def _wait_reaped(proc: asyncio.subprocess.Process, deadline: float) -> None:
    """Wait until the OS has reaped the child, absorbing a cancellation aimed at the cleanup itself.

    ``proc.returncode`` is set by the child watcher once ``waitpid`` has returned, so observing it
    (here, through ``Process.wait()``) is what makes "gone, not merely signalled" true. Cancellation
    must not skip that wait: a cancelled ``wait()`` is safe to retry (the transport ignores cancelled
    waiters and the watcher reaps the zombie either way), so a cancellation arriving during cleanup
    is swallowed here -- the caller's own exception still propagates once cleanup returns.
    """
    loop = asyncio.get_running_loop()
    while proc.returncode is None:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return  # bounded: stop observing rather than hang the caller
        try:
            await asyncio.wait_for(proc.wait(), remaining)
        except TimeoutError:
            return
        except asyncio.CancelledError:
            continue
        except Exception:
            return  # a failed wait is reported to nobody rather than replacing the caller's exception


async def _cleanup_process(proc: asyncio.subprocess.Process) -> None:
    """Terminate a still-running child and reap it; a no-op once it has exited.

    Called from the ``finally`` of every ``shell_run`` attempt, so a normal exit (already exited and
    reaped) and a natural exit racing the cleanup both fall through the first check, while a timeout,
    a cancellation and any other exception get the same kill-then-reap treatment. Nothing here can
    raise, which is what keeps the caller's exception type intact on the way out.
    """
    if proc.returncode is not None:
        return
    _signal_process(proc)
    deadline = asyncio.get_running_loop().time() + _CLEANUP_TIMEOUT_S
    await _drain_pipes(proc, deadline)
    await _wait_reaped(proc, deadline)


async def _spawn_process(
    spawn: Coroutine[Any, Any, asyncio.subprocess.Process],
) -> asyncio.subprocess.Process:
    """Create the child, keeping the handle when the caller is cancelled while creation is in flight.

    ``create_subprocess_*`` returns only once asyncio has finished building the transport and
    protocol, while the OS process exists from the moment the platform spawn call returns: a
    cancellation delivered in that window abandons a live child that no frame holds a reference to.
    The creation therefore runs as its own task, and the handle is acquired under a shield, so a
    cancellation cannot drop it -- this frame keeps waiting for the process, disposes of it (it is
    the only frame that has it), and only then lets the cancellation continue to the caller.

    How long creation takes is the caller's own business until the caller is cancelled; from then on
    the wait is bounded by the same budget as the rest of the cleanup, because a stop must not park
    behind an unresponsive creation. A creation that misses that budget cannot be owned by anybody
    here: the cancellation wins and a child it may have produced is left to the OS -- the trade is a
    possible orphan instead of a task that never finishes, and it is stated rather than pretended away.

    This window is also where the cleanup starts: a cancellation arriving before the platform spawn
    call has even run still produces a child that is disposed of here, because whether one exists is
    not knowable from the outside.
    """
    task = asyncio.ensure_future(spawn)
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        timeout = None if cancelled is None else _CLEANUP_TIMEOUT_S
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout)
        except TimeoutError:
            break  # only the cancelled branch has a timeout: the handle is not coming
        except asyncio.CancelledError as exc:
            if task.cancelled():
                break  # the creation itself was cancelled: there is nothing to own yet
            # Our own cancellation: the creation keeps running, and the child it produces is ours.
            cancelled = cancelled or exc
        except Exception as exc:
            if cancelled is None:
                raise
            raise cancelled from exc  # already cancelled: the caller must still see that
    if cancelled is None:
        return task.result()  # which raises the creation's own failure, if it failed
    if task.done() and not task.cancelled() and task.exception() is None:
        await _cleanup_process(task.result())
    raise cancelled


def shell_run(
    command: str | list[str],
    *,
    timeout_s: float | None = 60.0,
    check: bool = True,
    name: str | None = None,
) -> TaskSpec:
    """Run a subprocess (standard library) and return stdout/stderr as the result.

    **Use the argv form to pass ``value`` to the command** —
    ``command=["python", "postprocess.py", "--input", "{value}"]`` — which runs via
    ``create_subprocess_exec`` and never involves a shell: the literal substring ``"{value}"`` in
    each argv element is replaced with the upstream artifact (JSON-encoded), so it reaches the
    child process as one literal argument no matter what characters it contains. The replacement
    is a plain substring swap, not ``str.format()`` — other braces in an argument (a ``jq``
    filter, a Python dict literal) are left alone and are never treated as placeholders. This is
    required whenever ``value`` comes from an untrusted source (model/judge output, external
    data), which is the common case in evaluation pipelines — no quoting scheme applied to a
    single substitution can make it safe to insert into an arbitrary shell-syntax position
    (unquoted, inside ``'...'``, inside ``"..."``, inside command substitution, ...), because that
    safety depends on *where* in the template the substitution lands, which is up to whoever
    wrote the template.

    A plain string ``command`` is run through the system shell (``create_subprocess_shell``) for
    when you need actual shell features (pipes, globbing, redirection, `&&`). Because of the
    above, a string command may **not** reference ``{value}`` at all — constructing one that does
    raises :class:`ConfigError` immediately, rather than silently running something unsafe.

    The guarantee is "no *implicit* shell for the argv form", not "safe with any program": if an
    argv command's own program is itself a shell or another interpreter (e.g.
    ``["sh", "-c", "echo {value}"]``), that interpreter will parse the substituted argument as its
    own syntax, and the usual injection risk applies again — that interpreter's input-safety rules
    are then the caller's responsibility, not something this function can enforce.

    **Process lifetime.** The task owns the process it starts, from the moment the OS child exists —
    process creation included, so a cancellation landing after the child has been created but before
    a handle reached this frame does not abandon it. On every exit path — a normal exit,
    ``timeout_s`` expiring, the coroutine being cancelled, any other exception — a child that is
    still running is SIGKILLed and then *reaped* before the exception propagates: cancellation
    still arrives at the caller as ``CancelledError`` and a timeout as ``TimeoutError``, but nothing
    keeps running behind them. A child that already exited is left alone. On POSIX the child runs in
    its own session, so cleanup signals the whole process group (a shell pipeline, or whatever an
    argv program spawned); on Windows only the direct child is terminated, because the standard
    library has no process-group signalling there.
    """
    if not isinstance(command, str):
        command = list(command)  # snapshot argv so caller mutation cannot change declared behavior

    # A literal substring check, not str.format()/Formatter parsing: neither a string command nor
    # an argv element should have to avoid unrelated brace syntax (a jq filter, a Python literal)
    # just because the framework also uses braces for its one placeholder.
    if isinstance(command, str) and "{value}" in command:
        raise ConfigError(
            "shell_run: a string command may not interpolate {value} — there is no shell quoting "
            "rule that stays safe regardless of where in the template the substitution lands. "
            "Use the argv form instead: command=[..., '{value}', ...], which passes value as one "
            "literal argument via create_subprocess_exec (no shell involved)."
        )

    async def _impl(value: Any, ctx: Any) -> Any:
        if isinstance(command, str):
            cmd: str | list[str] = command
            spawn = asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **_SPAWN_KWARGS,
            )
        else:
            encoded = json.dumps(value, default=str)
            cmd = [part.replace("{value}", encoded) for part in command]
            spawn = asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **_SPAWN_KWARGS,
            )
        proc = await _spawn_process(spawn)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout_s)
        finally:
            # One cleanup for every exit path, process creation included (that one runs before this
            # frame holds a handle, so `_spawn_process` disposes of it itself). A normal exit finds
            # the child already reaped and does nothing; a timeout, a cancellation or any other
            # exception kills the child (POSIX: its process group) and waits for the reap before
            # that exception propagates.
            await _cleanup_process(proc)
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
    return build_task_spec(
        _impl, name=name or "shell.run",
        parameters={"command": command, "timeout_s": timeout_s, "check": check},
    )


def write_jsonl(path: str, *, mode: str = "a", name: str | None = None) -> TaskSpec:
    """Append the artifact to a JSONL file (standard library). Scoring is out of scope for the framework; this is just one export mechanism."""

    def _impl(value: Any, ctx: Any) -> Any:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, mode, encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")
        return {"written": path, "bytes": len(json.dumps(value, default=str))}

    _impl.__name__ = name or "file.write_jsonl"
    return build_task_spec(_impl, name=name or "file.write_jsonl", parameters={"path": path, "mode": mode})


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
