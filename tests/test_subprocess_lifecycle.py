"""Subprocess lifecycle for ``shell_run`` (issue #33): cancellation, timeouts and Runner stops must
terminate *and reap* the child, and on POSIX shell mode must take its descendants with it.

The bug this file pins: cancelling the coroutine returned by ``shell_run(...)`` raised
``CancelledError`` while the child kept running, and the timeout path called ``proc.kill()`` and
re-raised without waiting for the reap — so ``os.kill(pid, 0)`` still succeeded after the coroutine
was already gone.

Two rules keep these tests honest and non-flaky:

* readiness is signalled by the child itself (it writes its own PID to a file) and the test waits for
  that file with a deadline — nothing here sleeps for a fixed time hoping the child has started;
* process state is asserted against the real OS: ``os.kill(pid, 0)`` must fail, which is only true
  once the child has actually been *reaped* (a zombie still answers signal 0), and it is asserted as
  soon as the awaiting code returns, with no polling that could forgive a slow cleanup. The one
  exception is the test that breaks the reap wait on purpose: there the reaping is left to the child
  watcher, so that test polls with a deadline instead. The ``tracked_pids`` fixture SIGKILLs every PID
  a test registered when the test ends, so a red test cannot leave a live child behind; the children
  also exit on their own after 30s as a second net.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shlex
import signal
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from helpers import run

from pyattacker import Runner, pipeline
from pyattacker.tasks import shell_run

_CHILD_SCRIPT = '''\
"""Test child: report readiness with a PID file (when the signalling is about *this* process)."""
import os
import subprocess
import sys
import time
from pathlib import Path

Path(sys.argv[1]).write_text(str(os.getpid()))
if len(sys.argv) > 2 and sys.argv[2] == "--spawn":  # a descendant that reports its own PID
    subprocess.Popen([sys.executable, __file__, sys.argv[3]])
print("ready", flush=True)
time.sleep(30)  # bounded lifetime: even a leaked child dies on its own
'''


@pytest.fixture(scope="session")
def child_script(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One child program for the whole file: report readiness, optionally spawn a descendant, wait."""
    path = tmp_path_factory.mktemp("shell-lifecycle") / "child.py"
    path.write_text(_CHILD_SCRIPT, encoding="utf-8")
    return path


@pytest.fixture
def tracked_pids() -> Callable[..., None]:
    """Guarantee that a failing test leaves no live child: kill every PID the test ever saw."""
    pids: list[int] = []

    def track(*new_pids: int) -> None:
        pids.extend(new_pids)

    yield track
    for pid in pids:
        # already reaped, or exited between the liveness check and the kill
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)


def _child_command(child_script: Path, pid_file: Path, *, spawn: Path | None = None) -> list[str]:
    command = [sys.executable, str(child_script), str(pid_file)]
    if spawn is not None:
        command += ["--spawn", str(spawn)]
    return command


async def _wait_for_pid(pid_file: Path, *, timeout_s: float = 10.0) -> int:
    """Wait for the child's readiness signal — its own PID — with a deadline, never a fixed sleep."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if pid_file.exists():
            text = pid_file.read_text(encoding="utf-8").strip()
            if text:
                return int(text)
        await asyncio.sleep(0.01)
    raise AssertionError(f"the child never signalled readiness: {pid_file} stayed empty for {timeout_s}s")


def _assert_reaped(pid: int) -> None:
    """The observable guarantee: the OS no longer knows this PID.

    ``os.kill(pid, 0)`` succeeds for a running process *and* for a zombie nobody has waited for, so a
    failure here means either "still running" or "killed but not reaped" — exactly the two states
    the fix must rule out.
    """
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def _await_cancellation(task: asyncio.Task[object]) -> None:
    """Await a cancelled task under a hard deadline, so a hung cleanup fails instead of blocking pytest."""
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 30.0)


async def _wait_until_gone(pid: int, *, timeout_s: float = 5.0) -> None:
    """Poll with a deadline for a killed child to disappear from the OS's view.

    Only needed when the *reap wait* itself failed, so the reaping is left to the child watcher.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"PID {pid} was still known to the OS {timeout_s}s after it was killed")


async def _cancel_quietly(task: asyncio.Task[object]) -> None:
    """Cancel and forget a task, bounded: teardown must not hang even if the task is somehow stuck."""
    task.cancel()
    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 30.0)


def test_cancelling_the_task_kills_and_reaps_the_argv_child(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """The issue's minimal example: cancel the task, and the child is already gone and reaped when
    the ``CancelledError`` surfaces (no signal survives on its PID, and it is not a zombie either)."""
    pid_file = tmp_path / "cancelled.pid"

    async def scenario() -> int:
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None)
        task = asyncio.ensure_future(spec(None, None))
        try:
            pid = await _wait_for_pid(pid_file)  # the child is running, not merely spawned
            tracked_pids(pid)
            task.cancel()
            await _await_cancellation(task)
        finally:
            await _cancel_quietly(task)
        return pid

    _assert_reaped(run(scenario()))


def test_timeout_kills_and_reaps_the_argv_child(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """A timeout must not only kill the child but wait for the reap: the ``TimeoutError`` surfaces
    with the child already reaped. The child's readiness signal is written long before the 2s
    deadline (interpreter startup is ~50ms), so the PID is known when the timeout fires."""
    pid_file = tmp_path / "timed_out.pid"

    async def scenario() -> int:
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=2.0)
        task = asyncio.ensure_future(spec(None, None))
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(task, 30.0)
            pid = await _wait_for_pid(pid_file)  # it did run: the timeout fired during its sleep
            tracked_pids(pid)
        finally:
            await _cancel_quietly(task)
        return pid

    _assert_reaped(run(scenario()))


def test_runner_stop_kills_and_reaps_the_child(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """The Runner's own stop path: a graceful stop cancels the straggler after ``grace_s``, and the
    child is already gone and reaped by the time the run reports back."""
    pid_file = tmp_path / "stopped.pid"

    async def scenario() -> tuple[int, object]:
        runner = Runner(store=":memory:", handle_signals=False, grace_s=0.05)
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None)
        run_task = asyncio.ensure_future(runner.run_async(pipeline("stopped", spec).map([{"i": 0}])))
        try:
            pid = await _wait_for_pid(pid_file)  # the pipeline is in flight, inside the child
            tracked_pids(pid)
            runner.stop("test")
            report = await asyncio.wait_for(run_task, 30.0)
        finally:
            await _cancel_quietly(run_task)
            runner.close()
        return pid, report

    pid, report = run(scenario())
    assert report.status == "interrupted"
    assert report.stop_reason == "test"
    _assert_reaped(pid)


def test_a_runner_level_task_timeout_kills_and_reaps_the_child(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """A task-level ``timeout_s`` (including the deadline a fan-out group carries) is enforced by the
    Runner with its own ``wait_for``, so it reaches ``shell_run`` as a cancellation rather than as its
    own timeout: the child must still be killed and reaped, and the pipeline records the timeout."""
    pid_file = tmp_path / "task_timeout.pid"

    async def scenario() -> tuple[int, object]:
        runner = Runner(store=":memory:", handle_signals=False)
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None).with_overrides(timeout_s=2.0)
        run_task = asyncio.ensure_future(runner.run_async(pipeline("task-timeout", spec).map([{"i": 0}])))
        try:
            pid = await _wait_for_pid(pid_file)  # the child is running well before the 2s deadline
            tracked_pids(pid)
            report = await asyncio.wait_for(run_task, 30.0)
        finally:
            await _cancel_quietly(run_task)
            runner.close()
        return pid, report

    pid, report = run(scenario())
    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    _assert_reaped(pid)


def test_a_second_cancellation_during_cleanup_still_reaps_the_child(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """Cleanup must survive a cancellation aimed at itself: the second ``cancel()`` lands one
    event-loop step later, while the first is being cleaned up. The child is still reaped and the
    caller still sees a cancellation — an implementation that awaits the reap unshielded could lose
    it, and one that retries forever would trip the 30s deadline."""
    pid_file = tmp_path / "double_cancel.pid"

    async def scenario() -> int:
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None)
        task = asyncio.ensure_future(spec(None, None))
        try:
            pid = await _wait_for_pid(pid_file)
            tracked_pids(pid)
            task.cancel()
            await asyncio.sleep(0)  # let the coroutine take its first step into the cleanup
            task.cancel()  # ... and cancel the cleanup itself
            await _await_cancellation(task)
        finally:
            await _cancel_quietly(task)
        return pid

    _assert_reaped(run(scenario()))


def test_a_failing_cleanup_cannot_replace_the_callers_exception(
    tmp_path: Path,
    tracked_pids: Callable[..., None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup is best effort by construction: a reader left in a bad state by the cancelled
    ``communicate()``, a reap wait that fails, or a signal call that raises after delivering its
    signal must not change what the caller sees. The caller still gets its own ``CancelledError``
    (not the ``ConnectionResetError``/``RuntimeError`` cleanup ran into), and the child is still
    killed even though only the *reporting* of the kill fails.

    The three failures are injected at the seams the cleanup helpers use. The child is deliberately
    silent and the broken reader is armed only after the read task is parked inside the real
    ``read()``: a child that wrote to the pipe would let ``communicate()`` re-enter the patched call
    on the normal path, which would fail the run before cleanup and prove nothing.
    """
    pid_file = tmp_path / "broken_cleanup.pid"
    code = "import os, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(30)"
    armed = False
    real_read = asyncio.StreamReader.read
    real_killpg = os.killpg

    async def gated_read(stream: asyncio.StreamReader, *args: object) -> bytes:
        if armed:
            raise ConnectionResetError("reader left in a bad state by the cancelled communicate()")
        return await real_read(stream, *args)

    async def broken_wait(*args: object, **kwargs: object) -> int:
        raise ConnectionResetError("child watcher never reported the exit")

    def broken_killpg(pid: int, sig: int) -> None:
        real_killpg(pid, sig)
        raise RuntimeError("signal delivered, then the call failed")

    monkeypatch.setattr(asyncio.StreamReader, "read", gated_read)
    monkeypatch.setattr(asyncio.subprocess.Process, "wait", broken_wait)
    monkeypatch.setattr(os, "killpg", broken_killpg)

    async def scenario() -> int:
        nonlocal armed
        spec = shell_run([sys.executable, "-c", code, str(pid_file)], timeout_s=None)
        task = asyncio.ensure_future(spec(None, None))
        try:
            pid = await _wait_for_pid(pid_file)
            tracked_pids(pid)
            armed = True  # from here on, only the cleanup may see the broken reader
            task.cancel()
            await _await_cancellation(task)
            await _wait_until_gone(pid)  # killed even though the reap wait itself failed
        finally:
            await _cancel_quietly(task)
        return pid

    _assert_reaped(run(scenario()))


def test_a_child_that_already_exited_is_left_alone(
    tmp_path: Path, tracked_pids: Callable[..., None]
) -> None:
    """The normal-exit leg of the same cleanup path: a command that exits on its own keeps its
    result, is reaped, and is not signalled (the process is already gone when cleanup looks)."""
    pid_file = tmp_path / "exited.pid"
    code = "import os, sys; open(sys.argv[1], 'w').write(str(os.getpid()))"
    result = run(shell_run([sys.executable, "-c", code, str(pid_file)])(None, None))

    assert result["returncode"] == 0
    assert result["stdout"] == ""
    pid = int(pid_file.read_text(encoding="utf-8"))
    tracked_pids(pid)
    _assert_reaped(pid)


@pytest.mark.skipif(
    os.name != "posix",
    reason="process-group signalling is POSIX-only: Windows has no os.killpg and asyncio cannot "
    "signal a group there, so the descendant guarantee is not offered (see docs/reference.md)",
)
def test_shell_mode_kills_the_whole_process_group(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """A string command runs through a shell, so the task's direct child is the shell and the real
    work is a descendant. Cancelling the task must take the descendant with it: without the process
    group, the descendant would keep running (and hold the pipe) after the shell was killed."""
    shell_pid_file = tmp_path / "shell.pid"
    child_pid_file = tmp_path / "shell_child.pid"
    command = (
        f"echo $$ > {shlex.quote(str(shell_pid_file))}; "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(child_script))} "
        f"{shlex.quote(str(child_pid_file))} & wait"
    )

    async def scenario() -> tuple[int, int]:
        task = asyncio.ensure_future(shell_run(command, timeout_s=None)(None, None))
        try:
            shell_pid = await _wait_for_pid(shell_pid_file)  # the direct child: the shell
            child_pid = await _wait_for_pid(child_pid_file)  # the descendant: up and running
            tracked_pids(shell_pid, child_pid)
            assert shell_pid != child_pid
            task.cancel()
            await _await_cancellation(task)
        finally:
            await _cancel_quietly(task)
        return shell_pid, child_pid

    shell_pid, child_pid = run(scenario())
    _assert_reaped(shell_pid)
    _assert_reaped(child_pid)


@pytest.mark.skipif(
    os.name != "posix",
    reason="process-group signalling is POSIX-only: Windows has no os.killpg and asyncio cannot "
    "signal a group there, so the descendant guarantee is not offered (see docs/reference.md)",
)
def test_argv_mode_kills_descendants_too(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """The argv form starts the program directly, but that program may spawn children of its own;
    on POSIX the same group signal reaches them, which is what the documented guarantee claims."""
    parent_pid_file = tmp_path / "argv_parent.pid"
    child_pid_file = tmp_path / "argv_child.pid"
    command = _child_command(child_script, parent_pid_file, spawn=child_pid_file)

    async def scenario() -> tuple[int, int]:
        task = asyncio.ensure_future(shell_run(command, timeout_s=None)(None, None))
        try:
            parent_pid = await _wait_for_pid(parent_pid_file)
            child_pid = await _wait_for_pid(child_pid_file)  # the descendant: up and running
            tracked_pids(parent_pid, child_pid)
            assert parent_pid != child_pid
            task.cancel()
            await _await_cancellation(task)
        finally:
            await _cancel_quietly(task)
        return parent_pid, child_pid

    parent_pid, child_pid = run(scenario())
    _assert_reaped(parent_pid)
    _assert_reaped(child_pid)
