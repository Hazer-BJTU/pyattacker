"""Subprocess lifecycle for ``shell_run`` (issue #33): cancellation, timeouts and Runner stops must
terminate *and reap* the child, and on POSIX shell mode must take its descendants with it.

The bug this file pins: cancelling the coroutine returned by ``shell_run(...)`` raised
``CancelledError`` while the child kept running, and the timeout path called ``proc.kill()`` and
re-raised without waiting for the reap — so ``os.kill(pid, 0)`` still succeeded after the coroutine
was already gone. The same ownership question applies while the process is still being created, so
that window has a test of its own.

Two rules keep these tests honest and non-flaky:

* readiness is signalled by the child itself (it writes its own PID to a file) and the test waits for
  that file with a deadline — nothing here sleeps for a fixed time hoping the child has started;
* process state is asserted against the real OS: ``_assert_reaped`` requires the platform's "this PID
  is gone" answer (POSIX signal 0, which a zombie still answers, so it also catches "killed but not
  waited for"), and it is asserted as soon as the awaiting code returns, with no polling that could
  forgive a slow cleanup. The one exception is the test that breaks the reap wait on purpose: there
  the reaping is left to the child watcher, so that test polls with a deadline instead. The
  ``tracked_pids`` fixture kills every PID a test registered when the test ends, so a red test cannot
  leave a live child behind; the children also exit on their own after 30s as a second net.

Platform scope: the tests run everywhere, but the OS-level probes are platform-specific and only the
POSIX ones are exercised by CI (Linux). The two descendant tests are skipped off POSIX because the
guarantee they check is POSIX-only (see `docs/reference.md`), and the Windows probe branches in
``_force_kill`` / ``_assert_reaped`` follow the documented Windows behaviour of ``os.kill`` (a
non-console signal opens the process and terminates it) without being able to verify it here.
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


def _force_kill(pid: int) -> None:
    """Terminate a child we no longer hold a handle for, using what each platform offers.

    POSIX: ``SIGKILL``. Windows: ``os.kill(pid, 0)`` *is* the terminate call there (a non-console
    signal maps to ``TerminateProcess`` with that exit code) — the Windows branch is not exercised by
    this project's Linux-only CI.
    """
    if os.name == "posix":
        os.kill(pid, signal.SIGKILL)
    else:  # pragma: no cover - Windows has no ``signal.SIGKILL``; see the docstring
        os.kill(pid, 0)


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
            _force_kill(pid)


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


def _process_state(pid: int) -> str:
    """Best-effort OS view of a PID, used only to say *why* a reaping assertion failed.

    Linux ``/proc`` reports a zombie as ``state=Z`` and a live process as ``state=S``/``R``; elsewhere
    the field is unavailable and the message says so.
    """
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split(") ", 1)[1].split()
    except (OSError, IndexError):
        return "state unknown"
    return f"state={fields[0]} ppid={fields[1]}"


def _assert_reaped(pid: int) -> None:
    """The observable guarantee: the OS no longer knows this PID.

    POSIX: ``os.kill(pid, 0)`` succeeds for a running process *and* for a zombie nobody has waited
    for, so requiring ``ProcessLookupError`` fails for both "still running" and "killed but not
    reaped". Windows: a non-console signal is not a liveness probe — ``os.kill`` opens the process and
    terminates it — so a live PID makes the call succeed (and disposes of the leak) while a dead one
    raises ``OSError``, which is what is required there. That branch is not exercised by CI.
    """
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        raise AssertionError(f"PID {pid} is still known to the OS ({_process_state(pid)})")
    with pytest.raises(OSError):  # pragma: no cover - Windows: see the docstring
        os.kill(pid, 0)


async def _await_cancellation(task: asyncio.Task[object]) -> None:
    """Await a cancelled task under a hard deadline, so a hung cleanup fails instead of blocking pytest."""
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 30.0)


async def _wait_until_gone(pid: int, *, timeout_s: float = 5.0) -> None:
    """Poll with a deadline for a killed child to disappear from the OS's view.

    Used where the *reap wait* itself is broken on purpose, so the reaping is left to the child
    watcher; everywhere else the reap is awaited and asserted immediately with ``_assert_reaped``.
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


def _pid_is_dead(pid: int) -> bool:
    """True once the PID is gone, or (where ``/proc`` says so) a zombie: dead, however reaped.

    A descendant is nobody's child in this process, so its zombie is reaped by init, at a time this
    test does not control; on Linux the process state can tell a zombie from a live process without
    waiting for that. Elsewhere the PID has to disappear.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return _process_state(pid).startswith("state=Z")


async def _wait_until_dead(pid: int, *, timeout_s: float = 5.0) -> None:
    """Wait with a deadline for a killed *descendant* to stop running.

    Descendants are signalled by the process-group kill but reaped by init, so this is the strongest
    observation available for them: gone, or a zombie that can never run again. The deadline is what
    still makes it a kill test — a descendant that survived cleanup sleeps for 30s, far past it.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if _pid_is_dead(pid):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"PID {pid} survived its process-group kill for {timeout_s}s ({_process_state(pid)})")


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


def test_cancellation_during_process_creation_still_kills_and_reaps_the_child(
    tmp_path: Path,
    child_script: Path,
    tracked_pids: Callable[..., None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The OS child exists before ``create_subprocess_exec`` has returned a handle to ``shell_run``,
    and a cancellation delivered in that window must not abandon it.

    The window is deliberately widened rather than raced: the wrapped ``loop.subprocess_exec`` holds
    the real result until the test releases it, so the child is created, running and past its own
    readiness signal while the caller is still parked inside process creation. Cancelling there is
    exactly the case where no frame of the old code held a handle, so nothing could clean it up. The
    creation must not be cancelled either — the fix keeps waiting for the handle, kills and reaps the
    child, and only then lets the ``CancelledError`` surface.

    (The widening is an injection on purpose: in CPython 3.11/3.12 the stock implementation closes the
    transport when its own internal wait is cancelled, but that is an implementation detail of
    ``_make_subprocess_transport``, not something ``shell_run`` should have to rely on — and anything
    that yields in that window, an event-loop wrapper or a custom policy, leaks without it.)
    """
    pid_file = tmp_path / "spawn_window.pid"
    in_spawn_window = asyncio.Event()
    release_spawn = asyncio.Event()
    real_subprocess_exec = asyncio.BaseEventLoop.subprocess_exec

    async def gated_subprocess_exec(
        loop: asyncio.BaseEventLoop, *args: object, **kwargs: object
    ) -> object:
        created = await real_subprocess_exec(loop, *args, **kwargs)
        # The child now exists; the caller is still inside create_subprocess_exec().
        in_spawn_window.set()
        await release_spawn.wait()
        return created

    monkeypatch.setattr(asyncio.BaseEventLoop, "subprocess_exec", gated_subprocess_exec)

    async def scenario() -> int:
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None)
        task = asyncio.ensure_future(spec(None, None))
        try:
            await asyncio.wait_for(in_spawn_window.wait(), 10.0)  # the OS child exists *now*
            pid = await _wait_for_pid(pid_file)  # ... and is running, not merely spawned
            tracked_pids(pid)
            task.cancel()  # cancels the caller while it is still waiting for the Process object
            release_spawn.set()  # let creation return, so ownership of the child can be taken
            await _await_cancellation(task)
        finally:
            release_spawn.set()  # never leave the injected spawn window closed
            await _cancel_quietly(task)
        return pid

    _assert_reaped(run(scenario()))


def test_a_cancellation_is_not_parked_behind_an_unresponsive_creation(
    tmp_path: Path,
    child_script: Path,
    tracked_pids: Callable[..., None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other side of the spawn window: a creation that never returns must not hold the cancellation.

    The wrapped ``loop.subprocess_exec`` creates the child and then never returns, and the cleanup
    budget is shortened for the test, so the task has to surface ``CancelledError`` instead of waiting
    for a handle that never arrives. A child that exists at that point cannot be owned by anybody in
    the library — the test takes back the abandoned transport it injected (which kills and reaps that
    child) and records the PID for teardown, which is exactly the trade the bounded wait makes: a
    possible orphan instead of a task that never finishes.
    """
    pid_file = tmp_path / "stuck_creation.pid"
    entered = asyncio.Event()
    release = asyncio.Event()
    abandoned: list[object] = []
    real_subprocess_exec = asyncio.BaseEventLoop.subprocess_exec

    async def stuck_subprocess_exec(
        loop: asyncio.BaseEventLoop, *args: object, **kwargs: object
    ) -> object:
        created = await real_subprocess_exec(loop, *args, **kwargs)
        abandoned.append(created)  # the handle the library is about to give up on
        entered.set()
        await release.wait()  # never set while the assertion runs
        return created

    monkeypatch.setattr(asyncio.BaseEventLoop, "subprocess_exec", stuck_subprocess_exec)
    monkeypatch.setattr("pyattacker.tasks._CLEANUP_TIMEOUT_S", 0.2)

    async def scenario() -> None:
        spec = shell_run(_child_command(child_script, pid_file), timeout_s=None)
        task = asyncio.ensure_future(spec(None, None))
        try:
            await asyncio.wait_for(entered.wait(), 10.0)  # the child exists, the handle is pending
            tracked_pids(await _wait_for_pid(pid_file))
            task.cancel()
            # `asyncio.wait` never cancels the task it watches, so a missing bound fails the
            # assertion below instead of hanging this test (or the suite) forever.
            done, _pending = await asyncio.wait({task}, timeout=3.0)
            assert task in done, "the cancellation was parked behind a creation that never returned"
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()  # let the injected creation finish so nothing is left pending
            await _cancel_quietly(task)
            for transport, *_ in abandoned:  # test-owned cleanup of what the library had to drop
                transport.close()  # type: ignore[attr-defined]

    run(scenario())


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

    The three failures are injected at the seams the cleanup helpers use — the kill seam is
    platform-specific, matching the branch ``_signal_process`` takes (``os.killpg`` on POSIX,
    ``Process.kill`` for the direct child elsewhere). The child is deliberately silent and the broken
    reader is armed only after the read task is parked inside the real ``read()``: a child that wrote
    to the pipe would let ``communicate()`` re-enter the patched call on the normal path, which would
    fail the run before cleanup and prove nothing.
    """
    pid_file = tmp_path / "broken_cleanup.pid"
    code = "import os, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(30)"
    armed = False
    real_read = asyncio.StreamReader.read

    async def gated_read(stream: asyncio.StreamReader, *args: object) -> bytes:
        if armed:
            raise ConnectionResetError("reader left in a bad state by the cancelled communicate()")
        return await real_read(stream, *args)

    async def broken_wait(*args: object, **kwargs: object) -> int:
        raise ConnectionResetError("child watcher never reported the exit")

    monkeypatch.setattr(asyncio.StreamReader, "read", gated_read)
    monkeypatch.setattr(asyncio.subprocess.Process, "wait", broken_wait)
    if os.name == "posix":  # the signal seam `_signal_process` uses there: the whole process group
        real_killpg = os.killpg

        def broken_killpg(pid: int, sig: int) -> None:
            real_killpg(pid, sig)
            raise RuntimeError("signal delivered, then the call failed")

        monkeypatch.setattr(os, "killpg", broken_killpg)
    else:  # pragma: no cover - the seam `_signal_process` uses off POSIX: the direct child
        real_kill = asyncio.subprocess.Process.kill

        def broken_kill(proc: asyncio.subprocess.Process) -> None:
            real_kill(proc)
            raise RuntimeError("signal delivered, then the call failed")

        monkeypatch.setattr(asyncio.subprocess.Process, "kill", broken_kill)

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
    group, the descendant would keep running (and hold the pipe) after the shell was killed. The
    descendant is not this process's child, so it is observed as dead-within-a-deadline rather than
    strictly reaped (see ``_wait_until_dead``); the shell, which the task does own, is asserted reaped."""
    shell_pid_file = tmp_path / "shell.pid"
    child_pid_file = tmp_path / "shell_child.pid"
    command = (
        f"echo $$ > {shlex.quote(str(shell_pid_file))}; "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(child_script))} "
        f"{shlex.quote(str(child_pid_file))} & wait"
    )

    async def scenario() -> int:
        task = asyncio.ensure_future(shell_run(command, timeout_s=None)(None, None))
        try:
            shell_pid = await _wait_for_pid(shell_pid_file)  # the direct child: the shell
            child_pid = await _wait_for_pid(child_pid_file)  # the descendant: up and running
            tracked_pids(shell_pid, child_pid)
            assert shell_pid != child_pid
            task.cancel()
            await _await_cancellation(task)
            await _wait_until_dead(child_pid)
        finally:
            await _cancel_quietly(task)
        return shell_pid

    _assert_reaped(run(scenario()))


@pytest.mark.skipif(
    os.name != "posix",
    reason="process-group signalling is POSIX-only: Windows has no os.killpg and asyncio cannot "
    "signal a group there, so the descendant guarantee is not offered (see docs/reference.md)",
)
def test_argv_mode_kills_descendants_too(
    tmp_path: Path, child_script: Path, tracked_pids: Callable[..., None]
) -> None:
    """The argv form starts the program directly, but that program may spawn children of its own;
    on POSIX the same group signal reaches them, which is what the documented guarantee claims. The
    spawned descendant is observed as dead-within-a-deadline, the argv program itself — the child this
    task owns and reaps — as strictly reaped."""
    parent_pid_file = tmp_path / "argv_parent.pid"
    child_pid_file = tmp_path / "argv_child.pid"
    command = _child_command(child_script, parent_pid_file, spawn=child_pid_file)

    async def scenario() -> int:
        task = asyncio.ensure_future(shell_run(command, timeout_s=None)(None, None))
        try:
            parent_pid = await _wait_for_pid(parent_pid_file)
            child_pid = await _wait_for_pid(child_pid_file)  # the descendant: up and running
            tracked_pids(parent_pid, child_pid)
            assert parent_pid != child_pid
            task.cancel()
            await _await_cancellation(task)
            await _wait_until_dead(child_pid)
        finally:
            await _cancel_quietly(task)
        return parent_pid

    _assert_reaped(run(scenario()))
