"""The tutorial is executable: every block marked ``# tutorial/<name>.py`` is run for real.

`docs/tutorial.md` is a teaching document, which makes it exactly the kind of file that rots silently.
The marker comment is the contract: a block that carries it is a complete program, so this test extracts
those blocks, runs each one in its own temporary directory with the current interpreter, and fails if any
of them does not exit cleanly. Blocks without the marker are fragments and are ignored on purpose.

The programs are independent, so they run concurrently; four at a time keeps the suite fast without
turning the run into a CPU stampede.
"""

from __future__ import annotations

import concurrent.futures
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TUTORIAL = ROOT / "docs" / "tutorial.md"
BLOCK = re.compile(r"^```python\n(?P<body>.*?)^```$", re.MULTILINE | re.DOTALL)
MARKER = re.compile(r"^# tutorial/(?P<name>[\w.\-]+\.py)$")
TIMEOUT_S = 180


def _runnable_blocks() -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for match in BLOCK.finditer(TUTORIAL.read_text(encoding="utf-8")):
        body = match.group("body")
        marker = MARKER.match(body.splitlines()[0]) if body.strip() else None
        if marker is not None:
            found.append((marker.group("name"), body))
    return found


BLOCKS = _runnable_blocks()


def test_tutorial_has_no_uninjected_placeholders():
    assert "<!-- code:" not in TUTORIAL.read_text(encoding="utf-8")


def test_tutorial_exposes_runnable_steps():
    names = [name for name, _ in BLOCKS]
    assert len(names) >= 12, f"expected the tutorial's step programs, found {names}"
    assert names == sorted(names), f"tutorial steps should be in file order, got {names}"
    assert len(set(names)) == len(names)


def test_every_runnable_block_is_a_complete_program():
    for name, body in BLOCKS:
        assert body.startswith(f"# tutorial/{name}"), name
        assert 'if __name__ == "__main__"' not in body, (
            f"{name}: a step program must run top to bottom, not only under __main__"
        )
        elided = [line for line in body.splitlines() if line.strip() in ("...", "# ...")]
        assert not elided, f"{name}: elided code cannot be executed"


def test_tutorial_programs_run(tmp_path: Path):
    # The step programs import pyattacker; point the child at src/ as well so the test verifies the
    # checkout rather than depending on how the parent process happened to be installed.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), os.environ.get("PYTHONPATH", "")])}

    def _run(item: tuple[str, str]) -> tuple[str, subprocess.CompletedProcess[str]]:
        name, body = item
        workdir = tmp_path / name.removesuffix(".py")
        workdir.mkdir(parents=True, exist_ok=True)
        script = workdir / name
        script.write_text(body, encoding="utf-8")
        # cwd is the step's own directory: the relative "runs/..." paths in the tutorial must behave
        # exactly as they do for a reader who copies the block into a file.
        proc = subprocess.run(
            [sys.executable, str(script)],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            env=env,
        )
        return name, proc

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(_run, BLOCKS))

    failures = [
        f"{name}: exit {proc.returncode}\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        for name, proc in results
        if proc.returncode != 0
    ]
    assert not failures, "\n\n".join(failures)
    empty = [name for name, proc in results if not proc.stdout.strip()]
    assert not empty, f"these steps printed nothing, so the tutorial's output blocks cannot be right: {empty}"
