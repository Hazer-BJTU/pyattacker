"""README and CLI-reference examples are executable, not decoration.

`docs/tutorial.md` already runs every block marked ``# tutorial/<name>.py`` (see
``tests/test_tutorial.py``). This module applies the same contract to the two documents a reader
copies from first, with the same kind of marker as the first line of the block:

* ``# example/<name>.py`` — a complete program. It is written to a temporary directory and executed
  with the current interpreter and ``cwd`` set to that directory, so an undefined name, a relative
  store path that assumes the checkout, or a network call fails CI.
* ``# example/<name>.yaml`` — a complete config. It is written out and passed to
  ``pyattacker validate`` through the CLI entry point, which is exactly what the document tells the
  reader to run. That is what catches copy-paste breakage such as a bare ``on:`` retry key (YAML 1.1
  parses it as boolean ``true``).

Blocks without a marker are fragments on purpose — a bash transcript, the "where your own client
goes" snippet — and are ignored.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pyattacker.cli import main

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = {
    "README.md": ROOT / "README.md",
    "docs/cli.md": ROOT / "docs" / "cli.md",
}
BLOCK = re.compile(r"^```(?P<lang>python|yaml)\n(?P<body>.*?)^```$", re.MULTILINE | re.DOTALL)
MARKER = re.compile(r"^# example/(?P<name>[\w.\-]+\.(?:py|yaml))$")
TIMEOUT_S = 180


def _examples() -> list[tuple[str, str, str]]:
    """(document, name, body) for every marked block, in document order."""
    found: list[tuple[str, str, str]] = []
    for document, path in DOCUMENTS.items():
        for match in BLOCK.finditer(path.read_text(encoding="utf-8")):
            body = match.group("body")
            marker = MARKER.match(body.splitlines()[0]) if body.strip() else None
            if marker is None:
                continue
            name = marker.group("name")
            expected_lang = "python" if name.endswith(".py") else "yaml"
            assert match.group("lang") == expected_lang, f"{document}: {name} must be a {expected_lang} block"
            found.append((document, name, body))
    return found


EXAMPLES = _examples()
PYTHON_EXAMPLES = [item for item in EXAMPLES if item[1].endswith(".py")]
YAML_EXAMPLES = [item for item in EXAMPLES if item[1].endswith(".yaml")]


def test_the_key_examples_are_marked_for_execution():
    names = {name for _, name, _ in EXAMPLES}
    assert {"readme_quickstart.py", "readme_qa_eval.yaml", "cli_config_reference.yaml"} <= names, names
    assert len(names) == len(EXAMPLES), f"duplicate example names: {sorted(names)}"


def test_marked_blocks_start_with_their_marker_and_are_complete_programs():
    for document, name, body in PYTHON_EXAMPLES:
        assert body.startswith(f"# example/{name}"), name
        assert 'if __name__ == "__main__"' not in body, f"{document}: {name} must run top to bottom"
        elided = [line for line in body.splitlines() if line.strip() in ("...", "# ...")]
        assert not elided, f"{document}: {name} cannot be executed with elided code in it"


@pytest.mark.parametrize(
    ("document", "name", "body"), PYTHON_EXAMPLES, ids=[f"{document}:{name}" for document, name, _ in PYTHON_EXAMPLES]
)
def test_marked_python_examples_run_offline(tmp_path, document, name, body):
    workdir = tmp_path / name.removesuffix(".py")
    workdir.mkdir(parents=True, exist_ok=True)
    script = workdir / name
    script.write_text(body, encoding="utf-8")
    # As in tests/test_tutorial.py: point the child at src/ so this tests the checkout, and run in
    # the example's own directory so `runs/qa.db` behaves exactly as it does for a reader.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), os.environ.get("PYTHONPATH", "")])}

    proc = subprocess.run(
        [sys.executable, str(script)], cwd=workdir, capture_output=True, text=True, timeout=TIMEOUT_S, env=env
    )

    assert proc.returncode == 0, f"{document} {name} exited {proc.returncode}\n{proc.stdout}\n{proc.stderr}"
    assert proc.stdout.strip(), f"{document} {name} printed nothing, so its quoted output cannot be right"


@pytest.mark.requires_yaml
@pytest.mark.parametrize(
    ("document", "name", "body"), YAML_EXAMPLES, ids=[f"{document}:{name}" for document, name, _ in YAML_EXAMPLES]
)
def test_marked_yaml_examples_pass_validate(tmp_path, capsys, document, name, body):
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    capsys.readouterr()  # discard anything earlier in the session

    rc = main(["validate", "-c", str(path)])

    captured = capsys.readouterr()
    assert rc == 0, f"{document} {name} did not validate (exit {rc})\n{captured.err}"
    assert captured.out.strip().startswith("{"), f"{document} {name}: validate printed no config summary"
