"""The documented facts that have a source of truth in the code are asserted, not trusted.

The other documentation tests cover what is checkable as *code and structure*:
``tests/test_tutorial.py`` and ``tests/test_docs_examples.py`` execute every marked block, and
``tests/test_docs_i18n.py`` holds the Chinese documents to the English ones. None of them can see prose that
states a fact the repository already knows somewhere else — a version header left behind by a release, a
"fourteen runnable steps" claim after three steps were added, or a monitoring route named in the reference
that the server does not serve.

Issue #60 was exactly that drift, and the answer is to give each of those claims a source of truth:

* the version in the header of both design documents, against ``pyproject.toml``;
* every "N runnable steps" claim, against the number of ``# tutorial/<name>.py`` blocks in the tutorial;
* every route in the monitoring section's endpoint table, against the routes ``StatsServer`` actually
  answers (the table, not the prose: a sentence saying there is no ``/artifacts`` endpoint is not an
  advertisement for one).

The checks are deliberately narrow, and each fails only when a number or a name genuinely disagrees with
the code. Whether prose is *accurate* remains a review question — there is no test for "the warning
describes the right thing as exposed", which is why that sentence now names the routes it means.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pyattacker import MemoryStore
from pyattacker.server import StatsServer

ROOT = Path(__file__).resolve().parents[1]
TUTORIAL = ROOT / "docs" / "tutorial.md"
DESIGN_DOCS = ["docs/design.md", "docs/zh-CN/design.md"]
REFERENCE_DOCS = ["docs/reference.md", "docs/zh-CN/reference.md"]
STEP_CLAIM_DOCS = [*REFERENCE_DOCS, "README.md", "README.zh-CN.md"]

# The tutorial programs are the steps a reader can run; `## Step 0` is an install check with no program.
TUTORIAL_BLOCK = re.compile(r"^# tutorial/[\w.\-]+\.py$", re.MULTILINE)
# "seventeen runnable steps" / "17 个可运行步骤"
EN_STEP_CLAIM = re.compile(r"(\w+)[ -]runnable steps")
ZH_STEP_CLAIM = re.compile(r"(\d+)\s*个可运行步骤")
NUMBER_WORDS = {
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
    "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
}
# The version a design document states about itself, compared for equality (never as a substring):
# "> Version: 0.2.0 ..." / the Chinese header line. The fullwidth colon is escaped because ruff flags it
# in a literal (RUF001). `\d+(?:\.\d+)*` also rejects "0.2.0.1" and "pre-0.2.0".
VERSION_CLAIM = re.compile(r">\s*(?:Version:|版本\uff1a)\s*v?(\d+(?:\.\d+)*)")
# Routes are read from the monitoring section's endpoint table, not from prose: `\`/x\`` appearing in a
# sentence ("there is no `/artifacts` endpoint") is not an advertisement, and a parser that cannot tell the
# difference would demand a route that does not exist.
ROUTE = re.compile(r"`(/[A-Za-z0-9_.\-/]*)`")
ROUTE_TABLE_ROW = re.compile(r"^\|.*`/")


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _tutorial_step_programs() -> int:
    return len(TUTORIAL_BLOCK.findall(TUTORIAL.read_text(encoding="utf-8")))


def test_the_version_the_design_documents_state_is_the_packaged_one():
    """A release bumps ``pyproject.toml``; the design header is what everyone forgets to bump with it.

    The documented version is extracted and compared for **equality** — a substring test would accept
    ``10.2.0`` or ``0.2.0.1`` for ``0.2.0``, which is exactly the drift this guard exists to catch.
    """
    packaged = re.search(r'^version = "([^"]+)"', _read("pyproject.toml"), re.MULTILINE)
    assert packaged, "pyproject.toml has no version"
    expected = packaged.group(1)

    for document in DESIGN_DOCS:
        header = [
            line for line in _read(document).splitlines()[:12]
            if line.startswith("> Version:") or line.startswith("> 版本\uff1a")
        ]
        assert len(header) == 1, f"{document}: expected exactly one version line, found {header}"
        stated = VERSION_CLAIM.search(header[0])
        assert stated, f"{document}: no version number in {header[0]!r}"
        assert stated.group(1) == expected, f"{document} states {stated.group(1)!r}, package is {expected!r}"


@pytest.mark.parametrize("document", STEP_CLAIM_DOCS)
def test_every_runnable_step_claim_counts_the_tutorial_programs(document):
    """The tutorial grew from fourteen steps to seventeen; the count is in four documents."""
    body = _read(document)
    claims = EN_STEP_CLAIM.findall(body) + ZH_STEP_CLAIM.findall(body)
    assert claims, f"{document}: no 'runnable steps' claim found (did the wording move?)"

    actual = _tutorial_step_programs()
    assert actual > 0, "no `# tutorial/<name>.py` blocks found — the marker regex has drifted"
    for claim in claims:
        asserted = NUMBER_WORDS.get(claim.lower()) if claim.isalpha() else int(claim)
        assert asserted == actual, f"{document}: claims {claim!r} runnable steps, the tutorial has {actual}"


@pytest.mark.parametrize("document", REFERENCE_DOCS)
def test_every_monitoring_route_the_reference_advertises_is_actually_served(document):
    """Issue #60: the reference promised payloads on routes that did not exist.

    Only the monitoring section's endpoint table counts as an advertisement. Route-looking code spans in the
    prose around it do not: "there is no `/artifacts` endpoint" must not make the guard demand one, which is
    why the parser reads table rows instead of every backticked ``/…`` in the section.
    """
    body = _read(document)
    heading = re.compile(r"^## (?:Monitoring|监控)\s*$", re.MULTILINE)
    match = heading.search(body)
    assert match, f"{document}: no monitoring section"
    section = body[match.end():].split("\n## ", 1)[0]
    table = [line for line in section.splitlines() if ROUTE_TABLE_ROW.match(line)]
    assert table, f"{document}: no route table found in the monitoring section"
    documented = set(ROUTE.findall("\n".join(table)))
    assert documented, f"{document}: the endpoint table names no routes"

    server = StatsServer(MemoryStore(), port=0)
    try:
        for path in sorted(documented):
            status, _ = server.payload(path, {})
            assert status == 200, f"{document} advertises {path!r}, which the server answers {status} for"
    finally:
        server.stop()
