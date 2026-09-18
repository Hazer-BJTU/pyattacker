"""Bilingual documentation: the Chinese translation is verified, not trusted.

``docs/tutorial.md``, ``README.md`` and ``docs/cli.md`` are executable (see ``test_tutorial.py`` and
``test_docs_examples.py``); the Chinese documents mirror them. A mirror is only worth having if it
cannot drift silently, so this module enforces the contract the translation was written under:

* every English document listed here has a Chinese counterpart, and both sides carry a language
  switcher that links to the other one;
* the fenced code blocks are byte-identical, in the same order with the same info strings — so the
  tests that execute the English blocks cover the Chinese documents too, and a translation can never
  fork the code it explains;
* headings match one-for-one in level, because the Chinese anchors are derived from the Chinese
  headings and links between the documents were rewritten positionally;
* every relative link and every in-document anchor in *both* languages resolves, so the bilingual
  switch does not ship dead links.

The check is deliberately about structure, not style: prose quality is a review question, while
"the Chinese README still runs the same program" is a machine question.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# English document -> Chinese counterpart. The Chinese documents live in docs/zh-CN/ so that a
# reader who lands on the English tree never has to guess which files are translations.
PAIRS: dict[str, str] = {
    "README.md": "README.zh-CN.md",
    "docs/tutorial.md": "docs/zh-CN/tutorial.md",
    "docs/reference.md": "docs/zh-CN/reference.md",
    "docs/design.md": "docs/zh-CN/design.md",
    "docs/cli.md": "docs/zh-CN/cli.md",
    "docs/benchmark.md": "docs/zh-CN/benchmark.md",
    "docs/backward.md": "docs/zh-CN/backward.md",
    "docs/releasing.md": "docs/zh-CN/releasing.md",
    "examples/llm_eval/README.md": "examples/llm_eval/README.zh-CN.md",
    "examples/plugin_package/README.md": "examples/plugin_package/README.zh-CN.md",
}

BLOB = "https://github.com/Hazer-BJTU/pyattacker/blob/main/"
CJK = re.compile(r"[\u4e00-\u9fff]")
LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
MARKER = re.compile(r"^# (?:tutorial|example)/(?P<name>[\w.\-]+\.(?:py|yaml))$")


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _fence_mask(lines: list[str]) -> list[bool]:
    """True for every line inside a fenced block, fence markers included."""
    inside = False
    mask = []
    for line in lines:
        if line.lstrip().startswith("```"):
            mask.append(True)
            inside = not inside
        else:
            mask.append(inside)
    assert not inside, "unbalanced code fence"
    return mask


def _code_blocks(relative: str) -> list[str]:
    lines = _read(relative).splitlines()
    mask = _fence_mask(lines)
    blocks: list[str] = []
    current: list[str] | None = None
    for index, line in enumerate(lines):
        if not mask[index]:
            continue
        if line.lstrip().startswith("```"):
            if current is None:
                current = [line]
            else:
                current.append(line)
                blocks.append("\n".join(current))
                current = None
        elif current is not None:
            current.append(line)
    return blocks


def _headings(relative: str) -> list[tuple[int, str]]:
    lines = _read(relative).splitlines()
    mask = _fence_mask(lines)
    return [
        (len(match.group(1)), match.group(2).strip())
        for index, line in enumerate(lines)
        if not mask[index] and (match := re.match(r"^(#{1,6})\s+(.*)$", line))
    ]


def _slugify(text: str) -> str:
    """GitHub's heading anchor, near enough: markdown stripped, punctuation dropped, spaces hyphens."""
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    return "".join(
        char if char.isalnum() or char in "-_" else "-" if char.isspace() else ""
        for char in text.lower()
    )


def _anchors(relative: str) -> set[str]:
    seen: dict[str, int] = {}
    anchors = set()
    for _level, text in _headings(relative):
        base = _slugify(text)
        count = seen.get(base, 0)
        seen[base] = count + 1
        anchors.add(base if count == 0 else f"{base}-{count}")
    return anchors


def _links(relative: str) -> list[tuple[int, str]]:
    lines = _read(relative).splitlines()
    mask = _fence_mask(lines)
    found = []
    for index, line in enumerate(lines):
        if not mask[index]:
            found += [(index + 1, match.group(1)) for match in LINK.finditer(line)]
    return found


def _resolve(target: str, document: str) -> tuple[str, str] | None:
    """(repository-relative path, fragment) for a link this repository can resolve, else None."""
    path, _, fragment = target.partition("#")
    if path.startswith("http"):
        return (path[len(BLOB) :], fragment) if path.startswith(BLOB) else None
    if not path:
        return document, fragment
    return posixpath.normpath(posixpath.join(posixpath.dirname(document), path)), fragment


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_chinese_counterpart_exists(english: str, chinese: str):
    assert (ROOT / chinese).is_file(), f"{chinese} is missing; {english} has no Chinese counterpart"


def test_no_orphan_chinese_documents():
    found = {path.relative_to(ROOT).as_posix() for path in ROOT.glob("docs/zh-CN/*.md")}
    found |= {path.relative_to(ROOT).as_posix() for path in ROOT.glob("*.zh-CN.md")}
    found |= {path.relative_to(ROOT).as_posix() for path in ROOT.glob("examples/*/README.zh-CN.md")}
    assert found == set(PAIRS.values()), "a Chinese document is not declared in PAIRS"


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_language_switchers_link_both_ways(english: str, chinese: str):
    forward = posixpath.relpath(chinese, posixpath.dirname(english) or ".")
    back = posixpath.relpath(english, posixpath.dirname(chinese) or ".")
    english_targets = [target for _, target in _links(english)]
    chinese_targets = [target for _, target in _links(chinese)]
    assert forward in english_targets, f"{english} does not link to its Chinese counterpart ({forward})"
    assert back in chinese_targets, f"{chinese} does not link back to {english} ({back})"


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_code_blocks_are_identical(english: str, chinese: str):
    """The code is the one thing a translation must not touch.

    Keeping the fences byte-identical is what lets the executable-documentation tests cover both
    languages with one run: the Chinese block *is* the English block.
    """
    source, translated = _code_blocks(english), _code_blocks(chinese)
    assert len(source) == len(translated), (
        f"{chinese} has {len(translated)} code blocks, {english} has {len(source)}"
    )
    for index, (a, b) in enumerate(zip(source, translated, strict=True)):
        assert a == b, f"{chinese}: code block {index + 1} differs from {english}\n--- {english}\n{a}\n--- {chinese}\n{b}"


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_heading_levels_match(english: str, chinese: str):
    source = [level for level, _ in _headings(english)]
    translated = [level for level, _ in _headings(chinese)]
    assert source == translated, f"{chinese}: heading levels do not follow {english}"


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_runnable_block_markers_match(english: str, chinese: str):
    def markers(document: str) -> list[str]:
        return [
            line
            for block in _code_blocks(document)
            for line in block.splitlines()[:1]
            if MARKER.match(line)
        ]

    assert markers(english) == markers(chinese), f"{chinese}: runnable block markers differ"


@pytest.mark.parametrize("english,chinese", sorted(PAIRS.items()))
def test_translation_is_not_a_stub(english: str, chinese: str):
    # Structural checks would pass on a document whose prose was never written; a floor on Chinese
    # characters catches an empty or near-empty translation without pretending to judge quality. The
    # smallest document in PAIRS (the plugin example, 23 lines) carries 174, so the floor is a
    # tripwire for "nobody translated this", not a quality bar.
    count = len(CJK.findall(_read(chinese)))
    assert count >= 100, f"{chinese} (the translation of {english}) has only {count} Chinese characters"


@pytest.mark.parametrize("document", sorted([*PAIRS, *PAIRS.values()]))
def test_repository_links_resolve(document: str):
    """Every repository-internal link — path and anchor — resolves, in both languages."""
    broken = []
    for line, target in _links(document):
        resolved = _resolve(target, document)
        if resolved is None:
            continue
        path, fragment = resolved
        if not (ROOT / path).exists():
            broken.append(f"{document}:{line}: missing {path} (from {target})")
            continue
        if fragment and (ROOT / path).is_file() and fragment not in _anchors(path):
            broken.append(f"{document}:{line}: missing anchor #{fragment} in {path} (from {target})")
    assert not broken, "\n".join(broken)
