"""Pipeline: linear composition, build-time type validation, content-addressed identity and seed expansion."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

import pytest

from pyattacker import Retrying, pipeline, task
from pyattacker.errors import PipelineBuildError


@dataclass
class Question:
    qid: str


@dataclass
class Answer:
    qid: str
    text: str


@dataclass
class ChildAnswer(Answer):
    confident: bool = True


@dataclass
class Other:
    nope: int


@task("fetch")
def fetch(seed: dict) -> Question:
    return Question(qid=seed["qid"])


@task("ask")
def ask(question: Question) -> Answer:
    return Answer(qid=question.qid, text="...")


@task("ask_child")
def ask_child(question: Question) -> ChildAnswer:
    return ChildAnswer(qid=question.qid, text="...", confident=True)


@task("consume_base")
def consume_base(answer: Answer) -> Answer:
    return answer


@task("consume_other")
def consume_other(other: Other) -> Other:
    return other


@task("no_annotations")
def no_annotations(value):
    return value


def test_chain_is_linear_and_ordered():
    chain = fetch | ask | consume_base
    assert [t.name for t in chain] == ["fetch", "ask", "consume_base"]
    tpl = pipeline("qa", chain, tags={"bench": "demo"})
    assert tpl.task_names() == ["fetch", "ask", "consume_base"]
    assert tpl.tags == {"bench": "demo"}
    assert tpl.n_tasks == 3


def test_mismatched_artifact_type_fails_at_build_time():
    with pytest.raises(PipelineBuildError) as excinfo:
        pipeline("bad", fetch | consume_other)
    assert "artifact types do not chain" in str(excinfo.value)
    assert "Question" in str(excinfo.value)


def test_subclass_output_is_accepted_by_base_input():
    tpl = pipeline("sub", fetch | ask_child | consume_base)
    assert tpl.n_tasks == 3


def test_unannotated_tasks_are_permissive():
    tpl = pipeline("loose", fetch | no_annotations | consume_other)
    assert tpl.n_tasks == 3


def test_empty_pipeline_is_rejected():
    with pytest.raises(PipelineBuildError):
        pipeline("empty")


def test_spec_digest_tracks_task_code():
    base = pipeline("d1", fetch | ask)
    mutated = dataclasses.replace(ask, code_digest="changed")
    changed = pipeline("d2", fetch | mutated)
    assert base.spec_digest != changed.spec_digest

    base_free = pipeline("d3", fetch | ask, include_code=False)
    changed_free = pipeline("d4", fetch | mutated, include_code=False)
    assert base_free.spec_digest == changed_free.spec_digest


def test_map_is_content_addressed_and_expands_repeats():
    tpl = pipeline("m", fetch | ask)
    seeds = [{"qid": "q1"}, {"qid": "q2"}]

    single = list(tpl.map(seeds))
    assert len(single) == 2
    assert single[0].pipeline_id != single[1].pipeline_id
    # Expanding the same seed again → the exact same pipeline_id (the basis for idempotency / resume)
    assert [s.pipeline_id for s in tpl.map(seeds)] == [s.pipeline_id for s in single]

    repeated = list(tpl.map(seeds, repeats=3))
    assert len(repeated) == 6
    first_seed = [s for s in repeated if s.seed["qid"] == "q1"]
    assert len({s.pipeline_id for s in first_seed}) == 3
    assert len({s.seed_digest for s in first_seed}) == 1  # one seed, three independent samples


def test_key_of_provides_explicit_stable_ids():
    tpl = pipeline("k", fetch | ask)
    specs = list(tpl.map([{"qid": "q7"}], key_of=lambda seed: f"k:{seed['qid']}"))
    assert specs[0].pipeline_id == "k:q7"

    multi = list(tpl.map([{"qid": "q7"}], repeats=2, key_of=lambda seed: f"k:{seed['qid']}"))
    assert [s.pipeline_id for s in multi] == ["k:q7#0", "k:q7#1"]


def test_seed_digest_is_stable_for_dataclasses():
    tpl = pipeline("s", fetch | ask)
    a = tpl.bind(Question(qid="x"))
    b = tpl.bind(Question(qid="x"))
    c = tpl.bind(Question(qid="y"))
    assert a.seed_digest == b.seed_digest
    assert a.pipeline_id == b.pipeline_id
    assert a.seed_digest != c.seed_digest


def test_retry_spec_is_part_of_task_fingerprint():
    strict = dataclasses.replace(ask, retry=Retrying(max_attempts=1))
    loose = dataclasses.replace(ask, retry=Retrying(max_attempts=5))
    assert pipeline("r1", fetch | strict).spec_digest != pipeline("r2", fetch | loose).spec_digest
