"""Sharding: spec parsing, key routing, dataset partitioning and per-shard store paths.

Sharding is deliberately a *pure function of the pipeline key*, so every expectation in this
module is deterministic: no retries, no timers, no randomness. The important contracts are
that a partition loses nothing and duplicates nothing, and that the same key always lands in
the same shard — in this process and in the next one.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap

import pytest

from pyattacker import ConfigError, SqliteStore, digest_of, pipeline
from pyattacker.cli import main
from pyattacker.merge import merge_reports
from pyattacker.shard import (
    describe_shard,
    in_shard,
    parse_shard,
    shard_env,
    shard_index,
    shard_paths,
    shard_specs,
    shard_store_path,
)
from pyattacker.tasks import flaky

TEMPLATE = pipeline("shard-demo", flaky(0))
SEEDS = [{"i": index} for index in range(25)]


# --------------------------------------------------------------------- parse_shard
def test_parse_shard_accepts_text_tuple_and_none():
    assert parse_shard("1/4") == (1, 4)
    assert parse_shard("0/1") == (0, 1)
    assert parse_shard(" 2/3 ") == (2, 3)
    assert parse_shard((1, 4)) == (1, 4)
    assert parse_shard(None) is None


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("4/4", "shard index must be in [0, 4), got 4"),
        ("1/1", "shard index must be in [0, 1), got 1"),
        ("-1/4", "shard index must be in [0, 4), got -1"),
        ("0/0", "shard count must be >= 1, got 0"),
        ("1/-2", "shard count must be >= 1, got -2"),
        ("x/y", "shard must look like 'index/count', got 'x/y'"),
        ("1/", "shard must look like 'index/count', got '1/'"),
        ("no-slash", "shard must look like 'index/count', got 'no-slash'"),
        ((3, 2), "shard index must be in [0, 2), got 3"),
    ],
)
def test_parse_shard_rejects_bad_specs(value, message):
    with pytest.raises(ConfigError, match=re.escape(message)):
        parse_shard(value)


# --------------------------------------------------------------------- shard_index
def test_shard_index_is_in_range_and_repeatable():
    keys = [f"key-{index}" for index in range(50)]
    for count in (1, 2, 3, 4, 7, 16):
        first = [shard_index(key, count) for key in keys]
        assert all(0 <= index < count for index in first)
        assert [shard_index(key, count) for key in keys] == first
        assert shard_index("key-0", count) == first[0]


def test_shard_index_rejects_count_below_one():
    with pytest.raises(ConfigError, match="shard count must be >= 1, got 0"):
        shard_index("key", 0)
    with pytest.raises(ConfigError, match="shard count must be >= 1, got -3"):
        shard_index("key", -3)


def test_shard_index_spreads_many_keys_evenly():
    keys = [f"key-{index}" for index in range(400)]
    counts = [0, 0, 0, 0]
    for key in keys:
        counts[shard_index(key, 4)] += 1

    assert sum(counts) == 400
    assert all(count > 0 for count in counts)
    assert min(counts) >= 80 and max(counts) <= 120  # 100 expected per shard, well within +-20%


def test_shard_index_is_content_addressed_and_stable_across_processes():
    keys = [f"key-{index}" for index in range(16)]
    expected = [shard_index(key, 4) for key in keys]

    # By construction the index is a prefix of the key's content digest: no process-local state
    # (PYTHONHASHSEED, dict order, ...) can influence it.
    assert expected == [int(digest_of(key)[:16], 16) % 4 for key in keys]

    # ... and an actual second process, with a pinned (but different) hash seed, agrees.
    code = (
        "import json, sys; from pyattacker.shard import shard_index; "
        "keys = json.loads(sys.argv[1]); print(json.dumps([shard_index(k, 4) for k in keys]))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code, json.dumps(keys)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PYTHONHASHSEED": "12345"},
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == expected


def test_in_shard_is_consistent_with_shard_index():
    keys = [f"key-{index}" for index in range(30)]
    for count in (1, 2, 5, 8):
        for key in keys:
            owner = shard_index(key, count)
            assert in_shard(key, owner, count)
            assert [in_shard(key, index, count) for index in range(count)] == [
                index == owner for index in range(count)
            ]
    assert [in_shard("key-0", 0, 1)] == [True]


# --------------------------------------------------------------------- shard_specs
def test_shard_specs_partitions_every_spec_exactly_once():
    specs = list(TEMPLATE.map(SEEDS))
    assert len(specs) == 25
    expected = {spec.pipeline_id for spec in specs}

    for count in (2, 3, 4, 7):
        assigned: list[str] = []
        for index in range(count):
            part = list(shard_specs(specs, index, count))
            assert part, f"shard {index}/{count} should own something for this fixed key set"
            assigned.extend(spec.pipeline_id for spec in part)

        assert len(assigned) == 25  # no loss
        assert len(set(assigned)) == 25  # no duplication
        assert set(assigned) == expected
        # every spec is routed to exactly one shard, and it is the one in_shard() names
        assert all(
            sum(1 for index in range(count) if in_shard(spec.pipeline_id, index, count)) == 1
            for spec in specs
        )


def test_shard_specs_count_at_most_one_yields_everything():
    specs = list(TEMPLATE.map(SEEDS))
    every_id = [spec.pipeline_id for spec in specs]

    assert [spec.pipeline_id for spec in shard_specs(specs, 0, 1)] == every_id
    assert [spec.pipeline_id for spec in shard_specs(specs, 0, 0)] == every_id  # count <= 1 means "no sharding"


# ------------------------------------------------------------------ store paths/env
def test_shard_store_path_derives_one_file_per_shard():
    assert shard_store_path("runs/x.db", 1, 4) == "runs/x.shard1of4.db"
    assert shard_store_path("runs/x.db", 0, 2) == "runs/x.shard0of2.db"
    assert shard_store_path("/abs/runs/qa.sqlite", 3, 4) == "/abs/runs/qa.shard3of4.sqlite"
    assert shard_store_path("runs/x", 0, 2) == "runs/x.shard0of2.db"  # no extension -> .db
    assert shard_store_path("runs/x.db", 0, 1) == "runs/x.db"  # count <= 1 -> base unchanged
    assert shard_store_path("runs/x.db", 0, 0) == "runs/x.db"
    assert shard_store_path(":memory:", 1, 4) == ":memory:"
    assert shard_store_path("memory", 1, 4) == "memory"


def test_shard_paths_lists_every_shard_in_index_order():
    assert shard_paths("runs/x.db", 4) == [
        "runs/x.shard0of4.db",
        "runs/x.shard1of4.db",
        "runs/x.shard2of4.db",
        "runs/x.shard3of4.db",
    ]
    assert len(shard_paths("runs/x.db", 3)) == 3
    assert shard_paths("runs/x.db", 1) == ["runs/x.db"]


def test_shard_env_names_the_shard_for_children():
    assert shard_env(1, 4) == {"PYATACKER_SHARD": "1/4"}
    assert shard_env(0, 1) == {"PYATACKER_SHARD": "0/1"}
    assert set(shard_env(3, 4)) == {"PYATACKER_SHARD"}


def test_describe_shard_round_trips_through_parse_shard():
    assert describe_shard(2, 4) == "2/4"
    assert parse_shard(describe_shard(2, 4)) == (2, 4)
    assert parse_shard(describe_shard(0, 1)) == (0, 1)


# --------------------------------------------------- retry + resource pool + sharding (combined)

# A task that both retries deterministically (by ctx.attempt, no RNG) and actually acquires a
# real lease from a real pool -- none of the built-in mock tasks do both at once. Written to a
# file on disk (module form, not `@task` in this test module) because `--shards` spawns genuine
# child *processes*: they re-import the declarative config's `use:` target from scratch, so it
# has to be importable on its own, not just defined in this process's memory.
_COMBINED_TASK_MODULE = '''
from pyattacker import RetryableError, task


@task("combo.flaky_pool", retry={"max_attempts": 3, "base": 0.001, "cap": 0.01}, resource="apis")
async def combo_flaky_pool(value, ctx):
    async with ctx.acquire() as lease:
        if ctx.attempt == 1:
            raise RetryableError("first attempt fails on purpose")
        return {"value": value, "resource": lease.resource.id}
'''

_COMBINED_CONFIG = """
pools:
  apis:
    kind: llm
    capacity: 2
    algorithm: backoff
    resources:
      - id: api-1
        options: {model: gpt-4o}
      - id: api-2
        options: {model: gpt-4o}
source:
  kind: range
  n: 12
pipeline:
  name: combo
  tasks:
    - use: combo_flaky_pool_task:combo_flaky_pool
run:
  concurrency: 3
"""


def test_shards_with_retrying_resource_pool_pipelines_merge_cleanly(tmp_path, capsys, monkeypatch):
    """None of the three subsystems has ever been exercised together before this test:

    * `retry`/backoff (the task above fails attempt 1 and succeeds attempt 2 -- decided by
      ``ctx.attempt``, not by chance, so the expected attempt/lease counts below are exact
      rather than "at least"),
    * a resource pool with a real ``ctx.acquire()`` and `backoff` acquire algorithm, and
    * `--shards`, which forks two separate child *processes* — each with its own event loop,
      its own pool instances, its own retry state.

    Passing individually says nothing about this combination: a resource pool's waiter queue or
    a task's retry state could, in principle, leak across what should be fully independent shard
    children. This proves they don't: the merged view accounts for every pipeline exactly once,
    and every pipeline took exactly 2 attempts (1 failure + 1 success), each of which acquired a
    real lease from that shard's own pool.
    """
    (tmp_path / "combo_flaky_pool_task.py").write_text(_COMBINED_TASK_MODULE, encoding="utf-8")
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join([str(tmp_path), os.environ.get("PYTHONPATH", "")])
    )
    monkeypatch.syspath_prepend(str(tmp_path))  # the parent's own --shards preflight also imports it
    cfg = tmp_path / "combo.yaml"
    cfg.write_text(textwrap.dedent(_COMBINED_CONFIG), encoding="utf-8")
    base = tmp_path / "combo.db"
    capsys.readouterr()

    try:
        rc = main(["run", "-c", str(cfg), "--shards", "2", "--jobs", "2", "--store", str(base)])
    except OSError as exc:  # pragma: no cover - only on an environment that cannot spawn a child
        pytest.skip(f"cannot spawn shard children in this environment: {exc}")

    assert rc == 0
    shard0, shard1 = tmp_path / "combo.shard0of2.db", tmp_path / "combo.shard1of2.db"
    assert shard0.exists() and shard1.exists()

    store0, store1 = SqliteStore(str(shard0), read_only=True), SqliteStore(str(shard1), read_only=True)
    try:
        rows0, rows1 = store0.pipelines(), store1.pipelines()
        ids0 = {r.pipeline_id for r in rows0}
        ids1 = {r.pipeline_id for r in rows1}
        assert not (ids0 & ids1)  # every pipeline is owned by exactly one shard
        assert len(ids0) + len(ids1) == 12
        assert all(r.state == "succeeded" for r in rows0 + rows1)
        # deterministic: attempt 1 always fails, attempt 2 always succeeds
        assert all(r.attempts_total == 2 for r in rows0 + rows1)

        attempts0 = sum(r.attempts_total for r in rows0)
        attempts1 = sum(r.attempts_total for r in rows1)
        assert attempts0 == 2 * len(rows0)
        assert attempts1 == 2 * len(rows1)

        resources0 = store0.resources(pool="apis")
        resources1 = store1.resources(pool="apis")
        assert len(resources0) == 2 and len(resources1) == 2  # each shard's own pool, independently populated
        # every attempt takes exactly one lease: the pool actually did the work, not a stub
        assert sum(r["stats"]["leases"] for r in resources0) == attempts0
        assert sum(r["stats"]["leases"] for r in resources1) == attempts1
    finally:
        store0.close()
        store1.close()

    merged = merge_reports([str(shard0), str(shard1)])
    assert merged.stats()["pipelines"]["by_state"] == {"succeeded": 12}
    assert merged.stats()["attempts_total"] == attempts0 + attempts1
