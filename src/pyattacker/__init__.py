"""pyattacker —— an artifact-centric async task orchestration framework.

Core model (five concepts, in data-flow order)::

    artifact  a task's persisted state (content-addressed, persisted as soon as it is produced → task-level checkpoint)
    task      a unary function (artifact) -> artifact, the smallest unit of scheduling
    pipeline  several tasks chained linearly; the unit of **completion** and **resume**, semantically independent of each other
    resource  a concurrency-safe publishable/subscribable resource (such as a provider endpoint)
    algorithm the strategy for "how to take a resource from the pool" (wait/backoff/switch pool/pick the least busy)

Minimal usage::

    from pyattacker import Runner, pipeline, task, Pool, Resource

    @task("fetch")
    def fetch(seed, ctx): ...

    @task("ask", resource="apis", retry={"max_attempts": 3})
    async def ask(row, ctx):
        async with ctx.acquire(model="gpt-4o") as lease:   # returned on exit, guaranteed even on exception
            return await lease.client.chat(row["q"])

    pool = Pool("apis", [Resource.create("llm", capacity=4, options={...})], algorithm="backoff")
    with Runner(store="runs.db", pools=[pool], concurrency=16) as runner:
        report = runner.run(pipeline("qa", fetch | ask).map(dataset))
        print(report.summary())
"""

from __future__ import annotations

from .algorithm import (
    Backoff,
    Failover,
    Immediate,
    LeastBusy,
    QuotaAware,
    Sticky,
    Wait,
    resolve_algorithm,
)
from .artifact import Artifact, CodecRegistry, canonical_json, digest_of
from .declarative import load_spec
from .export import FORMATS, ROW_KINDS, export_store, export_stores, iter_rows
from .merge import MergedReport, merge_reports
from .shard import in_shard, parse_shard, shard_index, shard_specs, shard_store_path
from .errors import (
    AcquireTimeout,
    ArtifactCodecError,
    BudgetExceeded,
    ConfigError,
    FatalError,
    LeaseLeakError,
    PipelineBuildError,
    PoolNotFound,
    PyAttackerError,
    ResourceUnavailable,
    RetryableError,
    RunInterrupted,
    error_class_of,
)
from .pipeline import Chain, PipelineSpec, PipelineTemplate, compute_spec_digest, pipeline, with_retry
from .resource import Bus, Lease, Pool, Resource, ResourceEvent, ResourceState
from .runner import RunConfig, RunReport, Runner
from .store import AttemptRecord, EventRecord, MemoryStore, PipelineRecord, SqliteStore, open_store
from .task import Retrying, TaskContext, TaskSpec, task
from .tasks import (
    boom,
    delay,
    echo,
    flaky,
    jsonl_source,
    leaky,
    seed_factory,
    shell_run,
    simulate_llm,
    write_jsonl,
)

__version__ = "0.0.1"

__all__ = [
    "__version__",
    # core objects
    "Runner",
    "RunConfig",
    "RunReport",
    "pipeline",
    "task",
    "Pool",
    "Resource",
    "Lease",
    "Bus",
    "ResourceEvent",
    "ResourceState",
    "Retrying",
    "TaskSpec",
    "TaskContext",
    "Chain",
    "PipelineSpec",
    "PipelineTemplate",
    "compute_spec_digest",
    "with_retry",
    "Artifact",
    "CodecRegistry",
    "canonical_json",
    "digest_of",
    # algorithms
    "resolve_algorithm",
    "Immediate",
    "Wait",
    "Backoff",
    "LeastBusy",
    "Failover",
    "Sticky",
    "QuotaAware",
    # store
    "open_store",
    "MemoryStore",
    "SqliteStore",
    "AttemptRecord",
    "EventRecord",
    "PipelineRecord",
    # declarative
    "load_spec",
    # sharding, merging, export
    "shard_index",
    "in_shard",
    "shard_specs",
    "shard_store_path",
    "parse_shard",
    "merge_reports",
    "MergedReport",
    "iter_rows",
    "export_store",
    "export_stores",
    "ROW_KINDS",
    "FORMATS",
    # built-in tasks
    "echo",
    "flaky",
    "delay",
    "boom",
    "leaky",
    "simulate_llm",
    "shell_run",
    "write_jsonl",
    "jsonl_source",
    "seed_factory",
    # exceptions
    "PyAttackerError",
    "ConfigError",
    "PipelineBuildError",
    "ArtifactCodecError",
    "ResourceUnavailable",
    "AcquireTimeout",
    "PoolNotFound",
    "LeaseLeakError",
    "RetryableError",
    "FatalError",
    "BudgetExceeded",
    "RunInterrupted",
    "error_class_of",
]
