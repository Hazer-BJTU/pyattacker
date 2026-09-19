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
from .artifact import (
    Artifact,
    BytesCodec,
    Codec,
    CodecRegistry,
    Encoded,
    JsonCodec,
    canonical_json,
    digest_of,
)
from .backends import ArtifactBackend, FileBackend, InlineBackend, NullBackend, resolve_backend
from .declarative import load_spec
from .errors import (
    AcquireTimeout,
    ArtifactCodecError,
    BudgetExceeded,
    ConfigError,
    CorruptCheckpoint,
    FatalError,
    LeaseLeakError,
    PipelineBuildError,
    PipelineIdentityConflict,
    PluginError,
    PoolNotFound,
    PyAttackerError,
    ResourceUnavailable,
    RetryableError,
    RunInterrupted,
    StoreFeatureUnsupported,
    StoreUnavailable,
    WorkerCrashed,
    error_class_of,
)
from .export import FORMATS, ROW_KINDS, export_store, export_stores, iter_rows
from .handoff import Handoff
from .history import HistoryArtifact
from .merge import MergedReport, merge_reports
from .pipeline import Chain, PipelineSpec, PipelineTemplate, compute_spec_digest, pipeline, with_retry
from .plugins import PLUGINS, PluginRegistry, list_plugins
from .resource import Bus, Lease, Pool, Resource, ResourceEvent, ResourceState
from .runner import RunConfig, Runner, RunReport
from .server import StatsServer
from .shard import in_shard, parse_shard, shard_index, shard_specs, shard_store_path
from .store import (
    AttemptRecord,
    EventRecord,
    HandoffRecord,
    MemoryStore,
    PipelineRecord,
    SqliteStore,
    open_store,
)
from .task import UNSET, Retrying, TaskContext, TaskSpec, build_task_spec, task
from .tasks import (
    boom,
    delay,
    echo,
    fanout,
    flaky,
    jsonl_source,
    leaky,
    seed_factory,
    shell_run,
    simulate_llm,
    write_jsonl,
)

__version__ = "0.3.0"

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
    "UNSET",
    "build_task_spec",
    "Chain",
    "PipelineSpec",
    "PipelineTemplate",
    "compute_spec_digest",
    "with_retry",
    "Handoff",
    "HistoryArtifact",
    "Artifact",
    "Codec",
    "CodecRegistry",
    "JsonCodec",
    "BytesCodec",
    "Encoded",
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
    "HandoffRecord",
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
    # plugins
    "PLUGINS",
    "PluginRegistry",
    "list_plugins",
    # artifact backends
    "ArtifactBackend",
    "InlineBackend",
    "FileBackend",
    "NullBackend",
    "resolve_backend",
    # monitoring endpoint
    "StatsServer",
    # built-in tasks
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
    # exceptions
    "PyAttackerError",
    "ConfigError",
    "PipelineBuildError",
    "PipelineIdentityConflict",
    "CorruptCheckpoint",
    "PluginError",
    "ArtifactCodecError",
    "ResourceUnavailable",
    "AcquireTimeout",
    "PoolNotFound",
    "LeaseLeakError",
    "RetryableError",
    "FatalError",
    "BudgetExceeded",
    "RunInterrupted",
    "StoreUnavailable",
    "StoreFeatureUnsupported",
    "WorkerCrashed",
    "error_class_of",
]
