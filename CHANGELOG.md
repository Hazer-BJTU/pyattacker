# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] — 2026-09-17

Documentation and release tooling. **No library changes** — the code in this release is identical to 0.1.0,
which is part of why it is a good first exercise of the publishing pipeline.

### Added

* **Tag-driven publishing.** `.github/workflows/release.yml` re-runs the full suite on 3.11 and 3.12, lints,
  smoke-tests the CLI and the examples, then builds — and refuses to upload unless the tag matches
  `__version__`, `twine check --strict` passes, the sdist rebuilds and passes its own tests, and the wheel
  installs and runs `pyattacker demo`. The upload uses OIDC trusted publishing, so no API token lives in the
  repository, and it waits on the `pypi` environment's required reviewer. The GitHub release takes its notes
  from this file.
* **[docs/releasing.md](docs/releasing.md)** — the maintainer's checklist: trusted-publisher setup, the two
  version fields that must agree, the TestPyPI rehearsal, why each pre-upload check exists, and how to
  recover from an upload that failed.
* **PyPI and Python-version badges** in the README. The Python one reads the package metadata, so it cannot
  drift from the classifiers.

### Fixed

* The `v0.1.0` tag and GitHub release now exist. 0.1.0 reached PyPI before the publishing workflow did, so it
  had left no tag, no release page, and a changelog link that led nowhere.

## [0.1.0] — 2026-09-17

First release. An artifact-centric async orchestration kernel: it runs many independent pipelines to
completion, resumably and observably, without touching the network itself.

### Core

* **Five-concept kernel** — artifact, task, pipeline, resource, algorithm. A task is a unary
  `(artifact) -> artifact` function, sync or async; a pipeline is a linear chain of them and the unit of
  completion; pipelines share nothing but resource pools.
* **Task-level checkpoints.** Every artifact is persisted the moment it is produced, so `resume` restarts at
  the first task that produced nothing and re-sends nothing that already succeeded. The seed artifact is
  persisted too, so recovery does not depend on the original dataset file.
* **Content-addressed identity.** `pipeline_key` is a digest of the task-chain fingerprint, the seed, and the
  repeat index, which makes reruns idempotent, results reproducible, and `map(seeds, repeats=k)` (pass@k) free.
  Task source digests are included, so changing task code is treated as a new pipeline rather than reusing a
  stale checkpoint.
* **Lease safety contract.** Resources are only usable through a lease, and a task never holds one after it
  ends: `async with ctx.acquire(...)` returns on exit, on exception, on cancellation and on timeout, and the
  escape hatch is force-reclaimed with a `lease.leaked` event. Reclaim is a pure synchronous function, so
  `CancelledError` cannot interrupt it. `strict_leases=True` turns a leak into a task failure.
* **Resource pools** with per-resource capacity, a `READY/DEGRADED/DEAD/REVOKED` state machine driven by
  explicit `lease.report(...)`, quota accounting, publish/subscribe, and a signal bus.
* **Seven acquisition algorithms** — `wait`, `backoff`, `least_busy`, `failover`, `sticky`, `quota_aware`,
  `immediate` — kept orthogonal to retry policy: the algorithm decides how to *get* a resource, retry decides
  what to do *after* a failure.
* **Failure classification and retry.** `error_class_of` is a pure function mapping status codes and exception
  types to `timeout`/`rate_limit`/`upstream`/`fatal`/`connection`/`invalid`/`unknown`; `Retrying` adds
  exponential backoff with full jitter and a total-time budget. No retry by default. **Every retry decision is
  persisted** — `{retry, reason, delay_s, error_class, attempt, max_attempts, retry_after}`.

### Scheduling and persistence

* **Backoff parks the pipeline instead of sleeping in a worker**, so `concurrency` means attempts in flight.
  A parked pipeline is never mistaken for a dead one: the run waits for it, and a stop records it as resumable.
* **Targeted pool wakeups** — releasing a resource wakes only the waiters whose selector can use it.
* **Write-behind batching** for append-only facts (attempts, events), while state writes — pipelines, tasks,
  artifacts — always go straight through. `SIGKILL` can cost the last batch of history, never a checkpoint.
  `--no-write-behind` opts out.
* **SQLite store** (WAL, `synchronous=NORMAL`) across seven tables plus an in-memory store with identical
  semantics. WAL's one-writer-many-readers model is what lets `watch`/`report`/`serve` run beside a live run.
* **Wait-time metrics** — `waits_total`, `wait_ms_avg`, `p50`, `p95`, `max`, and an `acquire.slow_wait` event
  past `slow_wait_ms`. Suspected deadlocks emit `acquire.suspected_deadlock`.

### Scale, I/O and ecosystem

* **Deterministic sharding** — `--shard I/N` for one process doing its share, `--shards N` to spawn children
  locally. Assignment is a pure function of the pipeline key, so the split is stable and resume-safe.
* **Merged reports** across shard stores: de-duplicated by `pipeline_id`, statistics recomputed, folded row
  count reported.
* **Export** in five row shapes (`pipelines`, `tasks`, `attempts`, `events`, `artifacts`) and three formats
  (`jsonl`, `json`, `csv`, with an `extra` column for late keys).
* **Entry-point plugins** for tasks, algorithms, codecs and stores. Built-ins resolve first, and a plugin that
  raises on import is recorded rather than fatal — `pyattacker plugins` shows both.
* **Artifact backends** — `inline` (default), content-addressed `file://` spilling above a threshold with
  transparent hydration on read, and `null`.
* **`fanout(...)`** for genuine branching inside one task, with group-level retry granularity as the stated price.
* **Read-only HTTP endpoint** — `pyattacker serve`, a dependency-free dashboard plus JSON. Loopback-only and
  unauthenticated by design; it is a debug view, not a service.
* **CLI** — `run`, `resume`, `report`, `watch`, `export`, `serve`, `plugins`, `validate`, `demo`, with exit
  codes `0`/`1`/`2`/`130`.
* **Declarative layer** — YAML/TOML/JSON describing composition and resources, with `${ENV}` expansion and
  `--strict-env`.

### Documentation and testing

* A fourteen-step [tutorial](docs/tutorial.md) whose every code block is extracted and executed by the test
  suite, so documentation that rots fails CI.
* A [design document](docs/design.md) stating the six invariants, the lease contract, the data model, and
  twelve known tradeoffs up front.
* A [CLI reference](docs/cli.md) and four worked examples, including one that *measures* the checkpoint
  granularity tradeoff rather than asserting it.
* The suite is offline and deterministic — all time goes through an injectable `Clock` — and runs in seconds.
  CI covers Python 3.11 and 3.12, lint, the examples, and a build.

### Known limitations

Deliberate, and documented in [`docs/design.md`](docs/design.md) §8: stop conditions are best-effort;
write-behind can lose the tail of history (never a checkpoint); `journal=summary` and the `null` backend trade
away recovery granularity; resource health relies on explicit reporting; `quota_aware` is a preference, not a
hard limit; asyncio tasks only (wrap blocking code with `asyncio.to_thread`); one writer per database; shard
balance is statistical; a merged report is a union, not a sum; a pipeline is a linear chain; the HTTP endpoint
is unauthenticated.

[0.1.1]: https://github.com/Hazer-BJTU/pyattacker/releases/tag/v0.1.1
[0.1.0]: https://github.com/Hazer-BJTU/pyattacker/releases/tag/v0.1.0
