# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

* **Advanced feature: handoffs — a task can skip ahead, on the record (opt-in, experimental).** A task may
  return `Handoff.to(target, value)` to continue at a declared later station, or `Handoff.end(value)` to
  finish the pipeline immediately. Until now the alternatives were to run the remaining stations anyway, to
  fold the branch into one task with `fanout` (losing per-step records), or to raise — which records the
  pipeline as *failed*, which is a lie. A handoff says what actually happened: this row skipped stations 3–5 and
  continued at station 6, or finished here. It is also a **durable checkpoint**: the source task lands
  `handed_off`, its attempt keeps `outcome="handed_off"`, the entry artifact is referenced or stored, a
  `handoffs` ledger row records from/to and why, and the cursor moves — all in **one atomic commit**, so a
  killed process resumes *at the target* with the entry state and never re-runs the source task.
  `Observable changes:` a new append-only `handoffs` table (`store.handoffs(...)`, nested in the `pipelines`
  export row, counted by `stats()["handoffs_total"]`, shown by `report`/`watch`/`/pipelines`); the attempt
  outcome `handed_off`; handoff payload artifacts at `seq >= n_tasks`, so a payload can never overwrite a
  task slot; the `pipeline.handoff` event; `Handoff` and `HandoffRecord` in the public API; and
  `control={"edges": {...}}` on `pipeline(...)` plus the declarative `pipeline.control`. Edges are declared
  rather than derived, so an undeclared or backward target is a fatal error instead of a silent jump, and
  `fanout` rejects a directive returned by one of its branches. **A pipeline without a `control` block is
  untouched**: no new rows, no counter changes, and a byte-identical `spec_digest` (pinned by a literal test),
  so no stored checkpoint, pipeline id or shard assignment is invalidated. Handoffs need a store that can
  commit them atomically; both built-in backends can, and a store that cannot is refused up front with a
  `ConfigError` rather than silently writing a non-durable jump. Marked *experimental until 1.0*;
  backward/revoke handoffs, joins and cross-pipeline jumps are not included. See
  [`docs/design.md` §4.8](docs/design.md#48-advanced-handoffs--declared-forward-jumps-opt-in-experimental),
  the [API reference](docs/reference.md#advanced-handoffs-opt-in) and
  [tutorial step 15](docs/tutorial.md#step-15--advanced-skipping-stations-handoffs).

### Fixed

* Review follow-up: freeze resolved control topology; atomically reset current task/chain artifact state
  on control-enabled seed replays while retaining history; expose handoff identity/watermark and active
  versus historical API counts; treat unencodable handoff payloads as fatal; correct return-only
  annotation documentation. Handoff-capable custom stores must also provide `reset_pipeline(record)`.

* Handoff follow-up: isolate historical ledger rows on whole-pipeline restarts with a durable
  `handoff_floor` watermark (including old-store migration and read-only compatibility); roll back failed
  SQLite handoff commits; select a single final artifact after reruns; reject duplicate source aliases;
  preserve numeric source disambiguation in JSON/TOML/YAML and effective config; restrict the annotation
  escape to return unions containing `Handoff`.

### Changed

* **The sdist no longer ships the branding images.** `assets/` was in the sdist include list, and PNG
  barely compresses, so the two logo files were 883 KB of the 1.26 MB `pyattacker-0.2.0.tar.gz` that PyPI
  serves — two thirds of the download for an archive whose purpose is to be rebuilt and tested. Nothing
  that unpacks an sdist needs them, and the README loads its banner over an absolute
  `raw.githubusercontent.com` URL, so the PyPI description is unaffected; the tarball is back to 439 KB.
  Both `ci.yml` and `release.yml` now fail the build if the sdist grows past 1 MiB, and
  `tests/test_packaging.py` fails if a README image ever points into the checkout again.
* **A release goes through TestPyPI before it reaches PyPI.** The scratch-index upload was a rehearsal a
  maintainer had to opt into, and the `v0.2.0` tag skipped it — so the files PyPI received were the first copy
  of that version any index had ever served. Every tag now publishes to TestPyPI, installs those files back by
  name into a clean environment, asserts that the installed CLI reports the version being released, and only
  then uploads to PyPI. The check is a job of its own with no OIDC token, so the job that holds a credential
  that can publish does nothing else, and `pypi` now depends on it. A dispatched run still rehearses when
  *Publish to TestPyPI* is checked and stops before both uploads otherwise. `docs/releasing.md` records the
  consequence this makes load-bearing: a filename an index has seen can never be uploaded again, so a commit
  after a rehearsal means a new version number, not a re-run.

### Fixed

* **A worker that dies outside its own handlers no longer hangs the run.** Run completion was tracked purely
  by pipeline counters (`pipelines_done` against `pipelines_admitted`), so a `BaseException` that is not
  `CancelledError` — a custom subclass raised by a store or backend hook, for example — escaped the worker's
  handler chain, ended the task, and left the run waiting forever on a condition the dead worker could no
  longer satisfy: no error, no exit code, no terminal row and no event (only asyncio's "Task exception was
  never retrieved" on stderr). Worker lifetime is now supervised separately from pipeline accounting, by a
  done-callback on every worker task that also sees a death *after* the handler chain (queue or worker
  housekeeping). The run is hard-stopped (`stop_reason == "worker_crashed"`) without manufacturing
  `pipelines_done`; the pipeline the dead worker was holding is recorded `failed` — or `interrupted` when the
  run was already winding down — with the escaping exception on the row, or created if the crash happened
  before its row existed, while a pipeline that was already terminal keeps the state it earned; admission and
  the shutdown sentinels hand items over through an abort-aware put, so a producer parked on the full queue of
  a dead worker is released instead of hanging one step earlier; `runner.worker_crashed` records the pipeline,
  the exception and its traceback; and `run_async` raises the new `WorkerCrashed` (original exception as
  `__cause__`) after closing the run record as `interrupted`, with the CLI exiting 2 on a named error. If the
  store cannot record the crash either, the existing `StoreUnavailable` fatal path is taken instead of
  retrying a broken store. `KeyboardInterrupt`/`SystemExit` still tear the loop down, so a narrow guard inside
  the worker records the row and the event before they continue on their way. Cancellation and the ordinary
  internal-`Exception` recovery path are unchanged.
* **A pipeline whose cursor already reached the end can be resumed again.** The success path advanced the
  checkpoint cursor and wrote the terminal state as two separate store calls, so a store failure in the
  final write (or a kill between the two) could leave a `failed`/`interrupted` row with
  `n_tasks_done == n_tasks_total`. Every later run then raised `IndexError: tuple index out of range` inside
  `_open_pipeline`, surfaced only as `runner.internal_error`, while overwriting the row's original failure
  with the framework's own crash. Such a row is now settled as completed: the last artifact is verified the
  same way an ordinary resumed checkpoint is (present, payload kept, decodable), marked final, and the
  pipeline is recorded `succeeded` without re-running a task, with `pipeline.terminal_repaired` carrying the
  previous state, error and run. A cursor *past* the end is not a state the Runner can create: it is recorded
  as `CorruptCheckpoint` and reported as `pipeline.corrupt_cursor` instead of being promoted to success, and
  the stored value is left as the evidence; an artifact that is missing, payload-less or undecodable falls
  back to the documented restart-from-zero rule (`pipeline.checkpoint_missing` /
  `pipeline.checkpoint_unusable`). The repair never destroys the failure it is repairing before the outcome is
  durable — `mark_final` runs first, the terminal transition (state, cursor, owning run, cleared failure
  fields) is a single write where the store offers the optional `settle_pipeline` capability, and a repair
  that fails in **either** step — marking the artifact final or settling the row — leaves the row untouched
  (original failure and owning run included) and records `pipeline.terminal_repair_failed` with the phase,
  so the next attempt still reports the original cause instead of the repair's own error. `mark_final` is
  now contractually idempotent and is skipped when the artifact is already final. Because such a row keeps
  its original owner, the failed repair is counted in the new `RunReport.repair_failures` (surfaced by
  `summary()`, `to_dict()` and the CLI exit code) instead of silently leaving the run looking successful;
  and on the two-write fallback for stores without `settle_pipeline`, a failed metadata cleanup after a
  durable `succeeded` is reported as `pipeline.terminal_cleanup_failed` rather than as a failed repair. The final task now also marks its artifact final **before**
  committing the terminal state and advances the cursor in that same call, so a crash in between re-runs the
  final task (the documented at-least-once boundary) rather than leaving a `succeeded` pipeline whose final
  artifact was never marked — a state no later run could repair, because a succeeded pipeline is skipped
  forever.

## [0.2.0] — 2026-09-17

### Added

* **Paged store reads (`pyattacker.store.iter_*`).** Whole-kind reads for exports no longer have to
  materialize a table: `iter_pipelines` / `iter_tasks` / `iter_attempts` / `iter_events` /
  `iter_artifacts` stream records in the documented order — and every order ends in a unique key
  (`task_run_id` / `artifact_id` are part of the `tasks` / `artifacts` cursors, because the natural
  `(pipeline_id, seq)` / `seq` prefixes are not unique), so a batch boundary can neither drop nor
  duplicate a row. They use an optional `PagedStore` extension when the store has one and fall back
  to the list API when it does not — so third-party stores keep working unchanged. `SqliteStore`
  implements the extension with keyset pagination (`WHERE key > last ORDER BY key LIMIT 1000`);
  reading a store that is still being written is defined per kind (`events`/`attempts` are bounded
  by the high-water mark of their monotonic keys, `pipelines`/`tasks`/`artifacts` are a best-effort
  traversal); and `WriteBehindStore` flushes before every paged read exactly as its list APIs do.
  See [stores](docs/reference.md#paged-reads-and-third-party-stores).

* **`pyattacker bench` — a simulation that compares the acquire algorithms.** A scenario states the
  assumptions about a provider as data (a capacity cycle, a token bucket that tightens when pushed,
  log-normal latency with a slow tail, independent failures plus correlated storms, three endpoints of
  different character, declared quotas); a closed loop of workers drives the real `Pool`, the real
  `TaskContext` and the real algorithm against it; time is simulated, so a ten-minute scenario costs
  seconds. It reports a vector of metrics — throughput, job tail latency, refusals provoked, capacity
  utilisation, endpoint fairness — with the spread over seeds, and names the best algorithm per metric
  instead of inventing a weighted score. The world is a black box by construction, each request's draws
  are indexed by `(endpoint, ordinal)` so two algorithms share the same exogenous randomness and the
  same time-indexed conditions (their realized state still diverges, because the bucket and the
  in-flight count react to what each of them did), and the same seed reproduces the numbers exactly. `docs/benchmark.md` documents the assumptions, the metrics, the
  current results and their limits.

* **`Retrying.decide`** — the retry decision (retry or give up, and after how long) is now a method on
  the policy rather than a block inside the Runner's attempt loop, so the benchmark can ask the same
  question outside a run. Behaviour is unchanged; `tests/test_retry_policy.py` pins the rules directly.

### Changed

* **`TaskSpec.with_overrides` distinguishes "not given" from `None`.** An omitted keyword keeps the
  current value (as before), an explicit `None` now **clears** `resource`, `algorithm`, `timeout_s`
  or `version` instead of being silently dropped, and `UNSET` (exported from `pyattacker`) means
  "not given" even when the key is present — which is how the declarative loader forwards a config
  without clearing what it did not mention. `None` for a field that cannot be empty is a
  `ConfigError`, not a no-op.

* **One shared validation entry (`load_spec`) for `validate` and every `run` mode.** Unknown fields
  (with a "did you mean" for typos), wrong types, out-of-range numbers, unknown pool references
  (including the `resource` a `use:` factory declares itself), unresolvable algorithms, a bad
  `source:` declaration, a section that is not a mapping, and an `artifact_backend` that
  `resolve_backend` could not construct (a missing file-backend `root`, an unparseable JSON-string
  spec, an unknown `kind`) are exit code `2` before a store or a shard child exists, and each message
  names the field path. The `run:` block now also accepts `artifact_backend`, `write_behind`,
  `write_batch` and `flush_interval`, which the CLI used to filter out silently.

* **Export `limit` is one rule per row kind, and it counts that kind's rows.** `None` (the default)
  exports the complete history, `0` exports no rows, a positive `N` exports the first N rows of the
  kind's documented order and a negative value is a `ConfigError`. Two things follow for callers:
  `limit` on `kind="artifacts"` used to count *pipelines* — it now counts artifact rows, like every
  other kind — and a truncated `events` export now keeps the *oldest* rows instead of the newest.
  `iter_rows` documents the order per kind. The CLI's `--limit` belongs to `run`, not `export`, so no
  CLI behaviour changes.

* **A scenario can declare the algorithms it cannot exercise** (`Scenario.unsuited`): `bursty_provider`
  lists `failover` (one pool gives it nothing to fail over to) and `least_busy` (the pool's default
  selection is already least-busy-first, so it is the same code path as `wait`). Those are skipped by
  default and marked N/A when asked for, instead of being ranked on a number that cannot mean anything.

* **The benchmark's refusals are `PyAttackerError`s** (`BenchmarkError`, and beside `BenchmarkTimeout` a
  new `BenchmarkStalled` for a world whose every resource is dead or revoked), so the CLI reports them as
  a message and exit code 2 rather than as a traceback — and `BenchmarkStalled` arrives in about a second
  instead of after the whole wall-clock budget.

* **The two accounting rows that carried a direction no longer do** (`jobs_failed`, `jobs_unstarted`).
  Each can be minimised by doing less work — never start a job and nothing fails; start everything and fail
  it and nothing is left unstarted — so they stay in the table, where the totals have to close, and award
  no star.

* **Benchmark budgets are validated instead of clamped.** `run_benchmark(seeds=0)` ran one seed
  (`range(max(1, seeds))`) while the CLI announced "x 0 seed(s)", i.e. it printed a different experiment
  than it ran; `Scenario` now refuses `jobs < 1`, `concurrency < 1`, `horizon_s <= 0`, `steps_per_job < 1`
  and `calls_per_step < 1`, `run_benchmark`/`Harness` refuse a non-positive seed count or wall budget, and
  `ScaledClock` refuses a non-positive speedup (as a `ConfigError`, so the CLI exits 2 with a message
  rather than a traceback). The CLI checks them before it prints its progress header, so the
  announcement cannot describe a sweep that will not happen.

* **Documentation and CLI text that had drifted from the code.** `BenchmarkReport.winners()` and
  `docs/benchmark.md` still described the independent-samples standard error the report no longer uses, the
  virtual-vs-real clock table quoted throughput from the retired fixed-denominator formula, the worked
  example still said "21 runs" where the scenario now runs 15, `Harness.run`'s timeout was documented as
  `asyncio.wait_for` where it races the workers against a supervisor, `bench`'s progress header counted
  the framework's seven algorithms where the scenario would run five (as did the README's "about 20
  seconds" and `--algorithms`'s "every built-in algorithm"), and `jobs_done` was described as "jobs that
  finished before the horizon" although the horizon stops *admission* and lets a step in flight finish
  (hence a 600s scenario with a makespan above 600s).

* **PyYAML is no longer a dependency — it is the optional `yaml` extra.** `pip install pyattacker` now
  installs nothing at all: the kernel and the declarative layer's JSON/TOML paths are the standard library.
  Only reading a `.yaml`/`.yml` config needs the parser, so it moved to `pyattacker[yaml]`. The loader
  already imported it lazily and chose the parser from the file suffix; what changed is that the error now
  says so — a `.yaml` file without the extra raises `ConfigError: … needs the optional 'yaml' extra:
  pip install "pyattacker[yaml]"` naming the file that pulled it in, and the CLI reports it as the usual
  config error (exit code 2), and a PyYAML that is installed but broken surfaces its own error instead of
  that hint. ``import pyattacker``, the CLI, every SDK-only program and every JSON/TOML config keep working
  with no third-party package installed, and the suite keeps proving it: a new CI job installs the bare
  package and runs the suite with the `requires_yaml` tests deselected, and the release workflow checks the
  built wheel in both directions — without the extra (PyYAML absent, JSON works, YAML asks for it) and with
  it (PyYAML resolves from the published metadata, a YAML config validates).

* **The workflows moved off the deprecated Node 20 action runtime.** `actions/checkout` v4 → v7,
  `actions/upload-artifact` v4 → v7, `actions/download-artifact` v4 → v8, and `astral-sh/setup-uv` v5 →
  v10.1.0 — the last one pinned to a commit, because setup-uv stopped publishing floating major tags at v8
  and now recommends pinning. The two publishing jobs also stop asking for a dependency cache: they never
  check out the repository, so there is no lockfile to key one on, and the run did nothing but warn about it.

### Fixed

* `--no-write-behind` now applies in every mode (one process, `--shard`, and the children of
  `--shards`), and `--artifact-backend` reaches those children instead of being dropped from their
  command line. `runs.config_json` records the effective backend and write-behind mode, plus the
  batch knobs when batching is on.

* The README quickstart is a complete program that runs offline with no undefined names, and the
  README/CLI-reference YAML examples quote the retry key (`"on"`), which YAML 1.1 otherwise parses
  as the boolean `true`. The examples in both documents are now executed by the test suite.

* **`shell_run` left its child running when the task was cancelled**, and killed without reaping on
  timeout: cancelling `asyncio.create_task(shell_run(cmd, timeout_s=None)(None, None))` raised
  `CancelledError` while `os.kill(pid, 0)` on the child still succeeded. Every exit path — normal exit,
  `timeout_s`, coroutine cancellation (including while the process is still being created), any other
  exception — now runs one cleanup: a child still running is `SIGKILL`ed and **reaped** before the
  original exception propagates, so cancellation still arrives as `CancelledError` and a timeout still
  as `TimeoutError`, and a child that already exited is left alone.
  Observable change: a timeout now also waits for the reap before raising. On POSIX each child is started
  in its own session and cleanup signals the whole process group, so a shell pipeline or an argv program's
  descendants die with it; Windows has no process-group signalling in the standard library, so there only
  the direct child is terminated — stated as a platform limit in
  [the `shell_run` reference](docs/reference.md#shell_run).

* **`export` silently stopped at 100 000 events, and dropped the oldest ones.** `iter_rows` asked the
  store for `limit=100000` even when the caller asked for no limit, and SQLite answers that with the
  *newest* rows — so on a store with more events than that, a "complete" export quietly began in the
  middle and the first events were gone. `None` now means the complete history, and the read is paged:
  an export takes bounded keyset batches instead of `fetchall`-ing the table, and the
  `pipelines`/`artifacts` paths no longer ignore the limit or collect every pipeline id first.

* Resume now rejects an existing pipeline key whose task or seed digest differs, preserving its
  historical result/checkpoint and raising `PipelineIdentityConflict` (CLI exit 2). In-flight
  pipelines cancelled during the stop are immediately finalized as interrupted.

* Task fingerprint v2 includes built-in factory parameters, ordered fanout children and strategy,
  all retry fields and declared algorithm configuration. Tasks/config entries accept finite-JSON
  `config` and an explicit `version` for dynamic code or external behavior. `include_code=False`
  excludes child source digests recursively while retaining declared behavior. Built-in algorithm
  execution uses the captured configuration snapshot, including normalized Wait fallbacks; custom
  fingerprint hooks are checked before execution/acquisition. Identity JSON rejects coercions such
  as non-string object keys and tuples.

* Legacy fingerprint stores remain readable but are not automatically migrated: default pipeline
  IDs and shard assignments change, and explicit old keys conflict. The Runner warns when the
  oldest stored pipeline uses the legacy format. Finish old runs with the old package, then use
  a new store; see [resume identity](docs/reference.md#resume-identity).

* **Storm scheduling in the benchmark was traffic-dependent.** The simulated provider decided its
  weather when a request arrived and remembered "checked until now + window", so a request at t=9 could
  suppress a window a request at t=10 would have evaluated: two algorithms then met different worlds,
  which quietly breaks the comparison the benchmark exists to make. Storm state is now a pure function of
  `(seed, endpoint, t)`, anchored to fixed time windows, and the regression test compares two *different*
  traffic schedules at the same timestamps instead of replaying one schedule twice.

* **A circuit-break cooldown announced itself to nobody.** `Pool` degrades a resource for `cooldown_s` and
  recovers it lazily, so a pool whose every resource is cooling down had no event left to broadcast: a
  waiter parked past the deadline (a starvation bug in real time, a hang under a simulated clock). The
  pool now arms one task, only while somebody waits, that sleeps on the pool's clock until the earliest
  future cooldown and broadcasts — and ignores already-expired and permanently dead slots, both of which
  otherwise re-arm a zero-delay timer forever. It also tracks the deadline it armed, not just the task,
  because a cooldown that starts later can still end earlier: a 5s cooldown beginning at t=5 has to replace
  a 30s timer armed at t=0, or the waiters wake twenty simulated seconds after the resource they were
  waiting for came back. Both numbers are pinned by
  `tests/test_lease_safety.py::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it`, which
  wakes at t=30 without the re-arm.

* **`wall_s` on a benchmark report was never assigned**, so every report and every JSON payload claimed
  0.0s of wall clock. It is measured now, and it is declared `neutral`: it is what the sweep cost, not
  evidence about an algorithm.

* **Winner uncertainty threw away the experimental design.** Every algorithm runs every seed, so the runs
  are paired and what matters is the spread of the per-seed *difference*; the report used the
  independent-samples form, which is wider exactly when common random numbers worked. It now compares
  paired differences, and `SIGNIFICANCE_K = 2.0` is documented as a heuristic rather than as "95%": with
  three seeds the Student-t critical value is 4.3.

* **Fail-fast could win latency and throughput rows.** Throughput divided by each run's own makespan (so
  abandoning work early manufactured throughput) and latency was collected only for jobs that succeeded
  (so an algorithm that finished 0.23% of the work could lead a latency row). Throughput divides by the
  run's own makespan again — the honest definition, now that the gate below makes it safe, and no longer a
  linear rescaling of `jobs_done` that cannot tell a run which finished in 200s from one which took 500s —
  the latency metrics are named `successful_job_latency_*`, and every *quality* metric refuses to crown an
  algorithm that completed less than 90% of the best completion, naming the excluded ones in the table,
  the markdown and the JSON.

* **The completion gate was too narrow to do its job.** It covered the rows where the metric is literally
  undefined without completions (throughput, latency, per-completed-job attempts) and left the rates
  exposed — but the client controls the denominator of those too: `refusal_rate` is
  `refusals / requests_sent`, and an algorithm that abandons contention before asking sends no request to
  be refused; `error_rate` and `failed_attempt_rate` have the same shape; `retry_rate` is low for an
  algorithm that never gets far enough to meet a retryable failure; and `utilization` integrates offered
  capacity over a run whose length the client chose. `immediate` was therefore still eligible to tie for
  `refusal_rate` at 7 of 3000 jobs completed. The flag is now
  `Metric.requires_comparable_completion` and it covers every directional row except `jobs_done` (which
  *is* the comparison of how much work got done) and the `leases_active_at_end` correctness counter; the
  table marks all thirteen, and a test hands a do-nothing algorithm the best value on each of them to
  prove it cannot win any.

* **Two metrics did not mean what their names said.** `retry_rate` counted every failed attempt, including
  the ones the retry policy abandoned without retrying, and `attempts_per_job` counted step attempts per
  *completed* job while documenting a "1.0 = no retries" baseline that this scenario's three-step jobs can
  never reach. The first is now `failed_attempt_rate`, a real `retry_rate` counts the retries the policy
  actually scheduled (the difference between them is the share of failures judged hopeless), and
  `attempts_per_completed_job` documents its real baselines — `steps_per_job` for a clean run, with
  attempts spent on jobs that later failed in the numerator only — beside a new `attempt_inflation`:
  attempts per *attempted* step, where 1.0 really does mean "not one step had to be repeated".

* **The completion baseline ignored which algorithms a scenario can exercise.** An unsuited algorithm run
  because it was asked for by name could set the 90% floor and exclude every algorithm the scenario was
  actually about. The baseline now comes from comparable algorithms only — and a gated row with a
  single eligible contestant has no winner at all, with the report naming it (`not_compared` in the JSON,
  `only X was eligible` in the markdown) instead of crowning a one-horse race. A report containing one
  algorithm never awards a star on any row: a star is a comparative statement.

* **Client-side randomness depended on worker assignment.** One `Random` per worker was consumed by the
  acquire algorithm, the retry policy and every later job, so an algorithm that changed its own timing
  changed its own future randomness. The two subsystems now draw from separate streams derived from
  `(seed, job, step, attempt)`.

* **`ResourceUnavailable` was a hidden special case** in the harness: the algorithm declining to wait
  failed the step without consulting `Retrying`. It now goes through `Retrying.decide()` like any other
  failure, so a scenario that retries it (`on=`, or `retry_unknown=True`) gets the framework's semantics.

* **`endpoint_spread` was declared "lower is better"**, which embeds the assumption that an even split
  across deliberately unlike endpoints is good; it is a diagnostic now, and so is `wall_s`.

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

[Unreleased]: https://github.com/Hazer-BJTU/pyattacker/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Hazer-BJTU/pyattacker/releases/tag/v0.2.0
[0.1.1]: https://github.com/Hazer-BJTU/pyattacker/releases/tag/v0.1.1
[0.1.0]: https://github.com/Hazer-BJTU/pyattacker/releases/tag/v0.1.0
