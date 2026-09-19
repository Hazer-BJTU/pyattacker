"""Command-line entry point.

Exit code convention: ``0`` all succeeded / ``1`` something failed / ``2`` config or usage error / ``130`` interrupted.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from . import __version__
from .declarative import RUN_FIELDS, load_spec
from .errors import ConfigError, PyAttackerError
from .export import FORMATS, ROW_KINDS, export_store, export_stores
from .merge import merge_reports
from .monitor import read_snapshot, render_snapshot, watch
from .pipeline import pipeline
from .plugins import PLUGINS, list_plugins
from .resource import Pool, Resource
from .runner import RunConfig, Runner
from .server import StatsServer
from .shard import parse_shard, shard_env, shard_paths, shard_specs, shard_store_path
from .store import SqliteStore
from .store.base import count_events
from .tasks import echo, simulate_llm

__all__ = ["main"]


def _warn_unresolved_env(spec: Any, *, leading_blank_line: bool = False) -> None:
    """Same protection ``validate`` has always had, now also on the commands that actually spend the value."""
    if spec.unresolved_env:
        prefix = "\n" if leading_blank_line else ""
        print(f"{prefix}Warning: unresolved environment variables {sorted(set(spec.unresolved_env))}", file=sys.stderr)


@contextlib.contextmanager
def _shard_env_preview(count: int) -> Iterator[None]:
    """Make the orchestrator-owned shard variables visible for the parent's env preflight.

    Only stands in for a variable the *user's own environment* does not already define, and only
    for the duration of one ``load_spec()`` call — the children get the real per-shard value from
    :func:`shard_env` when they load the config again.
    """
    added = {k: v for k, v in shard_env(0, count).items() if k not in os.environ}
    os.environ.update(added)
    try:
        yield
    finally:
        for key in added:
            del os.environ[key]


def _open_readonly(path: str, backend: Any = None) -> SqliteStore:
    """Open an existing store read-only; give a human-readable error when the file is missing.

    ``backend`` matters for reads too: payloads spilled out of the database are hydrated through it.
    """
    if not Path(path).exists():
        raise ConfigError(f"store does not exist: {path} (run 'run' or 'demo' first to create one)")
    return SqliteStore(path, read_only=True, backend=backend)


def _table(rows: Sequence[Sequence[Any]], header: Sequence[str]) -> str:
    widths = [len(str(h)) for h in header]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    lines = ["  ".join(str(h).ljust(widths[i]) for i, h in enumerate(header))]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)))
    return "\n".join(lines)


# --------------------------------------------------------------------- run
def _cmd_run(args: argparse.Namespace, *, resume: bool = False) -> int:
    """One process, one shard (or all of it). ``run --shards N`` orchestrates children of this."""
    shard = parse_shard(getattr(args, "shard", None))
    if getattr(args, "shards", None):
        if shard is not None:
            raise ConfigError("--shard and --shards are mutually exclusive")
        return _run_shards(args, resume=resume)

    spec = load_spec(args.config, strict_env=args.strict_env)
    _warn_unresolved_env(spec)
    # `RUN_FIELDS` is the same schema `load_spec` validated against, so a config field can never be
    # silently dropped here again (artifact_backend and the write-behind knobs used to be).
    run_cfg = {k: v for k, v in spec.run.items() if k in RUN_FIELDS}
    if args.store:
        run_cfg["store"] = args.store
    if shard is not None and not args.store:
        # Each shard owns a file: SQLite has a single writer, so scaling out means more files,
        # not more writers on one file. An explicit --store is used verbatim (that is how the
        # parent of `run --shards` hands each child its already-derived path).
        base = str(run_cfg.get("store") or ":memory:")
        if base == ":memory:":
            raise ConfigError("--shard needs a file-backed store (set run.store or pass --store)")
        run_cfg["store"] = shard_store_path(base, shard[0], shard[1])
    if args.concurrency:
        run_cfg["concurrency"] = args.concurrency
    if args.journal:
        run_cfg["journal"] = args.journal
    if args.label:
        run_cfg["label"] = args.label
    if args.strict_leases:
        run_cfg["strict_leases"] = True
    if args.stop_after_failures is not None:
        run_cfg["stop_after_failures"] = args.stop_after_failures
    if args.retry_succeeded:
        run_cfg["retry_succeeded"] = True
    if args.fresh_restart:
        run_cfg["fresh_restart"] = True
    if args.artifact_backend:
        run_cfg["artifact_backend"] = args.artifact_backend
    if args.no_write_behind:
        # An explicit flag always wins over a config that asks for batching (the CLI overrides the
        # `run:` block, the other way round from how the config's own value reaches RunConfig).
        run_cfg["write_behind"] = False
    meta = dict(run_cfg.get("meta") or {})
    if shard is not None:
        meta["shard"] = f"{shard[0]}/{shard[1]}"
    if meta:
        run_cfg["meta"] = meta
    config = RunConfig(**run_cfg)
    if args.no_signals:
        config.handle_signals = False

    runner = Runner(pools=spec.pools, config=config)
    stop_progress = threading.Event()
    progress = None
    if args.progress and str(config.store) != ":memory:":
        progress = threading.Thread(
            target=_progress_loop,
            args=(str(config.store), lambda: runner.run_id, stop_progress),
            daemon=True,
        )
        progress.start()
    specs = spec.pipelines(limit=args.limit)
    if shard is not None:
        specs = shard_specs(specs, shard[0], shard[1])
    try:
        report = runner.run(specs, resume=resume)
    except KeyboardInterrupt:  # pragma: no cover - signal handling already took over
        print("\ninterrupted (KeyboardInterrupt)", file=sys.stderr)
        return 130
    finally:
        stop_progress.set()
        if progress is not None:
            progress.join(timeout=2)
    if args.summary_format == "json":
        payload = report.to_dict()
        payload["store"] = str(config.store)
        payload["shard"] = f"{shard[0]}/{shard[1]}" if shard else "1/1"
        print(json.dumps(payload, ensure_ascii=False, default=str))
    else:
        print(report.summary())
    failed = report.stats.get("pipelines", {}).get("by_state", {}).get("failed", 0)
    if report.status == "interrupted":
        print("Re-run the same command with --resume to continue: unfinished tasks resume from their checkpoint", file=sys.stderr)
        return 130
    # `repair_failures` is run-local on purpose: a pipeline this run could not settle out of a torn
    # terminal state keeps the row (and therefore the run_id) of the run that created it, so it can never
    # appear in the run-scoped by_state above. Without this term the command would exit 0 after a failed
    # repair, which is the silent-success shape this whole path exists to remove.
    return 1 if failed or report.repair_failures else 0


# ------------------------------------------------------------------- shards
_CHILD_PASSTHROUGH = (
    ("limit", "--limit"),
    ("concurrency", "--concurrency"),
    ("journal", "--journal"),
    ("label", "--label"),
    ("stop_after_failures", "--stop-after-failures"),
    # A shard child is the single-shard command, so every flag that changes *where state lives*
    # has to travel with it; --artifact-backend used to be dropped here, which silently sent each
    # child to the default inline backend while the parent's report described something else.
    ("artifact_backend", "--artifact-backend"),
)
_CHILD_FLAGS = (
    ("retry_succeeded", "--retry-succeeded"),
    # A restart has to survive sharding too, or `run --shards N --fresh-restart` would restart only
    # the shard the parent itself opened.
    ("fresh_restart", "--fresh-restart"),
    ("strict_leases", "--strict-leases"),
    ("no_write_behind", "--no-write-behind"),
    ("no_signals", "--no-signals"),
    ("strict_env", "--strict-env"),
)


def _child_argv(args: argparse.Namespace, index: int, count: int, store: str, resume: bool) -> list[str]:
    argv = [
        sys.executable, "-m", "pyattacker", "resume" if resume else "run",
        "-c", str(args.config),
        "--shard", f"{index}/{count}",
        "--store", store,
        "--summary-format", "json",
    ]
    for attr, flag in _CHILD_PASSTHROUGH:
        value = getattr(args, attr, None)
        if value is not None:
            argv += [flag, str(value)]
    for attr, flag in _CHILD_FLAGS:
        if getattr(args, attr, False):
            argv.append(flag)
    return argv


def _run_shards(args: argparse.Namespace, *, resume: bool = False) -> int:
    """Spawn one child per shard locally, then print the merged view.

    Local fan-out is a convenience, not the model: each child is exactly the single-shard
    command, so the same flags work on a cluster where you launch them yourself.
    """
    count = int(args.shards)
    if count < 2:
        raise ConfigError("--shards must be >= 2 (use a single run otherwise)")
    # The parent never runs any pipeline itself — every ${VAR} the config references is only
    # actually spent inside a shard child, which gets shard_env() injected before it loads the
    # config again (see _child_argv). So the parent's env check must see the same shard-only
    # variables a child would, or a config that only references e.g. ${PYATACKER_SHARD} would
    # falsely warn (or, with --strict-env, refuse to even start the children that would succeed).
    with _shard_env_preview(count):
        cfg = load_spec(args.config, strict_env=args.strict_env)
    _warn_unresolved_env(cfg)
    base = str(args.store or cfg.run.get("store") or ":memory:")
    if base == ":memory:":
        raise ConfigError("--shards needs a file-backed store (set run.store or pass --store)")
    paths = shard_paths(base, count)
    jobs = max(1, min(int(args.jobs or count), count))
    stem, suffix = os.path.splitext(base)
    print(f"running {count} shards ({jobs} at a time) -> {stem}.shard*of{count}{suffix or '.db'}")

    pending = list(range(count))
    running: dict[subprocess.Popen, int] = {}
    results: dict[int, tuple[int, str, str]] = {}
    while pending or running:
        while pending and len(running) < jobs:
            index = pending.pop(0)
            argv = _child_argv(args, index, count, paths[index], resume)
            env = {**os.environ, **shard_env(index, count)}
            running[
                subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
            ] = index
        finished = [proc for proc in running if proc.poll() is not None]
        if not finished:
            time.sleep(0.2)
            continue
        for proc in finished:
            index = running.pop(proc)
            out, err = proc.communicate()
            results[index] = (proc.returncode or 0, out, err)

    print()
    for index in range(count):
        code, out, err = results[index]
        payload = _last_json_line(out)
        if payload is None:
            print(f"  shard {index}/{count}  exit={code}  (no summary; see stderr)")
            tail = (err or out).strip().splitlines()[-3:]
            for line in tail:
                print(f"      {line}")
            continue
        states = payload.get("pipelines", {}).get("by_state", {})
        rendered = " ".join(f"{k}={v}" for k, v in sorted(states.items())) or "no pipelines"
        print(
            f"  shard {index}/{count}  exit={code}  {rendered}"
            f"  attempts={payload.get('attempts_total', 0)}"
            f"  {payload.get('duration_ms', 0) / 1000:.2f}s"
        )
        if code != 0 and err.strip():
            for line in err.strip().splitlines()[-2:]:
                print(f"      {line}")

    existing = [path for path in paths if Path(path).exists()]
    if existing:
        merged = merge_reports([SqliteStore(path, read_only=True) for path in existing])
        print()
        print(merged.summary())
    codes = [code for code, _, _ in results.values()]
    if any(code == 130 for code in codes):
        return 130
    return max(codes) if codes else 0


def _last_json_line(text: str) -> dict[str, Any] | None:
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None


def _progress_loop(store_path: str, run_id_getter: Any, stop: threading.Event) -> None:
    """Read the same SQLite file from another thread (WAL allows one writer, many readers) — shows that a separate process can watch it too."""
    try:
        store = SqliteStore(store_path, read_only=True)
    except Exception:
        return
    try:
        while not stop.wait(1.0):
            run_id = run_id_getter()
            if not run_id:
                continue
            try:
                snapshot = store.stats(run_id)
            except Exception:
                continue
            snapshot["run_id"] = run_id
            sys.stderr.write("\r" + " | ".join(
                f"{k}={v}" for k, v in sorted(snapshot.get("pipelines", {}).get("by_state", {}).items())
            ) + f" | attempts={snapshot.get('attempts_total', 0)}   ")
            sys.stderr.flush()
    finally:
        store.close()


# ------------------------------------------------------------------ report
def _cmd_report(args: argparse.Namespace) -> int:
    paths = list(args.store)
    if len(paths) > 1:
        return _report_merged(paths, args)
    store = _open_readonly(paths[0], args.artifact_backend)
    try:
        run_id = args.run_id or _latest_run_id(store)
        if run_id is None:
            print("(no pipeline records in the store)")
            return 0
        stats = store.stats(run_id)
        print(render_snapshot(read_snapshot(store, run_id, errors=args.errors), width=24))
        errors = store.errors(run_id=run_id, limit=args.errors)
        if errors:
            print("\nFailure details:")
            print(
                _table(
                    [
                        [e["name"], e["failed_task"], e["error_type"], str(e["error_message"])[:60]]
                        for e in errors
                    ],
                    ["pipeline", "task", "error", "message"],
                )
            )
        # Repair failures: associate them with the pipelines in this report, not globally.
        # The repair event's run_id may differ from the pipeline row's run_id (the row stays
        # owned by the original failed run), so we derive the scope from the selected pipelines.
        report_pipelines = store.pipelines(run_id=run_id)
        repair_failed_pids = set()
        repair_details = []
        for row in report_pipelines:
            pid = row.pipeline_id
            count = count_events(store, kind="pipeline.terminal_repair_failed", pipeline_id=pid)
            if count > 0:
                repair_failed_pids.add(pid)
                # Get the latest failure detail for this pipeline
                evs = store.events(kind="pipeline.terminal_repair_failed", pipeline_id=pid, limit=1)
                if evs:
                    repair_details.append((pid, evs[-1].data.get("phase", "?"), count))
        if repair_failed_pids:
            print(f"\nTerminal repair failures: {len(repair_failed_pids)} pipeline(s)")
            for pid, phase, count in sorted(repair_details):
                suffix = f" ({count}x)" if count > 1 else ""
                print(f"  - {pid}: {phase}{suffix}")
        stats["repair_failures"] = len(repair_failed_pids)
        if args.json:
            print(json.dumps(stats, ensure_ascii=False, indent=2))
    finally:
        store.close()
    return 0


def _report_merged(paths: list[str], args: argparse.Namespace) -> int:
    """A report over several shards: rows are de-duplicated first, then statistics recomputed."""
    stores = [_open_readonly(path, args.artifact_backend) for path in paths]
    try:
        merged = merge_reports(stores, run_id=args.run_id)
    finally:
        for store in stores:
            store.close()
    print(merged.summary())
    if args.json:
        print(json.dumps(merged.stats(), ensure_ascii=False, indent=2))
    return 0  # a report is a read-only view; exit codes belong to `run`


def _latest_run_id(store: Any) -> str | None:
    rows = store.pipelines(limit=100000)
    return rows[-1].run_id if rows else None


# ------------------------------------------------------------------- watch
def _cmd_watch(args: argparse.Namespace) -> int:
    store = _open_readonly(args.store)
    try:
        run_id = args.run_id or _latest_run_id(store)
        asyncio.run(
            watch(
                store,
                run_id=run_id,
                interval=args.interval,
                clear=not args.no_clear,
                iterations=args.iterations,
            )
        )
    except KeyboardInterrupt:
        return 0
    finally:
        store.close()
    return 0


# ------------------------------------------------------------------ export
def _cmd_export(args: argparse.Namespace) -> int:
    paths = list(args.store)
    if len(paths) > 1 and args.rows == "pipelines":
        # Several shards: merging is what makes the file usable (the same pipeline can appear in
        # more than one shard after a shard-count change), so it is not optional here.
        stores = [_open_readonly(path, args.artifact_backend) for path in paths]
        try:
            merged = merge_reports(stores, run_id=args.run_id)
            count = merged.export(args.output, fmt=args.format)
        finally:
            for store in stores:
                store.close()
        print(f"exported {count} merged pipelines from {len(paths)} stores → {args.output}")
        return 0

    stores = [_open_readonly(path, args.artifact_backend) for path in paths]
    try:
        if len(stores) == 1:
            count = export_store(
                stores[0], args.output, kind=args.rows, fmt=args.format, run_id=args.run_id
            )
        else:
            count = export_stores(
                stores, args.output, kind=args.rows, fmt=args.format, run_id=args.run_id
            )
    finally:
        for store in stores:
            store.close()
    print(f"exported {count} {args.rows} rows ({args.format}) → {args.output}")
    return 0


# ------------------------------------------------------------------- serve
def _cmd_serve(args: argparse.Namespace) -> int:
    """Read-only HTTP view of a store: a dashboard for humans, JSON for everything else."""
    if not Path(args.store).exists():
        raise ConfigError(f"store does not exist: {args.store} (run something first)")
    server = StatsServer(args.store, host=args.host, port=args.port, run_id=args.run_id).start()
    print(f"serving {args.store} at {server.url}  (read-only; Ctrl-C to stop)")
    print(f"  {server.url}/            dashboard")
    print(f"  {server.url}/stats       JSON snapshot")
    print(f"  {server.url}/events?limit=50")
    try:
        server.wait()
    except KeyboardInterrupt:  # pragma: no cover - interactive
        pass
    finally:
        server.stop()
    return 0


# ----------------------------------------------------------------- plugins
def _cmd_plugins(args: argparse.Namespace) -> int:
    """What is installed right now, and what failed to load."""
    rows = list_plugins()
    if args.json:
        print(json.dumps({"plugins": rows, "errors": PLUGINS.errors}, ensure_ascii=False, indent=2))
        return 0
    if not rows:
        groups = ", ".join(sorted({f"pyattacker.{g}" for g in ("tasks", "algorithms", "codecs", "stores")}))
        print("no plugins installed")
        print(f"declare one with an entry-point group such as: {groups}")
        print('example:  [project.entry-points."pyattacker.tasks"]')
        print('          my_judge = "my_pkg.tasks:my_judge"')
        return 0
    print(
        _table(
            [[r["group"], r["name"], r["target"], "ok" if r["ok"] else "FAILED"] for r in rows],
            ["group", "name", "target", "status"],
        )
    )
    for key, message in PLUGINS.errors.items():
        print(f"  ! {key}: {message}", file=sys.stderr)
    return 0


# ---------------------------------------------------------------- validate
def _cmd_validate(args: argparse.Namespace) -> int:
    spec = load_spec(args.config, strict_env=args.strict_env)
    print(json.dumps(spec.describe(), ensure_ascii=False, indent=2))
    _warn_unresolved_env(spec, leading_blank_line=True)
    return 0


# -------------------------------------------------------------------- demo
def _demo_pipeline(fail_rate: float = 0.0):
    fetcher = echo.with_overrides(name="demo.fetch")
    ask = simulate_llm(fail_rate=fail_rate, latency_ms=4.0, tokens=64, name="demo.ask", resource="apis")
    judge = simulate_llm(fail_rate=fail_rate / 2, latency_ms=3.0, tokens=16, name="demo.judge", resource="judge")
    metrics = echo.with_overrides(name="demo.metrics")
    return pipeline("demo.qa", fetcher | ask | judge | metrics, tags={"demo": True})


def _cmd_demo(args: argparse.Namespace) -> int:
    apis = Pool(
        "apis",
        [
            Resource.create("llm", id=f"api-{i}", capacity=2, options={"model": "gpt-4o", "api_key": "sk-demo"})
            for i in range(1, 4)
        ],
        algorithm="backoff",
    )
    judge_pool = Pool(
        "judge",
        [Resource.create("llm", id="judge-1", capacity=1, options={"model": "gpt-4o-mini"})],
        algorithm="wait",
    )
    template = _demo_pipeline(fail_rate=args.fail_rate)
    runner = Runner(
        store=args.store,
        pools=[apis, judge_pool],
        concurrency=args.concurrency,
        config=RunConfig(
            store=args.store,
            concurrency=args.concurrency,
            label="demo",
            handle_signals=False,
        ),
    )
    with runner:
        report = runner.run(template.map({"i": i, "q": f"question-{i}"} for i in range(args.pipelines)))
        print(report.summary())
        print("\nFinal resource pool state:")
        print(
            _table(
                [
                    [s["id"], s["kind"], s["state"], f"{s['active']}/{s['capacity']}", s["leases"], s["failed"]]
                    for s in apis.snapshot() + judge_pool.snapshot()
                ],
                ["resource", "kind", "state", "active", "leases", "failed"],
            )
        )
        print("\nRecent events (structured log, queryable per pipeline):")
        for event in runner.store.events(limit=8):
            print(f"  [{event.scope}] {event.kind} {json.dumps(event.data, ensure_ascii=False, default=str)[:90]}")
        if args.export:
            count = report.export_jsonl(args.export)
            print(f"\nExported {count} pipeline records → {args.export}")
            print(f"Hint: pyattacker report {args.store}")
    return 0


# -------------------------------------------------------------------- main
def _add_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-c", "--config", required=True, help="yaml/toml/json config")
    parser.add_argument("--limit", type=int, default=None, help="run only the first N pipelines")
    parser.add_argument(
        "--store",
        default=None,
        help="override run.store (used verbatim with --shard; otherwise run.store gets a shard suffix)",
    )
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--journal", choices=["full", "summary"], default=None)
    parser.add_argument("--label", default=None)
    parser.add_argument("--resume", action="store_true", help="skip completed work and resume from the checkpoint")
    parser.add_argument("--retry-succeeded", action="store_true")
    parser.add_argument(
        "--fresh-restart",
        action="store_true",
        help="start over from the seed: discard checkpoints/traversal and reset the control budget",
    )
    parser.add_argument("--strict-leases", action="store_true", help="treat leaked leases as a task failure")
    parser.add_argument("--stop-after-failures", type=int, default=None)
    parser.add_argument("--no-signals", action="store_true")
    parser.add_argument(
        "--artifact-backend",
        default=None,
        metavar="SPEC",
        help="where artifact payloads live: inline (default), null, file:///path, or a JSON spec",
    )
    parser.add_argument("--progress", action="store_true", help="print live progress from a second connection")
    parser.add_argument(
        "--shard",
        default=None,
        metavar="I/N",
        help="run only this shard of the dataset (each shard writes its own store file)",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=None,
        help="convenience: spawn N children locally, one per shard, then merge their reports",
    )
    parser.add_argument("--jobs", type=int, default=None, help="how many shard children run at once")
    parser.add_argument(
        "--summary-format",
        choices=["text", "json"],
        default="text",
        help="json prints one summary object per run (used by --shards)",
    )
    parser.add_argument(
        "--no-write-behind",
        action="store_true",
        help="commit every attempt/event immediately instead of batching them",
    )
    parser.add_argument(
        "--strict-env",
        action="store_true",
        help="fail (exit 2) instead of warning when the config references an unset ${VAR}",
    )


# ---------------------------------------------------------------- bench
def _cmd_bench(args: argparse.Namespace) -> int:
    """Compare acquire algorithms on a simulated provider. Simulation only: no network, ever."""
    from .benchmark import METRICS, SCENARIOS, get_scenario, run_benchmark
    from .benchmark.report import default_algorithms

    if args.list:
        print("scenarios:")
        for name, scenario in SCENARIOS.items():
            print(f"  {name}  ({scenario.jobs} jobs, {scenario.concurrency} workers, {scenario.horizon_s:.0f}s horizon)")
            print(f"      {scenario.summary}")
            if scenario.unsuited:
                unsuited = "; ".join(f"{algorithm} ({reason})" for algorithm, reason in scenario.unsuited)
                print(f"      not applicable here, run only if asked for: {unsuited}")
        print("\nalgorithms (all built-ins; each scenario skips the ones it marks not applicable):")
        print("  " + ", ".join(default_algorithms()))
        print("\nmetrics (name [unit] direction):")
        for name, metric in METRICS.items():
            print(f"  {name} [{metric.unit}] {metric.better}: {metric.description}")
        return 0

    scenario = get_scenario(args.scenario)
    overrides = {}
    if args.jobs is not None:
        overrides["jobs"] = args.jobs
    if args.concurrency is not None:
        overrides["concurrency"] = args.concurrency
    if args.horizon is not None:
        overrides["horizon_s"] = args.horizon
    if overrides:
        scenario = scenario.with_overrides(**overrides)
    algorithms = [name.strip() for name in args.algorithms.split(",") if name.strip()] if args.algorithms else None
    # Validated here rather than only in the library so the announcement below cannot describe a sweep
    # that will not happen: `--seeds 0` used to print "x 0 seed(s)" and then quietly run one.
    if args.seeds < 1:
        raise ConfigError(f"--seeds must be at least 1, got {args.seeds}: a sweep needs a seed to be reproducible")
    if args.wall_budget <= 0:
        raise ConfigError(f"--wall-budget must be positive, got {args.wall_budget}")
    if args.speedup <= 0:
        raise ConfigError(f"--speedup must be positive, got {args.speedup}")
    if algorithms:
        for name, reason in scenario.unsuited:
            if name in algorithms:
                print(
                    f"warning: {name} is marked not applicable to {scenario.name} ({reason}); "
                    "its column will be marked N/A",
                    file=sys.stderr,
                )

    def progress(result) -> None:
        if args.quiet:
            return
        metrics = result.metrics
        print(
            f"  {result.algorithm:<12} seed {result.seed}  "
            f"{metrics['jobs_done']:.0f} done / {metrics['jobs_failed']:.0f} failed  "
            f"{metrics['throughput_rps']:.2f} jobs/s  util {metrics['utilization']:.2f}  "
            f"({metrics['wall_s']:.1f}s wall)",
            file=sys.stderr,
        )

    if not args.quiet:
        # The header counts what will actually run: a scenario that cannot exercise an algorithm
        # leaves it out, and the count has to agree with the columns and with `run_benchmark`.
        planned = algorithms if algorithms else default_algorithms(scenario)
        print(
            f"benchmarking {len(planned)} algorithms "
            f"x {args.seeds} seed(s) on {scenario.name} "
            f"({scenario.jobs} jobs x {scenario.steps_per_job} steps x {scenario.calls_per_step} calls, "
            f"{scenario.concurrency} workers)",
            file=sys.stderr,
        )
    factory = None
    if args.clock == "real":
        from .benchmark import ScaledClock

        factory = lambda: ScaledClock(args.speedup)  # noqa: E731 - a one-line factory reads better here
    report = run_benchmark(
        scenario,
        algorithms,
        seeds=args.seeds,
        wall_budget=args.wall_budget,
        on_run=progress,
        clock_factory=factory,
    )

    if args.json:
        payload = json.dumps(report.to_dict(), ensure_ascii=False, indent=2)
        if args.json == "-":
            print(payload)
        else:
            path = Path(args.json)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload + "\n", encoding="utf-8")
            print(f"wrote {path}", file=sys.stderr)
    if args.markdown:
        path = Path(args.markdown)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report.render_markdown() + "\n", encoding="utf-8")
        print(f"wrote {path}", file=sys.stderr)
    if not args.quiet or not (args.json or args.markdown):
        print(report.render_table())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyattacker",
        description="artifact-centric async task orchestration: pipeline / task / resource pool",
    )
    parser.add_argument("--version", action="version", version=f"pyattacker {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="run pipelines from a declarative config")
    _add_run_args(p_run)
    p_run.set_defaults(func=_cmd_run)

    p_resume = sub.add_parser("resume", help="equivalent to run --resume")
    _add_run_args(p_resume)
    p_resume.set_defaults(func=lambda args: _cmd_run(args, resume=True))

    p_report = sub.add_parser("report", help="print statistics and failures (several stores = merged view)")
    p_report.add_argument("store", nargs="+", help="one store, or several shard stores to merge")
    p_report.add_argument("--run-id", default=None)
    p_report.add_argument("--errors", type=int, default=10)
    p_report.add_argument("--json", action="store_true")
    p_report.add_argument("--artifact-backend", default=None, metavar="SPEC")
    p_report.set_defaults(func=_cmd_report)

    p_watch = sub.add_parser("watch", help="live monitoring (a separate process can watch the same store)")
    p_watch.add_argument("store")
    p_watch.add_argument("--run-id", default=None)
    p_watch.add_argument("--interval", type=float, default=1.0)
    p_watch.add_argument("--iterations", type=int, default=None)
    p_watch.add_argument("--no-clear", action="store_true")
    p_watch.set_defaults(func=_cmd_watch)

    p_export = sub.add_parser("export", help="export records as jsonl/json/csv")
    p_export.add_argument("store", nargs="+", help="one store, or several shard stores to merge")
    p_export.add_argument("output")
    p_export.add_argument("--run-id", default=None, help="export only pipelines touched by one run")
    p_export.add_argument("--rows", choices=list(ROW_KINDS), default="pipelines")
    p_export.add_argument("--format", choices=list(FORMATS), default="jsonl", dest="format")
    p_export.add_argument("--artifact-backend", default=None, metavar="SPEC")
    p_export.set_defaults(func=_cmd_export)

    p_serve = sub.add_parser("serve", help="serve a read-only HTTP view of a store")
    p_serve.add_argument("store")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8787)
    p_serve.add_argument("--run-id", default=None)
    p_serve.set_defaults(func=_cmd_serve)

    p_plugins = sub.add_parser("plugins", help="list installed plugins (entry points)")
    p_plugins.add_argument("--json", action="store_true")
    p_plugins.set_defaults(func=_cmd_plugins)

    p_validate = sub.add_parser("validate", help="validate a declarative config and print a summary")
    p_validate.add_argument("-c", "--config", required=True)
    p_validate.add_argument("--strict-env", action="store_true")
    p_validate.set_defaults(func=_cmd_validate)

    p_bench = sub.add_parser("bench", help="benchmark acquire algorithms against a simulated provider")
    p_bench.add_argument("--scenario", default="bursty_provider", help="which simulated world to run in (--list shows them)")
    p_bench.add_argument("--list", action="store_true", help="list scenarios, algorithms and metrics, then exit")
    p_bench.add_argument(
        "--algorithms",
        default=None,
        help="comma-separated names (default: every algorithm the scenario can exercise; see --list)",
    )
    p_bench.add_argument("--seeds", type=int, default=3, help="how many seeds to average over")
    p_bench.add_argument("--jobs", type=int, default=None, help="override the scenario's job count")
    p_bench.add_argument("--concurrency", type=int, default=None, help="override the scenario's worker count")
    p_bench.add_argument("--horizon", type=float, default=None, help="override the scenario's simulated-time horizon")
    p_bench.add_argument("--wall-budget", type=float, default=600.0, help="real seconds any single run may take")
    p_bench.add_argument(
        "--clock",
        choices=("virtual", "real"),
        default="virtual",
        help="virtual = simulated time that costs nothing; real = the same scenario in compressed real time "
        "(used to validate the simulator, and much slower)",
    )
    p_bench.add_argument("--speedup", type=float, default=10.0, help="compression factor for --clock real")
    p_bench.add_argument("--json", default=None, metavar="PATH", help="write the full report as JSON ('-' for stdout)")
    p_bench.add_argument("--markdown", default=None, metavar="PATH", help="write the report as a markdown table")
    p_bench.add_argument("--quiet", action="store_true", help="no progress on stderr")
    p_bench.set_defaults(func=_cmd_bench)

    p_demo = sub.add_parser("demo", help="run simulated tasks with zero config to verify the install")
    p_demo.add_argument("--store", default="runs/demo.db")
    p_demo.add_argument("--pipelines", type=int, default=50)
    p_demo.add_argument("--concurrency", type=int, default=8)
    p_demo.add_argument("--fail-rate", type=float, default=0.15, help="simulated failure ratio, useful for observing retries")
    p_demo.add_argument("--export", default=None)
    p_demo.set_defaults(func=_cmd_demo)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2
    except PyAttackerError as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:  # pragma: no cover
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
