"""Plugin discovery (entry points) and the built-in ``fanout`` task factory.

Everything here runs without installing anything: :class:`PluginRegistry` takes an injectable
provider, so a fake entry point with ``.name`` / ``.value`` / ``.load()`` is enough.

Coverage
* ``GROUPS`` -> entry-point group names; discovery, caching, sorted names, refresh
* unknown group -> ``PluginError``; a provider that raises and an entry point whose ``.load()``
  raises are recorded in ``.errors`` and skipped, never propagated
* resolvers: ``task`` / ``algorithm`` / ``codec`` / ``store_factory`` (URI-scheme keyed) /
  ``describe`` / ``install_codecs`` (including a registration failure)
* integration: ``declarative.resolve_target``, ``algorithm.resolve_algorithm`` and
  ``store.open_store`` find a plugin through the module-level ``PLUGINS``, and built-ins still win
* ``fanout``: concurrent branches on the same input, raise/collect error modes, retry-policy
  adoption, and the two ``ValueError`` contracts
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any

import pytest
from helpers import run

from pyattacker import Backoff, MemoryStore, Retrying, Runner, pipeline, task
from pyattacker.algorithm import resolve_algorithm
from pyattacker.artifact import CodecRegistry, Encoded
from pyattacker.declarative import resolve_target
from pyattacker.errors import ConfigError, PluginError, PoolNotFound, RetryableError
from pyattacker.plugins import GROUPS, PluginRegistry, list_plugins
from pyattacker.store import open_store
from pyattacker.task import build_task_spec
from pyattacker.tasks import BUILTIN_TASKS, echo, fanout


# ----------------------------------------------------------------- fake entry points
class FakeEntryPoint:
    """Exactly the three members ``EntryPointLike`` needs, plus a load counter."""

    def __init__(self, name: str, value: str, loader: Callable[[], Any]) -> None:
        self.name = name
        self.value = value
        self._loader = loader
        self.loads = 0

    def load(self) -> Any:
        self.loads += 1
        return self._loader()


def _explode(message: str) -> Any:
    raise ValueError(message)


def _fake_registry(
    entries_by_group: Mapping[str, Mapping[str, FakeEntryPoint]],
    calls: list[str] | None = None,
) -> PluginRegistry:
    def provider(group: str) -> Mapping[str, FakeEntryPoint]:
        if calls is not None:
            calls.append(group)
        return entries_by_group.get(group, {})

    return PluginRegistry(provider)


# --------------------------------------------------------------- discovery and caching
def test_groups_map_to_entry_point_group_names():
    assert GROUPS == {
        "tasks": "pyattacker.tasks",
        "algorithms": "pyattacker.algorithms",
        "codecs": "pyattacker.codecs",
        "stores": "pyattacker.stores",
    }

    seen: list[str] = []
    registry = PluginRegistry(provider=lambda group: seen.append(group) or {})
    registry.entry_points("tasks")
    registry.entry_points("stores")
    assert seen == ["pyattacker.tasks", "pyattacker.stores"]  # short names are mapped, not passed through


def test_entry_points_names_and_caching():
    entries = {
        "b": FakeEntryPoint("b", "pkg:b", lambda: "B"),
        "a": FakeEntryPoint("a", "pkg:a", lambda: "A"),
    }
    calls: list[str] = []

    def provider(group: str) -> Mapping[str, FakeEntryPoint]:
        calls.append(group)
        return entries

    registry = PluginRegistry(provider)
    assert registry.names("tasks") == ["a", "b"]  # sorted
    assert registry.entry_points("tasks") == entries
    assert calls == ["pyattacker.tasks"]  # discovered once, then cached
    assert registry.names("tasks") == ["a", "b"]
    assert calls == ["pyattacker.tasks"]
    assert registry.errors == {}
    assert registry.load("tasks", "missing") is None  # unknown name is not an error


def test_unknown_group_raises_plugin_error_everywhere():
    registry = PluginRegistry(provider=lambda group: {})
    with pytest.raises(PluginError) as excinfo:
        registry.entry_points("nope")
    assert str(excinfo.value) == (
        "unknown plugin group 'nope'; available: ['algorithms', 'codecs', 'stores', 'tasks']"
    )
    for call in (
        lambda: registry.names("nope"),
        lambda: registry.load("nope", "x"),
        lambda: registry.load_all("nope"),
    ):
        with pytest.raises(PluginError, match="unknown plugin group 'nope'"):
            call()


# --------------------------------------------------------- failure tolerance contract
def test_provider_failure_is_recorded_and_skipped():
    def provider(group: str) -> Mapping[str, FakeEntryPoint]:
        raise RuntimeError("entry points unavailable")

    registry = PluginRegistry(provider)
    assert registry.entry_points("tasks") == {}
    assert registry.names("tasks") == []
    assert registry.load("tasks", "anything") is None
    assert registry.load_all("tasks") == {}
    assert registry.errors == {"tasks:*": "RuntimeError: entry points unavailable"}
    assert registry.describe() == []  # a dead provider is reported, not raised


def test_broken_entry_point_is_recorded_skipped_and_cached():
    good = FakeEntryPoint("good", "pkg:good", lambda: 41 + 1)
    broken = FakeEntryPoint("broken", "pkg:broken", lambda: _explode("nope"))
    registry = _fake_registry({"pyattacker.tasks": {"good": good, "broken": broken}})

    assert registry.load_all("tasks") == {"good": 42}  # the broken one is skipped
    assert broken.loads == 1
    assert registry.load("tasks", "broken") is None
    assert broken.loads == 1  # a failed load is cached too: it is not retried on every call
    assert registry.errors == {"tasks:broken": "ValueError: nope"}

    rows = {row["name"]: row for row in registry.describe()}
    assert rows["broken"] == {
        "group": "tasks",
        "name": "broken",
        "target": "pkg:broken",
        "ok": False,
        "error": "ValueError: nope",
    }
    assert rows["good"] == {
        "group": "tasks",
        "name": "good",
        "target": "pkg:good",
        "ok": True,
        "error": None,
    }
    assert list_plugins(registry) == registry.describe()


def test_refresh_forgets_discovery_and_errors():
    entries: dict[str, FakeEntryPoint] = {"a": FakeEntryPoint("a", "pkg:a", lambda: "A")}
    calls: list[str] = []
    registry = _fake_registry({"pyattacker.tasks": entries}, calls=calls)

    assert registry.load_all("tasks") == {"a": "A"}
    assert calls == ["pyattacker.tasks"]
    registry.refresh()
    assert registry.errors == {}
    assert registry.load_all("tasks") == {"a": "A"}
    assert calls == ["pyattacker.tasks", "pyattacker.tasks"]  # refreshed: the provider is consulted again


# ------------------------------------------------------------------------ resolvers
class _PluginAlgo:
    name = "plug.algo"

    def __init__(self, tag: str = "ok") -> None:
        self.tag = tag

    async def acquire(self, pool: Any, **kwargs: Any) -> str:  # pragma: no cover - shape only
        return "lease"


class _NeedyAlgo:
    def __init__(self, required: Any) -> None:  # no default: instantiation must fail
        self.required = required


class _AcquireStub:
    name = "stub"

    async def acquire(self, pool: Any, **kwargs: Any) -> str:  # pragma: no cover - shape only
        return "lease"


class _CodecInstance:
    name = "plug-codec"

    def can_encode(self, obj: Any) -> bool:
        return False

    def dumps(self, obj: Any) -> bytes:  # pragma: no cover - shape only
        return b""

    def loads(self, data: bytes) -> bytes:  # pragma: no cover - shape only
        return data


def test_task_algorithm_codec_and_store_resolvers():
    marker = object()
    stub = _AcquireStub()
    codec_instance = _CodecInstance()

    def store_factory(spec: str, **kwargs: Any) -> tuple[str, str, dict[str, Any]]:
        return ("store", spec, kwargs)

    registry = _fake_registry(
        {
            "pyattacker.tasks": {"task_a": FakeEntryPoint("task_a", "pkg.tasks:task_a", lambda: marker)},
            "pyattacker.algorithms": {
                "algo_a": FakeEntryPoint("algo_a", "pkg.algo:_PluginAlgo", lambda: _PluginAlgo),
                "algo_factory": FakeEntryPoint("algo_factory", "pkg.algo:make", lambda: (lambda: stub)),
                "needy": FakeEntryPoint("needy", "pkg.algo:_NeedyAlgo", lambda: _NeedyAlgo),
            },
            "pyattacker.codecs": {
                "codec_a": FakeEntryPoint("codec_a", "pkg.codec:_CodecInstance", lambda: codec_instance),
                "codec_class": FakeEntryPoint("codec_class", "pkg.codec:_CodecInstance", lambda: _CodecInstance),
            },
            "pyattacker.stores": {"scheme_a": FakeEntryPoint("scheme_a", "pkg.store:open", lambda: store_factory)},
        }
    )

    assert registry.task("task_a") is marker
    assert registry.task("missing") is None

    algo = registry.algorithm("algo_a")  # a class is instantiated
    assert type(algo) is _PluginAlgo and algo.tag == "ok"
    assert registry.algorithm("algo_factory") is stub  # a zero-arg factory is called
    assert registry.algorithm("needy") is None
    assert registry.errors["algorithms:needy"].startswith("cannot instantiate:")
    assert "_NeedyAlgo.__init__()" in registry.errors["algorithms:needy"]

    assert registry.codec("codec_a") is codec_instance  # an instance passes through
    assert type(registry.codec("codec_class")) is _CodecInstance  # a class is instantiated
    assert registry.codec("missing") is None

    assert registry.store_factory("scheme_a://bucket/run.db") is store_factory
    assert registry.store_factory("scheme_a://anything") is store_factory
    assert registry.store_factory("plain/path.db") is None  # no "://" -> never even looked up
    assert registry.store_factory("scheme_b://x") is None  # unknown scheme


class _UpperCodec:
    """A codec whose output makes it obvious the plugin (not json/bytes) was used."""

    name = "upper"

    def can_encode(self, obj: Any) -> bool:
        return False

    def dumps(self, obj: Any) -> bytes:
        return str(obj).upper().encode("utf-8")

    def loads(self, data: bytes) -> str:
        return data.decode("utf-8").lower()


class _Widget:
    def __init__(self, label: str) -> None:
        self.label = label


class _WidgetCodec:
    """A zero-arg codec *class* — the documented entry-point shape."""

    name = "widget"

    def can_encode(self, obj: Any) -> bool:
        return isinstance(obj, _Widget)

    def dumps(self, obj: Any) -> bytes:
        return b"widget!"

    def loads(self, data: bytes) -> _Widget:
        return _Widget(data.decode("utf-8"))


class _NeedyCodec:
    """Looks like a codec class, but its constructor needs an argument."""

    name = "needy"

    def __init__(self, required: Any) -> None:  # pragma: no cover - never reached when the bug is fixed
        self.required = required


def test_install_codecs_registers_instances_and_records_failures():
    upper = FakeEntryPoint("upper", "pkg.codec:_UpperCodec", lambda: _UpperCodec())
    broken = FakeEntryPoint("broken", "pkg.codec:broken", lambda: _explode("cannot build"))
    registry = _fake_registry({"pyattacker.codecs": {"upper": upper, "broken": broken}})

    codecs = CodecRegistry()
    assert registry.install_codecs(codecs) == ["upper"]
    assert registry.errors == {"codecs:broken": "ValueError: cannot build"}
    encoded = Encoded(type_name="str", codec="upper", data=b"HELLO", digest="d", size=5)
    assert codecs.load_raw(encoded) == "hello"

    class _PickyRegistry:
        def register(self, codec: Any, *, name: str | None = None) -> None:
            raise ValueError(f"refusing {name}")

    assert registry.install_codecs(_PickyRegistry()) == []
    assert registry.errors["codecs:upper"] == "ValueError: refusing upper"
    assert registry.errors["codecs:broken"] == "ValueError: cannot build"


def test_install_codecs_instantiates_a_zero_arg_codec_class():
    entry = FakeEntryPoint("widget", "pkg.codec:_WidgetCodec", lambda: _WidgetCodec)
    registry = _fake_registry({"pyattacker.codecs": {"widget": entry}})

    codecs = CodecRegistry()
    assert registry.install_codecs(codecs) == ["widget"]
    encoded = codecs.dump(_Widget("x"))  # must use the plugin codec, not json/bytes
    assert encoded.codec == "widget"
    assert encoded.data == b"widget!"


def test_install_codecs_records_a_codec_class_that_cannot_be_instantiated():
    entry = FakeEntryPoint("needy", "pkg.codec:_NeedyCodec", lambda: _NeedyCodec)
    registry = _fake_registry({"pyattacker.codecs": {"needy": entry}})

    # A broken plugin must be recorded and skipped, never propagated (plugins.py module contract).
    assert registry.install_codecs(CodecRegistry()) == []
    assert "codecs:needy" in registry.errors
    assert "required" in registry.errors["codecs:needy"]  # the cause is reported, not swallowed


# ------------------------------------------------------- integration with the kernel
def test_resolve_target_finds_plugins_but_builtins_win(monkeypatch):
    marker = object()
    entries = {
        "my_task": FakeEntryPoint("my_task", "pkg.tasks:my_task", lambda: marker),
        "echo": FakeEntryPoint("echo", "pkg.tasks:echo", lambda: "plugin echo"),
    }
    monkeypatch.setattr("pyattacker.plugins.PLUGINS", _fake_registry({"pyattacker.tasks": entries}))

    assert resolve_target("my_task") is marker  # found after the built-ins
    assert resolve_target("echo") is BUILTIN_TASKS["echo"]  # a plugin cannot shadow a built-in

    with pytest.raises(ConfigError) as excinfo:
        resolve_target("missing_task")
    message = str(excinfo.value)
    assert "cannot resolve use: 'missing_task'" in message
    assert "installed task plugins: ['echo', 'my_task']" in message


def test_resolve_algorithm_finds_plugins_but_builtins_win(monkeypatch):
    entries = {
        "my_algo": FakeEntryPoint("my_algo", "pkg.algo:_PluginAlgo", lambda: _PluginAlgo),
        "backoff": FakeEntryPoint("backoff", "pkg.algo:_PluginAlgo", lambda: _PluginAlgo),
    }
    monkeypatch.setattr("pyattacker.plugins.PLUGINS", _fake_registry({"pyattacker.algorithms": entries}))

    plugin = resolve_algorithm("my_algo")
    assert type(plugin) is _PluginAlgo and plugin.name == "plug.algo"
    assert type(resolve_algorithm({"name": "my_algo"})) is _PluginAlgo

    builtin = resolve_algorithm("backoff")
    assert type(builtin) is Backoff and builtin.name == "backoff"

    with pytest.raises(PoolNotFound, match="does not take parameters"):
        resolve_algorithm({"name": "my_algo", "base": 0.5})


def test_open_store_uses_a_store_plugin_keyed_by_uri_scheme(monkeypatch, tmp_path):
    specs: list[tuple[str, dict[str, Any]]] = []

    def store_factory(spec: str, **kwargs: Any) -> MemoryStore:
        specs.append((spec, kwargs))
        return MemoryStore(journal=kwargs.get("journal", "full"))

    entries = {"memdb": FakeEntryPoint("memdb", "pkg.store:open_store", lambda: store_factory)}
    groups: list[str] = []
    monkeypatch.setattr(
        "pyattacker.plugins.PLUGINS", _fake_registry({"pyattacker.stores": entries}, calls=groups)
    )

    store = open_store("memdb://runs/x", journal="summary")
    try:
        assert specs == [("memdb://runs/x", {"journal": "summary"})]
        assert store.journal == "summary"
    finally:
        store.close()
    assert groups == ["pyattacker.stores"]

    # no "://" -> the store plugin group is never consulted at all
    plain = open_store(str(tmp_path / "plain.db"))
    plain.close()
    assert groups == ["pyattacker.stores"]


# ------------------------------------------------------------------------- fanout
class _RecordingCtx:
    """fanout only ever calls ``ctx.emit``."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def emit(self, kind: str, **data: Any) -> None:
        self.events.append((kind, data))


FANOUT_SYNC: dict[str, Any] = {}


async def _needs_peer_a(value: Any, ctx: Any) -> dict[str, Any]:
    FANOUT_SYNC["a_ready"].set()
    await asyncio.wait_for(FANOUT_SYNC["b_ready"].wait(), 2.0)  # would time out if branches ran in series
    return {"branch": "a", "value": value}


async def _needs_peer_b(value: Any, ctx: Any) -> dict[str, Any]:
    FANOUT_SYNC["b_ready"].set()
    await asyncio.wait_for(FANOUT_SYNC["a_ready"].wait(), 2.0)
    return {"branch": "b", "value": value}


FANOUT_CALLS: dict[str, int] = {"stable": 0, "flaky": 0}


@task("fan.stable", retry=Retrying(max_attempts=1))
def fan_stable(value: Any, ctx: Any) -> dict[str, Any]:
    FANOUT_CALLS["stable"] += 1
    return {"branch": "stable", "value": value}


@task("fan.flaky", retry=Retrying(max_attempts=3, base=0.001, cap=0.002))
def fan_flaky(value: Any, ctx: Any) -> dict[str, Any]:
    FANOUT_CALLS["flaky"] += 1
    if FANOUT_CALLS["flaky"] < 3:
        raise RetryableError("flaky branch is not ready")
    return {"branch": "flaky", "attempt": ctx.attempt}


@task("fan.bad", retry=Retrying(max_attempts=1))
def fan_bad(value: Any, ctx: Any) -> Any:
    raise RetryableError("branch down", error_class="upstream")


def test_fanout_runs_branches_concurrently_on_the_same_input():
    async def _case() -> tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]:
        FANOUT_SYNC.clear()
        FANOUT_SYNC.update(a_ready=asyncio.Event(), b_ready=asyncio.Event())
        group = fanout(
            build_task_spec(_needs_peer_a, name="peer.a"),
            build_task_spec(_needs_peer_b, name="peer.b"),
        )
        ctx = _RecordingCtx()
        result = await group({"i": 1}, ctx)
        return result, ctx.events

    result = run(_case())
    assert result == (
        {
            "peer.a": {"branch": "a", "value": {"i": 1}},
            "peer.b": {"branch": "b", "value": {"i": 1}},
        },
        [("fanout.done", {"branches": 2, "failed": 0, "failed_branches": []})],
    )


def test_fanout_raise_keeps_the_original_exception_and_records_the_branch():
    group = fanout(fan_stable, fan_bad, name="fanout.raise")
    assert group.name == "fanout.raise"
    assert group.retry.max_attempts == 1

    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("fanout-raise", group).map([{"i": 1}]))

    assert report.stats["pipelines"]["by_state"] == {"failed": 1}
    attempt = runner.store.attempts()[0]
    assert attempt.error_type == "RetryableError"  # the original class survives, so retry classification works
    assert attempt.error_class == "upstream"  # the branch's own error_class is preserved
    assert attempt.error_message == "branch down"

    events = runner.store.events(limit=100)
    branch_failed = [e for e in events if e.kind == "fanout.branch_failed"]
    assert [(e.data["branch"], e.data["error"]) for e in branch_failed] == [
        ("fan.bad", "RetryableError: branch down")
    ]
    done = [e for e in events if e.kind == "fanout.done"]
    assert len(done) == 1
    assert (done[0].data["branches"], done[0].data["failed"], done[0].data["failed_branches"]) == (
        2,
        1,
        ["fan.bad"],
    )


def test_fanout_collect_never_raises_and_marks_every_branch():
    group = fanout(fan_stable, fan_bad, on_error="collect")
    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("fanout-collect", group).map([{"i": 5}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    row = next(iter(runner.store.export_rows()))
    assert row["artifacts"][-1]["payload"] == {
        "fan.stable": {"ok": True, "value": {"branch": "stable", "value": {"i": 5}}},
        "fan.bad": {"ok": False, "error": "RetryableError: branch down"},
    }


def test_fanout_adopts_the_most_forgiving_child_policy_and_uses_it():
    FANOUT_CALLS.update(stable=0, flaky=0)
    group = fanout(fan_stable, fan_flaky)
    assert group.retry is fan_flaky.retry  # the forgiving child's policy object, not a copy
    assert group.retry.max_attempts == 3

    runner = Runner(store=":memory:", handle_signals=False)
    report = runner.run(pipeline("fanout-retry", group).map([{"i": 9}]))

    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
    assert (FANOUT_CALLS["flaky"], FANOUT_CALLS["stable"]) == (3, 3)  # the group retried as a whole
    row = next(iter(runner.store.export_rows()))
    assert row["tasks"][0]["attempts_used"] == 3
    assert row["artifacts"][-1]["payload"] == {
        "fan.stable": {"branch": "stable", "value": {"i": 9}},
        "fan.flaky": {"branch": "flaky", "attempt": 3},
    }


def test_fanout_retry_override_wins_over_children():
    override = Retrying(max_attempts=7, base=0.001, cap=0.002)
    assert fanout(fan_stable, fan_flaky, retry=override).retry is override


def test_fanout_shares_a_resource_only_when_every_branch_agrees():
    shared_a = build_task_spec(_needs_peer_a, name="r.a", resource="apis")
    shared_b = build_task_spec(_needs_peer_b, name="r.b", resource="apis")
    unset = build_task_spec(_needs_peer_b, name="r.c")
    assert fanout(shared_a, shared_b).resource == "apis"
    assert fanout(shared_a, unset).resource is None


def test_fanout_requires_branches_and_valid_on_error():
    with pytest.raises(ValueError, match="fanout needs at least one task"):
        fanout()
    with pytest.raises(ValueError, match="on_error must be 'raise' or 'collect'"):
        fanout(echo, on_error="nonsense")
