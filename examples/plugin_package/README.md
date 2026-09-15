# Example plugin package

A minimal, complete third-party plugin: two tasks (one plain, one factory), an acquisition
algorithm and a codec — each published through a `pyattacker.*` entry-point group.

```bash
# from the repository root
uv pip install -e examples/plugin_package --no-deps
uv run pyattacker plugins                     # they are listed now
uv run pyattacker validate -c examples/plugin_tasks.yaml
uv run pyattacker run -c examples/plugin_tasks.yaml
```

Resolution order is **built-ins → plugins → `module:attribute`**, so a plugin can never shadow
`echo`, `wait` or any other built-in by accident. A plugin that raises on import is recorded in
`pyattacker plugins` output instead of breaking the run.

| Group | Value | What it must look like |
|---|---|---|
| `pyattacker.tasks` | a `TaskSpec`, or a factory returning one | `use: word_count` in a config |
| `pyattacker.algorithms` | a class with `name` + `acquire(...)`, or an instance | `algorithm: least_loaded` |
| `pyattacker.codecs` | a `Codec` instance or a zero-arg class | chosen automatically by payload type |
| `pyattacker.stores` | `factory(spec, *, journal) -> Store`, keyed by URI scheme | `store: "s3://bucket/runs.db"` |
