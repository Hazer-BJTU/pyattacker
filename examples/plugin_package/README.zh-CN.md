# 示例插件包

[English](README.md) | **简体中文**

一个最小但完整的第三方插件示例：两个任务（一个普通任务、一个工厂任务）、一种资源获取算法、一个编解码器——每一项都通过 `pyattacker.*` 入口点注册。

```bash
# from the repository root
uv pip install -e examples/plugin_package --no-deps
uv run pyattacker plugins                     # they are listed now
uv run pyattacker validate -c examples/plugin_tasks.yaml
uv run pyattacker run -c examples/plugin_tasks.yaml
```

解析顺序是 **内置项 → 插件 → `module:attribute`**，所以插件不会意外覆盖 `echo`、`wait` 或任何内置功能。导入时挂掉的插件会在 `pyattacker plugins` 输出里列出来，不会让整个运行中断。

| 入口点组 | 注册的内容 | 用法示例 |
|---|---|---|
| `pyattacker.tasks` | 一个 `TaskSpec`，或返回它的工厂函数 | 配置里写 `use: word_count` |
| `pyattacker.algorithms` | 带 `name` + `acquire(...)` 方法的类，或直接是实例 | `algorithm: least_loaded` |
| `pyattacker.codecs` | 一个 `Codec` 实例，或无参类 | 根据产物类型自动匹配 |
| `pyattacker.stores` | `factory(spec, *, journal) -> Store`，按 URI scheme 注册 | `store: "s3://bucket/runs.db"` |
