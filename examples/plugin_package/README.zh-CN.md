# 示例插件包

[English](README.md) | **简体中文**

一个最小而完整的第三方插件：两个任务（一个普通任务、一个工厂任务）、一种获取
算法和一个编解码器——每一项都通过某个 `pyattacker.*` 入口点组发布。

```bash
# from the repository root
uv pip install -e examples/plugin_package --no-deps
uv run pyattacker plugins                     # they are listed now
uv run pyattacker validate -c examples/plugin_tasks.yaml
uv run pyattacker run -c examples/plugin_tasks.yaml
```

解析顺序是 **内置项 → 插件 → `module:attribute`**，因此插件绝不会意外遮蔽
`echo`、`wait` 或任何其他内置项。导入时抛出异常的插件会被记录在
`pyattacker plugins` 的输出里，而不会让运行中断。

| 组 | 值 | 它必须是什么样子 |
|---|---|---|
| `pyattacker.tasks` | 一个 `TaskSpec`，或返回它的工厂 | 配置中的 `use: word_count` |
| `pyattacker.algorithms` | 一个带 `name` + `acquire(...)` 的类，或一个实例 | `algorithm: least_loaded` |
| `pyattacker.codecs` | 一个 `Codec` 实例或无参类 | 由载荷类型自动选择 |
| `pyattacker.stores` | `factory(spec, *, journal) -> Store`，以 URI scheme 为键 | `store: "s3://bucket/runs.db"` |
