# pyattacker

[English](../index.md) | **简体中文**

为模型评估和其他数据集任务构建可恢复的 Python 工作流。
在一个 Runner 下组合任务链、共享资源池、重试与持久检查点。

[开始学习](tutorial.md){ .md-button .md-button--primary }
[Python API 参考](reference.md){ .md-button }

## 从这里开始

安装 Python 3.11 或更高版本，然后安装库：

```bash
python -m pip install pyattacker
pyattacker demo
```

使用 YAML 配置时安装 `pyattacker[yaml]`，基础库没有运行时依赖。
本站跟随 `main` 分支，已安装的发布版本可能尚未包含全部功能。
要体验开发版本，请克隆仓库并按照[教程](tutorial.md)中的安装步骤操作。

## 选择阅读路径

| 你的目标 | 阅读 |
| --- | --- |
| 编写任务并运行数据集 | [分步教程](tutorial.md) |
| 共享端点、重试失败和恢复工作 | [资源、重试与恢复教程](tutorial.md) |
| 在一个 Runner 下比较多个实验 | [实验套件](suites.md) |
| 查询 Python 接口和行为 | [API 参考](reference.md) |
| 使用 JSON、TOML 或 YAML 驱动运行 | [CLI 与配置](cli.md) |
| 理解检查点和调度保证 | [架构设计](design.md) |
| 比较资源调度策略 | [基准测试](benchmark.md) |

## 理解数据保留方式

`Runner` 默认使用内存存储。需要跨进程恢复时，请选择文件存储。
`concurrency` 限制执行中的尝试数；`max_admitted` 限制排队、执行及延迟流水线的总数。
载荷、输入缓冲和存储累积数据也会占用内存。扩大工作负载前请阅读
[RunConfig](reference.md#runconfig)。

## 探索与贡献

查看[可运行示例](https://github.com/Hazer-BJTU/pyattacker/tree/main/examples)、
[更新记录](https://github.com/Hazer-BJTU/pyattacker/blob/main/CHANGELOG.md)，或
[反馈问题](https://github.com/Hazer-BJTU/pyattacker/issues)。
本地预览与发布方法见[文档维护](documentation.md)。
