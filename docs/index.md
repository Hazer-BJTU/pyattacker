# pyattacker

**English** | [简体中文](zh-CN/index.md)

Build resumable Python workflows for model evaluations and other dataset-driven tasks.
Combine task chains, shared resource pools, retries and durable checkpoints under one Runner.

[Start the tutorial](tutorial.md){ .md-button .md-button--primary }
[Python API reference](reference.md){ .md-button }

## Start here

Install Python 3.11 or later, then install the library:

```bash
python -m pip install pyattacker
pyattacker demo
```

For YAML configurations, install `pyattacker[yaml]`. The base library has no runtime dependencies.
This site follows the `main` branch; your installed release may not yet include every feature shown.
To try the development version, clone the repository and follow the [tutorial setup](tutorial.md#step-0--install-and-sanity-check).

## Choose a path

| Your goal | Read |
| --- | --- |
| Write a task and run a dataset | [Step-by-step tutorial](tutorial.md) |
| Share endpoints, retry failures and resume work | [Resources, retry and recovery tutorial](tutorial.md#find-what-you-need) |
| Compare several experiments under one Runner | [Experiment suites](suites.md) |
| Look up Python interfaces and behavior | [API reference](reference.md) |
| Run from JSON, TOML or YAML | [CLI and configuration](cli.md) |
| Understand checkpoints and scheduling guarantees | [Architecture](design.md) |
| Compare resource scheduling strategies | [Benchmarking](benchmark.md) |

## Know what is retained

`Runner` defaults to an in-memory store. Choose a file-backed store to retain checkpoints across
processes. `concurrency` limits executing attempts; `max_admitted` bounds queued, executing and
delayed pipelines together. Payloads, input buffers and accumulated store data also consume memory.
See [RunConfig](reference.md#runconfig) before scaling a workload.

## Explore and contribute

Browse the [runnable examples](https://github.com/Hazer-BJTU/pyattacker/tree/main/examples),
[release history](https://github.com/Hazer-BJTU/pyattacker/blob/main/CHANGELOG.md), or
[report an issue](https://github.com/Hazer-BJTU/pyattacker/issues).
For local previews and publishing, see [Maintaining the documentation](documentation.md).
