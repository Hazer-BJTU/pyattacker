<p align="center">
  <img src="https://raw.githubusercontent.com/Hazer-BJTU/pyattacker/ff819b1710d26eadf7c2e89465044975aef8eb8e/assets/logo/title.png" width="600" alt="pyattacker">
</p>

[![PyPI](https://img.shields.io/pypi/v/pyattacker)](https://pypi.org/project/pyattacker/)
[![CI](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml/badge.svg)](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/pyattacker)](https://pypi.org/project/pyattacker/)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE)

[English](README.md) | **简体中文**

> 把数万个独立任务一路跑到完成——可恢复、可观测，而且不必
> 第五次重新实现端点池、重试和“哪些行已经跑过”。

## 它要解决的场景

设想一个耗时很长的工作流测试、一次模型基准测试，或任何由许多独立条目组成的实验。
三小时过去，一个网络请求失败，进程随之死掉——而你完全不知道每个条目实际上
走到了 *哪个阶段*。从头重跑意味着要为每个已经
成功的请求再付一次代价，于是你开始写 `results.jsonl`，外加一段“跳过已有内容”的检查。

接着提供方开始对你限流。单个 API key 会把你并行的请求串成
一条队列，于是你加了第二个 key、第三个 key，而现在你需要决定哪个请求去往哪里、当某个端点开始返回 429
时会发生什么，以及一次失败到底意味着 *重试* 还是 *放弃*。
在这中间的某个时刻，`asyncio.Semaphore` 不再够用，你开始写一个调度器。

pyattacker 就是把那个调度器抽取出来，并让它变得平淡无奇：

* **一次失败只让你损失一个任务，而不是整轮运行**——每个任务的输出在产生的那一刻就被持久化，
  因此 `resume` 从持久化的任务检查点继续；当崩溃发生在检查点持久化之前时，外部副作用仍然需要
  幂等性；
* **端点是资源池（pool），而不是全局变量**——每个端点都有容量、健康状态和配额，通过 `async with` 获取租约，
  并有七种策略来决定用哪一个以及如何等待；
* **记录是可查询的，而不是一个日志文件**——每一次尝试、每一个重试决策（`{retry, reason,
  delay_s, error_class}`）、每一个中间工件，都存放在 SQLite 里，你在一轮运行仍在
  继续时就能对它 `SELECT`。

它 **不碰网络**：openai/anthropic 的调用由你写，它负责处理这些调用周围的一切。
基础安装完全没有任何依赖——项目中唯一的第三方代码是一个 YAML
解析器，它是可选的，只有 `.yaml`/`.yml` 配置才需要它。

**第一次来？** [教程](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md) 从一个五行程序一直讲到分片、可恢复的
模型评测。其中的每个代码片段都由测试套件实际执行。

## 安装

```bash
uv add pyattacker            # or: pip install pyattacker
uv add "pyattacker[yaml]"    # only for .yaml/.yml configs; JSON and TOML need no extra
```

从克隆的仓库安装：

```bash
git clone https://github.com/Hazer-BJTU/pyattacker && cd pyattacker
uv sync
uv run pyattacker demo       # zero-config smoke test: 50 simulated pipelines, retries, a report
```

需要 Python 3.11+。`import pyattacker`、CLI 以及除 YAML 之外的每种配置格式，在未安装任何
第三方包时都能工作；缺少该 extra 的 `.yaml`/`.yml` 配置属于配置错误（退出码 2），
并会指出需要安装哪个 extra。

## 核心模型

五个概念，这就是全部词汇：

| 概念 | 含义 | 一句话 |
|---|---|---|
| **工件**（artifact） | 任务被持久化的状态 | 内容寻址，**一经产生立即持久化** → 检查点粒度 = 任务 |
| **任务**（task） | 调度的最小单位 | 一元 `(artifact) -> artifact` 函数，同步或异步——或者 `-> artifact \| Handoff`，用于向前跳过（[进阶](README.zh-CN.md#进阶交接可选启用)） |
| **流水线**（pipeline） | 完成的单位 | `fetch \| ask \| judge \| metrics` 线性串联，彼此在语义上相互独立 |
| **资源**（resource） | 一种可租用的外部能力 | 一个端点 / 一个 key；一旦放进资源池，就能以并发安全的方式发布和订阅 |
| **算法**（algorithm） | 获取资源的策略 | `wait`、`backoff`、`least_busy`、`failover`、`sticky`、`quota_aware`、`immediate`——与“失败时重试”正交 |

## 30 秒快速上手（SDK）

下面的程序是完整的：它定义了用到的每个名字，可离线运行，并且每次提交都由测试
套件执行。其中只涉及内置的模拟——不需要 API key，也不需要网络。

```python
# example/readme_quickstart.py
"""A first run: one custom task, a pool of two simulated endpoints, four pipelines, one report."""
from pyattacker import Pool, Resource, Retrying, Runner, pipeline, task


@task("fetch", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, base=0.5, cap=30.0), timeout_s=60)
async def fetch(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:                     # returned on exit; returned on exception too
        await ctx.clock.sleep(0.001)                       # stand-in for the network call
        lease.report(ok=True, usage={"tokens": 128})       # report health/quota back to the pool
        return {"q": row["q"], "a": f"answer from {lease.resource.id}"}


def dataset_rows():
    """Your dataset. A generator works too: `map` streams it, so memory stays O(concurrency)."""
    return [{"q": f"question-{i}"} for i in range(4)]


pool = Pool("apis",
            [Resource.create("llm", id=f"api-{i}", capacity=4) for i in range(1, 3)],
            algorithm="backoff")

template = pipeline("qa", fetch)

with Runner(store="runs/qa.db", pools=[pool], concurrency=8) as runner:
    report = runner.run(template.map(dataset_rows()))
    print(report.summary())
    report.export_jsonl("runs/qa.jsonl")
```

`ctx.acquire()` 会回退到任务声明的 `resource`，而 `lease.report(ok=...)` 会为资源池的
健康与配额核算提供数据。

真正发起调用的地方在哪里——这部分属于你，而且它要在框架已经跑通之后再加：

```python
@task("ask", resource="apis", retry={"max_attempts": 3}, timeout_s=60)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:
        text = await lease.client.chat(row["q"])         # lease.client is built by resource.factory
        return {"q": row["q"], "a": text}

pool = Pool("apis",
            [Resource.create("llm", capacity=4, options={"model": "gpt-4o"},
                             factory=lambda res: MyClient(res.options)) for _ in range(8)],
            algorithm="backoff")
```

`MyClient` 是你的客户端类。`factory` 对每个资源只调用一次，而且是惰性调用，发生在第一次获取租约时；
它返回什么，`lease.client` 就是什么——pyattacker 自己从不打开 socket。

想要 pass@k？`template.map(rows, repeats=3)`——同一个种子会展开成三条相互独立的流水线。

## 租约安全：框架最难的保证

资源租用是你的代码与框架之间最关键的交互点，因此：

```python
async with ctx.acquire(resource) as lease:   # ← the only recommended form
    for chunk in chunks:                      # repeatedly acquiring/releasing inside a loop is fine too
        ...
```

* 异常、取消、`timeout_s` 超时——**总是会被归还**；
* 通过逃生通道 `await ctx.acquire_lease()` 忘了归还？任务结束时它会被 **强制回收**，
  并记录一个 `lease.leaked` 事件；
* 强制回收是一个 **纯同步函数**，`CancelledError` 无法打断它——这正是“任务结束后
  绝不持有资源”能够成立的原因；
* 需要更严格的行为时，用 `strict_leases=True`：一次泄漏会立刻让该任务失败（`LeaseLeakError`）。

## 信守承诺的调度

* **重试退避不会占住 worker。** 当策略想要再试一次时，流水线会被挂起到延迟队列，
  worker 随即去接手其他工作。因此 `concurrency` 意味着 *在途的尝试数*，而不是
  *坐着等 30 秒退避的流水线数*。
* **挂起的流水线绝不会被误认为死掉的流水线。** 一轮运行的结束也会等待挂起的流水线；
  停止会把它们记录为 `interrupted`，检查点保持完好，因此 `resume` 会把它们接续起来。
* **资源池的唤醒是定向的。** 归还一个资源只会唤醒选择器能使用它的那些等待者，
  而不是所有被阻塞的流水线。
* **历史是批处理的，检查点不是。** 尝试和事件按批写入（按大小、按间隔、
  按心跳以及运行结束时），而工件和检查点总是立即提交。`SIGKILL` 可能让你丢掉最后一批历史，
  但绝不会丢掉检查点。`--no-write-behind` 可以关掉这一行为。
* **等待是被度量的。** 资源池统计会报告 `waits_total`、`wait_ms_avg`、`p50`、`p95` 和 `max`，而等待
  超过 `slow_wait_ms` 会发出一个 `acquire.slow_wait` 事件，你可以对它告警。

## 语义恢复

每个成功的任务都会持久化它的工件并推进检查点。恢复时：

```python
runner.run(template.map(rows), resume=True)   # or pyattacker resume -c config.yaml
```

* 任务/输入同一性匹配且已经成功的流水线 → 直接跳过；
* 失败的流水线 → 从 **第一个未产出工件的任务** 继续：**如果任务 C 挂了，那么只要任务 B 的检查点已持久化，
  就只有任务 C 会重跑**；
* 种子工件也会被持久化 → 恢复 **不依赖原始数据集文件**；
* **发生了交接**的流水线（[进阶](README.zh-CN.md#进阶交接可选启用)）会在它跳转到的那个站点恢复，
  并使用账本记录的入口状态——发起交接的那个任务不会重跑；
* 修改了任务的源代码（`spec_digest` 包含源码摘要）→ 视为新的流水线，因此旧结果
  不会被错误地复用。工厂参数、fanout 子任务、重试/算法策略以及显式的任务 `config`/`version` 也都包含在内。
  显式 key 会拒绝已变更的定义或输入。

**升级已有的存储：** fingerprint v2 会改变默认 ID 和分片分配；遗留的工作可能会重跑，
旧的显式 key 会冲突。请用旧版本的包把旧的运行跑完，然后换用新的存储。迁移指导、动态函数
和外部配置见 [恢复同一性与幂等性参考](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#恢复同一性)
文档。

## 进阶：交接（可选启用）

**进阶层级：可选启用、会改变执行模型、普通流水线不需要、在 1.0 之前属于实验性。**
任务可以判定链的剩余部分不再需要运行，并 *明确说出来*，而不是伪造一次失败或把分支藏在某一步之中。
它会 **返回** 一条指令——`Handoff.to(target, value)` 用于在声明的后续站点继续，
`Handoff.end(value)` 用于就地结束流水线：

```python
# example/readme_handoff.py
"""A gate that skips the stations it does not need, and records why."""

from pyattacker import Handoff, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"q": seed["q"], "confidence": seed["confidence"]}


@task("ask")
async def ask(row: dict, ctx) -> dict:
    await ctx.clock.sleep(0.001)                       # <- your HTTP call
    return {**row, "answer": f"answer-for:{row['q']}"}


@task("judge")
def judge(row: dict) -> Handoff | dict:
    if row["confidence"] >= 0.9:
        return Handoff.end({**row, "verdict": "confident"}, reason="already good enough")
    if row["confidence"] >= 0.5:
        return Handoff.to("report", {**row, "verdict": "ok"}, reason="metrics not needed")
    return {**row, "verdict": "needs-metrics"}


@task("metrics")
def metrics(row: dict) -> dict:
    return {**row, "score": round(row["confidence"] * 10, 1)}


@task("report")
def report(row: dict) -> dict:
    return {**row, "reported": True}


# Edges are declared, never derived: judge may continue at report, or end the pipeline.
template = pipeline("qa", prepare | ask | judge | metrics | report,
                    control={"edges": {"judge": ["report", "end"]}})

with Runner(store=":memory:", concurrency=4) as runner:
    report_obj = runner.run(template.map([
        {"q": "2+2", "confidence": 0.95},                        # judge ends the pipeline here
        {"q": "17*23", "confidence": 0.6},                       # judge skips metrics, continues at report
        {"q": "prove sqrt(2) is irrational", "confidence": 0.2},  # no handoff: the whole chain runs
    ]))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        ran = [t.name for t in store.tasks(record.pipeline_id)]
        hops = store.handoffs(pipeline_id=record.pipeline_id)
        print(f"{record.state}  ran={ran}  skipped={len(template.tasks) - len(ran)}  handoffs={len(hops)}")
```

* **声明式的正向边。** 沿着未声明的边、或来自没有
  `control` 块的流水线的 `Handoff`，都是致命的配置错误——绝不重试，也绝不是静默跳转。目标必须
  严格晚于其来源；从最后一个任务发起 `end` 会被拒绝，因为那不会做任何事。
* **交接是一种处置结果，而不是失败。** 它是一个返回值，因此重试策略永远看不到它，
  任务侧的 `except Exception:` 也无法吞掉它；租约的归还与成功时完全一样，而
  被取消或超时的尝试永远不会走到这个返回。
* **它是一个持久化的检查点。** 跳转是原子提交的（源任务、尝试、入口工件、
  账本行和游标一起提交），因此被杀的进程会带着记录的入口状态 **在目标处** 恢复，
  不会重跑源任务。在启用控制流的流水线上，`n_tasks_done` 是一个 *位置*，
  而不是进度计数：被跳过的站点没有任务行。`report`/`watch` 统计所选
  范围内的提交数（过滤时是单次运行，否则是全部历史）；`/pipelines.handoffs` 统计活跃执行记录，
  `handoffs_historical` 统计全部账本记录。一次恢复可以使用早先某次运行的活跃交接，同时不记录新的交接。
  流水线导出会包含账本同一性和水位，用来区分这些范围。
* **可选启用且保持惰性。** 没有 `control` 块时什么都不会改变——一行数据、一个计数器，
  以及 `spec_digest` 的一个字节都不会变。
* **反向遍历需要单独声明。** `Handoff.rewind(target, value)` 把作者选定的状态
  发送给一个更早的任务；`Handoff.retry_all()` 从最初绑定的种子重新开始。需要声明
  `control.rewind` / `control.retry_all` 以及有限的 `control.max_handoffs`。可选的 `HistoryArtifact`
  载荷提供显式的快照与恢复；普通字典仍然由作者控制。
  访问（visits）与确切的工件出现记录都会保留历史，使恢复安全。API、预算与恢复边界见
  [reference → 进阶：反向遍历](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#进阶反向遍历rewindretry-allvisits)，
  可运行的程序见
  [tutorial 第 16–17 步](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md#第-16-步--高级用回退和全部重试重新生成)。

API 就是一个类（[`Handoff`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#进阶交接可选启用)）、
一个声明——正向跳转用 `control={"edges": {...}}`，反向遍历用 `control.rewind` / `control.retry_all` /
`control.max_handoffs`——再加一项可选的存储能力；无法原子提交交接的自定义存储会被当场拒绝，
而不是写入一个扛不住崩溃的跳转。
演练：[教程第 15 步](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md#第-15-步--高级跳过站点交接)。
模型与规则：[设计 §4.8](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md#48-进阶交接--声明式正向跳转可选启用实验性)。

当流水线从种子重新开始时，先前的任务行和链上的工件会与游标/水位一起被原子重置；
先前的交接仍留在它的历史中，但不再充当
检查点。之后的恢复只遵循当前这一次执行的交接，而完成时会选定一个
最终工件。支持交接的自定义存储必须把持久化的 `handoff_floor` 水位与游标一起保存，
并实现原子的 `reset_pipeline(record)`；见 [恢复契约](docs/zh-CN/reference.md#表与读取器)。

## 跨进程运行（分片）

SQLite 只接受一个写入者，而内核是单个事件循环，所以扩展意味着 **多个进程各自拥有独立的存储**，
之后再合并。流水线的分片来自它的内容寻址 key，因此同一个数据集总是以相同的
方式切分，而 `--resume` 会把每条流水线放回它原来的位置：

```bash
# convenience: N children, then a merged report
uv run pyattacker run -c examples/qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db
# -> runs/qa.shard0of4.db … runs/qa.shard3of4.db

# or drive each shard yourself (cluster, scheduler, four terminals)
uv run pyattacker run -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
uv run pyattacker resume -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db

# one coherent answer out of N files (de-duplicated, statistics recomputed)
uv run pyattacker report runs/qa.shard*of4.db
uv run pyattacker export runs/qa.shard*of4.db runs/all.jsonl
uv run pyattacker export runs/qa.shard*of4.db runs/tasks.csv --rows tasks --format csv
```

`--rows` 选择形状：`pipelines`（嵌套，默认——包含它的任务、工件和交接）、`tasks`、
`attempts`（包含每个重试 `decision`）、`events`、`artifacts`。`--format` 选择 `jsonl`、`json` 或 `csv`。

## 声明式（简单任务）

YAML 描述的是 **组合与资源**；逻辑留在 Python 里（`use: my_pkg.tasks:ask`）。
读取 `.yaml`/`.yml` 文件是 pyattacker 中唯一需要第三方库的部分，因此它属于
`yaml` extra；用 JSON 或 TOML 写的同样配置可用标准库读取。加载器会
按文件后缀逐个判断，并在解析器缺失时说明需要安装哪个 extra。

下面的代码块是一份完整的配置——每个 `use:` 目标都是内置的，因此它原样就能通过校验。
把它保存为 `qa.yaml`；`examples/qa_eval.yaml` 形状相同，只是注释更多。

```yaml
# example/readme_qa_eval.yaml
run:   { store: runs/demo.db, concurrency: 8, label: demo }
pools:
  apis:
    kind: llm
    algorithm: backoff
    resources: [ { id: api-1, capacity: 4, options: { model: gpt-4o } } ]
pipeline:
  name: qa
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: pyattacker.tasks:simulate_llm            # a factory: `kwargs` are its arguments
      resource: apis
      algorithm: backoff
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }
      retry: { max_attempts: 3, "on": [RetryableError, TimeoutError] }
source: { kind: range, n: 100 }
```

裸写 `on:` 是 YAML 1.1 的布尔键，而不是重试字段：重试相关的 key 必须加引号。加载器
会检出这个错误并明确指出来，而不是静默忽略该策略。

```bash
uv run pyattacker validate -c qa.yaml            # parse, check, print the effective config
uv run pyattacker run      -c qa.yaml --progress
uv run pyattacker watch    runs/demo.db          # open another process to monitor it live
uv run pyattacker report   runs/demo.db --errors 20
uv run pyattacker export   runs/demo.db out.jsonl
uv run pyattacker serve    runs/demo.db          # HTTP dashboard + JSON endpoints
uv run pyattacker plugins                        # installed plugins
```

退出码：`0` 全部成功 / `1` 部分失败 / `2` 配置错误 / `130` 中断。
每个子命令的每个标志：[`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/cli.md)。

## 记录与监控

一条流水线的完整记录 = 由 `pipeline_id` 查询的六张表：状态与检查点、
每个任务的最终状态、完整的尝试历史（包括**每一次重试决策**
`{retry, reason, delay_s, error_class}`）、中间与最终工件、控制流账本
（`handoffs`，普通流水线中为空），以及结构化事件流。
`pyattacker report/watch` 直接消费这些事实。

## 扩展

**插件**就是普通的 `importlib.metadata` 入口点 —— 安装一个包，它的名字便可在任意配置中
使用：

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"        # a TaskSpec, or a factory returning one
[project.entry-points."pyattacker.algorithms"]
my_algo  = "my_pkg.algo:MyAlgorithm"
[project.entry-points."pyattacker.stores"]
s3       = "my_pkg.s3:open_store"         # keyed by URI scheme: store = "s3://bucket/runs.db"
```

```bash
uv run pyattacker plugins                 # what is installed, and what failed to load
```

内置项优先解析（插件无法遮蔽 `echo`），导入时抛错的插件会被记录，
而不会致命。完整可运行的示例：[`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.zh-CN.md)。

**大载荷**可以存放在数据库之外：

```bash
uv run pyattacker run -c examples/qa_eval.yaml --artifact-backend file:///data/blobs
# or, in the config:  artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

文件按内容寻址、原子写入，并在读取时回填 —— 因此恢复后的运行会透明地
复用溢出的检查点。`null` 保留摘要、丢弃字节；`inline`（默认）
把一切保留在存储中。

**零依赖的监控端点**：

```bash
uv run pyattacker serve runs/qa.db        # http://127.0.0.1:8787
# /  dashboard   /stats  /events  /pipelines  /resources  /errors   (JSON)
```

它每次请求都会打开一个全新的只读连接，因此它能在正在运行的运行旁边正常工作。它**没有
认证**，并且只绑定回环地址：它会暴露你的载荷，所以请把它当作调试视图，不要在
没有自己的代理的情况下把它放到公网接口上。

**在步骤内分支** —— `fanout(a, b)` 在同一输入上并发运行多个任务，
返回 `{task_name: value}`。重试粒度变成整个组，这是不把流水线
变成 DAG 的诚实代价。组是 Runner 看到的唯一规格（spec），因此当子任务全部
一致时，`resource`、`algorithm` 和 `timeout_s` 从子任务继承（此时 `timeout_s`
约束整个组）。

## 算法基准测试

某个工作负载该用哪种获取算法？资源池自带七种，而诚实的答案取决于
提供方 —— 所以专门为此做了模拟。场景会陈述各项假设（容量周期、
压力下会收紧的令牌桶、带慢尾的延迟、相互独立的故障与
相关的风暴、三个特性各异的端点），客户端是由 worker 组成的闭环，
驱动真实的 `Pool` 与真实算法，时间是模拟的，因此十分钟的场景只需
几秒。

```bash
uv run pyattacker bench                       # the scenario's 5 algorithms x 3 seeds, about 13 seconds
uv run pyattacker bench --list                # the scenarios, the algorithms, and what each metric means
uv run pyattacker bench --algorithms wait,backoff --seeds 5 --json runs/bench.json
```

它刻意做成黑盒：提供方从不向算法暴露自身状态，它的“心情”是
时间的函数，而不是由谁在请求决定的，并且每次请求的抽样按序号索引 —— 因此
两个算法会遇到相同的*外生*随机性与相同的天气，比较发生在
算法之间而不是心情之间。（它们实际达成的提供方状态仍会分化，因为令牌桶
与在途数量会对各自的行为做出反应 —— 这种分化本身就是测量结果。）
它报告的是指标向量（吞吐量、尾延迟、重试、引发的拒绝、容量
利用率、跨端点公平性），而不是一个加权总分，并且会逐项指标给出胜者
—— 包括胜者是“无人”的情况、只剩一个算法可比较的情况，而绝不会给
只完成了一小部分工作的算法：一个放弃队列的策略无法靠
缩小分母来赢得一项速率指标。

[`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md) 中列出了各项假设、指标、当前数值，以及
它们没有说明的内容。

## 文档

| 文档 | 内容 |
|---|---|
| [`docs/tutorial.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md) | 十七个可运行的步骤，从“单个任务”到分片评估与高级的交接/反向层级——包含回退、全部重试与载荷历史；每个步骤都由测试套件执行 |
| [`docs/reference.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md) | 所有公开的类与函数：签名、参数、示例——包括高级的反向遍历层级与 `HistoryArtifact` |
| [`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/cli.md) | 每个子命令、每个标志、退出码、配置参考 |
| [`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md) | 概念模型、六条不变量、租约契约、数据模型、权衡 |
| [`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md) | 算法基准测试：场景假设了什么、各指标含义、如何阅读表格 |
| [`CHANGELOG.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/CHANGELOG.md) | 每个发布版本改动了什么 |
| [`docs/releasing.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/releasing.md) | 面向维护者：一个发布版本如何切出并发布 |
| [`docs/`](https://github.com/Hazer-BJTU/pyattacker/tree/main/docs) | 这些文档的英文原版；`tests/test_docs_i18n.py` 保证中英代码块、标题与链接保持同步 |

## 示例

| 示例 | 展示内容 |
|---|---|
| [`examples/quickstart.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/quickstart.py) | 60 行代码里的 SDK：自定义客户端工厂、重试、恢复 |
| [`examples/llm_eval/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/llm_eval/README.zh-CN.md) | 一次完整的评估 —— prepare → 2-turn model call → 3 个评委 → reduce，采用**两种流水线形态**，并*实测*了检查点粒度的权衡（grouped 重发了 2 个本已成功的评委请求；split 重发 0 个） |
| [`examples/sharded.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/sharded.py) | 一个数据集分布在 N 个存储上，然后合并出报告 |
| [`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.zh-CN.md) | 一个真实可安装的插件：任务、一个算法、一个编解码器 |
| [`examples/qa_eval.yaml`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/qa_eval.yaml) | 声明式路径的端到端演示 |

```bash
uv run python examples/quickstart.py
uv run python -m examples.llm_eval.demo
uv run python examples/sharded.py
uv run pyattacker run -c examples/qa_eval.yaml --limit 40
```

## 不在范围内

这些是设计决策，而不是缺失的功能：

* **网络请求** —— openai/anthropic 协议由你自己编写。内核从不打开套接字。
* **语义归约** —— accuracy、pass@k、F1 以及任何跨流水线聚合。把工件导出后
  在外部计算，或者用这些原语写一条汇聚流水线。
* **DAG 编排** —— 流水线是一条线性链；在任务内部用 `fanout` 分支。唯一的
  例外是可选启用的 [交接](README.zh-CN.md#进阶交接可选启用)：它会沿声明的边改变
  链的遍历顺序，而绝不改变其拓扑（没有汇合节点、没有第二个入口点、没有跨流水线的
  跳转）。
* **服务网关** —— 唯一的 HTTP 界面就是上面那个只读调试端点。
* **分布式调度** —— 用 `--shard` 横向扩展；多进程就是上限。

[`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md) 的第 1 节和第 11 节精确陈述了这条边界，第 8 节则列出了
每一项已知权衡及其原因。

## 状态

**未发布 —— 高级控制流（可选启用）。** 任务现在可以
[交接](README.zh-CN.md#进阶交接可选启用)：返回一个 `Handoff` 来跳过已声明的站点或提前结束流水线，
并记录在持久账本中，恢复时会从该账本继续。它是可选启用且惰性的 —— 没有
`control` 块的流水线不会写入新行，并保持逐字节相同的 `spec_digest` —— 且在 1.0 之前
标记为实验性。反向遍历现在加入了声明的回退与全部重试，以及 visits 和可选的载荷历史。

**0.2.0 —— 一项基准测试、更严格的同一性，以及三处正确性修复。** 新增：`pyattacker bench`，一个
模拟的提供方世界，它按一组指标构成的向量来比较各获取算法，而不是给出一个加权
分数（[`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md)）；
`Retrying.decide`，把重试决策作为策略上的方法；以及按类别分页的整体读取
（`pyattacker.store.iter_*`，由可选的 `PagedStore` 扩展提供支持），因此导出大型存储不再
需要把它整体物化。0.1.x 本身已实现 M0–M4 计划的一切：内核、持久化
与任务级恢复、重试与错误分类、带 7 种获取算法的资源池、
延迟延续与 write-behind 批处理、分片与合并报告、三种格式下的五种导出形态、
入口点插件、外部工件后端、fan-out 辅助函数，以及 HTTP 监控
端点。

有三处改动值得在升级前读一读。**PyYAML 不再是依赖** —— 基础安装
完全不含任何依赖，因此 `.yaml`/`.yml` 配置需要 `pip install "pyattacker[yaml]"`。**`with_overrides`
区分“未给出”与 `None`**：省略的关键字保留原值，显式传入的 `None` 现在会清除
`resource`/`algorithm`/`timeout_s`/`version`，而新的 `UNSET` 哨兵在调用方
转发 dict 时表示“未给出”。**统一的校验入口**现在同时支撑 `validate` 和所有 `run` 模式，因此
它能接受的配置就是能运行的配置：未知字段、类型错误、错误的资源池引用、格式错误的工件后端
以及拼写错误，都会在任何东西启动之前以退出码 2 和字段路径失败。导出的 `limit` 现在对每种
行类别都只表示一个含义，而 `None` 表示完整导出。

自 0.1.1 起修复：任务被取消时 `shell_run` 泄漏了子进程，超时时只杀死而不
回收 —— 现在每条退出路径（包括创建进程）都会杀死并回收，在 POSIX 上还会向整个
进程组发送信号；导出事件会在最新的 100 000 行处静默停止；声明式 `run:` 块
丢弃了 `artifact_backend`、write-behind 和批处理开关，`--no-write-behind` 在单进程下
从未生效，`--artifact-backend` 也从未传递到分片子进程；恢复现在会拒绝任务或
种子摘要已变化的流水线键，而不是悄悄复用陈旧结果；基准测试也收集了自己的一批修复
（见变更日志）。该 API 还很年轻：从此遵循语义化版本，但在
1.0 之前请预期仍会有调整。

留待以后：分布式调度器、Parquet 导出、blob 垃圾回收，以及一等公民的
`Parallel`/`Gather` 节点。

## 开发

```bash
uv sync                      # create the venv + install the dev group (which includes the optional yaml extra)
uv run pytest                # the whole suite: zero network, a few seconds
uv run ruff check            # lint (configuration lives in pyproject.toml, with reasons for each exception)
uv run pyattacker demo       # end-to-end smoke test
uv run pyattacker bench      # compare the acquire algorithms in simulation (about 13 seconds)
uv build                     # sdist + wheel
```

测试离线且具有确定性（时间经由可注入的 `Clock`）。教程中的代码块由
`tests/test_tutorial.py` 提取并执行，因此会腐坏的文档会让 CI 失败。

## 许可证

MIT —— 见 [LICENSE](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE)。

