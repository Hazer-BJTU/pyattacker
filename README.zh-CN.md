<p align="center">
  <img src="https://raw.githubusercontent.com/Hazer-BJTU/pyattacker/ff819b1710d26eadf7c2e89465044975aef8eb8e/assets/logo/title.png" width="600" alt="pyattacker">
</p>

[![PyPI](https://img.shields.io/pypi/v/pyattacker)](https://pypi.org/project/pyattacker/)
[![CI](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml/badge.svg)](https://github.com/Hazer-BJTU/pyattacker/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/pyattacker)](https://pypi.org/project/pyattacker/)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE)

[English](README.md) | **简体中文**

> 把几万条独立任务一口气跑完——崩了能续、跑着能看，而且不用第五次重写端点池、重试逻辑和"哪些行已经跑过"。

## 它解决什么问题

想象你在跑一个超长的工作流测试、一轮模型评测，或者任何由大量独立条目组成的实验。三小时过去了，一个网络请求挂了，进程跟着死了——而你完全不知道每条数据到底跑到了*哪一步*。从头重跑？那已经成功的请求又得付一遍钱。于是你开始写 `results.jsonl`，再加个"跳过已完成的"判断。

紧接着，接口方开始限流。一把 API key 把你的并发请求串成了单队列，你只好加第二把、第三把——然后问题来了：哪个请求走哪把 key？某个端点开始返回 429 怎么办？一次失败到底该*重试*还是*放弃*？折腾到最后，`asyncio.Semaphore` 已经不够用了，你发现自己在写一个调度器。

pyattacker 就是那个调度器——把它抽出来，打磨到"无聊"的程度：

* **崩了只损失一个任务，不丢整轮进度。** 每个任务的输出一产生就立刻落盘，`resume` 直接从持久化的检查点接着跑。当然，如果你在检查点落盘之前就崩了，外部副作用还是得自己做幂等。
* **端点是资源池，不是全局变量。** 每个端点有自己的容量、健康状态和配额，通过 `async with` 租用，内置 7 种策略决定选哪个、怎么等。
* **记录可查询，不是只能翻日志。** 每一次尝试、每一次重试决策（`{retry, reason, delay_s, error_class}`）、每一个中间产物，全在 SQLite 里——跑着的时候就能直接 `SELECT`。

它**不碰网络**：openai/anthropic 的调用你自己写，它只管调用之外的所有事。基础安装零依赖——整个项目唯一的第三方代码是一个 YAML 解析器，而且是可选的，只有用 `.yaml`/`.yml` 配置时才需要。

**第一次来？** [教程](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md) 从五行代码讲起，一路讲到分片、可恢复的模型评测。里面每个代码片段都有测试跑着，不会过期。

## 安装

```bash
uv add pyattacker            # or: pip install pyattacker
uv add "pyattacker[yaml]"    # only for .yaml/.yml configs; JSON and TOML need no extra
```

从源码安装：

```bash
git clone https://github.com/Hazer-BJTU/pyattacker && cd pyattacker
uv sync
uv run pyattacker demo       # zero-config smoke test: 50 simulated pipelines, retries, a report
```

需要 Python 3.11+。`import pyattacker`、CLI 以及除 YAML 外的所有配置格式，在不装任何第三方包的情况下都能跑。缺了 `yaml` extra 就用 `.yaml`/`.yml` 配置？会报配置错误（退出码 2），并告诉你该装哪个 extra。

## 核心模型

五个概念，就这些：

| 概念 | 含义 | 一句话 |
|---|---|---|
| **artifact（产物）** | 任务持久化后的状态 | 内容寻址，**一产生就落盘** → 检查点粒度 = 任务 |
| **task（任务）** | 调度的最小单位 | 一个一元函数 `(artifact) -> artifact`，同步异步都行——也可以 `-> artifact \| Handoff` 来跳过后续步骤（[交接](README.zh-CN.md#交接可选启用)） |
| **pipeline（流水线）** | 完成和恢复的单位 | `fetch \| ask \| judge \| metrics` 线性串起来，各步语义独立 |
| **resource（资源）** | 可租用的外部能力 | 一个端点 / 一把 key；放进资源池后可以并发安全地发布和订阅 |
| **algorithm（算法）** | 获取资源的策略 | `wait`、`backoff`、`least_busy`、`failover`、`sticky`、`quota_aware`、`immediate`——和"失败了怎么重试"是两回事 |

## 30 秒上手（SDK）

下面这段程序是完整的：所有用到的名字都定义了，离线能跑，每次提交测试套件都会执行它。只用了内置的模拟器——不需要 API key，不需要网络。

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

`ctx.acquire()` 不指定参数时，自动用任务声明的 `resource`。`lease.report(ok=...)` 把健康和配额信息喂回资源池。

真正的网络调用写在哪？——你自己写，等框架跑通了再加：

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

`MyClient` 是你自己的客户端类。`factory` 对每个资源只调一次，而且是懒加载——第一次租约的时候才调。它返回什么，`lease.client` 就是什么。pyattacker 自己不开 socket。

要 pass@k？`template.map(rows, repeats=3)`——同一条种子数据自动展开成三条互相独立的流水线。

## 租约安全：框架最硬的保证

资源租用是你和框架之间最关键的交互点，所以：

```python
async with ctx.acquire(resource) as lease:   # ← the only recommended form
    for chunk in chunks:                      # repeatedly acquiring/releasing inside a loop is fine too
        ...
```

* 不管是异常、取消还是 `timeout_s` 超时，租约**一定会被归还**。
* 用了逃生通道 `await ctx.acquire_lease()` 忘了还？任务结束时会被**强制回收**，同时记一条 `lease.leaked` 事件。
* 回收是**纯同步操作**，`CancelledError` 打断不了它——这就是"任务结束后绝不占着资源不放"能做到的原因。
* 想要更严格？开 `strict_leases=True`：一旦泄漏，当前任务直接失败（`LeaseLeakError`）。

## 说到做到的调度

* **重试退避不占 worker。** 策略要求再试一次时，流水线被挂到延迟队列上，worker 立刻去接别的活。所以 `concurrency` 指的是*同时在跑的尝试数*，不是*排着队等退避的流水线数*。
* **挂起的流水线不会被当成死的。** 一轮跑完之前会等挂起的流水线。主动停止时把它们标记为 `interrupted`，检查点完好，`resume` 接着跑。
* **资源池唤醒是定向的。** 归还一个资源时，只叫醒能用它的那些等待者，不是所有阻塞的流水线都叫醒。
* **历史批量写，检查点即时写。** 尝试记录和事件按批次写入（按大小、按间隔、按心跳、以及跑完时），但 artifact 和检查点永远是立即提交的。`SIGKILL` 最多丢最后一批历史，绝不会丢检查点。不想要批量写？`--no-write-behind` 关掉。
* **等待时长有记录。** 资源池统计里有 `waits_total`、`wait_ms_avg`、`p50`、`p95` 和 `max`。等待超过 `slow_wait_ms` 会触发一条 `acquire.slow_wait` 事件，你可以拿去告警。

## 断点续跑

每个任务成功后，产物立刻落盘，检查点往前推。续跑时：

```python
runner.run(template.map(rows), resume=True)   # or pyattacker resume -c config.yaml
```

* 任务和输入都没变、已经成功的流水线 → 直接跳过。
* 失败的流水线 → 从**第一个没产出 artifact 的任务**接着跑：任务 C 挂了，只要 B 的检查点已经落盘，就只重跑 C。
* 种子数据也持久化了 → 续跑**不需要原始数据集文件**。
* 走过交接的流水线（[交接](README.zh-CN.md#交接可选启用)）会在跳转目标处恢复，用账本里记好的入口状态——发起跳转的那个任务不重跑。
* 改了任务源码（`spec_digest` 包含源码摘要）→ 视为全新流水线，不会拿旧结果瞎复用。工厂参数、fanout 子任务、重试/算法策略、显式的 `config`/`version` 都算在内。手动指定 key 的话，定义或输入变了会直接拒绝。

**升级已有存储：** fingerprint v2 改了默认 ID 和分片分配方式。旧任务可能会重跑，旧的手动 key 会冲突。建议用旧版本把手上的跑完，再换新存储。迁移指引、动态函数和外部配置详见 [恢复同一性与幂等性参考](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#恢复同一性)。

<a id="进阶交接可选启用"></a>

## 交接（可选启用）

**交接、正向跳转和回退都是正式的控制流特性，通过 `control` 显式声明后启用。**

任务可以判断"后面的步骤不用跑了"，然后*明确说出来*，而不是硬编一个失败或者把分支藏在某一步里面。它**返回**一条指令——`Handoff.to(target, value)` 表示跳到指定的后续步骤继续跑，`Handoff.end(value)` 表示直接收尾：

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

* **边是显式声明的，不是猜的。** 走了没声明的边，或者一个没有 `control` 块的流水线里出现了 `Handoff`，都是致命配置错误——不重试，也不会静默跳转。目标步骤必须在来源步骤之后；最后一步发起 `end` 会被拒绝（因为没意义）。
* **交接是一种处置结果，不是失败。** 它是个返回值，所以重试策略根本看不到它，任务里写的 `except Exception:` 也吞不掉它。租约的归还和成功时完全一样，被取消或超时的尝试压根走不到返回这一步。
* **它是持久化的检查点。** 跳转是原子提交的（源任务、尝试、入口产物、账本记录、游标一起提交），进程被杀了会在目标步骤恢复，带着记录好的入口状态，不会重跑源任务。开了控制流的流水线里，`n_tasks_done` 是个*位置*，不是进度计数——被跳过的步骤没有任务记录。`watch` 默认统计选定运行的提交数（`--run-id all` 查看全部历史）；`/pipelines.handoffs` 统计活跃执行记录，`handoffs_historical` 统计全部账本记录。续跑时可以用之前某次运行的活跃交接，同时不记录新的交接。导出的流水线数据里会带上账本身份和水位标记，用来区分这些范围。
* **不开就完全没影响。** 没有 `control` 块的流水线，一行数据、一个计数器、`spec_digest` 的一个字节都不会变。
* **往回跳需要单独声明。** `Handoff.rewind(target, value)` 把作者指定的状态送回更早的步骤；`Handoff.retry_all()` 从最初的种子重新开始。需要声明 `control.rewind` / `control.retry_all`，还要设一个有限的 `control.max_handoffs`。可选的 `HistoryArtifact` 载荷可以做显式快照和恢复；普通字典的控制权完全在作者手里。访问记录（visits）和精确的产物出现记录会保留历史，保证恢复安全。API、预算和恢复边界见 [reference → 反向遍历](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#反向遍历rewindretry-allvisits)，可运行的示例见 [tutorial 第 16–17 步](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md#第-16-步--用回退和全部重试重新生成)。

API 就是一个类（[`Handoff`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#交接可选启用)）、一条声明——正向跳转用 `control={"edges": {...}}`，反向遍历用 `control.rewind` / `control.retry_all` / `control.max_handoffs`——再加一项可选的存储能力。无法原子提交交接的自定义存储会直接被拒绝，不会让你写一个崩了就丢的跳转。

上手示例：[教程第 15 步](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md#第-15-步--跳过站点交接)。模型和规则：[设计 §4.8](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md#48-交接--声明式正向跳转可选启用)。

流水线从种子重新开始时，之前的任务记录和链上产物会和游标/水位一起原子重置。之前的交接还留在历史里，但不再当检查点用。之后的续跑只看当前这轮执行的交接，完成时选一个最终产物。支持交接的自定义存储必须把持久化的 `handoff_floor` 水位和游标一起存，还要实现原子的 `reset_pipeline(record)`。详见 [恢复契约](docs/zh-CN/reference.md#表与读取器)。

## 跨进程跑（分片）

SQLite 只允许一个写入者，内核又是单事件循环，所以横向扩展的方式是：**多进程各跑各的存储，最后合并**。每条流水线属于哪个分片，是由它的内容寻址 key 决定的——同一批数据永远按同样的方式切分，`--resume` 会把每条流水线放回原来的分片：

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

`--rows` 选导出形状：`pipelines`（嵌套，默认——带任务、产物和交接记录）、`tasks`、`attempts`（含每次重试的 `decision`）、`events`、`artifacts`。`--format` 选 `jsonl`、`json` 或 `csv`。

## 声明式配置（写 YAML）

YAML 只管**组装和资源**，业务逻辑还是写在 Python 里（`use: my_pkg.tasks:ask`）。读 `.yaml`/`.yml` 文件是 pyattacker 唯一需要第三方库的地方，所以做成了 `yaml` extra。同样的配置写成 JSON 或 TOML 就用标准库读，不装任何额外包。加载器按文件后缀自动判断格式，缺了解析器会告诉你该装哪个 extra。

下面这段是完整的配置——每个 `use:` 都是内置的，直接就能过校验。存成 `qa.yaml` 就能用；`examples/qa_eval.yaml` 是同款，只是注释更多。

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

注意：裸写 `on:` 是 YAML 1.1 的布尔键，不是重试策略的字段——重试相关的 key 必须加引号。加载器会检测到这个错误并明确指出来，不会静默忽略。

```bash
uv run pyattacker validate -c qa.yaml            # parse, check, print the effective config
uv run pyattacker run      -c qa.yaml --progress
uv run pyattacker watch    runs/demo.db          # open another process to monitor it live
uv run pyattacker report   runs/demo.db --errors 20
uv run pyattacker export   runs/demo.db out.jsonl
uv run pyattacker serve    runs/demo.db          # HTTP dashboard + JSON endpoints
uv run pyattacker plugins                        # installed plugins
```

退出码：`0` 全部成功 / `1` 部分失败 / `2` 配置错误 / `130` 中断。每个子命令的每个参数详见 [`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/cli.md)。

## 记录和监控

一条流水线的完整记录 = 按 `pipeline_id` 查的六张表：状态和检查点、每个任务的最终状态、完整的尝试历史（包括**每一次重试决策** `{retry, reason, delay_s, error_class}`）、中间和最终产物、控制流账本（`handoffs`，普通流水线是空的），以及结构化事件流。`pyattacker report/watch` 直接基于这些数据出报告。

## 扩展

**插件**就是普通的 `importlib.metadata` 入口点——装个包，它的名字就能在配置里用：

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

内置项优先解析（插件盖不住 `echo`），导入时抛错的插件会被记录下来，不会让整个运行挂掉。完整示例：[`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.zh-CN.md)。

**大产物**可以存在数据库外面：

```bash
uv run pyattacker run -c examples/qa_eval.yaml --artifact-backend file:///data/blobs
# or, in the config:  artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

文件按内容寻址、原子写入，读的时候自动加载回来——续跑时透明复用溢出的检查点。`null` 只存摘要、丢字节；`inline`（默认）全存在数据库里。

**零依赖的监控面板**：

```bash
uv run pyattacker serve runs/qa.db        # http://127.0.0.1:8787
# /  dashboard   /stats  /metrics  /events  /pipelines  /resources  /errors   (JSON)
```

每个请求都新开一个只读连接，所以可以和正在跑的进程并存。**没有认证**，只绑回环地址——它没有 artifact 路由，但会暴露这次运行记下来的东西：事件的 `data`、存下来的错误信息。当调试工具用就好，别裸暴露到公网。
应用可用 `runner.report_metric("accuracy", value, display="percent")` 汇报实时指标；框架只展示，
不计算指标。参见 [`examples/live_metrics.py`](examples/live_metrics.py)。

**步骤内分支**——`fanout(a, b)` 拿同一份输入并发跑多个任务，返回 `{task_name: value}`。重试粒度变成整个组，这是不把流水线做成 DAG 的代价。组是 Runner 看到的唯一规格，所以子任务们对 `resource`、`algorithm`、`timeout_s` 达成一致时，这些参数从子任务继承（此时 `timeout_s` 限制整个组）。

## 算法基准测试

该用哪种获取算法？资源池内置了 7 种，而答案取决于你的接口方——所以专门做了个模拟器。场景里定义了各种假设（容量周期、压力下收紧的令牌桶、带慢尾的延迟、独立故障和关联风暴、三个性格不同的端点），客户端是 worker 组成的闭环，跑的是真 `Pool` 和真算法，时间是模拟的——十分钟的场景几秒钟就跑完。

```bash
uv run pyattacker bench                       # the scenario's 5 algorithms x 3 seeds, about 13 seconds
uv run pyattacker bench --list                # the scenarios, the algorithms, and what each metric means
uv run pyattacker bench --algorithms wait,backoff --seeds 5 --json runs/bench.json
```

它故意做成黑盒：接口方从不暴露内部状态，它的"脾气"是时间的函数，不因请求方不同而变化，每次请求的抽样按序号索引——所以两个算法遇到的是*同样的*随机数和同样的"天气"，比的是算法本身，不是运气。（实际运行中接口方状态还是会分化，因为令牌桶和在途请求数会对行为做出反应——这种分化本身就是测量的一部分。）

它输出的是一组指标（吞吐量、尾延迟、重试次数、被拒次数、容量利用率、跨端点公平性），不是一个加权总分，而且会逐指标给出胜者——包括"没人赢"的情况、只剩一个算法可比的情况，绝不会给只跑了一小部分的算法评奖：一个连队列都不敢排的策略，靠缩小分母赢不了速率指标。

[`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md) 里有场景假设、指标含义、当前数值，以及这些数据不能说明什么。

## 文档导航

| 文档 | 内容 |
|---|---|
| [`docs/tutorial.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/tutorial.md) | 17 个可运行步骤，从"一个任务"到分片评测、交接和反向遍历——回退、全量重试、载荷历史都有；每个步骤都有测试执行 |
| [`docs/reference.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md) | 所有公开类和函数：签名、参数、示例——包括反向遍历和 `HistoryArtifact` |
| [`docs/cli.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/cli.md) | 每个子命令、每个参数、退出码、配置参考 |
| [`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md) | 概念模型、六条不变量、租约契约、数据模型、设计权衡 |
| [`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md) | 算法基准测试：场景假设了什么、指标怎么看、表格怎么读 |
| [`CHANGELOG.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/CHANGELOG.md) | 每个版本改了什么 |
| [`docs/releasing.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/releasing.md) | 给维护者看的：怎么切版本、怎么发布 |
| [`docs/`](https://github.com/Hazer-BJTU/pyattacker/tree/main/docs) | 英文原版文档；`tests/test_docs_i18n.py` 保证中英代码块、标题和链接同步 |

## 示例

| 示例 | 展示什么 |
|---|---|
| [`examples/quickstart.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/quickstart.py) | 60 行代码的 SDK：自定义客户端工厂、重试、断点续跑 |
| [`examples/llm_eval/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/llm_eval/README.zh-CN.md) | 完整评测——prepare → 两轮模型调用 → 三个评委 → 汇总，两种流水线形态，还*实测*了检查点粒度的取舍（分组形态多花了 2 个已成功评委的请求；拆分形态多花 0 个） |
| [`examples/sharded.py`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/sharded.py) | 一个数据集拆到 N 个存储，最后合并报告 |
| [`examples/plugin_package/`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/plugin_package/README.zh-CN.md) | 一个完整可安装的插件：任务、算法、编解码器 |
| [`examples/qa_eval.yaml`](https://github.com/Hazer-BJTU/pyattacker/blob/main/examples/qa_eval.yaml) | 声明式配置的端到端演示 |

```bash
uv run python examples/quickstart.py
uv run python -m examples.llm_eval.demo
uv run python examples/sharded.py
uv run pyattacker run -c examples/qa_eval.yaml --limit 40
```

## 不做什么

以下是有意的设计取舍，不是缺功能：

* **网络请求**——openai/anthropic 的调用你自己写。内核从不碰 socket。
* **指标计算**——准确率、pass@k、F1，以及任何跨流水线的汇总计算。把产物导出来在外面算，或者用原语自己拼一条汇总流水线。
* **DAG 编排**——流水线就是一条直线链，要分支就在任务内部用 `fanout`。唯一例外是可选的[交接](README.zh-CN.md#交接可选启用)：它只是沿着声明的边改变链的遍历方式，拓扑不变（没有汇合节点、没有第二个入口、没有跨流水线跳转）。
* **服务网关**——唯一的 HTTP 接口就是上面那个只读调试面板。
* **分布式调度**——横向扩展用 `--shard`，多进程就是上限。

[`docs/design.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/design.md) 第 1 节和第 11 节精确划了这条边界，第 8 节列了所有已知取舍及其原因。

## 当前状态

**0.3.1——应用汇报的实时指标。** 应用可用 `Runner.report_metric()` 或 `TaskContext.report_metric()`
把准确率等数值汇报到只读监控面板。完成回调提供已提交的流水线结果，指标计算仍由应用负责。
面板和 `watch` 默认统一跟随同一个运行。参见 [`examples/live_metrics.py`](examples/live_metrics.py)。

**0.3.0——控制流（可选），以及中文文档。** 任务现在可以做[交接](README.zh-CN.md#交接可选启用)：返回一个 `Handoff` 跳过声明的后续步骤或提前收尾，记录在持久化账本里，续跑时从账本接着走。需要手动开启，不开完全没影响——没有 `control` 块的流水线不写新数据，`spec_digest` 逐字节不变。交接与反向遍历在 0.3.0 发布时属于实验性特性；当前开发版本将其提升为正式特性（见 [Unreleased](CHANGELOG.md#unreleased)）。反向遍历现在支持声明式回退和全量重试，带 visits 和可选的载荷历史。整套文档也提供了简体中文版（[`README.zh-CN.md`](README.zh-CN.md)、[`docs/zh-CN/`](https://github.com/Hazer-BJTU/pyattacker/tree/main/docs/zh-CN)），由 CI 保持同步。升级时有一个改名要知道：`MergedReport.events_total` 现在叫 `source_events_total`，因为它是合并报告里**唯一**不去重、按源库原始累加的计数（旧名字仍可作为废弃别名使用）。

**0.2.0——基准测试、更严格的身份校验、三处正确性修复。** 新增 `pyattacker bench`：一个模拟接口方的世界，用一组指标对比各获取算法，而不是一个加权分数（[`docs/benchmark.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/benchmark.md)）。新增 `Retrying.decide`：把重试决策暴露为策略上的方法。新增分页的整类读取（`pyattacker.store.iter_*`，由可选的 `PagedStore` 扩展提供），导出大存储不再需要全量加载。0.1.x 已经实现了 M0–M4 计划的全部内容：内核、持久化和任务级恢复、重试和错误分类、带 7 种获取算法的资源池、延迟延续和 write-behind 批处理、分片和合并报告、三种格式五种导出形状、入口点插件、外部产物后端、fan-out 辅助函数、HTTP 监控端点。

升级前有三个变化要注意。**PyYAML 不再是必装依赖**——基础安装零依赖，所以用 `.yaml`/`.yml` 配置需要 `pip install "pyattacker[yaml]"`。**`with_overrides` 区分"没传"和传了 `None`**：不传的参数保持原值，显式传 `None` 现在会清空 `resource`/`algorithm`/`timeout_s`/`version`，新增的 `UNSET` 哨兵表示"调用方没传"。**统一校验入口**现在同时支撑 `validate` 和所有 `run` 模式——配置能过校验就能跑：未知字段、类型错误、池引用错误、格式错误的产物后端、拼写错误，都会在启动前以退出码 2 和字段路径报错。导出的 `limit` 现在对每种行类型语义一致，`None` 表示完整导出。

0.1.1 之后修了这些：任务被取消时 `shell_run` 会漏子进程，超时只杀不回收——现在所有退出路径（包括创建进程）都会杀并回收，POSIX 上还会给整个进程组发信号；导出事件在最新 10 万行处静默停止；声明式 `run:` 块丢掉了 `artifact_backend`、write-behind 和批处理参数，`--no-write-behind` 单进程下从没生效，`--artifact-backend` 也没传到分片进程；续跑现在会拒绝任务或种子摘要变了的 key，而不是悄悄用旧结果；基准测试自己也修了一批问题（见 changelog）。API 还年轻：从现在开始遵守语义化版本，但 1.0 之前预期还会调整。

以后再做：分布式调度器、Parquet 导出、blob 垃圾回收、一等公民的 `Parallel`/`Gather` 节点。

## 开发

```bash
uv sync                      # create the venv + install the dev group (which includes the optional yaml extra)
uv run pytest                # the whole suite: zero network, a few seconds
uv run ruff check            # lint (configuration lives in pyproject.toml, with reasons for each exception)
uv run pyattacker demo       # end-to-end smoke test
uv run pyattacker bench      # compare the acquire algorithms in simulation (about 13 seconds)
uv build                     # sdist + wheel
```

测试离线且确定性（时间走可注入的 `Clock`）。教程里的代码块由 `tests/test_tutorial.py` 提取并执行——文档写错了 CI 就会挂。

## 许可证

MIT——见 [LICENSE](https://github.com/Hazer-BJTU/pyattacker/blob/main/LICENSE)。

## 实验集合

把已有配置文件组合到同一个 Runner，共享资源池和总并发。可选的按子实验分目录布局分别保存检查点、结果和文件产物；Suite 报告包含恢复前已完成的结果。参见 [Suite 指南](docs/zh-CN/suites.md) 和 [可运行配置](examples/suites/suite.json)。
