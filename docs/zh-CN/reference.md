# API 参考

[English](../reference.md) | **简体中文**

`pyattacker` 里每个公开名称的签名、参数和用法示例。

**想找其他内容？** [`docs/tutorial.md`](tutorial.md) 一步步带你上手；
[`docs/cli.md`](cli.md) 讲命令行用法；[`docs/design.md`](design.md) 解释框架为什么长这样。

## 此处使用的约定

* [目录](reference.md#目录)表里列的所有东西都能从顶层包导入：
  `from pyattacker import Runner`。少数辅助类型由这些 API *返回*，但本身没导出（`PoolStats`、`DeclarativeSpec`、`TaskRecord`、`RunRecord`、`Store` 协议）；遇到这种情况本页会给子模块名，你一般不用手动 import。
* 签名按源码里的写法。签名里的 `*` 表示后面所有参数都是仅限关键字（keyword-only）参数。
* **这里的默认值很重要。** 两个尤其容易踩坑：`Retrying(max_attempts=1)` 意味着除非你主动要求，否则*不重试*；`Runner(store=":memory:")` 意味着除非传路径，否则什么都不持久化。
* **有些示例是完整程序。** 第一行是 `# reference/<name>.py` 的代码块会在每次测试时被写出并执行
  （见 [`tests/test_docs_examples.py`](../../tests/test_docs_examples.py)）——README 和 CLI 文档共享这个约定。没标记的代码块是片段。

## 目录

| 章节 | 名称 |
|---|---|
| [任务](reference.md#任务) | `task`, `build_task_spec`, `TaskSpec`, `Retrying`, `TaskContext`, `with_retry` |
| [流水线](reference.md#流水线) | `pipeline`, `PipelineTemplate`, `PipelineSpec`, `Chain`, `compute_spec_digest` |
| [进阶：交接](reference.md#进阶交接可选启用) | `Handoff`、`control=` 声明、`HandoffRecord` |
| [进阶：反向遍历](reference.md#进阶反向遍历rewindretry-allvisits) | `Handoff.rewind`、`Handoff.retry_all`、反向的 `control` 键、访问、预算、恢复 |
| [运行](reference.md#运行) | `Runner`, `RunConfig`, `RunReport`, worker 存活检测 |
| [资源](reference.md#资源) | `Resource`, `Pool`, `Lease`, `PoolStats`, `Bus`, `ResourceState`, `ResourceEvent` |
| [获取算法](reference.md#获取算法) | `Wait`, `Backoff`, `LeastBusy`, `Failover`, `Sticky`, `QuotaAware`, `Immediate`, `resolve_algorithm` |
| [错误](reference.md#错误) | 异常层次结构、`error_class_of` |
| [工件与编解码器](reference.md#工件与编解码器) | `Artifact`, `Codec`, `CodecRegistry`, `HistoryArtifact`, `JsonCodec`, `BytesCodec`, `Encoded`, `canonical_json`, `digest_of` |
| [存储](reference.md#存储) | `open_store`, `SqliteStore`, `MemoryStore`, 记录类型 |
| [工件后端](reference.md#工件后端) | `InlineBackend`, `FileBackend`, `NullBackend`, `resolve_backend` |
| [分片与合并](reference.md#分片与合并) | `shard_index`, `in_shard`, `shard_specs`, `shard_store_path`, `parse_shard`, `merge_reports`, `MergedReport` |
| [导出](reference.md#导出) | `iter_rows`, `export_store`, `export_stores`, `ROW_KINDS`, `FORMATS` |
| [声明式配置](reference.md#声明式配置) | `load_spec`, `DeclarativeSpec` |
| [插件](reference.md#插件) | `PLUGINS`, `PluginRegistry`, `list_plugins` |
| [内置任务](reference.md#内置任务) | `echo`, `fanout`, `flaky`, `delay`, `boom`, `leaky`, `simulate_llm`, `shell_run`, `write_jsonl`, `jsonl_source`, `seed_factory` |
| [监控](reference.md#监控) | `StatsServer` |

---

## 任务

任务是一元函数：一个值进，一个值出。可以同步可以异步，接受 `(value)` 或 `(value, ctx)`。其他签名装饰时就抛 `ConfigError`。

### `task`

```python
@task(name=None, *, resource=None, algorithm=None, retry=None, timeout_s=None, config=None, version=None) -> TaskSpec
```

把函数变成 `TaskSpec`。可以裸用（`@task`）、带名字用（`@task("ask")`）、或者带选项用。

| 参数 | 类型 | 默认值 | 含义 |
|---|---|---|---|
| `name` | `str` | 函数名 | 任务在记录和事件里的名字 |
| `resource` | `str` | `None` | 没指定名字的 `ctx.acquire()` 调用用的默认资源池 |
| `algorithm` | `str` 或算法 | `None` | 默认获取策略；回退到资源池自己的策略 |
| `retry` | `Retrying` 或 `dict` | 不重试 | 某次尝试抛异常时的策略 |
| `timeout_s` | `float` | `None` | 单次尝试的墙钟时间上限；**仅异步任务** |
| `config` | JSON 映射 | `{}` | 声明的行为，会快照进恢复指纹 |
| `version` | `str` | `None` | 外部行为或动态代码的显式版本号 |

```python
from pyattacker import Retrying, task

@task("prepare")                                   # sync, no context
def prepare(seed: dict) -> dict:
    return {"q": seed["question"]}

@task("ask", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, base=0.5, cap=30.0), timeout_s=60)
async def ask(row: dict, ctx) -> dict:             # async, with context
    async with ctx.acquire(model="gpt-4o") as lease:
        return {"a": await lease.client.chat(row["q"])}
```

同步任务设 `timeout_s` 会被接受但没用——同步函数占着事件循环，框架没机会取消它。阻塞工作放到 `await asyncio.to_thread(...)` 后面。

任务需要的不止是值和上下文？从闭包里拿：

```python
def make_judge(model: str, threshold: float):
    @task(f"judge.{model}", resource="judges", config={"model": model, "threshold": threshold})
    async def judge(row: dict, ctx) -> dict:
        async with ctx.acquire(model=model) as lease:
            return {**row, "pass": await lease.client.score(row) >= threshold}
    return judge

pipeline("eval", prepare | make_judge("gpt-4o", 0.8))
```

### `build_task_spec`

```python
build_task_spec(fn, *, name=None, resource=None, algorithm=None, retry=None,
                timeout_s=None, registry=None, config=None, version=None,
                children=(), parameters=None) -> TaskSpec
```

`@task` 背后的函数。装饰时还不知道目标就直接调它——声明式层就是这么处理 `use:` 的。传一个已有的 `TaskSpec` 加覆盖项，返回新 spec，不是就地改它。

### `TaskSpec`

任务的不可变描述。你很少自己构造它；`@task` 交给你，你再传给 `pipeline(...)`。

| 属性 | 含义 |
|---|---|
| `name` | 任务在记录里的名字 |
| `fn` | 被包装的可调用对象 |
| `resource`, `algorithm`, `retry`, `timeout_s` | 声明的各项策略 |
| `accepts`, `returns` | 类型提示，构建期链校验用 |
| `takes_ctx` | `fn` 是否接受 `(value, ctx)` |
| `module`, `qualname`, `code_digest` | 标识代码这个版本；并入流水线摘要 |
| `config`, `version` | 显式声明的行为和版本 |
| `parameters`, `children` | 工厂参数和嵌套 spec；和用户 config 分开记录 |

| 方法 | 返回值 |
|---|---|
| `is_async` | `fn` 是否为协程函数 |
| `fingerprint(*, include_code=True)` | 给 `spec_digest` 的字典；含嵌套 spec |
| `runtime_algorithm()` | 从捕获的同一性配置派生的运行时算法 |
| `with_overrides(**kwargs)` | 替换了指定字段的新 spec |

`with_overrides` 有三种情形，都对用户可见：

* **没传**的关键字保持当前值；
* 显式的 `None` 会**清空**可空字段——`resource`、`algorithm`、`timeout_s`、`version`。工厂自己的 `resource=` 或 `algorithm=` 就是这么被去掉的；
* `UNSET`（导出为 `pyattacker.UNSET`）即使键存在也表示"没传"，所以总是发同一组键的构建器可以转发自己的字典，不清空调用方没提的东西。声明式加载器就是这么做的。

不可空字段（`name`、`fn`、`retry`、`children`、`config`、`parameters`）传 `None` 会抛 `ConfigError`，不会静默什么都不做。清空 `config` 意味着 `config={}`；清空 `children` 意味着 `children=()`。

两个 `TaskSpec` 用 `|` 组合成 `Chain`。`spec | other` 本身不做任何校验；校验发生在 `pipeline(...)` 里。

### `Retrying`

```python
Retrying(max_attempts=1, on=(), retry_classified=True, retry_unknown=False,
         base=0.5, factor=2.0, cap=30.0, jitter="full", max_total_s=None)
```

| 字段 | 默认值 | 含义 |
|---|---|---|
| `max_attempts` | `1` | 含首次的总尝试次数——**默认不重试** |
| `on` | `()` | 额外视为可重试的异常类型 |
| `retry_classified` | `True` | 重试可重试类别（`rate_limit`、`timeout`、`connection`、`upstream`、`retryable`） |
| `retry_unknown` | `False` | 也重试 `unknown`——`ResourceUnavailable` 需要它 |
| `base`, `factor`, `cap` | `0.5`, `2.0`, `30.0` | 延迟为 `min(cap, base * factor**(attempt-1))`，然后加抖动 |
| `jitter` | `"full"` | `"none"`、`"full"` 或 `"equal"` |
| `max_total_s` | `None` | 已用时间 + 下一次延迟超过这个值就放弃 |

```python
Retrying(max_attempts=5, base=0.5, cap=30.0)                    # typical API client
Retrying(max_attempts=3, on=(MyProviderError,))                 # add your own exception type
Retrying(max_attempts=3, retry_unknown=True)                    # retry capacity shortages too
Retrying(max_attempts=10, max_total_s=120.0)                    # bounded by time, not just count
```

接受 `Retrying` 的地方都接受 `dict`，YAML 形式就靠这个：
`retry={"max_attempts": 3, "on": ["RetryableError"]}`。

| 方法 | 返回值 |
|---|---|
| `should_retry(exc, error_class=None)` | 这个策略下这个异常可重试吗 |
| `delay_for(attempt, rng, retry_after=None)` | 延迟秒数；服务端给了建议就以 `retry_after` 为准 |

### `TaskContext`

双参数任务收到的 `ctx`。`Runner` 按每次尝试构造；永远不用你构造。

| 属性 | 含义 |
|---|---|
| `pipeline_id`, `run_id`, `task_name`, `seq` | 正在跑的活的身份 |
| `attempt` | 从 1 开始的尝试序号——`if ctx.attempt > 1:` 就是你检测重试的方式 |
| `bus` | 这次运行的 `Bus` |
| `store` | 这次运行的存储，需要读历史的任务用 |
| `meta` | 自由形式的字典，每次尝试一份 |

#### `ctx.acquire`

```python
ctx.acquire(pool=None, *, algorithm=None, timeout=None, where=None, **selector)
```

返回一个产出 `Lease` 的异步上下文管理器。**这是用资源的推荐方式**——
每条退出路径都归还租约，包括异常、取消和超时。

| 参数 | 含义 |
|---|---|
| `pool` | 资源池名或对象；默认用任务的 `resource=` |
| `algorithm` | 这次调用覆盖获取策略 |
| `timeout` | 没及时拿到资源就抛 `AcquireTimeout` |
| `where` | `Callable[[Resource], bool]`，选择器表达不了的谓词 |
| `**selector` | 按 `id`、`kind`、`tags`、`options` 匹配，含点路径 |

```python
async with ctx.acquire(model="gpt-4o") as lease:                     # by option
    ...
async with ctx.acquire("judges", id="judge-a", timeout=30) as lease: # explicit pool, bounded wait
    ...
async with ctx.acquire(where=lambda r: r.options["ctx_len"] >= 32000) as lease:
    ...
```

默认 `wait` 算法下，匹配**不到**任何资源的选择器会永远等。宁可报错不要挂着，就传
`timeout=` 或用 `algorithm="immediate"`。

| 其他方法 | 用途 |
|---|---|
| `await ctx.acquire_lease(...)` | 返回裸 `Lease` 的逃生通道；你必须自己还。忘了还的租约任务结束时强制回收，记 `lease.leaked` |
| `ctx.publish_resource(pool, resource)` | 运行时加资源；其他流水线立刻能租 |
| `ctx.revoke_resource(pool, resource_id, reason="")` | 撤回某个资源 |
| `ctx.subscribe(pool, events=None)` | 资源池事件的异步迭代器 |
| `ctx.held_leases()` | 这次尝试当前持有的租约 |
| `ctx.reclaim_now()` | 强制归还当前持有的全部租约；同步、不可中断 |
| `ctx.emit(kind, **data)` | 把你自己的事件写进这次运行的事件流 |

```python
@task("adaptive", resource="apis")
async def adaptive(row: dict, ctx) -> dict:
    if ctx.attempt > 1:
        ctx.emit("my.retrying", attempt=ctx.attempt, reason="previous attempt timed out")
    async with ctx.acquire() as lease:
        return {"a": await lease.client.chat(row["q"])}
```

### `with_retry`

```python
with_retry(spec, **retry) -> TaskSpec
```

某个任务改了重试策略后的副本——不同策略复用同一个任务，不用重新定义。

```python
from pyattacker import with_retry

careful = with_retry(ask, max_attempts=6, cap=60.0)
pipeline("qa", prepare | careful | judge)
```

注意这会改流水线的 `spec_digest`，从而改身份：用
`careful` 构建的流水线和用 `ask` 构建的流水线不共享检查点。

---

## 流水线

流水线是任务的线性链，是完成和恢复的单位。给它种子之前它是*模板*；
`map()` 把每个种子变成独立的 `PipelineSpec`。

### `pipeline`

```python
pipeline(name, *tasks_or_chain, tags=None, include_code=True, registry=None, control=None) -> PipelineTemplate
```

| 参数 | 含义 |
|---|---|
| `name` | 流水线名，记在每一行上 |
| `*tasks_or_chain` | 一条链（`a \| b \| c`），或多个独立参数传的 `TaskSpec` |
| `tags` | 自由形式字典，随每条流水线存着，供后续过滤 |
| `include_code` | 为 `True`（默认）时，每个任务的源码摘要都是流水线身份的一部分 |
| `registry` | 非 JSON 工件类型用的自定义 `CodecRegistry` |
| `control` | **进阶**（见[交接](reference.md#进阶交接可选启用)）：哪个任务能向哪里交接；`None`（默认）让流水线保持普通线性链 |

```python
from pyattacker import pipeline

template = pipeline("qa", prepare | ask | judge, tags={"bench": "mmlu"})
template = pipeline("qa", prepare, ask, judge)          # equivalent
template = pipeline("qa", ask)                          # a single task is a valid pipeline
```

链**在这里**校验，不是跑到一半才：`prepare` 返回 `dict` 而下一个任务要 `int`，
这里立刻抛 `PipelineBuildError`。校验看注解——接受子类，`Any` 或没注解视为宽松，裸容器接受参数化形式。`returns` 注解里的 `Handoff` 成员是个逃生通道：`-> Handoff | Report` 按 `Report` 校验，单独的 `-> Handoff` 能接任何类型（这条路径上的任务不产工件）。

`include_code=False` 适用于你确实想让任务体变、又不想丢现有检查点的情况。默认值走安全方向：代码变了就是新流水线。

### `PipelineTemplate`

| 属性 / 方法 | 返回值 |
|---|---|
| `name`, `tags`, `tasks`, `spec_digest` | 声明内容 |
| `n_tasks` | 链里有多少个任务 |
| `task_names` | 按顺序的任务名 |
| `describe()` | 可转 JSON 的概要（就是 `validate` 打印的），有解析后的 `control` 块也会包含 |
| `control` | 解析后的边计划（**进阶**），或 `None` |
| `bind(seed, *, key=None, repeat=0)` | 从一个种子得到一个 `PipelineSpec` |
| `map(seeds, *, repeats=1, key_of=None)` | `PipelineSpec` 的惰性迭代器 |

#### `map`

```python
map(seeds, *, repeats=1, key_of=None) -> Iterator[PipelineSpec]
```

| 参数 | 含义 |
|---|---|
| `seeds` | **任何可迭代对象**，包括生成器——惰性消费 |
| `repeats` | 每个种子 k 条独立流水线：pass@k 和自洽采样 |
| `key_of` | `Callable[[seed], str]`，用你自己的稳定 id 代替内容寻址 id |

```python
runner.run(template.map(rows))                              # one pipeline per row
runner.run(template.map(rows, repeats=5))                   # pass@5
runner.run(template.map(rows, key_of=lambda r: r["qid"]))   # ids from your dataset's primary key

def stream():                                                # memory stays O(concurrency)
    with open("dataset.jsonl") as fh:
        for line in fh:
            yield json.loads(line)

runner.run(template.map(stream()))
```

`repeats>1` 且指定了 `key_of` 时，键变成 `f"{key}#{repeat}"`。

### `PipelineSpec`

一条等着跑的流水线：种子加链。`spec.pipeline_id`（也是 `spec.key`）是
内容寻址的身份，恢复和分片就靠它。

| 属性 | 含义 |
|---|---|
| `pipeline_id` / `key` | 规格（spec）摘要、种子摘要和重复索引的 `blake2b` |
| `seed` | 数据集里的一行 |
| `repeat` | 这是 pass@k 里的第几个样本 |
| `name`、`tasks`、`n_tasks`、`tags` | 继承自模板 |
| `control` | 解析后的边计划（**进阶**），或 `None` |

### `Chain`

`a | b | c` 产出的东西。`chain.tasks` 是规格组成的元组。只有你写以编程方式组合流水线的代码时才需要这个类型：

```python
from pyattacker import Chain

steps = prepare | ask
if with_judging:
    steps = steps | judge
template = pipeline("qa", steps)
```

### `compute_spec_digest`

```python
compute_spec_digest(tasks, *, include_code=True, control=None) -> str
```

任务链指纹（`v2:` 加 32 字符十六进制摘要）。跑任何东西之前检查代码改动会不会让现有检查点失效：

```python
from pyattacker import compute_spec_digest

if compute_spec_digest(new_chain.tasks) != stored_digest:
    print("this chain will start fresh pipelines, not resume the old ones")
```

摘要涵盖每个任务的名字、资源、`timeout_s`、源码摘要，以及会改变步骤耗时的五个重试字段（`max_attempts`、`base`、`factor`、`cap`、`jitter`）。它有意排除 `on`、`retry_unknown` 和 `max_total_s`。

`control` 块**只有存在时**才折进去，所以这个特性没改变任何现有摘要：没 `control` 的流水线保持精确身份（因而也保持流水线 id、检查点和分片分配），和交接出现之前完全一样。声明的边以解析后的形式参与摘要——seq 目标、排好序——所以目标写成名字还是 seq 是同一条流水线。

---

## 进阶：交接（可选启用）

**进阶层级：可选启用、改执行模型、普通流水线不需要、1.0 前实验性。** 任务可以通过返回一条框架自带的指令而不是值来*向前跳*；流水线在声明里更靠后的位置继续（或就地结束），框架持久记录这次跳转。这里什么都不声明的流水线完全不受影响——
完整论证见设计文档
[§4.8](design.md#48-进阶交接--声明式正向跳转可选启用实验性)。反向遍历——`Handoff.rewind`、
`Handoff.retry_all` 和可选的载荷历史——是同一特性里*单独声明*的层级：见
[进阶：反向遍历](#进阶反向遍历rewindretry-allvisits)。

### `Handoff`

```python
Handoff.to(target, value=UNSET, *, reason="") -> Handoff
Handoff.end(value=UNSET, *, reason="") -> Handoff
```

| 字段 / 方法 | 含义 |
|---|---|
| `target` | 任务名、任务的 seq，或 `END` 时为 `None` |
| `value` | 目标的入口状态。`UNSET`（默认）表示"复用本任务收到的工件"；`None` 是真实的载荷 |
| `reason` | 自由格式字符串，记在账本和 `pipeline.handoff` 事件里 |
| `is_end` | 这个指令是否结束流水线 |
| `reuses_input` | 目标是否带着本任务自己的输入工件进入 |

```python
from pyattacker import Handoff, TaskContext, task

@task("judge")
async def judge(value: Verdict, ctx: TaskContext) -> Handoff | Report:
    if value.good_enough:
        return Handoff.end(value.as_report(), reason="already good enough")
    if not value.needs_metrics:
        return Handoff.to("report", value.as_report(), reason="metrics not needed")
    return await write_report(value)

template = pipeline("qa", retrieve | ask | judge | report,
                    control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})
```

用之前值得知道的规则：

* **交接是一次返回，绝不是失败。** 不会问重试策略（不会加 `decision` 原因，
  `retry.on=(Exception,)` / `retry_unknown=True` 也没法把它变成重试），`async with
  ctx.acquire(...)` 已经还了租约，被取消或超时的尝试永远走不到这个返回。`strict_leases=True` 下，泄漏的租约会让任务失败，交接也不会被认。
* **边是声明的，不是推导的。** 没 `control` 块就返回 `Handoff`，或者沿着不是*从这个任务*声明的边返回，都是 `FatalError`（绝不重试，也绝不静默跳转）。
* **只能正向。** 目标必须严格晚于源。`"end"` 是合法目标，
  但从最后一个任务发除外：那里没效果，会被拒。
* **重复的任务名要 seq。** "ask" 出现两次会因有歧义被拒；改成以 seq `2` 为目标。
* **载荷不做类型检查。** 交接的参数是任意值，不是源任务正常的
  返回类型；它像任何工件一样用流水线的 `CodecRegistry` 编码。
* **交接绝不会来自 N 个分支之一。** `fanout` 会拒绝分支返回的指令，
  因为记录里一个分组就是一步，一次控制转移没法归因到若干并发分支中的某一个。
* **游标变成位置。** 开了控制流的流水线上，`n_tasks_done` 表示执行到哪，
  不是跑了多少任务，跳过的槽位也没任务行。别把它渲染成完成百分比；
  交接计数就在旁边露出来。
* **稳定性。** 上面这些保证是稳定部分；写法形式（`Handoff`、`control`）
  1.0 前还可能变。反向遍历是一个*单独声明*的可选层级，不是这个正向模型的一部分——见 [进阶：反向遍历](#进阶反向遍历rewindretry-allvisits)。

### 一次跳转记录的内容

| 位置 | 记录的内容 |
|---|---|
| `tasks.state = "handed_off"` | 源任务干净结束，没产工件 |
| `attempts.outcome = "handed_off"` | 发生跳转的那次尝试，`decision` 为空 |
| `artifacts` | 入口状态：复用的工件，或位于 `seq = n_tasks + k` 的新载荷 |
| `handoffs` | 账本行：from/to、`entry_artifact_id`、`entry_reused`、`reason` |
| `pipeline.handoff` 事件 | 审计轨迹（强杀可能丢它；账本是权威） |

`HandoffRecord`（由 `pyattacker` 导出）就是那条账本行：`handoff_id`、`pipeline_id`、`run_id`、
`from_seq`、`from_task`、`to_seq`/`to_task`（`END` 时为 `None`）、`entry_seq`、`entry_artifact_id`、
`entry_reused`、`reason`、`ts`。

---

## 进阶：反向遍历（rewind、retry-all、visits）

**进阶层级：可选启用、改流水线遍历方式、1.0 前实验性。** 反向操作和上面的正向模型分开
声明，所以什么都不声明的流水线保留身份、访问 0 的工件地址、随机流和 `spec_digest`。当一个站点判定**更早**的某个站点必须带着作者选定的状态重跑——校验失败后重新生成、用不同参数重试某个步骤——而且两次运行都得留在记录里，不能折成一个任务，再读这节。
[tutorial](tutorial.md#第-16-步--高级用回退和全部重试重新生成) 用两步把它搭起来，第二步讲
[载荷历史](tutorial.md#第-17-步--高级让载荷自带历史)。

### `Handoff.rewind` 与 `Handoff.retry_all`

```python
Handoff.rewind(target, value, *, reason="") -> Handoff   # explicit state is required; None is a value
Handoff.retry_all(*, reason="") -> Handoff               # restart at seq 0 from the original bound seed
```

| 字段 / 方法 | 含义 |
|---|---|
| `target` | 任务名或任务的 seq，始终**严格早于**源 |
| `value` | 目标入口状态。`rewind` 要求给；`retry_all` 不接受任何值 |
| `reason` | 自由文本字符串，记到交接账本和 `pipeline.handoff` 事件里 |
| `operation` | `"rewind"` 或 `"retry_all"`：账本行说这次转移是什么 |

两者都像 `Handoff.to` 和 `Handoff.end` 一样从任务里返回，正向规则照旧：交接是返回值不是失败
（不问重试策略，租约已经释放，被取消或超时的尝试永远走不到这个返回），指令永远逃不出
`fanout` 分支，未声明或形式错的指令是 `FatalError` 不是静默跳转。不同点在于：

* **状态你选；框架不回滚字典。** `rewind` 要求显式值——`None` 是真实的值，不是"复用输入"
  ——而且目的地必须是已声明、严格更早的任务，用唯一任务名或它的 seq 指定。自回退和把 `end` 当
  回退目标都会被拒；链里重复出现的名字必须用 seq 指定；**目标之前**的结果保持有效，目标及其之后的结果变成历史，各自访问重新跑。
* **正向还是正向。** `Handoff.to()` 继续用 `control.edges`，永远不获得隐式反向语义；
  纯反向流水线，正向声明可选。
* **`retry_all` 重放绑定的种子。** 它从 seq 0 重新开始，种子是**绑定时刻捕获的字节新鲜解码**出来的
  ——事后改某个任务的输入或 `spec.seed` 不改实际跑的内容。它不接受替换值：想用
  *不同*状态重启，从更靠后的任务调 `Handoff.rewind(0, chosen_state)`。它可以声明在第一个任务上，
  包括单任务流水线；不创建另一个映射行或重复项、不重置资源、也不启动另一次 CLI 运行——
  它清空有效的任务结果，同时保留访问、工件、尝试和已消耗的控制预算。
* **异常重试是另一套机制。** `Retrying` 在同一个访问内重试一次尝试；`rewind` 和 `retry_all` 是返回的
  控制指令，绝不触发失败重试策略。编写错误和预算耗尽都是致命失败，策略没法重试。

```python
# reference/backward_rewind.py
"""The backward tier in one program: a validator sends the work back to the generator."""

from pyattacker import Handoff, Runner, pipeline, task


@task("generate")
def generate(state: dict, ctx) -> dict:
    # <- your model call; a revisit is a genuinely new sample
    return {**state, "sample": ctx.visit, "ok": ctx.visit >= 1}


@task("validate")
def validate(row: dict, ctx) -> Handoff | dict:
    if not row["ok"]:
        # Explicit state: the author decides what the generator receives next.
        return Handoff.rewind("generate", {"prompt": row["prompt"], "feedback": "retry warmer"},
                              reason="invalid sample")
    return row


template = pipeline(
    "rewind",
    generate | validate,
    control={
        "rewind": {"validate": ["generate"]},   # a strictly earlier destination, declared
        "max_handoffs": 3,                      # required: the loop budget for this pipeline
    },
)

with Runner(store=":memory:", max_handoffs=10) as runner:   # the runtime ceiling; the lower limit wins
    report = runner.run(template.map([{"prompt": "Return JSON"}]))
    store = runner.store
    record = next(iter(store.pipelines()))
    print(report.stats["pipelines"]["by_state"], "position:", record.n_tasks_done)
    print([(row.seq, row.visit, row.name, row.state) for row in store.tasks(record.pipeline_id)])
    print("budget consumed:", store.visit_state(record.pipeline_id)["handoffs"])
```

### `control` 块

| 键 | 形态 | 含义 |
|---|---|---|
| `edges` | `{source: [later targets]}` | 正向跳转（`Handoff.to` / `Handoff.end`）；只声明反向操作时可省 |
| `rewind` | `{source: [strictly earlier targets]}` | 允许从每个来源发 `Handoff.rewind` |
| `retry_all` | `[sources]` | 允许从每个来源发 `Handoff.retry_all` |
| `max_handoffs` | 正整数 | 一旦有 `rewind` 或 `retry_all` 就**必需**：这条流水线的控制预算上限 |

`max_handoffs` 和其他声明一样过校验：`3.0`、`True`、`"3"`、`0` 都会被拒，没任何反向操作
却给了 `max_handoffs` 也会被拒（`control: max_handoffs requires backward operations`）。
`RunConfig.max_handoffs`（默认 1000，配置里写 `run.max_handoffs`）是**运行时上限**：实际限制是
`min(control.max_handoffs, run.max_handoffs)`，所以一次运行可以调低某条流水线的预算，但永远不能调高。
全新开始会重置它。名字和 seq 的解析和 `edges` 完全相同（精确任务名优先于数字字符串，重复出现的
名字必须用 seq 指定），每个问题都以配置字段路径报告——见
[CLI → 进阶反向控制声明](cli.md#进阶反向控制声明)。

```python
# reference/backward_retry_all.py
"""Retry-all restarts the pipeline from the seed the run was bound to."""

from pyattacker import Handoff, Runner, pipeline, task

entries = []


@task("prepare")
def prepare(seed: dict, ctx) -> dict:
    entries.append(ctx.visit)   # a fresh entry after retry-all, not a retry of a failed attempt
    return {"prompt": seed["prompt"], "prepared": True}


@task("check")
def check(row: dict, ctx) -> Handoff | dict:
    if ctx.visit == 0:
        return Handoff.retry_all(reason="new preparation")   # source declared in control.retry_all
    return row


template = pipeline("retry-all", prepare | check,
                    control={"retry_all": ["check"], "max_handoffs": 2})

with Runner(store=":memory:") as runner:
    report = runner.run(template.map([{"prompt": "one row"}]))
    store = runner.store
    record = next(iter(store.pipelines()))
    print(report.stats["pipelines"]["by_state"])
    print("prepare entries:", entries)
    print("transfers:", store.visit_state(record.pipeline_id)["handoffs"])
```

### 访问与工件发生实例

`ctx.visit` 对**每个站点**都从 0 开始，每次全新进入该站点递增——回退后的普通后继进入也算——
`ctx.attempt` 编号的是*同一个访问内*的尝试。两者合起来让一次重新生成保持可见，不是藏起来。

| 位置 | 说明 |
|---|---|
| `pipeline_id:seq` | 访问 0 的工件 id；重访用 `pipeline_id:seq#visit` |
| `TaskRecord.visit`、`AttemptRecord.visit`、`Artifact.visit` | 这行是哪个发生实例 |
| `store.get_artifact(pipeline_id, seq)` | 该站点**有效**的输出，在它当前的访问 |
| `store.get_artifact_by_id(artifact_id)` | 一个**精确的历史**发生实例，包括已被取代的那些 |
| `store.visit_state(pipeline_id)` | 游标、待定入口、有效槽位、按 seq 的计数器、已消耗的交接数 |
| `PipelineRecord.n_tasks_done` | 反向流水线里的*位置*，不是完成度计数 |

访问也参与派生随机性：RNG 和 `ctx.seed` 在访问 0 时和旧派生方式逐字节一致，重访把访问计入，
所以一次重新生成采出不同的样本。它们不会让外部副作用变成恰好一次——每次重新生成都该是新的
外部操作时，把 `ctx.visit` 纳入幂等键（见[外部副作用](#外部副作用)）。

待定入口引用它确切的输入，恢复保留它的访问并继续已消耗的尝试编号：尝试编号在任务代码跑之前就
保留，所以硬杀可能在已完成的尝试行里留空缺，但永远不会重用某个编号。未提交的工作，框架的恢复
保证是至少一次。

### 预算与终止

反向遍历在结构上不再有限，这正是预算必须显式且强制的原因。开反向的流水线上每次非终止转移都
计数，包括正向的 `edges` 转移，而且**N 恰好允许 N 次转移**：第 N+1 次在发布转移或使结果失效*之前*
被拒，所以记录还是精确描述已提交的内容。`END` 到了上限可以直接完成，不消耗另一次转移。

已消耗的计数跨 resume、全部重试和载荷缺失时的自动种子回退保留；只有显式的全新开始才开新的
预算生命周期。访问和审计行也在这次重启里保留，所以历史发生实例仍然能寻址。

### 恢复与所有权

打开一条已存储的行做什么取决于那行，规则是给人读的，不是让人猜的：

| 已存储的行 | 这次运行会做什么 |
| --- | --- |
| `failed`、`interrupted` | 普通检查点恢复：精确的持久化访问——访问编号、待定入口、已消耗的尝试编号——继续 |
| `running`，`resume=True` | 操作者声明上一个所有者已经消失。`interrupt_stale` 先回收心跳过期的行；然后精确的持久化访问继续 |
| `running`，没 `resume` | **跳过**，绝不接管：持久化的待定访问可以在崩溃后继续，所以第二个写入者会把一次遍历分叉。`pipeline.skipped` 带 `reason="owned_by_another_run"` 和所有者的运行 id |
| 行已持久化，遍历消失 | 拒绝：`corrupt visit checkpoint: missing traversal state`。`fresh_restart=True` 是文档给的丢弃重来方式 |
| `succeeded` | 跳过，除非 `retry_succeeded=True`；这时重启从绑定种子以全新预算运行 |

`fresh_restart=True` 是唯一会丢持久化进度的开关：它清空有效的遍历和任何待定入口（把留下还在飞
中的每个任务行结算为 `interrupted`），从不可变的绑定种子重新开始，重置控制预算并使先前的账本失效
——同时保留访问计数器和带访问限定的审计行，所以历史发生实例仍然能寻址，遍历丢了的存储从这些
行重建计数器。它也适用于正向流水线，意思是"忽略检查点，重跑整条链"：追加式历史
也会保留，但任务和链上工件的地址按设计重用，不是留作独立的发生实例（
[存储恢复契约](#表与读取器) 把这区别讲清楚了）。和 `retry_succeeded=True` 结合，可以重启
一条已经成功过的流水线。全新开始发 `pipeline.restarted`（带被丢弃的游标），不是
`pipeline.checkpoint_missing`：什么都没丢。

待定载荷缺失或不可用会发 `pipeline.checkpoint_missing` 并建一次种子重放，保留预算和计数器。
流水线首次执行永远不走这条路径——它的输入就是它已经持有的绑定种子，所以 `journal="summary"`
存储（它写的种子载荷被有意丢掉）不会报一次它从未有过的检查点故障。摘要日志和 `null` 后端可以
在进程里跑循环，但恢复不了它们缺的载荷。指向 seq 0 的待定回退用它选定的载荷，不触发
种子重置。反向转移通过定时器泵重新排队，把 worker 让给其他流水线。

### 检视一次反向运行

只有开反向的流水线，它的流水线导出才多一条 `control` 遍历记录。它包含 `cursor`、`pending`、有效
的 `active` 槽位、持久化的 `counters`、已消耗的 `handoffs`、`version`、精确的当前 `input` 和 `terminal`
引用。嵌套的任务行和工件行含 id、访问和 `active` 标记，单独的任务/尝试/工件导出含 `visit`
（尝试还带它们的 task-run id）。既有的顶层导出行种类这个闭集没变。HTTP `/pipelines` 视图对
开反向的行含遍历状态和 `cursor_kind="position"`；纯正向的行两者都没有。

报告的作用域是它覆盖的那次运行，统计那次运行里重复的访问和尝试，所以 resume 后它显示
新运行的工作量，存储和导出保留此前每一行。这些合计是工作量，不是完成百分比。

### 存储能力

本节背后的可选存储方法——`visit_state`、`reset_visits`、`commit_entry`、`commit_visit_attempt`、
`commit_visit_success`、`commit_control_transition`、`repair_visit_terminal` 和 `get_artifact_by_id`
——在[存储 → 可选的存储能力](#可选的存储能力)里说。它们在基础 `Store` 协议之外，所以第三方后端
对普通流水线仍然可用；在没过探测的存储上打开开反向的流水线，开始之前就被拒抛
`ConfigError`，绝不会降级成非持久循环。

同一项特性还管存储的**特性级别**和随之的兼容性规则：存储一直停在 `base`，直到首次提交
重访；打开未知级别的构建直接拒该存储；`visits-v1` 的 SQLite 存储启用写入保护机制，
防不具备谱系感知的写入者。见[存储 → 存储兼容性与备份](#存储兼容性与备份)。

---

## 运行

### `Runner`

```python
Runner(*, store=":memory:", pools=(), concurrency=16, clock=None, bus=None,
       registry=None, config=None, **config_overrides)
```

调度器。它管着存储、资源池和 worker 槽位。任何 `RunConfig` 字段都可以
直接当关键字参数传。

```python
from pyattacker import Runner

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(rows))
    print(report.summary())
```

当上下文管理器用。退出它也关存储，所以要在 `with` 块*内部*读**基于文件的**存储
里的行，或者之后用 `open_store` 重新打开那个文件。

| 方法 | 用途 |
|---|---|
| `run(specs, *, resume=False, **overrides)` | 跑至完成，返回 `RunReport`。在 `asyncio.run` 里包 `run_async` |
| `await run_async(specs, *, resume=False, **overrides)` | 同上，但在已有的事件循环里 |
| `stats()` | 实时快照；运行中途安全调 |
| `stop(reason="user")` | 请求运行优雅停：不再接新活，排空在途工作 |
| `stopping` | 是否在停 |
| `run_id` | 当前运行的 id |
| `add_pool(pool)` / `pool(name)` | 注册或拿资源池 |
| `close()` | 关存储（上下文管理器会做） |

```python
report = runner.run(template.map(rows), resume=True)              # resume
report = runner.run(template.map(rows), concurrency=8)            # override for one call

live = runner.stats()                                             # while running
print(live["in_flight_pipelines"], live["delayed_pipelines"])      # in flight vs parked in backoff
```

`run()` 接受任何 `PipelineSpec` 迭代器，所以无论数据集多大，用生成器都能让内存平稳。

### `RunConfig`

决定一次运行形态的一切。传一个 `RunConfig`，或者把它的字段当关键字参数传给 `Runner`。

| 字段 | 默认值 | 含义 |
|---|---|---|
| `store` | `":memory:"` | `":memory:"`、SQLite 路径、插件 URI，或已打开的存储实例 |
| `journal` | `"full"` | `"full"` 留工件载荷（**任务级恢复必需**）；`"summary"` 只留元数据 |
| `concurrency` | `16` | 最多同时在途的尝试数；挂在重试退避里的流水线不占槽位 |
| `label` | `""` | 记在这次运行上的标签 |
| `run_id` | `None` | 显式运行 id；默认时间戳 + 摘要 |
| `resume` | `False` | 调度前把被已死运行遗弃的流水线标成可恢复 |
| `retry_succeeded` | `False` | 重跑已标成功的流水线。只判定资格：绝不丢未成功流水线的检查点或遍历记录 |
| `fresh_restart` | `False` | 让已接纳的流水线从绑定种子重新开始：丢检查点/遍历记录，重置控制预算。只追加的历史保留；反向流水线还留访问发生实例和计数器 |
| `heartbeat_s` | `5.0` | 这次运行的心跳多久写一次 |
| `grace_s` | `5.0` | 优雅关闭在取消 worker 之前等多久 |
| `stale_after_s` | `30.0` | 某次运行的心跳早于这个时长，它正在跑的流水线视为已遗弃 |
| `strict_leases` | `False` | 泄漏的租约让任务失败（`LeaseLeakError`），不悄悄强制回收 |
| `stop_after_failures` | `None` | N 次失败后停接工作（尽力而为） |
| `stop_after_s` | `None` | 过了这么长墙钟时间后停接工作 |
| `handle_signals` | `True` | 装 SIGINT/SIGTERM 处理器，调 `stop()` |
| `write_behind` | `None` | 批处理只追加事实；`None` 表示对基于文件的存储开启 |
| `write_batch` | `128` | write-behind 生效时的批大小 |
| `flush_interval` | `1.0` | 两次刷写之间的秒数 |
| `artifact_backend` | `None` | 载荷放哪：`None`/`"inline"`、`"file:///path"`、`"null"`，或规格字典 |
| `max_handoffs` | `1000` | 开反向的流水线中非终止控制转移的运行时上限；实际限制为 `min(control.max_handoffs, this)`（见 [反向遍历](#进阶反向遍历rewindretry-allvisits)） |
| `notes`、`meta` | `""`、`{}` | 自由形式，记在这次运行上 |

```python
from pyattacker import RunConfig, Runner

config = RunConfig(store="runs/qa.db", concurrency=64, journal="full",
                   strict_leases=True, stop_after_failures=50)

with Runner(config=config, pools=[pool]) as runner:
    report = runner.run(template.map(rows))
```

`stop_after_failures` 在接纳时和每次完成时求值，所以已经接纳的流水线还会跑完——
它是止血，不是让时间倒流。

### `RunReport`

`run()` 返回的东西。

| 属性 / 方法 | 含义 |
|---|---|
| `run_id`、`status`、`duration_ms` | 运行的身份和结果 |
| `stats` | 完整统计字典，含 `stats["pipelines"]["by_state"]` |
| `skipped` | 多少流水线被跳过：因为它们已成功，或者（反向遍历时）某条 `running` 行属另一次运行而 `resume` 没认领它 |
| `leases_leaked` | 多少租约必须强制回收 |
| `repair_failures` | 这次运行写不进最终状态、使其脱离撕裂终态的流水线；它是运行局部的，这类失败只有这里可见（该行保留原始属主） |
| `stop_reason` | 运行为何提前停（如果确实提前停了） |
| `summary()` | 人类可读的多行报告 |
| `to_dict()` | 同样的事实，机器可读 |
| `export_jsonl(path, *, scope="store", run_id=None)` | 写流水线行，返回数量 |

```python
report = runner.run(template.map(rows))

print(report.summary())                                  # for a human
metrics = report.to_dict()                               # for a dashboard
print(report.stats["pipelines"]["by_state"])             # {'succeeded': 98, 'failed': 2}

failed = report.stats["pipelines"]["by_state"].get("failed", 0) + report.repair_failures
if report.status != "completed" or failed:
    report.export_jsonl("runs/failures.jsonl")
    raise SystemExit(1)                                  # fail your CI job
```

### Worker 存活检测

完成按计数判定，所以一次运行要求每条已接纳的流水线都到终态。若一个 worker 在自身处理器
之外死——一个不是 `CancelledError` 的 `BaseException`，由框架自己的代码或它调的东西
抛，比如第三方存储钩子——它再也到不了终态，这次运行本来会永远等下去。取而代之：

* 运行被硬停（`stop_reason == "worker_crashed"`），其他 worker 也被切断，不再
  接任何新活；
* 死 worker 当时持有的流水线记 `failed`——如果运行已经在停，记为
  `interrupted`——逃逸的异常记在那行上；已经终态的流水线保留状态，
  状态从存储读，不是运行器的内存记录（存储不用回写交给它的记录，
  所以以持久化的那行为准）；
* `runner.worker_crashed` 事件带那个流水线、那个异常和它的回溯；
* `run_async` 抛 `WorkerCrashed`（运行记录以 `interrupted` 关后），把原始异常
  作为 `__cause__`。`run()` 传播它，CLI 带具名错误退出，状态码 2，所以崩溃的
  worker 不会看起来像一次完成的运行。

```python
from pyattacker import WorkerCrashed

with Runner(store="runs/qa.db", pools=[pool]) as runner:
    try:
        report = runner.run(template.map(rows))
    except WorkerCrashed as exc:
        print(f"lost {exc.pipeline_id}: {exc.__cause__!r}")   # the run's record is already durable
        raise
```

*由任务*抛的 `BaseException` 永远不走这条路径（任务自己的处理像对待普通异常一样把它
收住），取消保持语义：被取消的 worker 记 `interrupted` 再重新抛。
`KeyboardInterrupt` 和 `SystemExit` 是唯一边界：asyncio 会在任何监督者来得及跑之前停
事件循环，所以 worker 自己记那行和事件，中断原样传播——调用方看到的是它自己的中断，
只有那条运行记录落到完不成的状态。

---

## 资源

`Resource` 是一项具体外部能力——一个端点、一把 API key、一个本地 worker。`Pool` 是
它们的一组，加一个默认获取策略。资源池是流水线之间**唯一**共享的状态。

### `Resource`

```python
Resource.create(kind="generic", *, id=None, options=None, tags=None, capacity=1,
                factory=None, degrade_after=3, dead_after=8, cooldown_s=30.0, **meta) -> Resource
```

| 参数 | 默认值 | 含义 |
|---|---|---|
| `kind` | `"generic"` | 你自己选的类别（`"llm"`、`"gpu"`、……）；可参与选择器匹配 |
| `id` | 自动生成 | 稳定标识符，记在每个租约上 |
| `options` | `{}` | 你工厂读的配置：base url、key、model。**可参与选择器匹配，含点路径** |
| `tags` | `{}` | 额外的、可参与选择器匹配的标签 |
| `capacity` | `1` | **这一个资源**允许的并发租约数 |
| `factory` | `None` | `Callable[[Resource], client]`，每个资源惰性调一次，首次租约时 |
| `degrade_after` | `3` | 资源被熔断前的连续失败次数 |
| `dead_after` | `8` | 资源被标失效前的连续失败次数 |
| `cooldown_s` | `30.0` | 降级的资源多久不参与轮转 |

```python
from pyattacker import Resource

def build_client(res: Resource):
    return OpenAI(base_url=res.options["base_url"], api_key=res.options["api_key"])

resources = [
    Resource.create("llm", id=f"key-{i}", capacity=8, factory=build_client,
                    options={"base_url": "https://api.example/v1", "api_key": key,
                             "model": "gpt-4o", "quota": {"tokens": 2_000_000}},
                    tags={"tier": "prod"})
    for i, key in enumerate(api_keys)
]
```

`capacity` 按资源计，资源池总容量是各资源之和。把*那个总和*和 `concurrency` 比：
worker 比容量多就意味着 worker 在等资源池。

工厂是 client 待的地方：它只建一次，这个资源的每个租约共享。工厂抛异常，
租约被拒抛 `ResourceUnavailable`（绝不会给你一个 `client` 是 `None` 的租约），同时记
`resource.factory_failed` 事件，反复失败让这个资源走同样的降级/失效路径。冷却
清掉已存的错误，所以偶发的工厂失败还能得到一次真正的重试。

| 属性 / 方法 | 含义 |
|---|---|
| `id`, `kind`, `options`, `tags`, `capacity`, `meta` | 构造时一致 |
| `spec()` | 可直接序列化为 JSON 的描述，密钥已掩码 |
| `lookup(key)` | 对某个选择器 key 返回 `(found, value)`，含点路径 |

### `Pool`

```python
Pool(name, resources=(), *, kind=None, algorithm=None, bus=None, clock=None,
     deadlock_warn_s=5.0, on_event=None)
```

| 参数 | 默认值 | 含义 |
|---|---|---|
| `name` | — | 任务引用它的方式（`resource="apis"`） |
| `resources` | `()` | 它启动时持有的资源 |
| `kind` | `None` | 之后加的资源的默认 kind |
| `algorithm` | `Wait()` | 未覆盖这个设置的任务用的默认获取策略 |
| `deadlock_warn_s` | `5.0` | 当持有此资源池中某个资源的任务为另一个资源等这么久，发 `acquire.suspected_deadlock` |
| `on_event` | `None` | 每个资源池事件的回调 |

```python
from pyattacker import Pool

pool = Pool("apis", resources, algorithm="backoff")
judges = Pool("judges", [Resource.create("llm", id=f"j-{n}", capacity=1, options={"model": n})
                         for n in ("judge-a", "judge-b", "judge-c")],
              algorithm="least_busy")

with Runner(store="runs/qa.db", pools=[pool, judges], concurrency=32) as runner:
    ...
```

| 方法 | 用途 |
|---|---|
| `add(resource)` | 运行时加一个；选择器匹配的等待者被唤醒 |
| `revoke(resource_id, reason="")` | 撤回一个 |
| `resources()` | 当前列表 |
| `stats(**selector)` | 聚合的 `PoolStats` |
| `snapshot()` | 每个资源的字典：state、leases、active、capacity |
| `subscribe(events=None)` | 资源池事件的异步迭代器 |

```python
print(pool.stats().utilization, pool.stats().waiting)
for slot in pool.snapshot():
    print(slot["id"], slot["state"], f"{slot['active']}/{slot['capacity']}")
```

两者运行期间都安全调——`watch` 和 `serve` 就是这么做的。

运行途中加端点也可以，等待中的流水线会用上新的：

```python
@task("discover")
async def discover(row: dict, ctx) -> dict:
    for endpoint in await find_new_endpoints():
        ctx.publish_resource("apis", Resource.create("llm", capacity=4, options=endpoint))
    return row
```

### `Lease`

`ctx.acquire(...)` 产出的对象。一个租约就是对某个资源的一次使用。

| 属性 | 含义 |
|---|---|
| `client` | 工厂建的对象——你要调的就是它 |
| `resource` | 它背后的 `Resource` |
| `options` | `lease.resource.options` 的快捷方式 |
| `held_ms` | 这个租约已持多久 |
| `task_name` | 持有它的任务 |

#### `lease.report`

```python
lease.report(*, ok=True, latency_ms=None, usage=None, error=None) -> None
```

**资源池正是借此知道一切。** 框架不猜某次失败是不是端点的错，所以
健康的端点绝不会因为你的 JSON 解析器出错就被熔断。

| 参数 | 作用 |
|---|---|
| `ok=False` | 计一次连续失败，供 `degrade_after` / `dead_after` |
| `ok=True` | 清失败计数器——这是一次恢复，能把降级或失效的资源拉活 |
| `latency_ms` | 为每个资源维护 EMA |
| `usage` | 累配额计数器，`quota_aware` 按它排序 |
| `error` | 记下来供诊断 |

```python
async with ctx.acquire(model="gpt-4o") as lease:
    try:
        started = time.perf_counter()
        response = await lease.client.chat(row["q"])
    except ProviderOverloaded as exc:
        lease.report(ok=False, error=exc)          # the endpoint's fault: count it
        raise RetryableError("overloaded", error_class="upstream") from exc
    except json.JSONDecodeError:
        lease.report(ok=True)                       # our parsing bug, not the endpoint's
        raise
    lease.report(ok=True, latency_ms=(time.perf_counter() - started) * 1000,
                 usage={"tokens": response.usage.total_tokens})
    return {"a": response.text}
```

| 其他方法 | 用途 |
|---|---|
| `degrade(reason="")` | 立刻让这个资源退出轮转——比如你已知它配额耗尽 |
| `release_now()` | 同步归还；逃生通道的对应手段，用 `async with` 时不需要 |

### `PoolStats`

`pool.stats()` 返回（定义在 `pyattacker.resource`，没在顶层导出）。

| 字段 | 含义 |
|---|---|
| `active`, `capacity`, `utilization` | 当前占用 |
| `ready`, `degraded`, `dead` | 按状态统计的资源数 |
| `waiting` | 当前排队的获取者数 |
| `leases_total`, `ok_total`, `failed_total` | 吞吐 |
| `total`, `revoked` | 资源数 |
| `leaked_total` | 不得不强制回收的租约 |
| `waits_total`, `wait_ms_avg`, `wait_ms_p50`, `wait_ms_p95`, `wait_ms_max` | 获取资源要多久 |
| `usage` | 累积的 `lease.report(usage=...)` 计数器 |

`wait_ms_p95` 一直涨，说明瓶颈在你的资源池，不是提供方。

### `Bus`

轻量跨流水线信号总线，用于协调的推送一侧。

```python
count = ctx.bus.publish("found_answer", qid=row["qid"])      # returns subscriber count

async for message in ctx.bus.subscribe("found_answer"):      # "*" for everything
    ...
```

### `ResourceState`

`READY`, `DEGRADED`, `DEAD`, `REVOKED`。字符串枚举，所以日志里 `str(state)` 显示 `"ResourceState.READY"`
而线上格式（wire format）用 `.value`。

状态转换：连续失败 `degrade_after` 次后 `READY` → `DEGRADED`，持续 `cooldown_s`；连续失败到 `dead_after`
次后 → `DEAD`。一次成功的 `report(ok=True)` 把降级或失效的资源拉回 `READY`。冷却
到期*不会*重置失败计数器——只有成功才会。

### `ResourceEvent`

资源池的一次状态变化，同时投递给等待者、订阅者、`events` 表和监控
快照。字段：`kind`, `pool`, `resource_id`, `data`, `ts`，加 `as_dict()`。

---

## 获取算法

算法决定**怎么从资源池拿资源**。它和重试是两条独立的轴，重试决定
**活失败后**做什么。可以按资源池设（`Pool(..., algorithm=...)`）、按任务
（`@task(algorithm=...)`）、或按调用设（`ctx.acquire(algorithm=...)`）。

每个算法都能按名字用，所以 `algorithm="backoff"` 和 `algorithm=Backoff(cap=60)` 都
行；YAML 配置用的就是字符串形式。

| 算法 | 没空闲资源时 | 什么时候用 |
|---|---|---|
| `Wait()` **（默认）** | 排队直到有槽位释放或 `timeout` 到期 | 稳态吞吐 |
| `Backoff()` | 指数退避 + 抖动等 | 提供方已饱和；避免释放时大家一起冲 |
| `LeastBusy()` | 选负载率最低的，之后回退 | 容量不等的多个端点 |
| `Failover()` | 按顺序试各资源池，之后回退 | 主/备提供方、分层 key |
| `Sticky()` | 优先用这条流水线用过的资源 | prompt 缓存、热连接 |
| `QuotaAware()` | 按剩余配额排序 | 预算受限的端点 |
| `Immediate()` | 立刻抛 `ResourceUnavailable` | 卸负载不排队 |

```python
Wait(timeout=None)
Backoff(base=0.2, factor=2.0, cap=10.0, jitter="full", max_wait=None)
LeastBusy(fallback=None)                 # fallback defaults to Wait()
Failover(pools=(), fallback=None)
Sticky(fallback=None)
QuotaAware(metric="tokens", reserve=0.05)
Immediate()
```

```python
from pyattacker import Backoff, Failover, Pool, QuotaAware, task

# Back off on a busy pool instead of piling up.
pool = Pool("apis", resources, algorithm=Backoff(base=0.5, cap=60.0))

# Spend the big-quota key first; declare quota in options, consume it via lease.report(usage=...).
budgeted = Pool("apis", resources, algorithm=QuotaAware(metric="tokens", reserve=0.05))

# Primary, then backup, then wait on the primary.
@task("ask", resource="primary", algorithm=Failover(pools=["primary", "backup", "spot"]))
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        return {"a": await lease.client.chat(row["q"])}
```

依赖它们之前，三个行为值得知道：

* **`Failover` 不循环。** 它按顺序把列出的每个资源池各试一次；都没容量，它的
  `fallback`（默认 `Wait()`）只挂在 `pools[0]` 上。理解成"先试这些，最后定在
  主资源池上"。
* **`QuotaAware` 只是偏好，不是限制。** 所有候选都耗尽时，最好的那个仍会被
  发出去——拒活比超支糟。要硬停，在任务里跟踪预算
  然后抛异常。
* **`Immediate` 抛 `ResourceUnavailable`，归类成 `unknown`**，默认重试策略
  不重试 `unknown`。这是有意的——容量上的失误就该大声暴露。想重试容量问题，把 `Immediate` 和
  `Retrying(retry_unknown=True)` 搭配。

获取时的退避和重试时的退避是两个旋钮：`Backoff` 决定为一个*槽位*等
多久，`Retrying` 决定一次*失败*后等多久。

### `resolve_algorithm`

```python
resolve_algorithm(spec) -> AcquireAlgorithm
```

把名字、字典或实例转成算法。内置算法解析先于插件，插件盖不住
`wait`。

```python
resolve_algorithm("backoff")
resolve_algorithm({"name": "backoff", "cap": 60.0})
```

写自己的算法，实现 `AcquireAlgorithm` 协议（`pyattacker.algorithm`），在
`pyattacker.algorithms` 入口点组下注册——见 [插件](reference.md#插件) 和 `examples/plugin_package/`。

---

## 错误

失败就是普通异常；没有要满足的状态机。框架对你抛的任何异常
分类，问你的重试策略怎么办。

### 层次结构

```
PyAttackerError
├── ConfigError              a config or declaration mistake (CLI exit code 2)
│   └── PipelineIdentityConflict  stored key has a different task or seed digest
├── PipelineBuildError       the task chain does not type-check
├── PluginError              a plugin failed to load or resolve
├── ArtifactCodecError       a payload could not be encoded or decoded
├── ResourceError
│   ├── ResourceUnavailable  no resource could be leased
│   │   └── AcquireTimeout   ... within the timeout
│   ├── PoolNotFound         no pool by that name
│   └── LeaseLeakError       a lease outlived its task, under strict_leases=True
├── RetryableError           you are declaring this failure retryable
├── FatalError               you are declaring this failure final
├── BudgetExceeded           a run budget was spent
├── RunInterrupted           the run was stopped
├── CorruptCheckpoint        a stored checkpoint contradicts the pipeline definition
├── StoreUnavailable         the store became untrustworthy
└── WorkerCrashed            a worker died outside its own handlers
```

### 错误类别

`error_class_of(exc)` 把任意异常映射成下列字符串之一：

| 类别 | 默认是否重试 | 识别来源 |
|---|---|---|
| `rate_limit` | 是 | status 425/429 |
| `timeout` | 是 | `TimeoutError`、status 408/504 |
| `connection` | 是 | `ConnectionError` |
| `upstream` | 是 | status 500/502/503/505/507/529 |
| `retryable` | 是 | 未显式指定类别的 `RetryableError` |
| `invalid` | 否 | `ValueError`、`TypeError`、… |
| `fatal` | 否 | `FatalError`、任何其他 4xx |
| `cancelled` | 否 | 取消 |
| `unknown` | 否 | 其他一切，包括 `ResourceUnavailable` |

状态码依次从 `status`、`status_code`、`http_status` 或 `code` 读，回退到
`exc.response.status_code`——不用 import 就能覆盖常见的提供方 SDK。

```python
from pyattacker import error_class_of

error_class_of(HttpError(429))        # 'rate_limit'
error_class_of(TimeoutError())        # 'timeout'
error_class_of(ValueError("bad"))     # 'invalid'
```

### 自行掌控重试决策

```python
from pyattacker import FatalError, RetryableError

# Retryable, with the class and the server's suggested delay.
raise RetryableError("rate limited", error_class="rate_limit", retry_after=response.headers["retry-after"])

# Never retried, whatever max_attempts says.
raise FatalError("the prompt exceeds the model's context window")
```

自己的异常类上设 `error_class` 属性同样有效；异常暴露 `Retry-After`
响应头，也会从中读 `retry_after`。

每个决策都持久化，所以"它为什么重试了五次 / 为什么停了"是一次查询，不是
考古。

```python
for attempt in store.attempts(pipeline_id=pid):
    print(attempt.attempt_no, attempt.error_class, attempt.decision)
    # {'retry': True, 'reason': 'retryable', 'delay_s': 0.7, 'error_class': 'rate_limit', ...}
```

`reason` 是 `ok`、`retryable`、`attempts_exhausted`、`policy_declined`、`total_budget` 之一。

---

## 工件与编解码器

工件（artifact）是任务被持久化的输出。任务一成功就写，这正是让
检查点粒度是任务而不是流水线的原因。

### `Artifact`

| 字段 | 含义 |
|---|---|
| `pipeline_id`, `seq` | 它的身份；`seq=-1` 是流水线的种子。开控制的流水线上，交接载荷位于 `seq >= n_tasks`（见[交接](reference.md#进阶交接可选启用)），所以 `seq` 只有对链才是任务位置。在[开反向](#进阶反向遍历rewindretry-allvisits)的流水线上，第一个发生实例保留 `pipeline_id:seq`，重访把 id 限定为 `pipeline_id:seq#visit` |
| `task_name` | 哪个任务产的 |
| `type_name`, `codec` | 怎么恢复它 |
| `digest`, `size` | 载荷的 `blake2b` 和长度 |
| `payload` | 编码后的字节；字节在后端里或被丢了时为 `None` |
| `blob_ref` | 字节不是内联存放时的位置 |
| `is_final` | 它是否是这条流水线的最终输出 |
| `available` | 载荷是否真能读回来——**恢复时查的就是这个** |
| `encoded()` | 一个可直接交给 `registry.load(...)` 的 `Encoded` 三元组 |

```python
artifact = store.get_artifact(pipeline_id, 1)
if artifact.available:
    value = runner.registry.load(artifact.encoded())
```

相同载荷产生相同摘要，所以原样返回输入的任务不占额外存储。

### `CodecRegistry`

决定值怎么变字节。JSON 能处理 dict、list、标量和 dataclass；给任务标
返回类型，那个类就自动注册，所以恢复出的检查点以你自己的类型返回，
不是 dict。

| 方法 | 用途 |
|---|---|
| `register(codec, *, for_types=(), name=None)` | 加一个编解码器，可选绑定到特定类型 |
| `register_type(cls)` | 显式注册一个 dataclass（可作装饰器） |
| `codec_for(obj)` | 哪个编解码器会处理这个值 |
| `dump(obj)` / `load(encoded)` | 编码 / 解码 |
| `type_name_of(obj)` | 记下来的类型名 |

```python
import numpy as np
from pyattacker import CodecRegistry, Runner, pipeline

class NumpyCodec:
    name = "npy"
    def can_encode(self, obj): return isinstance(obj, np.ndarray)
    def dumps(self, obj):
        buf = io.BytesIO(); np.save(buf, obj); return buf.getvalue()
    def loads(self, data): return np.load(io.BytesIO(data), allow_pickle=False)

registry = CodecRegistry()
registry.register(NumpyCodec(), for_types=(np.ndarray,))

template = pipeline("embed", embed_task, registry=registry)
with Runner(store="runs/embed.db", registry=registry) as runner:   # the same registry in both places
    runner.run(template.map(rows))
```

注册表必须**同时**交给模板（用于种子）和 `Runner`（用于检查点）。认领某载荷的编解码器
优先于更早的注册，所以专用编解码器不会沦为 JSON 兜底背后的死代码；显式 `for_types=`
胜过自动扫描。

内置编解码器：`JsonCodec`（默认）和 `BytesCodec`（原始 `bytes`/`bytearray`）。把你自己的编解码器发
到 `pyattacker.codecs` 入口点组下，它自己就装好了。

### `HistoryArtifact`

一个可选的载荷基类：让载荷自带应用状态的**具名**快照。它是解码后的载荷，不是持久化 `Artifact` 记录
的子类，runner 里也没有任何代码读它来决定下一步去哪：它存在是为了让一次
[回退](#进阶反向遍历rewindretry-allvisits)能送回作者选定的状态，同时它途经的那些状态仍然可检视。
普通字典还是普通字典——任务进入或完成时不自动生成快照。教程第 17 步
[把它搭了出来](tutorial.md#第-17-步--高级让载荷自带历史)。

```python
HistoryArtifact(state, *, history=None, selected=None, next_snapshot=0)
```

| 成员 | 含义 |
|---|---|
| `state` | 当前应用状态，分离的深拷贝 |
| `history` | 全部快照，最旧在前，分离的记录：`{id, label, state, metadata}` |
| `selected` | `restore` 最近一次选中的快照 id，或 `None` |
| `checkpoint(label, *, metadata=None)` | 新值，带一个分离的快照；标签唯一，`snapshot:` 保留给稳定 id（`snapshot:0` 等） |
| `with_state(value)` | 新值，替换当前状态但不追加快照 |
| `snapshot(id_or_label)` | 一条分离的快照记录；未知或有歧义的选择器抛 `KeyError` |
| `restore(id_or_label)` | 新值，其状态是该快照，**并且**其 `selected` 指向它；整段历史都保留，后续阶段仍可检视 |
| `prune(*selectors)` | 新值，去掉那些快照；选择器指向被选中的快照时抛 `ValueError`，id 永不重用 |

```python
# reference/history_artifact.py
"""HistoryArtifact: named, detached snapshots inside an application payload."""

from pyattacker import CodecRegistry, HistoryArtifact


class GenerationState(HistoryArtifact):
    pass


registry = CodecRegistry()
registry.register_type(GenerationState)   # the codec restores the subclass, not a bare HistoryArtifact

state = GenerationState({"prompt": "Return JSON", "temperature": 0.2})
state = state.checkpoint("before-generation")
state = state.with_state({**state.state, "answer": "invalid"}).checkpoint("after-generation")
restored = state.restore("before-generation")   # selected, with the later snapshot still in history
next_state = restored.with_state({**restored.state, "temperature": 0.7})

print("labels:", [row["label"] for row in next_state.history])
print("selected:", next_state.selected)
print("round trip equal:", registry.load(registry.dump(next_state)).state == next_state.state)
try:
    next_state.prune("before-generation")   # the selected snapshot cannot be pruned
except ValueError as exc:
    print("prune refused:", exc)
```

这类载荷要遵守的规则：

* **嵌套可变值永远不和保留的快照互为别名。** `state`、`history` 和 `snapshot(...)` 返回的都是分离的
  拷贝，所以改当前状态改不了过去。
* **状态和 metadata 必须可 JSON 序列化。** 编码会拒绝客户端、租约和其他运行时对象。
* **版本化的 `history-v1` 编解码器同时留快照和已注册的子类类型。** 子类继承基类构造函数（应用
  字段放 `state` 里）；自定义构造函数或额外属性的序列化不在这个接口范围，未注册的子类明确
  解码失败，不是以普通 `HistoryArtifact` 身份回来。
* **持久化是提交的事，不是 `checkpoint()` 的事。** 任务里调 `checkpoint()` 不碰存储；runner
  在提交任务输出或控制转移时持久化载荷，那次提交之前崩溃可能丢内存里的快照。
* **历史自包含，会增长**，随快照数量和大小增长——有意识地 prune。它不取代框架的执行
  账本、任务行或访问记录。

### 辅助函数

| 函数 | 用途 |
|---|---|
| `canonical_json(obj)` | 确定性 JSON：键有序，无多余空白 |
| `digest_of(data)` | 内容寻址用的 `blake2b` 摘要 |
| `Encoded` | 一个冻结的 `(type_name, codec, data)` 三元组 |

---

## 存储

存储保存框架记的每项事实。SQLite 是默认实现；内存存储语义完全相同，正是测试该用的那种。

### `open_store`

```python
open_store(spec, *, journal="full", write_behind=None, batch_size=128,
           flush_interval=1.0, clock=None, backend=None) -> Store
```

```python
from contextlib import closing
from pyattacker import open_store

with closing(open_store("runs/qa.db")) as store:      # reopen a finished run, any process
    print(store.stats()["pipelines"])
    for row in store.errors(limit=20):
        print(row["name"], row["failed_task"], row["error_type"])
```

`spec` 可以是 `":memory:"`、一个路径、一个插件 URI（`"s3://bucket/runs.db"`），或已打开的存储。
支持运行写存储的同时读它——WAL 允许一个写入者多个读取者。

### 表与读取器

| 表 | 每行对应 | 读取方式 |
|---|---|---|
| `runs` | 一次运行 | `store.get_run(id)` |
| `pipelines` | 一条流水线：状态、检查点游标、摘要、标签 | `store.pipelines(...)`, `store.export_rows()` |
| `tasks` | 一个任务：最终状态、已用尝试次数、耗时、错误 | `store.tasks(...)` |
| `attempts` | 一次尝试：结果（`succeeded`/`failed`/`timeout`/`cancelled`/`handed_off`）、错误类别、**重试决策**、租约、耗时 | `store.attempts(...)` |
| `artifacts` | 一个工件 | `store.artifacts(pid)`, `store.get_artifact(pid, seq)` |
| `handoffs` | 一次交接：起点/目标位置、入口工件、是否复用、原因 | `store.handoffs(...)`（**可选能力**），嵌套在 `export_rows()` 中 |
| `events` | 一个结构化事件 | `store.events(...)` |
| `resources` | 一个资源：规格（spec，密钥已脱敏）和健康统计 | 包含在 `stats()` 中 |

| 方法 | 返回值 |
|---|---|
| `stats(run_id=None)` | 计数、状态分布、延迟百分位；`handoffs_total` 统计记下来的跳转次数（普通运行为 0） |
| `errors(*, run_id=None, limit=20)` | 失败项，含任务名、错误类型和消息 |
| `export_rows(*, run_id=None)` | 嵌套的流水线行：含任务、工件和交接 |
| `attempts(*, pipeline_id=None, ...)` | 尝试历史 |
| `events(*, pipeline_id=None, run_id=None, kind=None, limit=...)` | 事件流，可按事件类型过滤 |
| `count_events(*, kind=None, run_id=None, pipeline_id=None)` | 匹配事件的精确计数，通过聚合查询（不物化日志） |
| `close()` | 关连接 |

```python
# "Why did this pipeline take 40 seconds?"
for attempt in store.attempts(pipeline_id=pid):
    print(f"{attempt.task_name} #{attempt.attempt_no} {attempt.outcome} "
          f"{attempt.duration_ms}ms class={attempt.error_class} {attempt.decision}")

# The full story of one pipeline.
print([event.kind for event in store.events(pipeline_id=pid)])
```

带类型化字段的记录类型：`PipelineRecord`、`AttemptRecord`、`EventRecord` 和 `HandoffRecord`
已导出；`TaskRecord`、`RunRecord` 和 `Store` 协议在 `pyattacker.store.base`。`SqliteStore` 和
`MemoryStore` 是这些实现，`open_store` 是你拿实例的方式。

`journal="summary"` 留摘要和元数据，不留载荷。它省空间，代价是丢任务级
恢复——没有可恢复的工件时，被恢复的流水线从头跑，记
`pipeline.checkpoint_missing`。

存储的游标也会被修复，不是盲目信。如果一条 `failed`/`interrupted` 流水线的
`n_tasks_done` 已经等于 `n_tasks_total`，它属于被撕裂的收尾（存储故障，或最终写入期间被杀）：
每个任务都写过检查点，所以下次运行像普通恢复那样校验最后一个工件——
存在、载荷留着、能解码——然后标为最终工件，写终止行的最终状态（状态、
游标、所属运行，以及清掉的失败字段；存储提供可选
`settle_pipeline` 能力时一次写完），记 `pipeline.terminal_repaired`，带上那行原来
的状态/错误/运行，然后以 `succeeded` 结束，不重跑任何东西。两种情况**不会**修复，而且不一样：*越过末尾*的游标不是 Runner 能造的状态，所以报
`CorruptCheckpoint`（`pipeline.corrupt_cursor`），永不升成成功，存储的值原样留着
当证据；而缺、无载荷或解不出的终止工件，回退到
普通的从零重启规则（`pipeline.checkpoint_missing` / `pipeline.checkpoint_unusable`）。一次修复
如果在收尾的**任一步**失败——最终标记或终止行的最终状态写——那行就保持
不变，包括原始失败和所属运行，记带阶段信息的
`pipeline.terminal_repair_failed`，所以后续尝试仍报原始原因。这样的行从不属执行修复的那次运行，
失败永远不出现在按运行范围统计的 `stats` 里；它计入
`RunReport.repair_failures`，正是这让 CLI 退出码为 `1`，不是报一次干净运行。

**事后**跑的 `pyattacker report <store>` 现在会显示失败的终端修复尝试：它从事件日志里查询
报告范围内的流水线是否有 `pipeline.terminal_repair_failed` 事件，并打印
`Terminal repair failures: N pipeline(s)` 段落，列出每个受影响的流水线和失败阶段。
计数按 `pipeline_id` 去重，所以同一条流水线出现在多个存储里不会重复计数。
实时 `run` 的退出码仍然是它刚完成的那次尝试的权威信号；事后报告是历史视图。
开控制的流水线加一个分支，在那些规则**之前**查
（[交接](reference.md#进阶交接可选启用)）：如果最新活动账本行的目标等于游标或在它前方，
运行用记录的入口工件*从那个目标*恢复，绝不重跑源任务；持久的
`END` 行根据它自己的入口工件写最终状态。目标在游标后方的行已被
后续正向进展消费，所以走普通的 `artifact(cursor - 1)` 规则。如果
入口载荷没了（`journal=summary`、null 后端、删了的 blob），流水线从种子
重启记 `pipeline.checkpoint_missing`，和丢了线性检查点一样。

从 seq 0 重启（显式传 `fresh_restart=True`、已在 `retry_succeeded=True` 下成功过的流水线，
或不可用的检查点）会在任务跑之前，持久地把
`PipelineRecord.handoff_floor` 推进到最新的账本 ID。等于或低于这个
水位的行仍留在只追加的历史和导出里，但没法驱动新执行的恢复。
对开控制的流水线，`reset_pipeline(record)` 在提交游标/水位的同时，
删此前的当前任务行和链工件（`0 <= seq < n_tasks`）。种子和高位段
载荷工件，以及只追加的 attempts/events/handoffs 都留着；留着的工件，最终标记会被
清。所以 `tasks()` 和链工件导出描述的是当前这次执行，包括没行的
被跳过站点。历史上复用的入口地址可能引用已删/已替换的链槽位；
账本留的是来源信息，不是那些槽位的不可变快照。

`fresh_restart=True` 是唯一会故意丢检查点的开关，对反向和正向
流水线一视同仁：流水线从绑定种子重跑，这次打开跳过 `resume`/修复规则，
这次打开记 `pipeline.restarted`（带它丢的游标），不是
`pipeline.checkpoint_missing`，因为没丢任何东西。所有只追加的都留着——attempts、events、
handoffs，对反向流水线还有访问计数器，所以历史发生实例仍可寻址。
因此框架不再能用的检查点不是死路，耗尽的
反向遍历预算也不是；见[恢复与所有权](#恢复与所有权)。相比之下，
`retry_succeeded=True` 只放宽*哪些*流水线有资格再跑——它绝不丢
未成功流水线的状态。

水位在后续恢复和进程重启后保留；按当前 `run_id` 过滤会
在第二次被中断的恢复后错丢一个有效交接。提供交接的自定义存储
必须在流水线的读写里持久化这个字段，提供原子重置能力。以可写方式打开 SQLite 用
默认值 0 迁移旧数据库；只读工具把缺的列当 0，不迁移。

成功完成为每条流水线选恰好一个 `is_final` 工件，清较早执行的
旧最终标记。历史载荷和账本行仍可查。

所以 `mark_final` 在契约上幂等，工件已经是
最终工件时直接跳过。

没 `settle_pipeline` 能力的存储用两次写入做同样的修复，两步
被有意区别对待：失败的**终止状态转换**是修复失败（可重试，如
上文），失败的**元数据清理**不是——那行已经持久地 `succeeded`，流水线
已修复，只是失败文本陈旧。第二种发 `pipeline.terminal_cleanup_failed`，
不计为失败，因为已成功的行会永远跳过，清理永远
重试不了。已成功行上的陈旧失败元数据是这类存储有文档说明的降级保证；
内置后端一次写完该行的最终状态，永远遇不到这种情况。

### 分页读取与第三方存储

上面的列表方法是**必需**接口，它们可以物化结果——
这正是让 `merge_reports` 和一份小报告好写的原因。必须保持内存有界的
整类读取（比如大存储的导出）走 `iter_*` 辅助函数：

| 辅助函数 | 按此顺序产出 |
|---|---|
| `iter_pipelines(store, *, run_id=None, state=None)` | `PipelineRecord`，先按 `created_at` 再按 `pipeline_id` |
| `iter_tasks(store, pipeline_id=None, *, run_id=None)` | `TaskRecord`，按 `pipeline_id`、`seq`，再按 `task_run_id` |
| `iter_attempts(store, *, run_id=None, pipeline_id=None)` | `AttemptRecord`，按 `attempt_id`（写入顺序） |
| `iter_events(store, *, pipeline_id=None, run_id=None, kind=None)` | `EventRecord`，按 `event_id`（写入顺序，最旧在前），可按事件类型过滤 |
| `iter_artifacts(store, *, pipeline_id)` | 某条流水线的 `Artifact`，先按 `seq` 再按 `artifact_id` |

每种顺序都**以唯一键结尾**，不是装饰性细节：`tasks` 以
`task_run_id` 为键，`artifacts` 以 `artifact_id` 为键，所以 `(pipeline_id, seq)` 和 `seq` 在契约上
并不唯一。分页游标是严格的 `>` 比较，只按非唯一前缀分页会悄悄丢
和某页最后一行并列的每一行。`pipeline_id`（主键）、`event_id` 和
`attempt_id`（单调计数器）本身就唯一。

```python
from pyattacker.store import iter_events

for event in iter_events(store, run_id=run_id):   # one batch in memory, not the whole log
    ...
```

`PagedStore` 是让那些读取分批的**可选**扩展：存储要实现
上面这些签名的 `iter_pipelines` / `iter_tasks` / `iter_attempts` / `iter_events` / `iter_artifacts`，
每次查询最多读 `ITER_BATCH_SIZE`（1000）行，按文档所述顺序产出。
`SqliteStore` 用键集分页实现全部五个方法（`WHERE <key> > <last row of the batch>
ORDER BY <key> LIMIT 1000`），所以没有查询返回超过一批，也没有读取游标在
处理某一行时开着。`MemoryStore` 遍历它的活动容器；对它来说，内存有界是固有 的。

**读还在写的存储。** 每页是它自己的语句——没有
长期读事务，也没有整个存储的时间点快照。迭代器
保证什么取决于它的键是否单调：

* `events` 和 `attempts` **以高水位为界**（`MAX(event_id)` / `MAX(attempt_id)`
  取自匹配的行，读第一页时定）。此后追加的行不属
  这次遍历，长时间导出不追移动的尾巴；新迭代器能看到它们。这两类
  都是只追加的，所以水位是键区间的真实快照。
* `pipelines`、`tasks` 和 `artifacts` 是**尽力而为的遍历**：没有可用于定界的单调键
  （`created_at` 由调用方给，`task_run_id`/`artifact_id` 不按时间排序），所以
  游标前方插入的行可能出现在导出里，后方插入的不会。对活动存储的
  导出是"我遍历期间存在且可达的一切"，不是
  快照；要可复现就导出完成的存储。

对不实现该扩展的存储，兼容性规则如下——第三方存储
插件层是公开 API，现有插件针对列表方法写：

* 每个 `iter_*` 辅助函数**存在时**用存储原生的分页方法，否则
  **委托给列表 API**（`iter_pipelines` → `pipelines()`、`iter_tasks` → `tasks()`、
  `iter_attempts` → `attempts()`、`iter_artifacts` → `artifacts()`，以及 `iter_events` → `events()`，
  用它能表达的最大 limit，因为该列表 API 自己的 `limit` 意思是"最近的
  N 条"，表达不了"全部"）；
* 这个回退是对的，但物化整类数据，所以第三方存储能得完整导出，
  内存特征和它的列表 API 一样。实现这五个方法才能升级它；
* `Store` 仍是 `open_store()` 唯一查的协议，加这个扩展不破坏任何东西。
  `WriteBehindStore` 实现了它，像它其他读取视图一样在每次分页读取前刷新。

### 可选的存储能力

**`commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)`**,
**`handoffs(*, pipeline_id=None, run_id=None, limit=None)`** 和 **`reset_pipeline(record)`** 是进阶
交接功能背后的可选能力，和 `resources()` 一样"不属于协议"。这次提交是**一次原子
写入**：把源任务终结为 `handed_off`，插入被交接的尝试，持久化载荷
并在它链上的地址分配，追加账本行并移动游标——对 `END`，还把
入口工件标为最终工件，把流水线的最终状态写为 `succeeded`。原子性是这项能力的要求，
不是额外的好处：这里刻意不设第二套恢复协议，所以不能把该操作作为单一单元完成的存储，
不得暴露该方法；在这样的存储上打开声明了 `control` 的流水线快速失败，抛
`ConfigError` 指出缺的 commit/reset 能力，不写一次非持久的跳转。`handoffs()` 读账本时
最旧在前，`limit` 留最新 N 条（仍最旧在前），和 `events`/`attempts` 一样。`supports_handoff(store)`
是探测函数：它解开 `WriteBehindStore`，后者转发这个能力时先刷缓冲的
attempts/events，通过这个提交写被交接的尝试，不走自己的缓冲区。
失败时必须在任何后续事件或清理写入之前回滚数据库写入。外部 blob 后端
失败后可能留一个未引用的 blob；但它不能让部分提交的检查点可见。
支持交接的存储还必须在读写里留 `PipelineRecord.handoff_floor`（见
上文重启规则）。
两个内置后端都实现了它；不实现它的第三方存储跑不了开控制的
流水线，它上面的普通流水线不受影响。

**`reset_pipeline(record)`** 必须原子地删此前的当前任务行和链工件槽位，
清其余工件的最终标记，持久化重置后的流水线行，含游标和水位。
留种子、高位段载荷和只追加的历史。它只在开控制的种子启动时跑；
在某个目标处恢复保持当前状态不变。`WriteBehindStore` 在重置前先刷。缺
这个方法的存储不能宣称支持交接；普通的无控制流水线仍支持。

**`settle_pipeline(pipeline_id, *, state, n_tasks_done, run_id)`** 是另一项可选能力，同样
和 `resources()` 一样"不属于协议"：一次写入，把某行移到终止状态、
重新绑定所属运行，并清失败字段。终止游标修复完一条
流水线时（见上文 § 存储），`Runner._settle_succeeded` 用它，因为那里的写入如果被撕裂，可能
在该行写最终状态之前丢原始失败。`SqliteStore` 和 `MemoryStore` 实现了它，
`WriteBehindStore` 透传它，因为它是状态写入，不是批处理事实。没它的存储仍
可用：Runner 回退到 `finish_pipeline`（原子写状态和游标），然后一次清理用的
`upsert_pipeline`，最坏情况是已写最终状态的行还带着旧失败文本。

**`visit_state(pipeline_id)`**, **`reset_visits(record, seed, *, fresh_budget=False)`**,
**`commit_entry(task)`**, **`commit_visit_attempt(pipeline, task)`**,
**`commit_visit_success(pipeline, task, attempt, artifact, *, final)`**,
**`commit_control_transition(record, *, pipeline, task, attempt, payload, entry_id, target_task, limit)`**,
**`repair_visit_terminal(record)`** 和 **`get_artifact_by_id(artifact_id)`** 是可选能力，
在[反向遍历](#进阶反向遍历rewindretry-allvisits)背后，同样在协议外。`store/visits.py` 的 `VisitStore` 持
共享的状态转换语义，两个内置后端都从它派生，所以后端只需提供一个原子
写入边界，加自己的底层行写入。每个操作是一次提交：入口分配推进
按 seq 的访问计数器并记待定输入，普通成功一并写输出的发生实例和
有效槽位，控制转换还使活动后缀失效、消耗一个
预算单位，并分配目标入口。一次控制转移把它的源访问与尝试、入口发生实例、账本行、控制计数和分配好的目标入口
一起提交；开反向的流水线上一次正向转移同样消耗预算，但不向后移游标。SQLite 用
`BEGIN IMMEDIATE` 串行化这些能力事务，`MemoryStore` 在一次能力写入失败时恢复自己的状态：写入失败的
blob 发不出它的引用，回滚的数据库事务可能留一个无人引用的 blob。`supports_visits(store)` 是探测函数；它解开
`WriteBehindStore`（后者同步委托这些操作之前先刷），要求有访问
方法、`feature_level()` **加上** `commit_handoff`、`reset_pipeline` 和 `handoffs`，因为一次反向
转换通过这同一次提交写它的账本行和源任务。`feature_level()` 是必需的，
不是可选的：下面的兼容性规则是这项能力的一部分，不是额外要求。没过
探测的存储打开开反向的流水线时被拒抛 `ConfigError`，绝不降级为
非持久循环。

### 存储兼容性与备份

SQLite 对旧存储的升级是追加式的：访问列默认 0，出一张遍历状态表，记一个 `store_meta` 特性
级别。纯正向工作永远不离开 `base`。级别在给某站点分配第一个第二次发生实例的*同一个事务*里变成
`visits-v1`，永远不回退——审计行不删，它们存在过这个事实也不删。

那也是谱系不感知的写入者开始读不懂该存储的时刻，所以 `visits-v1` 的 SQLite 存储启用
**写入保护机制**：任何还没声明有访问谱系感知的连接对 `pipelines`、`tasks` 和 `artifacts` 做
`INSERT`/`UPDATE`/`DELETE` 都会显式失败（`no such function:
pyattacker_store_requires_visits_aware_writer`）。原始读取和旧式读取不被拦——守卫保护的是状态，
不是访问——但不懂重访谱系的构建去解读它不受支持：这样的读取者判不出哪个发生实例有效，
所以它的输出描述的是这些行，不是这次执行。守卫保证破坏性那一半：在这个特性之前发布的写入者
没法悄悄改错的发生实例，它第一次写就失败。打开自己不认识级别的构建直接拒该存储
（`StoreFeatureUnsupported`，只读也一样），不报一套它看不见的谱系。

实际后果：

* **备份。** 对运行中的数据库用 SQLite 自带的备份 API（`sqlite3 <store> ".backup <copy>"`，或
  `Connection.backup()`），写入者在跑时它是安全的；把活动数据库文件连同它的 `-wal`/`-shm`
  边文件一起拷，只在没写入者时才可靠。SQL 转储同样能恢复——`sqlite3 <store> .dump |
  sqlite3 <copy>` 先写表数据、后建守卫触发器，所以不具备感知的连接也能重放它，副本连同守卫
  和特性级别一起继承过去。
* 从 `sqlite3` 命令行写受守卫保护的存储，需要在该连接上注册守卫函数（或删触发器）；两者都在
  受支持的接口之外。
* 唯一受支持的回到 `base` 的方式，是由懂该级别的构建执行迁移。
* 多个 runner 同时跑同一条逻辑流水线不是受支持的调度模式；彼此独立的分片行仍独立（没 `resume`
  时 `running` 行绝不被接管，见[恢复与所有权](#恢复与所有权)）。

---

## 工件后端

后端决定载荷的*字节*放哪。无论哪种后端，存储都留摘要、大小和编解码器，所以
工件对上层所有组件来说还是同一个对象。

| 后端 | 行为 |
|---|---|
| `InlineBackend()` **（默认）** | 载荷存存储里 |
| `FileBackend(root=..., min_bytes=262144)` | 达到或超过 `min_bytes` 的内容写按内容寻址的文件，读时再水合（hydrate） |
| `NullBackend()` | 留摘要，丢字节 |

```python
from pyattacker import FileBackend, Runner

with Runner(store="runs/qa.db", artifact_backend=FileBackend(root="/data/blobs")) as runner:
    ...

# Equivalent, and what the CLI and YAML accept:
Runner(store="runs/qa.db", artifact_backend="file:///data/blobs")
Runner(store="runs/qa.db", artifact_backend={"kind": "file", "root": "/data/blobs",
                                            "min_bytes": 262144})
```

文件按内容寻址并原子写，所以相同载荷合并成一个文件，共享的根目录安全服务多次运行。
通过已溢写的检查点恢复是透明的。

`NullBackend` 让你丢恢复粒度，和 `journal="summary"` 完全一样：没载荷就没
可用于恢复的检查点。blob 文件缺失时的行为也是刻意如此——`available` 变 false，工作重做，
不静默跳过。

`resolve_backend(spec)` 把字符串或 dict 转成后端。`FileBackend.stats()` 报告它写了
什么。你自己的后端需要独立 URI scheme，通过 `pyattacker.stores` 分发它。

---

## 分片与合并

一个存储只接一个写入者，所以横向扩展是多进程各自持存储，事后再合并。
分片分配是内容寻址的流水线键的纯函数，同一份数据集总是按同样方式切，恢复的流水线回到
拥有它的那个分片。

| 函数 | 用途 |
|---|---|
| `shard_index(key, count)` | 某个流水线键属于哪个分片 |
| `in_shard(key, index, count)` | 它是否属于当前这个分片 |
| `shard_specs(specs, index, count, *, key="pipeline_id")` | 把规格（spec）流过滤成单个分片 |
| `shard_store_path(base, index, count)` | `runs/qa.db` → `runs/qa.shard0of4.db` |
| `parse_shard(text)` | `"0/4"` → `(0, 4)`，带校验 |

```python
from pyattacker import merge_reports, shard_specs, shard_store_path

specs = list(template.map(rows))
paths = []
for index in range(4):
    mine = list(shard_specs(specs, index, 4))
    path = shard_store_path("runs/qa.db", index, 4)
    with Runner(store=path, pools=make_pools(), concurrency=16) as runner:
        runner.run(iter(mine))
    paths.append(path)

merged = merge_reports(paths)
print(merged.summary())
merged.export("runs/all.jsonl")
```

分配是 `blake2b(key) % count`，不是 Python 里加盐的 `hash()`，所以它在不同进程和机器之间
稳定。均衡是统计意义的：12 条流水线分 3 片得 6/2/4 很正常，规模大了趋于
均匀。实践中你通常让 CLI 派生进程——`--shards 4 --jobs 4`——只有自己驱动集群才
用这些函数。

### `merge_reports`

```python
merge_reports(paths) -> MergedReport
```

按 `pipeline_id` 去重（最优状态胜出，最晚完成者打破平局），然后根据合并后的行**重算**
统计。合并幂等，所以同一个存储被算两次不会虚增任何数字。

| `MergedReport` 成员 | 含义 |
|---|---|
| `rows` | 合并后的流水线行 |
| `duplicates` | 被折掉的行数 |
| `sources` | 哪些存储参与 |
| `stats()` | 重算的统计 |
| `summary()` | 人类可读的摘要 |
| `errors(limit=20)` | 所有分片的失败 |
| `export(path, *, fmt="jsonl", kind="pipelines")` | 写出合并后的视图 |

---

## 恢复同一性

任务指纹记它的名字/目标、声明的资源和超时、每个重试字段
（含异常的模块名/限定名）、任务算法配置、`config`、`version`
以及工厂 `parameters`。`fanout` 还记它的有序子指纹和 `on_error`。
内置工厂自动记它的行为参数。用户的 `config` 和工厂参数彼此独立，所以声明式覆盖
抹不掉内置任务的行为同一性。

`config` 和 `parameters` 只接受 null、bool、int、有限 float、字符串、列表，以及键为字符串的
对象；它们在构建规格时被快照。元组、非字符串对象键、JSON 原始类型的 Python 子类以及
循环引用都被拒，不被强制转换。任意闭包、客户端、全局变量、导入的辅助函数、端点选项，
以及资源池默认算法都**不**查。来自这些来源的行为必须显式声明：

```python
from pyattacker import task

@task("ask", config={"model": "model-a", "temperature": 0.2}, version="prompt-v2")
async def ask(row, ctx):
    ...  # use the same declared model, temperature and prompt revision in your client call
```

内置任务算法在构造 TaskSpec 时捕获规范化后的配置。
Runner 从同一份快照构建全新的运行时实例，所以之后对传入的算法实例或映射做的修改
不改已记录的行为。嵌套 fallback 被快照，
对 Sticky、LeastBusy、Failover 和 QuotaAware，隐式 Wait fallback 规范化为显式 `Wait()`。
Failover 的元组/列表资源池名序列刻意规范化为 JSON 列表。

自定义任务算法必须提供返回同一严格 JSON 取值域的 `fingerprint()`，否则
任务必须提供 `version=`，算法行为变了就递增它。钩子结果随规格
捕获，在任务执行前和每次获取前检查，含自定义
fallback；一旦漂移，在该算法跑之前失败。自定义钩子必须描述配置，
不是不断变化的计数器，它的配置在一次获取期间必须稳定。
仅依赖 version 的自定义算法必须支持独立的 `deepcopy`；定义在构造时复制，
为每个任务生成彼此分离的运行时副本。内置的字符串、
config 映射和等价实例规范化为同一指纹。机密信息和运行时客户端绝不放进同一性配置。

`pipeline(..., include_code=False)` 递归移源码摘要，含 fanout 子任务。
它留工厂参数、config、version 和各项策略。源码检查失败时改用
模块名/限定名，所以动态定义的函数需要显式 version 区分同名的
实现。改导入的辅助函数同样要更新 config/version。

默认 ID 对规格摘要、种子摘要和重复索引哈希。定义/输入没变时，
ID 和分片不变；行为变了产新 ID，可能迁到另一个分片。
显式的 `bind(key=...)`、`map(key_of=...)` 和声明式的 `source.key_field` 留传入的
ID，但 Runner 在跳过或恢复**之前**查已存的规格和种子摘要。
不匹配抛 `PipelineIdentityConflict`（`ConfigError`，CLI 退出码 2），中断新运行，
冲突流水线已存的定义、结果和检查点保持原样。
已经打开的进行中流水线立刻以 interrupted 收尾，持久化的
检查点游标保留；监控不用等之后恢复来修它们的状态。
改动过的工作用新 key 或新存储。`retry_succeeded=True` 不能当冲突的覆盖开关。

### 升级现有存储

新规格摘要带 `v2:` 前缀；所有默认流水线 ID 都从旧指纹
格式变。旧记录仍可读/导出，但不自动迁移或复用：
旧指纹缺验证等价性所需的信息。打开一个存储，它最旧的流水线
带旧版摘要，就在任务开始前发警告和 `run.legacy_identity` 事件。
默认 ID 会重跑工作；显式旧 key 会冲突。用旧版本的包完成耗时的旧运行，
然后为 v2 启新存储。别通过重写旧版摘要绕过验证。
分片运行，这次身份变更也改分片分配；完成旧运行时，
把旧分片存储和旧包放一起用。

### 外部副作用

恢复跳过检查点游标和工件都持久化的任务。它不保证
请求或文件写入恰好执行一次：提供方可能已经完成请求，进程随后崩，
或者写检查点时失败，于是这个任务可能再跑。journal 用 summary、
载荷为 null/缺失以及检查点不可用时，也可能重放更早的任务。

你的提供方支持幂等键，就从稳定的流水线身份和任务身份派生一个键，
比如 `f"{ctx.pipeline_id}:{ctx.seq}"`，多次重试之间复用它，别把
尝试序号也加进去。开反向的流水线上，每次重新生成都该是新操作，就要把 `ctx.visit`
加进去，比如 `f"{ctx.pipeline_id}:{ctx.seq}#{ctx.visit}"`。交接载荷*不是*
任务身份，所以绝不要
用工件地址当外部工作的键。遵守提供方的保留窗口和 API 契约。文件汇聚器，
按身份 upsert，或为每条流水线/每个任务写一个原子替换的文件；单纯追加式
`write_jsonl` 任务重放时可能产重复行。资源租约安全不能让这些
外部副作用变幂等。

---

## 导出

五种行形态，三种格式。行以有界批从存储流式读出；合并报告
（把 N 个存储汇成一个连贯答案）是唯一必须把行留内存的地方。

| 名称 | 值 |
|---|---|
| `ROW_KINDS` | `("pipelines", "tasks", "attempts", "events", "artifacts")` |
| `FORMATS` | `("jsonl", "json", "csv")` |

| 函数 | 用途 |
|---|---|
| `iter_rows(store, *, kind="pipelines", run_id=None, limit=None)` | 以 dict 流式出行 |
| `export_store(store, path, *, kind="pipelines", fmt="jsonl", run_id=None, limit=None)` | 写单个存储，返回行数 |
| `export_stores(paths, path, *, kind=..., fmt=..., limit=...)` | 把多个分片存储写成一个拼接文件 |

每种 kind 一行，顺序就是 `limit` 截断依据的顺序：

| `kind` | 每行对应 | 顺序 |
|---|---|---|
| `pipelines`（默认） | 一条流水线，嵌套——含任务、工件和交接 | `created_at`，然后是 `pipeline_id` |
| `tasks` | 一个任务：最终状态、耗时、错误、用的租约 | `pipeline_id`、`seq`，再是 `task_run_id` |
| `attempts` | 一次尝试，含每次重试的 `decision` | `attempt_id`（写入顺序） |
| `events` | 一条结构化事件 | `event_id`（写入顺序，最旧在前） |
| `artifacts` | 一个工件，含中间工件 | 流水线顺序，然后是 `seq`，再是 `artifact_id` |

`limit` 对所有 kind 含义相同——它数的是该 kind 的行数，`artifacts` 也一样，
而过去这里数的是流水线：

* `None`（默认）：**完整**历史，不截断；
* `0`：没行；
* `N > 0`：按上述顺序的前 N 行；
* 负值：`ConfigError`。

（`export_stores` 对每个存储分别应用 limit，所以每个存储最多贡献它的前 N 行。）

每种排序都以一个唯一键结尾，所以跨批次边界的读取不丢行也不
重复——即使很多流水线共享同一个 `created_at`，或若干任务或
工件共享同一个 `(pipeline_id, seq)` / `seq`，schema 不禁这种情况。

```python
from pyattacker import export_store, iter_rows

# Compute your own metric — the framework stores facts and leaves semantics to you.
correct = sum(1 for row in iter_rows(store, kind="pipelines")
              if row["state"] == "succeeded" and row["artifacts"][-1]["payload"]["correct"])
print(f"accuracy: {correct / total:.1%}")

# Retry analysis in a spreadsheet.
export_store(store, "attempts.csv", kind="attempts", fmt="csv")
```

`pipelines` 行是嵌套的——含任务、工件和交接——这也是它成默认的原因。
`handoffs` 列表含 `handoff_id` 和提交它的 `run_id`；流水线行含
`handoff_floor`。高于该水位的 ID 属活动执行历史；等于或低于它的 ID 是
历史记录，恢复时排除。`store.handoffs(pipeline_id=...)` 含所有记录。
`stats(run_id)["handoffs_total"]` 只统计这次运行提交的交接，所以恢复能用上次运行留的活动交接，
同时报告零个新交接。普通流水线，
`handoffs` 列表是 `[]`；否则每记一次跳转就一个对象（见
[handoffs](reference.md#进阶交接可选启用)）；它是嵌套结构，不是独立的行 kind，所以 `ROW_KINDS`
不变。CSV 从最初几行取表头，
之后出现的键折进 `extra` 列，所以内存平稳，没字段
被静默丢。

**导出活动中的存储。** 导出不是事务：它一次读一页，所以能保证什么取决于 kind（细节见
[stores](reference.md#分页读取与第三方存储)）。
`events` 和 `attempts` 受导出开始时取的单调键高水位限制——之后写的行不含，
重新导出就能看到它们。
`pipelines`、`tasks` 和 `artifacts` 是尽力而为的遍历：先于游标写的行可能出现，
晚于游标的不会。要可复现的文件，导出完成的存储。

**流式在哪里成立、哪里不成立。** `jsonl` 和 `json` 逐行写，`csv` 只缓冲
生成表头需要的 `header_rows` 前缀。存储一侧，`tasks`/`attempts`/`events` 通过分页辅助函数
以 `ITER_BATCH_SIZE`（1000）行为一批读，`artifacts` 先给流水线分页，
再流式读每条流水线的工件，所以内存单位是**一条流水线**，不是整个存储。一条
`pipelines` 行本身嵌套，所以导出该 kind 时一次物化一条流水线的任务和工件。
`merge_reports` 是刻意的例外：按 `pipeline_id` 去重需要每条流水线的胜出行，
所以合并后的行留内存（它用聚合查询统计事件/尝试，
不是读日志）。

---

## 声明式配置

`load_spec` 把 YAML/TOML/JSON 文件读成资源池、一个流水线模板和一个种子来源，返回
`DeclarativeSpec`（定义在 `pyattacker.declarative`）。文件描述**组装和资源**；
你的逻辑仍在 Python 里，由 `use:` 指向。

解析器按文件后缀选，只有 YAML 解析器是可选的：`.json` 和 `.toml` 用标准库
读，`.yaml`/`.yml` 需要 `yaml` 额外依赖（extra，`pip install "pyattacker[yaml]"`）。
没它时，`load_spec` 抛一个同时指出文件和该额外依赖的 `ConfigError`——检查发生在
读该文件时，所以只碰 JSON/TOML 的进程永远不用装 PyYAML。

```python
from pyattacker import Runner, load_spec

spec = load_spec("qa.yaml")                 # raises ConfigError on a bad file
print(spec.describe())                       # what `pyattacker validate` prints

with Runner(pools=spec.pools, **spec.run) as runner:
    report = runner.run(spec.pipelines(limit=100))
```

`load_spec` 是共享的校验入口：`run`、`validate` 和每个 `--shards` 子进程都走它，
被它拒的配置在存储出现之前以退出码 `2` 结束。它查未知字段、字段类型、数值
范围、资源池引用（含 `use:` 工厂自己声明的 `resource`）、算法名称和
参数，以及 `source:` 声明——总是指出字段路径。它只做声明检查：
不开任何数据集，也不构造工件后端。[`docs/cli.md`](cli.md#validate--不运行就检查配置)
列了覆盖的内容。

配置没提的字段保留 `use:` 目标声明的值；显式写 `field: null` 清
`resource`、`algorithm`、`timeout_s` 或 `version`。这条规则和它的 SDK 写法
（[`TaskSpec.with_overrides`](reference.md#taskspec)）是同一个。

| `DeclarativeSpec` 成员 | 含义 |
|---|---|
| `template`、`pools`、`run`、`source` | 解析出的各小节 |
| `unresolved_env` | 没解析出值的 `${VAR}` 引用 |
| `seeds()` | 种子可迭代 |
| `pipelines(*, limit=None)` | `PipelineSpec` 流 |
| `describe()` | 可直接转 JSON 的摘要 |

`load_spec(path, strict_env=True)` 在 `${VAR}` 未设时抛异常不是给警告——值得在 CI 用，
因为 CI 里静默为空的 API key 比任务失败更糟。

两个细节值得知道：

* `use:` 按形态解析。带冒号的名字（`my_pkg.tasks:ask`）直接导入；裸名字
  （`echo`）先在内置任务里找，然后才查插件——所以插件永远盖不住内置任务。返回
  `TaskSpec` 的可调用对象当工厂，用 `args`/`kwargs` 调。
* **YAML 1.1 把裸的 `on:` 键解析成布尔值 `true`。** retry 块里写 `"on": [RetryableError]`。
  加载器会发现这个错误并提示你。

完整文件格式见 [`docs/cli.md`](cli.md#配置文件参考)。

---

## 插件

插件就是普通的 `importlib.metadata` 入口点。装个包，它的名字任何配置里都能用。

| 组 | 提供的内容 |
|---|---|
| `pyattacker.tasks` | 一个 `TaskSpec`，或返回它的工厂 → `use: my_task` |
| `pyattacker.algorithms` | 一个获取算法 → `algorithm: my_algo` |
| `pyattacker.codecs` | 一个编解码器，创建第一个 `Runner` 时装上 |
| `pyattacker.stores` | 以 URI scheme 为键的存储工厂 → `store: "s3://bucket/runs.db"` |

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"
[project.entry-points."pyattacker.algorithms"]
my_algo = "my_pkg.algo:MyAlgorithm"
```

```python
from pyattacker import PLUGINS, list_plugins

for item in list_plugins():
    print(item["group"], item["name"], item.get("error", "ok"))

print(PLUGINS.errors())        # {name: reason} for everything that failed to load
```

内置项先解析，导入时抛异常的插件记下来不传播——所以坏插件
既不拖垮一次运行，也不静默失败。`pyattacker plugins` 打同样的
信息。`PluginRegistry` 是类型；`PLUGINS` 是进程级实例。完整可用的示例包
见 [`examples/plugin_package/`](../../examples/plugin_package/README.zh-CN.md)。

---

## 内置任务

其中三个用于生产（`fanout`、`shell_run`、`write_jsonl`）；其余都是模拟工作，让你没网络也能
演练整套机制。配置里它们带 `mock.*` 名字：`use: pyattacker.tasks:flaky`。

### `fanout`

```python
fanout(*specs, name=None, on_error="raise", retry=None) -> TaskSpec
```

一个任务内部，对 **同一** 输入并发跑多个任务，返回 `{task_name: value}`。这就是
不把流水线变成 DAG 的前提下，表达真分支步骤的方式。

```python
from pyattacker import fanout, pipeline

judges = fanout(judge("judge-a"), judge("judge-b"), judge("judge-c"), name="judges")

@task("reduce")
def reduce_scores(row: dict) -> dict:
    branches = list(row.values())
    return {"qid": branches[0]["qid"],
            "verdicts": {b["judge"]: b["verdict"] for b in branches}}

template = pipeline("eval", prepare | ask | judges | reduce_scores)
```

三个后果，都是有意的：

* **重试粒度是分组级的。** 一个分支失败重试整个 fan-out；子任务各自的策略
  不逐分支生效。分组采用最宽容的子任务策略，除非你传
  `retry=`。
* **分支共享父任务的上下文**，所以它们的租约和事件都记在 fan-out 任务之下——
  每个步骤一条完整记录。
* **Runner 只看到分组规格（spec）**，所以只有每个子任务都一致时，`resource`、`algorithm` 和
  `timeout_s` 才从子任务提取。这就是为什么上面三个 judge 必须声明相同的资源池和
  算法。

权衡：一个检查点里有三个请求，意味着失败后恢复时重发全部三个。某个请求
开销大，就把它单独作为一个任务——`examples/llm_eval/` 对两种形态都做了测量。

### `shell_run`

```python
shell_run(command, *, timeout_s=60.0, check=True, name=None) -> TaskSpec
```

跑子进程并返回它的 stdout/stderr。

```python
# The argv form: no shell, and "{value}" arrives as one literal argument.
shell_run(["python", "postprocess.py", "--input", "{value}"])
```

**命令需要工件时，用 argv 形式。** 它通过 `create_subprocess_exec` 跑，所以替换后的
值无论含什么字符，都作为一个字面量参数传给子进程。字符串形式直接拒绝 `{value}`，
不把它插进 shell 命令行。替换只是普通子串替换，不是 `str.format()`，所以其他花括号
（比如 `jq` 过滤器、字典字面量）原样留着。

保证是"没有*隐式* shell"，不是"对任何程序都安全"：你的 argv 本身调了
解释器（`["sh", "-c", ...]`），那个解释器怎么处理输入，你自己判断。

**进程生命周期。** 任务拥有它启动的进程，每条退出路径都执行同一套清理——
包括进程创建在内。OS 子进程在 `create_subprocess_exec` /
`create_subprocess_shell` 返回句柄之前就已存在，落在这个窗口的取消
不允许把它弃之不顾：任务继续等句柄，处置子进程，然后才让取消继续。
正常退出不受干预（结果照常返回，`check=False` 仍报非零 `returncode`，
不抛异常）。`timeout_s` 到期、协程被取消（`Runner` 停、分组任务超时、
外层 `asyncio` 取消）或有其他异常逃逸，仍在跑的子进程被 `SIGKILL` 杀，
然后被 **回收**，之后异常才继续传回调用方：取消仍以 `CancelledError` 到达，
超时仍以 `TimeoutError` 到达，但它们身后不留任何正在跑的进程。
没有优雅的 `SIGTERM` 窗口——清理不等子进程结束。等 OS 报退出有上限
（5 秒），只对 OS 永远不报已退出的进程有意义；清理自己遇到的任何问题
（`communicate()` 被取消而处于坏状态的读取器、一次失败的信号）都不允许替换调用方的
异常：清理是尽力而为，
调用方的错误类型不是。

**后代进程。** POSIX 上，每个子进程在自己的会话里启动（`start_new_session=True`），所以
清理向整个进程组发信号，不只针对一个 PID。字符串命令，覆盖流水线或
子 shell 的每个部分；argv 形式，覆盖该程序和该程序派生的任何进程。
没有"只杀直接子进程"的模式，也没有进程组之外的 cgroup/pidfd 机制。
Windows 上，标准库不支持向进程组发信号（`os.killpg` 不存在，`asyncio` 没法
向子进程的进程组发 `CTRL_BREAK_EVENT`），所以那里只终止直接子进程，
shell 命令的后代可能在任务结束后继续活——这是有文档的局限，
只在 POSIX 上验证过。POSIX 上子进程自成会话首进程，它不收
Ctrl-C 等终端产生的信号；停它的是任务自己的取消。清理只在子进程
本身尚未被回收时起作用：已经正常退出的命令不被追查，所以它刻意留的
进程（shell 的 `&`、守护进程）不被杀——子进程一旦被回收，
它的 PID（也是进程组 id）可能已被复用，
所以向那个进程组发信号本来也不安全。

### `write_jsonl`

```python
write_jsonl(path, *, mode="a", name=None) -> TaskSpec
```

作为最后一步，把每个工件追加到 JSONL 文件。这是若干导出机制中的一种——
`export_store` 和 `report.export_jsonl` 通常更合适。

### 模拟任务

| 任务 | 签名 | 行为 |
|---|---|---|
| `echo` | `echo`（是规格，不是工厂函数） | 原样返回该值 |
| `flaky` | `flaky(fail_times=2, *, error="retryable", message=..., retry=None)` | 失败 N 次后成功——用来演练重试 |
| `delay` | `delay(seconds=1.0)` | 等待，观察并发 |
| `boom` | `boom(message="boom", *, error="retryable")` | 总失败；`error` 可以是 `retryable`、`fatal` 或 `invalid` |
| `leaky` | `leaky(*, pool=None)` | 故意泄漏租约，用来演示强制回收 |
| `simulate_llm` | `simulate_llm(*, latency_ms=5.0, fail_rate=0.0, error="rate_limit", tokens=32, resource=None, **selector)` | 获取 → 等待 → 上报 → 返回：演示资源池和退避的主力 |

```python
from pyattacker import Runner, pipeline, simulate_llm

# A pool exercised at a 30% failure rate, with no network anywhere.
template = pipeline("smoke", simulate_llm(resource="apis", fail_rate=0.3, latency_ms=20))
```

把 `ctx.clock.sleep` 换成一次 HTTP 调用，`simulate_llm` 就成真任务——它就是按这种形态
写成的完整示例。

### 种子辅助函数

| 函数 | 用途 |
|---|---|
| `jsonl_source(path, *, limit=None)` | 把 JSONL 文件当种子流式读，每行对应一条流水线 |
| `seed_factory(kind="range", *, n=10, path=None, limit=None)` | 配置的 `source:` 块最终解析成的实现：`range` 或 `jsonl` |

```python
from pyattacker import jsonl_source

runner.run(template.map(jsonl_source("dataset.jsonl", limit=500)))
```

---

## 监控

### `runner.stats()`

进程内实时快照。运行中途安全调。

```python
live = runner.stats()
live["in_flight_pipelines"]      # attempts actually running
live["delayed_pipelines"]        # parked in a retry backoff, holding no worker
live["counters"]                 # admitted / succeeded / failed / done
live["pools"]                    # per-pool occupancy, health and wait percentiles
```

### `StatsServer`

```python
StatsServer(store, *, host="127.0.0.1", port=8787, run_id=None, errors=10)
```

无依赖的只读 HTTP 视图。它为每个请求开一条全新的只读连接，所以和正在跑的
任务并排跑。

```python
from pyattacker import StatsServer

with StatsServer("runs/qa.db", port=8787) as server:
    print(server.url)            # http://127.0.0.1:8787
    server.wait()                # or just let your program continue
```

| 端点 | 返回 |
|---|---|
| `/` | 一个小巧的自动刷新仪表盘 |
| `/stats`, `/events`, `/pipelines`, `/resources`, `/errors` | JSON |

`/stats` 带 `handoffs_total`（所选运行的提交数，没运行过滤器则全部提交），每行 `/pipelines` 带
`handoffs`（活动执行记录的计数）、`handoffs_historical`（全部记录的计数）和
`handoff_floor`，紧挨着 `n_tasks_done`/`n_tasks_total`——开控制流的流水线上，这两者是链中的
**位置**，不是已跑任务数，所以非零的 `handoffs` 才说明"这条
流水线跳过了站点"。

**它没认证，会把你的工件载荷给出来。** 正因如此，它只绑回环地址。把它
暴露到其他任何地方之前，先架你自己的代理。`pyattacker serve` 就是同一功能的
命令行版本。

`pyattacker.monitor.render_snapshot(snapshot)` 和 `read_snapshot(store)` 是 `pyattacker watch` 背后的
终端渲染器，你想嵌同样的视图就用。从子模块导入它们：
`from pyattacker.monitor import render_snapshot, read_snapshot`。

---

## 接下来读什么

| 文档 | 内容 |
|---|---|
| [`docs/tutorial.md`](tutorial.md) | 引导式路径：十四个可运行的步骤 |
| [`docs/cli.md`](cli.md) | 命令、flag、退出码、配置文件格式 |
| [`docs/design.md`](design.md) | 模型、不变量，以及这些 API 背后的权衡 |
| [`examples/`](../../examples) | 完整程序，包括对多种流水线形态的实测比较 |
