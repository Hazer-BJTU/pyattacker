# API 参考

[English](../reference.md) | **简体中文**

`pyattacker` 中的每个公开名称，及其签名、参数和用法示例。

**想找其他内容？** [`docs/tutorial.md`](tutorial.md) 逐步讲解工作流程；
[`docs/cli.md`](cli.md) 记录命令行；[`docs/design.md`](design.md) 解释这套模型为何
如此设计。

## 此处使用的约定

* [目录](reference.md#目录) 表中的所有内容都可以从顶层包导入：
  `from pyattacker import Runner`。少数辅助类型由这些 API *返回*，但自身并未
  导出（`PoolStats`、`DeclarativeSpec`、`TaskRecord`、`RunRecord`、`Store` 协议）；
  遇到这种情况时本页会给出子模块名，通常你无需手动导入它们。
* 签名按其在源码中的形式书写。签名中的 `*` 表示其后的所有参数
  均为仅限关键字（keyword-only）参数。
* **此处的默认值很重要。** 有两个默认值尤其令人意外：`Retrying(max_attempts=1)` 意味着除非你主动要求，
  否则*不重试*；`Runner(store=":memory:")` 意味着除非传入路径，否则不会持久化任何内容。
* **有些示例是完整程序。** 第一行是 `# reference/<name>.py` 的代码块会在每次测试运行时被写出并执行
  （见 [`tests/test_docs_examples.py`](../../tests/test_docs_examples.py)）——README 与 CLI 文档共享
  这一契约。没有标记的代码块是刻意的片段。

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

任务是一元函数：一个值进，一个值出。它可以是同步或异步的，并且接受
`(value)` 或 `(value, ctx)`。任何其他签名都会在装饰时抛出 `ConfigError`。

### `task`

```python
@task(name=None, *, resource=None, algorithm=None, retry=None, timeout_s=None, config=None, version=None) -> TaskSpec
```

把函数转成 `TaskSpec`。可以裸用（`@task`）、带名称使用（`@task("ask")`），或带选项使用。

| 参数 | 类型 | 默认值 | 含义 |
|---|---|---|---|
| `name` | `str` | 函数名 | 任务在记录和事件中的名称 |
| `resource` | `str` | `None` | 未指定名称的 `ctx.acquire()` 调用所使用的默认资源池 |
| `algorithm` | `str` 或算法 | `None` | 默认获取策略；回退到资源池自身的策略 |
| `retry` | `Retrying` 或 `dict` | 不重试 | 某次尝试抛出异常时应用的策略 |
| `timeout_s` | `float` | `None` | 单次尝试的墙钟时间上限；**仅异步任务** |
| `config` | JSON 映射 | `{}` | 声明的行为，会被快照进恢复指纹 |
| `version` | `str` | `None` | 为外部行为或动态代码显式指定的修订版本 |

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

在同步任务上设置 `timeout_s` 会被接受，但无法生效——同步函数占着事件循环，因此
框架没有任何时点可以取消它。请把阻塞型工作放到 `await asyncio.to_thread(...)` 后面。

若任务需要的不止是值和上下文，则从闭包中获取：

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

`@task` 所基于的函数。当装饰时还不知道目标时直接调用它——声明式层就是
这样处理 `use:` 的。传入已有的 `TaskSpec` 并附带覆盖项，返回的是新
spec，而不是就地修改它。

### `TaskSpec`

任务的不可变描述。你很少会亲自构造它；它由 `@task` 交给你，你再传给
`pipeline(...)`。

| 属性 | 含义 |
|---|---|
| `name` | 任务在记录中出现的名称 |
| `fn` | 被包装的可调用对象 |
| `resource`, `algorithm`, `retry`, `timeout_s` | 声明的各项策略 |
| `accepts`, `returns` | 类型提示，用于构建期的链校验 |
| `takes_ctx` | `fn` 是否接受 `(value, ctx)` |
| `module`, `qualname`, `code_digest` | 标识代码的这一版本；会被并入流水线摘要 |
| `config`, `version` | 显式声明的行为和修订版本 |
| `parameters`, `children` | 工厂参数和嵌套 spec；与用户 config 分开记录 |

| 方法 | 返回值 |
|---|---|
| `is_async` | `fn` 是否为协程函数 |
| `fingerprint(*, include_code=True)` | 提供给 `spec_digest` 的字典；包含嵌套 spec |
| `runtime_algorithm()` | 由捕获到的同一性配置派生出的运行时算法 |
| `with_overrides(**kwargs)` | 替换了指定字段的新 spec |

`with_overrides` 有三种不同情形，且都对用户可见：

* **未传入**的关键字保持当前值；
* 显式的 `None` 会**清空**支持为空的字段——`resource`、`algorithm`、
  `timeout_s`、`version`。工厂自身的 `resource=` 或 `algorithm=` 就是这样被移除的；
* `UNSET`（导出为 `pyattacker.UNSET`）即使在键存在时也表示“未传入”，因此
  总是发出同一组键的构建器可以转发自己的字典，而不会清空调用方未提及的所有内容。
  声明式加载器正是这么做的。

对不能为空的字段（`name`、`fn`、`retry`、`children`、`config`、`parameters`）传 `None`
会抛出 `ConfigError`，而不是静默地什么都不做。清空 `config` 意味着 `config={}`；清空 `children`
意味着 `children=()`。

两个 `TaskSpec` 用 `|` 组合成 `Chain`。`spec | other` 自身不做任何校验；校验发生在
`pipeline(...)` 中。

### `Retrying`

```python
Retrying(max_attempts=1, on=(), retry_classified=True, retry_unknown=False,
         base=0.5, factor=2.0, cap=30.0, jitter="full", max_total_s=None)
```

| 字段 | 默认值 | 含义 |
|---|---|---|
| `max_attempts` | `1` | 包含首次在内的总尝试次数——**默认不重试** |
| `on` | `()` | 额外视为可重试的异常类型 |
| `retry_classified` | `True` | 重试可重试的错误类别（`rate_limit`、`timeout`、`connection`、`upstream`、`retryable`） |
| `retry_unknown` | `False` | 也重试 `unknown`——`ResourceUnavailable` 需要它 |
| `base`, `factor`, `cap` | `0.5`, `2.0`, `30.0` | 延迟为 `min(cap, base * factor**(attempt-1))`，随后施加抖动（jitter） |
| `jitter` | `"full"` | `"none"`、`"full"` 或 `"equal"` |
| `max_total_s` | `None` | 一旦已用时间加上下一次延迟会超过该值就放弃 |

```python
Retrying(max_attempts=5, base=0.5, cap=30.0)                    # typical API client
Retrying(max_attempts=3, on=(MyProviderError,))                 # add your own exception type
Retrying(max_attempts=3, retry_unknown=True)                    # retry capacity shortages too
Retrying(max_attempts=10, max_total_s=120.0)                    # bounded by time, not just count
```

凡是接受 `Retrying` 的地方都可以接受 `dict`，YAML 形式正是靠这一点生效：
`retry={"max_attempts": 3, "on": ["RetryableError"]}`。

| 方法 | 返回值 |
|---|---|
| `should_retry(exc, error_class=None)` | 在该策略下这个异常是否可重试 |
| `delay_for(attempt, rng, retry_after=None)` | 延迟秒数；当服务端给出建议时以 `retry_after` 为准 |

### `TaskContext`

双参数任务收到的 `ctx`。由 `Runner` 按每次尝试构造；永远不由你构造。

| 属性 | 含义 |
|---|---|
| `pipeline_id`, `run_id`, `task_name`, `seq` | 进行中工作的同一性 |
| `attempt` | 从 1 开始的尝试序号——`if ctx.attempt > 1:` 就是你检测重试的方式 |
| `bus` | 本次运行的 `Bus` |
| `store` | 本次运行的存储，供需要读取历史的任务使用 |
| `meta` | 自由形式的字典，每次尝试各一份 |

#### `ctx.acquire`

```python
ctx.acquire(pool=None, *, algorithm=None, timeout=None, where=None, **selector)
```

返回一个产出 `Lease` 的异步上下文管理器。**这是使用资源的推荐方式**——
在每条退出路径上都会归还租约，包括异常、取消和超时。

| 参数 | 含义 |
|---|---|
| `pool` | 资源池名称或对象；默认为任务的 `resource=` |
| `algorithm` | 为这一次调用覆盖获取策略 |
| `timeout` | 若未及时获得资源则抛出 `AcquireTimeout` |
| `where` | `Callable[[Resource], bool]`，用于选择器无法表达的谓词 |
| `**selector` | 按 `id`、`kind`、`tags` 和 `options` 匹配，包括点路径 |

```python
async with ctx.acquire(model="gpt-4o") as lease:                     # by option
    ...
async with ctx.acquire("judges", id="judge-a", timeout=30) as lease: # explicit pool, bounded wait
    ...
async with ctx.acquire(where=lambda r: r.options["ctx_len"] >= 32000) as lease:
    ...
```

在默认的 `wait` 算法下，匹配**不到**任何资源的选择器会永远等待。如果你宁愿报错也不愿挂起，请传
`timeout=` 或使用 `algorithm="immediate"`。

| 其他方法 | 用途 |
|---|---|
| `await ctx.acquire_lease(...)` | 返回裸 `Lease` 的逃生通道；你必须自行归还。忘记归还的租约会在任务结束时被强制回收，并记录为 `lease.leaked` |
| `ctx.publish_resource(pool, resource)` | 在运行时添加资源；其他流水线可以立即租用它 |
| `ctx.revoke_resource(pool, resource_id, reason="")` | 撤回某个资源 |
| `ctx.subscribe(pool, events=None)` | 资源池事件的异步迭代器 |
| `ctx.held_leases()` | 本次尝试当前持有的租约 |
| `ctx.reclaim_now()` | 强制归还当前持有的全部租约；同步且不可中断 |
| `ctx.emit(kind, **data)` | 把你自己的事件写入本次运行的事件流 |

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

某个任务在重试策略改变后的副本——用于在不同策略下复用该任务，而无需
重新定义它。

```python
from pyattacker import with_retry

careful = with_retry(ask, max_attempts=6, cap=60.0)
pipeline("qa", prepare | careful | judge)
```

注意，这会改变流水线的 `spec_digest`，从而改变其同一性：用
`careful` 构建的流水线与用 `ask` 构建的流水线不共享检查点。

---

## 流水线

流水线是任务的线性链，也是完成与恢复的单位。在你给它种子之前，它是一个*模板*；
`map()` 会把每个种子转成独立的 `PipelineSpec`。

### `pipeline`

```python
pipeline(name, *tasks_or_chain, tags=None, include_code=True, registry=None, control=None) -> PipelineTemplate
```

| 参数 | 含义 |
|---|---|
| `name` | 流水线名称，会记录在每一行上 |
| `*tasks_or_chain` | 一条链（`a \| b \| c`），或作为多个独立参数传入的若干 `TaskSpec` |
| `tags` | 自由形式的字典，随每条流水线一起存储，供后续过滤 |
| `include_code` | 为 `True`（默认值）时，每个任务的源码摘要都是流水线同一性的一部分 |
| `registry` | 用于非 JSON 工件类型的自定义 `CodecRegistry` |
| `control` | **进阶**（见[交接](reference.md#进阶交接可选启用)）：哪个任务可以向哪里交接；`None`（默认值）会让流水线保持为普通的线性链 |

```python
from pyattacker import pipeline

template = pipeline("qa", prepare | ask | judge, tags={"bench": "mmlu"})
template = pipeline("qa", prepare, ask, judge)          # equivalent
template = pipeline("qa", ask)                          # a single task is a valid pipeline
```

链**在这里**校验，而不是在运行中途：如果 `prepare` 返回 `dict`，而下一个任务要求 `int`，
这里会立即抛出 `PipelineBuildError`。校验依据注解——接受子类，`Any` 或
缺少注解都视为宽松，裸容器接受其参数化形式。`returns` 注解中的 `Handoff` 成员
是一种逃生通道：`-> Handoff | Report` 按 `Report` 校验，而单独的 `-> Handoff`
可以与任何类型链接（该路径上的任务完全不产生工件）。

`include_code=False` 适用于你确实想让任务体发生变化、又不愿放弃
现有检查点的情况。默认值取的是安全方向：代码被修改就意味着新的流水线。

### `PipelineTemplate`

| 属性 / 方法 | 返回值 |
|---|---|
| `name`, `tags`, `tasks`, `spec_digest` | 声明内容 |
| `n_tasks` | 链中有多少个任务 |
| `task_names` | 按顺序排列的任务名称 |
| `describe()` | 可转成 JSON 的概要（即 `validate` 打印的内容），当存在解析后的 `control` 块时也会包含它 |
| `control` | 解析后的边计划（**进阶**），或 `None` |
| `bind(seed, *, key=None, repeat=0)` | 由一个种子得到一个 `PipelineSpec` |
| `map(seeds, *, repeats=1, key_of=None)` | `PipelineSpec` 的惰性迭代器 |

#### `map`

```python
map(seeds, *, repeats=1, key_of=None) -> Iterator[PipelineSpec]
```

| 参数 | 含义 |
|---|---|
| `seeds` | **任何可迭代对象**，包括生成器——它是惰性消费的 |
| `repeats` | 每个种子 k 条独立流水线：pass@k 与自洽性采样 |
| `key_of` | `Callable[[seed], str]`，用你自己提供的稳定 id 代替按内容寻址的 id |

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

当 `repeats>1` 且指定了 `key_of` 时，键会变成 `f"{key}#{repeat}"`。

### `PipelineSpec`

一条等待运行的流水线：种子加上链。`spec.pipeline_id`（也是 `spec.key`）是
按内容寻址的同一性，正是它让恢复与分片得以工作。

| 属性 | 含义 |
|---|---|
| `pipeline_id` / `key` | 规格（spec）摘要、种子摘要和重复索引的 `blake2b` |
| `seed` | 数据集中的一行 |
| `repeat` | 这是 pass@k 中的第几个样本 |
| `name`、`tasks`、`n_tasks`、`tags` | 继承自模板 |
| `control` | 解析后的边计划（**进阶**），或 `None` |

### `Chain`

`a | b | c` 产生的东西。`chain.tasks` 是规格组成的元组。只有在你编写以编程方式组合
流水线的代码时，才需要用到这个类型：

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

任务链指纹（`v2:` 后接 32 个字符的十六进制摘要）。可用于在运行任何东西之前，检查某次代码改动
是否会让现有检查点失效：

```python
from pyattacker import compute_spec_digest

if compute_spec_digest(new_chain.tasks) != stored_digest:
    print("this chain will start fresh pipelines, not resume the old ones")
```

摘要涵盖每个任务的名称、资源、`timeout_s`、源码摘要，以及会改变步骤耗时的五个重试字段
（`max_attempts`、`base`、`factor`、`cap`、`jitter`）。它有意排除
`on`、`retry_unknown` 和 `max_total_s`。

`control` 块**仅在存在时**才会被折算进去，因此这一特性没有改变任何既有的摘要：不含 `control` 的
流水线保持其精确的同一性（因而也保持流水线 id、检查点和分片分配），与交接（handoff）出现之前
完全一致。声明的边以解析后的形式参与摘要计算——seq 目标、已排序——因此把目标写成名称
还是写成 seq，都是同一条流水线。

---

## 进阶：交接（可选启用）

**进阶层级：可选启用、会改变执行模型、普通流水线不需要、在 1.0 之前属于实验性。** 任务可以通过返回一条
框架自有的指令而不是一个值来*向前跳过*；流水线会在声明中更靠后的位置继续（或就地结束），
框架会持久地记录这次跳转。在这里什么都不声明的流水线完全不受影响——
完整论证见设计文档
[§4.8](design.md#48-进阶交接--声明式正向跳转可选启用实验性)。反向遍历——`Handoff.rewind`、
`Handoff.retry_all` 以及可选的载荷历史——是同一特性中*单独声明*的层级：见
[进阶：反向遍历](#进阶反向遍历rewindretry-allvisits)。

### `Handoff`

```python
Handoff.to(target, value=UNSET, *, reason="") -> Handoff
Handoff.end(value=UNSET, *, reason="") -> Handoff
```

| 字段 / 方法 | 含义 |
|---|---|
| `target` | 任务名、任务的 seq，或在 `END` 的情况下为 `None` |
| `value` | 目标的入口状态。`UNSET`（默认值）表示“复用本任务收到的工件”；`None` 是一个真实的载荷 |
| `reason` | 自由格式字符串，记录在账本和 `pipeline.handoff` 事件中 |
| `is_end` | 该指令是否结束流水线 |
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

使用前值得了解的规则：

* **交接是一次返回，绝不是失败。** 不会查询重试策略（不会添加 `decision` 原因，
  而 `retry.on=(Exception,)` / `retry_unknown=True` 也无法把它变成一次重试），`async with
  ctx.acquire(...)` 已经归还了它的租约，被取消或超时的尝试也永远不会走到这次
  返回。在 `strict_leases=True` 下，泄漏的租约会让任务失败，交接也不会被承认。
* **边是声明的，不是推导出来的。** 返回 `Handoff` 时若没有 `control` 块，或沿着一条并非从*那个*任务
  声明的边返回，都属于 `FatalError`（绝不重试，也绝不是静默跳转）。
* **只能正向。** 目标必须严格晚于其来源。`"end"` 是合法的目标，
  但从最后一个任务出发时除外：在那里它没有任何效果，会被拒绝。
* **重复的任务名需要 seq。** “ask” 出现两次会因有歧义而被拒绝；请改为以 seq `2` 为目标。
* **载荷不做类型检查。** 交接的参数是任意值，而不是来源任务正常的
  返回类型；它会像任何工件一样，用流水线的 `CodecRegistry` 编码。
* **交接绝不会来自 N 个分支之一。** `fanout` 会拒绝由分支返回的指令，
  因为在记录中一个分组就是一步，而一次控制转移无法归因于若干个并发分支
  中的某一个。
* **游标变成位置。** 在启用控制流的流水线上，`n_tasks_done` 表示执行到哪里，
  而不是跑了多少个任务，跳过的槽位也没有任务行。不要把它渲染成完成百分比；
  取而代之的是，交接计数就在它旁边暴露出来。
* **稳定性。** 上面这些保证属于稳定的部分；拼写形式（`Handoff`、`control`）
  在 1.0 之前仍可能改变。反向遍历是一个*单独声明*的可选启用层级，而不是这个正向模型的
  一部分——见 [进阶：反向遍历](#进阶反向遍历rewindretry-allvisits)。

### 一次跳转记录的内容

| 位置 | 记录的内容 |
|---|---|
| `tasks.state = "handed_off"` | 来源任务干净地结束，且没有产生工件 |
| `attempts.outcome = "handed_off"` | 发生跳转的那次尝试，`decision` 为空 |
| `artifacts` | 入口状态：复用的工件，或位于 `seq = n_tasks + k` 的新载荷 |
| `handoffs` | 账本行：from/to、`entry_artifact_id`、`entry_reused`、`reason` |
| `pipeline.handoff` 事件 | 审计轨迹（强杀可能丢失它；账本是权威） |

`HandoffRecord`（由 `pyattacker` 导出）就是那条账本行：`handoff_id`、`pipeline_id`、`run_id`、
`from_seq`、`from_task`、`to_seq`/`to_task`（`END` 时为 `None`）、`entry_seq`、`entry_artifact_id`、
`entry_reused`、`reason`、`ts`。

---

## 进阶：反向遍历（rewind、retry-all、visits）

**进阶层级：可选启用、会改变流水线的遍历方式、在 1.0 之前属于实验性。** 反向操作与上面的正向模型分开
声明，因此一条什么都不声明的流水线会保留它的同一性、它在访问 0 的工件地址、它的随机流以及它的
`spec_digest`。当一个站点判定**更早**的某个站点必须带着作者选定的状态重跑时读这一节——校验失败后
重新生成、用不同参数重试某个步骤——并且两次运行都必须留在记录里，而不是折叠成一个任务。
[tutorial](tutorial.md#第-16-步--高级用回退和全部重试重新生成) 用两步把它搭起来，第二步讲的是
[载荷历史](tutorial.md#第-17-步--高级让载荷自带历史)。

### `Handoff.rewind` 与 `Handoff.retry_all`

```python
Handoff.rewind(target, value, *, reason="") -> Handoff   # explicit state is required; None is a value
Handoff.retry_all(*, reason="") -> Handoff               # restart at seq 0 from the original bound seed
```

| 字段 / 方法 | 含义 |
|---|---|
| `target` | 任务名或任务的 seq，始终**严格早于**来源 |
| `value` | 目标入口状态。`rewind` 要求提供；`retry_all` 不接受任何值 |
| `reason` | 自由文本字符串，记录到交接账本和 `pipeline.handoff` 事件中 |
| `operation` | `"rewind"` 或 `"retry_all"`：账本行说这次转移是什么 |

两者都像 `Handoff.to` 和 `Handoff.end` 一样从任务中返回，正向规则也照旧成立：交接是返回值而不是失败
（不会咨询重试策略，租约已经释放，被取消或超时的尝试永远不会走到这个返回），指令永远无法从
`fanout` 分支里逃出，未声明或形式错误的指令是 `FatalError` 而不是静默跳转。不同之处在于：

* **状态由你选择；框架不做任何回滚。** `rewind` 要求显式的值——`None` 是真实的值，不是“复用输入”
  ——并且目的地必须是已声明、严格更早的任务，用唯一的任务名或它的 seq 指定。自回退和把 `end` 当作
  回退目标都会被拒绝；链中重复出现的名字必须用 seq 指定；**目标之前**的结果保持有效，而目标及其
  之后的结果变为历史，并带着各自的访问重新运行。
* **正向仍然是正向。** `Handoff.to()` 继续使用 `control.edges`，永远不会获得隐式的反向语义；
  对于纯反向的流水线，正向声明是可选的。
* **`retry_all` 重放绑定的种子。** 它会从 seq 0 重新开始，种子是**绑定时刻捕获的字节新鲜解码**出来的
  结果，因此事后修改某个任务的输入或 `spec.seed` 不会改变实际运行的内容。它不接受替换值：若想用
  *不同*状态重启，请从更靠后的任务调用 `Handoff.rewind(0, chosen_state)`。它可以声明在第一个任务上，
  包括单任务流水线；它不会创建另一个映射行或重复项、不会重置资源、也不会启动另一次 CLI 运行——
  它会清空有效的任务结果，同时保留访问、工件、尝试以及已消耗的控制预算。
* **异常重试是另一套机制。** `Retrying` 在同一个访问内重试一次尝试；`rewind` 和 `retry_all` 是返回的
  控制指令，绝不触发失败重试策略。编写错误和预算耗尽都属于致命失败，该策略无法重试。

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
| `edges` | `{source: [later targets]}` | 正向跳转（`Handoff.to` / `Handoff.end`）；只声明反向操作时可以省略 |
| `rewind` | `{source: [strictly earlier targets]}` | 允许从每个来源发出 `Handoff.rewind` |
| `retry_all` | `[sources]` | 允许从每个来源发出 `Handoff.retry_all` |
| `max_handoffs` | 正整数 | 一旦出现 `rewind` 或 `retry_all` 就**必需**：这条流水线有限的控制预算 |

`max_handoffs` 和其他声明一样要经过校验：`3.0`、`True`、`"3"` 和 `0` 都会被拒绝，而没有任何反向操作
却给了 `max_handoffs` 也会被拒绝（`control: max_handoffs requires backward operations`）。
`RunConfig.max_handoffs`（默认 1000，在配置中写作 `run.max_handoffs`）是**运行时上限**：实际限制为
`min(control.max_handoffs, run.max_handoffs)`，因此一次运行可以调低某条流水线的预算，但永远不能调高。
全新开始会重置它。名字和 seq 的解析方式与 `edges` 完全相同（精确的任务名优先于数字字符串，重复出现的
名字必须用 seq 指定），每个问题都会以配置字段路径的形式报告——见
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

`ctx.visit` 对**每个站点**都从 0 开始，并在每次全新进入该站点时递增——回退之后的普通后继进入也算——
而 `ctx.attempt` 编号的是*同一个访问内*的尝试。两者合起来才让一次重新生成保持可见，而不是被藏起来。

| 位置 | 说明 |
|---|---|
| `pipeline_id:seq` | 访问 0 的工件 id；重访使用 `pipeline_id:seq#visit` |
| `TaskRecord.visit`、`AttemptRecord.visit`、`Artifact.visit` | 该行是哪一个发生实例 |
| `store.get_artifact(pipeline_id, seq)` | 该站点**有效**的输出，处于它当前的访问 |
| `store.get_artifact_by_id(artifact_id)` | 一个**精确的历史**发生实例，包含已被取代的那些 |
| `store.visit_state(pipeline_id)` | 游标、待定入口、有效槽位、按 seq 的计数器、已消耗的交接数 |
| `PipelineRecord.n_tasks_done` | 反向流水线中的*位置*，不是完成度计数 |

访问也参与派生随机性：RNG 和 `ctx.seed` 在访问 0 时与旧的派生方式逐字节一致，而在重访时把访问计入，
因此一次重新生成会采出不同的样本。它们并不会让外部副作用变成恰好一次——当每次重新生成都应当是一次
新的外部操作时，请把 `ctx.visit` 纳入幂等键（见[外部副作用](#外部副作用)）。

待定入口引用它确切的输入，恢复会保留它的访问并继续已消耗的尝试编号：尝试编号在任务代码运行之前就已
保留，因此硬杀可能在已完成的尝试行中留下空缺，但永远不会重用某个编号。对未提交的工作，框架的恢复
保证是至少一次。

### 预算与终止

反向遍历在结构上不再有限，这正是预算必须显式且强制的原因。启用反向的流水线里每一次非终止转移都要
计数，包括正向的 `edges` 转移，而且**N 恰好允许 N 次转移**：第 N+1 次会在发布转移或使结果失效*之前*
被拒绝，因此记录仍然精确描述了已提交的内容。`END` 可以在到达上限时直接完成，而不消耗另一次转移。

已消耗的计数会跨 resume、全部重试以及载荷缺失时的自动种子回退保留下来；只有显式的全新开始才会开启
新的预算生命周期。访问和审计行也会在这次重启中保留，因此历史发生实例仍然可以寻址。

### 恢复与所有权

打开一条已存储的行会做什么取决于那一行，这些规则是让人读的，不是让人猜的：

| 已存储的行 | 这次运行会做什么 |
| --- | --- |
| `failed`、`interrupted` | 普通检查点恢复：精确的持久化访问——访问编号、待定入口、已消耗的尝试编号——继续 |
| `running`，`resume=True` | 操作者声明上一个所有者已经消失。`interrupt_stale` 会先回收心跳过期的行；随后精确的持久化访问继续 |
| `running`，没有 `resume` | **跳过**，绝不接管：持久化的待定访问可以在崩溃后继续，因此第二个写入者会把一次遍历分叉。`pipeline.skipped` 会带上 `reason="owned_by_another_run"` 以及所有者的运行 id |
| 行已持久化，遍历消失 | 拒绝：`corrupt visit checkpoint: missing traversal state`。`fresh_restart=True` 是文档给出的丢弃并重来的方式 |
| `succeeded` | 跳过，除非 `retry_succeeded=True`；此时重启会从绑定的种子以全新预算运行 |

`fresh_restart=True` 是唯一会丢弃持久化进度的开关：它清空有效的遍历和任何待定入口（把它留下仍在飞行
中的每个任务行结算为 `interrupted`），从不可变的绑定种子重新开始，重置控制预算并使先前的账本失效
——同时保留访问计数器和带访问限定的审计行，因此历史发生实例仍然可以寻址，而遍历丢失的存储会从这些
行重建它的计数器。它同样适用于正向流水线，在那里它的意思是“忽略检查点，重跑整条链”：追加式历史
也会保留，但任务和链上工件的地址会按设计被重用，而不是保留为独立的发生实例（
[存储恢复契约](#表与读取器) 把这一区别讲清楚了）。把它和 `retry_succeeded=True` 结合使用，可以重启
一条已经成功过的流水线。全新开始发出的是 `pipeline.restarted`（带上被丢弃的游标），而不是
`pipeline.checkpoint_missing`：什么都没有丢。

待定载荷缺失或不可用会发出 `pipeline.checkpoint_missing` 并建立一次种子重放，保留预算和计数器。
流水线的首次执行永远不会走这条路径——它的输入就是它已经持有的绑定种子，因此 `journal="summary"`
存储（其写入的种子载荷被有意丢弃）不会报告一次它从未有过的检查点故障。摘要日志和 `null` 后端可以
在进程内跑循环，但无法恢复它们缺失的载荷。一次指向 seq 0 的待定回退会使用它选定的载荷，而不是触发
种子重置。反向转移通过定时器泵重新排队，从而把 worker 让给其他流水线。

### 检视一次反向运行

只有启用反向的流水线，其流水线导出才会多一条 `control` 遍历记录。它包含 `cursor`、`pending`、有效的
`active` 槽位、持久化的 `counters`、已消耗的 `handoffs`、`version`、精确的当前 `input` 以及 `terminal`
引用。嵌套的任务行和工件行包含 id、访问和 `active` 标记，而单独的任务/尝试/工件导出会包含 `visit`
（尝试还带有它们的 task-run id）。既有的顶层导出行种类这个闭集没有改变。HTTP `/pipelines` 视图对
启用反向的行会包含遍历状态和 `cursor_kind="position"`；纯正向的行两者都没有。

报告的作用域是它所覆盖的那次运行，统计的是那次运行里重复的访问和尝试，因此在一次 resume 之后它显示
的是新运行的工作量，而存储和导出保留此前每一行。这些合计是工作量，不是完成百分比。

### 存储能力

本节背后的可选存储方法——`visit_state`、`reset_visits`、`commit_entry`、`commit_visit_attempt`、
`commit_visit_success`、`commit_control_transition`、`repair_visit_terminal` 和 `get_artifact_by_id`
——在[存储 → 可选的存储能力](#可选的存储能力)中说明。它们位于基础 `Store` 协议之外，因此第三方后端
对普通流水线仍然可用；在未通过探测的存储上打开启用反向的流水线，会在开始之前就被拒绝并抛出
`ConfigError`，绝不会被降级为非持久的循环。

同一项特性还掌管存储的**特性级别**以及随之而来的兼容性规则：存储会一直停留在 `base`，直到首次提交
重访；打开未知级别的构建会直接拒绝该存储；处于 `visits-v1` 的 SQLite 存储会武装写入者守卫，
防止不具备谱系感知的写入者。见[存储 → 存储兼容性与备份](#存储兼容性与备份)。

---

## 运行

### `Runner`

```python
Runner(*, store=":memory:", pools=(), concurrency=16, clock=None, bus=None,
       registry=None, config=None, **config_overrides)
```

调度器。它拥有存储、资源池和 worker 槽位。任何 `RunConfig` 字段都可以
直接作为关键字参数传入。

```python
from pyattacker import Runner

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(rows))
    print(report.summary())
```

把它当作上下文管理器使用。关闭它也会关闭存储，因此要在 `with` 块*内部*读取**基于文件的**存储
里的行，或者在之后用 `open_store` 重新打开该文件。

| 方法 | 用途 |
|---|---|
| `run(specs, *, resume=False, **overrides)` | 运行至完成，返回一个 `RunReport`。在 `asyncio.run` 中包装 `run_async` |
| `await run_async(specs, *, resume=False, **overrides)` | 同上，但在已有的事件循环内 |
| `stats()` | 实时快照；可在运行中途安全调用 |
| `stop(reason="user")` | 请求运行优雅停止：停止接纳新工作，排空在途工作 |
| `stopping` | 是否处于停止过程中 |
| `run_id` | 当前运行的 id |
| `add_pool(pool)` / `pool(name)` | 注册或获取资源池 |
| `close()` | 关闭存储（上下文管理器会做这件事） |

```python
report = runner.run(template.map(rows), resume=True)              # resume
report = runner.run(template.map(rows), concurrency=8)            # override for one call

live = runner.stats()                                             # while running
print(live["in_flight_pipelines"], live["delayed_pipelines"])      # in flight vs parked in backoff
```

`run()` 接受任何 `PipelineSpec` 迭代器，因此无论数据集多大，用生成器都能让内存保持平稳。

### `RunConfig`

决定一次运行形态的一切。传入一个 `RunConfig`，或者把它的字段作为关键字参数传给 `Runner`。

| 字段 | 默认值 | 含义 |
|---|---|---|
| `store` | `":memory:"` | `":memory:"`、SQLite 路径、插件 URI，或一个已打开的存储实例 |
| `journal` | `"full"` | `"full"` 保留工件载荷（**任务级恢复所必需**）；`"summary"` 只保留元数据 |
| `concurrency` | `16` | 最多同时在途的尝试数；挂起在重试退避中的流水线不占用槽位 |
| `label` | `""` | 记录在该次运行上的标签 |
| `run_id` | `None` | 显式的运行 id；默认为时间戳 + 摘要 |
| `resume` | `False` | 在调度前把被已死亡运行遗弃的流水线标记为可恢复 |
| `retry_succeeded` | `False` | 重新运行已标记为成功的流水线。仅用于判定资格：它绝不会丢弃尚未成功的流水线的检查点或遍历记录 |
| `fresh_restart` | `False` | 让已接纳的流水线从绑定的种子重新开始：丢弃检查点/遍历记录，重置控制预算。只追加的历史会保留下来；反向流水线还会保留其访问发生实例与计数器 |
| `heartbeat_s` | `5.0` | 该运行的心跳多久写入一次 |
| `grace_s` | `5.0` | 优雅关闭在取消 worker 之前等待的时长 |
| `stale_after_s` | `30.0` | 若某次运行的心跳早于这个时长，它正在运行的流水线就被视为已遗弃 |
| `strict_leases` | `False` | 泄漏的租约会让任务失败（`LeaseLeakError`），而不是被悄悄强制回收 |
| `stop_after_failures` | `None` | 在 N 次失败后停止接纳工作（尽力而为） |
| `stop_after_s` | `None` | 经过这么长的墙钟时间后停止接纳工作 |
| `handle_signals` | `True` | 安装调用 `stop()` 的 SIGINT/SIGTERM 处理器 |
| `write_behind` | `None` | 批处理只追加的事实；`None` 表示对基于文件的存储开启 |
| `write_batch` | `128` | write-behind 生效时的批大小 |
| `flush_interval` | `1.0` | 两次刷写之间的秒数 |
| `artifact_backend` | `None` | 载荷存放的位置：`None`/`"inline"`、`"file:///path"`、`"null"`，或一个规格字典 |
| `max_handoffs` | `1000` | 启用反向的流水线中非终止控制转移的运行时上限；实际限制为 `min(control.max_handoffs, this)`（见 [反向遍历](#进阶反向遍历rewindretry-allvisits)） |
| `notes`、`meta` | `""`、`{}` | 自由格式，记录在该次运行上 |

```python
from pyattacker import RunConfig, Runner

config = RunConfig(store="runs/qa.db", concurrency=64, journal="full",
                   strict_leases=True, stop_after_failures=50)

with Runner(config=config, pools=[pool]) as runner:
    report = runner.run(template.map(rows))
```

`stop_after_failures` 在接纳时和每次完成时求值，因此已经接纳的流水线仍会跑完——
它是止血，而不是让时间倒流。

### `RunReport`

`run()` 返回的内容。

| 属性 / 方法 | 含义 |
|---|---|
| `run_id`、`status`、`duration_ms` | 运行的同一性与结果 |
| `stats` | 完整的统计字典，包括 `stats["pipelines"]["by_state"]` |
| `skipped` | 有多少流水线被跳过：因为它们已经成功，或者（反向遍历时）因为某条 `running` 行属于另一次运行而 `resume` 没有认领它 |
| `leases_leaked` | 有多少租约必须强制回收 |
| `repair_failures` | 本次运行无法写入最终状态、使其脱离撕裂终态的流水线；它是运行局部的，因此这类失败只有在这里才可见（该行保留其原始属主） |
| `stop_reason` | 运行为何提前停止（如果确实提前停止了） |
| `summary()` | 人类可读的多行报告 |
| `to_dict()` | 同样的事实，机器可读 |
| `export_jsonl(path, *, scope="store", run_id=None)` | 写出流水线行，返回数量 |

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

完成是按计数判定的，所以一次运行要求每条已接纳的流水线都到达终态。若一个 worker 在自身处理器
之外死亡——一个不是 `CancelledError` 的 `BaseException`，由框架自己的代码或它所调用的东西
抛出，例如第三方存储钩子——它就再也无法到达终态，而这次运行本来会永远等下去。
取而代之的做法是：

* 运行被硬停止（`stop_reason == "worker_crashed"`），因此其他 worker 也被切断，不再
  接纳任何新工作；
* 死亡 worker 当时持有的流水线被记为 `failed`——如果运行已经在停止过程中，则记为
  `interrupted`——并把逃逸的异常记在该行上；已经处于终态的流水线保留其状态，
  该状态是从存储读取的，而不是来自运行器的内存记录（存储并不需要回写到交给它的记录里，
  因此以持久化的那一行为准）；
* `runner.worker_crashed` 事件携带该流水线、该异常及其回溯；
* `run_async` 会抛出 `WorkerCrashed`（在运行记录以 `interrupted` 关闭之后），并把原始异常
  作为 `__cause__`。`run()` 会传播它，CLI 带一个具名错误退出，状态码为 2，因此崩溃的 worker
  不会看起来像一次已完成的运行。

```python
from pyattacker import WorkerCrashed

with Runner(store="runs/qa.db", pools=[pool]) as runner:
    try:
        report = runner.run(template.map(rows))
    except WorkerCrashed as exc:
        print(f"lost {exc.pipeline_id}: {exc.__cause__!r}")   # the run's record is already durable
        raise
```

*由任务*抛出的 `BaseException` 永远不会走到这条路径（任务自身的处理会像对待普通异常一样把它
收住），而取消保持其语义：被取消的 worker 记录 `interrupted` 并重新抛出。
`KeyboardInterrupt` 和 `SystemExit` 是唯一的边界：asyncio 会在任何监督者来得及运行之前停下
事件循环，因此 worker 自己记录该行和该事件，而中断仍然原样传播——调用方看到的是它自己的中断，
只有那条运行记录会落到无法完成的状态。

---

## 资源

`Resource` 是一项具体的外部能力——一个端点、一个 API key、一个本地 worker。`Pool` 是
它们的一组，外加一个默认的获取策略。资源池是流水线之间**唯一**共享的状态。

### `Resource`

```python
Resource.create(kind="generic", *, id=None, options=None, tags=None, capacity=1,
                factory=None, degrade_after=3, dead_after=8, cooldown_s=30.0, **meta) -> Resource
```

| 参数 | 默认值 | 含义 |
|---|---|---|
| `kind` | `"generic"` | 你自己选择的类别（`"llm"`、`"gpu"`、……）；可参与选择器匹配 |
| `id` | 自动生成 | 稳定的标识符，记录在每个租约上 |
| `options` | `{}` | 你的工厂读取的配置：base url、key、model。**可参与选择器匹配，包括点路径** |
| `tags` | `{}` | 额外的、可参与选择器匹配的标签 |
| `capacity` | `1` | **这一个资源**允许的并发租约数 |
| `factory` | `None` | `Callable[[Resource], client]`，每个资源惰性调用一次，在首次租约时 |
| `degrade_after` | `3` | 资源被熔断之前的连续失败次数 |
| `dead_after` | `8` | 资源被标记为失效之前的连续失败次数 |
| `cooldown_s` | `30.0` | 降级的资源在多久内不参与轮转 |

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

`capacity` 是按资源计的，因此资源池的总容量是各资源之和。把*那个总和*与 `concurrency` 比较：
worker 多于容量就意味着 worker 在等资源池。

工厂是客户端（client）所在之处：它只构建一次，并由该资源的每个租约共享。如果工厂抛出异常，
该租约会被拒绝，抛出 `ResourceUnavailable`（绝不会给出 `client` 为 `None` 的租约），同时记录一个
`resource.factory_failed` 事件，反复失败会让该资源走上同样的降级/失效路径。冷却会清除已存储的
错误，因此偶发的工厂失败仍能得到一次真正的重试。

| 属性 / 方法 | 含义 |
|---|---|
| `id`, `kind`, `options`, `tags`, `capacity`, `meta` | 与构造时一致 |
| `spec()` | 可直接序列化为 JSON 的描述，其中的密钥已掩码 |
| `lookup(key)` | 对某个选择器 key 返回 `(found, value)`，包括点路径 |

### `Pool`

```python
Pool(name, resources=(), *, kind=None, algorithm=None, bus=None, clock=None,
     deadlock_warn_s=5.0, on_event=None)
```

| 参数 | 默认值 | 含义 |
|---|---|---|
| `name` | — | 任务引用它的方式（`resource="apis"`） |
| `resources` | `()` | 它启动时持有的资源 |
| `kind` | `None` | 之后添加的资源的默认 kind |
| `algorithm` | `Wait()` | 未覆盖该设置的任务所用的默认获取策略 |
| `deadlock_warn_s` | `5.0` | 当持有此资源池中某个资源的任务为另一个资源等待这么久时，发出 `acquire.suspected_deadlock` |
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
| `add(resource)` | 在运行时添加一个；选择器匹配的等待者会被唤醒 |
| `revoke(resource_id, reason="")` | 撤回一个 |
| `resources()` | 当前列表 |
| `stats(**selector)` | 聚合得到的 `PoolStats` |
| `snapshot()` | 每个资源的字典：state、leases、active、capacity |
| `subscribe(events=None)` | 资源池事件的异步迭代器 |

```python
print(pool.stats().utilization, pool.stats().waiting)
for slot in pool.snapshot():
    print(slot["id"], slot["state"], f"{slot['active']}/{slot['capacity']}")
```

两者都可以在运行期间安全调用——`watch` 和 `serve` 就是这么做的。

在运行途中添加端点同样可行，等待中的流水线会把它们取用起来：

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
| `client` | 工厂构建出的对象——你要调用的就是它 |
| `resource` | 它背后的 `Resource` |
| `options` | `lease.resource.options` 的快捷方式 |
| `held_ms` | 该租约已持有多久 |
| `task_name` | 持有它的任务 |

#### `lease.report`

```python
lease.report(*, ok=True, latency_ms=None, usage=None, error=None) -> None
```

**资源池正是借此获知一切。** 框架不会猜测某次失败是不是端点的过错，因此
健康的端点绝不会因为你的 JSON 解析器出错而被熔断。

| 参数 | 作用 |
|---|---|
| `ok=False` | 计入一次连续失败，供给 `degrade_after` / `dead_after` |
| `ok=True` | 清除失败计数器——这是一次恢复，能让已降级或已失效的资源复活 |
| `latency_ms` | 为每个资源维护 EMA |
| `usage` | 累加配额计数器，`quota_aware` 依据它排序 |
| `error` | 记录下来用于诊断 |

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
| `degrade(reason="")` | 立即让该资源退出轮转——例如你已知它的配额耗尽 |
| `release_now()` | 同步归还；逃生通道的对应手段，使用 `async with` 时不需要 |

### `PoolStats`

由 `pool.stats()` 返回（定义在 `pyattacker.resource` 中，未在顶层导出）。

| 字段 | 含义 |
|---|---|
| `active`, `capacity`, `utilization` | 当前占用情况 |
| `ready`, `degraded`, `dead` | 按状态统计的资源数量 |
| `waiting` | 当前排队的获取者数量 |
| `leases_total`, `ok_total`, `failed_total` | 吞吐量 |
| `total`, `revoked` | 资源数量 |
| `leaked_total` | 不得不被强制回收的租约 |
| `waits_total`, `wait_ms_avg`, `wait_ms_p50`, `wait_ms_p95`, `wait_ms_max` | 获取资源需要多长时间 |
| `usage` | 累计的 `lease.report(usage=...)` 计数器 |

`wait_ms_p95` 不断攀升，说明瓶颈在你的资源池而不是提供方。

### `Bus`

轻量的跨流水线信号总线，用于协调的推送一侧。

```python
count = ctx.bus.publish("found_answer", qid=row["qid"])      # returns subscriber count

async for message in ctx.bus.subscribe("found_answer"):      # "*" for everything
    ...
```

### `ResourceState`

`READY`, `DEGRADED`, `DEAD`, `REVOKED`。字符串枚举，因此日志中 `str(state)` 显示为 `"ResourceState.READY"`
而线上格式（wire format）使用 `.value`。

状态转换：连续失败 `degrade_after` 次后由 `READY` → `DEGRADED`，并持续 `cooldown_s`；连续失败达到 `dead_after`
次后 → `DEAD`。一次成功的 `report(ok=True)` 会把已降级或已失效的资源拉回 `READY`。冷却
到期并*不会*重置失败计数器——只有成功才会。

### `ResourceEvent`

资源池中的一次状态变化，会同时投递给等待者、订阅者、`events` 表和监控
快照。字段：`kind`, `pool`, `resource_id`, `data`, `ts`，外加 `as_dict()`。

---

## 获取算法

算法决定**如何从资源池中取出资源**。它与重试是两条独立的轴，重试决定
**工作失败之后**该做什么。可以按资源池设置（`Pool(..., algorithm=...)`）、按任务
（`@task(algorithm=...)`）、或按调用设置（`ctx.acquire(algorithm=...)`）。

每个算法都可以按名称使用，因此 `algorithm="backoff"` 和 `algorithm=Backoff(cap=60)` 都
有效；YAML 配置使用的就是字符串形式。

| 算法 | 没有空闲资源时 | 适用场景 |
|---|---|---|
| `Wait()` **（默认）** | 排队直到有槽位释放或 `timeout` 到期 | 稳态吞吐量 |
| `Backoff()` | 以指数退避 + 抖动等待 | 提供方已饱和；避免在释放时引发惊群效应 |
| `LeastBusy()` | 选择负载率最低者，之后回退 | 容量不等的多个端点 |
| `Failover()` | 按顺序尝试各资源池，之后回退 | 主/备提供方、分层的 key |
| `Sticky()` | 优先使用该流水线已用过的资源 | prompt 缓存、热连接 |
| `QuotaAware()` | 按剩余配额排序 | 受预算限制的端点 |
| `Immediate()` | 立即抛出 `ResourceUnavailable` | 卸载负载而不是排队 |

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

在依赖它们之前，有三个行为值得了解：

* **`Failover` 不会循环。** 它按顺序把列出的每个资源池各尝试一次；如果它们都没有容量，它的
  `fallback`（默认是 `Wait()`）只会挂起在 `pools[0]` 上。可以理解为“先试这些，最后定在
  主资源池上”。
* **`QuotaAware` 只是一种偏好，不是限制。** 当所有候选都耗尽时，其中最好的那个仍会被
  发放出去——拒绝工作比超支更糟。若要硬性停止，请在任务中跟踪预算
  并抛出异常。
* **`Immediate` 抛出 `ResourceUnavailable`，它被归类为 `unknown`**，而默认重试策略
  不会重试 `unknown`。这是有意为之——容量上的失误本就该大声暴露出来。若想重试容量问题，请把 `Immediate` 与
  `Retrying(retry_unknown=True)` 搭配使用。

获取时的退避与重试时的退避是两个不同的旋钮：`Backoff` 决定为一个*槽位*等待
多久，`Retrying` 决定在一次*失败*之后等待多久。

### `resolve_algorithm`

```python
resolve_algorithm(spec) -> AcquireAlgorithm
```

把名称、字典或实例转换为算法。内建算法的解析先于插件，因此插件无法
遮蔽 `wait`。

```python
resolve_algorithm("backoff")
resolve_algorithm({"name": "backoff", "cap": 60.0})
```

要编写自己的算法，请实现 `AcquireAlgorithm` 协议（`pyattacker.algorithm`），并在
`pyattacker.algorithms` 入口点组下注册它——参见 [插件](reference.md#插件) 和 `examples/plugin_package/`。

---

## 错误

失败就是一个普通异常；没有需要满足的状态机。框架会对你抛出的任何异常
进行分类，并询问你的重试策略该怎么做。

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

`error_class_of(exc)` 把任意异常映射为下列字符串之一：

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

状态码依次从 `status`、`status_code`、`http_status` 或 `code` 读取，回退到
`exc.response.status_code`——这样无需导入就能覆盖常见的提供方 SDK。

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

在你自己的异常类上设置 `error_class` 属性同样有效；若异常暴露了 `Retry-After`
响应头，也会从中读取 `retry_after`。

每个决策都会被持久化，因此“它为什么重试了五次 / 为什么停止了”是一次查询，而不是
一场考古。

```python
for attempt in store.attempts(pipeline_id=pid):
    print(attempt.attempt_no, attempt.error_class, attempt.decision)
    # {'retry': True, 'reason': 'retryable', 'delay_s': 0.7, 'error_class': 'rate_limit', ...}
```

`reason` 是 `ok`、`retryable`、`attempts_exhausted`、`policy_declined`、`total_budget` 之一。

---

## 工件与编解码器

工件（artifact）是任务被持久化的输出。任务一成功就会写入，这正是让
检查点粒度是任务而不是流水线的原因。

### `Artifact`

| 字段 | 含义 |
|---|---|
| `pipeline_id`, `seq` | 它的同一性；`seq=-1` 是流水线的种子。在启用控制的流水线上，交接载荷位于 `seq >= n_tasks`（见[交接](reference.md#进阶交接可选启用)），因此 `seq` 只有对链而言才是任务位置。在[启用反向](#进阶反向遍历rewindretry-allvisits)的流水线中，第一个发生实例保留 `pipeline_id:seq`，而重访会把 id 限定为 `pipeline_id:seq#visit` |
| `task_name` | 由哪个任务产生 |
| `type_name`, `codec` | 如何恢复它 |
| `digest`, `size` | 载荷的 `blake2b` 及其长度 |
| `payload` | 编码后的字节；当字节存放在某个后端中或已被丢弃时为 `None` |
| `blob_ref` | 字节不是内联存放时所在的位置 |
| `is_final` | 它是否为该流水线的最终输出 |
| `available` | 载荷是否真的能读回来——**这正是恢复时检查的内容** |
| `encoded()` | 一个可直接交给 `registry.load(...)` 的 `Encoded` 三元组 |

```python
artifact = store.get_artifact(pipeline_id, 1)
if artifact.available:
    value = runner.registry.load(artifact.encoded())
```

相同的载荷会产生相同的摘要，因此原样返回输入的任务不会占用额外的存储。

### `CodecRegistry`

决定值如何变成字节。JSON 能处理 dict、list、标量和 dataclass；为任务标注
返回类型，该类就会被自动注册，因此恢复后的检查点会以你自己的类型返回，
而不是 dict。

| 方法 | 用途 |
|---|---|
| `register(codec, *, for_types=(), name=None)` | 添加一个编解码器，可选地绑定到特定类型 |
| `register_type(cls)` | 显式注册一个 dataclass（可用作装饰器） |
| `codec_for(obj)` | 哪个编解码器会处理这个值 |
| `dump(obj)` / `load(encoded)` | 编码 / 解码 |
| `type_name_of(obj)` | 记录下来的类型名 |

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

必须把注册表**同时**交给模板（用于种子）和 `Runner`（用于检查点）。声称能处理某载荷的编解码器
优先于更早的注册，因此专用编解码器不会沦为 JSON 兜底实现背后的死代码；显式的 `for_types=`
胜过自动扫描。

内置编解码器：`JsonCodec`（默认）和 `BytesCodec`（原始 `bytes`/`bytearray`）。把你自己的编解码器发布
到 `pyattacker.codecs` 入口点组下，它就会自行安装。

### `HistoryArtifact`

一个可选的载荷基类：让载荷自带应用状态的**具名**快照。它是解码后的载荷，不是持久化 `Artifact` 记录
的子类，runner 中也没有任何代码会读它来决定执行下一步去哪：它的存在是为了让一次
[回退](#进阶反向遍历rewindretry-allvisits)能送回作者选定的状态，同时它途经的那些状态仍然可检视。
普通字典仍然是普通字典——任务进入或完成时不会自动生成快照。教程在第 17 步
[把它搭了出来](tutorial.md#第-17-步--高级让载荷自带历史)。

```python
HistoryArtifact(state, *, history=None, selected=None, next_snapshot=0)
```

| 成员 | 含义 |
|---|---|
| `state` | 当前应用状态，作为分离的深拷贝 |
| `history` | 全部快照，最旧的在前，作为分离的记录：`{id, label, state, metadata}` |
| `selected` | `restore` 最近一次选中的快照 id，或 `None` |
| `checkpoint(label, *, metadata=None)` | 新值，附带一个分离的快照；标签唯一，`snapshot:` 保留给稳定 id（`snapshot:0` 等） |
| `with_state(value)` | 新值，替换当前状态但不追加快照 |
| `snapshot(id_or_label)` | 一条分离的快照记录；未知或有歧义的选择器抛出 `KeyError` |
| `restore(id_or_label)` | 新值，其状态是该快照，**并且**其 `selected` 指向它；整段历史都保留，因此后续阶段仍可检视 |
| `prune(*selectors)` | 新值，去掉那些快照；当选择器指向被选中的快照时抛出 `ValueError`，并且 id 永不被重用 |

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

* **嵌套可变值永远不会与保留的快照互为别名。** `state`、`history` 和 `snapshot(...)` 返回的都是分离的
  拷贝，因此修改当前状态改不了过去。
* **状态与 metadata 必须可 JSON 序列化。** 编码会拒绝客户端、租约以及其他运行时对象。
* **版本化的 `history-v1` 编解码器同时保留快照和已注册的子类类型。** 子类继承基类的构造函数（应用
  字段放在 `state` 里）；自定义构造函数或额外属性的序列化不在这个接口的范围内，而未注册的子类会明确
  解码失败，而不是以普通 `HistoryArtifact` 的身份回来。
* **持久化是提交的事，不是 `checkpoint()` 的事。** 在任务里调用 `checkpoint()` 不会碰存储；runner
  在提交任务输出或控制转移时持久化该载荷，因此在那次提交之前崩溃可能丢掉内存里的快照。
* **历史是自包含的，并且会增长**，随快照的数量和大小增长——要有意识地 prune。它不取代框架的执行
  账本、任务行或访问记录。

### 辅助函数

| 函数 | 用途 |
|---|---|
| `canonical_json(obj)` | 确定性 JSON：键有序，不含无意义的空白 |
| `digest_of(data)` | 用于内容寻址的 `blake2b` 摘要 |
| `Encoded` | 一个冻结的 `(type_name, codec, data)` 三元组 |

---

## 存储

存储保存框架记录的每项事实。SQLite 是默认实现；内存存储具有完全相同的
语义，正是测试中应该使用的那种。

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
支持在运行写入存储的同时读取该存储——WAL 允许一个写入者和多个读取者。

### 表与读取器

| 表 | 每行对应 | 读取方式 |
|---|---|---|
| `runs` | 一次运行 | `store.get_run(id)` |
| `pipelines` | 一条流水线：状态、检查点游标、摘要、标签 | `store.pipelines(...)`, `store.export_rows()` |
| `tasks` | 一个任务：最终状态、已用尝试次数、耗时、错误 | `store.tasks(...)` |
| `attempts` | 一次尝试：结果（`succeeded`/`failed`/`timeout`/`cancelled`/`handed_off`）、错误类别、**重试决策**、租约、耗时 | `store.attempts(...)` |
| `artifacts` | 一个工件 | `store.artifacts(pid)`, `store.get_artifact(pid, seq)` |
| `handoffs` | 一次交接：起点/目标位置、入口工件、是否被复用、原因 | `store.handoffs(...)`（**可选能力**），嵌套在 `export_rows()` 中 |
| `events` | 一个结构化事件 | `store.events(...)` |
| `resources` | 一个资源：规格（spec，密钥已脱敏）和健康统计 | 包含在 `stats()` 中 |

| 方法 | 返回值 |
|---|---|
| `stats(run_id=None)` | 计数、状态分布、延迟百分位数；`handoffs_total` 统计记录下来的跳转次数（普通运行为 0） |
| `errors(*, run_id=None, limit=20)` | 失败项，含任务名、错误类型和消息 |
| `export_rows(*, run_id=None)` | 嵌套的流水线行：包含任务、工件和交接 |
| `attempts(*, pipeline_id=None, ...)` | 尝试历史 |
| `events(*, pipeline_id=None, limit=...)` | 事件流 |
| `close()` | 关闭连接 |

```python
# "Why did this pipeline take 40 seconds?"
for attempt in store.attempts(pipeline_id=pid):
    print(f"{attempt.task_name} #{attempt.attempt_no} {attempt.outcome} "
          f"{attempt.duration_ms}ms class={attempt.error_class} {attempt.decision}")

# The full story of one pipeline.
print([event.kind for event in store.events(pipeline_id=pid)])
```

带类型化字段的记录类型：`PipelineRecord`、`AttemptRecord`、`EventRecord` 和 `HandoffRecord`
已导出；`TaskRecord`、`RunRecord` 和 `Store` 协议位于 `pyattacker.store.base`。`SqliteStore` 和
`MemoryStore` 是这些实现，而 `open_store` 是你获取实例的方式。

`journal="summary"` 保留摘要和元数据，但不保留载荷。它能节省空间，代价是失去任务级
恢复——没有可恢复的工件时，被恢复的流水线会从头开始，并记录
`pipeline.checkpoint_missing`。

存储的游标也会被修复，而不是被盲目信任。如果一条 `failed`/`interrupted` 流水线的
`n_tasks_done` 已经等于 `n_tasks_total`，它就属于被撕裂的收尾（存储故障，或在最终写入期间被杀掉）：
每个任务都已写入检查点，因此下一次运行会像普通恢复那样校验最后一个工件——
存在、载荷保留、可解码——然后将其标记为最终工件，并写入终止行的最终状态（状态、
游标、所属运行，以及被清空的失败字段；在存储提供可选
`settle_pipeline` 能力时一次写入完成），记录 `pipeline.terminal_repaired`，其中带有该行原本
携带的状态/错误/运行，然后以 `succeeded` 结束，不重新运行任何内容。有两种情况**不会**修复，而且它们
并不相同：*越过末尾*的游标不是 Runner 能创建的状态，因此会被报告为
`CorruptCheckpoint`（`pipeline.corrupt_cursor`），绝不会被提升为成功，存储的值会原样保留
作为证据；而缺失、无载荷或无法解码的终止工件，则回退到
普通的从零重启规则（`pipeline.checkpoint_missing` / `pipeline.checkpoint_unusable`）。一次修复
如果在其收尾的**任一步骤**失败——最终标记或终止行的最终状态写入——该行就保持
不变，包括原始失败和所属运行，并记录带有阶段信息的
`pipeline.terminal_repair_failed`，因此后续尝试仍会报告原始原因。由于这样的行从不属于
执行修复的那次运行，该失败永远不会出现在按运行范围统计的 `stats` 中；它会计入
`RunReport.repair_failures`，正是这一点让 CLI 退出码为 `1`，而不是报告一次干净的运行。

**事后**执行的 `pyattacker report <store>` 仅根据流水线行重建其视图，因此它无法说明
后续某次运行曾尝试修复这样的流水线并失败了：该行仍然描述原始失败，
而修复尝试存在于事件流中（`pipeline.terminal_repair_failed`）。请把该命令的
输出视为流水线的状态，而不是每次尝试的历史；当场那次 `run` 的退出码才是
对它刚完成的那次尝试的权威信号。
启用控制的流水线会增加一个分支，该分支在那些规则**之前**被查询
（[交接](reference.md#进阶交接可选启用)）：如果最新的活动账本行的目标等于游标或位于其前方，则
运行会使用记录的入口工件*从该目标处*恢复，绝不重跑源任务；而一条持久的
`END` 行则根据它自己的入口工件写入最终状态。目标位于游标后方的行已经被
后续的正向进展所消耗，因此改为适用普通的 `artifact(cursor - 1)` 规则。如果
入口载荷已不存在（`journal=summary`、null 后端、已删除的 blob），流水线会从种子
重启并记录 `pipeline.checkpoint_missing`，完全就像丢失了一个线性检查点。

从 seq 0 重启（显式传入 `fresh_restart=True`、已在 `retry_succeeded=True` 下成功过的流水线，
或不可用的检查点）会在任务执行之前，持久地把
`PipelineRecord.handoff_floor` 推进到最新的账本 ID。等于或低于该
水位的行仍保留在仅追加的历史和导出中，但无法为新的执行驱动恢复。
对于启用控制的流水线，`reset_pipeline(record)` 在提交该游标/水位的同时，
会删除此前的当前任务行和链工件（`0 <= seq < n_tasks`）。种子和高位段
载荷工件，以及仅追加的 attempts/events/handoffs 都会保留；保留下来的工件，其最终标记会被
清除。因此 `tasks()` 和链工件导出描述的是当前这次执行，其中包括没有行的
被跳过站点。历史上被复用的入口地址可能引用已删除/已替换的链槽位；
账本保留的是来源信息，而不是那些槽位的不可变快照。

`fresh_restart=True` 是唯一会故意丢弃检查点的开关，对反向和正向
流水线一视同仁：流水线会从其绑定的种子重新运行，本次打开会跳过 `resume`/修复规则，
而这次打开记录的是 `pipeline.restarted`（连同它丢弃的游标），而不是
`pipeline.checkpoint_missing`，因为没有丢失任何东西。所有仅追加的内容都会留存——attempts、events、
handoffs，以及对反向流水线而言的访问计数器，因此历史发生实例仍然可寻址。
因此，框架不再能使用的检查点绝不是死路，已耗尽的
反向遍历预算也不是；见[恢复与所有权](#恢复与所有权)。相比之下，
`retry_succeeded=True` 只是放宽了*哪些*流水线有资格再次运行——它绝不会丢弃
尚未成功的流水线的状态。

水位在后续的恢复和进程重启后依然保留；按当前 `run_id` 过滤会
在第二次被中断的恢复之后错误地丢弃一个有效交接。提供交接的自定义存储
必须在流水线的读取和写入中持久化该字段，并提供原子重置能力。以可写方式打开 SQLite 时会用
默认值 0 迁移较旧的数据库；只读工具把缺失的列视为 0，而不做迁移。

成功完成会为每条流水线选定恰好一个 `is_final` 工件，并清除较早执行的
旧最终标记。历史载荷和账本行仍可供检查。

因此 `mark_final` 在契约上是幂等的，而当工件已经是
最终工件时，它会被直接跳过。

没有 `settle_pipeline` 能力的存储用两次写入完成同样的修复，而这两个步骤
被有意区别对待：失败的**终止状态转换**属于修复失败（可重试，如
上文所述），而失败的**元数据清理**则不是——该行已经持久地处于 `succeeded`，因此流水线
已被修复，只是它的失败文本陈旧。第二种情况会发出 `pipeline.terminal_cleanup_failed`，
且不计为失败，因为已成功的行会被永远跳过，而清理永远
无法重试。已成功行上的陈旧失败元数据是这类存储有文档说明的降级保证；
内置后端一次写入就完成该行的最终状态，永远不会遇到这种情况。

### 分页读取与第三方存储

上面的列表方法是**必需**接口，它们可以物化自己的结果——
这正是让 `merge_reports` 和一份小型报告容易编写的原因。必须保持内存有界的
整类读取（例如大型存储的导出）则改为走 `iter_*` 辅助函数：

| 辅助函数 | 按此顺序产出 |
|---|---|
| `iter_pipelines(store, *, run_id=None, state=None)` | `PipelineRecord`，先按 `created_at` 再按 `pipeline_id` |
| `iter_tasks(store, pipeline_id=None, *, run_id=None)` | `TaskRecord`，按 `pipeline_id`、`seq`，再按 `task_run_id` |
| `iter_attempts(store, *, run_id=None, pipeline_id=None)` | `AttemptRecord`，按 `attempt_id`（写入顺序） |
| `iter_events(store, *, pipeline_id=None, run_id=None)` | `EventRecord`，按 `event_id`（写入顺序，最旧在前） |
| `iter_artifacts(store, *, pipeline_id)` | 某条流水线的 `Artifact`，先按 `seq` 再按 `artifact_id` |

上述每一种顺序都**以唯一键结尾**，这不是装饰性细节：`tasks` 以
`task_run_id` 为键，`artifacts` 以 `artifact_id` 为键，因此 `(pipeline_id, seq)` 和 `seq` 在契约上
并不唯一。分页游标是严格的 `>` 比较，因此只按非唯一前缀分页会悄悄丢掉
与某页最后一行并列的每一行。`pipeline_id`（主键）、`event_id` 和
`attempt_id`（单调计数器）本身就唯一。

```python
from pyattacker.store import iter_events

for event in iter_events(store, run_id=run_id):   # one batch in memory, not the whole log
    ...
```

`PagedStore` 是让那些读取分批进行的**可选**扩展：存储要实现
上面这些签名的 `iter_pipelines` / `iter_tasks` / `iter_attempts` / `iter_events` / `iter_artifacts`，
每次查询至多读取 `ITER_BATCH_SIZE`（1000）行，并按文档所述顺序产出。
`SqliteStore` 用键集分页实现全部五个方法（`WHERE <key> > <last row of the batch>
ORDER BY <key> LIMIT 1000`），因此没有查询会返回超过一批的数据，也没有读取游标会在
处理某一行时保持打开。`MemoryStore` 遍历其活动容器；对它来说，内存有界是固有的。

**读取仍在写入中的存储。** 每一页都是它自己的语句——不存在
长期存在的读事务，也没有整个存储的时间点快照。迭代器
所保证的内容取决于它的键是否单调：

* `events` 和 `attempts` **以高水位为界**（`MAX(event_id)` / `MAX(attempt_id)`
  取自匹配的行，于读取第一页时确定）。此后追加的行不属于
  该次遍历，因此长时间的导出不会追赶移动的尾部；新的迭代器能看到它们。这两类
  都是仅追加的，因此该水位是键区间的真实快照。
* `pipelines`、`tasks` 和 `artifacts` 是**尽力而为的遍历**：不存在可用于定界的单调键
  （`created_at` 由调用方提供，`task_run_id`/`artifact_id` 不按时间排序），因此
  在游标前方插入的行可以出现在导出中，而在游标后方插入的行则不能。对活动存储的
  导出是“我遍历期间存在且可达的一切”，而不是
  快照；当你需要可复现性时，请重新导出已完成的存储。

对于不实现该扩展的存储，兼容性规则如下——第三方存储
插件层是公开 API，而现有插件是针对列表方法编写的：

* 每个 `iter_*` 辅助函数**在存在时**使用存储原生的分页方法，否则
  **委托给列表 API**（`iter_pipelines` → `pipelines()`、`iter_tasks` → `tasks()`、
  `iter_attempts` → `attempts()`、`iter_artifacts` → `artifacts()`，以及 `iter_events` → `events()`，
  并使用它能表达的最大 limit，因为该列表 API 自身的 `limit` 意思是“最近的
  N 条”，无法表达“全部”）；
* 该回退是正确的，但会物化整类数据，因此第三方存储能获得完整的导出，
  其内存特征与它的列表 API 相同。实现这五个方法才能升级它；
* `Store` 仍是 `open_store()` 唯一检查的协议，因此添加该扩展不会破坏任何东西。
  `WriteBehindStore` 实现了它，并像它的其他读取视图一样在每次分页读取前刷新。

### 可选的存储能力

**`commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)`**,
**`handoffs(*, pipeline_id=None, run_id=None, limit=None)`** 和 **`reset_pipeline(record)`** 是进阶
交接功能背后的可选能力，与 `resources()` 一样秉持“不属于协议”的精神。该提交是**一次原子
写入**：把源任务终结为 `handed_off`，插入被交接的尝试，持久化载荷
并为其分配链之上的地址，追加账本行并移动游标——对于 `END`，还要把
入口工件标记为最终工件，并把流水线的最终状态写入为 `succeeded`。原子性是这项能力的要求，
而不是额外的好处：这里刻意不设第二套恢复协议，因此无法把该操作作为单一单元完成的存储，
不得暴露该方法；在这样的存储上打开声明了 `control` 的流水线会快速失败，抛出
`ConfigError` 并指出缺少的 commit/reset 能力，而不是写入一次非持久的跳转。`handoffs()` 读取账本时
最旧在前，而 `limit` 保留最新的 N 条（仍最旧在前），与 `events`/`attempts` 一样。`supports_handoff(store)`
是探测函数：它会解开 `WriteBehindStore`，后者在转发该能力时会先刷新缓冲的
attempts/events，并通过该提交写入被交接的尝试，而不是通过自己的缓冲区。
失败时必须在任何后续的事件或清理写入之前回滚数据库写入。外部 blob 后端
在失败后可能保留一个未被引用的 blob；但它不得让部分提交的检查点变得可见。
支持交接的存储还必须在写入和读取中保留 `PipelineRecord.handoff_floor`（见
上文的重启规则）。
两个内置后端都实现了它；不实现它的第三方存储就是无法运行启用控制的
流水线，而它上面的普通流水线不受影响。

**`reset_pipeline(record)`** 必须原子地删除此前的当前任务行和链工件槽位，
清除其余工件的最终标记，并持久化重置后的流水线行，其中包括游标和水位。
保留种子、高位段载荷和仅追加的历史。它只在启用控制的种子启动时运行；
在某个目标处恢复会保持当前状态不变。`WriteBehindStore` 在重置前会先刷新。缺少
该方法的存储不能宣称支持交接；普通的无控制流水线仍然受支持。

**`settle_pipeline(pipeline_id, *, state, n_tasks_done, run_id)`** 是另一项可选能力，同样
秉持与 `resources()` 相同的“不属于协议”的精神：一次写入，把某行移到其终止状态、
重新绑定所属运行，并一并清除失败字段。当终止游标修复完成一条
流水线时（见上文 § 存储），`Runner._settle_succeeded` 会使用它，因为那里的写入若被撕裂，可能
在该行写入最终状态之前丢失原始失败。`SqliteStore` 和 `MemoryStore` 实现了它，而
`WriteBehindStore` 会将它透传，因为它是状态写入，而不是批处理的事实。没有它的存储仍然
可用：Runner 会回退到 `finish_pipeline`（原子地写入状态和游标），随后执行一次清理用的
`upsert_pipeline`，其最坏情况是已写入最终状态的行仍然带着旧的失败文本。

**`visit_state(pipeline_id)`**, **`reset_visits(record, seed, *, fresh_budget=False)`**,
**`commit_entry(task)`**, **`commit_visit_attempt(pipeline, task)`**,
**`commit_visit_success(pipeline, task, attempt, artifact, *, final)`**,
**`commit_control_transition(record, *, pipeline, task, attempt, payload, entry_id, target_task, limit)`**,
**`repair_visit_terminal(record)`** 和 **`get_artifact_by_id(artifact_id)`** 是可选能力，
位于[反向遍历](#进阶反向遍历rewindretry-allvisits)背后，同样在协议之外。`store/visits.py` 的 `VisitStore` 持有
共享的状态转换语义，两个内置后端都从它派生，因此后端只需提供一个原子
写入边界，加上自己的底层行写入。每个操作都是一次提交：入口分配会推进
按 seq 的访问计数器并记录待定输入，普通成功会一并写入输出的发生实例和
有效槽位，而控制转换还会使活动后缀失效、消耗一个
预算单位，并分配目标入口。一次控制转移会把它的源访问与尝试、入口发生实例、账本行、控制计数以及分配好的目标入口
一起提交；启用反向的流水线里一次正向转移同样消耗预算，但不向后移动游标。SQLite 用
`BEGIN IMMEDIATE` 串行化这些能力事务，`MemoryStore` 在一次能力写入失败时恢复自己的状态：写入失败的
blob 无法发布它的引用，而回滚的数据库事务可能留下一个无人引用的 blob。`supports_visits(store)` 是探测函数；它会解开
`WriteBehindStore`（后者在同步委托这些操作之前会先刷新），并要求具备访问
方法、`feature_level()` **加上** `commit_handoff`、`reset_pipeline` 和 `handoffs`，因为一次反向
转换会通过这同一次提交写入其账本行和源任务。`feature_level()` 是必需的，
而不是可选的：下面的兼容性规则是这项能力的一部分，而不是额外要求。未通过
探测的存储会在打开启用反向的流水线时被拒绝并抛出 `ConfigError`，绝不会被降级为
非持久的循环。

### 存储兼容性与备份

SQLite 对旧存储的升级是追加式的：访问列默认为 0，出现一张遍历状态表，并记录一个 `store_meta` 特性
级别。纯正向的工作永远不会离开 `base`。级别会在为某站点分配第一个第二次发生实例的*同一个事务*里变成
`visits-v1`，并且永远不会回退——审计行不会被删除，它们存在过这个事实也一样。

那也正是谱系不感知的写入者开始无法解读该存储的时刻，因此处于 `visits-v1` 的 SQLite 存储会武装一个
**写入者守卫**：任何尚未声明具备访问谱系感知的连接对 `pipelines`、`tasks` 和 `artifacts` 执行
`INSERT`/`UPDATE`/`DELETE` 都会响亮地失败（`no such function:
pyattacker_store_requires_visits_aware_writer`）。原始读取和旧式读取不被阻止——守卫保护的是状态，
不是访问——但由不理解重访谱系的构建去解读它是不受支持的：这样的读取者无法判定哪个发生实例有效，
因此它的输出描述的是这些行，而不是这次执行。守卫保证的是破坏性的那一半：在此特性之前发布的写入者
无法悄悄改写错误的发生实例，因为它会在第一次写入时就失败。打开自己不认识级别的构建会直接拒绝该存储
（`StoreFeatureUnsupported`，只读也一样），而不是报告一套它看不见的谱系。

实际后果：

* **备份。** 对运行中的数据库请使用 SQLite 自带的备份 API（`sqlite3 <store> ".backup <copy>"`，或
  `Connection.backup()`），它在写入者正在运行时是安全的；把活动数据库文件连同它的 `-wal`/`-shm`
  边文件一起复制，只有在没有写入者时才可靠。SQL 转储同样可以正常恢复——`sqlite3 <store> .dump |
  sqlite3 <copy>` 会先写表数据、后创建守卫触发器，因此不具备感知的连接也能重放它，而副本会连同守卫
  和特性级别一起继承过去。
* 从 `sqlite3` 命令行写入受守卫保护的存储，需要在该连接上注册守卫函数（或者删掉触发器）；两者都在
  受支持的接口之外。
* 唯一受支持的回到 `base` 的方式，是由理解该级别的构建执行迁移。
* 多个 runner 同时执行同一条逻辑流水线不是受支持的调度模式；彼此独立的分片行仍然独立（没有 `resume`
  时 `running` 行仍然绝不被接管，见[恢复与所有权](#恢复与所有权)）。

---

## 工件后端

后端决定载荷的*字节*存放在哪里。无论采用哪种后端，存储都会保留摘要、大小和编解码器，因此
工件对其上层的所有组件来说仍是同一个对象。

| 后端 | 行为 |
|---|---|
| `InlineBackend()` **（默认）** | 载荷存放在存储中 |
| `FileBackend(root=..., min_bytes=262144)` | 达到或超过 `min_bytes` 的内容写入按内容寻址的文件，读取时再水合（hydrate） |
| `NullBackend()` | 保留摘要，丢弃字节 |

```python
from pyattacker import FileBackend, Runner

with Runner(store="runs/qa.db", artifact_backend=FileBackend(root="/data/blobs")) as runner:
    ...

# Equivalent, and what the CLI and YAML accept:
Runner(store="runs/qa.db", artifact_backend="file:///data/blobs")
Runner(store="runs/qa.db", artifact_backend={"kind": "file", "root": "/data/blobs",
                                            "min_bytes": 262144})
```

文件按内容寻址并原子写入，因此相同的载荷会合并为一个文件，共享的根目录可以安全地服务多次运行。
通过已溢写的检查点恢复是透明的。

`NullBackend` 会让你损失恢复粒度，和 `journal="summary"` 完全一样：没有载荷就意味着没有
可用于恢复的检查点。blob 文件缺失时的行为也是刻意如此——`available` 变为 false，工作会被重做，
而不是被静默跳过。

`resolve_backend(spec)` 把字符串或 dict 转成后端。`FileBackend.stats()` 报告它已写入
的内容。如果你自己的后端需要独立的 URI scheme，可通过 `pyattacker.stores` 分发它。

---

## 分片与合并

一个存储只接受一个写入者，因此横向扩展意味着多个进程各自持有存储，事后再合并。
分片分配是内容寻址的流水线键的纯函数，所以同一数据集总是以相同方式切分，恢复的流水线也会回到
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

分配方式是 `blake2b(key) % count`，而不是 Python 中加盐的 `hash()`，因此它在不同进程和机器之间
都是稳定的。均衡是统计意义上的：12 条流水线分到 3 个分片得到 6/2/4 很正常，规模变大后会趋于
均匀。实践中你通常让 CLI 负责派生进程——`--shards 4 --jobs 4`——只有在自己驱动集群时才
使用这些函数。

### `merge_reports`

```python
merge_reports(paths) -> MergedReport
```

按 `pipeline_id` 去重（最优状态胜出，最晚完成者打破平局），然后根据合并后的行**重新计算**
统计信息。合并是幂等的，因此同一个存储被计数两次不会虚增任何数字。

| `MergedReport` 成员 | 含义 |
|---|---|
| `rows` | 合并后的流水线行 |
| `duplicates` | 被折叠掉的行数 |
| `sources` | 有哪些存储参与 |
| `stats()` | 重新计算的统计信息 |
| `summary()` | 人类可读的摘要 |
| `errors(limit=20)` | 所有分片中的失败 |
| `export(path, *, fmt="jsonl", kind="pipelines")` | 写出合并后的视图 |

---

## 恢复同一性

任务指纹记录其名称/目标、声明的资源和超时、每个重试字段
（包括异常的模块名/限定名）、任务算法配置、`config`、`version`
以及工厂 `parameters`。`fanout` 还会记录其有序的子指纹和 `on_error`。
内置工厂会自动记录其行为参数。用户的 `config` 与工厂参数彼此独立，因此声明式覆盖
无法抹掉内置任务的行为同一性。

`config` 和 `parameters` 只接受 null、bool、int、有限 float、字符串、列表，以及键为字符串的
对象；它们会在构建规格时被快照。元组、非字符串的对象键、JSON 原始类型的 Python 子类以及
循环引用都会被拒绝，而不是被强制转换。任意的闭包、客户端、全局变量、导入的辅助函数、端点选项，
以及资源池默认算法都**不会**被检查。来自这些来源的行为必须显式声明：

```python
from pyattacker import task

@task("ask", config={"model": "model-a", "temperature": 0.2}, version="prompt-v2")
async def ask(row, ctx):
    ...  # use the same declared model, temperature and prompt revision in your client call
```

内置任务算法在构造 TaskSpec 时捕获其规范化后的配置。
Runner 从同一份快照构建全新的运行时实例，因此之后对传入的算法实例或映射所做的修改
不会改变已记录的行为。嵌套 fallback 会被快照，
对于 Sticky、LeastBusy、Failover 和 QuotaAware，隐式的 Wait fallback 会规范化为显式的 `Wait()`。
Failover 的元组/列表资源池名称序列会刻意规范化为 JSON 列表。

自定义任务算法必须提供返回同一严格 JSON 取值域的 `fingerprint()`，否则
任务必须提供 `version=`，并在算法行为变化时递增它。钩子结果会随规格一起
捕获，并在任务执行前和每次获取前检查，包括自定义
fallback；一旦漂移，就会在该算法运行前失败。自定义钩子必须描述配置，
而不是不断变化的计数器，并且其配置在一次获取期间必须保持稳定。
仅依赖 version 的自定义算法必须支持独立的 `deepcopy`；定义在构造时复制，
并为每个任务生成彼此分离的运行时副本。内置的字符串、
config 映射和等价实例会规范化为同一指纹。机密信息和运行时客户端绝不应放入同一性配置。

`pipeline(..., include_code=False)` 会递归移除源码摘要，包括 fanout 的子任务。
它保留工厂参数、config、version 和各项策略。源码检查失败时会改用
模块名/限定名，因此动态定义的函数需要显式的 version 才能区分同名的
实现。对导入的辅助函数的修改同样需要更新 config/version。

默认 ID 对规格摘要、种子摘要和重复索引做哈希。定义/输入未变时，
ID 和分片保持不变；行为变化会产生新 ID，并可能迁移到另一个分片。
显式的 `bind(key=...)`、`map(key_of=...)` 和声明式的 `source.key_field` 会保留传入的
ID，但 Runner 会在跳过或恢复**之前**检查已存储的规格和种子摘要。
不匹配会抛出 `PipelineIdentityConflict`（`ConfigError`，CLI 退出码 2），中断新的运行，
并让冲突流水线已存储的定义、结果和检查点保持原样。
已经打开的进行中流水线会立即以 interrupted 状态收尾，其持久化的
检查点游标得以保留；监控无需等到之后恢复来修复它们的状态。
改动过的工作请使用新的 key 或存储。`retry_succeeded=True` 不能作为冲突的覆盖开关。

### 升级现有存储

新的规格摘要带 `v2:` 前缀；所有默认流水线 ID 都会从旧的指纹
格式改变。旧记录仍可读取/导出，但不会被自动迁移或复用：
旧指纹缺少验证等价性所需的信息。打开一个存储，如果它最旧的流水线
带有旧版摘要，就会在任务开始前发出警告和 `run.legacy_identity` 事件。
默认 ID 会重跑工作；显式的旧 key 会冲突。请用旧版本的包完成耗时的旧运行，
然后为 v2 启动新存储。不要通过重写旧版摘要来绕过验证。
对于分片运行，这次同一性变更也会改变分片分配；完成旧运行时，
请把旧的分片存储与旧包放在一起使用。

### 外部副作用

恢复会跳过检查点游标和工件均已持久化的任务。它不保证
请求或文件写入恰好执行一次：提供方可能已经完成请求，而进程随后崩溃，
或者在写入检查点时失败，于是该任务可能再次运行。journal 使用 summary、
载荷为 null/缺失以及检查点不可用时，也可能需要重放更早的任务。

如果你的提供方支持幂等键，请从稳定的流水线同一性和任务同一性派生一个键，
例如 `f"{ctx.pipeline_id}:{ctx.seq}"`，并在多次重试之间复用它，而不是把
尝试序号也包含进去。在启用反向的流水线中，当每次重新生成都应是新操作时，就要把 `ctx.visit`
包含进去，例如 `f"{ctx.pipeline_id}:{ctx.seq}#{ctx.visit}"`。交接载荷*不是*
任务同一性，因此绝不要
用工件地址作为外部工作的键。请遵守提供方的保留窗口和 API 契约。对于文件汇聚器，
按同一性执行 upsert，或者为每条流水线/每个任务写入一个原子替换的文件；单纯的追加式
`write_jsonl` 任务在重放时可能产生重复行。资源租约安全并不能让这些
外部副作用变成幂等的。

---

## 导出

五种行形态，三种格式。行以有界批次从存储中流式读出；合并报告
（把 N 个存储汇成一个连贯答案）是唯一必须把行留在内存中的地方。

| 名称 | 值 |
|---|---|
| `ROW_KINDS` | `("pipelines", "tasks", "attempts", "events", "artifacts")` |
| `FORMATS` | `("jsonl", "json", "csv")` |

| 函数 | 用途 |
|---|---|
| `iter_rows(store, *, kind="pipelines", run_id=None, limit=None)` | 以 dict 形式流式输出行 |
| `export_store(store, path, *, kind="pipelines", fmt="jsonl", run_id=None, limit=None)` | 写入单个存储，返回行数 |
| `export_stores(paths, path, *, kind=..., fmt=..., limit=...)` | 把多个分片存储写成一个拼接文件 |

每种 kind 一行，顺序即 `limit` 截断所依据的顺序：

| `kind` | 每行对应 | 顺序 |
|---|---|---|
| `pipelines`（默认） | 一条流水线，嵌套——包含任务、工件和交接 | `created_at`，然后是 `pipeline_id` |
| `tasks` | 一个任务：最终状态、耗时、错误、使用的租约 | `pipeline_id`、`seq`，然后是 `task_run_id` |
| `attempts` | 一次尝试，包含每次重试的 `decision` | `attempt_id`（写入顺序） |
| `events` | 一条结构化事件 | `event_id`（写入顺序，最旧的在前） |
| `artifacts` | 一个工件，包含中间工件 | 流水线顺序，然后是 `seq`，再是 `artifact_id` |

`limit` 对所有 kind 含义相同——它统计的是该 kind 的行数，`artifacts` 也是如此，
而在过去这里统计的是流水线：

* `None`（默认值）：**完整**历史，不做任何截断；
* `0`：没有行；
* `N > 0`：按上述顺序的前 N 行；
* 负值：`ConfigError`。

（`export_stores` 对每个存储分别应用 limit，因此每个存储最多贡献它的前 N 行。）

每种排序都以一个唯一键结尾，因此跨越批次边界的读取既不会丢行，也不会
重复行——即使许多流水线共享同一个 `created_at`，或者若干任务或
工件共享同一个 `(pipeline_id, seq)` / `seq`，而 schema 并不禁止这种情况。

```python
from pyattacker import export_store, iter_rows

# Compute your own metric — the framework stores facts and leaves semantics to you.
correct = sum(1 for row in iter_rows(store, kind="pipelines")
              if row["state"] == "succeeded" and row["artifacts"][-1]["payload"]["correct"])
print(f"accuracy: {correct / total:.1%}")

# Retry analysis in a spreadsheet.
export_store(store, "attempts.csv", kind="attempts", fmt="csv")
```

`pipelines` 行是嵌套的——包含任务、工件和交接——这也是它成为默认值的原因。
`handoffs` 列表包含 `handoff_id` 和提交它的 `run_id`；流水线行包含
`handoff_floor`。高于该水位的 ID 属于活动执行历史；等于或低于它的 ID 是
历史记录，被排除在恢复之外。`store.handoffs(pipeline_id=...)` 包含所有记录。
`stats(run_id)["handoffs_total"]` 只统计该次运行提交的交接，因此一次恢复可以使用上一次运行留下的活动交接，
同时报告零个新交接。对于普通流水线，
`handoffs` 列表是 `[]`；否则每记录一次跳转就有一个对象（见
[handoffs](reference.md#进阶交接可选启用)）；它是嵌套结构，不是独立的行 kind，所以 `ROW_KINDS`
保持不变。CSV 从最初几行取表头，
并把之后出现的键折叠进 `extra` 列，因此内存保持平稳，不会有字段
被静默丢弃。

**导出一个活动中的存储。** 导出不是事务：它一次读一页，因此它能保证什么取决于 kind（细节见
[stores](reference.md#分页读取与第三方存储)）。
`events` 和 `attempts` 受导出开始时取得的单调键高水位限制——之后写入的行不会被包含，
而重新导出就能看到它们。
`pipelines`、`tasks` 和 `artifacts` 是尽力而为的遍历：先于游标写入的行可能出现，
晚于游标写入的则不会。需要可复现的文件时，请导出已完成的存储。

**流式在哪些地方成立，哪些地方不成立。** `jsonl` 和 `json` 逐行写出，`csv` 只缓冲
生成表头所需的 `header_rows` 前缀。在存储一侧，`tasks`/`attempts`/`events` 通过分页辅助函数
以 `ITER_BATCH_SIZE`（1000）行为一批读取，而 `artifacts` 先给流水线分页，
再流式读取每一条流水线的工件，因此内存单位是**一条流水线**，而不是整个存储。一条
`pipelines` 行本身是嵌套的，所以导出该 kind 时会一次物化一条流水线的任务和工件。
`merge_reports` 是刻意的例外：按 `pipeline_id` 去重需要每条流水线的胜出行，
因此合并后的行会留在内存中（它用聚合查询统计事件/尝试，
而不是读取日志）。

---

## 声明式配置

`load_spec` 把一个 YAML/TOML/JSON 文件读成资源池、一个流水线模板和一个种子来源，返回
`DeclarativeSpec`（定义在 `pyattacker.declarative` 中）。该文件描述**组合与资源**；
你的逻辑仍留在 Python 中，由 `use:` 指向。

解析器根据文件后缀选择，只有 YAML 解析器是可选的：`.json` 和 `.toml` 用标准库
读取，而 `.yaml`/`.yml` 需要 `yaml` 额外依赖（extra，`pip install "pyattacker[yaml]"`）。
没有它时，`load_spec` 会抛出一个同时指出文件和该额外依赖的 `ConfigError`——这项检查发生在
读取该文件时，因此只接触 JSON/TOML 的进程永远不需要安装 PyYAML。

```python
from pyattacker import Runner, load_spec

spec = load_spec("qa.yaml")                 # raises ConfigError on a bad file
print(spec.describe())                       # what `pyattacker validate` prints

with Runner(pools=spec.pools, **spec.run) as runner:
    report = runner.run(spec.pipelines(limit=100))
```

`load_spec` 是共享的校验入口：`run`、`validate` 以及每个 `--shards` 子进程都会走它，
被它拒绝的配置会在存储出现之前以退出码 `2` 结束。它会检查未知字段、字段类型、数值
范围、资源池引用（包括 `use:` 工厂自己声明的 `resource`）、算法名称和
参数，以及 `source:` 声明——并且总会指出字段路径。它只做声明检查：
不会打开任何数据集，也不会构造工件后端。[`docs/cli.md`](cli.md#validate--不运行就检查配置)
列出了覆盖的内容。

配置未提及的字段会保留 `use:` 目标所声明的值；显式写 `field: null` 会清除
`resource`、`algorithm`、`timeout_s` 或 `version`。这条规则及其 SDK 写法
（[`TaskSpec.with_overrides`](reference.md#taskspec)）是同一个。

| `DeclarativeSpec` 成员 | 含义 |
|---|---|
| `template`、`pools`、`run`、`source` | 解析出的各个小节 |
| `unresolved_env` | 未能解析出任何值的 `${VAR}` 引用 |
| `seeds()` | 种子可迭代对象 |
| `pipelines(*, limit=None)` | `PipelineSpec` 流 |
| `describe()` | 可直接转成 JSON 的摘要 |

`load_spec(path, strict_env=True)` 在 `${VAR}` 未设置时抛异常而不是给出警告——值得在 CI 中使用，
因为在 CI 里，静默为空的 API key 比任务失败更糟。

有两个细节值得了解：

* `use:` 按形态解析。带冒号的名称（`my_pkg.tasks:ask`）直接导入；裸名称
  （`echo`）先在内置任务中查找，然后才查插件——因此插件永远不会遮蔽内置任务。返回
  `TaskSpec` 的可调用对象会被当作工厂，并以 `args`/`kwargs` 调用。
* **YAML 1.1 会把裸的 `on:` 键解析为布尔值 `true`。** 在 retry 块中请写 `"on": [RetryableError]`。
  加载器会检测出这个错误并提示你。

完整的文件格式见 [`docs/cli.md`](cli.md#配置文件参考)。

---

## 插件

插件就是普通的 `importlib.metadata` 入口点。安装一个包，它的名称就能在任何配置中使用。

| 组 | 提供的内容 |
|---|---|
| `pyattacker.tasks` | 一个 `TaskSpec`，或返回它的工厂 → `use: my_task` |
| `pyattacker.algorithms` | 一个获取算法 → `algorithm: my_algo` |
| `pyattacker.codecs` | 一个编解码器，在创建第一个 `Runner` 时装上 |
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

内置项优先解析，导入时抛异常的插件会被记录而不是向外传播——因此坏掉的插件
既不会拖垮一次运行，也不会静默失败。`pyattacker plugins` 会打印同样的
信息。`PluginRegistry` 是类型；`PLUGINS` 是进程级实例。一个完整可用的示例包
见 [`examples/plugin_package/`](../../examples/plugin_package/README.zh-CN.md)。

---

## 内置任务

其中三个用于生产环境（`fanout`、`shell_run`、`write_jsonl`）；其余均为模拟工作，让你可以在没有网络的情况下
演练整套机制。在配置中它们带有 `mock.*` 名称：`use: pyattacker.tasks:flaky`。

### `fanout`

```python
fanout(*specs, name=None, on_error="raise", retry=None) -> TaskSpec
```

在一个任务内部，对 **同一** 输入并发运行多个任务，并返回 `{task_name: value}`。这就是
在不把流水线变成 DAG 的前提下，表达真正分支步骤的方式。

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

三个后果，都是有意为之：

* **重试粒度是分组级的。** 一个分支失败会重试整个 fan-out；子任务各自的策略
  不会逐分支生效。分组会采用最宽容的子任务策略，除非你传入
  `retry=`。
* **分支共享父任务的上下文**，因此它们的租约和事件都记录在 fan-out 任务之下——
  每个步骤一条完整记录。
* **Runner 只看到分组规格（spec）**，因此只有当每个子任务都一致时，`resource`、`algorithm` 和
  `timeout_s` 才会从子任务中提取出来。这就是为什么上面的三个 judge 必须声明相同的资源池和
  算法。

权衡：一个检查点里有三个请求，意味着一旦失败，恢复时会重新发送全部三个请求。如果某个请求
开销很大，就把它单独作为一个任务——`examples/llm_eval/` 对两种形态都做了测量。

### `shell_run`

```python
shell_run(command, *, timeout_s=60.0, check=True, name=None) -> TaskSpec
```

运行子进程并返回其 stdout/stderr。

```python
# The argv form: no shell, and "{value}" arrives as one literal argument.
shell_run(["python", "postprocess.py", "--input", "{value}"])
```

**当命令需要工件时，使用 argv 形式。** 它通过 `create_subprocess_exec` 运行，因此替换后的
值无论包含什么字符，都会作为一个字面量参数传给子进程。字符串形式会直接拒绝 `{value}`，
而不是把它插入 shell 命令行。替换只是普通的子串替换，不是 `str.format()`，因此其他花括号
（比如 `jq` 过滤器、字典字面量）会原样保留。

保证是“没有*隐式* shell”，而不是“对任何程序都安全”：如果你的 argv 本身调用了
解释器（`["sh", "-c", ...]`），那么该解释器如何处理输入，就由你自己来判断。

**进程生命周期。** 任务拥有它启动的进程，每条退出路径都执行同一套清理——
包括进程创建在内。OS 子进程在 `create_subprocess_exec` /
`create_subprocess_shell` 返回句柄之前就已经存在，因此落在这个窗口中的取消
不允许把它弃之不顾：任务会继续等待该句柄，处置掉子进程，然后才让取消继续。
正常退出不受干预（其结果照常返回，`check=False` 仍会报告非零的 `returncode`，
而不是抛出异常）。如果 `timeout_s` 到期、协程被取消（`Runner` 停止、分组任务超时、
外层 `asyncio` 取消）或有其他异常逃逸，仍在运行的子进程会被 `SIGKILL` 杀死，
然后被 **回收**，之后该异常才继续传回调用方：取消仍以 `CancelledError` 的形式到达，
超时仍以 `TimeoutError` 的形式到达，但它们身后不会留下任何正在运行的进程。
没有优雅的 `SIGTERM` 窗口——清理不会等待子进程结束。等待 OS 报告退出是有上限的
（5 秒），这只对 OS 永远不会报告已退出的进程才有意义；而清理自身遇到的任何问题
（因 `communicate()` 被取消而处于坏状态的读取器、一次失败的信号）都不允许替换调用方的
异常：清理是尽力而为，
调用方的错误类型则不是。

**后代进程。** 在 POSIX 上，每个子进程都在自己的会话中启动（`start_new_session=True`），因此
清理会向整个进程组发送信号，而不是只针对一个 PID。对于字符串命令，这覆盖流水线或
子 shell 的每个部分；对于 argv 形式，它覆盖该程序以及该程序派生的任何进程。
没有“只杀死直接子进程”的模式，也没有进程组之外的 cgroup/pidfd 机制。
在 Windows 上，标准库不支持向进程组发送信号（`os.killpg` 不存在，且 `asyncio` 无法
向子进程的进程组发送 `CTRL_BREAK_EVENT`），因此在那里只会终止直接子进程，
shell 命令的后代进程可能在任务结束后继续存活——这是有文档记录的局限，
且仅在 POSIX 上验证过。由于在 POSIX 上子进程自成会话首进程，它不会收到
Ctrl-C 等由终端产生的信号；停止它的是任务自身的取消。清理只在子进程
本身尚未被回收时起作用：已经正常退出的命令不会被追查，因此它刻意留下的
进程（shell 的 `&`、守护进程）不会被杀死——而且子进程一旦被回收，
其 PID（同时也是进程组 id）可能已被复用，
所以向该进程组发送信号本来也不安全。

### `write_jsonl`

```python
write_jsonl(path, *, mode="a", name=None) -> TaskSpec
```

作为最后一步，把每个工件追加到 JSONL 文件中。这是若干导出机制中的一种——
`export_store` 和 `report.export_jsonl` 通常更合适。

### 模拟任务

| 任务 | 签名 | 行为 |
|---|---|---|
| `echo` | `echo`（是规格，不是工厂函数） | 原样返回该值 |
| `flaky` | `flaky(fail_times=2, *, error="retryable", message=..., retry=None)` | 失败 N 次后成功——用于演练重试 |
| `delay` | `delay(seconds=1.0)` | 等待，用于观察并发 |
| `boom` | `boom(message="boom", *, error="retryable")` | 总是失败；`error` 可以是 `retryable`、`fatal` 或 `invalid` |
| `leaky` | `leaky(*, pool=None)` | 故意泄漏租约，用来演示强制回收 |
| `simulate_llm` | `simulate_llm(*, latency_ms=5.0, fail_rate=0.0, error="rate_limit", tokens=32, resource=None, **selector)` | 获取 → 等待 → 上报 → 返回：演示资源池和退避的主力 |

```python
from pyattacker import Runner, pipeline, simulate_llm

# A pool exercised at a 30% failure rate, with no network anywhere.
template = pipeline("smoke", simulate_llm(resource="apis", fail_rate=0.3, latency_ms=20))
```

把 `ctx.clock.sleep` 换成一次 HTTP 调用，`simulate_llm` 就成了一个真实任务——它就是按这种形态
写成的完整示例。

### 种子辅助函数

| 函数 | 用途 |
|---|---|
| `jsonl_source(path, *, limit=None)` | 把 JSONL 文件作为种子流式读取，每行对应一个流水线 |
| `seed_factory(kind="range", *, n=10, path=None, limit=None)` | 配置的 `source:` 块最终解析成的实现：`range` 或 `jsonl` |

```python
from pyattacker import jsonl_source

runner.run(template.map(jsonl_source("dataset.jsonl", limit=500)))
```

---

## 监控

### `runner.stats()`

进程内的实时快照。可在运行中途安全调用。

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

无依赖的只读 HTTP 视图。它为每个请求打开一条全新的只读连接，因此可以与正在运行的
任务并排运行。

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

`/stats` 携带 `handoffs_total`（所选运行的提交数，没有运行过滤器时则是全部提交），每行 `/pipelines` 携带
`handoffs`（活动执行记录的计数）、`handoffs_historical`（全部记录的计数）和
`handoff_floor`，紧挨着 `n_tasks_done`/`n_tasks_total`——在启用控制流的流水线上，这两者是链中的
**位置**，而不是已运行任务的数量，因此非零的 `handoffs` 才说明“这条
流水线跳过了站点”。

**它没有身份验证，而且会提供你的工件载荷。** 正因如此，它只绑定到回环地址。在把它
暴露到其他任何地方之前，请先架设你自己的代理。`pyattacker serve` 就是同一功能的
命令行版本。

`pyattacker.monitor.render_snapshot(snapshot)` 和 `read_snapshot(store)` 是 `pyattacker watch` 背后的
终端渲染器，如果你想嵌入同样的视图。从子模块导入它们：
`from pyattacker.monitor import render_snapshot, read_snapshot`。

---

## 接下来读什么

| 文档 | 内容 |
|---|---|
| [`docs/tutorial.md`](tutorial.md) | 引导式路径：十四个可运行的步骤 |
| [`docs/cli.md`](cli.md) | 命令、标志、退出码、配置文件格式 |
| [`docs/design.md`](design.md) | 模型、不变量，以及这些 API 背后的权衡 |
| [`examples/`](../../examples) | 完整程序，包括对多种流水线形态的实测比较 |

