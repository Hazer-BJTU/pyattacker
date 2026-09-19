# 教程：从一个任务到可恢复的模型评测

[English](../tutorial.md) | **简体中文**

这是一份循序渐进的 pyattacker 入门指南——从五行程序，一路讲到可恢复、可分片的模型评测。

**你是谁：** 用 Python 跑 LLM 或 agent 评测，熟悉 `asyncio`，想要的是底层基建——端点资源池、并发控制、重试机制、"哪些行已经跑过了"，以及每一次请求的完整记录。

**怎么读：**

* 每一步都是一个完整程序。先跑起来，看输出，再读解释。
* 以 `# tutorial/step_NN_....py` 开头的代码块会被自动抽取成独立文件，在每次测试时由
  [`tests/test_tutorial.py`](../../tests/test_tutorial.py) 实际执行。把其中一个保存成注释里指定的文件名，直接运行即可。
* 所有示例都不访问网络。真实程序里该写 `await self.http.post(...)` 的地方，示例改用 `await asyncio.sleep(...)`——注释 `# <- your HTTP call` 标出了该位置。

**其他文档：** [`docs/reference.md`](reference.md) 是完整 API 参考；
[`docs/cli.md`](cli.md) 讲命令行用法；[`docs/design.md`](design.md) 解释框架为什么长这样；[`examples/`](../../examples) 有完整可运行的示例。

## 找到你需要的内容

第一次按顺序读。之后用这张表快速定位。

| 我想要…… | 步骤 | 参考 |
|---|---|---|
| 写第一个任务并跑起来 | [第 1 步](tutorial.md#第-1-步--一个种子一个任务一次运行) | [任务](reference.md#任务) |
| 跑整个数据集，或每行取 k 个样本 | [第 2 步](tutorial.md#第-2-步--一次处理多个输入) | [`map`](reference.md#map) |
| 把多个步骤串起来 | [第 3 步](tutorial.md#第-3-步--串联任务) | [流水线](reference.md#流水线) |
| 了解会保存什么、什么时候保存 | [第 4 步](tutorial.md#第-4-步--检查点就是工件) | [工件](reference.md#工件与编解码器) |
| 把负载分散到多个端点或 key | [第 5 步](tutorial.md#第-5-步--端点作为资源池) | [资源](reference.md#资源) |
| 所有资源都忙的时候怎么等 | [第 6 步](tutorial.md#第-6-步--如何选择等待方式) | [算法](reference.md#获取算法) |
| 安全地使用资源 | [第 7 步](tutorial.md#第-7-步--租约契约) | [`Lease`](reference.md#lease) |
| 重试失败，并搞清楚为什么放弃 | [第 8 步](tutorial.md#第-8-步--失败分类重试记录) | [`Retrying`](reference.md#retrying)、[错误](reference.md#错误) |
| 崩溃后接着跑，不重复花钱 | [第 9 步](tutorial.md#第-9-步--恢复什么会重跑什么不会) | [`Runner`](reference.md#runner) |
| 查询记录、导出结果 | [第 10 步](tutorial.md#第-10-步--读取记录) | [存储](reference.md#存储)、[导出](reference.md#导出) |
| 用 YAML 和 CLI 驱动 | [第 11 步](tutorial.md#第-11-步--声明式路径与-cli) | [`docs/cli.md`](cli.md) |
| 用多个进程跑 | [第 12 步](tutorial.md#第-12-步--分片与合并) | [分片](reference.md#分片与合并) |
| 在步骤内分支 | [第 13 步](tutorial.md#第-13-步--压轴一个小型模型评估) | [`fanout`](reference.md#fanout) |
| 存自定义类型或大载荷，发插件 | [第 14 步](tutorial.md#第-14-步--自定义类型blob插件) | [编解码器](reference.md#codecregistry)、[后端](reference.md#工件后端)、[插件](reference.md#插件) |
| 监控正在跑的运行 | [第 14 步](tutorial.md#第-14-步--自定义类型blob插件) | [监控](reference.md#监控) |
| 在任务内部跳过链中剩余部分（高级） | [第 15 步](tutorial.md#第-15-步--高级跳过站点交接) | [交接](reference.md#进阶交接可选启用) |
| 把工作送回更早的站点（高级） | [第 16 步](tutorial.md#第-16-步--高级用回退和全部重试重新生成) | [反向遍历](reference.md#进阶反向遍历rewindretry-allvisits) |
| 在载荷内部快照与恢复状态（高级） | [第 17 步](tutorial.md#第-17-步--高级让载荷自带历史) | [`HistoryArtifact`](reference.md#historyartifact) |

---

## 第 0 步 —— 安装与健全性检查

```bash
uv sync                    # or: python -m venv .venv && .venv/bin/pip install -e .
uv run pyattacker demo     # zero-config smoke test: 50 simulated pipelines, retries, a report
uv run pytest -q           # the whole suite, offline, a few seconds
```

`pyattacker demo` 是最快的方式——看一眼一次运行长什么样：

```text
run ... status=completed  wall=1.2s
  pipelines: total=50 succeeded=50
  ...
```

环境要求：Python 3.11+，没了——基础安装零依赖。第 11 步会读 YAML 配置，那是唯一需要额外安装的地方（`uv add "pyattacker[yaml]"`）；其他步骤什么都不用装。

---

## 第 1 步 —— 一个种子、一个任务、一次运行

```python
# tutorial/step_01_smallest.py
"""Step 1 - the smallest useful program: one seed, one task, one persisted result."""

from pyattacker import Runner, pipeline, task


@task("double")
def double(seed: dict) -> dict:
    """A task is a unary function: one artifact in, one artifact out."""
    return {"n": seed["n"], "doubled": seed["n"] * 2}


template = pipeline("doubling", double)

with Runner(store="runs/step01.db", concurrency=2, label="step-1") as runner:
    report = runner.run(template.map([{"n": 21}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    print(f"pipeline {row['pipeline_id'][:12]} state={row['state']} "
          f"tasks={row['n_tasks_done']}/{row['n_tasks_total']}")
    print("final artifact:", row["artifacts"][-1]["payload"])
```

```text
run run-... status=completed  wall=0.00s
  pipelines: total=1 succeeded=1
  pipeline latency ms: p50=0.487 p95=0.487 max=0.487
  attempts: total=1
  tasks: double=1
pipeline b3265e1db9b8 state=succeeded tasks=1/1
final artifact: {'doubled': 42, 'n': 21}
```

五个概念，就是全部词汇：

| 概念 | 在本程序中 | 它是什么 |
|---|---|---|
| **工件（artifact）** | `{"n": 21, "doubled": 42}` | 任务持久化后的输出，一产出就写盘 |
| **任务（task）** | `double` | 一元函数 `(artifact) -> artifact`；同步异步都行，不用继承任何基类 |
| **流水线（pipeline）** | `pipeline("doubling", double)` | 任务的线性链；完成和恢复的单位 |
| **种子（seed）** | `[{"n": 21}]` | 数据集里的一行；`template.map(seeds)` 给每行生成一条流水线 |
| **运行器（runner）** | `Runner(store=..., concurrency=2)` | 调度器：管着存储、资源池和 worker 槽位 |

三件事现在就得知道：

* 任务只接受**一个**参数；如果需要框架上下文，就接受**两个**（`value, ctx`）。多一个少一个装饰时都会报 `ConfigError`。额外状态放进闭包就行。
* `Runner` 是上下文管理器，退出 `with` 块就关存储。要读**基于文件**的存储，就在 `with` 块里读，或者之后重新打开那个文件（第 10 步讲）。内存存储没这问题。
* `store=":memory:"` 是默认值，测试时就用它；给个路径就是持久化的 SQLite 文件。

---

## 第 2 步 —— 一次处理多个输入

流水线模板是可复用的：`map()` 把一串种子变成一批语义上完全独立的流水线。

```python
# tutorial/step_02_map.py
"""Step 2 - one template, many inputs: map() turns a stream of seeds into independent pipelines."""

import asyncio
import time

from pyattacker import Runner, pipeline, task


def dataset(n: int):
    """Any iterable works; a generator keeps memory flat no matter how large the dataset is."""
    for i in range(n):
        yield {"qid": f"q{i:02d}", "value": i}


@task("square")
async def square(row: dict) -> dict:
    await asyncio.sleep(0.02)  # <- in real life: one HTTP request
    return {"qid": row["qid"], "squared": row["value"] ** 2}


template = pipeline("squares", square)

with Runner(store="runs/step02.db", concurrency=4, label="step-2") as runner:
    started = time.perf_counter()
    report = runner.run(template.map(dataset(12)))
    elapsed = time.perf_counter() - started
    print(report.summary())
    print(f"12 pipelines x 20ms of work, concurrency=4 -> {elapsed:.2f}s wall clock")

    # pass@k: one seed expands into k independent pipelines, each with its own checkpoint.
    for spec in template.map([{"qid": "q00", "value": 0}], repeats=3):
        print(f"  repeat={spec.repeat} key={spec.key[:12]}")
```

```text
run run-... status=completed  wall=0.07s
  pipelines: total=12 succeeded=12
  pipeline latency ms: p50=20.734 p95=21.868 max=22.085
  attempts: total=12
  tasks: square=12
12 pipelines x 20ms of work, concurrency=4 -> 0.07s wall clock
  repeat=0 key=b8c5e6d11da5
  repeat=1 key=418c035f6b74
  repeat=2 key=055e9cb3f519
```

* `map()` 接受**任何可迭代对象**，包括生成器，而且是惰性的。内存占用保持在 O(`concurrency`)——一千万行的数据集和十行数据集，内存开销一样。
* `concurrency=4` 指同时有四个**在飞的尝试**，不是四条活的流水线——流水线在等重试退避的时候不占 worker 槽位（第 8 步讲）。
* 任务体必须真的 `await` 点什么，工作才会重叠。并发 4 的时候，12 × 20ms 大概 0.07s 跑完；如果任务把事件循环堵死，就得 0.24s。
* `repeats=3` 就是 pass@k 和自洽采样：一个种子、三条流水线、三个独立检查点。用 `key_of=lambda row: row["qid"]` 可以自己指定 id，不用默认的内容寻址。
* `spec.key`（和 `pipeline_id` 是同一个值）来自任务链摘要 + 种子内容 + 重复序号，所以重跑同一份数据集，id 完全一样——恢复和分片就靠这个。

---

## 第 3 步 —— 串联任务

`a | b | c` 就是一条流水线。每个任务的工件就是下一个任务的输入，链在**构建时**校验，不是跑到一半才报错。

```python
# tutorial/step_03_chain.py
"""Step 3 - chaining: prepare | answer | judge. Each task's artifact is the next task's input."""

from pyattacker import PipelineBuildError, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"question": seed["q"], "context": ["doc-1", "doc-2"]}


@task("answer")
def answer(row: dict) -> dict:
    return {**row, "answer": f"answer to {row['question']!r}"}


@task("judge")
def judge(row: dict) -> dict:
    return {"question": row["question"], "answer": row["answer"], "score": len(row["answer"]) % 5}


@task("expects_int")
def expects_int(value: int) -> int:
    return value


# The chain is validated where it is built, not in the middle of a run:
try:
    pipeline("broken", prepare | expects_int)
except PipelineBuildError as exc:
    print("rejected at construction time:", exc)

template = pipeline("rag", prepare | answer | judge, tags={"example": "step-3"})

with Runner(store="runs/step03.db", concurrency=2, label="step-3") as runner:
    report = runner.run(template.map([{"q": "why is the sky blue?"}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    print("tasks:")
    for item in row["tasks"]:
        print(f"  seq={item['seq']} {item['name']:<8} {item['state']:<9} "
              f"attempts={item['attempts_used']} duration_ms={round(item['duration_ms'], 3)}")
    print("artifacts (the seed, then one per task):")
    for item in row["artifacts"]:
        print(f"  seq={item['seq']:>2} {item['task']:<10} digest={item['digest'][:12]} "
              f"payload={item['payload']!r}")
    print("final artifact:", row["artifacts"][-1]["payload"])
```

```text
rejected at construction time: artifact types do not chain: task 'prepare' produces dict, but task 'expects_int' requires int
run run-... status=completed  wall=0.00s
  pipelines: total=1 succeeded=1
  ...
tasks:
  seq=0 prepare  succeeded attempts=1 duration_ms=0.016
  seq=1 answer   succeeded attempts=1 duration_ms=0.005
  seq=2 judge    succeeded attempts=1 duration_ms=0.004
artifacts (the seed, then one per task):
  seq=-1 __seed__   digest=ce5d39232b24 payload={'q': 'why is the sky blue?'}
  seq= 0 prepare    digest=0ad2b84efc93 payload={'context': ['doc-1', 'doc-2'], 'question': 'why is the sky blue?'}
  seq= 1 answer     digest=c6fbc3132821 payload={'answer': "answer to 'why is the sky blue?'", 'context': [...], 'question': '...'}
  seq= 2 judge      digest=ffc919adcb7e payload={'answer': "answer to 'why is the sky blue?'", 'question': '...', 'score': 2}
final artifact: {'answer': "answer to 'why is the sky blue?'", 'question': 'why is the sky blue?', 'score': 2}
```

* 链的校验看的是**类型注解**：返回 `Question` 的任务可以接到接受 `Question` 的任务后面（子类也行），`Any` 或没注解不做检查，裸容器可以接参数化形式（`dict` ← `dict[str, Any]`）。不匹配就在构建流水线时抛 `PipelineBuildError`。
* `seq` 是任务在链里的位置。`seq=-1` 是种子：数据集那行本身也存成了工件——所以恢复的时候不需要原始数据集文件也能跑（第 9 步讲）。
* 最终工件是最后一个任务的输出，标了 `is_final=True`。
* 流水线是**线性的**。真要分支——三个评委、k 个样本、多个指标——用 `fanout(...)` 把分支放在单个任务内部（第 13 步讲）。

---

## 第 4 步 —— 检查点就是工件

每个任务成功就立刻把自己的工件落盘，所以检查点的粒度就是任务。

```python
# tutorial/step_04_checkpoint.py
"""Step 4 - what a checkpoint is: one content-addressed artifact per task, durable immediately."""

from dataclasses import dataclass

from pyattacker import Runner, pipeline, task


@dataclass
class Answer:
    qid: str
    text: str


@task("fetch")
def fetch(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": seed["question"]}


@task("answer")
def answer(row: dict) -> Answer:
    """An annotated type is registered automatically, so a restored checkpoint is an Answer again."""
    return Answer(qid=row["qid"], text=f"answer to {row['question']!r}")


@task("score")
def score(ans: Answer) -> dict:
    return {"qid": ans.qid, "score": len(ans.text) % 5}


template = pipeline("qa", fetch | answer | score, tags={"stage": "tutorial"})
rows = [{"qid": "q1", "question": "why?"}, {"qid": "q2", "question": "how?"}]

with Runner(store="runs/step04.db", concurrency=2) as runner:
    first = runner.run(template.map(rows))
    print("first run: ", first.stats["pipelines"]["by_state"])

    # Same seeds + same task source -> the same content-addressed keys -> nothing runs again.
    again = runner.run(template.map(rows))
    print("second run: skipped =", again.skipped,
          "(a skipped pipeline is not rewritten, so it still belongs to the first run)")

    row = next(iter(runner.store.export_rows()))
    print(f"\npipeline {row['pipeline_id'][:12]} tags={row['tags']}")
    for item in row["artifacts"]:
        print(f"  seq={item['seq']:>2} {item['task']:<10} {item['type']:<7} codec={item['codec']:<5} "
              f"digest={item['digest'][:10]} final={item['is_final']} payload={item['payload']!r}")

    checkpoint = runner.store.get_artifact(row["pipeline_id"], 1)
    restored = runner.registry.load(checkpoint.encoded())
    print("checkpoint at seq=1 restores as:", type(restored).__name__, restored)


@task("score")
def score_changed(ans: Answer) -> dict:
    """Same name, different body: the source digest changes, so this is a *different* pipeline."""
    return {"qid": ans.qid, "score": len(ans.text) % 7}


changed = pipeline("qa", fetch | answer | score_changed, tags={"stage": "tutorial"})
print("\nchanged task source -> new pipeline:", template.spec_digest != changed.spec_digest)
print("old spec_digest:", template.spec_digest[:16], "new spec_digest:", changed.spec_digest[:16])
```

```text
first run:  {'succeeded': 2}
second run: skipped = 2 (a skipped pipeline is not rewritten, so it still belongs to the first run)

pipeline 46122647cecb tags={'stage': 'tutorial'}
  seq=-1 __seed__   dict    codec=json  digest=354ba03eb6 final=False payload={'qid': 'q1', 'question': 'why?'}
  seq= 0 fetch      dict    codec=json  digest=354ba03eb6 final=False payload={'qid': 'q1', 'question': 'why?'}
  seq= 1 answer     Answer  codec=json  digest=a14ab1a820 final=False payload={'qid': 'q1', 'text': "answer to 'why?'"}
  seq= 2 score      dict    codec=json  digest=29d44d6ed5 final=True payload={'qid': 'q1', 'score': 1}
checkpoint at seq=1 restores as: Answer Answer(qid='q1', text="answer to 'why?'")

changed task source -> new pipeline: True
old spec_digest: 5808fe46061bad55 new spec_digest: 01b60826ed0465ea
```

每个任务成功时，按顺序发生四件事：工件字节写盘、任务行记录完成、`n_tasks_done` 往前推，然后下一个任务才启动。

* **内容寻址。** 这里 `seq=-1` 和 `seq=0` 是同一个摘要，因为 `fetch` 原样返回了种子——相同的载荷就是相同的字节，只存一份。工件的身份是 `(pipeline_id, seq)`。
* **数据类能往返。** 标注返回类型（`-> Answer`）就会自动注册这个类，所以恢复出来的检查点是 `Answer`，不是 `dict`。二进制类型就注册编解码器（第 14 步讲）。
* **改了任务代码，就是新流水线。** 流水线 key 包含每个任务源码的摘要，所以你改了任务体，旧检查点就不会被复用——不会拿不同代码的结果当自己的。想让任务体变了还复用检查点，就给 `pipeline(...)` 传 `include_code=False`；工厂参数、子任务和声明的策略还是会影响身份。手动指定的 key 会拒绝任务/输入不匹配的情况。v2 存储升级和外部副作用幂等性，见[恢复同一性](reference.md#恢复同一性)。
* **重跑同一份种子等于没跑。** 第二次运行报 `skipped=2`。被跳过的流水线不会被改写，它还是属于第一次真正跑了它的那次运行。

---

## 第 5 步 —— 端点作为资源池

`Resource` 是一项具体能力（某个端点、某把 key、某个本地 worker）。`Pool` 就是一组这样的资源，加上一套等待策略。任务通过 `ctx.acquire(...)` 拿其中一个。

```python
# tutorial/step_05_pool.py
"""Step 5 - endpoints as a pool: a factory makes a client, capacity bounds concurrency, selectors route."""

import asyncio

from pyattacker import Pool, Resource, Runner, pipeline, task


class Client:
    """The shape your real provider client should have: built from resource.options, owned by the pool."""

    def __init__(self, options: dict) -> None:
        self.model = options["model"]

    async def chat(self, prompt: str) -> str:
        await asyncio.sleep(0.01)  # <- your HTTP call
        return f"[{self.model}] {prompt}"


def build_client(resource: Resource) -> Client:
    return Client(resource.options)


@task("ask", resource="apis", timeout_s=5)
async def ask(row: dict, ctx) -> dict:
    model = "gpt-4o" if row["hard"] else "gpt-4o-mini"
    async with ctx.acquire(model=model) as lease:  # only a matching resource is handed out
        text = await lease.client.chat(row["question"])
        lease.report(ok=True, latency_ms=10, usage={"tokens": len(text)})
        return {"question": row["question"], "answer": text, "served_by": lease.resource.id}


pool = Pool(
    "apis",
    [
        Resource.create("llm", id="api-a", capacity=2, options={"model": "gpt-4o"}, factory=build_client),
        Resource.create("llm", id="api-b", capacity=2, options={"model": "gpt-4o"}, factory=build_client),
        Resource.create("llm", id="api-c", capacity=4, options={"model": "gpt-4o-mini"}, factory=build_client),
    ],
)

rows = [{"question": f"q{i}", "hard": i % 2 == 0} for i in range(6)]

with Runner(store="runs/step05.db", pools=[pool], concurrency=8) as runner:
    report = runner.run(pipeline("qa", ask).map(rows))
    print(report.summary())

    print("resource state (capacity is per resource, so this pool can hand out 8 leases at once):")
    for slot in pool.snapshot():
        print(f"  {slot['id']:<6} state={slot['state']:<7} leases={slot['leases']} "
              f"active_now={slot['active']}/{slot['capacity']}")

    stats = pool.stats()
    print(f"pool totals: leases={stats.leases_total} ok={stats.ok_total} "
          f"waiting={stats.waiting} utilization={stats.utilization}")
    print("usage reported through lease.report():", stats.usage)
```

```text
run run-... status=completed  wall=0.02s
  pipelines: total=6 succeeded=6
  ...

resource state (capacity is per resource, so this pool can hand out 8 leases at once):
  api-a  state=ready   leases=2 active_now=0/2
  api-b  state=ready   leases=1 active_now=0/2
  api-c  state=ready   leases=3 active_now=0/4
pool totals: leases=6 ok=6 waiting=0 utilization=0.0
usage reported through lease.report(): {'tokens': 81.0}
```

* `capacity` 是**资源**级别的，不是资源池级别的：`api-a` 和 `api-b` 各允许 2 个并发，`api-c` 允许 4 个，所以这个池子一共能同时发 8 个请求。把这个总数和 `concurrency` 比一下——worker 比容量多，就意味着 worker 要在资源池上排队。
* `factory=` **每个资源只调一次**，懒执行，第一次租约的时候才调；之后这个资源的所有租约共享同一个 client 对象。你的 SDK client 或连接池就放这里。如果 factory 抛异常，框架会用 `ResourceUnavailable` 拒绝这次租约（绝不会给你一个 `client` 是 `None` 的租约），记一条 `resource.factory_failed` 事件；反复失败最终会把这个资源标成 `dead`。
* 选择器（`ctx.acquire(model="gpt-4o-mini")`）按 `options`、`tags`、`id`、`kind` 匹配，也支持用点号路径深入嵌套 options（`"quota.tokens"`）——这样才能路由到对的 client。
* 资源池的所有信息都来自 `lease.report(...)`：`ok=False` 用来熔断，`latency_ms` 维护 EMA，`usage={"tokens": n}` 累积配额供排序用（第 6 步讲）。
* `pool.snapshot()` 是逐资源视图，`pool.stats()` 是聚合视图。运行期间调用都是安全的。

> **坑：** 用默认的 `wait` 算法时，选择器**匹配不到**任何资源会永久卡死。要么确保每个选择器都有对应资源，要么传 `timeout=` / 用 `algorithm="immediate"`——这样至少会报错，不会干等着。

---

## 第 6 步 —— 如何选择等待方式

怎么拿资源是策略问题，怎么用资源是你的代码。获取算法可以按任务设，也可以按资源池设。

```python
# tutorial/step_06_algorithms.py
"""Step 6 - how a task waits for a resource is a policy, orthogonal to what the task does."""

import asyncio

from pyattacker import Pool, Resource, Runner, pipeline, task


def make_pool(name: str, algorithm: str, *, resources: int = 1, capacity: int = 1,
              options: dict | None = None) -> Pool:
    return Pool(
        name,
        [
            Resource.create("llm", id=f"{name}-{i}", capacity=capacity,
                            options=dict(options or {"model": "m"}))
            for i in range(resources)
        ],
        algorithm=algorithm,
    )


def experiment(algorithm: str, rows: list, *, resources: int = 1, capacity: int = 1,
               concurrency: int = 4):
    """One task body, one pool, four acquire policies."""

    @task("ask", resource=f"p-{algorithm}", algorithm=algorithm)
    async def ask(row: dict, ctx) -> dict:
        async with ctx.acquire() as lease:
            await asyncio.sleep(0.02)  # the request
            return {"i": row["i"], "resource": lease.resource.id}

    pool = Pool(f"p-{algorithm}", [
        Resource.create("llm", id=f"p-{algorithm}-{i}", capacity=capacity, options={"model": "m"})
        for i in range(resources)
    ], algorithm=algorithm)
    with Runner(store=f"runs/step06_{algorithm}.db", pools=[pool], concurrency=concurrency) as runner:
        report = runner.run(pipeline(f"t-{algorithm}", ask).map(rows))
        error_types = sorted({e["error_type"] for e in runner.store.errors()})
    return report, pool, error_types


rows4 = [{"i": i} for i in range(4)]

# immediate: fail fast when the pool is saturated (note: ResourceUnavailable is classified "unknown",
# so the default policy does not retry it - add retry_unknown=True if you want capacity retries)
report, _, error_types = experiment("immediate", rows4)
print("immediate :", report.stats["pipelines"]["by_state"], "-> errors:", error_types)

# wait (the default): queue up until a slot frees
report, _, _ = experiment("wait", rows4)
print("wait      :", report.stats["pipelines"]["by_state"], "(4 x 20ms of work on a single slot)")

# least_busy: spread over the resources instead of piling onto the first free one
report, pool, _ = experiment("least_busy", [{"i": i} for i in range(6)],
                             resources=2, concurrency=2)
print("least_busy:", report.stats["pipelines"]["by_state"],
      "leases per resource:", {s["id"]: s["leases"] for s in pool.snapshot()})

# quota_aware: prefer the resource with the most quota left (declared in options, consumed via report)
pool = make_pool("p-quota", "quota_aware", resources=2)
pool.resources()[0].options["quota"] = {"tokens": 1000}
pool.resources()[1].options["quota"] = {"tokens": 100_000}


@task("budgeted", resource="p-quota", algorithm="quota_aware")
async def budgeted(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=True, usage={"tokens": 1000})
        return {"i": row["i"], "resource": lease.resource.id}


with Runner(store="runs/step06_quota.db", pools=[pool], concurrency=1) as runner:
    runner.run(pipeline("t-quota", budgeted).map([{"i": i} for i in range(3)]))
    picked = [row["artifacts"][-1]["payload"]["resource"] for row in runner.store.export_rows()]
print("quota_aware:", picked, "(roomiest first, then the top-up, then the exhausted one again)")

# sticky: a pipeline keeps the resource it already used (prompt caches, warm connections)
pool = make_pool("p-sticky", "sticky", resources=2)


@task("two_calls", resource="p-sticky", algorithm="sticky")
async def two_calls(row: dict, ctx) -> dict:
    async with ctx.acquire() as first:
        first_id = first.resource.id
    async with ctx.acquire() as second:
        return {"first": first_id, "second": second.resource.id, "same": first_id == second.resource.id}


with Runner(store="runs/step06_sticky.db", pools=[pool], concurrency=1) as runner:
    runner.run(pipeline("t-sticky", two_calls).map([{"i": 0}]))
    payload = next(iter(runner.store.export_rows()))["artifacts"][-1]["payload"]
print("sticky    :", payload)
```

```text
immediate : {'failed': 3, 'succeeded': 1} -> errors: ['ResourceUnavailable']
wait      : {'succeeded': 4} (4 x 20ms of work on a single slot)
least_busy: {'succeeded': 6} leases per resource: {'p-least_busy-0': 3, 'p-least_busy-1': 3}
quota_aware: ['p-quota-1', 'p-quota-0', 'p-quota-1'] (roomiest first, then the top-up, then the exhausted one again)
sticky    : {'first': 'p-sticky-0', 'same': True, 'second': 'p-sticky-0'}
```

| 算法 | 没空闲资源时的行为 | 什么时候用 |
|---|---|---|
| `wait` *（默认）* | 排队等槽位释放或 `timeout=` 超时 | 稳态负载，更看重吞吐而不是延迟 |
| `backoff` | 指数退避 + 抖动（`base`、`factor`、`cap`、`max_wait`） | 接口方已经饱和，避免释放时大家一起冲 |
| `least_busy` | 选负载比例最低的资源，否则退化为等待 | 多个容量不一样的端点 |
| `failover` | 按顺序试一组资源池，然后回退 | 主备接口方，不同层用不同 key |
| `sticky` | 优先用这条流水线已经用过的资源 | prompt/前缀缓存、热连接、粘性会话 |
| `quota_aware` | 先按剩余*比例*排，再按绝对余量排 | 预算有限的端点，`options={"quota": {"tokens": N}}` |
| `immediate` | 立刻抛 `ResourceUnavailable` | 宁可丢负载也不想排队 |

输出里有两个点值得注意：

* `immediate` 让 4 条流水线挂了 3 条。`ResourceUnavailable` 归类成 `unknown`，默认重试策略不重试 `unknown`。想让容量问题也重试，就设 `Retrying(max_attempts=3, retry_unknown=True)`。
* `quota_aware` 先选了 `p-quota-1`（两个都是新的，绝对余量大的平局胜出），然后是 `p-quota-0`（还没用过，比例更优），等那个 1000 token 的资源用完了又回到 `p-quota-1`。配额只是偏好，不是硬停——要硬限制就自己在任务里跟踪预算然后抛错。

获取时的退避（`backoff`）和重试时的退避（`Retrying`）是**两个独立的旋钮**：前者决定等槽位等多久，后者决定失败后等多久。

---

## 第 7 步 —— 租约契约

`async with ctx.acquire(...)` 在任何退出路径上都会归还资源。这一步把每条路径都走一遍。

```python
# tutorial/step_07_lease_safety.py
"""Step 7 - the lease contract: every exit path returns the resource, or the framework reclaims it and says so."""

import asyncio

from pyattacker import Pool, Resource, Retrying, Runner, pipeline, task


def make_pool() -> Pool:
    return Pool("apis", [Resource.create("llm", id="api-1", capacity=1)], algorithm="wait")


def event_kinds(store) -> list[str]:
    return [e.kind for e in store.events(limit=100) if e.kind.startswith(("resource.", "lease."))]


# 1) an exception inside the block
@task("raises_while_holding", resource="apis", retry=Retrying(max_attempts=1))
async def raises_while_holding(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=False, error="boom")
        raise RuntimeError("the request blew up")  # __aexit__ still runs and still returns the lease


pool = make_pool()
with Runner(store="runs/step07a.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("raises", raises_while_holding).map([{"i": 0}]))
    print("1) exception:", report.stats["pipelines"]["by_state"],
          "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active, "ready =", pool.stats().ready)


# 2) the escape hatch, with the release forgotten
@task("forgets_to_release", resource="apis")
async def forgets_to_release(row: dict, ctx) -> dict:
    lease = await ctx.acquire_lease()  # tracked by ctx, but not returned here
    return {"held": lease.resource.id}


pool = make_pool()
with Runner(store="runs/step07b.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("leaky", forgets_to_release).map([{"i": 0}]))
    print("\n2) forgotten release:", report.stats["pipelines"]["by_state"],
          "| leases_leaked =", report.leases_leaked, "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active,
      "(the task still succeeded - a leak is reported, not hidden)")


# 3) the same leak, with strict_leases on
pool = make_pool()
with Runner(store="runs/step07c.db", pools=[pool], concurrency=1, strict_leases=True) as runner:
    report = runner.run(pipeline("leaky", forgets_to_release).map([{"i": 0}]))
    print("\n3) strict_leases:", report.stats["pipelines"]["by_state"],
          "| error:", sorted({e["error_type"] for e in runner.store.errors()}),
          "| leases_leaked =", report.leases_leaked)


# 4) cancellation by timeout_s
@task("too_slow", resource="apis", timeout_s=0.01)
async def too_slow(row: dict, ctx) -> dict:
    async with ctx.acquire() as lease:
        lease.report(ok=True)
        await asyncio.sleep(5)  # cancelled by timeout_s; the lease is returned on the way out
        return {"never": "reached"}


pool = make_pool()
with Runner(store="runs/step07d.db", pools=[pool], concurrency=1) as runner:
    report = runner.run(pipeline("slow", too_slow).map([{"i": 0}]))
    attempt = runner.store.attempts()[0]
    print("\n4) timeout_s:", report.stats["pipelines"]["by_state"],
          "| error_class =", attempt.error_class, "| events:", event_kinds(runner.store))
print("   pool afterwards: active =", pool.stats().active,
      "(cancellation cannot strand a resource)")
```

```text
1) exception: {'failed': 1} | events: ['resource.leased', 'resource.released']
   pool afterwards: active = 0 ready = 1

2) forgotten release: {'succeeded': 1} | leases_leaked = 1 | events: ['resource.leased', 'lease.leaked', 'resource.leaked']
   pool afterwards: active = 0 (the task still succeeded - a leak is reported, not hidden)

3) strict_leases: {'failed': 1} | error: ['LeaseLeakError'] | leases_leaked = 1

4) timeout_s: {'failed': 1} | error_class = timeout | events: ['resource.leased', 'resource.released']
   pool afterwards: active = 0 (cancellation cannot strand a resource)
```

| 场景 | 保证 |
|---|---|
| `async with ctx.acquire(...)` 正常退出 | 退出时同步归还 |
| 块里抛异常 | 照样归还；`__aexit__` 会执行，也不吞异常 |
| 循环里 acquire → use → release | 每轮都归还，真正让出并发 |
| `timeout_s` 超时、运行被取消、Ctrl-C | `finally` 里同步归还 |
| `await ctx.acquire_lease()` 后忘了还 | 任务结束时强制回收，记 `lease.leaked` + `resource.leaked` |
| 任务返回了还占着租约 | 不可能——回收在写任务行之前就做了 |

你该怎么做：

* 用 `async with ctx.acquire(...) as lease:`，而且只在请求期间拿着租约。长时间持有是合法的，但那就是让资源池饥饿的原因。
* 忘了还会被报告，不会被藏着：查 `report.leases_leaked`，盯 `lease.leaked` 事件，CI 里开 `strict_leases=True`——直接让任务失败，不只是报告。
* 从一个资源池拿了资源，又向同一个资源池申请第二个，这是一种死锁形态。资源池会在 `deadlock_warn_s`（默认 5s）后发 `acquire.suspected_deadlock`，不会安静挂着。要么一开始就把两个都拿到，要么用两个资源池。

---

## 第 8 步 —— 失败：分类、重试、记录

失败就是普通的 Python 异常。框架会给它分类，问你的策略怎么办，然后把每一次尝试和每一个决策都记下来。

```python
# tutorial/step_08_retry.py
"""Step 8 - failure handling: classify once, retry by policy, keep the decision in the record."""

from pyattacker import (
    FatalError,
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    error_class_of,
    pipeline,
    task,
)


class HttpError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


print("classification is one pure function over any exception:")
for exc in (HttpError(429), HttpError(503), HttpError(404), TimeoutError(),
            ConnectionError("reset"), ValueError("bad json"), Exception("?")):
    print(f"  {type(exc).__name__:<16}{str(exc):<12} -> {error_class_of(exc)}")

CALLS = {"n": 0}


@task("ask", resource="apis", retry=Retrying(max_attempts=4, base=0.01, cap=0.05))
async def ask(row: dict, ctx) -> dict:
    CALLS["n"] += 1
    async with ctx.acquire() as lease:
        if ctx.attempt <= 2:
            lease.report(ok=False, error="429")
            # A retryable error carries its own class, and retry_after is a server-suggested delay.
            raise RetryableError("rate limited", error_class="rate_limit", retry_after=0.01)
        lease.report(ok=True)
        return {"i": row["i"], "attempts": ctx.attempt}


@task("always_fatal", resource="apis", retry=Retrying(max_attempts=5))
def always_fatal(row: dict) -> dict:
    raise FatalError("the request itself is invalid; another attempt cannot help")


fatal_specs = list(pipeline("fatal", always_fatal).map([{"i": 9}]))

with Runner(store="runs/step08.db", pools=[Pool("apis", [Resource.create("llm", id="api-1", capacity=4)])],
            concurrency=2) as runner:
    report = runner.run(pipeline("retrying", ask).map([{"i": 1}]))
    print("\nretried pipeline:", report.stats["pipelines"]["by_state"],
          "| requests made:", CALLS["n"])
    for attempt in runner.store.attempts():
        print(f"  attempt {attempt.attempt_no} {attempt.outcome:<9} class={attempt.error_class or '-':<11} "
              f"delay_s={attempt.retry_delay_s} why={attempt.decision.get('reason')}")

    fatal = runner.run(iter(fatal_specs))
    attempts = runner.store.attempts(pipeline_id=fatal_specs[0].pipeline_id)
    print("fatal pipeline:  ", fatal.stats["pipelines"]["by_state"],
          f"| attempts used: {len(attempts)} of max_attempts=5 ->", attempts[0].error_class)
```

```text
classification is one pure function over any exception:
  HttpError       HTTP 429     -> rate_limit
  HttpError       HTTP 503     -> upstream
  HttpError       HTTP 404     -> fatal
  TimeoutError                 -> timeout
  ConnectionError reset        -> connection
  ValueError      bad json     -> invalid
  Exception       ?            -> unknown

retried pipeline: {'succeeded': 1} | requests made: 3
  attempt 1 failed    class=rate_limit  delay_s=0.01 why=retryable
  attempt 2 failed    class=rate_limit  delay_s=0.01 why=retryable
  attempt 3 succeeded class=-           delay_s=None why=ok
fatal pipeline:   {'failed': 1} | attempts used: 1 of max_attempts=5 -> fatal
```

默认重试：`retryable`、`rate_limit`、`timeout`、`connection`、`upstream`。默认不重试：
`invalid`、`fatal`、`cancelled`、`unknown`。分类会读 `TimeoutError`、`ConnectionError`、`error_class` 属性，以及 `status` / `status_code` / `http_status` / `code` 上的 HTTP 状态码（回退到 `exc.response.status_code`）——覆盖了常见的接口方 SDK，不用 import 它们。完整表格见[参考文档](reference.md#错误类别)。

你最常用的 `Retrying` 字段：

| 字段 | 默认值 | 意思 |
|---|---|---|
| `max_attempts` | `1` | 总尝试次数——**默认不重试** |
| `on` | `()` | 额外视为可重试的异常类型 |
| `retry_unknown` | `False` | 也重试 `unknown`，比如 `ResourceUnavailable` |
| `base`、`factor`、`cap` | `0.5`、`2.0`、`30.0` | 指数退避的上下界 |
| `max_total_s` | `None` | 尝试次数 + 延迟超过这个预算就放弃 |

引导它的方式：

* `raise RetryableError("rate limited", error_class="rate_limit", retry_after=0.01)`——带上错误类别，还遵守服务端建议的延迟。异常暴露 `Retry-After` 头时也会从中读 `retry_after`。
* `raise FatalError(...)`——永不重试，不管 `max_attempts` 设了多少。
* `with_retry(ask, max_attempts=5)`——用不同策略复用同一个任务。

重试退避不会占着 worker：流水线挂起，worker 去干别的，所以 `concurrency` 始终名副其实。退避期间结束的运行会把挂着的流水线记成 `interrupted`，检查点完好，`resume` 能接着跑。

---

## 第 9 步 —— 恢复：什么会重跑，什么不会

一次运行能恢复，是因为存储里已经有了剩下的活需要的所有东西。

```python
# tutorial/step_09_resume.py
"""Step 9 - resume: a failed pipeline continues at the first task that produced no artifact."""

from pyattacker import RetryableError, Retrying, Runner, pipeline, task

STATE = {"judge_works": False}
SENT = {"ask": 0, "judge": 0}


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"qid": seed["qid"], "prompt": f"explain {seed['qid']}"}


@task("ask", retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
def ask(row: dict) -> dict:
    SENT["ask"] += 1  # <- in real life this is the request you do not want to pay for twice
    return {**row, "answer": "42"}


@task("judge", retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
def judge(row: dict) -> dict:
    SENT["judge"] += 1
    if not STATE["judge_works"]:
        raise RetryableError("the judge model is down", error_class="upstream")
    return {**row, "score": 1}


template = pipeline("eval", prepare | ask | judge)
rows = [{"qid": f"q{i}"} for i in range(3)]
specs = list(template.map(rows))

with Runner(store="runs/step09.db", concurrency=2) as runner:
    first = runner.run(iter(specs))
    print("round 1:", first.stats["pipelines"]["by_state"], "| requests sent:", SENT,
          "| attempts recorded:", len(runner.store.attempts()))
    print("  failed at:", sorted({e["failed_task"] for e in runner.store.errors()}))

    STATE["judge_works"] = True
    second = runner.run(iter(template.map(rows)), resume=True)
    print("round 2:", second.stats["pipelines"]["by_state"], "| requests sent:", SENT,
          "| skipped:", second.skipped)
    print("  -> ask was not re-sent; only the failed task ran again")

    row = next(iter(runner.store.export_rows()))
    print(f"\n  checkpoint: {row['n_tasks_done']}/{row['n_tasks_total']} tasks done, state={row['state']}")
    for attempt in runner.store.attempts(pipeline_id=specs[0].pipeline_id):
        print(f"    {attempt.task_name:<8} attempt {attempt.attempt_no} {attempt.outcome:<9} "
              f"run={attempt.run_id[4:]}")

    seed_artifact = runner.store.get_artifact(specs[0].pipeline_id, -1)
    print("  the seed is a stored artifact too: seq=-1 ->", seed_artifact.type_name,
          seed_artifact.digest[:10])
```

```text
round 1: {'failed': 3} | requests sent: {'ask': 3, 'judge': 6} | attempts recorded: 12
  failed at: ['judge']
round 2: {'succeeded': 3} | requests sent: {'ask': 3, 'judge': 9} | skipped: 0
  -> ask was not re-sent; only the failed task ran again

  checkpoint: 3/3 tasks done, state=succeeded
    prepare  attempt 1 succeeded run=20260915-213904-b464d3
    ask      attempt 1 succeeded run=20260915-213904-b464d3
    judge    attempt 1 failed    run=20260915-213904-b464d3
    judge    attempt 2 failed    run=20260915-213904-b464d3
    judge    attempt 1 succeeded run=20260915-213904-548d63
  the seed is a stored artifact too: seq=-1 -> dict d6125621c2
```

规则按这个顺序适用：

1. 流水线已经 `succeeded` → 直接跳过。传 `retry_succeeded=True` / `--retry-succeeded` 可以照样重跑（它会从种子开始，因为已完成的流水线没有检查点可以接着跑）。要 *丢* 掉没成功流水线的检查点，用 `fresh_restart=True` / `--fresh-restart`——同一个开关也会重置已经耗尽的反向遍历预算。只追加的历史（attempts、events、handoffs）保留；反向流水线还会留着它的访问记录和计数器，正向流水线按设计复用它的任务/工件地址。
2. 流水线是 `failed` 或 `interrupted`，且 `n_tasks_done > 0` → 加载 `n_tasks_done - 1` 处的工件，从下一个 `seq` 继续。**这就是第 2 轮里 `ask` 什么都没发的原因**：judge 是第一个没有工件的任务，所以只有 judge 跑了。
3. 流水线是 `failed` 或 `interrupted`，但游标已经到了末尾（`n_tasks_done == n_tasks_total`）→ 每个任务都写过检查点了，于是流水线被修复成 `succeeded`，**不重跑任何东西**：最后一个工件会过校验（存在、有载荷、能解码）、标成 final，`pipeline.terminal_repaired` 会带上这行原来的状态、错误和运行。这就是存储故障或最终写入时被杀留出来的形态。工件过不了校验就走规则 5 和 6；修复本身失败——把工件标 final 或写最终状态——那行保持原样（包括最初的失败），`pipeline.terminal_repair_failed` 记这次尝试，下次运行还是报最初的原因。
4. 游标 *超过* 末尾（`n_tasks_done > n_tasks_total`）→ 这不是 Runner 能造出来的状态：这行记成 `CorruptCheckpoint`，报 `pipeline.corrupt_cursor`，永远不会升成成功，存储里的游标原样留着当证据。
5. 工件行没了，或者载荷没留下来（`journal=summary`、`null` 后端）→ 流水线从 `seq=0` 重新开始，记 `pipeline.checkpoint_missing`（终止工件载荷没了也走这条，覆盖规则 3 的情况）。
6. 工件还在但解不出来（编解码器被删了、dataclass 改了、载荷坏了）→ 同样重新开始，记成 `pipeline.checkpoint_unusable`，两种原因能区分开（也覆盖规则 3 里终止工件存在但解不出来的情况）。
7. 否则流水线是新的，种子工件在第一个任务跑之前就存好了。

三个注意点：

* 第 2 轮 `skipped=0` 是对的：没有一条流水线成功过，三条都在 *failed* 状态然后被恢复了。`skipped` 只数规则 1，不是"没重复干的活"。
* 恢复时，数据集流只负责 *标识* 流水线——恢复后任务的输入来自存储。流水线中途中断的运行不需要数据集文件还在，只要还能枚举出同样的种子就行。
* 每次尝试都有自己的 `run_id`，所以记录能看出哪次运行做了哪部分活。

值得留意的事件：`pipeline.resumed`、`pipeline.skipped`、`pipeline.restarted`、
`pipeline.checkpoint_missing`、`pipeline.checkpoint_unusable`、`pipeline.deferred_interrupted`、
`pipeline.terminal_repaired`、`pipeline.terminal_repair_failed`、`pipeline.terminal_cleanup_failed`、
`pipeline.corrupt_cursor`。

还有两个运行级事件值得同样对待。`runner.internal_error` 是框架层面的意外，记在某条流水线上，运行继续。`runner.worker_crashed` 更严重：某个 worker 死于自己处理逻辑之外的原因（比如存储钩子抛了个非取消的 `BaseException`），运行就会停，这个 worker 当时持有的流水线记成 `failed`——如果运行已经在停了，就记成 `interrupted`——`run()` 会抛 `WorkerCrashed`，把原始异常当原因。看到这个事件就说明这次运行没按它自己预期完成：看流水线行，修原因，然后 `--resume` 重跑。

---

## 第 10 步 —— 读取记录

框架知道的一切都存在几张表里。其中五张以 `pipeline_id` 为键，也就是五种可导出的行类型（`--rows`）；`runs` 和 `resources` 有 Python 读取器，但没有导出途径。

```python
# tutorial/step_10_records.py
"""Step 10 - reading the record: five tables, five row kinds, three formats."""

from contextlib import closing

from pyattacker import ROW_KINDS, Runner, export_store, iter_rows, open_store, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"i": seed["i"], "value": seed["i"] * 2}


@task("check")
def check(row: dict) -> dict:
    if row["i"] % 2:  # deliberately fail the odd rows so the error surface has something to show
        raise ValueError("odd rows are not supported")
    return {"i": row["i"], "ok": True}


STORE = "runs/step10.db"
with Runner(store=STORE, concurrency=2, label="step-10") as runner:
    report = runner.run(pipeline("checked", prepare | check).map([{"i": i} for i in range(4)]))

    print(report.summary())
    print("\nto_dict() gives you the machine-readable version of the same facts:")
    print(" ", {k: v for k, v in report.to_dict().items() if k != "pipelines"})

    print("\nlive counters (safe to call while a run is in flight):")
    live = runner.stats()
    print(" ", {k: live[k] for k in ("in_flight_pipelines", "delayed_pipelines", "buffered")})
    print("  counters:", dict(live["counters"]))

    print("\nerrors():")
    for item in runner.store.errors():
        print(f"  {item['name']}/{item['failed_task']}: {item['error_type']}: {item['error_message']}")

    row = next(item for item in runner.store.export_rows() if item["state"] == "failed")
    print(f"\none failed pipeline {row['pipeline_id'][:12]}:")
    for item in row["tasks"]:
        print(f"  seq={item['seq']} {item['name']:<8} {item['state']:<9} "
              f"attempts={item['attempts_used']} error={item['error_class']}")
    for attempt in runner.store.attempts(pipeline_id=row["pipeline_id"]):
        print(f"  attempt {attempt.attempt_no} of {attempt.task_name}: outcome={attempt.outcome} "
              f"decision={attempt.decision.get('reason')} leases={len(attempt.leases)} "
              f"duration_ms={attempt.duration_ms}")
    print("  events:", [e.kind for e in runner.store.events(pipeline_id=row["pipeline_id"])][:8])

    print("\nrow kinds:")
    for kind in ROW_KINDS:
        print(f"  {kind:<10} {sum(1 for _ in iter_rows(runner.store, kind=kind))} rows")

    count = export_store(runner.store, "runs/step10_attempts.csv", kind="attempts", fmt="csv")
    print(f"\nexported {count} attempt rows as CSV")

# The store is a plain SQLite file: reopen it from anywhere, any process.
with closing(open_store(STORE)) as reopened:
    counts = reopened.stats()
    print("reopened:", counts["pipelines"]["total"], "pipelines,",
          counts["attempts_total"], "attempts,", counts["events_total"], "events")
```

```text
run run-... status=completed  wall=0.00s
  pipelines: total=4 failed=2 succeeded=2
  attempts: total=8
  tasks: check=4 prepare=4
  errors (2):
    - checked/check: ValueError: odd rows are not supported

to_dict() gives you the machine-readable version of the same facts:
  {'run_id': 'run-...', 'status': 'completed', 'duration_ms': 3.973, 'skipped': 0, 'leases_leaked': 0,
   'stop_reason': None, 'tasks': {'by_name': {'check': 4, 'prepare': 4}, ...}, 'attempts_total': 8, 'events_total': 13}

live counters (safe to call while a run is in flight):
  {'in_flight_pipelines': 0, 'delayed_pipelines': 0, 'buffered': {'pending': 0, 'flushes': 5, ...}}
  counters: {'pipelines_admitted': 4, 'pipelines_succeeded': 2, 'pipelines_done': 4, 'pipelines_failed': 2}

errors():
  checked/check: ValueError: odd rows are not supported

one failed pipeline f41b0b0541a2:
  seq=0 prepare  succeeded attempts=1 error=None
  seq=1 check    failed    attempts=1 error=invalid
  attempt 1 of prepare: outcome=succeeded decision=ok leases=0 duration_ms=0.004
  attempt 1 of check: outcome=failed decision=attempts_exhausted leases=0 duration_ms=0.004
  events: ['task.succeeded', 'task.failed', 'pipeline.failed']

row kinds:
  pipelines  4 rows
  tasks      8 rows
  attempts   8 rows
  events     13 rows
  artifacts  10 rows

exported 8 attempt rows as CSV
reopened: 4 pipelines, 8 attempts, 13 events
```

| 表 | 每行代表 | 里面有 | 读取器 |
|---|---|---|---|
| `runs` | 一次运行 | label、status、heartbeat、配置快照、版本、主机 | `store.get_run(id)`、`store.stats()` |
| `pipelines` | 一条流水线 | state、检查点（`n_tasks_done/…`）、key、种子/规格摘要、tags、失败信息、恢复链 | `store.pipelines(...)`、`store.export_rows()` |
| `tasks` | 流水线里的一个任务 | 最终状态、已用尝试数、耗时、工件 id、错误类别 | `store.tasks(...)`、`iter_rows(store, kind="tasks")` |
| `attempts` | 一次尝试 | 结果、错误类别/类型/回溯、**重试决策**、已用租约、耗时 | `store.attempts(...)`、`kind="attempts"` |
| `events` | 一个事件 | 结构化事件流：每个任务/流水线/资源的每次状态转换 | `store.events(...)`、`kind="events"` |
| `artifacts` | 一个工件 | 载荷、摘要、编解码器、类型、`blob_ref` | `store.artifacts(pid)`、`kind="artifacts"` |

* `report.summary()` 是给人看的，`report.to_dict()` 是给仪表盘用的，`runner.stats()` 用于实时视图（运行中途也安全），`store.errors()` 用于失败列表。
* `attempts` 是回答"为什么这花了 40 秒"的表：每次尝试的耗时、它的租约日志，还有它的决策（`retry`、`reason`、`delay_s`、`error_class`）。
* 导出形态：`--rows pipelines|tasks|attempts|events|artifacts` × `--format jsonl|json|csv`。CSV 会把嵌套值压成紧凑 JSON，电子表格里打开干干净净。
* 只追加的表（attempts、events）按批写；状态（artifacts、checkpoints）同步写。`SIGKILL` 最多丢最后一批历史，绝不会丢检查点。`--no-write-behind` 会立刻提交所有内容。

---

## 第 11 步 —— 声明式路径与 CLI

组装和资源可以放 YAML 里，逻辑还是写在 Python 里。声明式层只做三件事：挑任务、串成链、配资源池。

这是第一个需要标准库之外东西的步骤，也是额外依赖（extra）登场的地方：
`uv add "pyattacker[yaml]"`（或 `pip install "pyattacker[yaml]"`）。只有 YAML *解析器* 是可选的——这一层本身，还有用 `.json` 或 `.toml` 写的同一份配置，没有它照样工作。解析器按文件后缀选，所以没装这个额外依赖的机器上，`.yaml` 文件得到的是一个点明要装 extra 的配置错误，不是一段 import traceback。

```python
# tutorial/step_11_declarative.py
"""Step 11 - the declarative path: composition and resources in YAML, logic still in Python."""

import json
from pathlib import Path

from pyattacker import Runner, load_spec

CONFIG = """
run:      { store: runs/step11.db, concurrency: 4, label: declarative }
pools:
  apis:
    kind: llm
    algorithm: backoff        # back off instead of failing when every slot is busy
    capacity: 2               # pool-level default, overridable per resource
    degrade_after: 3          # 3 consecutive failures -> circuit-break for cooldown_s
    cooldown_s: 10
    resources:
      - { id: api-1, options: { model: gpt-4o, base_url: "https://api-a.example/v1",
                                api_key: "${OPENAI_KEY:-sk-demo}" } }
      - { id: api-2, options: { model: gpt-4o } }
pipeline:
  name: qa_eval
  tags: { bench: tutorial }
  tasks:
    - use: pyattacker.tasks:echo                 # replace with "your.pkg.tasks:fetch"
    - use: pyattacker.tasks:simulate_llm         # replace with "your.pkg.tasks:ask_model"
      resource: apis
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.0, tokens: 64, model: gpt-4o }
      retry: { max_attempts: 3, base: 0.01, cap: 0.05, "on": [RetryableError, TimeoutError] }
source: { kind: range, n: 8 }
"""

Path("qa.yaml").write_text(CONFIG, encoding="utf-8")
spec = load_spec("qa.yaml")  # <- builds the pools + the pipeline template, imports nothing of yours yet

print(json.dumps(spec.describe(), indent=2))
print("unresolved ${ENV} references:", spec.unresolved_env or "none")

with Runner(pools=spec.pools, **spec.run) as runner:
    report = runner.run(spec.pipelines())
    print(report.summary())
```

```text
{
  "config": "qa.yaml",
  "pipeline": {"name": "qa_eval", "tasks": ["mock.echo", "mock.llm"],
               "tags": {"bench": "tutorial"}, "spec_digest": "ad271b5b5b00fcb2..."},
  "pools": {"apis": {"kind": "llm", "resources": 2, "capacity": 4, "algorithm": "backoff"}},
  "run": {"store": "runs/step11.db", "concurrency": 4, "label": "declarative"},
  "source": {"kind": "range", "n": 8},
  "unresolved_env": []
}
unresolved ${ENV} references: none
run run-... status=completed  wall=0.01s
  pipelines: total=8 succeeded=8
  tasks: mock.echo=8 mock.llm=8
```

同一份配置从命令行用：

```bash
uv run pyattacker validate -c qa.yaml            # parse + summarise, run nothing (exit 2 on a config error)
uv run pyattacker run      -c qa.yaml --progress # run; --limit N for a slice
uv run pyattacker resume   -c qa.yaml            # same as run --resume
uv run pyattacker report   runs/step11.db --errors 20
uv run pyattacker watch    runs/step11.db        # live view from a second process
uv run pyattacker export   runs/step11.db out.jsonl --rows attempts --format csv
uv run pyattacker serve    runs/step11.db        # read-only HTTP debug endpoint (Step 14)
uv run pyattacker plugins                        # entry points, and which ones failed to load
```

配置段：

| 配置段 | 键 |
|---|---|
| `run` | `store`、`concurrency`、`journal`（`full` 留载荷，`summary` 只留摘要）、`label`、`strict_leases`、`stop_after_failures`、`stop_after_s`、`retry_succeeded`、`fresh_restart`、`heartbeat_s`、`grace_s`、`stale_after_s`、`notes` |
| `pools.<name>` | `kind`、`algorithm`、`capacity`（资源的默认值）、`degrade_after`、`dead_after`、`cooldown_s`、`deadlock_warn_s`、`resources: [{id, kind, capacity, options, tags}]` |
| `pipeline` | `name`、`tags`、`include_code`，以及 `tasks: [{use, name, resource, algorithm, timeout_s, retry, args, kwargs}]` |
| `source` | `kind: range`（`n`）或 `kind: jsonl`（`path`、`limit`），外加 `repeats`、`key_field` |

几个省时间的细节：

* `use:` 按形态解析：名字里有冒号的就是 `module:attribute`，直接导入；没冒号的先在内置项里找，再在已安装的插件里找——插件盖不住 `echo`。`pyattacker.tasks:simulate_llm` 和 `your_pkg.tasks:ask_model` 没插件也能用。工厂（返回 `TaskSpec` 的可调用对象）会用 `args`/`kwargs` 调用。
* `${VAR}` 和 `${VAR:-default}` 会在每个字符串*值*里展开（键不受影响）。没解析到的名字由 `validate` 和 `describe()["unresolved_env"]` 报告；`--strict-env` 会把它们当错误。
* **YAML 1.1 坑：** 裸写的 `on:` 会被解析成布尔值 `true`。写成 `"on": [RetryableError, TimeoutError]`——加载器会发现这个错误并提示你。
* 退出码：`0` 全成功，`1` 部分失败，`2` 配置错，`130` 被中断。

每条命令每个 flag 都在 [`docs/cli.md`](cli.md) 里。

---

## 第 12 步 —— 分片与合并

横向扩展就是多进程，各有各的存储，事后再合并。流水线分到哪个片由它的内容寻址 key 决定，所以同一份数据集永远按同样方式切。

```python
# tutorial/step_12_shards.py
"""Step 12 - scale out: deterministic shards with their own stores, merged into one answer."""

from pyattacker import (
    Runner,
    merge_reports,
    pipeline,
    shard_index,
    shard_specs,
    shard_store_path,
    task,
)


@task("ask")
def ask(seed: dict) -> dict:
    return {"i": seed["i"], "answer": seed["i"] * 3}


template = pipeline("eval", ask, tags={"example": "step-12"})
specs = list(template.map([{"i": i} for i in range(12)]))
SHARDS = 3
BASE = "runs/step12.db"

paths = []
for index in range(SHARDS):
    mine = list(shard_specs(specs, index, SHARDS))  # blake2b(pipeline_key) % SHARDS == index
    path = shard_store_path(BASE, index, SHARDS)  # runs/step12.shard0of3.db ...
    with Runner(store=path, concurrency=2, label=f"shard-{index}") as runner:
        report = runner.run(iter(mine))
    paths.append(path)
    print(f"shard {index}: {len(mine):>2} pipelines -> {report.stats['pipelines']['by_state']} -> {path}")

merged = merge_reports(paths)
print("\n" + merged.summary())
print("pipeline rows:", len(merged.rows), "| duplicates dropped:", merged.duplicates,
      "| sources:", len(merged.sources))

# Merging is idempotent: a duplicated store is de-duplicated by pipeline_id, best state wins.
again = merge_reports([*paths, paths[0]])
print("with one store counted twice:", len(again.rows), "rows,", again.duplicates, "duplicates dropped")
print("  attempts:", merged.stats()["attempts_total"], "->", again.stats()["attempts_total"],
      "| source_events:", merged.stats()["source_events_total"], "->",
      again.stats()["source_events_total"], "(raw, so it follows the sources)")

merged.export("runs/step12-all.jsonl")
with open("runs/step12-all.jsonl", encoding="utf-8") as handle:
    print("merged export rows:", sum(1 for _ in handle))

# Sharding is content-addressed, not round-robin: re-running the same seeds replays the same split.
print("shard of the first pipeline:", shard_index(specs[0].pipeline_id, SHARDS), "of", SHARDS)
```

```text
shard 0:  6 pipelines -> {'succeeded': 6} -> runs/step12.shard0of3.db
shard 1:  5 pipelines -> {'succeeded': 5} -> runs/step12.shard1of3.db
shard 2:  1 pipelines -> {'succeeded': 1} -> runs/step12.shard2of3.db

merged 12 pipelines from 3 store(s)
  succeeded=12  attempts=12 source_events=27
  tasks: ask=12
pipeline rows: 12 | duplicates dropped: 0 | sources: 3
with one store counted twice: 12 rows, 6 duplicates dropped
  attempts: 12 -> 12 | source_events: 27 -> 40 (raw, so it follows the sources)
merged export rows: 12
shard of the first pipeline: 1 of 3
```

```bash
# N children, one per shard, then a merged report (results land in runs/qa.shard0of4.db …)
uv run pyattacker run -c examples/qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db

# or drive each shard yourself — cluster, job scheduler, four terminals
uv run pyattacker run    -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
uv run pyattacker resume -c examples/qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db

# one coherent answer out of N files
uv run pyattacker report runs/qa.shard*of4.db
uv run pyattacker export runs/qa.shard*of4.db runs/all.jsonl
```

* `shard_index(key, N)` 用 `blake2b` 而不是 Python 的 `hash()`（后者按进程加盐）对 key 哈希，所以切分跨多次运行、跨机器都稳定。`--shard 2/4 --resume` 会把每条流水线放回原来的位置。各分片大小只是*大致*相等——12 条切成 6/5/1 不是 bug。
* 合并按 `pipeline_id` 去重，保留最好的状态（succeeded > failed > interrupted，平了取最晚完成的），并且从存活下来的行**重算**工作量计数——`attempts_total`、`handoffs_total`。部分重叠的重跑，或者同一个存储算了两次，结果还是一个答案。事件日志是刻意的例外：事件不挂在流水线行上，所以 `source_events_total` 是你传入那些源库的原始总数，名字就是这么起的，不假装成别的口径。
* `merge_reports([...])` 返回一个 `MergedReport`，带 `.summary()`、`.stats()`、`.errors()` 和 `.export(path, fmt=..., kind=...)`。

---

## 第 13 步 —— 压轴：一个小型模型评估

这就是一次真实评估的样子：prepare → 两轮模型调用 → 三个评委 → reduce，配两个模型资源池、重试、一次接口方故障，还有一次我们真的去度量代价的恢复。

```python
# tutorial/step_13_capstone.py
"""Step 13 - capstone: a small model evaluation, with the price of its checkpoint granularity measured."""

import asyncio

from pyattacker import (
    Pool,
    Resource,
    RetryableError,
    Retrying,
    Runner,
    fanout,
    pipeline,
    task,
)


class RequestLog:
    """Counts what actually left the process - the number this whole exercise is about."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def note(self, model: str) -> None:
        self.sent.append(model)

    def counts(self, since: int = 0) -> dict[str, int]:
        out: dict[str, int] = {}
        for model in self.sent[since:]:
            out[model] = out.get(model, 0) + 1
        return out


LOG = RequestLog()
DOWN: set[str] = {"judge-b"}  # the provider having a bad day; cleared before the resume round


class Backend:
    """Your client. The framework never opens a socket; this class is where the network lives."""

    def __init__(self, log: RequestLog, model: str) -> None:
        self.log = log
        self.model = model

    async def chat(self, prompt: str) -> str:
        self.log.note(self.model)
        await asyncio.sleep(0.002)  # <- the HTTP request
        if self.model in DOWN:
            raise RetryableError(f"{self.model} is unavailable", error_class="upstream")
        return f"[{self.model}] {prompt[:40]}"


def backend_factory(resource: Resource) -> Backend:
    return Backend(LOG, resource.options["model"])


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"qid": seed["qid"], "question": seed["question"]}


@task("ask", resource="models", timeout_s=10)
async def ask(row: dict, ctx) -> dict:
    """One task = one step of the pipeline, but it may make as many requests as it needs."""
    messages = [f"Q: {row['question']}"]
    async with ctx.acquire(model="eval-model") as lease:
        for _ in range(2):  # a two-turn conversation, inside a single checkpoint
            messages.append(await lease.client.chat(" | ".join(messages)))
        lease.report(ok=True, usage={"tokens": 64})
    return {**row, "transcript": messages}


def judge(model: str):
    @task(f"judge.{model}", resource="judges", algorithm="least_busy",
          retry=Retrying(max_attempts=2, base=0.005, cap=0.02))
    async def judge_with(row: dict, ctx) -> dict:
        async with ctx.acquire(model=model) as lease:
            verdict = await lease.client.chat(row["transcript"][-1])
            lease.report(ok=True, usage={"tokens": 8})
            return {**row, "judge": model, "verdict": verdict}

    return judge_with


judges = fanout(judge("judge-a"), judge("judge-b"), judge("judge-c"), name="judges")


@task("reduce")
def reduce_scores(row: dict) -> dict:
    """Aggregation across pipelines stays yours to write - the framework stores it, it does not judge it."""
    branches = list(row.values())
    return {
        "qid": branches[0]["qid"],
        "verdicts": {b["judge"]: b["verdict"] for b in branches},
        "n_models": len(branches),
    }


def make_pools() -> list[Pool]:
    return [
        Pool("models", [Resource.create("llm", id="m-1", capacity=4,
                                        options={"model": "eval-model"}, factory=backend_factory)]),
        Pool("judges", [Resource.create("llm", id=f"j-{name}", capacity=1,
                                        options={"model": name}, factory=backend_factory)
                        for name in ("judge-a", "judge-b", "judge-c")]),
    ]


template = pipeline("eval", prepare | ask | judges | reduce_scores, tags={"bench": "tutorial"})
rows = [{"qid": "q1", "question": "2 + 2 = ?"}, {"qid": "q2", "question": "3 + 4 = ?"}]

with Runner(store="runs/step13.db", pools=make_pools(), concurrency=4) as runner:
    first = runner.run(template.map(rows))
    print("round 1:", first.stats["pipelines"]["by_state"],
          "| requests:", LOG.counts(),
          "| failed at:", sorted({e["failed_task"] for e in runner.store.errors()}))

    DOWN.clear()
    mark = len(LOG.sent)
    second = runner.run(template.map(rows), resume=True)
    print("round 2:", second.stats["pipelines"]["by_state"],
          "| requests:", LOG.counts(mark), "| skipped:", second.skipped)
    print("total requests:", len(LOG.sent))

    row = next(iter(runner.store.export_rows()))
    print("\nfinal artifact:", row["artifacts"][-1]["payload"])

# The resume was honest but not free: judge-a and judge-c were re-sent even though their verdicts
# were already on disk. Putting the three judges in three *tasks* (C1 | C2 | C3) makes the
# checkpoint finer: the same failure would resume at C2, and re-send nothing that succeeded.
# examples/llm_eval/ measures both shapes side by side.
print("\nthe grouped shape re-sent verdicts that were already persisted:",
      sum(LOG.counts(mark).values()) - 2, "of", len(LOG.sent) - mark, "requests in round 2")
```

```text
round 1: {'failed': 2} | requests: {'eval-model': 4, 'judge-a': 4, 'judge-b': 4, 'judge-c': 4} | failed at: ['judges']
round 2: {'succeeded': 2} | requests: {'judge-a': 2, 'judge-b': 2, 'judge-c': 2} | skipped: 0
total requests: 22

final artifact: {'n_models': 3, 'qid': 'q1',
                 'verdicts': {'judge-a': '...', 'judge-b': '...', 'judge-c': '...'}}

the grouped shape re-sent verdicts that were already persisted: 4 of 6 requests in round 2
```

每部分是干嘛的：

* **`ask` 在同一个任务里发两次请求。** 多轮对话、工具循环，或者"重试解析直到过校验"的循环，都属于同一个任务内部。一个步骤，一个工件。
* **`fanout(...)` 把分支留在任务内部。** 三个评委在同一输入上并发跑，返回 `{task_name: value}`。子任务共享父任务的上下文，所以它们的租约和事件落在同一条记录里。Runner 只看到组 spec，所以只有所有子任务都一致时，才从子任务里提 `resource`、`algorithm`、`timeout_s`——这就是三个评委都声明同一个资源池和算法的原因。
* **按角色分资源池。** `models` 一个资源容量 4；`judges` 每个模型各一个资源容量 1，三个分支永远不会互相排队。
* **故障变成了数据。** `judge-b` 的 `RetryableError(error_class="upstream")` 会被分类，按组策略重试，记成一次失败，标清楚是哪个任务。
* **恢复会暴露它的代价。** 第 2 轮把整个 judges 组重跑了，所以 `judge-a` 和 `judge-c` 被再发了一遍——尽管它们的判定就在磁盘上——六次请求里四次是重复的。

最后一点是步骤要分支时你必须做的选择：

> 如果一次请求很贵或很慢，就给它单独一个任务。如果一个步骤是在一堆廉价调用上 fanout，一个任务加一个检查点才更划算。

把三个评委当三个任务（`C1 | C2 | C3`）检查点更细：同样的失败会在 `C2` 恢复，不重发任何已经成功的内容，代价是多两个流水线步骤。
[`examples/llm_eval/`](../../examples/llm_eval/README.zh-CN.md) 在同一批任务上实现了两种形态并做了对比——一个评审端点宕机时，分组形态重发了 2 个已成功的请求，拆分形态重发了 0 个。

---

## 第 14 步 —— 自定义类型、blob、插件

三个扩展点几乎覆盖一切：你的类型用编解码器、载荷放哪用工件后端、代码能从配置文件寻址用入口点插件。

```python
# tutorial/step_14_extending.py
"""Step 14 - your own types and your own blobs: a codec, a size comparison, and an artifact backend."""

import json
import struct
from pathlib import Path

from pyattacker import CodecRegistry, FileBackend, Runner, list_plugins, pipeline, task


class Embedding:
    """A type the built-in JSON codec would store as a fat list of floats."""

    def __init__(self, values: list[float]) -> None:
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    def __repr__(self) -> str:
        return f"Embedding({len(self.values)} floats)"


class EmbeddingCodec:
    """Four methods: a name, a claim on a type, and an encode/decode pair."""

    name = "f32"

    def can_encode(self, obj: object) -> bool:
        return isinstance(obj, Embedding)

    def dumps(self, obj: Embedding) -> bytes:
        return struct.pack(f"<{len(obj.values)}f", *obj.values)

    def loads(self, data: bytes) -> Embedding:
        return Embedding(list(struct.unpack(f"<{len(data) // 4}f", data)))


# A private registry: the same object must be given to the pipeline (for seeds) and to the Runner.
registry = CodecRegistry()
registry.register(EmbeddingCodec(), for_types=(Embedding,))


@task("embed")
def embed(seed: dict) -> Embedding:
    return Embedding([float(i) for i in range(seed["dim"])])


@task("norm")
def norm(vec: Embedding) -> dict:
    return {"dim": len(vec), "norm": round(sum(v * v for v in vec.values) ** 0.5, 3)}


template = pipeline("embedding", embed | norm, registry=registry)

with Runner(store="runs/step14.db", pools=[], concurrency=2, registry=registry,
            # Spill payloads of any size to content-addressed files instead of the database.
            artifact_backend=FileBackend(root="runs/blobs", min_bytes=0)) as runner:
    report = runner.run(template.map([{"dim": 384}]))
    print(report.summary())

    row = next(iter(runner.store.export_rows()))
    for item in row["artifacts"]:
        shown = str(item["payload"])[:36]
        print(f"  seq={item['seq']:>2} {item['task']:<10} type={item['type']:<10} codec={item['codec']:<5} "
              f"payload={shown + '...' if len(shown) == 36 else shown}")

    pipeline_id = row["pipeline_id"]
    vector = runner.store.get_artifact(pipeline_id, 0)
    print("built-in json would need", len(json.dumps([float(i) for i in range(384)])), "bytes;",
          "the f32 codec used", vector.size, "bytes")
    print("the store kept only a reference:", f"blobs/{vector.digest[:2]}/{vector.digest[2:]}",
          "- the payload is not in the database")
    print("restored from the blob:", type(registry.load(vector.encoded())).__name__,
          registry.load(vector.encoded()))

files = [p for p in sorted(Path("runs/blobs").rglob("*")) if p.is_file()]
print("blobs on disk:", len(files), "files,", sorted(p.stat().st_size for p in files), "bytes")
print("installed plugins:", len(list_plugins()),
      "(entry points; see examples/plugin_package for a complete one)")
```

```text
run run-... status=completed  wall=0.01s
  pipelines: total=1 succeeded=1
  ...
  seq=-1 __seed__   type=dict       codec=json  payload={'dim': 384}
  seq= 0 embed      type=Embedding  codec=f32   payload=AAAAAAAAgD8AAABAAABAQAAAgEAAAKBAAADA...
  seq= 1 norm       type=dict       codec=json  payload={'dim': 384, 'norm': 4335.978}
built-in json would need 2578 bytes; the f32 codec used 1536 bytes
the store kept only a reference: blobs/15/0b9b3a521bb02c0f88eab2716c9d61 - the payload is not in the database
restored from the blob: Embedding Embedding(384 floats)
blobs on disk: 3 files, [11, 27, 1536] bytes
```

* **编解码器**要四个成员：`name`、`can_encode(obj)`、`dumps(obj) -> bytes`、`loads(bytes) -> obj`。用 `registry.register(codec, for_types=(MyType,))` 注册，把*同一个*注册表传给用于种子的 `pipeline(..., registry=...)` 和用于工件的 `Runner(registry=...)`。认领某类载荷的编解码器压过内置的 JSON 兜底。
* **工件后端**决定载荷字节放哪：`None`/`"inline"` 留在数据库里，`"null"` 丢掉字节但留摘要，`"file:///data/blobs"`（或 `{"kind": "file", "root": ..., "min_bytes": 262144}`）溢到内容寻址的文件里，读取时再水合回来。上面的 `min_bytes=0` 什么都溢出去，所以工件行显示的是引用而不是字节。
* **插件**是 `importlib.metadata` 入口点，分四组：`pyattacker.tasks`、`pyattacker.algorithms`、`pyattacker.codecs` 和 `pyattacker.stores`（按 URI scheme 索引，`store = "s3://bucket/runs.db"` 就能用）。内置项先解析，导入时报错的插件记下来不致命——`pyattacker plugins` 两者都列。完整示例包在 [`examples/plugin_package/`](../../examples/plugin_package/README.zh-CN.md)。

**监控正在跑的运行。** `pyattacker watch runs/qa.db` 让你从第二个进程看终端视图，`pyattacker serve runs/qa.db` 提供 HTTP 仪表盘，在 `/stats`、`/events`、`/pipelines`、`/resources` 和 `/errors` 提供 JSON。两个都开只读连接，能和正在跑的任务并排用。HTTP 端点没认证，会把你的载荷给出来——只绑回环地址。
* **监控**：`pyattacker serve runs/qa.db` 是零依赖的只读 HTTP 视图（`/`、
  `/stats`、`/events`、`/pipelines`、`/resources`、`/errors`），每个请求开个新连接，和正在跑的任务并行无碍。绑回环地址、没认证——当调试视图用，别当仪表盘用。

---

## 第 15 步 —— 高级：跳过站点（交接）

**这是框架里唯一的进阶特性：可选启用，改执行模型，1.0 之前都标实验性。** 这步之前的东西都不依赖它，没声明它的流水线和这个特性不存在时完全一样。当你的某个步骤判断*链的其余部分不用跑了*，再读这步。

场景是这样：`judge` 能看出某个答案已经够好了，或者 `metrics` 这一步对这一行没必要。以前的选择是要么照常跑完剩余任务，要么用 `fanout` 把所有东西折进一个任务（丢了逐步记录），要么抛错——抛错会把流水线记成 **failed**，这是在撒谎。**交接**（handoff）说清楚实际发生了什么：这一行跳过了站点 3–5，从站点 6 继续，或者就在这里结束。

```python
# tutorial/step_15_handoff.py
"""Step 15 (advanced) - a task hands off: skip stations, or finish the pipeline, and nothing lies about it."""

import json
from contextlib import closing

from pyattacker import Handoff, Runner, open_store, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"q": seed["q"], "confidence": seed["confidence"]}


@task("ask")
async def ask(row: dict, ctx) -> dict:
    # <- your HTTP call: the model answers, and reports how sure it is
    await ctx.clock.sleep(0.001)
    return {**row, "answer": f"answer-for:{row['q']}"}


@task("judge")
def judge(row: dict) -> Handoff | dict:
    """Three outcomes: finish here, skip metrics, or carry on down the chain."""
    if row["confidence"] >= 0.9:
        # already good enough: end the pipeline with this as its final artifact
        return Handoff.end({"q": row["q"], "answer": row["answer"], "verdict": "confident"},
                           reason="already good enough")
    if row["confidence"] >= 0.5:
        # not worth the metrics call, but the report is still wanted: continue at "report"
        return Handoff.to("report", {"q": row["q"], "answer": row["answer"], "verdict": "ok"},
                          reason="metrics not needed")
    return {**row, "verdict": "needs-metrics"}


@task("metrics")
def metrics(row: dict) -> dict:
    return {**row, "score": round(row["confidence"] * 10, 1)}


@task("report")
def report(row: dict) -> dict:
    return {**row, "reported": True}


# The edges are declared, not derived: "report" is what judge may jump to, and END finishes the pipeline.
template = pipeline(
    "qa",
    prepare | ask | judge | metrics | report,
    control={"edges": {"judge": ["report", "end"]}},
)

rows = [
    {"q": "capital of France", "confidence": 0.95},   # judge ends the pipeline (metrics and report skipped)
    {"q": "17 * 23", "confidence": 0.6},              # judge skips metrics, continues at report
    {"q": "prove sqrt(2) is irrational", "confidence": 0.2},  # judge hands nothing off: the full chain runs
]

with Runner(store="runs/handoff.db", concurrency=4) as runner:
    report_obj = runner.run(template.map(rows))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        ran = [t.name for t in store.tasks(record.pipeline_id)]
        skipped = [t.name for t in template.tasks if t.name not in ran]
        print(f"\n{record.pipeline_id[:8]}  state={record.state}  ran={ran}  skipped={skipped}")
        for hop in store.handoffs(pipeline_id=record.pipeline_id):
            where = "END" if hop.to_seq is None else hop.to_task
            print(f"   jumped: {hop.from_task} -> {where}  ({hop.reason})  entry={hop.entry_artifact_id}")

# The record is a plain store: reopen it whenever, and read the same ledger back.
with closing(open_store("runs/handoff.db")) as reopened:
    print("\nhandoffs recorded:", reopened.stats()["handoffs_total"])
    row = next(iter(reopened.export_rows()))
    print("first exported pipeline's ledger:", json.dumps(row["handoffs"], ensure_ascii=False))
```

三行数据走了三条不同的路，记录如实说了，而且它们都没失败：

```text
ec5fc1e5  state=succeeded  ran=['prepare', 'ask', 'judge']  skipped=['metrics', 'report']
   jumped: judge -> END  (already good enough)  entry=ec5fc1e5dff6a260512e579a276d11e0:5
cdce72ca  state=succeeded  ran=['prepare', 'ask', 'judge', 'report']  skipped=['metrics']
   jumped: judge -> report  (metrics not needed)  entry=cdce72cae311e870d87d85e9dbcaa280:5
260cf635  state=succeeded  ran=['prepare', 'ask', 'judge', 'metrics', 'report']  skipped=[]
```

用之前有几点值得知道：

* **它是返回值，不是异常。** 重试策略永远看不到它，任务侧的 `except Exception:` 吞不掉它，`async with ctx.acquire(...)` 退出时已经还了租约。被取消或超时的尝试走不到返回那行，所以不会有半途转移的状态。
* **边是声明出来的，写错就很醒目。** 从没有 `control` 块的流水线返回 `Handoff`，或者沿着一条不是*从这个任务*声明的边返回，都会触发 `FatalError`——永不重试，也不会静默跳转。目标必须严格晚于源（`edges` 只允许向前），链里出现两次的名字必须给 seq，最后一个任务发 `end` 会被拒——因为它什么都不做。
* **交接就是检查点，所以恢复从目标处继续。** 进程在跳转后死了，下次 `resume=True` 会带着记下来的入口状态从目标开始，**不会**重跑源任务。`handoffs` 行让这事可能：它记了目标和入口工件，入口工件要么是源任务收到的那个（不带值的 `Handoff.to(target)`），要么是存在链之上自己地址处的新载荷（`seq >= n_tasks`）。
* **`n_tasks_done` 变成了一个位置。** 跳过的槽位没有任务行，所以开了 control 的流水线上，`n_tasks_done / n_tasks_total` 不是完成百分比——要看实际发生了什么，靠 `store.handoffs()` 和 `stats()["handoffs_total"]`。`report`、`watch`、`/pipelines` 和每种导出都会在旁边显示交接计数。
* **别一上来就用它。** 交接是关于当前流水线的*调度声明*："这一行应该从那边继续"。条件还是写在任务代码里，步骤内分支还是 `fanout`，遍历数据集还是 `map`。一条几乎全是交接的流水线说明这个问题要的是图引擎，这个框架不是。
* **进阶层级。** 可选启用、改执行模型、1.0 前实验性：上面这些保证是稳定的，写法还可能变。单独声明的反向操作用访问和有限预算；见[第 16 步](tutorial.md#第-16-步--高级用回退和全部重试重新生成)和 [reference → 进阶：反向遍历](reference.md#进阶反向遍历rewindretry-allvisits)。两个内置存储都能提交交接；不能提交的自定义存储一开始就被拒，抛 `ConfigError`，不会写一次崩溃后活不下来的跳转。

整条流水线重启（显式 `fresh_restart=True`、在 `retry_succeeded=True` 下已经成功的流水线，或入口载荷丢失）会把之前的交接、尝试和事件留作历史，但在从 seq 0 重新开始之前，连同持久账本水位一起原子地清掉当前的任务/链工件状态。之后失败没法恢复旧的跳转，也找不回旧的 END 结果。后续的目标恢复保留当前水位，反复中断时还在用生效中的交接。完成时留下一个最终工件。实现后端时见
[存储恢复契约](reference.md#表与读取器)。

---

## 第 16 步 —— 高级：用回退和全部重试重新生成

**和第 15 步一样，进阶、可选启用、1.0 前实验性。** 它改了链的遍历方式：校验器可以把工作*送回*更早的站点，而不是让这一行失败，或者在一个任务内部循环。当某个步骤判断更早的步骤该带着不同状态重新跑，再读这步。

场景是这样：`validate` 拒了一个结构化答案，修复方式是带着同样的提示词再加错误反馈重新生成一次。把这个循环折进一个任务，会把生成和校验压成一条记录——"这一行需要重新生成几次"恰恰会在它本身就是度量指标的地方变得不可见。
**回退**（rewind）会把两次生成保留成彼此独立的访问，各自有自己的任务、尝试和工件行。

```python
# tutorial/step_16_backward.py
"""Step 16 (advanced) - a validator rewinds to the generator; state is what the author chose."""

from pyattacker import Handoff, Runner, pipeline, task


@task("prepare")
def prepare(seed: dict) -> dict:
    return {"prompt": seed["prompt"], "temperature": 0.2}


@task("generate")
def generate(state: dict, ctx) -> dict:
    # <- your model call: a revisit is a genuinely new sample
    return {**state, "answer": f"sample-{ctx.visit}", "valid": ctx.visit >= 1}


@task("validate")
def validate(row: dict, ctx) -> Handoff | dict:
    if not row["valid"]:
        # Explicit state: the author decides what the generator gets, the framework guesses nothing.
        return Handoff.rewind(
            "generate",
            {"prompt": row["prompt"], "temperature": 0.7, "feedback": "answer did not parse"},
            reason="invalid structured output",
        )
    return {**row, "validated": True}


@task("report")
def report(row: dict, ctx) -> dict:
    # ctx.visit counts entries into *this* station, and report ran once — so it is 0 here. How many
    # regenerations the row needed is the generate station's counter, printed from the store below.
    return {"answer": row["answer"], "temperature": row["temperature"]}


# `rewind` is declared from validate to the strictly earlier generate; max_handoffs is required and is
# the loop budget: transfer N+1 fails before it can invalidate anything.
template = pipeline(
    "regenerate",
    prepare | generate | validate | report,
    control={"rewind": {"validate": ["generate"]}, "max_handoffs": 3},
)

with Runner(store="runs/backward.db", max_handoffs=10) as runner:   # 10 is a ceiling, not a floor
    report_obj = runner.run(template.map([{"prompt": "Return JSON with one field"}]))
    print(report_obj.summary())
    store = runner.store
    for record in store.pipelines():
        print(f"\n{record.pipeline_id[:8]}  state={record.state}  position={record.n_tasks_done}")
        for row in store.tasks(record.pipeline_id):
            print(f"   seq={row.seq} visit={row.visit} {row.name:9s} {row.state}")
        for hop in store.handoffs(pipeline_id=record.pipeline_id):
            print(f"   {hop.operation}: {hop.from_task} -> {hop.to_task}  (visit {hop.from_visit} -> {hop.to_visit})")
    state = store.visit_state(record.pipeline_id)
    print("\nbudget consumed:", state["handoffs"], " visits per station:", state["counters"])
    print("effective outputs:", {s: store.get_artifact(record.pipeline_id, s).visit for s in range(4)})
    # Every occurrence is still there by exact id, including the superseded first generation.
    print("first generation kept:", store.get_artifact_by_id(f"{record.pipeline_id}:1").payload)
```

这一行被生成了两次，记录显示两次访问，不是把重试藏起来：

```text
succeeded  position=4
   seq=0 visit=0 prepare   succeeded
   seq=1 visit=0 generate  succeeded
   seq=1 visit=1 generate  succeeded
   seq=2 visit=0 validate  handed_off
   seq=2 visit=1 validate  succeeded
   seq=3 visit=0 report    succeeded
   rewind: validate -> generate  (visit 0 -> 1)

budget consumed: 1  visits per station: {'0': 0, '1': 1, '2': 1, '3': 0}
effective outputs: {0: 0, 1: 1, 2: 1, 3: 0}
```

用之前有几点值得知道：

* **状态你选；框架不会回滚字典。** `Handoff.rewind(target, value)` 要求显式给值（`None` 也是真实的值），目标必须是已声明的、严格更早的任务。目标之前的结果保持生效；目标及其之后的一切变成历史，以新访问重新跑。
* **`Handoff.retry_all()` 从最初绑定的种子重新开始**——那个种子是绑定时捕获的字节重新解出来的——之后改 `spec.seed` 或某个任务的输入都不会变它。重启想以不同状态开始，就用 `Handoff.rewind(0, chosen_state)`。
* **循环有界，所以终止不是结构性的。** 反向计划必须给 `control.max_handoffs`，`RunConfig.max_handoffs`（默认 1000，配置文件里 `run.max_handoffs`）可以调低：生效的限制是两者较小值。`END` 永不消耗一次转移。失败的转移在任何东西失效*之前*被拒，所以记录还是精确描述已提交的内容。
* **用完预算怎么脱身、怎么脱离 `running` 行，都是显式操作。** `resume=True` 保留已消耗的预算继续当前遍历，所以一条*因为*转移次数用完而失败的流水线需要 `fresh_restart=True` / `--fresh-restart`：新预算生命周期、从绑定种子重新开始，访问计数器和审计行都保留。还显示 `running` 的行——硬杀进程留下的形态——只有 `resume=True` 才会认领它；不带它，运行跳过这行，不分叉一条可能还归另一次运行所有的遍历。
* **崩溃丢不了谱系。** 入口、访问分配、生效槽位和待处理输入原子地一起提交，所以恢复带着所选载荷继续同一次访问，不重放整条流水线。框架给不了的是外部副作用的恰好一次语义——每次重新生成都该是一次新的外部操作时，把 `ctx.visit` 放进幂等键。
* **访问 0 不变。** 从不回退的流水线保留 `pipeline_id:seq` 形式的 id、同样的随机流和同样的 `spec_digest`，所以让流水线换地址的是添加声明这个动作，不是升级包。
* **普通字典还是普通字典。** `HistoryArtifact` 是可选的载荷基类，做快照/恢复的簿记；运行器里没有任何东西会读它来决定下一步去哪。

完整接口——访问/发生实例（occurrence）模型、预算生命周期、存储能力和
`HistoryArtifact` 编解码器——见 [reference → 进阶：反向遍历](reference.md#进阶反向遍历rewindretry-allvisits)，可选的载荷历史在下一步。

---

## 第 17 步 —— 高级：让载荷自带历史

**同样是高级、可选启用，而且完全不改调度。** 第 15、16 步决定流水线*去哪里*；这步是回退的可选搭档，用于你送回的那个状态本身需要带具名检查点的场合。

场景：`validate` 想把 `generate` 送回**那个失败样本之前**的状态，而不是作者手搓的字典。手搓那个字典很常见，第 16 步就是这么做的，本身没任何问题。但当载荷*本身*就是应用的状态机——它有若干阶段，早先阶段值得留着，"从阶段 2 重新生成，同时让阶段 3 留在记录里"才是你要的操作——`HistoryArtifact` 就是这个可选的载荷基类：它带着应用状态的分离快照，以及对应的版本化编解码器。

```python
# tutorial/step_17_history.py
"""Step 17 (advanced) - optional payload history: named checkpoints carried inside the payload."""

from pyattacker import CodecRegistry, Handoff, HistoryArtifact, Runner, pipeline, task


class DraftState(HistoryArtifact):
    """Application state with its own snapshots; the runner stores it and never reads it."""


@task("prepare")
def prepare(seed: dict) -> DraftState:
    state = DraftState({"prompt": seed["prompt"], "temperature": 0.2})
    return state.checkpoint("prepared")


@task("generate")
def generate(state: DraftState, ctx) -> DraftState:
    # <- your model call: a revisit is a genuinely new sample, so the label names the visit
    draft = state.with_state({**state.state, "answer": f"sample-{ctx.visit}", "valid": ctx.visit >= 1})
    return draft.checkpoint(f"sample-{ctx.visit}", metadata={"temperature": draft.state["temperature"]})


@task("validate")
def validate(state: DraftState, ctx) -> Handoff | DraftState:
    if not state.state["valid"]:
        # Go back to the snapshot taken before this sample, then choose what the generator sees next.
        base = state.restore("prepared")
        return Handoff.rewind(
            "generate",
            base.with_state({**base.state, "temperature": 0.7}),
            reason="answer did not parse",
        )
    return state


@task("report")
def report(state: DraftState, ctx) -> dict:
    # The decoded payload still carries every snapshot, in order, plus the one that is selected.
    return {
        "answer": state.state["answer"],
        "labels": [row["label"] for row in state.history],
        "selected": state.selected,
    }


registry = CodecRegistry()
registry.register_type(DraftState)   # decoding restores DraftState, not a bare HistoryArtifact

# The same registry goes to the template (for seeds) and to the Runner (for checkpoints) - Step 14's rule.
template = pipeline(
    "draft",
    prepare | generate | validate | report,
    registry=registry,
    control={"rewind": {"validate": ["generate"]}, "max_handoffs": 3},
)

with Runner(store=":memory:", registry=registry) as runner:
    report_obj = runner.run(template.map([{"prompt": "Return JSON with one field"}]))
    print(report_obj.summary())
    store = runner.store
    record = next(iter(store.pipelines()))
    for row in store.tasks(record.pipeline_id):
        print(f"   seq={row.seq} visit={row.visit} {row.name:9s} {row.state}")
    validate_output = runner.registry.load(store.get_artifact(record.pipeline_id, 2).encoded())
    print("\nfinal artifact:", runner.registry.load(store.get_artifact(record.pipeline_id, 3).encoded()))
    print("snapshots:", [row["label"] for row in validate_output.history])
    print("selected:", validate_output.selected)
    pruned = validate_output.prune("sample-0")   # explicit, and never the selected snapshot
    print("after pruning sample-0:", [row["label"] for row in pruned.history])
    try:
        pruned.prune("prepared")
    except ValueError as exc:
        print("prune refused:", exc)
```

遍历过程和第 16 步一样；新的一点是载荷自己记住了它走过的位置：

```text
   seq=0 visit=0 prepare   succeeded
   seq=1 visit=0 generate  succeeded
   seq=1 visit=1 generate  succeeded
   seq=2 visit=0 validate  handed_off
   seq=2 visit=1 validate  succeeded
   seq=3 visit=0 report    succeeded

final artifact: {'answer': 'sample-1', 'labels': ['prepared', 'sample-0', 'sample-1'], 'selected': 'snapshot:0'}
snapshots: ['prepared', 'sample-0', 'sample-1']
selected: snapshot:0
after pruning sample-0: ['prepared', 'sample-1']
prune refused: cannot prune the selected snapshot
```

动手之前值得知道的事：

* **它是载荷，不是记录。** `HistoryArtifact` 是解码后的应用值；它*不是*持久化的 `Artifact` 行，快照历史也绝不取代框架的执行账本、任务行或访问记录。runner 里没有任何代码会读它来决定下一步去哪——你返回的 `Handoff.rewind` 仍然是唯一移动流水线的东西。普通字典还是普通字典：任务进入或完成时不会自动生成快照。
* **快照是分离的，嵌套修改改不了过去。** `state`、`history` 和 `snapshot(...)` 返回的都是深拷贝；`checkpoint(label, *, metadata=None)` 追加一个带稳定 id（`snapshot:0`、`snapshot:1`……）和唯一标签的快照，`snapshot:` 前缀留给这些 id。`with_state(value)` 替换当前状态但*不*追加快照；`restore(id_or_label)` 替换当前状态*并且*记 `selected`，同时保留整段历史，让后续阶段仍可检视；`prune(*selectors)` 是显式丢快照的方式——选择器指向被选中的快照时它抛 `ValueError` 不照做，id 不会重用。
* **持久化是提交的事，不是 `checkpoint()` 的事。** 在任务里调 `checkpoint()` 不碰存储。runner 在提交任务输出或控制转移时把载荷（连同历史）一起持久化，和其他工件完全一样。那次提交之前崩溃，内存里的快照就丢了，编解码器帮不上忙。
* **编解码器需要那个类。** 状态和 metadata 必须可 JSON 序列化，版本化的 `history-v1` 编解码器恢复的不只是快照，还有已注册的子类：把同一个 `CodecRegistry` 交给 `pipeline(..., registry=...)` 和 `Runner(..., registry=...)`（第 14 步），用 `register_type` 注册子类——未注册的子类明确解码失败，不会以 `HistoryArtifact` 身份回来。子类继承基类构造函数，应用字段放 `state` 里；自定义构造函数和额外属性不在这个接口范围。
* **历史随载荷一起长。** 每个快照都完整保留，长期存活的值的体积会随快照数量和大小增长；要有意识地 prune。"把这一行送回去"这种用法，常见形态是每次访问留一个快照，外加你想回到的那些状态——上面的程序就是这么做的。

---

## 速查表

| 我想…… | 这样做 |
|---|---|
| 在一个数据集上跑一个任务 | `Runner(store=..., pools=[...]).run(template.map(rows))` |
| 每行 k 个样本 | `template.map(rows, repeats=k)` |
| 从我的数据集生成稳定 id | `template.map(rows, key_of=lambda r: r["qid"])` |
| 不碰磁盘地测试 | `Runner(store=":memory:")` |
| 查看发生了什么 | `report.summary()`、`report.to_dict()`、`store.errors()` |
| 查看为什么慢 | `store.attempts(pipeline_id=...)` → `duration_ms`、`decision`、`leases` |
| 崩溃后恢复 | `runner.run(specs, resume=True)` 或 `pyattacker resume -c cfg.yaml` |
| 重跑我不信任的结果 | `retry_succeeded=True` / `--retry-succeeded` |
| 从种子重新开始一条流水线，保留审计行 | `fresh_restart=True` / `--fresh-restart` |
| 限制影响范围 | `stop_after_failures=N`、`stop_after_s=T`、`--limit N` |
| 限制每个端点的并发 | `Resource.create(..., capacity=N)` |
| 快速失败不排队 | `algorithm="immediate"` + `Retrying(retry_unknown=True)` |
| 扛过 429 | `raise RetryableError(..., error_class="rate_limit", retry_after=...)` |
| 把大载荷存到数据库之外 | `--artifact-backend file:///data/blobs` |
| 用四个进程 | `--shards 4 --jobs 4`，然后对分片文件跑 `report`/`export` |
| 在步骤内分支 | `fanout(task_a, task_b)` |
| 有记录地向前跳 / 提前结束 | 在声明了 `control={"edges": {...}}` 的流水线上 `return Handoff.to("report", v)` / `Handoff.end(v)` |
| 把一个站点送回更早的站点（高级） | 在声明了 `control={"rewind": {...}, "max_handoffs": N}` 的流水线上 `return Handoff.rewind("generate", chosen_state)` |
| 让整条流水线从自己的种子重新开始（高级） | 在声明了 `control={"retry_all": [...], "max_handoffs": N}` 的流水线上 `return Handoff.retry_all()` |
| 在载荷内部保留具名状态检查点（高级） | `class S(HistoryArtifact)`，然后 `s.checkpoint("label")` / `.restore(...)` / `.prune(...)`，并在两个注册表里登记 |
| 让我的代码能从 YAML 用 | 不用插件：`use: my_pkg.tasks:my_task`；在 `pyattacker.tasks` 有入口点：`use: my_task` |

所有类和函数，含签名和参数表：[`docs/reference.md`](reference.md)。

## 疑难排查

**"我的任务接收三个参数。"** 任务是一元的：`(value)` 或 `(value, ctx)`。额外状态放进工厂闭包（`def make_task(model): @task(...) async def t(value, ctx): ...; return t`）——第 6 步用的就是这个模式。

**"恢复重跑了整条流水线。"** 存储几乎肯定是用 `journal: summary` 写的，留了摘要没留载荷，检查点解不出来。查 `pipeline.checkpoint_missing` 事件。用 `journal: full`（默认值）。

**"什么都没跑，也没有任何行。"** 每条流水线都因为已经成功被跳过了——看 `report.skipped`。这是预期行为，第二次完全相同的运行也这样。

**"一条流水线以 `unknown` 失败了。"** 这个异常没法归类（比如 `ResourceUnavailable`，或者裸的 `Exception`）。要么映射它——`raise RetryableError(...)`——要么在确实想重试任何东西时设 `retry_unknown=True`。

**"它挂住了，没输出。"** 要么是选择器匹配不到任何资源而算法是 `wait`；要么是一个任务占了某个资源池的租约，又向同一个资源池申请另一个（查 `acquire.suspected_deadlock` 事件）；要么是你自己代码里某个 `await` 永不返回。任务上的 `timeout_s=` 和 acquire 回合上的 `timeout=` 都会变成错误。

**"我的资源永远不回来。"** 它会回来的——查 `leases_leaked` 和 `lease.leaked` 事件：你用了 `await ctx.acquire_lease()` 没还。用 `async with`，CI 里开 `strict_leases=True` 让这种情况醒目地失败。

**"存储文件被占用 / 一个进程不够用。"** SQLite 只允许一个写入者。分片到几个各自有存储的进程（第 12 步），别让多次运行指向同一个文件。

**"我的工件字节去哪了？"** 设了 `journal: summary` 或 `null` 后端，就只留摘要。否则查 `blob_ref`：载荷可能在文件后端里，读取时透明水合。

**"一次运行在重试退避里花了 30 秒，我丢了并发。"** 你没丢：这条流水线挂在延迟队列里，worker 去做了别的。`runner.stats()["delayed_pipelines"]` 显示当前挂着多少条。

**"反向流水线在建流水线时就失败，或者跑着跑着预算就耗尽了。"** 只要声明了 `rewind` 或 `retry_all`，`control.max_handoffs` 就是必需的，必须是正整数，所以 `0`、`"3"`、`3.0` 和 `True` 都会被拒。每次非终止转移消耗它，跨 resume 保留，所以一条*因为*预算耗尽而失败的流水线需要 `fresh_restart=True` / `--fresh-restart`：只写 `resume=True` 只会重放同一个致命错误。硬杀后还标 `running` 的行只有 `resume=True` 才会被认领；否则这次运行跳过它，不分叉一条遍历出去。

**"`Handoff.rewind` 抛 `FatalError`。"** 目标必须是已声明的、严格更早的任务，用唯一任务名或它的 seq 指定。自回退、`end`、流水线没声明的目的地都是编写错误，所以是致命错误、永不重试；`retry_all` 同样需要它的源出现在 `control.retry_all` 中，不接受任何值。

## 接下来该看什么

想找某个特定功能而不是整篇文档？见开头附近的
[找到你需要的内容](tutorial.md#找到你需要的内容)。

| 资源 | 里面有什么 |
|---|---|
| [`docs/reference.md`](reference.md) | 每个公开类和函数：签名、参数、示例 |
| [`docs/cli.md`](cli.md) | 每个子命令和 flag、退出码、配置文件参考 |
| [`docs/design.md`](design.md) | 概念模型、六条不变量、租约契约、数据模型、权衡 |
| [`README.md`](../../README.zh-CN.md) | 精简导览：调度保证、分片、插件、不在范围内的内容 |
| [`examples/quickstart.py`](../../examples/quickstart.py) | 60 行讲完 SDK，含一轮恢复 |
| [`examples/llm_eval/`](../../examples/llm_eval/README.zh-CN.md) | 完整评测、两种流水线形态、实测的检查点粒度 |
| [`examples/sharded.py`](../../examples/sharded.py) | 一个数据集分布在 N 个存储上，然后合并报告 |
| [`examples/plugin_package/`](../../examples/plugin_package/README.zh-CN.md) | 可安装的插件：任务、一个算法、一个编解码器 |
| [`examples/qa_eval.yaml`](../../examples/qa_eval.yaml) | 声明式路径，端到端 |
