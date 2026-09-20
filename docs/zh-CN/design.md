# pyattacker 设计文档

[English](../design.md) | **简体中文**

> 版本：0.3.1——M0–M5 已完成，含 §4.8 那套可选启用的高级控制流（正向交接与反向遍历），1.0 之前标实验性。
> 见第 9 节"已实现 / 留待以后"。
> 一句话概括：**一个以 artifact 为中心的异步任务编排框架，用 pipeline 作为完成单位，把 resource pool 作为唯一的共享面。**
> 它不碰网络、不做归约、也不做 DAG 调度——只管一件事："把几万条彼此独立的 pipeline 可靠、可恢复、可观测地跑到完成"。

---

## 1. 范围与边界

| 框架负责的 | 框架不负责的 |
|---|---|
| 任务编排、并发控制、资源租用与回收 | HTTP 请求、认证、SSE 解析（openai/anthropic 协议你自己写） |
| artifact 持久化（产出即落盘）与任务级检查点 | 语义归约（accuracy / F1 / pass@k——任何跨 pipeline 聚合） |
| 失败分类、重试决策、退避策略、熔断 | 数据集下载与清洗（它只接受可迭代的种子流） |
| 结构化日志、运行清单、实时监控、导出 | 多轮 agent 状态机、DAG 依赖调度、服务化 / 网关代理 |
| 资源池发布/订阅、健康与配额核算 | 分布式调度（跨进程分片属于后续增量） |

**"不做归约"的精确边界**：框架会算*运行统计*（流水线数量、成功率、延迟分布、错误分类、资源池利用率），但绝不会对 artifact 做*语义聚合*。要 accuracy 有两条路，都不需要框架介入：

1. 导出 artifact（`export` / `report.export_jsonl`），在外面自己算；
2. 写一条**汇聚流水线**（sink pipeline）：用一个任务把结果发到某个资源/总线上，订阅者消费——归约就变成你用框架原语自己拼的东西。

应用还可以通过 `Runner.report_metric()` 汇报自己算好的最新值。这只是传输和展示，不改变
语义归约的边界。数值按运行（可选按流水线）划分作用域，同步覆盖写入，并由只读的
`/metrics` 端点读取。Runner 的存储连接仍是唯一写入者。

---

## 2. 核心不变量

这六条是设计的地基，任何改动都不能破：

1. **流水线之间零语义耦合。** 唯一的共享面是资源池（包括总线信号）。所以并发很简单：每条流水线一个协程，没有全局依赖图。
2. **任务 = 一元 `(Artifact) -> Artifact` 函数**，线性串起来，不分支、不汇合。需要 fanout/循环/批处理时，任务自己在内部 `asyncio.gather`。任务也可以做**交接（handoff）**（§4.8，高级特性，需显式开启）：链仍然是一串没有汇合的一元任务，任务字面上还是 `(Artifact) -> Artifact | Handoff`；变的只是*遍历顺序*——沿声明的正向边走，不是拓扑或 artifact 契约变了。
3. **artifact 一产出就立即落盘**，所以**检查点粒度 = 任务**，不是流水线。
4. **失败就是异常**，不搞状态机。重试有两条正交的线：
   任务级（用同样的输入 artifact 重放）和流水线级（整条重跑）。
5. **资源只能通过租约使用，用完必须还**；任务结束时绝不占着资源（见 §4.2）。
6. 持久化层只写**事实**（artifact / 尝试 / 事件），绝不写语义指标。

---

## 3. 概念模型

```
        seed (one row of the dataset)
              │
              ▼
   ┌──── pipeline (unit of completion and recovery; semantically independent of each other) ────┐
   │                                                                                            │
   │   task A ──artifact──▶ task B ──artifact──▶ task C ──artifact──▶ task D ──▶ final artifact │
   │   (fetch)                  (async request)      (async evaluate) (compute metrics)         │
   │                             │                     │                                        │
   └─────────────────────────────┼─────────────────────┼────────────────────────────────────────┘
                                 │ acquire/lease       │
                                 ▼                     ▼
                        ┌───────── resource pool (the only shared surface) ──────────┐
                        │  resource#1  resource#2  resource#3 …                      │
                        │  state: ready/degraded/dead/revoked  health/quota stats    │
                        │  event stream: published/leased/degraded/recovered/revoked…│
                        └────────────────────────────────────────────────────────────┘
                                 ▲                     │
                                 │ publish/subscribe   │ algorithm decides "how to wait, how long, which pool to switch to"
```

| 概念 | 定义 | 代码 |
|---|---|---|
| **artifact** | 任务持久化后的状态。内容寻址（blake2b），一产出就落盘；`seq=-1` 是流水线的种子输入 | `artifact.py` |
| **task** | 一元 `(artifact) -> artifact` 函数；可以带 `resource` / `algorithm` / `retry` / `timeout_s` | `task.py` |
| **pipeline** | 任务的线性链，是**完成**和**恢复**的单位；`map(seeds)` 把它展开成互相独立的实例 | `pipeline.py` |
| **resource** | 一类可租用的外部能力（一个端点 / 一把 key / 一个本地 worker） | `resource.py` |
| **pool** | 一组资源 + 一个默认获取算法 + 一份状态事件日志 | `resource.py` |
| **lease** | 一次租约；`lease.client` 是工厂造出来的可用对象；用完必须还 | `resource.py` |
| **algorithm** | "怎么从资源池里拿一个资源"的策略（拉取侧） | `algorithm.py` |
| **bus** | 轻量的跨流水线信号总线（推送侧） | `resource.py` |

### 3.1 流水线身份（identity）是内容寻址的

```
pipeline_key = blake2b(canonical_json({
    spec_digest,          # v2 task-chain fingerprint: config/parameters/children/policies + source digest
    seed_digest,          # digest of the seed contents (that row of the dataset)
    repeat,               # which sample of pass@k this is
}))
```

三个直接推论：

* **幂等**：重跑同一条样本不会多出一条流水线，`resume` 只会跳过已成功的。
* **可复现**：`spec_digest` 包含每个任务的**源码摘要**，所以改了任务代码等于换了流水线，旧检查点不会被错用（`include_code=False` 可以关这个行为）。
* **白送 pass@k**：`template.map(seeds, repeats=3)` 一下展开成三条独立流水线，共享同一个种子摘要。

开了交接的流水线（§4.8），它的 `control` 块**只有存在时**才算指纹的一部分：没有 control 的流水线，摘要和以前完全一样——还是那份任务指纹列表——所以这个特性不会让任何已存的摘要、检查点或分片分配失效（`v3:` 迁移才会）。

规格（spec）指纹带 `v2:` 版本前缀。手动指定的 key 保留你给的标识，但已存的 spec/seed 摘要必须匹配才会跳过或恢复。旧存储还能读；默认 ID 会变，手动传的旧 key 会冲突。闭包、全局变量、端点选项和资源池默认值都要求在 config/version 里声明。迁移和外部副作用幂等性见 [恢复同一性](reference.md#恢复同一性)；检查点不保证恰好一次调用。

---

## 4. 关键机制

### 4.1 任务级检查点与恢复

每个任务成功后，按顺序做三件事：

1. `store.put_artifact(...)`——artifact 落盘（内容寻址 + 去重）；
2. `store.record_task(...)`——记任务的最终状态、耗时、错误、用到的租约；
3. 检查点游标往前推——中间任务执行 `store.upsert_pipeline(record.n_tasks_done = seq + 1)`，最后一个任务执行终结性的 `store.finish_pipeline(..., n_tasks_done = n_tasks)`。

最后一个任务的顺序是**先标 final，再写终结状态**：`mark_final` 先跑，`finish_pipeline` 后跑，游标前进是*跟着* `finish_pipeline` 走的，不是单独写的。这两个顺序都重要，各自覆盖对方修不了的撕裂写：终结写完了、`mark_final` 之前崩了，会留一条永远 `succeeded` 的流水线（永远被跳过），最终 artifact 从没被标记；终结之前崩了，代价只是重跑最后一个任务——也就是文档说的至少一次（at-least-once）边界。

恢复算法（`Runner._open_pipeline` / `Runner._drive`）：

```
resume(spec):
    rec = store.get_pipeline(spec.pipeline_id)
    if rec exists and (rec.spec_digest != spec.spec_digest or rec.seed_digest != spec.seed_digest):
        → PipelineIdentityConflict; preserve existing pipeline and checkpoint
    if rec.state == succeeded and not retry_succeeded:  → skip (counted as skipped)
    if rec.state in (failed, interrupted) and spec declares control and the newest active handoff row is an END:
        → mark_final(entry artifact) + settle succeeded   # before the linear rule below, which cannot
          decide it: an early END left no artifact at n_tasks - 1; an unusable entry instead rewinds
          the cursor to 0 and emits checkpoint_missing (§4.8.4)
    if rec.state in (failed, interrupted) and rec.n_tasks_done >= n_tasks:
        if rec.n_tasks_done > n_tasks:    # not a state the Runner can create
            → record CorruptCheckpoint, emit pipeline.corrupt_cursor, leave the cursor as evidence
        elif artifact(n_tasks - 1) is available and decodes:
            → mark_final first when the artifact is not already final (idempotent either way, and nothing
              has been destroyed at this point), then settle the terminal row in one write — state=succeeded,
              cursor=n_tasks, run_id=current, failure fields cleared — and emit pipeline.terminal_repaired
              carrying the previous state/error/run. Nothing is rerun.
        else:
            → start = 0   # payload dropped (checkpoint_missing) or undecodable (checkpoint_unusable);
                          # the ordinary restart rule below applies
    start = 0
    if rec.state in (failed, interrupted) and rec.n_tasks_done > 0:
        h = newest handoff row for pid with handoff_id > rec.handoff_floor
        if h is not None and h.to_seq is not None and h.to_seq >= rec.n_tasks_done:
            entry = store.get_artifact(pid, h.entry_seq)      # the ledger names the entry state (§4.8.3)
            if entry.available: start = h.to_seq; prev_value = decode(entry)
            else:               start = 0   # the entry payload is gone → restart from the seed, loudly
        else:
            prev = store.get_artifact(pid, rec.n_tasks_done - 1)
            if prev.available:  start = rec.n_tasks_done; prev_value = decode(prev)
            else:               start = 0   # journal=summary stores no payload → the whole pipeline must be rerun (an event is left behind)
    if start == 0 and spec declares control:
        rec.handoff_floor = newest ledger ID (or 0); persist with the reset cursor before tasks
    for seq in range(start, n_tasks):  ...
```

先查交接分支，而且它收得很窄：只有**最新的活跃**账本行（`handoff_id > handoff_floor`）才可能待处理，因为只向前的交接目标严格递增，`to_seq` 低于游标的行已经被后续前进消费掉了，走普通 artifact 规则。已消费的行不花钱，待处理的行从目标处恢复，不重跑源任务。

终结流程的**任何一步**——final 标记或写终结状态——挂了，都由恢复路径兜住：那行完全保持原样（原来的失败、游标、所属 run），`pipeline.terminal_repair_failed` 会连同阶段一起记这次尝试，所以后续运行还是报原始原因，不是修复自己的错误。要是让这类异常逃到 worker 的通用内部错误路径，它会用这个错误重写那行，把修复本该保住的原始信息搞没——所以整个终结流程是受控操作。同理，存储提供了 `settle_pipeline` 时，终结状态转换（状态、游标、所属 run、失败字段）是单次写入；artifact 已经是 final 就跳过 `mark_final`——对观测不到这一点的崩溃场景，记成幂等。

因为那行保留原来的所有者，这次失败不算在执行修复的那次 run 的统计里；它进 `RunReport.repair_failures`，CLI 退出码和摘要都用这个数——修复失败绝不会被报成干净运行。没有 `settle_pipeline` 的存储，两次写入的回退路径分别归类：写终结状态失败算修复失败（见上文），元数据清理失败不算——那行已经持久地处于 `succeeded`，残留的失败文本是这类存储的降级保证，用 `pipeline.terminal_cleanup_failed` 报告。

关键好处：**任务 C 挂了就只重跑 C，B 的请求不会重发**；而且种子 artifact 也持久化了，**恢复不依赖原始数据集文件**。

### 4.2 ★ 租约安全契约

任务里拿/还资源，是**你的代码和框架之间最关键的交互**。契约如下：

| 场景 | 保证 | 实现位置 |
|---|---|---|
| 正常结束 `async with ctx.acquire(...)` | 退出时同步归还 | `_LeaseGuard.__aexit__` |
| 块里抛异常 | 同上——`__aexit__` 照跑，不吞异常 | 同上 |
| 循环里 acquire→use→release | 每轮立即归还，并发真正让出来（不累积占用） | 看你怎么写 + 同一机制 |
| `CancelledError`（外部取消 / 超时） | `finally` 里同步归还 | `Runner._execute_task` |
| 用了逃生通道 `await ctx.acquire_lease()` 忘了还 | 任务结束时**强制回收**，记 `lease.leaked` 事件 + 计数器 | `TaskContext.reclaim_now` |
| 任务返回后还占着租约 | 不可能：先回收、后持久化，两者都是同步的 | 同上 |

这一点成立的根本原因：**回收是纯同步函数**。`reclaim_now()` 不做任何 `await`，`lease.release_now()` 也一样，所以 `CancelledError` / `asyncio.wait_for` 超时 / 任何异常都**打断不了**它。资源池状态的分配和释放全同步做；单线程 asyncio 下没有"先检查再被抢占"的窗口，所以不需要锁。

三个配套设计：

* **资源健康靠任务显式上报**（`lease.report(ok=False)`）；框架不替你猜"这次失败是不是资源的问题"。好处是健康的端点不会因为一次 JSON 解析错误就被熔断。
* **泄漏可观测**：`lease.leaked` 事件 + `PoolStats.leaked_total` + `RunReport.leases_leaked`；要严格模式就用 `strict_leases=True`，一泄漏任务直接失败（`LeaseLeakError`）。
* **疑似死锁会告警**：任务已经占了本资源池的某个资源，又向同一资源池申请另一个，而资源池没空闲容量，等超过 `deadlock_warn_s`（默认 5s）就发 `acquire.suspected_deadlock` 事件。

### 4.3 资源池

**状态机**（`state_at` 惰性推进，没有后台任务）：

```
READY ──consecutive failures ≥ degrade_after──▶ DEGRADED(blocked_until = now+cooldown_s) ──cooldown expires──▶ READY
   │
   └──consecutive failures ≥ dead_after──▶ DEAD        (report(ok=True) can pull DEGRADED/DEAD back to READY)
REVOKED ◀── explicit revoke / revoked from within a task
```

注意：冷却到期**不会清零** `consecutive_failures`（只有成功才清零）。否则，"降级阈值 < 死亡阈值"且资源池只有一个资源时，每次冷却都把计数器抹了，永远到不了 `DEAD`。

`factory` 抛过异常的资源走同一个状态机（`degrade_after`/`dead_after`/冷却），而且冷却到期*确实会*清掉这个资源存的客户端错误，下次租约时再调 factory——对暂时性的 factory 失败，DEGRADED 是真正的第二次机会，不只是 DEAD 前的延迟。

**发布/订阅**用两条语义不同、实现也不同的通道：

| 通道 | API | 用途 |
|---|---|---|
| 拉（获取） | `async with ctx.acquire(**selector) as lease:` | 等 + 租；选择器支持 `id`/`kind`/`tags`/`options` 和点分路径（`"a.b"`） |
| 推（订阅） | `ctx.subscribe(pool, ["resource.published"])`, `ctx.bus.subscribe(topic)` | 收资源发布/退役/降级/恢复信号 |
| 推（发布） | `ctx.publish_resource(pool, resource)`, `ctx.revoke_resource(...)` | 任务运行时发现新端点就注入，其他流水线立刻能租 |

资源池每次状态变化都发一个 `ResourceEvent`，同时喂给**四类**消费者：等待者（唤醒）、订阅者、`events` 表（结构化日志）和监控快照。一个事实，四个视图。

### 4.4 算法和重试是两条正交的轴

这部分最容易搞混，实现上刻意把两者分开：

| | 算法 | 重试 |
|---|---|---|
| 时机 | 干活**之前**：怎么等/选资源 | 干活失败**之后**：要不要再试 |
| 输入 | 资源池容量/健康/配额 | 异常类型 + 错误分类 |
| 内置 | `immediate`（没资源就失败）/ `wait`（默认）/ `backoff`（指数退避 + 全抖动）/ `least_busy`（选最闲的）/ `failover`（按顺序切资源池）/ `sticky`（留在本流水线用过的资源上）/ `quota_aware`（优先剩余配额最多的） | `Retrying(max_attempts, on, base, factor, cap, jitter, max_total_s)` |
| 声明位置 | `@task(algorithm="backoff")` 或 `pool.algorithm` | `@task(retry={"max_attempts": 4, "on": ["RetryableError"]})` |

`failover` 按顺序立刻试列表里每个资源池；都没容量的话，它的 `fallback`（默认是 `Wait()`）只挂在 `pools[0]` 上——不会在整个列表上轮一遍 fallback。把这个列表理解成"先试这些，最后落在主池上等"，不是"谁先空出来就等谁"。

**失败分类**（`errors.error_class_of`，纯函数，可单测）：`status`/`status_code` 属性里的 408/504 → `timeout`，425/429 → `rate_limit`，5xx → `upstream`，4xx → `fatal`；`TimeoutError` → `timeout`；`ConnectionError` → `connection`；`ValueError/TypeError/...` → `invalid`；其他一切 → `unknown`。你也可以自己接管，方法是抛 `RetryableError(msg, error_class=..., retry_after=...)` / `FatalError`，或者注册自己的分类器。

**默认不重试**（`max_attempts=1`），保持"失败就是失败"的简单语义；需要再显式开。
**每次重试决策都落盘**（`attempts.decision_json`）：`{retry, reason, delay_s, error_class, attempt, max_attempts, retry_after}`，其中 `reason ∈ {ok, retryable, attempts_exhausted, policy_declined, total_budget}`。`delay_s` 永远在（不需要延迟时是 `0.0`），读这个 schema 的代码不用防 key 缺失。所以"它为什么重试了 5 次 / 为什么放弃了"直接查数据库就行，不用翻日志猜。

### 4.5 延迟延续与批处理事实（M2）

两个吞吐机制，不改执行模型，只改调度和持久化方式：

**重试退避让流水线挂起，不是让 worker 睡觉。** `Runner._execute_task` 只跑一次尝试，然后把重试决策交回 `_drive`；策略要再试一次时，流水线进 `DelayQueue`（`scheduler.py`），worker 立刻去接别的活。一个 pump 任务在定时器到期后把挂起的状态移回工作队列。结果：

* `concurrency` 终于名副其实：它指在飞的尝试数，不是熬 30 秒退避的流水线数；
* worker 空闲不代表运行结束——`_wait_for_completion` 也会等挂起的流水线，退避不会被误判成中断；
* 关闭时先取消 pump，给每个 worker 发个哨兵，挂着的一切都记成 `pipeline.deferred_interrupted`——可恢复，不静默丢。

定时器走可注入的 `Clock`，`interruptible_sleep` 让 `clock.sleep` 和唤醒事件赛跑，提供两种行为：真实时钟下，新推的更早定时器会截断长等待；假时钟下，pump 立刻推进虚拟时间（测试保持确定性）。

**只追加的事实按批写。** `WriteBehindStore` 缓冲尝试和事件，按批刷（大小阈值、时间间隔、任何读 API、运行心跳、运行结束）。状态写入——`pipelines`、`tasks`、`artifacts`——永远直接落盘，因为还没落盘的检查点不算检查点。所以 `SIGKILL` 可能丢最后一批历史，所有检查点完好；`--no-write-behind` 用吞吐换每次尝试立即提交。

### 4.6 监控：运行状态与应用汇报

```python
snapshot = runner.stats()          # live in-process snapshot
# or cross-process: pyattacker watch runs.db   ← a read-only connection to the same SQLite file (WAL: one writer, many readers)
```

面板显示：流水线状态分布、p95/最大延迟、各任务计数，每个资源池的 `active/capacity`、`ready/degraded/dead`、`waiting`、吞吐和泄漏计数器，最近的错误。也可以编程用：`monitor.render_snapshot(stats)` / `monitor.watch(store)`。
应用汇报值与运行计数分开展示。应用可以观察已提交的流水线终态并汇报准确率，但对流水线
去重、以及从持久化产物恢复累计结果都由应用负责。

### 4.7 完成以计数为准，worker 生命周期受监督

`pipelines_done` 到 `pipelines_admitted` 时，一次运行结束。这个不变量只有在每个已接纳的流水线最终都走到 worker 自己的某条终结路径时才成立——不是 `CancelledError` 的 `BaseException`（比如存储钩子抛的自定义子类）哪条都走不到：它会逃出 worker 循环，worker 任务跟着结束，完成判定等的计数器永远不前进。以前这种情况运行会永远静默地等：没错误、没退出码、没终结状态行、没事件。

所以 worker 生命周期和流水线记账分开观测，方式是给每个 worker 任务挂完成回调。它还能看到处理器链 *之后*（队列或 worker 的清理工作）发生的死亡，归为运行级故障：

* **先停止，绝不记账。** 运行被硬停（`stop("worker_crashed")`，不再接新流水线），`pipelines_done` 为了满足计数器不变量刻意*不*递增：这次运行不是正常完成，真正释放等待者的是这次停止。
* **流水线被置为终态。** 一行没有存活 worker 拥有的 `running` 记录，正是这条路径要消除的静默状态，所以在途流水线记为 `failed`——运行已在收尾则记为 `interrupted`——把逃逸的异常写到那行上。worker 死时已经处于终态的流水线保留已有状态：历史不改写。**做决定的是持久化的行，不是运行器的内存记录**——存储不需要回写交给它的 `PipelineRecord`，那个条目可能是重试队列返回的状态，早先的终态写入已经持久化，信内存会把一行 `succeeded`（或带自己来源的普通失败）改写成这次崩溃。行不存在就创建，方式和内部错误路径一样。
* **工作队列被释放。** 满队列只会由 worker 排空，所以接纳流程和关闭哨兵通过感知中止的 put 移交条目，worker 一死就放弃。没有它，*生产者* 就会挂在已死 worker 的队列上，运行差一步就卡住。
* **故障是显式的。** `runner.worker_crashed` 会指名流水线、异常和回溯，`run_async` 会在运行记录以 `interrupted` 关闭后抛 `WorkerCrashed`——原始异常作为 `__cause__`。存储连这次崩溃也记不了，就走既有的 `StoreUnavailable` 致命路径，不重试已损坏的存储。

`KeyboardInterrupt` 和 `SystemExit` 是边界：asyncio 会把它们从任务里重新抛出，让事件循环停，完成回调根本不跑。worker 内部一处很窄的守卫会在它们继续传播之前记行和事件，但循环还是会拆：调用方看到的是自己发出的中断，运行记录不关闭。*任务自己* 抛的 `BaseException` 跟这个无关——任务自己的处理会收住它，和普通异常一样。取消也不受影响：被取消的 worker 还是会把流水线标成 `interrupted` 再重新抛，监督机制不会把刻意取消变成崩溃。

### 4.8 进阶：交接 —— 声明式正向跳转（可选启用，实验性）

下面只讲正向的契约，还是 v1 的路。启用反向的声明用 §4.8.8 里感知访问的契约和[反向参考](reference.md#进阶反向遍历rewindretry-allvisits)；只正向的遍历还是用既有的账本/游标恢复，不需要新身份或预算要求。

上面说的都是普通流水线：一条链一次走一个任务，每个任务返回下一个要吃的 artifact。本节介绍唯一会改链*遍历*方式的功能：某一步可以告诉链的剩余部分不用跑了——答案已经够好、样本不在范围内、有缓存结果——于是它可以**向前跳**，不用跑不需要的步骤，也不用把分支藏在一个任务里，或者靠抛错把流水线记成失败（那是在说谎）。这个功能刻意和其他部分隔开：

* **可选启用**——没有 `control` 声明时，流水线行为和以前完全一样，连 `spec_digest` 都逐字节相同（见 §3.1）。本节内容不适用于没主动要它的流水线；
* **进阶层级**——不是因为难用，而是因为它改执行模型。它写在自己的标题下、以次版本号发布、标了*1.0 之前实验性*：下面的保证是稳定部分，写法（`Handoff`、`control`）还可能变；
* **这个模型里只正向**——交接只能向*前*跳：`control.edges` 和 `Handoff.to()` 绝不搞隐式反向语义。这个能力的动因场景恰恰是反方向的（校验器把坏的模型输出**送回**生成器重采样）。§4.8.7 规定了那个模型，§4.8.8 把它做成一个*单独声明*的可选层级，所以本节描述的正向契约不变，不扩展。

#### 4.8.1 传递载体是返回值，不是控制流异常

任务通过**返回**一个框架自带的指令来交接，不是返回一个值：

```python
from pyattacker import Handoff

@task("judge")
async def judge(value: Verdict, ctx: TaskContext) -> Handoff | Report:
    if value.good_enough:
        return Handoff.end(value.as_report(), reason="already good enough")          # finish here
    if not value.needs_metrics:
        return Handoff.to("report", value.as_report(), reason="metrics not needed")  # skip ahead
    return await write_report(value)                                                 # ordinary success
```

`Handoff.to(target, value=UNSET, *, reason="")` 指定一个目标——任务名、任务的 seq，或 `"end"`；`Handoff.end(value=UNSET, *, reason="")` 用这个值作为最终 artifact 结束流水线。`UNSET`（任务层表示"没给"的哨兵）意思是"目标用*本*任务收到的 artifact 进入"；显式给的值会成为它自己的载荷 artifact。`None` 是合法载荷，所以只有 `UNSET` 表示"复用"。

因为这个指令是**返回值**，它不是失败，也没有新的失败路径：

* 重试策略永远不会被问到，所以 `retry.on=(Exception,)` 和 `retry_unknown=True` 没法把交接变成重试，也不会新增 `decision.reason` 取值；
* 不涉及任何异常类，所以任务侧的 `except Exception:` 或 `try/finally` 吞不掉也取消不了这次传递——把控制转移藏在异常里，正是本设计要避免的；
* 租约退出时和成功时一样还：`async with ctx.acquire(...)` 已经还了，`finally: ctx.reclaim_now()` 照跑。`strict_leases=True` 加一个泄漏的租约还是任务失败，交接不生效；
* 取消和 `timeout_s` 不受影响：被取消或超时的尝试走不到那个返回。

这份诚实的代价是刻意接受的：交接只能在任务能 `return` 的地方发生，调用栈深处的辅助函数必须把指令往上传。这正是预期的权衡——显式、可审查的交接比看不见的控制转移好——而且它让任务签名保持诚实（`-> Handoff | Report`）。

#### 4.8.2 交接是一次持久化的状态转移，不是控制流把戏

运行器在尝试内部拦截指令——在它被编码成 artifact 之前——把它变成一次有记录的跳转。五项事实一起写（见 §4.8.4）：

| 事实 | 位置 | 含义 |
|---|---|---|
| 源任务行 | `tasks.state` | `handed_off`——它干净结束，没产生 artifact |
| 尝试行 | `attempts.outcome` | `handed_off`，`decision` 空（本来就没决策） |
| 入口 artifact | `artifacts` | 目标的输入：源任务收到的 artifact，或新载荷 |
| 账本行 | `handoffs` | from/to、入口引用、是否复用，以及作者给的原因 |
| 游标 | `pipelines.n_tasks_done` | 目标的位置（`END` 时是 `n_tasks`） |

正是账本让"这条流水线的任务列表为什么跳了某些步骤"可以直接从存储里查到，它也是**恢复的事实来源**。`pipeline.handoff` 事件是审计踪迹：硬杀可能丢事件，账本永远权威——这是唯一要记住的不对称。

#### 4.8.3 入口状态总有一个在自己地址的持久化引用

不带值的 `Handoff.to(target)` 记 `state.artifact.id`——本任务收到的 artifact——所以账本的 `entry_artifact_id` 永不为 null。显式给的值由项目的 `CodecRegistry` 编码，写成普通 artifact，它的 `seq` 在**提交内部**按 `n_tasks + k` 分配（`k` = 这条流水线已记的交接数）。于是文档化的 artifact 顺序变成：

```
seed (-1)  →  chain (0 … n-1)  →  handoff payloads (>= n)
```

两个后果很重要。载荷永远不盖任务槽位（这正是天真的"写进槽位 `t-1`"设计会掉的坑），而且它记在发起交接的那个任务名下。此外，`Artifact.seq` 不再全局等于任务位置：链上的任务它是任务位置，种子是 `-1`，一旦开了控制流，它就是 `>= n_tasks` 的载荷地址。只看链的读取方保持原有语义。

#### 4.8.4 提交，以及它支撑的恢复规则

交接是检查点，所以写入顺序是契约的一部分。存储新增一项**可选**能力 `commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)`——沿用 `resources()`/`settle_pipeline` 的先例——它在一次操作里**原子地**完成整个转移：终结源任务、插入这条已交接的尝试、持久化载荷（分配地址）、追加账本行、移动游标（流水线还是 `running`）。对 `END`，它还在同一次提交里把入口 artifact 标成 final，把流水线结算为 `succeeded`。

原子性是这项能力的*要求*，不是额外好处，因为这里刻意不设第二套恢复协议：不能作为一个单元提交的存储，直接不暴露这个方法；在这样的存储上开一条声明了 `control` 的流水线会快速失败，抛指名这个能力的 `ConfigError`。这条规则是为了防止"静默地不持久"的交接。（`WriteBehindStore` 会转发这项能力，先冲掉缓冲的尝试和事件，把这条已交接的尝试走提交写，不走缓冲区。）

于是恢复只有一条新分支，在那些线性终态规则**之前**查：

```
newest ledger row h for the pipeline with handoff_id > rec.handoff_floor
if rec.state in (failed, interrupted) and h is not None:
    if h.to_seq is None:                 # END that did not finish writing
        entry = artifact(h.entry_seq)
        if usable: mark_final(entry); settle succeeded       # never re-run the source task
        else:      cursor = 0; checkpoint_missing            # the ordinary restart-from-zero rule
    elif h.to_seq >= rec.n_tasks_done:   # the commit landed, the target did not finish
        entry = artifact(h.entry_seq)
        if usable: start at h.to_seq with decode(entry)
        else:      cursor = 0; checkpoint_missing
    else:                                # consumed by later forward progress
        the ordinary artifact(cursor - 1) rule
```

每次从种子重新开始，都要在跑任务之前通过 `reset_pipeline(record)` 把 `rec.handoff_floor = newest ledger ID` 和重置后的游标一起持久化。重置会原子地移除当前任务行和链上的 artifact（seq 0..n-1），清掉之前的 final 标记，把游标/水位一起写。种子 artifact 和高位载荷 artifact、尝试、事件、交接都作为历史保留。这样在不删历史的前提下，把交接排除在一次放弃的执行之外；在恢复的目标处，水位不变。SQLite 以 0 为默认值迁移这列，只读读取方也容忍它缺。失败的 SQLite 交接事务会在之后任何事件或清理写入提交之前回滚。完成时选一个 final artifact，清掉较早的 final 标记。

只有最新的活跃行可能待处理，因为只正向的交接目标严格递增——低于游标的 `to_seq` 必然已经越过。必须先查 `END` 分支，因为提前的 `END` 在 `n_tasks - 1` 处根本没留 artifact，线性终态修复甚至判不了这种情况。

#### 4.8.5 游标是一个位置，遍历在结构上终止

`n_tasks_done` 保留它的字段和"下一个要跑的 seq"这层意思。对开了控制流的流水线，它是**位置**，不是已跑任务数：被跳过的槽位从没跑过，所以它们的任务行不存在，`n_tasks_done == n_tasks_total` 也不再意味着"每个任务都跑了"。这里没引入新进度字段，任何界面都不能给这样的流水线把 `n_tasks_done / n_tasks_total` 渲染成完成百分比（`report`/`watch`/`/pipelines` 就是因此在旁边露交接数量）。

终止还是结构性的，不靠预算：交接只能指向严格更晚的位置，所以游标严格递增，链最多走一遍。这就是本版本不需要循环预算的原因——也是 v2 反向场景不引入预算就加不进来的原因（§4.8.7）。

#### 4.8.6 声明边，以及校验会（和不会）查什么

```python
pipeline("qa", retrieve | ask | judge | report,
         control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})
```

边是**声明出来的，不是推出来的**：沿没声明的边返回 `Handoff` 是致命错误（绝不静默跳转，绝不重试），每条已声明的边在构建流水线时解析并做范围检查：

* 源和目标都得存在，重名任务必须用数字 seq 消歧（错误信息会说匹配到了哪些 seq）；
* 目标必须严格晚于源（只正向）；
* `"end"` 是合法目标，但从最后一个任务发起除外——那里没效果，会被拒；
* `edges` 块没有未知 key——`mode` 刻意省略，因为正向模式只有一种。反向操作是单独的 key（`rewind`、`retry_all`、`max_handoffs`），同一个入口校验，§4.8.8 规定。

校验**只做结构检查**。交接的载荷是任意实参，不是源任务通常的返回类型，所以 `source.returns -> target.accepts` 刻意不查：它会拒合法交接（比如 judge 把 `value.more_queries()` 交给 `ask`），又会接受非法交接。

它新增的一条标注规则：标注里的 `Handoff` 成员是逃逸。`returns` 侧，`-> Handoff | Report` 按 `Report` 接链，单独的 `-> Handoff` 能接任何类型，因为这样的任务在那条路径上不产 artifact。逃逸只适用于产出/返回标注；接受侧不变，因为运行器从不把指令当 artifact 传。声明式层接受和 `pipeline.control` 相同的块，用字段路径（`pipeline.control.edges['judge'][0]`），同一个入口校验，所以 `validate` 和 `run` 会以退出码 2 拒相同配置。

解析后的控制计划防御性地复制任务名和目标序列，然后冻结这个映射。运行时拓扑在验证后不能改，也不会和算出的 `spec_digest` 偏离。编不了码的显式交接载荷会抛 `FatalError`，激进的重试策略重放不了返回了非法指令的任务。

#### 4.8.7 本版本刻意不包含的东西

整项能力的动因场景是**相反**的方向，写下来也是对这项能力承诺的一部分：

```
ask(temperature=0.2) ─▶ validate ─▶ (invalid) ⇢ revoke to ask(temperature=0.7) ─▶ validate ─▶ …
```

模型评估是采样，不是函数调用：结构化输出步骤经常产出结构上无效的结果，校验器能认出来。这时候流水线应该*回到*生成器再试一次，可能还带着 artifact 里不同的参数。任务内部循环不好表达这一点——它把生成、校验、中间步骤塌成一条记录、一份租约历史、一个重试策略和一个超时，"这个样本需要重新生成几次"恰好在它本身就是测量目标的地方变得不可见。撤销式交接让每次重新生成都成为生成步骤的一次真实访问，有自己的尝试记录，整件事崩了还能恢复，几千个样本的运行还能在样本中途恢复，不是从头重跑。正是它决定了这里若干记录决策的形态，所以下面的模型现在就写清楚，不等到以后才发现：

| 没包含的 | 原因，以及需要什么 |
|---|---|
| 反向/撤销式交接 | 这项能力的动因场景：校验器把工作**送回**生成器。它需要访问模型——任务和尝试上的 `(seq, visit)` 身份、持久的按 seq 计数并在入口记录的同一次提交里推进的计数器、感知访问的 RNG（`ctx.seed` 目前是 `digest(pipeline_id\|seq\|attempt)`，所以再访问会看到完全相同的随机性）、循环预算（终止不再是结构性的）、不把访问次数呈现为完成度的进度报告。这里的记录决策——账本、入口 artifact 地址、原子提交、位置游标——都是为了让那个模型不加改动就能加进来而选的，§4.8.8 把它作为单独声明的可选层级加入。 |
| 声明的 DAG、join、fan-in | 链保持链。交接是关于一条流水线的调度语句，不是图的边。 |
| 跨流水线交接 | 流水线语义独立；唯一共享界面还是资源池。 |
| 运行时临时造目标 | 边是声明出来的，拼错或类型错的目标大声失败，不静默重塑流水线。 |
| 来自 `fanout` 分支的交接 | 一个组在记录里是一步（`fanout` 在一个任务内部跑子任务），控制转移归因不到 N 个并发分支里的某一个。返回的指令会让组以清晰的 `FatalError` 失败，不在收集的载荷内部传递。 |
| 载荷类型检查 | 见 §4.8.6：没有自己声明的载荷契约，它就没有可靠定义。 |

#### 4.8.8 进阶 v2：回退、全量重试与可选载荷历史

它作为单独的可选能力实现：`control.rewind` 声明严格更早的目标，`control.retry_all` 声明源，`control.max_handoffs` 限制遍历。回退需要作者显式选的入口状态；全量重试解码绑定时捕获的原始种子。带历史的载荷是可选的，绝不驱动调度。
[反向参考](reference.md#进阶反向遍历rewindretry-allvisits)规定接口，[tutorial 第 16–17 步](tutorial.md#第-16-步--高级用回退和全部重试重新生成)用可运行程序把它搭起来。

持久化的遍历记录有按 seq 的计数器、生效的槽位到访问映射和待处理的入口，包括那次确切的输入发生实例。每个新入口分配一次访问；恢复复用待处理的访问。成功时一起提交输出/任务/尝试/生效映射/游标。控制转移还提交后缀失效、预算消耗和目标入口分配。游标比较和历史完成行判不了一次反向转移是否已被消费。在 seq 0 恢复是一次真实入口。

访问保留 visit 0 的任务/artifact 身份，给后续 ID 加限定，参与 RNG。回退/全量重试保留历史。预算计数在恢复和载荷缺失回退后保留。完成与终结用确切的发生实例身份。反向重新入队释放 worker。快照历史是应用管理的 JSON 状态，配带版本的编解码器，不是执行账本。

两条规则让模型在边界处保持诚实。**所有权：** 恢复一条反向流水线是在延续一次确切的持久化访问，所以 `running` 行绝不会被隐式接管——`resume=True` 是操作者声称先前的所有者已经消失，没有它，那行被跳过、保持原样（正向路径保留更早的从零重启规则，这也是它不在通用打开路径里的原因）。**丢弃状态是显式的：** `fresh_restart=True` 是唯一会丢检查点或遍历的开关。它从绑定的种子重新开始，重置控制预算，让先前的账本水位失效，同时把被丢的遍历遗留的在途任务行结算为 `interrupted`，只追加的历史和（对反向流水线）访问计数器和发生实例保留——历史上的发生实例仍可寻址，遍历已丢的存储会从自己的行重建计数器。`retry_succeeded` 还是资格开关（"也接纳已成功的流水线"），不再意味着丢任何东西。

存储记它的磁盘模型走到哪一步了（`store/visits.py`）：第一次重新访问提交前是 `base`，之后是 `visits-v1`，和让它成立的那次发生实例写在同一事务里。未知层级打开时被拒，不自行解释；`visits-v1` 的 SQLite 存储启用一道写入者守卫，拒任何未声明访问谱系感知的连接写入——这个标记是为了让不感知谱系的写入者大声失败，不改写错的发生实例。迁移对只正向的工作保持增量式，这类工作永远不离开 `base`。

---

## 5. 数据模型（SQLite，WAL + `synchronous=NORMAL`）

三层事实，职责不重叠：

| 表 | 是什么 | 语义 |
|---|---|---|
| `runs` | 一次运行 | heartbeat、state、配置快照、version、host、seed |
| `pipelines` | **当前状态** | 恢复时一条 SQL 选出待处理的活；`n_tasks_done` 是检查点游标 |
| `tasks` | 每个任务的当前状态 | 就地覆盖；记 `attempts_used`、耗时、错误、用了几个租约 |
| `attempts` | **只追加的历史** | 每次尝试一行，跨恢复连续编号，永不覆盖 |
| `artifacts` | 状态载体 | 内容寻址 + `payload BLOB`；`is_final` 标最终产物。`seq` 链上的任务是任务位置，种子是 `-1`，开了控制流的流水线上是 **`>= n_tasks` 的交接载荷地址**（§4.8.3） |
| `events` | **结构化日志** | `scope ∈ run/pipeline/task/pool/resource`；一条流水线的完整故事 = 按 `pipeline_id` 查 |
| `handoffs` | **控制流历史**（进阶，可选） | 只追加：每个任务发起的跳转一行，指明 from/to 位置和持久化入口 artifact；恢复依据的权威记录（§4.8） |
| `resources` | 资源池的最终状态 | 资源规格（key 已脱敏）+ 健康统计 |

**一条流水线的完整记录**：

```sql
SELECT * FROM pipelines WHERE pipeline_id = ?;                  -- state and checkpoint
SELECT * FROM tasks     WHERE pipeline_id = ? ORDER BY seq;     -- final state of each task
SELECT * FROM attempts  WHERE pipeline_id = ? ORDER BY seq, attempt_no;  -- full history and retry decisions
SELECT * FROM artifacts WHERE pipeline_id = ? ORDER BY seq;     -- intermediate states, payloads, the final product
SELECT * FROM handoffs  WHERE pipeline_id = ? ORDER BY handoff_id;-- control-flow history: where it jumped, and why
SELECT * FROM events    WHERE pipeline_id = ? ORDER BY event_id;-- structured log
```

`journal` 模式：`full` 存 artifact 载荷（**恢复的前提**）；`summary` 只存摘要和元数据（省空间，代价是中间 artifact 没法复用，恢复只能重跑整条流水线，留一个 `pipeline.checkpoint_missing` 事件）。

**写入策略**：所有存储方法都是同步的——像"尝试开始前记 running 行"这种关键写入不被取消打断。批处理写 / write-behind 合并是后续优化，不改接口。

**读取策略**：上面的列表查询可以物化结果——报告本来就需要一个列表。必须保持有界的整类读取（比如导出大存储）走可选的分页迭代扩展：按键集分批读 `ITER_BATCH_SIZE` 行，排序键以唯一列结尾，批次边界不漏不重；嵌套的 `pipelines` 行物化一条流水线，这就是文档化的内存单位。键单调时（`event_id`、`attempt_id`），迭代器以启动时拿的高水位为界，对活跃存储的导出不追不断移动的尾巴；`pipelines`/`tasks`/`artifacts` 没有单调键，文档里说成对活跃存储的尽力遍历。`Store` 不变，只实现列表 API 的第三方存储还是完整的，只是内存不再有界——见[存储参考](reference.md#分页读取与第三方存储)。

---

## 6. 两种用法

### 6.1 SDK（主要形式）

```python
from pyattacker import Pool, Resource, RetryableError, Retrying, Runner, pipeline, task

class Client:                      # your own network code, the framework does not touch it
    def __init__(self, options): self.options = options
    async def chat(self, prompt):  ...

@task("fetch")
def fetch(row: dict) -> dict:
    return {"q": row["q"]}

@task("ask", resource="apis", algorithm="backoff",
      retry=Retrying(max_attempts=4, on=(RetryableError, TimeoutError), base=0.5, cap=30.0),
      timeout_s=60)
async def ask(row: dict, ctx) -> dict:
    async with ctx.acquire(model="gpt-4o") as lease:   # returned on exit; returned on exception too
        text = await lease.client.chat(row["q"])
        lease.report(ok=True, usage={"tokens": 128})
        return {"a": text}

@task("judge", resource="judges")
async def judge(row, ctx): ...

@task("metrics")                    # synchronous pure-computation task
def metrics(row) -> dict: ...

pool = Pool("apis", [Resource.create("llm", capacity=4,
                                     options={"base_url": ..., "api_key": "${OPENAI_KEY}", "model": "gpt-4o"},
                                     factory=lambda res: Client(res.options))
                     for _ in range(8)],
            algorithm="backoff")

template = pipeline("qa_eval", fetch | ask | judge | metrics, tags={"bench": "mmlu"})

with Runner(store="runs/qa.db", pools=[pool], concurrency=64) as runner:
    report = runner.run(template.map(dataset_rows, repeats=3))   # a generator, streaming, memory O(concurrency)
    print(report.summary())
    report.export_jsonl("runs/qa.jsonl")
```

恢复：`runner.run(template.map(dataset_rows), resume=True)`——已成功的流水线跳过，失败的从**第一个没产出 artifact 的任务**接着跑。

完整实战示例——数据准备、两轮模型调用、三个评委、用户自己写的归约，有分组形态也有按评委拆分的流水线形态，检查点开销是测出来的不是断言出来的——在 `examples/llm_eval/`。

### 6.2 声明式（简单任务）

声明式形式只描述**组装和资源**；逻辑还在 Python 里（`use: myproj.tasks:ask_model`）。文档可以是 YAML、TOML 或 JSON；后缀决定用哪个解析器，只有 YAML 是可选依赖（§8.13）。

```yaml
run:   { store: runs/demo.db, concurrency: 8, journal: full, label: demo }
pools:
  apis:
    kind: llm
    algorithm: backoff
    resources:
      - { id: api-1, capacity: 4, options: { model: gpt-4o, api_key: "${OPENAI_KEY}" } }
pipeline:
  name: qa_eval
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: pyattacker.tasks:simulate_llm
      resource: apis
      algorithm: backoff
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }   # factory arguments
      # a bare `on` is a YAML 1.1 boolean key: the retry key must be quoted
      retry: { max_attempts: 3, base: 0.2, "on": [RetryableError, TimeoutError] }
source: { kind: jsonl, path: data.jsonl, limit: 100, key_field: id, repeats: 1 }
```

```bash
pyattacker validate -c pyattacker.yaml     # validate and print the effective config
pyattacker run      -c pyattacker.yaml --progress
pyattacker resume   -c pyattacker.yaml     # = run --resume
pyattacker watch    runs/demo.db           # open another process to watch it live
pyattacker report   runs/demo.db --errors 20
pyattacker export   runs/demo.db out.jsonl
pyattacker demo                            # verify the installation with zero configuration
```

退出码：`0` 全成功 / `1` 部分失败 / `2` 配置错 / `130` 中断。

### 6.3 分片、合并与导出（M3）

内核是单循环的，SQLite 只接一个写入者，所以横向扩展意味着**多进程各有各的存储**，事后合并。分片分配是内容寻址的 `pipeline_key` 的纯函数，同一份数据集永远按同样方式切，`--resume` 把每条流水线送回原来的分片。

```bash
# convenience: spawn N children locally, wait, then print the merged view
pyattacker run -c qa_eval.yaml --shards 4 --jobs 4 --store runs/qa.db
# -> runs/qa.shard0of4.db … runs/qa.shard3of4.db

# or drive the shards yourself (a cluster, a scheduler, N terminals)
pyattacker run -c qa_eval.yaml --shard 0/4 --store runs/qa.shard0.db
pyattacker run -c qa_eval.yaml --shard 1/4 --store runs/qa.shard1.db

# resume is per shard: rerun the exact same command with --resume (or --shards N)
pyattacker resume -c qa_eval.yaml --shards 4 --store runs/qa.db

# one coherent answer out of N files
pyattacker report runs/qa.shard*of4.db
pyattacker export runs/qa.shard*of4.db runs/all.jsonl
pyattacker export runs/qa.shard*of4.db runs/tasks.csv --rows tasks --format csv
```

`--shard I/N` 原样用显式给的 `--store`；没显式给时，`run.store` 加 `.shardIofN` 后缀，两个子进程绝不抢同一个文件。每个子进程的环境里带 `PYATACKER_SHARD=I/N`，任务可以记自己的来源。

**行形态**（`--rows`）给消费结果的人用：`pipelines`（嵌套，默认）、`tasks`、`attempts`（带重试 `decision`）、`events`、`artifacts`。**格式**（`--format`）：`jsonl`、`json`、`csv`。CSV 表头取最前面的 `header_rows` 行，之后才出现的字段折进 `extra` 列，内存平稳，不悄丢字段。每种类型完整导出——`events` 过去在最新 10 万行处截断——读取按有界批次；见[导出参考](reference.md#导出)。

### 6.4 插件、后端与监控端点（M4）

**插件**就是普通的 `importlib.metadata` 入口点——没有注册表文件，没有导入期魔法：

```toml
[project.entry-points."pyattacker.tasks"]
my_judge = "my_pkg.tasks:my_judge"          # a TaskSpec, or a factory returning one
[project.entry-points."pyattacker.algorithms"]
my_algo  = "my_pkg.algo:MyAlgorithm"
[project.entry-points."pyattacker.codecs"]
my_codec = "my_pkg.codec:MyCodec"
[project.entry-points."pyattacker.stores"]
s3       = "my_pkg.s3:open_store"           # keyed by URI scheme
```

之后 `use: my_judge`、`algorithm: my_algo`、`store: "s3://bucket/runs.db"` 直接可用。三条规则避免这事变成负担：

* **内置项优先**，插件盖不住 `echo` 或 `wait`；
* **坏插件只记不抛**——`pyattacker plugins` 列哪些加载成功、哪些失败，健康的继续干活。插件做的任何事都不许逃到 `Runner.__init__` 外面，编解码器插件正是在那里装的；
* **声明自己能处理某载荷的编解码器插件优先于更早注册**，专门的编解码器不沦为 JSON 兜底后的死代码。显式 `for_types=` 映射还是比扫描强，因为点名类型比"我能编码它"声明更强。

完整示例在 `examples/plugin_package/`。

**artifact 后端**决定载荷字节存哪。`inline`（默认）留在存储里；`file:///data/blobs` 把超阈值的溢到内容寻址文件；`null` 存摘要丢字节。存储自己的行保留 `digest`/`size`/`codec`，加不透明的 `blob_ref`，读时重新水合出 `payload`——对上游来说 artifact 还是同一个对象，内容寻址意味着相同载荷合并成一个文件，共享后端能安全服务多次运行。

```toml
[run]
artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

**监控端点**：`pyattacker serve runs/qa.db` 起一个零依赖的只读 HTTP 视图（每个请求新开一条只读连接，和正在跑的运行并存）。`/stats`、`/events`、`/pipelines`、`/resources`、`/errors` 返回 JSON；`/` 是自动刷新的小仪表盘。没有 artifact 路由：它暴露的是这次运行*记下来*的东西——每条事件的 `data` 原样返回，失败流水线和 `recent_errors` 上的 `error_message` 也在——所以任务打日志时塞进去的内容、异常信息里带的内容，在那里都读得到。正因如此只绑回环地址、没认证，当调试工具用。

**fanout（扇出）**：`fanout(a, b, ...)` 在*同一份*输入上并发跑多个任务，而且是在**一个任务内部**——这就是不把流水线变成 DAG 的前提下表达真分支步骤的方式。权衡是明说的：重试粒度变成整个组，组采用最宽松的子任务策略。因为运行器只看到组的 spec，`resource`、`algorithm`、`timeout_s` 从子任务提升到组——但只有每个子任务都一致才这么做，一个组不能同时表示两种策略。一等公民的 `Parallel`/`Gather` 节点有意不在范围内——正是一元任务模型让内核（和它的恢复机制）保持小巧。

---

## 7. 模块结构

```
src/pyattacker/
  artifact.py     Artifact / Codec / content addressing      (no internal dependencies)
  errors.py       exception hierarchy + pure error-classification functions
  task.py         @task / TaskSpec / TaskContext / lease tracking and force-reclaim
  pipeline.py     Chain composition / artifact type chaining checks / pipeline_key / map(repeats)
  handoff.py      the Handoff directive + the resolved, validated control-edge plan (M5, advanced)
  resource.py     Resource / Pool / Lease / Bus / state machine and event stream
  algorithm.py    acquisition algorithms (immediate/wait/backoff/least_busy/failover)
  runner.py       pipeline coroutine scheduling / task-level checkpoint / retry / graceful shutdown / stats
  scheduler.py    delayed continuations: park a pipeline instead of sleeping in a worker
  store/          base (records + protocol) / memory / sqlite / writebehind (batched append-only facts)
  monitor.py      snapshot rendering + watch
  shard.py        deterministic shard assignment + per-shard store paths (M3)
  merge.py        join N shard stores: de-duplicate by pipeline_id, recompute statistics (M3)
  export.py       row shapes (pipelines/tasks/attempts/events/artifacts) and formats (jsonl/json/csv) (M3)
  declarative.py  YAML/TOML → pools + pipeline + source (including ${ENV} expansion)
  plugins.py      entry-point discovery for tasks/algorithms/codecs/stores (M4)
  backends.py     where artifact payloads live: inline / content-addressed files / null (M4)
  server.py       read-only HTTP view of a store: /stats, /events, /pipelines (M4)
  cli.py          run/resume/report/watch/export/serve/plugins/validate/demo/bench (+ --shard / --shards)
  __main__.py     `python -m pyattacker`, used by the shard children
  benchmark/      clock (simulated time) / scenario (assumptions as data) / provider (the black-box
                  world) / harness (closed-loop client) / metrics / report — drives pools, not runs
  tasks/          built-in utility tasks: mock.* / fanout / shell.run / file.write_jsonl / jsonl_source
```

依赖方向严格单向：`errors → artifact → task → handoff → pipeline → resource/algorithm → store → runner → cli`，`resource` 不反向依赖 `algorithm`（算法通过 `pool.acquire` 注入）。
`shard`/`merge`/`export` 在内核旁边：它们读存储和流水线流，内核里没东西依赖它们。`benchmark/` 也在旁边，但低一层：它直接驱动 `Pool` 和各算法，从不进一次运行——这正是它的模拟时钟保持精确的原因（见 §8.14）。

---

## 8. 已知权衡与局限（有意为之，事先写明）

1. **停止条件只能尽力而为**：`stop_after_failures` 在准入时和每次流水线完成时算，已经准入的流水线（约 `2 × concurrency` 条）还会跑。重试退避大的时候，挂起意味着很多流水线同时*在飞*，预算耗尽前可能有更多被准入。预算只能止血，不能让时间倒流。
2. **write-behind 会丢尾巴**：尝试和事件批量写（按大小或间隔触发，心跳和运行结束再各刷一次），`SIGKILL` 可能丢最后一批没发的数据。每个检查点和 artifact 都是同步写的，恢复后还是只重跑真正没完成的部分。想每次尝试都提交？用 `--no-write-behind`。
3. **`journal=summary` 牺牲恢复粒度**：不存载荷就没有中间 artifact 可复用，整条流水线得重跑。要恢复能力就必须 `full`。
4. **资源健康靠显式上报**：只有 `lease.report(ok=False)` 才算一次失败；框架不猜。
5. **`quota_aware` 是偏好，不是硬限**：所有候选都超配额时，还是发最好的那个，因为拒活比超支糟。要硬停，就在任务自己的预算耗尽后从任务里抛异常。
6. **任务可以是同步的，也可以是异步的，但事件循环只有一个线程**：普通 `def` 是直接内联调用的，整个执行期间占着事件循环——这也是为什么它的 `timeout_s` 收得下却触发不了：框架根本没有可以取消它的时机。阻塞代码请自己包成 `await asyncio.to_thread(...)`（一行代码，换来不用锁、也不背线程安全包袱的资源池）。
7. **每个库只有一个写入者**：横向扩展就是多进程，各有各的存储（`--shard i/N`），绝不在一个文件上放更多写入者——SQLite 只允许单写入者（WAL 保持一个写入者多个读取者，这就是 `watch`/`report` 能和正在跑的运行并存的原因）。
8. **分片均衡是统计意义的，不精确**：分配是 `hash(pipeline_key) % N`，因为另一种做法（对流轮询）每次数据集变了所有流水线都挪，还破坏恢复。4 个分片 20 条流水线，有时会看到 6/14；规模上去就均匀了。
9. **合并后的报告是并集，不是求和**：分片数变了之后，同一条流水线可能同时在两个分片里，`merge_reports` 按 `pipeline_id` 去重（状态最优者胜，完成时间最晚者平），基于存活下来的行*重算*工作量计数——`attempts_total`、`handoffs_total`。它会报自己折了多少行，这个数字永远不藏。事件日志没法归给两份副本里的哪一份，所以它既不被重算，也不被悄悄混进同一口径：它以 `source_events_total` 的原始形态报出来，名字就说明了它是什么。
10. **流水线是一条线性链**：内核按"节点 + 依赖边"实现，加 `Parallel/Gather` 只是语法糖，但它有意不对外。请在任务内部用 `fanout(...)`：分支在记录里还是一步，代价是重试粒度变成组级。唯一补充是 §4.8 里可选的交接：它改链的*遍历*方式——沿声明的边走，不改拓扑——还是没有 join，没有第二个入口。
11. **`null` 后端牺牲恢复粒度**：丢载荷意味着中间 artifact 没法复用，`resume` 重跑整条流水线——和 `journal=summary` 一样的权衡。blob 文件缺失的行为也刻意一致：`available` 变 false，活重做。
12. **HTTP 端点默认无认证，只绑回环。** 它是看你这次运行*记下来*的东西的调试视图——事件的 `data` 原样呈现、失败流水线上的错误信息也在——而不是 artifact 表的视图，那张表根本没有路由。所以它仍然是"你的数据挂在一个开放端口上"：任务把某行数据打进日志，或者把它写进异常信息里抛出来，就等于公开了它。要绑别的地方，前面自己加代理，绑公网之前先想清楚。
13. **YAML 是 extra，不是依赖**：`dependencies` 为空，声明式层用标准库读 JSON 和 TOML，`pip install pyattacker` 不拉任何东西。`.yaml`/`.yml` 配置要 `pip install "pyattacker[yaml]"`，没有它时加载器读那个文件那一刻抛 `ConfigError`，指明 extra 和文件——检查按文件、按后缀做，绝不在导入时做。代价是想要 YAML 的多一步安装；好处是其他所有人，包括只用 SDK 的，都不为自己从不调的解析器付代价。
14. **基准测试是模拟，数字是假设的产物。** 它在一个人为写定的世界里比较获取算法（容量周期、令牌桶、尾延迟、风暴、三个端点），让"这里哪个算法更好"成为有答案、有种子的问题。它不测任何人的接口方，改一个假设排名就可能变；七个内置算法里两个在这个世界完全测不了——`failover` 只有一个资源池时没东西可切，`least_busy` 就是资源池默认选择策略的另一个名字——场景会声明这一点（标 N/A），不藏在看似合理的数字后面。见 `docs/benchmark.md`。
15. **子进程树只在 POSIX 上保证。** `shell_run` 每条退出路径都杀并回收它启动的进程——正常退出、`timeout_s`、取消、任何其他异常——POSIX 上还给子进程整个进程组发信号，shell 流水线或 argv 程序的后代也一并终止。Windows 标准库没进程组信号机制（`os.killpg` 不存在，`asyncio` 没法向子进程所在组发 `CTRL_BREAK_EVENT`），Windows 上只杀直接子进程，后代可能比任务活得久。离线测试在 POSIX 上验证后代保证，其他平台跳过这两个用例，说明原因。
16. **交接是可选加入的，隔离起来了**（§4.8）。声明了 `control` 的流水线是拿一项保证换另一项：它的游标变成*位置*，不是进度计数（被跳过的槽位没有任务行，`n_tasks_done / n_tasks_total` 对这样的流水线不是完成百分比），它的恢复依赖 `handoffs` 账本可读——这就是为什么缺原子 `commit_handoff` 能力的存储直接被拒，不降成非持久跳转。这个特性 1.0 之前标实验性：上面的保证是稳定部分，写法还可能变。只正向的路径不回访；join 和跨流水线传输都不含；一条几乎全是交接的流水线，说明你要的是图引擎，这个框架不是。

---

## 9. 测试策略与当前状态

**零网络、零外部服务**——全在内置 mock 任务上跑。核心断言：

* `tests/test_lease_safety.py`——§4.2 契约的每一条：异常/取消/超时/逃生通道泄漏/循环 acquire，外加"运行结束后资源池 `active == 0`"。
* `tests/test_runner.py`——★ 恢复：第一轮全失败 → 第二轮 `resume=True` → **较早任务的调用次数不增加**，只有失败的任务重跑，第三轮全是 `skipped`；`journal=summary` 检查点不可用时的行为；重试决策字段；并发上限；关闭条件；导出。
* `tests/test_pipeline.py`——构造期 artifact 类型链式校验、源摘要影响身份、`map(repeats)` 和显式 key。
* `tests/test_scheduler.py`——M2 基础设施：`DelayQueue` 顺序、更早的定时器打断更长等待、取消永不丢挂起条目、`WriteBehindStore` 的缓冲/刷写触发条件，以及状态写入永不缓冲这条规则。
* `tests/test_algorithms.py`——资源获取策略（`sticky` 亲和、`quota_aware` 排序、`least_busy`、`failover`）和资源池等待时间指标。
* `tests/test_m2.py`——通过 `Runner` 端到端验证 M2 承诺：`concurrency=1` 时，一条流水线因重试挂起期间，另一条会完成（按事件顺序断言，不按时序），被停止的运行把挂起的流水线记为可恢复，运行结束不留缓冲事实。
* `tests/test_shard.py`——划分完备（每条流水线恰好一个分片）、确定、内容寻址/跨进程稳定，`parse_shard`/`shard_index`/`shard_env` 校验，`--shards N` 起真实子进程，带重试的资源池流水线在它们之间干净合并。
* `tests/test_export.py`——每种行形态和每种格式，CSV `extra` 列对应 `header_rows` 后才出现的 key，`merge_reports` 按最优状态/最晚完成折重复 `pipeline_id`，CLI 分片路径：带显式存储的 `run --shard i/N`、`--shards N`、JSON 摘要、跨分片存储合并的 `report`/`export`，某分片无处可写时的 `ConfigError`。
* `tests/test_plugins.py`——用注入的入口点提供方做发现和解析（不用安装）：内置项胜出、抛异常的插件只记不传，`use:`/`algorithm:`/存储 scheme 解析都能到插件。
* `tests/test_server.py`——通过真实回环请求测 HTTP 端点：JSON 形态、限制、404，服务器运行期间启动的一次运行出现在 `/stats`。
* `tests/test_backends.py`——超阈值溢出、读时水合、内容寻址去重、`journal=summary` 任何地方不留内容，以及**穿过已溢出检查点的恢复**。
* `tests/test_packaging.py`——`pyproject.toml` 版本和运行的包一致、没意外引入的依赖、每个模块能导入、每个承诺的名都导出了。
* `tests/test_optional_yaml.py`——用户实际遇到的 extra（§8.13）：SDK 在 PyYAML 缺席时照常跑、JSON/TOML 配置能加载、`.yaml`/`.yml` 文件抛的 `ConfigError` 指明文件和 extra、*坏的* PyYAML 暴露自己的错而不是那条提示，CLI 把缺 extra 变成退出码 2。`sys.modules["yaml"] = None` 模拟缺席，所以这一切每次普通测试运行都跑，不只在没 PyYAML 的那个任务里跑。
* `tests/test_benchmark_*.py`——模拟基准，测的是它声称的东西，不是输出：虚拟时钟手算时间线（32 个并发 1 秒睡眠总共花 1 秒，可运行 worker 永不被跳过）、接口方自身动态行为（端点满载时 429 拒绝、一次拒绝消耗未来额度、风暴是时间的函数不是流量的函数）、怎样才公平（两个算法对同一个 `(endpoint, ordinal)` 拿到完全相同的抽样；没东西把环境引用交给算法；同一个种子精确复现同样数字），全程不建网络连接。虚拟时钟还和压缩的实时时钟对照，后者构造上就对，但太慢没法实际用。
* **依赖 YAML 的测试标 `requires_yaml`**，不用 `importorskip()` 保护，所以缺 extra 时开发套件*失败*，不悄悄跳过自己三分之一用例。CI 跑两遍：一遍带 extra（全用例），一遍只 `pip install` 装的包、带 `-m "not requires_yaml"`。哪边是保证才是关键——正面用例正常测，负面用例显式测。发布工作流在构建出的 wheel 上闭环验证，两个方向都做：不带 extra（PyYAML 缺席、JSON 可用、YAML 要 extra）和带它（PyYAML 从已发布元数据解析出来，YAML 配置能过校验）。
* `tests/test_artifact.py` / `test_store.py` / `test_declarative.py` / `test_cli.py`——编解码器、存储语义和两种存储间一致性、配置解析、CLI 端到端。
* `tests/test_errors.py`——`error_class_of`/`is_retryable_class`/`retry_after_of` 作为纯函数：每个 `_STATUS_RULES` 区间、`FatalError`/`TimeoutError`/`ConnectionError` 分支、显式 `.error_class` 优先级，从直接属性和响应头两处提取服务器建议的 `retry_after`。
* `tests/test_handoff.py`——这个进阶特性的端到端测试：无 control 的运行可证明不受影响（`spec_digest` 逐字不变、没新增行）、正向交接跳步骤、沿未声明边返回的指令致命不重试、`END` 把入口 artifact 定稿、**真被 SIGKILL 的进程**在目标处恢复不重跑源任务、已消费的账本行回退到普通 artifact 规则、入口载荷丢失从零重跑、提交原子、已交接的尝试绝不走 write-behind 缓冲、缺这个能力的存储被拒（同存储上无 control 流水线照常工作），校验、`fanout` 拒绝、租约、超时、每个可观测面都固定。
* `tests/test_monitor.py`——`watch` 的终端渲染器：进度条截断/取整、运行范围还是整个存储范围的快照、资源池进度条，泄漏租约/正在停止的指示。
* `tests/test_tasks.py`——`shell_run`：字符串形式和 argv 形式；字符串命令直接拒 `{value}` 插值，argv 命令把替换后的值当单个字面量参数，走 `create_subprocess_exec`，不做隐式 shell 解释。（如果 argv 形式自己显式调了 shell 或其他解释器，比如 `["sh", "-c", ...]`，那个解释器的输入安全语义由调用方负责——这里的保证是"没有*隐式* shell"，不是"对任何程序都安全"。）其他内置 mock 任务只在别的测试文件需要替身时顺带测到，没有专门测试文件。
* `tests/test_subprocess_lifecycle.py`——操作系统视角的 `shell_run` 进程生命周期。取消、`timeout_s` 到期、`Runner` 停止、针对清理本身的取消，都让子进程被杀*并回收*（`os.kill(pid, 0)` 必须失败，同时抓"还在跑"和"杀了没回收"），自己退出的子进程不干预，结果保留。有个测试在*进程还在创建时*取消调用方：故意拉大窗口，方法是持有包装的 `loop.subprocess_exec` 直到测试释放它，因为这时子进程已存在，没有栈帧持有句柄——这是个没法指望测试故意命中的竞态（原版 3.11/3.12 实现只是碰巧在自己内部等取消时才关传输层，这不是 `shell_run` 能依赖的保证）。后代进程测试取消一个字符串命令（shell 正等一条真实流水线），再取消一个自己派生了子进程的 argv 程序，断言每个 PID 都消失——清理只杀直接子进程的话它们就失败。子进程写自己的 PID 表明就绪（没地方固定 sleep），`tracked_pids` fixture 即使测试失败也杀测试见过的每个 PID，失败测试留不下活进程。这两个后代进程测试非 POSIX 平台跳过，给平台原因（见 §8.15）；kill 和存活探测遵循平台自身语义，CI 只覆盖 POSIX 分支。
* `tests/test_tutorial.py`——`docs/tutorial.md` 里每个标了完整程序的代码块（`# tutorial/<name>.py`）都抽取出来真跑，教程不会悄悄烂掉、和真实 API 脱节。
* `tests/test_docs_examples.py` / `tests/test_docs_i18n.py` / `tests/test_docs_facts.py`——文档本身也在被测。每个 `# example/<name>.py`、`# reference/<name>.py`、`# example/<name>.yaml` 代码块都在临时目录里真跑一遍；中文文档被英文文档管着（对照文件存在、围栏代码块按顺序逐字节一致、标题层级、语言切换器、可运行标记，以及每一条相对链接和锚点都对得上）；而那些会悄悄烂掉的事实是被断言出来的、不靠自觉——本文件里的版本号对 `pyproject.toml`、"N 个可运行步骤"这类说法对 `# tutorial/` 代码块的数量、监控端点表里列出的每一条路由对 `StatsServer` 真实应答的路由。行文准确性仍然靠人工评审：机器只查有唯一事实来源的部分。

所有和时间相关的逻辑（退避、熔断冷却）走可注入的 `Clock`，测试用 `tests/helpers.py::FakeClock` 把时间变成可控变量，测试既确定又快。整个测试套件几秒跑完——跑 `uv run pytest` 看当前用例数（本文故意不写死这个数字，每加删一个测试具体数字就过时）——所以没理由不跑。
`ruff check` 在 `pyproject.toml` 配置下干净，每条被忽略的规则都带理由——lint 例外应该是个论证，不是意外。

**已实现（M0–M4，即 0.1.0 计划全部内容）**：五个概念的完整内核、内存/SQLite 存储、任务级检查点与恢复、重试与错误分类、资源池状态机与发布/订阅、7 种获取算法、声明式层、CLI、内置 mock 工具任务、延迟延续（不占 worker 的退避）、尝试/事件 write-behind 批处理、按资源定向唤醒、资源池等待时间指标、带合并报告的确定性分片、三种格式五种行形态、入口点插件、外部 artifact 后端、fanout 辅助函数、只读 HTTP 监控端点。

**已实现（M5，可选启用，1.0 之前标实验性）**：声明式控制流——`Handoff` 指令配流水线上的 `control=`、只追加的 `handoffs` 账本、原子的 `commit_handoff` / `commit_control_transition`、账本优先的恢复（被杀进程在目标处继续，不重跑源任务）、反向遍历（`rewind`、`retry_all`、有限的控制预算）、带访问限定（visit）的发生实例身份与"生效版本 vs 精确版本"的 artifact 读取，以及可选的 `HistoryArtifact` 载荷。没有 `control` 块的流水线可证明不受影响：不写新行，`spec_digest` 逐字节不变。

**留待以后（0.1.0 之后）**：分布式调度器、Parquet 导出、blob 垃圾回收（`FileBackend` 内容寻址，孤儿 blob 安全但永不删）、一等公民 `Parallel`/`Gather` 节点——最后一项只有实践证明一元任务模型限制太大才做。

---

## 10. 里程碑

| | 目标 | 完成标准 |
|---|---|---|
| **M0 骨架** ✅ | 五个概念的内核 + 内存存储 + 线性执行 + 内置 mock + CLI | `pyattacker demo` 端到端跑通 |
| **M1 持久化与恢复** ✅ | 全 SQLite 表、任务级检查点、resume、结构化事件、SIGINT | SIGKILL 后 resume，不重发较早任务 |
| **M2 更聪明的资源与重试** ✅ | write-behind、让出 worker 的退避、按资源定向唤醒、配额感知算法、更细的 `acquire` 指标 | 资源池饱和时退避可观测，能从 `events` 重放 |
| **M3 规模与易用性** ✅ | `--shard i/N` + `--shards N`、合并报告、分片工具、多形态/多格式导出 | 多进程跑同一份数据集 |
| **M4 生态** ✅ | 入口点插件、外部 artifact 后端、fanout 辅助、HTTP 监控端点、0.1.0 打包 | 第三方能发任务包 |
| **M5 进阶控制流（可选）** ✅ | 声明的正向交接（`Handoff`、`control=`、`handoffs` 账本、原子 `commit_handoff`、账本优先恢复）与反向遍历（回退、全量重试、访问记录、有限控制预算、可选的 `HistoryArtifact` 载荷历史） | 交接是持久检查点：被杀进程带入口状态在目标处恢复，无 control 的流水线可证明不受影响 |

---

## 11. 非目标（写进 README，防范围蔓延）

* 不提供 HTTP 客户端 / 接口方 SDK 适配层（任务你自己写；有意的设计，不是缺功能）
* 不做 DAG / 多轮 agent 编排（流水线保持线性；fanout 在任务内部做，唯一例外是 §4.8 里可选的交接——没有声明的图，没有 join，没有跨流水线编排）
* 不做语义归约（accuracy / pass@k / 任何跨流水线聚合）
* 不做服务化 / 网关 / 代理
* 不做数据集存储（它只接可迭代的种子流 + 一个 `jsonl_source` 工具）
* 不做分布式调度（`--shard` 就是多进程上限）

## Suite 存储与作用域

Suite 在线性流水线执行之上增加稳定的实验身份，不引入流水线之间的依赖。每个成员有自己的本地资源名映射，显式别名指向共享 Pool 实例。目录库保存定义摘要、提交成员关系和数据源耗尽状态，各数据子库拥有本地原子的任务及 visit 检查点。拆分存储不创建独立调度器，也不要求跨库检查点事务。每次 run 的流水线和任务执行结果保存在对应数据子库，通过 SQLite 触发器与检查点在同一事务中更新，恢复后仍保留原 run 的状态、跳过记录、尝试数和时间戳；可变流水线行继续作为恢复依据。载荷不保留历史版本，历史导出会省略已被后续 run 接管的产物。选择性执行以只读方式打开无关成员库，只结束选中成员的运行记录。

[完整指南与示例](suites.md)。
