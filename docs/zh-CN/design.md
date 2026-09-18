# pyattacker 设计文档

[English](../design.md) | **简体中文**

> 版本：0.1.0（M0–M4 已完成，M5 高级控制流进行中 —— 见第 9 节“已实现 / 留待以后”）
> 一句话概括：**一个以工件（artifact）为中心的异步任务编排框架，用流水线作为完成单位，把资源池作为唯一的共享面。**
> 它不碰网络、不做归约、也不做 DAG 调度 —— 只负责“把数万条彼此独立的流水线可靠、可恢复、可观测地跑到完成”。

---

## 1. 范围与边界

| 框架负责的内容 | 框架不负责的内容 |
|---|---|
| 任务编排、并发控制、资源租用与回收 | HTTP 请求、认证、SSE 解析（openai/anthropic 协议是你自己的事） |
| 工件持久化（产出即持久化）与任务级检查点 | 语义归约（accuracy / F1 / pass@k —— 任何跨流水线聚合） |
| 失败分类、重试决策、退避策略、熔断 | 数据集下载与清洗（它只接受可迭代的种子流） |
| 结构化日志、运行清单、实时监控、导出 | 多轮 agent 状态机、DAG 依赖调度、服务化 / 网关代理 |
| 资源池发布/订阅、健康与配额核算 | 分布式调度（跨进程分片属于后续增量） |

**“不做归约”的精确边界**：框架确实会计算*运行统计*（流水线数量、
成功率、延迟分布、错误分类、资源池利用率），但绝不会对工件执行
*语义聚合*。如果你想要 accuracy，有两条路径，两者都不需要
框架介入：

1. 导出工件（`export` / `report.export_jsonl`），在外部自行计算；
2. 编写一条 **汇聚流水线**（sink pipeline）：用一个任务把结果发布到某个资源/总线上，由订阅者
   消费它们 —— 归约就变成你用框架原语自己搭建的东西。

---

## 2. 核心不变量

这六条是设计的基础；任何改动都不得破坏它们：

1. **流水线之间零语义耦合。** 唯一的共享面是资源池（包括总线
   信号）。因此并发是平凡的：每条流水线一个协程，没有全局依赖图。
2. **任务 = 一元 `(Artifact) -> Artifact` 函数**，线性串联，不分支、不汇合。
   需要 fanout（扇出）/循环/批处理时，由任务自己在内部执行 `asyncio.gather`。任务也可以
   改为**交接（handoff）**（§4.8，高级特性，需显式启用）：链仍然是一串没有汇合的一元任务，
   任务字面上仍是 `(Artifact) -> Artifact | Handoff`；改变的只是*遍历顺序* ——
   沿声明的正向边遍历，而不是拓扑或工件契约。
3. **工件一经产出立即持久化**，因此**检查点粒度 = 任务**，而不是流水线。
4. **失败即异常**，不引入状态机。重试有两个正交的边界：
   任务级（用相同的输入工件重放）和流水线级（完整重跑）。
5. **资源只能通过租约使用，且必须归还**；任务结束时绝不持有资源
   （见 §4.2）。
6. 持久化层只写入**事实**（工件 / 尝试 / 事件），绝不写入语义指标。

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
| **工件（artifact）** | 任务被持久化的状态。内容寻址（blake2b），一经产出立即持久化；`seq=-1` 是流水线的种子输入 | `artifact.py` |
| **任务（task）** | 一元 `(artifact) -> artifact` 函数；可携带 `resource` / `algorithm` / `retry` / `timeout_s` | `task.py` |
| **流水线（pipeline）** | 任务的线性链，是**完成**与**恢复**的单位；`map(seeds)` 将它展开为彼此独立的实例 | `pipeline.py` |
| **资源（resource）** | 一类可租用的外部能力（一个端点 / 一个 key / 一个本地 worker） | `resource.py` |
| **资源池（pool）** | 一组资源 + 一个默认获取算法 + 一份状态事件日志 | `resource.py` |
| **租约（lease）** | 一次租约；`lease.client` 是工厂构建出的可用对象；必须归还 | `resource.py` |
| **算法（algorithm）** | “如何从资源池中取出一个资源”的策略（拉取侧） | `algorithm.py` |
| **总线（bus）** | 轻量的跨流水线信号总线（推送侧） | `resource.py` |

### 3.1 流水线同一性（identity）是内容寻址的

```
pipeline_key = blake2b(canonical_json({
    spec_digest,          # v2 task-chain fingerprint: config/parameters/children/policies + source digest
    seed_digest,          # digest of the seed contents (that row of the dataset)
    repeat,               # which sample of pass@k this is
}))
```

三个直接推论：

* **幂等**：重跑同一个样本不会产生第二条流水线，`resume` 只会跳过
  那些已经成功的流水线。
* **可复现**：`spec_digest` 包含每个任务的**源码摘要**，因此改动任务代码等同于
  换掉流水线，旧检查点不会被错误复用（`include_code=False` 可关闭这一行为）。
* **免费获得 pass@k**：`template.map(seeds, repeats=3)` 一次展开为三条独立流水线，
  它们共享同一个种子摘要。

启用了交接的流水线（§4.8），其 `control` 块**只有存在时**才是该指纹的一部分：
不含 control 的流水线，其摘要与以往完全一样，仍是那份任务指纹列表，因此该特性
不会使任何已存储的摘要、检查点或分片分配失效（而 `v3:` 迁移则会）。

规格（spec）指纹带 `v2:` 版本前缀。显式 key 保留其提供的标识，但
已存储的 spec/seed 摘要必须匹配才能跳过或恢复。旧存储仍可读取；
默认 ID 会变化，显式传入的旧 key 会冲突。闭包、全局变量、端点选项和
资源池默认值都要求声明任务的 config/version。迁移与外部副作用幂等性见
[恢复同一性](reference.md#恢复同一性)；检查点不保证恰好一次的调用。

---

## 4. 关键机制

### 4.1 任务级检查点与恢复

每个任务成功后，按顺序发生三件事：

1. `store.put_artifact(...)` —— 工件被持久化（内容寻址 + 去重）；
2. `store.record_task(...)` —— 记录任务的最终状态以及耗时、错误和用到的租约；
3. 检查点游标前进 —— 中间任务执行 `store.upsert_pipeline(record.n_tasks_done = seq + 1)`，
   最后一个任务则执行终结性的 `store.finish_pipeline(..., n_tasks_done = n_tasks)`。

对最后一个任务，顺序是**先标记 final，再写终结状态**：`mark_final` 先于 `finish_pipeline` 运行，
游标前进是*随* `finish_pipeline` 一起发生的，而不是单独写入。这两种顺序
都很重要，各自覆盖对方无法修复的撕裂写入：在终结写入之后、
`mark_final` 之前崩溃，会留下一条永远 `succeeded` 的流水线（永远被跳过），其最终工件从未
被标记；而在终结写入之前崩溃，代价只是重跑最后一个任务 —— 也就是文档所述的
至少一次（at-least-once）边界。

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

先查询交接分支，而且它被刻意限定得很窄：只有**最新的活跃**账本行（`handoff_id > handoff_floor`）才可能
处于待处理状态，因为只向前推进的交接其目标严格递增，所以 `to_seq` 低于游标的行
已被后续前进消费掉，适用普通的工件规则。已被消费的行
因此不付出任何代价，待处理的行则从目标处恢复，不重跑源任务。

该终结流程的**任一步** —— final 标记或写入终结状态 —— 失败，都由恢复路径
兜住：该行完全保持原样（原本的失败、游标、所属 run），
`pipeline.terminal_repair_failed` 会连同阶段一起记录这次尝试，因此后续运行仍会报告
原始原因，而不是修复自身的错误。若放任这类异常逃逸到 worker 的
通用内部错误路径，就会用该错误重写这一行，并摧毁修复本应
保全的来源信息，因此整个终结流程是一次受控操作。同样的道理也解释了
为什么当存储提供 `settle_pipeline` 时，终结状态转换（状态、游标、所属 run、失败字段）
是单次存储写入，以及为什么工件已经是 final 时会跳过 `mark_final` —— 并
针对无法观测到这一点的崩溃场景，将其记录为幂等。

由于该行保留原来的所有者，这次失败不会出现在执行修复的那次运行的 run 级
统计中；它计入 `RunReport.repair_failures`，CLI 退出码和摘要都使用这个计数，
因此失败的修复绝不会被报告为一次干净的运行。对于没有 `settle_pipeline` 的存储，
两次写入的回退路径会分别归类各个步骤：写入终结状态失败属于修复失败（见上文），
而元数据清理失败则不属于 —— 该行已经持久地处于 `succeeded`，残留的失败文本
是这类存储已记录的降级保证，会以 `pipeline.terminal_cleanup_failed` 报告。

关键收益：**如果任务 C 失败，只需重跑任务 C，任务 B 的请求不会重发**；而且
由于种子工件也会持久化，**恢复不依赖原始数据集文件**。

### 4.2 ★ 租约安全契约

在任务内部获取/归还资源，是**你的代码与
框架之间最关键的一处交互**。契约如下：

| 场景 | 保证 | 实现位置 |
|---|---|---|
| 正常结束 `async with ctx.acquire(...)` | 退出时同步归还 | `_LeaseGuard.__aexit__` |
| 块内抛出异常 | 同上 —— `__aexit__` 仍会运行，且不会吞掉异常 | 同上 |
| 在循环内 acquire→use→release | 每轮迭代都立即归还，因此并发真正得到让出（占用不会累积） | 取决于你的写法 + 同一机制 |
| `CancelledError`（外部取消 / 超时） | 在 `finally` 中同步归还 | `Runner._execute_task` |
| 逃生通道 `await ctx.acquire_lease()` 却忘了归还 | 任务结束时**强制回收**，并记录 `lease.leaked` 事件 + 计数器 | `TaskContext.reclaim_now` |
| 任务返回后仍持有租约 | 不可能：先回收、后持久化，且两者都是同步的 | 同上 |

这一点成立的根本原因：**回收是纯同步函数**。
`reclaim_now()` 不执行任何 `await`，`lease.release_now()` 同样如此，因此
`CancelledError` / `asyncio.wait_for` 超时 / 任何异常都**无法打断**它。
资源池状态的分配与释放全部同步执行；在单线程 asyncio 下不存在
“先检查后被抢占”的窗口，因此也就不需要锁。

三个配套的设计选择：

* **资源健康由任务显式上报**（`lease.report(ok=False)`）；框架不会
  替你猜测“这次失败是不是资源的问题”。好处是健康的端点
  不会因为一次 JSON 解析错误而被熔断。
* **泄漏可观测**：`lease.leaked` 事件 + `PoolStats.leaked_total` + `RunReport.leases_leaked`；
  需要严格模式时，使用 `strict_leases=True`，一旦泄漏该任务立即失败（`LeaseLeakError`）。
* **疑似死锁会发出警告**：当一个任务已经持有本资源池的某个资源，
  又向同一资源池申请另一个资源，而该资源池没有空闲容量时，等待超过
  `deadlock_warn_s`（默认 5s）就会发出 `acquire.suspected_deadlock` 事件。

### 4.3 资源池

**状态机**（`state_at` 惰性推进，无后台任务）：

```
READY ──consecutive failures ≥ degrade_after──▶ DEGRADED(blocked_until = now+cooldown_s) ──cooldown expires──▶ READY
   │
   └──consecutive failures ≥ dead_after──▶ DEAD        (report(ok=True) can pull DEGRADED/DEAD back to READY)
REVOKED ◀── explicit revoke / revoked from within a task
```

注意：冷却到期**不会重置** `consecutive_failures`（只有成功时才清零）。
否则，当“降级阈值 < 死亡阈值”且资源池只持有一个资源时，每次冷却
都会抹掉计数器，`DEAD` 将永远无法达到。

`factory` 抛过异常的资源遵循同一状态机（`degrade_after`/`dead_after`/冷却），
而且冷却到期*确实会*清除该资源已存储的客户端错误，因此下一次租用尝试时
会再次调用 factory —— 对暂时性的 factory 失败而言，DEGRADED 是真正的第二次机会，
而不只是 DEAD 之前的延迟。

**发布/订阅**使用两个语义不同、实现也不同的通道：

| 通道 | API | 用途 |
|---|---|---|
| 拉取（获取） | `async with ctx.acquire(**selector) as lease:` | 等待 + 租用；选择器支持 `id`/`kind`/`tags`/`options` 以及点分路径（`"a.b"`） |
| 推送（订阅） | `ctx.subscribe(pool, ["resource.published"])`, `ctx.bus.subscribe(topic)` | 接收资源发布/退役/降级/恢复信号 |
| 推送（发布） | `ctx.publish_resource(pool, resource)`, `ctx.revoke_resource(...)` | 任务在运行时发现新端点后将其注入，其他流水线可以立即租用它 |

资源池中的每次状态变化都会发出一个 `ResourceEvent`，它同时喂给**四类**消费者：等待者
（唤醒）、订阅者、`events` 表（结构化日志）和监控快照。一个事实，四种视图。

### 4.4 算法与重试是两条正交的轴

这是最容易混淆的部分，因此实现刻意把两者分开：

| | 算法 | 重试 |
|---|---|---|
| 时机 | 做工作**之前**：如何等待/选择资源 | 工作失败**之后**：是否再试一次 |
| 输入 | 资源池容量/健康/配额 | 异常类型 + 错误分类 |
| 内置 | `immediate`（不可用即失败） / `wait`（默认） / `backoff`（指数退避 + 完全抖动） / `least_busy`（选最空闲的） / `failover`（按顺序切换资源池） / `sticky`（停留在本流水线已经用过的资源上） / `quota_aware`（优先剩余配额最多的） | `Retrying(max_attempts, on, base, factor, cap, jitter, max_total_s)` |
| 声明位置 | `@task(algorithm="backoff")` 或 `pool.algorithm` | `@task(retry={"max_attempts": 4, "on": ["RetryableError"]})` |

`failover` 会按顺序立即尝试列表中的每个资源池；如果都没有容量，它的 `fallback`
（默认是 `Wait()`）只挂起在 `pools[0]` 上 —— 不会把 fallback 在整个列表上轮一遍。
请把这个列表理解为“先试这些，最后落在主池上”，而不是“谁先空出来
就等谁”。

**失败分类**（`errors.error_class_of`，纯函数，可单元测试）：
`status`/`status_code` 属性中的 408/504 → `timeout`，425/429 → `rate_limit`，5xx → `upstream`，4xx → `fatal`；
`TimeoutError` → `timeout`；`ConnectionError` → `connection`；`ValueError/TypeError/...` → `invalid`；其他
一切 → `unknown`。你也可以直接接管，方法是抛出
`RetryableError(msg, error_class=..., retry_after=...)` / `FatalError`，或者注册自己的分类器。

**默认不重试**（`max_attempts=1`），保持简单的“失败就是失败”语义；需要时
再显式开启。
**每个重试决策都会被持久化**（`attempts.decision_json`）：`{retry, reason, delay_s, error_class, attempt, max_attempts, retry_after}`，
其中 `reason ∈ {ok, retryable, attempts_exhausted, policy_declined, total_budget}`。`delay_s` 始终存在
（无需延迟时为 `0.0`），因此读取该 schema 的代码不必防范键缺失。
于是“它为什么重试了 5 次 / 为什么放弃了”可以直接从数据库里查出来，而不必
翻日志去猜。

### 4.5 延迟延续与批处理事实（M2）

两个吞吐量机制，它们不改变执行模型，只改变调度与持久化的方式：

**重试退避让流水线挂起，而不是让 worker 睡眠。** `Runner._execute_task` 只运行一次
尝试，然后把重试决策交回 `_drive`；当策略希望再试一次时，流水线状态
进入 `DelayQueue`（`scheduler.py`），worker 立即去取其他工作。单个 pump 任务
在定时器到期后把挂起的状态移回工作队列。结果：

* `concurrency` 终于名副其实：它指在飞的尝试数，而不是正在熬 30 秒退避的流水线；
* worker 空闲并不代表运行结束 —— `_wait_for_completion` 也会等待挂起的流水线，
  因此退避绝不会被误认为中断；
* 关闭时先取消 pump，给每个 worker 发一个哨兵，仍在挂起的一切都会
  被记录为 `pipeline.deferred_interrupted` —— 可恢复，绝不静默丢弃。

定时器经由可注入的 `Clock`，而 `interruptible_sleep` 让 `clock.sleep` 与唤醒
事件赛跑，从而提供所需的两种行为：使用真实时钟时，新推入的更早定时器会截断长等待；
使用假时钟时，pump 会立即推进虚拟时间（因此测试保持确定性）。

**只追加的事实按批处理。** `WriteBehindStore` 缓冲尝试与事件，并按批刷写
（大小阈值、时间间隔、任何读 API、运行心跳、运行结束）。状态写入 ——
`pipelines`、`tasks`、`artifacts` —— 始终直接落盘，因为尚未持久化的检查点
就不算检查点。因此 `SIGKILL` 可能丢掉最后一批历史，而所有检查点保持
完好；`--no-write-behind` 用吞吐量换取每次尝试立即提交。

### 4.6 监控：关心流量与阻塞，而不是指标

```python
snapshot = runner.stats()          # live in-process snapshot
# or cross-process: pyattacker watch runs.db   ← a read-only connection to the same SQLite file (WAL: one writer, many readers)
```

面板展示：流水线状态分布、p95/最大延迟、各任务计数，以及每个资源池的
`active/capacity`、`ready/degraded/dead`、`waiting`、吞吐量与泄漏计数器，以及最近的错误。
它也可以编程使用：`monitor.render_snapshot(stats)` / `monitor.watch(store)`。

### 4.7 完成以计数为准，worker 生命周期受监督

当 `pipelines_done` 达到 `pipelines_admitted` 时，一次运行结束。该不变量只有在每个已接纳的
流水线最终都走到 worker 自身的某条终结路径时才成立 —— 而非 `CancelledError` 的
`BaseException`（例如存储钩子抛出的自定义子类）哪条都走不到：它会逃出
worker 循环，worker 任务随之结束，而完成判定所等待的计数器永远不会前进。过去，这次运行会
永远、静默地等待：没有错误、没有退出码、没有终结状态行、没有事件。

因此，worker 的生命周期与流水线记账分开观测，方式是为每个
worker 任务挂一个完成回调。它还能看到处理器链 *之后*（队列或 worker 的清理工作）发生的死亡，并把它归为
一个运行级故障：

* **先停止，绝不记账。** 运行被硬停止（`stop("worker_crashed")`，不再接纳新流水线），
  `pipelines_done` 为了满足计数器不变量而刻意 *不* 递增：这次运行并非
  正常完成，真正释放等待者的是这次停止。
* **流水线被置为终态。** 一行没有存活 worker 拥有的 `running` 记录，正是这条路径
  要消除的静默状态，因此在途流水线被记为 `failed`——若这次运行已在收尾，则记为 `interrupted`——
  并把逃逸的异常写到该行上。一条在 worker 死亡时已经处于终态的流水线
  保留它已经取得的状态：历史不会被改写。**做决定的是持久化的行，而不是运行器的
  内存记录**——存储并不需要回写交给它的 `PipelineRecord`，而且该
  条目可能是经由重试队列返回的状态，而它早先做出的终态写入已经
  持久化，因此信任内存就会把一行 `succeeded`（或一次带有自身
  来源信息的普通失败）改写成这次崩溃。如果该行尚不存在就创建它，方式与内部错误路径
  相同。
* **工作队列被释放。** 满队列只会由 worker 排空，因此接纳流程和关闭
  哨兵通过一个感知中止的 put 移交条目，一旦有 worker 死亡就放弃。没有它，
  *生产者* 就会挂起在已死 worker 的队列上，运行会提前一步卡住。
* **故障是显式的。** `runner.worker_crashed` 会指名流水线、异常及其回溯，
  `run_async` 会在运行记录以 `interrupted` 关闭之后抛出 `WorkerCrashed`——原始异常
  作为 `__cause__`。如果存储连这次崩溃也无法记录，就走既有的 `StoreUnavailable` 致命
  路径，而不是重试一个已损坏的存储。

`KeyboardInterrupt` 和 `SystemExit` 是边界：asyncio 会把它们从任务中重新抛出，让事件循环
停止，这意味着完成回调根本不会运行。worker 内部一处范围很窄的守卫会在它们继续
传播之前记录行和事件，但循环仍然会拆除：调用方看到的是自己发出的中断，
而运行记录不会被关闭。*由任务* 抛出的 `BaseException` 与此无关——任务
自身的处理会把它收住，就像处理普通异常一样。取消同样不受影响：被取消的
worker 仍会把它的流水线标记为 `interrupted` 并重新抛出，监督机制也不会把一次刻意的取消
变成崩溃。

### 4.8 进阶：交接 —— 声明式正向跳转（可选启用，实验性）

下文仅正向的契约仍是 v1 的路径。启用反向的声明使用 §4.8.8 中
感知访问的契约以及[反向指南](backward.md)；仅正向的遍历仍使用其既有的
账本/游标恢复，不需要新的同一性或预算要求。

以上内容描述的是普通流水线：一条链一次走一个任务，每个任务返回
下一个任务要消费的工件。本节介绍唯一一项会改变该链 *遍历* 方式的功能：
某一步可以告知链的其余部分不再需要运行——答案已经足够好、
样本不在范围内、已有缓存结果——于是它可以 **向前跳过**，而不必运行它并不
需要的站点，也不必把分支藏在一个任务内部，或因抛错而把流水线记为失败（那是在说
谎）。这项功能被刻意与其他部分隔开：

* **可选启用** —— 没有 `control` 声明时，流水线行为与之前完全一致，连
  `spec_digest` 也逐字节相同（见 §3.1）。本节内容不适用于未主动要求它的流水线；
* **进阶层级** —— 不是因为它难以调用，而是因为它改变了执行模型。它
  记录在自己的标题下、以次版本号发布，并标记为 *1.0 之前实验性*：下文
  的保证是稳定的部分，而写法（`Handoff`、`control`）仍可能变化；
* **在此模型中仅正向** —— 交接只能向 *前* 跳过：`control.edges` 和 `Handoff.to()` 绝不
  获得隐式的反向语义。这项能力的动因场景恰恰是相反的方向（校验器把
  不良的模型输出 **送回** 生成器以重新采样）。§4.8.7 规定了那个
  模型，§4.8.8 把它实现为一个 *单独声明* 的可选启用层级，因此本节描述的
  正向契约保持不变，而不是被扩展。

#### 4.8.1 传递载体是返回值，而不是控制流异常

任务通过 **返回** 一个框架自有的指令来交接，而不是返回一个值：

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

`Handoff.to(target, value=UNSET, *, reason="")` 指定一个目标——任务名、任务的 seq，或 `"end"`；
`Handoff.end(value=UNSET, *, reason="")` 以该值作为最终工件结束流水线。
`UNSET`（任务层用来表示“未给出”的哨兵值）意为“目标以 *本*
任务收到的工件进入”；显式给出的值会成为它自己的载荷工件。`None` 是合法的载荷，
因此只有 `UNSET` 表示“复用”。

由于该指令是一个 **返回值**，所以它不是失败，也不存在新的失败路径：

* 重试策略永远不会被查询，因此 `retry.on=(Exception,)` 和 `retry_unknown=True` 无法把
  交接变成重试，也不会新增任何 `decision.reason` 取值；
* 不涉及任何异常类，因此任务侧的 `except Exception:` 或 `try/finally` 无法吞掉或
  取消这次传递——把控制转移藏在异常里，正是本设计要避免的；
* 租约在退出时与成功时一样被归还：`async with ctx.acquire(...)` 已经
  归还了它们，`finally: ctx.reclaim_now()` 仍会运行。`strict_leases=True` 加上一个泄漏的租约
  仍然是任务失败，交接不会生效；
* 取消和 `timeout_s` 不受影响：被取消或超时的尝试永远走不到那个返回。

这份诚实的代价是被刻意接受的：交接只能发生在任务能够 `return` 的地方，因此调用栈
深处的辅助函数必须把该指令向上传回。这正是预期的权衡——显式、可审查的
交接胜过看不见的控制转移——而且它让任务的签名保持诚实
（`-> Handoff | Report`）。

#### 4.8.2 交接是一次持久化的状态转移，而不是控制流把戏

运行器在尝试内部拦截该指令——在它被编码为工件之前——并把它
变成一次有记录的跳转。五项事实一起写入（见 §4.8.4）：

| 事实 | 位置 | 含义 |
|---|---|---|
| 源任务行 | `tasks.state` | `handed_off`——它干净地结束，没有产生工件 |
| 尝试行 | `attempts.outcome` | `handed_off`，`decision` 为空（本来就没有决策） |
| 入口工件 | `artifacts` | 目标的输入：源任务收到的工件，或一个新载荷 |
| 账本行 | `handoffs` | from/to、入口引用、是否被复用，以及作者给出的原因 |
| 游标 | `pipelines.n_tasks_done` | 目标的位置（`END` 时为 `n_tasks`） |

正是账本让“这条流水线的任务列表为什么跳过了某些站点”可以直接从
存储中得到答案，它也是 **恢复的事实来源**。`pipeline.handoff` 事件是审计踪迹：
硬杀可能丢失事件，而账本始终权威——这是唯一需要记住的不对称之处。

#### 4.8.3 入口状态总是有一个位于自身地址的持久化引用

不带值的 `Handoff.to(target)` 会记录 `state.artifact.id`——即本任务收到的工件——因此
账本的 `entry_artifact_id` 永不为 null。显式给出的值由项目的 `CodecRegistry` 编码，
并写成一个普通工件，其 `seq` 在**提交内部**按 `n_tasks + k` 分配（`k` = 该流水线
已记录的交接数）。于是文档化的工件顺序变为：

```
seed (-1)  →  chain (0 … n-1)  →  handoff payloads (>= n)
```

有两个后果很重要。载荷永远不会覆盖任务槽位（这正是天真的“把它写进槽位
`t-1`”设计会掉进的陷阱），而且它是以发起交接的那个任务的名字记录的。此外，
`Artifact.seq` 不再全局等同于任务位置：对链上的任务它是任务位置，
对种子是 `-1`，而一旦启用控制流，它就是大于等于 `n_tasks` 的载荷地址。那些
只会看到链的读取方保持其既有语义。

#### 4.8.4 提交，以及它所支持的恢复规则

交接是一个检查点，因此它的写入顺序是契约的一部分。存储新增一项 **可选**
能力 `commit_handoff(record, *, task, attempt, payload=None, cursor, final=False)`——沿用
`resources()`/`settle_pipeline` 的先例——它在一次操作中 **原子地** 完成整个转移：终结
源任务、插入这条已交接的尝试、持久化载荷（分配它的地址）、追加账本
行，并在保持流水线 `running` 的同时移动游标。对 `END`，它还会在同一次提交中
把入口工件标记为最终，并把流水线结算为 `succeeded`。

原子性是这项能力的 *要求*，而不是额外好处，因为这里刻意不设第二套
恢复协议：无法作为一个单元提交的存储，直接不暴露该方法；在这样的存储上打开一条
声明了 `control` 的流水线会快速失败，并抛出指名该能力的 `ConfigError`。这条规则
存在的目的是防止出现“静默地不持久”的交接。（`WriteBehindStore` 会转发这项
能力，先冲刷缓冲的尝试和事件，并把当前这条已交接的尝试经由
提交写入，而不是经由它的缓冲区。）

于是恢复只有一条新分支，它在那些线性终态规则 **之前** 被查询：

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

每次从种子重新开始时，都要在执行任务之前通过 `reset_pipeline(record)` 把
`rec.handoff_floor = newest ledger ID` 与重置后的游标一起持久化。重置会原子地移除当前任务行
和链上的工件（seq 0..n-1），清除先前的最终标记，并把游标/水位一起写入。
种子工件与高位载荷工件、尝试、事件和交接都作为历史保留。这样就在不删除历史的前提下，把交接排除在一次被放弃的执行之外；
在被恢复的目标处，水位保持不变。SQLite 以 0 为默认值迁移该列，
只读读取方也容忍它缺失。失败的 SQLite 交接事务会在之后任何
事件或清理写入提交之前回滚。完成时会选定一个最终工件，清除较早的最终标记。

只有最新的活跃行才可能是待处理的，因为仅正向的交接其目标是严格递增的——
低于游标的 `to_seq` 必然已经被越过。必须先查询 `END` 分支，因为提前的
`END` 在 `n_tasks - 1` 处根本没有留下工件，线性终态修复甚至无法判定
这种情况。

#### 4.8.5 游标是一个位置，遍历仍然在结构上终止

`n_tasks_done` 保留它的字段和“下一个要运行的 seq”这一含义。对启用了控制流的流水线，它是一个
**位置**，而不是已执行任务的数量：被跳过的槽位从未运行，因此它们的任务行不存在，
`n_tasks_done == n_tasks_total` 也不再意味着“每个任务都运行了”。这里没有引入新的进度字段，
任何界面也不得为这样的流水线把 `n_tasks_done / n_tasks_total` 渲染成完成百分比
（`report`/`watch`/`/pipelines` 正是因此才在旁边暴露交接数量）。

终止仍然是结构性的，而不是靠预算：交接只能指向严格更晚的位置，因此
游标严格递增，链至多被走一遍。这正是本版本无需循环预算的原因——
也是为什么 v2 的反向场景不引入预算就无法加入（§4.8.7）。

#### 4.8.6 声明边，以及校验会（和不会）检查什么

```python
pipeline("qa", retrieve | ask | judge | report,
         control={"edges": {"judge": ["report", "end"], "ask": ["report"]}})
```

边是 **声明出来的，而不是推导出来的**：沿未声明的边返回 `Handoff` 是致命错误（绝不是
静默跳转，也绝不重试），而每条已声明的边都会在构建流水线时被解析并做范围检查：

* 源和目标都必须存在，重复出现的任务名必须用它的数字 seq 消歧
  （错误信息会说明匹配到了哪些 seq）；
* 目标必须严格晚于它的源（仅正向）；
* `"end"` 是合法目标，但从最后一个任务发起时除外——那里它没有任何效果，会被拒绝；
* `edges` 块没有未知键——`mode` 被刻意省略，因为正向模式只有
  一种。反向操作是单独的键（`rewind`、`retry_all`、`max_handoffs`），由同一个
  入口点校验，并在 §4.8.8 中规定。

校验 **只做结构检查**。交接的载荷是一个任意实参，而不是源任务通常的
返回类型，因此 `source.returns -> target.accepts` 被刻意不检查：它会拒绝合法的
交接（比如 judge 把 `value.more_queries()` 交给 `ask` 步骤），又会接受非法的交接。

它新增的一条标注规则是：标注中的 `Handoff` 成员是一种逃逸。在 `returns` 一侧，
`-> Handoff | Report` 按 `Report` 接链，而单独的 `-> Handoff` 可与任何类型接链，因为这样的任务
在那条路径上不产生工件。逃逸只适用于产出/返回标注；接受
一侧保持不变，因为运行器从不把指令当作工件传递。声明式层接受与 `pipeline.control` 相同的块，使用
字段路径（`pipeline.control.edges['judge'][0]`），并经由同一个入口点校验，因此 `validate` 和
`run` 会以退出码 2 拒绝相同的配置。

解析后的控制计划会防御性地复制任务名和目标序列，然后冻结该映射。
运行时拓扑在验证之后无法更改，也不会与计算出的 `spec_digest` 发生偏离。一个
无法编码的显式交接载荷会抛出 `FatalError`，因此激进的重试策略无法重放
返回了该非法指令的任务。

#### 4.8.7 本版本刻意不包含的内容

整项能力的动因场景是 **相反** 的方向，把它写下来也是
对这项能力承诺的一部分：

```
ask(temperature=0.2) ─▶ validate ─▶ (invalid) ⇢ revoke to ask(temperature=0.7) ─▶ validate ─▶ …
```

模型评估是采样，而不是函数调用：结构化输出步骤经常产出
结构上无效的结果，而校验器能识别出来。此时流水线应当 *回到* 生成器
再试一次，可能还带着工件中携带的不同参数。任务内部的循环无法很好地表达
这一点——它把生成、校验和中间步骤塌缩成一条记录、一份租约历史、
一个重试策略和一个超时，于是“这个样本需要重新生成几次”恰好在它本身就是
测量目标的地方变得不可见。撤销式交接会让每次重新生成都成为生成步骤的一次真实访问，
拥有自己的尝试记录，并让整件事崩溃后可恢复，因此对数千个样本的运行
仍能在样本中途恢复，而不是从头重跑。正是它决定了这里若干记录决策的
形态，也正因如此，下面的模型现在就写清楚，而不是等到以后才发现：

| 未包含的内容 | 原因，以及需要什么 |
|---|---|
| 反向/撤销式交接 | 这项能力的动因场景：校验器把工作 **送回** 生成器。它需要一个访问模型——任务和尝试上的 `(seq, visit)` 同一性、一个持久的按 seq 计数并在入口记录的同一次提交中推进的计数器、感知访问的 RNG（`ctx.seed` 目前是 `digest(pipeline_id\|seq\|attempt)`，因此再次访问会看到完全相同的随机性）、循环预算（终止不再是结构性的），以及不会把访问次数呈现为完成度的进度报告。这里的记录决策——账本、入口工件地址、原子提交、位置游标——都是为了让那个模型可以在不改动它们的前提下加入而选定的，而 §4.8.8 把它作为单独声明的可选启用层级加入。 |
| 声明的 DAG、join、fan-in | 链保持为链。交接是关于一条流水线的调度语句，而不是图的边。 |
| 跨流水线的交接 | 流水线在语义上保持独立；唯一共享的界面仍然是资源池。 |
| 运行时临时造出的目标 | 边是声明出来的，因此拼错或类型错误的目标会大声失败，而不是静默地重塑流水线。 |
| 来自 `fanout` 分支的交接 | 一个组在记录中是一个步骤（`fanout` 在一个任务内部运行它的子任务），因此控制转移无法归因到 N 个并发分支中的某一个。返回的指令会让该组以清晰的 `FatalError` 失败，而不是在收集到的载荷内部传递。 |
| 载荷类型检查 | 见 §4.8.6：若没有自己声明的载荷契约，它就没有可靠的定义。 |

#### 4.8.8 进阶 v2：回退、全部重试与可选载荷历史

它作为一项单独的可选启用能力实现：`control.rewind` 声明严格更早的目标，
`control.retry_all` 声明源，`control.max_handoffs` 限定遍历。回退需要显式
由作者选定的入口状态；全部重试会解码绑定时捕获的原始种子。携带历史的
载荷是可选的，且绝不驱动调度。[反向指南](backward.md) 描述了该接口。

持久化的遍历记录拥有按 seq 的计数器、生效的槽位到访问映射以及待处理的入口，
包括它那一次确切的输入发生实例。每个全新入口都会分配一次访问；恢复会复用待处理的访问。
成功时会把输出/任务/尝试/生效映射/游标一起提交。控制转移还会提交
后缀失效、预算消耗和目标入口分配。游标比较和历史
完成行无法判定一次反向转移是否已被消费。在 seq 0 处恢复是一次真实的入口。

访问会保留 visit 0 的任务/工件同一性，为后续 ID 加上限定，并参与 RNG。
回退/全部重试会保留历史记录。预算计数在恢复和载荷缺失回退后仍然保留。
完成与终结使用确切的发生实例同一性。反向重新入队会释放 worker。
快照历史是应用管理的 JSON 状态，配有带版本的编解码器，而不是执行账本。

有两条规则让模型在边界处保持诚实。**所有权：** 恢复一条反向流水线是在延续一次
确切的持久化访问，因此 `running` 行绝不会被隐式接管——`resume=True` 是操作者
声称先前的所有者已经消失，没有它，该行会被跳过并保持原样（正向路径
保留它更早的从零重启规则，这也是为什么它不在通用的打开路径里）。
**丢弃状态是显式的：** `fresh_restart=True` 是唯一会丢弃检查点或
遍历的开关。它从绑定的种子重新开始，重置控制预算并使先前的账本
水位失效，同时把被丢弃的遍历遗留的在途任务行结算为 `interrupted`，而只追加的
历史以及（对反向流水线而言）访问计数器和发生实例会保留下来——因此历史上的发生实例
仍然可以寻址，而一个
遍历已丢失的存储会从它自己的行重建计数器。`retry_succeeded` 仍是一个资格
开关（“也接纳已成功的流水线”），不再意味着丢弃任何东西。

存储会记录它的磁盘模型走到了哪一步（`store/visits.py`）：在第一次重新访问被
提交之前是 `base`，之后是 `visits-v1`，并与使其成立的那次发生实例写在同一事务中。未知的
层级会在打开时被拒绝，而不是被自行解释；处于 `visits-v1` 的 SQLite 存储会启用一道写入者守卫，
拒绝任何未声明访问谱系感知的连接写入——这个标记的存在是为了让
不感知谱系的写入者大声失败，而不是改写错误的发生实例。迁移对
仅正向的工作保持增量式，而这类工作永远不会离开 `base`。

---

## 5. 数据模型（SQLite，WAL + `synchronous=NORMAL`）

三层事实，职责互不重叠：

| 表 | 是什么 | 语义 |
|---|---|---|
| `runs` | 一次运行 | heartbeat、state、配置快照、version、host、seed |
| `pipelines` | **当前状态** | 恢复时用一条 SQL 查询选出待处理的工作；`n_tasks_done` 是检查点游标 |
| `tasks` | 每个任务的当前状态 | 就地覆盖；记录 `attempts_used`、耗时、错误、已用租约数 |
| `attempts` | **只追加的历史** | 每次尝试一行，跨恢复连续编号，永不覆盖 |
| `artifacts` | 状态载体 | 内容寻址 + `payload BLOB`；`is_final` 标记最终产物。`seq` 对链上的任务是任务位置，对种子是 `-1`，而在启用控制流的流水线上是 **大于等于 `n_tasks` 的交接载荷地址**（§4.8.3） |
| `events` | **结构化日志** | `scope ∈ run/pipeline/task/pool/resource`；一条流水线的完整故事 = 按 `pipeline_id` 查询 |
| `handoffs` | **控制流历史**（进阶，可选启用） | 只追加：每个由任务发起的跳转一行，指明 from/to 位置和持久化入口工件；恢复所依据的权威记录（§4.8） |
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

`journal` 模式：`full` 会存储工件载荷（**恢复的前提条件**）；`summary` 只保留
摘要和元数据（节省空间，代价是中间工件无法被复用，因此恢复只能
重跑整条流水线，并且它会留下一个 `pipeline.checkpoint_missing` 事件）。

**写入策略**：所有存储方法都是同步的——这能让诸如“在尝试开始前记录 running 行”这类
关键写入不被取消打断。批处理写入 / write-behind 合并是
后续的优化，不影响接口。

**读取策略**：上面的列表查询可以物化它们的结果——报告本来就需要一个列表。必须保持
有界的整类读取（例如导出大型存储）则改走可选的分页迭代
扩展：按键集分批读取 `ITER_BATCH_SIZE` 行，排序键以一个唯一列结尾，
这样一个批次边界既不会漏掉一行，也不会重复一行；嵌套的 `pipelines` 行会物化
一条流水线，这就是文档化的内存单位。当键单调时（`event_id`、`attempt_id`），
迭代器以启动时取得的高水位为界，因此对活跃存储的导出不会去追
一个不断移动的尾部；`pipelines`/`tasks`/`artifacts` 没有单调键，文档中将其描述为对
活跃存储的尽力遍历。`Store` 不变，因此只实现列表
API 的第三方存储仍然是完整的，只是内存占用不再有界——参见
[存储参考](reference.md#分页读取与第三方存储)。

---

## 6. 两种使用方式

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

恢复：`runner.run(template.map(dataset_rows), resume=True)` —— 已经成功的流水线会被跳过，而
失败的流水线则从**第一个没有产出工件的任务**继续。

一个完整的实战示例 —— 数据准备、一次两轮模型调用、三个评委、一段用户自行编写的
归约逻辑，既有分组形态也有按评委划分的流水线形态，其中检查点开销是测量
出来的而不是断言出来的 —— 位于 `examples/llm_eval/`。

### 6.2 声明式（简单任务）

声明式形式只描述**组合与资源**；逻辑仍然留在 Python 中
（`use: myproj.tasks:ask_model`）。文档可以是 YAML、TOML 或 JSON；由后缀决定用哪个解析器，
其中只有 YAML 是可选依赖（§8.13）。

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

退出码：`0` 全部成功 / `1` 部分失败 / `2` 配置错误 / `130` 被中断。

### 6.3 分片、合并与导出（M3）

内核是单循环的，而 SQLite 只接受一个写入者，所以横向扩展意味着**用多个各自持有
存储的进程**，事后再把它们合并起来。分片分配是内容寻址的 `pipeline_key` 的纯函数，因此
同一份数据集总是以相同方式切分，`--resume` 也会把每条流水线送回拥有它的那个分片。

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

`--shard I/N` 会原样采用显式给出的 `--store`；没有显式给出时，`run.store` 会加上 `.shardIofN` 后缀，这样两个
子进程就绝不会争抢同一个文件。每个子进程的环境里还会带上 `PYATACKER_SHARD=I/N`，因此任务
可以记录自己的来源信息。

**行形态**（`--rows`）供消费结果的一方使用：`pipelines`（嵌套，默认值）、`tasks`、
`attempts`（带重试 `decision`）、`events`、`artifacts`。**格式**（`--format`）：`jsonl`、`json`、`csv`。
CSV 的表头取自最前面的 `header_rows` 行，之后才出现的字段会折入 `extra`
列，这样内存占用保持平稳，又不会悄悄丢弃字段。每种类型都完整导出 —— `events`
过去会在最新的 100 000 行处截断 —— 读取也按有界批次进行；参见
[导出参考](reference.md#导出)。

### 6.4 插件、后端与监控端点（M4）

**插件**就是普通的 `importlib.metadata` 入口点 —— 没有注册表文件，也没有导入期魔法：

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

之后 `use: my_judge`、`algorithm: my_algo` 和 `store: "s3://bucket/runs.db"` 就直接可用了。
有三条规则避免这件事变成负担：

* **内置项优先解析**，因此插件永远无法遮蔽 `echo` 或 `wait`；
* **坏掉的插件只被记录，不会抛出** —— `pyattacker plugins` 会列出哪些加载成功、哪些
  失败，而健康的插件继续工作。插件做的任何事都不得逃逸到
  `Runner.__init__` 之外，而编解码器插件正是在那里安装的；
* **声明自己能处理某载荷的编解码器插件优先于更早的注册**，因此专门的
  编解码器不会沦为 JSON 兜底实现之后的死代码。显式的 `for_types=` 映射仍然胜过
  扫描，因为点名类型比“我能编码它”是更强的声明。

完整示例位于 `examples/plugin_package/`。

**工件后端**决定载荷字节存放在哪里。`inline`（默认）把它们留在存储里；
`file:///data/blobs` 把超过阈值的部分溢出到内容寻址文件；`null` 保留
摘要并丢弃字节。存储在自己的行里保留 `digest`/`size`/`codec`，再加上一个不透明的
`blob_ref`，并在读取时重新水合（hydrate）出 `payload` —— 因此对上游的一切来说，工件仍然是同一个对象，
而内容寻址意味着相同的载荷会合并成一个文件，并且一个共享的
后端可以安全地服务多次运行。

```toml
[run]
artifact_backend = { kind = "file", root = "/data/blobs", min_bytes = 262144 }
```

**监控端点**：`pyattacker serve runs/qa.db` 会启动一个零依赖的只读 HTTP 视图
（每个请求都新建一条只读连接，因此可以和正在进行的运行并存）。`/stats`、`/events`、
`/pipelines`、`/resources`、`/errors` 返回 JSON；`/` 是一个会自动刷新的小仪表盘。它只绑定
回环地址，也没有认证 —— 它会暴露你的载荷，所以请把它当作调试视图。

**fanout（扇出）**：`fanout(a, b, ...)` 在*同一份*输入上并发运行多个任务，而且是在**一个
任务内部**，这就是在不把流水线变成 DAG 的前提下表达真正分支步骤的方式。
权衡是明确的：重试粒度变成整个组，而组采用最
宽容的子任务策略。因为运行器只会看到组的规格（spec），`resource`、`algorithm`
和 `timeout_s` 会从子任务提升到组上 —— 但只有当每个子任务都一致时才这么做，
因为一个组不能同时表示两种不同的策略。一等公民的 `Parallel`/`Gather` 节点仍然
有意不在范围内 —— 正是一元任务模型让内核（及其恢复机制）保持
小巧。

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

依赖方向严格单向：`errors → artifact → task → handoff → pipeline → resource/algorithm → store → runner → cli`，
而 `resource` 不反向依赖 `algorithm`（算法通过 `pool.acquire` 注入）。
`shard`/`merge`/`export` 位于内核旁边：它们读取存储和流水线流，内核中没有任何东西
依赖它们。`benchmark/` 同样在它旁边，但低一层：它直接驱动 `Pool` 和各个算法，
从不进入一次运行，这正是它的模拟时钟保持精确的原因（见 §8.14）。

---

## 8. 已知权衡与局限（有意为之，事先写明）

1. **停止条件只能尽力而为**：`stop_after_failures` 在准入时以及每次
   流水线完成时求值，因此已经准入的流水线（大约 `2 × concurrency` 条）仍会运行。当重试
   退避很大时，挂起意味着许多流水线可以同时*处于进行中*，于是在预算耗尽前可能有更多流水线被准入。
   预算只能止血，不能让时间倒流。
2. **write-behind 会丢失尾部**：尝试和事件是批处理写入的（按大小或间隔触发，另外还会在每次
   心跳以及运行结束时刷写一次），因此一次 `SIGKILL` 可能丢掉最后一批尚未发送的数据。每个
   检查点和工件都是同步写入的，所以恢复后的运行仍然只重新执行
   真正未完成的部分。如果你更愿意为每次尝试付出一次提交的代价，请使用 `--no-write-behind`。
3. **`journal=summary` 牺牲恢复粒度**：不存储载荷就没有中间
   工件可复用，因此整条流水线都必须重跑。如果你需要恢复能力，就必须使用 `full`。
4. **资源健康度依赖显式上报**：只有 `lease.report(ok=False)` 才算作一次失败；
   框架不会去猜。
5. **`quota_aware` 是一种偏好，不是硬性限制**：当所有候选者都超出配额时，仍然会分发其中
   最好的一个，因为拒绝工作比超支更糟。如果需要硬性停止，就在任务自身预算耗尽后从任务里
   抛出异常。
6. **v1 只支持 asyncio 任务**：请在任务内部自行把阻塞代码包装成
   `await asyncio.to_thread(...)`（一行代码，换来一个不需要锁、也不承担
   线程安全负担的资源池）。
7. **每个数据库只有一个写入者**：横向扩展意味着更多进程，每个进程有自己的存储（`--shard i/N`），
   绝不在一个文件上放更多写入者 —— SQLite 只允许单个写入者（WAL 保持一个写入者加多个读取者，这
   正是 `watch`/`report` 能与正在进行的运行并存的原因）。
8. **分片均衡是统计意义上的，并不精确**：分配方式是 `hash(pipeline_key) % N`，因为另一种做法
   （对流做轮询）会在数据集每次变化时移动所有流水线，并且会破坏恢复。
   有 4 个分片、20 条流水线时，有时会看到 6/14 这样的划分；规模上去之后就会趋于均匀。
9. **合并后的报告是并集，不是求和**：分片数量变化后，同一条流水线可能同时存在于两个分片里，
   因此 `merge_reports` 按 `pipeline_id` 去重（状态最优者胜出，完成时间最晚者打破平局），并
   基于合并后的行*重新计算*统计量。它会报告自己折入了多少行，因此这个数字永远不会
   被隐藏。
10. **流水线是一条线性链**：内核是按“节点 + 依赖边”实现的，所以
    加入 `Parallel/Gather` 只是语法糖，但它被有意地不对外暴露。请改在任务内部使用 `fanout(...)`：
    分支在记录里仍然是一步，代价是重试粒度变成组级。
    唯一的补充说明是 §4.8 中可选加入的交接：它改变链的*遍历*
    方式 —— 沿声明的边进行，绝不改变其拓扑 —— 依然没有 join，也没有第二个入口点。
11. **`null` 后端会牺牲恢复粒度**：丢弃载荷意味着中间工件
    无法复用，因此 `resume` 会重跑整条流水线 —— 与 `journal=summary` 是同样的权衡。
    blob 文件缺失时的行为也刻意保持一致：`available` 变为 false，工作会被重做。
12. **HTTP 端点默认无认证，且只绑定回环地址。** 它是查看你这次运行载荷的调试视图，
    而不是一项服务。如果需要，就把它放到你自己的代理之后，并且在把它绑定到
    公网接口之前先想清楚。
13. **YAML 是额外依赖（extra），而不是依赖**：`dependencies` 为空，声明式层用标准库读取 JSON 和
    TOML，所以 `pip install pyattacker` 不会拉入任何东西。`.yaml`/`.yml` 配置需要
    `pip install "pyattacker[yaml]"`，没有它时加载器会在读取该文件的那一刻抛出 `ConfigError`，并指明
    该额外依赖和该文件 —— 检查按文件、依据其后缀进行，绝不在导入时进行。
    代价是想要 YAML 的使用者多一步安装；好处是其他所有人，包括
    每一位只用 SDK 的用户，都不必为自己从不调用的解析器付出任何代价。
14. **基准测试是模拟，其数字是假设的产物。** 它在一个人为写定的世界里比较
    获取算法（容量周期、令牌桶、尾延迟、风暴、
    三个端点），使“这里哪个算法更好”成为一个有答案、也有
    种子的问题。它并不是在测量任何人的提供方，改动一个假设就可能改变
    排名；七个内置算法中有两个在这个世界里完全无法演练 —— `failover` 只有一个资源池时
    没有可故障转移的对象，而 `least_busy` 不过是资源池默认选择策略的另一个名字 ——
    场景会声明这一点（并标记为 N/A），而不是把它藏在一个看似合理的数字背后。参见
    `docs/benchmark.md`。
15. **子进程树只在 POSIX 上得到保证。** `shell_run` 在每条退出路径上都会终止并回收它
    启动的进程 —— 正常退出、`timeout_s`、取消、任何其他异常 —— 并且在 POSIX 上
    它会向子进程的整个进程组发送信号，因此 shell 流水线或 argv 程序的后代进程也会一并
    被终止。Windows 的标准库没有进程组信号机制（`os.killpg` 不存在，
    而且 `asyncio` 无法向子进程所在的组发送 `CTRL_BREAK_EVENT`），因此在 Windows 上只会终止直接子进程，
    而后代进程可能比任务活得更久。离线测试在 POSIX 上验证后代进程的保证，
    在其他平台上则跳过这两个用例，并说明原因。
16. **交接是可选加入的，并且被隔离起来**（§4.8）。声明了 `control` 的流水线是在拿
    一项保证换另一项：它的游标变成*位置*，而不是进度计数（被跳过的槽位
    没有任务行，因此 `n_tasks_done / n_tasks_total` 对这样的流水线来说并不是完成百分比），
    并且它的恢复依赖于 `handoffs` 账本可读 —— 这正是为什么缺少原子
    `commit_handoff` 能力的存储会被直接拒绝，而不是被降级为一次
    非持久跳转。该特性在 1.0 之前标记为实验性：上述保证是稳定的
    部分，写法仍可能变化。仅正向的路径不会回访；join 和跨流水线传输都
    不包含在内；一条几乎全是交接的流水线，说明该问题需要的是图引擎，而这个框架
    并不是。

---

## 9. 测试策略与当前状态

**零网络、零外部服务** —— 一切都在内置的 mock 任务上运行。核心断言如下：

* `tests/test_lease_safety.py` —— §4.2 契约的每一条：异常/取消/超时/逃生通道
  泄漏/循环 acquire，外加“运行结束后资源池中 `active == 0`”。
* `tests/test_runner.py` —— ★ 恢复：第一轮全部失败 → 第二轮带 `resume=True` →
  **较早任务的调用次数不会增加**，只有失败的任务重跑，而第三轮完全是
  `skipped`；`journal=summary` 让检查点不可用时的行为；重试决策字段；
  并发上限；关闭条件；导出。
* `tests/test_pipeline.py` —— 构造期的工件类型链式校验、源摘要影响
  同一性、`map(repeats)` 与显式键。
* `tests/test_scheduler.py` —— M2 基础设施：`DelayQueue` 的顺序、更早的定时器会打断更长的
  等待、取消永远不会丢失挂起的条目、`WriteBehindStore` 的缓冲/刷写触发条件，以及
  状态写入永不缓冲这条规则。
* `tests/test_algorithms.py` —— 资源获取策略（`sticky` 亲和性、`quota_aware` 排序、
  `least_busy`、`failover`）以及资源池等待时间指标。
* `tests/test_m2.py` —— 通过 `Runner` 端到端验证 M2 的承诺：在 `concurrency=1` 时，另一条流水线
  会在一条流水线因重试而挂起期间完成（断言依据事件顺序而非时序），被停止的运行
  会把挂起的流水线记录为可恢复，而运行结束时不留下任何缓冲的事实。
* `tests/test_shard.py` —— 划分是完备的（每条流水线恰好属于一个分片）、确定且
  内容寻址/跨进程稳定，`parse_shard`/`shard_index`/`shard_env` 的校验，以及
  `--shards N` 会启动真实的子进程，并让一条带重试的资源池流水线在它们之间干净地合并。
* `tests/test_export.py` —— 每种行形态和每种格式，包括 CSV `extra` 列，它对应那些
  在 `header_rows` 之后才出现的键、`merge_reports` 按最优状态/最晚完成折叠重复的 `pipeline_id`，以及
  CLI 的分片路径：带显式存储的 `run --shard i/N`、`--shards N`、JSON 摘要、跨分片存储合并的
  `report`/`export`，以及某个分片无处可写时的 `ConfigError`。
* `tests/test_plugins.py` —— 使用注入的入口点提供方进行发现与解析（无需
  安装）：内置项胜出、抛异常的插件只被记录而不向外传播，`use:`/`algorithm:`/存储 scheme
  的解析都能到达插件。
* `tests/test_server.py` —— 通过真实回环请求测试 HTTP 端点：JSON 形态、限制、404，以及
  服务器运行期间启动的一次运行会出现在 `/stats` 里。
* `tests/test_backends.py` —— 超过阈值时溢出、读取时水合（hydrate）、内容寻址去重、
  `journal=summary` 在任何地方都不保留内容，以及**穿过已溢出检查点的恢复**。
* `tests/test_packaging.py` —— `pyproject.toml` 中的版本与正在运行的包一致、没有意外引入的
  依赖、每个模块都能导入，并且每个承诺的名称都被导出。
* `tests/test_optional_yaml.py` —— 用户实际遇到的额外依赖（§8.13）：SDK 在 PyYAML
  缺席时照常运行、JSON/TOML 配置可以加载、`.yaml`/`.yml` 文件抛出的 `ConfigError` 会指明该文件和
  该额外依赖、*损坏的* PyYAML 会暴露自己的错误而不是那条提示，并且 CLI 会把缺少额外依赖的情况变成
  退出码 2。`sys.modules["yaml"] = None` 模拟缺席，因此这一切都会在每次普通的测试
  运行中执行，而不是只在没有 PyYAML 的那个任务里执行。
* `tests/test_benchmark_*.py` —— 模拟基准测试，其测试针对的是它所声称的内容而不是它的
  输出：为虚拟时钟手工计算的时间线（32 个并发的 1 秒睡眠总共只花一秒，可运行的
  worker 永远不会被跳过）、提供方自身的动态行为（端点满载时以 429 拒绝、
  一次拒绝会消耗未来的额度、风暴是时间的函数而不是流量的函数）、怎样的比较才是
  公平的（两个算法对同一个 `(endpoint, ordinal)` 会得到完全相同的抽样；没有任何东西
  会把环境的引用交给算法；同一个种子会精确复现同样的数字），以及
  整个过程不建立任何网络连接。虚拟时钟还会与一个
  压缩过的实时时钟做对照，后者从构造上就是正确的，但太慢而无法实际使用。
* **依赖 YAML 的测试标记为 `requires_yaml`**，而不是用 `importorskip()` 加以保护，因此
  缺少该额外依赖时，开发用的测试套件会*失败*，而不是悄悄跳过自己三分之一的用例。
  因此 CI 会把测试套件跑两遍：一遍带额外依赖（全部用例），一遍针对仅用 `pip install` 安装的
  包、带上 `-m "not requires_yaml"`。哪一侧才是保证，这才是关键 —— 正面用例
  正常测试，负面用例显式测试。发布工作流在构建出的 wheel 上闭环验证，
  两个方向都做：不带额外依赖（PyYAML 缺席、JSON 可用、YAML 会要求该额外依赖）和带它（PyYAML
  能从已发布的元数据中解析出来，且 YAML 配置能通过校验）。
* `tests/test_artifact.py` / `test_store.py` / `test_declarative.py` / `test_cli.py` —— 编解码器、
  存储语义以及两种存储之间的一致性、配置解析、CLI 端到端。
* `tests/test_errors.py` —— 作为纯函数的 `error_class_of`/`is_retryable_class`/`retry_after_of`：每个
  `_STATUS_RULES` 区间、`FatalError`/`TimeoutError`/`ConnectionError` 分支、显式 `.error_class`
  的优先级，以及从直接属性和响应头两处提取服务器建议的 `retry_after`。
* `tests/test_handoff.py` —— 这一进阶特性的端到端测试：无 control 的运行可证明未受影响
  （`spec_digest` 逐字不变且没有新增行）、正向交接会跳过站点，而沿未声明边返回的指令
  是致命的且不会重试、`END` 会将其入口工件定稿、一个**真正被 SIGKILL 的
  进程**会在目标处恢复而不重跑源任务、已消费的账本行会回退到
  普通工件规则、入口载荷丢失会从零重新开始、提交是原子的，且被交接的
  尝试绝不会经过 write-behind 缓冲区、缺少该能力的存储会被拒绝（而同在一个存储上的
  无 control 流水线仍能正常工作），此外校验、`fanout` 拒绝、租约、
  超时以及每一个可观测面都被固定住。
* `tests/test_monitor.py` —— `watch` 的终端渲染器：进度条的截断/取整、运行范围还是
  整个存储范围的快照、资源池进度条，以及泄漏租约/正在停止的指示。
* `tests/test_tasks.py` —— `shell_run`：字符串形式与 argv 形式；字符串命令会直接拒绝 `{value}` 插值，
  而 argv 命令则把替换后的值作为单个字面量参数传入，经由
  `create_subprocess_exec`，不做隐式的 shell 解释。（如果 argv 形式自身的命令显式
  调用了 shell 或其他解释器，例如 `["sh", "-c", ...]`，那么该解释器的输入安全语义
  由调用方负责 —— 这里的保证是“没有*隐式* shell”，而不是“对任何程序都安全”。）
  其他内置 mock 任务只在其他测试文件需要替身任务时被顺带测到，
  而没有专门的测试文件。
* `tests/test_subprocess_lifecycle.py` —— 操作系统视角下 `shell_run` 的进程生命周期。取消、
  `timeout_s` 到期、`Runner` 停止，以及针对清理本身发起的取消，都会让子进程
  被杀死*并被回收*（`os.kill(pid, 0)` 必须失败，这同时能抓到“仍在运行”和“已杀死但未
  等待回收”），而自行退出的子进程则不会被干预，结果得以保留。有一个测试会在
  *进程仍在创建时*取消调用方：它有意拉大这个窗口，方法是持有被包装的
  `loop.subprocess_exec` 直到测试释放它，因为此时子进程已经存在，而没有任何
  栈帧持有句柄 —— 这是一场无法指望测试能故意命中的竞态（原版 3.11/3.12 实现
  只是碰巧在自身内部等待被取消时才关闭传输层，这并不是一个
  `shell_run` 可以依赖的保证）。后代进程测试会取消一个字符串命令，其 shell 正在等待一条真实的
  流水线，还会取消一个自行派生了子进程的 argv 程序，然后断言每个 PID 都已消失 —— 如果清理只杀死
  直接子进程，它们就会失败。子进程通过写入自己的 PID 来表明就绪（任何地方都没有固定
  睡眠），而 `tracked_pids` fixture 即使在测试失败时也会杀死测试见过的每个 PID，因此一个失败的
  测试不可能留下活着的进程。这两个后代进程测试在非 POSIX 平台上会跳过，并给出平台
  原因（见 §8.15）；kill 和存活探测遵循平台自身的语义，CI 只覆盖 POSIX
  分支。
* `tests/test_tutorial.py` —— `docs/tutorial.md` 中每个标记为完整程序的代码块
  （`# tutorial/<name>.py`）都会被抽取并真正运行，因此教程不会悄悄腐烂、与
  真实 API 脱节。

所有与时间相关的逻辑（退避、熔断冷却）都经由可注入的 `Clock`，测试则使用
`tests/helpers.py::FakeClock` 把时间变成可控变量，使测试既确定又快速。
整个测试套件几秒钟就跑完 —— 运行 `uv run pytest` 可以看当前的用例数（本文有意
不写死这个数字，因为每增删一个测试，具体数字就会过时）—— 所以没有
任何借口不去运行它。
`ruff check` 在 `pyproject.toml` 的配置下是干净的，其中每条被忽略的规则都带有
理由 —— lint 例外应当是一个论证，而不是一次意外。

**已实现（M0 – M4，即 0.1.0 计划中的全部内容）**：五个概念的完整内核、内存/SQLite 存储、任务级
检查点与恢复、重试与错误分类、资源池状态机与发布/订阅、
7 种获取算法、声明式层、CLI、内置的 mock 工具任务、延迟延续
（不占用 worker 的退避）、尝试/事件的 write-behind 批处理、按资源的定向唤醒、
资源池等待时间指标、带合并报告的确定性分片、三种导出格式下的五种行形态、
入口点插件、外部工件后端、fanout 辅助函数，以及只读的 HTTP 监控端点。

**留待以后（0.1.0 之后）**：分布式调度器、Parquet 导出、blob 垃圾回收
（`FileBackend` 是内容寻址的，因此孤儿 blob 是安全的，但永远不会被删除），以及一等公民的
`Parallel`/`Gather` 节点 —— 最后一项只有在实践中证明一元任务模型限制过大时才做。

---

## 10. 里程碑

| | 目标 | 完成标准 |
|---|---|---|
| **M0 骨架** ✅ | 五个概念的内核 + 内存存储 + 线性执行 + 内置 mock + CLI | `pyattacker demo` 端到端跑通 |
| **M1 持久化与恢复** ✅ | 全部 SQLite 表、任务级检查点、resume、结构化事件、SIGINT | 在 SIGKILL 之后 resume，无需重发较早的任务 |
| **M2 更聪明的资源与重试** ✅ | write-behind、让出 worker 的退避、按资源的定向唤醒、配额感知算法、更细的 `acquire` 指标 | 资源池饱和时退避可观测，并且可以从 `events` 重放 |
| **M3 规模与易用性** ✅ | `--shard i/N` + `--shards N`、合并报告、分片工具、多形态/多格式导出 | 多个进程运行同一份数据集 |
| **M4 生态** ✅ | 入口点插件、外部工件后端、fanout 辅助函数、HTTP 监控端点、0.1.0 打包 | 第三方可以发布任务包 |
| **M5 进阶控制流（可选加入）** 🚧 | 声明的正向交接（`Handoff`、`control=`、`handoffs` 账本、原子 `commit_handoff`、账本优先恢复） | 交接是一个持久检查点：被杀的进程会带着入口状态在目标处恢复，而无 control 的流水线可证明未受影响 |

---

## 11. 非目标（写进 README，以防范围蔓延）

* 不提供 HTTP 客户端 / 提供方 SDK 适配层（任务由你自己编写；这是有意的设计，而不是缺失的功能）
* 不做 DAG / 多轮智能体编排（流水线保持线性；fanout 在任务内部实现，
  唯一的例外是 §4.8 中可选加入的交接 —— 没有声明的图，没有 join，也没有
  跨流水线编排）
* 不做语义归约（accuracy / pass@k / 任何跨流水线聚合）
* 不做服务化 / 网关 / 代理
* 不做数据集存储（它只接受一个可迭代的种子流 + 一个 `jsonl_source` 工具）
* 不做分布式调度（`--shard` 就是多进程的上限）
