# 获取算法的基准测试

[English](../benchmark.md) | **简体中文**

`pyattacker.benchmark` 模拟一个 API 提供方，并让本库真实的获取算法
在其上运行。它回答一个问题：**这个负载该用哪个获取算法？** 场景
是输入，指标向量是输出，而一个结果可由场景名、种子和种子数
复现。该包仅依赖标准库，且不打开任何套接字。

## 1. 这是什么，以及它不是什么

它不是微基准测试：这里不测量这台机器，`wall_s` 是*成本*列，绝不是
质量信号。它是仿真，因此数字是写在场景中的假设的属性：
改动一个假设，排名就可能改变，因为基础场景的令牌桶
会惩罚猛打它的客户端。由此有两条规则：不设综合得分（第 6 节），以及场景可以
报告“无胜者”（基础场景刻意由三个性格不同的端点组成）。

## 2. 一次运行如何工作

`scenario -> simulated provider -> closed-loop workers -> metrics`。

* **场景**（`scenario.py`）—— 以冻结数据类表示的假设：周期、延迟、失败、限制、
  端点、任务形态、重试策略、预算。
* **提供方**（`provider.py`）—— `perform(endpoint_id)` 服务一个请求、用 429 拒绝它，或
  用一个厂商风格的错误让它失败；它知悉世界的状态，却不向客户端透露其中任何一点。
* **worker**（`harness.py`）—— `concurrency` 个任务领取包含 `steps_per_job` 个顺序步骤的任务，
  每个步骤通过 `ctx.acquire_lease(pool, algorithm=...)` 租用 `calls_per_step` 次。`Pool`、
  `TaskContext`、各个算法、`Retrying` 和 `errors.error_class_of` 用的都是真实实现。

**这里刻意不使用 `Runner`。** 它的调度、检查点和存储对
每个算法都相同，而且它包含仿真时钟无法控制的实时等待（一次心跳
`asyncio.sleep`、若干次 `asyncio.wait_for` 调用）。驱动它要么会让虚拟时钟运行挂住，要么
迫使运行改用真实时钟，而第 5 节的存在正是为了避开后者。出于同样的原因，这里传入
`deadlock_warn_s=None`：死锁启发式会消耗一个真实定时器。

## 3. 基础场景 `bursty_provider`

一个厂商，通过三个端点访问；3000 个任务 x 3 个步骤 x 2 次调用，12 个 worker，600s 的时间上限，
所有算法共用一个重试策略（`Retrying(max_attempts=5, base=0.5, factor=2.0, cap=8.0,
jitter="full")`）。`sticky` 和 `least_busy` 之所以有事可做，只是因为一个任务就是一条流水线，
场景测试对此做了断言（`tests/test_benchmark_scenario.py::test_the_base_scenario_covers_the_problems_it_claims_to`）。

| 假设（字段） | 它代表的现实问题 | 它区分的是什么 |
|---|---|---|
| 容量周期：`LoadCycle(period_s=120, trough=0.35, peak=1.0)`，从低谷开始；容量为 `max(1, round(capacity x weight x factor))` | 厂商自身的负载、你应得的公平份额、某个区域故障转移并带走你的容量 | 把容量视为静态的策略（`immediate`，以及 `quota_aware` 固定声明的配额）与会重试并重新排队的策略之间的对比。它让*何时*发问与*向何处*发问同等重要 |
| 被施压就收紧的令牌桶：按 `per_window/window_s` 补充，`burst` 允许突发；每次拒绝都把额度乘以 `tightening`（下限 `tightening_floor`），每个窗口按 `recovery_per_window` 恢复 | 带 `Retry-After` 的 `429`，这是写得再好的客户端仍然失败的最常见方式 | 分散负载并拿回容量的客户端，与持续猛打、持续失去容量的客户端之间的对比。`metered` 以 0.35 收紧并在 1.5s 后重试；其余以 0.5 收紧 |
| 并非单一数值的延迟：对数正态主体（`median_s`、`sigma`）加上一条慢尾（`tail_rate`、`tail_factor`）；三个端点分别使用 0.25s/0.5、1.2s/0.3 和 0.5s/0.6，尾部为 2%、1%、3% | 排在其他租户后面排队、GC 停顿、模型对长提示词输出缓慢 | “在途数量”和“最近最快”到底是不是可用的信号，并把 p50 的故事与 p95/p99 的故事区分开 |
| 包含相关性的失败：`FailureProfile`（默认 `error_rate=0.01`、`storm_rate=0.05`、`storm_duration_s=30`、`storm_error_rate=0.5`，每个端点各自覆盖）；风暴按 `(seed, endpoint, time window)` 决定 | 独立的瞬时抖动，加上一次提供方事故，或某个区域网络短暂变差 | 算法多快察觉到某个端点当前状况不佳，以及为弄明白这一点烧掉多少重试预算。错误是 `ProviderError(503)`、`ConnectionError`、`TimeoutError` 和 429，因此框架自己的分类逻辑会运行 |
| 几个性格不同的端点：`fast-flaky`（容量 6、0.25s、错误率 0.03、风暴 0.08 于 0.6）、`slow-steady`（容量 12 x 权重 1.2、1.2s、错误率 0.004、风暴 0.02）、`metered`（容量 4、0.5s、配额 180） | 两个厂商，外加一个受突发限制的试用 key；那个便宜又快的正是会变坏的一个 | 位置与时间之间的权衡。没有任何单一策略在这三者上都最好，这正是报告必须能给出多个胜者或没有胜者的原因 |
| 声明的配额：每个端点的 `quota_units`（1200/2400/180），通过 `lease.report(usage={"tokens": n})` 消耗 | 一个额度可见的套餐 | `quota_aware` 的排名信号。它是声明的元数据，而不是提供方的拒绝行为——见第 8 节 |

## 4. 为什么这个比较是客观的

| 规则 | 所在位置 | 强制它的测试 |
|---|---|---|
| 世界的脾气是时间的函数，而不是调用方的函数：容量是对 `t` 的算术，`storm_until(endpoint, t)` 是 `(seed, endpoint, t)` 的纯函数——风暴从其时间窗口的起点开始，因此天气不取决于请求何时到达 | `provider.py`（`capacity_at`、`storm_until`、`_storm_window`） | `tests/test_benchmark_provider.py::test_storm_state_does_not_depend_on_request_cadence` 在同一批时间戳上比较两份*不同*的流量安排，`::test_the_storm_interval_is_anchored_to_its_window` 则钉住这种锚定 |
| 公共随机数：延迟与失败的掷骰来自 `random.Random(f"{seed}:{endpoint}:{ordinal}")`，以请求*在该端点*的序号为索引。因此两个算法共享同一份**外生**随机性——同样的骰子、同样的天气——但并不共享相同的已实现提供方状态，后者会分化，因为令牌桶和在途数量会对各自的行为作出反应 | `provider.py`（`perform`） | `tests/test_benchmark_provider.py::test_each_request_sees_the_world_by_its_ordinal_whatever_the_timing`；`tests/test_benchmark_harness.py::test_two_algorithms_see_the_same_exogenous_draws` 比较两者都到达的每一个序号，并在共享请求少于 20 个时判定不通过 |
| 客户端侧的随机性跟随逻辑同一性，而不是 worker：获取算法和重试策略各自从 `Random(f"{seed}:{{acquire,retry}}:{job}:{step}:{attempt}")` 取数 | `harness.py`（`acquire_stream`、`retry_stream`） | `tests/test_benchmark_harness.py::test_client_randomness_follows_the_logical_identity_not_the_worker` |
| 场景会声明它无法检验的算法，这些算法不会依据毫无意义的数字参与排名 | `scenario.py`（`Scenario.unsuited`）、`report.py`（`default_algorithms`、`winners`） | `tests/test_benchmark_harness.py::test_a_scenario_declares_which_algorithms_it_cannot_exercise`、`::test_an_algorithm_the_scenario_cannot_exercise_is_marked_not_ranked` |
| 只有冷却结束才能终结的等待，是资源池时钟上的真实定时器，因此仿真时间会推进到它，等待者也会被唤醒——而且由*最早*的待定截止时间拥有该定时器 | `resource.py`（`_ensure_cooldown_notifier`、`_cooldown_deadline`） | `tests/test_lease_safety.py::test_a_wait_that_only_a_cooldown_can_end_actually_ends` 和 `::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it`（内核回归测试：没有这项修复，两者都会超时或延迟唤醒）、`tests/test_benchmark_harness.py::test_a_wait_that_only_a_cooldown_can_end_advances_simulated_time` |
| 只完成一小部分工作的算法不能在*任何*质量行上胜出——既包括没有完成量时其取值无定义的延迟与吞吐量行，也包括它部分掌控其分母的各种比率；被排除者在表格和 JSON 中都有具名说明，基线会忽略场景声明为不适用的算法，而只有一名合格竞争者的受门控行则完全没有胜者 | `report.py`（`COMPLETION_FLOOR`、`excluded_by_completion`、`comparable`、`unrivaled`、`winners`）、`metrics.py`（`Metric.requires_comparable_completion`） | `tests/test_benchmark_report.py::test_every_quality_row_requires_comparable_completion`、`::test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row`（对全部十三个受门控行参数化）、`::test_a_quality_metric_cannot_crown_an_algorithm_that_completed_almost_nothing`、`::test_the_completion_baseline_ignores_an_algorithm_the_scenario_cannot_exercise`、`::test_one_eligible_contestant_is_not_crowned` |
| 没有任何东西把环境的引用交给算法：测试装置只构造一个 `Pool` 和一个 `TaskContext`，别无其他；`SimulatedProvider` 对它保持私有，而 `capacity_at()` 公开只是为了指标能够对提供的容量做积分 | `harness.py`（`_context`、`_call_sequence`） | `tests/test_benchmark_harness.py::test_the_algorithm_is_handed_the_pool_but_never_the_environment` 遍历交给 spy 算法的每一样东西，只要场景或提供方可达就判为失败 |
| 相同的种子精确复现同样的数字（除 `wall_s` 外的每一个指标） | `scenario.seed` -> `Harness.seed` -> 每次运行的随机流 | `tests/test_benchmark_harness.py::test_the_same_seed_and_scenario_reproduce_the_same_numbers` |
| 没有套接字，包内没有任何东西能访问网络 | 仅导入标准库 | `tests/test_benchmark_harness.py::test_the_benchmark_opens_no_network_connections`；`::test_the_benchmark_package_imports_nothing_that_can_reach_the_network` |
| 错误就是厂商 SDK 会抛出的那些，因此由 `errors.error_class_of` 来分类，而不是手写标签 | `provider.py`（`ProviderError`） | `tests/test_benchmark_provider.py::test_failures_look_like_provider_errors_the_framework_can_classify` |
| 利用率的“提供的容量”不取决于客户端何时发出请求：周期用闭式积分计算，而不是对到达逐个求和 | `scenario.py`（`LoadCycle.integral`） | `tests/test_benchmark_scenario.py::test_the_closed_form_integral_matches_a_numeric_one` |

## 5. 时间

提供方的时间是仿真的，因为有意义的场景跨越数分钟（一个容量周期、一场风暴、一个
配额窗口），而每个算法都要花真实数分钟的比较根本不会有人去跑。`VirtualClock` 维护
一个定时器堆，把整个仿真推进到下一个截止时间；`sleep()` 是对 future 的真实 await，
因此调用方阻塞，由驱动器决定截止时间何时到来。

只有以下三条同时成立，时间才推进：(1) 有定时器处于待定状态；(2) 每个已注册的 worker 都已挂起，
不是在 `sleep()` 内部，就是在 `blocked()` 内部——后者是包裹 await 的标记，表示该 await 只能由一次归还或时间
终结（一次租约获取）；(3) 事件循环连续运行了 16 个探测回调，期间没有任何
可观察的活动。仅凭条件 2 还不够：一个协程可能停在本就可解的
await 里——一次在它排队期间已经触发的广播、一个刚刚归还的租约——不流逝任何时间它就可以运行。
条件 3 封住了这个窗口：探测通过 `loop.call_soon` 重新武装，而后者
会追加到就绪队列尾部，因此待处理的延续会在下一个探测节拍之前运行。测试套件里的
`FakeClock` 不可用，因为它在睡眠者*内部*推进时间（`t += seconds`）：
32 个 worker 同时睡 1s，会落到 32 个仿真秒。

手工推算的时间线把这条规则钉死（`tests/test_benchmark_clock.py`）：32 个并发的 1s 睡眠
让时钟停在 1.0，而不是 32；定时器按截止时间先后触发，同时仍在工作的 worker 会拖住
时间；纯 yield 既不推进时间，也永不挂起。

**对照真实时钟的差分校验。** `ScaledClock(speedup=10.0)` 改为压缩真实时间，
因此在两种时钟上运行同一个场景，能让推进规则里的缺陷表现为一个错误的数字，
而不是一次挂起。在 `bursty_provider` 上使用 `jobs=120, concurrency=6, horizon_s=90.0`、`wait`、种子
3：

| 指标 | `VirtualClock` | `ScaledClock(speedup=10.0)` |
|---|---|---|
| `jobs_done` | 120.0 | 120.0 |
| `makespan_s` | 80.253 | 81.916 |
| `refusal_rate` | 0.000 | 0.000 |
| `error_rate` | 0.029 | 0.029 |
| `utilization` | 0.316 | 0.310 |
| `throughput_rps` | 1.495 | 1.465 |

两者在完工时间上相差不到 2%，在利用率上相差 2%，这一对总共花掉 8.3s 墙钟时间。
`throughput_rps` 在两种时钟上都是 `jobs_done / makespan_s`，所以两者在吞吐量上一致，与
它们在完工时间上一致是同一句话——而不是第三项独立的测量。这确认的是
计时与吞吐量，而不是拒绝路径：在这个小世界里，需求从未打满容量，
两种时钟报告的拒绝数都是零。

**一次时钟本可能错过的等待。** 降级的资源*惰性*恢复——只有当有人调用 `state_at(now)` 时冷却才
到期——而这次到期本身不发出任何事件。因此，一个所有
资源都在冷却、且没有任何在途租约的资源池，已经没有任何东西可以广播：
等待者停在广播上，而它们真正等待的那个截止时间悄然流逝。在
仿真时钟下这就是挂起（已复现：`timers=0`、十二个等待者挂起、仿真时间冻结）；
在真实时间里，这是一个饥饿缺陷，只有在无关活动碰巧通知
资源池时才得以解决。`Pool` 现在每个资源池武装一个任务，且只在有人等待时武装，它在资源池的时钟上
睡到 DEGRADED 槽位上最早的*未来*冷却，然后广播。有三个细节很重要，而这三个
一开始都是缺陷：冷却已经到期的槽位绝不能武装零延迟定时器（那是一个
完全不推进仿真时间的活锁）；DEAD 或 REVOKED 的槽位必须忽略（它们永不
回来，而过期的 `blocked_until` 会让定时器不断重新武装）；以及必须记录武装的*截止时间*，而不仅仅是
被武装的任务，因为开始得更晚的冷却仍可能结束得更早——B 在
t=5 处中断并带 5s 冷却，而 A 的 30s 通知器已经在睡。保留旧定时器会让等待者在
t=30 而不是 t=10 被唤醒，比它们所等的资源恢复晚了二十个仿真秒
（`tests/test_lease_safety.py::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it` 用 `VirtualClock`
钉住了这两个数字）。

**一个再无可租之物的世界。** 如果每个端点最终都变成 DEAD 或 REVOKED——框架在连续失败达到 `dead_after`
次后会将一个端点永久退役——就再也发不出任何租约，于是
worker 挂起，时钟也没有可推进的目标。实时监督器会在 50ms 内察觉并
带着各项数字抛出 `BenchmarkStalled`，而不是烧掉墙钟预算：这是
场景的属性（一个可能永久死亡的端点集合），而不是算法或机器的属性。

**已知限制。** `Wait(timeout=...)` 和 `AcquireTimeout` 由事件循环的真实
时钟强制：`Wait` 阻塞在 `pool.wait_slot` 上，而后者在
`asyncio.wait_for(..., remaining)` 之下等待一个 `asyncio.Event`。因此虚拟时钟运行看不到仿真的获取
超时，所以场景避开获取超时——测试装置获取时不带 `timeout`——并用
`horizon_s` 限定运行，它会停止新任务，但从不截断已在途的任务。

## 6. 指标

`METRICS` 是测试装置输出键、报告列、JSON 字段名
和 `bench --list` 的唯一来源；有一个测试断言测试装置发出的正是这一集合
（`tests/test_benchmark_report.py::test_the_harness_emits_exactly_the_declared_metrics`）。

| 指标 | 单位 | 更优 | 含义 |
|---|---|---|---|
| `jobs_done` | jobs | 越高越好 | 在时间上限截断之前准入的工作中成功完成的任务数。截断之后不再准入新任务，但已在途的任务会跑完——这就是为什么一次 600s 的运行可以有略高于 600s 的完工时间 |
| `jobs_failed` | jobs | 中性 | 放弃的任务：某个步骤耗尽了重试预算。诊断性指标：算法可以通过从不启动那些本会失败的任务来缩小它 |
| `jobs_unstarted` | jobs | 中性 | 因为时间上限先到、worker 从未开始的任务。诊断性指标：算法可以通过启动一切并让它们失败来缩小它 |
| `makespan_s` | s | 中性 | 到最后一个 worker 停止为止的仿真秒数（时间上限，加上仍在途的一个步骤）；应与 `jobs_done` 一起看 |
| `throughput_rps` | jobs/s | 越高越好 * | 本次运行*自身*完工时间内每仿真秒完成的任务数——观测到的速率，而不是预算的容量 |
| `requests` | requests | 中性 | 发出的请求数，包括被拒绝的和重试的 |
| `attempts_per_completed_job` | attempts | 越低越好 * | 每个完成任务所花的步骤尝试数。零重试基线是 `steps_per_job`，而不是 1.0，且花在后来失败的任务上的尝试只计入分子 |
| `attempt_inflation` | ratio | 越低越好 * | 每个*已尝试*步骤的步骤尝试数（1.0 = 每个已尝试步骤都在第一次就成功）：剔除数量后的重试压力 |
| `failed_attempt_rate` | ratio | 越低越好 * | 失败的步骤尝试占全部步骤尝试的比例，包括策略随后放弃的那些 |
| `retry_rate` | ratio | 越低越好 * | 策略实际安排的每步骤尝试重试数。`failed_attempt_rate` 减去它就是被放弃的失败占比 |
| `refusal_rate` | ratio | 越低越好 * | 每个发出的请求中被提供方拒绝（429）的比例 |
| `error_rate` | ratio | 越低越好 * | 每个发出的请求中以错误失败的比例 |
| `successful_job_latency_p50_ms` | ms | 越低越好 * | *成功*任务的中位耗时，包含重试；名字本身就是限定条件 |
| `successful_job_latency_p95_ms` | ms | 越低越好 * | 成功任务耗时的 95 百分位：用户会察觉到的尾部 |
| `successful_job_latency_p99_ms` | ms | 越低越好 * | 成功任务耗时的 99 百分位 |
| `acquire_wait_p50_ms` | ms | 越低越好 * | 步骤等待租约的中位耗时（在成功的获取上统计） |
| `acquire_wait_p99_ms` | ms | 越低越好 * | 租约等待的 99 百分位：一个饱和的资源池会让愿意等待的调用方付出多少代价 |
| `request_latency_p50_ms` | ms | 中性 | 已服务请求延迟的中位数（世界的属性） |
| `utilization` | ratio | 越高越好 * | 已服务的工作·秒除以提供的容量·秒，在本次运行自身的完工时间上积分 |
| `endpoint_spread` | ratio | 中性 | 已准入请求在各端点间的分散程度；诊断性指标，因为这些端点刻意互不相同 |
| `leases_active_at_end` | leases | 越低越好 | 运行结束时仍持有的租约；必须为零。唯一一个有方向、但不构成质量主张的计数器 |
| `wall_s` | s | 中性 | 测试装置花在仿真上的真实秒数：成本，绝不是质量 |

`*` 标记**要求完成量可比**的行（`Metric.requires_comparable_completion`）。
规则并不是“没有完成量该指标就无定义”——那是它的第一版，而且
过于狭窄。规则是：

> 只有当各算法完成了可比数量的有效工作时，一个有方向的质量指标才可以给出比较性的胜者。
> `jobs_done` 本身就是“完成了多少工作”的比较，而
> 正确性/诊断计数器不作任何质量主张，所以它们是仅有的不受门控的行。

原因是，这里几乎每一个比率的分母都由客户端掌控。`refusal_rate` 是
`refusals / requests_sent`，而一个在发问之前就放弃竞争的算法根本不会发出可被拒绝的
请求；`error_rate` 和 `failed_attempt_rate` 形状相同；`retry_rate` 对一个
从未走得足够远、因而遇不上可重试失败的算法来说自然很低；而 `utilization` 是在一段
*长度*由客户端选定的运行上对提供的容量做积分，所以一次八秒就停下的运行，从不让
它未使用的容量有机会被使用。只完成 0.2% 工作的算法不该赢下
这些行中的任何一行，而 `test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row` 通过
把数值上最优的值交给什么都不做的算法，在每一行上检查了这一点。

被排除的算法会在 `best` 列和 JSON 的
`excluded_by_completion` 中连同其完成比例一起具名列出。没有这道门控，这张表的第一个版本曾让一个只完成
3000 个任务中 7.7 个的策略，于两行延迟和吞吐量上胜出，因为快速失败很快、
留下的慢任务也少。门控也是让诚实的吞吐量分母
（`jobs_done / makespan_s`）变得安全的原因，而它施加于各种比率则是出于上述理由：一个
只尝试三个简单步骤就放弃队列的算法，会读到 `attempt_inflation` 为 1.0、`retry_rate` 为 0.0
以及一个讨喜的 `refusal_rate`——全都是空洞的，全都被排除。

`attempts_per_completed_job` 和 `attempt_inflation` 回答不同的问题，而这个差别正是
重点：前者是完成要付出什么代价（当尝试被浪费在后来失败的任务上时它会变大），
后者是一次尝试被迫重复的频率（它不随数量增长，因此可以比较完成工作量不同的算法之间的重试
压力）。把重试策略纳入比较的基准测试会想要后者；
问“完成一个任务要我付出多少”的基准测试则想要前者。
`failed_attempt_rate` 和 `retry_rate` 的分法相反：前者统计每一次失败的尝试，
后者只统计导致再一次尝试的失败，所以两者之差就是策略
判定为无望的失败占比。

**一个向量，而不是一个综合值。** 把吞吐量、p99 延迟和“你把提供方惹毛了多少”
折成一个加权数字，会把权重——那才是真正的观点——藏进一个
看起来客观的算术结果里。报告改为逐指标列出最佳算法，于是读者能看到
某一行的胜者往往是另一行的败者。

**读表。** `*` 标记每一个有资格主张该行的算法：最佳均值，加上任何
差距与噪声无法区分的算法。两道护栏，取其中更宽的一道：

* `TIE_TOLERANCE = 0.01`，以相对值计——在三个种子下，1% 的差距说明不了任何事；以及
* `SIGNIFICANCE_K = 2.0` 个标准误，基于**配对的逐种子差值** `d_i = metric(challenger,
  seed_i) - metric(leader, seed_i)`，在两个算法都跑过的种子上按 `stdev(d) / sqrt(n)` 估计。

正是第二道护栏阻止报告在种子间波动 4% 的指标上把 1.3% 的胜绩判为胜出，
而它之所以是**配对**的，是因为每个算法都跑每个种子：两个
算法共有的种子间噪声相互抵消，因此被检验的是差值，而差值小恰恰说明公共随机
数起了作用。独立样本形式 `sd_leader^2/n + sd_challenger^2/n` 会丢掉这个设计
最强的性质——事实也确实如此，直到评审指出这一点。每个挑战者都与
领先者比较，而不是与不断扩张的组比较，这让规则保持一行之长、
含义一目了然。`SIGNIFICANCE_K = 2.0` 是一个**启发式**，刻意不描述为 95%：在
三个种子下 Student-t 临界值是 4.3，而宣称在 2.0 处显著会断言超过
实验所能支持的东西。

除了方向为 `neutral` 之外（`makespan_s`、`jobs_failed`、
`jobs_unstarted`、`requests`、`request_latency_p50_ms`、`wall_s`、`endpoint_spread`），一行最终没有标记还有三种情形：

* **合格竞争者少于两个。** 星号是一个比较性陈述，而一个算法不构成
  比较——要么因为只有一个跑了，要么因为完成度门控只留下一个。在后一种
  情况下，报告会点出留下的那个（JSON 中的 `not_compared`，markdown 中的 `only X was eligible`），
  因为那里的沉默会被读成“没人做得好”；
* **每个算法在该行都为零**（`tests/test_benchmark_report.py::test_a_metric_that_is_zero_for_everyone_crowns_nobody`）；
* 它被点名要求，但场景声明它不适用，于是该列留下 N/A，
  该算法也完全不在排名之中。

连同门控一起，这让胜者规则只需一句话：*质量行在完成工作量可比的
算法之间裁决，且只有在至少两个算法能够被比较时才裁决。*

只有一个种子时没有可供检验的离散度，因此在跑过的算法中由最佳均值胜出
——一个*种子*是弱比较，缺失的 `±` 说的正是这一点。而只有一个*算法*则是另一回事：
它永远不会胜出。

**种子** ——
`run_benchmark` 从 `scenario.seed` 推导种子列表（`seed + 0 .. seeds - 1`），让每个算法
在每个种子上运行，并报告均值以及来自 `aggregate()` 的 `min`、`max` 和样本 `stdev`；markdown
形式打印 `mean ±stdev`。在三个种子下，1% 的差距说明不了任何事，这就是为什么离散度
会打印在每个均值旁边，并喂给上面的显著性护栏。

## 7. 如何运行

`uv run pyattacker bench [options]`。完整的参数表在
[`docs/cli.md`](cli.md#bench--在模拟中比较获取算法)；`pyattacker bench --list`
会打印同样的接口面，外加场景、算法和每一个指标，当两者不一致时
就该运行它。

第 8 节引用其数字的完整示例是 `uv run pyattacker bench --markdown
/tmp/bench-full.md`：这个场景适用的五个算法，x 3 个种子 = 15 次运行，每次 3000 个任务 x 3
个步骤 x 2 次调用、12 个 worker，在一台笔记本级机器上耗时 **13 秒墙钟时间**（运行
之间的离散度来自机器；仿真本身是确定性的）。`--algorithms` 带上全部七个的代价
大约再多一半（实测 20.2s），并把两个不适用的列标为 N/A。在一个更小的
世界上对两个算法做快速检查（`--algorithms wait,immediate --seeds 1 --jobs 200 --concurrency 4
--horizon 60`）大约花 0.13s。

Python API 是同一套机制：

```python
from pyattacker.benchmark import Harness, get_scenario, run_benchmark

scenario = get_scenario("bursty_provider")
report = run_benchmark(scenario, ["wait", "least_busy"], seeds=3)
report.render_table()      # terminal table, `*` on each row's best mean
report.render_markdown()   # markdown, plus per-endpoint admissions
report.to_dict()           # JSON-serialisable, aggregates and winners included
one = Harness(scenario.with_overrides(jobs=120, concurrency=6, horizon_s=90.0), "wait", seed=3).run()
one.metrics["throughput_rps"], one.error_classes, one.endpoint_admitted
```

**编写你自己的场景。** 那些数据类就是 API：`Scenario`、`EndpointProfile`、
`LatencyProfile`、`FailureProfile`、`RateLimitProfile`、`LoadCycle`。`with_overrides(**changes)`
返回一份副本，不触碰 `SCENARIOS`，场景也可以手工构造
（`tests/test_benchmark_scenario.py::test_an_overridden_scenario_is_a_copy_and_leaves_the_registry_alone`、
`::test_a_scenario_can_be_built_by_hand`）。给它一个 `name`、一行 `summary`（markdown 渲染器
会打印它）和若干端点。含多个端点的场景在测试装置里仍然是**一个** `Pool`，
因此它目前还无法检验跨厂商的 `failover`（第 9 节）。

**留在预算之内。** 默认值离 10 分钟的上限还差得远：整轮 15 次运行
实测 13.0s（带上全部七个算法是 20.2s），而每次运行的成本从 0.09s（`immediate`）到
1.37s（`quota_aware`，它是唯一在各种子间变化较大的）。可调的旋钮是 `--jobs` 和
`--seeds`（它们直接缩放工作量），然后是 `--horizon`，再是 `--concurrency`（它改变的是世界，
而不只是成本）；`--wall-budget` 是护栏，不是调优旋钮。

**`BenchmarkTimeout`。** `Harness.run` 启动 worker 和一个监督器，等待先完成的
那一个，条件是实时的 `asyncio.wait(..., timeout=wall_budget)`，然后取消两者并抛出异常，而不是
返回部分指标——一次无法完成的运行会如实说明。如果仿真时间推进过，消息会
报告秒数和已完成任务数，并建议跑一次更便宜的；如果它从未推进，消息会说这次运行
“stalled, not slow”（worker 挂起、定时器待定、有东西在等待一个不可能发生的事件），
并给出 worker、挂起和待定的计数。监督器把另一种无望情形——每个
端点都永久 DEAD 或 REVOKED，再也发不出任何租约——在 50ms 墙钟时间内变成 `BenchmarkStalled`，
而不是一个预算形态的超时。这三种情形都由
`tests/test_benchmark_harness.py::test_the_wall_budget_is_real_time_and_reported_as_a_failure_not_a_result`、
`::test_a_stalled_simulation_says_so_instead_of_timing_out_silently` 和
`::test_a_world_with_no_lease_left_is_reported_in_a_second_not_a_budget` 覆盖。

## 8. 当前数据说明了什么

上面那一轮的 3 种子均值（种子 20260917、20260918、20260919），已包含评审后的修复：
`--markdown` 写出完整的表格，含 `min`/`max`/`stdev` 和逐端点准入情况，而 `--json`
带有胜者和被排除者。在 `best` 列中，`—` 表示该行区分不出任何人，`*`
标记要求完成量可比的行。

| 指标 | `immediate` | `wait` | `backoff` | `sticky` | `quota_aware` | best |
|---|---|---|---|---|---|---|
| `jobs_done` | 7.00 | 1,348.67 | 1,361.67 | 1,404.00 | 843.67 | `sticky`, `backoff` |
| `jobs_failed` | 2,993.00 | 547.00 | 508.00 | 545.67 | 1,088.67 | — |
| `jobs_unstarted` | 0.00 | 1,104.33 | 1,130.33 | 1,050.33 | 1,067.67 | — |
| `makespan_s` | 8.25 | 606.45 | 606.57 | 606.05 | 610.83 | — |
| `throughput_rps` | 0.8910 | 2.2241 | 2.2449 | 2.3167 | 1.3809 | `sticky`, `backoff` * |
| `attempts_per_completed_job` | 459.65 | 5.69 | 5.57 | 5.55 | 10.80 | `sticky`, `backoff` * |
| `attempt_inflation` | 1.0077 | 1.6160 | 1.5947 | 1.5899 | 2.3396 | `sticky`, `backoff` * |
| `failed_attempt_rate` | 0.9930 | 0.4516 | 0.4393 | 0.4401 | 0.6856 | `backoff`, `sticky`, `wait` * |
| `retry_rate` | 0.0077 | 0.3803 | 0.3725 | 0.3700 | 0.5705 | `sticky`, `backoff` * |
| `refusal_rate` | 0.3387 | 0.2600 | 0.2484 | 0.2573 | 0.4760 | `backoff`, `sticky` * |
| `error_rate` | 0.0191 | 0.0244 | 0.0254 | 0.0223 | 0.0218 | `wait`, `sticky` * |
| `successful_job_latency_p95_ms` | 7,755.55 | 9,603.35 | 9,456.53 | 9,288.31 | 9,910.48 | `sticky`, `wait` * |
| `utilization` | 0.4928 | 0.6313 | 0.6384 | 0.6351 | 0.5026 | `wait`, `backoff`, `sticky` * |
| `endpoint_spread` | 0.6538 | 0.5707 | 0.5640 | 0.5759 | 0.9368 | — |

`*` 标记本表中要求完成量可比的行：`immediate`（最佳
完成量的 0.5%）和 `quota_aware`（60.1%）被排除在每一行之外，并在
表格和 JSON 中都列为被排除者。这样的行总共有十三个——所示九行，加上
`successful_job_latency_p50_ms`、`successful_job_latency_p99_ms` 以及两个 `acquire_wait_*` 百分位；
后者因为宽度原因没有放进这张表，但都在 `--markdown` 的表里。唯一*没有*
被标记的有方向行是 `jobs_done`（比较本身）和 `leases_active_at_end`（一个正确性计数器）。

**`immediate` 被测量了，然后被排除。** 它完成了 3,000 个任务中的 7.00 个（0.23%），失败了
2,993.00 个。`jobs_unstarted`（0.00）不再是它的一个胜项——这个记账行现在是诊断性的，而
“启动一切、什么也不完成”并不是一种质量。它在 `successful_job_latency_p95_ms`
（7,755.55ms）和 `successful_job_latency_p99_ms`（8,152.07ms）上是*原始*领先者，而这
恰恰是完成度门控所阻止的快速失败假象：只达到最佳完成量的 0.5%，它被排除
在每一个质量行之外，表格也具名说明了这一点。它的 `failed_attempt_rate`（0.9930）是表中
最差的——它获准发出的几乎每一个请求都失败了——而它的 `retry_rate`（0.0077）是
最低的，因为在拒绝与它自己拒绝排队之间，它几乎安排不了任何重试。这种倒挂
正是这两个指标必须分开、也是第二个指标要受门控的原因：“安排的重试很少”
只有在一次几乎什么都没尝试的运行里才读起来像优点。同样的推理让它在本次
修订中从 `refusal_rate` 和 `error_rate` 两行中被移除：发出 71.7 个请求而不是 12,246 个，意味着
那些分母里几乎没有东西可被计入，所以那里的低比率只是在说明
它索取了多少。

**`backoff` 和 `sticky` 领先；`wait` 紧随其后。** 配对检验把它们在 `jobs_done`
（1,361.67 和 1,404.00，对 `wait` 的 1,348.67，逐种子离散度为 23-77 个任务）、吞吐量、
`successful_job_latency_p50_ms`（3,522ms 和 3,368ms，对 3,574ms）以及 `acquire_wait_p99_ms` 上放在一起。
只有 `sticky` 独自领先 `successful_job_latency_p99_ms`（11,668.53ms，对 `wait` 的 12,501.36ms）。差距
很小，诚实的总结是“这三个在这个世界里很难分开”，而不是“sticky 以 1.3%
领先”：配对差值比绝对离散度更紧，这正是种子的用处，
但它们在一次 10 分钟的仿真运行上仍然是个位数百分比。

**重试压力比延迟更能把它们分开。** 三个领跑者的 `attempt_inflation` 是 1.59-1.62，
而 `quota_aware` 是 2.34：领先者典型的一个步骤大约要尝试一次半，
`quota_aware` 则超过两次又三分之一。`wait` 落在 `failed_attempt_rate` 的平局之内，尽管
它的均值是三者中最高的（0.4516，而 `backoff` 是 0.4393），并且它的重试也是三者中
最多的（0.3803，对 0.3725）：是配对差值把它放进平局，而平局是对
三个种子能看见多少的诚实陈述。三者的 `attempts_per_completed_job` 是 5.55-5.69，
而这个场景的三个步骤若从不重复任何一个，读出来会是 3.00——
重复占了一个完成任务所付代价的很大一部分。

**`quota_aware` 落败，原因在机制中清晰可见。** 843.67 个任务，对 `wait` 的 1,348.67
（-37%）；`refusal_rate` 0.4760 对 0.2600；`endpoint_spread` 0.9368（五者中最失衡的）。它
按*声明的*配额排名（`QuotaAware.score` 读取 `resource.options["quota"]["tokens"]`），而那并不是
它实际会遇到的拒绝：提供方依据实时的在途容量和自身带收紧的令牌桶
来拒绝，声明的数字对这两者都不反映。在每个算法的最后一次运行（种子
20260919）中，它把 6,434 个已准入请求中的 2,136 个发往 `fast-flaky`（错误率和风暴率
最高的端点），只把 306 个发往 `metered`，而 `wait` 分别发了 4,618 和 1,300 个。它的
`error_rate`（0.0218）与其他算法相当，尽管它挑起的拒绝是别人的两倍：更少
流量发往不稳定的端点是一种不同的组合，而不是一个更好的客户端。

**有若干行不产生胜者，而这正是答案。** `makespan_s`、`requests`、`jobs_failed`、
`jobs_unstarted`、`request_latency_p50_ms`、`wall_s` 和 `endpoint_spread` 被声明为中性——
中间两个是因为它们的方向可以靠少做事来赢，而 `wall_s`/`endpoint_spread` 则是因为它们
一个是成本、一个是对一组刻意异构端点的描述（把更多流量发往可靠
端点反而会*提高*分散度）。`acquire_wait_p50_ms` 对每个完成了工作的候选者都是 0.00
（客户端很少等待*租约*：在这个场景里拦住请求的是提供方的拒绝，
而它是在租约已被持有时遇到的），经过完成度门控后它完全没有胜者。
`leases_active_at_end` 处处是 0.0，而在处处为零的规则下不产生胜者。`error_rate` 是一个
双向平局（`wait`、`sticky`），出现在四个完成量可比的算法之间：独立的失败
率在这里根本不是区分这些算法的东西。

**表中没有的东西。** `failover` 和 `least_busy` 被声明为不适用于这个场景，
默认不运行：只有一个资源池，`failover` 没有可转移的目标，而资源池的默认选择
本来就是最不忙者优先，所以 `least_busy` 在这里与 `wait` 走的是同一条代码路径。显式运行它们
会把两列都标为 N/A，而不是打印出让读者去比较的数字。这是基准测试
就框架暴露出来的一个事实，理应公开——见第 9 节的多厂商场景。

## 9. 局限与下一步

* **单一资源池。** `Harness` 构造一个 `Pool`，并以一个名字放进 `TaskContext.pools`。这
  就是 `failover` 和 `least_busy` 被声明为不适用（第 8 节）而不是参与排名的原因：多厂商
  场景需要多个资源池，而不只是多个端点，它是显而易见的下一个场景。
* **`sticky` 的亲和性只跨越一个步骤内的各次调用。** `TaskContext` 按每次尝试创建，
  因此第一次调用记录的亲和性对第二次可用，随后就被丢弃——每个任务六次调用
  中的两次。一个复用有回报的世界（每端点一个缓存）才能显示它值多少。
* **利用率对一个连续周期积分**（`capacity x weight x integral of factor`），而
  提供方执行的是整数 `max(1, round(capacity x weight x factor))`：这是对阶跃函数的
  一种平滑近似，在算法之间可比，但不精确。
* **完成度门控是一件粗放的工具。** 最佳完成量的 90% 是一个阈值，不是
  统计陈述，而且它施加于各种子上的*平均*完成量。一个感知完成量的延迟
  指标（比如受限平均完成时间）根本不需要阈值；如果延迟行最终被证明重要，
  它就是自然的下一步。
* **两个种子不是一个样本。** 配对检验对它能看见的噪声是诚实的，但在三个种子下
  它只能检测出较大的效应；`--seeds` 是旋钮，`±` 列是警告。
* **真实时钟模式按设计就是缓慢且 CPU 开销膨胀的。** 在 `speedup=20` 时，真实 CPU 时间让
  仿真老去 20 倍；它的存在是为了让 `VirtualClock` 保持诚实，而不是为了产出数字。
* **只随附一个场景。** `SCENARIOS` 只有一个条目，所以今天“这个排名”意味着“这一个
  世界里的排名”——与第 1 节同样的警告，只是来自另一个方向。

候选场景，大致按它们能增添的价值排序：**多厂商 failover**（两个资源池，其中一个
正在降级，这是测量 `failover` 并把 `least_busy` 与默认策略区分开的唯一办法）；
一个死掉的端点（容量压得很低，或风暴率为 1.0），看哪些策略会察觉、哪些会继续
瞄准它；一次故障后的惊群效应（端点恢复时每个 worker 同时重试，
而 `backoff` 的抖动应当在这里赚回成本）；以及一个因复用而有回报的每端点缓存——
`sticky` 正是为这个世界写的。
