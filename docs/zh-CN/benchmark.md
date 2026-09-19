# 获取算法的基准测试

[English](../benchmark.md) | **简体中文**

`pyattacker.benchmark` 模拟一个 API 接口方，让库里真实的获取算法在上面跑。回答的问题很简单：**这个负载该用哪个获取算法？** 场景是输入，指标向量是输出，结果可以用场景名、种子和种子数复现。这个包只依赖标准库，不开任何 socket。

## 1. 它是什么，不是什么

它不是微基准测试：这里不测本机性能，`wall_s` 是*成本*列，不是质量指标。它是仿真，所以数字取决于场景里写死的假设——改一个假设，排名就可能变，因为基础场景的令牌桶会惩罚猛打它的客户端。由此定了两条规矩：不搞综合得分（第 6 节），场景可以报告"没人赢"（基础场景故意放了三个性格不同的端点）。

## 2. 一次运行怎么跑

`场景 -> 模拟接口方 -> 闭环 worker -> 指标`。

* **场景**（`scenario.py`）—— 用冻结数据类定义的假设：周期、延迟、失败、限流、端点、任务形态、重试策略、预算。
* **接口方**（`provider.py`）—— `perform(endpoint_id)` 要么正常服务一个请求，要么用 429 拒绝，要么用厂商风格的错误让它失败；它知道世界的全部状态，但一个字都不告诉客户端。
* **worker**（`harness.py`）—— `concurrency` 个 worker 领任务，每个任务含 `steps_per_job` 个顺序步骤，每步通过 `ctx.acquire_lease(pool, algorithm=...)` 租 `calls_per_step` 次。`Pool`、`TaskContext`、各个算法、`Retrying` 和 `errors.error_class_of` 全是真实实现。

**这里故意不用 `Runner`。** 它的调度、检查点和存储对每个算法都一样，而且它里面有仿真时钟管不到的实时等待（心跳的 `asyncio.sleep`、若干次 `asyncio.wait_for`）。驱动它要么把虚拟时钟卡死，要么逼运行切换到真实时钟——第 5 节就是为了避免后者才存在的。同样的原因，这里传 `deadlock_warn_s=None`：死锁检测要吃一个真实定时器。

## 3. 基础场景 `bursty_provider`

一个厂商，三个端点；3000 个任务 × 3 步 × 2 次调用，12 个 worker，600s 时间上限，所有算法共用同一套重试策略（`Retrying(max_attempts=5, base=0.5, factor=2.0, cap=8.0, jitter="full")`）。`sticky` 和 `least_busy` 有事可做，纯粹因为一个任务就是一条流水线，场景测试对此做了断言（`tests/test_benchmark_scenario.py::test_the_base_scenario_covers_the_problems_it_claims_to`）。

| 假设（字段） | 对应什么现实问题 | 区分什么 |
|---|---|---|
| 容量周期：`LoadCycle(period_s=120, trough=0.35, peak=1.0)`，从低谷开始；容量 = `max(1, round(capacity × weight × factor))` | 厂商自己的负载波动、你应得的公平份额、某区域故障转移带走你的容量 | 把容量当静态值的策略（`immediate`，以及 `quota_aware` 写死的配额）和会重试重排的策略之间的对比。让*什么时候*问和*问谁*一样重要 |
| 被施压就收紧的令牌桶：按 `per_window/window_s` 补充，`burst` 允许突发；每次拒绝把额度乘以 `tightening`（下限 `tightening_floor`），每个窗口按 `recovery_per_window` 恢复 | 带 `Retry-After` 的 `429`——这是写得再好的客户端也常翻车的地方 | 分散负载慢慢拿回容量的客户端，和一直猛打一直丢容量的客户端之间的对比。`metered` 按 0.35 收紧，1.5s 后重试；其他按 0.5 |
| 延迟不是单一数值：对数正态主体（`median_s`、`sigma`）加一条慢尾（`tail_rate`、`tail_factor`）；三个端点分别是 0.25s/0.5、1.2s/0.3、0.5s/0.6，尾部概率 2%、1%、3% | 排队等其他租户、GC 停顿、模型长 prompt 输出慢 | "在途数量"和"最近最快"到底靠不靠谱，把 p50 的故事和 p95/p99 的故事分开 |
| 带相关性的失败：`FailureProfile`（默认 `error_rate=0.01`、`storm_rate=0.05`、`storm_duration_s=30`、`storm_error_rate=0.5`，每个端点可单独覆盖）；风暴按 `(seed, endpoint, time window)` 判定 | 独立的瞬时抖动，加上一次接口方事故，或者某区域网络短暂抽风 | 算法多快察觉某个端点现在不行了，以及为此烧多少重试预算。错误类型是 `ProviderError(503)`、`ConnectionError`、`TimeoutError` 和 429，框架自己的分类逻辑会跑起来 |
| 几个性格不同的端点：`fast-flaky`（容量 6、0.25s、错误率 0.03、风暴 0.08 概率、时长 0.6）、`slow-steady`（容量 12 × 权重 1.2、1.2s、错误率 0.004、风暴 0.02）、`metered`（容量 4、0.5s、配额 180） | 两家厂商，再加一个受突发限制的试用 key；那个又便宜又快的恰恰是最容易出问题的 | 位置和时间之间的权衡。没有哪个策略在这三者上全能，这就是为什么报告必须能输出多个胜者或零个 |
| 声明的配额：每个端点的 `quota_units`（1200/2400/180），通过 `lease.report(usage={"tokens": n})` 消耗 | 额度可见的套餐 | `quota_aware` 的排序信号。它是声明出来的元数据，不是接口方的拒绝行为——见第 8 节 |

## 4. 为什么这个对比是公平的

| 规则 | 在哪里 | 哪个测试保证 |
|---|---|---|
| 世界的脾气是时间的函数，不是调用方的函数：容量是对 `t` 的算术，`storm_until(endpoint, t)` 是 `(seed, endpoint, t)` 的纯函数——风暴从时间窗口起点开始，天气不取决于请求什么时候到 | `provider.py`（`capacity_at`、`storm_until`、`_storm_window`） | `tests/test_benchmark_provider.py::test_storm_state_does_not_depend_on_request_cadence` 在同一批时间戳上对比两种*不同*的流量安排，`::test_the_storm_interval_is_anchored_to_its_window` 钉住锚定 |
| 公共随机数：延迟和失败的骰子来自 `random.Random(f"{seed}:{endpoint}:{ordinal}")`，按请求*在该端点*的序号索引。两个算法共享同一套**外生**随机性——同样的骰子、同样的天气——但不共享相同的实际接口方状态，后者会分化，因为令牌桶和在途数量会对各自行为做出反应 | `provider.py`（`perform`） | `tests/test_benchmark_provider.py::test_each_request_sees_the_world_by_its_ordinal_whatever_the_timing`；`tests/test_benchmark_harness.py::test_two_algorithms_see_the_same_exogenous_draws` 对比两者都到达的每个序号，共享请求少于 20 个就判失败 |
| 客户端侧的随机数跟着逻辑身份走，不跟 worker：获取算法和重试策略各自从 `Random(f"{seed}:{acquire,retry}:{job}:{step}:{attempt}")` 取数 | `harness.py`（`acquire_stream`、`retry_stream`） | `tests/test_benchmark_harness.py::test_client_randomness_follows_the_logical_identity_not_the_worker` |
| 场景会声明它测不了的算法，这些算法不参与排名，不会拿没意义的数字凑数 | `scenario.py`（`Scenario.unsuited`）、`report.py`（`default_algorithms`、`winners`） | `tests/test_benchmark_harness.py::test_a_scenario_declares_which_algorithms_it_cannot_exercise`、`::test_an_algorithm_the_scenario_cannot_exercise_is_marked_not_ranked` |
| 只有冷却到期才能结束的等待，是资源池时钟上的真实定时器，仿真时间会推到它，等待者会被唤醒——而且由*最早*的待处理截止时间持有定时器 | `resource.py`（`_ensure_cooldown_notifier`、`_cooldown_deadline`） | `tests/test_lease_safety.py::test_a_wait_that_only_a_cooldown_can_end_actually_ends` 和 `::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it`（内核回归测试：不修的话两个都会超时或延迟唤醒）、`tests/test_benchmark_harness.py::test_a_wait_that_only_a_cooldown_can_end_advances_simulated_time` |
| 只干了一小部分活的算法，不能在*任何*质量行上赢——包括完成量不足时无定义的延迟和吞吐量，以及它部分掌控分母的各种比率；被排除的在表格和 JSON 里都具名说明，基线会忽略场景声明不适用的算法，只有一个合格竞争者的受门控行干脆没有胜者 | `report.py`（`COMPLETION_FLOOR`、`excluded_by_completion`、`comparable`、`unrivaled`、`winners`）、`metrics.py`（`Metric.requires_comparable_completion`） | `tests/test_benchmark_report.py::test_every_quality_row_requires_comparable_completion`、`::test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row`（对全部 13 个受门控行参数化）、`::test_a_quality_metric_cannot_crown_an_algorithm_that_completed_almost_nothing`、`::test_the_completion_baseline_ignores_an_algorithm_the_scenario_cannot_exercise`、`::test_one_eligible_contestant_is_not_crowned` |
| 算法拿不到环境的任何引用：测试装置只构造一个 `Pool` 和一个 `TaskContext`，别的不给；`SimulatedProvider` 对它保持私有，`capacity_at()` 公开只是为了指标能对提供的容量积分 | `harness.py`（`_context`、`_call_sequence`） | `tests/test_benchmark_harness.py::test_the_algorithm_is_handed_the_pool_but_never_the_environment` 遍历交给 spy 算法的每一样东西，只要能碰到场景或接口方就判失败 |
| 同一个种子精确复现同样的数字（除 `wall_s` 外所有指标） | `scenario.seed` → `Harness.seed` → 每次运行的随机流 | `tests/test_benchmark_harness.py::test_the_same_seed_and_scenario_reproduce_the_same_numbers` |
| 没有 socket，包内没有任何东西能访问网络 | 只 import 标准库 | `tests/test_benchmark_harness.py::test_the_benchmark_opens_no_network_connections`；`::test_the_benchmark_package_imports_nothing_that_can_reach_the_network` |
| 错误就是厂商 SDK 会抛的那种，由 `errors.error_class_of` 分类，不手写标签 | `provider.py`（`ProviderError`） | `tests/test_benchmark_provider.py::test_failures_look_like_provider_errors_the_framework_can_classify` |
| 利用率的"提供的容量"不取决于客户端什么时候发请求：周期用闭式积分算，不是一个个到到达点累加 | `scenario.py`（`LoadCycle.integral`） | `tests/test_benchmark_scenario.py::test_the_closed_form_integral_matches_a_numeric_one` |

## 5. 时间

接口方的时间是模拟的，因为有意义的场景要跑好几分钟（一个容量周期、一场风暴、一个配额窗口），而每个算法都花真实时间跑几分钟没人会跑。`VirtualClock` 维护一个定时器堆，把整个仿真推到下一个截止时间；`sleep()` 是对 future 的真实 await，调用方阻塞，由驱动器决定截止时间什么时候到。

只有三条同时满足，时间才往前走：(1) 有待处理的定时器；(2) 每个已注册的 worker 都挂起了——要么在 `sleep()` 里，要么在 `blocked()` 里——后者是包在 await 外面的标记，表示这个 await 只能由一次归还或时间到了（一次租约获取）来解开；(3) 事件循环连续跑了 16 个探测回调，中间没有任何可观察的活动。光有 (2) 不够：一个协程可能停在一个本来就能解的 await 上——一个在它排队时就已经触发的广播、一个刚归还的租约——不耗时间它就能跑。条件 (3) 把这个窗口堵上：探测通过 `loop.call_soon` 重新武装，它会追加到就绪队列尾部，所以待处理的续体会在下一个探测节拍之前跑完。测试里的 `FakeClock` 不能用，因为它在睡眠者*内部*推进时间（`t += seconds`）：32 个 worker 同时睡 1s，就变成 32 个仿真秒。

手工算的时间线把这条规则钉死（`tests/test_benchmark_clock.py`）：32 个并发的 1s 睡眠让时钟停在 1.0，不是 32；定时器按截止时间先后触发，还在干活的 worker 会拖住时间；纯 yield 既不推进时间，也永不挂起。

**和真实时钟的差分校验。** `ScaledClock(speedup=10.0)` 用压缩后的真实时间跑，所以在两种时钟上跑同一个场景，能把推进规则里的 bug 暴露成一个错的数字，而不是一次挂死。`bursty_provider` 上用 `jobs=120, concurrency=6, horizon_s=90.0`、`wait`、种子 3：

| 指标 | `VirtualClock` | `ScaledClock(speedup=10.0)` |
|---|---|---|
| `jobs_done` | 120.0 | 120.0 |
| `makespan_s` | 80.253 | 81.916 |
| `refusal_rate` | 0.000 | 0.000 |
| `error_rate` | 0.029 | 0.029 |
| `utilization` | 0.316 | 0.310 |
| `throughput_rps` | 1.495 | 1.465 |

完工时间差不到 2%，利用率差 2%，这一对总共花 8.3s 墙钟。`throughput_rps` 两种时钟上都是 `jobs_done / makespan_s`，所以吞吐量一致和完工时间一致是同一件事——不是第三个独立测量。这验证的是计时和吞吐量，不是拒绝路径：这个小世界里需求从没打满容量，两种时钟报告的拒绝数都是零。

**时钟可能错过的一次等待。** 降级的资源是*惰性*恢复的——只有有人调 `state_at(now)` 时冷却才到期——而且这次到期不发任何事件。所以，一个所有资源都在冷却、没有在途租约的资源池，已经没东西可广播了：等待者停在广播上，它们真正等的那个截止时间悄悄过去了。仿真时钟下这就是挂死（已复现：`timers=0`、12 个等待者挂起、仿真时间冻结）；真实时间下这是个饥饿 bug，只有碰巧有别的活动通知了资源池才解开。`Pool` 现在每个资源池武装一个定时器，只有在有人等的时候才武装，在资源池的时钟上睡到 DEGRADED 槽位上最早的*未来*冷却时间，然后广播。三个细节很重要，一开始都是 bug：冷却已经到期的槽位绝不能武装零延迟定时器（那是个完全不推进仿真时间的活锁）；DEAD 或 REVOKED 的槽位必须忽略（它们永远不回来，过期的 `blocked_until` 会让定时器反复武装）；必须记录武装的*截止时间*，不只是武装的任务——因为开始得晚的冷却可能结束得更早：B 在 t=5 挂了带 5s 冷却，A 的 30s 通知器已经在睡。留着旧定时器会让等待者在 t=30 而不是 t=10 被唤醒，比它们等的资源恢复晚了 20 个仿真秒（`tests/test_lease_safety.py::test_a_cooldown_that_ends_earlier_than_the_armed_one_replaces_it` 用 `VirtualClock` 钉住了这两个数字）。

**一个再无可租之物的世界。** 如果每个端点最后都变成 DEAD 或 REVOKED——框架连续失败 `dead_after` 次就把一个端点永久退役——就再也发不出租约，worker 挂起，时钟也没东西可推进了。实时监视器会在 50ms 内发现，带着所有数字抛 `BenchmarkStalled`，不烧墙钟预算：这是场景的属性（一个可能全死的端点集合），不是算法或机器的属性。

**已知限制。** `Wait(timeout=...)` 和 `AcquireTimeout` 由事件循环的真实时钟管：`Wait` 阻塞在 `pool.wait_slot` 上，后者在 `asyncio.wait_for(..., remaining)` 里等一个 `asyncio.Event`。所以虚拟时钟下看不到模拟的获取超时，场景就避开了获取超时——测试装置获取时不传 `timeout`——用 `horizon_s` 限制运行，它会停新任务，但从不截断已经在跑的。

## 6. 指标

`METRICS` 是测试装置输出键、报告列、JSON 字段名和 `bench --list` 的唯一来源；有个测试断言测试装置输出的正好就是这个集合（`tests/test_benchmark_report.py::test_the_harness_emits_exactly_the_declared_metrics`）。

| 指标 | 单位 | 越优 | 含义 |
|---|---|---|---|
| `jobs_done` | jobs | 越高越好 | 时间上限截断之前，入队的任务中成功完成的数量。截断后不再入队新任务，但已经在跑的会跑完——所以 600s 的运行完工时间可以略大于 600s |
| `jobs_failed` | jobs | 中性 | 放弃的任务：某步重试预算用完了。诊断指标：算法可以靠从不启动那些会失败的任务来把它做小 |
| `jobs_unstarted` | jobs | 中性 | 时间上限先到了，worker 从没开始的任务。诊断指标：算法可以靠全启动让它们失败来把它做小 |
| `makespan_s` | s | 中性 | 从开始到最后一个 worker 停下的仿真秒数（时间上限，加上还在跑的一步）；和 `jobs_done` 一起看 |
| `throughput_rps` | jobs/s | 越高越好 * | 本次运行*自己*的完工时间内，每仿真秒完成的任务数——观测到的速率，不是预算容量 |
| `requests` | requests | 中性 | 发出的请求数，包括被拒的和重试的 |
| `attempts_per_completed_job` | attempts | 越低越好 * | 每个完成任务花了多少步尝试。零重试基线是 `steps_per_job`，不是 1.0；花在后来失败的任务上的尝试只算分子 |
| `attempt_inflation` | ratio | 越低越好 * | 每个*已尝试*步骤的尝试次数（1.0 = 每个已尝试步骤第一次就成功）：剔了数量因素后的重试压力 |
| `failed_attempt_rate` | ratio | 越低越好 * | 失败的步骤尝试占全部步骤尝试的比例，包括策略后来放弃的那些 |
| `retry_rate` | ratio | 越低越好 * | 策略实际安排的每步重试次数。`failed_attempt_rate` 减它就是被放弃的失败占比 |
| `refusal_rate` | ratio | 越低越好 * | 发出的请求中被接口方拒绝（429）的比例 |
| `error_rate` | ratio | 越低越好 * | 发出的请求中出错失败的比例 |
| `successful_job_latency_p50_ms` | ms | 越低越好 * | *成功*任务的中位耗时，含重试；名字本身就是限定条件 |
| `successful_job_latency_p95_ms` | ms | 越低越好 * | 成功任务耗时的 95 分位：用户能感觉到的尾部 |
| `successful_job_latency_p99_ms` | ms | 越低越好 * | 成功任务耗时的 99 分位 |
| `acquire_wait_p50_ms` | ms | 越低越好 * | 步骤等租约的中位耗时（只统计成功的获取） |
| `acquire_wait_p99_ms` | ms | 越低越好 * | 租约等待的 99 分位：资源池饱和时愿意等的调用方要付多少代价 |
| `request_latency_p50_ms` | ms | 中性 | 已服务请求的延迟中位数（世界的属性） |
| `utilization` | ratio | 越高越好 * | 已服务的工作·秒除以提供的容量·秒，在本次运行自身完工时间上积分 |
| `endpoint_spread` | ratio | 中性 | 已入队请求在各端点间的分散程度；诊断指标，因为这些端点刻意不一样 |
| `leases_active_at_end` | leases | 越低越好 | 运行结束时还持有的租约；必须为零。唯一有方向但不构成质量主张的计数器 |
| `wall_s` | s | 中性 | 测试装置跑仿真花的真实秒数：成本，不是质量 |

标 `*` 的行**要求完成量可比**（`Metric.requires_comparable_completion`）。规则不是"没完成量就没定义"——那是第一版，太窄了。规则是：

> 只有各算法完成了可比数量的有效工作，有方向的质量指标才能给出比较性的胜者。`jobs_done` 本身就是"干了多少活"的比较，正确性/诊断计数器不做质量主张，所以它们是唯一不受门控的行。

原因是，这里几乎每个比率的分母都客户端说了算。`refusal_rate` 是 `refusals / requests_sent`，一个还没发问就放弃竞争的算法根本不会发出可被拒绝的请求；`error_rate` 和 `failed_attempt_rate` 形状一样；`retry_rate` 对一个走得不够远、遇不上可重试失败的算法来说天然很低；`utilization` 是在一段*长度*由客户端选的运行上对提供的容量积分，跑 8 秒就停的运行，没用到的容量永远没机会被用。只干了 0.2% 活的算法不该赢这些行里的任何一行，`test_an_algorithm_that_does_almost_nothing_cannot_win_any_quality_row` 就是把数值上最优的值塞给啥都不干的算法，逐行检查这一点。

被排除的算法会在 `best` 列和 JSON 的 `excluded_by_completion` 里具名列出，带完成比例。没有这道门控，表格第一版曾经让一个只完成了 3000 个任务里 7.7 个的策略，在两行延迟和吞吐量上赢——因为快速失败快，留下的慢任务也少。门控也是让诚实的吞吐量分母（`jobs_done / makespan_s`）变得安全的原因；它施加在比率上是因为上面说的：一个试三个简单步骤就放弃队列的算法，`attempt_inflation` 是 1.0、`retry_rate` 是 0.0、`refusal_rate` 还好看——全是空话，全被排除。

`attempts_per_completed_job` 和 `attempt_inflation` 回答的问题不一样，这个区别才是重点：前者是"完成要付什么代价"（尝试浪费在后来失败的任务上时它变大），后者是"一次尝试被迫重复的频率"（不随数量涨，所以可以比较完成量不同的算法之间的重试压力）。把重试策略纳入比较的基准会想要后者；问"完成一个任务花我多少"的基准会想要前者。`failed_attempt_rate` 和 `retry_rate` 的分法反过来：前者统计每一次失败尝试，后者只统计导致再试一次的失败，两者之差就是策略判为没救的失败占比。

**一个向量，不是一个综合分。** 把吞吐量、p99 延迟和"你把接口方惹毛了多少"折成一个加权数字，等于把权重——那才是真正的观点——藏进一个看起来客观的算术结果里。报告改成逐指标列最佳算法，读者能看到某行的胜者往往是另一行的败者。

**怎么读表。** `*` 标记每个有资格主张该行的算法：最佳均值，加上差距和噪声分不清的那些。两道护栏，取更宽的那个：

* `TIE_TOLERANCE = 0.01`，相对值——三个种子下 1% 的差距说明不了什么；
* `SIGNIFICANCE_K = 2.0` 个标准误，基于**配对的逐种子差值** `d_i = metric(challenger, seed_i) - metric(leader, seed_i)`，在两个算法都跑过的种子上按 `stdev(d) / sqrt(n)` 估计。

正是第二道护栏阻止报告在种子间波动 4% 的指标上把 1.3% 的胜绩判成胜出。它是**配对**的，因为每个算法跑每个种子：两个算法共有的种子间噪声互相抵消，检验的是差值——差值小恰恰说明公共随机数起了作用。独立样本的公式 `sd_leader²/n + sd_challenger²/n` 会丢掉这个设计最强的性质——事实也确实如此，直到评审指出这一点。每个挑战者和领先者比，不和不断膨胀的组比，规则保持一行长、意思清楚。`SIGNIFICANCE_K = 2.0` 是个**启发式**，故意不说成 95%：三个种子下 Student-t 临界值是 4.3，在 2.0 处宣称显著等于说了实验撑不起的话。

除了方向是 `neutral` 的（`makespan_s`、`jobs_failed`、`jobs_unstarted`、`requests`、`request_latency_p50_ms`、`wall_s`、`endpoint_spread`），一行最后没标记还有三种情况：

* **合格竞争者不到两个。** 星号是个比较性陈述，一个算法不构成比较——要么因为只跑了一个，要么因为完成度门控只留了一个。后一种情况报告会点出来留下的那个（JSON 里的 `not_compared`，markdown 里的 `only X was eligible`），因为沉默在那里会被读成"没人做得好"；
* **每个算法该行都是零**（`tests/test_benchmark_report.py::test_a_metric_that_is_zero_for_everyone_crowns_nobody`）；
* 它被点名要测，但场景声明它不适用，那列留 N/A，该算法也不参与排名。

加上门控，胜者规则一句话：*质量行在完成量可比的算法之间裁决，至少两个算法能比才裁决。*

只有一个种子时没有离散度可检验，跑过的算法里最佳均值赢——一个*种子*是弱比较，缺的 `±` 说的就是这个。但只有一个*算法*是另一回事：它永远不会赢。

**种子**——`run_benchmark` 从 `scenario.seed` 推种子列表（`seed + 0 .. seeds - 1`），每个算法在每个种子上跑，报告均值和 `aggregate()` 出来的 `min`、`max`、样本 `stdev`；markdown 打 `mean ±stdev`。三个种子下 1% 的差距说明不了什么，所以离散度打在每个均值旁边，喂给上面的显著性护栏。

## 7. 怎么跑

`uv run pyattacker bench [options]`。完整参数表在 [`docs/cli.md`](cli.md#bench--在模拟中比较获取算法)；`pyattacker bench --list` 会打同一个接口面，外加场景、算法和每个指标，两者不一致时就跑它。

第 8 节引用的完整示例是 `uv run pyattacker bench --markdown /tmp/bench-full.md`：这个场景适用的五个算法 × 3 个种子 = 15 次运行，每次 3000 个任务 × 3 步 × 2 次调用、12 个 worker，笔记本级别机器上花 **13 秒墙钟**（运行之间的离散度来自机器；仿真本身是确定性的）。`--algorithms` 把七个全带上的代价大概多一半（实测 20.2s），把两个不适用的列标 N/A。小世界上对两个算法快速检查（`--algorithms wait,immediate --seeds 1 --jobs 200 --concurrency 4 --horizon 60`）约 0.13s。

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

**写自己的场景。** 那些数据类就是 API：`Scenario`、`EndpointProfile`、`LatencyProfile`、`FailureProfile`、`RateLimitProfile`、`LoadCycle`。`with_overrides(**changes)` 返回副本，不碰 `SCENARIOS`；场景也可以手工构造（`tests/test_benchmark_scenario.py::test_an_overridden_scenario_is_a_copy_and_leaves_the_registry_alone`、`::test_a_scenario_can_be_built_by_hand`）。给它一个 `name`、一行 `summary`（markdown 渲染器会打出来）和若干端点。多端点的场景在测试装置里还是**一个** `Pool`，所以它目前还测不了跨厂商的 `failover`（第 9 节）。

**别超预算。** 默认值离 10 分钟上限远着呢：整轮 15 次运行实测 13.0s（七个算法全带上是 20.2s），每次运行成本从 0.09s（`immediate`）到 1.37s（`quota_aware`，唯一跨种子波动大的）。可调的旋钮是 `--jobs` 和 `--seeds`（直接缩放工作量），然后是 `--horizon`，再是 `--concurrency`（它改的是世界，不只是成本）；`--wall-budget` 是护栏，不是调优旋钮。

**`BenchmarkTimeout`。** `Harness.run` 启动 worker 和一个监视器，等先完成的那个，条件是实时的 `asyncio.wait(..., timeout=wall_budget)`，然后取消两者抛异常，不返回半截指标——跑不完就如实说。如果仿真时间推进过，消息会报秒数和完成任务数，建议跑个便宜的；如果从没推进，消息会说这次运行"stalled, not slow"（worker 挂起、定时器待处理、有东西在等一个不可能发生的事件），给 worker、挂起和待处理的计数。监视器把另一种没救的情况——每个端点永久 DEAD 或 REVOKED，再也发不出租约——在 50ms 墙钟内变成 `BenchmarkStalled`，不是预算超时。这三种情况由 `tests/test_benchmark_harness.py::test_the_wall_budget_is_real_time_and_reported_as_a_failure_not_a_result`、`::test_a_stalled_simulation_says_so_instead_of_timing_out_silently` 和 `::test_a_world_with_no_lease_left_is_reported_in_a_second_not_a_budget` 覆盖。

## 8. 当前数据说明什么

上面那轮的 3 种子均值（种子 20260917、20260918、20260919），已包含评审后的修复：`--markdown` 输出完整表格，含 `min`/`max`/`stdev` 和逐端点准入情况；`--json` 带胜者和被排除者。`best` 列里 `—` 表示该行分不出高下，`*` 标记要求完成量可比的行。

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

`*` 标记本表中要求完成量可比的行：`immediate`（最佳完成量的 0.5%）和 `quota_aware`（60.1%）被排除在每一行之外，表格和 JSON 里都列了。这样的行一共 13 个——表里 9 个，加上 `successful_job_latency_p50_ms`、`successful_job_latency_p99_ms` 和两个 `acquire_wait_*` 分位；后三个因为宽度没放进这张表，但都在 `--markdown` 里。唯一*没*被标记的有方向行是 `jobs_done`（比较本身）和 `leases_active_at_end`（正确性计数器）。

**`immediate` 测了，然后被排除。** 它完成了 3000 个任务里的 7.00 个（0.23%），失败了 2993.00 个。`jobs_unstarted`（0.00）不再是它的胜项——这个计数行是诊断性的，"全启动、什么都不完成"不是质量。它在 `successful_job_latency_p95_ms`（7755.55ms）和 `successful_job_latency_p99_ms`（8152.07ms）上是*原始*领先者，而这恰恰是完成度门控要挡的快速失败假象：只到最佳完成量的 0.5%，它被排除在每个质量行之外，表格也具名说明了。它的 `failed_attempt_rate`（0.9930）是表里最差的——获准发出的几乎每个请求都失败了——而它的 `retry_rate`（0.0077）最低，因为在拒绝和自己拒绝排队之间，它几乎安排不了重试。这种倒挂就是为什么这两个指标必须分开、第二个必须受门控："安排的重试很少"只有在一次几乎啥都没试的运行里读起来才像优点。同样的道理让它在本次修订中从 `refusal_rate` 和 `error_rate` 两行里移除：发 71.7 个请求而不是 12246 个，意味着分母里没东西可算，那里的低比率只是在说它要了多少。

**`backoff` 和 `sticky` 领先，`wait` 紧随其后。** 配对检验把它们放在一起比较：`jobs_done`（1361.67 和 1404.00，`wait` 是 1348.67，逐种子离散度 23–77 个任务）、吞吐量、`successful_job_latency_p50_ms`（3522ms 和 3368ms，`wait` 是 3574ms）、`acquire_wait_p99_ms`。只有 `sticky` 独自领先 `successful_job_latency_p99_ms`（11668.53ms，`wait` 是 12501.36ms）。差距很小，诚实的说法是"这三个在这个世界里很难分开"，不是"sticky 以 1.3% 领先"：配对差值比绝对离散度紧，这就是种子的用处，但在一次 10 分钟的仿真上它们仍然是个位数百分比。

**重试比重试延迟更能把它们分开。** 三个领跑者的 `attempt_inflation` 是 1.59–1.62，`quota_aware` 是 2.34：领跑者典型一步试一次半，`quota_aware` 要试两次多。`wait` 在 `failed_attempt_rate` 上和 others 打平——虽然均值是三者最高（0.4516，`backoff` 是 0.4393），重试也最多（0.3803，`backoff` 是 0.3725）：是配对差值把它放进平局，平局是对三个种子能看见多少的诚实陈述。三者的 `attempts_per_completed_job` 是 5.55–5.69，这个场景三步一次不重复的话应该是 3.00——重试占了完成一个任务的代价的大头。

**`quota_aware` 输了，原因在机制里清清楚楚。** 843.67 个任务，`wait` 是 1348.67（-37%）；`refusal_rate` 0.4760 对 0.2600；`endpoint_spread` 0.9368（五者里最不均衡）。它按*声明的*配额排序（`QuotaAware.score` 读 `resource.options["quota"]["tokens"]`），但那不是它实际会遇到的拒绝：接口方按实时在途容量和自己带收紧的令牌桶来拒绝，声明的数字对这两者都不反映。每个算法最后一次运行（种子 20260919）里，它把 6434 个已入队请求中的 2136 个发往 `fast-flaky`（错误率和风暴率最高的端点），只发 306 个到 `metered`；`wait` 分别是 4618 和 1300。它的 `error_rate`（0.0218）和其他算法差不多，尽管它挑起的拒绝是别人的两倍：更少流量去不稳定端点是另一种组合，不是更好的客户端。

**有几行没有胜者，这本身就是答案。** `makespan_s`、`requests`、`jobs_failed`、`jobs_unstarted`、`request_latency_p50_ms`、`wall_s`、`endpoint_spread` 声明为中性——中间两个是因为靠少做事就能赢，`wall_s`/`endpoint_spread` 一个是成本、一个是对一组刻意异构端点的描述（把更多流量发往可靠端点反而*提高*分散度）。`acquire_wait_p50_ms` 对每个完成了工作的候选都是 0.00（客户端很少等*租约*：这个场景里拦住请求的是接口方的拒绝，在租约已经拿到之后才遇到），完成度门控后这行完全没有胜者。`leases_active_at_end` 全是 0.0，全零的规则下不产胜者。`error_rate` 是双向平局（`wait`、`sticky`），四个完成量可比的算法之间：独立的失败率在这里根本区分不了这些算法。

**表里没有的东西。** `failover` 和 `least_busy` 声明为不适用这个场景，默认不跑：只有一个资源池，`failover` 没地方切；资源池默认选最不忙的，`least_busy` 和 `wait` 走同一条代码路径。显式跑它们两列都标 N/A，不给读者可以比较的数字。这是基准就框架暴露出来的一个事实，应该公开说——见第 9 节的多厂商场景。

## 9. 局限和下一步

* **单一资源池。** `Harness` 构造一个 `Pool`，放 `TaskContext.pools` 里一个名字。这就是为什么 `failover` 和 `least_busy` 声明为不适用（第 8 节）而不是参与排名：多厂商场景需要多个资源池，不只是多个端点，这是显而易见的下一个场景。
* **`sticky` 的亲和性只跨一个步骤内的多次调用。** `TaskContext` 每次尝试新建，第一次调用记住的亲和性第二次还能用，然后就丢了——每个任务 6 次调用里的 2 次。一个复用有回报的世界（每端点一个缓存）才能看出它值多少。
* **利用率对连续周期积分**（`capacity × weight × integral of factor`），接口方跑的是整数 `max(1, round(capacity × weight × factor))`：这是对阶跃函数的平滑近似，算法之间可比，但不精确。
* **完成度门控是个粗工具。** 最佳完成量的 90% 是个阈值，不是统计陈述，而且施加在各种子的*平均*完成量上。一个感知完成量的延迟指标（比如受限平均完成时间）根本不需要阈值；如果延迟行最后证明重要，它就是自然的下一步。
* **三个种子不是样本。** 配对检验对它能看见的噪声是诚实的，但三个种子下只能检出较大的效应；`--seeds` 是旋钮，`±` 列是警告。
* **真实时钟模式按设计就是慢的、吃 CPU 的。** `speedup=20` 时真实 CPU 时间让仿真快 20 倍；它存在是为了让 `VirtualClock` 保持诚实，不是为了出数字。
* **只附了一个场景。** `SCENARIOS` 只有一个条目，所以今天"这个排名"就是"这一个世界里的排名"——和第 1 节同样的警告，只是方向不同。

候选场景，按能加的价值排序：**多厂商 failover**（两个资源池，其中一个在降级，这是测 `failover`、把 `least_busy` 和默认策略区分开的唯一办法）；一个死掉的端点（容量压到很低，或风暴率 1.0），看哪些策略会察觉、哪些还会继续打它；故障恢复后的惊群效应（端点恢复时所有 worker 同时重试，`backoff` 的抖动应该在这里赚回成本）；一个因为复用而有回报的每端点缓存——`sticky` 就是为这个世界写的。
