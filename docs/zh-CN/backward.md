# 进阶：反向遍历与载荷历史

[English](../backward.md) | **简体中文**

反向遍历是可选启用的，在 1.0 之前属于实验性功能。它保持相同的流水线同一性、
资源池和数据集行，但允许已声明的重访。普通流水线以及仅正向的
控制声明保持原有的 ID、摘要和行为。

## 用普通字典回退

回退会改变下一个站点及其输入。状态由你选择；框架不会回滚
键，不会合并旧字典，也不会猜测较早的任务能接受什么。

```python
from pyattacker import Handoff, Runner, pipeline, task

@task("generate")
def generate(value, ctx):
    return {**value, "answer": "bad" if ctx.visit == 0 else "good"}

@task("validate")
def validate(value, ctx):
    if value["answer"] == "bad":
        return Handoff.rewind("generate", {
            "prompt": value["prompt"],
            "feedback": "Use the requested schema",
        }, reason="invalid structured output")
    return value

template = pipeline("regenerate", generate | validate, control={
    "rewind": {"validate": ["generate"]},
    "retry_all": ["validate"],
    "max_handoffs": 3,
})
with Runner(store=":memory:", max_handoffs=10) as runner:
    report = runner.run(template.map([{"prompt": "Return JSON"}]))
    assert report.stats["pipelines"]["by_state"] == {"succeeded": 1}
```

`Handoff.rewind(target, value, reason="")` 要求显式给出值；`None` 也是真实的值。
目标必须是已声明的、严格更早的任务，由唯一名称或数字 seq 标识。
自我回退以及以 `end` 为目标回退都会被拒绝。目标之前的结果仍然有效；
从目标开始（含目标）的结果变为历史结果。整个后缀会重新执行，并带有自己的访问计数。

现有的 `Handoff.to()` 严格正向，并使用 `control.edges`；它绝不会获得隐式的
反向语义。对于仅含反向的流水线，正向声明是可选的。任何指令
都无法从 `fanout` 分支中逃出。

## 重试整条流水线

`return Handoff.retry_all(reason="new preparation")` 要求 `control.retry_all` 中有该来源。
它会从 **原始绑定种子** 在 seq 0 处重新开始，该种子由绑定时捕获的字节重新解码而来。
对任务输入或 `spec.seed` 的嵌套修改不会改变该绑定种子。它不接受替换
值：请从较晚的任务使用 `Handoff.rewind(0, chosen_state)`，以不同的状态重新开始。

全部重试不会创建新的映射行/重复，不会重置资源，也不会启动另一次 CLI 运行。
它会清除有效的任务结果，同时保留访问、工件、尝试以及已消耗的
控制预算。它可以声明在第一个任务上，包括单任务流水线。

异常重试是另一回事：`Retrying` 在同一次访问内重试一次尝试；回退和全部重试
是作为返回值返回的控制指令。它们不会调用失败重试策略。编写错误和
控制预算耗尽属于致命失败，无法由该策略重试。

## 可选的携带历史的应用状态

`HistoryArtifact` 是已解码的应用载荷，而不是持久化的 `Artifact` 记录。字典
仍然是普通字典；任务进入或完成时都不会有自动快照。

```python
from pyattacker import CodecRegistry, HistoryArtifact

class GenerationState(HistoryArtifact):
    pass

registry = CodecRegistry()
registry.register_type(GenerationState)
state = GenerationState({"prompt": "Return JSON", "temperature": 0.2})
state = state.checkpoint("before-generation")
state = state.with_state({**state.state, "answer": "invalid"}).checkpoint("after-generation")
restored = state.restore("before-generation")
next_state = restored.with_state({**restored.state, "temperature": 0.7})
assert len(next_state.history) == 2  # restoration retains later history
assert registry.load(registry.dump(next_state)).state == next_state.state
```

把该注册表同时传给 `pipeline(..., registry=registry)` 和 `Runner(..., registry=registry)`。
返回 `Handoff.rewind("generate", next_state)` 来调度选中的状态。

- `checkpoint(label, *, metadata=None)` 返回一个新值，其中包含应用状态的分离快照。
  标签必须唯一；`snapshot:` 保留给诸如 `snapshot:0` 这样的稳定 ID。
- `with_state(value)` 替换当前状态，不追加快照。
- `snapshot(id_or_label)` 返回分离的快照记录；`history` 返回分离的记录。
- `restore(id_or_label)` 替换当前状态，保留完整历史，并暴露 `selected`。
- `prune(*selectors)` 显式移除快照，并且当某个选择器指向被选中的
  快照时会 **抛出** `ValueError`。ID 不会被重用。

嵌套的可变值通过这些接口绝不会与保留的快照产生别名。应用状态和
元数据必须可 JSON 序列化；编码会拒绝客户端、租约以及其他运行时对象。
带版本的 `history-v1` 编解码器保留快照和已注册的子类类型。子类继承
基类构造函数；应用字段请使用 `state`。自定义子类构造函数/额外属性
序列化不在这个初始接口的范围内。未注册的子类在解码时会显式失败。

历史是自包含的，会随快照的数量和大小增长；在适当的时候请显式 prune。
在任务内部调用 `checkpoint()` **不会**立即持久化它。运行器会随
任务的输出或控制转移提交一起持久化历史。在该提交之前崩溃可能丢失
内存中的快照。快照历史不会取代框架的执行账本或访问记录。

## 访问、恢复与有限预算

`ctx.visit` 对每个站点都从 0 开始，并在每次全新进入时递增，包括回退之后的普通后继
进入。ID 在访问 0 时保持 `pipeline_id:seq`，此后使用 `pipeline_id:seq#visit`。
工件携带产生它的访问，并以精确 ID 保留不可变发生实例。要获取精确的历史发生实例，
请使用 `store.get_artifact_by_id(id)`；
`store.get_artifact(pipeline_id, seq)` 选择该站点的有效输出。
`ctx.attempt` 在一次访问内为尝试编号。RNG/`ctx.seed` 的推导对访问 0
逐字节一致，并在重访时包含访问。当你希望每次重新生成/尝试都产生新的外部操作时，
应在外部幂等键中包含访问和尝试。

待处理条目引用其精确输入。恢复会保留其访问，并继续已消耗的尝试
编号。尝试编号在任务代码运行之前就已预留；硬杀死可能在已完成的
尝试行中留下空缺，但无法重用其编号。框架恢复对未提交的工作至少执行一次；
它无法保证外部副作用恰好执行一次。

两种内置存储都提供原子的 `reset_visits`、`commit_entry`、`commit_visit_attempt`、
`commit_visit_success`、`commit_control_transition` 和 `repair_visit_terminal`，以及 `visit_state`、
`get_artifact_by_id` 和 `feature_level()`。`supports_visits` 探测的正是这一集合 *外加* v1 账本
能力（`commit_handoff`、`reset_pipeline`、`handoffs`），因为控制转移通过它落地自己的账本行
和源任务行；缺少其中任何一项的存储，在打开启用反向的流水线时会被 `ConfigError` 拒绝，
绝不会降级为非持久循环。`feature_level()` 被特意纳入探测：无法声明其磁盘模型
已经走到哪一步的存储，同样无法遵守[兼容性规则](backward.md#存储兼容性)。write-behind 在委托这些操作之前
会先刷写，这一委托过程
是同步的。
控制转移会一并提交其源访问/尝试、条目发生实例、账本、控制计数和已分配的
目标条目，并且还会为 `rewind`/`retry_all` 使活动后缀失效（在这样的流水线中，正向
转移会消耗预算，但不会把游标往回移动）。
普通成功会一并提交其输出、已完成的访问、有效映射和下一个游标。SQLite
用 `BEGIN IMMEDIATE` 串行化能力事务；
MemoryStore 在能力写入失败时恢复其状态。失败的 blob 写入无法发布其引用；
已回滚的数据库事务可能留下未被引用的 blob。

声明反向操作时，要求 `control.max_handoffs` 为正整数（`3.0`、`True`、`"3"`
和 `0` 都会被拒绝）。运行时上限
`RunConfig.max_handoffs`（默认 1000，在配置中也接受 `run.max_handoffs`）可以调低该上限。
两者中较小者生效。这类流水线中的每次非终止交接都计数，包括正向转移；
`END` 可以在达到上限时结束，而不再消耗一次转移。预算 N 恰好允许 N 次转移。
下一次则会在发布转移或使结果失效之前失败。恢复、全部重试以及自动的
缺失载荷种子兜底都会保留该计数。只有显式全新开始（`fresh_restart=True`，或
对已经成功的流水线使用 `retry_succeeded=True`）才会开启新的预算生命周期；访问计数
和审计记录都会保留下来。这就是耗尽预算后的出路：如果一条流水线失败 *正是因为* 它
耗尽了预算，那就没有可恢复的进展，因此没有它，之后每次运行都会重放同一个致命
错误。单纯的 `resume=True` 会保留已消耗的预算并继续当前遍历，而
单独使用 `retry_succeeded=True` 绝不会重启未完成的遍历——它只接纳已经
成功的流水线（见下文 *恢复与所有权*）。

### 恢复与所有权

打开时的行为取决于存储的行，这些规则旨在明确，而不是靠自然涌现：

| 存储的行 | 本次运行的行为 |
| --- | --- |
| `failed`、`interrupted` | 普通检查点恢复：精确的持久访问——访问编号、待处理条目、已消耗的尝试编号——继续执行 |
| `running`、`resume=True` | 操作者声明之前的所有者已消失。`interrupt_stale` 会先强制回收心跳过期的行；随后精确的持久访问继续执行 |
| `running`、无 `resume` | **跳过**，绝不接管。持久待处理的访问可以在崩溃后继续，因此第二个写入者会让同一次遍历分叉。该行保持原样，`pipeline.skipped` 会带上 `reason="owned_by_another_run"` 以及所有者的 run id |
| 行持久但遍历已消失 | 拒绝：`corrupt visit checkpoint: missing traversal state`。`fresh_restart=True` 是丢弃它并重新开始的文档化方式 |
| `succeeded` | 跳过，除非 `retry_succeeded=True`；随后重启会从绑定种子开始，并使用全新预算 |

`fresh_restart=True` 是唯一会丢弃持久进展的开关：它清除有效遍历和
任何待处理条目（把它留下的每个在途任务行都写入最终状态 `interrupted`），从不可变的
绑定种子重新开始，重置控制预算并使之前的账本失效——同时保留访问计数
和带访问限定的审计行（任务、尝试、工件、访问），因此历史发生实例仍然
可寻址，而遍历已丢失的存储会从这些行重建其计数。它也适用于
正向流水线，在那里它的含义是“忽略检查点，重新运行整条链”：此时仅追加的
历史同样保留，但任务地址和链工件地址按设计会被重用，而不是作为
独立发生实例保留（[参考](reference.md#表与读取器) 对此差异有详细说明）。把它与
`retry_succeeded=True` 结合，可重启已经成功的流水线。全新开始会发出
`pipeline.restarted`（带有被丢弃的游标），而不是 `pipeline.checkpoint_missing`：没有任何东西丢失。

待处理载荷缺失/不可用时会发出 `pipeline.checkpoint_missing`，并建立种子重放，
同时保留预算和计数。流水线的首次执行绝不会走这条路径：它的输入就是
它已经持有的绑定种子，因此 `journal="summary"` 存储（其写入的种子载荷被有意
丢弃）不会报告它从未有过的检查点失败。摘要日志和 null 后端可以在进程内运行循环，
但无法恢复其缺失的载荷。到 seq 0 的待处理回退使用其选中的载荷，
而不会触发种子重置。反向转移通过定时器泵重新入队，以释放 worker
供其他流水线使用。

## 检查执行情况

流水线导出只为启用反向的流水线添加 `control` 遍历记录。它包含
`cursor`、`pending`、有效的 `active` 槽位、持久 `counters`、已消耗的 `handoffs`、`version`、
精确的当前 `input` 和 `terminal` 引用。嵌套的任务/工件行包含 ID、访问和
`active` 标记。单个任务/尝试/工件导出包含访问；尝试还包含 task-run ID。
现有的封闭顶层导出行种类保持不变。HTTP `/pipelines` 视图为启用反向的行包含
遍历状态和 `cursor_kind="position"`（仅正向的行两者都没有）。
报告的作用域限于它所覆盖的那次运行，统计该次运行中重复的访问和尝试，因此在
恢复之后，它显示新运行的工作量，而存储和导出保留此前的每一行。这些
总计是工作量，而不是完成百分比。

### 存储兼容性

SQLite 升级较旧的存储时会加上默认值为 0 的访问列、一张遍历状态表和
`store_meta` 特性级别。对于仅正向的用法，这是增量升级：存储会停留在 `base` 级别，
直到写入第一次重访。

在为某个站点分配第一个重复发生实例的 *同一个事务* 内，级别会变为 `visits-v1`，
而且它绝不会再降低——审计行不会被删除，因此它们存在这一事实也不会消失。
那也正是对谱系无感知的写入者开始无法解释该存储的时刻，因此 SQLite 存储会
启用一个 **写入者防护**：来自尚未声明访问谱系感知的连接，对 `pipelines`、`tasks` 和 `artifacts` 的
`INSERT`/`UPDATE`/`DELETE` 会大声失败（`no such function:
pyattacker_store_requires_visits_aware_writer`）。原始读取和旧式读取不会被阻止——防护保护的是
状态而非访问——但不理解重访感知谱系的构建对其做出的解释是
不受支持的：这样的读取者无法判定哪个发生实例有效，因此它的输出描述的是行，
而不是执行。防护所保证的是破坏性的一半：在此特性之前发布的写入者
无法悄悄改动错误的发生实例，因为它会在第一次写入时就失败。打开未知级别的
构建会直接拒绝该存储（`StoreFeatureUnsupported`，包括只读），而不是
报告它看不见的谱系。

实际后果：

* **备份。** 对于活动数据库，请使用 SQLite 自带的备份 API（`sqlite3 <store> ".backup <copy>"`，或
  `Connection.backup()`），它在写入者运行时是安全的；把活动数据库文件连同其 `-wal`/`-shm` 附属文件
  一起复制，只有在没有活动写入者时才可靠。SQL 转储也能正常恢复——
  `sqlite3 <store> .dump | sqlite3 <copy>` 会先写入表数据，再创建防护触发器，因此
  无感知的连接也能重放它，副本也会随之继承防护和特性级别。
* 从 `sqlite3` 写入受防护的存储，需要在该连接上注册防护函数（或删除
  触发器）——两者都超出受支持的接口范围；
* 回到 `base` 的唯一受支持方式，是由理解该级别的构建执行迁移。

同时运行多个运行器执行同一条逻辑流水线不是受支持的调度模式；独立的分片
行仍然相互独立（`running` 行在没有 `resume` 的情况下仍然绝不会被接管，见上文）。
设计与评审清单见 [#51](https://github.com/Hazer-BJTU/pyattacker/issues/51)。
