# 实验集合

[English](../suites.md) | **简体中文**

Suite 把多个独立实验交给同一个 Runner 执行。所有成员共享全局 worker 并发上限；显式绑定的资源池还会共享**同一个 Pool 实例**，包括容量、健康状态、冷却时间和配额。各子实验仍拥有独立的流水线身份、检查点、结果和应用指标。子实验之间没有依赖关系。

## 组合已有配置

可运行示例把同一份独立实验引用了两次，特意说明：不同成员中的相同样本会独立执行。

```bash
uv run pyattacker validate -c examples/suites/suite.json
uv run pyattacker run -c examples/suites/suite.json
uv run pyattacker resume -c examples/suites/suite.json --experiment baseline
uv run pyattacker report runs/suite-demo
uv run pyattacker watch runs/suite-demo --experiment candidate
uv run pyattacker serve runs/suite-demo
uv run pyattacker export runs/suite-demo runs/all.jsonl --rows results
uv run pyattacker export runs/suite-demo runs/exported --rows results --by-experiment
```

完整配置见 [examples/suites/suite.json](../../examples/suites/suite.json)，成员配置见 [experiment.json](../../examples/suites/experiment.json)。成员文件仍然能通过普通 `run -c` 单独运行。JSON 和 TOML 不需要额外依赖，YAML 需要 `yaml` extra。

```json
{
  "suite": {"id": "comparison"},
  "run": {"concurrency": 16},
  "output": {"root": "runs/comparison", "layout": "by_experiment"},
  "pools": {
    "shared": {"resources": [{"id": "api-1", "capacity": 4}]}
  },
  "experiments": {
    "baseline": {"config": "baseline.json", "pool_bindings": {"apis": "shared"}},
    "candidate": {"config": "candidate.json", "pool_bindings": {"apis": "shared"}}
  }
}
```

`experiments.ID.config` 相对于 **Suite 配置文件**解析。被引用配置内部的路径保持单独运行时的语义：`source.path` 仍然相对于进程的当前工作目录。任意任务参数都不会被自动改写，程序也不会改变工作目录。`output.root` 相对于 Suite 文件，CLI 的 `--output-root` 相对于当前工作目录。区分这两种路径，可以让原有成员配置保持不变。

Suite 的 `run` 块统一决定执行参数。成员的 `run` 块会通过校验，但不用于执行；`validate` 会列出这些成员的 `ignored_run_fields`、顶层运行参数和实际资源映射。CLI 执行参数覆盖 Suite 配置。Suite 模式拒绝 `run.store` 和 `--store`，只使用 `output.root` 作为存储入口。

`pool_bindings` 把成员内部的资源名映射到顶层共享池。映射既作用于任务声明，也作用于 `ctx.acquire("apis")` 等显式调用以及资源发布、订阅。绑定后的本地池定义由顶层定义替代。未绑定的池使用内部名称 `experiment:ID:NAME`，即使其他成员也叫 `apis`，仍然彼此独立。框架不会因为名字相同而自动合并池。

成员条目还支持 `label`（仅用于显示）和 `limit`（该成员展开后的流水线数量上限，包含 repeats）。CLI `--limit` 限制总提交数量。达到 limit 不代表数据源已耗尽。重复指定 `--experiment ID` 可以选择多个成员；未选中的成员保留原有状态。本期不支持嵌套 Suite 和参数矩阵展开。

## 身份、提交与恢复

`suite_id` 和 `experiment_id` 是稳定身份，`run_id` 标识一次启动。ID 长度为 1–80，只允许 ASCII 字母、数字、连字符和下划线，以字母或数字开头，并排除系统保留设备名。成员 ID 不允许仅大小写不同。

Suite 流水线 ID 包含 `(suite_id, experiment_id)` 的摘要以及原始本地 key 的摘要。仍保持 `key == pipeline_id`，额外用 `local_key` 保存原始 key，导出也保留 `repeat`。引用配置设置 `source.key_field` 时，本地 key 是样本字段值及原有的 repeat 后缀，不包含模板名称。修改显示名称或模板名、重新排列成员、添加其他成员，都不会改变既有 ID。Suite 之外的旧流水线继续使用原来的 ID 规则。

每个成员持久化自己的定义摘要，覆盖任务指纹、数据源配置、实际资源池声明及绑定、成员 limit。资源池声明在环境变量展开前计算摘要，因此轮换 `${API_KEY}` 不会改变身份；修改配置里的池参数或绑定仍会改变摘要。如果环境变量的变化改变了实验语义而非凭据，应使用新的成员 ID 或输出目录。在同一个 ID 下修改这些定义会被拒绝，应该换成员 ID 或输出目录。摘要并不是数据源文件全部字节的快照；输入必须保持稳定并且能够重新读取。原有的 spec/seed 身份校验继续保护显式 key，防止复用不匹配的工作。SDK 用户需要自行提供定义摘要，涵盖任务指纹之外的配置和输入语义。

输入工厂按轮转方式消费，提交队列保持有界。这改善了提交公平性，但不保证各成员获得相同的执行时间或资源份额。未知长度的数据源不会显示虚假的总量。持久化的 `source_exhausted`、数据源错误、limit 和每次运行的成员关系，把“已提交的任务都完成”与“整个实验完成”区分开来。展示的成员状态由数据源事实和流水线检查点重建；存储的状态汇总本身不是恢复检查点。

某个成员的数据源抛异常时，该成员标记失败，其他成员继续。普通任务失败也不会终止其他独立成员。存储或 worker 故障会停止整个 Suite。CLI 退出码继续使用 `0` 表示成功、`1` 表示任务或数据源失败、`2` 表示配置错误、`130` 表示中断。主动设置 limit 的运行可以退出 `0`，同时成员仍为 `incomplete`；判断数据是否全部处理，应看 `source_exhausted`。

恢复会跳过成功项，让未完成流水线从任务检查点继续，包含 handoff 和反向 visit 状态。未提交的输入仍需要原始配置和可重新读取的数据源；已持久化的种子不能重建尚未读取的尾部。Suite 输出目录只允许一个活动写入者，但支持独立的只读监控。Suite 拒绝 `--shard/--shards`，因为不同进程无法共享同一份实时资源配额；原有单实验分片功能不变。

## 输出布局

默认布局为 `combined`，可选的 `by_experiment` 会拆分完整状态，而不只是结果文件：

```text
combined/                       by_experiment/
  manifest.json                   manifest.json
  state.db                        suite.db
  artifacts/                      experiments/
                                    baseline/
                                      state.db
                                      artifacts/
                                    candidate/
                                      state.db
                                      artifacts/
```

只有启用随布局存储的文件后端时，才会创建 `artifacts/`，配置为 `run.artifact_backend: "file"` 或 CLI `--artifact-backend file`。默认仍把产物载荷保存在 SQLite。Suite 模式下，恰好为 `file` 的简写表示使用对应目录里的 `artifacts/`；显式路径、文件 URI 和后端映射仍保持普通外部后端语义。布局内的文件引用是相对路径，可以随整个输出目录移动。重新打开目录时沿用 manifest 里的后端；显式指定的后端配置必须与其完全一致，否则在打开数据库前报错，不支持原地切换后端。外部后端遵循自己的路径约定，不会被自动移动或复制。

`manifest.json` 包含布局版本、稳定的 Suite 身份、目录库位置，以及可选的后端配置。它不会复制环境变量展开后的成员配置或任务密钥。目录库保存成员定义摘要和每次运行的成员关系。单个任务的状态、产物、reset 和 visit 事务始终位于同一个数据库中，不存在跨库原子检查点，也不会在顶层保存第二份权威检查点。默认最多缓存八个子库连接，回收连接前会刷新历史记录。选择性运行只写入选中的成员库，重新打开被回收的连接也遵循这条规则；查询未选中成员不会新增运行记录或迁移其数据库。恢复时缺失子库会报错，不会悄悄创建空库。

结果只在显式导出时生成。`--by-experiment` 把输出参数视为目录，分别写入 `OUTPUT/ID/ROWS.FORMAT`，文件名对应行类型，例如 `results.jsonl` 或 `attempts.jsonl`，两种存储布局都适用。每个单库导出在写入成功后原子替换目标文件。拆分导出的原子性以文件为单位，不覆盖整个目录。

两种布局都依赖 `SuiteStore` 提供的实验状态和成员关系能力。Suite 执行暂不支持任意自定义 Store 插件，普通 Runner 仍然支持这些插件。已有输出根目录不能原地改变 Suite 身份或布局，本期没有在线布局转换功能。

## 结果、监控与指标

`report`、`watch`、`serve`、`export` 打开 Suite 目录时，默认使用**累计视图**，包含恢复时跳过的既有成功结果。`--experiment ID` 选择一个成员。`--run-id` 展示那次启动的持久化执行结果和数据源事实：run A 失败后，即使 run B 恢复成功，A 仍显示失败。恢复时跳过的成功检查点计为 `skipped`，没有执行开始时间，尝试数为零。执行耗时从本次启动内的执行开始计算，不包含两次启动之间的间隔。任务结果也保留对应的 run 身份，attempts/events 按实际产生它们的启动筛选。

每次检查点更新（包括 handoff/visit 转移）和 run 执行结果在同一 SQLite 事务内提交。进程崩溃仍可能丢失缓冲中的 attempt/event 明细，但已提交的执行结果和尝试计数会保留。恢复时，独占写锁也允许将目录库中仍标记为 `running` 的旧 run 结算为 `interrupted`，不改写未选中的成员库。结束时间与心跳时间记录恢复检测到中断的时刻（真实崩溃时刻未知），历史耗时因此不再持续增长。

载荷仍属于可变检查点：后续 run 接管检查点后，历史导出省略对应的 artifacts/results，不会返回后续运行的新载荷。需要最新成功结果时使用累计视图。

面板列出成员并提供详情链接。JSON 接口接受 `?experiment=ID`，可以与 `run_id` 组合；`/experiments` 返回成员、数据源状态和指标。各数据库的 event/attempt ID 仍是局部编号；消费多库审计日志时应结合 Suite、成员和流水线归属。共享资源事件不会被虚构成某个子实验独有。资源快照展示共享池的状态，不把池的活动强行归因到某个成员。

`--rows results` 每条流水线输出一行简洁结果，包括 key/local key、repeat、Suite/成员身份、状态、最终结果、产物可用性和错误字段。失败或中断的流水线不会被丢弃。原有 `pipelines`、`tasks`、`attempts`、`events`、`artifacts` 审计导出也会在适用时保留成员归属。导出采用有界迭代；拆分布局按成员 ID 顺序遍历，每个数据库内部使用原有的行顺序。运行中的导出继续遵循已有的尽力读取语义，不构成一致性快照。

`Runner.report_metric(..., experiment_id="baseline")` 写入成员级最新指标，`TaskContext.report_metric()` 保持流水线作用域。完成回调拿到的 `PipelineRecord` 包含 `suite_id`、`experiment_id`、`local_key` 和 `repeat`，应用可以据此维护各成员的累加器。恢复时应从持久化的最终结果重建累加器：此前成功而被跳过的流水线不会再次触发完成回调。指标按 run 划分，应用需要为新 run 重新汇报重建后的值。框架不会自动平均各成员准确率，也不会自行计算整体业务指标。

## Python API 与任务文件输出

```python
from pyattacker import ExperimentSpec, SuiteSpec, pipeline, task

@task("double")
def double(value, ctx):
    # ctx.suite_id, ctx.experiment_id, ctx.local_key and ctx.output_dir
    # identify the owning experiment without changing the input value.
    return value * 2

template = pipeline("numbers", double)
suite = SuiteSpec(
    id="numbers",
    experiments=[
        ExperimentSpec("first", lambda: template.map(range(3)), definition_digest="numbers-v1"),
        ExperimentSpec("second", lambda: template.map(range(3)), definition_digest="numbers-v1"),
    ],
    output_root="runs/numbers",
    layout="by_experiment",
)
with suite.runner(concurrency=4, handle_signals=False) as runner:
    report = runner.run(suite.pipelines(runner.store), resume=True)
    print(report.summary())
```

`ExperimentSpec.factory` 必须在每次调用时创建一个新的普通 PipelineSpec 迭代流。`pool_aliases` 把本地资源名映射到 `SuiteSpec.pools` 里的实际池名。`SuiteSpec.runner()` 拥有自己的 SuiteStore，应使用 Runner 上下文管理器关闭它。`SuiteSpec.pipelines(store, experiments=[...], limit=N)` 选择成员并输出带命名空间的流水线。`SuiteStore(root, read_only=True, experiment=ID)` 提供与 CLI 目录读取相同的接口，用完后需要关闭。

`ctx.output_dir` 在集中模式下指向 Suite 根目录，在拆分模式下指向成员目录。业务文件可以显式使用它。原有 `write_jsonl(path=...)` 和任意 Python 文件写入不会被拦截或重定向。应用仍然负责文件命名、并发追加，以及重试时的幂等性。框架检查点和最终结果导出不依赖这些业务文件。

## 查询 limit 与开销

`SuiteStore.tasks(limit=N)` 先按 pipeline、run 和成员筛选，再按
`(pipeline_id, seq, visit, task_run_id)` 在所选数据库间排序取前 N 条。
与 `MemoryStore.tasks`、`SqliteStore.tasks` 一致：`None` 返回全部，`0` 返回空列表；
负数或非整数 limit 抛出 `ValueError`。

`SuiteStore.events(limit=N)` 按 `(ts, event_id)` 取最新的匹配事件，结果升序返回。
全局或成员级 `handoffs(limit=N)` 按 `(ts, handoff_id)` 取最新记录，筛选在 limit 前执行。
ID 只在各自数据库内唯一；排序键完全相同时，保留稳定的成员遍历顺序（events 优先较早
成员，handoffs 优先较晚成员）。指定单个 `pipeline_id` 的 handoff 查询保留插入 ID 顺序，
以便恢复逻辑在时钟回退时仍能找到最后提交的转换。这些 Suite 列表接口同样支持
`None` 返回全部、`0` 返回空列表。

有限查询从每个数据库最多取 N 条原始候选记录，只解码最终选中的记录。合并候选所需内存
为 O(N)，依次访问各库，因此也支持 `max_open=1`。Suite 写入者会为全局、run、成员、
成员/run 时间查询以及单 pipeline 事件查询建立索引。这些索引占用磁盘并增加写入开销；
旧库第一次由新版本写入者打开时需要建索引。只读查询不修改数据库：无新索引的旧库仍可读，
但可能需要扫描和排序。稀疏 `kind` 等附加筛选条件仍可能扫描大量索引项。

事件计数及统计改用 SQL 计数和分组，无需解码事件、attempt、task 或 pipeline 的 payload。
历史 run 仍使用 admission 快照和本次 invocation 的 attempt 计数，包括 resume 后跳过的
pipeline。这不意味着全部统计为常数开销：分组聚合可能扫描匹配行；精确耗时分位数仍需读取
并排序全部有效 pipeline 耗时，数值内存 O(P)、排序 O(P log P)。完整导出仍使用分页历史读取。

在仓库运行 `PYTHONPATH=src python benchmarks/suite_queries.py`，可复现 1 万、10 万、
100 万事件规模与旧 Python top-N/计数路径的对比。脚本报告 combined 布局热连接查询的耗时
和 SQLite VM 指令数，合成数据插入耗时包含新索引维护成本。
