# CLI 参考

[English](../cli.md) | **简体中文**

```
pyattacker {run,resume,report,watch,export,serve,plugins,validate,demo}
```

这里的一切同样可以通过 `python -m pyattacker ...` 访问。命令分为三组：
**执行工作**（`run`、`resume`、`demo`）、**读取结果**（`report`、`watch`、`export`、`serve`），以及
**检查环境**（`validate`、`plugins`）。

## 退出码

| 代码 | 含义 |
|---|---|
| `0` | 所有流水线都成功（或因已完成而被跳过） |
| `1` | 本次运行已结束但部分流水线失败，或本次运行无法修复其中一条（`repair_failures`） |
| `2` | 配置错误 —— 什么都没运行；以具名框架错误（`WorkerCrashed`、`StoreUnavailable`）结束的运行也计入此类 |
| `130` | 被中断（SIGINT）；挂起和执行中的流水线都被记录为可恢复 |

`2` 意味着“修好配置”（或“读错误信息”）；`1` 意味着“读报告”。`130` 的运行总是可以安全地
`resume`。

`1` 涵盖两种不同的情况，报告会说明是哪一种：一条流水线在本次运行中进入了失败的
终态（计入 `pipelines.by_state`），以及一条本次运行无法将其从撕裂终态中写入最终状态的
流水线 —— 即报告（以及 `--summary-format json`）中的 `repair_failures`，因为它的
行有意保留最初的失败以及产生该失败的那次运行。

`run` 返回 `2` 并不总是配置错误：在自身处理器之外死掉的 worker 会抛出
`WorkerCrashed`（打印为 `WorkerCrashed: worker for pipeline … died with …`），此时它持有的
流水线已被记录为失败，运行记录也已关闭。运行自身的记录在错误之后仍然存在，因此
`pyattacker report <store>` 仍能显示发生了什么；修好根因后用 `--resume` 重新运行。

---

## `run` —— 执行声明式配置

```bash
pyattacker run -c config.yaml [options]
```

| 参数 | 作用 |
|---|---|
| `-c, --config PATH` | 配置文件：yaml、toml 或 json。后缀决定使用哪个解析器；yaml 需要可选额外依赖（extra） |
| `--limit N` | 只运行前 N 条流水线（在真实数据集上做冒烟测试） |
| `--store PATH` | 覆盖 `run.store`。与 `--shard` 一起使用时原样使用；否则会追加分片后缀 |
| `--concurrency N` | 覆盖 `run.concurrency` —— 指执行中的尝试数，而不是存活的流水线数 |
| `--journal {full,summary}` | `full`（默认）存储工件载荷，正是它让恢复能在任务粒度上工作；`summary` 只保留摘要 |
| `--label TEXT` | 记录在本次运行上的标签，便于日后区分不同的运行 |
| `--resume` | 跳过已完成的流水线，让失败的流水线从检查点重新开始 |
| `--retry-succeeded` | 与 `--resume` 一起使用时，连已成功的流水线也重跑。它只扩大*哪些*流水线符合条件，绝不会丢弃未完成流水线的检查点 |
| `--fresh-restart` | 让已准入的流水线从种子重新开始：丢弃检查点/遍历状态，重置控制预算。只追加的历史（尝试/事件/交接）保留；启用反向的流水线还额外保留其访问发生实例与计数器 |
| `--strict-leases` | 泄漏的租约会让其任务失败（`LeaseLeakError`），而不是被悄悄强制回收。值得在 CI 中开启 |
| `--stop-after-failures N` | N 条流水线失败后停止准入新工作（尽力而为：已准入的流水线仍会完成） |
| `--no-signals` | 不安装 SIGINT/SIGTERM 处理器 |
| `--artifact-backend SPEC` | 载荷字节存放的位置：`inline`（默认）、`null`、`file:///path` 或 JSON 规格 |
| `--progress` | 通过对同一存储的第二个连接打印实时进度 |
| `--shard I/N` | 只运行 `N` 个分片中的第 `I` 个。每个分片写自己的存储文件 |
| `--shards N` | 在本地派生 N 个子进程，每个分片一个，然后打印合并后的报告 |
| `--jobs N` | 同时运行多少个分片子进程（配合 `--shards`） |
| `--summary-format {text,json}` | `json` 为每次运行打印一个摘要对象 |
| `--no-write-behind` | 每次尝试和事件都立即提交，而不是批处理 |
| `--strict-env` | 配置中未设置的 `${VAR}` 直接以 `2` 退出，而不是发出警告 |

`--shard` 和 `--shards` 是两种替代方案：前者是一个进程完成自己那份工作（其余由你编排），
后者是便捷路径，由 pyattacker 在本地编排。分片分配是内容寻址的流水线键的
纯函数，因此同一数据集总是以相同方式切分，`--resume`
会让每条流水线回到拥有它的那个分片。每个子进程的环境变量里还会带上
`PYATACKER_SHARD=I/N`。

## `resume` —— `run --resume`

参数与 `run` 完全相同。把最初使用的那条命令原样重跑，只把 `run` 换成 `resume`：

```bash
pyattacker run    -c qa.yaml --shards 4 --store runs/qa.db
pyattacker resume -c qa.yaml --shards 4 --store runs/qa.db
```

实际会重跑的内容：成功的流水线什么都不重跑；失败或被中断的流水线，则从第一个没有产出工件的
任务开始，及其之后的全部任务。工件已在磁盘上的任务绝不会被重新执行，因此
已经花过钱的请求不会被重发。有两种情况会破坏这一点，二者都会留下一个
`pipeline.checkpoint_missing` 事件：`journal: summary` 与 `null` 工件后端，两者都不保留
检查点所需的载荷。

`--resume` 也是用来认领那些行状态仍写着 `running` 的流水线的 —— 这是硬杀进程留下的形态。
启用反向的流水线在没有它时绝不会被接管：本次运行会跳过那一行（`pipeline.skipped`，
`reason="owned_by_another_run"`），而不是分叉出另一场运行可能仍拥有的遍历。应当*丢弃*检查点
而不是恢复它的重启方式是 `--fresh-restart`（`fresh_restart=True`），它还会
重置已用尽的交接预算；见[反向遍历](reference.md#恢复与所有权)。

## `demo` —— 零配置验证安装

```bash
pyattacker demo [--store PATH] [--pipelines N] [--concurrency N] [--fail-rate F] [--export PATH]
```

模拟任务，无网络、无配置文件。`--fail-rate`（例如 `0.3`）会制造失败，这样你就能观察
重试并读取产生的重试决策记录。适合用作 CI 冒烟测试。

---

## `bench` —— 在模拟中比较获取算法

```bash
pyattacker bench [--scenario NAME] [--list] [--algorithms A,B] [--seeds N]
                 [--jobs N] [--concurrency N] [--horizon S] [--wall-budget S]
                 [--clock {virtual,real}] [--speedup F] [--json PATH] [--markdown PATH] [--quiet]
```

无网络，也没有提供方：场景是一个写定的世界（容量周期、令牌桶、延迟
尾部、故障风暴、三个性格不同的端点），客户端是一组 worker 构成的闭环，
驱动真实的 `Pool` 和真实的算法，而时间是模拟的。`docs/benchmark.md` 解释了
各种假设、各项指标以及如何读表；而这里的表格是参数总览。

| 参数 | 作用 |
|---|---|
| `--scenario NAME` | 在哪个模拟世界中运行；默认 `bursty_provider` |
| `--list` | 打印全部场景、全部算法以及每个指标及其单位和方向，然后以 0 退出 |
| `--algorithms A,B` | 逗号分隔的子集；默认包含场景能演练的每个算法 —— 若请求一个它声明不适用的算法，仍会运行它，只是该列显示 N/A（例如 `failover`，单资源池场景无法展现它的最佳状态） |
| `--seeds N` | 对多少个种子取平均，形式为 `scenario.seed + 0 .. N-1`（默认 3；至少 1） |
| `--jobs N` | 覆盖场景的作业数（至少 1） |
| `--concurrency N` | 覆盖 worker 数量 —— 即客户端的执行中数量，因此它既是假设也是成本调节项（至少 1） |
| `--horizon S` | 覆盖模拟时间视界，它会停止接纳新作业，而不是截断某个作业（正数） |
| `--wall-budget S` | 任何单次运行可花费的真实秒数（默认 600，正数）；无法完成的运行会抛出错误，而不是返回部分指标 |
| `--clock {virtual,real}` | `virtual`（默认）是不花任何代价的模拟时间；`real` 以压缩后的真实时间重放场景来验证模拟器，速度慢得多 |
| `--speedup F` | `--clock real` 的压缩系数（默认 10，正数） |
| `--json PATH` | 把完整报告写成 JSON（`-` 表示 stdout） |
| `--markdown PATH` | 把报告写成 markdown 表格，包含每个端点的准入情况 |
| `--quiet` | stderr 上不输出进度 |

表格输出到 stdout，进度输出到 stderr，因此 `pyattacker bench --json - --quiet | jq .` 可以组合使用。
退出码遵循 CLI 其余部分的约定：`0` 表示扫描完成，`2` 表示场景或
算法未知。无法完成的基准测试同样是 `2` —— 绝不输出部分表格。预算超出其
范围也会以同样方式被拒绝（`--seeds 0` 会提示 "at least 1"），而不是被夹到范围内：一次悄悄
跑了与所宣称不同的实验的扫描，会在正确的标题下报出错误的数字，
因为标题本身并没有写错。

---

## `report` —— 统计与失败

```bash
pyattacker report STORE [STORE ...] [--run-id ID] [--errors N] [--json] [--artifact-backend SPEC]
```

多个存储会合并成一个一致的视图：按 `pipeline_id` 去重（最佳状态胜出，最新的
完成时间用于平局裁决），统计值由合并后的行重新计算，并且它会告诉你折叠了多少行。
`--errors N` 打印前 N 个失败及其错误类别。`--json` 以对象形式给出同样的数字。
记录了交接的运行会在摘要行中说明（`... attempts: total=12 handoffs=2`），而
`export` 上的 `--rows pipelines` 会带上账本本身（见
[进阶：交接](reference.md#进阶交接可选启用)）。

## `watch` —— 实时监控

```bash
pyattacker watch STORE [--run-id ID] [--interval S] [--iterations N] [--no-clear]
```

对同一个 SQLite 文件的只读连接，因此它能在正在运行的流水线旁边运行（WAL 允许一个写入者和多个
读取者）。它显示流水线状态分布、延迟百分位、每个资源池的 `active/capacity`、
`ready/degraded/dead`、有多少流水线在等待或挂起，以及最近的错误。带交接的运行还会
在 attempts 行上显示 `handoffs=N`（所选范围内的提交数，不带 run 过滤器时是全部历史；一次恢复可能复用更早的活动交接）—— 在启用控制的流水线上，游标是位置而非
进度计数，所以正是这个数字解释了任务列表为何很短。`--iterations N` 让它
自行退出，这正是脚本中想要的。

## `export` —— 导出记录

```bash
pyattacker export STORE [STORE ...] OUTPUT [--rows SHAPE] [--format FMT] [--run-id ID]
```

| `--rows` | 每行对应 |
|---|---|
| `pipelines`（默认） | 一条流水线，嵌套 —— 包含任务和最终工件 |
| `tasks` | 一个任务：终态、耗时、错误、使用的租约 |
| `attempts` | 一次尝试，包含每次重试的 `decision` |
| `events` | 一个结构化事件 |
| `artifacts` | 一个工件，包含中间工件 |

`--format` 可以是 `jsonl`（默认）、`json` 或 `csv`。CSV 从首批行取得表头，并把后续出现的键
折叠进 `extra` 列，因此内存占用保持平稳，也不会有字段被悄悄丢弃。

每种类型都会完整导出：过去，事件超过 100 000 条的存储会从 `--rows events` 中丢失比最新
100 000 条更早的全部内容。`events`/`attempts` 的行按最旧优先输出，`tasks`/`artifacts` 按
流水线/`seq` 顺序输出，从存储中按有界批读取 —— [export
参考](reference.md#export)给出了每种类型的准确顺序、`limit` 规则、内存说明，以及当存储
仍在被写入时导出能保证什么、不能保证什么。

## `serve` —— 只读 HTTP 视图

```bash
pyattacker serve STORE [--host HOST] [--port PORT] [--run-id ID]
```

`/` 是一个会自动刷新的小型仪表盘；`/stats`、`/events`、`/pipelines`、`/resources`、`/errors` 返回 JSON。
每个请求都新建一个只读连接，因此它在正在运行的流水线旁边是安全的。

**它没有身份验证，并会暴露你的工件载荷。** 正因如此它只绑定回环地址。若要把它绑定到
其他地址，请先在前面放上你自己的代理。

---

## `validate` —— 不运行就检查配置

```bash
pyattacker validate -c config.yaml [--strict-env]
```

解析配置，解析每个 `use:` 目标和资源池，检查整个文档并打印
生效后的配置。有任何问题就以 `2` 退出并给出原因。`--strict-env` 会把未设置的
`${VAR}` 从警告提升为错误 —— 在 CI 中值得这么做，因为静默为空的 API key 比
失败的作业更糟。

`run` 和 `resume` 在创建存储之前会经过同样的检查，因此 `validate` 拒绝的配置在它们那里
同样是 `2`，并且不会启动任何任务。检查的内容如下，每条消息都带字段路径：

| 方面 | 示例 |
|---|---|
| 未知字段 | `run.concurency`（附带 "did you mean `run.concurrency`?"）、`pools.apis.capcity` |
| 字段类型 | `run.concurrency: "8"`、`pipeline.tasks[0].use: 5` |
| 数值范围 | `run.concurrency: 0`、`run.heartbeat_s` 小于等于 0、`pools.apis.capacity: 0` |
| 资源池引用 | `pipeline.resource`、`pipeline.tasks[0].resource`，以及 `use:` 工厂自己声明的 `resource` |
| 算法 | `algorithm: nosuchalgorithm`、`algorithm: {name: backoff, bse: 1}` |
| 工件后端 | 没有 `root` 的 `artifact_backend: {kind: file}`、未知的 `kind`、无法解析的 JSON 字符串规格，或不是非负整数的 `min_bytes` |
| 各节 | `pipeline:`（以及其他每一节）必须是映射 —— 标量或列表属于配置错误，而不是回溯 |
| source | `source.kind` 必须是 `range`/`jsonl`；`jsonl` 需要 `source.path`；`source.repeats` 至少为 1 |
| 重试 | `"on"` 名称，以及重试块的数值/布尔字段 |

它只是*声明*检查：不会打开 `source.path`、遍历数据集、构建
`artifact_backend`（构建它会创建对应目录），也不会调用任务。但它确实会检查
`artifact_backend` 是 `resolve_backend` 能构造出来的 —— 必填字段（例如文件后端的
`root`）也包含在内 —— 因此 `validate` 与 `run` 接受和拒绝的规格完全一致。`${VAR}` 值
按下文所述展开。

## `plugins` —— 已安装了什么

```bash
pyattacker plugins [--json]
```

列出为 `pyattacker.tasks`、`pyattacker.algorithms`、`pyattacker.codecs` 和
`pyattacker.stores` 发现的所有入口点，**包括加载失败的那些以及失败原因**。损坏的插件会被记录，
绝不会被抛出，因此它永远不会让一次运行崩溃 —— 但它也绝不会静默失败。

---

## 配置文件参考

下面的示例是 YAML，但每个接受 `-c` 的命令都接受同样的文档以 `.json` 或
`.toml` 形式给出；加载器根据后缀选择解析器。只有 YAML 解析器是可选额外依赖
（`pip install "pyattacker[yaml]"`）—— 缺少它会被报告为配置错误并指明该额外依赖，
退出码为 2，且发生在任何东西运行之前。这个配置块是完整的（每个 `use:` 目标都是内置的），
测试套件会让它通过 `validate`。

```yaml
# example/cli_config_reference.yaml
run:
  store: runs/demo.db          # path, ":memory:", or a plugin scheme like s3://bucket/runs.db
  concurrency: 8
  journal: full                # full | summary
  label: demo
  artifact_backend: { kind: file, root: /data/blobs, min_bytes: 262144 }

pools:
  apis:
    kind: llm
    algorithm: backoff         # wait | backoff | least_busy | failover | sticky | quota_aware | immediate
    resources:
      - id: api-1
        capacity: 4            # concurrent leases this endpoint allows
        options: { model: gpt-4o, api_key: "${OPENAI_KEY:-sk-demo}" }

pipeline:
  name: qa_eval
  tasks:
    - { use: pyattacker.tasks:echo }
    - use: pyattacker.tasks:simulate_llm   # or your own task: my_pkg.tasks:ask_model
      resource: apis
      algorithm: backoff
      timeout_s: 30
      kwargs: { latency_ms: 5, fail_rate: 0.1, tokens: 64 }   # factory arguments
      # YAML 1.1 parses a bare `on` as boolean true, so the retry key must be quoted
      retry: { max_attempts: 3, base: 0.2, "on": [RetryableError, TimeoutError] }

source: { kind: jsonl, path: data.jsonl, limit: 100, key_field: id, repeats: 1 }
```

`run:` 块接受的字段与 CLI 映射到 `RunConfig` 的字段完全一致：`store`、`journal`、
`concurrency`、`label`、`heartbeat_s`、`grace_s`、`stale_after_s`、`strict_leases`、
`stop_after_failures`、`stop_after_s`、`max_handoffs`、`retry_succeeded`、`fresh_restart`、`seed`、`notes`、
`write_behind`、
`write_batch`、`flush_interval`、`artifact_backend` 和 `meta`。其他任何字段都是配置错误，而不是
被悄悄忽略的一行。优先级是明确的：命令行上的参数胜过 `run:` 块，
而 `run:` 块胜过内置默认值。

`artifact_backend` 取与 `--artifact-backend` 相同的值：`"inline"`（默认，载荷留在
存储中）、`"null"`（保留摘要，丢弃字节）、`"file:///data/blobs"`，或上面的映射
形式。在 YAML 中要为它加引号：裸写的 `artifact_backend: null` 是 null *值*，其含义是 `inline`（与
省略该字段相同），而 `"null"` 是丢弃载荷字节的后端。恢复的运行需要
构成检查点的那些载荷，因此 `journal: summary` 和 null 后端都会让你失去任务级
恢复。

任务条目还接受 `config: {model: model-a}` 和 `version: "prompt-v2"`，用于表达无法
从源码推断的行为。它们会加入恢复指纹；工厂参数仍然
独立。使用 `source.key_field` 时，在已有键下改变任务同一性或种子内容
会引发配置错误（退出码 2）并保留旧结果。对改动过的工作请使用新的键/存储。
升级已有存储前请先看[恢复同一性](reference.md#恢复同一性)。

任务条目是**对解析后的 `use:` 目标的覆盖**，两种情况截然不同：条目没有提到的字段
保留目标声明的值（它自己的 `resource`、`algorithm`、
`timeout_s`、`retry`……），而显式的 `field: null` 会清空支持为空的字段
（`resource`、`algorithm`、`timeout_s`、`version`）。这与 SDK 中
[`TaskSpec.with_overrides`](reference.md#taskspec) 的规则相同。

`${VAR}` 从环境变量展开（见 `--strict-env`）。`source.kind` 为 `jsonl` 或 `range`；
`repeats: k` 即 pass@k —— 每个种子 k 条独立流水线。CLI 参数覆盖 `run:` 块。

### `pipeline.control` —— 进阶、可选的交接

配置还可以声明哪个任务允许通过返回
[`Handoff`](reference.md#进阶交接可选启用) 来**交接**（向前跳过），从而同一条流水线可以用声明式表达：

```yaml
pipeline:
  name: qa_eval
  control:
    edges:
      judge: [report, end]   # judge may continue at report, or finish the pipeline
      ask: [report]
  tasks:
    - { use: pyattacker.tasks:echo, name: fetch }
    - { use: my_pkg.tasks:ask_model, name: ask, resource: apis }
    - { use: my_pkg.tasks:judge, name: judge }
    - { use: my_pkg.tasks:report, name: report }
```

目标可以是任务名、任务的数字 seq，或 `end`，并且必须严格晚于它的来源
（`edges` 操作仅向前）。在链中出现两次的名称必须以 seq 给出。从*最后一个*
任务声明 `end` 会被拒绝，因为它不会产生任何效果。每个问题都以字段
路径报告 —— `pipeline.control.edges['judge'][0]: destination 'fetch' (seq 0) is not later than the source
'judge' (seq 2); v1 handoffs are forward-only` —— 在 `validate`（退出码 2）和 `run` 下都会如此，因为两者
走的是同一个校验入口。在 `edges` 块内没有其他键，本版本也没有 `mode`；
反向遍历改用它自己的 `rewind` / `retry_all` / `max_handoffs` 键（见
[进阶反向控制声明](cli.md#进阶反向控制声明)）。

数字来源键在 YAML、JSON 和 TOML 中都可用：JSON/TOML 把 seq 0 写成键 `"0"`
（例如 `"edges": {"0": [2]}`）。精确的任务名优先于数字字符串。数字
目标仍是整数。通过名称和 seq 重复声明同一个来源属于错误，
而不是悄悄替换其中一个列表。生效后的配置对重复的或
保留的（`end`）名称使用数字 seq，而不是发明一种 `name#N` 语法。

该特性属于**进阶**：它改变执行模型，因此是可选的、在 1.0 之前标记为实验性，
并且需要一个能原子提交交接的存储（两个内置后端都可以）。不带该
配置块的流水线在任何方面都不受影响。API 见
[参考 → 进阶：交接](reference.md#进阶交接可选启用)，模型见
[设计 §4.8](design.md#48-进阶交接--声明式正向跳转可选启用实验性)。

声明式层只描述**组合与资源**；逻辑仍留在 Python 中、位于 `use:` 之后。
它无法表达的东西，都是使用 SDK 的理由，而不是增加 YAML 的理由 —— 这条界线画在哪里，
参见 [`docs/tutorial.md`](tutorial.md)（见第 11 步）。

### 进阶反向控制声明

`pipeline.control` 还接受 `rewind: {source: [earlier_targets]}`、`retry_all: [sources]`，以及
反向遍历所必需的、必须为正的 `max_handoffs`。对于仅反向的方案，`edges` 是可选的；
已有的仅向前声明保持不变。`run.max_handoffs` 设置运行时的上限（默认
1000）。校验复用 Python 的名称/seq 解析，并报告配置字段路径。回退载荷
由任务代码而非配置选择。语法、访问模型、预算生命周期和载荷缺失恢复见
[reference → 进阶：反向遍历](reference.md#进阶反向遍历rewindretry-allvisits)；
可运行的 Python 示例（含 `HistoryArtifact`）见
[tutorial 第 16–17 步](tutorial.md#第-16-步--高级用回退和全部重试重新生成)。

## 另见

* [`docs/tutorial.md`](tutorial.md) —— 引导式路线，含可运行程序
* [`docs/reference.md`](reference.md) —— SDK 暴露的每个类和函数
* [`docs/design.md`](design.md) —— 为什么 CLI 是这些命令而没有别的
* [`README.md`](../../README.zh-CN.md) —— 简明导览
