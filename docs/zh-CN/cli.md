# CLI 参考

[English](../cli.md) | **简体中文**

```
pyattacker {run,resume,report,watch,export,serve,plugins,validate,demo}
```

所有命令也可以通过 `python -m pyattacker ...` 调用。分三组：
**跑任务**（`run`、`resume`、`demo`）、**看结果**（`report`、`watch`、`export`、`serve`）、**查环境**（`validate`、`plugins`）。

## 退出码

| 代码 | 含义 |
|---|---|
| `0` | 全部流水线成功（或已完成被跳过） |
| `1` | 运行结束但部分流水线失败，或者有流水线修不好（`repair_failures`） |
| `2` | 配置错误——啥都没跑；以 `WorkerCrashed`、`StoreUnavailable` 这类框架级错误结束的也算 |
| `130` | 被中断（SIGINT）；挂起和正在跑的流水线都会标记为可恢复 |

`2` 的意思是"改配置"（或者"看错误信息"）；`1` 的意思是"看报告"。`130` 退出的随时可以 `resume`。

`1` 涵盖两种情况，报告里会写清楚：一种是本次运行中流水线进了失败终态（算在 `pipelines.by_state` 里），另一种是本次运行没法把某条流水线从撕裂状态写回终态——就是报告（和 `--summary-format json`）里的 `repair_failures`，因为它的记录故意保留了最初的失败和那次运行的信息。

`run` 退出 `2` 不一定是配置问题：worker 在自己的处理逻辑之外挂了会抛 `WorkerCrashed`（打印出来是 `WorkerCrashed: worker for pipeline … died with …`），它持有的流水线已经记为失败，运行记录也已经关了。运行记录在错误之后还在，所以 `pyattacker report <store>` 还能看到发生了什么；修好根因后用 `--resume` 接着跑。

---

## `run` —— 运行声明式配置

```bash
pyattacker run -c config.yaml [options]
```

| 参数 | 作用 |
|---|---|
| `-c, --config PATH` | 配置文件：yaml、toml 或 json。按后缀选解析器，yaml 需要装可选的 `yaml` extra |
| `--limit N` | 只跑前 N 条流水线（在真实数据集上做冒烟测试用） |
| `--store PATH` | 覆盖 `run.store`。和 `--shard` 一起用就直接用这个路径，否则自动加分片后缀 |
| `--concurrency N` | 覆盖 `run.concurrency`——指的是*同时在跑的尝试数*，不是存活的流水线数 |
| `--max-admitted N` | 覆盖 `run.max_admitted`：每个 Runner 排队、执行、延迟流水线总上限（正整数；默认 `4 * concurrency`） |
| `--journal {full,summary}` | `full`（默认）存完整产物，这是任务级恢复的基础；`summary` 只存摘要 |
| `--label TEXT` | 给这次运行打个标签，方便以后区分 |
| `--resume` | 跳过已完成的流水线，失败的从检查点接着跑 |
| `--retry-succeeded` | 配 `--resume` 用，连成功的也重跑。它只是扩大*哪些*流水线参与，不会丢未完成流水线的检查点 |
| `--fresh-restart` | 让已经入队的流水线从种子重新开始：丢检查点和遍历状态，重置交接预算。只追加的历史（尝试/事件/交接记录）保留；开了反向遍历的流水线还额外保留访问实例和计数器 |
| `--strict-leases` | 租约泄漏直接让任务失败（`LeaseLeakError`），不偷偷强制回收。CI 里建议开 |
| `--stop-after-failures N` | 攒够 N 条失败就不再接新活（尽力而为：已经接了的会跑完） |
| `--no-signals` | 不装 SIGINT/SIGTERM 处理器 |
| `--artifact-backend SPEC` | 产物字节存哪：`inline`（默认）、`null`、`file:///path`，或 JSON 规格 |
| `--progress` | 开第二条连接实时打印进度 |
| `--shard I/N` | 只跑 N 个分片中的第 I 个。每个分片写自己的存储文件 |
| `--shards N` | 本地 fork N 个子进程，每个分片一个，最后合并出报告 |
| `--jobs N` | 同时跑多少个分片子进程（配合 `--shards`） |
| `--summary-format {text,json}` | `json` 的话每次运行输出一个摘要对象 |
| `--no-write-behind` | 每次尝试和事件都立即提交，不批量写 |
| `--strict-env` | 配置里没设的 `${VAR}` 直接退出码 2，不只是警告 |

`--shard` 和 `--shards` 是两种用法：前者是一个进程跑自己那份（其他你自己编排），后者是 pyattacker 本地帮你编排。分片分配是流水线 key 的纯函数——同一批数据永远按同样的方式切分，`--resume` 会把每条流水线放回原来的分片。每个子进程的环境变量里会带 `PYATACKER_SHARD=I/N`。

## `resume` —— `run --resume`

参数和 `run` 完全一样。把原来的命令原样再跑一遍，把 `run` 换成 `resume`：

```bash
pyattacker run    -c qa.yaml --shards 4 --store runs/qa.db
pyattacker resume -c qa.yaml --shards 4 --store runs/qa.db
```

实际会重跑什么：成功的流水线啥都不重跑；失败或被中断的，从第一个没产出 artifact 的任务开始，后面的全部重跑。产物已经在盘上的任务绝不会再执行一遍——已经花过钱的请求不会重发。有两种情况会打破这个保证，都会留一条 `pipeline.checkpoint_missing` 事件：`journal: summary` 和 `null` 产物后端，这两种都不存检查点需要的载荷。

`--resume` 也是用来认领那些状态还写着 `running` 的流水线的——这是硬杀进程留下的痕迹。开了反向遍历的流水线没它不会被接管：本次运行会跳过那一行（`pipeline.skipped`，`reason="owned_by_another_run"`），不会再分叉出另一场可能还在跑的遍历。要*丢*检查点而不是续跑，用 `--fresh-restart`（`fresh_restart=True`），它还会重置已经用完的交接预算。见[反向遍历](reference.md#恢复与所有权)。

## `demo` —— 零配置验证安装

```bash
pyattacker demo [--store PATH] [--pipelines N] [--concurrency N] [--fail-rate F] [--export PATH]
```

模拟任务，不联网、不要配置文件。`--fail-rate`（比如 `0.3`）会故意制造失败，方便你观察重试和重试决策记录。CI 冒烟测试够用。

---

## `bench` —— 在模拟中比较获取算法

```bash
pyattacker bench [--scenario NAME] [--list] [--algorithms A,B] [--seeds N]
                 [--jobs N] [--concurrency N] [--horizon S] [--wall-budget S]
                 [--clock {virtual,real}] [--speedup F] [--json PATH] [--markdown PATH] [--quiet]
```

不联网，也没有真实接口方：场景是个写死的世界（容量周期、令牌桶、延迟尾部、故障风暴、三个性格不同的端点），客户端是 worker 组成的闭环，跑的是真 `Pool` 和真算法，时间是模拟的。`docs/benchmark.md` 讲场景假设、指标含义和怎么读表；这里只列参数。

| 参数 | 作用 |
|---|---|
| `--scenario NAME` | 用哪个模拟世界，默认 `bursty_provider` |
| `--list` | 打印所有场景、所有算法、每个指标的单位和方向，然后退出码 0 |
| `--algorithms A,B` | 逗号分隔的子集；默认跑场景支持的所有算法——就算请求一个它声明不适用的算法，还是会跑，只是那列显示 N/A（比如单资源池场景跑 `failover` 看不出效果） |
| `--seeds N` | 跑 N 个种子取平均，种子是 `scenario.seed + 0 .. N-1`（默认 3，至少 1） |
| `--jobs N` | 覆盖场景的作业数（至少 1） |
| `--concurrency N` | 覆盖 worker 数量——也就是客户端的并发度，既是假设也是成本调节项（至少 1） |
| `--horizon S` | 覆盖模拟时间范围，到点就不接新作业了，不会中途截断正在跑的作业（正数） |
| `--wall-budget S` | 单次运行最多花多少真实秒（默认 600，正数）；跑不完直接报错，不返回半截指标 |
| `--clock {virtual,real}` | `virtual`（默认）是零成本的模拟时间；`real` 用压缩后的真实时间重放场景来验证模拟器，慢得多 |
| `--speedup F` | `--clock real` 的压缩倍数（默认 10，正数） |
| `--json PATH` | 完整报告写成 JSON（`-` 表示 stdout） |
| `--markdown PATH` | 报告写成 markdown 表格，含每个端点的准入情况 |
| `--quiet` | stderr 不打进度 |

表格输出到 stdout，进度输出到 stderr，所以 `pyattacker bench --json - --quiet | jq .` 可以串起来用。退出码和其他命令一致：`0` 跑完了，`2` 是场景或算法名不对。跑不完的基准也是 `2`——绝不会吐半截表格。参数越界也直接拒（`--seeds 0` 会提示 "at least 1"），不会偷偷夹到范围内：一个悄悄跑了和你说的不一样的实验，在正确的标题下报了错的数字——因为标题本身没错。

---

## `report` —— 统计和失败详情

```bash
pyattacker report STORE [STORE ...] [--run-id ID] [--errors N] [--json] [--artifact-backend SPEC]
```

多个存储会合并成一个一致的视图：按 `pipeline_id` 去重（状态最好的胜出，平局按最新完成时间判），工作量计数从存活下来的行重算，还会告诉你折叠了多少行。有一个计数刻意保持原始口径：摘要行里的 `source_events=` 是你传入那些存储的事件日志总和，重复的也算——因为事件并不挂在流水线行上。`--errors N` 打印前 N 个失败和错误类别。`--json` 用对象格式输出同样的数字。

如果存储里有过终端修复失败的尝试，report 会单独列出（`Terminal repair failures: N pipeline(s)`），每条注明流水线和失败阶段。这是事后可观测性指标：实时运行的 `RunReport.repair_failures` 是运行本地的计数器，事后视图从事件日志里查询这些流水线的修复失败历史——范围和报告里的流水线一致，不是全局统计。

开了交接的运行会在摘要行里注明（`... attempts: total=12 handoffs=2`），`export` 的 `--rows pipelines` 会带上账本本身（见[交接](reference.md#交接可选启用)）。

## `watch` —— 实时监控

```bash
pyattacker watch STORE [--run-id ID] [--interval S] [--iterations N] [--no-clear]
```

不指定 `--run-id` 时，`watch` 跟随最近启动的运行。使用 `--run-id all` 可查看全库运行统计；
该视图不显示某个运行的应用汇报指标。

对同一个 SQLite 文件开只读连接，所以能和正在跑的流水线并存（WAL 允许一个写入者多个读取者）。显示流水线状态分布、延迟百分位、每个资源池的 `active/capacity`、`ready/degraded/dead`、多少流水线在等待或挂起、最近的错误。开了交接的运行还会在 attempts 行显示 `handoffs=N`（选定运行的提交数，使用 `--run-id all` 则是全部历史；续跑可能复用更早的活跃交接）——开了控制流的流水线，游标是位置不是进度计数，所以任务列表很短的时候就是这个数字在起作用。`--iterations N` 让它自己退出，脚本里用很方便。

## `export` —— 导出记录

```bash
pyattacker export STORE [STORE ...] OUTPUT [--rows SHAPE] [--format FMT] [--run-id ID]
```

| `--rows` | 每行对应 |
|---|---|
| `pipelines`（默认） | 一条流水线，嵌套——带任务和最终产物 |
| `tasks` | 一个任务：终态、耗时、错误、用了哪个租约 |
| `attempts` | 一次尝试，含每次重试的 `decision` |
| `events` | 一个结构化事件 |
| `artifacts` | 一个产物，包括中间产物 |
| `results` | 一条流水线的最终结果、状态、错误和实验归属 |

`--format` 可以是 `jsonl`（默认）、`json` 或 `csv`。CSV 从第一批行取表头，后面出现的新 key 折进 `extra` 列，内存占用平稳，也不会悄悄丢字段。

每种类型都会完整导出：以前的版本事件超过 10 万条的存储，`--rows events` 只会导最新 10 万条。`events`/`attempts` 按最旧在前输出，`tasks`/`artifacts` 按流水线/`seq` 顺序输出，从存储里按有界批次读——[导出参考](reference.md#导出)里写了每种类型的准确顺序、`limit` 规则、内存说明，以及存储还在写的时候导出能保证什么、不能保证什么。

## `serve` —— 只读 HTTP 视图

```bash
pyattacker serve STORE [--host HOST] [--port PORT] [--run-id ID]
```

不指定 `--run-id` 时，面板跟随最近启动的运行。使用 `--run-id all` 可查看全库视图，
也可打开 `/?run_id=ID` 查看指定运行。

`/` 是个自动刷新的小仪表盘；`/stats`、`/metrics`、`/events`、`/pipelines`、`/resources`、`/errors` 返回 JSON。每个请求都新开一个只读连接，所以和正在跑的流水线并存是安全的。

**没有认证，也没有 artifact 路由。** 它暴露的是这次运行记下来的内容——事件的 `data` 原样呈现，存下来的 `error_message` 字段也在——所以默认只绑回环地址。要绑别的地址？前面自己加个代理。

---

## `validate` —— 不运行就检查配置

```bash
pyattacker validate -c config.yaml [--strict-env]
```

解析配置、解析每个 `use:` 目标和资源池引用，检查整个文件，打印生效后的配置。有问题就退出码 2 并告诉你原因。`--strict-env` 把没设的 `${VAR}` 从警告升级成错误——CI 里建议开，静默为空的 API key 比直接失败更糟。

`run` 和 `resume` 在创建存储之前也走同样的检查，所以 `validate` 拒的配置它们也会拒（退出码 2），一个任务都不会启动。检查的内容如下，每条消息都带字段路径：

| 检查项 | 例子 |
|---|---|
| 未知字段 | `run.concurency`（附带 "did you mean `run.concurrency`?"）、`pools.apis.capcity` |
| 字段类型 | `run.concurrency: "8"`、`pipeline.tasks[0].use: 5` |
| 数值范围 | `run.concurrency: 0`、`run.heartbeat_s` 小于等于 0、`pools.apis.capacity: 0` |
| 资源池引用 | `pipeline.resource`、`pipeline.tasks[0].resource`，以及 `use:` 工厂自己声明的 `resource` |
| 算法 | `algorithm: nosuchalgorithm`、`algorithm: {name: backoff, bse: 1}` |
| 产物后端 | `artifact_backend: {kind: file}` 缺 `root`、未知的 `kind`、解析不了的 JSON 字符串规格、或 `min_bytes` 不是非负整数 |
| 各节结构 | `pipeline:`（以及每一节）必须是 map——标量或列表都算配置错误，不会走到运行时 |
| source | `source.kind` 必须是 `range`/`jsonl`；`jsonl` 要 `source.path`；`source.repeats` 至少 1 |
| 重试 | `"on"` 里的名字、重试块的数值/布尔字段 |

它只做*声明式*检查：不会打开 `source.path`、不会遍历数据集、不会构建 `artifact_backend`（构建它会创建目录）、不会调任务。但它确实会检查 `artifact_backend` 是 `resolve_backend` 能造出来的——必填字段（比如文件后端的 `root`）也查——所以 `validate` 和 `run` 接受/拒绝的规格是完全一致的。`${VAR}` 的展开方式见下文。

## `plugins` —— 已安装的插件

```bash
pyattacker plugins [--json]
```

列出为 `pyattacker.tasks`、`pyattacker.algorithms`、`pyattacker.codecs`、`pyattacker.stores` 发现的所有入口点，**包括加载失败的和失败原因**。坏插件会被记下来，不会抛异常让运行崩——但也绝不会静默失败。

---

## 配置文件参考

下面的例子是 YAML，但每个接 `-c` 的命令都吃同样的内容，写成 `.json` 或 `.toml` 也行；加载器按后缀选解析器。只有 YAML 解析器是可选的 extra（`pip install "pyattacker[yaml]"`）——缺了会报配置错误并告诉你装哪个 extra，退出码 2，而且在任何东西跑起来之前就报了。这个配置块是完整的（每个 `use:` 都是内置的），测试套件会拿它过 `validate`。

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

`run:` 块接的字段和 CLI 映射到 `RunConfig` 的字段完全一致：`store`、`journal`、
`concurrency`、`max_admitted`、`label`、`heartbeat_s`、`grace_s`、`stale_after_s`、`strict_leases`、
`stop_after_failures`、`stop_after_s`、`max_handoffs`、`retry_succeeded`、`fresh_restart`、`seed`、`notes`、
`write_behind`、
`write_batch`、`flush_interval`、`artifact_backend` 和 `meta`。其他字段都是配置错误，不会被静默忽略。优先级很明确：命令行参数 > `run:` 块 > 内置默认值。

`artifact_backend` 的取值和 `--artifact-backend` 一样：`"inline"`（默认，产物留在存储里）、`"null"`（只存摘要，丢字节）、`"file:///data/blobs"`，或上面的 map 形式。YAML 里记得加引号：裸写的 `artifact_backend: null` 是 null *值*，等于 `inline`（和不写一样）；`"null"` 才是丢字节的后端。续跑需要构成检查点的那些产物，所以 `journal: summary` 和 null 后端都会让你失去任务级恢复。

任务条目还接 `config: {model: model-a}` 和 `version: "prompt-v2"`，用来表达没法从源码推断的行为。它们会进恢复指纹；工厂参数还是独立算。用了 `source.key_field` 时，在已有 key 下改任务同一性或种子内容会报配置错误（退出码 2），旧结果保留。改了就用新的 key/存储。升级已有存储前先看[恢复同一性](reference.md#恢复同一性)。

任务条目是**对解析后的 `use:` 目标的覆盖**，两种情况不一样：条目里没提的字段保持目标自己声明的值（它自己的 `resource`、`algorithm`、`timeout_s`、`retry`……），显式写 `field: null` 会清空支持为空的字段（`resource`、`algorithm`、`timeout_s`、`version`）。规则和 SDK 里的 [`TaskSpec.with_overrides`](reference.md#taskspec) 一样。

`${VAR}` 从环境变量展开（见 `--strict-env`）。`source.kind` 是 `jsonl` 或 `range`；`repeats: k` 就是 pass@k——每个种子 k 条独立流水线。CLI 参数覆盖 `run:` 块。

<a id="pipelinecontrol--进阶可选的交接"></a>

### `pipeline.control` —— 显式声明的交接

配置还能声明哪个任务允许通过返回 [`Handoff`](reference.md#交接可选启用) 来**交接**（向前跳过），这样同一条流水线就能用声明式表达：

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

目标可以是任务名、任务的数字 seq，或 `end`，必须严格晚于来源（`edges` 只向前）。链里出现两次的同名任务必须用 seq 指定。*最后一个*任务声明 `end` 会被拒，因为没意义。每个问题都带字段路径报出来——`pipeline.control.edges['judge'][0]: destination 'fetch' (seq 0) is not later than the source 'judge' (seq 2); v1 handoffs are forward-only`——`validate`（退出码 2）和 `run` 都走同一个校验入口，表现一致。`edges` 块里没有别的 key，本版本也没有 `mode`；反向遍历用自己的 `rewind` / `retry_all` / `max_handoffs` key（见[反向控制声明](cli.md#反向控制声明)）。

数字源 key 在 YAML、JSON、TOML 里都能用：JSON/TOML 把 seq 0 写成 key `"0"`（比如 `"edges": {"0": [2]}`）。精确任务名优先于数字字符串。数字目标还是整数。同一个来源既用名称又用 seq 声明是错的，不会悄悄替换其中一个。生效后的配置对重名的或 `end` 这种保留名用数字 seq 表示，不会发明 `name#N` 语法。

交接是**正式的控制流特性**：需要显式声明 `control`，并使用支持原子提交交接的存储（两个内置后端都支持）。不带这个配置块的流水线完全不受影响。API 见[参考 → 交接](reference.md#交接可选启用)，模型见[设计 §4.8](design.md#48-交接--声明式正向跳转可选启用)。

声明式层只管**组装和资源**；逻辑还在 Python 里，在 `use:` 后面。它表达不了的东西，都该用 SDK 而不是往 YAML 里加东西——这条界线画在哪，看 [`docs/tutorial.md`](tutorial.md)（第 11 步）。

<a id="进阶反向控制声明"></a>

### 反向控制声明

`pipeline.control` 还接 `rewind: {source: [earlier_targets]}`、`retry_all: [sources]`，以及反向遍历必须的正整数 `max_handoffs`。纯反向的方案里 `edges` 是可选的；已有的纯向前声明不变。`run.max_handoffs` 设运行时上限（默认 1000）。校验复用 Python 的名称/seq 解析，报配置字段路径。回退载荷由任务代码选，不是配置选。语法、访问模型、预算生命周期和载荷缺失恢复见 [reference → 反向遍历](reference.md#反向遍历rewindretry-allvisits)；可运行的 Python 示例（含 `HistoryArtifact`）见 [tutorial 第 16–17 步](tutorial.md#第-16-步--用回退和全部重试重新生成)。

## 另见

* [`docs/tutorial.md`](tutorial.md) —— 引导式教程，带可运行程序
* [`docs/reference.md`](reference.md) —— SDK 暴露的每个类和函数
* [`docs/design.md`](design.md) —— 为什么 CLI 是这些命令、没有别的
* [`README.md`](https://github.com/Hazer-BJTU/pyattacker/blob/main/README.zh-CN.md) —— 简明导览

## Suite 命令

Suite 配置复用 `run`、`resume`、`validate`。`run/resume` 支持重复的 `--experiment ID` 和 `--output-root`；`report/watch/export/serve` 支持 Suite 目录与 `--experiment ID`。`export --rows results` 输出简洁最终结果，`--by-experiment` 写入 OUTPUT/ID/ROWS.FORMAT。Suite 拒绝 `--store`、`run.store` 和分片参数。目录读取默认展示累计结果，`--run-id` 选择一次启动的提交记录与历史。

[完整指南与示例](suites.md)。
