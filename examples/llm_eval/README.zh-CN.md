# 示例：一次 LLM 评测的两种流水线形态

[English](README.md) | **简体中文**

这是一个完整的实例，讲的正是这类框架总被重写去解决的事情：用多个评委给模型答案打分，
为每个请求留下可用的记录，并在失败后继续运行，而不必为失败重复付费。

```bash
uv run python -m examples.llm_eval.demo     # the guided run (~1s, fully deterministic)
```

## 场景

```
        seed ─▶ A prepare ─▶ B ask ─▶ C score (3 judges) ─▶ D reduce
```

| 阶段 | 做什么 | 为什么值得关注 |
|---|---|---|
| **A prepare** | 规范化空白字符，检查该行是否可用 | 普通任务：先校验再花费 token，输入有问题时抛出 `FatalError` |
| **B ask** | 主模型，**在同一个任务内被调用两次**（两轮对话） | 每一轮都获取并归还资源池，因此缓慢的对话不会在轮次之间占用并发槽位 |
| **C score** | 三种评委配置给答案打分 | 有两种形态，见下文 |
| **D reduce** | 对三个分数取平均，判定通过/失败 | **用户代码**：框架只记录事实，从不做归约 |

`backend.py` 是一个独立模块，放着模拟的提供方。它复现了那些
让评测变难的性质——延迟、偶发失败、格式错误的 JSON、按端点区分的状态——而且
别无其他。回复由请求推导而来，因此相同的运行会产生相同的输出，
也正因如此，下面这些数字才是一次测量，而不是一则轶事。

## 两种形态

**分组形态（Grouped）**——一个任务，三个并发评委：

```
A ─▶ B ─▶ C score_all ─▶ D          checkpoint: ○────────○────────○───────○
```

**拆分形态（Split）**——每个评委一个任务：

```
A ─▶ B ─▶ C1 strict ─▶ C2 balanced ─▶ C3 terse ─▶ D
                                            checkpoint: ○──○──○──○──○──○──○
```

流水线是一条线性的链，因此每个评委会把它继承到的分数继续向前传递。这种
累加器惯用法，是表达“并且还要”的标准做法，又不会把流水线变成
有向无环图（DAG）。

这里的链之所以是线性的，自有其原因：这个 demo 希望*每个*评委都对每一行运行，它的
重点是检查点粒度，而不是路由。如果某个步骤反而认为，对某一行来说链的其余部分
没有必要——比如一个自信的答案不需要任何指标，或者一个样本不在范围内——那么可选的
[交接](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#进阶交接可选启用)
会把这一点记录在案（`Handoff.to("reduce", ...)` 向前跳过，`Handoff.end(...)` 结束该
行），被跳过的站点不会留下任务行，恢复时会从目标处继续。这也不会
把流水线变成 DAG：边是声明式的、只能正向，而且仍然没有汇合节点。

## 这个 demo 测出了什么

第 1 轮把 `mock-judge-terse` 停用一整轮。第 2 轮把它恢复，并恢复两条
流水线：

```
shape    round 1  round 2  sent  needed  wasted judge calls
-------  -------  -------  ----  ------  ------------------
grouped  8        3        11    3       2
split    6        1        7     3       0
```

* **sent**——两轮中发给提供方的每一个请求。
* **wasted judge calls**——重新计算了已经成功的分数的评委请求。

日志里能看到两件事：

```
split    resumed at seq=4 (0=prepare 1=ask 2=C1 3=C2 4=C3 5=reduce) — C1 and C2 were not re-executed
grouped  resumed at seq=2 — it has no checkpoint inside C
```

**拆分形态的优势在于检查点粒度。** 任务的工件在产生的那一刻
就被持久化，因此 C3 处的失败会*从 C3 处*恢复：strict 与 balanced 的分数会从
存储中读回，永远不会被重新请求。分组任务内部没有检查点，因此任何
失败——包括崩溃后的恢复——都会重放整个分组。有 *k* 个评委时，分组
形态每次重试、每次恢复都要重发 *k* 个请求；拆分形态则恰好只重发一个。

代价也同样诚实：三个任务而不是一个，意味着三个工件、三条任务行，以及
三对 `acquire`/`release` 而不是一对——记账稍多，但每个评委变得
可以单独重试，也能在记录中单独看到。

**该怎么选：** 当评委调用昂贵或缓慢时、当单个评委不稳定时，或者
当评测长到重启一次都很痛苦时，选拆分形态。当各分支开销很低，或者
当你确实希望该步骤具有全有或全无语义时，选分组形态。

## 值得借鉴的模式

* **在任务内循环，在请求之间归还。** `ask` 调用模型两次，但只在每次调用期间
  持有租约：
  ```python
  for turn in range(1, TURNS + 1):
      async with ctx.acquire() as lease:      # released before the next turn
          reply = await lease.client.complete(messages)
          lease.report(ok=True, usage={"tokens": len(reply.split())})
  ```
* **校验模型输出，再对失败分类。** 一个返回 `{score: 4` 的评委并不是
  网络问题；`_parse_score` 会抛出 `RetryableError(error_class="invalid_response")`，好让
  策略重试它；而如果只抛出原本的 `json.JSONDecodeError`，它会被归类为
  致命错误。
* **上报资源健康状况。** 格式错误的回复会调用 `lease.report(ok=False, error=...)`，正是
  这一点让资源池可以降级持续表现异常的端点。
* **绝不要让 fanout（扇出）的请求变成孤儿。** `score_all` 会使用
  `asyncio.gather(..., return_exceptions=True)`，并且只在所有分支都结束后才抛出异常。
  普通的快速失败 `gather` 在第一个异常时就返回，而其他请求仍在
  执行中——它们继续持有租约，其重试只是在重复没人会去读取的工作。
* **轮次中途的取消或崩溃会留下可恢复的状态。** 上面的第 1 轮以一个失败的
  流水线结束，第 2 轮把它跑完；不需要手工重建任何东西。

## 让它更贴近真实

```python
# random (but reproducible) failures instead of a scripted switch
pools = make_pools(log, failure_rate=0.05)
# malformed judge output
Resource.create(..., options={"malformed_rate": 0.03})
```

## 查看结果

```bash
uv run pyattacker report runs/llm_eval_clean.db            # states, latency, errors
uv run pyattacker serve  runs/llm_eval_clean.db            # live HTTP dashboard
uv run pyattacker export runs/llm_eval_clean.db out.jsonl  # pipelines + artifacts
uv run pyattacker export runs/llm_eval_clean.db tasks.csv --rows tasks --format csv
```
