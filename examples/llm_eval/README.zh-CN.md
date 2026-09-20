# 示例：两种流水线形态的 LLM 评测

[English](README.md) | **简体中文**

这是一个完整的端到端示例，展示了这类框架通常要解决的问题：用多个评委给模型答案打分、给每次请求留底、崩了能接着跑——不用为失败的请求重复花钱。

```bash
uv run python -m examples.llm_eval.demo     # the guided run (~1s, fully deterministic)
```

## 场景

```
        seed ─▶ A prepare ─▶ B ask ─▶ C score (3 judges) ─▶ D reduce
```

| 阶段 | 做什么 | 为什么值得关注 |
|---|---|---|
| **A prepare** | 规范化输入格式，检查数据是否可用 | 普通任务：先校验再花 token，输入有问题直接抛 `FatalError` |
| **B ask** | 调主模型，**同一个任务内调两次**（两轮对话） | 每轮都单独获取和归还资源，慢对话不会跨轮占着并发槽位 |
| **C score** | 三种评委配置分别打分 | 有两种实现形态，见下文 |
| **D reduce** | 三个分数取平均，判定通过/不通过 | **用户代码**：框架只记录事实，不算分 |

`backend.py` 是个独立模块，放着模拟的接口方。它复现了真实评测中那些烦人的特性——延迟、偶发故障、格式错误的 JSON、各端点独立的状态——除此之外没别的。回复内容由请求参数决定，所以同一份输入永远出同一份输出，下面的数字是实测结果，不是拍脑袋。

## 两种形态

**分组形态（Grouped）**——一个任务里跑三个并发评委：

```
A ─▶ B ─▶ C score_all ─▶ D          checkpoint: ○────────○────────○───────○
```

**拆分形态（Split）**——每个评委一个任务：

```
A ─▶ B ─▶ C1 strict ─▶ C2 balanced ─▶ C3 terse ─▶ D
                                            checkpoint: ○──○──○──○──○──○──○
```

流水线是一条直线链，所以每个评委会把拿到的分数往下传。这种累加器写法是"还要再做一件事"的标准表达，又不会把流水线变成 DAG。

这里的链是直线型是有原因的：这个 demo 想让*每个*评委都跑遍每一行，重点是检查点粒度，不是路由。如果某一步觉得某一行后面不用跑了——比如答案已经足够确定不需要算分，或者样本不在评测范围内——那就可以用可选的[交接](https://github.com/Hazer-BJTU/pyattacker/blob/main/docs/zh-CN/reference.md#交接可选启用)功能，把决定记录下来（`Handoff.to("reduce", ...)` 跳过中间步骤直接去 reduce，`Handoff.end(...)` 直接结束），被跳过的步骤不留任务记录，续跑时从目标步骤接着走。这也没把流水线变成 DAG：边是声明好的、只能向前、没有汇合节点。

## 这个 demo 测出了什么

第一轮把 `mock-judge-terse` 停掉。第二轮恢复它，并续跑之前没跑完的两条流水线：

```
shape    round 1  round 2  sent  needed  wasted judge calls
-------  -------  -------  ----  ------  ------------------
grouped  8        3        11    3       2
split    6        1        7     3       0
```

* **sent**——两轮加起来发给接口方的总请求数。
* **wasted judge calls**——重发了已经成功算过分的评委请求。

日志里能看到两个细节：

```
split    resumed at seq=4 (0=prepare 1=ask 2=C1 3=C2 4=C3 5=reduce) — C1 and C2 were not re-executed
grouped  resumed at seq=2 — it has no checkpoint inside C
```

**拆分形态的优势在检查点粒度。** 任务产物一产生就落盘，所以 C3 挂了就*从 C3 开始*恢复：strict 和 balanced 的分数直接从存储读回来，不会重新请求。分组任务内部没有检查点，所以任何失败——包括崩溃恢复——都会把整组重跑。有 *k* 个评委时，分组形态每次重试、每次恢复都要重发 *k* 个请求；拆分形态只重发一个。

代价也很实在：三个任务变一个，意味着三份产物、三条任务记录、三对 `acquire`/`release`——记账多一点，但每个评委可以单独重试，也能在记录里单独看到。

**怎么选：** 评委调用贵、慢，或者单个评委不稳定，或者评测跑到重启一次都心疼的时候，选拆分形态。各分支开销很小，或者你确实想要"要么全做要么全不做"的语义，选分组形态。

## 值得参考的写法

* **在任务内循环，每轮请求之间释放资源。** `ask` 调两次模型，但每次调用期间才持有租约：
  ```python
  for turn in range(1, TURNS + 1):
      async with ctx.acquire() as lease:      # released before the next turn
          reply = await lease.client.complete(messages)
          lease.report(ok=True, usage={"tokens": len(reply.split())})
  ```
* **先校验输出，再按错误类型分类。** 返回 `{score: 4` 这种半截 JSON 的评委，不是网络问题；`_parse_score` 会抛 `RetryableError(error_class="invalid_response")`，让策略去重试。如果只抛原始的 `json.JSONDecodeError`，会被当成致命错误。
* **上报资源健康状况。** 格式错误的回复会调 `lease.report(ok=False, error=...)`——资源池就是靠这个自动降级持续出问题的端点。
* **fanout 的子任务不能变孤儿。** `score_all` 用了 `asyncio.gather(..., return_exceptions=True)`，所有分支都结束后才统一抛异常。默认的快速失败 `gather` 碰到第一个异常就返回了，其他请求还在跑——它们还占着租约，重试也只是在重复没人读的工作。
* **中途取消或崩溃也能接着跑。** 第一轮跑完留了两条失败的流水线，第二轮接着跑，不用手动重建任何状态。

## 贴近真实场景

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
