# 发布

[English](../releasing.md) | **简体中文**

给有 push 权限的维护者看的。发布流程是自动化的：你推一个标签，[`.github/workflows/release.yml`](https://github.com/Hazer-BJTU/pyattacker/blob/main/.github/workflows/release.yml) 搞定剩下所有事。

## 一次性设置：可信发布

工作流通过 OIDC 向 PyPI 认证，仓库里不存 API token，也没有需要轮换的东西。需要在两个索引上各配一个*待定发布者*（pending publisher），以及对应的 GitHub 环境。

### 1. 在 PyPI 上注册发布者

到 <https://pypi.org/manage/project/pyattacker/settings/publishing/> 添加一个 GitHub 发布者：

| 字段 | 值 |
|---|---|
| Owner | `Hazer-BJTU` |
| Repository | `pyattacker` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

再到 <https://test.pypi.org/manage/account/publishing/> 用环境名 `testpypi` 配一遍。第二个不能省：每个标签都会先发 TestPyPI，确认没问题才发 PyPI，没配它整个发布流程都跑不起来。TestPyPI 就是个试验场，多一份拷贝不花钱。

环境名不能随便填。PyPI 会拿它和工作流里的 `environment:` 比对，对不上的话上传会以一个莫名其妙的 403 失败，而不是告诉你哪里错了。

### 2. 创建 GitHub 环境

在 **Settings → Environments** 里创建 `pypi` 和 `testpypi` 两个环境。

`pypi` 那个建议把自己加为 **required reviewer**。这样推标签后会暂停一下：构建照常跑，跑完等你确认才上传。这是发布前最后一个能撤回的节点，代价就是多点一下。

再给 `pypi` 配部署分支规则，标签模式用 `v*`，这样只有标签能触发这个环境的身份，分支推送借不走。

## 执行一次发布

**1. 全部合入 `main`，更新变更日志。**

`CHANGELOG.md` 里要有一个 `## [X.Y.Z] — YYYY-MM-DD` 的小节，工作流会把它原样拿出来当 GitHub 发布说明。没有这个小节也能发，但会带个占位符和警告。

**2. 改两个地方的版本号**——必须一致，标签和任何一个对不上，工作流都会拒绝发布。还需同步 `docs/design.md`、`docs/zh-CN/design.md`
开头的版本号，以及两个 README 的当前状态摘要：

```bash
# pyproject.toml:  version = "X.Y.Z"
# src/pyattacker/__init__.py:  __version__ = "X.Y.Z"
uv sync                     # refresh uv.lock
uv run pytest               # tests/test_packaging.py asserts the two agree
```

**3. 先预演一下你要打标签的那个提交**（可选——标签本身也会跑同样的流程，这一步的意义是趁版本号还在你手里的时候发现问题）：

Actions → Release → **Run workflow**，勾选 *Publish to TestPyPI*。这会跑完整校验、传到 TestPyPI、再从 TestPyPI 装回来——和标签做的三件事一模一样。然后自己验证一下：

```bash
uv venv /tmp/verify --python 3.11
uv pip install --python /tmp/verify/bin/python \
  --index-url https://test.pypi.org/simple/ \
  "pyattacker==X.Y.Z"
/tmp/verify/bin/pyattacker --version   # must print X.Y.Z, not the previously released version
/tmp/verify/bin/pyattacker demo --pipelines 20
rm -rf /tmp/verify
```

每个参数都不能少。`--index-url` 确保装的是 TestPyPI 的包，不是 PyPI 的。一个索引就够了，因为基础包**零依赖**——PyYAML 是可选的 `yaml` extra（见 README）——不需要从别的地方拉东西。只有两种情况需要加 `--extra-index-url https://pypi.org/simple/ --index-strategy unsafe-best-match`：预演更早的版本（它的元数据还要求 `pyyaml`），或者预演 `yaml` extra 本身（`"pyattacker[yaml]==X.Y.Z"`）。TestPyPI 上的 `pyyaml` 冻结在旧版本，而且 uv 里 `--extra-index-url` *优先于* `--index-url`，所以第二个索引一参与，`pyattacker` 自己就会从 PyPI（上一个版本在的地方）解析，不是从 TestPyPI。那样装是装得上，还会"欣然"报告一个*更旧*的版本，等于什么都没验证。这就是为什么固定版本、策略标志和 `--version` 断言必须一起用。

预演会用真实版本号上传，所以**先提交再预演，别反过来**：任何索引见过的文件名都不能再传第二次，内容不同也不行，删了也不行（[PyPI 的规则](https://pypi.org/help/#file-name-reuse)对 TestPyPI 同样适用）。预演完了发现问题再修？那就得换个新版本号了——标签阶段也会告诉你这事。

**4. 打标签并推送。** 这一步才是真正发版：

```bash
git checkout main && git pull --ff-only origin main
git tag -a vX.Y.Z -m "pyattacker X.Y.Z"
git push origin vX.Y.Z
```

然后工作流会在 3.11 和 3.12 上跑测试、构建并检查产物、发到 TestPyPI、从 TestPyPI 装回来跑 CLI、再发到 PyPI、最后用 changelog 小节和附带文件创建 GitHub Release。`pypi` 环境的 required reviewer 就是那个不可撤回步骤之前的暂停点。

**5. 最终确认：**

```bash
uv venv /tmp/final --python 3.11
uv pip install --python /tmp/final/bin/python pyattacker==X.Y.Z
/tmp/final/bin/pyattacker --version
/tmp/final/bin/pyattacker demo --pipelines 20
rm -rf /tmp/final
```

## 工作流上传前会做哪些检查

标签可以打在任何提交上，包括从没跑过 CI 的，所以发布会重跑所有检查，不会想当然：

| 检查项 | 为什么要查 |
|---|---|
| 3.11 和 3.12 上跑完整测试、lint、CLI 和示例冒烟测试 | 被打标签的提交本身必须是绿的 |
| 标签和 `__version__` 一致 | 不一致的标签会发出一个再也找不回来的版本 |
| `twine check --strict` | README 在 PyPI 上得能正常渲染，相对链接在那里是打不开的 |
| sdist 不含 `.claude/`、`.venv/`、`.pyc` | 0.1.0 就出过一次本地配置文件被打进包的事故 |
| sdist 不超过 1 MiB | `assets/` 把 883 KB 的 logo PNG 塞进 1.26 MB 的 tarball，0.2.0 就这么发出去了 |
| 单独拿 sdist 能重新构建并通过测试 | 不能重新构建的源码包不算源码分发 |
| wheel 能装上，`pyattacker demo` 能跑 | 能抓出损坏的入口点或缺模块的问题 |
| 从 TestPyPI 按名字装回来，版本号对得上 | 已发布的元数据得能按用户实际安装的方式解析，没人跑的预演证明不了任何东西 |

## 出了问题怎么办

**已发布的版本号不能重用。** PyPI 允许你删发布，但不允许重新传同一个版本号。有问题的包到了 PyPI？撤回（yank）它，发个补丁版本——别想着替换。

**上传失败但标签已经推了。** 修好原因，删掉标签重新推（`git push --delete origin vX.Y.Z`，然后重新打标签）。还没发任何东西之前删标签是安全的；一旦到了 PyPI，就只能换新版本号。

**重跑提示文件已存在。** 这是 `--check-url` 在干活：索引上已经有*哈希相同*的文件就跳过，所以中途挂掉的发布直接重跑就行——修好原因后用 `gh run rerun <run-id> --failed`，不用新标签。比较是按哈希不是按文件名，所以这只在提交没变的时候管用。

**报 `Local file and index file do not match` 或 `Filename has been previously used`。** TestPyPI 上这个版本的文件字节和你现在的不一样：预演之后又提交了一次，重新构建的 sdist（它包含 `docs/`）哈希就变了。删发布没用，两个索引都不允许重用文件名（[删了也不行](https://pypi.org/help/#file-name-reuse)）。解决办法是换个新版本号：改 `pyproject.toml` 和 `src/pyattacker/__init__.py`，加 changelog 小节，预演一遍，再打标签。另一种做法是把预演过的那个提交当正式发布，把标签移到它上面（删了重推）——只要还没到 PyPI 就是安全的。

**PyPI 返回 403。** 十有八九是发布者配置的问题：核对 owner、repository、工作流文件名（`release.yml`）和环境名是否和作业里写的完全一致。

## 手动发布

不推荐，OIDC 设置本来就是为了免 token——手动发等于把 API token 又请回来。非要这么干的话：

```bash
rm -rf dist && uv build
uvx twine check --strict dist/*
UV_PUBLISH_TOKEN=<token> uv publish dist/*
```
