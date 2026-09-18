# 发布

[English](../releasing.md) | **简体中文**

面向拥有 push 权限的维护者。发布是自动化的：你推送一个标签，
[`.github/workflows/release.yml`](../../.github/workflows/release.yml) 会完成其余所有事情。

## 一次性设置：可信发布

工作流通过 OIDC 向 PyPI 认证，因此仓库里没有 API token，也没有任何需要
轮换的东西。它需要在每个索引上有一个*待定发布者*（pending publisher），以及一个匹配的 GitHub 环境。

### 1. 在 PyPI 上注册发布者

在 <https://pypi.org/manage/project/pyattacker/settings/publishing/> 添加一个 GitHub 发布者：

| 字段 | 值 |
|---|---|
| Owner | `Hazer-BJTU` |
| Repository | `pyattacker` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

在 <https://test.pypi.org/manage/account/publishing/> 用环境 `testpypi` 重复一遍。第二个
并非可选：每个标签都会先发布到 TestPyPI，并在触碰 PyPI 之前从那里把文件装回来，
所以没有它就无法开始一次发布。TestPyPI 是试验场，多这一份副本不花任何代价。

环境名称不是可选项。PyPI 会拿它与工作流的 `environment:` 比对，一旦不匹配，
上传就会以一个令人困惑的 403 失败，而不是给出清晰的错误。

### 2. 创建 GitHub 环境

在 **Settings → Environments** 中创建 `pypi` 和 `testpypi`。

对于 `pypi`，可以考虑把自己加为 **required reviewer**。这会把一次标签推送变成一次暂停：
构建照常运行，然后等待你批准上传。这是发布仍可取消的最后一刻，
而代价只是一次点击。

通过 `pypi` 的部署分支规则（用 `v*` 作为标签模式）把它限制为只接受标签，
这样该环境的身份就无法被分支推送借用。

## 执行一次发布

**1. 把所有内容合入 `main` 并更新变更日志。**

`CHANGELOG.md` 需要一个 `## [X.Y.Z] — YYYY-MM-DD` 小节：工作流会原样提取它作为 GitHub
发布说明。没有该小节时发布仍会进行，但会带一个占位符和一条警告。

**2. 在两处提升版本号** —— 两者必须一致，如果标签与其中任何一处不一致，
工作流会拒绝这次发布：

```bash
# pyproject.toml:  version = "X.Y.Z"
# src/pyattacker/__init__.py:  __version__ = "X.Y.Z"
uv sync                     # refresh uv.lock
uv run pytest               # tests/test_packaging.py asserts the two agree
```

**3. 预演你将要打标签的提交**（可选 —— 标签自身会运行同样的阶段，所以这一步的意义在于
趁版本号还由你掌控时发现问题）：

Actions → Release → **Run workflow**，勾选 *Publish to TestPyPI*。这会运行完整校验、
上传到 TestPyPI，并从那里把文件装回来 —— 与标签所做的三件事完全相同。然后
自己检查结果：

```bash
uv venv /tmp/verify --python 3.11
uv pip install --python /tmp/verify/bin/python \
  --index-url https://test.pypi.org/simple/ \
  "pyattacker==X.Y.Z"
/tmp/verify/bin/pyattacker --version   # must print X.Y.Z, not the previously released version
/tmp/verify/bin/pyattacker demo --pipelines 20
rm -rf /tmp/verify
```

这里的每个标志都不可或缺。`--index-url` 才让安装来自 TestPyPI 而不是 PyPI。
一个索引就够了，因为基础包**完全没有依赖** —— PyYAML 是可选的 `yaml`
额外依赖（extra，见 README）—— 所以不必从别处获取任何东西。有两种情况仍需要更宽的形式
`--extra-index-url https://pypi.org/simple/ --index-strategy unsafe-best-match`：预演更早的发布
（其元数据仍要求 `pyyaml`），以及预演该额外依赖本身（`"pyattacker[yaml]==X.Y.Z"`）。TestPyPI 的
`pyyaml` 冻结在 3.11，而在 uv 中 `--extra-index-url` *优先于* `--index-url`，所以只要第二个索引
参与进来，`pyattacker` 自身就会从 PyPI（上一个发布所在之处）解析，而不是从 TestPyPI 解析。那样的
安装会成功，并欣然报告*更旧*的版本，什么也没有验证到 —— 这就是为什么固定版本、策略
标志与 `--version` 断言必须放在一起。

预演会以真实的版本号发布，所以**要先提交再预演，而不是反过来**：任何索引见过的
文件名都永远无法再次上传，即使内容不同、即使把它删除之后也不行
（[PyPI 的规则](https://pypi.org/help/#file-name-reuse)同样适用于 TestPyPI）。预演之后再
修复就意味着换一个新的版本号，而这正是标签阶段会告诉你的事情。

**4. 打标签并推送。** 这一步才真正执行发布：

```bash
git checkout main && git pull --ff-only origin main
git tag -a vX.Y.Z -m "pyattacker X.Y.Z"
git push origin vX.Y.Z
```

随后工作流会在 3.11 和 3.12 上运行测试套件，构建并检查工件，把它们发布到
TestPyPI，从 TestPyPI 安装回来并在那里运行 CLI，发布到 PyPI，并用变更日志小节和
附带的文件创建 GitHub 发布。`pypi` 环境的 required reviewer 就是那个
无法撤回的步骤之前的暂停点。

**5. 确认：**

```bash
uv venv /tmp/final --python 3.11
uv pip install --python /tmp/final/bin/python pyattacker==X.Y.Z
/tmp/final/bin/pyattacker --version
/tmp/final/bin/pyattacker demo --pipelines 20
rm -rf /tmp/final
```

## 工作流在上传前会检查什么

标签可以指向任何提交，包括从未通过 CI 的提交，所以发布会重新运行一切，
而不是轻信它已经通过：

| 检查项 | 它存在的理由 |
|---|---|
| 在 3.11 和 3.12 上运行完整测试套件、lint、CLI 与示例冒烟测试 | 被打标签的提交本身必须是绿的 |
| 标签与 `__version__` 一致 | 不一致的标签会产生一个再也没人能找回来的发布 |
| `twine check --strict` | README 必须在 PyPI 上正常渲染，而相对链接在那里无法解析 |
| sdist 不含 `.claude/`、`.venv/`、`.pyc` | 这一项在 0.1.0 中抓到过一个泄漏的本地配置文件 |
| sdist 保持在 1 MiB 以下 | `assets/` 把 883 KB 的 logo PNG 塞进了 1.26 MB 的 tarball，而 0.2.0 把它发布了出去 |
| 单独用 sdist 就能重新构建并通过其测试 | 无法重新构建包的 sdist 算不上源码分发 |
| wheel 能安装且 `pyattacker demo` 能运行 | 能抓到损坏的入口点或缺失的模块 |
| 发布的文件能按名称从 TestPyPI 安装并报告 `__version__` | 已发布的元数据必须按用户解析它的方式解析，而没人运行的预演证明不了任何东西 |

## 如果出了问题

**已发布的版本无法重用。** PyPI 允许你删除一个发布，但绝不允许重新上传该
版本号。如果有问题的工件到了 PyPI，就撤回（yank）它并发布一个补丁版本 —— 不要试图替换它。

**上传失败但标签已推送。** 修好原因，删除并重新推送标签
（`git push --delete origin vX.Y.Z`，然后重新打标签）。在尚未发布任何东西时删除标签是安全的；
一旦 PyPI 拥有了该版本，就改为换一个新的版本号。

**重新运行提示文件已存在。** 这正是 `--check-url` 在履行职责：索引上已存在
*哈希相同*的文件会被跳过，所以中途失败的发布直接重新运行即可 —— 修好原因并使用
`gh run rerun <run-id> --failed`，不需要新标签。比较是按哈希而非文件名进行的，因此这只在
提交未变时成立。

**`Local file and index file do not match`，或 `Filename has been previously used`。** TestPyPI 持有的这个
版本来自不同的字节：预演之后再有一次提交会重新构建 sdist —— 它包含
`docs/` —— 并改变其哈希。删除该发布没有用，因为两个索引都绝不允许
文件名被重用（[删除之后也不行](https://pypi.org/help/#file-name-reuse)）。补救办法是换一个新的
版本号：提升 `pyproject.toml` 与 `src/pyattacker/__init__.py`，添加变更日志小节，预演
它，然后打标签。另一种做法是把预演过的提交当作正式发布，把标签移到它上面
（删除并重新推送标签），这在没有任何东西到达 PyPI 之前是安全的。

**PyPI 返回 403。** 几乎总是发布者配置的问题：检查 owner、repository、工作流
文件名（`release.yml`）和环境名称是否与作业声明的完全一致。

## 手动发布

并不需要，而且值得避免 —— 它会把 OIDC 设置本来就要消除的 API token 又带回来。如果你
非要如此：

```bash
rm -rf dist && uv build
uvx twine check --strict dist/*
UV_PUBLISH_TOKEN=<token> uv publish dist/*
```
