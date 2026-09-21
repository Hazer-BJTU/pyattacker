# 文档维护

[English](../documentation.md) | **简体中文**

网站直接使用 `docs/` 中的 Markdown。Python API 参考目前仍是人工维护的 Markdown；
第一版网站迁移不包含自动提取 API。

## 本地预览

在仓库根目录安装锁定的文档依赖并启动服务：

```bash
uv sync --locked --group docs
uv run --group docs mkdocs serve
```

打开 MkDocs 输出的地址，通常为 `http://127.0.0.1:8000/pyattacker/`。
`/pyattacker/` 前缀与 GitHub Pages 一致，便于在本地检查相对链接。

## 检查修改

```bash
uv run --group docs mkdocs build --strict
uv run pytest tests/test_docs_i18n.py tests/test_docs_examples.py tests/test_tutorial.py
```

生成的 `site/` 目录被 Git 忽略。文档依赖放在 `docs` 依赖组中，更新时提交 `uv.lock`；
它们不属于库的运行时依赖。
新增页面应加入 `mkdocs.yml`，提供中文对应页和双向语言链接。
扩展 `tests/test_docs_i18n.py` 的文档配对表；中英文可运行代码块应保持一致。
保留教程和参考中的示例标记，让测试继续执行它们。
指向 `docs/` 之外文件的链接使用完整 GitHub URL，文档内部使用相对链接。
严格构建会检查页面链接和锚点，但不检查外部 URL 的可访问性。

## 发布

仓库 Pages 来源为 **GitHub Actions**。Documentation workflow 在 PR、推送到 `main`
和手动触发时构建。PR 只验证；推送到 `main` 或在 `main` 上手动触发时上传站点，
通过 `github-pages` environment 部署。不需要自定义域名。

站点地址为 <https://hazer-bjtu.github.io/pyattacker/>。构建或部署失败时查看仓库 Actions。
如果 environment 设置了保护规则，部署可能需要审批。
只有部署 job 获得 Pages 与 OIDC 写权限。

## 版本策略

第一版网站跟随 `main`，每页显示开发版提示，并非最新已发布包的固定版本参考。
后续可增加按版本发布，在开发文档之外提供 stable 文档。
