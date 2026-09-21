# Maintaining the documentation

**English** | [简体中文](zh-CN/documentation.md)

The website uses the Markdown files in `docs/` directly. The Python API reference is currently
curated Markdown; automatic API extraction is not part of this first site migration.

## Preview locally

From the repository root, install the locked documentation dependencies and run the server:

```bash
uv sync --locked --group docs
uv run --group docs mkdocs serve
```

Open the address printed by MkDocs (normally `http://127.0.0.1:8000/pyattacker/`).
The `/pyattacker/` prefix matches GitHub Pages, so relative links can be checked locally.

## Check a change

```bash
uv run --group docs mkdocs build --strict
uv run pytest tests/test_docs_i18n.py tests/test_docs_examples.py tests/test_tutorial.py
```

The generated `site/` directory is ignored by Git. Keep dependencies in the `docs` dependency
group and commit `uv.lock` updates. They are not library runtime dependencies.
Add new pages to `mkdocs.yml`, with a Chinese counterpart and reciprocal language links.
Extend the document pairs in `tests/test_docs_i18n.py`; keep runnable code blocks identical
between languages. Existing tutorial/reference markers must remain so the tests execute examples.
Links to files outside `docs/` should use full GitHub URLs; Markdown pages inside `docs/` use
relative links. Strict builds check page links and anchors, but do not check external URL availability.

## Publish

The repository's Pages source is **GitHub Actions**. The Documentation workflow builds on PRs,
pushes to `main`, and manual runs. PRs only validate; pushes to `main` or manual runs on `main`
upload the site and deploy through the `github-pages` environment. No custom domain is required.

The site is published at <https://hazer-bjtu.github.io/pyattacker/>. Check the repository's Actions
tab if a build or deployment fails. Environment protection rules may require an approval before
deployment. The deployment job alone receives Pages and OIDC write permissions.

## Version policy

The first site tracks `main` and displays a development-version notice on every page.
It is not a frozen reference for the latest package release. A later versioned publishing setup
can add a stable release alongside development documentation.
