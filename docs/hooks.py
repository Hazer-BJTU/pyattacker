"""Preserve the repository's GitHub-style heading links in the rendered website."""


def github_slug(text: str, separator: str) -> str:
    # Markdown has already stripped inline markup before calling the TOC slugger.
    # Keep Unicode and repeated spaces: 'Step 1 — task' becomes 'step-1--task'.
    return "".join(
        char if char.isalnum() or char in "-_" else separator if char.isspace() else ""
        for char in text.lower()
    )


def on_config(config):
    config.mdx_configs["toc"]["slugify"] = github_slug
    return config
