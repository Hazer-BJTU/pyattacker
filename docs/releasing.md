# Releasing

**English** | [简体中文](zh-CN/releasing.md)

Written for maintainers with push access. Publishing is automated: you push a tag, and
[`.github/workflows/release.yml`](https://github.com/Hazer-BJTU/pyattacker/blob/main/.github/workflows/release.yml) does the rest.

## One-time setup: trusted publishing

The workflow authenticates to PyPI with OIDC, so there is no API token in the repository and nothing to
rotate. It needs a *pending publisher* on each index and a matching GitHub environment.

### 1. Register the publisher on PyPI

At <https://pypi.org/manage/project/pyattacker/settings/publishing/>, add a GitHub publisher:

| Field | Value |
|---|---|
| Owner | `Hazer-BJTU` |
| Repository | `pyattacker` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

Repeat at <https://test.pypi.org/manage/account/publishing/> with environment `testpypi`. This second one is
not optional: every tag publishes to TestPyPI first and installs the files back from it before PyPI is
touched, so a release cannot start without it. TestPyPI is scratch, so the extra copy costs nothing.

The environment name is not optional. PyPI matches it against the workflow's `environment:`, and a mismatch
fails the upload with a confusing 403 rather than a clear error.

### 2. Create the GitHub environments

At **Settings → Environments**, create `pypi` and `testpypi`.

For `pypi`, consider adding yourself as a **required reviewer**. That turns a tag push into a pause: the
build runs and then waits for you to approve the upload. It is the last point at which a release is still
cancellable, and it costs one click.

Restrict `pypi` to tags via its deployment-branch rule (`v*` as a tag pattern) so the environment's identity
cannot be borrowed by a branch push.

## Cutting a release

**1. Land everything on `main` and update the changelog.**

`CHANGELOG.md` needs a `## [X.Y.Z] — YYYY-MM-DD` section: the workflow extracts it verbatim as the GitHub
release notes. No section means the release still publishes, with a placeholder and a warning.

**2. Bump the version in two places** — they must agree, and the workflow refuses the release if the tag
disagrees with either:

```bash
# pyproject.toml:  version = "X.Y.Z"
# src/pyattacker/__init__.py:  __version__ = "X.Y.Z"
uv sync                     # refresh uv.lock
uv run pytest               # tests/test_packaging.py asserts the two agree
```

**3. Rehearse the commit you are going to tag** (optional — the tag runs the same stage itself, so this is
about finding problems while the version is still yours to change):

Actions → Release → **Run workflow**, with *Publish to TestPyPI* checked. This runs the full verification,
uploads to TestPyPI, and installs the files back from there — the same three things the tag does. Then check
the result yourself:

```bash
uv venv /tmp/verify --python 3.11
uv pip install --python /tmp/verify/bin/python \
  --index-url https://test.pypi.org/simple/ \
  "pyattacker==X.Y.Z"
/tmp/verify/bin/pyattacker --version   # must print X.Y.Z, not the previously released version
/tmp/verify/bin/pyattacker demo --pipelines 20
rm -rf /tmp/verify
```

Every flag here is load-bearing. `--index-url` is what makes the install come from TestPyPI rather than PyPI.
One index is enough because the base package has **no dependencies at all** — PyYAML is the optional `yaml`
extra (see the README) — so nothing has to be fetched from anywhere else. Two cases still need the wider form
`--extra-index-url https://pypi.org/simple/ --index-strategy unsafe-best-match`: rehearsing an older release,
whose metadata still required `pyyaml`, and rehearsing the extra itself (`"pyattacker[yaml]==X.Y.Z"`). TestPyPI's
`pyyaml` is frozen at 3.11, and `--extra-index-url` *outranks* `--index-url` in uv, so as soon as a second index
is in play `pyattacker` itself resolves from PyPI — where the last release lives — instead of from TestPyPI. That
install succeeds and happily reports the *older* version, verifying nothing, which is why the pin, the strategy
flag and the `--version` assertion belong together.

The rehearsal publishes under the real version number, so **commit before rehearsing, not after**: a filename
that any index has seen can never be uploaded again, not even for different content and not even after
deleting it ([PyPI's rule](https://pypi.org/help/#file-name-reuse) applies to TestPyPI too). A fix after a
rehearsal means a new version number, which is exactly what the tag stage would tell you.

**4. Tag and push.** This is the step that publishes:

```bash
git checkout main && git pull --ff-only origin main
git tag -a vX.Y.Z -m "pyattacker X.Y.Z"
git push origin vX.Y.Z
```

The workflow then runs the suite on 3.11 and 3.12, builds and checks the artifacts, publishes them to
TestPyPI, installs them back from TestPyPI and runs the CLI there, publishes to PyPI, and creates the GitHub
release with the changelog section and the files attached. The `pypi` environment's required reviewer is the
pause in front of the one step that cannot be taken back.

**5. Confirm:**

```bash
uv venv /tmp/final --python 3.11
uv pip install --python /tmp/final/bin/python pyattacker==X.Y.Z
/tmp/final/bin/pyattacker --version
/tmp/final/bin/pyattacker demo --pipelines 20
rm -rf /tmp/final
```

## What the workflow checks before it uploads

A tag can point at any commit, including one that never passed CI, so the release re-runs everything rather
than trusting that it did:

| Check | Why it is there |
|---|---|
| full suite on 3.11 and 3.12, lint, CLI and example smoke tests | the tagged commit itself has to be green |
| tag matches `__version__` | a tag that disagrees produces a release nobody can find again |
| `twine check --strict` | the README has to render on PyPI, where relative links do not resolve |
| sdist carries no `.claude/`, `.venv/`, `.pyc` | this caught a leaked local config file in 0.1.0 |
| sdist stays under 1 MiB | `assets/` put 883 KB of logo PNG into a 1.26 MB tarball, which 0.2.0 published |
| sdist alone rebuilds and passes its tests | an sdist that cannot rebuild the package is not a source distribution |
| wheel installs and `pyattacker demo` runs | catches a broken entry point or a missing module |
| the released files install from TestPyPI by name and report `__version__` | the published metadata has to resolve the way a user resolves it, and a rehearsal nobody runs proves nothing |

## If something goes wrong

**A published version cannot be reused.** PyPI lets you delete a release but never re-upload that version
number. If a bad artifact reaches PyPI, yank it and publish a patch release — do not try to replace it.

**The upload failed but the tag is pushed.** Fix the cause, delete and re-push the tag
(`git push --delete origin vX.Y.Z`, then re-tag). Deleting a tag is safe while nothing has been published;
once PyPI has the version, move to a new version number instead.

**A re-run says the files already exist.** That is `--check-url` doing its job: a file already on the index
*with the same hash* is skipped, so a publish that died halfway can simply be re-run — fix the cause and use
`gh run rerun <run-id> --failed`, no new tag needed. The comparison is by hash, not by filename, so this only
holds while the commit is unchanged.

**`Local file and index file do not match`, or `Filename has been previously used`.** TestPyPI holds this
version from different bytes: a rehearsal followed by a further commit rebuilds the sdist — which ships
`docs/` — and changes its hash. Deleting the release does not help, because neither index ever allows a
filename to be reused ([not even after deletion](https://pypi.org/help/#file-name-reuse)). The remedy is a new
version number: bump `pyproject.toml` and `src/pyattacker/__init__.py`, add the changelog section, rehearse
that, and tag it. The alternative is to treat the rehearsed commit as the release and move the tag onto it
(delete and re-push the tag), which is safe while nothing has reached PyPI.

**403 from PyPI.** Almost always the publisher configuration: check that the owner, repository, workflow
filename (`release.yml`) and environment name match exactly what the job declares.

## Manual publishing

Not needed, and worth avoiding — it reintroduces the API token the OIDC setup exists to eliminate. If you
must:

```bash
rm -rf dist && uv build
uvx twine check --strict dist/*
UV_PUBLISH_TOKEN=<token> uv publish dist/*
```
