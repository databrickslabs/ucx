# Local Development — Dependency Resolution via Internal Proxy

## Background

After the supply chain lockdown (`#4739`, Apr 2026), all CI package downloads
were routed through the internal JFrog Artifactory mirror at
`databricks.jfrog.io/artifactory/api/pypi/db-pypi/simple`.

In CI this is handled automatically by the `.github/actions/jfrog-auth` action,
which sets `UV_INDEX_URL` and authenticates via GitHub OIDC. That mechanism
only works inside GitHub Actions runners.

As a result, running `make dev` or `make test` locally stopped working for
anyone whose machine could not reach PyPI directly (or whose `uv.lock` was
generated against the JFrog mirror). The workaround at the time was to obtain
a personal API key from `databricks.jfrog.io` and set it manually — this
guidance is now outdated.

## Current state (internal proxy available)

An internal proxy is now available that routes PyPI requests through JFrog
without requiring individual authentication. The `uv` toolchain can be pointed
at the proxy so local development works the same way as CI, with no personal
API keys needed.

## Fix required

The following changes need to be made:

### 1. `pyproject.toml` — set the proxy as the default uv index

```toml
[tool.uv]
required-version = "~= 0.11.0"
index-url = "https://<internal-proxy-url>/pypi/simple"
```

Replace `<internal-proxy-url>` with the actual internal proxy address.
This means `uv sync` will use the proxy on every developer machine without
any extra environment variables.

### 2. `Makefile` — no change needed

`make dev` calls `uv sync --all-extras`, which will automatically pick up
the `index-url` from `pyproject.toml` once the above is set.

### 3. `docs/ucx/docs/dev/contributing.mdx` — add a note

Add a section under "Setup" explaining that:
- Dependencies are resolved via the internal proxy
- No JFrog API key is needed
- If the proxy is not reachable (e.g. off-VPN), `uv sync` will fall back to
  PyPI — if that also fails, connect to VPN first

### 4. Re-generate `uv.lock` (if needed)

If the current `uv.lock` was generated against JFrog and contains hashes that
do not match what the proxy serves, regenerate it:

```bash
uv lock
```

Then commit the updated `uv.lock`.

## Where the old guidance came from

The `jfrog-auth` action (`jfrog-auth/action.yml:174`) sets:

```yaml
- name: Configure uv for JFrog
  env:
    UV_INDEX_URL: 'https://databricks.jfrog.io/artifactory/api/pypi/db-pypi/simple'
  run: |
    printf '%s=%s\n' 'UV_INDEX_URL' "${UV_INDEX_URL}" >> "${GITHUB_ENV}"
    printf '%s=%s\n' 'UV_FROZEN' '1' >> "${GITHUB_ENV}"
```

This only runs in GitHub Actions. For local use I suggested replicating it by
exporting `UV_INDEX_URL` with a personal JFrog token — that is the workaround
that should be replaced by the `pyproject.toml` change above.

## Relevant commits and PRs

| Ref | What |
|---|---|
| `14b54d88` / #4739 | Supply chain lockdown — introduced JFrog routing, switched hatch → uv |
| `45a9a372` / #4804 | Refactored pip/JFrog credentials to use `.netrc` instead of URL-embedded tokens |
| This doc | Records the gap and the fix needed for local dev |
