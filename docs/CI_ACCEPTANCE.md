# Locked Overwatch CI and acceptance boundaries

These source changes target Overwatch issue #2 and the existing draft PR #1.
They do not establish successful private authentication, backend tests or the
producer-to-visible-UI acceptance gate. Keep PR #1 draft until its live gates pass.

## Supported runtime and unchanged dependency sources

Use Python `>=3.13.12,<3.14`. The workflow selects `3.13.12`; neither
`pyproject.toml` nor `uv.lock` is changed. `typesafe` and `tspath` remain bound to
the named `tscore` index and the existing `subprocess` keyring configuration.
No public package substitutes, unlocked install or version relaxation is allowed.

For a local checkout, retain the **existing authorized** CodeArtifact/keyring
route. Check that its credential is current without printing it. uv also supports
`UV_INDEX_TSCORE_USERNAME=aws` and `UV_INDEX_TSCORE_PASSWORD` for this exact named
index. Set the password only through the existing secret manager, not a shell
command containing its value, a checked-in dotenv file, a URL or a log.
The preflight deliberately does not dump variables or query a credential store.
A present value is not proof of authorization; only the actual locked install is.

```sh
python scripts/ci_prerequisites.py
uv sync --locked --group dev --python 3.13.12
uv run --locked --no-sync ruff check src tests scripts
uv run --locked --no-sync pytest -q -ra --junitxml=backend-junit.xml
npm ci --ignore-scripts --no-audit --no-fund
npm run typecheck
npm test
npm run build
```

## Hosted credential boundary

A maintainer must configure the `overwatch-ci` GitHub environment with required
reviewers and bind the already authorized, short-lived index credential as
`TSCORE_CODEARTIFACT_TOKEN`. This patch neither obtains that credential nor changes
AWS access policies. Rotate/refresh expired credentials using the existing approved
route. Check the environment and secret configuration before running credentialed
jobs; this source file cannot enforce GitHub environment protection by itself.

The token is scoped only to the prerequisite/install steps, never frontend or
test steps. No `pull_request_target` trigger, OIDC/cloud deployment, cache upload,
secret artifact upload or automatic remote telemetry is added. Review the exact
PR commit before permitting the environment: private package installation executes
trusted dependency/build code. Fork PRs are not given the token and cannot count a
skipped backend as success. The preflight exits 2 on a foreign/missing repository
identity or missing token. Missing/expired access is an environment gate, not a
passing backend test result. Do not promote untrusted fork code merely to obtain
credentials.

Stable job names are `backend-lint-and-locked-tests` and
`frontend-typecheck-vitest-build`. This patch does not configure branch rules or
claim these jobs ran. Before merge, inspect both checks on **the current PR #1 head**.

## Fresh producer and running UI remain separate

The normal suite retains explicit skip reasons. A skipped
`tests/test_kev_laya_pipeline.py` is not a successful Layev pipeline. For issue #3,
provide the separately authorized fresh producer export, isolated registry/cache
paths, unskipped pipeline receipt, the actual report JSON, browser errors and
populated UI screenshot. Do not inject a fixture or use hosted CI green as a
substitute. Run the full suite after supplying that prerequisite on the supported
local environment. Confirm which checkout the running service uses, preserving
unrelated files and existing run identities. Never terminate WSL.

## Source references

- Repository dependency source: `pyproject.toml` at
  `11f32c597f3763137b041e7d867d346e782b3bf9`.
- uv named-index environment authentication:
  https://docs.astral.sh/uv/concepts/indexes/#authentication
- uv existing subprocess keyring route:
  https://docs.astral.sh/uv/concepts/authentication/http/#keyring-providers

Local source/preflight tests are not the locked Overwatch backend suite.
