"""Pure prerequisite/workflow contract checks, not authenticated backend evidence."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("overwatch_ci_prerequisites", ROOT / "scripts/ci_prerequisites.py")
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def authorized_shape():
    return {
        "GITHUB_REPOSITORY": "fixture/Overwatch",
        "OVERWATCH_CI_HEAD_REPOSITORY": "fixture/Overwatch",
        "UV_INDEX_TSCORE_PASSWORD": "synthetic-not-a-real-credential",
    }


@pytest.mark.parametrize("version", [(3, 12, 99), (3, 13, 5), (3, 13, 11), (3, 14, 0), (4, 0, 0)])
def test_unsupported_python_is_not_a_pass(version):
    assert MODULE.blockers(version, authorized_shape(), ci=True) == [
        "unsupported_python_requires_3_13_12_to_before_3_14",
    ]


@pytest.mark.parametrize("version", [(3, 13, 12), (3, 13, 13), (3, 13, 99)])
def test_supported_python_only_establishes_prerequisite_shape(version):
    assert MODULE.blockers(version, authorized_shape(), ci=True) == []


def test_local_preserves_external_keyring_route():
    assert MODULE.blockers((3, 13, 12), {}, ci=False) == []


def test_missing_token_is_blocked():
    env = authorized_shape()
    env.pop("UV_INDEX_TSCORE_PASSWORD")
    assert MODULE.blockers((3, 13, 12), env, ci=True) == ["missing_authorized_tscore_index_token"]


@pytest.mark.parametrize("head", ["", "other/fork"])
def test_untrusted_or_unknown_repo_is_blocked(head):
    env = authorized_shape()
    env["OVERWATCH_CI_HEAD_REPOSITORY"] = head
    reasons = MODULE.blockers((3, 13, 12), env, ci=True)
    assert reasons == ["credentialed_backend_requires_reviewed_same_repository_code"]
    assert env["UV_INDEX_TSCORE_PASSWORD"] not in repr(reasons)


def test_cli_missing_prerequisites_is_nonzero_and_secret_safe():
    import os
    env = dict(os.environ)
    for key in ("GITHUB_REPOSITORY", "OVERWATCH_CI_HEAD_REPOSITORY", "UV_INDEX_TSCORE_PASSWORD"):
        env.pop(key, None)
    result = subprocess.run([sys.executable, str(ROOT / "scripts/ci_prerequisites.py"), "--ci"],
                            env=env, text=True, capture_output=True, timeout=10, check=False)
    assert result.returncode == 2
    assert "missing_authorized_tscore_index_token" in result.stdout
    assert not result.stderr


def workflow():
    return yaml.load((ROOT / ".github/workflows/ci.yml").read_text(), Loader=yaml.BaseLoader)


def test_workflow_never_uses_privileged_pr_trigger_or_suppresses_failure():
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    data = workflow()
    assert set(data["on"]) == {"pull_request", "push", "workflow_dispatch"}
    assert "pull_request_target" not in text
    assert "continue-on-error" not in text
    assert data["permissions"] == {"contents": "read"}
    assert all("if" not in job for job in data["jobs"].values())


def test_credentials_only_reach_preflight_and_locked_install():
    data = workflow()
    backend = data["jobs"]["backend"]
    assert backend["environment"] == "overwatch-ci"
    assert "env" not in backend
    secret_steps = [step for step in backend["steps"] if "secrets." in repr(step)]
    assert len(secret_steps) == 2
    assert {step["run"] for step in secret_steps} == {
        "python scripts/ci_prerequisites.py --ci",
        "uv sync --locked --group dev --python 3.13.12",
    }
    assert "secrets." not in repr(data["jobs"]["frontend"])
    assert "cache" not in repr(data["jobs"])


def test_workflow_pin_and_required_commands():
    import re
    data = workflow()
    steps = [step for job in data["jobs"].values() for step in job["steps"]]
    uses = [step["uses"] for step in steps if "uses" in step]
    assert all(re.fullmatch(r"actions/[\w-]+@[a-f0-9]{40}", ref) for ref in uses)
    runs = [step["run"] for step in steps if "run" in step]
    for command in ["npm ci --ignore-scripts --no-audit --no-fund", "npm run typecheck",
                    "npm test", "npm run build", "uv run --locked --no-sync ruff check src tests scripts",
                    "uv run --locked --no-sync pytest -q -ra --junitxml=backend-junit.xml"]:
        assert command in runs
    python_steps = [step for step in steps if step.get("uses", "").startswith("actions/setup-python@")]
    assert len(python_steps) == 1 and python_steps[0]["with"]["python-version"] == "3.13.12"
    assert data["jobs"]["backend"]["name"] == "backend-lint-and-locked-tests"
    assert data["jobs"]["frontend"]["name"] == "frontend-typecheck-vitest-build"
