"""Secret-safe CI prerequisite checks. No credential lookup or network requests."""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping


def blockers(version: tuple[int, ...], environ: Mapping[str, str], *, ci: bool) -> list[str]:
    """Return public diagnostic codes only; never include environment values."""
    result = []
    if not (version[:2] == (3, 13) and version[2] >= 12):
        result.append("unsupported_python_requires_3_13_12_to_before_3_14")
    if ci:
        repository = environ.get("GITHUB_REPOSITORY", "")
        head = environ.get("OVERWATCH_CI_HEAD_REPOSITORY", "")
        if not repository or not head or head != repository:
            result.append("credentialed_backend_requires_reviewed_same_repository_code")
        if not environ.get("UV_INDEX_TSCORE_PASSWORD", ""):
            result.append("missing_authorized_tscore_index_token")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci", action="store_true")
    args = parser.parse_args()
    reasons = blockers(tuple(sys.version_info[:3]), os.environ, ci=args.ci)
    for reason in reasons:
        print("BLOCKED: " + reason)
    if reasons:
        return 2
    print("Prerequisite shape checks passed; authentication and locked install remain unverified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
