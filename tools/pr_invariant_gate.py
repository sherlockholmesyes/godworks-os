#!/usr/bin/env python3
"""Validate the PR body invariant gate.

On pull_request events this reads the GitHub event payload and checks that the
PR body names the invariant, the strengthened property, non-scope, a
fail-under-broken gate, and validation. On push/manual local runs without a PR
event it exits successfully so the same workflow can run on `main`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any


REQUIRED_SECTIONS = [
    "summary",
    "invariant touched",
    "invariant strengthened",
    "explicit non-scope",
    "fail-under-broken gate",
    "validation",
]


def section_pattern(title: str) -> re.Pattern[str]:
    return re.compile(rf"(?im)^##+\s+{re.escape(title)}\s*$")


def read_pr_body_from_event(path: Path) -> str | None:
    with path.open("r", encoding="utf-8") as f:
        event: dict[str, Any] = json.load(f)
    pr = event.get("pull_request")
    if not isinstance(pr, dict):
        return None
    body = pr.get("body")
    return body if isinstance(body, str) else ""


def validate_body(body: str, where: str) -> None:
    if not body.strip():
        raise SystemExit(f"{where}: PR body is empty")

    missing = [title for title in REQUIRED_SECTIONS if not section_pattern(title).search(body)]
    if missing:
        raise SystemExit(f"{where}: missing required PR sections: {', '.join(missing)}")

    lowered = body.lower()
    if "fail-under-broken" in lowered and "none" in lowered.split("fail-under-broken", 1)[1][:120]:
        raise SystemExit(f"{where}: fail-under-broken gate must not be 'none'")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--body-file", type=Path)
    parser.add_argument("--event-path", type=Path)
    args = parser.parse_args()

    if args.body_file:
        body = args.body_file.read_text(encoding="utf-8")
        validate_body(body, str(args.body_file))
        print("pr_invariant_gate ok: body-file")
        return

    event_path = args.event_path or (
        Path(os.environ["GITHUB_EVENT_PATH"]) if os.environ.get("GITHUB_EVENT_PATH") else None
    )
    if event_path is None:
        print("pr_invariant_gate skipped: no pull_request event")
        return

    body = read_pr_body_from_event(event_path)
    if body is None:
        print("pr_invariant_gate skipped: event is not a pull_request")
        return

    validate_body(body, str(event_path))
    print("pr_invariant_gate ok: pull_request body")


if __name__ == "__main__":
    main()

