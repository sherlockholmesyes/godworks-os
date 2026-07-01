#!/usr/bin/env python3
"""Validate the public system-laws gate index.

This is intentionally small and dependency-free. It does not prove the broker
runtime by itself; it proves that each published law row names an executable
gate and the failure it is expected to catch.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_LAWS = {
    "SYS-EXECUTION-001",
    "SYS-CONSISTENCY-001",
    "SYS-TIME-001",
    "SYS-FAILURE-001",
    "SYS-DATA-LIFECYCLE-001",
}

REQUIRED_KEYS = [
    "law_id",
    "priority",
    "title",
    "law_statement",
    "runtime_boundary",
    "visibility_rule",
    "current_gates",
    "fail_under_broken",
    "known_gaps",
    "non_scope",
]

GATE_KEYS = ["kind", "command", "proves"]
FAIL_GATE_KEYS = ["breakage", "gate", "expected_failure"]


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for n, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{n}: invalid JSONL row: {exc}") from exc
            if not isinstance(row, dict):
                raise SystemExit(f"{path}:{n}: row must be a JSON object")
            rows.append(row)
    return rows


def require_keys(obj: dict[str, Any], keys: list[str], where: str) -> None:
    missing = [key for key in keys if key not in obj]
    if missing:
        raise SystemExit(f"{where}: missing required keys: {', '.join(missing)}")


def require_nonempty_string(obj: dict[str, Any], key: str, where: str) -> None:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"{where}: {key} must be a non-empty string")


def require_nonempty_list(obj: dict[str, Any], key: str, where: str) -> list[Any]:
    value = obj.get(key)
    if not isinstance(value, list) or not value:
        raise SystemExit(f"{where}: {key} must be a non-empty list")
    return value


def validate_gate_list(row: dict[str, Any], where: str) -> None:
    gates = require_nonempty_list(row, "current_gates", where)
    for i, gate in enumerate(gates, start=1):
        label = f"{where}.current_gates[{i}]"
        if not isinstance(gate, dict):
            raise SystemExit(f"{label}: gate must be an object")
        require_keys(gate, GATE_KEYS, label)
        for key in GATE_KEYS:
            require_nonempty_string(gate, key, label)
        if gate["kind"] not in {"ci", "unit", "integration", "runtime", "tool"}:
            raise SystemExit(f"{label}: kind must be ci, unit, integration, runtime, or tool")


def validate_fail_gate_list(row: dict[str, Any], where: str) -> None:
    gates = require_nonempty_list(row, "fail_under_broken", where)
    for i, gate in enumerate(gates, start=1):
        label = f"{where}.fail_under_broken[{i}]"
        if not isinstance(gate, dict):
            raise SystemExit(f"{label}: fail gate must be an object")
        require_keys(gate, FAIL_GATE_KEYS, label)
        for key in FAIL_GATE_KEYS:
            require_nonempty_string(gate, key, label)


def validate_rows(rows: list[dict[str, Any]], where: str) -> None:
    seen: set[str] = set()
    for i, row in enumerate(rows, start=1):
        label = f"{where}:{i}"
        require_keys(row, REQUIRED_KEYS, label)
        for key in [
            "law_id",
            "priority",
            "title",
            "law_statement",
            "runtime_boundary",
            "visibility_rule",
        ]:
            require_nonempty_string(row, key, label)
        law_id = row["law_id"]
        if law_id in seen:
            raise SystemExit(f"{label}: duplicate law_id {law_id}")
        seen.add(law_id)
        if row["priority"] not in {"P0", "P1"}:
            raise SystemExit(f"{label}: priority must be P0 or P1")
        validate_gate_list(row, label)
        validate_fail_gate_list(row, label)
        require_nonempty_list(row, "known_gaps", label)
        require_nonempty_list(row, "non_scope", label)

    missing = sorted(REQUIRED_LAWS - seen)
    extra = sorted(seen - REQUIRED_LAWS)
    if missing:
        raise SystemExit(f"{where}: missing required laws: {', '.join(missing)}")
    if extra:
        raise SystemExit(f"{where}: unknown law ids: {', '.join(extra)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--laws", required=True, type=Path)
    args = parser.parse_args()

    rows = load_jsonl(args.laws)
    validate_rows(rows, str(args.laws))
    print(f"system_laws_lint ok: laws={len(rows)}")


if __name__ == "__main__":
    main()

