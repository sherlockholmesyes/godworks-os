#!/usr/bin/env python3
"""Validate the public agent-reality-gate scaffold.

This intentionally avoids external dependencies. It is not a full JSON Schema
implementation; it checks the small contract used by docs/ops.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


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


def load_law_ids(path: Path | None) -> set[str]:
    if path is None:
        return set()
    rows = load_jsonl(path)
    ids: set[str] = set()
    for i, row in enumerate(rows, start=1):
        law_id = row.get("law_id")
        if not isinstance(law_id, str) or not law_id:
            raise SystemExit(f"{path}:{i}: law_id must be a non-empty string")
        if law_id in ids:
            raise SystemExit(f"{path}:{i}: duplicate law_id {law_id}")
        ids.add(law_id)
    return ids


def require_keys(obj: dict[str, Any], keys: list[str], where: str) -> None:
    missing = [key for key in keys if key not in obj]
    if missing:
        raise SystemExit(f"{where}: missing required keys: {', '.join(missing)}")


def require_nonempty_list(obj: dict[str, Any], key: str, where: str) -> None:
    value = obj.get(key)
    if not isinstance(value, list) or not value:
        raise SystemExit(f"{where}: {key} must be a non-empty list")


def require_known_laws(obj: dict[str, Any], valid_laws: set[str], where: str) -> None:
    require_nonempty_list(obj, "system_laws", where)
    laws = obj["system_laws"]
    unknown: list[str] = []
    for law in laws:
        if not isinstance(law, str) or not law:
            raise SystemExit(f"{where}: system_laws entries must be non-empty strings")
        if valid_laws and law not in valid_laws:
            unknown.append(law)
    if unknown:
        raise SystemExit(f"{where}: unknown system_laws: {', '.join(unknown)}")


def validate_eval_rows(rows: list[dict[str, Any]], valid_laws: set[str], where: str) -> None:
    seen: set[str] = set()
    required = [
        "id",
        "kind",
        "input_summary",
        "system_laws",
        "expected_findings",
        "required_gates",
        "must_not_claim",
    ]
    for i, row in enumerate(rows, start=1):
        label = f"{where}:{i}"
        require_keys(row, required, label)
        row_id = row["id"]
        if not isinstance(row_id, str) or not row_id:
            raise SystemExit(f"{label}: id must be a non-empty string")
        if row_id in seen:
            raise SystemExit(f"{label}: duplicate id {row_id}")
        seen.add(row_id)
        require_known_laws(row, valid_laws, label)
        require_nonempty_list(row, "required_gates", label)
        if not isinstance(row.get("must_not_claim"), list):
            raise SystemExit(f"{label}: must_not_claim must be a list")


def validate_trace(schema: dict[str, Any], trace: dict[str, Any], valid_laws: set[str], where: str) -> None:
    required = schema.get("required")
    if not isinstance(required, list):
        raise SystemExit("schema: required must be a list")
    require_keys(trace, [str(key) for key in required], where)

    if trace.get("trace_version") != schema.get("version"):
        raise SystemExit(
            f"{where}: trace_version {trace.get('trace_version')} does not match "
            f"schema version {schema.get('version')}"
        )

    if trace.get("decision") not in {"accept", "revise", "reject", "park"}:
        raise SystemExit(f"{where}: decision must be accept, revise, reject, or park")

    require_known_laws(trace, valid_laws, where)

    inputs = trace.get("inputs")
    if not isinstance(inputs, dict):
        raise SystemExit(f"{where}: inputs must be an object")
    require_nonempty_list(inputs, "files", f"{where}.inputs")

    actions = trace.get("actions")
    if not isinstance(actions, list) or not actions:
        raise SystemExit(f"{where}: actions must be a non-empty list")
    for i, action in enumerate(actions, start=1):
        if not isinstance(action, dict):
            raise SystemExit(f"{where}.actions[{i}]: action must be an object")
        require_keys(action, ["type", "summary"], f"{where}.actions[{i}]")

    verification = trace.get("verification")
    if not isinstance(verification, dict):
        raise SystemExit(f"{where}: verification must be an object")
    commands = verification.get("commands")
    runtime_gates = verification.get("runtime_gates")
    if not commands and not runtime_gates:
        raise SystemExit(
            f"{where}: verification must include at least one command or runtime gate"
        )

    if trace.get("promotion_candidate") is True and trace.get("decision") != "accept":
        raise SystemExit(f"{where}: promotion_candidate requires decision=accept")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--laws", type=Path)
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--evals", required=True, type=Path)
    parser.add_argument("--trace", action="append", default=[], type=Path)
    args = parser.parse_args()

    schema = load_json(args.schema)
    if not isinstance(schema, dict):
        raise SystemExit("schema must be a JSON object")

    valid_laws = load_law_ids(args.laws)

    eval_rows = load_jsonl(args.evals)
    validate_eval_rows(eval_rows, valid_laws, str(args.evals))

    for trace_path in args.trace:
        trace = load_json(trace_path)
        if not isinstance(trace, dict):
            raise SystemExit(f"{trace_path}: trace must be a JSON object")
        validate_trace(schema, trace, valid_laws, str(trace_path))

    print(
        f"agent_reality_gate_lint ok: eval_cases={len(eval_rows)} "
        f"traces={len(args.trace)}"
    )


if __name__ == "__main__":
    main()
