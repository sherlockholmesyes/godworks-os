#!/usr/bin/env python3
"""Validate that system-law cargo test gates resolve to real tests.

`system_laws_lint.py` checks that each law names gates and fail-under-broken
expectations. This script binds those named `cargo test ... <filter>` gates to
the current Rust test inventory, so a stale or invented law gate cannot stay
green just because the JSON shape is valid.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


FLAG_TAKES_VALUE = {
    "-p",
    "--package",
    "--test",
    "--bin",
    "--example",
    "--bench",
    "--features",
    "--exclude",
    "--manifest-path",
    "--target",
    "--target-dir",
    "--jobs",
    "--config",
    "--message-format",
}


@dataclass(frozen=True)
class GateCommand:
    law_id: str
    gate_index: int
    command: str
    test_filter: str


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


def split_command(command: str) -> list[str]:
    try:
        return shlex.split(command, posix=os.name != "nt")
    except ValueError as exc:
        raise SystemExit(f"cannot parse command {command!r}: {exc}") from exc


def is_cargo_test(tokens: list[str]) -> bool:
    if len(tokens) < 2:
        return False
    exe = Path(tokens[0]).name.lower()
    return exe in {"cargo", "cargo.exe"} and tokens[1] == "test"


def cargo_test_filter(command: str) -> str | None:
    tokens = split_command(command)
    if not is_cargo_test(tokens):
        return None

    i = 2
    while i < len(tokens):
        token = tokens[i]
        if token == "--":
            break
        if token.startswith("--") and "=" in token:
            i += 1
            continue
        if token in FLAG_TAKES_VALUE:
            i += 2
            continue
        if token.startswith("-"):
            i += 1
            continue
        return token

    raise SystemExit(
        f"cargo test command must name a narrow test filter, got: {command}"
    )


def collect_gate_commands(rows: list[dict[str, Any]]) -> tuple[list[GateCommand], int]:
    gates: list[GateCommand] = []
    skipped = 0
    for row_index, row in enumerate(rows, start=1):
        law_id = row.get("law_id")
        if not isinstance(law_id, str) or not law_id:
            raise SystemExit(f"row {row_index}: law_id must be a non-empty string")
        current_gates = row.get("current_gates")
        if not isinstance(current_gates, list) or not current_gates:
            raise SystemExit(f"{law_id}: current_gates must be a non-empty list")
        for gate_index, gate in enumerate(current_gates, start=1):
            if not isinstance(gate, dict):
                raise SystemExit(f"{law_id}.current_gates[{gate_index}]: gate must be an object")
            command = gate.get("command")
            if not isinstance(command, str) or not command.strip():
                raise SystemExit(f"{law_id}.current_gates[{gate_index}]: command must be non-empty")
            test_filter = cargo_test_filter(command)
            if test_filter is None:
                skipped += 1
                continue
            gates.append(GateCommand(law_id, gate_index, command, test_filter))
    return gates, skipped


def load_test_inventory(path: Path | None) -> list[str]:
    if path is not None:
        with path.open("r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    proc = subprocess.run(
        ["cargo", "test", "--workspace", "--all-targets", "--", "--list"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stdout)
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def validate_gates(gates: list[GateCommand], test_inventory: list[str]) -> None:
    missing: list[str] = []
    for gate in gates:
        if not any(gate.test_filter in test_name for test_name in test_inventory):
            missing.append(
                f"{gate.law_id}.current_gates[{gate.gate_index}] "
                f"filter={gate.test_filter!r} command={gate.command!r}"
            )
    if missing:
        raise SystemExit(
            "system law cargo test gates not found in current test inventory:\n"
            + "\n".join(missing)
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--laws", required=True, type=Path)
    parser.add_argument(
        "--test-list-file",
        type=Path,
        help="Use a saved cargo-test inventory instead of running cargo.",
    )
    args = parser.parse_args()

    rows = load_jsonl(args.laws)
    gates, skipped = collect_gate_commands(rows)
    if not gates:
        raise SystemExit(f"{args.laws}: no cargo test gates found")

    test_inventory = load_test_inventory(args.test_list_file)
    validate_gates(gates, test_inventory)
    print(
        "system_laws_test_inventory ok: "
        f"cargo_gates={len(gates)} skipped_non_cargo={skipped}"
    )


if __name__ == "__main__":
    main()
