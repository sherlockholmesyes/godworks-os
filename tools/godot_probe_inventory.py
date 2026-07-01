#!/usr/bin/env python3
"""Validate that Godot probe assets stay wired to the client bridge contract.

This is not a replacement for `scripts/run_godot_probes.ps1`: the live probe
still needs a Godot 4.x binary. This inventory gate is the CI-safe guard that
keeps the fixture, scripts, and docs from drifting while CI does not provision
Godot.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_PROBES = [
    "client_bridge_contract_probe.gd",
    "client_bridge_tcp_resync_probe.gd",
    "cross_broker_handoff_probe.gd",
    "godot_2d_physics_probe.gd",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"missing required Godot probe asset: {path}") from exc


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(read_text(path))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"{path}: fixture root must be a JSON object")
    return value


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> None:
    root = repo_root()
    probe_dir = root / "client_probes" / "godot"
    fixture = root / "tests" / "fixtures" / "client_bridge" / "godot-resync-contract.json"
    runner = root / "scripts" / "run_godot_probes.ps1"
    bootstrap = root / "scripts" / "ensure_godot_4_3.ps1"
    docs = root / "docs" / "sdk" / "client-sdk-alpha.md"
    live_gate_docs = root / "docs" / "ops" / "live-game-reality-gates.md"

    project_text = read_text(probe_dir / "project.godot")
    require("Godworks OS Client Probes" in project_text, "project.godot must name the Godworks probe project")
    require("PackedStringArray(\"4.3\")" in project_text, "project.godot must pin the Godot 4.3 feature rail")

    adapter_text = read_text(probe_dir / "client_bridge_contract_adapter.gd")
    for method in [
        "snapshot_contract",
        "finish_full_resync",
        "apply_stream_op",
        "on_transport_closed",
    ]:
        require(method in adapter_text, f"Godot bridge adapter must expose {method}")

    runner_text = read_text(runner)
    bootstrap_text = read_text(bootstrap)
    require("4.3-stable" in bootstrap_text, "Godot bootstrap must pin the 4.3 stable release")
    require("8F2C75B734BD956027AE3CA92C41F78B5D5A255DACC0F20E4E3C523C545AD410" in bootstrap_text,
            "Godot bootstrap must pin the portable zip SHA256")
    for script in REQUIRED_PROBES:
        read_text(probe_dir / script)
        require(script in runner_text, f"run_godot_probes.ps1 must invoke {script}")
    require("godworks_broker.exe" in runner_text, "Godot runner must start the real broker binary")
    require("GW_AUTH_CLAIMS" in runner_text, "Godot runner must exercise token-bound broker claims")

    fixture_json = load_json(fixture)
    require(
        fixture_json.get("name") == "godot_bridge_resync_contract_v1",
        "Godot bridge fixture name drifted",
    )
    steps = fixture_json.get("steps")
    require(isinstance(steps, list) and len(steps) >= 8, "Godot bridge fixture needs a non-trivial step list")
    step_kinds = {str(step.get("kind", "")) for step in steps if isinstance(step, dict)}
    for kind in ["stream", "transport_closed", "begin_full_resync", "finish_full_resync"]:
        require(kind in step_kinds, f"Godot bridge fixture must include {kind}")
    expected = fixture_json.get("expected_snapshot")
    require(isinstance(expected, dict), "Godot bridge fixture must include expected_snapshot")
    require(expected.get("phase") == "Live", "Godot bridge expected snapshot must end Live")
    require(expected.get("entity_count") == 2, "Godot bridge expected snapshot entity_count drifted")

    sdk_docs = read_text(docs)
    require("client_bridge_contract_probe.gd" in sdk_docs, "client SDK docs must reference the Godot fixture probe")
    require("client_bridge_tcp_resync_probe.gd" in sdk_docs, "client SDK docs must reference the TCP resync probe")
    require("ensure_godot_4_3.ps1" in sdk_docs, "client SDK docs must reference the portable Godot bootstrap")

    live_docs = read_text(live_gate_docs)
    require("scripts\\run_godot_probes.ps1" in live_docs or "scripts/run_godot_probes.ps1" in live_docs,
            "live-game docs must publish the Godot probe runner")
    require("ensure_godot_4_3.ps1" in live_docs, "live-game docs must publish the portable Godot bootstrap")

    print(
        "godot_probe_inventory ok: "
        f"probes={len(REQUIRED_PROBES)} fixture_steps={len(steps)} "
        f"expected_entities={expected.get('entity_count')}"
    )


if __name__ == "__main__":
    main()
