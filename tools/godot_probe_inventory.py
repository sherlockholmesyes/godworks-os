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
    "godot_3d_contract_probe.gd",
    "client_bridge_tcp_resync_probe.gd",
    "cross_broker_handoff_probe.gd",
    "godot_2d_physics_probe.gd",
]

REQUIRED_3D_COMPONENTS = {
    "core.pos3",
    "core.vel3",
    "core.rot3",
    "core.lin3",
    "core.ang3",
    "core.physics_body",
}


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


def validate_3d_fixture(path: Path) -> dict[str, Any]:
    fixture = load_json(path)
    require(fixture.get("name") == "godot_3d_contract_v1", "Godot 3D fixture name drifted")
    schema = fixture.get("spatial_schema")
    require(isinstance(schema, dict), "Godot 3D fixture must include spatial_schema")
    require(schema.get("spatial_dim") == "D3", "Godot 3D fixture must pin D3")
    require(
        schema.get("coordinate_codec") == "debug_f64_3",
        "Godot 3D fixture must pin debug_f64_3",
    )
    partition = schema.get("partition_schema")
    require(isinstance(partition, dict), "Godot 3D fixture must include partition_schema")
    require(partition.get("kind") == "grid3d", "Godot 3D fixture must pin grid3d")
    for key in ["cols", "rows", "layers"]:
        require(
            isinstance(partition.get(key), int) and partition[key] > 0,
            f"Godot 3D fixture partition_schema.{key} must be positive",
        )
    require(
        fixture.get("component_registry_version") == 1,
        "Godot 3D fixture must pin component_registry_version 1",
    )
    physics_components = fixture.get("physics_island_components")
    require(
        isinstance(physics_components, list),
        "Godot 3D fixture must include physics_island_components",
    )
    require(
        REQUIRED_3D_COMPONENTS.issubset(set(map(str, physics_components))),
        "Godot 3D fixture must include all required 3D physics-island components",
    )
    entities = fixture.get("entities")
    require(isinstance(entities, list) and entities, "Godot 3D fixture must include entities")
    expected_scene = fixture.get("expected_scene")
    require(isinstance(expected_scene, dict), "Godot 3D fixture must include expected_scene")
    require(expected_scene.get("node_count") == len(entities), "Godot 3D fixture node_count drifted")
    for entity in entities:
        require(isinstance(entity, dict), "Godot 3D fixture entity must be object")
        components = entity.get("components")
        require(isinstance(components, dict), "Godot 3D fixture entity components must be object")
        require(
            REQUIRED_3D_COMPONENTS.issubset(set(map(str, components.keys()))),
            "Godot 3D fixture entity must include all required 3D components",
        )
    return fixture


def main() -> None:
    root = repo_root()
    probe_dir = root / "client_probes" / "godot"
    fixture = root / "tests" / "fixtures" / "client_bridge" / "godot-resync-contract.json"
    fixture_3d = root / "tests" / "fixtures" / "client_bridge" / "godot-3d-contract.json"
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
    require("GW_GODOT_3D_FIXTURE" in runner_text, "Godot runner must pass the 3D fixture path")

    fixture_json = load_json(fixture)
    fixture_3d_json = validate_3d_fixture(fixture_3d)
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
    require("godot_3d_contract_probe.gd" in sdk_docs, "client SDK docs must reference the Godot 3D contract probe")
    require("client_bridge_tcp_resync_probe.gd" in sdk_docs, "client SDK docs must reference the TCP resync probe")
    require("ensure_godot_4_3.ps1" in sdk_docs, "client SDK docs must reference the portable Godot bootstrap")

    live_docs = read_text(live_gate_docs)
    require("scripts\\run_godot_probes.ps1" in live_docs or "scripts/run_godot_probes.ps1" in live_docs,
            "live-game docs must publish the Godot probe runner")
    require("ensure_godot_4_3.ps1" in live_docs, "live-game docs must publish the portable Godot bootstrap")
    require("godot_3d_contract_probe.gd" in live_docs, "live-game docs must publish the Godot 3D contract gate")

    print(
        "godot_probe_inventory ok: "
        f"probes={len(REQUIRED_PROBES)} fixture_steps={len(steps)} "
        f"expected_entities={expected.get('entity_count')} "
        f"fixture_3d={fixture_3d_json.get('name')}"
    )


if __name__ == "__main__":
    main()
