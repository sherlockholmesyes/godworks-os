extends SceneTree
## Headless Godot contract probe for the shared ClientBridge resync fixture.
##
## This is not the production Godot bridge. It replays
## tests/fixtures/client_bridge/godot-resync-contract.json through the same
## Godot-side adapter that the TCP probe uses, while Rust ClientBridge remains
## the source of truth.

const BridgeAdapter = preload("res://client_bridge_contract_adapter.gd")

var _bridge = BridgeAdapter.new()

func _env(key: String, fallback: String) -> String:
	var value := OS.get_environment(key)
	return fallback if value == "" else value

func _default_fixture_path() -> String:
	return ProjectSettings.globalize_path("res://../../tests/fixtures/client_bridge/godot-resync-contract.json")

func _load_fixture() -> Dictionary:
	var path := _env("GW_CLIENT_BRIDGE_FIXTURE", _default_fixture_path())
	var file := FileAccess.open(path, FileAccess.READ)
	if file == null:
		print("GODOT CLIENT-BRIDGE CONTRACT: FAIL fixture_open path=", path, " err=", FileAccess.get_open_error())
		quit(2)
		return {}
	var parsed = JSON.parse_string(file.get_as_text())
	if not (parsed is Dictionary):
		print("GODOT CLIENT-BRIDGE CONTRACT: FAIL fixture_parse path=", path)
		quit(2)
		return {}
	return parsed

func _replay_step(step: Dictionary) -> void:
	match str(step.get("kind", "")):
		"stream":
			_bridge.apply_stream_op(step.get("op", {}))
		"mark_live":
			_bridge.mark_live()
		"transport_closed":
			_bridge.on_transport_closed()
		"transport_connecting":
			_bridge.on_transport_connecting()
		"begin_full_resync":
			_bridge.begin_full_resync()
		"finish_full_resync":
			_bridge.finish_full_resync(step.get("op", {}))
		_:
			print("GODOT CLIENT-BRIDGE CONTRACT: FAIL unknown_step kind=", step.get("kind", ""))
			quit(2)

func _init():
	var fixture := _load_fixture()
	for step in fixture.get("steps", []):
		if not (step is Dictionary):
			print("GODOT CLIENT-BRIDGE CONTRACT: FAIL bad_step")
			quit(2)
			return
		_replay_step(step)

	var actual := _bridge.snapshot_contract()
	var expected = fixture.get("expected_snapshot", {})
	var mismatch := _bridge.deep_mismatch(actual, expected)
	if mismatch == "":
		print("GODOT CLIENT-BRIDGE CONTRACT: PASS -- fixture=", fixture.get("name", "unknown"), " entities=", actual["entity_count"])
		quit(0)
	else:
		print("GODOT CLIENT-BRIDGE CONTRACT: FAIL mismatch=", mismatch)
		print("actual=", JSON.stringify(actual))
		print("expected=", JSON.stringify(expected))
		quit(1)
