extends SceneTree
## Headless Godot contract probe for the shared ClientBridge resync fixture.
##
## This is not the production Godot bridge and not a second reusable cache. It is
## a small engine-side runner for tests/fixtures/client_bridge/godot-resync-contract.json
## so future Godot binding work has to match the Rust ClientBridge contract first.

var _entities := {}
var _rejections := []
var _critical_depth := 0
var _phase := "Disconnected"

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

func _clear_stream_state() -> void:
	_entities.clear()
	_rejections.clear()
	_critical_depth = 0

func _ensure_row(entity: String) -> Dictionary:
	if not _entities.has(entity):
		_entities[entity] = {
			"components": {},
			"authority": {},
			"ghost": false,
			"owner_region": null,
		}
	return _entities[entity]

func _merge_components(row: Dictionary, components) -> void:
	if not (components is Dictionary):
		return
	for key in components.keys():
		row["components"][str(key)] = components[key]
	_refresh_metadata(row)

func _refresh_metadata(row: Dictionary) -> void:
	if row["components"].has("ghost"):
		row["ghost"] = bool(row["components"]["ghost"])
	if row["components"].has("owner_region"):
		row["owner_region"] = row["components"]["owner_region"]

func _apply_stream_op(op: Dictionary) -> void:
	match str(op.get("op", "")):
		"AddEntity":
			var entity := str(op.get("entity", ""))
			if entity == "":
				return
			_merge_components(_ensure_row(entity), op.get("components", {}))
		"RemoveEntity":
			_entities.erase(str(op.get("entity", "")))
		"AuthorityChange":
			var entity := str(op.get("entity", ""))
			var component := str(op.get("comp", op.get("component", "")))
			if entity == "" or component == "":
				return
			var row := _ensure_row(entity)
			row["authority"][component] = {
				"authoritative": bool(op.get("authoritative", false)),
				"authority_epoch": int(op.get("authority_epoch", op.get("epoch", 0))),
				"mode": str(op.get("mode", "")),
			}
		"ComponentUpdate":
			var entity := str(op.get("entity", ""))
			var component := str(op.get("comp", op.get("component", "")))
			if entity == "" or component == "":
				return
			var row := _ensure_row(entity)
			row["components"][component] = op.get("value", null)
			_refresh_metadata(row)
		"UpdateRejected":
			_rejections.append({
				"entity": op.get("entity", null),
				"component": op.get("comp", op.get("component", null)),
				"reason": str(op.get("reason", "")),
			})
		"CriticalSection":
			var phase := str(op.get("phase", "")).to_lower()
			if phase in ["begin", "start", "open", "prepare"]:
				_critical_depth += 1
			elif phase in ["end", "finish", "close", "commit"]:
				_critical_depth = max(0, _critical_depth - 1)
		"MeshGhost":
			var entity := str(op.get("entity", ""))
			if entity == "":
				return
			var row := _ensure_row(entity)
			_merge_components(row, op.get("components", {}))
			for component in ["pos", "vel"]:
				if op.has(component):
					row["components"][component] = op[component]
			row["ghost"] = true
			row["components"]["ghost"] = true
			row["owner_region"] = op.get("owner_region", null)
			if row["owner_region"] != null:
				row["components"]["owner_region"] = row["owner_region"]
		"MeshGhostRemove":
			_entities.erase(str(op.get("entity", "")))
		_:
			pass

func _finish_full_resync(op: Dictionary) -> void:
	_clear_stream_state()
	for raw_row in op.get("entities", []):
		if not (raw_row is Dictionary):
			continue
		var entity := str(raw_row.get("entity", ""))
		if entity == "":
			continue
		var row := _ensure_row(entity)
		_merge_components(row, raw_row.get("components", {}))
		for component in ["pos", "vel", "region", "ghost", "owner_region"]:
			if raw_row.has(component):
				row["components"][component] = raw_row[component]
		var authority = raw_row.get("authority", {})
		if authority is Dictionary:
			for component in authority.keys():
				var spec = authority[component]
				if not (spec is Dictionary):
					continue
				row["authority"][str(component)] = {
					"authoritative": true,
					"authority_epoch": int(spec.get("authority_epoch", spec.get("epoch", 0))),
					"mode": str(spec.get("mode", "")),
				}
		_refresh_metadata(row)
	_phase = "Live"

func _replay_step(step: Dictionary) -> void:
	match str(step.get("kind", "")):
		"stream":
			_apply_stream_op(step.get("op", {}))
		"mark_live":
			_phase = "Live"
		"transport_closed":
			_clear_stream_state()
			_phase = "Disconnected"
		"transport_connecting":
			_phase = "Connecting"
		"begin_full_resync":
			_clear_stream_state()
			_phase = "Resyncing"
		"finish_full_resync":
			_finish_full_resync(step.get("op", {}))
		_:
			print("GODOT CLIENT-BRIDGE CONTRACT: FAIL unknown_step kind=", step.get("kind", ""))
			quit(2)

func _sorted_keys(dict: Dictionary) -> Array:
	var keys := []
	for key in dict.keys():
		keys.append(str(key))
	keys.sort()
	return keys

func _row_contract(entity: String, row: Dictionary) -> Dictionary:
	var authority := {}
	for component in _sorted_keys(row["authority"]):
		authority[component] = row["authority"][component]
	return {
		"entity": entity,
		"ghost": row["ghost"],
		"owner_region": row["owner_region"],
		"position2": row["components"].get("pos", null),
		"component_keys": _sorted_keys(row["components"]),
		"authority": authority,
	}

func _snapshot_contract() -> Dictionary:
	var rows := []
	for entity in _sorted_keys(_entities):
		rows.append(_row_contract(entity, _entities[entity]))
	return {
		"phase": _phase,
		"critical_depth": _critical_depth,
		"entity_count": _entities.size(),
		"rejection_count": _rejections.size(),
		"entities": rows,
	}

func _deep_mismatch(actual, expected, path: String = "$") -> String:
	var actual_type := typeof(actual)
	var expected_type := typeof(expected)
	var numeric := actual_type in [TYPE_INT, TYPE_FLOAT] and expected_type in [TYPE_INT, TYPE_FLOAT]
	if numeric:
		return "" if abs(float(actual) - float(expected)) < 0.000001 else path
	if actual_type != expected_type:
		return path + " type"
	if actual is Dictionary:
		if actual.size() != expected.size():
			return path + " size"
		for key in expected.keys():
			if not actual.has(key):
				return path + "." + str(key) + " missing"
			var mismatch := _deep_mismatch(actual[key], expected[key], path + "." + str(key))
			if mismatch != "":
				return mismatch
		return ""
	if actual is Array:
		if actual.size() != expected.size():
			return path + " size"
		for i in range(actual.size()):
			var mismatch := _deep_mismatch(actual[i], expected[i], path + "[" + str(i) + "]")
			if mismatch != "":
				return mismatch
		return ""
	return "" if actual == expected else path

func _init():
	var fixture := _load_fixture()
	for step in fixture.get("steps", []):
		if not (step is Dictionary):
			print("GODOT CLIENT-BRIDGE CONTRACT: FAIL bad_step")
			quit(2)
			return
		_replay_step(step)

	var actual := _snapshot_contract()
	var expected = fixture.get("expected_snapshot", {})
	var mismatch := _deep_mismatch(actual, expected)
	if mismatch == "":
		print("GODOT CLIENT-BRIDGE CONTRACT: PASS -- fixture=", fixture.get("name", "unknown"), " entities=", actual["entity_count"])
		quit(0)
	else:
		print("GODOT CLIENT-BRIDGE CONTRACT: FAIL mismatch=", mismatch)
		print("actual=", JSON.stringify(actual))
		print("expected=", JSON.stringify(expected))
		quit(1)
