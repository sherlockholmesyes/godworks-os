extends RefCounted
## Minimal Godot-side adapter for the shared ClientBridge snapshot contract.
##
## This is a probe helper, not the production bridge. The purpose is to keep the
## fixture runner and the real TCP probe on one Godot-side contract surface while
## Rust ClientBridge remains the source of truth.

var entities := {}
var rejections := []
var critical_depth := 0
var phase := "Disconnected"

func clear_stream_state() -> void:
	entities.clear()
	rejections.clear()
	critical_depth = 0

func on_transport_closed() -> void:
	clear_stream_state()
	phase = "Disconnected"

func on_transport_connecting() -> void:
	phase = "Connecting"

func begin_full_resync() -> void:
	clear_stream_state()
	phase = "Resyncing"

func mark_live() -> void:
	phase = "Live"

func _ensure_row(entity: String) -> Dictionary:
	if not entities.has(entity):
		entities[entity] = {
			"components": {},
			"authority": {},
			"ghost": false,
			"owner_region": null,
		}
	return entities[entity]

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

func apply_stream_op(op: Dictionary) -> void:
	match str(op.get("op", "")):
		"AddEntity":
			var entity := str(op.get("entity", ""))
			if entity == "":
				return
			_merge_components(_ensure_row(entity), op.get("components", {}))
		"RemoveEntity":
			entities.erase(str(op.get("entity", "")))
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
			rejections.append({
				"entity": op.get("entity", null),
				"component": op.get("comp", op.get("component", null)),
				"reason": str(op.get("reason", "")),
			})
		"CriticalSection":
			var section_phase := str(op.get("phase", "")).to_lower()
			if section_phase in ["begin", "start", "open", "prepare"]:
				critical_depth += 1
			elif section_phase in ["end", "finish", "close", "commit"]:
				critical_depth = max(0, critical_depth - 1)
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
			entities.erase(str(op.get("entity", "")))
		_:
			pass

func finish_full_resync(op: Dictionary) -> void:
	clear_stream_state()
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
	phase = "Live"

func sorted_keys(dict: Dictionary) -> Array:
	var keys := []
	for key in dict.keys():
		keys.append(str(key))
	keys.sort()
	return keys

func row_contract(entity: String, row: Dictionary) -> Dictionary:
	var authority := {}
	for component in sorted_keys(row["authority"]):
		authority[component] = row["authority"][component]
	return {
		"entity": entity,
		"ghost": row["ghost"],
		"owner_region": row["owner_region"],
		"position2": row["components"].get("pos", null),
		"component_keys": sorted_keys(row["components"]),
		"authority": authority,
	}

func snapshot_contract() -> Dictionary:
	var rows := []
	for entity in sorted_keys(entities):
		rows.append(row_contract(entity, entities[entity]))
	return {
		"phase": phase,
		"critical_depth": critical_depth,
		"entity_count": entities.size(),
		"rejection_count": rejections.size(),
		"entities": rows,
	}

func has_entity(entity: String) -> bool:
	return entities.has(entity)

func entity_position2(entity: String):
	if not entities.has(entity):
		return null
	return entities[entity]["components"].get("pos", null)

func deep_mismatch(actual, expected, path: String = "$") -> String:
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
			var mismatch := deep_mismatch(actual[key], expected[key], path + "." + str(key))
			if mismatch != "":
				return mismatch
		return ""
	if actual is Array:
		if actual.size() != expected.size():
			return path + " size"
		for i in range(actual.size()):
			var mismatch := deep_mismatch(actual[i], expected[i], path + "[" + str(i) + "]")
			if mismatch != "":
				return mismatch
		return ""
	return "" if actual == expected else path
