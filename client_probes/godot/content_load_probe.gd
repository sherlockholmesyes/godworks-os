extends SceneTree
## Headless Godot client proof for Godworks OS content manifests.
##
## It connects to a live broker through the public length-prefixed JSON wire,
## creates one asset-bearing entity, queries it back, then resolves
## asset_manifest + content_manifest the way a client loader would.

var _tcp := StreamPeerTCP.new()
var _buf := PackedByteArray()

func _env(key: String, fallback: String) -> String:
	var value := OS.get_environment(key)
	return fallback if value == "" else value

func _frame(obj: Dictionary) -> PackedByteArray:
	var body := JSON.stringify(obj).to_utf8_buffer()
	var n := body.size()
	var out := PackedByteArray()
	out.append((n >> 24) & 0xFF)
	out.append((n >> 16) & 0xFF)
	out.append((n >> 8) & 0xFF)
	out.append(n & 0xFF)
	out.append_array(body)
	return out

func _send(obj: Dictionary) -> bool:
	return _tcp.put_data(_frame(obj)) == OK

func _poll_frames() -> Array:
	_tcp.poll()
	var avail := _tcp.get_available_bytes()
	if avail > 0:
		var data := _tcp.get_data(avail)
		if data[0] == OK:
			_buf.append_array(data[1])
	var frames := []
	while _buf.size() >= 4:
		var n := (_buf[0] << 24) | (_buf[1] << 16) | (_buf[2] << 8) | _buf[3]
		if _buf.size() < 4 + n:
			break
		var body := _buf.slice(4, 4 + n)
		_buf = _buf.slice(4 + n)
		var obj = JSON.parse_string(body.get_string_from_utf8())
		if obj is Dictionary:
			frames.append(obj)
	return frames

func _string_set(value) -> Dictionary:
	var out := {}
	if value is Array:
		for item in value:
			if item is String and item != "":
				out[item] = true
	return out

func _nonempty_string(value: Dictionary, keys: Array) -> bool:
	for key in keys:
		if value.has(key) and value[key] is String and value[key] != "":
			return true
	return false

func _map_has_nonempty_string(map_value, key: String) -> bool:
	return map_value is Dictionary and map_value.has(key) and map_value[key] is String and map_value[key] != ""

func _validate_client_load(response: Dictionary, entity_id: String) -> bool:
	var asset_manifest = response.get("asset_manifest", {})
	var content_manifest = response.get("content_manifest", {})
	if not (asset_manifest is Dictionary and content_manifest is Dictionary):
		return false
	var asset_rows = asset_manifest.get("assets", [])
	var entity_assets = asset_manifest.get("entity_assets", {})
	var packages = content_manifest.get("packages", [])
	var entity_packages = content_manifest.get("entity_packages", {})
	if not (asset_rows is Array and entity_assets is Dictionary and packages is Array and entity_packages is Dictionary):
		return false

	var assets_by_id := {}
	for asset in asset_rows:
		if asset is Dictionary and asset.get("id", "") is String and asset.get("id", "") != "":
			assets_by_id[asset["id"]] = asset

	var packages_by_id := {}
	for package in packages:
		if package is Dictionary and package.get("id", "") is String and package.get("id", "") != "":
			packages_by_id[package["id"]] = package

	var required_assets := _string_set(entity_assets.get(entity_id, []))
	var required_packages := _string_set(entity_packages.get(entity_id, []))
	if required_assets.is_empty() or required_packages.is_empty():
		return false

	var loaded_assets := {}
	for package_id in required_packages.keys():
		if not packages_by_id.has(package_id):
			return false
		for asset_id in _string_set(packages_by_id[package_id].get("assets", [])).keys():
			loaded_assets[asset_id] = true

	for asset_id in required_assets.keys():
		if not loaded_assets.has(asset_id) or not assets_by_id.has(asset_id):
			return false
		var asset: Dictionary = assets_by_id[asset_id]
		if not _nonempty_string(asset, ["uri", "path"]) or not _nonempty_string(asset, ["hash", "sha256"]):
			return false
		var package_carries_asset := false
		for package_id in required_packages.keys():
			var package: Dictionary = packages_by_id[package_id]
			if _string_set(package.get("assets", [])).has(asset_id) \
				and _map_has_nonempty_string(package.get("uris", {}), asset_id) \
				and _map_has_nonempty_string(package.get("hashes", {}), asset_id):
				package_carries_asset = true
				break
		if not package_carries_asset:
			return false
	return true

func _init():
	var host := _env("GW_HOST", "127.0.0.1")
	var port := int(_env("GW_PORT", "7777"))
	var entity_id := _env("GW_CONTENT_ENTITY", "godot-content-probe")
	var request_id := "godot-content-query"

	var err := _tcp.connect_to_host(host, port)
	if err != OK:
		print("GODOT CONTENT-LOAD: FAIL connect_error=", err)
		quit(2)
		return

	var connected := false
	var response := {}
	var elapsed := 0
	while elapsed < 6000:
		_tcp.poll()
		if _tcp.get_status() == StreamPeerTCP.STATUS_CONNECTED:
			connected = true
			break
		OS.delay_msec(50)
		elapsed += 50
	if not connected:
		print("GODOT CONTENT-LOAD: FAIL connect_timeout host=", host, " port=", port)
		quit(2)
		return

	_send({"op":"WorkerConnect","worker_id":"godot-content-probe","region":"EARTH","attributes":["observer","physics"],"proto":1})
	_send({"op":"Interest","center":[0.0,0.0],"radius":100.0})
	_send({
		"op":"CreateEntity",
		"request_id":"create-godot-content-probe",
		"entity":entity_id,
		"region":"EARTH",
		"components":{
			"pos":[2.0,0.0],
			"vel":[0.0,0.0],
			"asset":{"id":"mesh/godot-probe","uri":"res://probe/body.glb","kind":"mesh","package":"pkg/godot-probe","hash":"sha256:probe-mesh"},
			"asset_dependencies":[
				{"id":"mat/godot-probe","uri":"res://probe/body.tres","kind":"material","package":"pkg/godot-shared","hash":"sha256:probe-mat"},
				{"id":"tex/godot-probe","uri":"res://probe/body.png","kind":"texture","package":"pkg/godot-probe","hash":"sha256:probe-tex"}
			]
		}
	})
	OS.delay_msec(250)
	_send({"op":"EntityQuery","request_id":request_id,"query":{"type":"entity","entity":entity_id}})

	elapsed = 0
	while elapsed < 4000:
		for obj in _poll_frames():
			if obj.get("op", "") == "EntityQueryResponse" and obj.get("request_id", "") == request_id:
				response = obj
				break
		if not response.is_empty():
			break
		OS.delay_msec(50)
		elapsed += 50

	var count_ok: bool = response.get("count", 0) == 1
	var load_ok: bool = count_ok and _validate_client_load(response, entity_id)
	print("GODOT CONTENT-LOAD | connected=", connected, " count=", response.get("count", -1), " content_load_ok=", 1 if load_ok else 0)
	if load_ok:
		print("GODOT CONTENT-LOAD: PASS -- public EntityQueryResponse resolved to a client package load-set")
		_tcp.disconnect_from_host()
		quit(0)
	else:
		print("GODOT CONTENT-LOAD: FAIL -- response=", JSON.stringify(response))
		_tcp.disconnect_from_host()
		quit(1)
