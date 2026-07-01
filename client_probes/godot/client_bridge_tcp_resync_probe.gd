extends SceneTree
## Headless Godot proof that a real broker socket checkout drives the bridge
## resync contract.
##
## This is narrower than the cross-broker probe: it does not prove authority
## transfer. It proves a real Godot runtime can reconnect, issue a full
## EntityQuery checkout, and rebuild the bridge snapshot from broker output.

const BridgeAdapter = preload("res://client_bridge_contract_adapter.gd")

var _bridge = BridgeAdapter.new()
var _owner_tcp := StreamPeerTCP.new()
var _viewer_tcp := StreamPeerTCP.new()
var _owner_buf := PackedByteArray()
var _viewer_buf := PackedByteArray()

var _created := {}
var _deleted_stale := false
var _resync_response_seen := false
var _query_error := ""

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

func _send_owner(obj: Dictionary) -> bool:
	return _owner_tcp.put_data(_frame(obj)) == OK

func _send_viewer(obj: Dictionary) -> bool:
	return _viewer_tcp.put_data(_frame(obj)) == OK

func _poll_frames(tcp: StreamPeerTCP, buf: PackedByteArray) -> Array:
	tcp.poll()
	if tcp.get_status() != StreamPeerTCP.STATUS_CONNECTED:
		return [[], buf]
	var avail := tcp.get_available_bytes()
	if avail > 0:
		var data := tcp.get_data(avail)
		if data[0] == OK:
			buf.append_array(data[1])
	var frames := []
	while buf.size() >= 4:
		var n := (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3]
		if buf.size() < 4 + n:
			break
		var body := buf.slice(4, 4 + n)
		buf = buf.slice(4 + n)
		var obj = JSON.parse_string(body.get_string_from_utf8())
		if obj is Dictionary:
			frames.append(obj)
	return [frames, buf]

func _poll_owner() -> Array:
	var result := _poll_frames(_owner_tcp, _owner_buf)
	_owner_buf = result[1]
	return result[0]

func _poll_viewer() -> Array:
	var result := _poll_frames(_viewer_tcp, _viewer_buf)
	_viewer_buf = result[1]
	return result[0]

func _connect_client(tcp: StreamPeerTCP, host: String, port: int, label: String) -> bool:
	var err := tcp.connect_to_host(host, port)
	if err != OK:
		print("GODOT CLIENT-BRIDGE TCP: FAIL connect_error label=", label, " err=", err)
		return false
	var elapsed := 0
	while elapsed < 6000:
		tcp.poll()
		if tcp.get_status() == StreamPeerTCP.STATUS_CONNECTED:
			return true
		OS.delay_msec(50)
		elapsed += 50
	print("GODOT CLIENT-BRIDGE TCP: FAIL connect_timeout label=", label, " host=", host, " port=", port)
	return false

func _handle_owner_frame(obj: Dictionary, stale_id: String, fresh_id: String) -> void:
	var op := str(obj.get("op", ""))
	if op == "CreateEntityResponse" and obj.get("success", false):
		_created[str(obj.get("entity", ""))] = true
	elif op == "DeleteEntityResponse" and obj.get("entity", "") == stale_id and obj.get("success", false):
		_deleted_stale = true

func _handle_viewer_frame(obj: Dictionary, query_id: String) -> void:
	var op := str(obj.get("op", ""))
	if op == "EntityQueryResponse" and obj.get("request_id", "") == query_id:
		_bridge.finish_full_resync(obj)
		_resync_response_seen = true
	else:
		_bridge.apply_stream_op(obj)

func _pump_for(ms: int, query_id: String, stale_id: String, fresh_id: String) -> void:
	var elapsed := 0
	while elapsed < ms:
		for obj in _poll_owner():
			_handle_owner_frame(obj, stale_id, fresh_id)
		for obj in _poll_viewer():
			_handle_viewer_frame(obj, query_id)
		OS.delay_msec(25)
		elapsed += 25

func _wait_until(ms: int, query_id: String, stale_id: String, fresh_id: String, predicate: Callable) -> bool:
	var elapsed := 0
	while elapsed < ms:
		_pump_for(100, query_id, stale_id, fresh_id)
		elapsed += 100
		if predicate.call():
			return true
	return predicate.call()

func _connect_owner(host: String, port: int, token: String) -> bool:
	if not _connect_client(_owner_tcp, host, port, "owner"):
		return false
	_send_owner({"op":"WorkerConnect","worker_id":"godot-bridge-owner","region":"W","attributes":["physics","server"],"auth_token":token,"proto":1})
	_send_owner({"op":"Interest","center":[-2.0,0.0],"radius":200.0,"full_radius":200.0})
	return true

func _connect_viewer(host: String, port: int, token: String) -> bool:
	_viewer_tcp = StreamPeerTCP.new()
	_viewer_buf.clear()
	if not _connect_client(_viewer_tcp, host, port, "viewer"):
		return false
	_send_viewer({"op":"WorkerConnect","worker_id":"godot-bridge-viewer","region":"OBS","attributes":["observer"],"auth_token":token,"proto":1})
	_send_viewer({"op":"Interest","center":[-2.0,0.0],"radius":200.0,"full_radius":200.0})
	return true

func _create_entity(entity_id: String, pos: Array, kind: String) -> void:
	_send_owner({
		"op":"CreateEntity",
		"request_id":"create-" + entity_id,
		"entity":entity_id,
		"region":"W",
		"components":{
			"pos":pos,
			"vel":[0.0,0.0],
			"kind":kind
		}
	})

func _snapshot_has_only_fresh(fresh_id: String, stale_id: String) -> bool:
	var snap := _bridge.snapshot_contract()
	if snap["phase"] != "Live" or snap["entity_count"] != 1:
		_query_error = JSON.stringify(snap)
		return false
	if _bridge.has_entity(stale_id):
		_query_error = "stale entity survived resync: " + JSON.stringify(snap)
		return false
	if not _bridge.has_entity(fresh_id):
		_query_error = "fresh entity missing after resync: " + JSON.stringify(snap)
		return false
	var pos = _bridge.entity_position2(fresh_id)
	if not (pos is Array) or pos.size() < 2 or abs(float(pos[0]) + 2.0) > 0.000001:
		_query_error = "fresh position mismatch: " + JSON.stringify(snap)
		return false
	return true

func _disconnect_viewer() -> void:
	_viewer_tcp.disconnect_from_host()
	_bridge.on_transport_closed()

func _disconnect_all() -> void:
	_owner_tcp.disconnect_from_host()
	_viewer_tcp.disconnect_from_host()

func _init():
	var host := _env("GW_HOST", "127.0.0.1")
	var port := int(_env("GW_PORT", "7811"))
	var stale_id := _env("GW_BRIDGE_STALE_ENTITY", "godot-bridge-stale")
	var fresh_id := _env("GW_BRIDGE_FRESH_ENTITY", "godot-bridge-fresh")
	var owner_token := _env("GW_GODOT_OWNER_TOKEN", "godot-owner-token")
	var obs_token := _env("GW_GODOT_OBS_TOKEN", "godot-observer-token")
	var query_id := "godot-bridge-real-resync"

	_bridge.on_transport_connecting()
	if not _connect_owner(host, port, owner_token):
		quit(2)
		return
	if not _connect_viewer(host, port, obs_token):
		quit(2)
		return
	_bridge.mark_live()
	_pump_for(500, query_id, stale_id, fresh_id)

	_create_entity(stale_id, [-3.0, 0.0], "stale_before_reconnect")
	_create_entity(fresh_id, [-2.0, 0.0], "fresh_after_reconnect")

	var initial_ok := _wait_until(4000, query_id, stale_id, fresh_id, func():
		return _created.has(stale_id) and _created.has(fresh_id) and _bridge.has_entity(stale_id) and _bridge.has_entity(fresh_id)
	)
	if not initial_ok:
		print("GODOT CLIENT-BRIDGE TCP: FAIL initial_stream created=", JSON.stringify(_created),
			" snapshot=", JSON.stringify(_bridge.snapshot_contract()))
		_disconnect_all()
		quit(1)
		return

	_disconnect_viewer()
	_send_owner({"op":"DeleteEntity","request_id":"delete-" + stale_id,"entity":stale_id})
	var delete_ok := _wait_until(4000, query_id, stale_id, fresh_id, func(): return _deleted_stale)
	if not delete_ok:
		print("GODOT CLIENT-BRIDGE TCP: FAIL delete_stale_not_confirmed")
		_disconnect_all()
		quit(1)
		return

	_bridge.on_transport_connecting()
	_bridge.begin_full_resync()
	if not _connect_viewer(host, port, obs_token):
		_disconnect_all()
		quit(2)
		return
	_send_viewer({"op":"EntityQuery","request_id":query_id,"query":{"type":"all"}})
	var resync_ok := _wait_until(4000, query_id, stale_id, fresh_id, func():
		return _resync_response_seen and _snapshot_has_only_fresh(fresh_id, stale_id)
	)

	print("GODOT CLIENT-BRIDGE TCP | initial_ok=", 1 if initial_ok else 0,
		" delete_ok=", 1 if delete_ok else 0,
		" resync_response=", 1 if _resync_response_seen else 0,
		" final_entities=", _bridge.snapshot_contract()["entity_count"])
	if resync_ok:
		print("GODOT CLIENT-BRIDGE TCP: PASS -- real broker reconnect checkout rebuilt Godot bridge snapshot and removed stale entity")
		_disconnect_all()
		quit(0)
	else:
		print("GODOT CLIENT-BRIDGE TCP: FAIL -- query_error=", _query_error,
			" snapshot=", JSON.stringify(_bridge.snapshot_contract()))
		_disconnect_all()
		quit(1)
