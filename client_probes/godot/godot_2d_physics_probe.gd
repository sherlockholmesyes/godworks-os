extends SceneTree
## Headless Godot proof that a 2D physics body can drive a broker seam.
##
## This is deliberately narrower than a game demo. The source of movement is a
## real Godot CharacterBody2D using move_and_collide(); the broker still owns
## authority, handoff, public state, and stale-owner fencing.

var _tcp_w := StreamPeerTCP.new()
var _tcp_e := StreamPeerTCP.new()
var _buf_w := PackedByteArray()
var _buf_e := PackedByteArray()
var _scene_root: Node2D
var _body: CharacterBody2D

var _create_ok := false
var _w_authority_ok := false
var _e_authority_ok := false
var _w_removed_ok := false
var _stale_rejected := false
var _stale_reason := ""
var _e_pos_updates := 0
var _e_probe_seen := false
var _e_query_ok := false
var _e_query_error := ""
var _godot_motion_ok := false
var _godot_crossed_boundary := false

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

func _send_w(obj: Dictionary) -> bool:
	return _tcp_w.put_data(_frame(obj)) == OK

func _send_e(obj: Dictionary) -> bool:
	return _tcp_e.put_data(_frame(obj)) == OK

func _poll_frames_w() -> Array:
	_tcp_w.poll()
	var avail := _tcp_w.get_available_bytes()
	if avail > 0:
		var data := _tcp_w.get_data(avail)
		if data[0] == OK:
			_buf_w.append_array(data[1])
	var frames := []
	while _buf_w.size() >= 4:
		var n := (_buf_w[0] << 24) | (_buf_w[1] << 16) | (_buf_w[2] << 8) | _buf_w[3]
		if _buf_w.size() < 4 + n:
			break
		var body := _buf_w.slice(4, 4 + n)
		_buf_w = _buf_w.slice(4 + n)
		var obj = JSON.parse_string(body.get_string_from_utf8())
		if obj is Dictionary:
			frames.append(obj)
	return frames

func _poll_frames_e() -> Array:
	_tcp_e.poll()
	var avail := _tcp_e.get_available_bytes()
	if avail > 0:
		var data := _tcp_e.get_data(avail)
		if data[0] == OK:
			_buf_e.append_array(data[1])
	var frames := []
	while _buf_e.size() >= 4:
		var n := (_buf_e[0] << 24) | (_buf_e[1] << 16) | (_buf_e[2] << 8) | _buf_e[3]
		if _buf_e.size() < 4 + n:
			break
		var body := _buf_e.slice(4, 4 + n)
		_buf_e = _buf_e.slice(4 + n)
		var obj = JSON.parse_string(body.get_string_from_utf8())
		if obj is Dictionary:
			frames.append(obj)
	return frames

func _vec2_from_pair(value) -> Vector2:
	if value is Array and value.size() >= 2 and (value[0] is int or value[0] is float) and (value[1] is int or value[1] is float):
		return Vector2(float(value[0]), float(value[1]))
	return Vector2(1.0e30, 1.0e30)

func _bad_vec2() -> Vector2:
	return Vector2(1.0e30, 1.0e30)

func _ensure_body(entity_id: String, pos: Vector2) -> void:
	if _scene_root == null:
		_scene_root = Node2D.new()
		_scene_root.name = "Godworks2DPhysicsScene"
		get_root().add_child(_scene_root)
	if _body == null:
		_body = CharacterBody2D.new()
		_body.name = entity_id
		_body.set_meta("entity_id", entity_id)
		var shape := CollisionShape2D.new()
		shape.name = "ProbeCircle"
		var circle := CircleShape2D.new()
		circle.radius = 0.25
		shape.shape = circle
		_body.add_child(shape)
		_scene_root.add_child(_body)
	_body.position = pos

func _physics_step_body(motion: Vector2) -> Vector2:
	var before := _body.position
	var collision := _body.move_and_collide(motion)
	var after := _body.position
	if collision == null and after.distance_to(before) > 0.001:
		_godot_motion_ok = true
	if after.x >= 0.5:
		_godot_crossed_boundary = true
	return after

func _handle_frame(side: String, obj: Dictionary, entity_id: String, query_id: String) -> void:
	var op := str(obj.get("op", ""))
	if op == "CreateEntityResponse" and side == "w" and obj.get("entity", "") == entity_id:
		_create_ok = obj.get("success", false)
	elif op == "AuthorityChange" and obj.get("entity", "") == entity_id:
		var comp := str(obj.get("comp", ""))
		var auth: bool = obj.get("authoritative", false)
		if side == "w" and comp == "pos" and auth:
			_w_authority_ok = true
		elif side == "e" and comp == "pos" and auth:
			_e_authority_ok = true
	elif op == "RemoveEntity" and side == "w" and obj.get("entity", "") == entity_id:
		_w_removed_ok = true
	elif op == "UpdateRejected" and side == "w" and obj.get("entity", "") == entity_id and obj.get("comp", "") == "godot_physics_probe":
		_stale_rejected = true
		_stale_reason = str(obj.get("reason", "unknown"))
	elif side == "e" and op == "AddEntity" and obj.get("entity", "") == entity_id:
		_ensure_body(entity_id, _body.position if _body != null else Vector2.ZERO)
	elif side == "e" and op == "ComponentUpdate" and obj.get("entity", "") == entity_id:
		var comp := str(obj.get("comp", ""))
		if comp == "pos":
			var pos := _vec2_from_pair(obj.get("value", []))
			if pos != _bad_vec2():
				_ensure_body(entity_id, pos)
				_body.position = pos
				if pos.x >= 0.5:
					_e_pos_updates += 1
		elif comp == "godot_physics_probe":
			var value: Variant = obj.get("value", {})
			if value is Dictionary and value.get("mode", "") == "godot_2d_physics":
				_e_probe_seen = true
	elif side == "e" and op == "EntityQueryResponse" and obj.get("request_id", "") == query_id:
		_e_query_ok = _validate_e_query(obj, entity_id)
		if not _e_query_ok:
			_e_query_error = JSON.stringify(obj)

func _pump_for(ms: int, entity_id: String, query_id: String) -> void:
	var elapsed := 0
	while elapsed < ms:
		for obj in _poll_frames_w():
			_handle_frame("w", obj, entity_id, query_id)
		for obj in _poll_frames_e():
			_handle_frame("e", obj, entity_id, query_id)
		OS.delay_msec(25)
		elapsed += 25

func _wait_until(ms: int, entity_id: String, query_id: String, predicate: Callable) -> bool:
	var elapsed := 0
	while elapsed < ms:
		_pump_for(100, entity_id, query_id)
		elapsed += 100
		if predicate.call():
			return true
	return predicate.call()

func _connect_client(tcp: StreamPeerTCP, host: String, port: int, label: String) -> bool:
	var err := tcp.connect_to_host(host, port)
	if err != OK:
		print("GODOT 2D PHYSICS: FAIL connect_error label=", label, " err=", err)
		return false
	var elapsed := 0
	while elapsed < 6000:
		tcp.poll()
		if tcp.get_status() == StreamPeerTCP.STATUS_CONNECTED:
			return true
		OS.delay_msec(50)
		elapsed += 50
	print("GODOT 2D PHYSICS: FAIL connect_timeout label=", label, " host=", host, " port=", port)
	return false

func _validate_e_query(response: Dictionary, entity_id: String) -> bool:
	if response.get("count", 0) < 1:
		return false
	var rows = response.get("entities", [])
	if not (rows is Array):
		return false
	for row in rows:
		if not (row is Dictionary) or row.get("entity", "") != entity_id:
			continue
		if row.get("region", "") != "E":
			return false
		if row.get("ghost", false):
			return false
		var pos := _vec2_from_pair(row.get("pos", []))
		if pos == _bad_vec2() or pos.x < 1.5:
			return false
		var comps = row.get("components", {})
		if not (comps is Dictionary):
			return false
		var probe = comps.get("godot_physics_probe", {})
		return probe is Dictionary and probe.get("mode", "") == "godot_2d_physics" and int(probe.get("seq", 0)) == 1
	return false

func _disconnect_all() -> void:
	_tcp_w.disconnect_from_host()
	_tcp_e.disconnect_from_host()

func _init():
	call_deferred("_run")

func _run() -> void:
	var host := _env("GW_HOST", "127.0.0.1")
	var port_w := int(_env("GW_PORT_W", "7821"))
	var port_e := int(_env("GW_PORT_E", "7822"))
	var entity_id := _env("GW_GODOT_2D_ENTITY", "godot-2d-physics-probe")
	var w_token := _env("GW_GODOT_W_TOKEN", "godot-cross-w-token")
	var e_token := _env("GW_GODOT_E_TOKEN", "godot-cross-e-token")
	var query_id := "godot-2d-physics-query"

	if not _connect_client(_tcp_w, host, port_w, "W"):
		quit(2)
		return
	if not _connect_client(_tcp_e, host, port_e, "E"):
		quit(2)
		return

	_send_w({"op":"WorkerConnect","worker_id":"godot-2d-owner-W","region":"W","attributes":["physics","server"],"auth_token":w_token,"proto":1})
	_send_w({"op":"Interest","center":[-2.0,0.0],"radius":200.0,"full_radius":200.0})
	_send_e({"op":"WorkerConnect","worker_id":"godot-2d-owner-E","region":"E","attributes":["physics","server"],"auth_token":e_token,"proto":1})
	_send_e({"op":"Interest","center":[2.0,0.0],"radius":200.0,"full_radius":200.0})
	_pump_for(500, entity_id, query_id)

	_ensure_body(entity_id, Vector2(-2.0, 0.0))
	await process_frame
	await physics_frame
	_send_w({
		"op":"CreateEntity",
		"request_id":"create-godot-2d-physics-probe",
		"entity":entity_id,
		"region":"W",
		"components":{
			"pos":[_body.position.x,_body.position.y],
			"vel":[0.8,0.0],
			"kind":"godot_2d_physics_probe",
			"physics_body":{"engine":"Godot","body":"CharacterBody2D","shape":"CircleShape2D"}
		}
	})
	_wait_until(2500, entity_id, query_id, func(): return _create_ok and _w_authority_ok)

	for i in range(5):
		var pos := _physics_step_body(Vector2(0.8, 0.0))
		_send_w({"op":"UpdateComponent","entity":entity_id,"comp":"pos","value":[pos.x,pos.y]})
		_pump_for(650, entity_id, query_id)

	_wait_until(6000, entity_id, query_id, func(): return _e_authority_ok)

	if _e_authority_ok:
		_send_e({"op":"UpdateComponent","entity":entity_id,"comp":"pos","value":[2.5,0.0]})
		_send_e({"op":"UpdateComponent","entity":entity_id,"comp":"godot_physics_probe","value":{"mode":"godot_2d_physics","seq":1}})
		_pump_for(1200, entity_id, query_id)

	_send_w({"op":"UpdateComponent","entity":entity_id,"comp":"godot_physics_probe","value":{"mode":"stale_w_owner","seq":1}})
	_pump_for(900, entity_id, query_id)

	_send_e({"op":"EntityQuery","request_id":query_id,"query":{"type":"region","region":"E"}})
	_wait_until(3000, entity_id, query_id, func(): return _e_query_ok)

	var e_scene_ok := _body != null and _body.position == Vector2(2.5, 0.0) and _e_pos_updates >= 1 and _e_probe_seen
	var stream_ok := _create_ok and _w_authority_ok and _w_removed_ok and _e_authority_ok and _godot_motion_ok and _godot_crossed_boundary and e_scene_ok and _e_query_ok and _stale_rejected
	print("GODOT 2D PHYSICS | create_ok=", 1 if _create_ok else 0,
		" w_authority_ok=", 1 if _w_authority_ok else 0,
		" godot_motion_ok=", 1 if _godot_motion_ok else 0,
		" crossed_boundary=", 1 if _godot_crossed_boundary else 0,
		" w_removed_ok=", 1 if _w_removed_ok else 0,
		" e_authority_ok=", 1 if _e_authority_ok else 0,
		" e_scene_ok=", 1 if e_scene_ok else 0,
		" e_query_ok=", 1 if _e_query_ok else 0,
		" stale_rejected=", 1 if _stale_rejected else 0)
	if stream_ok:
		print("GODOT 2D PHYSICS: PASS -- Godot CharacterBody2D crossed W->E, E write is public, stale W owner fenced")
		_disconnect_all()
		quit(0)
	else:
		print("GODOT 2D PHYSICS: FAIL -- stale_reason=", _stale_reason, " query_error=", _e_query_error)
		_disconnect_all()
		quit(1)
