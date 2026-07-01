extends SceneTree
## Headless Godot contract probe for the 3D-ready spatial rail.
##
## This does not claim a full 3D runtime. It proves that the Godot-side contract
## can consume D3 spatial metadata, stable 3D component names, and physics-island
## authority shape without collapsing the API back to D2-only assumptions.

const REQUIRED_COMPONENTS := [
	"core.pos3",
	"core.vel3",
	"core.rot3",
	"core.lin3",
	"core.ang3",
	"core.physics_body",
]

var _scene_root: Node3D

func _env(key: String, fallback: String) -> String:
	var value := OS.get_environment(key)
	return fallback if value == "" else value

func _default_fixture_path() -> String:
	return ProjectSettings.globalize_path("res://../../tests/fixtures/client_bridge/godot-3d-contract.json")

func _fail(reason: String) -> void:
	print("GODOT 3D CONTRACT: FAIL ", reason)
	quit(1)

func _load_fixture() -> Dictionary:
	var path := _env("GW_GODOT_3D_FIXTURE", _default_fixture_path())
	var file := FileAccess.open(path, FileAccess.READ)
	if file == null:
		print("GODOT 3D CONTRACT: FAIL fixture_open path=", path, " err=", FileAccess.get_open_error())
		quit(2)
		return {}
	var parsed = JSON.parse_string(file.get_as_text())
	if not (parsed is Dictionary):
		print("GODOT 3D CONTRACT: FAIL fixture_parse path=", path)
		quit(2)
		return {}
	return parsed

func _require(condition: bool, reason: String) -> void:
	if not condition:
		_fail(reason)

func _number_array(value, size: int, label: String) -> Array:
	_require(value is Array, label + " must be array")
	var items: Array = value
	_require(items.size() == size, label + " must have size " + str(size))
	var out := []
	for item in items:
		_require(typeof(item) == TYPE_FLOAT or typeof(item) == TYPE_INT, label + " must contain numbers")
		out.append(float(item))
	return out

func _vec3(value, label: String) -> Vector3:
	var nums := _number_array(value, 3, label)
	return Vector3(nums[0], nums[1], nums[2])

func _quat(value, label: String) -> Quaternion:
	var nums := _number_array(value, 4, label)
	return Quaternion(nums[0], nums[1], nums[2], nums[3]).normalized()

func _nearly_equal(a: float, b: float, eps: float = 0.0001) -> bool:
	return abs(a - b) <= eps

func _vec3_equal(a: Vector3, b: Vector3) -> bool:
	return _nearly_equal(a.x, b.x) and _nearly_equal(a.y, b.y) and _nearly_equal(a.z, b.z)

func _quat_equal(a: Quaternion, b: Quaternion) -> bool:
	var dot: float = abs(a.dot(b))
	return _nearly_equal(dot, 1.0, 0.0001)

func _validate_spatial_schema(fixture: Dictionary) -> void:
	var schema = fixture.get("spatial_schema", {})
	_require(schema is Dictionary, "spatial_schema must be object")
	_require(schema.get("spatial_dim", "") == "D3", "spatial_dim must be D3")
	_require(schema.get("coordinate_codec", "") == "debug_f64_3", "coordinate_codec must be debug_f64_3")
	var partition = schema.get("partition_schema", {})
	_require(partition is Dictionary, "partition_schema must be object")
	_require(partition.get("kind", "") == "grid3d", "partition_schema.kind must be grid3d")
	for key in ["cols", "rows", "layers"]:
		_require(int(partition.get(key, 0)) > 0, "partition_schema." + key + " must be positive")
	_require(int(fixture.get("component_registry_version", 0)) == 1, "component_registry_version must be 1")

func _validate_required_components(fixture: Dictionary) -> void:
	var physics_components = fixture.get("physics_island_components", [])
	_require(physics_components is Array, "physics_island_components must be array")
	for component in REQUIRED_COMPONENTS:
		_require(component in physics_components, "missing physics_island component " + component)

func _spawn_entity(entity: Dictionary) -> CharacterBody3D:
	var components = entity.get("components", {})
	_require(components is Dictionary, "entity components must be object")
	for component in REQUIRED_COMPONENTS:
		_require(components.has(component), "entity missing component " + component)

	var body := CharacterBody3D.new()
	body.name = str(entity.get("entity", "unnamed"))
	body.position = _vec3(components["core.pos3"], "core.pos3")
	body.velocity = _vec3(components["core.vel3"], "core.vel3")
	body.basis = Basis(_quat(components["core.rot3"], "core.rot3"))
	body.set_meta("godworks_entity", str(entity.get("entity", "")))
	body.set_meta("godworks_component_keys", components.keys().duplicate())
	body.set_meta("godworks_lin3", _vec3(components["core.lin3"], "core.lin3"))
	body.set_meta("godworks_ang3", _vec3(components["core.ang3"], "core.ang3"))

	var shape := CollisionShape3D.new()
	shape.shape = SphereShape3D.new()
	body.add_child(shape)
	_scene_root.add_child(body)
	return body

func _snapshot_body(body: CharacterBody3D) -> Dictionary:
	var keys: Array = body.get_meta("godworks_component_keys")
	keys.sort()
	var q := body.basis.get_rotation_quaternion().normalized()
	return {
		"entity": body.get_meta("godworks_entity"),
		"position3": [body.position.x, body.position.y, body.position.z],
		"velocity3": [body.velocity.x, body.velocity.y, body.velocity.z],
		"rotation_quat": [q.x, q.y, q.z, q.w],
		"component_keys": keys,
	}

func _verify_expected(expected: Dictionary, bodies: Array) -> void:
	_require(int(expected.get("node_count", -1)) == bodies.size(), "node_count mismatch")
	var nodes = expected.get("nodes", [])
	_require(nodes is Array, "expected_scene.nodes must be array")
	_require(nodes.size() == bodies.size(), "expected_scene.nodes count mismatch")
	for i in range(nodes.size()):
		var expected_node: Dictionary = nodes[i]
		var body: CharacterBody3D = bodies[i]
		var actual := _snapshot_body(body)
		_require(actual["entity"] == expected_node.get("entity", ""), "entity mismatch")
		_require(_vec3_equal(body.position, _vec3(expected_node.get("position3", []), "expected position3")), "position3 mismatch")
		_require(_vec3_equal(body.velocity, _vec3(expected_node.get("velocity3", []), "expected velocity3")), "velocity3 mismatch")
		_require(_quat_equal(body.basis.get_rotation_quaternion().normalized(), _quat(expected_node.get("rotation_quat", []), "expected rotation_quat")), "rotation_quat mismatch")
		var expected_keys: Array = expected_node.get("component_keys", [])
		expected_keys.sort()
		_require(actual["component_keys"] == expected_keys, "component_keys mismatch")

func _init():
	var fixture := _load_fixture()
	_require(fixture.get("name", "") == "godot_3d_contract_v1", "fixture name drifted")
	_validate_spatial_schema(fixture)
	_validate_required_components(fixture)

	_scene_root = Node3D.new()
	_scene_root.name = "Godworks3DContractRoot"
	root.add_child(_scene_root)

	var entities = fixture.get("entities", [])
	_require(entities is Array, "entities must be array")
	var bodies := []
	for entity in entities:
		_require(entity is Dictionary, "entity must be object")
		bodies.append(_spawn_entity(entity))

	_verify_expected(fixture.get("expected_scene", {}), bodies)
	print("GODOT 3D CONTRACT: PASS -- fixture=", fixture.get("name", "unknown"), " nodes=", bodies.size())
	quit(0)
