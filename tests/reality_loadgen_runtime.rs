use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn unique_wal(label: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "godworks_{label}_{}_{}.wal.jsonl",
            std::process::id(),
            ts
        ))
        .to_string_lossy()
        .into_owned()
}

fn wait_for_port(port: u16) {
    for _ in 0..80 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        sleep(Duration::from_millis(25));
    }
    panic!("broker did not open port {port}");
}

fn start_broker(port: u16, region: &str, mesh: Option<(String, u16)>) -> Child {
    start_broker_with_wal(port, region, mesh, unique_wal(region), None)
}

fn start_broker_with_wal(
    port: u16,
    region: &str,
    mesh: Option<(String, u16)>,
    wal: String,
    restore_offset: Option<u64>,
) -> Child {
    start_broker_with_wal_and_env(port, region, mesh, wal, restore_offset, &[])
}

fn start_broker_with_wal_and_env(
    port: u16,
    region: &str,
    mesh: Option<(String, u16)>,
    wal: String,
    restore_offset: Option<u64>,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_godworks_broker"));
    cmd.env("GW_HOST", "127.0.0.1")
        .env("GW_PORT", port.to_string())
        .env("GW_WAL", wal)
        .env("GW_DURABLE_FLUSH_MS", "5")
        .env("GW_ADVERTISE", format!("{region}=127.0.0.1:{port}"))
        .env_remove("GW_MESH_EAST")
        .env_remove("GW_MESH_ACK_DROP")
        .env_remove("GW_MESH_ADOPT_DROP")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some((target_region, target_port)) = mesh {
        cmd.env(
            "GW_MESH",
            format!("{target_region}=127.0.0.1:{target_port}"),
        );
    } else {
        cmd.env_remove("GW_MESH");
    }
    if let Some(offset) = restore_offset {
        cmd.env("GW_RESTORE_OFFSET", offset.to_string());
    } else {
        cmd.env_remove("GW_RESTORE_OFFSET");
    }
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    let child = cmd.spawn().expect("spawn broker");
    wait_for_port(port);
    child
}

fn stop(child: &mut Child) {
    if child.try_wait().expect("try_wait").is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn parse_result_line(stdout: &str) -> HashMap<String, String> {
    let line = stdout
        .lines()
        .find(|line| line.starts_with("reality_loadgen "))
        .unwrap_or_else(|| panic!("missing reality_loadgen result line:\n{stdout}"));
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn metric_u64(metrics: &HashMap<String, String>, key: &str) -> u64 {
    metrics
        .get(key)
        .unwrap_or_else(|| panic!("missing metric {key}: {metrics:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("bad integer metric {key}: {metrics:?}"))
}

fn metric_f64(metrics: &HashMap<String, String>, key: &str) -> f64 {
    metrics
        .get(key)
        .unwrap_or_else(|| panic!("missing metric {key}: {metrics:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("bad float metric {key}: {metrics:?}"))
}

fn frame(value: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("json frame body");
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn read_frame(stream: &mut TcpStream) -> Value {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).expect("read frame header");
    let n = u32::from_be_bytes(hdr) as usize;
    let mut body = vec![0u8; n];
    stream.read_exact(&mut body).expect("read frame body");
    serde_json::from_slice(&body).expect("decode frame json")
}

fn connect_worker(port: u16, worker_id: &str, region: &str, attributes: &[&str]) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect worker");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .write_all(&frame(&json!({
            "op": "WorkerConnect",
            "worker_id": worker_id,
            "region": region,
            "attributes": attributes,
        })))
        .expect("send WorkerConnect");
    stream
}

fn wait_for_response(stream: &mut TcpStream, op: &str, request_id: &str) -> Value {
    for _ in 0..256 {
        let f = read_frame(stream);
        if f.get("op").and_then(Value::as_str) == Some(op)
            && f.get("request_id").and_then(Value::as_str) == Some(request_id)
        {
            return f;
        }
    }
    panic!("did not receive {op} for request_id={request_id}");
}

fn create_entity(stream: &mut TcpStream, eid: &str, region: &str, x: f64, tag: &str) {
    create_entity_with_components(
        stream,
        eid,
        region,
        json!({
            "pos": [x, 0.0],
            "vel": [0.0, 0.0],
            "kind": tag,
        }),
    );
}

fn create_entity_with_components(
    stream: &mut TcpStream,
    eid: &str,
    region: &str,
    components: Value,
) {
    let request_id = format!("create-{eid}");
    stream
        .write_all(&frame(&json!({
            "op": "CreateEntity",
            "request_id": request_id,
            "entity": eid,
            "region": region,
            "components": components,
        })))
        .expect("send CreateEntity");
    let response = wait_for_response(stream, "CreateEntityResponse", &request_id);
    assert_eq!(
        response.get("success").and_then(Value::as_bool),
        Some(true),
        "create failed: {response}"
    );
}

fn snapshot_marker(port: u16, snapshot_id: &str) -> Value {
    let request_id = format!("snapshot-{snapshot_id}");
    let mut stream = connect_worker(port, "snapshot-coordinator", "OBS", &["snapshot"]);
    stream
        .write_all(&frame(&json!({
            "op": "SnapshotMarker",
            "request_id": request_id,
            "snapshot_id": snapshot_id,
        })))
        .expect("send SnapshotMarker");
    wait_for_response(&mut stream, "SnapshotManifest", &request_id)
}

fn drain_broker(port: u16, request_id: &str) -> Value {
    let mut stream = connect_worker(port, &format!("drain-{request_id}"), "OBS", &["inspector"]);
    stream
        .write_all(&frame(&json!({
            "op": "Drain",
            "request_id": request_id,
        })))
        .expect("send Drain");
    wait_for_response(&mut stream, "DrainAck", request_id)
}

fn entity_query(port: u16, request_id: &str) -> Value {
    entity_query_with_query(port, request_id, json!({"type": "all"}))
}

fn entity_query_with_query(port: u16, request_id: &str, query: Value) -> Value {
    let mut stream = connect_worker(
        port,
        &format!("observer-{request_id}"),
        "OBS",
        &["observer"],
    );
    stream
        .write_all(&frame(&json!({
            "op": "EntityQuery",
            "request_id": request_id,
            "query": query,
        })))
        .expect("send EntityQuery");
    wait_for_response(&mut stream, "EntityQueryResponse", request_id)
}

fn inspector_query(port: u16, request_id: &str) -> Value {
    let mut stream = connect_worker(
        port,
        &format!("inspector-{request_id}"),
        "OBS",
        &["inspector"],
    );
    stream
        .write_all(&frame(&json!({
            "op": "InspectorQuery",
            "request_id": request_id,
            "max_entities": 100,
        })))
        .expect("send InspectorQuery");
    wait_for_response(&mut stream, "InspectorFrame", request_id)
}

fn broker_u64(frame: &Value, field: &str) -> u64 {
    frame
        .get("broker")
        .and_then(|v| v.get(field))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("missing broker.{field} in frame: {frame}"))
}

fn entity_ids(frame: &Value) -> Vec<String> {
    let mut ids: Vec<String> = frame
        .get("entities")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing entities in query response: {frame}"))
        .iter()
        .filter_map(|row| {
            row.get("entity")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    ids.sort();
    ids
}

fn wait_for_broker_state(
    port: u16,
    label: &str,
    expected_entities: u64,
    expected_pending_mesh: u64,
) -> Value {
    let mut last = Value::Null;
    for i in 0..80 {
        let frame = inspector_query(port, &format!("{label}-{i}"));
        if broker_u64(&frame, "entity_count") == expected_entities
            && broker_u64(&frame, "pending_mesh") == expected_pending_mesh
        {
            return frame;
        }
        last = frame;
        sleep(Duration::from_millis(100));
    }
    panic!(
        "broker {label} never reached entity_count={expected_entities}, pending_mesh={expected_pending_mesh}; last={last}"
    );
}

#[test]
fn cross_broker_reality_loadgen_requires_mesh_adoption() {
    let port_w = free_port();
    let port_e = free_port();
    assert_ne!(port_w, port_e);

    let mut broker_e = start_broker(port_e, "E", None);
    let mut broker_w = start_broker(port_w, "W", Some(("E".to_string(), port_e)));
    sleep(Duration::from_millis(900));

    let output = Command::new(env!("CARGO_BIN_EXE_reality_loadgen"))
        .env("GW_HOST", "127.0.0.1")
        .env("GW_TARGET", port_w.to_string())
        .env("GW_TARGET_E", port_e.to_string())
        .env("GW_ENTITIES", "4")
        .env("GW_TICKS", "60")
        .env("GW_HZ", "35")
        .env("GW_EVENT_BURST", "6")
        .env("GW_REQUIRE_MESH", "1")
        .env("GW_REQUIRE_WRITER_SWAP", "1")
        .env("GW_REQUIRE_ASSEMBLY_HANDOFF", "1")
        .env("GW_REQUIRE_PHYSICS_PAYLOAD", "1")
        .env("GW_REQUIRE_ASSET_MANIFEST", "1")
        .env("GW_REQUIRE_CONTENT_MANIFEST", "1")
        .env("GW_REQUIRE_SCHEMA_MANIFEST", "1")
        .env("GW_REQUIRE_QBI_AST", "1")
        .env("GW_REQUIRE_MONITOR_HEALTH", "1")
        .env("GW_SLOW_VIEWER", "1")
        .output()
        .expect("run reality_loadgen");

    stop(&mut broker_w);
    stop(&mut broker_e);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "reality_loadgen failed: status={:?}\nstdout={stdout}\nstderr={stderr}",
        output.status
    );

    let metrics = parse_result_line(&stdout);
    assert_eq!(metrics.get("result").map(String::as_str), Some("pass"));
    assert_eq!(
        metrics.get("mode").map(String::as_str),
        Some("cross-broker")
    );
    assert_eq!(metrics.get("failures").map(String::as_str), Some("none"));
    assert_eq!(metric_u64(&metrics, "entities"), 4);
    assert!(
        metric_u64(&metrics, "east_authority_gain") >= 4,
        "east broker saw visibility but not authority adoption: {metrics:?}"
    );
    assert!(
        metric_u64(&metrics, "east_visible") > 0,
        "east broker never saw the crossed bodies: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "handoff_probe_ok"),
        4,
        "new owner could not write a post-adopt component visible through query: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "handoff_probe_rejected"),
        4,
        "old owner was not fenced after adopt: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "assembly_child_ok"),
        4,
        "root assembly children did not transfer with their parents across the broker seam: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "assembly_probe_rejected"),
        4,
        "old owner was not fenced from writing assembly children after adopt: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "physics_payload_ok"),
        4,
        "full physics payload did not survive cross-broker handoff and E-side write: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "physics_clock_ok"),
        4,
        "physics gen/sim_time/t_server were not monotonic after handoff: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "asset_manifest_ok"),
        4,
        "asset manifest did not carry every crossed body's visible dependencies: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "content_manifest_ok"),
        4,
        "content manifest did not produce a package load plan for every crossed body: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "content_load_ok"),
        4,
        "headless client could not resolve every crossed body's package load plan: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "schema_manifest_ok"),
        4,
        "schema manifest did not carry every crossed body's component ABI: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "qbi_ast_ok"),
        4,
        "QBI boolean AST did not select exactly the crossed bodies: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "health_ok"),
        2,
        "both brokers must expose healthy runtime snapshots after the cross-broker load: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "monitor_tick_ok"),
        2,
        "both brokers must prove the monitor loop ticked during the load: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "monitor_queue_ok"),
        2,
        "monitor work queues must drain after the handoff/load window: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "health_queue_backlog"),
        0,
        "runtime health queues were not empty at the stable post-load cut: {metrics:?}"
    );
    assert!(
        metric_f64(&metrics, "health_max_tick_lag_ms").is_finite(),
        "tick lag must be a finite runtime metric: {metrics:?}"
    );
    assert!(
        metric_f64(&metrics, "health_max_lock_ms").is_finite(),
        "max lock hold must be a finite runtime metric: {metrics:?}"
    );
}

#[test]
fn snapshot_marker_restore_offset_rolls_back_post_cut_entities() {
    let port = free_port();
    let wal = unique_wal("snapshot_restore_runtime");
    let mut broker = start_broker_with_wal(port, "EARTH", None, wal.clone(), None);

    let mut owner = connect_worker(port, "earth-owner", "EARTH", &["physics"]);
    create_entity(&mut owner, "pre-0", "EARTH", 1.0, "pre-cut");
    create_entity(&mut owner, "pre-1", "EARTH", 2.0, "pre-cut");
    create_entity(&mut owner, "pre-2", "EARTH", 3.0, "pre-cut");

    let before_cut = entity_query(port, "before-cut");
    assert_eq!(before_cut.get("count").and_then(Value::as_u64), Some(3));

    let manifest = snapshot_marker(port, "cut-1");
    assert_eq!(
        manifest.get("entity_count").and_then(Value::as_u64),
        Some(3),
        "snapshot manifest did not name the live pre-cut population: {manifest}"
    );
    let offset = manifest
        .get("wal_offset")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("snapshot manifest missing wal_offset: {manifest}"));
    assert!(
        offset > 0,
        "snapshot offset must name a concrete WAL cut: {manifest}"
    );

    create_entity(&mut owner, "post-0", "EARTH", 4.0, "post-cut");
    create_entity(&mut owner, "post-1", "EARTH", 5.0, "post-cut");
    let live_after_cut = entity_query(port, "after-cut");
    assert_eq!(live_after_cut.get("count").and_then(Value::as_u64), Some(5));
    assert!(
        entity_ids(&live_after_cut).contains(&"post-0".to_string()),
        "live world never accepted the post-cut entity: {live_after_cut}"
    );

    drop(owner);
    stop(&mut broker);

    let mut restored = start_broker_with_wal(port, "EARTH", None, wal.clone(), Some(offset));
    let restored_query = entity_query(port, "restored-cut");
    stop(&mut restored);
    let _ = std::fs::remove_file(&wal);

    let restored_ids = entity_ids(&restored_query);
    assert_eq!(
        restored_query.get("count").and_then(Value::as_u64),
        Some(3),
        "restore offset did not roll the world back to the snapshot count: {restored_query}"
    );
    assert!(
        restored_ids.iter().all(|eid| eid.starts_with("pre-")),
        "restore leaked post-cut entities across the snapshot artifact: {restored_ids:?}"
    );
    assert!(
        !restored_ids.iter().any(|eid| eid.starts_with("post-")),
        "post-cut entities survived GW_RESTORE_OFFSET rollback: {restored_ids:?}"
    );
}

#[test]
fn snapshot_vector_restores_in_flight_mesh_handoff_exactly_once() {
    let port_w = free_port();
    let port_e = free_port();
    assert_ne!(port_w, port_e);
    let wal_w = unique_wal("snapshot_vector_w");
    let wal_e = unique_wal("snapshot_vector_e");

    let mut broker_e = start_broker_with_wal_and_env(
        port_e,
        "E",
        None,
        wal_e.clone(),
        None,
        &[("GW_MESH_ADOPT_DROP", "1")],
    );
    let mut broker_w = start_broker_with_wal(
        port_w,
        "W",
        Some(("E".to_string(), port_e)),
        wal_w.clone(),
        None,
    );
    sleep(Duration::from_millis(900));

    let mut owner_w = connect_worker(port_w, "w-owner", "W", &["physics"]);
    create_entity(&mut owner_w, "in-flight-ship", "W", -1.0, "pre-handoff");
    assert_eq!(
        entity_query(port_w, "pre-handoff")
            .get("count")
            .and_then(Value::as_u64),
        Some(1)
    );

    let drain = drain_broker(port_w, "send-in-flight");
    assert_eq!(
        drain.get("no_neighbour").and_then(Value::as_bool),
        Some(false),
        "source broker had no mesh neighbour: {drain}"
    );
    wait_for_broker_state(port_w, "source-in-flight", 0, 1);
    wait_for_broker_state(port_e, "target-pre-adopt", 0, 0);

    let manifest_w = snapshot_marker(port_w, "vector-source");
    let manifest_e = snapshot_marker(port_e, "vector-target");
    assert_eq!(
        manifest_w.get("entity_count").and_then(Value::as_u64),
        Some(0),
        "source cut still had a local entity instead of an in-flight handoff: {manifest_w}"
    );
    assert_eq!(
        manifest_w.get("pending_mesh").and_then(Value::as_u64),
        Some(1),
        "source cut did not name the in-flight handoff: {manifest_w}"
    );
    assert_eq!(
        manifest_e.get("entity_count").and_then(Value::as_u64),
        Some(0),
        "target adopted despite GW_MESH_ADOPT_DROP: {manifest_e}"
    );
    let offset_w = manifest_w
        .get("wal_offset")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("source manifest missing wal_offset: {manifest_w}"));
    let offset_e = manifest_e
        .get("wal_offset")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("target manifest missing wal_offset: {manifest_e}"));

    drop(owner_w);
    stop(&mut broker_w);
    stop(&mut broker_e);

    let mut restored_e = start_broker_with_wal(port_e, "E", None, wal_e.clone(), Some(offset_e));
    let mut restored_w = start_broker_with_wal(
        port_w,
        "W",
        Some(("E".to_string(), port_e)),
        wal_w.clone(),
        Some(offset_w),
    );

    let restored_e_frame = wait_for_broker_state(port_e, "target-restored", 1, 0);
    let restored_w_frame = wait_for_broker_state(port_w, "source-acked", 0, 0);
    let restored_query_e = entity_query(port_e, "target-restored-query");
    let restored_query_w = entity_query(port_w, "source-restored-query");

    stop(&mut restored_w);
    stop(&mut restored_e);
    let _ = std::fs::remove_file(&wal_w);
    let _ = std::fs::remove_file(&wal_e);

    assert_eq!(
        broker_u64(&restored_e_frame, "entity_count") + broker_u64(&restored_w_frame, "entity_count"),
        1,
        "restored vector did not conserve the in-flight entity exactly once: W={restored_w_frame}, E={restored_e_frame}"
    );
    assert_eq!(
        restored_query_e.get("count").and_then(Value::as_u64),
        Some(1),
        "target did not receive the restored in-flight handoff: {restored_query_e}"
    );
    assert!(
        entity_ids(&restored_query_e).contains(&"in-flight-ship".to_string()),
        "target restored a different entity: {restored_query_e}"
    );
    assert_eq!(
        restored_query_w.get("count").and_then(Value::as_u64),
        Some(0),
        "source kept a duplicate local entity after vector restore: {restored_query_w}"
    );
}

#[test]
fn entity_query_returns_asset_manifest_for_visible_dependencies_only() {
    let port = free_port();
    let mut broker = start_broker(port, "EARTH", None);
    let mut owner = connect_worker(port, "earth-owner-assets", "EARTH", &["physics"]);

    create_entity_with_components(
        &mut owner,
        "visible-ship",
        "EARTH",
        json!({
            "pos": [1.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/ship", "uri": "res://ships/ship.glb", "kind": "mesh", "hash": "sha256:ship"},
            "asset_dependencies": [
                {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material"},
                {"id": "tex/ship", "uri": "res://textures/ship.png", "kind": "texture"}
            ]
        }),
    );
    create_entity_with_components(
        &mut owner,
        "visible-crate",
        "EARTH",
        json!({
            "pos": [2.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/crate", "uri": "res://props/crate.glb", "kind": "mesh", "hash": "sha256:crate"},
            "asset_dependencies": [
                {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material"}
            ]
        }),
    );
    create_entity_with_components(
        &mut owner,
        "far-tower",
        "EARTH",
        json!({
            "pos": [100.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/far-tower", "uri": "res://props/far_tower.glb", "kind": "mesh"}
        }),
    );

    let response = entity_query_with_query(
        port,
        "asset-interest",
        json!({"type": "sphere", "center": [0.0, 0.0], "radius": 10.0}),
    );
    stop(&mut broker);

    assert_eq!(response.get("count").and_then(Value::as_u64), Some(2));
    let manifest = response
        .get("asset_manifest")
        .unwrap_or_else(|| panic!("EntityQueryResponse missing asset_manifest: {response}"));
    assert_eq!(
        manifest.get("count").and_then(Value::as_u64),
        Some(4),
        "manifest must dedupe visible entity assets/deps only: {manifest}"
    );
    let asset_ids: Vec<String> = manifest
        .get("assets")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("manifest missing assets: {manifest}"))
        .iter()
        .filter_map(|asset| asset.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    assert!(
        asset_ids.contains(&"mesh/ship".to_string()),
        "{asset_ids:?}"
    );
    assert!(
        asset_ids.contains(&"mesh/crate".to_string()),
        "{asset_ids:?}"
    );
    assert!(
        asset_ids.contains(&"mat/shared".to_string()),
        "{asset_ids:?}"
    );
    assert!(asset_ids.contains(&"tex/ship".to_string()), "{asset_ids:?}");
    assert!(
        !asset_ids.contains(&"mesh/far-tower".to_string()),
        "asset interest leaked a non-query-visible entity dependency: {asset_ids:?}"
    );
    assert_eq!(
        asset_ids
            .iter()
            .filter(|id| id.as_str() == "mat/shared")
            .count(),
        1,
        "shared dependency must be manifest-deduped: {asset_ids:?}"
    );
    let entity_assets = manifest
        .get("entity_assets")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("manifest missing entity_assets: {manifest}"));
    assert!(entity_assets.contains_key("visible-ship"));
    assert!(entity_assets.contains_key("visible-crate"));
    assert!(!entity_assets.contains_key("far-tower"));
}

#[test]
fn entity_query_returns_content_manifest_package_plan_for_visible_assets_only() {
    let port = free_port();
    let mut broker = start_broker(port, "EARTH", None);
    let mut owner = connect_worker(port, "earth-owner-content", "EARTH", &["physics"]);

    create_entity_with_components(
        &mut owner,
        "visible-ship",
        "EARTH",
        json!({
            "pos": [1.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/ship", "uri": "res://ships/ship.glb", "kind": "mesh", "package": "ships/base", "hash": "sha256:ship"},
            "asset_dependencies": [
                {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material", "package": "common/materials", "hash": "sha256:shared"},
                {"id": "tex/ship", "uri": "res://textures/ship.png", "kind": "texture", "package": "ships/base", "hash": "sha256:shiptex"}
            ]
        }),
    );
    create_entity_with_components(
        &mut owner,
        "visible-crate",
        "EARTH",
        json!({
            "pos": [2.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/crate", "uri": "res://props/crate.glb", "kind": "mesh", "package": "props/base", "hash": "sha256:crate"},
            "asset_dependencies": [
                {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material", "package": "common/materials", "hash": "sha256:shared"}
            ]
        }),
    );
    create_entity_with_components(
        &mut owner,
        "far-secret",
        "EARTH",
        json!({
            "pos": [100.0, 0.0],
            "vel": [0.0, 0.0],
            "asset": {"id": "mesh/secret", "uri": "res://secret/secret.glb", "kind": "mesh", "package": "secret/pkg"}
        }),
    );

    let response = entity_query_with_query(
        port,
        "content-interest",
        json!({"type": "sphere", "center": [0.0, 0.0], "radius": 10.0}),
    );
    stop(&mut broker);

    assert_eq!(response.get("count").and_then(Value::as_u64), Some(2));
    let manifest = response
        .get("content_manifest")
        .unwrap_or_else(|| panic!("EntityQueryResponse missing content_manifest: {response}"));
    assert_eq!(manifest.get("version").and_then(Value::as_u64), Some(1));
    assert_eq!(manifest.get("asset_count").and_then(Value::as_u64), Some(4));
    assert_eq!(
        manifest.get("package_count").and_then(Value::as_u64),
        Some(3)
    );

    let packages = manifest
        .get("packages")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("content manifest missing packages: {manifest}"));
    let package_ids: Vec<String> = packages
        .iter()
        .filter_map(|pkg| pkg.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    for required in ["common/materials", "props/base", "ships/base"] {
        assert!(
            package_ids.contains(&required.to_string()),
            "missing package {required}: {manifest}"
        );
    }
    assert!(
        !package_ids.contains(&"secret/pkg".to_string()),
        "content manifest leaked a non-visible package: {manifest}"
    );

    let ships_pkg = packages
        .iter()
        .find(|pkg| pkg.get("id").and_then(Value::as_str) == Some("ships/base"))
        .unwrap_or_else(|| panic!("missing ships/base package: {manifest}"));
    assert_eq!(
        ships_pkg.get("asset_count").and_then(Value::as_u64),
        Some(2)
    );
    let ship_assets: Vec<String> = ships_pkg
        .get("assets")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("ships/base missing assets: {ships_pkg}"))
        .iter()
        .filter_map(|asset| asset.as_str().map(str::to_string))
        .collect();
    assert!(ship_assets.contains(&"mesh/ship".to_string()));
    assert!(ship_assets.contains(&"tex/ship".to_string()));
    assert!(
        ships_pkg
            .get("hashes")
            .and_then(Value::as_object)
            .map(|hashes| hashes.contains_key("mesh/ship"))
            .unwrap_or(false),
        "package plan must carry asset hashes when present: {ships_pkg}"
    );

    let entity_packages = manifest
        .get("entity_packages")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("content manifest missing entity_packages: {manifest}"));
    let ship_packages: Vec<String> = entity_packages
        .get("visible-ship")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing visible-ship packages: {manifest}"))
        .iter()
        .filter_map(|package| package.as_str().map(str::to_string))
        .collect();
    assert!(ship_packages.contains(&"common/materials".to_string()));
    assert!(ship_packages.contains(&"ships/base".to_string()));
    assert!(!entity_packages.contains_key("far-secret"));
}

#[test]
fn entity_query_returns_schema_manifest_for_visible_components_only() {
    let port = free_port();
    let mut broker = start_broker(port, "EARTH", None);
    let mut owner = connect_worker(port, "earth-owner-schema", "EARTH", &["physics"]);

    create_entity_with_components(
        &mut owner,
        "visible-probe",
        "EARTH",
        json!({
            "pos": [1.0, 0.0],
            "vel": [0.0, 0.0],
            "physics": {
                "pos": [1.0, 0.0, 0.0],
                "rot": [0.0, 0.0, 0.0, 1.0],
                "lin": [0.1, 0.0, 0.0],
                "ang": [0.0, 0.0, 0.0],
                "at_rest": false,
                "gen": 7,
                "t_server": 10,
                "sim_time": 20
            },
            "asset": {"id": "mesh/probe", "uri": "res://probe.glb", "kind": "mesh"},
            "asset_dependencies": [{"id": "mat/probe", "kind": "material"}]
        }),
    );
    create_entity_with_components(
        &mut owner,
        "far-secret",
        "EARTH",
        json!({
            "pos": [100.0, 0.0],
            "vel": [0.0, 0.0],
            "hidden_logic": {"script": "server-only"}
        }),
    );

    let response = entity_query_with_query(
        port,
        "schema-interest",
        json!({"type": "sphere", "center": [0.0, 0.0], "radius": 10.0}),
    );
    stop(&mut broker);

    assert_eq!(response.get("count").and_then(Value::as_u64), Some(1));
    let manifest = response
        .get("schema_manifest")
        .unwrap_or_else(|| panic!("EntityQueryResponse missing schema_manifest: {response}"));
    assert_eq!(manifest.get("abi_version").and_then(Value::as_u64), Some(1));

    let component_names: Vec<String> = manifest
        .get("components")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("schema manifest missing components: {manifest}"))
        .iter()
        .filter_map(|component| {
            component
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    assert!(
        component_names.contains(&"physics".to_string()),
        "{component_names:?}"
    );
    assert!(
        component_names.contains(&"asset".to_string()),
        "{component_names:?}"
    );
    assert!(
        component_names.contains(&"asset_dependencies".to_string()),
        "{component_names:?}"
    );
    assert!(
        !component_names.contains(&"hidden_logic".to_string()),
        "schema manifest leaked a non-visible entity component: {component_names:?}"
    );

    let entity_components = manifest
        .get("entity_components")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("schema manifest missing entity_components: {manifest}"));
    let visible_components: Vec<String> = entity_components
        .get("visible-probe")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing visible-probe components: {manifest}"))
        .iter()
        .filter_map(|component| component.as_str().map(str::to_string))
        .collect();
    assert!(visible_components.contains(&"physics".to_string()));
    assert!(visible_components.contains(&"asset".to_string()));
    assert!(!entity_components.contains_key("far-secret"));

    let physics_schema = manifest
        .get("components")
        .and_then(Value::as_array)
        .and_then(|components| {
            components
                .iter()
                .find(|component| component.get("name").and_then(Value::as_str) == Some("physics"))
        })
        .and_then(|component| component.get("schemas"))
        .and_then(Value::as_array)
        .and_then(|schemas| schemas.first())
        .unwrap_or_else(|| panic!("missing physics schema: {manifest}"));
    let physics_fields = physics_schema
        .get("fields")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("physics schema missing fields: {physics_schema}"));
    for field in [
        "pos", "rot", "lin", "ang", "at_rest", "gen", "t_server", "sim_time",
    ] {
        assert!(
            physics_fields.contains_key(field),
            "missing {field}: {physics_schema}"
        );
    }
}

#[test]
fn entity_query_supports_qbi_boolean_constraint_ast() {
    let port = free_port();
    let mut broker = start_broker(port, "EARTH", None);
    let mut owner = connect_worker(port, "earth-owner-qbi", "EARTH", &["physics"]);

    create_entity_with_components(
        &mut owner,
        "target-ore",
        "EARTH",
        json!({
            "pos": [2.0, 1.0],
            "vel": [0.0, 0.0],
            "ore_resource": {"tier": 5, "grade": "rich"},
            "asset": {"id": "ore/metal", "kind": "ore"},
            "stats": {"yields": [100, 25]}
        }),
    );
    create_entity_with_components(
        &mut owner,
        "wrong-component",
        "EARTH",
        json!({
            "pos": [1.0, 1.0],
            "vel": [0.0, 0.0],
            "hidden_logic": {"script": "server-only"}
        }),
    );
    create_entity_with_components(
        &mut owner,
        "far-ore",
        "EARTH",
        json!({
            "pos": [50.0, 0.0],
            "vel": [0.0, 0.0],
            "ore_resource": {"tier": 5, "grade": "rich"},
            "asset": {"id": "ore/far", "kind": "ore"},
            "stats": {"yields": [100, 25]}
        }),
    );
    create_entity_with_components(
        &mut owner,
        "poor-ore",
        "EARTH",
        json!({
            "pos": [2.5, 1.0],
            "vel": [0.0, 0.0],
            "ore_resource": {"tier": 3, "grade": "poor"},
            "asset": {"id": "ore/poor", "kind": "ore"},
            "stats": {"yields": [40, 10]}
        }),
    );

    let response = entity_query_with_query(
        port,
        "qbi-ast",
        json!({
            "type": "and",
            "constraints": [
                {"type": "sphere", "center": [0.0, 0.0], "radius": 5.0},
                {"type": "box", "min": [0.0, 0.0], "max": [5.0, 3.0]},
                {
                    "type": "or",
                    "constraints": [
                        {"type": "component", "comp": "ore_resource"},
                        {"type": "entity", "entity": "target-ore"}
                    ]
                },
                {"type": "value", "path": ["asset", "kind"], "eq": "ore"},
                {"type": "value", "component": "ore_resource", "field": "tier", "gte": 5},
                {"type": "field", "path": "stats.yields.0", "gte": 100},
                {"type": "not", "constraint": {"type": "component", "comp": "hidden_logic"}},
                {"type": "region", "region": "EARTH"}
            ]
        }),
    );
    stop(&mut broker);

    assert_eq!(response.get("count").and_then(Value::as_u64), Some(1));
    assert_eq!(entity_ids(&response), vec!["target-ore".to_string()]);
}
