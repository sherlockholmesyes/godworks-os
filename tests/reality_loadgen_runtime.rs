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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_godworks_broker"));
    cmd.env("GW_HOST", "127.0.0.1")
        .env("GW_PORT", port.to_string())
        .env("GW_WAL", wal)
        .env("GW_DURABLE_FLUSH_MS", "5")
        .env("GW_ADVERTISE", format!("{region}=127.0.0.1:{port}"))
        .env_remove("GW_MESH_EAST")
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
    let request_id = format!("create-{eid}");
    stream
        .write_all(&frame(&json!({
            "op": "CreateEntity",
            "request_id": request_id,
            "entity": eid,
            "region": region,
            "components": {
                "pos": [x, 0.0],
                "vel": [0.0, 0.0],
                "kind": tag,
            },
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

fn entity_query(port: u16, request_id: &str) -> Value {
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
            "query": {"type": "all"},
        })))
        .expect("send EntityQuery");
    wait_for_response(&mut stream, "EntityQueryResponse", request_id)
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
        .env("GW_REQUIRE_PHYSICS_PAYLOAD", "1")
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
        metric_u64(&metrics, "physics_payload_ok"),
        4,
        "full physics payload did not survive cross-broker handoff and E-side write: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "physics_clock_ok"),
        4,
        "physics gen/sim_time/t_server were not monotonic after handoff: {metrics:?}"
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
