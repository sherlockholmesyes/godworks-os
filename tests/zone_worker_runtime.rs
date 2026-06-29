use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

fn start_broker(label: &str, port: u16) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_godworks_broker"))
        .env("GW_HOST", "127.0.0.1")
        .env("GW_PORT", port.to_string())
        .env("GW_WAL", unique_wal(label))
        .env("GW_DURABLE_FLUSH_MS", "5")
        .env("GW_BOUNDARY", "0")
        .env_remove("GW_BOUNDARIES")
        .env_remove("GW_GRID2D")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn broker");
    wait_for_port(port);
    child
}

#[allow(clippy::too_many_arguments)]
fn start_worker(
    port: u16,
    region: &str,
    worker_id: &str,
    spawn_n: usize,
    spawn_box: Option<&str>,
    duration: &str,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_zone_worker"));
    cmd.env("GW_ZW_HOST", "127.0.0.1")
        .env("GW_ZW_PORT", port.to_string())
        .env("GW_ZW_REGION", region)
        .env("GW_ZW_ID", worker_id)
        .env("GW_ZW_SPAWN", spawn_n.to_string())
        .env("GW_ZW_DURATION", duration)
        .env("GW_ZW_HZ", "30")
        .env("GW_ZW_SPAWN_VEL", "0,0")
        .env("GW_ZW_SPAWN_SPEED", "0")
        .env("GW_ZW_SEED", "42")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(b) = spawn_box {
        cmd.env("GW_ZW_SPAWN_BOX", b);
    } else {
        cmd.env_remove("GW_ZW_SPAWN_BOX");
    }
    cmd.spawn().expect("spawn zone_worker")
}

#[allow(clippy::too_many_arguments)]
fn start_worker_with_motion(
    port: u16,
    region: &str,
    worker_id: &str,
    spawn_n: usize,
    spawn_box: Option<&str>,
    spawn_vel: &str,
    spawn_speed: &str,
    duration: &str,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_zone_worker"));
    cmd.env("GW_ZW_HOST", "127.0.0.1")
        .env("GW_ZW_PORT", port.to_string())
        .env("GW_ZW_REGION", region)
        .env("GW_ZW_ID", worker_id)
        .env("GW_ZW_SPAWN", spawn_n.to_string())
        .env("GW_ZW_DURATION", duration)
        .env("GW_ZW_HZ", "30")
        .env("GW_ZW_SPAWN_VEL", spawn_vel)
        .env("GW_ZW_SPAWN_SPEED", spawn_speed)
        .env("GW_ZW_SEED", "4242")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(b) = spawn_box {
        cmd.env("GW_ZW_SPAWN_BOX", b);
    } else {
        cmd.env_remove("GW_ZW_SPAWN_BOX");
    }
    cmd.spawn().expect("spawn zone_worker")
}

fn stop(child: &mut Child) {
    if child.try_wait().expect("try_wait").is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn wait_output(child: Child) -> Output {
    child.wait_with_output().expect("wait worker")
}

fn stderr_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn parse_summary(stderr: &str) -> Value {
    const PREFIX: &str = "zone_worker_summary ";
    let line = stderr
        .lines()
        .rev()
        .find(|line| line.starts_with(PREFIX))
        .unwrap_or_else(|| panic!("missing zone_worker_summary line\n{stderr}"));
    serde_json::from_str(line.trim_start_matches(PREFIX))
        .unwrap_or_else(|err| panic!("invalid zone_worker_summary json: {err}\nline={line}"))
}

fn summary_usize(summary: &Value, key: &str) -> usize {
    summary
        .get(key)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("summary missing numeric key {key}: {summary}")) as usize
}

fn extract_eid(line: &str) -> Option<String> {
    let rest = line.split("e=").nth(1)?;
    Some(rest.split_whitespace().next()?.to_string())
}

fn parse_spawn_x(line: &str) -> Option<(String, f64)> {
    let eid = extract_eid(line)?;
    let pos = line.split("pos=[").nth(1)?;
    let x = pos.split(',').next()?.parse::<f64>().ok()?;
    Some((eid, x))
}

fn frame(value: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("json");
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn read_frame(stream: &mut TcpStream) -> Value {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).expect("read frame hdr");
    let n = u32::from_be_bytes(hdr) as usize;
    let mut body = vec![0u8; n];
    stream.read_exact(&mut body).expect("read frame body");
    serde_json::from_slice(&body).expect("json frame")
}

fn inspector_frame(port: u16, request_id: &str) -> Value {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect inspector");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set inspector read timeout");
    stream
        .write_all(&frame(&json!({
            "op": "WorkerConnect",
            "worker_id": format!("inspector-{request_id}"),
            "region": "MESH",
            "attributes": ["inspector"]
        })))
        .expect("connect frame");
    stream
        .write_all(&frame(&json!({
            "op": "InspectorQuery",
            "request_id": request_id,
            "max_entities": 500
        })))
        .expect("query frame");
    for _ in 0..256 {
        let f = read_frame(&mut stream);
        if f.get("op").and_then(Value::as_str) == Some("InspectorFrame")
            && f.get("request_id").and_then(Value::as_str) == Some(request_id)
        {
            return f;
        }
    }
    panic!("InspectorFrame not received");
}

#[test]
fn create_storm_named_region_every_created_entity_ends_owned() {
    let port = free_port();
    let mut broker = start_broker("named_region_retain", port);
    let worker_id = "zw-EARTH-retain";
    let worker = start_worker(port, "EARTH", worker_id, 100, Some("-18,18,-18,18"), "2.0");
    let out = wait_output(worker);
    stop(&mut broker);
    assert!(out.status.success(), "zone_worker failed: {:?}", out.status);
    let stderr = stderr_text(&out);
    let summary = parse_summary(&stderr);
    assert_eq!(summary.get("region").and_then(Value::as_str), Some("EARTH"));
    assert_eq!(summary_usize(&summary, "auth_gain"), 100, "{stderr}");
    assert_eq!(summary_usize(&summary, "auth_loss"), 0, "{stderr}");
    assert_eq!(summary_usize(&summary, "rejects"), 0, "{stderr}");
    assert_eq!(summary_usize(&summary, "owned"), 100, "{stderr}");
}

#[test]
fn create_storm_w_strip_gain_matches_position_derived_region() {
    let port = free_port();
    let mut broker = start_broker("w_strip_position", port);
    let worker = start_worker(port, "W", "zw-W-position", 100, None, "2.0");
    let out = wait_output(worker);
    stop(&mut broker);
    assert!(out.status.success(), "zone_worker failed: {:?}", out.status);
    let stderr = stderr_text(&out);

    let spawns: HashMap<String, f64> = stderr.lines().filter_map(parse_spawn_x).collect();
    let gained: Vec<String> = stderr
        .lines()
        .filter(|line| line.contains("AUTH-GAIN"))
        .filter_map(extract_eid)
        .collect();
    let initial_x_neg = spawns.values().filter(|x| **x < 0.0).count();

    assert_eq!(spawns.len(), 100, "{stderr}");
    assert_eq!(initial_x_neg, 50, "{stderr}");
    assert_eq!(gained.len(), initial_x_neg, "{stderr}");
    for eid in gained {
        let x = spawns
            .get(&eid)
            .copied()
            .unwrap_or_else(|| panic!("AUTH-GAIN without spawn line: {eid}\n{stderr}"));
        assert!(
            x < 0.0,
            "W worker gained {eid} even though its initial x was {x}\n{stderr}"
        );
    }
}

#[test]
fn dense_seam_with_matching_e_worker_conserves_authority() {
    let port = free_port();
    let mut broker = start_broker("dense_seam_conservation", port);
    let mut east = start_worker(port, "E", "zw-E-conserve", 0, None, "6.0");
    sleep(Duration::from_millis(250));
    let mut west = start_worker(
        port,
        "W",
        "zw-W-conserve",
        100,
        Some("-18,18,-18,18"),
        "6.0",
    );
    sleep(Duration::from_millis(2200));

    let frame = inspector_frame(port, "dense-seam");
    stop(&mut west);
    stop(&mut east);
    stop(&mut broker);

    let entities = frame
        .get("entities")
        .and_then(Value::as_array)
        .expect("entities array");
    let real_entities: Vec<&Value> = entities
        .iter()
        .filter(|e| e.get("ghost").and_then(Value::as_bool) != Some(true))
        .collect();
    assert_eq!(
        real_entities.len(),
        100,
        "InspectorFrame did not conserve entity count: {frame}"
    );

    let mut w_owned = 0usize;
    let mut e_owned = 0usize;
    let mut bad = Vec::new();
    for entity in real_entities {
        match entity
            .get("authority")
            .and_then(|a| a.get("pos"))
            .and_then(|p| p.get("owner"))
            .and_then(Value::as_str)
        {
            Some("zw-W-conserve") => w_owned += 1,
            Some("zw-E-conserve") => e_owned += 1,
            other => bad.push(json!({"entity": entity.get("entity"), "owner": other})),
        }
    }
    assert!(
        bad.is_empty(),
        "entities without exactly one W/E pos owner: {bad:?}\nframe={frame}"
    );
    assert_eq!(
        w_owned + e_owned,
        100,
        "authority was not conserved across seam: W={w_owned}, E={e_owned}, frame={frame}"
    );
}

#[test]
fn moving_w_bodies_handoff_to_e_without_rejects_or_stale_ownership() {
    let port = free_port();
    let mut broker = start_broker("moving_w_to_e_lifecycle", port);
    let east = start_worker_with_motion(port, "E", "zw-E-lifecycle", 0, None, "0,0", "0", "5.0");
    sleep(Duration::from_millis(250));
    let west = start_worker_with_motion(
        port,
        "W",
        "zw-W-lifecycle",
        24,
        Some("-4,-2,-1,1"),
        "10,0",
        "0",
        "5.0",
    );

    let west_out = wait_output(west);
    let east_out = wait_output(east);
    let frame = inspector_frame(port, "moving-w-to-e");
    stop(&mut broker);

    assert!(
        west_out.status.success(),
        "west zone_worker failed: {:?}",
        west_out.status
    );
    assert!(
        east_out.status.success(),
        "east zone_worker failed: {:?}",
        east_out.status
    );
    let west_stderr = stderr_text(&west_out);
    let east_stderr = stderr_text(&east_out);
    let west_summary = parse_summary(&west_stderr);
    let east_summary = parse_summary(&east_stderr);

    assert_eq!(
        summary_usize(&west_summary, "auth_gain"),
        24,
        "west did not gain all spawned bodies\n{west_stderr}"
    );
    assert_eq!(
        summary_usize(&west_summary, "auth_loss"),
        24,
        "west did not release every crossing body\nwest={west_stderr}\neast={east_stderr}"
    );
    assert!(
        summary_usize(&east_summary, "auth_gain") >= 24,
        "east did not adopt the crossing bodies\nwest={west_stderr}\neast={east_stderr}"
    );
    assert_eq!(summary_usize(&west_summary, "rejects"), 0, "{west_stderr}");
    assert_eq!(summary_usize(&east_summary, "rejects"), 0, "{east_stderr}");
    assert_eq!(summary_usize(&west_summary, "owned"), 0, "{west_stderr}");

    let entities = frame
        .get("entities")
        .and_then(Value::as_array)
        .expect("entities array");
    let real_entities: Vec<&Value> = entities
        .iter()
        .filter(|e| e.get("ghost").and_then(Value::as_bool) != Some(true))
        .collect();
    assert_eq!(
        real_entities.len(),
        24,
        "entity lifecycle lost bodies: {frame}"
    );

    for entity in real_entities {
        let owner = entity
            .get("authority")
            .and_then(|a| a.get("pos"))
            .and_then(|p| p.get("owner"))
            .and_then(Value::as_str);
        assert_eq!(
            owner,
            Some("zw-E-lifecycle"),
            "crossed entity retained stale/non-east authority: {entity}"
        );
    }
}
