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

fn count(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

fn parse_done_value(stderr: &str, key: &str) -> Option<usize> {
    let done = stderr
        .lines()
        .rev()
        .find(|line| line.contains("done tick="))?;
    for part in done.split_whitespace() {
        if let Some(v) = part.strip_prefix(&format!("{key}=")) {
            return v.parse().ok();
        }
    }
    None
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
    assert_eq!(count(&stderr, "AUTH-GAIN"), 100, "{stderr}");
    assert_eq!(count(&stderr, "AUTH-LOSS"), 0, "{stderr}");
    assert_eq!(count(&stderr, "REJECTED"), 0, "{stderr}");
    assert_eq!(parse_done_value(&stderr, "owned"), Some(100), "{stderr}");
    assert_eq!(parse_done_value(&stderr, "rejects"), Some(0), "{stderr}");
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
