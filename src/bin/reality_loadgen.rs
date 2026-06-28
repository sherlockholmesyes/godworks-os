//! godworks reality_loadgen
//!
//! A product/reality harness for the broker op-mix, not a raw writes/sec microbench.
//! It drives the live wire through create, interest/AOI, multi-fidelity updates,
//! EntityEvent, CommandRequest/Response, local or cross-broker seam movement, and an
//! optional slow viewer that does not drain its socket.
//!
//! Topologies:
//! - one broker: GW_TARGET=7777, owner W and owner E both connect to it.
//! - two brokers: GW_TARGET=7801 GW_TARGET_E=7802, with the W broker configured to
//!   mesh E externally. The harness only speaks the public worker protocol.
//!
//! Parseable final line starts with:
//!   reality_loadgen result=pass|fail ...

use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

fn frame(v: &Value) -> Vec<u8> {
    let b = serde_json::to_vec(v).unwrap();
    let n = b.len() as u32;
    let mut o = Vec::with_capacity(4 + b.len());
    o.extend_from_slice(&n.to_be_bytes());
    o.extend_from_slice(&b);
    o
}

async fn read_frame<R: AsyncReadExt + Unpin>(rd: &mut R) -> Option<Value> {
    let mut h = [0u8; 4];
    rd.read_exact(&mut h).await.ok()?;
    let n = u32::from_be_bytes(h) as usize;
    let mut b = vec![0u8; n];
    rd.read_exact(&mut b).await.ok()?;
    serde_json::from_slice(&b).ok()
}

async fn send_json(wr: &Arc<Mutex<OwnedWriteHalf>>, v: &Value) -> std::io::Result<()> {
    let mut wr = wr.lock().await;
    wr.write_all(&frame(v)).await
}

async fn write_raw(stream: &mut TcpStream, v: &Value) -> std::io::Result<()> {
    stream.write_all(&frame(v)).await
}

#[derive(Default)]
struct Counters {
    add_entity: AtomicU64,
    remove_entity: AtomicU64,
    component_update: AtomicU64,
    coarse_update: AtomicU64,
    authority_gain: AtomicU64,
    authority_loss: AtomicU64,
    update_rejected: AtomicU64,
    handoff_probe_rejected: AtomicU64,
    entity_event: AtomicU64,
    visual_event: AtomicU64,
    command_request: AtomicU64,
    command_response: AtomicU64,
    entity_query_response: AtomicU64,
    mesh_ghost: AtomicU64,
    disconnects: AtomicU64,
    frames: AtomicU64,
}

impl Counters {
    fn add(&self, op: &str, f: &Value) {
        self.frames.fetch_add(1, Ordering::Relaxed);
        match op {
            "AddEntity" => {
                self.add_entity.fetch_add(1, Ordering::Relaxed);
            }
            "RemoveEntity" => {
                self.remove_entity.fetch_add(1, Ordering::Relaxed);
            }
            "ComponentUpdate" => {
                self.component_update.fetch_add(1, Ordering::Relaxed);
                if f.get("fidelity").and_then(|v| v.as_str()) == Some("coarse") {
                    self.coarse_update.fetch_add(1, Ordering::Relaxed);
                }
            }
            "AuthorityChange" => {
                if f.get("authoritative")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.authority_gain.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.authority_loss.fetch_add(1, Ordering::Relaxed);
                }
            }
            "UpdateRejected" => {
                self.update_rejected.fetch_add(1, Ordering::Relaxed);
                if f.get("comp").and_then(|v| v.as_str()) == Some("handoff_probe") {
                    self.handoff_probe_rejected.fetch_add(1, Ordering::Relaxed);
                }
            }
            "EntityEvent" => {
                self.entity_event.fetch_add(1, Ordering::Relaxed);
                if f.get("class").and_then(|v| v.as_str()) == Some("visual") {
                    self.visual_event.fetch_add(1, Ordering::Relaxed);
                }
            }
            "CommandRequest" => {
                self.command_request.fetch_add(1, Ordering::Relaxed);
            }
            "CommandResponse" => {
                self.command_response.fetch_add(1, Ordering::Relaxed);
            }
            "EntityQueryResponse" => {
                self.entity_query_response.fetch_add(1, Ordering::Relaxed);
            }
            "MeshGhost" => {
                self.mesh_ghost.fetch_add(1, Ordering::Relaxed);
            }
            "Disconnect" => {
                self.disconnects.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn get(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }
}

struct Endpoint {
    rd: OwnedReadHalf,
    wr: Arc<Mutex<OwnedWriteHalf>>,
}

async fn connect_endpoint(
    host: &str,
    port: u16,
    wid: &str,
    region: &str,
    attributes: &[&str],
) -> std::io::Result<Endpoint> {
    let stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true).ok();
    let (rd, mut wr) = stream.into_split();
    wr.write_all(&frame(&json!({
        "op":"WorkerConnect",
        "worker_id":wid,
        "region":region,
        "attributes":attributes,
        "proto":1
    })))
    .await?;
    Ok(Endpoint {
        rd,
        wr: Arc::new(Mutex::new(wr)),
    })
}

async fn connect_slow_viewer(host: &str, port: u16, wid: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true).ok();
    write_raw(
        &mut stream,
        &json!({"op":"WorkerConnect","worker_id":wid,"region":"OBS","attributes":["observer"],"proto":1}),
    )
    .await?;
    write_raw(
        &mut stream,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":5.0,"coarse_rate":3,"coarse_grid":0.25}),
    )
    .await?;
    Ok(stream)
}

fn spawn_reader(
    name: &'static str,
    mut rd: OwnedReadHalf,
    counters: Arc<Counters>,
    responder: Option<Arc<Mutex<OwnedWriteHalf>>>,
    stop: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            let Some(f) = read_frame(&mut rd).await else {
                break;
            };
            let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
            counters.add(op, &f);
            if op == "CommandRequest" {
                if let Some(wr) = responder.as_ref() {
                    let req_id = f
                        .get("request_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let entity = f
                        .get("entity")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let _ = send_json(
                        wr,
                        &json!({
                            "op":"CommandResponse",
                            "request_id":req_id,
                            "success":true,
                            "payload":{"handled_by":name,"entity":entity}
                        }),
                    )
                    .await;
                }
            }
        }
    });
}

fn env_u16(k: &str, default: u16) -> u16 {
    env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(k: &str, default: u64) -> u64 {
    env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(k: &str, default: f64) -> f64 {
    env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn wait_until<F>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> bool,
{
    let started = Instant::now();
    while started.elapsed() < timeout {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    pred()
}

async fn query_entities(host: &str, port: u16, request_id: &str) -> std::io::Result<Value> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true).ok();
    write_raw(
        &mut stream,
        &json!({"op":"WorkerConnect","worker_id":format!("rlg-query-{request_id}"),"region":"OBS","attributes":["observer"],"proto":1}),
    )
    .await?;
    write_raw(
        &mut stream,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0}),
    )
    .await?;
    write_raw(
        &mut stream,
        &json!({"op":"EntityQuery","request_id":request_id,"query":{"type":"sphere","center":[0.0,0.0],"radius":100.0}}),
    )
    .await?;
    for _ in 0..256 {
        if let Some(frame) = read_frame(&mut stream).await {
            if frame.get("op").and_then(|v| v.as_str()) == Some("EntityQueryResponse")
                && frame.get("request_id").and_then(|v| v.as_str()) == Some(request_id)
            {
                return Ok(frame);
            }
        } else {
            break;
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "EntityQueryResponse not received",
    ))
}

fn count_handoff_probe_ok(query: &Value, entities: u64) -> u64 {
    let Some(rows) = query.get("entities").and_then(|v| v.as_array()) else {
        return 0;
    };
    let mut ok = 0u64;
    for i in 0..entities {
        let eid = format!("rlg-body-{i}");
        let matched = rows.iter().any(|row| {
            row.get("entity").and_then(|v| v.as_str()) == Some(eid.as_str())
                && row
                    .get("components")
                    .and_then(|c| c.get("handoff_probe"))
                    .and_then(|p| p.get("writer"))
                    .and_then(|v| v.as_str())
                    == Some("E")
        });
        if matched {
            ok += 1;
        }
    }
    ok
}

fn body_y(i: u64) -> f64 {
    (i as f64 % 8.0) * 0.35 - 1.2
}

fn initial_physics_gen(i: u64) -> u64 {
    10 + i
}

fn initial_physics_sim_time(i: u64) -> u64 {
    1_000 + i
}

fn initial_physics_t_server(i: u64) -> u64 {
    100 + i
}

fn initial_physics_payload(i: u64, x: f64, y: f64) -> Value {
    json!({
        "pos":[x, y, 0.0],
        "rot":[0.0, 0.0, 0.0, 1.0],
        "lin":[0.08, 0.0, 0.0],
        "ang":[0.0, 0.0, 0.0],
        "at_rest":false,
        "gen":initial_physics_gen(i),
        "t_server":initial_physics_t_server(i),
        "sim_time":initial_physics_sim_time(i),
        "writer":"W_INIT"
    })
}

fn e_physics_payload(i: u64, x: f64, y: f64) -> Value {
    json!({
        "pos":[x, y, 0.0],
        "rot":[0.0, 0.0, 0.70710678, 0.70710678],
        "lin":[0.08, 0.01 * i as f64, 0.0],
        "ang":[0.0, 0.25, 0.0],
        "at_rest":false,
        "gen":0,
        "t_server":0,
        "sim_time":0,
        "writer":"E"
    })
}

fn value_f64_at(v: &Value, idx: usize) -> Option<f64> {
    v.as_array()?.get(idx)?.as_f64()
}

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() <= 0.000001
}

fn array_matches(actual: Option<&Value>, expected: &[f64]) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    expected.iter().enumerate().all(|(idx, want)| {
        value_f64_at(actual, idx)
            .map(|got| approx(got, *want))
            .unwrap_or(false)
    })
}

fn count_physics_payload_ok(query: &Value, entities: u64) -> u64 {
    let Some(rows) = query.get("entities").and_then(|v| v.as_array()) else {
        return 0;
    };
    let mut ok = 0u64;
    for i in 0..entities {
        let eid = format!("rlg-body-{i}");
        let y = body_y(i);
        let x = 3.25 + i as f64 * 0.01;
        let matched = rows.iter().any(|row| {
            if row.get("entity").and_then(|v| v.as_str()) != Some(eid.as_str()) {
                return false;
            }
            let Some(physics) = row.get("components").and_then(|c| c.get("physics")) else {
                return false;
            };
            physics.get("writer").and_then(|v| v.as_str()) == Some("E")
                && physics.get("at_rest").and_then(|v| v.as_bool()) == Some(false)
                && array_matches(physics.get("pos"), &[x, y, 0.0])
                && array_matches(physics.get("rot"), &[0.0, 0.0, 0.70710678, 0.70710678])
                && array_matches(physics.get("lin"), &[0.08, 0.01 * i as f64, 0.0])
                && array_matches(physics.get("ang"), &[0.0, 0.25, 0.0])
        });
        if matched {
            ok += 1;
        }
    }
    ok
}

fn count_physics_clock_ok(query: &Value, entities: u64) -> u64 {
    let Some(rows) = query.get("entities").and_then(|v| v.as_array()) else {
        return 0;
    };
    let mut ok = 0u64;
    for i in 0..entities {
        let eid = format!("rlg-body-{i}");
        let matched = rows.iter().any(|row| {
            if row.get("entity").and_then(|v| v.as_str()) != Some(eid.as_str()) {
                return false;
            }
            let Some(physics) = row.get("components").and_then(|c| c.get("physics")) else {
                return false;
            };
            let gen_ok =
                physics.get("gen").and_then(|v| v.as_u64()) == Some(initial_physics_gen(i) + 1);
            let sim_ok = physics.get("sim_time").and_then(|v| v.as_u64())
                == Some(initial_physics_sim_time(i) + 16);
            let t_server_ok = physics
                .get("t_server")
                .and_then(|v| v.as_u64())
                .map(|v| v > initial_physics_t_server(i))
                .unwrap_or(false);
            gen_ok && sim_ok && t_server_ok
        });
        if matched {
            ok += 1;
        }
    }
    ok
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let host = env::var("GW_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port_w = env_u16("GW_TARGET", 7777);
    let port_e = env_u16("GW_TARGET_E", port_w);
    let entities = env_u64("GW_ENTITIES", 12).max(1);
    let ticks = env_u64("GW_TICKS", 90).max(1);
    let hz = env_f64("GW_HZ", 30.0).max(1.0);
    let event_burst = env_u64("GW_EVENT_BURST", 16);
    let enable_slow = env::var("GW_SLOW_VIEWER")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(true);
    let require_mesh = env::var("GW_REQUIRE_MESH")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(port_e != port_w);
    let require_writer_swap = env::var("GW_REQUIRE_WRITER_SWAP")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(true);
    let require_physics_payload = env::var("GW_REQUIRE_PHYSICS_PAYLOAD")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(true);
    let cross_broker = port_e != port_w;
    let dt = Duration::from_secs_f64(1.0 / hz);
    let stop = Arc::new(AtomicBool::new(false));
    let all = Arc::new(Counters::default());
    let east = Arc::new(Counters::default());
    let caller_counters = Arc::new(Counters::default());

    let owner_w = connect_endpoint(&host, port_w, "rlg-owner-W", "W", &["physics", "server"])
        .await
        .unwrap_or_else(|e| panic!("connect owner W {host}:{port_w}: {e}"));
    let owner_w_wr = owner_w.wr.clone();
    spawn_reader(
        "owner-W",
        owner_w.rd,
        all.clone(),
        Some(owner_w_wr.clone()),
        stop.clone(),
    );
    send_json(
        &owner_w_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":8.0,"coarse_rate":2,"coarse_grid":0.2}),
    )
    .await
    .unwrap();

    let owner_e = connect_endpoint(&host, port_e, "rlg-owner-E", "E", &["physics", "server"])
        .await
        .unwrap_or_else(|e| panic!("connect owner E {host}:{port_e}: {e}"));
    let owner_e_wr = owner_e.wr.clone();
    spawn_reader(
        "owner-E",
        owner_e.rd,
        east.clone(),
        Some(owner_e_wr.clone()),
        stop.clone(),
    );
    send_json(
        &owner_e_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":8.0,"coarse_rate":2,"coarse_grid":0.2}),
    )
    .await
    .unwrap();

    let viewer_w = connect_endpoint(&host, port_w, "rlg-view-W", "OBS", &["observer"])
        .await
        .unwrap_or_else(|e| panic!("connect viewer W {host}:{port_w}: {e}"));
    let viewer_w_wr = viewer_w.wr.clone();
    spawn_reader("viewer-W", viewer_w.rd, all.clone(), None, stop.clone());
    send_json(
        &viewer_w_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":4.0,"coarse_rate":4,"coarse_grid":1.0}),
    )
    .await
    .unwrap();

    let viewer_e = connect_endpoint(&host, port_e, "rlg-view-E", "OBS", &["observer"])
        .await
        .unwrap_or_else(|e| panic!("connect viewer E {host}:{port_e}: {e}"));
    let viewer_e_wr = viewer_e.wr.clone();
    spawn_reader("viewer-E", viewer_e.rd, east.clone(), None, stop.clone());
    send_json(
        &viewer_e_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":4.0,"coarse_rate":4,"coarse_grid":1.0}),
    )
    .await
    .unwrap();

    let caller = connect_endpoint(&host, port_w, "rlg-caller", "OBS", &["observer"])
        .await
        .unwrap_or_else(|e| panic!("connect caller {host}:{port_w}: {e}"));
    let caller_wr = caller.wr.clone();
    spawn_reader(
        "caller",
        caller.rd,
        caller_counters.clone(),
        None,
        stop.clone(),
    );
    send_json(
        &caller_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0}),
    )
    .await
    .unwrap();

    let _slow = if enable_slow {
        Some(
            connect_slow_viewer(&host, port_w, "rlg-slow-view")
                .await
                .unwrap_or_else(|e| panic!("connect slow viewer {host}:{port_w}: {e}")),
        )
    } else {
        None
    };

    tokio::time::sleep(Duration::from_millis(250)).await;

    for i in 0..entities {
        let y = body_y(i);
        let eid = format!("rlg-body-{i}");
        send_json(
            &owner_w_wr,
            &json!({
                "op":"CreateEntity",
                "request_id":format!("create-{i}"),
                "entity":eid,
                "region":"W",
                "components":{
                    "pos":[-2.0,y],
                    "vel":[0.08,0.0],
                    "mass":1.0 + (i % 3) as f64,
                    "contact_radius":0.75,
                    "physics":initial_physics_payload(i, -2.0, y),
                    "sim_time":0,
                    "gen":0
                }
            }),
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    send_json(
        &caller_wr,
        &json!({
            "op":"CommandRequest",
            "entity":"rlg-body-0",
            "request_id":"rlg-cmd-0",
            "command":"poke",
            "payload":{"force":[1.0,0.0]}
        }),
    )
    .await
    .unwrap();

    for n in 0..event_burst {
        send_json(
            &owner_w_wr,
            &json!({
                "op":"EntityEvent",
                "entity":"rlg-body-0",
                "event":"hit",
                "class":"critical",
                "payload":{"n":n}
            }),
        )
        .await
        .unwrap();
    }
    for n in 0..event_burst {
        send_json(
            &owner_w_wr,
            &json!({
                "op":"EntityEvent",
                "entity":"rlg-body-0",
                "event":"spark",
                "class":"visual",
                "coalesce_key":"rlg-spark",
                "payload":{"n":n}
            }),
        )
        .await
        .unwrap();
    }

    let started = Instant::now();
    for tick in 0..ticks {
        let x = -2.0 + (tick as f64 / ticks as f64) * 5.0;
        let mut pos_updates = Vec::with_capacity(entities as usize);
        let mut vel_updates = Vec::with_capacity(entities as usize);
        for i in 0..entities {
            let y = body_y(i);
            let eid = format!("rlg-body-{i}");
            pos_updates.push(json!([eid, [x, y]]));
            let eid = format!("rlg-body-{i}");
            vel_updates.push(json!([eid, [0.08, 0.0]]));
        }
        send_json(
            &owner_w_wr,
            &json!({"op":"BatchUpdate","comp":"pos","updates":pos_updates}),
        )
        .await
        .unwrap();
        send_json(
            &owner_w_wr,
            &json!({"op":"BatchUpdate","comp":"vel","updates":vel_updates}),
        )
        .await
        .unwrap();
        tokio::time::sleep(dt).await;
    }

    let mut handoff_probe_ok = 0u64;
    let mut physics_payload_ok = 0u64;
    let mut physics_clock_ok = 0u64;
    let mut handoff_probe_query_error: Option<String> = None;
    if require_writer_swap {
        let gained = wait_until(Duration::from_secs(3), || {
            Counters::get(&east.authority_gain) >= entities
        })
        .await;
        if gained {
            if require_physics_payload {
                for i in 0..entities {
                    let eid = format!("rlg-body-{i}");
                    let y = body_y(i);
                    send_json(
                        &owner_e_wr,
                        &json!({
                            "op":"UpdateComponent",
                            "entity":eid,
                            "comp":"physics",
                            "value":e_physics_payload(i, 3.25 + i as f64 * 0.01, y)
                        }),
                    )
                    .await
                    .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            for i in 0..entities {
                let eid = format!("rlg-body-{i}");
                send_json(
                    &owner_e_wr,
                    &json!({
                        "op":"UpdateComponent",
                        "entity":eid,
                        "comp":"handoff_probe",
                        "value":{"writer":"E","seq":i}
                    }),
                )
                .await
                .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            for i in 0..entities {
                let eid = format!("rlg-body-{i}");
                send_json(
                    &owner_w_wr,
                    &json!({
                        "op":"UpdateComponent",
                        "entity":eid,
                        "comp":"handoff_probe",
                        "value":{"writer":"W_STALE","seq":i}
                    }),
                )
                .await
                .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(600)).await;
            match query_entities(&host, port_e, "rlg-handoff-probe").await {
                Ok(query) => {
                    handoff_probe_ok = count_handoff_probe_ok(&query, entities);
                    physics_payload_ok = count_physics_payload_ok(&query, entities);
                    physics_clock_ok = count_physics_clock_ok(&query, entities);
                }
                Err(e) => {
                    handoff_probe_query_error = Some(e.to_string());
                }
            }
        }
    }

    send_json(
        &viewer_w_wr,
        &json!({"op":"EntityQuery","request_id":"rlg-query-W","query":{"type":"sphere","center":[0.0,0.0],"radius":100.0}}),
    )
    .await
    .unwrap();
    send_json(
        &viewer_e_wr,
        &json!({"op":"EntityQuery","request_id":"rlg-query-E","query":{"type":"sphere","center":[0.0,0.0],"radius":100.0}}),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1200)).await;
    stop.store(true, Ordering::Relaxed);

    let add_total = Counters::get(&all.add_entity) + Counters::get(&east.add_entity);
    let update_total = Counters::get(&all.component_update) + Counters::get(&east.component_update);
    let event_total = Counters::get(&all.entity_event) + Counters::get(&east.entity_event);
    let command_responses = Counters::get(&caller_counters.command_response);
    let query_responses =
        Counters::get(&all.entity_query_response) + Counters::get(&east.entity_query_response);
    let east_visible = Counters::get(&east.add_entity) + Counters::get(&east.component_update);
    let east_authority_gain = Counters::get(&east.authority_gain);
    let handoff_probe_rejected =
        Counters::get(&all.handoff_probe_rejected) + Counters::get(&east.handoff_probe_rejected);
    let rejections = Counters::get(&all.update_rejected) + Counters::get(&east.update_rejected);

    let mut failures = Vec::new();
    if add_total == 0 {
        failures.push("no_add_entity");
    }
    if update_total == 0 {
        failures.push("no_component_update");
    }
    if event_total == 0 {
        failures.push("no_entity_event");
    }
    if command_responses == 0 {
        failures.push("no_command_response");
    }
    if query_responses == 0 {
        failures.push("no_entity_query_response");
    }
    if require_mesh && east_visible == 0 {
        failures.push("no_east_visibility");
    }
    if require_mesh && east_authority_gain < entities {
        failures.push("no_east_authority_gain");
    }
    if require_writer_swap && handoff_probe_ok < entities {
        failures.push("post_adopt_e_write_not_visible");
    }
    if require_writer_swap && handoff_probe_rejected < entities {
        failures.push("stale_w_writer_not_rejected");
    }
    if require_writer_swap && handoff_probe_query_error.is_some() {
        failures.push("handoff_probe_query_failed");
    }
    if require_physics_payload && physics_payload_ok < entities {
        failures.push("physics_payload_not_visible");
    }
    if require_physics_payload && physics_clock_ok < entities {
        failures.push("physics_clock_not_monotonic");
    }

    let result = if failures.is_empty() { "pass" } else { "fail" };
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "reality_loadgen result={} mode={} entities={} ticks={} elapsed={:.2} add={} updates={} coarse={} events={} visual_events={} command_req_owner={} command_resp_caller={} query_resp={} rejections={} east_add={} east_updates={} east_visible={} east_authority_gain={} handoff_probe_ok={} handoff_probe_rejected={} physics_payload_ok={} physics_clock_ok={} mesh_ghosts={} slow_viewer={} failures={}",
        result,
        if cross_broker { "cross-broker" } else { "single-broker" },
        entities,
        ticks,
        elapsed,
        add_total,
        update_total,
        Counters::get(&all.coarse_update) + Counters::get(&east.coarse_update),
        event_total,
        Counters::get(&all.visual_event) + Counters::get(&east.visual_event),
        Counters::get(&all.command_request) + Counters::get(&east.command_request),
        command_responses,
        query_responses,
        rejections,
        Counters::get(&east.add_entity),
        Counters::get(&east.component_update),
        east_visible,
        east_authority_gain,
        handoff_probe_ok,
        handoff_probe_rejected,
        physics_payload_ok,
        physics_clock_ok,
        Counters::get(&all.mesh_ghost) + Counters::get(&east.mesh_ghost),
        if enable_slow { 1 } else { 0 },
        if failures.is_empty() { "none".to_string() } else { failures.join(",") }
    );

    if !failures.is_empty() {
        std::process::exit(2);
    }
}
