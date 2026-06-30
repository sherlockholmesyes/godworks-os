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

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
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

fn auth_token() -> Option<String> {
    env::var("GW_AUTH_TOKEN").ok().filter(|s| !s.is_empty())
}

fn worker_connect_value(wid: &str, region: &str, attributes: &[&str]) -> Value {
    let mut value = json!({
        "op":"WorkerConnect",
        "worker_id":wid,
        "region":region,
        "attributes":attributes,
        "proto":1
    });
    if let Some(token) = auth_token() {
        value["auth_token"] = json!(token);
    }
    value
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

#[derive(Default)]
struct ExactRejections {
    by_comp: StdMutex<HashMap<String, HashMap<String, u64>>>,
}

impl ExactRejections {
    fn record(&self, f: &Value) {
        let Some(comp) = f.get("comp").and_then(Value::as_str) else {
            return;
        };
        let Some(entity) = f.get("entity").and_then(Value::as_str) else {
            return;
        };
        let mut by_comp = self.by_comp.lock().unwrap();
        *by_comp
            .entry(comp.to_string())
            .or_default()
            .entry(entity.to_string())
            .or_default() += 1;
    }

    fn count_entities_with_rejects(&self, comp: &str, prefix: &str, count: u64) -> u64 {
        let by_comp = self.by_comp.lock().unwrap();
        let Some(by_entity) = by_comp.get(comp) else {
            return 0;
        };
        (0..count)
            .filter(|i| {
                let eid = format!("{prefix}{i}");
                by_entity.get(&eid).copied().unwrap_or(0) > 0
            })
            .count() as u64
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
    wr.write_all(&frame(&worker_connect_value(wid, region, attributes)))
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
        &worker_connect_value(wid, "OBS", &["observer"]),
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
    exact_rejections: Option<Arc<ExactRejections>>,
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
            if op == "UpdateRejected" {
                if let Some(exact) = exact_rejections.as_ref() {
                    exact.record(&f);
                }
            }
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

async fn query_entities_with_query(
    host: &str,
    port: u16,
    request_id: &str,
    query: Value,
) -> std::io::Result<Value> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true).ok();
    write_raw(
        &mut stream,
        &worker_connect_value(&format!("rlg-query-{request_id}"), "OBS", &["observer"]),
    )
    .await?;
    write_raw(
        &mut stream,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0}),
    )
    .await?;
    write_raw(
        &mut stream,
        &json!({"op":"EntityQuery","request_id":request_id,"query":query}),
    )
    .await?;
    for _ in 0..128 {
        match tokio::time::timeout(Duration::from_millis(750), read_frame(&mut stream)).await {
            Ok(Some(frame))
                if frame.get("op").and_then(Value::as_str) == Some("EntityQueryResponse")
                    && frame.get("request_id").and_then(Value::as_str) == Some(request_id) =>
            {
                return Ok(frame);
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "EntityQueryResponse not received",
    ))
}

fn count_entities_at_pos_x(frame: &Value, prefix: &str, count: u64, expected_x: f64) -> u64 {
    let Some(rows) = frame.get("entities").and_then(Value::as_array) else {
        return 0;
    };
    (0..count)
        .filter(|i| {
            let eid = format!("{prefix}{i}");
            rows.iter().any(|row| {
                row.get("entity").and_then(Value::as_str) == Some(eid.as_str())
                    && row
                        .get("pos")
                        .and_then(Value::as_array)
                        .and_then(|pos| pos.first())
                        .and_then(Value::as_f64)
                        .is_some_and(|x| (x - expected_x).abs() <= 0.000_001)
            })
        })
        .count() as u64
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
    let cross_broker = port_e != port_w;
    let dt = Duration::from_secs_f64(1.0 / hz);
    let stop = Arc::new(AtomicBool::new(false));
    let all = Arc::new(Counters::default());
    let east = Arc::new(Counters::default());
    let caller_counters = Arc::new(Counters::default());
    let exact_rejections = Arc::new(ExactRejections::default());

    let owner_w = connect_endpoint(&host, port_w, "rlg-owner-W", "W", &["physics", "server"])
        .await
        .unwrap_or_else(|e| panic!("connect owner W {host}:{port_w}: {e}"));
    let owner_w_wr = owner_w.wr.clone();
    spawn_reader(
        "owner-W",
        owner_w.rd,
        all.clone(),
        Some(exact_rejections.clone()),
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
        Some(exact_rejections.clone()),
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
    spawn_reader(
        "viewer-W",
        viewer_w.rd,
        all.clone(),
        None,
        None,
        stop.clone(),
    );
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
    spawn_reader(
        "viewer-E",
        viewer_e.rd,
        east.clone(),
        None,
        None,
        stop.clone(),
    );
    send_json(
        &viewer_e_wr,
        &json!({"op":"Interest","center":[0.0,0.0],"radius":100.0,"full_radius":4.0,"coarse_rate":4,"coarse_grid":1.0}),
    )
    .await
    .unwrap();

    let caller = connect_endpoint(&host, port_w, "rlg-caller", "CLIENT", &["role.client"])
        .await
        .unwrap_or_else(|e| panic!("connect caller {host}:{port_w}: {e}"));
    let caller_wr = caller.wr.clone();
    spawn_reader(
        "caller",
        caller.rd,
        caller_counters.clone(),
        None,
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
        let y = (i as f64 % 8.0) * 0.35 - 1.2;
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
            let y = (i as f64 % 8.0) * 0.35 - 1.2;
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

    let mut handoff_pos_ok = 0;
    let mut stale_pos_rejected_exact = 0;
    let mut stale_pos_overwrite_blocked = 0;
    let mut handoff_pos_query_error = "none".to_string();
    if require_mesh {
        let e_probe_x = 3.25;
        let stale_probe_x = 4.25;
        let mut e_pos_updates = Vec::with_capacity(entities as usize);
        let mut stale_w_pos_updates = Vec::with_capacity(entities as usize);
        for i in 0..entities {
            let y = (i as f64 % 8.0) * 0.35 - 1.2;
            let eid = format!("rlg-body-{i}");
            e_pos_updates.push(json!([eid, [e_probe_x, y]]));
            let eid = format!("rlg-body-{i}");
            stale_w_pos_updates.push(json!([eid, [stale_probe_x, y]]));
        }

        send_json(
            &owner_e_wr,
            &json!({"op":"BatchUpdate","comp":"pos","updates":e_pos_updates}),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        match query_entities_with_query(
            &host,
            port_e,
            "rlg-handoff-pos",
            json!({"type":"sphere","center":[0.0,0.0],"radius":100.0}),
        )
        .await
        {
            Ok(frame) => {
                handoff_pos_ok = count_entities_at_pos_x(&frame, "rlg-body-", entities, e_probe_x);
            }
            Err(e) => {
                handoff_pos_query_error = format!("handoff_query_error:{e}");
            }
        }

        send_json(
            &owner_w_wr,
            &json!({"op":"BatchUpdate","comp":"pos","updates":stale_w_pos_updates}),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        stale_pos_rejected_exact =
            exact_rejections.count_entities_with_rejects("pos", "rlg-body-", entities);

        match query_entities_with_query(
            &host,
            port_e,
            "rlg-stale-pos",
            json!({"type":"sphere","center":[0.0,0.0],"radius":100.0}),
        )
        .await
        {
            Ok(frame) => {
                stale_pos_overwrite_blocked =
                    count_entities_at_pos_x(&frame, "rlg-body-", entities, e_probe_x);
            }
            Err(e) => {
                handoff_pos_query_error = format!("stale_query_error:{e}");
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
    if require_mesh && handoff_pos_ok < entities {
        failures.push("no_east_pos_write_visibility");
    }
    if require_mesh && stale_pos_rejected_exact < entities {
        failures.push("no_exact_stale_pos_reject");
    }
    if require_mesh && stale_pos_overwrite_blocked < entities {
        failures.push("stale_pos_overwrite_visible");
    }

    let result = if failures.is_empty() { "pass" } else { "fail" };
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "reality_loadgen result={} mode={} entities={} ticks={} elapsed={:.2} add={} updates={} coarse={} events={} visual_events={} command_req_owner={} command_resp_caller={} query_resp={} rejections={} east_add={} east_updates={} east_visible={} east_authority_gain={} handoff_pos_ok={} stale_pos_rejected_exact={} stale_pos_overwrite_blocked={} handoff_pos_query_error={} mesh_ghosts={} slow_viewer={} failures={}",
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
        handoff_pos_ok,
        stale_pos_rejected_exact,
        stale_pos_overwrite_blocked,
        handoff_pos_query_error,
        Counters::get(&all.mesh_ghost) + Counters::get(&east.mesh_ghost),
        if enable_slow { 1 } else { 0 },
        if failures.is_empty() { "none".to_string() } else { failures.join(",") }
    );

    if !failures.is_empty() {
        std::process::exit(2);
    }
}
