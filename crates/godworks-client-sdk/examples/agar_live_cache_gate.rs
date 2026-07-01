//! Live Agar client-cache gate.
//!
//! This example is intentionally a thin transport harness around
//! `godworks_client_sdk::ClientBridge`. The cache remains owned by the SDK; the
//! example only opens the current length-prefixed JSON debug wire, decodes typed
//! ops, and asserts that a real CLIENT stream builds a non-empty positional
//! cache.

use godworks_client_sdk::{ClientBridge, ClientConnectionPhase};
use godworks_protocol::{json::decode_json_value, Op, DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION};
use serde_json::{json, Value};
use std::{
    env,
    io::{self, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Stats {
    frames: u64,
    stream_ops: u64,
    add_entities: u64,
    component_updates: u64,
    batch_updates: u64,
    remove_entities: u64,
    ignored_ops: u64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let host = env::var("GW_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("GW_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(7777);
    let token = env::var("GW_BROWSER_TOKEN").unwrap_or_else(|_| "browser-token".to_string());
    let duration = Duration::from_millis(
        env::var("GW_AGAR_SDK_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2_500),
    );
    let min_entities = env::var("GW_AGAR_SDK_MIN_ENTITIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let min_stream_ops = env::var("GW_AGAR_SDK_MIN_STREAM_OPS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3);

    let mut stream = TcpStream::connect((host.as_str(), port))
        .map_err(|err| format!("agar sdk cache gate could not connect to {host}:{port}: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .map_err(|err| format!("failed to set read timeout: {err}"))?;

    write_frame(
        &mut stream,
        &json!({
            "op": "WorkerConnect",
            "worker_id": "agar-sdk-cache-gate",
            "region": "CLIENT",
            "proto": PROTOCOL_VERSION,
            "attributes": ["role.client"],
            "auth_token": token,
        }),
    )
    .map_err(|err| format!("failed to send WorkerConnect: {err}"))?;
    write_frame(
        &mut stream,
        &json!({
            "op": "Interest",
            "center": [60.0, 60.0],
            "radius": 1.0e9,
            "full_radius": 1.0e9,
        }),
    )
    .map_err(|err| format!("failed to send Interest: {err}"))?;

    let mut bridge = ClientBridge::new();
    bridge.on_transport_connecting();
    bridge.mark_live();

    let deadline = Instant::now() + duration;
    let mut stats = Stats::default();
    let mut auth_reject: Option<Value> = None;
    let mut decode_errors: Vec<String> = Vec::new();

    while Instant::now() < deadline {
        let Some(value) =
            read_frame(&mut stream).map_err(|err| format!("failed to read broker frame: {err}"))?
        else {
            continue;
        };
        stats.frames += 1;
        match decode_json_value(&value) {
            Ok(Op::AuthReject(op)) => {
                auth_reject = Some(json!({
                    "worker_id": op.worker_id.map(|id| id.as_ref().to_string()),
                    "error": op.error,
                    "reason": op.reason,
                }));
                break;
            }
            Ok(op) => {
                if is_cache_stream_op(&op) {
                    stats.stream_ops += 1;
                    match &op {
                        Op::AddEntity(_) => stats.add_entities += 1,
                        Op::ComponentUpdate(_) => stats.component_updates += 1,
                        Op::BatchUpdate(_) => stats.batch_updates += 1,
                        Op::RemoveEntity(_) => stats.remove_entities += 1,
                        _ => {}
                    }
                } else {
                    stats.ignored_ops += 1;
                }
                bridge.apply_stream_op(&op);
            }
            Err(err) => {
                decode_errors.push(format!("{err:?}"));
                if decode_errors.len() > 8 {
                    break;
                }
            }
        }
    }

    let snapshot = bridge.snapshot();
    let positioned_entities = snapshot
        .entities
        .iter()
        .filter(|entity| entity.position2.is_some())
        .count();
    let moving_entities = snapshot
        .entities
        .iter()
        .filter(|entity| {
            entity
                .components
                .get("vel")
                .and_then(|value| value.as_array())
                .map(|items| {
                    let vx = items.first().and_then(Value::as_f64).unwrap_or(0.0);
                    let vy = items.get(1).and_then(Value::as_f64).unwrap_or(0.0);
                    vx.abs() + vy.abs() > 0.001
                })
                .unwrap_or(false)
        })
        .count();

    let mut failures = Vec::new();
    if let Some(reject) = auth_reject {
        failures.push(format!("CLIENT stream auth rejected: {reject}"));
    }
    if !decode_errors.is_empty() {
        failures.push(format!(
            "failed to decode broker frame(s): {}",
            decode_errors.join("; ")
        ));
    }
    if snapshot.phase != ClientConnectionPhase::Live {
        failures.push(format!(
            "client bridge phase is not Live: {:?}",
            snapshot.phase
        ));
    }
    if snapshot.entity_count < min_entities {
        failures.push(format!(
            "client cache has too few entities: {} < {}",
            snapshot.entity_count, min_entities
        ));
    }
    if positioned_entities == 0 {
        failures.push("client cache has no positioned entities".to_string());
    }
    if stats.stream_ops < min_stream_ops {
        failures.push(format!(
            "client stream yielded too few cache ops: {} < {}",
            stats.stream_ops, min_stream_ops
        ));
    }
    if snapshot.rejection_count > 0 {
        failures.push(format!(
            "client cache observed UpdateRejected frames: {}",
            snapshot.rejection_count
        ));
    }

    let report = json!({
        "ok": failures.is_empty(),
        "duration_ms": duration.as_millis(),
        "frames": stats.frames,
        "stream_ops": stats.stream_ops,
        "add_entities": stats.add_entities,
        "component_updates": stats.component_updates,
        "batch_updates": stats.batch_updates,
        "remove_entities": stats.remove_entities,
        "ignored_ops": stats.ignored_ops,
        "entity_count": snapshot.entity_count,
        "positioned_entities": positioned_entities,
        "moving_entities": moving_entities,
        "critical_depth": snapshot.critical_depth,
        "rejection_count": snapshot.rejection_count,
    });

    if failures.is_empty() {
        println!("{report}");
        Ok(())
    } else {
        let mut failed = report;
        failed["failures"] = json!(failures);
        eprintln!("{failed}");
        Err("agar SDK client cache gate failed".to_string())
    }
}

fn is_cache_stream_op(op: &Op) -> bool {
    matches!(
        op,
        Op::AddEntity(_)
            | Op::RemoveEntity(_)
            | Op::AddComponent(_)
            | Op::RemoveComponent(_)
            | Op::ComponentUpdate(_)
            | Op::BatchUpdate(_)
            | Op::AuthorityChange(_)
            | Op::UpdateRejected(_)
            | Op::CriticalSection(_)
            | Op::MeshGhost(_)
            | Op::MeshGhostRemove(_)
    )
}

fn write_frame(stream: &mut TcpStream, value: &Value) -> io::Result<()> {
    let body = serde_json::to_vec(value)?;
    if body.len() > DEFAULT_MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame exceeds max size: {}", body.len()),
        ));
    }
    let mut header = [0u8; 4];
    header.copy_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&header)?;
    stream.write_all(&body)?;
    stream.flush()
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Value>> {
    let mut header = [0u8; 4];
    if let Err(err) = stream.read_exact(&mut header) {
        if matches!(
            err.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        ) {
            return Ok(None);
        }
        return Err(err);
    }
    let len = u32::from_be_bytes(header) as usize;
    if len > DEFAULT_MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("incoming frame exceeds max size: {len}"),
        ));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
