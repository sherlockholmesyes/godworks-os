//! godworks_loadgen — a NATIVE (Rust) load generator, so the CLIENT is not the floor.
//!
//! The reference harness capped both servers at the reference client's rate; this drives at
//! native speed so the measured rate reflects the SERVER. Same wire (length-prefixed JSON).
//!
//! GW_ZONES controls concurrency for the multicore test: 1 = one region-W owner pushing
//! flat-out (single-core op ceiling); 2 = W AND E owners pushing concurrently. If the
//! server serializes op processing on a GLOBAL lock, 2 zones ~= 1 zone (no scaling); if
//! the lock is sharded per region, 2 zones ~= 2x (two cores). That is the decisive test
//! of whether the bottleneck is the language or the lock granularity.
//!
//! Reports one parseable line: total writes/sec (summed across zones) + emits/sec (viewer-0).

use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn frame(v: &Value) -> Vec<u8> {
    let b = serde_json::to_vec(v).unwrap();
    let n = b.len() as u32;
    let mut o = Vec::with_capacity(4 + b.len());
    o.extend_from_slice(&n.to_be_bytes());
    o.extend_from_slice(&b);
    o
}

fn auth_token() -> Option<String> {
    env::var("GW_AUTH_TOKEN").ok().filter(|s| !s.is_empty())
}

fn worker_connect_value(worker_id: String, region: &str) -> Value {
    let mut value = json!({"op":"WorkerConnect","worker_id":worker_id,"region":region});
    if let Some(token) = auth_token() {
        value["auth_token"] = json!(token);
    }
    value
}

async fn read_frame<R: AsyncReadExt + Unpin>(rd: &mut R) -> Option<Value> {
    let mut h = [0u8; 4];
    rd.read_exact(&mut h).await.ok()?;
    let n = u32::from_be_bytes(h) as usize;
    let mut b = vec![0u8; n];
    rd.read_exact(&mut b).await.ok()?;
    serde_json::from_slice(&b).ok()
}

/// One zone, end-to-end against ITS OWN broker `port`: a counting viewer + an owner that
/// pushes UpdateComponent(pos) flat-out within the region (no handoff) until `stop`. If two
/// zones share a port -> the global-lock test (one broker). If they target different ports
/// -> the distributed test (a broker PER ZONE = zones-are-servers; should scale by core).
async fn drive_zone(
    port: u16,
    region: &'static str,
    eid: &'static str,
    x_base: f64,
    sent: Arc<AtomicU64>,
    recv: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let host = "127.0.0.1";
    // per-zone counting viewer on this zone's broker
    let vs = TcpStream::connect((host, port))
        .await
        .expect("viewer connect");
    vs.set_nodelay(true).ok();
    let (mut vrd, vwr) = vs.into_split();
    let mut vwr = vwr;
    vwr.write_all(&frame(&worker_connect_value(
        format!("lg-obs-{region}"),
        "OBS",
    )))
    .await
    .unwrap();
    let recv2 = recv.clone();
    tokio::spawn(async move {
        let _keep = vwr;
        loop {
            match read_frame(&mut vrd).await {
                None => break,
                Some(f) => {
                    if f.get("op").and_then(|v| v.as_str()) == Some("ComponentUpdate")
                        && f.get("comp").and_then(|v| v.as_str()) == Some("pos")
                        && f.get("entity").and_then(|v| v.as_str()) == Some(eid)
                    {
                        recv2.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });
    // owner
    let owner = TcpStream::connect((host, port))
        .await
        .expect("owner connect");
    owner.set_nodelay(true).ok();
    let (mut ord, mut owr) = owner.into_split();
    owr.write_all(&frame(&worker_connect_value(
        format!("lg-{region}"),
        region,
    )))
    .await
    .unwrap();
    owr.write_all(&frame(&json!({"op":"CreateEntity","entity":eid,"region":region,"components":{"pos":[x_base,0.0,0.0]}}))).await.unwrap();
    tokio::spawn(async move {
        loop {
            if read_frame(&mut ord).await.is_none() {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut i: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..512 {
            let x = x_base + ((i % 100) as f64) * 0.01;
            owr.write_all(&frame(
                &json!({"op":"UpdateComponent","entity":eid,"comp":"pos","value":[x,0.0]}),
            ))
            .await
            .unwrap();
            i += 1;
            sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let port_w: u16 = env::var("GW_TARGET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7777);
    // zone E's broker: same port = global-lock test (one broker); different = per-zone brokers
    let port_e: u16 = env::var("GW_TARGET_E")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(port_w);
    let dur: f64 = env::var("GW_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5.0);
    let n_zones: usize = env::var("GW_ZONES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let sent = Arc::new(AtomicU64::new(0));
    let recv = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    handles.push(tokio::spawn(drive_zone(
        port_w,
        "W",
        "L0",
        -2.0,
        sent.clone(),
        recv.clone(),
        stop.clone(),
    )));
    if n_zones >= 2 {
        handles.push(tokio::spawn(drive_zone(
            port_e,
            "E",
            "L1",
            1.0,
            sent.clone(),
            recv.clone(),
            stop.clone(),
        )));
    }
    tokio::time::sleep(Duration::from_millis(600)).await; // spawn + warm up

    let recv_start = recv.load(Ordering::Relaxed);
    let sent_start = sent.load(Ordering::Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(dur)).await;
    stop.store(true, Ordering::Relaxed);
    let elapsed = t0.elapsed().as_secs_f64();
    for h in handles {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    let sent_total = sent.load(Ordering::Relaxed).saturating_sub(sent_start);
    let recv_total = recv.load(Ordering::Relaxed).saturating_sub(recv_start);
    let mode = if port_e != port_w {
        "per-zone-brokers"
    } else {
        "one-broker"
    };
    println!(
        "zones={} mode={} writes_sent={} writes_per_sec={:.0} updates_recv={} emits_per_sec={:.0} dur={:.2}",
        n_zones, mode, sent_total, sent_total as f64 / elapsed, recv_total, recv_total as f64 / elapsed, elapsed
    );
}
