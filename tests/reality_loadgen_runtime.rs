use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const W_RLG_TOKEN: &str = "rlg-w-token";
const E_RLG_TOKEN: &str = "rlg-e-token";
const OBS_RLG_TOKEN: &str = "rlg-obs-token";
const CLIENT_RLG_TOKEN: &str = "rlg-client-token";
const MESH_RLG_TOKEN: &str = "rlg-mesh-token";
const RLG_AUTH_CLAIMS: &str = "rlg-w-token:W:physics|server,rlg-e-token:E:physics|server,rlg-obs-token:OBS:observer,rlg-client-token:CLIENT:role.client,rlg-mesh-token:MESH:role.mesh";

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

fn start_broker(
    port: u16,
    region: &str,
    mesh: Option<(String, u16)>,
    auth_token: Option<&str>,
    auth_claims: Option<&str>,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_godworks_broker"));
    cmd.env("GW_HOST", "127.0.0.1")
        .env("GW_PORT", port.to_string())
        .env("GW_WAL", unique_wal(region))
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
    if let Some(token) = auth_token {
        cmd.env("GW_AUTH_TOKEN", token);
    } else {
        cmd.env_remove("GW_AUTH_TOKEN");
    }
    if let Some(claims) = auth_claims {
        cmd.env("GW_AUTH_CLAIMS", claims);
    } else {
        cmd.env_remove("GW_AUTH_CLAIMS");
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

fn run_cross_broker_reality_loadgen(auth_claims: Option<&str>) {
    let port_w = free_port();
    let port_e = free_port();
    assert_ne!(port_w, port_e);

    let mut broker_e = start_broker(port_e, "E", None, None, auth_claims);
    let mut broker_w = start_broker(
        port_w,
        "W",
        Some(("E".to_string(), port_e)),
        None,
        auth_claims,
    );
    sleep(Duration::from_millis(900));

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_reality_loadgen"));
    cmd.env("GW_HOST", "127.0.0.1")
        .env("GW_TARGET", port_w.to_string())
        .env("GW_TARGET_E", port_e.to_string())
        .env("GW_ENTITIES", "4")
        .env("GW_TICKS", "60")
        .env("GW_HZ", "35")
        .env("GW_EVENT_BURST", "6")
        .env("GW_REQUIRE_MESH", "1")
        .env("GW_SLOW_VIEWER", "1");
    if auth_claims.is_some() {
        cmd.env("GW_AUTH_TOKEN_W", W_RLG_TOKEN)
            .env("GW_AUTH_TOKEN_E", E_RLG_TOKEN)
            .env("GW_AUTH_TOKEN_OBS", OBS_RLG_TOKEN)
            .env("GW_AUTH_TOKEN_CLIENT", CLIENT_RLG_TOKEN)
            .env("GW_AUTH_TOKEN_MESH", MESH_RLG_TOKEN)
            .env_remove("GW_AUTH_TOKEN");
    } else {
        cmd.env_remove("GW_AUTH_TOKEN")
            .env_remove("GW_AUTH_TOKEN_W")
            .env_remove("GW_AUTH_TOKEN_E")
            .env_remove("GW_AUTH_TOKEN_OBS")
            .env_remove("GW_AUTH_TOKEN_CLIENT")
            .env_remove("GW_AUTH_TOKEN_MESH");
    }
    let output = cmd.output().expect("run reality_loadgen");

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
        metric_u64(&metrics, "handoff_pos_ok"),
        4,
        "east owner did not publish a visible post-handoff pos write for every body: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "stale_pos_rejected_exact"),
        4,
        "old W owner was not rejected for the exact crossed body set: {metrics:?}"
    );
    assert_eq!(
        metric_u64(&metrics, "stale_pos_overwrite_blocked"),
        4,
        "stale W writes changed visible post-handoff position: {metrics:?}"
    );
    assert_eq!(
        metrics.get("handoff_pos_query_error").map(String::as_str),
        Some("none"),
        "post-handoff query failed: {metrics:?}"
    );
}

#[test]
fn cross_broker_reality_loadgen_requires_mesh_adoption() {
    run_cross_broker_reality_loadgen(Some(RLG_AUTH_CLAIMS));
}

#[test]
fn cross_broker_reality_loadgen_with_claim_auth_still_adopts_mesh() {
    run_cross_broker_reality_loadgen(Some(RLG_AUTH_CLAIMS));
}
