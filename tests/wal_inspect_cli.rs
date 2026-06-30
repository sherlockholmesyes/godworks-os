use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use godworks_broker::wal::{wal_v1_envelope_line, wal_v1_header_line};
use serde_json::{json, Value};

fn temp_wal(name: &str, lines: &[String]) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "godworks_wal_inspect_{name}_{}_{}.wal",
        std::process::id(),
        nanos
    ));
    let mut body = lines.join("\n");
    body.push('\n');
    fs::write(&path, body).unwrap();
    path.to_string_lossy().to_string()
}

#[test]
fn wal_inspect_reports_unknown_kinds_without_refusing_clean_wal() {
    let path = temp_wal(
        "unknown",
        &[
            wal_v1_header_line(),
            wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
            wal_v1_envelope_line(&json!({"kind":"future_transition","entity":"e1"})),
        ],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_wal_inspect"))
        .arg(&path)
        .output()
        .expect("wal_inspect should run");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["selected_event_count"], 2);
    assert_eq!(value["unknown_kind_count"], 1);
    assert_eq!(value["unknown_kinds"]["future_transition"], 1);
    assert!(value["error"].is_null());

    let _ = fs::remove_file(path);
}

#[test]
fn wal_inspect_refuses_mid_corruption() {
    let path = temp_wal(
        "mid_corruption",
        &[
            wal_v1_header_line(),
            wal_v1_envelope_line(&json!({"kind":"register","entity":"e1"})),
            "{\"_c\":0,\"_d\":\"{\\\"kind\\\":\\\"write\\\"}\"}".to_string(),
            wal_v1_envelope_line(&json!({"kind":"write","entity":"e1"})),
        ],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_wal_inspect"))
        .arg(&path)
        .output()
        .expect("wal_inspect should run");
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["error"]
        .as_str()
        .unwrap_or("")
        .contains("RestoreIntegrityError"));

    let _ = fs::remove_file(path);
}
