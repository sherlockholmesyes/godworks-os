use std::env;
use std::fs;
use std::process;

use serde_json::{json, Value};

const REDACTED_KEYS: &[&str] = &["auth_token", "value", "payload", "components", "updates"];

#[derive(Default, Debug, PartialEq, Eq)]
struct ReplayEvalReport {
    events: usize,
    errors: Vec<String>,
}

impl ReplayEvalReport {
    fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    fn to_json(&self) -> Value {
        json!({
            "events": self.events,
            "ok": self.is_ok(),
            "error_count": self.errors.len(),
            "errors": self.errors,
        })
    }
}

fn main() {
    let Some(path) = env::args().nth(1) else {
        eprintln!("usage: replay_eval <GW_REPLAY_TAPE.jsonl>");
        process::exit(2);
    };
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(err) => {
            eprintln!("failed to read replay tape '{path}': {err}");
            process::exit(2);
        }
    };
    let report = validate_tape(&content);
    println!("{}", report.to_json());
    if !report.is_ok() {
        process::exit(1);
    }
}

fn validate_tape(content: &str) -> ReplayEvalReport {
    let mut report = ReplayEvalReport::default();
    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        report.events += 1;
        let event = match serde_json::from_str::<Value>(line) {
            Ok(event) => event,
            Err(err) => {
                report
                    .errors
                    .push(format!("line {line_no}: invalid json: {err}"));
                continue;
            }
        };
        validate_no_redacted_keys(&event, line_no, "$", &mut report.errors);
        validate_event_contract(&event, line_no, &mut report.errors);
    }
    report
}

fn validate_no_redacted_keys(value: &Value, line_no: usize, path: &str, errors: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}.{key}");
                if REDACTED_KEYS.contains(&key.as_str()) {
                    errors.push(format!(
                        "line {line_no}: redacted key leaked at {child_path}"
                    ));
                }
                validate_no_redacted_keys(child, line_no, &child_path, errors);
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                validate_no_redacted_keys(child, line_no, &format!("{path}[{idx}]"), errors);
            }
        }
        _ => {}
    }
}

fn validate_event_contract(event: &Value, line_no: usize, errors: &mut Vec<String>) {
    let Some(kind) = event.get("kind").and_then(Value::as_str) else {
        errors.push(format!("line {line_no}: missing event kind"));
        return;
    };
    if kind.starts_with("broker_") {
        require_str(event, line_no, "spatial_dim", errors);
        require_str(event, line_no, "coordinate_codec", errors);
        require_u64(event, line_no, "component_registry_version", errors);
        require_partition_schema(event, line_no, errors);
    }
    match kind {
        "broker_handoff" => {
            require_str(event, line_no, "path", errors);
            require_str(event, line_no, "entity", errors);
            require_u64(event, line_no, "authority_epoch", errors);
            require_u64(event, line_no, "durable_gen", errors);
        }
        "broker_ingress" => {
            require_str(event, line_no, "outcome", errors);
            let Some(summary) = event.get("op_summary") else {
                errors.push(format!("line {line_no}: missing op_summary"));
                return;
            };
            require_str(summary, line_no, "op", errors);
            require_u64(summary, line_no, "wire_bytes", errors);
            if event.get("outcome").and_then(Value::as_str) == Some("rejected")
                && event.get("reason").and_then(Value::as_str) == Some("role_policy_error")
                && summary
                    .get("op")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .is_empty()
            {
                errors.push(format!(
                    "line {line_no}: role-policy ingress reject must name rejected op"
                ));
            }
        }
        "broker_outbound" => {
            require_str(event, line_no, "op", errors);
            if event.get("op").and_then(Value::as_str) == Some("UpdateRejected")
                && event.get("error").and_then(Value::as_str) == Some("role_policy_error")
            {
                require_str(event, line_no, "rejected_op", errors);
                require_str(event, line_no, "peer_role", errors);
            }
        }
        "broker_connect" => {
            require_str(event, line_no, "outcome", errors);
            if event.get("credential_present").is_none() {
                errors.push(format!(
                    "line {line_no}: connect event must report credential_present"
                ));
            }
        }
        _ => {}
    }
}

fn require_partition_schema(event: &Value, line_no: usize, errors: &mut Vec<String>) {
    let Some(schema) = event.get("partition_schema") else {
        errors.push(format!("line {line_no}: missing partition_schema"));
        return;
    };
    let Some(map) = schema.as_object() else {
        errors.push(format!("line {line_no}: partition_schema must be object"));
        return;
    };
    let Some(kind) = map.get("kind").and_then(Value::as_str) else {
        errors.push(format!(
            "line {line_no}: partition_schema.kind must be a string"
        ));
        return;
    };
    match kind {
        "grid2d" => {
            require_positive_u64(schema, line_no, "cols", errors);
            require_positive_u64(schema, line_no, "rows", errors);
        }
        "strip1d" => {
            require_u64(schema, line_no, "boundary_count", errors);
        }
        other => errors.push(format!(
            "line {line_no}: unknown partition_schema.kind {other}"
        )),
    }
}

fn require_str(value: &Value, line_no: usize, key: &str, errors: &mut Vec<String>) {
    if get_path(value, key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        errors.push(format!("line {line_no}: missing string {key}"));
    }
}

fn require_u64(value: &Value, line_no: usize, key: &str, errors: &mut Vec<String>) {
    if get_path(value, key).and_then(Value::as_u64).is_none() {
        errors.push(format!("line {line_no}: missing u64 {key}"));
    }
}

fn require_positive_u64(value: &Value, line_no: usize, key: &str, errors: &mut Vec<String>) {
    if get_path(value, key)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .is_none()
    {
        errors.push(format!("line {line_no}: missing positive u64 {key}"));
    }
}

fn get_path<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in key.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_tape_accepts_role_policy_and_handoff() {
        let tape = r#"
{"kind":"broker_ingress","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"outcome":"rejected","reason":"role_policy_error","op_summary":{"op":"CreateEntity","wire_bytes":64}}
{"kind":"broker_outbound","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"op":"UpdateRejected","error":"role_policy_error","rejected_op":"CreateEntity","peer_role":"client"}
{"kind":"broker_handoff","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"grid2d","cols":2,"rows":2},"path":"local","entity":"ship","authority_epoch":3,"durable_gen":8}
"#;
        let report = validate_tape(tape);
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(report.events, 3);
    }

    #[test]
    fn handoff_without_spatial_metadata_fails() {
        let tape = r#"{"kind":"broker_handoff","path":"local","entity":"ship","authority_epoch":3,"durable_gen":8}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing string spatial_dim")));
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing partition_schema")));
    }

    #[test]
    fn broker_event_without_component_registry_version_fails() {
        let tape = r#"{"kind":"broker_ingress","spatial_dim":"D2","coordinate_codec":"debug_f64_2","partition_schema":{"kind":"strip1d","boundary_count":1},"outcome":"dispatched","op_summary":{"op":"UpdateComponent","wire_bytes":64}}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing u64 component_registry_version")));
    }

    #[test]
    fn redacted_key_anywhere_fails() {
        let tape = r#"{"kind":"broker_ingress","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"outcome":"dispatched","op_summary":{"op":"UpdateComponent","wire_bytes":64,"value":"secret"}}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("redacted key leaked")));
    }

    #[test]
    fn role_policy_outbound_without_rejected_op_fails() {
        let tape = r#"{"kind":"broker_outbound","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"op":"UpdateRejected","error":"role_policy_error","peer_role":"client"}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing string rejected_op")));
    }

    #[test]
    fn grid2d_without_dimensions_fails() {
        let tape = r#"{"kind":"broker_handoff","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"grid2d"},"path":"local","entity":"ship","authority_epoch":3,"durable_gen":8}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing positive u64 cols")));
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing positive u64 rows")));
    }

    #[test]
    fn strip1d_without_boundary_count_fails() {
        let tape = r#"{"kind":"broker_handoff","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d"},"path":"local","entity":"ship","authority_epoch":3,"durable_gen":8}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("missing u64 boundary_count")));
    }

    #[test]
    fn unknown_partition_schema_kind_fails() {
        let tape = r#"{"kind":"broker_handoff","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"sector"},"path":"local","entity":"ship","authority_epoch":3,"durable_gen":8}"#;
        let report = validate_tape(tape);
        assert!(report
            .errors
            .iter()
            .any(|err| err.contains("unknown partition_schema.kind sector")));
    }
}
