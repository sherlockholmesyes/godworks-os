use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::process;

use godworks_core::{
    ModelFeatureBlock, ModelFeatureBlockKind, ModelFeatureBlockProvenance,
    MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
};
use serde_json::{json, Value};

const FORBIDDEN_SOURCE_KEYS: &[&str] = &["auth_token", "value", "payload", "components", "updates"];

#[derive(Clone, Debug)]
struct FeatureBuildConfig {
    project_id: String,
    dataset_id: String,
    trace_id: String,
    source_artifact: String,
}

impl FeatureBuildConfig {
    fn provenance(&self) -> ModelFeatureBlockProvenance {
        ModelFeatureBlockProvenance::new(
            self.project_id.clone(),
            self.dataset_id.clone(),
            self.trace_id.clone(),
            self.source_artifact.clone(),
        )
    }
}

#[derive(Default, Debug)]
struct ReplaySummary {
    events: u64,
    ingress_events: u64,
    outbound_events: u64,
    handoff_events: u64,
    mesh_handoff_events: u64,
    rejected_ingress_events: u64,
    update_rejected_events: u64,
    total_wire_bytes: u64,
    max_authority_epoch: u64,
    max_durable_gen: u64,
    spatial_dims: BTreeSet<String>,
    coordinate_codecs: BTreeSet<String>,
    partition_schemas: BTreeSet<String>,
    component_registry_versions: BTreeSet<String>,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        eprintln!(
            "usage: model_feature_block <replay|reality-loadgen|agar-live-gate> <path> <project_id> <dataset_id> <trace_id>"
        );
        process::exit(2);
    }

    let mode = &args[1];
    let path = &args[2];
    let content = match read_text_artifact(path) {
        Ok(content) => content,
        Err(err) => {
            eprintln!("failed to read '{path}': {err}");
            process::exit(2);
        }
    };
    let cfg = FeatureBuildConfig {
        project_id: args[3].clone(),
        dataset_id: args[4].clone(),
        trace_id: args[5].clone(),
        source_artifact: format!("{mode}:{path}"),
    };

    let blocks = match mode.as_str() {
        "replay" => build_replay_feature_blocks(&content, &cfg),
        "reality-loadgen" => build_reality_loadgen_feature_blocks(&content, &cfg),
        "agar-live-gate" => build_agar_live_gate_feature_blocks(&content, &cfg),
        _ => Err(format!(
            "unknown mode '{mode}', expected replay, reality-loadgen, or agar-live-gate"
        )),
    };

    let blocks = match blocks {
        Ok(blocks) => blocks,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };

    for block in blocks {
        println!("{}", block_to_json(&block));
    }
}

fn read_text_artifact(path: &str) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|err| err.to_string())?;
    decode_text_artifact_bytes(&bytes)
}

fn decode_text_artifact_bytes(bytes: &[u8]) -> Result<String, String> {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return String::from_utf8(bytes[3..].to_vec()).map_err(|err| err.to_string());
    }
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return decode_utf16_artifact(&bytes[2..], false);
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        return decode_utf16_artifact(&bytes[2..], true);
    }
    String::from_utf8(bytes.to_vec()).map_err(|err| err.to_string())
}

fn decode_utf16_artifact(bytes: &[u8], big_endian: bool) -> Result<String, String> {
    if !bytes.len().is_multiple_of(2) {
        return Err("UTF-16 artifact has an odd byte length".to_string());
    }
    let units = bytes.chunks_exact(2).map(|chunk| {
        if big_endian {
            u16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            u16::from_le_bytes([chunk[0], chunk[1]])
        }
    });
    String::from_utf16(&units.collect::<Vec<_>>()).map_err(|err| err.to_string())
}

fn build_replay_feature_blocks(
    content: &str,
    cfg: &FeatureBuildConfig,
) -> Result<Vec<ModelFeatureBlock>, String> {
    let mut summary = ReplaySummary::default();
    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        let event = serde_json::from_str::<Value>(line)
            .map_err(|err| format!("line {line_no}: invalid replay json: {err}"))?;
        reject_forbidden_source_keys(&event, line_no, "$")?;
        summarize_replay_event(&event, line_no, &mut summary)?;
    }

    if summary.events == 0 {
        return Err("replay artifact contains no events".to_string());
    }

    let base = replay_base_dimensions(&summary);
    let ingress =
        ModelFeatureBlock::new(ModelFeatureBlockKind::IngressRejectCost, cfg.provenance())
            .with_metric("event_count", summary.events as f64)
            .with_metric("ingress_events", summary.ingress_events as f64)
            .with_metric("outbound_events", summary.outbound_events as f64)
            .with_metric(
                "rejected_ingress_events",
                summary.rejected_ingress_events as f64,
            )
            .with_metric(
                "update_rejected_events",
                summary.update_rejected_events as f64,
            )
            .with_metric("total_wire_bytes", summary.total_wire_bytes as f64)
            .with_dimension("artifact_kind", "replay_tape")
            .with_dimension("spatial_dim", base.spatial_dim.as_str())
            .with_dimension("coordinate_codec", base.coordinate_codec.as_str())
            .with_dimension("partition_schema", base.partition_schema.as_str())
            .with_dimension(
                "component_registry_version",
                base.component_registry_version.as_str(),
            );

    let handoff = ModelFeatureBlock::new(ModelFeatureBlockKind::HandoffPressure, cfg.provenance())
        .with_metric("event_count", summary.events as f64)
        .with_metric("handoff_events", summary.handoff_events as f64)
        .with_metric("mesh_handoff_events", summary.mesh_handoff_events as f64)
        .with_metric("max_authority_epoch", summary.max_authority_epoch as f64)
        .with_metric("max_durable_gen", summary.max_durable_gen as f64)
        .with_dimension("artifact_kind", "replay_tape")
        .with_dimension("spatial_dim", base.spatial_dim.as_str())
        .with_dimension("coordinate_codec", base.coordinate_codec.as_str())
        .with_dimension("partition_schema", base.partition_schema.as_str())
        .with_dimension(
            "component_registry_version",
            base.component_registry_version.as_str(),
        );

    validate_blocks(vec![ingress, handoff])
}

fn summarize_replay_event(
    event: &Value,
    line_no: usize,
    summary: &mut ReplaySummary,
) -> Result<(), String> {
    let kind = event
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("line {line_no}: missing replay event kind"))?;
    summary.events += 1;
    match kind {
        "broker_ingress" => {
            summary.ingress_events += 1;
            if event.get("outcome").and_then(Value::as_str) == Some("rejected") {
                summary.rejected_ingress_events += 1;
            }
        }
        "broker_outbound" => {
            summary.outbound_events += 1;
            if event.get("op").and_then(Value::as_str) == Some("UpdateRejected") {
                summary.update_rejected_events += 1;
            }
        }
        "broker_handoff" => {
            summary.handoff_events += 1;
            if event.get("path").and_then(Value::as_str) == Some("cross_broker_emit") {
                summary.mesh_handoff_events += 1;
            }
            summary.max_authority_epoch = summary.max_authority_epoch.max(
                event
                    .get("authority_epoch")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
            summary.max_durable_gen = summary.max_durable_gen.max(
                event
                    .get("durable_gen")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
        }
        _ => {}
    }

    if let Some(bytes) = event
        .get("op_summary")
        .and_then(|summary| summary.get("wire_bytes"))
        .and_then(Value::as_u64)
    {
        summary.total_wire_bytes = summary.total_wire_bytes.saturating_add(bytes);
    }
    insert_if_str(event, "spatial_dim", &mut summary.spatial_dims);
    insert_if_str(event, "coordinate_codec", &mut summary.coordinate_codecs);
    if let Some(version) = event
        .get("component_registry_version")
        .and_then(Value::as_u64)
    {
        summary
            .component_registry_versions
            .insert(version.to_string());
    }
    if let Some(schema) = event.get("partition_schema") {
        if let Some(kind) = schema.get("kind").and_then(Value::as_str) {
            summary.partition_schemas.insert(kind.to_string());
        }
    }
    Ok(())
}

struct ReplayBaseDimensions {
    spatial_dim: String,
    coordinate_codec: String,
    partition_schema: String,
    component_registry_version: String,
}

fn replay_base_dimensions(summary: &ReplaySummary) -> ReplayBaseDimensions {
    ReplayBaseDimensions {
        spatial_dim: one_or_mixed(&summary.spatial_dims),
        coordinate_codec: one_or_mixed(&summary.coordinate_codecs),
        partition_schema: one_or_mixed(&summary.partition_schemas),
        component_registry_version: one_or_mixed(&summary.component_registry_versions),
    }
}

fn one_or_mixed(values: &BTreeSet<String>) -> String {
    if values.len() == 1 {
        values.iter().next().cloned().unwrap_or_default()
    } else {
        "mixed".to_string()
    }
}

fn build_reality_loadgen_feature_blocks(
    content: &str,
    cfg: &FeatureBuildConfig,
) -> Result<Vec<ModelFeatureBlock>, String> {
    let Some(line) = content.lines().find_map(normalized_reality_loadgen_line) else {
        return Err("missing parseable reality_loadgen result line".to_string());
    };
    let fields = parse_key_value_line(line);
    let result = fields
        .get("result")
        .map(String::as_str)
        .unwrap_or("unknown");
    let mode = fields.get("mode").map(String::as_str).unwrap_or("unknown");
    let failures = fields
        .get("failures")
        .map(String::as_str)
        .unwrap_or("unknown");

    let mut outcome = ModelFeatureBlock::new(ModelFeatureBlockKind::Outcome, cfg.provenance())
        .with_dimension("artifact_kind", "reality_loadgen")
        .with_dimension("result", result)
        .with_dimension("mode", mode)
        .with_dimension("failures", failures);

    for (field, metric) in [
        ("entities", "entities"),
        ("ticks", "ticks"),
        ("elapsed", "elapsed"),
        ("add", "add_frames"),
        ("updates", "component_update_frames"),
        ("coarse", "coarse_component_update_frames"),
        ("events", "entity_event_frames"),
        ("visual_events", "visual_event_frames"),
        ("command_req_owner", "command_req_owner"),
        ("command_resp_caller", "command_resp_caller"),
        ("query_resp", "query_resp"),
        ("rejections", "rejections"),
        ("east_add", "east_add_frames"),
        ("east_updates", "east_component_update_frames"),
        ("east_visible", "east_visible_frames"),
        ("east_authority_gain", "east_authority_gain"),
        ("handoff_pos_ok", "handoff_pos_ok"),
        ("stale_pos_rejected_exact", "stale_pos_rejected_exact"),
        ("stale_pos_overwrite_blocked", "stale_pos_overwrite_blocked"),
        ("mesh_ghosts", "mesh_ghosts"),
        ("slow_viewer", "slow_viewer"),
    ] {
        if let Some(value) = fields.get(field) {
            let value = value
                .parse::<f64>()
                .map_err(|err| format!("invalid numeric reality_loadgen field {field}: {err}"))?;
            outcome = outcome.with_metric(metric, value);
        }
    }

    let handoff = ModelFeatureBlock::new(ModelFeatureBlockKind::HandoffPressure, cfg.provenance())
        .with_metric(
            "east_authority_gain",
            parse_metric(&fields, "east_authority_gain")?,
        )
        .with_metric("handoff_pos_ok", parse_metric(&fields, "handoff_pos_ok")?)
        .with_metric(
            "stale_pos_rejected_exact",
            parse_metric(&fields, "stale_pos_rejected_exact")?,
        )
        .with_metric(
            "stale_pos_overwrite_blocked",
            parse_metric(&fields, "stale_pos_overwrite_blocked")?,
        )
        .with_metric("mesh_ghosts", parse_metric(&fields, "mesh_ghosts")?)
        .with_dimension("artifact_kind", "reality_loadgen")
        .with_dimension("result", result)
        .with_dimension("mode", mode);

    validate_blocks(vec![outcome, handoff])
}

fn build_agar_live_gate_feature_blocks(
    content: &str,
    cfg: &FeatureBuildConfig,
) -> Result<Vec<ModelFeatureBlock>, String> {
    let artifact = serde_json::from_str::<Value>(content.trim_start_matches('\u{feff}').trim())
        .map_err(|err| format!("invalid agar live gate json: {err}"))?;
    reject_forbidden_source_keys(&artifact, 1, "$")?;
    if artifact.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err("agar live gate artifact is not ok".to_string());
    }

    let family = agar_gate_family(&artifact);
    let mut outcome = ModelFeatureBlock::new(ModelFeatureBlockKind::Outcome, cfg.provenance())
        .with_metric("ok", 1.0)
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", family);

    if let Some(bytes) = json_number_at(&artifact, &["wal", "bytes"]) {
        outcome = outcome.with_metric("wal_bytes", bytes);
    }
    if let Some(entities) = json_number_at(&artifact, &["brokerView", "entities"]) {
        outcome = outcome.with_metric("broker_view_entities", entities);
    }
    if let Some(security) = artifact.get("security").and_then(Value::as_object) {
        outcome = outcome.with_metric(
            "security_checks",
            security
                .values()
                .filter(|value| value.as_bool() == Some(true))
                .count() as f64,
        );
    }

    let mut blocks = vec![outcome];

    if let Some(entity_density) = agar_entity_density_block(&artifact, cfg, family) {
        blocks.push(entity_density);
    }
    if let Some(worker_load) = agar_worker_load_block(&artifact, cfg, family)? {
        blocks.push(worker_load);
    }
    if let Some(handoff) = agar_handoff_block(&artifact, cfg, family) {
        blocks.push(handoff);
    }

    validate_blocks(blocks)
}

fn parse_key_value_line(line: &str) -> BTreeMap<String, String> {
    line.split_whitespace()
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn normalized_reality_loadgen_line(line: &str) -> Option<&str> {
    let line = line.trim_start_matches('\u{feff}').trim_start();
    line.starts_with("reality_loadgen ").then_some(line)
}

fn parse_metric(fields: &BTreeMap<String, String>, key: &str) -> Result<f64, String> {
    let value = fields
        .get(key)
        .ok_or_else(|| format!("missing numeric reality_loadgen field {key}"))?;
    value
        .parse::<f64>()
        .map_err(|err| format!("invalid numeric reality_loadgen field {key}: {err}"))
}

fn agar_gate_family(artifact: &Value) -> &'static str {
    if artifact.get("monitor").is_some() {
        "mit_clone_adapter"
    } else {
        "godworks_agar_v2"
    }
}

fn agar_entity_density_block(
    artifact: &Value,
    cfg: &FeatureBuildConfig,
    family: &str,
) -> Option<ModelFeatureBlock> {
    let live_entities = json_number_at(artifact, &["monitor", "entities"])
        .or_else(|| json_number_at(artifact, &["max_entities"]))?;
    let mut block = ModelFeatureBlock::new(ModelFeatureBlockKind::EntityDensity, cfg.provenance())
        .with_metric("live_entities", live_entities)
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", family);

    for (path, metric) in [
        (&["monitor", "players"][..], "live_players"),
        (&["samples"][..], "samples"),
        (&["max_owners"][..], "max_owners"),
        (&["player_owner_count"][..], "player_owner_count"),
        (&["unknown_owner_frames"][..], "unknown_owner_frames"),
        (&["duplicate_frames"][..], "duplicate_frames"),
    ] {
        if let Some(value) = json_number_at(artifact, path) {
            block = block.with_metric(metric, value);
        }
    }

    Some(block)
}

fn agar_worker_load_block(
    artifact: &Value,
    cfg: &FeatureBuildConfig,
    family: &str,
) -> Result<Option<ModelFeatureBlock>, String> {
    let Some(loads) = json_number_array_at(artifact, &["monitor", "loads"])? else {
        return Ok(None);
    };
    if loads.is_empty() {
        return Ok(None);
    }
    let sum: f64 = loads.iter().sum();
    let mean = sum / loads.len() as f64;
    let min = loads.iter().copied().fold(f64::INFINITY, f64::min);
    let peak = loads.iter().copied().fold(0.0, f64::max);
    let mut block = ModelFeatureBlock::new(ModelFeatureBlockKind::WorkerLoad, cfg.provenance())
        .with_metric("worker_count", loads.len() as f64)
        .with_metric("load_mean", mean)
        .with_metric("load_min", min)
        .with_metric("load_peak", peak)
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", family);

    for (path, metric) in [
        (&["monitor", "rebalanceCount"][..], "rebalance_count"),
        (
            &["monitor", "dynamicWidthClasses"][..],
            "dynamic_width_classes",
        ),
        (
            &["monitor", "dynamicHeightClasses"][..],
            "dynamic_height_classes",
        ),
    ] {
        if let Some(value) = json_number_at(artifact, path) {
            block = block.with_metric(metric, value);
        }
    }

    Ok(Some(block))
}

fn agar_handoff_block(
    artifact: &Value,
    cfg: &FeatureBuildConfig,
    family: &str,
) -> Option<ModelFeatureBlock> {
    let has_handoff_metrics = artifact.get("observed_owner_changes").is_some()
        || artifact.get("player_owner_count").is_some()
        || artifact
            .get("brokerView")
            .and_then(Value::as_object)
            .is_some();
    if !has_handoff_metrics {
        return None;
    }

    let mut block =
        ModelFeatureBlock::new(ModelFeatureBlockKind::HandoffPressure, cfg.provenance())
            .with_dimension("artifact_kind", "agar_live_gate")
            .with_dimension("gate_family", family);

    for (path, metric) in [
        (&["observed_owner_changes"][..], "observed_owner_changes"),
        (&["player_owner_count"][..], "player_owner_count"),
        (&["player_path"][..], "player_path"),
        (&["player_max_displacement"][..], "player_max_displacement"),
        (&["client_truth_matches"][..], "client_truth_matches"),
        (&["client_truth_mismatches"][..], "client_truth_mismatches"),
        (
            &["probe_max_missing_before_handoff_streak"][..],
            "probe_max_missing_before_handoff_streak",
        ),
        (&["brokerView", "entities"][..], "broker_view_entities"),
    ] {
        if let Some(value) = json_number_at(artifact, path) {
            block = block.with_metric(metric, value);
        }
    }
    for (path, metric) in [
        (&["initial_command_ok"][..], "initial_command_ok"),
        (
            &["command_after_handoff_ok"][..],
            "command_after_handoff_ok",
        ),
    ] {
        if let Some(value) = json_bool_at(artifact, path) {
            block = block.with_metric(metric, if value { 1.0 } else { 0.0 });
        }
    }
    if let Some(owners) = artifact
        .get("brokerView")
        .and_then(|view| view.get("owners"))
        .and_then(Value::as_array)
    {
        block = block.with_metric("broker_owner_count", owners.len() as f64);
    }

    (!block.metrics.is_empty()).then_some(block)
}

fn json_number_at(value: &Value, path: &[&str]) -> Option<f64> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_f64()
}

fn json_bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_bool()
}

fn json_number_array_at(value: &Value, path: &[&str]) -> Result<Option<Vec<f64>>, String> {
    let mut cursor = value;
    for key in path {
        let Some(next) = cursor.get(*key) else {
            return Ok(None);
        };
        cursor = next;
    }
    let Some(items) = cursor.as_array() else {
        return Err(format!("expected numeric array at {}", path.join(".")));
    };
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_f64() else {
            return Err(format!("expected numeric value at {}", path.join(".")));
        };
        values.push(value);
    }
    Ok(Some(values))
}

fn validate_blocks(blocks: Vec<ModelFeatureBlock>) -> Result<Vec<ModelFeatureBlock>, String> {
    for block in &blocks {
        block
            .validate()
            .map_err(|err| format!("feature block validation failed: {err:?}"))?;
    }
    Ok(blocks)
}

fn block_to_json(block: &ModelFeatureBlock) -> Value {
    json!({
        "kind": block.kind.as_wire_str(),
        "schema_version": MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
        "project_id": block.provenance.project_id,
        "dataset_id": block.provenance.dataset_id,
        "trace_id": block.provenance.trace_id,
        "source_artifact": block.provenance.source_artifact,
        "redacted": block.redacted,
        "metrics": block.metrics,
        "dimensions": block.dimensions,
    })
}

fn reject_forbidden_source_keys(value: &Value, line_no: usize, path: &str) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}.{key}");
                if FORBIDDEN_SOURCE_KEYS.contains(&key.as_str()) {
                    return Err(format!(
                        "line {line_no}: forbidden raw source key at {child_path}"
                    ));
                }
                reject_forbidden_source_keys(child, line_no, &child_path)?;
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                reject_forbidden_source_keys(child, line_no, &format!("{path}[{idx}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn insert_if_str(event: &Value, key: &str, values: &mut BTreeSet<String>) {
    if let Some(value) = event.get(key).and_then(Value::as_str) {
        values.insert(value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> FeatureBuildConfig {
        FeatureBuildConfig {
            project_id: "arena".to_string(),
            dataset_id: "dataset-v1".to_string(),
            trace_id: "trace-42".to_string(),
            source_artifact: "test-artifact".to_string(),
        }
    }

    #[test]
    fn text_artifact_decoder_accepts_powershell_utf16_json() {
        let mut bytes = vec![0xFF, 0xFE];
        for unit in "{\"ok\":true}\n".encode_utf16() {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }

        assert_eq!(
            decode_text_artifact_bytes(&bytes).unwrap(),
            "{\"ok\":true}\n"
        );
    }

    #[test]
    fn replay_builder_emits_valid_redacted_feature_blocks() {
        let tape = r#"
{"kind":"broker_ingress","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"outcome":"dispatched","durable_gen":1,"pending_gen":1,"op_summary":{"op":"UpdateComponent","wire_bytes":96}}
{"kind":"broker_outbound","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"op":"UpdateRejected","error":"role_policy_error","rejected_op":"CreateEntity","peer_role":"client","durable_gen":1}
{"kind":"broker_handoff","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"grid2d","cols":2,"rows":2},"path":"cross_broker_emit","entity":"ship-1","from":"Z0_0","to":"Z1_0","authority_epoch":3,"durable_gen":8}
"#;
        let blocks = build_replay_feature_blocks(tape, &cfg()).unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::IngressRejectCost);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].metrics["event_count"], 3.0);
        assert_eq!(blocks[1].metrics["mesh_handoff_events"], 1.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
            let json = block_to_json(&block);
            let encoded = serde_json::to_string(&json).unwrap();
            assert!(!encoded.contains("auth_token"));
            assert!(!encoded.contains("payload"));
            assert!(!encoded.contains("components"));
            assert!(!encoded.contains("updates"));
        }
    }

    #[test]
    fn replay_builder_rejects_raw_source_payload_before_summary() {
        let tape = r#"{"kind":"broker_ingress","spatial_dim":"D2","coordinate_codec":"debug_f64_2","component_registry_version":1,"partition_schema":{"kind":"strip1d","boundary_count":1},"outcome":"dispatched","op_summary":{"op":"UpdateComponent","wire_bytes":64},"payload":{"secret":true}}"#;

        let err = build_replay_feature_blocks(tape, &cfg()).unwrap_err();
        assert!(err.contains("forbidden raw source key"));
    }

    #[test]
    fn reality_loadgen_builder_emits_valid_outcome_and_handoff_blocks() {
        let output = "noise\n\u{feff}  reality_loadgen result=pass mode=cross-broker entities=12 ticks=90 elapsed=3.00 add=24 updates=180 coarse=12 events=32 visual_events=16 command_req_owner=1 command_resp_caller=1 query_resp=2 rejections=12 east_add=12 east_updates=70 east_visible=82 east_authority_gain=12 handoff_pos_ok=12 stale_pos_rejected_exact=12 stale_pos_overwrite_blocked=12 handoff_pos_query_error=none mesh_ghosts=3 slow_viewer=1 failures=none\n";
        let blocks = build_reality_loadgen_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].dimensions["result"], "pass");
        assert_eq!(blocks[0].metrics["component_update_frames"], 180.0);
        assert_eq!(blocks[1].metrics["handoff_pos_ok"], 12.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn reality_loadgen_builder_rejects_non_finite_metrics() {
        let output = "reality_loadgen result=pass mode=cross-broker entities=12 ticks=90 elapsed=NaN add=24 updates=180 coarse=12 events=32 visual_events=16 command_req_owner=1 command_resp_caller=1 query_resp=2 rejections=12 east_add=12 east_updates=70 east_visible=82 east_authority_gain=12 handoff_pos_ok=12 stale_pos_rejected_exact=12 stale_pos_overwrite_blocked=12 mesh_ghosts=3 slow_viewer=1 failures=none";

        let err = build_reality_loadgen_feature_blocks(output, &cfg()).unwrap_err();
        assert!(err.contains("NonFiniteMetric"));
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_mit_clone_blocks() {
        let output = r#"{
          "ok": true,
          "game": "http://127.0.0.1:3000/",
          "monitor": {
            "entities": 1081,
            "players": 32,
            "rebalanceCount": 0,
            "loads": [81,90,86,97],
            "dynamicWidthClasses": 4,
            "dynamicHeightClasses": 13
          },
          "brokerView": {
            "entities": 34,
            "owners": ["mit-Z3_3","mit-Z1_2","mit-Z2_3"]
          },
          "wal": {
            "path": "C:\\local\\mirror.wal",
            "bytes": 30656189
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::WorkerLoad);
        assert_eq!(blocks[3].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].metrics["wal_bytes"], 30656189.0);
        assert_eq!(blocks[1].metrics["live_entities"], 1081.0);
        assert_eq!(blocks[2].metrics["load_peak"], 97.0);
        assert_eq!(blocks[3].metrics["broker_owner_count"], 3.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_godworks_agar_v2_blocks() {
        let output = r#"{
          "ok": true,
          "samples": 90,
          "max_entities": 180,
          "max_owners": 16,
          "unknown_owner_frames": 0,
          "duplicate_frames": 0,
          "player_owner_count": 3,
          "observed_owner_changes": 2,
          "player_path": 55.5,
          "player_max_displacement": 44.0,
          "client_truth_matches": 40,
          "client_truth_mismatches": 0,
          "probe_max_missing_before_handoff_streak": 0,
          "initial_command_ok": true,
          "command_after_handoff_ok": true,
          "security": {
            "peer_declared_mesh_rejected": true,
            "wrong_claim_mesh_rejected": true
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].metrics["security_checks"], 2.0);
        assert_eq!(blocks[1].metrics["live_entities"], 180.0);
        assert_eq!(blocks[2].metrics["command_after_handoff_ok"], 1.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_rejects_failed_gate_or_raw_payload() {
        let failed = r#"{"ok":false,"monitor":{"entities":100,"players":2}}"#;
        assert!(build_agar_live_gate_feature_blocks(failed, &cfg())
            .unwrap_err()
            .contains("not ok"));

        let raw = r#"{"ok":true,"max_entities":10,"payload":{"raw":true}}"#;
        assert!(build_agar_live_gate_feature_blocks(raw, &cfg())
            .unwrap_err()
            .contains("forbidden raw source key"));
    }
}
