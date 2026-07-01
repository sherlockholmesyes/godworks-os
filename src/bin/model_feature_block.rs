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
            "usage: model_feature_block <replay|reality-loadgen> <path> <project_id> <dataset_id> <trace_id>"
        );
        process::exit(2);
    }

    let mode = &args[1];
    let path = &args[2];
    let content = match fs::read_to_string(path) {
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
        _ => Err(format!(
            "unknown mode '{mode}', expected replay or reality-loadgen"
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
}
