use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::process;

use godworks_core::{
    ModelFeatureBlock, ModelFeatureBlockKind, ModelFeatureBlockProvenance,
    MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
};
use serde_json::{json, Value};

const FORBIDDEN_SOURCE_KEYS: &[&str] = &[
    "auth_token",
    "value",
    "payload",
    "components",
    "updates",
    "lastCommand",
    "lastTarget",
    "target",
];

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
    if artifact.get("gate").and_then(Value::as_str)
        == Some("mit_clone_broker_command_stress_ladder")
    {
        return build_mit_clone_stress_ladder_feature_blocks(&artifact, cfg);
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
    for (path, metric) in [
        (&["capacity", "samples"][..], "capacity_samples"),
        (&["capacity", "okSamples"][..], "capacity_ok_samples"),
        (&["capacity", "durationMs"][..], "capacity_duration_ms"),
        (
            &["capacity", "loadPeakToMeanMax"][..],
            "capacity_load_peak_to_mean_max",
        ),
    ] {
        if let Some(value) = json_number_at(&artifact, path) {
            outcome = outcome.with_metric(metric, value);
        }
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
    if artifact.get("gate").and_then(Value::as_str)
        == Some("mit_clone_broker_command_stress_ladder")
        || artifact.get("monitor").is_some()
        || artifact.get("playableSeam").is_some()
        || artifact.get("brokerCommand").is_some()
        || artifact.get("brokerCommandCapacity").is_some()
    {
        "mit_clone_adapter"
    } else {
        "godworks_agar_v2"
    }
}

fn build_mit_clone_stress_ladder_feature_blocks(
    artifact: &Value,
    cfg: &FeatureBuildConfig,
) -> Result<Vec<ModelFeatureBlock>, String> {
    let rows = artifact
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| "MIT clone stress ladder artifact has no rows".to_string())?;
    if rows.is_empty() {
        return Err("MIT clone stress ladder artifact has no rows".to_string());
    }

    let row_count = rows.len() as f64;
    let passed_rows = rows
        .iter()
        .filter(|row| row.get("ok").and_then(Value::as_bool) == Some(true))
        .count() as f64;
    if passed_rows < row_count {
        return Err("MIT clone stress ladder contains failed profile rows".to_string());
    }

    let max_bot_count = max_row_metric(rows, "botCount")?;
    let min_players = min_row_metric(rows, "playersMin")?;
    let max_players = max_row_metric(rows, "playersMax")?;
    let min_entities = min_row_metric(rows, "entitiesMin")?;
    let max_entities = max_row_metric(rows, "entitiesMax")?;
    let min_worker_slots = min_row_metric(rows, "workerSlotsMin")?;
    let max_worker_slots = max_row_metric(rows, "workerSlotsMax")?;
    let max_peak_to_mean = max_row_metric(rows, "loadPeakToMeanMax")?;
    let total_completed = sum_row_metric(rows, "completedPlayers")?;
    let total_failed = sum_row_metric(rows, "failedPlayers")?;
    let total_responses = sum_row_metric(rows, "totalCommandResponses")?;
    let total_owner_matches = sum_row_metric(rows, "totalCommandOwnerMatches")?;
    let min_post_seam_path = min_row_metric(rows, "minPostSeamPath")?;
    let all_post_seam_ok = rows
        .iter()
        .all(|row| row.get("allPostSeamCommandOk").and_then(Value::as_bool) == Some(true));

    let outcome = ModelFeatureBlock::new(ModelFeatureBlockKind::Outcome, cfg.provenance())
        .with_metric("ok", 1.0)
        .with_metric("ladder_profiles", row_count)
        .with_metric("ladder_passed_profiles", passed_rows)
        .with_metric("ladder_max_bot_count_green", max_bot_count)
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", "mit_clone_adapter")
        .with_dimension("gate", "mit_clone_broker_command_stress_ladder");

    let density = ModelFeatureBlock::new(ModelFeatureBlockKind::EntityDensity, cfg.provenance())
        .with_metric("capacity_entities_min", min_entities)
        .with_metric("capacity_entities_max", max_entities)
        .with_metric("capacity_players_min", min_players)
        .with_metric("capacity_players_max", max_players)
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", "mit_clone_adapter")
        .with_dimension("gate", "mit_clone_broker_command_stress_ladder");

    let worker_load = ModelFeatureBlock::new(ModelFeatureBlockKind::WorkerLoad, cfg.provenance())
        .with_metric("capacity_workers_min", min_worker_slots)
        .with_metric("capacity_workers_max", max_worker_slots)
        .with_metric("capacity_load_peak_to_mean_max", max_peak_to_mean)
        .with_metric(
            "capacity_rebalance_delta",
            sum_row_metric(rows, "rebalanceDelta")?,
        )
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", "mit_clone_adapter")
        .with_dimension("gate", "mit_clone_broker_command_stress_ladder");

    let handoff = ModelFeatureBlock::new(ModelFeatureBlockKind::HandoffPressure, cfg.provenance())
        .with_metric("broker_command_completed_players", total_completed)
        .with_metric("broker_command_failed_players", total_failed)
        .with_metric("broker_command_total_responses", total_responses)
        .with_metric("broker_command_total_owner_matches", total_owner_matches)
        .with_metric("broker_command_min_post_seam_path", min_post_seam_path)
        .with_metric(
            "broker_command_all_post_seam_ok",
            if all_post_seam_ok { 1.0 } else { 0.0 },
        )
        .with_dimension("artifact_kind", "agar_live_gate")
        .with_dimension("gate_family", "mit_clone_adapter")
        .with_dimension("gate", "mit_clone_broker_command_stress_ladder");

    validate_blocks(vec![outcome, density, worker_load, handoff])
}

fn row_metric(row: &Value, key: &str) -> Result<f64, String> {
    row.get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("MIT clone stress ladder row missing numeric {key}"))
}

fn min_row_metric(rows: &[Value], key: &str) -> Result<f64, String> {
    let mut min = f64::INFINITY;
    for row in rows {
        min = min.min(row_metric(row, key)?);
    }
    Ok(min)
}

fn max_row_metric(rows: &[Value], key: &str) -> Result<f64, String> {
    let mut max = f64::NEG_INFINITY;
    for row in rows {
        max = max.max(row_metric(row, key)?);
    }
    Ok(max)
}

fn sum_row_metric(rows: &[Value], key: &str) -> Result<f64, String> {
    let mut sum = 0.0;
    for row in rows {
        sum += row_metric(row, key)?;
    }
    Ok(sum)
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
        (&["capacity", "entitiesMin"][..], "capacity_entities_min"),
        (&["capacity", "entitiesMean"][..], "capacity_entities_mean"),
        (&["capacity", "playersMin"][..], "capacity_players_min"),
        (&["capacity", "playersMean"][..], "capacity_players_mean"),
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
        (&["capacity", "workersMin"][..], "capacity_workers_min"),
        (&["capacity", "workersMax"][..], "capacity_workers_max"),
        (&["capacity", "loadMeanMin"][..], "capacity_load_mean_min"),
        (&["capacity", "loadMeanMax"][..], "capacity_load_mean_max"),
        (&["capacity", "loadPeakMin"][..], "capacity_load_peak_min"),
        (&["capacity", "loadPeakMax"][..], "capacity_load_peak_max"),
        (
            &["capacity", "rebalanceDelta"][..],
            "capacity_rebalance_delta",
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
        || artifact.get("playableSeam").is_some()
        || artifact.get("brokerCommand").is_some()
        || artifact.get("brokerCommandCapacity").is_some()
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
        (&["playableSeam", "blockChanges"][..], "shard_block_changes"),
        (&["playableSeam", "path"][..], "player_path"),
        (&["playableSeam", "postSeamPath"][..], "post_seam_path"),
        (&["playableSeam", "commandCount"][..], "command_count"),
        (&["playableSeam", "serverFrames"][..], "server_frames"),
        (
            &["playableSeam", "maxMissingStreak"][..],
            "probe_max_missing_streak",
        ),
        (
            &["brokerCommand", "ownerChanges"][..],
            "broker_command_owner_changes",
        ),
        (
            &["brokerCommand", "blockChanges"][..],
            "broker_command_block_changes",
        ),
        (&["brokerCommand", "path"][..], "broker_command_path"),
        (
            &["brokerCommand", "postSeamPath"][..],
            "broker_command_post_seam_path",
        ),
        (
            &["brokerCommand", "commandResponses"][..],
            "broker_command_responses",
        ),
        (
            &["brokerCommand", "commandOwnerMatches"][..],
            "broker_command_owner_matches",
        ),
        (
            &["brokerCommand", "bridgeCommandCount"][..],
            "broker_command_bridge_count",
        ),
        (
            &["brokerCommandCapacity", "controlledPlayers"][..],
            "broker_command_controlled_players",
        ),
        (
            &["brokerCommandCapacity", "completedPlayers"][..],
            "broker_command_completed_players",
        ),
        (
            &["brokerCommandCapacity", "minCommandResponses"][..],
            "broker_command_min_responses",
        ),
        (
            &["brokerCommandCapacity", "totalCommandResponses"][..],
            "broker_command_total_responses",
        ),
        (
            &["brokerCommandCapacity", "totalCommandOwnerMatches"][..],
            "broker_command_total_owner_matches",
        ),
        (
            &["brokerCommandCapacity", "minOwnerChanges"][..],
            "broker_command_min_owner_changes",
        ),
        (
            &["brokerCommandCapacity", "maxOwnerChanges"][..],
            "broker_command_max_owner_changes",
        ),
        (
            &["brokerCommandCapacity", "minBlockChanges"][..],
            "broker_command_min_block_changes",
        ),
        (
            &["brokerCommandCapacity", "maxBlockChanges"][..],
            "broker_command_max_block_changes",
        ),
        (
            &["brokerCommandCapacity", "minPath"][..],
            "broker_command_min_path",
        ),
        (
            &["brokerCommandCapacity", "minPostSeamPath"][..],
            "broker_command_min_post_seam_path",
        ),
        (
            &["probe_max_missing_before_handoff_streak"][..],
            "probe_max_missing_before_handoff_streak",
        ),
        (&["brokerView", "entities"][..], "broker_view_entities"),
        (&["brokerView", "entitiesMax"][..], "broker_view_entities"),
        (&["brokerView", "ownerCountMax"][..], "broker_owner_count"),
        (
            &["brokerView", "mitOwnerCountMax"][..],
            "broker_mit_owner_count",
        ),
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
        (
            &["playableSeam", "brokerMirrorMatched"][..],
            "broker_mirror_matched",
        ),
        (
            &["brokerCommandCapacity", "allPostSeamCommandOk"][..],
            "broker_command_all_post_seam_ok",
        ),
    ] {
        if let Some(value) = json_bool_at(artifact, path) {
            block = block.with_metric(metric, if value { 1.0 } else { 0.0 });
        }
    }
    if let Some(first_block) = artifact
        .get("playableSeam")
        .or_else(|| artifact.get("brokerCommand"))
        .and_then(|seam| seam.get("firstBlock"))
        .and_then(Value::as_str)
    {
        block = block.with_dimension("first_block", first_block);
    }
    if let Some(final_block) = artifact
        .get("playableSeam")
        .or_else(|| artifact.get("brokerCommand"))
        .and_then(|seam| seam.get("finalBlock"))
        .and_then(Value::as_str)
    {
        block = block.with_dimension("final_block", final_block);
    }
    if let Some(first_owner) = artifact
        .get("brokerCommand")
        .and_then(|seam| seam.get("firstOwner"))
        .and_then(Value::as_str)
    {
        block = block.with_dimension("first_owner", first_owner);
    }
    if let Some(final_owner) = artifact
        .get("brokerCommand")
        .and_then(|seam| seam.get("finalOwner"))
        .and_then(Value::as_str)
    {
        block = block.with_dimension("final_owner", final_owner);
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
    fn agar_live_gate_builder_emits_valid_playable_seam_blocks() {
        let output = r#"{
          "ok": true,
          "game": "http://127.0.0.1:3000",
          "monitorUrl": "http://127.0.0.1:8091/state",
          "brokerViewUrl": "http://127.0.0.1:8092/state",
          "playableSeam": {
            "probeName": "gw_seam_test",
            "socketId": "sock-1",
            "firstBlock": "W1_2",
            "finalBlock": "W2_2",
            "blockChanges": 1,
            "path": 675.0,
            "postSeamPath": 37.5,
            "commandCount": 41,
            "serverFrames": 111,
            "maxMissingStreak": 1,
            "brokerMirrorMatched": true,
            "brokerMirrorOwner": "mit-Z2_2"
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].dimensions["gate_family"], "mit_clone_adapter");
        assert_eq!(blocks[1].dimensions["first_block"], "W1_2");
        assert_eq!(blocks[1].dimensions["final_block"], "W2_2");
        assert_eq!(blocks[1].metrics["shard_block_changes"], 1.0);
        assert_eq!(blocks[1].metrics["player_path"], 675.0);
        assert_eq!(blocks[1].metrics["post_seam_path"], 37.5);
        assert_eq!(blocks[1].metrics["broker_mirror_matched"], 1.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_broker_command_blocks() {
        let output = r#"{
          "ok": true,
          "bridgeUrl": "http://127.0.0.1:8093",
          "monitor": {
            "entities": 944,
            "players": 36,
            "rebalanceCount": 2,
            "loads": [60,61,71,64]
          },
          "brokerCommand": {
            "entity": "sock-1:0",
            "socketId": "sock-1",
            "firstOwner": "mit-Z1_1",
            "finalOwner": "mit-Z2_1",
            "ownerChanges": 1,
            "firstBlock": "W1_1",
            "finalBlock": "W2_1",
            "blockChanges": 1,
            "path": 284.5,
            "postSeamPath": 32.25,
            "commandResponses": 7,
            "commandOwnerMatches": 7,
            "bridgeCommandCount": 7
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::WorkerLoad);
        assert_eq!(blocks[3].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].dimensions["gate_family"], "mit_clone_adapter");
        assert_eq!(blocks[3].dimensions["first_owner"], "mit-Z1_1");
        assert_eq!(blocks[3].dimensions["final_owner"], "mit-Z2_1");
        assert_eq!(blocks[3].metrics["broker_command_owner_changes"], 1.0);
        assert_eq!(blocks[3].metrics["broker_command_responses"], 7.0);
        assert_eq!(blocks[3].metrics["broker_command_post_seam_path"], 32.25);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_capacity_blocks() {
        let output = r#"{
          "ok": true,
          "gate": "mit_clone_capacity",
          "monitor": {
            "entities": 1108,
            "players": 40,
            "rebalanceCount": 7,
            "loads": [71,72,70,73],
            "dynamicWidthClasses": 4,
            "dynamicHeightClasses": 11
          },
          "capacity": {
            "samples": 30,
            "okSamples": 29,
            "durationMs": 15100,
            "entitiesMin": 1012,
            "entitiesMax": 1108,
            "entitiesMean": 1066.4,
            "playersMin": 37,
            "playersMax": 40,
            "playersMean": 39.1,
            "workersMin": 16,
            "workersMax": 16,
            "loadMeanMin": 66.5,
            "loadMeanMax": 72.25,
            "loadPeakMin": 81,
            "loadPeakMax": 97,
            "loadPeakToMeanMax": 1.35,
            "rebalanceStart": 6,
            "rebalanceEnd": 7,
            "rebalanceDelta": 1
          },
          "brokerView": {
            "samples": 30,
            "entitiesMax": 40,
            "ownerCountMax": 16,
            "mitOwnerCountMax": 16
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::WorkerLoad);
        assert_eq!(blocks[3].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].metrics["capacity_ok_samples"], 29.0);
        assert_eq!(blocks[1].metrics["capacity_entities_min"], 1012.0);
        assert_eq!(blocks[1].metrics["capacity_players_mean"], 39.1);
        assert_eq!(blocks[2].metrics["capacity_workers_min"], 16.0);
        assert_eq!(blocks[2].metrics["capacity_load_peak_max"], 97.0);
        assert_eq!(blocks[3].metrics["broker_mit_owner_count"], 16.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_broker_command_capacity_blocks() {
        let output = r#"{
          "ok": true,
          "gate": "mit_clone_broker_command_capacity",
          "monitor": {
            "entities": 1130,
            "players": 42,
            "rebalanceCount": 8,
            "loads": [71,72,70,73],
            "dynamicWidthClasses": 4,
            "dynamicHeightClasses": 11
          },
          "capacity": {
            "samples": 18,
            "okSamples": 16,
            "durationMs": 21000,
            "entitiesMin": 1010,
            "entitiesMax": 1130,
            "entitiesMean": 1080.5,
            "playersMin": 38,
            "playersMax": 42,
            "playersMean": 40.2,
            "workersMin": 16,
            "workersMax": 16,
            "loadMeanMin": 62.1,
            "loadMeanMax": 72.25,
            "loadPeakMin": 81,
            "loadPeakMax": 99,
            "loadPeakToMeanMax": 1.37,
            "rebalanceStart": 7,
            "rebalanceEnd": 8,
            "rebalanceDelta": 1
          },
          "brokerView": {
            "samples": 18,
            "entitiesMax": 44,
            "ownerCountMax": 16,
            "mitOwnerCountMax": 16
          },
          "brokerCommandCapacity": {
            "controlledPlayers": 4,
            "completedPlayers": 4,
            "minCommandResponses": 5,
            "totalCommandResponses": 27,
            "totalCommandOwnerMatches": 27,
            "minOwnerChanges": 1,
            "maxOwnerChanges": 2,
            "minBlockChanges": 1,
            "maxBlockChanges": 3,
            "minPath": 147.5,
            "minPostSeamPath": 22.0,
            "allPostSeamCommandOk": true,
            "players": [
              {
                "entity": "sock-1:0",
                "socketId": "sock-1",
                "ownerChanges": 1,
                "blockChanges": 1,
                "path": 211.0,
                "postSeamPath": 22.0,
                "commandResponses": 5,
                "commandOwnerMatches": 5
              }
            ]
          }
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::WorkerLoad);
        assert_eq!(blocks[3].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].dimensions["gate_family"], "mit_clone_adapter");
        assert_eq!(blocks[0].metrics["capacity_ok_samples"], 16.0);
        assert_eq!(blocks[1].metrics["capacity_players_mean"], 40.2);
        assert_eq!(blocks[2].metrics["capacity_workers_min"], 16.0);
        assert_eq!(blocks[3].metrics["broker_command_controlled_players"], 4.0);
        assert_eq!(blocks[3].metrics["broker_command_completed_players"], 4.0);
        assert_eq!(blocks[3].metrics["broker_command_min_responses"], 5.0);
        assert_eq!(
            blocks[3].metrics["broker_command_total_owner_matches"],
            27.0
        );
        assert_eq!(blocks[3].metrics["broker_command_min_owner_changes"], 1.0);
        assert_eq!(blocks[3].metrics["broker_command_min_post_seam_path"], 22.0);
        assert_eq!(blocks[3].metrics["broker_command_all_post_seam_ok"], 1.0);
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

        let raw_command = r#"{"ok":true,"max_entities":10,"brokerCommandCapacity":{"lastCommand":{"target":{"x":1,"y":2}}}}"#;
        assert!(build_agar_live_gate_feature_blocks(raw_command, &cfg())
            .unwrap_err()
            .contains("forbidden raw source key"));
    }

    #[test]
    fn agar_live_gate_builder_emits_valid_mit_clone_ladder_blocks() {
        let output = r#"{
          "ok": true,
          "gate": "mit_clone_broker_command_stress_ladder",
          "generatedAt": "2026-07-01T12:07:57.6795128-03:00",
          "rows": [
            {
              "ok": true,
              "botCount": 40,
              "commandPlayers": 8,
              "minCompleted": 4,
              "minPlayersRequired": 30,
              "stackExitCode": 0,
              "gateExitCode": 0,
              "stackLog": ".local/agar_mit_clone_ladder/bots_40.stack.raw.log",
              "gateLog": ".local/agar_mit_clone_ladder/bots_40.gate.raw.log",
              "entitiesMin": 1085,
              "entitiesMax": 1097,
              "playersMin": 43,
              "playersMax": 48,
              "samples": 28,
              "okSamples": 28,
              "workerSlotsMin": 16,
              "workerSlotsMax": 16,
              "rebalanceDelta": 0,
              "loadPeakToMeanMax": 1.2578616352201257,
              "brokerMirrorEntitiesMax": 48,
              "brokerMirrorOwnerCountMax": 15,
              "completedPlayers": 4,
              "failedPlayers": 0,
              "totalCommandResponses": 84,
              "totalCommandOwnerMatches": 84,
              "minPostSeamPath": 28.57172610222915,
              "allPostSeamCommandOk": true
            }
          ]
        }"#;
        let blocks = build_agar_live_gate_feature_blocks(output, &cfg()).unwrap();

        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].kind, ModelFeatureBlockKind::Outcome);
        assert_eq!(blocks[1].kind, ModelFeatureBlockKind::EntityDensity);
        assert_eq!(blocks[2].kind, ModelFeatureBlockKind::WorkerLoad);
        assert_eq!(blocks[3].kind, ModelFeatureBlockKind::HandoffPressure);
        assert_eq!(blocks[0].dimensions["gate_family"], "mit_clone_adapter");
        assert_eq!(
            blocks[0].dimensions["gate"],
            "mit_clone_broker_command_stress_ladder"
        );
        assert_eq!(blocks[0].metrics["ladder_max_bot_count_green"], 40.0);
        assert_eq!(blocks[1].metrics["capacity_entities_min"], 1085.0);
        assert_eq!(blocks[1].metrics["capacity_players_max"], 48.0);
        assert_eq!(blocks[2].metrics["capacity_workers_min"], 16.0);
        assert!(
            (blocks[2].metrics["capacity_load_peak_to_mean_max"] - 1.2578616352201257).abs()
                < 0.00000000000001
        );
        assert_eq!(blocks[3].metrics["broker_command_completed_players"], 4.0);
        assert_eq!(blocks[3].metrics["broker_command_failed_players"], 0.0);
        assert_eq!(blocks[3].metrics["broker_command_total_responses"], 84.0);
        assert_eq!(
            blocks[3].metrics["broker_command_total_owner_matches"],
            84.0
        );
        assert_eq!(blocks[3].metrics["broker_command_all_post_seam_ok"], 1.0);
        for block in blocks {
            assert_eq!(block.validate(), Ok(()));
        }
    }

    #[test]
    fn agar_live_gate_builder_rejects_failed_mit_clone_ladder_row() {
        let output = r#"{
          "ok": true,
          "gate": "mit_clone_broker_command_stress_ladder",
          "rows": [
            {
              "ok": false,
              "botCount": 80,
              "entitiesMin": 1000,
              "entitiesMax": 1200,
              "playersMin": 60,
              "playersMax": 70,
              "workerSlotsMin": 16,
              "workerSlotsMax": 16,
              "rebalanceDelta": 1,
              "loadPeakToMeanMax": 1.5,
              "completedPlayers": 2,
              "failedPlayers": 6,
              "totalCommandResponses": 10,
              "totalCommandOwnerMatches": 8,
              "minPostSeamPath": 0,
              "allPostSeamCommandOk": false
            }
          ]
        }"#;

        assert!(build_agar_live_gate_feature_blocks(output, &cfg())
            .unwrap_err()
            .contains("failed profile rows"));
    }
}
