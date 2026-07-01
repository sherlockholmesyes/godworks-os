use std::collections::BTreeSet;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process;

use godworks_core::{
    ModelActionKind, ModelActionMode, ModelActionProposal, ModelActionProvenance,
    ModelDatasetManifest, ModelFeatureBlock, ModelFeatureBlockKind, ModelFeatureBlockProvenance,
    ModelPromotionDecision, ModelPromotionRecord,
};
use serde_json::{json, Value};

const FEATURES_FILE: &str = "features.jsonl";
const MANIFEST_FILE: &str = "manifest.json";

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("ingest") if args.len() == 3 => {
            let manifest = ingest_dataset(Path::new(&args[1]), Path::new(&args[2]))?;
            println!("{}", manifest_to_json(&manifest));
            Ok(())
        }
        Some("promote") if args.len() == 7 => {
            let record = persist_promotion_record(
                Path::new(&args[1]),
                Path::new(&args[2]),
                Path::new(&args[3]),
                &args[4],
                &args[5],
                Path::new(&args[6]),
            )?;
            println!("{}", promotion_record_to_json(&record));
            Ok(())
        }
        _ => Err("usage:\n  model_dataset_store ingest <feature_blocks.jsonl> <dataset_dir>\n  model_dataset_store promote <dataset_dir> <proposal.json> <replay_eval_report.json> <promotion_id> <accept|reject> <out.json>".to_string()),
    }
}

fn ingest_dataset(input_jsonl: &Path, dataset_dir: &Path) -> Result<ModelDatasetManifest, String> {
    let (blocks, normalized_lines) = read_feature_blocks(input_jsonl)?;
    let manifest = ModelDatasetManifest::from_feature_blocks(&blocks)
        .map_err(|err| format!("dataset manifest validation failed: {err:?}"))?;
    manifest
        .validate()
        .map_err(|err| format!("dataset manifest validation failed: {err:?}"))?;

    fs::create_dir_all(dataset_dir).map_err(|err| {
        format!(
            "failed to create dataset dir '{}': {err}",
            dataset_dir.display()
        )
    })?;
    write_new_utf8(
        &dataset_dir.join(FEATURES_FILE),
        &(normalized_lines.join("\n") + "\n"),
    )?;
    write_new_utf8(
        &dataset_dir.join(MANIFEST_FILE),
        &manifest_to_json(&manifest).to_string(),
    )?;
    Ok(manifest)
}

fn persist_promotion_record(
    dataset_dir: &Path,
    proposal_path: &Path,
    replay_eval_report_path: &Path,
    promotion_id: &str,
    decision: &str,
    out_path: &Path,
) -> Result<ModelPromotionRecord, String> {
    let manifest = read_manifest(&dataset_dir.join(MANIFEST_FILE))?;
    let proposal = read_proposal(proposal_path)?;
    let replay_eval_artifact = validate_replay_eval_report(replay_eval_report_path)?;
    let decision = ModelPromotionDecision::from_wire_str(decision)
        .ok_or_else(|| format!("invalid promotion decision '{decision}'"))?;
    let record = ModelPromotionRecord::new(
        promotion_id,
        manifest,
        proposal,
        decision,
        replay_eval_artifact,
    );
    record
        .validate()
        .map_err(|err| format!("promotion record validation failed: {err:?}"))?;
    write_new_utf8(out_path, &promotion_record_to_json(&record).to_string())?;
    Ok(record)
}

fn read_feature_blocks(path: &Path) -> Result<(Vec<ModelFeatureBlock>, Vec<String>), String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("failed to read '{}': {err}", path.display()))?;
    let mut blocks = Vec::new();
    let mut normalized_lines = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line_no = idx + 1;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|err| format!("line {line_no}: invalid feature block json: {err}"))?;
        let block = feature_block_from_json(&value, line_no)?;
        block
            .validate()
            .map_err(|err| format!("line {line_no}: feature block validation failed: {err:?}"))?;
        normalized_lines.push(feature_block_to_json(&block).to_string());
        blocks.push(block);
    }
    if blocks.is_empty() {
        return Err("feature block input is empty".to_string());
    }
    Ok((blocks, normalized_lines))
}

fn feature_block_from_json(value: &Value, line_no: usize) -> Result<ModelFeatureBlock, String> {
    require_object_keys(
        value,
        &[
            "kind",
            "schema_version",
            "project_id",
            "dataset_id",
            "trace_id",
            "source_artifact",
            "redacted",
            "metrics",
            "dimensions",
        ],
        &format!("line {line_no}"),
    )?;

    let kind = ModelFeatureBlockKind::from_wire_str(required_str(value, "kind")?)
        .ok_or_else(|| format!("line {line_no}: invalid feature block kind"))?;
    let schema_version = required_u64(value, "schema_version")?;
    let provenance = ModelFeatureBlockProvenance::new(
        required_str(value, "project_id")?,
        required_str(value, "dataset_id")?,
        required_str(value, "trace_id")?,
        required_str(value, "source_artifact")?,
    )
    .with_schema_version(schema_version);
    let mut block =
        ModelFeatureBlock::new(kind, provenance).with_redacted(required_bool(value, "redacted")?);

    for (name, metric) in required_object(value, "metrics")? {
        let metric = metric
            .as_f64()
            .ok_or_else(|| format!("line {line_no}: metric '{name}' must be a finite number"))?;
        block = block.with_metric(name, metric);
    }
    for (name, dimension) in required_object(value, "dimensions")? {
        let dimension = dimension
            .as_str()
            .ok_or_else(|| format!("line {line_no}: dimension '{name}' must be a string"))?;
        block = block.with_dimension(name, dimension);
    }
    Ok(block)
}

fn read_manifest(path: &Path) -> Result<ModelDatasetManifest, String> {
    let value = read_json(path)?;
    let manifest = manifest_from_json(&value)?;
    manifest
        .validate()
        .map_err(|err| format!("manifest validation failed: {err:?}"))?;
    Ok(manifest)
}

fn manifest_from_json(value: &Value) -> Result<ModelDatasetManifest, String> {
    require_object_keys(
        value,
        &[
            "schema_version",
            "project_id",
            "dataset_id",
            "feature_schema_version",
            "feature_block_count",
            "trace_ids",
            "source_artifacts",
        ],
        "manifest",
    )?;
    Ok(ModelDatasetManifest {
        project_id: required_str(value, "project_id")?.to_string(),
        dataset_id: required_str(value, "dataset_id")?.to_string(),
        schema_version: required_u64(value, "schema_version")?,
        feature_schema_version: required_u64(value, "feature_schema_version")?,
        feature_block_count: required_usize(value, "feature_block_count")?,
        trace_ids: required_string_array(value, "trace_ids")?,
        source_artifacts: required_string_array(value, "source_artifacts")?,
    })
}

fn read_proposal(path: &Path) -> Result<ModelActionProposal, String> {
    let value = read_json(path)?;
    proposal_from_json(&value)
}

fn proposal_from_json(value: &Value) -> Result<ModelActionProposal, String> {
    require_optional_object_keys(
        value,
        &[
            "kind",
            "mode",
            "project_id",
            "model_id",
            "dataset_id",
            "source_trace_id",
        ],
        &["validator_id"],
        "proposal",
    )?;
    let kind = ModelActionKind::from_wire_str(required_str(value, "kind")?)
        .ok_or_else(|| "proposal.kind: unknown action kind".to_string())?;
    let mode = ModelActionMode::from_wire_str(required_str(value, "mode")?)
        .ok_or_else(|| "proposal.mode: unknown action mode".to_string())?;
    let provenance = ModelActionProvenance::new(
        required_str(value, "project_id")?,
        required_str(value, "model_id")?,
        required_str(value, "dataset_id")?,
        required_str(value, "source_trace_id")?,
    );
    let mut proposal = ModelActionProposal::new(kind, mode, provenance);
    if let Some(validator_id) = value.get("validator_id").and_then(Value::as_str) {
        proposal = proposal.with_validator(validator_id);
    }
    proposal
        .validate()
        .map_err(|err| format!("proposal validation failed: {err:?}"))?;
    Ok(proposal)
}

fn validate_replay_eval_report(path: &Path) -> Result<String, String> {
    let value = read_json(path)?;
    require_object_keys(
        &value,
        &["events", "ok", "error_count", "errors"],
        "replay_eval_report",
    )?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err("replay_eval_report.ok must be true".to_string());
    }
    if required_u64(&value, "error_count")? != 0 {
        return Err("replay_eval_report.error_count must be 0".to_string());
    }
    if required_u64(&value, "events")? == 0 {
        return Err("replay_eval_report.events must be positive".to_string());
    }
    if !value
        .get("errors")
        .and_then(Value::as_array)
        .map(|errors| errors.is_empty())
        .unwrap_or(false)
    {
        return Err("replay_eval_report.errors must be empty".to_string());
    }
    Ok(format!("replay_eval:{}", normalize_path(path)))
}

fn feature_block_to_json(block: &ModelFeatureBlock) -> Value {
    json!({
        "kind": block.kind.as_wire_str(),
        "schema_version": block.provenance.schema_version,
        "project_id": block.provenance.project_id,
        "dataset_id": block.provenance.dataset_id,
        "trace_id": block.provenance.trace_id,
        "source_artifact": block.provenance.source_artifact,
        "redacted": block.redacted,
        "metrics": block.metrics,
        "dimensions": block.dimensions,
    })
}

fn manifest_to_json(manifest: &ModelDatasetManifest) -> Value {
    json!({
        "schema_version": manifest.schema_version,
        "project_id": manifest.project_id,
        "dataset_id": manifest.dataset_id,
        "feature_schema_version": manifest.feature_schema_version,
        "feature_block_count": manifest.feature_block_count,
        "trace_ids": manifest.trace_ids,
        "source_artifacts": manifest.source_artifacts,
    })
}

fn proposal_to_json(proposal: &ModelActionProposal) -> Value {
    let mut value = json!({
        "kind": proposal.kind.as_wire_str(),
        "mode": proposal.mode.as_wire_str(),
        "project_id": proposal.provenance.project_id,
        "model_id": proposal.provenance.model_id,
        "dataset_id": proposal.provenance.dataset_id,
        "source_trace_id": proposal.provenance.source_trace_id,
    });
    if let Some(validator_id) = &proposal.validator_id {
        value["validator_id"] = json!(validator_id);
    }
    value
}

fn promotion_record_to_json(record: &ModelPromotionRecord) -> Value {
    json!({
        "promotion_id": record.promotion_id,
        "schema_version": record.schema_version,
        "dataset": manifest_to_json(&record.dataset),
        "proposal": proposal_to_json(&record.proposal),
        "decision": record.decision.as_wire_str(),
        "replay_eval_artifact": record.replay_eval_artifact,
    })
}

fn read_json(path: &Path) -> Result<Value, String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("failed to read '{}': {err}", path.display()))?;
    serde_json::from_str(&content)
        .map_err(|err| format!("failed to parse '{}': {err}", path.display()))
}

fn write_new_utf8(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create '{}': {err}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| format!("failed to create new '{}': {err}", path.display()))?;
    file.write_all(content.as_bytes())
        .map_err(|err| format!("failed to write '{}': {err}", path.display()))?;
    file.write_all(b"\n")
        .map_err(|err| format!("failed to finish '{}': {err}", path.display()))
}

fn required_object<'a>(
    value: &'a Value,
    key: &str,
) -> Result<&'a serde_json::Map<String, Value>, String> {
    value
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{key}: expected object"))
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{key}: expected non-empty string"))
}

fn required_bool(value: &Value, key: &str) -> Result<bool, String> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("{key}: expected bool"))
}

fn required_u64(value: &Value, key: &str) -> Result<u64, String> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{key}: expected non-negative integer"))
}

fn required_usize(value: &Value, key: &str) -> Result<usize, String> {
    let raw = required_u64(value, key)?;
    usize::try_from(raw).map_err(|_| format!("{key}: integer does not fit usize"))
}

fn required_string_array(value: &Value, key: &str) -> Result<Vec<String>, String> {
    let items = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{key}: expected array"))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_str() else {
            return Err(format!("{key}: expected string array"));
        };
        if value.trim().is_empty() {
            return Err(format!("{key}: contains empty string"));
        }
        out.push(value.to_string());
    }
    Ok(out)
}

fn require_object_keys(value: &Value, required: &[&str], label: &str) -> Result<(), String> {
    require_optional_object_keys(value, required, &[], label)
}

fn require_optional_object_keys(
    value: &Value,
    required: &[&str],
    optional: &[&str],
    label: &str,
) -> Result<(), String> {
    let Some(map) = value.as_object() else {
        return Err(format!("{label}: expected object"));
    };
    for key in required {
        if !map.contains_key(*key) {
            return Err(format!("{label}: missing {key}"));
        }
    }
    let allowed: BTreeSet<&str> = required
        .iter()
        .copied()
        .chain(optional.iter().copied())
        .collect();
    for key in map.keys() {
        if !allowed.contains(key.as_str()) {
            return Err(format!("{label}: unexpected key {key}"));
        }
    }
    Ok(())
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use godworks_core::{
        MODEL_DATASET_MANIFEST_SCHEMA_VERSION, MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
    };
    use std::path::PathBuf;

    fn unique_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!("godworks_model_dataset_store_{name}_{nanos}"))
    }

    fn write_text(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    fn feature_block(trace_id: &str, source_artifact: &str) -> Value {
        json!({
            "kind": "WorkerLoad",
            "schema_version": MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
            "project_id": "arena",
            "dataset_id": "dataset-v1",
            "trace_id": trace_id,
            "source_artifact": source_artifact,
            "redacted": true,
            "metrics": {
                "worker_load_mean": 0.42,
                "worker_count": 4.0
            },
            "dimensions": {
                "source": "agar_live_gate",
                "profile": "bots_64"
            }
        })
    }

    fn proposal_json(dataset_id: &str) -> Value {
        json!({
            "kind": "RecommendPartitionMap",
            "mode": "guarded",
            "project_id": "arena",
            "model_id": "micro-balancer-v1",
            "dataset_id": dataset_id,
            "source_trace_id": "trace-a",
            "validator_id": "partition-map-validator"
        })
    }

    fn clean_replay_eval() -> Value {
        json!({
            "events": 3,
            "ok": true,
            "error_count": 0,
            "errors": []
        })
    }

    #[test]
    fn dataset_store_ingests_valid_feature_blocks_without_overwrite() {
        let dir = unique_dir("ingest_valid");
        let input = dir.join("blocks.jsonl");
        let store = dir.join("dataset");
        let blocks = [
            feature_block("trace-a", "agar-a"),
            feature_block("trace-b", "agar-b"),
        ]
        .into_iter()
        .map(|block| block.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        write_text(&input, &blocks);

        let manifest = ingest_dataset(&input, &store).unwrap();

        assert_eq!(manifest.project_id, "arena");
        assert_eq!(manifest.dataset_id, "dataset-v1");
        assert_eq!(
            manifest.schema_version,
            MODEL_DATASET_MANIFEST_SCHEMA_VERSION
        );
        assert_eq!(
            manifest.feature_schema_version,
            MODEL_FEATURE_BLOCK_SCHEMA_VERSION
        );
        assert_eq!(manifest.feature_block_count, 2);
        assert_eq!(manifest.trace_ids, vec!["trace-a", "trace-b"]);
        assert!(store.join(FEATURES_FILE).exists());
        assert!(store.join(MANIFEST_FILE).exists());
        assert!(ingest_dataset(&input, &store).is_err());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn dataset_store_rejects_mixed_dataset_before_write() {
        let dir = unique_dir("mixed_dataset");
        let input = dir.join("blocks.jsonl");
        let store = dir.join("dataset");
        let mut mixed = feature_block("trace-b", "agar-b");
        mixed["dataset_id"] = json!("other-dataset");
        let blocks = [feature_block("trace-a", "agar-a"), mixed]
            .into_iter()
            .map(|block| block.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        write_text(&input, &blocks);

        assert!(ingest_dataset(&input, &store).is_err());
        assert!(!store.join(FEATURES_FILE).exists());
        assert!(!store.join(MANIFEST_FILE).exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn dataset_store_rejects_raw_or_unredacted_feature_blocks() {
        let dir = unique_dir("raw_unredacted");
        let input = dir.join("blocks.jsonl");
        let store = dir.join("dataset");
        let mut raw = feature_block("trace-a", "agar-a");
        raw["redacted"] = json!(false);
        write_text(&input, &raw.to_string());

        assert!(ingest_dataset(&input, &store).is_err());
        assert!(!store.join(FEATURES_FILE).exists());

        let mut extra = feature_block("trace-a", "agar-a");
        extra["payload"] = json!({"raw": true});
        write_text(&input, &extra.to_string());

        assert!(ingest_dataset(&input, &store).is_err());
        assert!(!store.join(FEATURES_FILE).exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn promotion_requires_replay_eval_artifact_and_matching_provenance() {
        let dir = unique_dir("promotion_valid");
        let input = dir.join("blocks.jsonl");
        let store = dir.join("dataset");
        let proposal = dir.join("proposal.json");
        let replay = dir.join("replay_eval.json");
        let out = dir.join("promotion.json");
        write_text(&input, &feature_block("trace-a", "agar-a").to_string());
        ingest_dataset(&input, &store).unwrap();
        write_text(&proposal, &proposal_json("dataset-v1").to_string());
        write_text(&replay, &clean_replay_eval().to_string());

        let record =
            persist_promotion_record(&store, &proposal, &replay, "promotion-1", "accept", &out)
                .unwrap();

        assert_eq!(record.promotion_id, "promotion-1");
        assert_eq!(record.decision, ModelPromotionDecision::Accept);
        assert!(record.replay_eval_artifact.starts_with("replay_eval:"));
        assert!(out.exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn promotion_rejects_failed_replay_eval_or_dataset_mismatch_before_write() {
        let dir = unique_dir("promotion_rejects");
        let input = dir.join("blocks.jsonl");
        let store = dir.join("dataset");
        let proposal = dir.join("proposal.json");
        let replay = dir.join("replay_eval.json");
        let out = dir.join("promotion.json");
        write_text(&input, &feature_block("trace-a", "agar-a").to_string());
        ingest_dataset(&input, &store).unwrap();

        let failed_replay = json!({
            "events": 3,
            "ok": false,
            "error_count": 1,
            "errors": ["bad"]
        });
        write_text(&proposal, &proposal_json("dataset-v1").to_string());
        write_text(&replay, &failed_replay.to_string());
        assert!(persist_promotion_record(
            &store,
            &proposal,
            &replay,
            "promotion-1",
            "accept",
            &out
        )
        .is_err());
        assert!(!out.exists());

        write_text(&proposal, &proposal_json("other-dataset").to_string());
        write_text(&replay, &clean_replay_eval().to_string());
        assert!(persist_promotion_record(
            &store,
            &proposal,
            &replay,
            "promotion-1",
            "accept",
            &out
        )
        .is_err());
        assert!(!out.exists());

        let _ = fs::remove_dir_all(dir);
    }
}
