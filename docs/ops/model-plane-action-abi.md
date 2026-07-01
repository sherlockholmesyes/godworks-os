# Model Plane Action ABI

The model plane is an advisor over the deterministic broker kernel. It may
observe traces, train project-local models, and emit typed proposals. It must
not become a hidden control plane.

## Contract

Model input is represented as `ModelFeatureBlock` in `godworks-core`.
Model output is represented as `ModelActionProposal` in `godworks-core`.

Feature block kinds are typed observations:

- `WorkerLoad`
- `AoiFidelityPressure`
- `EntityDensity`
- `HandoffPressure`
- `IngressRejectCost`
- `WalSync`
- `Outcome`

Every feature block must carry project-local provenance:

- `project_id`
- `dataset_id`
- `trace_id`
- `source_artifact`
- `schema_version`

Feature blocks must be redacted, replayable summaries. Metrics must be finite
numbers. Dimension/metric names must not encode raw auth tokens, secrets,
component bodies, payloads, or update bodies.

## Feature Block Builder

`model_feature_block` is the first executable bridge from current runtime
artifacts into model-plane inputs. It does not train a model and does not mutate
runtime state; it only emits validated JSONL summaries shaped by
`ModelFeatureBlock`.

```bash
cargo run --bin model_feature_block -- replay \
  tests/fixtures/replay_eval/valid-minimal.jsonl \
  arena replay-dataset-v1 trace-fixture

cargo run --bin model_feature_block -- reality-loadgen \
  .local/reality_loadgen.out \
  arena rlg-dataset-v1 trace-rlg

cargo run --bin model_feature_block -- agar-live-gate \
  .local/agar_mit_clone_logs/agar-live-gate.json \
  arena agar-live-dataset-v1 trace-agar-live
```

Replay input produces `IngressRejectCost` and `HandoffPressure` blocks.
`reality_loadgen` output produces `Outcome` and `HandoffPressure` blocks.
`agar-live-gate` output produces validated `Outcome`, `EntityDensity`, optional
`WorkerLoad`, and optional `HandoffPressure` blocks from live Agar gate
summaries. This includes the MIT-clone playable seam gate and broker-command
gate: a real player joins the stock clone, crosses dynamic shard blocks in the
`:8091` monitor, and the stronger gate drives one controlled player through
broker `CommandRequest` frames with accepted pre/post-seam `CommandResponse`
frames from the current `:8092` Godworks owner. Those summaries can feed future
per-project load/micro-balancer datasets as redacted facts such as entity
density, worker load, shard-block changes, owner changes, post-seam movement,
command-response count, and owner-match count.

The builder does not copy raw WAL paths, component bodies, payloads, or tokens
into model-plane data. It rejects source replay artifacts that still contain raw
redacted keys such as `auth_token`, `payload`, `components`, or `updates`,
failed Agar gates (`ok:false`), and non-finite numeric metrics before they can
enter a dataset.

Allowed action kinds are proposal-only:

- `RecommendPartitionMap`
- `AdjustInterestFidelity`
- `RecommendWorkerScale`
- `MarkHandoffRisk`
- `AntiCheatFlag`
- `NpcIntent`
- `Noop`

Promotion modes are:

- `observe`
- `shadow`
- `advisor`
- `guarded`

`guarded` is still not a runtime mutation. It only means a deterministic
validator may consider the proposal for a later WAL-backed runtime action.
Guarded proposals require a `validator_id`.

Every proposal must carry provenance:

- `project_id`
- `model_id`
- `dataset_id`
- `source_trace_id`

## Forbidden Shape

The model action vocabulary intentionally does not include direct runtime
mutations such as:

- authority grant or revoke;
- component update or batch update;
- mesh handoff emission;
- partition-map activation;
- WAL bypass.

Those actions remain broker/runtime responsibilities and must pass role policy,
validators, epochs, WAL, and versioned activation contracts.

## Current Gate

```bash
cargo test -p godworks-core model_feature_block_requires_redacted_finite_replayable_features
cargo test -p godworks-core model_feature_block_rejects_raw_secret_or_payload_shapes
cargo test -p godworks-core model_feature_block_contract_pins_project_local_provenance
cargo test --bin model_feature_block replay_builder -- --test-threads=1
cargo test --bin model_feature_block reality_loadgen_builder -- --test-threads=1
cargo test --bin model_feature_block agar_live_gate_builder -- --test-threads=1
cargo test -p godworks-core model_action_contract_rejects_direct_runtime_mutation
cargo test -p godworks-core model_action_proposal_requires_provenance_and_guarded_validator
```

These tests should fail if feature blocks accept unredacted/raw runtime bodies,
non-finite metrics, missing project-local provenance, raw replay source keys, or
unvalidated `reality_loadgen`/Agar live-gate metrics; if a failed live game gate
can enter the dataset as a success; if the public model action vocabulary starts
accepting direct runtime mutations; or if guarded proposals can be emitted
without validator provenance.
