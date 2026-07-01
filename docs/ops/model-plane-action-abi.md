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
cargo test -p godworks-core model_action_contract_rejects_direct_runtime_mutation
cargo test -p godworks-core model_action_proposal_requires_provenance_and_guarded_validator
```

These tests should fail if feature blocks accept unredacted/raw runtime bodies,
non-finite metrics, or missing project-local provenance; if the public model
action vocabulary starts accepting direct runtime mutations; or if guarded
proposals can be emitted without validator provenance.
