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

`ModelDatasetManifest` is the typed cut over validated feature blocks. It is
constructed only from a non-empty set of redacted `ModelFeatureBlock`s that
share one `project_id`, one `dataset_id`, and one feature schema version. The
manifest records the feature block count, trace ids, and source artifacts. It
does not copy raw source artifacts into the dataset.

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
`:8091` monitor, the stronger gate drives one controlled player through broker
`CommandRequest` frames with accepted pre/post-seam `CommandResponse` frames
from the current `:8092` Godworks owner, the broker-command capacity gate
requires several controlled players to satisfy the same broker-routed seam proof
under live bot load, and the capacity gate records a sustained local floor from
the same live monitor. The MIT stress ladder summary is also accepted as a
multi-profile live artifact: it records the observed local floor across bot
profiles while preserving only aggregate profile metrics, not raw command
targets or player identity traces. Those summaries can feed future per-project
load/micro-balancer datasets as redacted facts such as entity density, worker
load, shard-block changes, owner changes, post-seam movement, controlled-player
completion count, command-response count, owner-match count, sustained
player/entity floors, profile pass counts, load peak/mean pressure, command ACK
latency, and local process CPU/RSS pressure.

The builder does not copy raw WAL paths, component bodies, payloads, command
targets, or tokens into model-plane data. It rejects source replay artifacts that
still contain raw redacted keys such as `auth_token`, `payload`, `components`,
`updates`, `lastCommand`, `lastTarget`, or `target`, failed Agar gates
(`ok:false`), and non-finite numeric metrics before they can enter a dataset.

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

`ModelPromotionRecord` binds a proposal to a validated dataset manifest and a
replay/eval artifact. It can record an accept/reject decision for later
validator consideration, but it is still not a runtime mutation or a training
claim. Promotion validation rejects missing replay/eval evidence, invalid
dataset manifests, dataset/proposal provenance mismatches, and guarded
proposals without validator provenance.

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
cargo test -p godworks-core model_dataset_manifest_accepts_only_one_valid_project_dataset_cut
cargo test -p godworks-core model_promotion_record_requires_dataset_proposal_and_replay_eval_binding
```

These tests should fail if feature blocks accept unredacted/raw runtime bodies,
non-finite metrics, missing project-local provenance, raw replay source keys, or
unvalidated `reality_loadgen`/Agar live-gate metrics; if a failed live game gate
or failed MIT ladder profile can enter the dataset as a success; if the public
model action vocabulary starts accepting direct runtime mutations; if guarded
proposals can be emitted without validator provenance; if mixed project/dataset
feature blocks can form one dataset manifest; or if promotion records can omit
replay/eval evidence or mismatch dataset/proposal provenance.

## Non-Scope

This ABI does not yet provide a physical dataset store, model training loop,
model registry, or runtime activation path. The current layer is only the typed
contract that a future `model_dataset_store` or promotion runner must obey.
