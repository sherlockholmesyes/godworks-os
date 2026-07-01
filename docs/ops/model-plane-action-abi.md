# Model Plane Action ABI

The model plane is an advisor over the deterministic broker kernel. It may
observe traces, train project-local models, and emit typed proposals. It must
not become a hidden control plane.

## Contract

Model output is represented as `ModelActionProposal` in `godworks-core`.

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
cargo test -p godworks-core model_action_contract_rejects_direct_runtime_mutation
cargo test -p godworks-core model_action_proposal_requires_provenance_and_guarded_validator
```

These tests should fail if the public model action vocabulary starts accepting
direct runtime mutations or if guarded proposals can be emitted without
validator provenance.
