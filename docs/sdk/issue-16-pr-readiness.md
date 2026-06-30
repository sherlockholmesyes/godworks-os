# Issue 16 PR Readiness: Zone Worker SDK Frame Boundary

This packet summarizes the issue #16 slice as it exists on `main`:
`src/bin/zone_worker.rs` routes its TCP frame read/write and stable worker
operations through `godworks-worker-sdk` while preserving broker runtime
behavior and JSON wire shape.

Merged history:

```text
PR #17: SDK frame boundary and outbound helpers
PR #18: typed inbound handling and JSON bridge removal
```

## Scope

Changed in this branch:

- adds `godworks-worker-sdk` as the Rust worker-side frame/op helper crate;
- replaces the local `zone_worker` frame reader/writer with SDK `read_op` /
  `write_op`;
- emits stable outbound worker operations through SDK helpers:
  `WorkerConnect`, `Interest`, `CreateEntity`, `Fold`, `BatchUpdate(pos)`,
  `BatchUpdate(vel)`, `Heartbeat`, and `Disconnect`;
- handles inbound broker frames as typed `Op` variants instead of routing
  through a local JSON dispatcher bridge;
- keeps component payloads as explicit `serde_json::Value` data bags until a
  separate schema/codegen slice exists.

Not changed in this branch:

- broker authority, WAL, mesh, handoff, or recovery behavior;
- protocol JSON wire shape;
- physics/gameplay behavior in `zone_worker`;
- Godot/client, 3D, cloud/control-plane, or broad game-framework work.

## Migrated Sites

`src/bin/zone_worker.rs` now imports from `godworks-worker-sdk`:

```rust
batch_entry, circle_interest, create_entity_op, disconnect_op, fold_op,
heartbeat_op, read_op, worker_connect_op, write_op
```

The old local frame helpers were removed. `zone_worker` reads with `read_op`
and writes with `write_op`.

Outbound frames migrated to SDK helpers:

- worker connection: `worker_connect_op` with optional `auth_token`;
- AOI interest: `circle_interest`;
- entity creation: `create_entity_op`;
- fold/handoff request: `fold_op`;
- position and velocity batches: `BatchUpdate` plus `batch_entry`;
- heartbeat: `heartbeat_op`;
- disconnect: `disconnect_op`.

## Intentionally Raw

These remain intentionally raw in this PR:

- component payload values such as `pos`, `vel`, `mass`;
- `json!` values used as entity component bags;
- runtime summary and test fixture JSON.

Those are data payloads, not frame-boundary code. They should move only in a
later typed component/schema slice.

## Reviewer Gate

Run the issue-specific gate:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/issue16_gate.ps1
```

Add `-Full` to run the standard workspace gate after the issue-specific checks.

The issue-specific gate verifies:

- positive SDK boundary usage in `zone_worker`;
- no local `zone_worker` frame codec or direct serde frame I/O;
- SDK outbound helper wire shape;
- authority loss metadata;
- update rejection metadata;
- mesh handoff metadata, including current broker `src_region` spelling;
- existing zone-worker runtime behavior;
- existing cross-broker reality loadgen mesh adoption behavior.

## Standard Gate

Before pushing or opening a PR, run:

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets -- --test-threads=1
cargo build --workspace --release
```

The clippy gate denies warnings; do not add local `allow` / `expect`
suppression just to pass this slice.

## Guarded Failure Modes

This PR should fail review or tests if any of these regressions appear:

- `zone_worker` stops using SDK `read_op` / `write_op`;
- a local `zone_worker` frame codec returns;
- helper wire shape changes;
- authority/rejection/mesh metadata is narrowed or dropped;
- `MeshHandoff` only preserves an alias and not the current broker
  `src_region` wire key;
- zone-worker runtime handoff behavior regresses;
- cross-broker mesh adoption regresses.
