# zone_worker → Worker SDK migration plan

This is the next product-hardening slice after the Worker SDK MVP.

## Goal

Migrate `src/bin/zone_worker.rs` protocol I/O onto `godworks-worker-sdk` while preserving the existing broker wire and physics behavior.

This is not a gameplay rewrite. This is a proof that the SDK can drive the current real worker path.

## Current raw protocol sites

### Local framing helpers

`zone_worker` currently owns its own length-prefixed JSON helpers:

- `frame(v: &Value) -> Vec<u8>`
- `read_frame(...) -> Option<Value>`

These should be replaced by SDK-backed:

- `godworks_worker_sdk::write_op`
- `godworks_worker_sdk::read_op`
- `WorkerSession` or a narrow read/write adapter

### Outbound frames

Current outbound raw JSON sites to migrate or wrap losslessly:

- `WorkerConnect`
- `Interest`
- `CreateEntity`
- `Fold`
- `BatchUpdate` for `pos`
- `BatchUpdate` for `vel`
- `Heartbeat`
- `Disconnect`

### Inbound frames

Current inbound raw `Value` frames are consumed by `apply_op`:

- `AddEntity`
- `ComponentUpdate`
- `RemoveEntity`
- `AuthorityChange`
- `UpdateRejected`

The migration should initially keep `apply_op` behavior equivalent. It can either:

1. accept `Op` and use typed accessors directly; or
2. accept `WorkerFrame` and only convert to JSON for a staged transition.

Option 1 is preferred, but option 2 is acceptable only if it removes custom framing and does not lose fields.

## Strict invariants

- No broker runtime behavior changes.
- No wire shape changes.
- No Godot/client/3D/cloud/editor work.
- No new gameplay behavior.
- No SDK helper may discard metadata.
- `AuthorityChange` loss-imminent metadata must survive.
- `UpdateRejected` stale/ghost/owner metadata must survive.
- `MeshHandoff` authority/components must survive even if `zone_worker` does not act on all fields yet.

## Acceptance gates

Local/code gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets
cargo test --workspace --all-targets -- --test-threads=1
cargo build --workspace --release
```

Runtime smoke:

```bash
GW_WAL=.local/smoke.wal cargo run --bin godworks_broker
GW_ZW_REGION=W GW_ZW_ID=zw-W cargo run --bin zone_worker
GW_ZW_REGION=E GW_ZW_ID=zw-E cargo run --bin zone_worker
cargo run --bin reality_loadgen
```

## Recommended PR shape

One PR only:

```text
feat(zone-worker): use Worker SDK framing and typed ops
```

Suggested implementation order:

1. Add root crate dependencies needed by `zone_worker`:
   - `godworks-worker-sdk`
   - `godworks-protocol` if direct typed ops are needed
   - `godworks-core` if direct ids/spatial types are needed
2. Replace local `frame/read_frame` with SDK frame I/O.
3. Replace handshake with `WorkerConfig` / connect helper.
4. Replace `Interest` with SDK interest helper.
5. Replace `BatchUpdate` writes with typed `BatchUpdate` / `BatchUpdateEntry` helpers.
6. Replace `Heartbeat`, `Disconnect`, `Fold`, and `CreateEntity` with typed ops.
7. Convert `apply_op` from `Value` to `Op` / `WorkerFrame`, or stage through `encode_json_value` only if behavior is proven unchanged.
8. Add tests or fixtures that compare old JSON shapes to new encoded frames.

## Wrong × wrong guard

```text
wrong A: migrate zone_worker quickly by preserving hidden raw JSON construction everywhere
wrong B: rewrite apply_op behavior while changing transport at the same time
third wrong: a worker that compiles but no longer proves the real authority/handoff path
```

Keep this PR narrow: transport/SDK boundary first, behavior unchanged.
