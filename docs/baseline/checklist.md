# Baseline Validation Checklist

Run this checklist before large refactors, protocol changes, SDK extraction, or release candidates.

## Local build gate

```bash
just gate
```

Expected commands:

- `cargo fmt --all -- --check`
- `cargo check --all-targets`
- `cargo clippy --all-targets`
- `cargo test --all-targets`
- `cargo build --release`

## Runtime smoke test

Terminal 1:

```bash
GW_WAL=.local/smoke.wal cargo run --bin godworks_broker
```

Terminal 2:

```bash
GW_ZW_REGION=W GW_ZW_ID=zw-W cargo run --bin zone_worker
```

Terminal 3:

```bash
GW_ZW_REGION=E GW_ZW_ID=zw-E cargo run --bin zone_worker
```

Terminal 4:

```bash
cargo run --bin reality_loadgen
```

## Recovery smoke test

1. Start broker with `GW_WAL=.local/recovery.wal`.
2. Create/update entities through `reality_loadgen` or demo workers.
3. Stop broker.
4. Restart broker with the same WAL.
5. Verify entities recover and no tombstoned/departed entity resurrects.

## Mesh smoke test

1. Run two brokers with distinct WAL paths and ports.
2. Configure mesh/registry or static mesh according to the current branch docs.
3. Run W/E owners and observers.
4. Move entities across the seam.
5. Verify:
   - source removes local entity only after durable departure;
   - target adopts exactly once;
   - source receives `MeshAck`;
   - no entity is double-owned;
   - pending mesh entries drain.

## Metrics to record

- entities;
- workers;
- handoffs;
- mesh handoffs;
- update rejections;
- WAL bytes;
- tick lag;
- lock max hold;
- per-worker egress queue;
- RSS memory;
- p95/p99 command/update latency once load tooling supports it.

## Regression rule

If a change breaks this checklist, either fix the regression or update the checklist with a clear migration note and a dedicated issue.
