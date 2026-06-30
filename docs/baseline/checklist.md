# Baseline Validation Checklist

Run this checklist before large refactors, protocol changes, SDK extraction, or release candidates.

## Local build gate

```bash
just gate
```

Expected commands:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace --all-targets`
- `cargo build --workspace --release`

## Runtime smoke test

Use the checked-in smoke runner:

```powershell
.\scripts\runtime_smoke.ps1
```

The script runs two separate rulers:

- `broker + reality_loadgen`: the loadgen owns its own `rlg-owner-W` /
  `rlg-owner-E` clients and exercises create, AOI, updates, events, commands,
  queries, and slow-consumer handling.
- `broker + W/E zone_worker`: real `zone_worker` processes spawn W bodies,
  cross the W/E seam, hand authority to E, and emit structured
  `zone_worker_summary` lines.

Do not run `reality_loadgen` at the same time as long-lived `zone_worker`
processes claiming the same `W` / `E` regions. `reality_loadgen` registers its
own W/E owner clients; pre-claiming those regions makes the broker correctly
refuse the loadgen owners and turns the smoke test into an authority-lease
conflict instead of a product runtime ruler.

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
