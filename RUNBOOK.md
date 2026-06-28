# Godworks OS local runbook

This document covers local build, test, and runtime checks for the Godworks OS broker and worker binaries.

## Components

| Binary | Source | Role |
|---|---|---|
| `godworks_broker` | `src/main.rs` | Authoritative entity store, operation broker, partitioning, handoff, WAL recovery, and mesh links. |
| `zone_worker` | `src/bin/zone_worker.rs` | Rapier2D physics worker that owns bodies, simulates them, and sends component updates. |
| `loadgen` | `src/bin/loadgen.rs` | Synthetic protocol throughput driver. |
| `reality_loadgen` | `src/bin/reality_loadgen.rs` | Runtime gate for real broker/worker mesh adoption behavior. |

## Build

```powershell
cargo build --bins
cargo build --release --bins
```

Run the full test suite:

```powershell
cargo test -- --test-threads=1
```

Expected current shape:

- 29 unit tests in `src/main.rs`
- 1 cross-broker runtime test in `tests/reality_loadgen_runtime.rs`
- 3 zone-worker runtime tests in `tests/zone_worker_runtime.rs`

The current known warning is an unused helper in `zone_worker.rs`; it does not affect the runtime gates.

## Single Broker

Start a broker:

```powershell
$env:GW_PORT="7799"
$env:GW_BIND="127.0.0.1"
cargo run --bin godworks_broker
```

Useful broker environment variables:

| Variable | Meaning |
|---|---|
| `GW_PORT` | TCP port. Defaults to `7777`. |
| `GW_BIND` | Bind address. Use `127.0.0.1` locally or `0.0.0.0` for LAN tests. |
| `GW_BOUNDARY` | Single W/E split boundary. |
| `GW_BOUNDARIES` | Comma-separated N-zone strip boundaries, for example `50,100,150`. |
| `GW_GRID2D` | 2D grid partition, for example `4x4`. |
| `GW_ARENA` | 2D grid arena size, for example `120,120`. |
| `GW_MESH` | Cross-broker mesh peers. |
| `GW_ADVERTISE` | Region advertisement for mesh routing. |
| `GW_WAL` | WAL path. If unset, the broker uses its default local path. |
| `GW_RESTORE_OFFSET` | Restore from a WAL cut. |

Use a clean working directory for short experiments because the broker writes WAL files.

## Zone Worker

Start a local physics worker:

```powershell
$env:GW_ZW_HOST="127.0.0.1"
$env:GW_ZW_PORT="7799"
$env:GW_ZW_REGION="W"
$env:GW_ZW_SPAWN="100"
$env:GW_ZW_SPAWN_BOX="-18,18,-18,18"
$env:GW_ZW_WORLD="-20,20,-20,20"
$env:GW_ZW_DURATION="10"
$env:GW_ZW_HZ="30"
cargo run --bin zone_worker
```

Useful worker variables:

| Variable | Meaning |
|---|---|
| `GW_ZW_HOST` / `GW_ZW_PORT` | Broker address. |
| `GW_ZW_REGION` | Worker-owned region name. |
| `GW_ZW_ID` | Optional stable worker id. |
| `GW_ZW_SPAWN` | Number of bodies to spawn. |
| `GW_ZW_SPAWN_BOX` | Spawn area: `min_x,max_x,min_y,max_y`. |
| `GW_ZW_SPAWN_SPEED` | Initial velocity scale. |
| `GW_ZW_WORLD` | Bounce bounds: `min_x,max_x,min_y,max_y`. |
| `GW_ZW_INTEREST` | Worker interest radius. Defaults wide. |
| `GW_ZW_HZ` | Simulation/update frequency. |
| `GW_ZW_DURATION` | Stop after N seconds. Unset means run until interrupted. |
| `GW_ZW_CELL` / `GW_ZW_NEIGHBORS` | Fold-mode cell id and neighbor list. Unset uses automatic broker handoff. |

The worker prints one status line per second:

```text
[zw W] tick=... owned=... view=... rejects=... hz=...
```

`owned` is the number of bodies this worker currently has authority to simulate. `view` is the number of visible entities.

## Batch Updates

`zone_worker` sends per-tick position updates as one `BatchUpdate` frame and velocity updates as one `BatchUpdate` frame. That keeps the broker on the same authority, epoch-fence, WAL, and interest path as single updates while reducing frame count from roughly `2N` to `2` per tick.

The position batch is sent before the velocity batch so boundary crossings are processed before later velocity writes. If authority moves during the position update, stale velocity entries are rejected per entity.

## Mesh / Multi-Broker Checks

The integration tests are the fastest way to verify mesh adoption and worker authority conservation:

```powershell
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test dense_seam_with_matching_e_worker_conserves_authority -- --nocapture
```

`reality_loadgen` is stricter than a visibility check. In cross-broker mode it now proves the writer swap after handoff:

- the E-side owner receives authority for every crossed entity;
- the E-side owner writes `handoff_probe`, and a public `EntityQuery` sees that value;
- the old W-side owner attempts the same write and receives `UpdateRejected`.
- the E-side owner writes a full `physics` payload (`pos`, `rot`, `lin`, `ang`, `at_rest`, `gen`, `t_server`, `sim_time`) and a public `EntityQuery` sees the post-handoff value with monotonic clocks.

The parseable result line exposes this as `handoff_probe_ok=<N>`, `handoff_probe_rejected=<N>`, `physics_payload_ok=<N>`, and `physics_clock_ok=<N>`.

For manual experiments, run one broker per region and connect them with `GW_MESH` / `GW_ADVERTISE`. Keep broker and worker processes alive in foreground terminals or under a process manager; shell-backgrounded children may exit when their launcher exits.

## Runtime Notes

- A mesh handoff is only acknowledged after the receiver has a matching durable entity state.
- Existing-entity mesh retries are acknowledged only if the already-present entity matches the inbound adopt region and authority epoch.
- In 2D grid mode, mesh adopt commits the target grid cell before falling back to geometric position.
- WAL failures are treated as fail-closed for persistent transitions: do not publish a state that recovery cannot reproduce.
- The broker is still a compact prototype data plane. For larger deployments, measure monitor tick latency, mesh resend behavior, egress queue pressure, and per-zone worker density under the actual topology.

## Cleanup

Generated files are ignored by `.gitignore`:

```text
target/
*.wal
*.wal.jsonl
*.log
*.err
live_logs/
```

Remove old local WAL/log files before comparing fresh runs.
