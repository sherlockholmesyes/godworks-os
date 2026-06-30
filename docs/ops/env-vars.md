# Runtime Environment Inventory

This document records the current environment-variable surface. It is intentionally conservative: future work should move stable configuration into explicit config files while keeping env vars for overrides and test hooks.

## Broker/runtime variables

| Variable | Purpose |
|---|---|
| `GW_WAL` | Enable WAL at the given path. |
| `GW_WAL_COMPACT_BYTES` | Compact WAL after the configured byte threshold; `0` disables compaction. |
| `GW_WAL_FAIL` | Test hook: inject WAL failure. Must not be enabled in production. |
| `GW_RESTORE_OFFSET` | Restore only up to a consistent-cut WAL byte offset. |
| `GW_REPLAY_TAPE` | Optional redacted JSONL broker decision tape for offline replay/eval. Disabled when unset. |
| `GW_REPLAY_TAPE_CAPACITY` | Optional bounded in-memory replay-tape line buffer. Default `8192`; overflow drops tape lines, not runtime traffic. |
| `GW_BOUNDARY` | Single W/E strip boundary. |
| `GW_BOUNDARIES` | Comma-separated 1D strip boundaries for N-zone partitioning. |
| `GW_GRID2D` | Enable 2D grid partitioning, for example `4x4`. |
| `GW_ARENA` | Arena dimensions for 2D grid mode, for example `5000,5000`. |
| `GW_INTEREST_BAND` | Cross-broker seam ghost projection band. Default `0` disables it. |
| `GW_DRAIN_ON_START` | Start broker already draining. Test/ops hook. |
| `GW_DRAIN_NO_EXIT` | Keep broker alive after draining. Test hook. |
| `GW_MESH_ACK_DROP` | Test hook: drop mesh ACKs. |
| `GW_MESH_ADOPT_DROP` | Test hook: drop inbound mesh handoff adoption. |
| `GW_G2D_OFF` | Test hook: disable resolver behavior for a recovery proof. |
| `GW_REGISTRY` | Directory-based broker registry/service discovery. |
| `GW_AUTH_TOKEN` | Legacy/dev shared token accepted by any `WorkerConnect` region claim. Prefer `GW_AUTH_CLAIMS` for private alpha. |
| `GW_AUTH_CLAIMS` | Strict token-bound connection claims as `token:region:attr1\|attr2,token2:MESH:role.mesh`. The broker registers region/attributes from the token, rejects mismatched peer claims, and derives worker/client/observer/mesh role policy from the claim. |
| `GW_INGRESS_RATE_PER_SEC` | Per-peer ingress cost-unit refill rate. The broker charges by op class and large valid JSON payload size before dispatch. |
| `GW_INGRESS_BURST_FRAMES` | Legacy name for the per-peer ingress burst capacity, now interpreted as cost units rather than raw frame count. |

## Zone worker variables

| Variable | Purpose |
|---|---|
| `GW_ZW_HOST` | Broker host, default `127.0.0.1`. |
| `GW_ZW_PORT` | Broker port, default `7777`. |
| `GW_ZW_REGION` | Worker region, default `W`. |
| `GW_ZW_ID` | Worker id, default `zw-<region>`. |
| `GW_ZW_HZ` | Physics tick rate. |
| `GW_ZW_SPAWN` | Number of bots to spawn. |
| `GW_ZW_SPAWN_BOX` | Spawn area as `x0,x1,y0,y1`. |
| `GW_ZW_SPAWN_SPEED` | Random spawn velocity magnitude. |
| `GW_ZW_SPAWN_VEL` | Fixed initial velocity as `vx,vy`. |
| `GW_ZW_RADIUS` | Default collider radius. |
| `GW_ZW_REST` | Restitution. |
| `GW_ZW_INTEREST` | Worker AOI radius. |
| `GW_ZW_DURATION` | Optional runtime duration. |
| `GW_ZW_SEED` | Random seed. |
| `GW_ZW_WORLD` | Outer bounce walls as `x0,x1,y0,y1`. |
| `GW_ZW_CELL` | Authoritative cell as `x0,x1,y0,y1`; enables fold mode when neighbors exist. |
| `GW_ZW_NEIGHBORS` | Fold-mode neighbor map, for example `xlo:Z0,xhi:Z1,ylo:Z2,yhi:Z3`. |

## Reality loadgen variables

| Variable | Purpose |
|---|---|
| `GW_HOST` | Broker host for loadgen. |
| `GW_TARGET` | Primary broker port. |
| `GW_TARGET_E` | Secondary/east broker port; if different from `GW_TARGET`, loadgen treats the run as cross-broker. |
| `GW_ENTITIES` | Number of entities. |
| `GW_TICKS` | Number of movement ticks. |
| `GW_HZ` | Update frequency. |
| `GW_EVENT_BURST` | Number of critical/visual events to burst. |
| `GW_SLOW_VIEWER` | Enable slow viewer socket scenario. |
| `GW_REQUIRE_MESH` | Require mesh-specific success conditions. |

## Hardening backlog

- [ ] Replace production env-var sprawl with `godworks.toml` config files.
- [ ] Separate test hooks from production configuration.
- [ ] Add config validation with clear startup errors.
- [ ] Add docs/reference/config.md once the config schema exists.
- [ ] Add `godworks config check <path>` CLI command.
