# Live Game Reality Gates

Live game gates are part of the Godworks OS foundation, not release polish.
`cargo test` proves code paths; a live game gate proves that the broker, workers,
client-facing stream, game commands, and observable game state still compose into
a playable slice.

## Agar Reality Gate v2

Current command:

```powershell
.\examples\agar\run_agar_demo.ps1 -GateOnly
```

The gate starts one broker, a 4x4 worker pool, the browser gateway, and an
automated driver. It also launches a headless Chromium browser through the
dependency-free CDP pixel gate. It proves:

- a player can join through the product path;
- `CommandRequest` input reaches the current owner;
- a command sent after a handoff is acknowledged by the new owner;
- the player crosses partition seams;
- every observed entity has a real worker owner;
- observed frames do not duplicate entity ids;
- the non-privileged `CLIENT` stream matches the token-bound inspector truth;
- client-role peers cannot create entities, query inspector state, claim mesh
  privilege, or create platform authority components;
- the visible canvas is non-blank, reports `source: CLIENT stream`, and moves a
  visible player through the product browser path.
- a live Rust `godworks-client-sdk::ClientBridge` consumes the same broker
  `CLIENT` stream and builds a non-empty positional cache with no rejected
  updates.
- a multi-player soak phase joins several player probes through the same product
  path, repeatedly commands them across the grid, observes ownership changes for
  each player, verifies post-handoff command acknowledgements, and keeps
  `/client-state` aligned with inspector truth.
- the broker emits a live `GW_REPLAY_TAPE` artifact and `replay_eval` accepts it
  without redaction leaks, malformed spatial metadata, or protocol semantic
  contradictions.
- the demo WAL is accepted by `GW_RESTORE_DRYRUN=1` after the live cluster stops,
  with a non-empty recovered store, selected WAL events, no recovery error, and
  no unknown WAL event kinds.
- a restored broker booted from the same demo WAL returns an `InspectorFrame`
  whose entity IDs, positions, and logical `pos` owners agree with the final
  live broker/client cut captured before shutdown. This restore-only check
  normalizes `agar-Zx_y` worker IDs to `Zx_y` regions because workers are not
  reconnected for this query.

The gate is intentionally stronger than a synthetic protocol test because it
uses the actual demo cluster and live state. It is now the default Agar release
ruler, though longer external soak/stress profiles can still be added above it.

The probe player is protected from autonomous NPC eating by default so gameplay
randomness cannot delete the invariant carrier before the seam proof. Set
`GW_AGAR_PROTECT_PLAYERS=0` for chaos/gameplay runs where that protection is not
desired.

The visible browser canvas must consume `/client-state`, the same
non-privileged stream checked by the automated gate. `/state` and
`/broker-state` are broker-truth oracle endpoints for the gate and debugging;
they must not be the public render source.

The default soak knobs can be overridden for heavier manual runs:

```powershell
$env:GW_SOAK_PLAYERS = "4"
$env:GW_SOAK_MS = "30000"
$env:GW_SOAK_COMMAND_MS = "1500"
```

## MIT Clone Adapter Gate

The public-clean MIT clone adapter lives under `examples/agar_mit_clone`. It
reuses `owenashurst/agar.io-clone` locally instead of vendoring or rewriting the
game.

Commands:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -RunGate
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunPlayableSeamGate
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunBrokerCommandGate
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunBrokerCommandCapacityGate -CommandPlayers 4
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunCapacityGate
.\examples\agar_mit_clone\run_mit_clone_stress_ladder.ps1 -BotCounts 40 -CommandPlayers 8 -CommandCapacityMinCompleted 4
```

`-RunGate` proves the stock game is playable on `:3000`, the dynamic `:8091`
monitor sees live entities/players, the 4x4 worker-zone geometry is non-uniform
under load, and the optional `:8092` broker mirror/WAL are alive.

`-RunPlayableSeamGate` adds a stricter playable-path proof: it joins a real
stock-clone player, sends normal MIT clone movement commands, requires that
player to cross at least one dynamic `:8091` worker-zone boundary, and requires
continued movement after crossing. With `-MirrorBroker`, it also checks the
probe against the `:8092` Godworks mirror state.

`-RunBrokerCommandGate` adds the stronger command-routing proof for one
controlled stock-clone player. The gate starts a local command bridge, connects
to the broker as a `CLIENT`, sends movement only as broker `CommandRequest`
frames, and requires accepted `CommandResponse` frames from the current
Godworks `pos` authority owner before and after a broker ownership seam. A direct
Socket.IO command path cannot satisfy this gate because the gate waits for the
broker-routed response correlation key.

`-RunBrokerCommandCapacityGate` combines the broker-command proof with the
capacity ruler. The command bridge owns several selected stock-clone players,
the gate polls those exact entities rather than the aggregate bridge state, and
each completed player must cross a dynamic `:8091` block, cross a Godworks owner
seam in `:8092`, keep moving after the seam, and receive an accepted
post-seam `CommandResponse` from the current owner. Background bots can satisfy
the population floor, but they cannot satisfy the controlled-player metrics.

`-RunCapacityGate` is the first reproducible MIT-clone capacity floor. It samples
the live `:8091` dynamic-zone monitor for a sustained window and optionally the
`:8092` Godworks broker mirror. The default floor is intentionally modest and
repeatable on a local machine: at least 30 players, 800 live entities, 16 worker
load slots, 8 good samples, and observed non-uniform dynamic zone geometry. This
is not a maximum CCU benchmark; it is the live-game ruler that prevents
"capacity" from being inferred from one screenshot or one transient sample.

`run_mit_clone_stress_ladder.ps1` is the executable ladder form of that ruler.
The System Laws gate binds the local path for the observed floor profile:
`-BotCounts 40 -CommandPlayers 8 -CommandCapacityMinCompleted 4`. This is still
an observed local floor, not a maximum-player claim. Each ladder row records
command ACK latency plus local process CPU/RSS snapshots so the model-plane and
release notes can distinguish "green but expensive" from "green and cheap."
Larger profiles must be rerun and recorded before they can be advertised.
The sanitized fixture
`tests/fixtures/agar_mit_clone/ladder_40_200_telemetry.json` records one local
2026-07-01 floor run through `40,80,120,200` bot profiles, with the highest
green row at 200 bots / 197-206 live players. It is evidence for that local
machine and gate shape, not a general maximum CCU benchmark.

Boundary: even the broker-command MIT clone gate is not yet a full
Godworks-authoritative replacement for the MIT clone server. The stock clone
still owns its gameplay ecology and physics; Godworks proves live projection,
ownership, and broker-routed command control for controlled players.

## Godot Gates

Godot is the engine-facing proof path:

Current command, when a Godot 4.x binary is available through `GODOT_BIN` or
`godot` on `PATH`:

```powershell
.\scripts\run_godot_probes.ps1
```

For a portable local Godot 4.3 toolchain without installing a system package:

```powershell
$godot = .\scripts\ensure_godot_4_3.ps1
.\scripts\run_godot_probes.ps1 -Godot $godot
```

This runner is narrower than a full game demo, but it is already an
engine-facing gate:

- `client_bridge_contract_probe.gd` replays the shared
  `tests/fixtures/client_bridge/godot-resync-contract.json` transcript through
  the Godot adapter and checks the same snapshot contract exported by the Rust
  `ClientBridge`.
- `client_bridge_tcp_resync_probe.gd` connects to a real broker socket,
  disconnects a viewer, deletes stale state while offline, reconnects, performs
  a full `EntityQuery` checkout, and verifies the Godot bridge rebuilt the
  cache from broker output.
- `cross_broker_handoff_probe.gd` is the first Godot-side cross-broker handoff
  ruler.

CI does not currently provision a Godot binary. The CI-safe inventory gate is:

```bash
python3 tools/godot_probe_inventory.py
```

That inventory gate is not a substitute for live Godot execution; it keeps the
runner, fixture, probe scripts, docs, and expected snapshot shape from drifting
while the live Godot gate remains environment-dependent.

Local evidence from 2026-07-01: the portable Godot 4.3 runner passed the
fixture contract probe, real broker reconnect/resync probe, and cross-broker
handoff probe. The cross-broker probe verified W->E handoff, public E-side
write, and stale W-owner fencing.

Remaining Godot work:

- Godot 2D physics gate: a scene consumes the SDK bridge, sends movement or
  physics commands, crosses a seam, and agrees with broker state.
- Godot 3D contract gate: a scene or fixture exercises 3D-ready component names
  and spatial metadata without requiring a full 3D runtime rewrite.
- Later Godot 3D physics gate: a real 3D physics worker/client path, after the
  typed spatial contracts are stable.

## Release Rule

A game-facing release claim needs all relevant layers to agree:

```text
pixels -> client cache -> broker state -> replay tape -> WAL restore
```

If one layer is missing, the claim must name that gap. A green cargo baseline
means the code passed tests; a green live reality gate means the game-facing
distributed system still obeys the laws it claims to expose.
