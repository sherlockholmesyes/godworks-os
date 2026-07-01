# Live Game Reality Gates

Live game gates are part of the Godworks OS foundation, not release polish.
`cargo test` proves code paths; a live game gate proves that the broker, workers,
client-facing stream, game commands, and observable game state still compose into
a playable slice.

## Agar Reality Gate v1

Current command:

```powershell
.\examples\agar\run_agar_demo.ps1 -GateOnly
```

The gate starts one broker, a 4x4 worker pool, the browser gateway, and an
automated driver. It proves:

- a player can join through the product path;
- `CommandRequest` input reaches the current owner;
- a command sent after a handoff is acknowledged by the new owner;
- the player crosses partition seams;
- every observed entity has a real worker owner;
- observed frames do not duplicate entity ids;
- the non-privileged `CLIENT` stream matches the token-bound inspector truth;
- client-role peers cannot create entities, query inspector state, claim mesh
  privilege, or create platform authority components.

The gate is intentionally stronger than a synthetic protocol test because it
uses the actual demo cluster and live state. It is still not the full release
gate.

The visible browser canvas must consume `/client-state`, the same
non-privileged stream checked by the automated gate. `/state` and
`/broker-state` are broker-truth oracle endpoints for the gate and debugging;
they must not be the public render source.

## Agar Reality Gate v2

Before treating the demo as release-quality, promote v1 with these additional
checks:

- pixel proof: open `http://localhost:8091`, verify the canvas is non-blank,
  moving, sourced from `/client-state`, and aligned with the gate summary;
- SDK cache proof: route one client-facing check through
  `godworks-client-sdk::ClientBridge` instead of only the JavaScript gateway
  cache;
- replay proof: run with `GW_REPLAY_TAPE`, validate it with `replay_eval`, and
  confirm no secret or component-body leak;
- WAL restore proof: restart from `.local/agar/agar.wal` and verify the restored
  state has no duplicate owner, missing player, or resurrected deleted entity;
- duration proof: run a longer multi-client soak and keep owner count,
  duplicate frames, rejects, and command acknowledgements inside expected
  bounds.

## Godot Gates

Godot is the engine-facing proof path:

- Godot 2D physics gate: a Godot scene consumes the SDK bridge, sends movement or
  physics commands, crosses a seam, and agrees with broker state.
- Godot 3D contract gate: a Godot scene or fixture exercises 3D-ready component
  names and spatial metadata without requiring a full 3D runtime rewrite.
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
