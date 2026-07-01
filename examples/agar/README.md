# Godworks agar.io Reality Demo

This is a small live-game ruler for Godworks OS. It is not a fork of the MIT
agar.io clone; the clone is only a local reference for game feel. The demo uses
the current Godworks broker wire directly:

- one broker;
- a pool of one-region zone workers;
- a browser gateway;
- live player input through `CommandRequest`;
- live state read through both a non-privileged `CLIENT` stream and a
  token-bound `OBS` inspector connection;
- visible browser rendering from the non-privileged `CLIENT` stream;
- an adversarial reality gate that drives a player across seams and verifies
  ownership/conservation from live state.

## Run

```powershell
.\examples\agar\run_agar_demo.ps1
```

Open:

```text
http://localhost:8091
```

Run only the automated gate:

```powershell
.\examples\agar\run_agar_demo.ps1 -GateOnly
```

The runner sets `GW_AUTH_CLAIMS` deliberately. `OBS`, `CLIENT`, `MESH`, and
broker-owned attributes such as `inspector` cannot be self-declared by a peer in
the current security model.

Security shape:

- the browser never receives inspector, mesh, kernel-admin, or worker-region
  credentials;
- the HTTP gateway owns a trusted server-adapter token for spawn and command
  routing;
- the `OBS` + `inspector` connection is only a broker-truth oracle for the demo
  and gate;
- the non-privileged `CLIENT` connection is used as a product-path stream and is
  checked against inspector truth;
- the browser canvas renders `/client-state`, not the inspector snapshot;
- the gate intentionally checks that client-role peers cannot create entities,
  query inspector state, claim mesh privilege, or create platform authority
  components.

The demo does not self-declare `authority.mode` from workers. That field is a
platform-reserved component in Godworks; the example keeps the gameplay layer on
ordinary components and lets the broker own authority policy.

## Why This Shape

Old prototype residue: the 2D/D3 harness tried to claim multiple regions by
sending multiple `WorkerConnect` frames over one TCP connection. Current
Godworks has one `WorkerConnect` per connection, so this demo starts a worker
pool: each region/cell is one process and one connection.

The live gate is intentionally stronger than `cargo test`: it proves the browser
slice can join, send commands, receive command acknowledgements after handoff,
move, cross partition seams, keep every entity owned by a real worker, reject
privilege self-assignment, compare client-stream truth against inspector truth,
avoid duplicate entity ids in observed frames, and render a non-blank moving
canvas sourced from `/client-state`. `-GateOnly` also runs the broker with
`GW_REPLAY_TAPE` and validates the resulting live tape with `replay_eval`, so
the gate fails if runtime breadcrumbs leak secrets, lose spatial contract
metadata, or contradict protocol operation semantics. After the live cluster is
stopped, the same gate starts the broker in `GW_RESTORE_DRYRUN=1` against the
demo WAL and fails if recovery reports an error, an empty store, no selected WAL
events, or unknown WAL event kinds.

This is the current Agar Reality Gate v1. It is now bound to the System Laws
index as a runtime ruler, but it is not yet the full release gate. The remaining
promotion work is tracked in `docs/ops/live-game-reality-gates.md`:
`godworks-client-sdk` cache proof, restored-broker/client agreement, and longer
soak coverage.

The pixel gate launches Chrome/Edge through the Chrome DevTools Protocol without
Playwright/Puppeteer. If the browser is not in a standard location, set:

```powershell
$env:GW_PIXEL_BROWSER = "C:\Path\To\chrome.exe"
```

The demo protects `P-*` player probes from autonomous NPC eating by default so
the release gate measures seam handoff instead of random early deletion. Set
`GW_AGAR_PROTECT_PLAYERS=0` for chaos/gameplay runs that should allow NPCs to
eat players.

## Local Reference Clone

The full MIT reference clone, when restored by the development workflow, should
live outside this repository in a local scratchpad.

Do not vendor that full source tree into this repository unless there is a
separate product reason.
