# Godworks MIT agar.io Clone Adapter

This example reuses the MIT `owenashurst/agar.io-clone` instead of rewriting
the game. The clone source and `node_modules` are not vendored here. The runner
clones the game into `.local/agar_mit_clone` and copies only the thin Godworks
adapter tools into that local clone.

Ports:

- `http://localhost:3000` - playable stock Open Agar clone.
- `http://localhost:8091` - dynamic 4x4 shard monitor over the live clone feed.
- `http://localhost:8092` - optional Godworks broker ownership view for mirrored
  player cells.
- `http://localhost:8093` - optional controlled-player command bridge used only
  by the broker-command gate.

Run the playable clone plus dynamic shard monitor:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1
```

Run with a public Godworks broker mirror:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunGate
```

Run the playable seam gate:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunPlayableSeamGate
```

Run the broker-command seam gate:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunBrokerCommandGate
```

Run the broker-command capacity gate:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunBrokerCommandCapacityGate -CommandPlayers 4 -BotCount 40 -StopExisting
```

Run the capacity floor gate:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunCapacityGate -BotCount 40 -StopExisting
```

Run a broker-command stress ladder:

```powershell
.\examples\agar_mit_clone\run_mit_clone_stress_ladder.ps1 -BotCounts 40,80,120,200 -CommandPlayers 8 -CommandCapacityMinCompleted 4
```

Run the first Godworks-authoritative server mode:

```powershell
.\examples\agar_mit_clone\run_godworks_authoritative.ps1 -BuildBroker -StopExisting -RunGate
```

Run the Godworks-authoritative stress ladder:

```powershell
.\examples\agar_mit_clone\run_godworks_authoritative_stress_ladder.ps1 -BotCounts 20,50,100
```

Use `-StopExisting` when an old local demo owns the same ports.

## Promoted Artifact Map

This directory is the public-clean promotion of the local MIT-clone prototype.
It carries the Godworks adapter, monitors, and gates; it does not vendor the
clone source or `node_modules`.

| Prototype role | Public artifact |
| --- | --- |
| Stock playable clone | Cloned on demand into `.local/agar_mit_clone` by `run_mit_clone_adapter.ps1` |
| Spectator feed tap | `_gw_spectator_tap.js` |
| Bot/player load | `_gw_bots.js` |
| Dynamic 4x4 / 100x100 live shard monitor on `:8091` | `gw_shard_monitor.js` |
| Old D2/D3 mirror worker | Superseded by `gw_agar_mirror_worker.js`, one TCP connection per worker region |
| Old broker-view prototype tool | Superseded by `gw_broker_view.js` on `:8092` |
| Playable seam proof | `gw_agar_playable_seam_gate.js` |
| Broker-routed command proof | `gw_agar_broker_command_gate.js` |
| Multi-player broker-command capacity proof | `gw_agar_broker_command_capacity_gate.js` |
| Capacity floor proof | `gw_agar_capacity_gate.js` |
| Old D3 runner | Superseded by `run_mit_clone_adapter.ps1 -MirrorBroker ...` |
| Stress evidence | `run_mit_clone_stress_ladder.ps1` plus `tests/fixtures/agar_mit_clone/ladder_40_200_telemetry.json` |
| Godworks-authoritative v0 | `run_godworks_authoritative.ps1`, `gw_authoritative_server.js`, `gw_authoritative_zone_worker.js`, `gw_authoritative_gate.js` |
| Godworks-authoritative capacity floor | `gw_authoritative_bots.js`, `gw_authoritative_capacity_gate.js`, `run_godworks_authoritative_stress_ladder.ps1` |

The older prototype shape tried to claim several regions through one
`WorkerConnect` connection. Current public Godworks uses one
`WorkerConnect.region` per TCP connection, so the supported mirror starts one
worker process per grid cell. That is noisier operationally but cleaner as a
public product claim: the broker sees ordinary workers, authority moves through
the normal handoff path, and the gates cannot pass by relying on a private
multi-region shortcut.

## What This Proves

- The real MIT game remains playable on `:3000`.
- The `:8091` monitor reads the live game through spectator mode, maps all
  entities to a 100x100 grid, and dynamically reshapes a 4x4 worker-zone view
  by load with hysteresis.
- With `-MirrorBroker`, Godworks receives live player-cell positions from the
  clone through one zone-worker process per grid cell and exposes ownership in
  the `:8092` broker view.
- `-RunGate` checks the playable page, live entity/player feed, non-uniform
  dynamic shard geometry, optional Godworks mirror ownership, and optional WAL
  activity. Set `GW_AGAR_REQUIRE_REBALANCE_EVENT=1` before running the gate when
  you want a stress check that requires an actual rebalance event during the gate
  window. The default gate accepts already-dynamic non-uniform geometry, because
  a balanced live snapshot should not rebalance just to satisfy a test.
- `-RunPlayableSeamGate` joins a real stock-clone player, sends normal movement
  commands over the MIT clone Socket.IO protocol, requires the player to move
  across at least one dynamic `:8091` worker-zone boundary, and requires
  continued movement after crossing. With `-MirrorBroker`, the same probe is
  matched against the optional `:8092` Godworks broker mirror view.
- `-RunBrokerCommandGate` starts a controlled stock-clone player socket behind
  a local command bridge, then drives that player only by sending broker
  `CommandRequest` frames as a `CLIENT` peer. The broker must route each command
  to the current `pos` authority owner, the mirror worker must forward the
  command to the bridge, and the gate requires accepted `CommandResponse` frames
  before and after a Godworks ownership seam.
- `-RunBrokerCommandCapacityGate` starts multiple controlled stock-clone player
  sockets behind the same local command bridge and requires each selected player
  to move only through broker `CommandRequest` frames while the normal bot load
  keeps the live game populated. It combines the aggregate capacity floor with
  per-controlled-player owner/block changes, post-seam command ACKs, and
  selected-player survival checks.
- `-RunCapacityGate` samples the live `:8091` dynamic monitor for a sustained
  window and optionally samples the `:8092` broker mirror. By default it requires
  at least 30 players, 800 live entities, 16 worker load slots, 8 good samples,
  and observed dynamic zone geometry. This is a reproducible capacity floor,
  not a maximum-player benchmark.
- `run_mit_clone_stress_ladder.ps1` restarts the same real stack across several
  bot-count profiles, runs `-RunBrokerCommandCapacityGate` for each profile,
  writes raw per-profile logs, and emits `.local/agar_mit_clone_ladder/ladder_summary.json`.
  It is a measured floor/first-failure profile, not a maximum-player claim.
  The default starts 8 controlled stock-clone players and requires 4 completed
  seam proofs. Dead probes are recorded as failed players and do not count; this
  keeps normal Agar eating ecology live without letting one eaten probe hide the
  broker-command seam signal. Each row also records command ACK latency
  summaries plus process CPU and working-set snapshots for the local ports, so
  the observed floor has capacity context instead of a bare player count.

## Honest Boundary

`-RunCapacityGate` is a soak ruler, not an absolute benchmark claim. It answers
"this local machine sustained at least this configured floor under this adapter"
and emits a redacted JSON summary for model-plane ingestion. A real maximum
claim still needs a profile that records hardware, CPU/RSS, command ACK latency,
handoff success rate, and broker/client agreement under increasing bot/client
counts.

The stress ladder has the same boundary. It improves the evidence shape by
recording multiple profiles with command ACKs, broker/client agreement,
hardware context, process CPU/RSS snapshots, and command latency summaries, but
the result is still local-machine evidence. Treat the highest green row as an
observed floor until longer soak windows and larger hardware-annotated ladders
are recorded.

The playable seam gate is also not a broker-authoritative command claim. The MIT
clone still receives the probe movement command directly through its normal
Socket.IO input. Use `-RunBrokerCommandGate` for the stronger command-routing
proof. That proof is still limited to one controlled stock-clone player; it does
not claim the whole MIT clone server has been replaced by Godworks-authoritative
gameplay. `-RunBrokerCommandCapacityGate` strengthens that proof to several
controlled players under live bot load, but the stock clone still owns its food,
collision, split/eat, and rendering ecology.

Public demo claim: this is a playable MIT agar clone plus Godworks live
sharding, mirror ownership, and broker-routed controlled-player seam gates. It
is not yet a claim that the entire MIT clone server has been replaced by
Godworks-authoritative gameplay. The stock clone still owns food, collision,
eating, splitting, and rendering ecology. Godworks owns the adapter proof:
dynamic map partitioning, mirror ownership, broker command routing for selected
players, and measured local floor gates.

## Godworks-authoritative v0

`run_godworks_authoritative.ps1` is the first mode that removes the stock MIT
server from the authority path. It still reuses the MIT browser client and local
clone dependencies, but it does not start `npm start` / `src/server/server.js`.
Instead:

- the runner installs the clone dependencies when needed and builds the MIT
  browser bundle into `bin/client`;
- the runner starts a real Godworks broker with `GW_GRID2D=4x4`;
- it starts one `gw_authoritative_zone_worker.js` process per grid cell;
- each zone worker connects through normal `WorkerConnect.region`, spawns food,
  owns entities through broker authority, moves owned players, eats owned food
  or smaller players, and hands entities across broker grid seams;
- `gw_authoritative_server.js` serves the MIT client on `:3000`, accepts the
  clone Socket.IO protocol, asks the correct region worker to spawn each player,
  sends movement as broker `CommandRequest`, and emits `serverTellPlayerMove`
  from Godworks broker state;
- `gw_authoritative_gate.js` proves a real Socket.IO player receives frames,
  moves through Godworks commands, sees food, and gets fresh command responses
  from the broker path. The browser layer is also expected to show the normal
  MIT `Open Agar` client on `:3000`; pressing Play should enter a live canvas
  backed by this Godworks-authoritative server.

This is an authority-path milestone, not the whole final game. Current non-scope
for v0: split, mass eject, viruses, multi-cell merge timing, production
matchmaking, persistence of accounts, and a 10k-player capacity claim. Those are
now implementation work on top of the Godworks-authoritative path, not blockers
hidden behind the stock server.

## Godworks-authoritative Capacity Ladder

`run_godworks_authoritative_stress_ladder.ps1` measures capacity floors for the
authoritative path, not the old stock-server mirror path. For each configured
bot count it:

- starts a clean Godworks-authoritative stack;
- starts real Socket.IO player bots through the same `:3000` gateway as the
  browser client;
- samples `:3000/state` and the bot controller state;
- requires sustained players, player entities, total entities, all worker-owner
  slots, fresh broker command-response growth, zero fresh command-reject growth
  during the measured window by default, bounded transient stale/no-owner
  command retries, live bot frames, and live bots at the end of the window;
- writes `.local/agar_authoritative_ladder/ladder_summary.json` with host,
  logs, thresholds, initial/latest server state, initial/latest bot state, raw
  reject diagnostics, and process resource snapshots.

This is the first step toward larger-map and 10k-player work. Treat the highest
green row as an observed local floor under the current runner and hardware, not
as a maximum-player claim.
