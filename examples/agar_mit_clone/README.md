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

Use `-StopExisting` when an old local demo owns the same ports.

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

## Honest Boundary

`-RunCapacityGate` is a soak ruler, not an absolute benchmark claim. It answers
"this local machine sustained at least this configured floor under this adapter"
and emits a redacted JSON summary for model-plane ingestion. A real maximum
claim still needs a profile that records hardware, CPU/RSS, command ACK latency,
handoff success rate, and broker/client agreement under increasing bot/client
counts.

The playable seam gate is also not a broker-authoritative command claim. The MIT
clone still receives the probe movement command directly through its normal
Socket.IO input. Use `-RunBrokerCommandGate` for the stronger command-routing
proof. That proof is still limited to one controlled stock-clone player; it does
not claim the whole MIT clone server has been replaced by Godworks-authoritative
gameplay. `-RunBrokerCommandCapacityGate` strengthens that proof to several
controlled players under live bot load, but the stock clone still owns its food,
collision, split/eat, and rendering ecology.

The current public protocol uses one `WorkerConnect.region` per TCP connection.
The old scratchpad D3 multi-region worker harness is therefore not copied as a
product claim here. Broker-side dynamic rebalance needs a typed multi-region or
partition-map activation contract before it should be advertised from this
example.
