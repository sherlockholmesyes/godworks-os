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

Run the playable clone plus dynamic shard monitor:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1
```

Run with a public Godworks broker mirror:

```powershell
.\examples\agar_mit_clone\run_mit_clone_adapter.ps1 -MirrorBroker -BuildBroker -RunGate
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

## Honest Boundary

This is not a benchmark claim. It proves integration shape and live-game
visibility. Capacity must be measured by a separate soak profile with real
numbers for bot/client count, command ACK latency, handoff success, CPU/RSS, and
broker/client agreement.

The current public protocol uses one `WorkerConnect.region` per TCP connection.
The old scratchpad D3 multi-region worker harness is therefore not copied as a
product claim here. Broker-side dynamic rebalance needs a typed multi-region or
partition-map activation contract before it should be advertised from this
example.
