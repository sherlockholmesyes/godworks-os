# Current State Audit

This document is the starting point for hardening Godworks OS into a product-quality spatial backend.

## Verified scope

Godworks OS currently builds as a Rust server-side runtime, not a full client game engine. The repository contains these binaries:

- `godworks_broker` — authoritative store, operation router, handoff runtime, WAL durability, mesh runtime.
- `zone_worker` — a Rapier2D-based physics zone worker migrated onto the
  Rust Worker SDK framing and typed-op helpers.
- `reality_loadgen` — a product/reality harness that drives the live protocol through create, interest/AOI, updates, events, commands, queries, mesh movement, and slow-consumer behavior.

## Product classification

Current classification:

```text
spatial backend prototype / pre-1.0 distributed-authority runtime
```

Not yet:

```text
complete game engine
managed cloud platform
full SpatialOS replacement
3D production runtime
```

## Strong foundations already present

- Per-component authority with epoch fencing.
- Region ownership and region-worker leases.
- Local handoff and cross-broker mesh handoff.
- WAL durability with fail-closed behavior.
- WAL recovery and compaction primitives.
- Interest management / AOI.
- Coarse fidelity updates.
- Backpressure and bounded egress channels.
- Max frame size and per-peer ingress cost limits: peers are charged by op
  class and large valid JSON payload size before dispatch.
- WorkerConnect authentication with both legacy shared-token mode and strict
  token-bound region/attribute claims.
- Broker-side peer roles for worker/client/observer/mesh op-family policy.
  Clients cannot lease worker regions, observers cannot mutate entity state, and
  mesh links cannot act as gameplay workers.
- Global OBS visibility is claim-gated: an OBS peer needs `observer`, `debug`,
  or `inspector` to see the whole world without an AOI.
- `godworks-protocol`, `godworks-core`, and `godworks-worker-sdk` alpha crates.
- `godworks-client-sdk` alpha crate with a headless cache and transport-free
  `ClientBridge` facade for engine bindings.
- Stable built-in component registry v1 in `godworks-core`. The current JSON
  wire still uses names such as `pos` and `vel`, but registry IDs now provide
  the compatibility anchor for snapshots, replay/eval, SDKs, and a future
  binary codec. See `docs/protocol/component-registry.md`.
- `zone_worker` outbound protocol I/O through the Worker SDK, with typed inbound
  handling for authority/rejection/lifecycle-critical frames.
- Shared WAL decoder and `wal_inspect` CLI.
- Health/inspector-oriented runtime state.
- Optional redacted replay tape for broker connect, ingress decision, selected
  outbound decision frames, and handoff breadcrumbs.
- Rapier2D physics worker demo.
- Reality/load harness.
- Agent reality-gate scaffold for trace/eval/promotion of agent-produced changes.

## Major product gaps

### SDK and client integration

The server runtime is not enough for a game team. The product needs:

- Godot bridge/binding that consumes `godworks-client-sdk::ClientBridge`;
- typed component helpers;
- transport examples for reconnect/resync over real sockets;
- more examples that do not require hand-writing JSON frames.

### Protocol stability

The current wire is length-prefixed JSON. This is good for debugging and early compatibility, but the project needs:

- explicit protocol schema;
- versioned operation definitions;
- golden compatibility tests;
- binary codec path for production;
- max frame size and protocol error taxonomy.

### Operations

The runtime needs a repeatable operations story:

- config files, not only environment variables;
- Docker and Compose;
- Kubernetes smoke manifests;
- metrics exporter;
- structured logs;
- recovery CLI;
- runbooks.
- trace ledger for externally produced patches and reviews.

### Security

The product needs a formal security layer:

- broader deployment policy for worker/client/observer/mesh credentials;
- stronger mesh authentication beyond the current token-claim baseline;
- broader per-principal rate-limit policy;
- TLS/mTLS option.

### 3D

The current architecture is effectively 2D:

- entity `pos` and `vel` are two-element arrays;
- partitioning is 1D strip or 2D grid;
- the provided physics worker uses Rapier2D.

The first product slice should stay 2D. That keeps authority, handoff, WAL,
interest, SDK, and product ergonomics cheap to prove.

However, new schemas should not encode a hidden "2D forever" assumption. The
project now has a 3D foundation rail for contracts and fixtures:

- dimensioned spatial schema;
- coordinate codec terms for future fixed/mixed precision;
- 3D component and physics-island names;
- 3D AOI and partition schema names;
- snapshot/WAL/replay spatial metadata;
- protocol fixtures that preserve 3D component bags.
- built-in component registry entries for current 2D names and future 3D
  component names.

Full 3D runtime, a 3D physics worker, and a 3D client remain later work. See
`docs/spatial/3d-foundation.md`.

## Initial hardening milestones

1. Reproducible build and CI — done for the current Rust baseline.
2. Workspace/module split — protocol/core/worker-sdk crates exist.
3. Protocol v1 draft — initial draft exists.
4. WAL/recovery module and CLI — shared WAL reader and `wal_inspect` exist.
5. Worker SDK v0 — alpha exists and `zone_worker` uses it.
6. Client SDK v0 — alpha cache and engine bridge facade exist.
7. Godot bridge v0 — binding/example remains.
8. Top-down arena demo.
9. Security v0 — max-frame, ingress frame-rate, shared-token auth,
   token-bound WorkerConnect claims, and peer role policy exist; broader
   deployment/rate/TLS policy remains.
10. Ops/deployment layer.

## Definition of product beta

A beta-quality Godworks OS release should let a new developer run:

```bash
godworks dev up topdown-arena
```

and get:

- two brokers;
- multiple workers;
- a playable Godot client;
- seamless handoff;
- AOI/interest management;
- durable recovery;
- metrics dashboard;
- documented SDK usage.
