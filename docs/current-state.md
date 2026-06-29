# Current State Audit

This document is the starting point for hardening Godworks OS into a product-quality spatial backend.

## Verified scope

Godworks OS currently builds as a Rust server-side runtime, not a full client game engine. The repository contains these binaries:

- `godworks_broker` — authoritative store, operation router, handoff runtime, WAL durability, mesh runtime.
- `zone_worker` — a Rapier2D-based physics zone worker that speaks the Godworks wire protocol.
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
- Health/inspector-oriented runtime state.
- Rapier2D physics worker demo.
- Reality/load harness.

## Major product gaps

### SDK and client integration

The server runtime is not enough for a game team. The product needs:

- Rust worker SDK;
- client SDK;
- Godot bridge;
- typed component helpers;
- reconnect/resync behavior;
- examples that do not require hand-writing JSON frames.

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

### Security

The product needs a formal security layer:

- worker/client authentication;
- mesh authentication;
- frame size limits;
- rate limits;
- observer/global-interest permissions;
- token-bound worker attributes;
- TLS/mTLS option.

### 3D

The current architecture is effectively 2D:

- entity `pos` and `vel` are two-element arrays;
- partitioning is 1D strip or 2D grid;
- the provided physics worker uses Rapier2D.

The 1.0 product target should be 2D. 3D should be handled as a separate feasibility branch and later major version.

## Initial hardening milestones

1. Reproducible build and CI.
2. Workspace/module split.
3. Protocol v1 draft.
4. WAL/recovery module and CLI.
5. Worker SDK v0.
6. Client SDK v0.
7. Godot bridge v0.
8. Top-down arena demo.
9. Security v0.
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
