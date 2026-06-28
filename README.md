# Godworks OS

A distributed-authority backend for seamless, multi-server game worlds — SpatialOS-class,
in Rust. One world bigger than a single server: per-component authority, lossless zone
handoff, epoch-fenced writes, write-ahead-log durability, interest management, dynamic
load rebalancing, and a cross-broker mesh.

## Status

Pre-1.0, source-available under the Business Source License 1.1 (see `LICENSE`; converts
to Apache-2.0 on the Change Date). The SDK and the Godot-based client engine (Clockworks)
are separate and not part of this repository.

## Build

```
cargo build --release
./target/release/godworks_broker     # the broker (authoritative store + op-router + handoff)
./target/release/zone_worker         # a zone-worker (owns a region, simulates its entities)
```

## What it does

- **Per-component authority + lossless handoff** — each entity component has a single
  authoritative owner; as an entity crosses a zone boundary, authority is handed to the
  neighbouring zone's worker with no lost writes.
- **Partitioning** — 1D-strip and 2D-grid; **dynamic load rebalancing** shifts boundaries
  / reassigns zones as load moves.
- **Durability** — a write-ahead log; a restart recovers the live world.
- **Interest management / AOI** — workers see only what they declare interest in.
- **Cross-broker mesh** — a world spanning multiple broker processes / machines.
- **Wire** — a length-prefixed JSON op protocol (WorkerConnect / Interest / CreateEntity /
  UpdateComponent / AuthorityChange / ...).

## License

Business Source License 1.1 — free to read, modify, and use, with one restriction
(no competing hosted service) that lapses on the Change Date, when it becomes Apache-2.0.
For commercial or managed-hosting licensing, contact the Licensor.
