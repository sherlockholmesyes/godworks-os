# 3D Foundation Rail

Godworks OS proves its first product slice in 2D because that is the fastest way
to validate the hard runtime invariants: authority, handoff, WAL recovery,
interest management, worker SDK behavior, and product ergonomics.

That does not mean the architecture should grow 2D-only assumptions. New
contracts and fixtures should make the future 3D path explicit even while the
current runtime remains 2D.

## Rule

```text
2D runtime now.
3D-ready contracts now.
Full 3D runtime later.
```

The immediate work is not to replace the current broker, worker, or protocol
with a 3D implementation. The immediate work is to prevent protocol names,
component schemas, snapshots, WAL records, SDK assumptions, and replay data from
locking in a 2D-only shape.

## Required Contract Axes

Every new spatial-facing subsystem should have a clear answer for these axes:

- `spatial_dim`
- `coordinate_codec`
- `physics_island_schema`
- `partition_schema`

For the current product slice those values can describe a 2D implementation.
The important part is that the schema has a place for the answer.

## Coordinate Model

Do not treat "3D support" as a simple switch from `[x, y]` to `[x, y, z]`.
Large worlds need a coordinate contract that can support:

- fixed or integer world coordinates for stable network/storage identity;
- local floating-point frames for physics;
- explicit conversion and overflow behavior;
- debug JSON representation now and a binary representation later.

The first implementation should define the codec before changing the runtime.
Candidate concepts:

- `SpatialDim`
- `FixedCoord`
- `WorldPos2Fixed`
- `WorldPos3Fixed`
- `LocalFrame2`
- `LocalFrame3`
- `LocalPos2F32`
- `LocalPos3F32`
- `Quat32`

## Component Names

Do not add ad-hoc `pos`, `position`, `pos3`, `rot`, `rotation`, `lin`, and
`ang` variants over time. Use stable names or a component registry.

The built-in registry now assigns stable IDs to the current 2D wire names and
the future 3D rail names. See `docs/protocol/component-registry.md`.

Candidate core component names:

- `core.pos2`
- `core.vel2`
- `core.pos3`
- `core.vel3`
- `core.rot3`
- `core.lin3`
- `core.ang3`
- `core.at_rest`
- `core.local_frame`
- `core.physics_body`
- `core.physics_shape`
- `core.physics_material`

The existing component bag can carry these without changing the existing JSON
wire shape.

## Physics-Island Authority

Per-component authority remains the base invariant. Physics components that
must move together should also have an explicit island schema.

Candidate 3D physics island:

```text
core.pos3
core.vel3
core.rot3
core.lin3
core.ang3
core.at_rest
core.physics_body
```

The island migrates as one authority epoch. Gameplay components such as
inventory, ownership, quests, and economy do not move just because physics
crosses a spatial boundary.

## Interest And Partitioning

The current runtime can stay 2D, but contract names should not imply that all
future interest and partitioning is 2D.

Future-ready schema concepts:

- `AoiShape2`: circle, AABB;
- `AoiShape3`: sphere, AABB;
- `PartitionSchema::Grid2D`;
- `PartitionSchema::Grid3D`;
- layered or planetary partition schemas later.

3D queries do not need to be active in broker dispatch until a 3D partition map
exists. They should be parseable and covered by golden fixtures before they are
runtime-authoritative.

## WAL, Snapshot, And Replay

Snapshots, WAL-derived artifacts, and replay/model-plane events should carry
spatial metadata before they become long-lived product surfaces:

- `spatial_schema_version`
- `coordinate_codec_version`
- `component_registry_version`
- `partition_schema`
- physics-island schema/version

Replay and telemetry events should avoid training future evaluation tools on
2D-only names when the event is really spatial:

```json
{
  "kind": "broker_handoff",
  "spatial_dim": "D2",
  "coordinate_codec": "debug_f64_2",
  "partition_schema": "grid2d"
}
```

## Physics Backend

Do not hardwire a physics engine into the protocol or broker. Use an adapter
boundary first.

The first Rust-native proof can use a 3D sibling of the current 2D stack, but
the contract should leave room for other game-physics backends later.

## First Packets

Good early packets:

1. Add the spatial docs and schema terms.
2. Add core spatial types with tests for fixed coordinates, AABB/sphere
   containment, and cell id encoding.
3. Add protocol golden fixtures that preserve 3D component bags through
   `CreateEntity`, `MeshHandoff`, `AuthorityChange`, and `UpdateRejected`.
4. Add explicit spatial tags to snapshot/replay artifacts.
5. Add a physics backend trait before integrating any 3D physics engine.

## Non-Goals Now

- Do not replace current 2D position/AOI everywhere.
- Do not change the existing 2D JSON wire shape.
- Do not migrate `zone_worker` to 3D yet.
- Do not add a 3D physics engine directly into the broker.
- Do not require fixed-point coordinates for the current 2D runtime.
- Do not build a 3D client before server-side contracts are stable.
