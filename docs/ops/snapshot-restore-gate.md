# Snapshot Restore Gate

Godworks snapshots are WAL cut markers, not a second persistence format.
An admin peer sends `SnapshotMarker`; the broker first flushes staged durable
transitions, then appends a durable `snapshot_marker` record and replies with a
`SnapshotManifest` containing the current `wal_offset`.

`SnapshotManifest` is also the restore artifact contract. It carries enough
schema metadata for future SDKs, tools, replay/eval jobs, and 3D-ready clients
to interpret the cut without relying on out-of-band process state:

- `snapshot_manifest_version`;
- `snapshot_schema_version`;
- `spatial_schema_version`;
- `coordinate_codec_version`;
- `component_registry_version`;
- `partition_map_version`;
- `spatial_schema.spatial_dim`;
- `spatial_schema.coordinate_codec`;
- `spatial_schema.partition_schema`;
- `partition_map`.

For the current product slice, `spatial_schema` still describes the 2D runtime:
`D2`, `debug_f64_2`, and either `strip1d` or `grid2d`. The point is not to turn
snapshot restore into a second persistence system. The point is that the cut
names the same spatial/partition contract as replay and future protocol
fixtures.

`partition_map_version` is the revision number. `partition_map` is the
reproducible routing contract for that revision:

- `strip1d`: sorted cut `boundaries` and deterministic per-region `splits`;
- `grid2d`: `cols`, `rows`, `cell_w`, `cell_h`, and explicit `origin`.

The map is emitted from the same `ServerState` fields used by runtime routing;
it is not a second partition engine. Mesh topology is intentionally not part of
this versioned partition map, because mesh discovery and remote-link lifecycle
do not currently advance `partition_map_version`. A future mesh-topology
contract should carry its own provenance instead of hiding inside this one.

The protocol crate exposes two layers for `SnapshotManifest`:

- lossless JSON fields plus partial typed accessors for debugging and
  compatibility inspection;
- `SnapshotManifest::contract()` as the validated restore-artifact view.

`contract()` requires current schema versions, a `wal_offset`, cut summary
counts (`entity_count`, `pending_mesh`), strict `in_flight` consistency, a valid
`spatial_schema`, and a strict `partition_map` whose embedded version/schema
matches the manifest's top-level version/schema. Optional labels/telemetry such
as `request_id`, `snapshot_id`, `broker_id`, and `t_server` are preserved when
present. `authority_hash` is optional, but must be well-formed when present.
None of these checks change the lossless wire shape.

`component_registry_version = 1` means the cut can be interpreted against the
built-in registry in `godworks-core`; it does not mean unknown project-specific
components are rejected by the broker yet.

To restore a broker to that point-in-time cut, restart it with the same WAL and:

```text
GW_RESTORE_OFFSET=<SnapshotManifest.wal_offset>
```

The recovery contract is:

- queued durable transitions that are already accepted by the broker are flushed
  before the marker names the cut;
- records before the cut are replayed;
- records after the cut are ignored;
- `mesh_out` before the cut restores as an in-flight pending handoff on the
  source broker;
- a later `mesh_acked` clears that pending handoff during full recovery;
- a source broker never resurrects a mesh-departed entity locally.

The task-relative regression gates are:

```text
snapshot_marker_restore_offset_rolls_back_post_cut_entities
snapshot_marker_flushes_pending_update_before_cut
snapshot_manifest_carries_spatial_schema_contract
snapshot_manifest_carries_strip_partition_map_contract
snapshot_manifest_contract_accessors_match_current_wire_shape
snapshot_manifest_contract_rejects_future_versions
snapshot_manifest_contract_requires_wal_offset
snapshot_vector_restores_in_flight_mesh_handoff_exactly_once
```

These tests are deliberately about recoverable broker state, not general
compile-time health. They should fail if a snapshot cut cannot be replayed
exactly.
