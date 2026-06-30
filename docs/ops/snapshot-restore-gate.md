# Snapshot Restore Gate

Godworks snapshots are WAL cut markers, not a second persistence format.
An admin peer sends `SnapshotMarker`; the broker appends a durable
`snapshot_marker` record and replies with a `SnapshotManifest` containing the
current `wal_offset`.

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
- `spatial_schema.partition_schema`.

For the current product slice, `spatial_schema` still describes the 2D runtime:
`D2`, `debug_f64_2`, and either `strip1d` or `grid2d`. The point is not to turn
snapshot restore into a second persistence system. The point is that the cut
names the same spatial/partition contract as replay and future protocol
fixtures.

The protocol crate exposes typed `SnapshotManifest` accessors for those fields
and a current-version check. The JSON wire shape stays lossless and unchanged;
typed consumers no longer need to hand-parse the artifact contract.

`component_registry_version = 1` means the cut can be interpreted against the
built-in registry in `godworks-core`; it does not mean unknown project-specific
components are rejected by the broker yet.

To restore a broker to that point-in-time cut, restart it with the same WAL and:

```text
GW_RESTORE_OFFSET=<SnapshotManifest.wal_offset>
```

The recovery contract is:

- records before the cut are replayed;
- records after the cut are ignored;
- `mesh_out` before the cut restores as an in-flight pending handoff on the
  source broker;
- a later `mesh_acked` clears that pending handoff during full recovery;
- a source broker never resurrects a mesh-departed entity locally.

The task-relative regression gates are:

```text
snapshot_marker_restore_offset_rolls_back_post_cut_entities
snapshot_manifest_carries_spatial_schema_contract
snapshot_manifest_contract_accessors_match_current_wire_shape
snapshot_manifest_contract_rejects_future_versions
snapshot_vector_restores_in_flight_mesh_handoff_exactly_once
```

These tests are deliberately about recoverable broker state, not general
compile-time health. They should fail if a snapshot cut cannot be replayed
exactly.
