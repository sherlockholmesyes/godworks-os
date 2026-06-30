# Snapshot Restore Gate

Godworks snapshots are WAL cut markers, not a second persistence format.
An admin peer sends `SnapshotMarker`; the broker appends a durable
`snapshot_marker` record and replies with a `SnapshotManifest` containing the
current `wal_offset`.

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
snapshot_vector_restores_in_flight_mesh_handoff_exactly_once
```

These tests are deliberately about recoverable broker state, not general
compile-time health. They should fail if a snapshot cut cannot be replayed
exactly.
