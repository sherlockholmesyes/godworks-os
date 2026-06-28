# Snapshot artifact gate

Godworks OS supports a point-in-time broker snapshot through the existing WAL.

The snapshot artifact is not a separate database dump. It is a durable WAL cut:

- a privileged coordinator sends `SnapshotMarker`;
- the broker writes a marker record to the WAL;
- the broker returns a `SnapshotManifest`;
- the manifest's `wal_offset` can be passed back as `GW_RESTORE_OFFSET`;
- recovery replays only WAL records whose byte range ends at or before that offset.

## Manifest

`SnapshotManifest` includes:

| Field | Meaning |
|---|---|
| `snapshot_id` | Caller-provided snapshot name. |
| `broker_id` | Broker identity from `GW_BROKER_ID`, or `broker` by default. |
| `wal_offset` | WAL byte offset for the recoverable cut. |
| `entity_count` | Number of live local entities at the cut. |
| `authority_hash` | Compact authority fingerprint for comparing cuts. |
| `pending_mesh` | Number of pending outbound mesh handoffs. |
| `in_flight` | Current in-flight handoff records. |

## Runtime proof

The single-broker runtime test exercises the public TCP protocol and a real broker restart:

```powershell
cargo test snapshot_marker_restore_offset_rolls_back_post_cut_entities -- --nocapture
```

The test:

1. starts a broker with a unique WAL path;
2. creates three pre-cut entities through `CreateEntity`;
3. sends `SnapshotMarker` and records `SnapshotManifest.wal_offset`;
4. creates two post-cut entities;
5. confirms the live world has five entities;
6. stops the broker;
7. restarts the broker with the same `GW_WAL` and `GW_RESTORE_OFFSET`;
8. confirms the restored world has only the three pre-cut entities.

This gate fails if the manifest names a cut that recovery cannot reproduce, or if restore leaks post-cut state.

## Multi-broker vector proof

The vector restore gate covers a cross-broker handoff that is in flight at the snapshot cut:

```powershell
cargo test snapshot_vector_restores_in_flight_mesh_handoff_exactly_once -- --nocapture
```

The test:

1. starts an `E` broker with inbound mesh adoption intentionally dropped;
2. starts a `W` broker linked to `E`;
3. creates an entity on `W`;
4. drains it into the mesh handoff path;
5. waits until `W` has zero local entities and one `pending_mesh` record while `E` has zero entities;
6. takes a snapshot marker on both brokers;
7. restarts both brokers from their respective offsets, this time with normal adoption enabled;
8. waits for `W` to resend the recovered pending handoff and for `E` to adopt it;
9. confirms the restored cluster has exactly one copy of the entity on `E` and none on `W`.

This gate fails if source-side in-flight handoffs are not recovered from the WAL cut, if the target loses the entity, or if restore creates a duplicate.
