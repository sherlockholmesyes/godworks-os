# Response/query/event/admin protocol coverage notes

This branch is protocol-only. It does not change broker dispatch or runtime behavior.

## Runtime evidence used

- `src/main.rs` emits `CreateEntityResponse`, `DeleteEntityResponse`, `ReserveEntityIdsResponse`, `SetComponentAuthorityResponse`, and `ThresholdTxResponse` from the lifecycle/admin dispatch paths.
- `src/main.rs` routes `CommandRequest` and `CommandResponse` through the current authority holder.
- `src/main.rs` handles `EntityQuery` and emits `EntityQueryResponse` with visible authoritative and ghost rows.
- `src/main.rs` handles `InspectorQuery` and emits `InspectorFrame` containing broker, zone, worker, entity, and diagnostic state.
- `src/main.rs` handles transient `EntityEvent`, `FlagUpdate`, `Metrics`, `MeshGhost`, `MeshGhostRemove`, `Fold`, `ThresholdTx`, and `SnapshotMarker` wire families.

## Losslessness rule

For broad response/query/event/admin families, the typed protocol uses operation-specific JSON field bags. This keeps the enum explicit while preserving every current field exactly until the product SDK can narrow shapes safely.

This avoids the combined failure mode:

```text
broad typed enums without lossless tests + SDK migration = confidence theater and fossilized raw-JSON gaps
```

The SDK should only start after this boundary can roundtrip the current wire families without field loss.
