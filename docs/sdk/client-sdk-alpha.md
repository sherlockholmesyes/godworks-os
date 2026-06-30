# Client SDK alpha

`godworks-client-sdk` is the first thin client-side layer for Godworks OS. It is intentionally headless and transport-free: it consumes typed `godworks-protocol::Op` frames and maintains a local entity/component view.

## Scope

The alpha cache handles:

- `AddEntity` / `RemoveEntity`
- `ComponentUpdate` / `BatchUpdate`
- `AuthorityChange`
- `UpdateRejected`
- `CriticalSection` depth tracking
- `MeshGhost` / `MeshGhostRemove` read-only projection markers

The crate does not open sockets, run interpolation, predict movement, or expose an engine-specific API. Godot/Unity bridges should wrap this cache instead of duplicating protocol state handling in engine scripts.

## Contract

The cache preserves the server stream as the source of truth:

- `UpdateRejected` records the rejection and does not mutate entity state.
- Ghost entities remain tagged with `ghost=true` and `owner_region`.
- Authority changes are stored per component with epoch and mode.
- Critical sections are surfaced as stream structure, not hidden.

This is the product-facing starting point for issue #5: headless cache first, engine bridge second.
