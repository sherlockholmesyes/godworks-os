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
- reconnect/resync cache lifecycle, including full rebuild from
  `EntityQueryResponse`
- `ClientBridge`, a transport-free engine-facing facade over the cache

The crate does not open sockets, run interpolation, predict movement, or expose an engine-specific API. Godot/Unity bridges should wrap this cache instead of duplicating protocol state handling in engine scripts.

## Contract

The cache preserves the server stream as the source of truth:

- `UpdateRejected` records the rejection and does not mutate entity state.
- Ghost entities remain tagged with `ghost=true` and `owner_region`.
- Authority changes are stored per component with epoch and mode.
- Critical sections are surfaced as stream structure, not hidden.
- `reset_for_reconnect()` clears entities, authority grants, ghost mirrors,
  rejections, and critical-section depth; old stream state must not survive a
  new broker connection.
- `begin_resync()` starts a fresh checkout pass and clears the old view.
- `finish_resync_from_query_response()` treats a full `EntityQueryResponse` as
  a canonical checkout cut: absent old entities are removed, returned entities
  are rebuilt, and the cache becomes `Live`.

This is the product-facing starting point for issue #5: headless cache first, engine bridge second.

## Reconnect flow

Transport and engine bridges should keep the sequence explicit:

1. On socket drop or broker kick, call `reset_for_reconnect()`.
2. When opening a new connection, call `mark_connecting()`.
3. Before requesting a full checkout, call `begin_resync()`.
4. Apply the full checkout through `finish_resync_from_query_response()`.
5. Resume normal stream updates with `apply_op()`.

Do not merge old rows into a new checkout. Authority epochs from the old
connection are advisory history only; they must not drive writes after a
reconnect. The Godot cross-broker probe is a runtime ruler for the wire path,
not yet a reusable engine bridge.

## Engine bridge facade

`ClientBridge` is the first thin engine-facing layer. It is still headless and
transport-free, but it makes the reconnect/resync lifecycle explicit for a
Godot, Unity, or custom engine binding:

1. `on_transport_closed()`
2. `on_transport_connecting()`
3. `begin_full_resync()`
4. `finish_full_resync(EntityQueryResponse)`
5. `apply_stream_op(Op)`

The bridge owns one `ClientCache`. Engine bindings should read
`ClientBridge::snapshot()` and react to `ClientBridgeEvent` instead of
maintaining a second, parallel cache in engine scripts.

Two fail-under-broken tests pin the contract:

- `bridge_reconnect_resync_exports_only_the_new_checkout_cut` fails if stale
  entities, ghost rows, rejected writes, or critical-section depth survive a
  reconnect/full-resync cycle.
- `bridge_ordinary_query_response_does_not_replace_live_cache` fails if a
  partial `EntityQueryResponse` in normal live mode is accidentally treated as
  the canonical reconnect checkout.
