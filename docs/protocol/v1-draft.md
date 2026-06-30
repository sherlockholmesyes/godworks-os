# Godworks Wire Protocol v1 Draft

This is the draft protocol contract for the current length-prefixed JSON wire.

The short-term goal is to document the existing protocol before replacing ad-hoc JSON construction with typed protocol structs. The long-term goal is to keep JSON as the debug/development codec and add a binary production codec behind the same operation model.

## Framing

Current frame format:

```text
4-byte big-endian unsigned length
JSON body of exactly that length
```

Current hardening requirement:

```text
A broker must reject oversized frames before allocating the full body.
A broker must reject peers that exceed their inbound frame budget before the op is dispatched.
```

The typed protocol crate exposes `DEFAULT_MAX_FRAME_BYTES`; broker enforcement
is wired into the frame reader. The broker also applies a per-peer token bucket
(`GW_INGRESS_RATE_PER_SEC`, `GW_INGRESS_BURST_FRAMES`) and returns structured
`rate_limit_error` rejections when a peer exhausts its ingress cost budget. The
budget is cost-based, not a raw frame count: expensive ops such as
`CreateEntity`, `EntityQuery`, `BatchUpdate`, and authority/control ops consume
more units, and large-but-valid JSON payloads are charged from the received wire
body length.

## Version negotiation

Workers should connect with:

```json
{"op":"WorkerConnect","worker_id":"worker-1","region":"W","proto":1}
```

The broker must reject peers outside its supported protocol range and return a structured protocol rejection.

When `GW_AUTH_TOKEN` is configured on the broker, `WorkerConnect` must include
the matching `auth_token` string. A missing or mismatched token receives:

```json
{"op":"AuthReject","worker_id":"worker-1","error":"auth_error","reason":"authentication required"}
```

The peer is not registered and does not claim region ownership.

For private-alpha / production-style runs, prefer `GW_AUTH_CLAIMS` over the
single shared token. `GW_AUTH_CLAIMS` maps each token to broker-owned
connection claims:

```text
GW_AUTH_CLAIMS="w-secret:W:physics|sim,client-secret:CLIENT:role.client,mesh-secret:MESH:role.mesh"
```

In this mode the peer may not self-assign a different region or extra
attributes in `WorkerConnect`; the broker derives the registered region and
attributes from the token claim and rejects mismatches with `AuthReject` before
registration.

## Component identity

The v1 JSON wire still uses component names such as `pos`, `vel`, and
game-specific strings in component-bearing frames. Stable numeric component IDs
live in the built-in registry described by
`docs/protocol/component-registry.md`.

For v1:

```text
JSON name = current debug/development codec.
Component ID = compatibility anchor for snapshots, replay/eval, SDKs, and future binary codecs.
```

The broker does not reject unknown project-specific component names solely
because they are absent from the built-in registry. Higher layers may extend the
registry, but they must not reuse built-in IDs.

## Peer roles

The wire keeps the current `WorkerConnect` shape, but the broker derives a
broker-side role before a peer can mutate state:

```text
MESH or role.mesh      -> mesh
OBS or role.observer   -> observer
CLIENT or role.client  -> client
otherwise              -> worker
```

Roles are an authorization boundary, not a replacement for component authority.
A `client` may send `CommandRequest`, `Interest`, `EntityQuery`, and
`UpdateComponent`, but the normal ACL/authority/epoch gates still decide whether
the component write applies. A `mesh` peer is a cross-broker conduit and cannot
create entities or write components. An `observer` can read according to AOI or
observer claims but cannot mutate entity state. A `worker` keeps the existing
simulation/lifecycle surface, except mesh-family frames remain reserved for mesh
links.

## Operation groups

### Connection and liveness

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `WorkerConnect` | peer -> broker | no | Register a worker/client/mesh connection. |
| `AuthReject` | broker -> peer | no | Reject a connection before registration when connect auth fails. |
| `Disconnect` | peer -> broker | no | Graceful connection close. |
| `Heartbeat` | worker -> broker | no | Renew region lease for owned regions. |
| `Health` | peer -> broker | no | Request liveness/health snapshot. |

### Interest and visibility

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `Interest` | peer -> broker | no | Set AOI, full/coarse fidelity, and checkout visibility. |
| `AddEntity` | broker -> peer | no | Entity entered the peer view. |
| `RemoveEntity` | broker -> peer | no | Entity left the peer view. |
| `ComponentUpdate` | broker -> peer | no | Component delta for a visible entity. |
| `CriticalSection` | broker -> peer | no | Bracket initial checkout or atomic visibility groups. |

### Entity lifecycle

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `CreateEntity` | worker -> broker | yes | Create/register an entity. |
| `CreateEntityResponse` | broker -> worker | no | Create result when a request id is supplied. |
| `DeleteEntity` | worker -> broker | yes | Tombstone/delete an entity. |
| `DeleteEntityResponse` | broker -> worker | no | Delete result when a request id is supplied. |
| `ReserveEntityIds` | worker -> broker | yes | Reserve monotonic entity id range. |
| `ReserveEntityIdsResponse` | broker -> worker | no | Reserved id range response. |

### Component writes

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `UpdateComponent` | worker/client -> broker | yes | Update one component on one entity. |
| `BatchUpdate` | worker/client -> broker | yes | Update one component across many entities. |
| `AddComponent` | worker -> broker | yes | Add a dynamic component. |
| `RemoveComponent` | worker -> broker | yes | Remove a dynamic component. |
| `SetComponentAuthority` | admin/kernel worker -> broker | yes | Change owner/mode/epoch of a component. |
| `SetComponentAuthorityResponse` | broker -> admin/kernel worker | no | Authority-change result. |
| `UpdateRejected` | broker -> peer | no | Structured rejection for authority, ACL, WAL, protocol, or rate-limit failures. |

### Authority and handoff

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `AuthorityChange` | broker -> worker/client | no | Grant/revoke authority or warn of imminent loss. |
| `Fold` | worker -> broker | yes | Portal/non-local region transfer request by current owner. |
| `ThresholdTx` | worker -> broker | yes | Prepare/preload/commit/adopt/abort threshold transition. |
| `ThresholdTxResponse` | broker -> worker | no | Threshold transaction response when a request id is supplied. |

### Mesh

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `MeshHandoff` | broker -> broker | yes at source and target | Transfer an entity across broker boundary. |
| `MeshAck` | broker -> broker | yes at source | Confirm cross-broker handoff adoption. |
| `MeshGhost` | broker -> broker | no | Project read-only seam-near entity to neighbor. |
| `MeshGhostRemove` | broker -> broker | no | Retract read-only ghost projection. |

### Queries, commands, events

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `EntityQuery` | peer -> broker | no | Query visible entities. |
| `EntityQueryResponse` | broker -> peer | no | Entity query result. |
| `CommandRequest` | peer -> broker -> owner | no | Route command to current authority holder. |
| `CommandResponse` | owner -> broker -> caller | no | Route command response to original caller. |
| `EntityEvent` | owner -> broker -> interested peers | no | Transient event delivered through interest channel. |
| `FlagUpdate` | peer -> broker -> peers | no | Broadcast runtime flag. |
| `Metrics` | worker -> broker | no | Worker load input for rebalancing. |
| `LogMessage` | peer -> broker | no | Log/dev message accepted by broker and ignored. |

Operation semantics for component updates, authority, handoff, mesh, commands,
and events are defined in `docs/protocol/event-command-semantics.md`. In
short: component writes and authority/handoff controls are durable state;
visibility projections and result frames are transient; `EntityEvent` is
transient interest-delivered signal; and `CommandRequest`/`CommandResponse` is
routed RPC correlated by `request_id`.

Entity lifecycle semantics are defined in
`docs/protocol/entity-lifecycle-semantics.md`. In short: `CreateEntity`,
`DeleteEntity`, and `ReserveEntityIds` are persistent lifecycle operations;
their response frames are transient result frames.

### Inspector/admin

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `InspectorQuery` | inspector -> broker | no | Request full debug/ops state frame. |
| `InspectorFrame` | broker -> inspector | no | Debug/ops state frame. |
| `SnapshotMarker` | admin -> broker | yes | Mark coordinated snapshot/restore boundary. |
| `SnapshotManifest` | broker -> admin | no | Return snapshot cut metadata and schema/version contract. |

## JSON codec coverage

The typed JSON codec now covers the current v1 runtime wire in two layers:

1. Structured typed fields for the stable/loss-sensitive lifecycle and visibility path, including rich `CreateEntity.components` preservation.
2. Lossless operation-specific JSON field bags for response/query/event/admin families whose payloads are still broad runtime JSON and must not be narrowed before SDK work.

Covered operation families:

- connection/liveness: `WorkerConnect`, `Disconnect`, `Heartbeat`, `Health`
- visibility/interest: `Interest`, `CriticalSection`, `AddEntity`, `RemoveEntity`, `ComponentUpdate`
- lifecycle: `CreateEntity`, `CreateEntityResponse`, `DeleteEntity`, `DeleteEntityResponse`, `ReserveEntityIds`, `ReserveEntityIdsResponse`
- components/authority: `AddComponent`, `RemoveComponent`, `UpdateComponent`, `BatchUpdate`, `SetComponentAuthority`, `SetComponentAuthorityResponse`, `AuthorityChange`, `UpdateRejected`
- handoff/mesh: `Fold`, `ThresholdTx`, `ThresholdTxResponse`, `MeshHandoff`, `MeshAck`, `MeshGhost`, `MeshGhostRemove`
- query/commands/events: `EntityQuery`, `EntityQueryResponse`, `CommandRequest`, `CommandResponse`, `EntityEvent`, `FlagUpdate`, `Metrics`, `LogMessage`
- inspector/admin: `InspectorQuery`, `InspectorFrame`, `SnapshotMarker`, `SnapshotManifest`

## Persistent-operation rule

A persistent op must not mutate published world state until the durable transition is written and crossed through the durability barrier.

Required order:

```text
validate -> WAL append -> fsync/durable barrier -> in-memory mutation -> publish
```

## Error model to implement

Future typed protocol responses should classify errors as:

```text
protocol_error
auth_error
authority_error
acl_error
wal_error
rate_limit_error
region_error
mesh_error
not_found
conflict
```

## Typed protocol work items

- [x] Add `godworks-protocol` crate.
- [x] Define initial `Op` enum.
- [x] Add JSON codec for current wire compatibility.
- [x] Add golden roundtrip tests for lifecycle/visibility and response/query/event/admin families.
- [x] Add max frame size constant.
- [x] Complete typed JSON coverage for the current v1 runtime operation families.
- [x] Wire broker frame reader to `DEFAULT_MAX_FRAME_BYTES`.
- [x] Add basic broker ingress frame rate limit.
- [ ] Replace raw JSON construction in `zone_worker` with typed SDK calls.
- [ ] Keep protocol docs synchronized with the typed enum.
