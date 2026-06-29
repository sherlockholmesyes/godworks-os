# Godworks Wire Protocol v1 Draft

This is the draft protocol contract for the current length-prefixed JSON wire.

The short-term goal is to document the existing protocol before replacing ad-hoc JSON construction with typed protocol structs. The long-term goal is to keep JSON as the debug/development codec and add a binary production codec behind the same operation model.

## Framing

Current frame format:

```text
4-byte big-endian unsigned length
JSON body of exactly that length
```

Immediate hardening requirement:

```text
A broker must reject oversized frames before allocating the full body.
```

## Version negotiation

Workers should connect with:

```json
{"op":"WorkerConnect","worker_id":"worker-1","region":"W","proto":1}
```

The broker must reject peers outside its supported protocol range and return a structured protocol rejection.

## Operation groups

### Connection and liveness

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `WorkerConnect` | peer -> broker | no | Register a worker/client/mesh connection. |
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
| `DeleteEntity` | worker -> broker | yes | Tombstone/delete an entity. |
| `ReserveEntityIds` | worker -> broker | yes | Reserve monotonic entity id range. |

### Component writes

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `UpdateComponent` | worker/client -> broker | yes | Update one component on one entity. |
| `BatchUpdate` | worker/client -> broker | yes | Update one component across many entities. |
| `AddComponent` | worker -> broker | yes | Add a dynamic component. |
| `RemoveComponent` | worker -> broker | yes | Remove a dynamic component. |
| `SetComponentAuthority` | admin/kernel worker -> broker | yes | Change owner/mode/epoch of a component. |
| `UpdateRejected` | broker -> peer | no | Structured rejection for authority, ACL, WAL, protocol, or rate-limit failures. |

### Authority and handoff

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `AuthorityChange` | broker -> worker/client | no | Grant/revoke authority or warn of imminent loss. |
| `Fold` | worker -> broker | yes | Portal/non-local region transfer request by current owner. |
| `ThresholdTx` | worker -> broker | yes | Prepare/preload/commit/adopt/abort threshold transition. |

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

### Inspector/admin

| Op | Direction | Persistent | Purpose |
|---|---|---:|---|
| `InspectorQuery` | inspector -> broker | no | Request full debug/ops state frame. |
| `InspectorFrame` | broker -> inspector | no | Debug/ops state frame. |
| `SnapshotMarker` | admin -> broker | yes | Mark coordinated snapshot/restore boundary. |

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

- [ ] Add `godworks-protocol` crate.
- [ ] Define `Op` enum with serde support.
- [ ] Add JSON codec for current wire compatibility.
- [ ] Add golden roundtrip tests.
- [ ] Add max frame size constant.
- [ ] Replace raw JSON construction in `zone_worker` with typed SDK calls.
- [ ] Keep protocol docs synchronized with the typed enum.
