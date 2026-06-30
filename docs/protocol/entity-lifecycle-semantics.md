# Entity Lifecycle Semantics

Godworks v1 keeps entity lifecycle operations separate from component state and
transient result frames.

## Lifecycle Requests

`CreateEntity`, `DeleteEntity`, and `ReserveEntityIds` are persistent
operations. They mutate the durable identity/lifecycle layer and must not be
treated as ordinary transient RPC.

Rules:

- `CreateEntity` registers a durable entity before a success response can be
  relied on.
- `DeleteEntity` writes a durable tombstone so a later replay or restart does
  not resurrect the entity.
- `ReserveEntityIds` advances the durable entity-id high-water mark before
  returning a block, so a restart cannot reissue the same ids.
- lifecycle requests are part of the replay/recovery contract.

## Lifecycle Responses

`CreateEntityResponse`, `DeleteEntityResponse`, and
`ReserveEntityIdsResponse` are transient result frames.

Rules:

- responses are correlated by `request_id` when present;
- responses are not durable entity state;
- a missing response after reconnect does not imply the request did not commit;
- callers must check the durable world/replay state when recovery ambiguity
  matters.

`DeleteEntityResponse.idempotent=true` means the target was already tombstoned
or the delete had already completed. It does not authorize recreating the same
entity id.

## Canonical Operation Semantics

The public protocol crate owns the current operation-semantics table for this
rail:

```text
CreateEntity              persistent  entity_lifecycle   -> CreateEntityResponse
DeleteEntity              persistent  entity_lifecycle   -> DeleteEntityResponse
ReserveEntityIds          persistent  entity_lifecycle   -> ReserveEntityIdsResponse
CreateEntityResponse      transient   lifecycle_response
DeleteEntityResponse      transient   lifecycle_response
ReserveEntityIdsResponse  transient   lifecycle_response
```

Replay tapes may include these semantic tags inside `op_summary`. `replay_eval`
rejects known operations when supplied tags contradict the protocol table.

## Non-Goals In This Rail

This rail does not add:

- a new spawn scheduler;
- client-side spawn prediction;
- editor placement tools;
- snapshot import/export tooling;
- binary protocol frames;
- a gameplay inventory/ownership system.

Those can be built later on top of this lifecycle contract without changing the
current wire shape.
