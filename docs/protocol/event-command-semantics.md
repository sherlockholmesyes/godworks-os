# Event and Command Semantics

Godworks v1 distinguishes durable state, transient events, and routed commands.
They may all use the same length-prefixed JSON frame transport, but they do not
have the same persistence or replay meaning.

The public protocol crate owns the current operation-semantics table for this
rail. Replay tapes may include these semantic tags inside `op_summary`, and
`replay_eval` rejects known operations when supplied tags contradict the table:

```text
CommandRequest   transient  command_rpc  -> CommandResponse
CommandResponse  transient  command_rpc
EntityEvent      transient  entity_event
```

## Component State

`UpdateComponent`, `BatchUpdate`, `AddComponent`, and `RemoveComponent` mutate
entity state. They are persistent operations and must pass authority, role,
WAL, and recovery gates before observers rely on the result.

`ComponentUpdate` is the transient outbound projection of component state to a
peer's view. It is not the durable mutation itself.

Canonical component-state semantics:

```text
AddComponent      persistent  entity_update
RemoveComponent   persistent  entity_update
UpdateComponent   persistent  entity_update
BatchUpdate       persistent  entity_update
ComponentUpdate   transient   entity_update
```

## Authority, Handoff, Transactions, and Mesh

The same operation-semantics table also covers authority and handoff rails:

```text
SetComponentAuthority          persistent  authority_control  -> SetComponentAuthorityResponse
SetComponentAuthorityResponse  transient   authority_response
AuthorityChange                transient   authority_event
Fold                           persistent  handoff_control
ThresholdTx                    persistent  transaction_control -> ThresholdTxResponse
ThresholdTxResponse            transient   transaction_response
SnapshotMarker                 persistent  durability_control
MeshHandoff                    persistent  mesh_handoff        -> MeshAck
MeshAck                        persistent  mesh_handoff
MeshGhost                      transient   interest_projection
MeshGhostRemove                transient   interest_projection
```

`MeshGhost` and `MeshGhostRemove` are visibility projections. They must not be
treated as authority transfer or durable state.

## Entity Events

`EntityEvent` is transient.

Rules:

- events ride the interest/visibility channel;
- events are not component state;
- events are not stored in entity components;
- late joiners do not replay old one-shot events;
- the current authoritative owner of the entity is the sender that may emit the
  event;
- event payloads can carry `sim_time` and `gen` so clients can order rendering
  against the state stream;
- event `class` defaults to `critical` when omitted;
- non-critical events may use `coalesce_key` and `count` for storm-bounded
  delivery.

The protocol crate exposes semantic accessors for the current wire names:

```text
entity
event
payload
sim_time
gen
class
coalesce_key
count
```

The accessors do not narrow the JSON payload. They only make the current
semantic contract explicit for SDKs and tests.

## Commands

`CommandRequest` / `CommandResponse` is routed RPC over the entity authority
model.

Rules:

- `CommandRequest` targets an entity;
- the broker routes it to the current authority holder for that entity;
- `request_id` is the correlation key used to route the response back to the
  original caller;
- the broker records the routed entity, owner, authority component, and
  authority epoch for each pending command; a `CommandResponse` with the same
  `request_id` is forwarded only when it comes from that routed owner, names the
  same entity, and the authority has not moved to a different owner/epoch;
- forwarded `CommandResponse` frames include broker-written `routed_owner`,
  `authority_comp`, and `authority_epoch` so live gates and SDKs can verify the
  routed authority proof without racing a separate observer view;
- `caller` is broker-written when forwarding the request to an authority holder;
- `CommandResponse` is transient and not WAL-backed state;
- omitted `CommandResponse.success` means success by current broker convention;
- responders should echo `entity` at top level; the broker also accepts the
  legacy `payload.entity` echo while examples and SDKs converge on the explicit
  field;
- `idempotency_key` and `timeout_ms` are protocol-level semantic fields for SDK
  and future policy work, even though the current broker does not enforce full
  retry/timeout semantics yet.

Semantic accessors in `godworks-protocol` cover:

```text
CommandRequest:
  request_id
  entity
  command
  payload
  caller
  idempotency_key
  timeout_ms

CommandResponse:
  request_id
  entity
  routed_owner
  authority_comp
  authority_epoch
  success
  success_or_default
  reason
  payload
```

## Non-Goals In This Rail

This rail does not add:

- a new runtime command scheduler;
- command timeout enforcement;
- command retry storage;
- binary protocol frames;
- client SDK prediction or reconciliation;
- persistence for transient events.

Those can be built later on top of this semantic contract without changing the
current wire shape.
