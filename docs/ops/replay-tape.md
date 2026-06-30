# Replay Tape v0

`GW_REPLAY_TAPE=<path>` enables an optional JSONL observer for broker protocol
decisions. It is disabled by default and does not change broker authority,
handoff, WAL, role policy, or worker behavior.

The tape is a foundation for offline replay, evaluation, load analysis, and
future policy experiments. It is not an inference path and it is not a second
control plane.

## Events

The v0 tape records small redacted facts:

- `broker_connect` — WorkerConnect outcome, requested region, role,
  credential presence, protocol version, and attribute count.
- `broker_ingress` — peer, role, region, operation summary, wire size,
  durable generation, and gate outcome.
- `broker_outbound` — selected decision frames such as `UpdateRejected` and
  `AuthorityChange`.
- `broker_handoff` — local, mesh-out, or mesh-in handoff breadcrumbs with
  entity id, regions, authority epoch, durable generation, and lease epoch when
  present.

## Redaction

The tape must not become a secret or gameplay-payload log. It records sizes and
counts for large fields, not bodies.

The writer strips these keys recursively before a line reaches disk:

```text
auth_token
value
payload
components
updates
```

Callsites should still emit only summaries. The writer-side sanitizer is the
last line of defense against accidental raw-frame logging.

## Backpressure

The tape uses a bounded background writer. If the tape cannot keep up, broker
runtime continues and tape lines are dropped instead of blocking simulation.

`GW_REPLAY_TAPE_CAPACITY=<n>` sets the in-memory line buffer. The default is
8192.

## Current Limits

The v0 tape is not a full state replay engine. WAL remains the source of truth
for durable state recovery. The tape captures decision breadcrumbs for offline
analysis and regression gates.
