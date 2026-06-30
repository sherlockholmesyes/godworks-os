# Godworks OS Security Threat Model v0

This document captures the current broker security boundary for private-alpha
and open-source review. It is not a hosted-production hardening claim.

## Assets

- Authoritative entity state, component authority, authority epochs, and WAL
  durability.
- Region ownership and mesh handoff integrity.
- Broker memory, CPU, and egress capacity.
- Worker/client visibility boundaries.

## Trust Boundary

The public TCP wire is not trusted. A peer may send valid JSON, malformed JSON,
oversized frames, excessive valid frames, mismatched region claims, forged
attributes, broad interest requests, expensive queries, or mesh/control frames.

The broker is authoritative. Workers may simulate or render, but broker-side
authority, WAL, and epoch fences define the recoverable truth.

## Current Controls

- Frame size: frames larger than `DEFAULT_MAX_FRAME_BYTES` are rejected before
  allocating the body.
- Ingress cost budget: every registered peer has a token bucket. The cost is
  derived from op class and received wire body length, and is charged before
  dispatch. Rejected ops do not mutate RAM or WAL.
- WorkerConnect auth: `GW_AUTH_TOKEN` provides a legacy shared-token gate.
- Token-bound claims: `GW_AUTH_CLAIMS` maps tokens to broker-owned
  region/attribute claims. Peers cannot self-assign a different region or
  privileged attributes in strict mode.
- Peer role policy: the broker derives an internal role (`worker`, `client`,
  `observer`, or `mesh`) from broker-owned claims / legacy regions and rejects
  privileged op families before dispatch. Clients cannot lease worker regions,
  observers cannot write entity state, and mesh links can only use mesh-family
  traffic plus liveness.
- Global observer visibility: an `OBS` peer only gets whole-world visibility
  when its token claim grants `observer`, `debug`, or `inspector`.
- Mesh links: mesh peers can use a `MESH` claim token; lease epochs still fence
  stale broker incarnations.
- Egress bound: every peer has a bounded output channel. Degradable frames are
  shed before a stuck peer can grow unbounded memory; critical overflow causes
  disconnect and reconnect/recheckout.
- WAL fail-closed: persistent ops are rejected while the WAL is degraded.

## Current Non-Goals

- No public hosted multi-tenant security promise yet.
- No TLS/mTLS or external identity provider in this repository.
- No DDoS-grade perimeter controls; deployers must put the broker behind a
  network boundary until those controls exist.
- No final binary production protocol. Length-prefixed JSON remains the debug
  and alpha protocol.

## Remaining Work

- Add per-principal deployment policy: token rotation, token storage guidance,
  and recommended defaults for private alpha versus public testnets.
- Strengthen mesh auth beyond the first `MESH` claim token baseline.
- Add hosted-mode network guidance: TLS termination, allowlists, rate limits at
  the edge, and telemetry for rejected peers.
- Add compatibility tests for future binary codec limits so the security
  contract does not depend on JSON-only behavior.
