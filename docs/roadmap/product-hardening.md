# Product Hardening Roadmap

This roadmap turns Godworks OS from a pre-1.0 spatial backend into a usable
multiplayer world platform. The first product slice is 2D, but the contracts
must keep a 3D rail open from now on.

## Product target

Godworks OS 1.0 targets:

```text
2D distributed-authority multiplayer worlds, with 3D-ready spatial contracts
```

The product is a server-side spatial backend plus SDKs and tooling, not a full renderer/editor game engine.

## Release ladder

### v0.2 — Engineering baseline

- CI and reproducible toolchain. Status: current baseline exists.
- Developer command runner. Status: `just gate` exists when `just` is installed.
- Current-state audit. Status: exists and should be kept current.
- Protocol v1 draft inventory. Status: exists.
- Environment/config inventory. Status: exists.
- Initial workspace root. Status: complete.

### v0.4 — Protocol and SDK alpha

- `godworks-protocol` crate.
- Typed operation enum.
- JSON codec compatibility layer.
- Protocol golden tests.
- `godworks-worker-sdk` alpha.
- `zone_worker` migrated to SDK.

Status: implemented as the current protocol/worker baseline. Keep extending it
with compatibility tests rather than rebuilding a second protocol path.

### v0.6 — Client and demo alpha

- `godworks-client-sdk` alpha.
- Headless client cache.
- Godot bridge v0.
- Top-down arena demo with one broker.
- Two-broker seamless handoff demo.

### v0.8 — Ops and security beta

- Frame size limits.
- Broker ingress cost limits for small-frame floods, expensive ops, and large
  valid JSON payloads.
- Basic WorkerConnect auth.
- Token-bound WorkerConnect region/attribute claims.
- Global OBS visibility gated by observer/debug/inspector claims.
- Broker-side worker/client/observer/mesh role policy v0.
- Broader deployment policy for role credentials and per-principal defaults.
- Mesh auth beyond the current token-claim baseline.
- Broader per-principal rate-limit policy for public hosted deployments.
- Metrics exporter.
- Redacted replay tape for offline eval and policy experiments.
- 3D foundation rail: coordinate/schema docs, future-proof component names,
  partition schemas, and protocol fixtures that do not lock in 2D-only data.
- Stable built-in component registry v1 for current 2D names and future 3D
  component names.
- Agent contribution trace/eval gate.
- Docker Compose.
- WAL inspect/recovery CLI.
- Chaos/recovery tests.

### v1.0 beta — Usable 2D spatial OS

- Documented protocol v1.
- Worker SDK and client SDK usable by game code.
- Godot example project.
- Durable recovery and snapshot path.
- Observability dashboard.
- Deployment guide.
- Security model v0.
- Known limitations documented.

## Non-goals for 1.0

- 3D production support.
- A 3D physics worker or 3D client runtime.
- Managed hosted cloud product.
- Full visual world editor.
- Unity/Unreal official plugins.
- Global multi-region orchestration.

3D production runtime is out of scope for 1.0, but 3D-compatible contracts are
in scope. See `docs/spatial/3d-foundation.md`.

## First practical sequence

1. Protect the repo with CI and a local gate.
2. Document the current system before changing it.
3. Add a workspace root without changing behavior.
4. Extract typed protocol structs — done for the current JSON protocol.
5. Extract core entity/authority types — initial crate exists; keep moving shared
   types out of the broker as needed.
6. Extract WAL/recovery module — shared WAL decoder and inspector exist; broker
   recovery still owns the reducer.
7. Build worker SDK — alpha done.
8. Rewrite `zone_worker` on the SDK — done for the current worker protocol surface.
9. Add the 3D foundation rail: spatial docs, coordinate/schema terms, and
   protocol fixtures that preserve 3D component bags without changing current
   runtime behavior.
10. Add the stable component registry rail: numeric IDs for built-in current
    and future spatial components without changing JSON wire behavior.
11. Build client SDK.
12. Build playable top-down arena.

## Product beta definition

A new developer should be able to run:

```bash
just gate
# later:
godworks dev up topdown-arena
```

and get a working local cluster with:

- broker(s);
- workers;
- client/demo;
- AOI;
- handoff;
- WAL recovery;
- metrics;
- documented SDK calls.
