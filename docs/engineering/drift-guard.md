# Drift Guard

This document keeps the hardening work pointed at the product target instead of letting the project sprawl.

## Product vector

Godworks OS 1.0 is a 2D distributed-authority multiplayer backend with SDKs, docs, tests, recovery, observability, and a playable proof demo.

It is not trying to become all of these at once:

- a 3D runtime;
- a renderer/editor game engine;
- a managed cloud platform;
- a general-purpose database;
- a new physics engine;
- a pile of demos without SDK/product surface.

## Wrong × Wrong = Third Wrong

Use this as a hardening rule:

```text
wrong assumption × wrong implementation = third wrong system
```

Examples:

- If the protocol is undocumented and the SDK is built directly on raw JSON, the result is not a product SDK; it is protocol drift with nicer names.
- If the current 2D model is treated as secretly 3D-ready, the result is not 3D support; it is hidden coordinate and AOI bugs.
- If broker behavior is refactored before tests pin the current semantics, the result is not cleanup; it is untraceable behavior drift.
- If ops/security are delayed until after demos, the result is not a game platform; it is a local toy that cannot safely host players.

## Per-PR drift check

Every hardening PR should answer:

1. Does this move toward the 2D spatial backend product target?
2. Does it preserve current runtime behavior unless the PR explicitly documents a behavior change?
3. Does it reduce raw JSON, hidden state, undocumented config, or untested recovery paths?
4. Does it avoid starting 3D, cloud, or editor work before the SDK/protocol/ops foundation is stable?
5. Does it add tests/docs close to the code it changes?

If the answer is unclear, split the PR smaller.

## Current priority order

1. CI and reproducible gates.
2. Protocol documentation and typed codec.
3. Core type extraction.
4. WAL/recovery extraction.
5. Worker SDK.
6. Client SDK.
7. Godot bridge.
8. Top-down arena proof game.
9. Security and ops hardening.
10. 3D feasibility only after 2D product surface is stable.
