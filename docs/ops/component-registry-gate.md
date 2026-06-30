# Component Registry Gate

The built-in component registry is the stable identity floor for Godworks v1.
It does not change the current length-prefixed JSON wire: runtime frames still
carry names such as `pos`, `vel`, and game-specific component strings.

The registry exists so long-lived artifacts can agree on component identity:

- protocol structs;
- replay/eval events;
- snapshot manifests;
- SDK helpers;
- future binary codecs.

## Current Contract

The built-in registry is defined in `godworks-core`.

```text
component_registry_version = 1
```

This version is emitted in:

- `SnapshotManifest`;
- broker replay tape events;
- replay/eval fixture gates.

The protocol crate re-exports the registry types and supports
`SnapshotManifest` as a lossless JSON field-bag operation.

## Guardrail

Every broker replay event must carry `component_registry_version`. The
`replay_eval` binary rejects broker events that omit it.

The CI fixture gate includes:

- a valid minimal replay tape with registry version metadata;
- an invalid tape missing `component_registry_version`;
- existing partition-schema invalid tapes kept independent of the registry
  check.

## Verification

Verified locally:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets -- --test-threads=1
cargo build --workspace --release
```

The full test gate includes protocol roundtrip coverage, component registry
table/uniqueness tests, replay_eval negative checks, and replay tape runtime
events containing the registry version.
