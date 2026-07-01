# Component Registry v1

Godworks v1 still uses string component names on the length-prefixed JSON wire.
The registry adds stable numeric component identity without changing that wire
shape.

## Rule

```text
JSON names remain the debug/development codec.
Component IDs become the compatibility anchor.
```

The registry exists so snapshots, replay/eval artifacts, SDKs, and a future
binary codec can identify a component even if a human-facing name changes.

## Version

The current built-in registry version is:

```text
component_registry_version = 1
```

`SnapshotManifest` carries this version next to the spatial/schema metadata.
Broker replay/eval events also carry `component_registry_version`, so offline
validators and future model-plane datasets can reject tapes that have lost the
component identity contract.

## Built-In Components

The first registry version covers the current 2D runtime components and the
future 3D rail names:

| ID | Version | Canonical JSON name | Aliases | Kind |
|---:|---:|---|---|---|
| 10001 | 1 | `pos` | `core.pos2` | spatial |
| 10002 | 1 | `vel` | `core.vel2` | spatial |
| 10003 | 1 | `gen` | `core.gen` | metadata |
| 10004 | 1 | `sim_time` | `core.sim_time` | metadata |
| 10005 | 1 | `kind` | `core.kind` | metadata |
| 10006 | 1 | `parent` | `core.parent` | kernel |
| 10020 | 1 | `rot` | | physics |
| 10021 | 1 | `lin` | | physics |
| 10022 | 1 | `ang` | | physics |
| 10023 | 1 | `at_rest` | `core.at_rest` | physics |
| 11001 | 1 | `core.pos3` | | spatial |
| 11002 | 1 | `core.vel3` | | spatial |
| 11003 | 1 | `core.rot3` | | physics |
| 11004 | 1 | `core.lin3` | | physics |
| 11005 | 1 | `core.ang3` | | physics |
| 11006 | 1 | `core.local_frame` | | spatial |
| 11007 | 1 | `core.physics_body` | | physics |

The `pos` and `vel` names stay canonical for the current JSON wire. Their
`core.pos2` / `core.vel2` aliases give SDKs and future binary codecs a clearer
name without changing identity.

The 3D names are separate components, not aliases of the current 2D fields. The
first executable consumer is
`tests/fixtures/client_bridge/godot-3d-contract.json` plus
`client_probes/godot/godot_3d_contract_probe.gd`, which requires these names in
a real Godot 3D scene contract without changing the current 2D runtime wire
shape.

## Runtime Contract

This PR does not require the broker to reject unknown game-specific components.
The registry is the built-in identity floor. Projects may add game components in
higher layers, but they must not reuse built-in IDs.

Registry validation guarantees:

- no two entries share an ID;
- no canonical name or alias resolves to two IDs;
- legacy and alias names resolve to the same built-in identity where intended.

## Next Step

Later protocol work can attach component IDs to schema manifests, snapshots, and
binary frames. That should extend this registry rather than invent another
component identity layer.
