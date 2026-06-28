# Component Schema Content ABI

`EntityQueryResponse` includes a `schema_manifest` derived from the visible component rows in the query result.

This is the first ABI discovery surface for clients and workers. It does not validate writes yet and it does not replace a future package resolver. It gives a reader enough machine-readable shape information to parse the components it just received.

## Query Output

```json
{
  "schema_manifest": {
    "abi_version": 1,
    "component_count": 2,
    "components": [
      {
        "name": "physics",
        "version": 1,
        "authority_modes": ["server_physics_island"],
        "schemas": [
          {
            "type": "object",
            "fields": {
              "pos": {"type": "array", "len": 3, "items": [{"type": "number"}]},
              "rot": {"type": "array", "len": 4, "items": [{"type": "number"}]},
              "at_rest": {"type": "bool"}
            }
          }
        ]
      }
    ],
    "entity_components": {
      "body-1": ["physics"]
    }
  }
}
```

## Rules

- The manifest is derived from visible `EntityQuery` rows only.
- Non-visible entity components are excluded.
- Component descriptors are sorted by name.
- Each descriptor exposes a stable `version` field; current runtime-derived descriptors use version `1`.
- `authority_modes` is the set of observed authority modes for that component among the visible rows.
- `schemas` contains JSON shape hints derived from component values.

## Gates

```powershell
cargo test entity_query_returns_schema_manifest_for_visible_components_only -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
```

The direct test proves visible-only ABI discovery and checks the `physics` payload fields. The cross-broker runtime gate proves that the same ABI surface remains available after handoff through `schema_manifest_ok=<entities>`.
