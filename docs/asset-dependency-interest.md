# Asset Dependency Interest

`EntityQueryResponse` includes an `asset_manifest` derived from the entity rows that are visible to the requesting worker.

The manifest is a load plan for clients and workers. It keeps rendering/content dependencies on the same interest boundary as entity visibility:

- visible entity dependencies are included;
- non-visible entity dependencies are excluded;
- shared dependencies are deduped;
- no separate persistent asset database is introduced.

## Component Inputs

Asset references are ordinary entity components. The broker recognizes these component keys:

```text
asset
assets
asset_ref
asset_refs
asset_dependency
asset_dependencies
```

Each reference can be a string id, an object with `id` / `asset_id` / `uri` / `path`, or an array of references.

Example:

```json
{
  "asset": {"id": "mesh/ship", "uri": "res://ships/ship.glb", "kind": "mesh"},
  "asset_dependencies": [
    {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material"},
    {"id": "tex/ship", "uri": "res://textures/ship.png", "kind": "texture"}
  ]
}
```

## Query Output

The broker emits:

```json
{
  "asset_manifest": {
    "count": 3,
    "assets": [
      {"id": "mat/shared", "uri": "res://materials/shared.tres", "kind": "material"},
      {"id": "mesh/ship", "uri": "res://ships/ship.glb", "kind": "mesh"},
      {"id": "tex/ship", "uri": "res://textures/ship.png", "kind": "texture"}
    ],
    "entity_assets": {
      "ship-1": ["mat/shared", "mesh/ship", "tex/ship"]
    }
  }
}
```

## Gates

```powershell
cargo test entity_query_returns_asset_manifest_for_visible_dependencies_only -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
```

The first test proves the direct `EntityQuery` contract. The second proves that asset dependencies survive a cross-broker handoff and remain visible through the product runtime gate.
