# Content Package Resolver

`EntityQueryResponse` includes a `content_manifest` derived from the same visible assets as `asset_manifest`.

The resolver is intentionally not a second database. The source of truth remains the entity rows selected by interest/query:

```text
visible entities -> asset_manifest -> content_manifest
```

## Asset Inputs

Asset refs may optionally carry package metadata:

```json
{
  "asset": {
    "id": "mesh/ship",
    "uri": "res://ships/ship.glb",
    "kind": "mesh",
    "package": "ships/base",
    "hash": "sha256:ship"
  },
  "asset_dependencies": [
    {
      "id": "mat/shared",
      "uri": "res://materials/shared.tres",
      "kind": "material",
      "package": "common/materials",
      "hash": "sha256:shared"
    }
  ]
}
```

Recognized package fields:

```text
package
package_id
bundle
bundle_id
content_package
content_package_id
```

If no package field is present, the resolver falls back to the parent path of `uri` or `id`.

## Query Output

```json
{
  "content_manifest": {
    "version": 1,
    "asset_count": 2,
    "package_count": 2,
    "packages": [
      {
        "id": "common/materials",
        "asset_count": 1,
        "assets": ["mat/shared"],
        "uris": {"mat/shared": "res://materials/shared.tres"},
        "hashes": {"mat/shared": "sha256:shared"}
      },
      {
        "id": "ships/base",
        "asset_count": 1,
        "assets": ["mesh/ship"],
        "uris": {"mesh/ship": "res://ships/ship.glb"},
        "hashes": {"mesh/ship": "sha256:ship"}
      }
    ],
    "entity_packages": {
      "ship-1": ["common/materials", "ships/base"]
    }
  }
}
```

## Gates

```powershell
cargo test entity_query_returns_content_manifest_package_plan_for_visible_assets_only -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
```

The direct test proves visible-only package resolution, package dedupe, hash propagation, and no leakage of non-visible packages. The runtime gate proves the package plan remains available after a cross-broker handoff through `content_manifest_ok=<entities>`.
