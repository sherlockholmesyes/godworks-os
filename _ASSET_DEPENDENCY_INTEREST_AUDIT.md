# Asset Dependency Interest Audit

Date: 2026-06-28

## Ruler

`entity components -> interest projection -> EntityQuery rows -> asset_manifest -> runtime load plan`

The broker should not keep a second asset state machine. Asset dependencies belong to entity components, and the client load plan should be derived from the same visible rows returned by `EntityQuery`.

## Matrix Verdict

| Product | Verdict | Evidence |
|---|---|---|
| Components x Query | Clean | `EntityQueryResponse.asset_manifest` is derived only from rows that pass query matching and visibility. |
| Query x Content | Clean | Recognized component keys are `asset`, `assets`, `asset_ref`, `asset_refs`, `asset_dependency`, and `asset_dependencies`. |
| Content x Deduplication | Clean | Shared dependencies are deduped in the global manifest while `entity_assets` preserves each entity's load list. |
| Visibility x Non-leakage | Clean | The runtime test places a far entity with an asset and proves it is excluded from the visible manifest. |
| Handoff x Runtime | Clean | `reality_loadgen` creates asset-bearing bodies, crosses them to E, then requires `asset_manifest_ok=<entities>`. |

## Fixed In This Pass

- Added `asset_manifest` to `EntityQueryResponse`.
- Added direct runtime test `entity_query_returns_asset_manifest_for_visible_dependencies_only`.
- Extended `reality_loadgen` with asset-bearing bodies and `asset_manifest_ok`.
- Updated the runbook and gate docs.

## Verification

```powershell
cargo test entity_query_returns_asset_manifest_for_visible_dependencies_only -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
```

Current full suite result:

- 29 unit tests passed.
- 4 broker runtime tests passed.
- 3 zone-worker runtime tests passed.

Known unrelated warning: `zone_worker.rs::radius_for` is unused.

## Remaining Pressure

This closes the first asset dependency interest gate. It does not yet define the full component/schema/content ABI, asset package resolver, or real-client asset loading proof.
