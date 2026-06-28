# Content Package Resolver Gate - 2026-06-28

## Ruler

`visible entities -> asset manifest -> package load plan`

The asset manifest already exposed visible content dependencies. The remaining product gap was that a client still had to infer package boundaries by itself. This patch adds a package load plan without adding a separate content database.

## Fix

- Added `content_manifest` to `EntityQueryResponse`.
- The manifest is derived from the already-visible `asset_manifest`.
- Assets are grouped by `package`, `package_id`, `bundle`, `bundle_id`, `content_package`, or `content_package_id`.
- Assets without explicit package metadata fall back to the parent path of `uri` or `id`.
- Package rows carry `assets`, `uris`, and `hashes`.
- `entity_packages` maps visible entities to required package ids.

## Runtime Test

Direct query gate:

```powershell
cargo test entity_query_returns_content_manifest_package_plan_for_visible_assets_only -- --nocapture
```

Cross-broker runtime gate:

```powershell
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
```

`reality_loadgen` now requires `content_manifest_ok=<entities>` when content manifests are enabled.

Full verification:

```powershell
cargo fmt --check
cargo test -- --test-threads=1
git diff --check
rg -n "<public-leak-patterns>" . -S --glob "!target/**"
```

All passed locally. Full suite result: 29 unit + 7 broker runtime + 3 zone-worker runtime. The public leak scan returned no matches.

## Remaining Pressure

This closes the first package load-plan resolver. A real client package loading proof remains separate.
