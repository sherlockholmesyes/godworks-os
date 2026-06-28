# Component Schema ABI Audit

Date: 2026-06-28

## Ruler

`entity components -> interest projection -> EntityQuery rows -> schema_manifest -> client parse contract`

The schema surface should be derived from the same rows the client can see. A separate schema registry would be useful later, but it would be a second source of truth for this tranche.

## Matrix Verdict

| Product | Verdict | Evidence |
|---|---|---|
| Components x Query | Clean | `schema_manifest` is built only from components on visible query rows. |
| Query x ABI | Clean | Each descriptor includes component name, version, authority modes, and JSON shape hints. |
| Visibility x Non-leakage | Clean | The direct test places `hidden_logic` outside the query radius and proves it is excluded. |
| PhysicsPayload x Schema | Clean | The schema test checks the `physics` fields `pos`, `rot`, `lin`, `ang`, `at_rest`, `gen`, `t_server`, and `sim_time`. |
| Handoff x Runtime | Clean | `reality_loadgen` requires `schema_manifest_ok=<entities>` after cross-broker handoff. |

## Fixed In This Pass

- Added `schema_manifest` to `EntityQueryResponse`.
- Added runtime-derived JSON shape inference for visible components.
- Added direct test `entity_query_returns_schema_manifest_for_visible_components_only`.
- Extended `reality_loadgen` with `GW_REQUIRE_SCHEMA_MANIFEST` and `schema_manifest_ok`.
- Documented the first component/schema ABI contract.

## Verification

```powershell
cargo test entity_query_returns_schema_manifest_for_visible_components_only -- --nocapture
cargo test entity_query_returns_asset_manifest_for_visible_dependencies_only -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
cargo fmt --check
git diff --check
```

Full suite result: 29 unit tests, 5 broker runtime tests, and 3 zone-worker runtime tests passed. Public leak scan for closed-methodology strings, keys, and private reference markers returned no matches.

## Remaining Pressure

This is ABI discovery, not the full content system. Still open:

- write-time schema validation;
- stable numeric component ids and codegen;
- content package resolver;
- client-side package loading proof.
