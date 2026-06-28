# Query Constraint AST Audit

Date: 2026-06-28

## Ruler

`query AST -> one matcher -> entity/ghost rows -> manifests/runtime gate`

The query evaluator should be one recursive matcher shared by local entities and cross-broker ghost rows. A separate path for ghosts would make interest projection diverge at seams.

## Matrix Verdict

| Product | Verdict | Evidence |
|---|---|---|
| Boolean AST x EntityQuery | Clean | `and`, `or`, and `not` compose the existing atoms instead of bypassing them. |
| Value Predicate x Component Bag | Clean | Value paths start at the same component map that is emitted in query rows and manifests. |
| Entity rows x Ghost rows | Clean | Both call the same `matches_query_parts` function; only the row source differs. |
| Identity x Query | Clean | The matcher receives the entity id from the map key, which is the source of truth. |
| Runtime x Query | Clean | `reality_loadgen` queries with a nested AST after cross-broker handoff. |
| Broad-query failure | Clean | The runtime gate includes an in-radius decoy with the same broad components but the wrong `physics.writer`; a broad or component-only query cannot satisfy `qbi_ast_ok`. |

## Fixed In This Pass

- Added recursive boolean AST support for `EntityQuery`.
- Added `entity` / `entity_id` atom support.
- Added component value predicates with JSON equality/inequality and numeric comparisons.
- Unified entity and ghost query matching through one matcher.
- Added direct test `entity_query_supports_qbi_boolean_constraint_ast`.
- Extended `reality_loadgen` with `GW_REQUIRE_QBI_AST` and `qbi_ast_ok`.
- Documented the public query constraint contract.

## Verification

```powershell
cargo test entity_query_supports_qbi_boolean_constraint_ast -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
cargo fmt --check
git diff --check
```

Full suite result: 29 unit tests, 6 broker runtime tests, and 3 zone-worker runtime tests passed. Public leak scan for closed-methodology strings, keys, and private reference markers returned no matches.

## Remaining Pressure

This is a boolean AST plus first component-value predicates over the current atom set. Still open:

- richer operators such as string prefix, set membership, and array contains;
- cost limits beyond the current depth cap and frame cap;
- indexed query planning instead of scan evaluation;
- client SDK builders for safe query construction.
