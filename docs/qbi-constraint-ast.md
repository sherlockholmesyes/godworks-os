# Query Constraint AST

`EntityQuery` accepts a small boolean constraint AST. The AST is evaluated against the same visible entity and ghost rows that produce the query response, asset manifest, and schema manifest.

## Atoms

```json
{"type": "all"}
{"type": "sphere", "center": [0.0, 0.0], "radius": 100.0}
{"type": "box", "min": [-10.0, -10.0], "max": [10.0, 10.0]}
{"type": "component", "comp": "physics"}
{"type": "value", "path": ["physics", "writer"], "eq": "E"}
{"type": "field", "path": "ore_resource.tier", "gte": 5}
{"type": "region", "region": "E"}
{"type": "entity", "entity": "entity-id"}
```

The component atom also accepts `component` or `name` instead of `comp`. The entity atom also accepts `entity_id` or `id`.

The value atom also accepts `field` as an alias for `value`. Paths may be arrays (`["physics", "writer"]`) or dot strings (`"physics.writer"`). A value path starts at the entity component bag, so the first segment is normally the component name. Instead of `path`, callers may pass `component` plus `field`.

Supported value comparators:

```json
{"type": "value", "path": "asset.kind", "eq": "mesh"}
{"type": "value", "path": "ore_resource.tier", "ne": 1}
{"type": "value", "path": "physics.gen", "gt": 10}
{"type": "value", "path": "physics.gen", "gte": 10}
{"type": "value", "path": "physics.gen", "lt": 20}
{"type": "value", "path": "physics.gen", "lte": 20}
```

`equals` aliases `eq`; `not_equals` aliases `ne`. Numeric comparators require both sides to be JSON numbers.

## Boolean Nodes

```json
{
  "type": "and",
  "constraints": [
    {"type": "sphere", "center": [0.0, 0.0], "radius": 100.0},
    {"type": "component", "comp": "physics"}
  ]
}
```

```json
{
  "type": "or",
  "constraints": [
    {"type": "component", "comp": "ore_resource"},
    {"type": "entity", "entity": "known-id"}
  ]
}
```

```json
{
  "type": "not",
  "constraint": {"type": "component", "comp": "server_only"}
}
```

`and` also accepts `all_of`; `or` also accepts `any_of`; `not` also accepts `query`.

## Runtime Gate

```powershell
cargo test entity_query_supports_qbi_boolean_constraint_ast -- --nocapture
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
```

The direct test proves nested `and` / `or` / `not` selection plus component-payload value predicates. The cross-broker runtime gate uses the same AST after handoff and requires `qbi_ast_ok=<entities>`, with a decoy entity present to prove a broad or component-only query cannot pass.
