# Query Constraint AST

`EntityQuery` accepts a small boolean constraint AST. The AST is evaluated against the same visible entity and ghost rows that produce the query response, asset manifest, and schema manifest.

## Atoms

```json
{"type": "all"}
{"type": "sphere", "center": [0.0, 0.0], "radius": 100.0}
{"type": "box", "min": [-10.0, -10.0], "max": [10.0, 10.0]}
{"type": "component", "comp": "physics"}
{"type": "region", "region": "E"}
{"type": "entity", "entity": "entity-id"}
```

The component atom also accepts `component` or `name` instead of `comp`. The entity atom also accepts `entity_id` or `id`.

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

The direct test proves nested `and` / `or` / `not` selection. The cross-broker runtime gate uses the same AST after handoff and requires `qbi_ast_ok=<entities>`, with a decoy entity present to prove a broad query cannot pass.
