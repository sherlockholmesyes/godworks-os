# Reality Loadgen Authority Gate

`reality_loadgen` is a runtime gate for broker/worker behavior over the public wire protocol. It is not a raw throughput benchmark and it is stricter than a visibility check.

## What This Gate Proves

In cross-broker mode, the harness now verifies the writer swap after a seam handoff:

1. Bodies are created on the W broker and moved across the seam.
2. The E broker receives authority for each crossed body.
3. The E-side owner writes `handoff_probe`.
4. A public `EntityQuery` sees the E-written value on every crossed entity.
5. The old W-side owner attempts the same write and receives `UpdateRejected`.
6. Every body has a child entity with `parent=<root>`. The child does not self-zone, so it can only cross the process seam if the broker forwards the assembly.
7. The E-side owner writes `assembly_probe` to each child; a public `EntityQuery` sees the child in region `E` with the E-written value.
8. The old W-side owner attempts the child write and receives `UpdateRejected`.
9. The E-side owner writes a full `physics` payload (`pos`, `rot`, `lin`, `ang`, `at_rest`, `gen`, `t_server`, `sim_time`).
10. A public `EntityQuery` sees that payload and verifies broker-normalized monotonic clocks after handoff.
11. The same public `EntityQuery` returns an `asset_manifest` for every visible crossed body, including deduped shared dependencies.
12. The same public `EntityQuery` returns a `content_manifest` package load plan derived from those visible assets.
13. A headless client package loader consumes that public response and resolves every crossed body's package plan into loadable asset URIs and hashes.
14. The same public `EntityQuery` returns a `schema_manifest` for visible components, including the `physics` field shape.
15. A nested `and` / `or` / `not` `EntityQuery` constraint AST selects the crossed bodies by post-handoff region, spatial box, exact entity-id set, component payload value, and excludes an in-radius decoy with the same broad components.
16. The harness reads public `HealthFrame` snapshots from every participating broker after the load window and requires the monitor loop to have ticked and all monitor work queues to have drained.

The final line exposes the gate as:

```text
handoff_probe_ok=<N> handoff_probe_rejected=<N> assembly_child_ok=<N> assembly_probe_rejected=<N> physics_payload_ok=<N> physics_clock_ok=<N> asset_manifest_ok=<N> content_manifest_ok=<N> content_load_ok=<N> schema_manifest_ok=<N> qbi_ast_ok=<N> health_ok=<B> monitor_tick_ok=<B> monitor_queue_ok=<B> health_query_error=<reason|none> health_queue_backlog=<Q>
```

For a passing cross-broker run, all entity values must match `entities`, and the health values must match the broker count (`B=2` in cross-broker mode) with `health_queue_backlog=0`.

## Broker Behavior Covered

The source broker must reject stale writes to an entity that is no longer local after a cross-broker handoff. Silent drops are not enough for a real worker loop because the old writer needs an explicit negative signal.

The broker therefore emits:

```json
{"op":"UpdateRejected","reason":"entity not found or no longer local; write not applied"}
```

when an update targets a missing non-ghost entity.

The receiver must also carry the component bag across the seam and accept a post-adopt `physics` write from the new owner without losing payload fields or allowing `gen`, `sim_time`, or `t_server` to rewind.

Assembly handoff uses the same `parent` component the local handoff path already honors. In cross-broker mode the source broker derives the root's assembly members once, writes one durable `mesh_out_group` record for the whole source-side departure, then parks and sends every member as the existing per-entity `MeshHandoff`. Recovery expands the group record back into per-entity `pending_mesh` entries. This prevents a source recovery from reproducing only a prefix of the root+child departure, while preserving the existing two-process MeshHandoff/MeshAck protocol.

`EntityQueryResponse` derives `asset_manifest` from the visible entity rows. Asset references are ordinary components (`asset`, `assets`, `asset_ref`, `asset_refs`, `asset_dependency`, or `asset_dependencies`); the broker does not maintain a second persistent asset database. This keeps the content load plan tied to interest projection: non-visible entities do not leak dependencies, and shared dependencies are emitted once.

`EntityQueryResponse` derives `content_manifest` from the same `asset_manifest`. It groups assets into packages, carries URIs/hashes when present, and maps visible entities to the packages they require. This gives a client a package load plan without adding a second content state machine.

`reality_loadgen` then uses a headless client resolver against that public response. For each visible crossed body it resolves `entity_packages` to package rows, expands package assets, and requires every entity asset to have a package-carried URI and hash. This proves the manifest is an actionable client load plan, not just a server-side summary.

`EntityQueryResponse` also derives `schema_manifest` from the same visible rows. It exposes `abi_version`, component names, observed authority modes, JSON shape hints, and `entity_components`. It is an ABI discovery surface, not a separate schema registry.

`EntityQuery` supports a constraint AST over the same row source: `and`, `or`, `not`, `sphere`, `box`, `component`, `region`, `entity`, and component value predicates. The runtime gate uses every current atom after cross-broker handoff: it requires E-region membership, an E-side spatial box, an exact entity-id OR-list, `physics.writer == "E"`, `physics.at_rest == false`, and an in-radius decoy with `physics` and `handoff_probe` but `physics.writer != "E"`. A broad, component-only, or value-only query cannot pass.

`HealthFrame` now exposes monitor liveness and work queues from the same broker state the Inspector reads:

- `monitor_ticks` proves the 300ms monitor loop actually ran.
- `queues` contains `pending_updates`, `pending_handoffs`, `pending_failovers`, `pending_block_migrations`, `pending_commands`, `pending_handoff_intents`, `rebalance_jobs`, `event_outbox`, and `pending_mesh`.
- `egress` contains per-worker output backlog totals and drops.

The runtime gate waits for a stable post-load cut, then requires each broker to return `status=ok`, finite tick/lock metrics, `monitor_ticks>0`, and zero monitor queue backlog. The health prober connects as a non-interest control worker (`region=MESH`), so the health response is not hidden behind observer checkout traffic. This makes the monitor tick a product ruler rather than a passive debug endpoint.

## Run

```powershell
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
```

## Still Out Of Scope

This gate does not replace a real engine or legacy-client render proof. It now closes the cross-broker writer-swap, source-side assembly handoff grouping for parent/child entities, product physics payload continuity, first asset dependency interest check, first content package load-plan check, first headless client package-load proof, first component/schema ABI discovery check, full current query-constraint AST runtime check, and first monitor work-queue runtime check.
