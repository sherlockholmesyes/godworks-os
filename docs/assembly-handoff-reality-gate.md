# Assembly Handoff Reality Gate

Worlds-Adrift-style objects are not always single entities. A ship, cargo rig,
vehicle, or articulated prop can be a root plus child entities. A broker seam
handoff that moves only the root is a visible tear even if the single-entity
handoff test passes.

## Broker Contract

The broker treats a `parent` component as assembly membership:

- local handoff queues children under the same durable handoff barrier;
- cross-broker handoff derives the root assembly once;
- the source writes one `mesh_out_group` WAL record for the whole departure;
- recovery expands that group into ordinary per-entity `pending_mesh` resends;
- every member is still delivered with the existing `MeshHandoff` / `MeshAck`
  protocol, so the two-process handoff model stays unchanged.

This keeps the source-side durable cut coherent: recovery cannot reproduce only
a prefix of the root-plus-child departure.

## Runtime Gate

`reality_loadgen` now creates one child per crossed body:

```text
rlg-body-N
rlg-body-N-child  parent=rlg-body-N
```

The child does not move itself. It can only reach the E broker if the root's
cross-broker handoff carries the assembly.

The cross-broker integration gate requires:

- `assembly_child_ok == entities`: public `EntityQuery` sees each child in E
  with its parent link and an E-written `assembly_probe`;
- `assembly_probe_rejected == entities`: the stale W owner receives
  `UpdateRejected` when it tries to write the child after adoption.

Run:

```powershell
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture --test-threads=1
```

The full suite also keeps the lower-level source-generation unit test green:

```powershell
cargo test mesh_forward_moves_assembly_members_as_one_source_generation -- --nocapture
```
