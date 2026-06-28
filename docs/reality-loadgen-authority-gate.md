# Reality Loadgen Authority Gate

`reality_loadgen` is a runtime gate for broker/worker behavior over the public wire protocol. It is not a raw throughput benchmark and it is stricter than a visibility check.

## What This Gate Proves

In cross-broker mode, the harness now verifies the writer swap after a seam handoff:

1. Bodies are created on the W broker and moved across the seam.
2. The E broker receives authority for each crossed body.
3. The E-side owner writes `handoff_probe`.
4. A public `EntityQuery` sees the E-written value on every crossed entity.
5. The old W-side owner attempts the same write and receives `UpdateRejected`.

The final line exposes the gate as:

```text
handoff_probe_ok=<N> handoff_probe_rejected=<N>
```

For a passing cross-broker run, both values must match `entities`.

## Broker Behavior Covered

The source broker must reject stale writes to an entity that is no longer local after a cross-broker handoff. Silent drops are not enough for a real worker loop because the old writer needs an explicit negative signal.

The broker therefore emits:

```json
{"op":"UpdateRejected","reason":"entity not found or no longer local; write not applied"}
```

when an update targets a missing non-ghost entity.

## Run

```powershell
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
```

## Still Out Of Scope

This gate does not replace the later product gates for full physics-island payloads, component/schema/content ABI, asset dependency interest, monitor work queues, snapshot artifact export, or a real Worlds Adrift client proof. It only closes the cross-broker writer-swap reality check.
