# Monitor Runtime Health Gate - 2026-06-28

## Ruler

`space -> authority -> durable transition -> interest/query -> runtime ruler -> back to space`

Previous gates proved cross-broker writer swap, physics payload continuity, asset/schema discovery, QBI predicates, and snapshot restore. The remaining synthetic gap was runtime observability: the broker had monitor and health fields, but `reality_loadgen` did not require them to be true during a real handoff/load run.

## Finding

The monitor tick was a passive debug surface. A broken broker could pass the runtime gate as long as entity handoff and queries worked, even if the monitor loop was not ticking or work queues were stuck.

## Fix

- Added `monitor_ticks` to broker state and increment it in the 300ms monitor loop.
- Added shared `monitor_queues_snapshot` and `egress_snapshot` helpers.
- Exposed `monitor_ticks`, `queues`, and `egress` in both `HealthFrame` and `InspectorFrame`.
- Extended `reality_loadgen` to query `HealthFrame` from every participating broker after the load window.
- Added fail conditions:
  - `health_query_failed`
  - `monitor_health_not_ok`
  - `monitor_tick_not_observed`
  - `monitor_queues_not_drained`

## Runtime Test

`cross_broker_reality_loadgen_requires_mesh_adoption` now requires:

```text
health_ok=2 monitor_tick_ok=2 monitor_queue_ok=2 health_queue_backlog=0
```

This fails under the broken forms:

- no `HealthFrame`;
- missing `queues` or `egress`;
- monitor loop never ticks;
- pending monitor queues remain nonzero after the stable post-load cut.

## Verification

```powershell
cargo fmt --check
cargo test cross_broker_reality_loadgen_requires_mesh_adoption -- --nocapture
cargo test -- --test-threads=1
git diff --check
rg -n "<public-leak-patterns>" . -S --glob "!target/**"
```

All passed locally. The leak scan returned no matches.

## Remaining Pressure

This closes the first monitor work-queue reality gate. Remaining product/reality gaps are content package resolver and real legacy-client proof.
