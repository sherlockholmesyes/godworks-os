# Monitor Runtime Health Gate

`reality_loadgen` now treats broker health as part of the product runtime gate, not as a passive debug endpoint.

## Ruler

The broker is only healthy after a real load window if:

- the monitor loop ticked (`monitor_ticks > 0`);
- tick and lock metrics are finite and under the configured thresholds;
- all monitor work queues are visible;
- the stable post-load cut has zero queue backlog;
- egress backlog and drops are visible for slow-consumer diagnosis.

## Wire Surface

The existing `Health` op returns `HealthFrame` over the same length-prefixed JSON worker protocol as every other broker operation. No HTTP side channel is required.

`reality_loadgen` connects health probes as `region=MESH` control workers with a `health` attribute. They are not observers and do not receive interest checkout traffic before the `HealthFrame`; otherwise a large post-handoff checkout burst can hide the liveness answer behind unrelated `AddEntity` / `ComponentUpdate` frames.

`HealthFrame` includes:

- `monitor_ticks`
- `tick_lag_ms`
- `lock_max_hold_ms`
- `queues.pending_updates`
- `queues.pending_handoffs`
- `queues.pending_failovers`
- `queues.pending_block_migrations`
- `queues.pending_commands`
- `queues.pending_handoff_intents`
- `queues.rebalance_jobs`
- `queues.event_outbox`
- `queues.pending_mesh`
- `egress.out_queue_total`
- `egress.out_queue_max`
- `egress.dropped_total`
- `egress.slow_workers`

The Inspector frame embeds the same `queues` and `egress` objects, both derived by the same helper functions as `HealthFrame`.

## Runtime Gate

In cross-broker mode, `reality_loadgen` queries every participating broker after the handoff/load window. A pass requires:

```text
health_ok=2 monitor_tick_ok=2 monitor_queue_ok=2 health_queue_backlog=0
```

`GW_REQUIRE_MONITOR_HEALTH=0` can disable the assertion for compatibility probes, but the default is enabled. Thresholds are configurable with:

```text
GW_MAX_TICK_LAG_MS
GW_MAX_LOCK_HOLD_MS
```

The integration test `cross_broker_reality_loadgen_requires_mesh_adoption` keeps this gate on.
