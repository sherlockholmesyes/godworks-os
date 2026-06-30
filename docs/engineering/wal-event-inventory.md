# WAL event inventory

This is the issue #3 starting point: document the WAL/recovery surface before
extracting code into a dedicated module. The invariant is stricter than "write a
log": no live observer should see state, owner, epoch, topology, or identity
allocation that recovery cannot reproduce from the WAL.

## Format and replay path

The broker writes JSONL. A v1 WAL starts with:

```json
{"kind":"wal_header","wal_version":1}
```

Each v1 event line is wrapped as an integrity envelope:

```json
{"_c":1234567890,"_d":"<event-json-string>"}
```

`_c` is a CRC32 over the exact `_d` string. Recovery accepts legacy v0 bare
JSON event lines, but once a v1 header is present every event line must be a
valid envelope. Tail corruption is treated as an interrupted final write and is
truncated from replay. Mid-log corruption or a future `wal_version` makes
startup refuse to serve instead of recovering a partial fork.

Current replay path:

- write barrier helpers: `ServerState::wal_append`,
  `ServerState::wal_append_nosync`, `ServerState::wal_sync`
- encoding/version helpers: `godworks_broker::wal::wal_v1_envelope_line`,
  `godworks_broker::wal::wal_v1_header_line`
- integrity gate: `godworks_broker::wal::decode_wal_line`
- report + restore: `recover_from_wal_report`
- pure replay reducer: `apply_wal_events`
- startup hydration: `main` loads `GW_WAL`, optional `GW_RESTORE_OFFSET`,
  recovered topology, pending mesh handoffs, and entity id high-water mark
- operator inspection: `wal_inspect <path> [--up-to-offset <bytes>]` uses the
  same shared WAL reader as broker recovery and reports kind counts, unknown
  kinds, corruption/refusal status, and corrupt-tail truncation.

## Durability forms

| Form | Use | Durability rule |
| --- | --- | --- |
| Direct append + sync | single lifecycle/admin transitions | `wal_append` must succeed before RAM mutation or response |
| Staged append + group sync | high-rate writes, local handoff, failover, block migration | append `nosync`, stage RAM, `wal_sync` once, then apply canonical RAM and publish |
| Cross-broker seam | `mesh_out` / `mesh_acked` | source writes and fsyncs `mesh_out` before target can adopt; ack is durable before clearing resend state |
| Compaction snapshot | bounded WAL size | rewrite live entities as `register`, preserve tombstones, preserve latest topology |

If any persistent transition cannot cross its durability barrier, the broker
goes fail-closed for persistent ops and does not publish success.

## Event inventory

| WAL kind | Writer path | Replay effect | Notes / gate |
| --- | --- | --- | --- |
| `wal_header` | new or compacted v1 WAL | declares WAL format version | future versions are refused |
| `register` | `CreateEntity`, compaction, remote mesh adopt | insert live entity unless tombstoned | carries `pos`, `vel`, `components`, `region`, version, and optional full authority snapshot |
| `write` | prepared component update | update `pos`, `vel`, or dynamic component | staged group-sync path; replay keeps version |
| `component_add` | `AddComponent` | insert dynamic component and ensure component authority | direct durable-before-publish path |
| `component_remove` | `RemoveComponent` | remove dynamic component | direct durable-before-publish path |
| `delete_tombstone` | `DeleteEntity`, compaction | tombstone id and remove live entity | tombstone survives compaction so deleted ids cannot resurrect |
| `transfer` | local handoff / fold | move entity region, pos/vel, version, and authority | staged handoff under durable watermark |
| `authority_epoch` | compatibility / authority epoch transition | update per-component or physics-island epoch | older/scalar path coexists with full authority snapshots |
| `component_authority` | contact arbitration and `SetComponentAuthority` | set owner, epoch, and mode for one component | kernel/admin durability path; old worker cache must be revoked after success |
| `failover_grant` | lease-expired grant-only failover | update region owner and grant authority snapshots for many entities | staged group-sync; old dead owner is fenced by epoch, not waited on |
| `block_migration` | 2D rebalance block migration | atomically migrate a whole block's grants | whole block is one staged transition, not per-entity |
| `mesh_out` | source cross-broker forward | remove local copy and rebuild pending resend state until acked | source-durable-gen gate prevents neighbour adopting unrecoverable state |
| `mesh_acked` | source after target acknowledgement | clear pending resend state and keep local copy absent | WAL failure keeps pending handoff for resend |
| `partition_config` | partition boundary/split/mesh topology changes and compaction | restore latest routing topology before serving | prevents recovered placement/router disagreement |
| `snapshot_marker` | `SnapshotMarker` admin op | no entity mutation; durable named cut offset | disables compaction for the broker lifetime so `GW_RESTORE_OFFSET` remains byte-valid |
| `reserve_entity_ids` | `ReserveEntityIds` | advance entity id high-water mark | fsync before returning a block so restart never reissues ids |
| `threshold_prepare` | `ThresholdTx phase=prepare` | stage `threshold.tx` component | crash before commit recovers as abort by dropping non-commit threshold state |
| `threshold_preload_ready` | `ThresholdTx phase=preload_ready` | stage `threshold.tx` component | same abort-on-recovery rule as prepare |
| `threshold_commit` | `ThresholdTx phase=commit` | move region, advance authority, keep committed `threshold.tx` marker | commit is the authority-transfer linearization point |
| `threshold_adopt` | `ThresholdTx phase=adopt` | remove `threshold.tx` marker | final phase is idempotent cleanup |
| `threshold_abort` | threshold timeout or `ThresholdTx phase=abort` | remove `threshold.tx` marker | abort must be durable before cleanup is claimed |

## Recovery outputs

`recover_from_wal_report` currently returns:

- recovered entity store
- delete tombstones
- latest partition topology
- unacked `mesh_out` payloads for resend
- reserved entity id high-water mark
- integrity report with WAL version, selected/decoded event counts,
  corrupt-tail counts, unknown-kind inventory, truncated tail bytes, and refuse
  error

That shape is the extraction target for issue #3: the future WAL module should
return the same facts with named types, not leak `serde_json::Value` through the
rest of the broker.

## Existing validation anchors

The current unit/runtime suite covers these recovery classes in `src/main.rs`
and runtime tests:

- WAL failure does not publish create, update, delete, component add/remove,
  threshold commit, snapshot marker, mesh ack, or id reservation success.
- Local handoff and block migration recover with the expected owner/epoch.
- Old-owner stale writes are rejected after recovered handoff.
- `mesh_out` WAL failure does not send/remove; unacked mesh handoffs remain
  pending until durable ack.
- ReserveEntityIds persists the high-water mark.
- WAL compaction preserves delete tombstones and latest topology.
- Zone-worker and reality-loadgen tests run brokers with `GW_WAL`.

## Next extraction seams

1. Move the remaining broker-local WAL replay layer into named types without
   changing the on-disk format.
2. Replace the tuple return from `recover_from_wal_report` with a typed
   recovery report.
3. Keep snapshots as a separate layer; issue #3 should not become a full
   snapshot implementation.

## Gaps to close while extracting

- Recovery still ignores unknown WAL event kinds during broker replay for
  backward-compatible serving, but the shared reader and `wal_inspect` now count
  and flag unknown kinds so operator review can catch drift before a new
  transition stays invisible.
- Keep the runtime fail-closed persistent-op gate aligned with the public
  protocol's persistent operations; `ReserveEntityIds` and `MeshAck` are now
  covered by a regression test.
- The broker-contact `component_authority` path should not mutate canonical RAM
  before its authority WAL record is known durable.
- Add direct tests for dry-run report output and recovered `partition_config`.
- Snapshot restore cuts and recovered pending `mesh_out` handoffs are covered
  by broker replay tests in `src/main.rs`; keep those gates green before
  changing WAL event semantics.
