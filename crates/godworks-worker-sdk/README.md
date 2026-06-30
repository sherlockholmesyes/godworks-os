# Godworks Worker SDK MVP

This crate is a narrow Rust SDK for Godworks workers over the current v1 wire.

The SDK does not change broker runtime behavior. It wraps the existing length-prefixed JSON protocol with typed helpers from `godworks-protocol`, while preserving the full typed `Op` for every received frame.

## What this MVP supports

- Register a worker with `WorkerConnect`.
- Forward an optional connect `auth_token` when the broker is configured with
  `GW_AUTH_TOKEN`.
- Send `Interest` / AOI updates.
- Create entities with typed SDK helpers while preserving component payloads such as `pos`, `vel`, and `mass`.
- Receive and classify checkout, entity, component, event, authority, rejection, command, and mesh handoff frames.
- Send lifecycle/component ops: `CreateEntity`, `DeleteEntity`, `ReserveEntityIds`,
  `AddComponent`, `RemoveComponent`, `UpdateComponent`, and `BatchUpdate`.
- Send query/command/event ops without hand-writing the operation wrapper:
  `EntityQuery`, `CommandResponse`, and `EntityEvent`.
- Preserve raw/lossless fields through the underlying typed protocol model for broad or evolving frame shapes.

## Minimal worker shape

```rust
use godworks_core::Position2;
use godworks_worker_sdk::{
    batch_entry, create_entity_op, disconnect_op, WorkerConfig, WorkerFrameKind, WorkerSession,
};
use serde_json::json;

# async fn example<S>(stream: S) -> godworks_worker_sdk::Result<()>
# where
#     S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
# {
let config = WorkerConfig::new("physics-W", "W").with_attribute("physics");
let mut worker = WorkerSession::connect(stream, config).await?;

worker
    .set_circle_interest(Position2::new(0.0, 0.0), 256.0, Some(128.0))
    .await?;

worker
    .send_op(&create_entity_op(
        "ship-1",
        "W",
        json!({
            "pos": [10.0, 20.0],
            "vel": [1.0, 0.0],
            "mass": 12.5
        }),
    ))
    .await?;

worker
    .batch_update("pos", vec![batch_entry("ship-1", json!([11.0, 20.5]), Some(8))])
    .await?;
worker
    .batch_update("vel", vec![batch_entry("ship-1", json!([1.0, 0.25]), Some(8))])
    .await?;

while let Some(frame) = worker.recv_frame().await? {
    let raw_runtime_frame = frame.op();

    if let Some(authority) = frame.authority_change() {
        if !authority.authoritative {
            let target_region = authority.fields.fields.get("handoff_target_region");
            let epoch = authority.authority_epoch;

            // Stop writing this component, flush local state, or prepare handoff.
            let _ = (target_region, epoch, raw_runtime_frame);
            continue;
        }
    }

    if let Some(rejected) = frame.update_rejected() {
        let owner_region = rejected.fields.fields.get("owner_region");
        let reason = rejected.reason.as_str();

        // Retry/rebase policy belongs in the worker. The SDK preserves the
        // rejection metadata that policy needs.
        let _ = (owner_region, reason, raw_runtime_frame);
    }

    if frame.kind() == WorkerFrameKind::Other {
        // Keep the full typed Op available for runtime frames the SDK has not
        // promoted into a convenience accessor yet.
        let _ = raw_runtime_frame;
    }
}

worker.send_op(&disconnect_op()).await?;
# Ok(())
# }
```

SDK convenience must not drop raw/runtime metadata. `WorkerFrame` always keeps
the full typed `Op`, and broad runtime fields remain available through `fields`
bags such as `AuthorityChange.fields` and `UpdateRejected.fields`.

## Drift guard

This SDK is intentionally not a full game framework. The goal is to make the current spatial backend usable by workers without fossilizing raw JSON gaps. Worker SDK comes after the lossless protocol boundary and before Godot/client work.
