# Godworks Worker SDK MVP

This crate is a narrow Rust SDK for Godworks workers over the current v1 wire.

The SDK does not change broker runtime behavior. It wraps the existing length-prefixed JSON protocol with typed helpers from `godworks-protocol`, while preserving the full typed `Op` for every received frame.

## What this MVP supports

- Register a worker with `WorkerConnect`.
- Send `Interest` / AOI updates.
- Receive and classify checkout, entity, component, event, authority, rejection, command, and mesh handoff frames.
- Send `UpdateComponent` and `BatchUpdate` frames.
- Preserve raw/lossless fields through the underlying typed protocol model for broad or evolving frame shapes.

## Minimal worker shape

```rust
use godworks_core::Position2;
use godworks_worker_sdk::{WorkerConfig, WorkerSession};
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

while let Some(frame) = worker.recv_frame().await? {
    if let Some(authority) = frame.authority_change() {
        if !authority.authoritative {
            // Stop writing this component, flush local state, or prepare handoff.
            continue;
        }
    }

    if let Some(entity) = frame.add_entity() {
        // Components are preserved from the protocol boundary.
        let entity_id = entity.entity.as_ref();
        let _ = entity_id;
    }

    if let Some(component) = frame.component_update() {
        // Current broad component update payload is preserved in component.fields.
        let _ = component.fields.fields.get("value");
    }

    worker
        .update_component("ship-1", "pos", json!([10.0, 20.0]), Some(7))
        .await?;
}
# Ok(())
# }
```

## Drift guard

This SDK is intentionally not a full game framework. The goal is to make the current spatial backend usable by workers without fossilizing raw JSON gaps. Worker SDK comes after the lossless protocol boundary and before Godot/client work.
