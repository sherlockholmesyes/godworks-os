# Worker SDK MVP

This product-hardening slice adds a small Rust Worker SDK over the typed v1 protocol boundary.

## Scope

Included:

- worker registration through `WorkerConnect`;
- AOI / `Interest` helpers;
- length-prefixed JSON frame read/write helpers;
- typed send helpers for `UpdateComponent` and `BatchUpdate`;
- receive-side frame classification for checkout, entity visibility, component updates, events, authority changes, rejections, command requests, and mesh handoffs;
- lossless preservation of the underlying typed `Op` for broad protocol frames.

Excluded:

- broker runtime changes;
- Godot/client SDK work;
- 3D support;
- managed cloud/control-plane behavior;
- full game framework abstractions.

## Design rule

The SDK wraps the protocol boundary. It must not reintroduce ad-hoc raw JSON as the user-facing API, and it must not drop fields from broad runtime frames.

```text
failure mode A: build SDK helpers before the protocol boundary is lossless
failure mode B: SDK helpers silently discard broad frame metadata
guarded target state: a convenient API that keeps correctness bugs visible
```

## Minimal flow

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

worker
    .update_component("ship-1", "pos", json!([10.0, 20.0]), Some(7))
    .await?;

while let Some(frame) = worker.recv_frame().await? {
    if let Some(authority) = frame.authority_change() {
        if !authority.authoritative {
            // Stop writing that component and prepare handoff/flush behavior.
            continue;
        }
    }

    if let Some(event) = frame.entity_event() {
        // event.fields keeps the current broad JSON payload intact.
        let _ = event.fields.fields.get("payload");
    }
}
# Ok(())
# }
```

## Next after merge

After this MVP lands, the next slice should migrate `zone_worker` onto the SDK behind behavior-preserving tests. Do not start Godot or client SDK work before that migration proves the SDK can drive the current worker path.
