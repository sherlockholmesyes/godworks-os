# Worker SDK MVP

This product-hardening slice adds a small Rust Worker SDK over the typed v1 protocol boundary.

## Scope

Included:

- worker registration through `WorkerConnect`;
- optional `auth_token` forwarding for brokers configured with `GW_AUTH_TOKEN`;
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

This is the product-facing worker shape the SDK is meant to make boring:
connect, declare interest, create an entity, write epoch-fenced component
batches, react to authority/rejection metadata, then disconnect.

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

            // Stop writing that component and prepare handoff/flush behavior.
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

The convenience helpers do not narrow the wire contract. Every received frame
still carries its full typed `Op` through `WorkerFrame::op()` / `into_op()`, and
the current broad runtime metadata remains available through each frame's
`fields` bag. In particular, workers should read `AuthorityChange.fields` and
`UpdateRejected.fields` when making handoff, retry, or stale-epoch decisions.

## Current integration status

The real `zone_worker` now uses the SDK frame boundary and helper surface for
its worker protocol path. See `docs/sdk/issue-16-pr-readiness.md` for the
current migrated-site report and gate. Do not start Godot or client SDK work
from this document alone; first check the current hardening roadmap and the
latest worker/runtime gate.
