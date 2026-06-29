use godworks_core::Position2;
use godworks_worker_sdk::{
    batch_entry, create_entity_op, decode_frame_payload, disconnect_op, read_op, write_op,
    WorkerConfig, WorkerFrameKind, WorkerSession,
};
use serde_json::json;
use tokio::io::duplex;

#[tokio::main(flavor = "current_thread")]
async fn main() -> godworks_worker_sdk::Result<()> {
    let (worker_stream, mut broker_side) = duplex(8192);

    let config = WorkerConfig::new("physics-W", "W").with_attribute("physics");
    let mut worker = WorkerSession::connect(worker_stream, config).await?;

    let _connect = read_op(&mut broker_side).await?.expect("connect frame");

    worker
        .set_circle_interest(Position2::new(0.0, 0.0), 256.0, Some(128.0))
        .await?;
    let _interest = read_op(&mut broker_side).await?.expect("interest frame");

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
    let _create = read_op(&mut broker_side).await?.expect("create frame");

    worker
        .batch_update(
            "pos",
            vec![batch_entry("ship-1", json!([11.0, 20.5]), Some(8))],
        )
        .await?;
    let _pos_batch = read_op(&mut broker_side)
        .await?
        .expect("position batch frame");

    worker
        .batch_update(
            "vel",
            vec![batch_entry("ship-1", json!([1.0, 0.25]), Some(8))],
        )
        .await?;
    let _vel_batch = read_op(&mut broker_side)
        .await?
        .expect("velocity batch frame");

    let authority_loss = decode_frame_payload(
        json!({
            "op": "AuthorityChange",
            "entity": "ship-1",
            "comp": "pos",
            "authoritative": false,
            "authority_epoch": 8,
            "mode": "server_physics_island",
            "state": "AUTHORITY_LOSS_IMMINENT",
            "handoff_target_region": "E"
        })
        .to_string()
        .as_bytes(),
    )?;
    write_op(&mut broker_side, &authority_loss).await?;

    let rejection = decode_frame_payload(
        json!({
            "op": "UpdateRejected",
            "request_id": "batch-42",
            "entity": "ship-1",
            "comp": "vel",
            "reason": "stale authority epoch",
            "authority_epoch": 9,
            "owner_region": "E"
        })
        .to_string()
        .as_bytes(),
    )?;
    write_op(&mut broker_side, &rejection).await?;

    if let Some(frame) = worker.recv_frame().await? {
        assert_eq!(frame.kind(), WorkerFrameKind::AuthorityChange);
        if let Some(authority) = frame.authority_change() {
            if !authority.authoritative {
                let target_region = authority.fields.fields.get("handoff_target_region");
                println!(
                    "authority loss for {} at epoch {} -> {:?}",
                    authority.entity.as_ref(),
                    authority.authority_epoch,
                    target_region
                );
            }
        }
    }

    if let Some(frame) = worker.recv_frame().await? {
        assert_eq!(frame.kind(), WorkerFrameKind::UpdateRejected);
        if let Some(rejected) = frame.update_rejected() {
            println!(
                "rejected {:?}.{:?}: {} ({:?})",
                rejected.entity.as_ref().map(|entity| entity.as_ref()),
                rejected
                    .component
                    .as_ref()
                    .map(|component| component.as_ref()),
                rejected.reason,
                rejected.fields.fields.get("owner_region")
            );
        }
    }

    worker.send_op(&disconnect_op()).await?;
    let _disconnect = read_op(&mut broker_side).await?.expect("disconnect frame");

    Ok(())
}
