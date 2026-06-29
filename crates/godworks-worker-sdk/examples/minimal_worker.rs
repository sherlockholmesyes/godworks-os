use godworks_core::Position2;
use godworks_worker_sdk::{
    decode_frame_payload, read_op, write_op, WorkerConfig, WorkerSession,
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
        .update_component("ship-1", "pos", json!([10.0, 20.0]), Some(7))
        .await?;
    let _update = read_op(&mut broker_side).await?.expect("update frame");

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

    if let Some(frame) = worker.recv_frame().await? {
        if let Some(authority) = frame.authority_change() {
            if !authority.authoritative {
                println!("authority loss for {}", authority.entity.as_ref());
            }
        }
    }

    Ok(())
}
