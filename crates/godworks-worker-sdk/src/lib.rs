//! Minimal Godworks OS worker SDK.
//!
//! This crate intentionally stays narrow: it wraps the current length-prefixed
//! JSON v1 protocol with typed helpers while preserving the full typed `Op` for
//! every frame. The broker runtime is unchanged.

use std::fmt;
use std::io;

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId};
use godworks_protocol::json::{decode_json_value, encode_json_value};
use godworks_protocol::{
    AddEntity, AuthorityChange, BatchUpdate, BatchUpdateEntry, CommandRequest, ComponentUpdate,
    CriticalSection, EntityEvent, Interest, MeshHandoff, Op, UpdateComponent, UpdateRejected,
    WorkerConnect, DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type Result<T> = std::result::Result<T, WorkerSdkError>;

#[derive(Debug)]
pub enum WorkerSdkError {
    Io(io::Error),
    Json(serde_json::Error),
    Protocol(godworks_protocol::ProtocolError),
    FrameTooLarge { bytes: usize, max: usize },
}

impl fmt::Display for WorkerSdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Json(err) => write!(f, "json error: {err}"),
            Self::Protocol(err) => write!(f, "protocol error {:?}: {}", err.kind, err.message),
            Self::FrameTooLarge { bytes, max } => {
                write!(f, "frame size {bytes} exceeds max frame size {max}")
            }
        }
    }
}

impl std::error::Error for WorkerSdkError {}

impl From<io::Error> for WorkerSdkError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for WorkerSdkError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<godworks_protocol::ProtocolError> for WorkerSdkError {
    fn from(value: godworks_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    pub worker_id: PeerId,
    pub region: RegionId,
    pub attributes: Vec<String>,
    pub proto: Option<u64>,
}

impl WorkerConfig {
    pub fn new(worker_id: impl Into<PeerId>, region: impl Into<RegionId>) -> Self {
        Self {
            worker_id: worker_id.into(),
            region: region.into(),
            attributes: Vec::new(),
            proto: Some(PROTOCOL_VERSION),
        }
    }

    pub fn with_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attributes.push(attribute.into());
        self
    }

    pub fn connect_op(&self) -> Op {
        Op::WorkerConnect(WorkerConnect {
            worker_id: self.worker_id.clone(),
            region: self.region.clone(),
            proto: self.proto,
            attributes: self.attributes.clone(),
        })
    }
}

pub struct WorkerSession<S> {
    stream: S,
}

impl<S> WorkerSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn connect(mut stream: S, config: WorkerConfig) -> Result<Self> {
        write_op(&mut stream, &config.connect_op()).await?;
        Ok(Self { stream })
    }

    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    pub async fn send_op(&mut self, op: &Op) -> Result<()> {
        write_op(&mut self.stream, op).await
    }

    pub async fn recv_op(&mut self) -> Result<Option<Op>> {
        read_op(&mut self.stream).await
    }

    pub async fn recv_frame(&mut self) -> Result<Option<WorkerFrame>> {
        Ok(self.recv_op().await?.map(WorkerFrame::new))
    }

    pub async fn set_interest(&mut self, interest: Interest) -> Result<()> {
        self.send_op(&Op::Interest(interest)).await
    }

    pub async fn set_circle_interest(
        &mut self,
        center: Position2,
        radius: f64,
        full_radius: Option<f64>,
    ) -> Result<()> {
        self.set_interest(circle_interest(center, radius, full_radius))
            .await
    }

    pub async fn update_component(
        &mut self,
        entity: impl Into<EntityId>,
        component: impl Into<ComponentName>,
        value: Value,
        authority_epoch: Option<u64>,
    ) -> Result<()> {
        let op = Op::UpdateComponent(UpdateComponent {
            entity: entity.into(),
            component: component.into(),
            value,
            authority_epoch,
        });
        self.send_op(&op).await
    }

    pub async fn batch_update(
        &mut self,
        component: impl Into<ComponentName>,
        updates: Vec<BatchUpdateEntry>,
    ) -> Result<()> {
        let op = Op::BatchUpdate(BatchUpdate {
            component: component.into(),
            updates,
        });
        self.send_op(&op).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFrameKind {
    Checkout,
    EntityAdded,
    EntityRemoved,
    ComponentUpdate,
    EntityEvent,
    AuthorityChange,
    UpdateRejected,
    MeshHandoff,
    CommandRequest,
    Other,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkerFrame {
    op: Op,
}

impl WorkerFrame {
    pub fn new(op: Op) -> Self {
        Self { op }
    }

    pub fn kind(&self) -> WorkerFrameKind {
        match &self.op {
            Op::CriticalSection(_) => WorkerFrameKind::Checkout,
            Op::AddEntity(_) => WorkerFrameKind::EntityAdded,
            Op::RemoveEntity(_) => WorkerFrameKind::EntityRemoved,
            Op::ComponentUpdate(_) => WorkerFrameKind::ComponentUpdate,
            Op::EntityEvent(_) => WorkerFrameKind::EntityEvent,
            Op::AuthorityChange(_) => WorkerFrameKind::AuthorityChange,
            Op::UpdateRejected(_) => WorkerFrameKind::UpdateRejected,
            Op::MeshHandoff(_) => WorkerFrameKind::MeshHandoff,
            Op::CommandRequest(_) => WorkerFrameKind::CommandRequest,
            _ => WorkerFrameKind::Other,
        }
    }

    pub fn op(&self) -> &Op {
        &self.op
    }

    pub fn into_op(self) -> Op {
        self.op
    }

    pub fn checkout(&self) -> Option<&CriticalSection> {
        match &self.op {
            Op::CriticalSection(value) => Some(value),
            _ => None,
        }
    }

    pub fn add_entity(&self) -> Option<&AddEntity> {
        match &self.op {
            Op::AddEntity(value) => Some(value),
            _ => None,
        }
    }

    pub fn component_update(&self) -> Option<&ComponentUpdate> {
        match &self.op {
            Op::ComponentUpdate(value) => Some(value),
            _ => None,
        }
    }

    pub fn entity_event(&self) -> Option<&EntityEvent> {
        match &self.op {
            Op::EntityEvent(value) => Some(value),
            _ => None,
        }
    }

    pub fn authority_change(&self) -> Option<&AuthorityChange> {
        match &self.op {
            Op::AuthorityChange(value) => Some(value),
            _ => None,
        }
    }

    pub fn update_rejected(&self) -> Option<&UpdateRejected> {
        match &self.op {
            Op::UpdateRejected(value) => Some(value),
            _ => None,
        }
    }

    pub fn mesh_handoff(&self) -> Option<&MeshHandoff> {
        match &self.op {
            Op::MeshHandoff(value) => Some(value),
            _ => None,
        }
    }

    pub fn command_request(&self) -> Option<&CommandRequest> {
        match &self.op {
            Op::CommandRequest(value) => Some(value),
            _ => None,
        }
    }
}

pub fn circle_interest(center: Position2, radius: f64, full_radius: Option<f64>) -> Interest {
    Interest {
        aoi: Some(Aoi2::Circle { center, radius }),
        full_radius,
        coarse_rate: 1,
        coarse_grid: 0.0,
    }
}

pub fn batch_entry(
    entity: impl Into<EntityId>,
    value: Value,
    authority_epoch: Option<u64>,
) -> BatchUpdateEntry {
    BatchUpdateEntry {
        entity: entity.into(),
        value,
        authority_epoch,
    }
}

pub fn encode_frame_payload(op: &Op) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&encode_json_value(op))?)
}

pub fn decode_frame_payload(payload: &[u8]) -> Result<Op> {
    let value: Value = serde_json::from_slice(payload)?;
    Ok(decode_json_value(&value)?)
}

pub fn encode_len_prefixed_frame(op: &Op) -> Result<Vec<u8>> {
    let payload = encode_frame_payload(op)?;
    if payload.len() > DEFAULT_MAX_FRAME_BYTES {
        return Err(WorkerSdkError::FrameTooLarge {
            bytes: payload.len(),
            max: DEFAULT_MAX_FRAME_BYTES,
        });
    }

    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub async fn write_op<W>(writer: &mut W, op: &Op) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_len_prefixed_frame(op)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_op<R>(reader: &mut R) -> Result<Option<Op>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0_u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(WorkerSdkError::Io(err)),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > DEFAULT_MAX_FRAME_BYTES {
        return Err(WorkerSdkError::FrameTooLarge {
            bytes: len,
            max: DEFAULT_MAX_FRAME_BYTES,
        });
    }

    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    decode_frame_payload(&payload).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use godworks_protocol::json::encode_json_value;
    use serde_json::json;
    use tokio::io::duplex;

    fn assert_payload_roundtrip(value: Value) -> Op {
        let op = decode_frame_payload(value.to_string().as_bytes()).unwrap();
        assert_eq!(encode_json_value(&op), value);
        op
    }

    #[test]
    fn mesh_handoff_authority_components_roundtrip_losslessly() {
        let value = json!({
            "op": "MeshHandoff",
            "entity": "ship",
            "source_region": "W",
            "target": "E",
            "pos": [1.0, 2.0],
            "vel": [3.0, 4.0],
            "authority_epoch": 9,
            "lease_epoch": 11,
            "source_durable_gen": 12,
            "authority": {
                "pos": {
                    "owner": "zw-E",
                    "epoch": 9,
                    "mode": "server_physics_island"
                }
            },
            "components": {
                "mass": 2.0,
                "kind": "projectile"
            }
        });
        let op = assert_payload_roundtrip(value);
        let frame = WorkerFrame::new(op);
        assert_eq!(frame.kind(), WorkerFrameKind::MeshHandoff);
        assert!(frame.mesh_handoff().is_some());
    }

    #[test]
    fn authority_loss_and_update_rejected_metadata_roundtrip_losslessly() {
        assert_payload_roundtrip(json!({
            "op": "AuthorityChange",
            "entity": "ship",
            "comp": "pos",
            "authoritative": false,
            "authority_epoch": 12,
            "mode": "server_physics_island",
            "state": "AUTHORITY_LOSS_IMMINENT",
            "handoff_target": "zw-E",
            "handoff_target_region": "E"
        }));
        assert_payload_roundtrip(json!({
            "op": "UpdateRejected",
            "request_id": "admin-1",
            "entity": "ship",
            "comp": "pos",
            "reason": "stale authority epoch",
            "authority_epoch": 13,
            "ghost": true,
            "owner_region": "E"
        }));
    }

    #[tokio::test]
    async fn connect_interest_and_update_helpers_emit_typed_ops() {
        let (client_stream, mut broker_stream) = duplex(8192);
        let config = WorkerConfig::new("worker-1", "W").with_attribute("physics");
        let mut worker = WorkerSession::connect(client_stream, config).await.unwrap();

        worker
            .set_circle_interest(Position2::new(1.0, 2.0), 64.0, Some(32.0))
            .await
            .unwrap();
        worker
            .update_component("ship-1", "pos", json!([10.0, 20.0]), Some(7))
            .await
            .unwrap();
        worker
            .batch_update(
                "vel",
                vec![batch_entry("ship-1", json!([1.0, 0.0]), Some(7))],
            )
            .await
            .unwrap();

        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::WorkerConnect(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::Interest(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::UpdateComponent(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::BatchUpdate(_))
        ));
    }

    #[tokio::test]
    async fn worker_receives_classified_lossless_frames_from_mock_stream() {
        let (mut broker_stream, worker_stream) = duplex(8192);
        let mut worker = WorkerSession::new(worker_stream);

        let authority_op = decode_frame_payload(
            json!({
                "op": "AuthorityChange",
                "entity": "ship",
                "comp": "pos",
                "authoritative": false,
                "authority_epoch": 12,
                "mode": "server_physics_island",
                "state": "AUTHORITY_LOSS_IMMINENT",
                "handoff_target": "zw-E",
                "handoff_target_region": "E"
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        write_op(&mut broker_stream, &authority_op).await.unwrap();

        let event_op = decode_frame_payload(
            json!({
                "op": "EntityEvent",
                "entity": "ship",
                "event": "StatusChanged",
                "payload": { "hp": 80 },
                "gen": 3
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        write_op(&mut broker_stream, &event_op).await.unwrap();

        let first = worker.recv_frame().await.unwrap().unwrap();
        assert_eq!(first.kind(), WorkerFrameKind::AuthorityChange);
        assert_eq!(
            first.authority_change().unwrap().fields.fields.get("state"),
            Some(&json!("AUTHORITY_LOSS_IMMINENT"))
        );

        let second = worker.recv_frame().await.unwrap().unwrap();
        assert_eq!(second.kind(), WorkerFrameKind::EntityEvent);
        assert!(second.entity_event().is_some());
    }
}
