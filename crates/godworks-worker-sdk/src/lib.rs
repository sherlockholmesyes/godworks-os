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
    AddComponent, AddEntity, AuthReject, AuthorityChange, BatchUpdate, BatchUpdateEntry,
    CommandRequest, CommandResponse, ComponentUpdate, CreateEntity, CriticalSection, DeleteEntity,
    EntityEvent, EntityQuery, Fold, Heartbeat, Interest, JsonFields, MeshHandoff, Op,
    RemoveComponent, ReserveEntityIds, UpdateComponent, UpdateRejected, WorkerConnect,
    DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use serde_json::{json, Map, Value};
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
    pub auth_token: Option<String>,
}

impl WorkerConfig {
    pub fn new(worker_id: impl Into<PeerId>, region: impl Into<RegionId>) -> Self {
        Self {
            worker_id: worker_id.into(),
            region: region.into(),
            attributes: Vec::new(),
            proto: Some(PROTOCOL_VERSION),
            auth_token: None,
        }
    }

    pub fn with_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attributes.push(attribute.into());
        self
    }

    pub fn with_auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    pub fn connect_op(&self) -> Op {
        Op::WorkerConnect(WorkerConnect {
            worker_id: self.worker_id.clone(),
            region: self.region.clone(),
            proto: self.proto,
            attributes: self.attributes.clone(),
            auth_token: self.auth_token.clone(),
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

    pub async fn delete_entity(
        &mut self,
        entity: impl Into<EntityId>,
        request_id: Option<impl Into<String>>,
        authority_epoch: Option<u64>,
    ) -> Result<()> {
        self.send_op(&delete_entity_op(entity, request_id, authority_epoch))
            .await
    }

    pub async fn reserve_entity_ids(
        &mut self,
        request_id: Option<impl Into<String>>,
        count: u64,
    ) -> Result<()> {
        self.send_op(&reserve_entity_ids_op(request_id, count))
            .await
    }

    pub async fn add_component(
        &mut self,
        entity: impl Into<EntityId>,
        component: impl Into<ComponentName>,
        value: Value,
        authority_epoch: Option<u64>,
    ) -> Result<()> {
        self.send_op(&add_component_op(entity, component, value, authority_epoch))
            .await
    }

    pub async fn remove_component(
        &mut self,
        entity: impl Into<EntityId>,
        component: impl Into<ComponentName>,
        authority_epoch: Option<u64>,
    ) -> Result<()> {
        self.send_op(&remove_component_op(entity, component, authority_epoch))
            .await
    }

    pub async fn query_entities(&mut self, fields: Map<String, Value>) -> Result<()> {
        self.send_op(&entity_query_op(fields)).await
    }

    pub async fn respond_to_command(&mut self, fields: Map<String, Value>) -> Result<()> {
        self.send_op(&command_response_op(fields)).await
    }

    pub async fn emit_event(&mut self, fields: Map<String, Value>) -> Result<()> {
        self.send_op(&entity_event_op(fields)).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFrameKind {
    AuthReject,
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
            Op::AuthReject(_) => WorkerFrameKind::AuthReject,
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

    pub fn auth_reject(&self) -> Option<&AuthReject> {
        match &self.op {
            Op::AuthReject(value) => Some(value),
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

pub fn legacy_worker_connect_op(worker_id: impl Into<PeerId>, region: impl Into<RegionId>) -> Op {
    worker_connect_op(worker_id, region, None)
}

pub fn worker_connect_op(
    worker_id: impl Into<PeerId>,
    region: impl Into<RegionId>,
    auth_token: Option<String>,
) -> Op {
    Op::WorkerConnect(WorkerConnect {
        worker_id: worker_id.into(),
        region: region.into(),
        proto: None,
        attributes: Vec::new(),
        auth_token,
    })
}

pub fn heartbeat_op(worker_id: impl Into<PeerId>) -> Op {
    Op::Heartbeat(Heartbeat {
        worker_id: Some(worker_id.into()),
    })
}

pub fn disconnect_op() -> Op {
    Op::Disconnect
}

pub fn create_entity_op(
    entity: impl Into<EntityId>,
    region: impl Into<RegionId>,
    components: Value,
) -> Op {
    Op::CreateEntity(CreateEntity {
        entity: entity.into(),
        request_id: None,
        requested_region: Some(region.into()),
        components,
    })
}

pub fn delete_entity_op(
    entity: impl Into<EntityId>,
    request_id: Option<impl Into<String>>,
    authority_epoch: Option<u64>,
) -> Op {
    Op::DeleteEntity(DeleteEntity {
        entity: entity.into(),
        request_id: request_id.map(Into::into),
        authority_epoch,
    })
}

pub fn reserve_entity_ids_op(request_id: Option<impl Into<String>>, count: u64) -> Op {
    Op::ReserveEntityIds(ReserveEntityIds {
        request_id: request_id.map(Into::into),
        count,
    })
}

pub fn add_component_op(
    entity: impl Into<EntityId>,
    component: impl Into<ComponentName>,
    value: Value,
    authority_epoch: Option<u64>,
) -> Op {
    Op::AddComponent(AddComponent {
        entity: entity.into(),
        component: component.into(),
        value,
        authority_epoch,
    })
}

pub fn remove_component_op(
    entity: impl Into<EntityId>,
    component: impl Into<ComponentName>,
    authority_epoch: Option<u64>,
) -> Op {
    Op::RemoveComponent(RemoveComponent {
        entity: entity.into(),
        component: component.into(),
        authority_epoch,
    })
}

pub fn entity_query_op(fields: Map<String, Value>) -> Op {
    Op::EntityQuery(EntityQuery {
        fields: JsonFields { fields },
    })
}

pub fn command_response_op(fields: Map<String, Value>) -> Op {
    Op::CommandResponse(CommandResponse {
        fields: JsonFields { fields },
    })
}

pub fn entity_event_op(fields: Map<String, Value>) -> Op {
    Op::EntityEvent(EntityEvent {
        fields: JsonFields { fields },
    })
}

pub fn fold_op(entity: impl Into<EntityId>, region: impl Into<RegionId>, pos: [f32; 2]) -> Op {
    let entity = entity.into();
    let region = region.into();
    let mut fields = Map::new();
    fields.insert("entity".to_string(), json!(entity.as_ref()));
    fields.insert("region".to_string(), json!(region.as_ref()));
    fields.insert("pos".to_string(), json!([pos[0], pos[1]]));
    Op::Fold(Fold {
        fields: JsonFields { fields },
    })
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
    use godworks_protocol::{operation_semantics, OperationCategory, OperationPersistence};
    use serde_json::json;
    use tokio::io::duplex;

    fn assert_payload_roundtrip(value: Value) -> Op {
        let op = decode_frame_payload(value.to_string().as_bytes()).unwrap();
        assert_eq!(encode_json_value(&op), value);
        op
    }

    fn fields(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
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

    #[test]
    fn zone_worker_outbound_helpers_match_current_wire_shapes() {
        assert_eq!(
            encode_json_value(&legacy_worker_connect_op("zw-W", "W")),
            json!({"op":"WorkerConnect","worker_id":"zw-W","region":"W"})
        );
        assert_eq!(
            encode_json_value(&worker_connect_op(
                "zw-W",
                "W",
                Some("test-token".to_string())
            )),
            json!({"op":"WorkerConnect","worker_id":"zw-W","region":"W","auth_token":"test-token"})
        );
        assert_eq!(
            encode_json_value(&Op::Interest(circle_interest(
                Position2::new(0.0, 0.0),
                100.0,
                None
            ))),
            json!({"op":"Interest","center":[0.0,0.0],"radius":100.0})
        );
        assert_eq!(
            encode_json_value(&create_entity_op(
                "W-b0",
                "W",
                json!({"pos":[1.0,2.0],"vel":[3.0,4.0],"mass":2.5})
            )),
            json!({
                "op":"CreateEntity",
                "entity":"W-b0",
                "region":"W",
                "components":{"pos":[1.0,2.0],"vel":[3.0,4.0],"mass":2.5}
            })
        );
        assert_eq!(
            encode_json_value(&fold_op("W-b0", "E", [5.0, 6.0])),
            json!({"op":"Fold","entity":"W-b0","region":"E","pos":[5.0,6.0]})
        );
        assert_eq!(
            encode_json_value(&Op::BatchUpdate(BatchUpdate {
                component: "pos".into(),
                updates: vec![batch_entry("W-b0", json!([7.0, 8.0]), Some(9))]
            })),
            json!({"op":"BatchUpdate","comp":"pos","updates":[["W-b0",[7.0,8.0],9]]})
        );
        assert_eq!(
            encode_json_value(&heartbeat_op("zw-W")),
            json!({"op":"Heartbeat","worker_id":"zw-W"})
        );
        assert_eq!(
            encode_json_value(&disconnect_op()),
            json!({"op":"Disconnect"})
        );
    }

    #[test]
    fn sdk_lifecycle_query_command_event_helpers_match_current_wire_shapes() {
        assert_eq!(
            encode_json_value(&delete_entity_op("W-b0", Some("del-1"), Some(9))),
            json!({"op":"DeleteEntity","entity":"W-b0","request_id":"del-1","authority_epoch":9})
        );
        assert_eq!(
            encode_json_value(&reserve_entity_ids_op(Some("reserve-1"), 32)),
            json!({"op":"ReserveEntityIds","request_id":"reserve-1","count":32})
        );
        assert_eq!(
            encode_json_value(&add_component_op(
                "W-b0",
                "health",
                json!({"hp":100}),
                Some(5)
            )),
            json!({"op":"AddComponent","entity":"W-b0","comp":"health","value":{"hp":100},"authority_epoch":5})
        );
        assert_eq!(
            encode_json_value(&remove_component_op("W-b0", "health", Some(6))),
            json!({"op":"RemoveComponent","entity":"W-b0","comp":"health","authority_epoch":6})
        );
        assert_eq!(
            encode_json_value(&entity_query_op(fields(&[
                ("request_id", json!("q-1")),
                ("include_handoff_intent", json!(true)),
                (
                    "query",
                    json!({"type":"sphere","center":[0.0,0.0],"radius":50.0}),
                ),
            ]))),
            json!({"op":"EntityQuery","request_id":"q-1","include_handoff_intent":true,"query":{"type":"sphere","center":[0.0,0.0],"radius":50.0}})
        );
        assert_eq!(
            encode_json_value(&command_response_op(fields(&[
                ("request_id", json!("cmd-1")),
                ("success", json!(true)),
                ("payload", json!({"accepted":true})),
            ]))),
            json!({"op":"CommandResponse","request_id":"cmd-1","success":true,"payload":{"accepted":true}})
        );
        assert_eq!(
            encode_json_value(&entity_event_op(fields(&[
                ("entity", json!("W-b0")),
                ("event", json!("StatusChanged")),
                ("payload", json!({"amount":12})),
                ("sim_time", json!(123.5)),
                ("gen", json!(77)),
                ("class", json!("critical")),
            ]))),
            json!({"op":"EntityEvent","entity":"W-b0","event":"StatusChanged","payload":{"amount":12},"sim_time":123.5,"gen":77,"class":"critical"})
        );
    }

    #[test]
    fn sdk_lifecycle_command_event_helpers_bind_to_protocol_semantics() {
        let reserve = operation_semantics("ReserveEntityIds").expect("ReserveEntityIds semantics");
        assert_eq!(reserve.persistence, OperationPersistence::Persistent);
        assert_eq!(reserve.category, OperationCategory::EntityLifecycle);
        assert_eq!(reserve.response_op, Some("ReserveEntityIdsResponse"));

        let command_response =
            operation_semantics("CommandResponse").expect("CommandResponse semantics");
        assert_eq!(
            command_response.persistence,
            OperationPersistence::Transient
        );
        assert_eq!(command_response.category, OperationCategory::CommandRpc);

        let event = operation_semantics("EntityEvent").expect("EntityEvent semantics");
        assert_eq!(event.persistence, OperationPersistence::Transient);
        assert_eq!(event.category, OperationCategory::EntityEvent);
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
    async fn lifecycle_query_command_event_session_methods_emit_typed_ops() {
        let (client_stream, mut broker_stream) = duplex(8192);
        let config = WorkerConfig::new("worker-1", "W").with_attribute("physics");
        let mut worker = WorkerSession::connect(client_stream, config).await.unwrap();

        worker
            .delete_entity("ship-1", Some("del-1"), Some(8))
            .await
            .unwrap();
        worker
            .reserve_entity_ids(Some("reserve-1"), 4)
            .await
            .unwrap();
        worker
            .add_component("ship-1", "loot", json!(3), Some(9))
            .await
            .unwrap();
        worker
            .remove_component("ship-1", "loot", Some(10))
            .await
            .unwrap();
        worker
            .query_entities(fields(&[("request_id", json!("q-1"))]))
            .await
            .unwrap();
        worker
            .respond_to_command(fields(&[
                ("request_id", json!("cmd-1")),
                ("success", json!(true)),
            ]))
            .await
            .unwrap();
        worker
            .emit_event(fields(&[
                ("entity", json!("ship-1")),
                ("event", json!("StatusChanged")),
            ]))
            .await
            .unwrap();

        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::WorkerConnect(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::DeleteEntity(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::ReserveEntityIds(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::AddComponent(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::RemoveComponent(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::EntityQuery(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::CommandResponse(_))
        ));
        assert!(matches!(
            read_op(&mut broker_stream).await.unwrap(),
            Some(Op::EntityEvent(_))
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

    #[tokio::test]
    async fn worker_receives_auth_reject_as_typed_frame() {
        let (mut broker_stream, worker_stream) = duplex(8192);
        let mut worker = WorkerSession::new(worker_stream);

        let auth_reject = decode_frame_payload(
            json!({
                "op": "AuthReject",
                "worker_id": "zw-W",
                "error": "auth_error",
                "reason": "authentication required"
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        write_op(&mut broker_stream, &auth_reject).await.unwrap();

        let frame = worker.recv_frame().await.unwrap().unwrap();
        assert_eq!(frame.kind(), WorkerFrameKind::AuthReject);
        let reject = frame.auth_reject().unwrap();
        assert_eq!(reject.error, "auth_error");
        assert_eq!(reject.reason, "authentication required");
    }
}
