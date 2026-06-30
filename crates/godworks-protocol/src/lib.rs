//! Godworks OS protocol model.
//!
//! This crate starts as a typed boundary for the current length-prefixed JSON
//! protocol. The existing broker still owns runtime dispatch; future hardening
//! PRs should migrate raw JSON construction into this crate and then add codecs.

pub mod json;

pub use godworks_core::{
    ComponentId, ComponentKind, ComponentRegistry, ComponentSchema, ComponentVersion,
    CoordinateCodec, PartitionSchema, SpatialDim, SpatialSchema, COORDINATE_CODEC_VERSION,
    SPATIAL_SCHEMA_VERSION, STANDARD_COMPONENT_REGISTRY_VERSION,
};

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId, Velocity2};
use serde_json::{Map, Value};

/// Current broker protocol version.
pub const PROTOCOL_VERSION: u64 = 1;

/// Oldest protocol version accepted by the current broker.
pub const MIN_PROTOCOL_VERSION: u64 = 1;

/// Conservative frame ceiling for future hardened readers.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Current `SnapshotManifest` envelope version.
pub const SNAPSHOT_MANIFEST_VERSION: u64 = 1;

/// Current snapshot artifact schema version.
pub const SNAPSHOT_SCHEMA_VERSION: u64 = 1;

/// Returns whether a peer protocol version is currently supported.
pub const fn supports_protocol(version: u64) -> bool {
    version >= MIN_PROTOCOL_VERSION && version <= PROTOCOL_VERSION
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationPersistence {
    Persistent,
    Transient,
}

impl OperationPersistence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Transient => "transient",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationCategory {
    EntityLifecycle,
    LifecycleResponse,
    EntityUpdate,
    AuthorityControl,
    AuthorityResponse,
    AuthorityEvent,
    HandoffControl,
    TransactionControl,
    TransactionResponse,
    MeshHandoff,
    InterestProjection,
    DurabilityControl,
    CommandRpc,
    EntityEvent,
}

impl OperationCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EntityLifecycle => "entity_lifecycle",
            Self::LifecycleResponse => "lifecycle_response",
            Self::EntityUpdate => "entity_update",
            Self::AuthorityControl => "authority_control",
            Self::AuthorityResponse => "authority_response",
            Self::AuthorityEvent => "authority_event",
            Self::HandoffControl => "handoff_control",
            Self::TransactionControl => "transaction_control",
            Self::TransactionResponse => "transaction_response",
            Self::MeshHandoff => "mesh_handoff",
            Self::InterestProjection => "interest_projection",
            Self::DurabilityControl => "durability_control",
            Self::CommandRpc => "command_rpc",
            Self::EntityEvent => "entity_event",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationSemantics {
    pub op: &'static str,
    pub persistence: OperationPersistence,
    pub category: OperationCategory,
    pub response_op: Option<&'static str>,
}

pub const OPERATION_SEMANTICS: &[OperationSemantics] = &[
    OperationSemantics {
        op: "CreateEntity",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityLifecycle,
        response_op: Some("CreateEntityResponse"),
    },
    OperationSemantics {
        op: "CreateEntityResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::LifecycleResponse,
        response_op: None,
    },
    OperationSemantics {
        op: "DeleteEntity",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityLifecycle,
        response_op: Some("DeleteEntityResponse"),
    },
    OperationSemantics {
        op: "DeleteEntityResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::LifecycleResponse,
        response_op: None,
    },
    OperationSemantics {
        op: "ReserveEntityIds",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityLifecycle,
        response_op: Some("ReserveEntityIdsResponse"),
    },
    OperationSemantics {
        op: "ReserveEntityIdsResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::LifecycleResponse,
        response_op: None,
    },
    OperationSemantics {
        op: "AddComponent",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityUpdate,
        response_op: None,
    },
    OperationSemantics {
        op: "RemoveComponent",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityUpdate,
        response_op: None,
    },
    OperationSemantics {
        op: "UpdateComponent",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityUpdate,
        response_op: None,
    },
    OperationSemantics {
        op: "BatchUpdate",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::EntityUpdate,
        response_op: None,
    },
    OperationSemantics {
        op: "ComponentUpdate",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::EntityUpdate,
        response_op: None,
    },
    OperationSemantics {
        op: "SetComponentAuthority",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::AuthorityControl,
        response_op: Some("SetComponentAuthorityResponse"),
    },
    OperationSemantics {
        op: "SetComponentAuthorityResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::AuthorityResponse,
        response_op: None,
    },
    OperationSemantics {
        op: "AuthorityChange",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::AuthorityEvent,
        response_op: None,
    },
    OperationSemantics {
        op: "Fold",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::HandoffControl,
        response_op: None,
    },
    OperationSemantics {
        op: "ThresholdTx",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::TransactionControl,
        response_op: Some("ThresholdTxResponse"),
    },
    OperationSemantics {
        op: "ThresholdTxResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::TransactionResponse,
        response_op: None,
    },
    OperationSemantics {
        op: "CommandRequest",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::CommandRpc,
        response_op: Some("CommandResponse"),
    },
    OperationSemantics {
        op: "CommandResponse",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::CommandRpc,
        response_op: None,
    },
    OperationSemantics {
        op: "EntityEvent",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::EntityEvent,
        response_op: None,
    },
    OperationSemantics {
        op: "SnapshotMarker",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::DurabilityControl,
        response_op: None,
    },
    OperationSemantics {
        op: "MeshHandoff",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::MeshHandoff,
        response_op: Some("MeshAck"),
    },
    OperationSemantics {
        op: "MeshAck",
        persistence: OperationPersistence::Persistent,
        category: OperationCategory::MeshHandoff,
        response_op: None,
    },
    OperationSemantics {
        op: "MeshGhost",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::InterestProjection,
        response_op: None,
    },
    OperationSemantics {
        op: "MeshGhostRemove",
        persistence: OperationPersistence::Transient,
        category: OperationCategory::InterestProjection,
        response_op: None,
    },
];

pub fn operation_semantics(op: &str) -> Option<&'static OperationSemantics> {
    OPERATION_SEMANTICS
        .iter()
        .find(|semantics| semantics.op == op)
}

/// Coarse peer role inferred from the current `WorkerConnect.region` model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerRole {
    RegionWorker(RegionId),
    Observer,
    Mesh,
    Standby,
}

impl PeerRole {
    pub fn from_region(region: impl Into<RegionId>) -> Self {
        let region = region.into();
        match region.as_ref() {
            "OBS" => Self::Observer,
            "MESH" => Self::Mesh,
            "STANDBY" => Self::Standby,
            _ => Self::RegionWorker(region),
        }
    }
}

/// Draft typed operation model for the v1 wire.
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    WorkerConnect(WorkerConnect),
    AuthReject(AuthReject),
    Disconnect,
    Heartbeat(Heartbeat),
    Interest(Interest),
    CriticalSection(CriticalSection),
    AddEntity(AddEntity),
    RemoveEntity(RemoveEntity),
    CreateEntity(CreateEntity),
    CreateEntityResponse(CreateEntityResponse),
    DeleteEntity(DeleteEntity),
    DeleteEntityResponse(DeleteEntityResponse),
    ReserveEntityIds(ReserveEntityIds),
    ReserveEntityIdsResponse(ReserveEntityIdsResponse),
    AddComponent(AddComponent),
    RemoveComponent(RemoveComponent),
    UpdateComponent(UpdateComponent),
    ComponentUpdate(ComponentUpdate),
    BatchUpdate(BatchUpdate),
    SetComponentAuthority(SetComponentAuthority),
    SetComponentAuthorityResponse(SetComponentAuthorityResponse),
    AuthorityChange(AuthorityChange),
    UpdateRejected(UpdateRejected),
    Fold(Fold),
    ThresholdTx(ThresholdTx),
    ThresholdTxResponse(ThresholdTxResponse),
    EntityQuery(EntityQuery),
    EntityQueryResponse(EntityQueryResponse),
    InspectorQuery(InspectorQuery),
    InspectorFrame(InspectorFrame),
    CommandRequest(CommandRequest),
    CommandResponse(CommandResponse),
    EntityEvent(EntityEvent),
    FlagUpdate(FlagUpdate),
    Metrics(Metrics),
    SnapshotMarker(SnapshotMarker),
    SnapshotManifest(SnapshotManifest),
    MeshHandoff(MeshHandoff),
    MeshAck(MeshAck),
    MeshGhost(MeshGhost),
    MeshGhostRemove(MeshGhostRemove),
    LogMessage(LogMessage),
    Health,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConnect {
    pub worker_id: PeerId,
    pub region: RegionId,
    pub proto: Option<u64>,
    pub attributes: Vec<String>,
    pub auth_token: Option<String>,
}

impl WorkerConnect {
    pub fn role(&self) -> PeerRole {
        PeerRole::from_region(self.region.clone())
    }

    pub fn protocol_is_supported(&self) -> bool {
        self.proto.map(supports_protocol).unwrap_or(true)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthReject {
    pub worker_id: Option<PeerId>,
    pub error: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heartbeat {
    pub worker_id: Option<PeerId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Interest {
    pub aoi: Option<Aoi2>,
    pub full_radius: Option<f64>,
    pub coarse_rate: u64,
    pub coarse_grid: f64,
}

impl Default for Interest {
    fn default() -> Self {
        Self {
            aoi: None,
            full_radius: None,
            coarse_rate: 1,
            coarse_grid: 0.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JsonFields {
    pub fields: Map<String, Value>,
}

impl JsonFields {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    pub fn str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Value::as_str)
    }

    pub fn bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(Value::as_bool)
    }

    pub fn u64(&self, key: &str) -> Option<u64> {
        self.get(key).and_then(Value::as_u64)
    }

    pub fn f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(Value::as_f64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CriticalSection {
    pub phase: String,
    pub entity: Option<EntityId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AddEntity {
    pub entity: EntityId,
    pub components: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveEntity {
    pub entity: EntityId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateEntity {
    pub entity: EntityId,
    pub request_id: Option<String>,
    pub requested_region: Option<RegionId>,
    pub components: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateEntityResponse {
    pub fields: JsonFields,
}

impl CreateEntityResponse {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn entity(&self) -> Option<&str> {
        self.fields.str("entity")
    }

    pub fn success(&self) -> Option<bool> {
        self.fields.bool("success")
    }

    pub fn reason(&self) -> Option<&str> {
        self.fields.str("reason")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteEntity {
    pub entity: EntityId,
    pub request_id: Option<String>,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteEntityResponse {
    pub fields: JsonFields,
}

impl DeleteEntityResponse {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn entity(&self) -> Option<&str> {
        self.fields.str("entity")
    }

    pub fn success(&self) -> Option<bool> {
        self.fields.bool("success")
    }

    pub fn reason(&self) -> Option<&str> {
        self.fields.str("reason")
    }

    pub fn idempotent(&self) -> Option<bool> {
        self.fields.bool("idempotent")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveEntityIds {
    pub request_id: Option<String>,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReserveEntityIdsResponse {
    pub fields: JsonFields,
}

impl ReserveEntityIdsResponse {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn first_id(&self) -> Option<u64> {
        self.fields.u64("first_id")
    }

    pub fn count(&self) -> Option<u64> {
        self.fields.u64("count")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AddComponent {
    pub entity: EntityId,
    pub component: ComponentName,
    pub value: Value,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveComponent {
    pub entity: EntityId,
    pub component: ComponentName,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateComponent {
    pub entity: EntityId,
    pub component: ComponentName,
    pub value: Value,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComponentUpdate {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchUpdate {
    pub component: ComponentName,
    pub updates: Vec<BatchUpdateEntry>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchUpdateEntry {
    pub entity: EntityId,
    pub value: Value,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetComponentAuthority {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetComponentAuthorityResponse {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthorityChange {
    pub entity: EntityId,
    pub component: ComponentName,
    pub authoritative: bool,
    pub authority_epoch: u64,
    pub mode: String,
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateRejected {
    pub entity: Option<EntityId>,
    pub component: Option<ComponentName>,
    pub reason: String,
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fold {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThresholdTx {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThresholdTxResponse {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityQuery {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityQueryResponse {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InspectorQuery {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InspectorFrame {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandRequest {
    pub fields: JsonFields,
}

impl CommandRequest {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn entity(&self) -> Option<&str> {
        self.fields.str("entity")
    }

    pub fn command(&self) -> Option<&Value> {
        self.fields.get("command")
    }

    pub fn payload(&self) -> Option<&Value> {
        self.fields.get("payload")
    }

    pub fn caller(&self) -> Option<&str> {
        self.fields.str("caller")
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.fields.str("idempotency_key")
    }

    pub fn timeout_ms(&self) -> Option<u64> {
        self.fields.u64("timeout_ms")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandResponse {
    pub fields: JsonFields,
}

impl CommandResponse {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn success(&self) -> Option<bool> {
        self.fields.bool("success")
    }

    pub fn success_or_default(&self) -> bool {
        self.success().unwrap_or(true)
    }

    pub fn payload(&self) -> Option<&Value> {
        self.fields.get("payload")
    }

    pub fn reason(&self) -> Option<&str> {
        self.fields.str("reason")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityEvent {
    pub fields: JsonFields,
}

impl EntityEvent {
    pub fn entity(&self) -> Option<&str> {
        self.fields.str("entity")
    }

    pub fn event(&self) -> Option<&Value> {
        self.fields.get("event")
    }

    pub fn payload(&self) -> Option<&Value> {
        self.fields.get("payload")
    }

    pub fn sim_time(&self) -> Option<f64> {
        self.fields.f64("sim_time")
    }

    pub fn gen(&self) -> Option<u64> {
        self.fields.u64("gen")
    }

    pub fn class(&self) -> Option<&str> {
        self.fields.str("class")
    }

    pub fn class_or_default(&self) -> &str {
        self.class().unwrap_or("critical")
    }

    pub fn coalesce_key(&self) -> Option<&str> {
        self.fields.str("coalesce_key")
    }

    pub fn count(&self) -> Option<u64> {
        self.fields.u64("count")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FlagUpdate {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Metrics {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotMarker {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotManifest {
    pub fields: JsonFields,
}

impl SnapshotManifest {
    pub fn request_id(&self) -> Option<&str> {
        self.fields.str("request_id")
    }

    pub fn snapshot_id(&self) -> Option<&str> {
        self.fields.str("snapshot_id")
    }

    pub fn snapshot_manifest_version(&self) -> Option<u64> {
        self.fields.u64("snapshot_manifest_version")
    }

    pub fn snapshot_schema_version(&self) -> Option<u64> {
        self.fields.u64("snapshot_schema_version")
    }

    pub fn spatial_schema_version(&self) -> Option<u64> {
        self.fields.u64("spatial_schema_version")
    }

    pub fn coordinate_codec_version(&self) -> Option<u64> {
        self.fields.u64("coordinate_codec_version")
    }

    pub fn component_registry_version(&self) -> Option<u64> {
        self.fields.u64("component_registry_version")
    }

    pub fn partition_map_version(&self) -> Option<u64> {
        self.fields.u64("partition_map_version")
    }

    pub fn wal_offset(&self) -> Option<u64> {
        self.fields.u64("wal_offset")
    }

    pub fn entity_count(&self) -> Option<u64> {
        self.fields.u64("entity_count")
    }

    pub fn pending_mesh(&self) -> Option<u64> {
        self.fields.u64("pending_mesh")
    }

    pub fn broker_id(&self) -> Option<&str> {
        self.fields.str("broker_id")
    }

    pub fn authority_hash(&self) -> Option<&str> {
        self.fields.str("authority_hash")
    }

    pub fn spatial_schema(&self) -> Option<SpatialSchema> {
        parse_spatial_schema_contract(self.fields.get("spatial_schema")?)
    }

    pub fn has_current_versions(&self) -> bool {
        self.snapshot_manifest_version() == Some(SNAPSHOT_MANIFEST_VERSION)
            && self.snapshot_schema_version() == Some(SNAPSHOT_SCHEMA_VERSION)
            && self.spatial_schema_version() == Some(SPATIAL_SCHEMA_VERSION)
            && self.coordinate_codec_version() == Some(COORDINATE_CODEC_VERSION)
            && self.component_registry_version() == Some(STANDARD_COMPONENT_REGISTRY_VERSION)
    }
}

pub fn parse_spatial_schema_contract(value: &Value) -> Option<SpatialSchema> {
    let obj = value.as_object()?;
    let spatial_dim = obj
        .get("spatial_dim")
        .and_then(Value::as_str)
        .and_then(SpatialDim::from_wire_str)?;
    let coordinate_codec = obj
        .get("coordinate_codec")
        .and_then(Value::as_str)
        .and_then(CoordinateCodec::from_wire_str)?;
    let partition_schema = parse_partition_schema_contract(obj.get("partition_schema")?)?;
    Some(SpatialSchema {
        spatial_dim,
        coordinate_codec,
        partition_schema,
    })
}

pub fn parse_partition_schema_contract(value: &Value) -> Option<PartitionSchema> {
    let obj = value.as_object()?;
    match obj.get("kind").and_then(Value::as_str)? {
        "grid2d" => {
            let cols = obj.get("cols").and_then(Value::as_u64)?;
            let rows = obj.get("rows").and_then(Value::as_u64)?;
            PartitionSchema::grid2d(cols, rows).ok()
        }
        "strip1d" => {
            let boundary_count = obj.get("boundary_count").and_then(Value::as_u64)?;
            Some(PartitionSchema::strip1d(boundary_count))
        }
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MeshHandoff {
    pub entity: EntityId,
    pub source_region: Option<RegionId>,
    pub target_region: RegionId,
    pub pos: Position2,
    pub vel: Velocity2,
    pub authority_epoch: Option<u64>,
    pub lease_epoch: Option<u64>,
    pub source_durable_gen: Option<u64>,
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeshAck {
    pub entity: EntityId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MeshGhost {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MeshGhostRemove {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogMessage {
    pub fields: JsonFields,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolErrorKind {
    UnsupportedVersion,
    MalformedFrame,
    UnknownOperation,
    MissingRequiredField,
    OversizedFrame,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolError {
    pub kind: ProtocolErrorKind,
    pub message: String,
}

impl ProtocolError {
    pub fn unsupported_version(version: u64) -> Self {
        Self {
            kind: ProtocolErrorKind::UnsupportedVersion,
            message: format!(
                "unsupported protocol version {version}; supported range is {MIN_PROTOCOL_VERSION}..={PROTOCOL_VERSION}"
            ),
        }
    }

    pub fn malformed(message: impl Into<String>) -> Self {
        Self {
            kind: ProtocolErrorKind::MalformedFrame,
            message: message.into(),
        }
    }

    pub fn missing_field(field: &str) -> Self {
        Self {
            kind: ProtocolErrorKind::MissingRequiredField,
            message: format!("missing required field '{field}'"),
        }
    }

    pub fn unknown_operation(op: &str) -> Self {
        Self {
            kind: ProtocolErrorKind::UnknownOperation,
            message: format!("unknown operation '{op}'"),
        }
    }

    pub fn oversized_frame(bytes: usize, max: usize) -> Self {
        Self {
            kind: ProtocolErrorKind::OversizedFrame,
            message: format!("frame size {bytes} exceeds max frame size {max}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_range_matches_current_v1_contract() {
        assert!(supports_protocol(1));
        assert!(!supports_protocol(0));
        assert!(!supports_protocol(2));
    }

    #[test]
    fn worker_connect_classifies_reserved_regions() {
        assert_eq!(PeerRole::from_region("OBS"), PeerRole::Observer);
        assert_eq!(PeerRole::from_region("MESH"), PeerRole::Mesh);
        assert_eq!(PeerRole::from_region("STANDBY"), PeerRole::Standby);
        assert_eq!(
            PeerRole::from_region("W"),
            PeerRole::RegionWorker(RegionId::from("W"))
        );
    }

    #[test]
    fn worker_connect_protocol_defaults_to_legacy_allowed() {
        let legacy = WorkerConnect {
            worker_id: PeerId::from("w1"),
            region: RegionId::from("W"),
            proto: None,
            attributes: Vec::new(),
            auth_token: None,
        };
        let future = WorkerConnect {
            proto: Some(PROTOCOL_VERSION + 1),
            ..legacy.clone()
        };

        assert!(legacy.protocol_is_supported());
        assert!(!future.protocol_is_supported());
    }
}
