//! Godworks OS protocol model.
//!
//! This crate starts as a typed boundary for the current length-prefixed JSON
//! protocol. The existing broker still owns runtime dispatch; future hardening
//! PRs should migrate raw JSON construction into this crate and then add codecs.

pub mod json;

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId, Velocity2};
use serde_json::Value;

/// Current broker protocol version.
pub const PROTOCOL_VERSION: u64 = 1;

/// Oldest protocol version accepted by the current broker.
pub const MIN_PROTOCOL_VERSION: u64 = 1;

/// Conservative frame ceiling for future hardened readers.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Returns whether a peer protocol version is currently supported.
pub const fn supports_protocol(version: u64) -> bool {
    version >= MIN_PROTOCOL_VERSION && version <= PROTOCOL_VERSION
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
    Disconnect,
    Heartbeat(Heartbeat),
    Interest(Interest),
    CriticalSection(CriticalSection),
    AddEntity(AddEntity),
    RemoveEntity(RemoveEntity),
    CreateEntity(CreateEntity),
    DeleteEntity(DeleteEntity),
    ReserveEntityIds(ReserveEntityIds),
    AddComponent(AddComponent),
    RemoveComponent(RemoveComponent),
    UpdateComponent(UpdateComponent),
    BatchUpdate(BatchUpdate),
    AuthorityChange(AuthorityChange),
    UpdateRejected(UpdateRejected),
    MeshHandoff(MeshHandoff),
    MeshAck(MeshAck),
    Health,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConnect {
    pub worker_id: PeerId,
    pub region: RegionId,
    pub proto: Option<u64>,
    pub attributes: Vec<String>,
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
    pub requested_region: Option<RegionId>,
    pub pos: Position2,
    pub vel: Velocity2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteEntity {
    pub entity: EntityId,
    pub request_id: Option<String>,
    pub authority_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReserveEntityIds {
    pub request_id: Option<String>,
    pub count: u64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityChange {
    pub entity: EntityId,
    pub component: ComponentName,
    pub authoritative: bool,
    pub authority_epoch: u64,
    pub mode: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRejected {
    pub entity: Option<EntityId>,
    pub component: Option<ComponentName>,
    pub reason: String,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeshAck {
    pub entity: EntityId,
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
        };
        let future = WorkerConnect {
            proto: Some(PROTOCOL_VERSION + 1),
            ..legacy.clone()
        };

        assert!(legacy.protocol_is_supported());
        assert!(!future.protocol_is_supported());
    }
}
