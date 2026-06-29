//! Shared Godworks OS domain types.
//!
//! This crate is intentionally small at bootstrap time. It gives protocol, SDK,
//! testkit, and broker refactors a stable place to converge without changing the
//! current broker runtime behavior in the first hardening PR.

/// A stable entity identifier as seen on the Godworks wire.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityId(pub String);

impl From<&str> for EntityId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for EntityId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for EntityId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A component name as used by the component-authority model.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentName(pub String);

impl From<&str> for ComponentName {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for ComponentName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for ComponentName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A zone/region identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RegionId(pub String);

impl From<&str> for RegionId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for RegionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for RegionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A worker/client/broker-peer identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub String);

impl From<&str> for PeerId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for PeerId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for PeerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// 2D position for the 1.0 product target.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Position2 {
    pub x: f64,
    pub y: f64,
}

impl Position2 {
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub const fn to_array(self) -> [f64; 2] {
        [self.x, self.y]
    }
}

impl From<[f64; 2]> for Position2 {
    fn from(value: [f64; 2]) -> Self {
        Self {
            x: value[0],
            y: value[1],
        }
    }
}

impl From<Position2> for [f64; 2] {
    fn from(value: Position2) -> Self {
        value.to_array()
    }
}

/// 2D velocity for the 1.0 product target.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Velocity2 {
    pub x: f64,
    pub y: f64,
}

impl Velocity2 {
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub const fn to_array(self) -> [f64; 2] {
        [self.x, self.y]
    }
}

impl From<[f64; 2]> for Velocity2 {
    fn from(value: [f64; 2]) -> Self {
        Self {
            x: value[0],
            y: value[1],
        }
    }
}

impl From<Velocity2> for [f64; 2] {
    fn from(value: Velocity2) -> Self {
        value.to_array()
    }
}

/// Authority mode for a component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorityMode {
    ClientForwardSparse,
    ServerArbitrated,
    ServerPhysicsIsland,
    ThresholdOverlap,
    PersistentKernelLock,
}

impl AuthorityMode {
    pub const fn as_wire_str(&self) -> &'static str {
        match self {
            Self::ClientForwardSparse => "client_forward_sparse",
            Self::ServerArbitrated => "server_arbitrated",
            Self::ServerPhysicsIsland => "server_physics_island",
            Self::ThresholdOverlap => "threshold_overlap",
            Self::PersistentKernelLock => "persistent_kernel_lock",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "client_forward_sparse" => Some(Self::ClientForwardSparse),
            "server_arbitrated" => Some(Self::ServerArbitrated),
            "server_physics_island" => Some(Self::ServerPhysicsIsland),
            "threshold_overlap" => Some(Self::ThresholdOverlap),
            "persistent_kernel_lock" => Some(Self::PersistentKernelLock),
            _ => None,
        }
    }
}

/// Component authority snapshot that can be shared by broker, protocol, and SDK code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentAuthority {
    pub owner: Option<PeerId>,
    pub epoch: u64,
    pub mode: AuthorityMode,
}

/// Current 2D area-of-interest shape set.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Aoi2 {
    Circle { center: Position2, radius: f64 },
    Box { min: Position2, max: Position2 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_mode_roundtrips_wire_names() {
        let modes = [
            AuthorityMode::ClientForwardSparse,
            AuthorityMode::ServerArbitrated,
            AuthorityMode::ServerPhysicsIsland,
            AuthorityMode::ThresholdOverlap,
            AuthorityMode::PersistentKernelLock,
        ];

        for mode in modes {
            assert_eq!(
                AuthorityMode::from_wire_str(mode.as_wire_str()),
                Some(mode.clone())
            );
        }
    }

    #[test]
    fn position_velocity_array_conversion_matches_current_wire_shape() {
        let p = Position2::from([1.0, 2.0]);
        let v = Velocity2::from([3.0, 4.0]);

        assert_eq!(p.to_array(), [1.0, 2.0]);
        assert_eq!(v.to_array(), [3.0, 4.0]);
    }
}
