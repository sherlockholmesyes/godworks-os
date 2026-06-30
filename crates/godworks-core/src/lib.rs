//! Shared Godworks OS domain types.
//!
//! This crate is intentionally small at bootstrap time. It gives protocol, SDK,
//! testkit, and broker refactors a stable place to converge without changing the
//! current broker runtime behavior in the first hardening PR.

use std::collections::{BTreeMap, BTreeSet};

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

/// Stable numeric identity for a component schema.
///
/// The current JSON wire still uses component names such as `pos` and `vel`.
/// This id is the compatibility anchor for snapshots, replay/eval, SDKs, and a
/// future binary codec where renames must not change identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentId(pub u32);

impl ComponentId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Monotonic schema version for one component id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentVersion(pub u16);

impl ComponentVersion {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

/// The standard registry schema version carried by artifacts such as
/// `SnapshotManifest`.
pub const STANDARD_COMPONENT_REGISTRY_VERSION: u64 = 1;

/// Coarse component family. This is intentionally descriptive, not a runtime
/// authority decision; authority still lives in the broker per component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentKind {
    Spatial,
    Physics,
    Gameplay,
    Kernel,
    Metadata,
}

impl ComponentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spatial => "spatial",
            Self::Physics => "physics",
            Self::Gameplay => "gameplay",
            Self::Kernel => "kernel",
            Self::Metadata => "metadata",
        }
    }
}

/// One stable component schema entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComponentSchema {
    pub id: ComponentId,
    pub version: ComponentVersion,
    pub name: &'static str,
    pub kind: ComponentKind,
    pub aliases: &'static [&'static str],
}

impl ComponentSchema {
    pub const fn new(
        id: u32,
        version: u16,
        name: &'static str,
        kind: ComponentKind,
        aliases: &'static [&'static str],
    ) -> Self {
        Self {
            id: ComponentId::new(id),
            version: ComponentVersion::new(version),
            name,
            kind,
            aliases,
        }
    }

    pub fn names(self) -> impl Iterator<Item = &'static str> {
        std::iter::once(self.name).chain(self.aliases.iter().copied())
    }
}

pub const COMPONENT_POS2: ComponentSchema =
    ComponentSchema::new(10_001, 1, "pos", ComponentKind::Spatial, &["core.pos2"]);
pub const COMPONENT_VEL2: ComponentSchema =
    ComponentSchema::new(10_002, 1, "vel", ComponentKind::Spatial, &["core.vel2"]);
pub const COMPONENT_GEN: ComponentSchema =
    ComponentSchema::new(10_003, 1, "gen", ComponentKind::Metadata, &["core.gen"]);
pub const COMPONENT_SIM_TIME: ComponentSchema = ComponentSchema::new(
    10_004,
    1,
    "sim_time",
    ComponentKind::Metadata,
    &["core.sim_time"],
);
pub const COMPONENT_KIND: ComponentSchema =
    ComponentSchema::new(10_005, 1, "kind", ComponentKind::Metadata, &["core.kind"]);
pub const COMPONENT_PARENT: ComponentSchema =
    ComponentSchema::new(10_006, 1, "parent", ComponentKind::Kernel, &["core.parent"]);
pub const COMPONENT_ROT: ComponentSchema =
    ComponentSchema::new(10_020, 1, "rot", ComponentKind::Physics, &[]);
pub const COMPONENT_LIN: ComponentSchema =
    ComponentSchema::new(10_021, 1, "lin", ComponentKind::Physics, &[]);
pub const COMPONENT_ANG: ComponentSchema =
    ComponentSchema::new(10_022, 1, "ang", ComponentKind::Physics, &[]);
pub const COMPONENT_AT_REST: ComponentSchema = ComponentSchema::new(
    10_023,
    1,
    "at_rest",
    ComponentKind::Physics,
    &["core.at_rest"],
);
pub const COMPONENT_POS3: ComponentSchema =
    ComponentSchema::new(11_001, 1, "core.pos3", ComponentKind::Spatial, &[]);
pub const COMPONENT_VEL3: ComponentSchema =
    ComponentSchema::new(11_002, 1, "core.vel3", ComponentKind::Spatial, &[]);
pub const COMPONENT_ROT3: ComponentSchema =
    ComponentSchema::new(11_003, 1, "core.rot3", ComponentKind::Physics, &[]);
pub const COMPONENT_LIN3: ComponentSchema =
    ComponentSchema::new(11_004, 1, "core.lin3", ComponentKind::Physics, &[]);
pub const COMPONENT_ANG3: ComponentSchema =
    ComponentSchema::new(11_005, 1, "core.ang3", ComponentKind::Physics, &[]);
pub const COMPONENT_LOCAL_FRAME: ComponentSchema =
    ComponentSchema::new(11_006, 1, "core.local_frame", ComponentKind::Spatial, &[]);
pub const COMPONENT_PHYSICS_BODY: ComponentSchema =
    ComponentSchema::new(11_007, 1, "core.physics_body", ComponentKind::Physics, &[]);

/// Built-in registry entries that define the stable identity floor for current
/// 2D components and the future 3D rail. Game-specific components can extend
/// this registry in higher layers; they should not reuse these ids.
pub const STANDARD_COMPONENT_SCHEMAS: &[ComponentSchema] = &[
    COMPONENT_POS2,
    COMPONENT_VEL2,
    COMPONENT_GEN,
    COMPONENT_SIM_TIME,
    COMPONENT_KIND,
    COMPONENT_PARENT,
    COMPONENT_ROT,
    COMPONENT_LIN,
    COMPONENT_ANG,
    COMPONENT_AT_REST,
    COMPONENT_POS3,
    COMPONENT_VEL3,
    COMPONENT_ROT3,
    COMPONENT_LIN3,
    COMPONENT_ANG3,
    COMPONENT_LOCAL_FRAME,
    COMPONENT_PHYSICS_BODY,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentRegistryError {
    DuplicateId {
        id: ComponentId,
        first: String,
        second: String,
    },
    DuplicateName {
        name: String,
        first: ComponentId,
        second: ComponentId,
    },
}

/// A stable id/name lookup. The registry accepts aliases so current legacy JSON
/// names can coexist with future canonical SDK names without changing identity.
#[derive(Clone, Debug)]
pub struct ComponentRegistry {
    entries: Vec<ComponentSchema>,
    by_id: BTreeMap<ComponentId, ComponentSchema>,
    by_name: BTreeMap<&'static str, ComponentSchema>,
}

impl ComponentRegistry {
    pub fn new(entries: &[ComponentSchema]) -> Result<Self, ComponentRegistryError> {
        let mut by_id = BTreeMap::new();
        let mut by_name = BTreeMap::new();
        let mut seen_names = BTreeSet::new();

        for entry in entries {
            if let Some(first) = by_id.insert(entry.id, *entry) {
                return Err(ComponentRegistryError::DuplicateId {
                    id: entry.id,
                    first: first.name.to_string(),
                    second: entry.name.to_string(),
                });
            }
            for name in entry.names() {
                if !seen_names.insert(name) {
                    let first = by_name
                        .get(name)
                        .map(|schema: &ComponentSchema| schema.id)
                        .unwrap_or(entry.id);
                    return Err(ComponentRegistryError::DuplicateName {
                        name: name.to_string(),
                        first,
                        second: entry.id,
                    });
                }
                by_name.insert(name, *entry);
            }
        }

        Ok(Self {
            entries: entries.to_vec(),
            by_id,
            by_name,
        })
    }

    pub fn standard() -> Self {
        Self::new(STANDARD_COMPONENT_SCHEMAS)
            .expect("standard component registry must be internally consistent")
    }

    pub fn version(&self) -> u64 {
        STANDARD_COMPONENT_REGISTRY_VERSION
    }

    pub fn entries(&self) -> &[ComponentSchema] {
        &self.entries
    }

    pub fn get_by_id(&self, id: ComponentId) -> Option<ComponentSchema> {
        self.by_id.get(&id).copied()
    }

    pub fn resolve_name(&self, name: &str) -> Option<ComponentSchema> {
        self.by_name.get(name).copied()
    }

    pub fn canonical_name(&self, id: ComponentId) -> Option<&'static str> {
        self.get_by_id(id).map(|schema| schema.name)
    }

    pub fn id_for_name(&self, name: &str) -> Option<ComponentId> {
        self.resolve_name(name).map(|schema| schema.id)
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

    #[test]
    fn standard_component_registry_resolves_legacy_and_future_names() {
        let registry = ComponentRegistry::standard();

        assert_eq!(registry.version(), STANDARD_COMPONENT_REGISTRY_VERSION);
        assert_eq!(registry.id_for_name("pos"), Some(COMPONENT_POS2.id));
        assert_eq!(registry.id_for_name("core.pos2"), Some(COMPONENT_POS2.id));
        assert_eq!(registry.id_for_name("vel"), Some(COMPONENT_VEL2.id));
        assert_eq!(registry.id_for_name("core.vel2"), Some(COMPONENT_VEL2.id));
        assert_eq!(registry.id_for_name("core.pos3"), Some(COMPONENT_POS3.id));
        assert_eq!(
            registry.canonical_name(COMPONENT_POS2.id),
            Some(COMPONENT_POS2.name)
        );
        assert_eq!(
            registry.resolve_name("core.pos2").map(|schema| schema.name),
            Some("pos")
        );
    }

    #[test]
    fn standard_component_registry_has_unique_ids_and_names() {
        let registry = ComponentRegistry::standard();

        assert_eq!(registry.entries().len(), STANDARD_COMPONENT_SCHEMAS.len());
        for schema in STANDARD_COMPONENT_SCHEMAS {
            assert_eq!(registry.get_by_id(schema.id), Some(*schema));
            for name in schema.names() {
                assert_eq!(registry.resolve_name(name), Some(*schema));
            }
        }
    }

    #[test]
    fn standard_component_registry_table_is_the_compatibility_floor() {
        let table: Vec<(u32, &str, &str)> = STANDARD_COMPONENT_SCHEMAS
            .iter()
            .map(|schema| (schema.id.get(), schema.name, schema.kind.as_str()))
            .collect();

        assert_eq!(
            table,
            vec![
                (10_001, "pos", "spatial"),
                (10_002, "vel", "spatial"),
                (10_003, "gen", "metadata"),
                (10_004, "sim_time", "metadata"),
                (10_005, "kind", "metadata"),
                (10_006, "parent", "kernel"),
                (10_020, "rot", "physics"),
                (10_021, "lin", "physics"),
                (10_022, "ang", "physics"),
                (10_023, "at_rest", "physics"),
                (11_001, "core.pos3", "spatial"),
                (11_002, "core.vel3", "spatial"),
                (11_003, "core.rot3", "physics"),
                (11_004, "core.lin3", "physics"),
                (11_005, "core.ang3", "physics"),
                (11_006, "core.local_frame", "spatial"),
                (11_007, "core.physics_body", "physics"),
            ]
        );
    }

    #[test]
    fn component_registry_rejects_duplicate_id_or_name() {
        let duplicate_id = [
            ComponentSchema::new(90_001, 1, "game.a", ComponentKind::Gameplay, &[]),
            ComponentSchema::new(90_001, 1, "game.b", ComponentKind::Gameplay, &[]),
        ];
        assert!(matches!(
            ComponentRegistry::new(&duplicate_id),
            Err(ComponentRegistryError::DuplicateId { .. })
        ));

        let duplicate_name = [
            ComponentSchema::new(90_001, 1, "game.a", ComponentKind::Gameplay, &[]),
            ComponentSchema::new(90_002, 1, "game.b", ComponentKind::Gameplay, &["game.a"]),
        ];
        assert!(matches!(
            ComponentRegistry::new(&duplicate_name),
            Err(ComponentRegistryError::DuplicateName { .. })
        ));
    }
}
