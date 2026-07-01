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

/// Current spatial schema version carried by replay/snapshot artifacts.
pub const SPATIAL_SCHEMA_VERSION: u64 = 1;

/// Current coordinate codec version carried by replay/snapshot artifacts.
pub const COORDINATE_CODEC_VERSION: u64 = 1;

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

/// Spatial dimension contract for replay/snapshot artifacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpatialDim {
    D2,
    D3,
}

impl SpatialDim {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::D2 => "D2",
            Self::D3 => "D3",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "D2" => Some(Self::D2),
            "D3" => Some(Self::D3),
            _ => None,
        }
    }
}

/// Coordinate codec contract for replay/snapshot artifacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinateCodec {
    DebugF64_2,
    DebugF64_3,
}

impl CoordinateCodec {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::DebugF64_2 => "debug_f64_2",
            Self::DebugF64_3 => "debug_f64_3",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "debug_f64_2" => Some(Self::DebugF64_2),
            "debug_f64_3" => Some(Self::DebugF64_3),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionSchemaError {
    ZeroGridDimension,
}

/// Partition topology contract. This describes artifact/schema shape, not the
/// full runtime partition map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionSchema {
    Strip1D { boundary_count: u64 },
    Grid2D { cols: u64, rows: u64 },
    Grid3D { cols: u64, rows: u64, layers: u64 },
}

impl PartitionSchema {
    pub const fn strip1d(boundary_count: u64) -> Self {
        Self::Strip1D { boundary_count }
    }

    pub const fn grid2d(cols: u64, rows: u64) -> Result<Self, PartitionSchemaError> {
        if cols == 0 || rows == 0 {
            Err(PartitionSchemaError::ZeroGridDimension)
        } else {
            Ok(Self::Grid2D { cols, rows })
        }
    }

    pub const fn grid3d(cols: u64, rows: u64, layers: u64) -> Result<Self, PartitionSchemaError> {
        if cols == 0 || rows == 0 || layers == 0 {
            Err(PartitionSchemaError::ZeroGridDimension)
        } else {
            Ok(Self::Grid3D { cols, rows, layers })
        }
    }

    pub const fn kind(self) -> &'static str {
        match self {
            Self::Strip1D { .. } => "strip1d",
            Self::Grid2D { .. } => "grid2d",
            Self::Grid3D { .. } => "grid3d",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartitionMapSpecError {
    NonFiniteBoundary,
    UnsortedBoundary,
    NonFiniteGridCell,
    NonFiniteGridOrigin,
    NonPositiveGridCell,
}

/// A deterministic split inside one coarse 1D strip region.
#[derive(Clone, Debug, PartialEq)]
pub struct RegionSplitSpec {
    pub region: RegionId,
    pub boundaries: Vec<f64>,
}

impl RegionSplitSpec {
    pub fn new(
        region: impl Into<RegionId>,
        boundaries: Vec<f64>,
    ) -> Result<Self, PartitionMapSpecError> {
        validate_sorted_finite(&boundaries)?;
        Ok(Self {
            region: region.into(),
            boundaries,
        })
    }
}

/// The reproducible runtime partition map behind a `PartitionSchema`.
///
/// `PartitionSchema` describes artifact shape (`strip1d` vs `grid2d`). This
/// type carries the actual deterministic routing inputs that a snapshot or
/// external tool needs to reproduce region assignment for the current map.
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionMapSpec {
    Strip1D {
        boundaries: Vec<f64>,
        splits: Vec<RegionSplitSpec>,
    },
    Grid2D {
        cols: u64,
        rows: u64,
        cell_w: f64,
        cell_h: f64,
        origin: [f64; 2],
    },
    Grid3D {
        cols: u64,
        rows: u64,
        layers: u64,
        cell_w: f64,
        cell_h: f64,
        cell_d: f64,
        origin: [f64; 3],
    },
}

impl PartitionMapSpec {
    pub fn strip1d(
        boundaries: Vec<f64>,
        mut splits: Vec<RegionSplitSpec>,
    ) -> Result<Self, PartitionMapSpecError> {
        validate_sorted_finite(&boundaries)?;
        splits.sort_by(|a, b| a.region.cmp(&b.region));
        Ok(Self::Strip1D { boundaries, splits })
    }

    pub fn grid2d(
        cols: u64,
        rows: u64,
        cell_w: f64,
        cell_h: f64,
        origin: [f64; 2],
    ) -> Result<Self, PartitionMapSpecError> {
        PartitionSchema::grid2d(cols, rows)
            .map_err(|_| PartitionMapSpecError::NonPositiveGridCell)?;
        if !cell_w.is_finite() || !cell_h.is_finite() {
            return Err(PartitionMapSpecError::NonFiniteGridCell);
        }
        if cell_w <= 0.0 || cell_h <= 0.0 {
            return Err(PartitionMapSpecError::NonPositiveGridCell);
        }
        if !origin[0].is_finite() || !origin[1].is_finite() {
            return Err(PartitionMapSpecError::NonFiniteGridOrigin);
        }
        Ok(Self::Grid2D {
            cols,
            rows,
            cell_w,
            cell_h,
            origin,
        })
    }

    pub fn grid3d(
        cols: u64,
        rows: u64,
        layers: u64,
        cell_w: f64,
        cell_h: f64,
        cell_d: f64,
        origin: [f64; 3],
    ) -> Result<Self, PartitionMapSpecError> {
        PartitionSchema::grid3d(cols, rows, layers)
            .map_err(|_| PartitionMapSpecError::NonPositiveGridCell)?;
        if !cell_w.is_finite() || !cell_h.is_finite() || !cell_d.is_finite() {
            return Err(PartitionMapSpecError::NonFiniteGridCell);
        }
        if cell_w <= 0.0 || cell_h <= 0.0 || cell_d <= 0.0 {
            return Err(PartitionMapSpecError::NonPositiveGridCell);
        }
        if !origin[0].is_finite() || !origin[1].is_finite() || !origin[2].is_finite() {
            return Err(PartitionMapSpecError::NonFiniteGridOrigin);
        }
        Ok(Self::Grid3D {
            cols,
            rows,
            layers,
            cell_w,
            cell_h,
            cell_d,
            origin,
        })
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Strip1D { .. } => "strip1d",
            Self::Grid2D { .. } => "grid2d",
            Self::Grid3D { .. } => "grid3d",
        }
    }

    pub fn partition_schema(&self) -> PartitionSchema {
        match self {
            Self::Strip1D { boundaries, .. } => PartitionSchema::Strip1D {
                boundary_count: boundaries.len() as u64,
            },
            Self::Grid2D { cols, rows, .. } => PartitionSchema::Grid2D {
                cols: *cols,
                rows: *rows,
            },
            Self::Grid3D {
                cols, rows, layers, ..
            } => PartitionSchema::Grid3D {
                cols: *cols,
                rows: *rows,
                layers: *layers,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VersionedPartitionMap {
    pub version: u64,
    pub spec: PartitionMapSpec,
}

impl VersionedPartitionMap {
    pub const fn new(version: u64, spec: PartitionMapSpec) -> Self {
        Self { version, spec }
    }
}

fn validate_sorted_finite(values: &[f64]) -> Result<(), PartitionMapSpecError> {
    let mut prev = None;
    for value in values {
        if !value.is_finite() {
            return Err(PartitionMapSpecError::NonFiniteBoundary);
        }
        if let Some(prev) = prev {
            if *value <= prev {
                return Err(PartitionMapSpecError::UnsortedBoundary);
            }
        }
        prev = Some(*value);
    }
    Ok(())
}

/// The current spatial artifact contract shared by replay and snapshot rails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpatialSchema {
    pub spatial_dim: SpatialDim,
    pub coordinate_codec: CoordinateCodec,
    pub partition_schema: PartitionSchema,
}

impl SpatialSchema {
    pub const fn current_2d(partition_schema: PartitionSchema) -> Self {
        Self {
            spatial_dim: SpatialDim::D2,
            coordinate_codec: CoordinateCodec::DebugF64_2,
            partition_schema,
        }
    }

    pub const fn future_3d(partition_schema: PartitionSchema) -> Self {
        Self {
            spatial_dim: SpatialDim::D3,
            coordinate_codec: CoordinateCodec::DebugF64_3,
            partition_schema,
        }
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

    pub const fn authority_group(&self) -> AuthorityGroupKind {
        match self {
            Self::ClientForwardSparse | Self::ServerArbitrated => AuthorityGroupKind::Gameplay,
            Self::ServerPhysicsIsland => AuthorityGroupKind::PhysicsIsland,
            Self::ThresholdOverlap => AuthorityGroupKind::Threshold,
            Self::PersistentKernelLock => AuthorityGroupKind::Kernel,
        }
    }
}

/// Atomic authority group for components whose authority must move together.
///
/// This is a contract label, not a replacement for per-component authority.
/// Runtime can still own components independently unless a component is in an
/// explicit authority group such as the physics island.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuthorityGroupKind {
    PhysicsIsland,
    Gameplay,
    Threshold,
    Kernel,
}

impl AuthorityGroupKind {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::PhysicsIsland => "physics_island",
            Self::Gameplay => "gameplay",
            Self::Threshold => "threshold",
            Self::Kernel => "kernel",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "physics_island" => Some(Self::PhysicsIsland),
            "gameplay" => Some(Self::Gameplay),
            "threshold" => Some(Self::Threshold),
            "kernel" => Some(Self::Kernel),
            _ => None,
        }
    }
}

pub fn standard_component_schema_for_name(name: &str) -> Option<ComponentSchema> {
    STANDARD_COMPONENT_SCHEMAS
        .iter()
        .copied()
        .find(|schema| schema.names().any(|candidate| candidate == name))
}

pub fn standard_component_authority_group(name: &str) -> Option<AuthorityGroupKind> {
    let schema = standard_component_schema_for_name(name)?;
    match schema.id.get() {
        10_001 | 10_002 | 10_003 | 10_004 | 10_020 | 10_021 | 10_022 | 10_023 | 11_001 | 11_002
        | 11_003 | 11_004 | 11_005 | 11_006 | 11_007 => Some(AuthorityGroupKind::PhysicsIsland),
        10_006 => Some(AuthorityGroupKind::Kernel),
        _ => Some(AuthorityGroupKind::Gameplay),
    }
}

pub fn standard_component_default_authority_mode(name: &str) -> Option<AuthorityMode> {
    match standard_component_authority_group(name)? {
        AuthorityGroupKind::PhysicsIsland => Some(AuthorityMode::ServerPhysicsIsland),
        AuthorityGroupKind::Kernel => Some(AuthorityMode::PersistentKernelLock),
        AuthorityGroupKind::Gameplay => Some(AuthorityMode::ServerArbitrated),
        AuthorityGroupKind::Threshold => Some(AuthorityMode::ThresholdOverlap),
    }
}

/// Component authority snapshot that can be shared by broker, protocol, and SDK code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentAuthority {
    pub owner: Option<PeerId>,
    pub epoch: u64,
    pub mode: AuthorityMode,
}

/// Current schema version for model-plane feature blocks.
pub const MODEL_FEATURE_BLOCK_SCHEMA_VERSION: u64 = 1;

/// Typed feature families the model plane may learn from.
///
/// These are observations, not runtime commands. Runtime mutation remains under
/// broker validators, epochs, WAL, and versioned activation contracts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelFeatureBlockKind {
    WorkerLoad,
    AoiFidelityPressure,
    EntityDensity,
    HandoffPressure,
    IngressRejectCost,
    WalSync,
    Outcome,
}

impl ModelFeatureBlockKind {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::WorkerLoad => "WorkerLoad",
            Self::AoiFidelityPressure => "AoiFidelityPressure",
            Self::EntityDensity => "EntityDensity",
            Self::HandoffPressure => "HandoffPressure",
            Self::IngressRejectCost => "IngressRejectCost",
            Self::WalSync => "WalSync",
            Self::Outcome => "Outcome",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "WorkerLoad" => Some(Self::WorkerLoad),
            "AoiFidelityPressure" => Some(Self::AoiFidelityPressure),
            "EntityDensity" => Some(Self::EntityDensity),
            "HandoffPressure" => Some(Self::HandoffPressure),
            "IngressRejectCost" => Some(Self::IngressRejectCost),
            "WalSync" => Some(Self::WalSync),
            "Outcome" => Some(Self::Outcome),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelFeatureBlockValidationError {
    EmptyProjectId,
    EmptyDatasetId,
    EmptyTraceId,
    EmptySourceArtifact,
    InvalidSchemaVersion,
    UnredactedFeatureBlock,
    EmptyMetricSet,
    EmptyMetricName,
    NonFiniteMetric { name: String },
    ForbiddenFeatureName { name: String },
    EmptyDimensionName,
    EmptyDimensionValue { name: String },
}

/// Provenance for a project-local model-plane feature block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelFeatureBlockProvenance {
    pub project_id: String,
    pub dataset_id: String,
    pub trace_id: String,
    pub source_artifact: String,
    pub schema_version: u64,
}

impl ModelFeatureBlockProvenance {
    pub fn new(
        project_id: impl Into<String>,
        dataset_id: impl Into<String>,
        trace_id: impl Into<String>,
        source_artifact: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            dataset_id: dataset_id.into(),
            trace_id: trace_id.into(),
            source_artifact: source_artifact.into(),
            schema_version: MODEL_FEATURE_BLOCK_SCHEMA_VERSION,
        }
    }

    pub const fn with_schema_version(mut self, schema_version: u64) -> Self {
        self.schema_version = schema_version;
        self
    }
}

/// Redacted, replayable observation block for project-local micro-models.
///
/// A feature block may summarize runtime/replay/loadgen pressure, but it cannot
/// carry raw auth tokens or component/update payload bodies. Keeping this typed
/// is the first guardrail between "learn from traces" and "hidden control
/// plane".
#[derive(Clone, Debug, PartialEq)]
pub struct ModelFeatureBlock {
    pub kind: ModelFeatureBlockKind,
    pub provenance: ModelFeatureBlockProvenance,
    pub redacted: bool,
    pub metrics: BTreeMap<String, f64>,
    pub dimensions: BTreeMap<String, String>,
}

impl ModelFeatureBlock {
    pub fn new(kind: ModelFeatureBlockKind, provenance: ModelFeatureBlockProvenance) -> Self {
        Self {
            kind,
            provenance,
            redacted: true,
            metrics: BTreeMap::new(),
            dimensions: BTreeMap::new(),
        }
    }

    pub fn with_metric(mut self, name: impl Into<String>, value: f64) -> Self {
        self.metrics.insert(name.into(), value);
        self
    }

    pub fn with_dimension(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.dimensions.insert(name.into(), value.into());
        self
    }

    pub const fn with_redacted(mut self, redacted: bool) -> Self {
        self.redacted = redacted;
        self
    }

    pub fn validate(&self) -> Result<(), ModelFeatureBlockValidationError> {
        if self.provenance.project_id.trim().is_empty() {
            return Err(ModelFeatureBlockValidationError::EmptyProjectId);
        }
        if self.provenance.dataset_id.trim().is_empty() {
            return Err(ModelFeatureBlockValidationError::EmptyDatasetId);
        }
        if self.provenance.trace_id.trim().is_empty() {
            return Err(ModelFeatureBlockValidationError::EmptyTraceId);
        }
        if self.provenance.source_artifact.trim().is_empty() {
            return Err(ModelFeatureBlockValidationError::EmptySourceArtifact);
        }
        if self.provenance.schema_version == 0 {
            return Err(ModelFeatureBlockValidationError::InvalidSchemaVersion);
        }
        if !self.redacted {
            return Err(ModelFeatureBlockValidationError::UnredactedFeatureBlock);
        }
        if self.metrics.is_empty() {
            return Err(ModelFeatureBlockValidationError::EmptyMetricSet);
        }

        for (name, value) in &self.metrics {
            validate_feature_name(name)?;
            if !value.is_finite() {
                return Err(ModelFeatureBlockValidationError::NonFiniteMetric {
                    name: name.clone(),
                });
            }
        }

        for (name, value) in &self.dimensions {
            validate_feature_name(name)?;
            if value.trim().is_empty() {
                return Err(ModelFeatureBlockValidationError::EmptyDimensionValue {
                    name: name.clone(),
                });
            }
            validate_feature_name(value)?;
        }

        Ok(())
    }
}

fn validate_feature_name(name: &str) -> Result<(), ModelFeatureBlockValidationError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ModelFeatureBlockValidationError::EmptyMetricName);
    }
    let lower = trimmed.to_ascii_lowercase();
    for forbidden in [
        "auth_token",
        "token",
        "secret",
        "password",
        "payload",
        "component_body",
        "components",
        "updates",
    ] {
        if lower.contains(forbidden) {
            return Err(ModelFeatureBlockValidationError::ForbiddenFeatureName {
                name: name.to_string(),
            });
        }
    }
    Ok(())
}

/// Promotion mode for a model-plane proposal.
///
/// Model output may rank or recommend policy actions, but the deterministic
/// broker kernel stays the only place where authority, WAL, and component state
/// are mutated. `Guarded` means a validator may consider the proposal for a
/// later runtime action; it is still not a direct mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelActionMode {
    Observe,
    Shadow,
    Advisor,
    Guarded,
}

impl ModelActionMode {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Shadow => "shadow",
            Self::Advisor => "advisor",
            Self::Guarded => "guarded",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "observe" => Some(Self::Observe),
            "shadow" => Some(Self::Shadow),
            "advisor" => Some(Self::Advisor),
            "guarded" => Some(Self::Guarded),
            _ => None,
        }
    }
}

/// Public, validator-gated action vocabulary for the future model plane.
///
/// These are proposals. Runtime mutations such as authority grants, component
/// writes, WAL bypasses, or direct partition activation are intentionally not
/// representable here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelActionKind {
    RecommendPartitionMap,
    AdjustInterestFidelity,
    RecommendWorkerScale,
    MarkHandoffRisk,
    AntiCheatFlag,
    NpcIntent,
    Noop,
}

impl ModelActionKind {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::RecommendPartitionMap => "RecommendPartitionMap",
            Self::AdjustInterestFidelity => "AdjustInterestFidelity",
            Self::RecommendWorkerScale => "RecommendWorkerScale",
            Self::MarkHandoffRisk => "MarkHandoffRisk",
            Self::AntiCheatFlag => "AntiCheatFlag",
            Self::NpcIntent => "NpcIntent",
            Self::Noop => "Noop",
        }
    }

    pub fn from_wire_str(value: &str) -> Option<Self> {
        match value {
            "RecommendPartitionMap" => Some(Self::RecommendPartitionMap),
            "AdjustInterestFidelity" => Some(Self::AdjustInterestFidelity),
            "RecommendWorkerScale" => Some(Self::RecommendWorkerScale),
            "MarkHandoffRisk" => Some(Self::MarkHandoffRisk),
            "AntiCheatFlag" => Some(Self::AntiCheatFlag),
            "NpcIntent" => Some(Self::NpcIntent),
            "Noop" => Some(Self::Noop),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelActionValidationError {
    EmptyProjectId,
    EmptyModelId,
    EmptyDatasetId,
    EmptySourceTraceId,
    GuardedWithoutValidator,
}

/// Provenance required before model output can be considered by runtime policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelActionProvenance {
    pub project_id: String,
    pub model_id: String,
    pub dataset_id: String,
    pub source_trace_id: String,
}

impl ModelActionProvenance {
    pub fn new(
        project_id: impl Into<String>,
        model_id: impl Into<String>,
        dataset_id: impl Into<String>,
        source_trace_id: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            model_id: model_id.into(),
            dataset_id: dataset_id.into(),
            source_trace_id: source_trace_id.into(),
        }
    }
}

/// A model-plane proposal with enough provenance for replay, rejection, and
/// promotion auditing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelActionProposal {
    pub kind: ModelActionKind,
    pub mode: ModelActionMode,
    pub provenance: ModelActionProvenance,
    pub validator_id: Option<String>,
}

impl ModelActionProposal {
    pub fn new(
        kind: ModelActionKind,
        mode: ModelActionMode,
        provenance: ModelActionProvenance,
    ) -> Self {
        Self {
            kind,
            mode,
            provenance,
            validator_id: None,
        }
    }

    pub fn with_validator(mut self, validator_id: impl Into<String>) -> Self {
        self.validator_id = Some(validator_id.into());
        self
    }

    pub fn validate(&self) -> Result<(), ModelActionValidationError> {
        if self.provenance.project_id.trim().is_empty() {
            return Err(ModelActionValidationError::EmptyProjectId);
        }
        if self.provenance.model_id.trim().is_empty() {
            return Err(ModelActionValidationError::EmptyModelId);
        }
        if self.provenance.dataset_id.trim().is_empty() {
            return Err(ModelActionValidationError::EmptyDatasetId);
        }
        if self.provenance.source_trace_id.trim().is_empty() {
            return Err(ModelActionValidationError::EmptySourceTraceId);
        }
        if self.mode == ModelActionMode::Guarded
            && self
                .validator_id
                .as_ref()
                .map(|id| id.trim().is_empty())
                .unwrap_or(true)
        {
            return Err(ModelActionValidationError::GuardedWithoutValidator);
        }
        Ok(())
    }
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
    fn authority_modes_project_to_typed_authority_groups() {
        assert_eq!(
            AuthorityMode::ServerPhysicsIsland.authority_group(),
            AuthorityGroupKind::PhysicsIsland
        );
        assert_eq!(
            AuthorityMode::ServerArbitrated.authority_group(),
            AuthorityGroupKind::Gameplay
        );
        assert_eq!(
            AuthorityMode::ClientForwardSparse.authority_group(),
            AuthorityGroupKind::Gameplay
        );
        assert_eq!(
            AuthorityMode::ThresholdOverlap.authority_group(),
            AuthorityGroupKind::Threshold
        );
        assert_eq!(
            AuthorityMode::PersistentKernelLock.authority_group(),
            AuthorityGroupKind::Kernel
        );

        for group in [
            AuthorityGroupKind::PhysicsIsland,
            AuthorityGroupKind::Gameplay,
            AuthorityGroupKind::Threshold,
            AuthorityGroupKind::Kernel,
        ] {
            assert_eq!(
                AuthorityGroupKind::from_wire_str(group.as_wire_str()),
                Some(group)
            );
        }
    }

    #[test]
    fn standard_component_authority_groups_pin_2d_and_3d_physics_island() {
        for name in [
            "pos",
            "core.pos2",
            "vel",
            "core.vel2",
            "gen",
            "sim_time",
            "rot",
            "lin",
            "ang",
            "at_rest",
            "core.pos3",
            "core.vel3",
            "core.rot3",
            "core.lin3",
            "core.ang3",
            "core.local_frame",
            "core.physics_body",
        ] {
            assert_eq!(
                standard_component_authority_group(name),
                Some(AuthorityGroupKind::PhysicsIsland),
                "{name}"
            );
            assert_eq!(
                standard_component_default_authority_mode(name),
                Some(AuthorityMode::ServerPhysicsIsland),
                "{name}"
            );
        }

        assert_eq!(
            standard_component_authority_group("core.kind"),
            Some(AuthorityGroupKind::Gameplay)
        );
        assert_eq!(
            standard_component_default_authority_mode("core.kind"),
            Some(AuthorityMode::ServerArbitrated)
        );
        assert_eq!(
            standard_component_authority_group("core.parent"),
            Some(AuthorityGroupKind::Kernel)
        );
        assert_eq!(
            standard_component_default_authority_mode("core.parent"),
            Some(AuthorityMode::PersistentKernelLock)
        );
        assert_eq!(standard_component_authority_group("game.inventory"), None);
        assert_eq!(
            standard_component_default_authority_mode("game.inventory"),
            None
        );
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

    #[test]
    fn partition_schema_rejects_zero_grid_dimensions() {
        assert_eq!(
            PartitionSchema::grid2d(0, 2),
            Err(PartitionSchemaError::ZeroGridDimension)
        );
        assert_eq!(
            PartitionSchema::grid2d(2, 0),
            Err(PartitionSchemaError::ZeroGridDimension)
        );
        assert_eq!(
            PartitionSchema::grid2d(2, 3),
            Ok(PartitionSchema::Grid2D { cols: 2, rows: 3 })
        );
        assert_eq!(
            PartitionSchema::grid3d(2, 3, 0),
            Err(PartitionSchemaError::ZeroGridDimension)
        );
        assert_eq!(
            PartitionSchema::grid3d(2, 3, 4),
            Ok(PartitionSchema::Grid3D {
                cols: 2,
                rows: 3,
                layers: 4
            })
        );
    }

    #[test]
    fn current_spatial_schema_pins_d2_debug_f64_2() {
        let schema = SpatialSchema::current_2d(PartitionSchema::strip1d(1));

        assert_eq!(SPATIAL_SCHEMA_VERSION, 1);
        assert_eq!(COORDINATE_CODEC_VERSION, 1);
        assert_eq!(schema.spatial_dim.as_wire_str(), "D2");
        assert_eq!(schema.coordinate_codec.as_wire_str(), "debug_f64_2");
        assert_eq!(
            schema.partition_schema,
            PartitionSchema::Strip1D { boundary_count: 1 }
        );
    }

    #[test]
    fn future_3d_spatial_schema_pins_d3_debug_f64_3_grid3d() {
        let schema = SpatialSchema::future_3d(
            PartitionSchema::grid3d(2, 3, 4).expect("valid 3D grid schema"),
        );

        assert_eq!(SPATIAL_SCHEMA_VERSION, 1);
        assert_eq!(COORDINATE_CODEC_VERSION, 1);
        assert_eq!(schema.spatial_dim.as_wire_str(), "D3");
        assert_eq!(schema.coordinate_codec.as_wire_str(), "debug_f64_3");
        assert_eq!(
            schema.partition_schema,
            PartitionSchema::Grid3D {
                cols: 2,
                rows: 3,
                layers: 4
            }
        );
    }

    #[test]
    fn partition_map_spec_pins_deterministic_strip_inputs() {
        let map = PartitionMapSpec::strip1d(
            vec![-10.0, 0.0, 10.0],
            vec![
                RegionSplitSpec::new("Z1", vec![1.0, 2.0]).unwrap(),
                RegionSplitSpec::new("Z0", vec![-5.0]).unwrap(),
            ],
        )
        .unwrap();

        let PartitionMapSpec::Strip1D { boundaries, splits } = map else {
            panic!("expected strip map");
        };

        assert_eq!(boundaries, vec![-10.0, 0.0, 10.0]);
        assert_eq!(splits[0].region.as_ref(), "Z0");
        assert_eq!(splits[1].region.as_ref(), "Z1");
    }

    #[test]
    fn partition_map_spec_rejects_non_reproducible_boundaries() {
        assert_eq!(
            PartitionMapSpec::strip1d(vec![1.0, 1.0], Vec::new()),
            Err(PartitionMapSpecError::UnsortedBoundary)
        );
        assert_eq!(
            PartitionMapSpec::strip1d(vec![1.0, f64::NAN], Vec::new()),
            Err(PartitionMapSpecError::NonFiniteBoundary)
        );
        assert_eq!(
            RegionSplitSpec::new("W", vec![3.0, 2.0]),
            Err(PartitionMapSpecError::UnsortedBoundary)
        );
    }

    #[test]
    fn partition_map_spec_rejects_non_reproducible_grid_cells() {
        assert_eq!(
            PartitionMapSpec::grid2d(0, 2, 10.0, 10.0, [0.0, 0.0]),
            Err(PartitionMapSpecError::NonPositiveGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid2d(2, 2, f64::NAN, 10.0, [0.0, 0.0]),
            Err(PartitionMapSpecError::NonFiniteGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid2d(2, 2, 0.0, 10.0, [0.0, 0.0]),
            Err(PartitionMapSpecError::NonPositiveGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid2d(2, 2, 10.0, 10.0, [f64::NAN, 0.0]),
            Err(PartitionMapSpecError::NonFiniteGridOrigin)
        );
        assert_eq!(
            PartitionMapSpec::grid3d(2, 2, 0, 10.0, 10.0, 10.0, [0.0, 0.0, 0.0]),
            Err(PartitionMapSpecError::NonPositiveGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid3d(2, 2, 2, 10.0, f64::NAN, 10.0, [0.0, 0.0, 0.0]),
            Err(PartitionMapSpecError::NonFiniteGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid3d(2, 2, 2, 10.0, 10.0, -1.0, [0.0, 0.0, 0.0]),
            Err(PartitionMapSpecError::NonPositiveGridCell)
        );
        assert_eq!(
            PartitionMapSpec::grid3d(2, 2, 2, 10.0, 10.0, 10.0, [0.0, f64::NAN, 0.0]),
            Err(PartitionMapSpecError::NonFiniteGridOrigin)
        );
    }

    #[test]
    fn partition_map_spec_projects_to_partition_schema() {
        let strip = PartitionMapSpec::strip1d(
            vec![-10.0, 0.0, 10.0],
            vec![RegionSplitSpec::new("Z1", vec![3.0]).unwrap()],
        )
        .unwrap();
        assert_eq!(
            strip.partition_schema(),
            PartitionSchema::Strip1D { boundary_count: 3 }
        );

        let grid = PartitionMapSpec::grid2d(3, 2, 10.0, 20.0, [0.0, 0.0]).unwrap();
        assert_eq!(
            grid.partition_schema(),
            PartitionSchema::Grid2D { cols: 3, rows: 2 }
        );

        let grid3 = PartitionMapSpec::grid3d(3, 2, 4, 10.0, 20.0, 30.0, [0.0, 0.0, 0.0]).unwrap();
        assert_eq!(
            grid3.partition_schema(),
            PartitionSchema::Grid3D {
                cols: 3,
                rows: 2,
                layers: 4
            }
        );
    }

    #[test]
    fn model_action_contract_rejects_direct_runtime_mutation() {
        let allowed = [
            ModelActionKind::RecommendPartitionMap,
            ModelActionKind::AdjustInterestFidelity,
            ModelActionKind::RecommendWorkerScale,
            ModelActionKind::MarkHandoffRisk,
            ModelActionKind::AntiCheatFlag,
            ModelActionKind::NpcIntent,
            ModelActionKind::Noop,
        ];
        for action in allowed {
            assert_eq!(
                ModelActionKind::from_wire_str(action.as_wire_str()),
                Some(action)
            );
        }

        for forbidden in [
            "GrantAuthority",
            "RevokeAuthority",
            "SetComponentAuthority",
            "UpdateComponent",
            "BatchUpdate",
            "MeshHandoff",
            "ActivatePartitionMap",
            "BypassWal",
        ] {
            assert_eq!(ModelActionKind::from_wire_str(forbidden), None);
        }
    }

    #[test]
    fn model_action_proposal_requires_provenance_and_guarded_validator() {
        let provenance = ModelActionProvenance::new("arena", "load-v1", "trace-set-7", "trace-42");
        let proposal = ModelActionProposal::new(
            ModelActionKind::RecommendPartitionMap,
            ModelActionMode::Advisor,
            provenance.clone(),
        );
        assert_eq!(proposal.validate(), Ok(()));

        let guarded = ModelActionProposal::new(
            ModelActionKind::RecommendPartitionMap,
            ModelActionMode::Guarded,
            provenance.clone(),
        );
        assert_eq!(
            guarded.validate(),
            Err(ModelActionValidationError::GuardedWithoutValidator)
        );
        assert_eq!(
            guarded.with_validator("partition-map-validator").validate(),
            Ok(())
        );

        for bad in [
            ModelActionProvenance::new("", "load-v1", "trace-set-7", "trace-42"),
            ModelActionProvenance::new("arena", "", "trace-set-7", "trace-42"),
            ModelActionProvenance::new("arena", "load-v1", "", "trace-42"),
            ModelActionProvenance::new("arena", "load-v1", "trace-set-7", ""),
        ] {
            let proposal =
                ModelActionProposal::new(ModelActionKind::Noop, ModelActionMode::Observe, bad);
            assert!(proposal.validate().is_err());
        }
    }

    #[test]
    fn model_feature_block_requires_redacted_finite_replayable_features() {
        let provenance = ModelFeatureBlockProvenance::new(
            "arena",
            "load-dataset-v1",
            "trace-42",
            "replay_tape:agar.replay.jsonl",
        );

        let block = ModelFeatureBlock::new(ModelFeatureBlockKind::WorkerLoad, provenance.clone())
            .with_metric("tick_hz", 24.0)
            .with_metric("owned_entities", 120.0)
            .with_metric("pending_mesh", 2.0)
            .with_dimension("world_id", "arena-dev")
            .with_dimension("partition_schema", "grid2d");

        assert_eq!(block.validate(), Ok(()));
        assert_eq!(MODEL_FEATURE_BLOCK_SCHEMA_VERSION, 1);
        assert_eq!(
            ModelFeatureBlockKind::from_wire_str(ModelFeatureBlockKind::WorkerLoad.as_wire_str()),
            Some(ModelFeatureBlockKind::WorkerLoad)
        );

        let unredacted = block.clone().with_redacted(false);
        assert_eq!(
            unredacted.validate(),
            Err(ModelFeatureBlockValidationError::UnredactedFeatureBlock)
        );

        let non_finite = ModelFeatureBlock::new(ModelFeatureBlockKind::WorkerLoad, provenance)
            .with_metric("tick_hz", f64::NAN);
        assert_eq!(
            non_finite.validate(),
            Err(ModelFeatureBlockValidationError::NonFiniteMetric {
                name: "tick_hz".to_string()
            })
        );
    }

    #[test]
    fn model_feature_block_rejects_raw_secret_or_payload_shapes() {
        let provenance = ModelFeatureBlockProvenance::new(
            "arena",
            "load-dataset-v1",
            "trace-42",
            "replay_tape:agar.replay.jsonl",
        );

        for forbidden in [
            "auth_token",
            "worker_secret",
            "raw_payload_bytes",
            "component_body",
            "components_json",
            "updates_json",
        ] {
            let block =
                ModelFeatureBlock::new(ModelFeatureBlockKind::WorkerLoad, provenance.clone())
                    .with_metric("tick_hz", 24.0)
                    .with_dimension(forbidden, "present");

            assert!(matches!(
                block.validate(),
                Err(ModelFeatureBlockValidationError::ForbiddenFeatureName { .. })
            ));
        }
    }

    #[test]
    fn model_feature_block_contract_pins_project_local_provenance() {
        let valid = ModelFeatureBlock::new(
            ModelFeatureBlockKind::HandoffPressure,
            ModelFeatureBlockProvenance::new(
                "project-alpha",
                "handoff-v1",
                "trace-99",
                "reality_loadgen:cross-broker",
            ),
        )
        .with_metric("handoff_rate", 3.0)
        .with_dimension("region", "Z1_0");
        assert_eq!(valid.validate(), Ok(()));

        for bad in [
            ModelFeatureBlockProvenance::new("", "handoff-v1", "trace-99", "reality_loadgen"),
            ModelFeatureBlockProvenance::new("project-alpha", "", "trace-99", "reality_loadgen"),
            ModelFeatureBlockProvenance::new("project-alpha", "handoff-v1", "", "reality_loadgen"),
            ModelFeatureBlockProvenance::new("project-alpha", "handoff-v1", "trace-99", ""),
        ] {
            let block = ModelFeatureBlock::new(ModelFeatureBlockKind::HandoffPressure, bad)
                .with_metric("handoff_rate", 3.0);
            assert!(block.validate().is_err());
        }

        let bad_version = ModelFeatureBlock::new(
            ModelFeatureBlockKind::HandoffPressure,
            ModelFeatureBlockProvenance::new(
                "project-alpha",
                "handoff-v1",
                "trace-99",
                "reality_loadgen",
            )
            .with_schema_version(0),
        )
        .with_metric("handoff_rate", 3.0);
        assert_eq!(
            bad_version.validate(),
            Err(ModelFeatureBlockValidationError::InvalidSchemaVersion)
        );
    }
}
