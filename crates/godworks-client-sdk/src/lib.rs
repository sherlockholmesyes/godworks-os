//! Headless client-side cache for Godworks OS.
//!
//! This crate is intentionally transport-free. It applies typed protocol frames
//! to a local view of entities, components, ghost markers, authority grants, and
//! rejected writes. Engine bridges should build on this cache instead of
//! duplicating protocol state handling in engine scripts.

use godworks_core::{ComponentName, EntityId};
use godworks_protocol::{
    AddComponent, AddEntity, AuthorityChange, BatchUpdate, ComponentUpdate, CriticalSection,
    JsonFields, MeshGhost, Op, RemoveComponent, RemoveEntity, UpdateRejected,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// Latest known authority state for one component from this client's view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientAuthority {
    pub authoritative: bool,
    pub authority_epoch: u64,
    pub mode: String,
}

/// A rejected client/worker write observed on the stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRejection {
    pub entity: Option<EntityId>,
    pub component: Option<ComponentName>,
    pub reason: String,
}

/// One entity row in the client cache.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClientEntity {
    pub components: BTreeMap<ComponentName, Value>,
    pub authority: BTreeMap<ComponentName, ClientAuthority>,
    pub ghost: bool,
    pub owner_region: Option<String>,
}

impl ClientEntity {
    pub fn component(&self, component: impl AsRef<str>) -> Option<&Value> {
        self.components
            .get(&ComponentName::from(component.as_ref()))
    }

    pub fn position2(&self) -> Option<[f64; 2]> {
        position2_from_value(self.component("pos")?)
    }
}

/// Event returned by [`ClientCache::apply_op`] for consumers that need to react
/// without diffing the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientCacheEvent {
    Ignored,
    EntityAdded(EntityId),
    EntityRemoved(EntityId),
    ComponentUpdated(EntityId, ComponentName),
    AuthorityChanged(EntityId, ComponentName),
    UpdateRejected(ClientRejection),
    CriticalSectionChanged { depth: usize },
}

/// Transport-free cache for a Godworks client/observer stream.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClientCache {
    entities: BTreeMap<EntityId, ClientEntity>,
    rejections: Vec<ClientRejection>,
    critical_depth: usize,
}

impl ClientCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn critical_depth(&self) -> usize {
        self.critical_depth
    }

    pub fn rejections(&self) -> &[ClientRejection] {
        &self.rejections
    }

    pub fn entity(&self, entity: impl AsRef<str>) -> Option<&ClientEntity> {
        self.entities.get(&EntityId::from(entity.as_ref()))
    }

    pub fn contains(&self, entity: impl AsRef<str>) -> bool {
        self.entity(entity).is_some()
    }

    pub fn component(&self, entity: impl AsRef<str>, component: impl AsRef<str>) -> Option<&Value> {
        self.entity(entity)?.component(component)
    }

    pub fn position2(&self, entity: impl AsRef<str>) -> Option<[f64; 2]> {
        self.entity(entity)?.position2()
    }

    pub fn is_ghost(&self, entity: impl AsRef<str>) -> bool {
        self.entity(entity).map(|e| e.ghost).unwrap_or(false)
    }

    pub fn apply_op(&mut self, op: &Op) -> ClientCacheEvent {
        match op {
            Op::AddEntity(op) => self.apply_add_entity(op),
            Op::RemoveEntity(op) => self.apply_remove_entity(op),
            Op::AddComponent(op) => self.apply_add_component(op),
            Op::RemoveComponent(op) => self.apply_remove_component(op),
            Op::ComponentUpdate(op) => self.apply_component_update(op),
            Op::BatchUpdate(op) => self.apply_batch_update(op),
            Op::AuthorityChange(op) => self.apply_authority_change(op),
            Op::UpdateRejected(op) => self.apply_update_rejected(op),
            Op::CriticalSection(op) => self.apply_critical_section(op),
            Op::MeshGhost(op) => self.apply_mesh_ghost(op),
            Op::MeshGhostRemove(op) => self.apply_mesh_ghost_remove(&op.fields),
            _ => ClientCacheEvent::Ignored,
        }
    }

    fn apply_add_entity(&mut self, op: &AddEntity) -> ClientCacheEvent {
        let entity_id = op.entity.clone();
        let row = self.entities.entry(entity_id.clone()).or_default();
        if let Some(components) = op.components.as_ref() {
            merge_component_bag(row, components);
        }
        refresh_metadata(row);
        ClientCacheEvent::EntityAdded(entity_id)
    }

    fn apply_remove_entity(&mut self, op: &RemoveEntity) -> ClientCacheEvent {
        self.entities.remove(&op.entity);
        ClientCacheEvent::EntityRemoved(op.entity.clone())
    }

    fn apply_add_component(&mut self, op: &AddComponent) -> ClientCacheEvent {
        self.set_component(op.entity.clone(), op.component.clone(), op.value.clone())
    }

    fn apply_remove_component(&mut self, op: &RemoveComponent) -> ClientCacheEvent {
        if let Some(row) = self.entities.get_mut(&op.entity) {
            row.components.remove(&op.component);
            row.authority.remove(&op.component);
            refresh_metadata(row);
        }
        ClientCacheEvent::ComponentUpdated(op.entity.clone(), op.component.clone())
    }

    fn apply_component_update(&mut self, op: &ComponentUpdate) -> ClientCacheEvent {
        let Some(entity) = op.fields.str("entity").map(EntityId::from) else {
            return ClientCacheEvent::Ignored;
        };
        let Some(component) = component_name_from_fields(&op.fields) else {
            return ClientCacheEvent::Ignored;
        };
        let value = op.fields.get("value").cloned().unwrap_or(Value::Null);
        self.set_component(entity, component, value)
    }

    fn apply_batch_update(&mut self, op: &BatchUpdate) -> ClientCacheEvent {
        let mut last = ClientCacheEvent::Ignored;
        for update in &op.updates {
            last = self.set_component(
                update.entity.clone(),
                op.component.clone(),
                update.value.clone(),
            );
        }
        last
    }

    fn apply_authority_change(&mut self, op: &AuthorityChange) -> ClientCacheEvent {
        let row = self.entities.entry(op.entity.clone()).or_default();
        row.authority.insert(
            op.component.clone(),
            ClientAuthority {
                authoritative: op.authoritative,
                authority_epoch: op.authority_epoch,
                mode: op.mode.clone(),
            },
        );
        ClientCacheEvent::AuthorityChanged(op.entity.clone(), op.component.clone())
    }

    fn apply_update_rejected(&mut self, op: &UpdateRejected) -> ClientCacheEvent {
        let rejection = ClientRejection {
            entity: op.entity.clone(),
            component: op.component.clone(),
            reason: op.reason.clone(),
        };
        self.rejections.push(rejection.clone());
        ClientCacheEvent::UpdateRejected(rejection)
    }

    fn apply_critical_section(&mut self, op: &CriticalSection) -> ClientCacheEvent {
        match op.phase.to_ascii_lowercase().as_str() {
            "end" | "finish" | "close" | "commit" => {
                self.critical_depth = self.critical_depth.saturating_sub(1);
            }
            "begin" | "start" | "open" | "prepare" => {
                self.critical_depth = self.critical_depth.saturating_add(1);
            }
            _ => {}
        }
        ClientCacheEvent::CriticalSectionChanged {
            depth: self.critical_depth,
        }
    }

    fn apply_mesh_ghost(&mut self, op: &MeshGhost) -> ClientCacheEvent {
        let Some(entity) = op.fields.str("entity").map(EntityId::from) else {
            return ClientCacheEvent::Ignored;
        };
        let row = self.entities.entry(entity.clone()).or_default();
        if let Some(components) = op.fields.get("components") {
            merge_component_bag(row, components);
        }
        if let Some(pos) = op.fields.get("pos").cloned() {
            row.components.insert(ComponentName::from("pos"), pos);
        }
        if let Some(vel) = op.fields.get("vel").cloned() {
            row.components.insert(ComponentName::from("vel"), vel);
        }
        row.ghost = true;
        row.owner_region = op.fields.str("owner_region").map(str::to_string);
        if let Some(owner_region) = &row.owner_region {
            row.components.insert(
                ComponentName::from("owner_region"),
                Value::String(owner_region.clone()),
            );
        }
        row.components
            .insert(ComponentName::from("ghost"), Value::Bool(true));
        ClientCacheEvent::EntityAdded(entity)
    }

    fn apply_mesh_ghost_remove(&mut self, fields: &JsonFields) -> ClientCacheEvent {
        let Some(entity) = fields.str("entity").map(EntityId::from) else {
            return ClientCacheEvent::Ignored;
        };
        self.entities.remove(&entity);
        ClientCacheEvent::EntityRemoved(entity)
    }

    fn set_component(
        &mut self,
        entity: EntityId,
        component: ComponentName,
        value: Value,
    ) -> ClientCacheEvent {
        let row = self.entities.entry(entity.clone()).or_default();
        row.components.insert(component.clone(), value);
        refresh_metadata(row);
        ClientCacheEvent::ComponentUpdated(entity, component)
    }
}

fn merge_component_bag(row: &mut ClientEntity, components: &Value) {
    if let Some(map) = components.as_object() {
        merge_component_map(row, map);
    }
}

fn merge_component_map(row: &mut ClientEntity, components: &Map<String, Value>) {
    for (name, value) in components {
        row.components
            .insert(ComponentName::from(name.clone()), value.clone());
    }
    refresh_metadata(row);
}

fn refresh_metadata(row: &mut ClientEntity) {
    row.ghost = row
        .components
        .get(&ComponentName::from("ghost"))
        .and_then(Value::as_bool)
        .unwrap_or(row.ghost);
    row.owner_region = row
        .components
        .get(&ComponentName::from("owner_region"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| row.owner_region.clone());
}

fn component_name_from_fields(fields: &JsonFields) -> Option<ComponentName> {
    fields
        .str("comp")
        .or_else(|| fields.str("component"))
        .map(ComponentName::from)
}

fn position2_from_value(value: &Value) -> Option<[f64; 2]> {
    let values = value.as_array()?;
    Some([values.first()?.as_f64()?, values.get(1)?.as_f64()?])
}

#[cfg(test)]
mod tests {
    use super::*;
    use godworks_protocol::{BatchUpdateEntry, MeshGhostRemove};
    use serde_json::json;

    #[test]
    fn cache_applies_add_update_batch_and_remove() {
        let mut cache = ClientCache::new();

        assert_eq!(
            cache.apply_op(&Op::AddEntity(AddEntity {
                entity: EntityId::from("ship"),
                components: Some(json!({"kind":"sloop","pos":[1.0,2.0]})),
            })),
            ClientCacheEvent::EntityAdded(EntityId::from("ship"))
        );
        assert_eq!(cache.entity_count(), 1);
        assert_eq!(cache.position2("ship"), Some([1.0, 2.0]));

        cache.apply_op(&Op::ComponentUpdate(ComponentUpdate {
            fields: JsonFields {
                fields: Map::from_iter([
                    ("entity".to_string(), json!("ship")),
                    ("comp".to_string(), json!("pos")),
                    ("value".to_string(), json!([3.0, 4.0])),
                ]),
            },
        }));
        assert_eq!(cache.position2("ship"), Some([3.0, 4.0]));

        cache.apply_op(&Op::BatchUpdate(BatchUpdate {
            component: ComponentName::from("vel"),
            updates: vec![BatchUpdateEntry {
                entity: EntityId::from("ship"),
                value: json!([0.5, 0.0]),
                authority_epoch: Some(7),
            }],
        }));
        assert_eq!(cache.component("ship", "vel"), Some(&json!([0.5, 0.0])));

        cache.apply_op(&Op::AddComponent(AddComponent {
            entity: EntityId::from("ship"),
            component: ComponentName::from("health"),
            value: json!({"hp":100}),
            authority_epoch: Some(7),
        }));
        assert_eq!(cache.component("ship", "health"), Some(&json!({"hp":100})));

        cache.apply_op(&Op::RemoveComponent(RemoveComponent {
            entity: EntityId::from("ship"),
            component: ComponentName::from("health"),
            authority_epoch: Some(8),
        }));
        assert!(cache.component("ship", "health").is_none());

        assert_eq!(
            cache.apply_op(&Op::RemoveEntity(RemoveEntity {
                entity: EntityId::from("ship")
            })),
            ClientCacheEvent::EntityRemoved(EntityId::from("ship"))
        );
        assert!(!cache.contains("ship"));
    }

    #[test]
    fn cache_preserves_ghost_marker_and_owner_region() {
        let mut cache = ClientCache::new();
        cache.apply_op(&Op::MeshGhost(MeshGhost {
            fields: JsonFields {
                fields: Map::from_iter([
                    ("entity".to_string(), json!("remote")),
                    ("pos".to_string(), json!([9.0, 1.0])),
                    ("vel".to_string(), json!([0.0, 0.0])),
                    ("components".to_string(), json!({"kind":"unit"})),
                    ("owner_region".to_string(), json!("E")),
                ]),
            },
        }));

        let entity = cache.entity("remote").expect("ghost row");
        assert!(entity.ghost);
        assert_eq!(entity.owner_region.as_deref(), Some("E"));
        assert_eq!(entity.component("ghost"), Some(&json!(true)));
        assert_eq!(entity.component("owner_region"), Some(&json!("E")));
        assert_eq!(entity.position2(), Some([9.0, 1.0]));

        cache.apply_op(&Op::MeshGhostRemove(MeshGhostRemove {
            fields: JsonFields {
                fields: Map::from_iter([("entity".to_string(), json!("remote"))]),
            },
        }));
        assert!(!cache.contains("remote"));
    }

    #[test]
    fn cache_update_rejected_does_not_mutate_state() {
        let mut cache = ClientCache::new();
        cache.apply_op(&Op::AddEntity(AddEntity {
            entity: EntityId::from("ship"),
            components: Some(json!({"pos":[1.0,2.0]})),
        }));

        let event = cache.apply_op(&Op::UpdateRejected(UpdateRejected {
            entity: Some(EntityId::from("ship")),
            component: Some(ComponentName::from("pos")),
            reason: "stale authority_epoch".to_string(),
            fields: JsonFields { fields: Map::new() },
        }));

        assert_eq!(cache.position2("ship"), Some([1.0, 2.0]));
        assert_eq!(cache.rejections().len(), 1);
        assert_eq!(
            event,
            ClientCacheEvent::UpdateRejected(ClientRejection {
                entity: Some(EntityId::from("ship")),
                component: Some(ComponentName::from("pos")),
                reason: "stale authority_epoch".to_string(),
            })
        );
    }

    #[test]
    fn cache_tracks_authority_and_critical_section_depth() {
        let mut cache = ClientCache::new();

        cache.apply_op(&Op::CriticalSection(CriticalSection {
            phase: "begin".to_string(),
            entity: Some(EntityId::from("ship")),
        }));
        assert_eq!(cache.critical_depth(), 1);

        cache.apply_op(&Op::AuthorityChange(AuthorityChange {
            entity: EntityId::from("ship"),
            component: ComponentName::from("pos"),
            authoritative: true,
            authority_epoch: 12,
            mode: "client_forward_sparse".to_string(),
            fields: JsonFields { fields: Map::new() },
        }));
        let authority = cache
            .entity("ship")
            .and_then(|entity| entity.authority.get(&ComponentName::from("pos")))
            .expect("authority row");
        assert!(authority.authoritative);
        assert_eq!(authority.authority_epoch, 12);

        cache.apply_op(&Op::CriticalSection(CriticalSection {
            phase: "end".to_string(),
            entity: Some(EntityId::from("ship")),
        }));
        assert_eq!(cache.critical_depth(), 0);
    }
}
