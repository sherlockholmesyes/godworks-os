//! JSON codec for the current Godworks v1 debug/development wire.
//!
//! The broker still dispatches raw JSON today. This module is the compatibility
//! bridge: it documents and tests the current shape while giving future SDK work
//! a typed boundary.

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId, Velocity2};
use serde_json::{json, Map, Value};

use crate::{
    AddComponent, AddEntity, AuthorityChange, BatchUpdate, BatchUpdateEntry, CommandRequest,
    CommandResponse, ComponentUpdate, CreateEntity, CreateEntityResponse, CriticalSection,
    DeleteEntity, DeleteEntityResponse, EntityEvent, EntityQuery, EntityQueryResponse, FlagUpdate,
    Fold, Heartbeat, InspectorFrame, InspectorQuery, Interest, JsonFields, LogMessage, MeshAck,
    MeshGhost, MeshGhostRemove, MeshHandoff, Metrics, Op, ProtocolError, RemoveComponent,
    RemoveEntity, ReserveEntityIds, ReserveEntityIdsResponse, SetComponentAuthority,
    SetComponentAuthorityResponse, SnapshotMarker, ThresholdTx, ThresholdTxResponse,
    UpdateComponent, UpdateRejected, WorkerConnect,
};

/// Decode a JSON operation body into the typed v1 operation model.
pub fn decode_json_value(value: &Value) -> Result<Op, ProtocolError> {
    let op = required_str(value, "op")?;

    match op {
        "WorkerConnect" => decode_worker_connect(value),
        "Disconnect" => Ok(Op::Disconnect),
        "Heartbeat" => Ok(Op::Heartbeat(Heartbeat {
            worker_id: optional_str(value, "worker_id").map(PeerId::from),
        })),
        "Interest" => decode_interest(value),
        "CriticalSection" => Ok(Op::CriticalSection(CriticalSection {
            phase: optional_str(value, "phase").unwrap_or("").to_string(),
            entity: optional_str(value, "entity").map(EntityId::from),
        })),
        "AddEntity" => Ok(Op::AddEntity(AddEntity {
            entity: EntityId::from(required_str(value, "entity")?),
            components: value.get("components").cloned(),
        })),
        "RemoveEntity" => Ok(Op::RemoveEntity(RemoveEntity {
            entity: EntityId::from(required_str(value, "entity")?),
        })),
        "CreateEntity" => decode_create_entity(value),
        "CreateEntityResponse" => Ok(Op::CreateEntityResponse(CreateEntityResponse {
            fields: json_fields(value),
        })),
        "DeleteEntity" => Ok(Op::DeleteEntity(DeleteEntity {
            entity: EntityId::from(required_str(value, "entity")?),
            request_id: optional_str(value, "request_id").map(str::to_string),
            authority_epoch: authority_epoch(value),
        })),
        "DeleteEntityResponse" => Ok(Op::DeleteEntityResponse(DeleteEntityResponse {
            fields: json_fields(value),
        })),
        "ReserveEntityIds" => Ok(Op::ReserveEntityIds(ReserveEntityIds {
            request_id: optional_str(value, "request_id").map(str::to_string),
            count: optional_u64(value, "count")
                .or_else(|| optional_u64(value, "n"))
                .unwrap_or(0),
        })),
        "ReserveEntityIdsResponse" => Ok(Op::ReserveEntityIdsResponse(ReserveEntityIdsResponse {
            fields: json_fields(value),
        })),
        "AddComponent" => Ok(Op::AddComponent(AddComponent {
            entity: EntityId::from(required_str(value, "entity")?),
            component: ComponentName::from(required_component_name(value)?),
            value: value.get("value").cloned().unwrap_or(Value::Null),
            authority_epoch: authority_epoch(value),
        })),
        "RemoveComponent" => Ok(Op::RemoveComponent(RemoveComponent {
            entity: EntityId::from(required_str(value, "entity")?),
            component: ComponentName::from(required_component_name(value)?),
            authority_epoch: authority_epoch(value),
        })),
        "UpdateComponent" => decode_update_component(value),
        "ComponentUpdate" => Ok(Op::ComponentUpdate(ComponentUpdate {
            fields: json_fields(value),
        })),
        "BatchUpdate" => decode_batch_update(value),
        "SetComponentAuthority" => Ok(Op::SetComponentAuthority(SetComponentAuthority {
            fields: json_fields(value),
        })),
        "SetComponentAuthorityResponse" => Ok(Op::SetComponentAuthorityResponse(
            SetComponentAuthorityResponse {
                fields: json_fields(value),
            },
        )),
        "AuthorityChange" => decode_authority_change(value),
        "UpdateRejected" => Ok(Op::UpdateRejected(UpdateRejected {
            entity: optional_str(value, "entity").map(EntityId::from),
            component: optional_str(value, "comp")
                .or_else(|| optional_str(value, "component"))
                .map(ComponentName::from),
            reason: optional_str(value, "reason").unwrap_or("").to_string(),
            fields: json_fields(value),
        })),
        "Fold" => Ok(Op::Fold(Fold {
            fields: json_fields(value),
        })),
        "ThresholdTx" => Ok(Op::ThresholdTx(ThresholdTx {
            fields: json_fields(value),
        })),
        "ThresholdTxResponse" => Ok(Op::ThresholdTxResponse(ThresholdTxResponse {
            fields: json_fields(value),
        })),
        "EntityQuery" => Ok(Op::EntityQuery(EntityQuery {
            fields: json_fields(value),
        })),
        "EntityQueryResponse" => Ok(Op::EntityQueryResponse(EntityQueryResponse {
            fields: json_fields(value),
        })),
        "InspectorQuery" => Ok(Op::InspectorQuery(InspectorQuery {
            fields: json_fields(value),
        })),
        "InspectorFrame" => Ok(Op::InspectorFrame(InspectorFrame {
            fields: json_fields(value),
        })),
        "CommandRequest" => Ok(Op::CommandRequest(CommandRequest {
            fields: json_fields(value),
        })),
        "CommandResponse" => Ok(Op::CommandResponse(CommandResponse {
            fields: json_fields(value),
        })),
        "EntityEvent" => Ok(Op::EntityEvent(EntityEvent {
            fields: json_fields(value),
        })),
        "FlagUpdate" => Ok(Op::FlagUpdate(FlagUpdate {
            fields: json_fields(value),
        })),
        "Metrics" => Ok(Op::Metrics(Metrics {
            fields: json_fields(value),
        })),
        "SnapshotMarker" => Ok(Op::SnapshotMarker(SnapshotMarker {
            fields: json_fields(value),
        })),
        "MeshHandoff" => decode_mesh_handoff(value),
        "MeshAck" => Ok(Op::MeshAck(MeshAck {
            entity: EntityId::from(required_str(value, "entity")?),
        })),
        "MeshGhost" => Ok(Op::MeshGhost(MeshGhost {
            fields: json_fields(value),
        })),
        "MeshGhostRemove" => Ok(Op::MeshGhostRemove(MeshGhostRemove {
            fields: json_fields(value),
        })),
        "LogMessage" => Ok(Op::LogMessage(LogMessage {
            fields: json_fields(value),
        })),
        "Health" => Ok(Op::Health),
        other => Err(ProtocolError::unknown_operation(other)),
    }
}

/// Encode a typed operation into the current JSON operation body.
pub fn encode_json_value(op: &Op) -> Value {
    match op {
        Op::WorkerConnect(op) => encode_worker_connect(op),
        Op::Disconnect => json!({ "op": "Disconnect" }),
        Op::Heartbeat(op) => encode_heartbeat(op),
        Op::Interest(op) => encode_interest(op),
        Op::CriticalSection(op) => encode_critical_section(op),
        Op::AddEntity(op) => encode_add_entity(op),
        Op::RemoveEntity(op) => json!({
            "op": "RemoveEntity",
            "entity": op.entity.as_ref(),
        }),
        Op::CreateEntity(op) => encode_create_entity(op),
        Op::CreateEntityResponse(op) => encode_json_fields("CreateEntityResponse", &op.fields),
        Op::DeleteEntity(op) => encode_delete_entity(op),
        Op::DeleteEntityResponse(op) => encode_json_fields("DeleteEntityResponse", &op.fields),
        Op::ReserveEntityIds(op) => encode_reserve_entity_ids(op),
        Op::ReserveEntityIdsResponse(op) => encode_json_fields("ReserveEntityIdsResponse", &op.fields),
        Op::AddComponent(op) => encode_add_component(op),
        Op::RemoveComponent(op) => encode_remove_component(op),
        Op::UpdateComponent(op) => encode_update_component(op),
        Op::ComponentUpdate(op) => encode_json_fields("ComponentUpdate", &op.fields),
        Op::BatchUpdate(op) => encode_batch_update(op),
        Op::SetComponentAuthority(op) => encode_json_fields("SetComponentAuthority", &op.fields),
        Op::SetComponentAuthorityResponse(op) => {
            encode_json_fields("SetComponentAuthorityResponse", &op.fields)
        }
        Op::AuthorityChange(op) => encode_authority_change(op),
        Op::UpdateRejected(op) => encode_update_rejected(op),
        Op::Fold(op) => encode_json_fields("Fold", &op.fields),
        Op::ThresholdTx(op) => encode_json_fields("ThresholdTx", &op.fields),
        Op::ThresholdTxResponse(op) => encode_json_fields("ThresholdTxResponse", &op.fields),
        Op::EntityQuery(op) => encode_json_fields("EntityQuery", &op.fields),
        Op::EntityQueryResponse(op) => encode_json_fields("EntityQueryResponse", &op.fields),
        Op::InspectorQuery(op) => encode_json_fields("InspectorQuery", &op.fields),
        Op::InspectorFrame(op) => encode_json_fields("InspectorFrame", &op.fields),
        Op::CommandRequest(op) => encode_json_fields("CommandRequest", &op.fields),
        Op::CommandResponse(op) => encode_json_fields("CommandResponse", &op.fields),
        Op::EntityEvent(op) => encode_json_fields("EntityEvent", &op.fields),
        Op::FlagUpdate(op) => encode_json_fields("FlagUpdate", &op.fields),
        Op::Metrics(op) => encode_json_fields("Metrics", &op.fields),
        Op::SnapshotMarker(op) => encode_json_fields("SnapshotMarker", &op.fields),
        Op::MeshHandoff(op) => encode_mesh_handoff(op),
        Op::MeshAck(op) => json!({
            "op": "MeshAck",
            "entity": op.entity.as_ref(),
        }),
        Op::MeshGhost(op) => encode_json_fields("MeshGhost", &op.fields),
        Op::MeshGhostRemove(op) => encode_json_fields("MeshGhostRemove", &op.fields),
        Op::LogMessage(op) => encode_json_fields("LogMessage", &op.fields),
        Op::Health => json!({ "op": "Health" }),
    }
}

fn decode_worker_connect(value: &Value) -> Result<Op, ProtocolError> {
    let proto = optional_u64(value, "proto");
    if let Some(proto) = proto {
        if !crate::supports_protocol(proto) {
            return Err(ProtocolError::unsupported_version(proto));
        }
    }

    let attributes = value
        .get("attributes")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    Ok(Op::WorkerConnect(WorkerConnect {
        worker_id: PeerId::from(required_str(value, "worker_id")?),
        region: RegionId::from(required_str(value, "region")?),
        proto,
        attributes,
    }))
}

fn decode_interest(value: &Value) -> Result<Op, ProtocolError> {
    let center = optional_array2(value, "center");
    let radius = optional_f64(value, "radius");
    let aoi = center.zip(radius).map(|(center, radius)| Aoi2::Circle { center, radius });

    Ok(Op::Interest(Interest {
        aoi,
        full_radius: optional_f64(value, "full_radius")
            .or_else(|| optional_f64(value, "fullRadius")),
        coarse_rate: optional_u64(value, "coarse_rate")
            .or_else(|| optional_u64(value, "coarseRate"))
            .unwrap_or(1),
        coarse_grid: optional_f64(value, "coarse_grid")
            .or_else(|| optional_f64(value, "coarseGrid"))
            .unwrap_or(0.0),
    }))
}

fn decode_create_entity(value: &Value) -> Result<Op, ProtocolError> {
    Ok(Op::CreateEntity(CreateEntity {
        entity: EntityId::from(required_str(value, "entity")?),
        request_id: optional_str(value, "request_id").map(str::to_string),
        requested_region: optional_str(value, "region").map(RegionId::from),
        components: component_bag(value),
    }))
}

fn decode_update_component(value: &Value) -> Result<Op, ProtocolError> {
    Ok(Op::UpdateComponent(UpdateComponent {
        entity: EntityId::from(required_str(value, "entity")?),
        component: ComponentName::from(required_component_name(value)?),
        value: value.get("value").cloned().unwrap_or(Value::Null),
        authority_epoch: authority_epoch(value),
    }))
}

fn decode_batch_update(value: &Value) -> Result<Op, ProtocolError> {
    let component = ComponentName::from(required_component_name(value)?);
    let updates = if let Some(items) = value.get("updates").and_then(Value::as_array) {
        decode_batch_update_array(items)?
    } else if let Some(values) = value.get("values").and_then(Value::as_object) {
        decode_batch_update_map(value, values)
    } else {
        Vec::new()
    };

    Ok(Op::BatchUpdate(BatchUpdate { component, updates }))
}

fn decode_batch_update_array(items: &[Value]) -> Result<Vec<BatchUpdateEntry>, ProtocolError> {
    let mut updates = Vec::with_capacity(items.len());

    for item in items {
        if let Some(entry) = item.as_array() {
            let entity = entry
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::malformed("BatchUpdate entry missing entity"))?;

            updates.push(BatchUpdateEntry {
                entity: EntityId::from(entity),
                value: entry.get(1).cloned().unwrap_or(Value::Null),
                authority_epoch: entry.get(2).and_then(Value::as_u64),
            });
        } else if let Some(entry) = item.as_object() {
            let entity = entry.get("entity").and_then(Value::as_str).ok_or_else(|| {
                ProtocolError::malformed("BatchUpdate object entry missing entity")
            })?;

            updates.push(BatchUpdateEntry {
                entity: EntityId::from(entity),
                value: entry.get("value").cloned().unwrap_or(Value::Null),
                authority_epoch: entry.get("authority_epoch").and_then(Value::as_u64),
            });
        } else {
            return Err(ProtocolError::malformed(
                "BatchUpdate updates entries must be arrays or objects",
            ));
        }
    }

    Ok(updates)
}

fn decode_batch_update_map(value: &Value, values: &Map<String, Value>) -> Vec<BatchUpdateEntry> {
    let shared_epoch = authority_epoch(value);

    values
        .iter()
        .map(|(entity, update_value)| BatchUpdateEntry {
            entity: EntityId::from(entity.clone()),
            value: update_value.clone(),
            authority_epoch: shared_epoch,
        })
        .collect()
}

fn decode_authority_change(value: &Value) -> Result<Op, ProtocolError> {
    Ok(Op::AuthorityChange(AuthorityChange {
        entity: EntityId::from(required_str(value, "entity")?),
        component: ComponentName::from(optional_str(value, "comp").unwrap_or("pos")),
        authoritative: value
            .get("authoritative")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        authority_epoch: authority_epoch(value).unwrap_or(0),
        mode: optional_str(value, "mode").unwrap_or("").to_string(),
        fields: json_fields(value),
    }))
}

fn decode_mesh_handoff(value: &Value) -> Result<Op, ProtocolError> {
    let target = optional_str(value, "target")
        .or_else(|| optional_str(value, "region"))
        .ok_or_else(|| ProtocolError::missing_field("target"))?;

    Ok(Op::MeshHandoff(MeshHandoff {
        entity: EntityId::from(required_str(value, "entity")?),
        source_region: optional_str(value, "source_region")
            .or_else(|| optional_str(value, "src_region"))
            .map(RegionId::from),
        target_region: RegionId::from(target),
        pos: value.get("pos").map(pos2_from_value).unwrap_or_default(),
        vel: value.get("vel").map(vel2_from_value).unwrap_or_default(),
        authority_epoch: authority_epoch(value),
        lease_epoch: optional_u64(value, "lease_epoch"),
        source_durable_gen: optional_u64(value, "source_durable_gen"),
        fields: json_fields(value),
    }))
}

fn encode_worker_connect(op: &WorkerConnect) -> Value {
    let mut obj = object_with_op("WorkerConnect");
    obj.insert("worker_id".to_string(), json!(op.worker_id.as_ref()));
    obj.insert("region".to_string(), json!(op.region.as_ref()));

    if let Some(proto) = op.proto {
        obj.insert("proto".to_string(), json!(proto));
    }
    if !op.attributes.is_empty() {
        obj.insert("attributes".to_string(), json!(&op.attributes));
    }

    Value::Object(obj)
}

fn encode_heartbeat(op: &Heartbeat) -> Value {
    let mut obj = object_with_op("Heartbeat");
    if let Some(worker_id) = &op.worker_id {
        obj.insert("worker_id".to_string(), json!(worker_id.as_ref()));
    }
    Value::Object(obj)
}

fn encode_interest(op: &Interest) -> Value {
    let mut obj = object_with_op("Interest");

    if let Some(aoi) = op.aoi {
        match aoi {
            Aoi2::Circle { center, radius } => {
                obj.insert("center".to_string(), json!(center.to_array()));
                obj.insert("radius".to_string(), json!(radius));
            }
            Aoi2::Box { min, max } => {
                obj.insert("min".to_string(), json!(min.to_array()));
                obj.insert("max".to_string(), json!(max.to_array()));
            }
        }
    }
    if let Some(full_radius) = op.full_radius {
        obj.insert("full_radius".to_string(), json!(full_radius));
    }
    if op.coarse_rate != 1 {
        obj.insert("coarse_rate".to_string(), json!(op.coarse_rate));
    }
    if op.coarse_grid != 0.0 {
        obj.insert("coarse_grid".to_string(), json!(op.coarse_grid));
    }

    Value::Object(obj)
}

fn encode_critical_section(op: &CriticalSection) -> Value {
    let mut obj = object_with_op("CriticalSection");
    obj.insert("phase".to_string(), json!(op.phase.as_str()));
    if let Some(entity) = &op.entity {
        obj.insert("entity".to_string(), json!(entity.as_ref()));
    }
    Value::Object(obj)
}

fn encode_add_entity(op: &AddEntity) -> Value {
    let mut obj = object_with_op("AddEntity");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    if let Some(components) = &op.components {
        obj.insert("components".to_string(), components.clone());
    }
    Value::Object(obj)
}

fn encode_create_entity(op: &CreateEntity) -> Value {
    let mut obj = object_with_op("CreateEntity");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    if let Some(request_id) = &op.request_id {
        obj.insert("request_id".to_string(), json!(request_id));
    }
    if let Some(region) = &op.requested_region {
        obj.insert("region".to_string(), json!(region.as_ref()));
    }
    obj.insert("components".to_string(), op.components.clone());
    Value::Object(obj)
}

fn encode_delete_entity(op: &DeleteEntity) -> Value {
    let mut obj = object_with_op("DeleteEntity");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    if let Some(request_id) = &op.request_id {
        obj.insert("request_id".to_string(), json!(request_id));
    }
    if let Some(epoch) = op.authority_epoch {
        obj.insert("authority_epoch".to_string(), json!(epoch));
    }
    Value::Object(obj)
}

fn encode_reserve_entity_ids(op: &ReserveEntityIds) -> Value {
    let mut obj = object_with_op("ReserveEntityIds");
    if let Some(request_id) = &op.request_id {
        obj.insert("request_id".to_string(), json!(request_id));
    }
    obj.insert("count".to_string(), json!(op.count));
    Value::Object(obj)
}

fn encode_add_component(op: &AddComponent) -> Value {
    let mut obj = object_with_op("AddComponent");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    obj.insert("comp".to_string(), json!(op.component.as_ref()));
    obj.insert("value".to_string(), op.value.clone());
    if let Some(epoch) = op.authority_epoch {
        obj.insert("authority_epoch".to_string(), json!(epoch));
    }
    Value::Object(obj)
}

fn encode_remove_component(op: &RemoveComponent) -> Value {
    let mut obj = object_with_op("RemoveComponent");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    obj.insert("comp".to_string(), json!(op.component.as_ref()));
    if let Some(epoch) = op.authority_epoch {
        obj.insert("authority_epoch".to_string(), json!(epoch));
    }
    Value::Object(obj)
}

fn encode_update_component(op: &UpdateComponent) -> Value {
    let mut obj = object_with_op("UpdateComponent");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    obj.insert("comp".to_string(), json!(op.component.as_ref()));
    obj.insert("value".to_string(), op.value.clone());
    if let Some(epoch) = op.authority_epoch {
        obj.insert("authority_epoch".to_string(), json!(epoch));
    }
    Value::Object(obj)
}

fn encode_batch_update(op: &BatchUpdate) -> Value {
    let updates: Vec<Value> = op
        .updates
        .iter()
        .map(|entry| match entry.authority_epoch {
            Some(epoch) => json!([entry.entity.as_ref(), entry.value.clone(), epoch]),
            None => json!([entry.entity.as_ref(), entry.value.clone()]),
        })
        .collect();

    json!({
        "op": "BatchUpdate",
        "comp": op.component.as_ref(),
        "updates": updates,
    })
}

fn encode_authority_change(op: &AuthorityChange) -> Value {
    if !op.fields.fields.is_empty() {
        return encode_json_fields("AuthorityChange", &op.fields);
    }

    json!({
        "op": "AuthorityChange",
        "entity": op.entity.as_ref(),
        "comp": op.component.as_ref(),
        "authoritative": op.authoritative,
        "authority_epoch": op.authority_epoch,
        "mode": op.mode.as_str(),
    })
}

fn encode_update_rejected(op: &UpdateRejected) -> Value {
    if !op.fields.fields.is_empty() {
        return encode_json_fields("UpdateRejected", &op.fields);
    }

    let mut obj = object_with_op("UpdateRejected");
    if let Some(entity) = &op.entity {
        obj.insert("entity".to_string(), json!(entity.as_ref()));
    }
    if let Some(component) = &op.component {
        obj.insert("comp".to_string(), json!(component.as_ref()));
    }
    obj.insert("reason".to_string(), json!(op.reason.as_str()));
    Value::Object(obj)
}

fn encode_mesh_handoff(op: &MeshHandoff) -> Value {
    if !op.fields.fields.is_empty() {
        return encode_json_fields("MeshHandoff", &op.fields);
    }

    let mut obj = object_with_op("MeshHandoff");
    obj.insert("entity".to_string(), json!(op.entity.as_ref()));
    if let Some(source) = &op.source_region {
        obj.insert("source_region".to_string(), json!(source.as_ref()));
    }
    obj.insert("target".to_string(), json!(op.target_region.as_ref()));
    obj.insert("pos".to_string(), json!(op.pos.to_array()));
    obj.insert("vel".to_string(), json!(op.vel.to_array()));
    if let Some(epoch) = op.authority_epoch {
        obj.insert("authority_epoch".to_string(), json!(epoch));
    }
    if let Some(epoch) = op.lease_epoch {
        obj.insert("lease_epoch".to_string(), json!(epoch));
    }
    if let Some(gen) = op.source_durable_gen {
        obj.insert("source_durable_gen".to_string(), json!(gen));
    }
    Value::Object(obj)
}

fn encode_json_fields(op: &str, fields: &JsonFields) -> Value {
    let mut obj = object_with_op(op);
    for (key, value) in &fields.fields {
        if key != "op" {
            obj.insert(key.clone(), value.clone());
        }
    }
    Value::Object(obj)
}

fn object_with_op(op: &str) -> Map<String, Value> {
    let mut obj = Map::new();
    obj.insert("op".to_string(), json!(op));
    obj
}

fn json_fields(value: &Value) -> JsonFields {
    let mut fields = Map::new();
    if let Some(obj) = value.as_object() {
        for (key, field_value) in obj {
            if key != "op" {
                fields.insert(key.clone(), field_value.clone());
            }
        }
    }
    JsonFields { fields }
}

fn component_bag(value: &Value) -> Value {
    if let Some(components) = value.get("components") {
        return components.clone();
    }

    let mut components = Map::new();
    if let Some(pos) = value.get("pos") {
        components.insert("pos".to_string(), pos.clone());
    }
    if let Some(vel) = value.get("vel") {
        components.insert("vel".to_string(), vel.clone());
    }
    Value::Object(components)
}

fn required_component_name(value: &Value) -> Result<&str, ProtocolError> {
    optional_str(value, "comp")
        .or_else(|| optional_str(value, "component"))
        .ok_or_else(|| ProtocolError::missing_field("comp"))
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, ProtocolError> {
    optional_str(value, key).ok_or_else(|| ProtocolError::missing_field(key))
}

fn optional_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn authority_epoch(value: &Value) -> Option<u64> {
    optional_u64(value, "authority_epoch").or_else(|| optional_u64(value, "epoch"))
}\n
fn optional_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|n| u64::try_from(n).ok()))
    })
}

fn optional_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64)
}

fn optional_array2(value: &Value, key: &str) -> Option<Position2> {
    value.get(key).map(pos2_from_value)
}

fn pos2_from_value(value: &Value) -> Position2 {
    let arr = value.as_array();
    Position2::new(array_f64(arr, 0), array_f64(arr, 1))
}

fn vel2_from_value(value: &Value) -> Velocity2 {
    let arr = value.as_array();
    Velocity2::new(array_f64(arr, 0), array_f64(arr, 1))
}

fn array_f64(arr: Option<&Vec<Value>>, index: usize) -> f64 {
    arr.and_then(|arr| arr.get(index))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolErrorKind;
    use crate::PROTOCOL_VERSION;

    fn assert_roundtrip(raw: Value) {
        let decoded = decode_json_value(&raw).unwrap();
        assert_eq!(encode_json_value(&decoded), raw);
    }

    #[test]
    fn worker_connect_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "WorkerConnect",
            "worker_id": "zw-W",
            "region": "W",
            "attributes": ["physics", "server"],
            "proto": PROTOCOL_VERSION,
        }));
    }

    #[test]
    fn update_component_json_roundtrips_with_epoch() {
        assert_roundtrip(json!({
            "op": "UpdateComponent",
            "entity": "ship-1",
            "comp": "pos",
            "value": [12.5, -3.0],
            "authority_epoch": 42,
        }));
    }

    #[test]
    fn component_update_visibility_frame_roundtrips() {
        assert_roundtrip(json!({
            "op": "ComponentUpdate",
            "entity": "ship-1",
            "comp": "pos",
            "value": [12.5, -3.0],
            "fidelity": "coarse",
        }));
    }

    #[test]
    fn batch_update_array_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "BatchUpdate",
            "comp": "vel",
            "updates": [
                ["a", [1.0, 0.0], 7],
                ["b", [0.0, 1.0]],
            ],
        }));
    }

    #[test]
    fn critical_section_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "CriticalSection",
            "phase": "begin",
            "entity": "ship-1",
        }));
    }

    #[test]
    fn add_entity_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "AddEntity",
            "entity": "ship-1",
            "components": {
                "pos": [1.0, 2.0],
                "vel": [0.1, 0.0],
                "mass": 2.5,
            },
        }));
    }

    #[test]
    fn remove_entity_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "RemoveEntity",
            "entity": "ship-1",
        }));
    }

    #[test]
    fn create_entity_preserves_rich_component_bag() {
        assert_roundtrip(json!({
            "op": "CreateEntity",
            "request_id": "create-7",
            "entity": "body-7",
            "region": "W",
            "components": {
                "pos": [10.0, 0.0, 20.0],
                "vel": [1.0, 0.0, 0.0],
                "physics": {
                    "rot": 1.5,
                    "lin": [1.0, 2.0],
                    "ang": 0.25,
                    "at_rest": false
                },
                "schema": "body-v2",
                "kind": "unit"
            },
        }));
    }

    #[test]
    fn delete_entity_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "DeleteEntity",
            "request_id": "del-1",
            "entity": "ship-1",
            "authority_epoch": 9,
        }));
    }

    #[test]
    fn reserve_entity_ids_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "ReserveEntityIds",
            "request_id": "reserve-1",
            "count": 32,
        }));
    }

    #[test]
    fn dynamic_component_json_roundtrips() {
        assert_roundtrip(json!({
            "op": "AddComponent",
            "entity": "ship-1",
            "comp": "health",
            "value": { "hp": 100, "max": 100 },
            "authority_epoch": 5,
        }));
        assert_roundtrip(json!({
            "op": "RemoveComponent",
            "entity": "ship-1",
            "comp": "health",
            "authority_epoch": 6,
        }));
    }

    #[test]
    fn mesh_handoff_preserves_authority_and_components() {
        assert_roundtrip(json!({
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
                "kind": "unit"
            }
        }));
    }

    #[test]
    fn authority_change_preserves_loss_imminent_metadata() {
        assert_roundtrip(json!({
            "op": "AuthorityChange",
            "entity": "ship",
            "comp": "pos",
            "authoritative": false,
            "authority_epoch": 12,
            "mode": "server_physics_island",
            "state": "AUTHORITY_LOSS_IMMINENT",
            "handoff_target": "zw-E",
            "handoff_target_region": "E",
        }));
    }

    #[test]
    fn update_rejected_preserves_admin_stale_ghost_metadata() {
        assert_roundtrip(json!({
            "op": "UpdateRejected",
            "request_id": "admin-1",
            "entity": "ship",
            "comp": "pos",
            "reason": "stale authority epoch",
            "authority_epoch": 13,
            "ghost": true,
            "owner_region": "E",
        }));
    }

    #[test]
    fn entity_query_request_response_roundtrips() {
        assert_roundtrip(json!({
            "op": "EntityQuery",
            "request_id": "q-1",
            "include_handoff_intent": true,
            "query": { "type": "sphere", "center": [0.0, 0.0], "radius": 50.0 },
        }));
        assert_roundtrip(json!({
            "op": "EntityQueryResponse",
            "request_id": "q-1",
            "count": 1,
            "entities": [{
                "entity": "ship-1",
                "pos": [1.0, 2.0],
                "components": { "kind": "ship" },
                "region": "W",
                "authority": { "pos": { "owner": "zw-W", "epoch": 2 } },
            }],
        }));
    }

    #[test]
    fn command_request_response_roundtrips() {
        assert_roundtrip(json!({
            "op": "CommandRequest",
            "request_id": "cmd-1",
            "entity": "ship-1",
            "command": "ping",
            "payload": { "mode": "test" },
            "caller": "client-1",
        }));
        assert_roundtrip(json!({
            "op": "CommandResponse",
            "request_id": "cmd-1",
            "success": true,
            "payload": { "accepted": true },
        }));
    }

    #[test]
    fn entity_event_roundtrips_with_ordering_metadata() {
        assert_roundtrip(json!({
            "op": "EntityEvent",
            "entity": "ship-1",
            "event": "StatusChanged",
            "payload": { "amount": 12 },
            "sim_time": 123.5,
            "gen": 77,
            "class": "critical",
            "count": 3,
        }));
    }

    #[test]
    fn inspector_query_frame_roundtrips() {
        assert_roundtrip(json!({
            "op": "InspectorQuery",
            "request_id": "inspect-1",
            "max_entities": 128,
        }));
        assert_roundtrip(json!({
            "op": "InspectorFrame",
            "request_id": "inspect-1",
            "t_server": 123456,
            "broker": { "entity_count": 2, "handoffs": 1 },
            "zones": [{ "region": "W", "worker": "zw-W" }],
            "workers": [{ "worker_id": "zw-W", "region": "W" }],
            "entities": [{ "entity": "ship-1", "region": "W" }],
        }));
    }

    #[test]
    fn admin_response_ops_roundtrip() {
        assert_roundtrip(json!({
            "op": "CreateEntityResponse",
            "request_id": "create-1",
            "entity": "ship-1",
            "success": false,
            "reason": "draining",
        }));
        assert_roundtrip(json!({
            "op": "DeleteEntityResponse",
            "request_id": "delete-1",
            "entity": "ship-1",
            "success": true,
            "idempotent": true,
        }));
        assert_roundtrip(json!({
            "op": "ReserveEntityIdsResponse",
            "request_id": "reserve-1",
            "first_id": 1000,
            "count": 32,
        }));
        assert_roundtrip(json!({
            "op": "SetComponentAuthorityResponse",
            "request_id": "auth-1",
            "entity": "ship-1",
            "comp": "pos",
            "success": true,
            "owner": "zw-W",
            "authority_epoch": 4,
            "mode": "server_physics_island",
        }));
        assert_roundtrip(json!({
            "op": "ThresholdTxResponse",
            "request_id": "tx-1",
            "entity": "ship-1",
            "tx_id": "threshold-1",
            "phase": "commit",
            "success": true,
        }));
    }

    #[test]
    fn admin_request_ops_roundtrip() {
        assert_roundtrip(json!({
            "op": "SetComponentAuthority",
            "request_id": "auth-1",
            "entity": "ship-1",
            "comp": "pos",
            "owner": "zw-W",
            "mode": "server_physics_island",
            "authority_epoch": 4,
        }));
        assert_roundtrip(json!({
            "op": "ThresholdTx",
            "request_id": "tx-1",
            "entity": "ship-1",
            "tx_id": "threshold-1",
            "phase": "prepare",
            "from": "W",
            "to": "E",
            "components": ["pos", "vel"],
        }));
        assert_roundtrip(json!({
            "op": "SnapshotMarker",
            "request_id": "snap-1",
            "snapshot_id": "s-1",
            "offset": 2048,
        }));
    }

    #[test]
    fn fold_flag_metrics_and_log_roundtrip() {
        assert_roundtrip(json!({
            "op": "Fold",
            "entity": "ship-1",
            "region": "MARS",
            "pos": [100.0, 200.0],
        }));
        assert_roundtrip(json!({
            "op": "FlagUpdate",
            "flag": "double_xp",
            "value": true,
        }));
        assert_roundtrip(json!({
            "op": "Metrics",
            "load": 0.75,
        }));
        assert_roundtrip(json!({
            "op": "LogMessage",
            "level": "debug",
            "message": "hello",
        }));
    }

    #[test]
    fn mesh_ghost_ops_roundtrip() {
        assert_roundtrip(json!({
            "op": "MeshGhost",
            "entity": "enemy-1",
            "pos": [49.0, 0.0],
            "vel": [1.0, 0.0],
            "components": { "kind": "unit", "team": "red" },
            "owner_region": "E",
        }));
        assert_roundtrip(json!({
            "op": "MeshGhostRemove",
            "entity": "enemy-1",
        }));
    }

    #[test]
    fn mesh_handoff_accepts_current_source_aliases() {
        let raw = json!({
            "op": "MeshHandoff",
            "entity": "ship",
            "src_region": "W",
            "target": "E",
            "pos": [1.0, 2.0],
            "vel": [3.0, 4.0],
            "authority_epoch": 9,
            "lease_epoch": 11,
            "source_durable_gen": 12,
        });

        let decoded = decode_json_value(&raw).unwrap();
        match decoded {
            Op::MeshHandoff(handoff) => {
                assert_eq!(handoff.source_region, Some(RegionId::from("W")));
                assert_eq!(handoff.target_region, RegionId::from("E"));
                assert_eq!(handoff.pos, Position2::new(1.0, 2.0));
                assert_eq!(handoff.vel, Velocity2::new(3.0, 4.0));
            }
            other => panic!("unexpected op: {other:?}"),
        }
    }

    #[test]
    fn worker_connect_rejects_unsupported_protocol_version() {
        let raw = json!({
            "op": "WorkerConnect",
            "worker_id": "future-worker",
            "region": "W",
            "proto": PROTOCOL_VERSION + 1,
        });

        let err = decode_json_value(&raw).unwrap_err();
        assert_eq!(err.kind, ProtocolErrorKind::UnsupportedVersion);
    }

    #[test]
    fn unknown_operation_is_structured_error() {
        let raw = json!({ "op": "DefinitelyNotAnOp" });

        let err = decode_json_value(&raw).unwrap_err();
        assert_eq!(err.kind, ProtocolErrorKind::UnknownOperation);
    }
}
