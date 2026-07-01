//! JSON codec for the current Godworks v1 debug/development wire.
//!
//! The broker still dispatches raw JSON today. This module is the compatibility
//! bridge: it documents and tests the current shape while giving future SDK work
//! a typed boundary.

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId, Velocity2};
use serde_json::{json, Map, Value};

use crate::{
    AddComponent, AddEntity, AuthReject, AuthorityChange, BatchUpdate, BatchUpdateEntry,
    CommandRequest, CommandResponse, ComponentUpdate, CreateEntity, CreateEntityResponse,
    CriticalSection, DeleteEntity, DeleteEntityResponse, EntityEvent, EntityQuery,
    EntityQueryResponse, FlagUpdate, Fold, Heartbeat, InspectorFrame, InspectorQuery, Interest,
    JsonFields, LogMessage, MeshAck, MeshGhost, MeshGhostRemove, MeshHandoff, Metrics, Op,
    ProtocolError, RemoveComponent, RemoveEntity, ReserveEntityIds, ReserveEntityIdsResponse,
    SetComponentAuthority, SetComponentAuthorityResponse, SnapshotManifest, SnapshotMarker,
    ThresholdTx, ThresholdTxResponse, UpdateComponent, UpdateRejected, WorkerConnect,
};

pub fn decode_json_value(value: &Value) -> Result<Op, ProtocolError> {
    let op = required_str(value, "op")?;
    match op {
        "WorkerConnect" => decode_worker_connect(value),
        "AuthReject" => Ok(Op::AuthReject(AuthReject {
            worker_id: optional_str(value, "worker_id").map(PeerId::from),
            error: optional_str(value, "error")
                .unwrap_or("auth_error")
                .to_string(),
            reason: optional_str(value, "reason").unwrap_or("").to_string(),
        })),
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
        "SnapshotManifest" => Ok(Op::SnapshotManifest(SnapshotManifest {
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

pub fn encode_json_value(op: &Op) -> Value {
    match op {
        Op::WorkerConnect(op) => encode_worker_connect(op),
        Op::AuthReject(op) => encode_auth_reject(op),
        Op::Disconnect => json!({ "op": "Disconnect" }),
        Op::Heartbeat(op) => encode_heartbeat(op),
        Op::Interest(op) => encode_interest(op),
        Op::CriticalSection(op) => encode_critical_section(op),
        Op::AddEntity(op) => encode_add_entity(op),
        Op::RemoveEntity(op) => json!({ "op": "RemoveEntity", "entity": op.entity.as_ref() }),
        Op::CreateEntity(op) => encode_create_entity(op),
        Op::CreateEntityResponse(op) => encode_json_fields("CreateEntityResponse", &op.fields),
        Op::DeleteEntity(op) => encode_delete_entity(op),
        Op::DeleteEntityResponse(op) => encode_json_fields("DeleteEntityResponse", &op.fields),
        Op::ReserveEntityIds(op) => encode_reserve_entity_ids(op),
        Op::ReserveEntityIdsResponse(op) => {
            encode_json_fields("ReserveEntityIdsResponse", &op.fields)
        }
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
        Op::SnapshotManifest(op) => encode_json_fields("SnapshotManifest", &op.fields),
        Op::MeshHandoff(op) => encode_mesh_handoff(op),
        Op::MeshAck(op) => json!({ "op": "MeshAck", "entity": op.entity.as_ref() }),
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
        auth_token: optional_str(value, "auth_token").map(str::to_string),
    }))
}

fn decode_interest(value: &Value) -> Result<Op, ProtocolError> {
    let aoi = optional_array2(value, "center")
        .zip(optional_f64(value, "radius"))
        .map(|(center, radius)| Aoi2::Circle { center, radius });
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
    if let Some(auth_token) = &op.auth_token {
        obj.insert("auth_token".to_string(), json!(auth_token));
    }
    Value::Object(obj)
}

fn encode_auth_reject(op: &AuthReject) -> Value {
    let mut obj = object_with_op("AuthReject");
    if let Some(worker_id) = &op.worker_id {
        obj.insert("worker_id".to_string(), json!(worker_id.as_ref()));
    }
    obj.insert("error".to_string(), json!(op.error));
    obj.insert("reason".to_string(), json!(op.reason));
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
    json!({ "op": "BatchUpdate", "comp": op.component.as_ref(), "updates": updates })
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
}

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
    use crate::{operation_semantics, OperationCategory, OperationPersistence};
    use crate::{PartitionSchema, SpatialSchema};

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
    fn worker_connect_auth_token_roundtrips() {
        assert_roundtrip(json!({
            "op": "WorkerConnect",
            "worker_id": "zw-W",
            "region": "W",
            "auth_token": "test-token"
        }));
        assert_roundtrip(json!({
            "op": "AuthReject",
            "worker_id": "zw-W",
            "error": "auth_error",
            "reason": "authentication required"
        }));
    }

    #[test]
    fn component_update_visibility_frame_roundtrips() {
        assert_roundtrip(json!({
            "op": "ComponentUpdate",
            "entity": "node-1",
            "comp": "pos",
            "value": [12.5, -3.0],
            "fidelity": "coarse",
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
                "physics": { "rot": 1.5, "lin": [1.0, 2.0], "ang": 0.25, "at_rest": false },
                "schema": "body-v2",
                "kind": "unit"
            },
        }));
    }

    #[test]
    fn lifecycle_and_dynamic_component_ops_roundtrip() {
        assert_roundtrip(json!({ "op": "CriticalSection", "phase": "begin", "entity": "node-1" }));
        assert_roundtrip(
            json!({ "op": "AddEntity", "entity": "node-1", "components": { "mass": 2.5 } }),
        );
        assert_roundtrip(json!({ "op": "RemoveEntity", "entity": "node-1" }));
        assert_roundtrip(
            json!({ "op": "DeleteEntity", "request_id": "del-1", "entity": "node-1", "authority_epoch": 9 }),
        );
        assert_roundtrip(
            json!({ "op": "ReserveEntityIds", "request_id": "reserve-1", "count": 32 }),
        );
        assert_roundtrip(
            json!({ "op": "AddComponent", "entity": "node-1", "comp": "health", "value": { "hp": 100 }, "authority_epoch": 5 }),
        );
        assert_roundtrip(
            json!({ "op": "RemoveComponent", "entity": "node-1", "comp": "health", "authority_epoch": 6 }),
        );
        assert_roundtrip(
            json!({ "op": "UpdateComponent", "entity": "node-1", "comp": "pos", "value": [1.0, 2.0], "authority_epoch": 7 }),
        );
        assert_roundtrip(
            json!({ "op": "BatchUpdate", "comp": "vel", "updates": [["a", [1.0, 0.0], 7], ["b", [0.0, 1.0]]] }),
        );
    }

    #[test]
    fn mesh_handoff_preserves_authority_and_components() {
        assert_roundtrip(json!({
            "op": "MeshHandoff",
            "entity": "node-1",
            "source_region": "W",
            "target": "E",
            "pos": [1.0, 2.0],
            "vel": [3.0, 4.0],
            "authority_epoch": 9,
            "lease_epoch": 11,
            "source_durable_gen": 12,
            "authority": { "pos": { "owner": "zw-E", "epoch": 9, "mode": "server_physics_island" } },
            "components": { "mass": 2.0, "kind": "unit" }
        }));
    }

    #[test]
    fn mesh_handoff_roundtrips_current_broker_src_region_wire_shape() {
        assert_roundtrip(json!({
            "op": "MeshHandoff",
            "entity": "node-1",
            "src_region": "W",
            "target": "E",
            "pos": [1.0, 2.0],
            "vel": [3.0, 4.0],
            "authority_epoch": 9,
            "lease_epoch": 11,
            "source_durable_gen": 12,
            "authority": { "pos": { "owner": "zw-E", "epoch": 9, "mode": "server_physics_island" } },
            "components": { "mass": 2.0, "kind": "unit" }
        }));
    }

    #[test]
    fn authority_change_preserves_loss_imminent_metadata() {
        assert_roundtrip(json!({
            "op": "AuthorityChange",
            "entity": "node-1",
            "comp": "pos",
            "authoritative": false,
            "authority_epoch": 12,
            "mode": "server_physics_island",
            "state": "AUTHORITY_LOSS_IMMINENT",
            "handoff_target": "zw-E",
            "handoff_target_region": "E"
        }));
    }

    #[test]
    fn update_rejected_preserves_admin_stale_ghost_metadata() {
        assert_roundtrip(json!({
            "op": "UpdateRejected",
            "request_id": "admin-1",
            "entity": "node-1",
            "comp": "pos",
            "reason": "stale authority epoch",
            "authority_epoch": 13,
            "ghost": true,
            "owner_region": "E"
        }));
    }

    #[test]
    fn query_command_event_inspector_and_admin_frames_roundtrip() {
        assert_roundtrip(
            json!({ "op": "EntityQuery", "request_id": "q-1", "include_handoff_intent": true, "query": { "type": "sphere", "center": [0.0, 0.0], "radius": 50.0 } }),
        );
        assert_roundtrip(
            json!({ "op": "EntityQueryResponse", "request_id": "q-1", "count": 1, "entities": [{ "entity": "node-1", "pos": [1.0, 2.0], "components": { "kind": "unit" }, "region": "W", "authority": { "pos": { "owner": "zw-W", "epoch": 2 } } }] }),
        );
        assert_roundtrip(
            json!({ "op": "CommandRequest", "request_id": "cmd-1", "entity": "node-1", "command": "ping", "payload": { "mode": "test" }, "caller": "client-1" }),
        );
        assert_roundtrip(
            json!({ "op": "CommandResponse", "request_id": "cmd-1", "success": true, "payload": { "accepted": true } }),
        );
        assert_roundtrip(
            json!({ "op": "EntityEvent", "entity": "node-1", "event": "StatusChanged", "payload": { "amount": 12 }, "sim_time": 123.5, "gen": 77, "class": "critical", "count": 3 }),
        );
        assert_roundtrip(
            json!({ "op": "InspectorQuery", "request_id": "inspect-1", "max_entities": 128 }),
        );
        assert_roundtrip(
            json!({ "op": "InspectorFrame", "request_id": "inspect-1", "t_server": 123456, "broker": { "entity_count": 2 }, "zones": [], "workers": [], "entities": [] }),
        );
        assert_roundtrip(
            json!({ "op": "CreateEntityResponse", "request_id": "create-1", "entity": "node-1", "success": false, "reason": "draining" }),
        );
        assert_roundtrip(
            json!({ "op": "DeleteEntityResponse", "request_id": "delete-1", "entity": "node-1", "success": true, "idempotent": true }),
        );
        assert_roundtrip(
            json!({ "op": "ReserveEntityIdsResponse", "request_id": "reserve-1", "first_id": 1000, "count": 32 }),
        );
        assert_roundtrip(
            json!({ "op": "SetComponentAuthority", "request_id": "auth-1", "entity": "node-1", "comp": "pos", "owner": "zw-W", "mode": "server_physics_island", "authority_epoch": 4 }),
        );
        assert_roundtrip(
            json!({ "op": "SetComponentAuthorityResponse", "request_id": "auth-1", "entity": "node-1", "comp": "pos", "success": true, "owner": "zw-W", "authority_epoch": 4, "mode": "server_physics_island" }),
        );
        assert_roundtrip(
            json!({ "op": "ThresholdTx", "request_id": "tx-1", "entity": "node-1", "tx_id": "threshold-1", "phase": "prepare", "from": "W", "to": "E", "components": ["pos", "vel"] }),
        );
        assert_roundtrip(
            json!({ "op": "ThresholdTxResponse", "request_id": "tx-1", "entity": "node-1", "tx_id": "threshold-1", "phase": "commit", "success": true }),
        );
        assert_roundtrip(
            json!({ "op": "SnapshotMarker", "request_id": "snap-1", "snapshot_id": "s-1", "offset": 2048 }),
        );
        assert_roundtrip(json!({
            "op": "SnapshotManifest",
            "request_id": "snap-1",
            "snapshot_id": "s-1",
            "wal_offset": 2048,
            "snapshot_manifest_version": 1,
            "snapshot_schema_version": 1,
            "spatial_schema_version": 1,
            "coordinate_codec_version": 1,
            "component_registry_version": 1,
            "partition_map_version": 1,
            "spatial_schema": {
                "spatial_dim": "D2",
                "coordinate_codec": "debug_f64_2",
                "partition_schema": { "kind": "strip1d", "boundary_count": 1 }
            }
        }));
    }

    #[test]
    fn snapshot_manifest_contract_accessors_match_current_wire_shape() {
        let decoded = decode_json_value(&json!({
            "op": "SnapshotManifest",
            "request_id": "snap-1",
            "snapshot_id": "s-1",
            "broker_id": "broker-a",
            "wal_offset": 2048,
            "entity_count": 3,
            "pending_mesh": 1,
            "authority_hash": "12345",
            "snapshot_manifest_version": 1,
            "snapshot_schema_version": 1,
            "spatial_schema_version": 1,
            "coordinate_codec_version": 1,
            "component_registry_version": 1,
            "partition_map_version": 7,
            "spatial_schema": {
                "spatial_dim": "D2",
                "coordinate_codec": "debug_f64_2",
                "partition_schema": { "kind": "grid2d", "cols": 3, "rows": 2 }
            },
            "partition_map": {
                "version": 7,
                "kind": "grid2d",
                "cols": 3,
                "rows": 2,
                "cell_w": 10.0,
                "cell_h": 20.0,
                "origin": [0.0, 0.0]
            }
        }))
        .unwrap();
        let Op::SnapshotManifest(manifest) = decoded else {
            panic!("expected SnapshotManifest");
        };

        assert_eq!(manifest.request_id(), Some("snap-1"));
        assert_eq!(manifest.snapshot_id(), Some("s-1"));
        assert_eq!(manifest.broker_id(), Some("broker-a"));
        assert_eq!(manifest.wal_offset(), Some(2048));
        assert_eq!(manifest.entity_count(), Some(3));
        assert_eq!(manifest.pending_mesh(), Some(1));
        assert_eq!(manifest.authority_hash(), Some("12345"));
        assert!(manifest.has_current_versions());
        assert_eq!(manifest.partition_map_version(), Some(7));
        assert_eq!(
            manifest.spatial_schema(),
            Some(SpatialSchema::current_2d(PartitionSchema::Grid2D {
                cols: 3,
                rows: 2
            }))
        );
        assert_eq!(
            manifest.partition_map(),
            Some(crate::VersionedPartitionMap::new(
                7,
                crate::PartitionMapSpec::grid2d(3, 2, 10.0, 20.0, [0.0, 0.0]).unwrap()
            ))
        );
    }

    #[test]
    fn snapshot_manifest_contract_rejects_invalid_spatial_schema() {
        let decoded = decode_json_value(&json!({
            "op": "SnapshotManifest",
            "snapshot_manifest_version": 1,
            "snapshot_schema_version": 1,
            "spatial_schema_version": 1,
            "coordinate_codec_version": 1,
            "component_registry_version": 1,
            "partition_map_version": 1,
            "wal_offset": 1,
            "spatial_schema": {
                "spatial_dim": "D2",
                "coordinate_codec": "debug_f64_2",
                "partition_schema": { "kind": "grid2d", "cols": 0, "rows": 2 }
            }
        }))
        .unwrap();
        let Op::SnapshotManifest(manifest) = decoded else {
            panic!("expected SnapshotManifest");
        };

        assert_eq!(manifest.spatial_schema(), None);
        assert!(manifest.has_current_versions());
    }

    #[test]
    fn snapshot_manifest_contract_rejects_invalid_partition_map() {
        let decoded = decode_json_value(&json!({
            "op": "SnapshotManifest",
            "snapshot_manifest_version": 1,
            "snapshot_schema_version": 1,
            "spatial_schema_version": 1,
            "coordinate_codec_version": 1,
            "component_registry_version": 1,
            "partition_map_version": 1,
            "wal_offset": 1,
            "spatial_schema": {
                "spatial_dim": "D2",
                "coordinate_codec": "debug_f64_2",
                "partition_schema": { "kind": "grid2d", "cols": 2, "rows": 2 }
            },
            "partition_map": {
                "version": 1,
                "kind": "grid2d",
                "cols": 2,
                "rows": 2,
                "cell_w": 0.0,
                "cell_h": 20.0,
                "origin": [0.0, 0.0]
            }
        }))
        .unwrap();
        let Op::SnapshotManifest(manifest) = decoded else {
            panic!("expected SnapshotManifest");
        };

        assert_eq!(manifest.partition_map(), None);
        assert!(manifest.has_current_versions());
    }

    #[test]
    fn snapshot_manifest_contract_rejects_future_versions() {
        let decoded = decode_json_value(&json!({
            "op": "SnapshotManifest",
            "snapshot_manifest_version": 2,
            "snapshot_schema_version": 1,
            "spatial_schema_version": 1,
            "coordinate_codec_version": 1,
            "component_registry_version": 1,
            "partition_map_version": 1,
            "wal_offset": 1,
            "spatial_schema": {
                "spatial_dim": "D2",
                "coordinate_codec": "debug_f64_2",
                "partition_schema": { "kind": "strip1d", "boundary_count": 1 }
            }
        }))
        .unwrap();
        let Op::SnapshotManifest(manifest) = decoded else {
            panic!("expected SnapshotManifest");
        };

        assert_eq!(manifest.snapshot_manifest_version(), Some(2));
        assert!(!manifest.has_current_versions());
        assert_eq!(
            manifest.spatial_schema(),
            Some(SpatialSchema::current_2d(PartitionSchema::strip1d(1)))
        );
    }

    #[test]
    fn command_and_event_semantic_accessors_match_current_wire_names() {
        let command = decode_json_value(&json!({
            "op": "CommandRequest",
            "request_id": "cmd-1",
            "entity": "node-1",
            "command": "UseTool",
            "payload": { "slot": 2 },
            "caller": "client-1",
            "idempotency_key": "cmd-1/client-1",
            "timeout_ms": 250
        }))
        .unwrap();
        let Op::CommandRequest(command) = command else {
            panic!("expected CommandRequest");
        };
        assert_eq!(command.request_id(), Some("cmd-1"));
        assert_eq!(command.entity(), Some("node-1"));
        assert_eq!(command.command(), Some(&json!("UseTool")));
        assert_eq!(command.payload(), Some(&json!({ "slot": 2 })));
        assert_eq!(command.caller(), Some("client-1"));
        assert_eq!(command.idempotency_key(), Some("cmd-1/client-1"));
        assert_eq!(command.timeout_ms(), Some(250));

        let response = decode_json_value(&json!({
            "op": "CommandResponse",
            "request_id": "cmd-1",
            "success": false,
            "reason": "cooldown",
            "payload": { "remaining_ms": 50 }
        }))
        .unwrap();
        let Op::CommandResponse(response) = response else {
            panic!("expected CommandResponse");
        };
        assert_eq!(response.request_id(), Some("cmd-1"));
        assert_eq!(response.success(), Some(false));
        assert!(!response.success_or_default());
        assert_eq!(response.reason(), Some("cooldown"));
        assert_eq!(response.payload(), Some(&json!({ "remaining_ms": 50 })));

        let event = decode_json_value(&json!({
            "op": "EntityEvent",
            "entity": "node-1",
            "event": "StatusChanged",
            "payload": { "amount": 12 },
            "sim_time": 123.5,
            "gen": 77,
            "class": "visual",
            "coalesce_key": "node-1:status",
            "count": 3
        }))
        .unwrap();
        let Op::EntityEvent(event) = event else {
            panic!("expected EntityEvent");
        };
        assert_eq!(event.entity(), Some("node-1"));
        assert_eq!(event.event(), Some(&json!("StatusChanged")));
        assert_eq!(event.payload(), Some(&json!({ "amount": 12 })));
        assert_eq!(event.sim_time(), Some(123.5));
        assert_eq!(event.gen(), Some(77));
        assert_eq!(event.class(), Some("visual"));
        assert_eq!(event.class_or_default(), "visual");
        assert_eq!(event.coalesce_key(), Some("node-1:status"));
        assert_eq!(event.count(), Some(3));
    }

    #[test]
    fn lifecycle_response_semantic_accessors_match_current_wire_names() {
        let create = decode_json_value(&json!({
            "op": "CreateEntityResponse",
            "request_id": "create-1",
            "entity": "node-1",
            "success": false,
            "reason": "draining"
        }))
        .unwrap();
        let Op::CreateEntityResponse(create) = create else {
            panic!("expected CreateEntityResponse");
        };
        assert_eq!(create.request_id(), Some("create-1"));
        assert_eq!(create.entity(), Some("node-1"));
        assert_eq!(create.success(), Some(false));
        assert_eq!(create.reason(), Some("draining"));

        let delete = decode_json_value(&json!({
            "op": "DeleteEntityResponse",
            "request_id": "delete-1",
            "entity": "node-1",
            "success": true,
            "idempotent": true
        }))
        .unwrap();
        let Op::DeleteEntityResponse(delete) = delete else {
            panic!("expected DeleteEntityResponse");
        };
        assert_eq!(delete.request_id(), Some("delete-1"));
        assert_eq!(delete.entity(), Some("node-1"));
        assert_eq!(delete.success(), Some(true));
        assert_eq!(delete.reason(), None);
        assert_eq!(delete.idempotent(), Some(true));

        let reserve = decode_json_value(&json!({
            "op": "ReserveEntityIdsResponse",
            "request_id": "reserve-1",
            "first_id": 1000,
            "count": 32
        }))
        .unwrap();
        let Op::ReserveEntityIdsResponse(reserve) = reserve else {
            panic!("expected ReserveEntityIdsResponse");
        };
        assert_eq!(reserve.request_id(), Some("reserve-1"));
        assert_eq!(reserve.first_id(), Some(1000));
        assert_eq!(reserve.count(), Some(32));
    }

    #[test]
    fn command_response_and_event_defaults_match_broker_semantics() {
        let response = decode_json_value(&json!({
            "op": "CommandResponse",
            "request_id": "cmd-2"
        }))
        .unwrap();
        let Op::CommandResponse(response) = response else {
            panic!("expected CommandResponse");
        };
        assert_eq!(response.success(), None);
        assert!(response.success_or_default());

        let event = decode_json_value(&json!({
            "op": "EntityEvent",
            "entity": "node-1",
            "event": "Ping"
        }))
        .unwrap();
        let Op::EntityEvent(event) = event else {
            panic!("expected EntityEvent");
        };
        assert_eq!(event.class(), None);
        assert_eq!(event.class_or_default(), "critical");
    }

    #[test]
    fn lifecycle_command_and_event_semantics_are_canonical() {
        let create = operation_semantics("CreateEntity").expect("CreateEntity semantics");
        assert_eq!(create.persistence, OperationPersistence::Persistent);
        assert_eq!(create.category, OperationCategory::EntityLifecycle);
        assert_eq!(create.response_op, Some("CreateEntityResponse"));

        let delete = operation_semantics("DeleteEntity").expect("DeleteEntity semantics");
        assert_eq!(delete.persistence, OperationPersistence::Persistent);
        assert_eq!(delete.category, OperationCategory::EntityLifecycle);
        assert_eq!(delete.response_op, Some("DeleteEntityResponse"));

        let reserve = operation_semantics("ReserveEntityIds").expect("ReserveEntityIds semantics");
        assert_eq!(reserve.persistence, OperationPersistence::Persistent);
        assert_eq!(reserve.category, OperationCategory::EntityLifecycle);
        assert_eq!(reserve.response_op, Some("ReserveEntityIdsResponse"));

        for response in [
            "CreateEntityResponse",
            "DeleteEntityResponse",
            "ReserveEntityIdsResponse",
        ] {
            let semantics = operation_semantics(response).expect("lifecycle response semantics");
            assert_eq!(semantics.persistence, OperationPersistence::Transient);
            assert_eq!(semantics.category, OperationCategory::LifecycleResponse);
            assert_eq!(semantics.response_op, None);
        }

        let command = operation_semantics("CommandRequest").expect("CommandRequest semantics");
        assert_eq!(command.persistence, OperationPersistence::Transient);
        assert_eq!(command.category, OperationCategory::CommandRpc);
        assert_eq!(command.response_op, Some("CommandResponse"));

        let response = operation_semantics("CommandResponse").expect("CommandResponse semantics");
        assert_eq!(response.persistence, OperationPersistence::Transient);
        assert_eq!(response.category, OperationCategory::CommandRpc);
        assert_eq!(response.response_op, None);

        let event = operation_semantics("EntityEvent").expect("EntityEvent semantics");
        assert_eq!(event.persistence, OperationPersistence::Transient);
        assert_eq!(event.category, OperationCategory::EntityEvent);
        assert_eq!(event.response_op, None);
    }

    #[test]
    fn mesh_authority_update_and_durability_semantics_are_canonical() {
        for op in [
            "AddComponent",
            "RemoveComponent",
            "UpdateComponent",
            "BatchUpdate",
        ] {
            let semantics = operation_semantics(op).expect("entity update semantics");
            assert_eq!(semantics.persistence, OperationPersistence::Persistent);
            assert_eq!(semantics.category, OperationCategory::EntityUpdate);
            assert_eq!(semantics.response_op, None);
        }

        let component_update =
            operation_semantics("ComponentUpdate").expect("ComponentUpdate semantics");
        assert_eq!(
            component_update.persistence,
            OperationPersistence::Transient
        );
        assert_eq!(component_update.category, OperationCategory::EntityUpdate);

        let set_authority =
            operation_semantics("SetComponentAuthority").expect("SetComponentAuthority semantics");
        assert_eq!(set_authority.persistence, OperationPersistence::Persistent);
        assert_eq!(set_authority.category, OperationCategory::AuthorityControl);
        assert_eq!(
            set_authority.response_op,
            Some("SetComponentAuthorityResponse")
        );

        let authority_response = operation_semantics("SetComponentAuthorityResponse")
            .expect("SetComponentAuthorityResponse semantics");
        assert_eq!(
            authority_response.persistence,
            OperationPersistence::Transient
        );
        assert_eq!(
            authority_response.category,
            OperationCategory::AuthorityResponse
        );

        let authority_change =
            operation_semantics("AuthorityChange").expect("AuthorityChange semantics");
        assert_eq!(
            authority_change.persistence,
            OperationPersistence::Transient
        );
        assert_eq!(authority_change.category, OperationCategory::AuthorityEvent);

        let fold = operation_semantics("Fold").expect("Fold semantics");
        assert_eq!(fold.persistence, OperationPersistence::Persistent);
        assert_eq!(fold.category, OperationCategory::HandoffControl);

        let tx = operation_semantics("ThresholdTx").expect("ThresholdTx semantics");
        assert_eq!(tx.persistence, OperationPersistence::Persistent);
        assert_eq!(tx.category, OperationCategory::TransactionControl);
        assert_eq!(tx.response_op, Some("ThresholdTxResponse"));

        let tx_response =
            operation_semantics("ThresholdTxResponse").expect("ThresholdTxResponse semantics");
        assert_eq!(tx_response.persistence, OperationPersistence::Transient);
        assert_eq!(tx_response.category, OperationCategory::TransactionResponse);

        let snapshot = operation_semantics("SnapshotMarker").expect("SnapshotMarker semantics");
        assert_eq!(snapshot.persistence, OperationPersistence::Persistent);
        assert_eq!(snapshot.category, OperationCategory::DurabilityControl);

        let handoff = operation_semantics("MeshHandoff").expect("MeshHandoff semantics");
        assert_eq!(handoff.persistence, OperationPersistence::Persistent);
        assert_eq!(handoff.category, OperationCategory::MeshHandoff);
        assert_eq!(handoff.response_op, Some("MeshAck"));

        let ack = operation_semantics("MeshAck").expect("MeshAck semantics");
        assert_eq!(ack.persistence, OperationPersistence::Persistent);
        assert_eq!(ack.category, OperationCategory::MeshHandoff);
        assert_eq!(ack.response_op, None);

        for op in ["MeshGhost", "MeshGhostRemove"] {
            let semantics = operation_semantics(op).expect("mesh ghost semantics");
            assert_eq!(semantics.persistence, OperationPersistence::Transient);
            assert_eq!(semantics.category, OperationCategory::InterestProjection);
            assert_eq!(semantics.response_op, None);
        }
    }

    #[test]
    fn misc_and_mesh_visibility_frames_roundtrip() {
        assert_roundtrip(
            json!({ "op": "Fold", "entity": "node-1", "region": "MARS", "pos": [100.0, 200.0] }),
        );
        assert_roundtrip(json!({ "op": "FlagUpdate", "flag": "double_xp", "value": true }));
        assert_roundtrip(json!({ "op": "Metrics", "load": 0.75 }));
        assert_roundtrip(json!({ "op": "LogMessage", "level": "debug", "message": "hello" }));
        assert_roundtrip(
            json!({ "op": "MeshGhost", "entity": "node-2", "pos": [49.0, 0.0], "vel": [1.0, 0.0], "components": { "kind": "unit" }, "owner_region": "E" }),
        );
        assert_roundtrip(json!({ "op": "MeshGhostRemove", "entity": "node-2" }));
    }

    #[test]
    fn mesh_handoff_accepts_current_source_aliases() {
        let raw = json!({
            "op": "MeshHandoff",
            "entity": "node-1",
            "src_region": "W",
            "target": "E",
            "pos": [1.0, 2.0],
            "vel": [3.0, 4.0],
            "authority_epoch": 9,
            "lease_epoch": 11,
            "source_durable_gen": 12
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
    fn protocol_errors_are_structured() {
        let future = json!({
            "op": "WorkerConnect",
            "worker_id": "future",
            "region": "W",
            "proto": PROTOCOL_VERSION + 1
        });
        assert_eq!(
            decode_json_value(&future).unwrap_err().kind,
            ProtocolErrorKind::UnsupportedVersion
        );
        assert_eq!(
            decode_json_value(&json!({ "op": "Nope" }))
                .unwrap_err()
                .kind,
            ProtocolErrorKind::UnknownOperation
        );
    }
}
