//! JSON codec for the current Godworks v1 debug/development wire.
//!
//! The broker still dispatches raw JSON today. This module is the compatibility
//! bridge: it documents and tests the current shape while giving future SDK work
//! a typed boundary.

use godworks_core::{Aoi2, ComponentName, EntityId, PeerId, Position2, RegionId, Velocity2};
use serde_json::{json, Map, Value};

use crate::{
    supports_protocol, AuthorityChange, BatchUpdate, BatchUpdateEntry, CreateEntity, Heartbeat,
    Interest, MeshAck, MeshHandoff, Op, ProtocolError, UpdateComponent, UpdateRejected,
    WorkerConnect,
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
        "CreateEntity" => decode_create_entity(value),
        "UpdateComponent" => decode_update_component(value),
        "BatchUpdate" => decode_batch_update(value),
        "AuthorityChange" => decode_authority_change(value),
        "UpdateRejected" => Ok(Op::UpdateRejected(UpdateRejected {
            entity: optional_str(value, "entity").map(EntityId::from),
            component: optional_str(value, "comp")
                .or_else(|| optional_str(value, "component"))
                .map(ComponentName::from),
            reason: optional_str(value, "reason").unwrap_or("").to_string(),
        })),
        "MeshHandoff" => decode_mesh_handoff(value),
        "MeshAck" => Ok(Op::MeshAck(MeshAck {
            entity: EntityId::from(required_str(value, "entity")?),
        })),
        "Health" => Ok(Op::Health),
        other => Err(ProtocolError::unknown_operation(other)),
    }
}

/// Encode a typed operation into the current JSON operation body.
pub fn encode_json_value(op: &Op) -> Value {
    match op {
        Op::WorkerConnect(op) => {
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
        Op::Disconnect => json!({ "op": "Disconnect" }),
        Op::Heartbeat(op) => {
            let mut obj = object_with_op("Heartbeat");
            if let Some(worker_id) = &op.worker_id {
                obj.insert("worker_id".to_string(), json!(worker_id.as_ref()));
            }
            Value::Object(obj)
        }
        Op::Interest(op) => encode_interest(op),
        Op::CreateEntity(op) => {
            let mut components = Map::new();
            components.insert("pos".to_string(), json!(op.pos.to_array()));
            components.insert("vel".to_string(), json!(op.vel.to_array()));

            let mut obj = object_with_op("CreateEntity");
            obj.insert("entity".to_string(), json!(op.entity.as_ref()));
            if let Some(region) = &op.requested_region {
                obj.insert("region".to_string(), json!(region.as_ref()));
            }
            obj.insert("components".to_string(), Value::Object(components));
            Value::Object(obj)
        }
        Op::UpdateComponent(op) => {
            let mut obj = object_with_op("UpdateComponent");
            obj.insert("entity".to_string(), json!(op.entity.as_ref()));
            obj.insert("comp".to_string(), json!(op.component.as_ref()));
            obj.insert("value".to_string(), op.value.clone());
            if let Some(epoch) = op.authority_epoch {
                obj.insert("authority_epoch".to_string(), json!(epoch));
            }
            Value::Object(obj)
        }
        Op::BatchUpdate(op) => {
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
        Op::AuthorityChange(op) => json!({
            "op": "AuthorityChange",
            "entity": op.entity.as_ref(),
            "comp": op.component.as_ref(),
            "authoritative": op.authoritative,
            "authority_epoch": op.authority_epoch,
            "mode": op.mode.as_str(),
        }),
        Op::UpdateRejected(op) => {
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
        Op::MeshHandoff(op) => {
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
        Op::MeshAck(op) => json!({ "op": "MeshAck", "entity": op.entity.as_ref() }),
        Op::Health => json!({ "op": "Health" }),
    }
}

fn decode_worker_connect(value: &Value) -> Result<Op, ProtocolError> {
    let proto = optional_u64(value, "proto");
    if let Some(proto) = proto {
        if !supports_protocol(proto) {
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
    let aoi = center.zip(radius).map(|(center, radius)| Aoi2::Circle {
        center,
        radius,
    });

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
    let components = value.get("components");
    let pos = components
        .and_then(|c| c.get("pos"))
        .or_else(|| value.get("pos"))
        .map(pos2_from_value)
        .unwrap_or_default();
    let vel = components
        .and_then(|c| c.get("vel"))
        .or_else(|| value.get("vel"))
        .map(vel2_from_value)
        .unwrap_or_default();

    Ok(Op::CreateEntity(CreateEntity {
        entity: EntityId::from(required_str(value, "entity")?),
        requested_region: optional_str(value, "region").map(RegionId::from),
        pos,
        vel,
    }))
}

fn decode_update_component(value: &Value) -> Result<Op, ProtocolError> {
    Ok(Op::UpdateComponent(UpdateComponent {
        entity: EntityId::from(required_str(value, "entity")?),
        component: ComponentName::from(
            optional_str(value, "comp")
                .or_else(|| optional_str(value, "component"))
                .ok_or_else(|| ProtocolError::missing_field("comp"))?,
        ),
        value: value.get("value").cloned().unwrap_or(Value::Null),
        authority_epoch: optional_u64(value, "authority_epoch")
            .or_else(|| optional_u64(value, "epoch")),
    }))
}

fn decode_batch_update(value: &Value) -> Result<Op, ProtocolError> {
    let component = ComponentName::from(
        optional_str(value, "comp")
            .or_else(|| optional_str(value, "component"))
            .ok_or_else(|| ProtocolError::missing_field("comp"))?,
    );
    let mut updates = Vec::new();

    if let Some(items) = value.get("updates").and_then(Value::as_array) {
        for item in items {
            if let Some(entry) = item.as_array() {
                let entity = entry
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProtocolError::malformed("BatchUpdate entry missing entity"))?;
                let update_value = entry.get(1).cloned().unwrap_or(Value::Null);
                let authority_epoch = entry.get(2).and_then(Value::as_u64);
                updates.push(BatchUpdateEntry {
                    entity: EntityId::from(entity),
                    value: update_value,
                    authority_epoch,
                });
            } else if let Some(obj) = item.as_object() {
                let entity = obj
                    .get("entity")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ProtocolError::malformed("BatchUpdate object entry missing entity")
                    })?;
                updates.push(BatchUpdateEntry {
                    entity: EntityId::from(entity),
                    value: obj.get("value").cloned().unwrap_or(Value::Null),
                    authority_epoch: obj.get("authority_epoch").and_then(Value::as_u64),
                });
            } else {
                return Err(ProtocolError::malformed(
                    "BatchUpdate updates entries must be arrays or objects",
                ));
            }
        }
    } else if let Some(values) = value.get("values").and_then(Value::as_object) {
        let shared_epoch = optional_u64(value, "authority_epoch")
            .or_else(|| optional_u64(value, "epoch"));
        for (entity, update_value) in values {
            updates.push(BatchUpdateEntry {
                entity: EntityId::from(entity.clone()),
                value: update_value.clone(),
                authority_epoch: shared_epoch,
            });
        }
    }

    Ok(Op::BatchUpdate(BatchUpdate { component, updates }))
}

fn decode_authority_change(value: &Value) -> Result<Op, ProtocolError> {
    Ok(Op::AuthorityChange(AuthorityChange {
        entity: EntityId::from(required_str(value, "entity")?),
        component: ComponentName::from(optional_str(value, "comp").unwrap_or("pos")),
        authoritative: value
            .get("authoritative")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        authority_epoch: optional_u64(value, "authority_epoch")
            .or_else(|| optional_u64(value, "epoch"))
            .unwrap_or(0),
        mode: optional_str(value, "mode").unwrap_or("").to_string(),
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
        authority_epoch: optional_u64(value, "authority_epoch")
            .or_else(|| optional_u64(value, "epoch")),
        lease_epoch: optional_u64(value, "lease_epoch"),
        source_durable_gen: optional_u64(value, "source_durable_gen"),
    }))
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

fn object_with_op(op: &str) -> Map<String, Value> {
    let mut obj = Map::new();
    obj.insert("op".to_string(), json!(op));
    obj
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

fn optional_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
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
    Position2::new(
        arr.and_then(|a| a.first())
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        arr.and_then(|a| a.get(1))
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
    )
}

fn vel2_from_value(value: &Value) -> Velocity2 {
    let arr = value.as_array();
    Velocity2::new(
        arr.and_then(|a| a.first())
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        arr.and_then(|a| a.get(1))
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProtocolErrorKind, PROTOCOL_VERSION};

    #[test]
    fn worker_connect_json_roundtrips() {
        let raw = json!({
            "op": "WorkerConnect",
            "worker_id": "zw-W",
            "region": "W",
            "attributes": ["physics", "server"],
            "proto": PROTOCOL_VERSION
        });

        let decoded = decode_json_value(&raw).unwrap();
        assert_eq!(encode_json_value(&decoded), raw);
    }

    #[test]
    fn update_component_json_roundtrips_with_epoch() {
        let raw = json!({
            "op": "UpdateComponent",
            "entity": "ship-1",
            "comp": "pos",
            "value": [12.5, -3.0],
            "authority_epoch": 42
        });

        let decoded = decode_json_value(&raw).unwrap();
        assert_eq!(encode_json_value(&decoded), raw);
    }

    #[test]
    fn batch_update_array_json_roundtrips() {
        let raw = json!({
            "op": "BatchUpdate",
            "comp": "vel",
            "updates": [
                ["a", [1.0, 0.0], 7],
                ["b", [0.0, 1.0]]
            ]
        });

        let decoded = decode_json_value(&raw).unwrap();
        assert_eq!(encode_json_value(&decoded), raw);
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
    fn worker_connect_rejects_unsupported_protocol_version() {
        let raw = json!({
            "op": "WorkerConnect",
            "worker_id": "future-worker",
            "region": "W",
            "proto": PROTOCOL_VERSION + 1
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
