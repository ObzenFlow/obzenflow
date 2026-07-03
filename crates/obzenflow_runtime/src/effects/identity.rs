// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

pub(super) fn descriptor_for_effect<E>(
    effect: &E,
    stage_logic_version: String,
    effect_type: &'static str,
    schema_version: u32,
) -> Result<EffectDescriptor, EffectError>
where
    E: Effect,
{
    Ok(EffectDescriptor::new(
        effect_type,
        effect.label(),
        schema_version,
        stage_logic_version,
        hash_json_value(&effect.canonical_input())?,
    ))
}

pub(super) fn descriptor_hash(
    descriptor: &EffectDescriptor,
) -> Result<EffectDescriptorHash, EffectError> {
    Ok(EffectDescriptorHash::from(hash_json_value(
        &serde_json::to_value(descriptor).map_err(|e| EffectError::Serialization(e.to_string()))?,
    )?))
}

pub(super) fn hash_json_value(value: &Value) -> Result<String, EffectError> {
    let canonical = canonicalize_json_value(value.clone());
    let bytes =
        serde_json::to_vec(&canonical).map_err(|e| EffectError::Serialization(e.to_string()))?;
    Ok(hex_digest(digest(&SHA256, &bytes).as_ref()))
}

fn canonicalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::new();
            for (key, value) in entries {
                out.insert(key, canonicalize_json_value(value));
            }
            Value::Object(out)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(canonicalize_json_value).collect())
        }
        other => other,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Stage-local ordinal for deterministic output event identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectOutputOrdinal(u32);

impl EffectOutputOrdinal {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn checked_add(self, rhs: u32) -> Option<Self> {
        self.0.checked_add(rhs).map(Self)
    }
}

impl From<u32> for EffectOutputOrdinal {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl TryFrom<usize> for EffectOutputOrdinal {
    type Error = std::num::TryFromIntError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self::new(u32::try_from(value)?))
    }
}

pub fn deterministic_event_id(
    recorded_flow_id: impl AsRef<str>,
    stage_key: impl AsRef<str>,
    input_seq: StageInputPosition,
    output_ordinal: impl Into<EffectOutputOrdinal>,
) -> EventId {
    let output_ordinal = output_ordinal.into();
    let material = format!(
        "{recorded_flow_id}:{stage_key}:{}:{output_ordinal}",
        input_seq.0,
        recorded_flow_id = recorded_flow_id.as_ref(),
        stage_key = stage_key.as_ref(),
        output_ordinal = output_ordinal.get()
    );
    let hash = digest(&SHA256, material.as_bytes());
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&hash.as_ref()[..16]);
    EventId::from(obzenflow_core::Ulid(u128::from_be_bytes(id_bytes)))
}

pub fn deterministic_event_time(
    input_seq: StageInputPosition,
    output_ordinal: impl Into<EffectOutputOrdinal>,
) -> u64 {
    let output_ordinal = output_ordinal.into();
    input_seq
        .0
        .saturating_mul(1_000)
        .saturating_add(u64::from(output_ordinal.get()))
}

pub fn deterministic_effect_record_event_id(
    cursor: &EffectCursor,
    event_type: impl AsRef<str>,
) -> EventId {
    let material = format!(
        "effect-record:v1:{}:{}:{}:{}:{}",
        event_type.as_ref(),
        cursor.recorded_flow_id.as_str(),
        cursor.stage_key.as_str(),
        cursor.input_seq.get(),
        cursor.effect_ordinal.get()
    );
    let hash = digest(&SHA256, material.as_bytes());
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&hash.as_ref()[..16]);
    EventId::from(obzenflow_core::Ulid(u128::from_be_bytes(id_bytes)))
}

pub fn deterministic_effect_record_event_time(cursor: &EffectCursor) -> u64 {
    cursor
        .input_seq
        .get()
        .saturating_mul(1_000)
        .saturating_add(u64::from(cursor.effect_ordinal.get()))
}

#[allow(clippy::too_many_arguments)]
pub fn deterministic_typed_output_event<Out>(
    writer_id: WriterId,
    parent: &ChainEvent,
    output: Out,
    recorded_flow_id: impl AsRef<str>,
    stage_key: impl AsRef<str>,
    input_seq: StageInputPosition,
    output_ordinal: impl Into<EffectOutputOrdinal>,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Result<ChainEvent, EffectError>
where
    Out: TypedPayload,
{
    let output_ordinal = output_ordinal.into();
    let payload =
        serde_json::to_value(output).map_err(|e| EffectError::Serialization(e.to_string()))?;
    let mut event = ChainEventFactory::derived_data_event(
        writer_id,
        parent,
        Out::versioned_event_type(),
        payload,
        lineage,
    );
    event.id = deterministic_event_id(recorded_flow_id, stage_key, input_seq, output_ordinal);
    event.processing_info.event_time = deterministic_event_time(input_seq, output_ordinal);
    Ok(event)
}
