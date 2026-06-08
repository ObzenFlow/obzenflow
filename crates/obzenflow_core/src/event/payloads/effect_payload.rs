// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect-result payloads for replay-safe user effects.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const EFFECT_RECORD_EVENT_TYPE: &str = "obzenflow.effect_record.v1";
pub const CAPTURE_EVENT_TYPE: &str = "obzenflow.capture.v1";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectCursor {
    pub recorded_flow_id: String,
    pub stage_key: String,
    pub input_seq: u64,
    pub effect_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectDescriptor {
    pub effect_type: String,
    pub label: String,
    pub schema_version: u32,
    pub stage_logic_version: String,
    pub canonical_input_hash: String,
}

/// Stable hash of the effect descriptor used to reject replay drift.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EffectDescriptorHash(String);

impl EffectDescriptorHash {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for EffectDescriptorHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for EffectDescriptorHash {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for EffectDescriptorHash {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Position of one fact inside a multi-fact effect outcome group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutcomeFactOrdinal(u32);

impl OutcomeFactOrdinal {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for OutcomeFactOrdinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u32> for OutcomeFactOrdinal {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl TryFrom<usize> for OutcomeFactOrdinal {
    type Error = std::num::TryFromIntError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self::new(u32::try_from(value)?))
    }
}

/// Deterministic identifier shared by every fact from the same effect outcome.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EffectOutcomeGroupId(String);

impl EffectOutcomeGroupId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for EffectOutcomeGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for EffectOutcomeGroupId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for EffectOutcomeGroupId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Whether an effect-provenance fact is a reserved framework row or a user fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectFactOwner {
    #[default]
    User,
    Framework,
}

impl<'de> Deserialize<'de> for EffectFactOwner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EffectFactOwnerVisitor;

        impl Visitor<'_> for EffectFactOwnerVisitor {
            type Value = EffectFactOwner;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`user`, `framework`, or legacy framework-owned boolean")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(if value {
                    EffectFactOwner::Framework
                } else {
                    EffectFactOwner::User
                })
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "user" => Ok(EffectFactOwner::User),
                    "framework" => Ok(EffectFactOwner::Framework),
                    other => Err(E::unknown_variant(other, &["user", "framework"])),
                }
            }
        }

        deserializer.deserialize_any(EffectFactOwnerVisitor)
    }
}

impl EffectFactOwner {
    pub const fn is_user(&self) -> bool {
        matches!(self, Self::User)
    }

    pub const fn is_framework(&self) -> bool {
        matches!(self, Self::Framework)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EffectOutcomePayload {
    Succeeded {
        output: Value,
    },
    SucceededFact {
        event_type: String,
        output: Value,
        outcome_fact_ordinal: OutcomeFactOrdinal,
    },
    Failed {
        error_type: String,
        error_message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRecord {
    pub cursor: EffectCursor,
    pub descriptor_hash: EffectDescriptorHash,
    pub descriptor: EffectDescriptor,
    pub outcome: EffectOutcomePayload,
}

/// Replay identity for an effect-produced fact.
///
/// Vector clocks and event causality explain what happened-before what.
/// `EffectProvenance` explains which effect call this fact satisfies during
/// replay. It keeps the effect cursor, descriptor, and multi-fact outcome group
/// outside the domain payload while still letting `fx.perform` find and validate
/// the recorded fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectProvenance {
    /// Deterministic position of the `fx.perform` call that authored this fact.
    pub cursor: EffectCursor,
    /// Hash of the effect descriptor expected during replay.
    pub descriptor_hash: EffectDescriptorHash,
    /// Descriptor material retained so replay can diagnose hash drift loudly.
    pub descriptor: EffectDescriptor,
    /// Fact position inside the effect outcome group. Reserved framework rows
    /// do not set this; user-authored effect outcome facts must set it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome_fact_ordinal: Option<OutcomeFactOrdinal>,
    /// Deterministic id shared by every fact from the same effect outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<EffectOutcomeGroupId>,
    /// Distinguishes reserved framework rows from user-visible outcome facts.
    #[serde(
        default,
        alias = "framework_owned",
        skip_serializing_if = "EffectFactOwner::is_user"
    )]
    pub fact_owner: EffectFactOwner,
}

impl EffectProvenance {
    pub fn from_record(record: &EffectRecord, fact_owner: EffectFactOwner) -> Self {
        Self {
            cursor: record.cursor.clone(),
            descriptor_hash: record.descriptor_hash.clone(),
            descriptor: record.descriptor.clone(),
            outcome_fact_ordinal: None,
            group_id: Some(effect_outcome_group_id(&record.cursor)),
            fact_owner,
        }
    }
}

pub fn effect_outcome_group_id(cursor: &EffectCursor) -> EffectOutcomeGroupId {
    EffectOutcomeGroupId::new(format!(
        "effect-outcome:v1:{}:{}:{}:{}:{}:{}",
        cursor.recorded_flow_id.len(),
        cursor.recorded_flow_id.as_str(),
        cursor.stage_key.len(),
        cursor.stage_key.as_str(),
        cursor.input_seq,
        cursor.effect_ordinal
    ))
}

pub fn framework_effect_event_type(effect_type: &str) -> &'static str {
    if effect_type == "obzenflow.capture" {
        CAPTURE_EVENT_TYPE
    } else {
        EFFECT_RECORD_EVENT_TYPE
    }
}

pub fn is_framework_effect_event_type(event_type: &str) -> bool {
    event_type == EFFECT_RECORD_EVENT_TYPE || event_type == CAPTURE_EVENT_TYPE
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn effect_record(cursor: EffectCursor) -> EffectRecord {
        EffectRecord {
            cursor,
            descriptor_hash: "hash".into(),
            descriptor: EffectDescriptor {
                effect_type: "test.effect".to_string(),
                label: "test".to_string(),
                schema_version: 1,
                stage_logic_version: "v1".to_string(),
                canonical_input_hash: "input".to_string(),
            },
            outcome: EffectOutcomePayload::Succeeded {
                output: json!({"ok": true}),
            },
        }
    }

    #[test]
    fn effect_outcome_group_id_is_derived_from_cursor() {
        let cursor = EffectCursor {
            recorded_flow_id: "flow:with:colons".to_string(),
            stage_key: "stage".to_string(),
            input_seq: 7,
            effect_ordinal: 2,
        };
        let record = effect_record(cursor.clone());
        let expected = effect_outcome_group_id(&cursor);

        let provenance = EffectProvenance::from_record(&record, EffectFactOwner::Framework);

        assert_eq!(provenance.group_id.as_ref(), Some(&expected));
        assert_eq!(
            provenance
                .group_id
                .as_ref()
                .map(EffectOutcomeGroupId::as_str),
            Some("effect-outcome:v1:16:flow:with:colons:5:stage:7:2")
        );
    }

    #[test]
    fn effect_outcome_group_id_distinguishes_delimiter_boundaries() {
        let left = EffectCursor {
            recorded_flow_id: "a:b".to_string(),
            stage_key: "c".to_string(),
            input_seq: 1,
            effect_ordinal: 0,
        };
        let right = EffectCursor {
            recorded_flow_id: "a".to_string(),
            stage_key: "b:c".to_string(),
            input_seq: 1,
            effect_ordinal: 0,
        };

        assert_ne!(
            effect_outcome_group_id(&left),
            effect_outcome_group_id(&right)
        );
    }

    #[test]
    fn effect_fact_owner_reads_legacy_framework_owned_bool() {
        let provenance: EffectProvenance = serde_json::from_value(json!({
            "cursor": {
                "recorded_flow_id": "flow",
                "stage_key": "stage",
                "input_seq": 1,
                "effect_ordinal": 0
            },
            "descriptor_hash": "hash",
            "descriptor": {
                "effect_type": "test.effect",
                "label": "test",
                "schema_version": 1,
                "stage_logic_version": "v1",
                "canonical_input_hash": "input"
            },
            "framework_owned": true
        }))
        .expect("legacy framework_owned provenance should deserialize");

        assert_eq!(provenance.fact_owner, EffectFactOwner::Framework);

        let serialized = serde_json::to_value(&provenance).expect("provenance should serialize");
        assert_eq!(serialized["fact_owner"], "framework");
        assert!(serialized.get("framework_owned").is_none());
    }
}
