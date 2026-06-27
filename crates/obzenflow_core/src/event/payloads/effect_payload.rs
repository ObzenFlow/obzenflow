// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect-result payloads for replay-safe user effects.

use std::fmt;

use crate::event::types::EventType;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const EFFECT_RECORD_EVENT_TYPE: &str = "obzenflow.effect_record.v1";
pub const CAPTURE_EVENT_TYPE: &str = "obzenflow.capture.v1";

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.as_str()
            }
        }
    };
}

macro_rules! u32_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u32);

        impl $name {
            pub const fn new(value: u32) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u32 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<u32> for $name {
            fn from(value: u32) -> Self {
                Self::new(value)
            }
        }

        impl PartialEq<u32> for $name {
            fn eq(&self, other: &u32) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for u32 {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }
    };
}

string_newtype!(
    RecordedFlowId,
    "Flow id whose journaled effect records are being replayed."
);
string_newtype!(
    EffectStageKey,
    "Stage key component of an effect replay cursor."
);
string_newtype!(EffectType, "Stable effect declaration name.");
string_newtype!(
    EffectLabel,
    "Per-call effect label included in descriptor drift checks."
);
string_newtype!(
    StageLogicVersion,
    "Stage logic version included in effect descriptor drift checks."
);
string_newtype!(
    CanonicalInputHash,
    "Stable hash of an effect's canonical input material."
);
string_newtype!(
    EffectFailureKind,
    "Stable classification for a recorded effect failure."
);
string_newtype!(
    EffectFailureSource,
    "Stable label for the component that caused a recorded effect failure."
);
string_newtype!(
    EffectFailureCode,
    "Stable machine-readable reason for a recorded effect failure."
);

/// Stage input position captured in an effect replay cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EffectInputPosition(u64);

impl EffectInputPosition {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EffectInputPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for EffectInputPosition {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl PartialEq<u64> for EffectInputPosition {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<EffectInputPosition> for u64 {
    fn eq(&self, other: &EffectInputPosition) -> bool {
        *self == other.0
    }
}

u32_newtype!(
    EffectOrdinal,
    "Per-input position of an fx.perform or fx.capture call."
);
u32_newtype!(
    EffectSchemaVersion,
    "Schema version of an effect declaration."
);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectCursor {
    pub recorded_flow_id: RecordedFlowId,
    pub stage_key: EffectStageKey,
    pub input_seq: EffectInputPosition,
    pub effect_ordinal: EffectOrdinal,
}

impl EffectCursor {
    pub fn new(
        recorded_flow_id: impl Into<RecordedFlowId>,
        stage_key: impl Into<EffectStageKey>,
        input_seq: impl Into<EffectInputPosition>,
        effect_ordinal: impl Into<EffectOrdinal>,
    ) -> Self {
        Self {
            recorded_flow_id: recorded_flow_id.into(),
            stage_key: stage_key.into(),
            input_seq: input_seq.into(),
            effect_ordinal: effect_ordinal.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectDescriptor {
    pub effect_type: EffectType,
    pub label: EffectLabel,
    pub schema_version: EffectSchemaVersion,
    pub stage_logic_version: StageLogicVersion,
    pub canonical_input_hash: CanonicalInputHash,
}

impl EffectDescriptor {
    pub fn new(
        effect_type: impl Into<EffectType>,
        label: impl Into<EffectLabel>,
        schema_version: impl Into<EffectSchemaVersion>,
        stage_logic_version: impl Into<StageLogicVersion>,
        canonical_input_hash: impl Into<CanonicalInputHash>,
    ) -> Self {
        Self {
            effect_type: effect_type.into(),
            label: label.into(),
            schema_version: schema_version.into(),
            stage_logic_version: stage_logic_version.into(),
            canonical_input_hash: canonical_input_hash.into(),
        }
    }
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

/// Total number of facts in a multi-fact effect outcome group (FLOWIP-120q).
///
/// Stamped on every fact of the group so a reader can prove the group is
/// complete. Sizing the completeness check to this recorded count, rather than
/// to the number of records present, is what lets a group missing its
/// highest-ordinal fact be detected instead of passing as a smaller group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutcomeFactCount(u32);

impl OutcomeFactCount {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for OutcomeFactCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u32> for OutcomeFactCount {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryDisposition {
    Retryable,
    #[default]
    NotRetryable,
}

impl<'de> Deserialize<'de> for RetryDisposition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RetryDispositionVisitor;

        impl Visitor<'_> for RetryDispositionVisitor {
            type Value = RetryDisposition;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`retryable`, `not_retryable`, or legacy retryable boolean")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RetryDisposition::from_bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "retryable" => Ok(RetryDisposition::Retryable),
                    "not_retryable" => Ok(RetryDisposition::NotRetryable),
                    other => Err(E::unknown_variant(other, &["retryable", "not_retryable"])),
                }
            }
        }

        deserializer.deserialize_any(RetryDispositionVisitor)
    }
}

impl RetryDisposition {
    pub const fn from_bool(value: bool) -> Self {
        if value {
            Self::Retryable
        } else {
            Self::NotRetryable
        }
    }

    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }
}

/// Structured, policy-neutral cause attached to a recorded effect failure when
/// execution was rejected by a component other than the effect port itself,
/// for example a middleware guarding the effect boundary. `source` is the
/// rejecting component's label and `code` is a stable machine-readable reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectFailureCause {
    pub source: EffectFailureSource,
    pub code: EffectFailureCode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EffectOutcomePayload {
    Succeeded {
        output: Value,
    },
    SucceededFact {
        event_type: EventType,
        output: Value,
        outcome_fact_ordinal: OutcomeFactOrdinal,
        /// Cardinality of the outcome group this fact belongs to (FLOWIP-120q).
        /// Every fact of one effect outcome carries the same count.
        outcome_fact_count: OutcomeFactCount,
    },
    Failed {
        error_type: EffectFailureKind,
        error_message: String,
        #[serde(default, alias = "retryable")]
        retry: RetryDisposition,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<EffectFailureCause>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRecord {
    pub cursor: EffectCursor,
    pub descriptor_hash: EffectDescriptorHash,
    pub descriptor: EffectDescriptor,
    pub outcome: EffectOutcomePayload,
    /// Origin recorded on the fact's provenance (FLOWIP-120m). `None` on
    /// framework rows and pre-120h journals. Read back on replay so origin
    /// reconstruction never depends on registration fact-type membership,
    /// which becomes ambiguous once an outcome-shaped fallback synthesizes
    /// the effect's own facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<EffectFactOrigin>,
}

/// Replay identity for an effect-produced fact.
///
/// Vector clocks and event causality explain what happened-before what.
/// `EffectProvenance` explains which effect call this fact satisfies during
/// replay. It keeps the effect cursor, descriptor, and multi-fact outcome group
/// outside the domain payload while still letting `fx.perform` find and validate
/// the recorded fact.
/// How an effect outcome fact came to exist (FLOWIP-120h). Policy-neutral:
/// it says a middleware synthesized the group instead of the effect port
/// running, with a label naming which one, never a policy-specific type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EffectFactOrigin {
    /// The effect port executed and produced this outcome.
    Effect,
    /// A type-shaping middleware synthesized this outcome at the effect
    /// boundary (e.g. a circuit-breaker fallback or rejection branch).
    MiddlewareSynthesized { label: String },
}

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
    /// Cardinality of the effect outcome group (FLOWIP-120q). Set on the same
    /// user-authored outcome facts that set `outcome_fact_ordinal`; reserved
    /// framework rows leave it `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome_fact_count: Option<OutcomeFactCount>,
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
    /// Branch origin marker (FLOWIP-120h): set when a middleware synthesized
    /// the outcome group. Excluded from descriptor identity by construction;
    /// `None` on pre-120h events and ordinary effect outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<EffectFactOrigin>,
}

impl EffectProvenance {
    pub fn from_record(record: &EffectRecord, fact_owner: EffectFactOwner) -> Self {
        Self {
            cursor: record.cursor.clone(),
            descriptor_hash: record.descriptor_hash.clone(),
            descriptor: record.descriptor.clone(),
            outcome_fact_ordinal: None,
            outcome_fact_count: None,
            group_id: Some(effect_outcome_group_id(&record.cursor)),
            fact_owner,
            origin: record.origin.clone(),
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
        cursor.input_seq.get(),
        cursor.effect_ordinal.get()
    ))
}

pub fn framework_effect_event_type(effect_type: impl AsRef<str>) -> &'static str {
    if effect_type.as_ref() == "obzenflow.capture" {
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
            descriptor: EffectDescriptor::new("test.effect", "test", 1, "v1", "input"),
            outcome: EffectOutcomePayload::Succeeded {
                output: json!({"ok": true}),
            },
            origin: None,
        }
    }

    #[test]
    fn effect_outcome_group_id_is_derived_from_cursor() {
        let cursor = EffectCursor::new("flow:with:colons", "stage", 7, 2);
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
        let left = EffectCursor::new("a:b", "c", 1, 0);
        let right = EffectCursor::new("a", "b:c", 1, 0);

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

    #[test]
    fn effect_failure_reads_legacy_retryable_bool() {
        let outcome: EffectOutcomePayload = serde_json::from_value(json!({
            "outcome": "failed",
            "error_type": "remote",
            "error_message": "try again",
            "retryable": true
        }))
        .expect("legacy retryable failure should deserialize");

        assert_eq!(
            outcome,
            EffectOutcomePayload::Failed {
                error_type: "remote".into(),
                error_message: "try again".to_string(),
                retry: RetryDisposition::Retryable,
                cause: None,
            }
        );

        let serialized = serde_json::to_value(outcome).expect("failure should serialize");
        assert_eq!(serialized["retry"], "retryable");
        assert!(serialized.get("retryable").is_none());
    }

    #[test]
    fn effect_failure_cause_uses_typed_fields_with_string_wire_shape() {
        let cause: EffectFailureCause = serde_json::from_value(json!({
            "source": "circuit_breaker",
            "code": "rejected_circuit_open"
        }))
        .expect("string wire cause should deserialize into typed fields");

        assert_eq!(cause.source, "circuit_breaker");
        assert_eq!(cause.code, "rejected_circuit_open");

        let serialized = serde_json::to_value(&cause).expect("cause should serialize");
        assert_eq!(
            serialized,
            json!({
                "source": "circuit_breaker",
                "code": "rejected_circuit_open"
            })
        );
    }

    #[test]
    fn effect_provenance_origin_defaults_on_pre_120h_events() {
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
            }
        }))
        .expect("pre-120h provenance should deserialize with origin defaulted");

        assert_eq!(provenance.origin, None);

        let synthesized = EffectFactOrigin::MiddlewareSynthesized {
            label: "circuit_breaker".to_string(),
        };
        let round_tripped: EffectFactOrigin = serde_json::from_value(
            serde_json::to_value(&synthesized).expect("origin should serialize"),
        )
        .expect("origin should round-trip");
        assert_eq!(round_tripped, synthesized);
    }
}
