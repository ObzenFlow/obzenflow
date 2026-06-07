// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect-result payloads for replay-safe user effects.

use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EffectOutcomePayload {
    Succeeded {
        output: Value,
    },
    SucceededFact {
        event_type: String,
        output: Value,
        outcome_fact_ordinal: u32,
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
    pub descriptor_hash: String,
    pub descriptor: EffectDescriptor,
    pub outcome: EffectOutcomePayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectProvenance {
    pub cursor: EffectCursor,
    pub descriptor_hash: String,
    pub descriptor: EffectDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome_fact_ordinal: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub framework_owned: bool,
}

impl EffectProvenance {
    pub fn from_record(record: &EffectRecord, framework_owned: bool) -> Self {
        Self {
            cursor: record.cursor.clone(),
            descriptor_hash: record.descriptor_hash.clone(),
            descriptor: record.descriptor.clone(),
            outcome_fact_ordinal: None,
            group_id: Some(effect_outcome_group_id(&record.cursor)),
            framework_owned,
        }
    }
}

pub fn effect_outcome_group_id(cursor: &EffectCursor) -> String {
    format!(
        "effect-outcome:v1:{}:{}:{}:{}:{}:{}",
        cursor.recorded_flow_id.len(),
        cursor.recorded_flow_id.as_str(),
        cursor.stage_key.len(),
        cursor.stage_key.as_str(),
        cursor.input_seq,
        cursor.effect_ordinal
    )
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

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn effect_record(cursor: EffectCursor) -> EffectRecord {
        EffectRecord {
            cursor,
            descriptor_hash: "hash".to_string(),
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

        let provenance = EffectProvenance::from_record(&record, true);

        assert_eq!(provenance.group_id.as_deref(), Some(expected.as_str()));
        assert_eq!(
            provenance.group_id.as_deref(),
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
}
