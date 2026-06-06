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
            group_id: None,
            framework_owned,
        }
    }
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
