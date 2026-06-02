// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect-result payloads for replay-safe user effects.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
