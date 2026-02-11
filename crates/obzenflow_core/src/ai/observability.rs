// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{AiProvider, Usage, LLM_HASH_VERSION_SHA256_V1};
use crate::event::chain_event::ChainEvent;
use crate::event::context::observability_context::ObservabilityContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const LLM_METADATA_KEY: &str = "llm";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlmHashes {
    pub version: String,
    pub prompt_hash: String,
    pub params_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
}

impl LlmHashes {
    pub fn new(prompt_hash: String, params_hash: String) -> Self {
        Self {
            version: LLM_HASH_VERSION_SHA256_V1.to_string(),
            prompt_hash,
            params_hash,
            schema_hash: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlmCacheMode {
    Off,
    Record,
    Replay,
    ReplayOrRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LlmCacheInfo {
    pub mode: LlmCacheMode,
    pub hit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmObservability {
    pub schema_version: u32,
    pub provider: AiProvider,
    pub model: String,
    pub hashes: LlmHashes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<LlmCacheInfo>,
}

impl LlmObservability {
    pub const SCHEMA_VERSION_V1: u32 = 1;

    pub fn new(provider: AiProvider, model: impl Into<String>, hashes: LlmHashes) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION_V1,
            provider,
            model: model.into(),
            hashes,
            usage: None,
            cache: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LlmObservabilityError {
    #[error("observability.custom must be a JSON object to attach llm metadata")]
    CustomNotObject,

    #[error("failed to serialize llm metadata: {0}")]
    Serialization(String),

    #[error("failed to deserialize llm metadata: {0}")]
    Deserialization(String),
}

/// Attach LLM metadata under `ChainEvent.observability.custom["llm"]`.
///
/// This ensures `observability.custom` is always a JSON object when LLM metadata is present.
pub fn attach_llm_observability(
    event: &mut ChainEvent,
    llm: LlmObservability,
) -> Result<(), LlmObservabilityError> {
    let llm_value = serde_json::to_value(llm)
        .map_err(|err| LlmObservabilityError::Serialization(err.to_string()))?;

    let observability = event
        .observability
        .get_or_insert_with(ObservabilityContext::default);

    match observability.custom.as_mut() {
        Some(Value::Object(custom)) => {
            custom.insert(LLM_METADATA_KEY.to_string(), llm_value);
            Ok(())
        }
        Some(_) => Err(LlmObservabilityError::CustomNotObject),
        None => {
            let mut custom = serde_json::Map::new();
            custom.insert(LLM_METADATA_KEY.to_string(), llm_value);
            observability.custom = Some(Value::Object(custom));
            Ok(())
        }
    }
}

/// Read typed LLM metadata from `ChainEvent.observability.custom["llm"]`.
pub fn read_llm_observability(
    event: &ChainEvent,
) -> Result<Option<LlmObservability>, LlmObservabilityError> {
    let Some(observability) = &event.observability else {
        return Ok(None);
    };
    let Some(custom) = &observability.custom else {
        return Ok(None);
    };

    let Value::Object(map) = custom else {
        return Err(LlmObservabilityError::CustomNotObject);
    };

    let Some(value) = map.get(LLM_METADATA_KEY) else {
        return Ok(None);
    };

    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|err| LlmObservabilityError::Deserialization(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{LlmHashes, UsageSource};
    use crate::event::ChainEventFactory;
    use crate::id::StageId;
    use crate::WriterId;
    use serde_json::json;

    #[test]
    fn attach_and_read_llm_observability_round_trips() {
        let writer_id = WriterId::from(StageId::new());
        let mut event =
            ChainEventFactory::data_event(writer_id, "ticket.created", json!({"id": 1}));

        let mut llm = LlmObservability::new(
            AiProvider::new("ollama"),
            "llama3.1:8b",
            LlmHashes::new("a".repeat(64), "b".repeat(64)),
        );
        llm.usage = Some(Usage {
            source: UsageSource::Provider,
            input_tokens: 10,
            output_tokens: 20,
            total_tokens: 30,
        });

        attach_llm_observability(&mut event, llm.clone()).expect("attach should succeed");
        let decoded = read_llm_observability(&event)
            .expect("read should succeed")
            .expect("llm payload should exist");
        assert_eq!(decoded, llm);
    }

    #[test]
    fn attach_fails_when_custom_is_not_object() {
        let writer_id = WriterId::from(StageId::new());
        let mut event =
            ChainEventFactory::data_event(writer_id, "ticket.created", json!({"id": 1}));
        event.observability = Some(ObservabilityContext {
            custom: Some(Value::Array(vec![])),
            ..ObservabilityContext::default()
        });

        let llm = LlmObservability::new(
            AiProvider::new("openai"),
            "gpt-4.1-mini",
            LlmHashes::new("a".repeat(64), "b".repeat(64)),
        );

        let err = attach_llm_observability(&mut event, llm).expect_err("custom must be object");
        assert!(matches!(err, LlmObservabilityError::CustomNotObject));
    }
}
