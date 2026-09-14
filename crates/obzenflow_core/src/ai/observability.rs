// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{
    AiProvider, TokenEstimate, TokenEstimatorResolutionInfo, Usage, LLM_HASH_VERSION_SHA256_V1,
};
use crate::event::chain_event::ChainEvent;
use crate::event::observation::{CaptureStamp, ObservabilityContext, ObservationRecord};
use serde::{Deserialize, Serialize};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_input_tokens: Option<TokenEstimate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_input_resolution: Option<TokenEstimatorResolutionInfo>,
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
            estimated_input_tokens: None,
            estimated_input_resolution: None,
            cache: None,
        }
    }
}

/// Attach optional, typed metadata with a capture identity allocated by the owner.
pub fn attach_llm_observability(
    event: &mut ChainEvent,
    capture: CaptureStamp,
    llm: LlmObservability,
) {
    let mut observation = ObservabilityContext::new(capture);
    observation
        .records
        .push(ObservationRecord::Llm { metadata: llm });
    event.envelope.observability = Some(observation);
}

pub fn read_llm_observability(event: &ChainEvent) -> Option<&LlmObservability> {
    event
        .envelope
        .observability
        .as_ref()?
        .records
        .iter()
        .find_map(|record| match record {
            ObservationRecord::Llm { metadata } => Some(metadata),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{
        LlmHashes, TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo, UsageSource,
    };
    use crate::event::ChainEventFactory;
    use crate::id::StageId;
    use crate::{FlowId, WriterId};
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
        llm.estimated_input_resolution = Some(TokenEstimatorResolutionInfo::heuristic(
            "llama3.1:8b",
            TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer,
            Some("no tiktoken encoding".to_string()),
        ));

        attach_llm_observability(&mut event, capture(writer_id), llm.clone());
        let decoded = read_llm_observability(&event).expect("llm payload should exist");
        assert_eq!(decoded, &llm);
    }

    fn capture(observer: WriterId) -> CaptureStamp {
        use crate::event::observation::{CaptureReason, CaptureScope, CaptureSeq};
        CaptureStamp {
            capture_scope: CaptureScope {
                flow_id: FlowId::new(),
                resume_generation: Default::default(),
            },
            observer,
            capture_seq: CaptureSeq(1),
            capture_reason: CaptureReason::Record,
            observed_at_ms: 42,
        }
    }

    #[test]
    fn optional_metadata_has_a_closed_wire_shape() {
        let packet = ObservabilityContext::new(capture(WriterId::from(StageId::new())));
        let mut json = serde_json::to_value(packet).unwrap();
        json["custom"] = json!({"llm": {}});
        assert!(serde_json::from_value::<ObservabilityContext>(json).is_err());
    }
}
