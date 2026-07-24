// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-internal transport payloads for AI map-reduce composites.

use super::{
    CanonicalizationComponent, ChatCompletionCompleted, ChatRequest, ChunkExclusionReason,
    ChunkInfo, ChunkPlanningSummary, TokenCount,
};
use crate::{EventId, TypedPayload};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize)]
pub struct Many<T> {
    pub items: Vec<T>,
    pub planning: ChunkPlanningSummary,
}

impl<T> Default for Many<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        }
    }
}

impl<T> std::fmt::Debug for Many<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Many")
            .field("items_len", &self.items.len())
            .field("planning", &self.planning)
            .finish()
    }
}

impl<T> TypedPayload for Many<T>
where
    T: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.many";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReducePlanningManifest {
    pub job_key: EventId,
    pub chunk_count: usize,
    pub planning: ChunkPlanningSummary,
    /// Raw JSON payload of the outer seed event.
    ///
    /// This is used by the composite to provide the reduce handler with the
    /// original input without forcing users to reconstruct it from partials.
    pub seed_payload: Value,
    /// The seed's event type string, for debugging and observability.
    pub seed_event_type: String,
}

impl TypedPayload for AiMapReducePlanningManifest {
    const EVENT_TYPE: &'static str = "ai.map_reduce.planning_manifest";
    const SCHEMA_VERSION: u32 = 1;
}

/// Internal transport payload delivered to the finalise (reduce) stage.
///
/// The key ergonomic constraint is that the user-facing reduce contract is
/// `(Seed, Collected) -> Out`. The composite therefore pairs the original seed
/// with the collected partials before calling the user handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiMapReduceReduceInput<Seed, Collected> {
    pub job_key: EventId,
    pub seed: Seed,
    pub collected: Collected,
    pub planning: ChunkPlanningSummary,
}

impl<Seed, Collected> TypedPayload for AiMapReduceReduceInput<Seed, Collected>
where
    Seed: Serialize + DeserializeOwned,
    Collected: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.reduce_input";
    const SCHEMA_VERSION: u32 = 2;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiMapReduceTaggedPartial<T> {
    pub job_key: EventId,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub partial: T,
}

impl<T> TypedPayload for AiMapReduceTaggedPartial<T>
where
    T: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.tagged_partial";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceChunkFailed {
    pub job_key: EventId,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub cause: AiMapReduceRoleFailure,
}

impl TypedPayload for AiMapReduceChunkFailed {
    const EVENT_TYPE: &'static str = "ai.map_reduce.chunk_failed";
    const SCHEMA_VERSION: u32 = 2;
}

/// Credential-free role logic failures. Framework and provider failures are
/// added by the sealed generated adapters, not by user roles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AiRoleLogicFailure {
    Prompt { message: String },
    ResponseDecode { message: String },
    Parse { message: String },
    EmptyOutput,
}

/// Closed provider-facing failure taxonomy used by generated domain terminals.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AiProviderFailureKind {
    Timeout,
    Remote,
    RateLimited,
    Authentication,
    InvalidRequest,
    Unsupported,
    Other,
}

/// Closed generated-role failure contract. Its tag intentionally differs from
/// the nested logic tag so the wire shape is derivable and unambiguous.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "failure_type", rename_all = "snake_case")]
pub enum AiMapReduceRoleFailure {
    Logic {
        logic: AiRoleLogicFailure,
    },
    RequestCanonicalization {
        component: CanonicalizationComponent,
        message: String,
    },
    BoundaryRejected {
        source: String,
        code: String,
        message: String,
    },
    RecoveryAbandoned {
        last_started_attempt: u32,
        source: String,
        code: String,
        message: String,
    },
    Provider {
        provider_kind: AiProviderFailureKind,
        message: String,
    },
}

/// User-authored map role. The generated adapter owns effect execution,
/// target validation, labels, and durable protocol tagging.
pub trait AiMapRole<Item, Partial>: Send + Sync + 'static {
    type Prepared: Send + 'static;

    fn prepare(
        &self,
        items: &[Item],
        chunk: &ChunkInfo,
    ) -> Result<(ChatRequest, Self::Prepared), AiRoleLogicFailure>;

    fn interpret(
        &self,
        items: Vec<Item>,
        prepared: Self::Prepared,
        completion: ChatCompletionCompleted,
    ) -> Result<Partial, AiRoleLogicFailure>;
}

/// User-authored finalisation role. The generated adapter owns the single chat
/// effect and only exposes the domain seed and collected value.
pub trait AiFinaliseRole<Seed, Collected, Out>: Send + Sync + 'static {
    type Prepared: Send + 'static;

    fn prepare(
        &self,
        seed: &Seed,
        collected: &Collected,
    ) -> Result<(ChatRequest, Self::Prepared), AiRoleLogicFailure>;

    fn interpret(
        &self,
        seed: Seed,
        collected: Collected,
        prepared: Self::Prepared,
        completion: ChatCompletionCompleted,
    ) -> Result<Out, AiRoleLogicFailure>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AiMapReducePlanningFailure {
    OversizeItem {
        item_ordinal: usize,
        estimated_tokens: TokenCount,
        budget: TokenCount,
    },
    OversizeExhausted {
        item_ordinal: usize,
        reason: ChunkExclusionReason,
        last_estimated_tokens: TokenCount,
        budget: TokenCount,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReducePlanningFailed {
    pub job_key: EventId,
    pub cause: AiMapReducePlanningFailure,
}

impl TypedPayload for AiMapReducePlanningFailed {
    const EVENT_TYPE: &'static str = "ai.map_reduce.planning_failed";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceFinaliseFailed {
    pub job_key: EventId,
    pub cause: AiMapReduceRoleFailure,
}

impl TypedPayload for AiMapReduceFinaliseFailed {
    const EVENT_TYPE: &'static str = "ai.map_reduce.finalise_failed";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceJobFailed {
    pub job_key: EventId,
    pub chunk_count: usize,
    pub failed_indices: Vec<usize>,
}

impl TypedPayload for AiMapReduceJobFailed {
    const EVENT_TYPE: &'static str = "ai.map_reduce.job_failed";
    const SCHEMA_VERSION: u32 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn role_failure_wire_shape_has_one_outer_tag_and_a_nested_logic_tag() {
        let cases = [
            (
                AiMapReduceRoleFailure::Logic {
                    logic: AiRoleLogicFailure::Prompt {
                        message: "bad prompt".to_string(),
                    },
                },
                json!({
                    "failure_type": "logic",
                    "logic": {
                        "kind": "prompt",
                        "message": "bad prompt"
                    }
                }),
            ),
            (
                AiMapReduceRoleFailure::RequestCanonicalization {
                    component: CanonicalizationComponent::ResponseSchema,
                    message: "bad schema".to_string(),
                },
                json!({
                    "failure_type": "request_canonicalization",
                    "component": "response_schema",
                    "message": "bad schema"
                }),
            ),
            (
                AiMapReduceRoleFailure::BoundaryRejected {
                    source: "circuit_breaker".to_string(),
                    code: "open".to_string(),
                    message: "breaker open".to_string(),
                },
                json!({
                    "failure_type": "boundary_rejected",
                    "source": "circuit_breaker",
                    "code": "open",
                    "message": "breaker open"
                }),
            ),
            (
                AiMapReduceRoleFailure::RecoveryAbandoned {
                    last_started_attempt: 2,
                    source: "circuit_breaker".to_string(),
                    code: "open".to_string(),
                    message: "still open".to_string(),
                },
                json!({
                    "failure_type": "recovery_abandoned",
                    "last_started_attempt": 2,
                    "source": "circuit_breaker",
                    "code": "open",
                    "message": "still open"
                }),
            ),
            (
                AiMapReduceRoleFailure::Provider {
                    provider_kind: AiProviderFailureKind::InvalidRequest,
                    message: "invalid".to_string(),
                },
                json!({
                    "failure_type": "provider",
                    "provider_kind": "invalid_request",
                    "message": "invalid"
                }),
            ),
        ];

        for (failure, expected) in cases {
            let value = serde_json::to_value(&failure).expect("role failure serialises");
            assert_eq!(value, expected);
            assert_eq!(
                serde_json::from_value::<AiMapReduceRoleFailure>(value)
                    .expect("role failure deserialises"),
                failure
            );
        }
    }
}
