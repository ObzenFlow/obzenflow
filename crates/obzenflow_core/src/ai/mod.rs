// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Provider-agnostic AI contracts and utilities.
//!
//! This module defines the inner-layer contract surface for LLM integrations:
//! request/response DTOs, client ports, structured output helpers, and stable
//! observability/hash conventions.

mod canonical;
mod chat_budget;
mod chunking;
mod error;
mod hashing;
mod map_reduce;
mod model_profile;
mod observability;
mod ports;
mod structured_output;
mod token_estimation;
mod types;

pub use canonical::{canonical_json_bytes_v1, AI_MAP_REDUCE_COLLECTOR_FACT_FORMAT_V1};
pub use chat_budget::{
    plan_chat_input_budget, ChatBudgetError, ChatBudgetMessage, ChatBudgetPlan, ChatBudgetSpec,
    ChatBudgetTemplate,
};
pub use chunking::{
    plan_chunks_by_budget, ChunkEnvelope, ChunkExclusionReason, ChunkInfo, ChunkPlan,
    ChunkPlanningConfig, ChunkPlanningError, ChunkPlanningStats, ChunkPlanningSummary,
    ChunkRenderContext, OversizeExhaustion, OversizePolicy,
};
pub use error::{AiClientError, StructuredOutputError};
pub use hashing::{
    params_hash_for_chat, params_hash_for_embedding, prompt_hash_for_chat,
    prompt_hash_for_embedding_inputs, schema_hash_for_response_format, schema_hash_from_json,
    schema_hash_from_text, AiHashError, LLM_HASH_VERSION_SHA256_V1,
};
pub use map_reduce::{
    AiFinaliseRole, AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceJobFailed,
    AiMapReducePlanningFailed, AiMapReducePlanningFailure, AiMapReducePlanningManifest,
    AiMapReduceReduceInput, AiMapReduceRoleFailure, AiMapReduceTaggedPartial, AiMapRole,
    AiProviderFailureKind, AiRoleLogicFailure, Many,
};
pub use model_profile::{ChatModelProfile, ContextWindowSource};
pub use observability::{
    attach_llm_observability, read_llm_observability, LlmCacheInfo, LlmCacheMode, LlmHashes,
    LlmObservability, LlmObservabilityError, LLM_METADATA_KEY,
};
pub use ports::{ChatClient, EmbeddingClient};
pub use structured_output::{StructuredOutputSchema, StructuredOutputSpec, ValidationHook};
pub use token_estimation::{
    remaining_budget, split_to_budget, EstimateSource, HeuristicTokenEstimator,
    ResolvedTokenEstimator, SplitGroup, TokenCount, TokenEstimate, TokenEstimationError,
    TokenEstimator, TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo,
};
pub use types::{
    AiProvider, CanonicalizationComponent, ChatCompletionCompleted, ChatMessage, ChatParams,
    ChatRequest, ChatResponse, ChatResponseFormat, ChatRole, ChatTarget, EmbeddingParams,
    EmbeddingRequest, EmbeddingResponse, SystemPrompt, ToolCall, ToolDefinition, Usage,
    UsageSource, UserPrompt,
};
