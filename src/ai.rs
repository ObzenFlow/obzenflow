// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI facade for ObzenFlow
//!
//! An `ai!` stage runs an AI role, domain logic around one model call. The
//! role prepares each request and interprets each outcome; the handler value
//! chooses the stage's shape, and configuration chooses its model.
//!
//! ```ignore
//! use obzenflow::ai::{self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec};
//!
//! impl AiRole<TicketRaised> for Summarize {
//!     type Effect = ChatCompletion;
//!     type Out = TicketSummarized;
//!
//!     fn prepare(&self, ticket: &TicketRaised) -> Result<ChatRequestSpec, HandlerError> {
//!         Ok(ChatRequestSpec::new("Summarize the ticket concisely.", ticket.body.clone()))
//!     }
//!
//!     fn interpret(
//!         &self,
//!         ticket: TicketRaised,
//!         outcome: AiOutcome<ChatCompletionReply>,
//!     ) -> Result<TicketSummarized, HandlerError> {
//!         let reply = outcome.replied()?;
//!         Ok(TicketSummarized::new(ticket.id, reply.text()))
//!     }
//! }
//!
//! let summarize = ai::infer(Summarize);
//! let breaker = circuit_breaker().consecutive_failures(5);
//! // inside flow!: summarize = ai!(TicketRaised -> TicketSummarized with breaker => summarize);
//! ```
//!
//! `ai::map(role).chunks(n)` calls the role once per group of at most `n`
//! items, and `.reduce(role).on_eof()` or `.every_n_chunks(k)` reduces each
//! window of partials. Each stage's endpoint and model resolve from
//! `[ai.models]` or `[ai.embeddings]`, and strict replay serves recorded
//! outcomes from the models the run recorded.
//!
//! Only `ai!` performs AI effects.

pub use obzenflow_adapters::ai::{
    infer, map, ChatCompletion, ChatCompletionBuildError, Chunked, EmbeddingGeneration,
    EmbeddingGenerationBuildError, Infer, Keyed, Map, MapReduce, TriggeredMapReduce, Unkeyed,
};

pub use obzenflow_runtime::ai::{AiEffect, AiModelProviders, AiReply, AiRequest, AiRole};

pub use obzenflow_core::ai::{
    plan_chat_input_budget, plan_chunks_by_budget, remaining_budget, split_to_budget,
    AiClientError, AiEndpoint, AiKey, AiModelProviderError, AiModelSpec, AiOutcome, AiProvider,
    AiProviderFailureKind, AiUnavailable, AiUnavailableCause, ChatBindingFingerprint,
    ChatBudgetError, ChatBudgetMessage, ChatBudgetPlan, ChatBudgetSpec, ChatBudgetTemplate,
    ChatClient, ChatCompletionOutcome, ChatCompletionReply, ChatMessage, ChatModelDescription,
    ChatModelProfile, ChatModelProvider, ChatParams, ChatRequest, ChatRequestSpec, ChatResponse,
    ChatResponseFormat, ChatRole, ChatTarget, Chunk, ChunkEnvelope, ChunkExclusionReason,
    ChunkPlan, ChunkPlanningConfig, ChunkPlanningError, ChunkPlanningStats, ChunkPlanningSummary,
    ChunkRenderContext, ContextWindowSource, EmbeddingBindingFingerprint, EmbeddingClient,
    EmbeddingDimensions, EmbeddingGenerationOutcome, EmbeddingGenerationReply,
    EmbeddingModelDescription, EmbeddingModelProvider, EmbeddingParams, EmbeddingRequest,
    EmbeddingRequestSpec, EmbeddingResponse, EmbeddingTarget, EstimateSource, GroupOrdinal,
    HeuristicTokenEstimator, OversizeExhaustion, OversizePolicy, Partials, ResolvedTokenEstimator,
    SplitGroup, SystemPrompt, TokenCount, TokenEstimate, TokenEstimationError, TokenEstimator,
    TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo, ToolCall, ToolDefinition, Usage,
    UsageSource, UserPrompt, WindowOrdinal,
};

pub use obzenflow_infra::ai::{
    boxed_estimator_for_model, builtin_ai_models, estimator_for_model, resolve_chat_model_profile,
    resolve_estimator_for_model, Prompt,
};

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::TiktokenEstimator;

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::{NativeEmbeddingModelProvider, RigChatModelProvider};
