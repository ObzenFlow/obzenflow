// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI facade for ObzenFlow
//!
//! Standalone chat and embedding handlers are ordinary typed effectful
//! transforms. Their deterministic builders live in the adapters layer;
//! infrastructure-owned bindings split credential-free request identity from
//! deferred live provider registration. A materialised flow constructs them in
//! this order:
//!
//! ```ignore
//! let mut effect_ports = EffectPortRegistry::new();
//! let chat = ChatEffectBinding::from_config(&runtime_config.ai_models())?
//!     .install_into(&mut effect_ports)?;
//! let chat_handler = ChatTransformBuilder::from_binding(chat)
//!     .logic_version("ticket-summary-v1")
//!     .system("Summarise the ticket concisely.")
//!     .build_typed::<TicketRaised, TicketSummarised>(prompt, map_response)?;
//!
//! let stage = effectful_transform!(
//!     TicketRaised -> TicketSummarised
//!     uses at_least_once(ChatCompletion)
//!         via chat
//!         with ai_resilience()
//!     => chat_handler,
//!     observers: [],
//! );
//! ```
//!
//! The provider is resolved only for a live effect miss. Exact replay returns
//! the recorded reply before consulting that live authority.

pub use obzenflow_adapters::ai::{
    ChatCompletion, ChatCompletionBuildError, ChatTransform, ChatTransformBuilder,
    EmbeddingGeneration, EmbeddingGenerationBuildError, EmbeddingTransform,
    EmbeddingTransformBuilder,
};

pub use obzenflow_core::ai::{
    plan_chat_input_budget, plan_chunks_by_budget, remaining_budget, split_to_budget,
    ChatBindingFingerprint, ChatBudgetError, ChatBudgetMessage, ChatBudgetPlan, ChatBudgetSpec,
    ChatBudgetTemplate, ChatCompletionReply, ChatMessage, ChatModelProfile, ChatParams,
    ChatRequest, ChatRequestSpec, ChatResponse, ChatResponseFormat, ChatRole, ChatTarget,
    ChunkEnvelope, ChunkExclusionReason, ChunkInfo, ChunkPlan, ChunkPlanningConfig,
    ChunkPlanningError, ChunkPlanningStats, ChunkPlanningSummary, ChunkRenderContext,
    ContextWindowSource, EmbeddingBindingFingerprint, EmbeddingDimensions,
    EmbeddingGenerationReply, EmbeddingParams, EmbeddingRequest, EmbeddingRequestSpec,
    EmbeddingResponse, EmbeddingTarget, EstimateSource, HeuristicTokenEstimator,
    OversizeExhaustion, OversizePolicy, ResolvedTokenEstimator, SplitGroup, SystemPrompt,
    TokenCount, TokenEstimate, TokenEstimationError, TokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo, ToolCall, ToolDefinition, Usage, UsageSource, UserPrompt,
};

pub use obzenflow_infra::ai::{
    boxed_estimator_for_model, estimator_for_model, resolve_chat_model_profile,
    resolve_estimator_for_model, Prompt,
};

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::TiktokenEstimator;

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::{
    ChatEffectBinding, ChatEffectBindingError, EmbeddingEffectBinding, EmbeddingEffectBindingError,
    ModelConfig,
};
