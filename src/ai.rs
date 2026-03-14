// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI facade for ObzenFlow
//!
//! This module re-exports the provider-agnostic AI transforms from the adapters
//! layer, generic AI traits from the core layer, and infrastructure-backed
//! provider integrations from `obzenflow_infra`. This crate should remain a
//! facade: the actual Rig-backed builder implementation lives in the infra
//! layer and is re-exported here.

pub use obzenflow_adapters::ai::{ChatTransform, EmbeddingTransform};

pub use obzenflow_core::ai::{
    plan_chat_input_budget, plan_chunks_by_budget, remaining_budget, split_to_budget,
    ChatBudgetError, ChatBudgetMessage, ChatBudgetPlan, ChatBudgetSpec, ChatBudgetTemplate,
    ChatModelProfile, ChunkEnvelope, ChunkExclusionReason, ChunkPlan, ChunkPlanningConfig,
    ChunkPlanningError, ChunkPlanningStats, ChunkPlanningSummary, ChunkRenderContext,
    ContextWindowSource, EstimateSource, HeuristicTokenEstimator, OversizeExhaustion,
    OversizePolicy, ResolvedTokenEstimator, SplitGroup, TokenCount, TokenEstimate,
    TokenEstimationError, TokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo,
};

pub use obzenflow_infra::ai::{
    boxed_estimator_for_model, estimator_for_model, resolve_chat_model_profile,
    resolve_estimator_for_model,
};

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::TiktokenEstimator;

#[cfg(feature = "ai")]
pub use obzenflow_infra::ai::{
    ChatRequestTemplate, ChatTransformBuilder, ChatTransformBuilderWithContext, ChatTransformExt,
    EmbeddingTransformBuilder, EmbeddingTransformExt, ModelChatBuilder,
    ModelChatBuilderWithContext, ModelConfig,
};
