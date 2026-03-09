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
    remaining_budget, split_to_budget, EstimateSource, HeuristicTokenEstimator, SplitGroup,
    TokenCount, TokenEstimate, TokenEstimationError, TokenEstimator,
};

pub use obzenflow_infra::ai::{boxed_estimator_for_model, estimator_for_model};

#[cfg(feature = "ai-tiktoken")]
pub use obzenflow_infra::ai::TiktokenEstimator;

#[cfg(feature = "ai-rig")]
pub use obzenflow_infra::ai::{
    ChatRequestTemplate, ChatTransformBuilder, ChatTransformExt, EmbeddingTransformBuilder,
    EmbeddingTransformExt,
};
