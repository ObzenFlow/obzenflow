// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI infrastructure integrations.

#[cfg(feature = "ai-rig")]
pub mod rig;

#[cfg(feature = "ai-rig")]
mod rig_builder;

#[cfg(feature = "ai-rig")]
mod model_config;

mod model_profile;
mod token_estimation;

#[cfg(feature = "ai-tiktoken")]
mod tiktoken;

pub use model_profile::resolve_chat_model_profile;
pub use token_estimation::{
    boxed_estimator_for_model, estimator_for_model, resolve_estimator_for_model,
};

#[cfg(feature = "ai-tiktoken")]
pub use tiktoken::TiktokenEstimator;

#[cfg(feature = "ai-rig")]
pub use rig_builder::{
    llm_chat, AiChatTask, ChatRequestTemplate, ChatTransformBuilder, ChatTransformExt,
    EmbeddingTransformBuilder, EmbeddingTransformExt,
};

#[cfg(feature = "ai-rig")]
pub use model_config::ModelConfig;
