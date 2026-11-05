// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI infrastructure integrations.

#[cfg(feature = "ai-rig")]
pub mod rig;

#[cfg(feature = "ai-rig")]
mod endpoint_identity;

mod prompt;

#[cfg(feature = "ai-rig")]
mod model_resolvers;

#[cfg(feature = "ai-rig")]
mod native_embedding_model;

#[cfg(feature = "ai-rig")]
mod provider_errors;

mod model_profile;
mod token_estimation;

#[cfg(feature = "ai-tiktoken")]
mod tiktoken;

pub use model_profile::resolve_chat_model_profile;
pub use prompt::Prompt;
pub use token_estimation::{
    boxed_estimator_for_model, estimator_for_model, resolve_estimator_for_model,
};

#[cfg(feature = "ai-tiktoken")]
pub use tiktoken::TiktokenEstimator;

#[cfg(feature = "ai-rig")]
pub use model_resolvers::{NativeEmbeddingModelResolver, RigChatModelResolver};

#[cfg(feature = "ai-rig")]
pub use native_embedding_model::NativeEmbeddingModel;

/// The model resolvers a host installs in each flow's build context. With the
/// `ai-rig` feature these are rig chat and native embeddings; without it, none.
pub fn builtin_ai_models() -> obzenflow_runtime::ai::AiModelResolvers {
    let resolvers = obzenflow_runtime::ai::AiModelResolvers::default();
    #[cfg(feature = "ai-rig")]
    let resolvers = resolvers
        .chat(RigChatModelResolver)
        .embedding(NativeEmbeddingModelResolver);
    resolvers
}
