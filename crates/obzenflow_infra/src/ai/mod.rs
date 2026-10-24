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
mod model_providers;

#[cfg(feature = "ai-rig")]
mod native_embedding_client;

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
pub use model_providers::{NativeEmbeddingModelProvider, RigChatModelProvider};

#[cfg(feature = "ai-rig")]
pub use native_embedding_client::NativeEmbeddingClient;

/// The model providers a host installs in each flow's build context: rig
/// chat and native embeddings with the `ai-rig` feature, none without it.
pub fn builtin_ai_models() -> obzenflow_runtime::ai::AiModelProviders {
    let providers = obzenflow_runtime::ai::AiModelProviders::default();
    #[cfg(feature = "ai-rig")]
    let providers = providers
        .chat(RigChatModelProvider)
        .embedding(NativeEmbeddingModelProvider);
    providers
}
