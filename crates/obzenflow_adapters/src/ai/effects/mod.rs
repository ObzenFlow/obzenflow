// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod chat_completion;
mod embedding_generation;

pub use chat_completion::{
    ChatBindingEvidence, ChatBindingEvidenceBuildError, ChatCompletion, ChatCompletionBuildError,
    CHAT_CLIENT,
};
pub use embedding_generation::{
    EmbeddingBindingEvidence, EmbeddingBindingEvidenceBuildError, EmbeddingGeneration,
    EmbeddingGenerationBuildError, EMBEDDING_CLIENT,
};
