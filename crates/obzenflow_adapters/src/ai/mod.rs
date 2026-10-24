// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI effects and the handler values `ai!` stages are built from.

pub mod effects;
mod error_mapping;
mod shapes;

pub use effects::{
    ChatBindingEvidence, ChatBindingEvidenceBuildError, ChatCompletion, ChatCompletionBuildError,
    EmbeddingBindingEvidence, EmbeddingBindingEvidenceBuildError, EmbeddingGeneration,
    EmbeddingGenerationBuildError, CHAT_CLIENT, EMBEDDING_CLIENT,
};
pub use shapes::{
    infer, map, Chunked, Infer, Keyed, Map, MapReduce, TriggeredMapReduce, Unkeyed,
};
#[doc(hidden)]
pub use shapes::{AiKeySpec, AiTrigger, InferHasNoKey, KeyByBeforeChunks};
