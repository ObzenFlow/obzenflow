// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI-related adapters.
//!
//! This module contains runtime-facing handler implementations (transforms),
//! and error mapping for AI provider calls.

mod builders;
pub mod effects;
pub mod error_mapping;
pub mod transforms;

pub use builders::{ChatTransformBuilder, EmbeddingTransformBuilder};
pub use effects::{
    ChatCompletion, ChatCompletionBuildError, EmbeddingGeneration, EmbeddingGenerationBuildError,
};
pub use error_mapping::effect_error_to_handler_error;
pub use transforms::{ChatTransform, EmbeddingTransform};
