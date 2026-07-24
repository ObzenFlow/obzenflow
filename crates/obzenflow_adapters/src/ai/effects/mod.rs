// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod chat_completion;
mod map_reduce_generated;

pub use chat_completion::{ChatCompletion, ChatCompletionBuildError};
#[doc(hidden)]
pub use map_reduce_generated::{
    GeneratedAiFinaliseHandler, GeneratedAiMapHandler, FINALISE_CHAT_COMPLETION_LABEL,
    MAP_CHAT_COMPLETION_LABEL,
};
