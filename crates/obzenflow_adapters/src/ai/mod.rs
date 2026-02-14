// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI-related adapters.
//!
//! This module contains runtime-facing handler implementations (transforms),
//! and error mapping for AI provider calls.

pub mod error_mapping;
pub mod transforms;

pub use error_mapping::{
    ai_client_error_to_handler_error, ai_client_error_to_handler_error_with_context,
};
pub use transforms::{ChatTransform, EmbeddingTransform};
