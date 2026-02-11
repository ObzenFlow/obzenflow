// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::time::Duration;

/// Provider-agnostic client error taxonomy for AI calls.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum AiClientError {
    #[error("timeout: {message}")]
    Timeout { message: String },

    #[error("remote failure: {message}")]
    Remote { message: String },

    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },

    #[error("authentication failed: {message}")]
    Auth { message: String },

    #[error("invalid request: {message}")]
    InvalidRequest { message: String },

    #[error("unsupported operation: {message}")]
    Unsupported { message: String },

    #[error("other AI client error: {message}")]
    Other { message: String },
}

/// Provider/runtime-agnostic structured-output failure taxonomy.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum StructuredOutputError {
    #[error("invalid JSON: {message}")]
    InvalidJson { message: String },

    #[error("deserialization failed: {message}")]
    Deserialization { message: String },

    #[error("validation failed: {message}")]
    Validation { message: String },
}
