// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::time::Duration;

/// The provider's answer to one AI call that produced no reply.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum AiProviderError {
    #[error("timeout: {message}")]
    Timeout { message: String },

    /// The provider refused the connection, so it did not act on the call.
    #[error("connection refused: {message}")]
    ConnectFailed { message: String },

    #[error("remote failure: {message}")]
    Remote { message: String },

    /// The provider refused the call for now; `retry_after` is its delay, zero
    /// when it gave none.
    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Duration,
    },

    #[error("authentication failed: {message}")]
    Auth { message: String },

    #[error("invalid request: {message}")]
    InvalidRequest { message: String },

    #[error("unsupported operation: {message}")]
    Unsupported { message: String },

    #[error("provider failure: {message}")]
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
