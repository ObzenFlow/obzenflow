// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::time::Duration;

use super::ChatTarget;

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

    #[error("chat target mismatch: requested {requested}, bound {bound}")]
    TargetMismatch {
        requested: Box<ChatTarget>,
        bound: Box<ChatTarget>,
    },

    #[error("other AI client error: {message}")]
    Other { message: String },
}

impl AiClientError {
    pub fn target_mismatch(requested: ChatTarget, bound: ChatTarget) -> Self {
        Self::TargetMismatch {
            requested: Box::new(requested),
            bound: Box::new(bound),
        }
    }
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
