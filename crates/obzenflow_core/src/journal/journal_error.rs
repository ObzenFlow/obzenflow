// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// Journal errors (domain-level, not I/O specific)
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("Journal is full")]
    Full,

    #[error("Event not found: {0}")]
    EventNotFound(String),

    #[error("Writer not authorized")]
    Unauthorized,

    #[error("Causal ordering violation")]
    CausalityViolation,

    #[error("Subscription closed")]
    SubscriptionClosed,

    /// Generic implementation error with source
    #[error("Implementation error: {message}")]
    Implementation {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
