// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware behavior hints for static analysis and validation
//!
//! This module provides a lightweight, zero-cost way for middleware factories
//! to describe their behavior without runtime introspection or downcasting.

/// Static hints about a middleware's behavior
#[derive(Debug, Default, Clone)]
pub struct MiddlewareHints {
    /// Whether this middleware drops control events
    pub drops_control_events: bool,
    /// Batching behavior, if any
    pub batching: Option<BatchingHint>,
    /// Whether this middleware rate limits
    pub rate_limits: bool,
}

/// Hints about batching behavior
#[derive(Debug, Clone)]
pub struct BatchingHint {
    /// Whether batching has an upper bound
    pub bounded: bool,
    /// Timeout for flushing partial batches
    pub timeout_ms: Option<u64>,
}
