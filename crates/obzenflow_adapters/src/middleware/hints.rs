// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware behavior hints for static analysis and validation
//!
//! This module provides a lightweight, zero-cost way for middleware factories
//! to describe their behavior without runtime introspection or downcasting.

use std::time::Duration;

/// Static hints about a middleware's behavior
#[derive(Debug, Default, Clone)]
pub struct MiddlewareHints {
    /// Retry behavior, if any
    pub retry: Option<RetryHint>,
    /// Whether this middleware drops control events
    pub drops_control_events: bool,
    /// Batching behavior, if any
    pub batching: Option<BatchingHint>,
    /// Whether this middleware rate limits
    pub rate_limits: bool,
}

/// Hints about retry behavior
#[derive(Debug, Clone)]
pub struct RetryHint {
    /// Maximum number of retry attempts
    pub max_attempts: Attempts,
    /// Backoff strategy for retries
    pub backoff: BackoffKind,
}

/// Number of retry attempts
#[derive(Debug, Clone, PartialEq)]
pub enum Attempts {
    /// A specific finite number of attempts
    Finite(usize),
    /// Infinite retries (dangerous!)
    Infinite,
}

/// Backoff strategy for retries
#[derive(Debug, Clone)]
pub enum BackoffKind {
    /// Fixed delay between retries
    Fixed { delay: Duration },
    /// Exponential backoff with optional jitter
    Exponential {
        base: Duration,
        factor: f64,
        max: Option<Duration>,
    },
}

/// Hints about batching behavior
#[derive(Debug, Clone)]
pub struct BatchingHint {
    /// Whether batching has an upper bound
    pub bounded: bool,
    /// Timeout for flushing partial batches
    pub timeout_ms: Option<u64>,
}
