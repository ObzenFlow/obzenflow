// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source control strategies
//!
//! This module defines control strategies that are specific to
//! source stages. While `SignalGate` in
//! `stages::common::control_strategies` is concerned with how
//! stages handle *incoming* FlowControl (EOF/Drain/etc), source
//! strategies govern how sources decide to emit FlowControl on
//! completion or drain.
//!
//! An explicit strategy hook lets sources participate in the same
//! safety/contract story as other stages, while keeping the default
//! behaviour unchanged.

use std::collections::HashMap;

/// High-level decision a source control strategy can make
///
/// These are intentionally stage-agnostic; individual finite / infinite
/// source implementations map them to concrete FlowControl payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDecision {
    /// Use the stage's existing / default EOF semantics.
    ///
    /// - Finite sources: natural EOF based on handler completion.
    /// - Infinite sources: shutdown EOF used today during BeginDrain.
    DefaultEof,

    /// Emit a "poison" EOF to signal abnormal termination.
    ///
    /// Stages typically map this to `natural = false` in EOF payloads.
    PoisonEof,
}

/// Mutable context passed to source control strategies
///
/// Mirrors the role of `ProcessingContext` in common control strategies:
/// it allows strategies to track attempts, delays, and custom state across
/// multiple invocations for a given source.
#[derive(Debug, Clone)]
pub struct CompletionContext {
    /// Number of times shutdown has been attempted (for retry-like strategies)
    pub shutdown_attempts: usize,

    /// Whether we're currently in a delay/wait period
    pub in_delay: bool,

    /// Custom state that strategies can use for bookkeeping
    pub custom_state: HashMap<String, String>,
}

impl CompletionContext {
    pub fn new() -> Self {
        Self {
            shutdown_attempts: 0,
            in_delay: false,
            custom_state: HashMap::new(),
        }
    }
}

impl Default for CompletionContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Strategy trait for governing source shutdown / FlowControl emission
///
/// Implementations can influence how finite and infinite sources behave
/// when they complete naturally or when a drain/shutdown is requested.
pub trait CompletionGate: Send + Sync + std::fmt::Debug {
    /// Handle a natural completion for a finite source
    ///
    /// Called when a finite source's handler reports completion via
    /// `is_complete() == true` and the supervisor is ready to shut down.
    fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::DefaultEof
    }

    /// Handle a drain / shutdown request (finite or infinite)
    ///
    /// Called when the supervisor is initiating a graceful drain, e.g.
    /// due to BeginDrain or external shutdown signals.
    fn on_begin_drain(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::DefaultEof
    }
}

/// Default "Jonestown" source strategy
///
/// This mirrors the current behaviour for sources:
/// - Finite sources: emit their natural EOF when complete.
/// - Infinite sources: emit their existing shutdown EOF on drain.
///
/// It is intentionally conservative: it never changes the control action,
/// leaving all semantics to the existing FSM implementation.
#[derive(Debug)]
pub struct JonestownSourceStrategy;

impl CompletionGate for JonestownSourceStrategy {}
