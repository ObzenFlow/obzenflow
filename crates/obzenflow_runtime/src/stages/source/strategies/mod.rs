// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source control strategies
//!
//! This module defines control strategies that are specific to
//! source stages. While `SignalGate` in
//! `stages::common::control_strategies` is concerned with how
//! stages handle *incoming* FlowControl (EOF/Drain/etc), source
//! strategies govern how sources decide to emit FlowControl at
//! completion.
//!
//! An explicit strategy hook lets sources participate in the same
//! safety/contract story as other stages, while keeping the default
//! behaviour unchanged.

use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use std::collections::HashMap;

/// High-level decision a source control strategy can make
///
/// These are intentionally stage-agnostic; individual finite / infinite
/// source implementations map them to concrete FlowControl payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDecision {
    /// Natural EOF for an exhausted reader.
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

/// Strategy trait for governing source FlowControl emission at exhaustion.
pub trait CompletionGate: Send + Sync + std::fmt::Debug {
    /// Consulted only when the reader reported exhaustion; a stop never asks.
    fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::DefaultEof
    }
}

/// Default "Jonestown" source strategy: natural EOF at exhaustion.
#[derive(Debug)]
pub struct JonestownSourceStrategy;

impl CompletionGate for JonestownSourceStrategy {}

/// Why a source completes (FLOWIP-134i B5). The first cause observed wins, so a
/// source's own drain after exhaustion or replay keeps the earlier cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCompletionCause {
    /// The reader reported exhaustion.
    Exhausted,
    /// An operator, pipeline or boundary-policy stop arrived before exhaustion.
    Stopped,
    /// Replay exhaustion reproduces the archive's recorded kind (FLOWIP-095k);
    /// `None`: the archive committed no EOF.
    ReplayExhausted { recorded_kind: Option<EofKind> },
}

impl SourceCompletionCause {
    /// Only exhaustion consults the gate; a stop is always `Truncated`.
    pub(crate) fn eof_kind(
        self,
        gate: &dyn CompletionGate,
        ctx: &mut CompletionContext,
    ) -> EofKind {
        match self {
            Self::Exhausted => match gate.on_natural_completion(ctx) {
                CompletionDecision::PoisonEof => EofKind::Poison,
                CompletionDecision::DefaultEof => EofKind::Natural,
            },
            Self::Stopped => EofKind::Truncated,
            Self::ReplayExhausted { recorded_kind } => recorded_kind.unwrap_or(EofKind::Truncated),
        }
    }
}

/// First-writer-wins slot for a source's completion cause.
#[derive(Debug, Default)]
pub(crate) struct CompletionCauseSlot(Option<SourceCompletionCause>);

impl CompletionCauseSlot {
    /// Record why the source completes; a later cause never overrides it.
    pub(crate) fn record(&mut self, cause: SourceCompletionCause) {
        self.0.get_or_insert(cause);
    }

    /// Absent means a stop, so natural EOF requires recorded exhaustion.
    pub(crate) fn cause(&self) -> SourceCompletionCause {
        self.0.unwrap_or(SourceCompletionCause::Stopped)
    }
}
