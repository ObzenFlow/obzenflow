//! Source control strategies
//!
//! This module defines control strategies that are specific to
//! source stages. While `ControlEventStrategy` in
//! `stages::common::control_strategies` is concerned with how
//! stages handle *incoming* FlowControl (EOF/Drain/etc), source
//! strategies govern how sources decide to emit FlowControl on
//! completion or drain.
//!
//! FLOWIP-081a introduces an explicit strategy hook here so that
//! sources can participate in the same safety/contract story as
//! other stages, while keeping the default "Jonestown" behaviour
//! unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use obzenflow_core::circuit_breaker_registry;
use obzenflow_core::StageId;
use std::sync::atomic::Ordering;

/// High-level decision a source control strategy can make
///
/// These are intentionally stage-agnostic; individual finite / infinite
/// source implementations map them to concrete FlowControl payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceShutdownDecision {
    /// Use the stage's existing / default EOF semantics.
    ///
    /// - Finite sources: natural EOF based on handler completion.
    /// - Infinite sources: shutdown EOF used today during BeginDrain.
    DefaultEof,

    /// Emit a "poison" EOF to signal abnormal termination.
    ///
    /// Stages typically map this to `natural = false` in EOF payloads.
    PoisonEof,

    /// Emit a Drain control signal without EOF.
    ///
    /// This allows sources to request downstream shutdown while leaving
    /// EOF coordination to other components.
    DrainOnly,

    /// Take no action; allow the supervisor / FSM to behave as today.
    ///
    /// This is useful for strategies that only want to influence timing
    /// (e.g. delay) without changing the control action.
    Noop,
}

/// Mutable context passed to source control strategies
///
/// Mirrors the role of `ProcessingContext` in common control strategies:
/// it allows strategies to track attempts, delays, and custom state across
/// multiple invocations for a given source.
#[derive(Debug, Clone)]
pub struct SourceControlContext {
    /// Number of times shutdown has been attempted (for retry-like strategies)
    pub shutdown_attempts: usize,

    /// Whether we're currently in a delay/wait period
    pub in_delay: bool,

    /// Custom state that strategies can use for bookkeeping
    pub custom_state: HashMap<String, String>,
}

impl SourceControlContext {
    pub fn new() -> Self {
        Self {
            shutdown_attempts: 0,
            in_delay: false,
            custom_state: HashMap::new(),
        }
    }
}

impl Default for SourceControlContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Strategy trait for governing source shutdown / FlowControl emission
///
/// Implementations can influence how finite and infinite sources behave
/// when they complete naturally or when a drain/shutdown is requested.
pub trait SourceControlStrategy: Send + Sync + std::fmt::Debug {
    /// Handle a natural completion for a finite source
    ///
    /// Called when a finite source's handler reports completion via
    /// `is_complete() == true` and the supervisor is ready to shut down.
    fn on_natural_completion(
        &self,
        _ctx: &mut SourceControlContext,
    ) -> SourceShutdownDecision {
        SourceShutdownDecision::DefaultEof
    }

    /// Handle a drain / shutdown request (finite or infinite)
    ///
    /// Called when the supervisor is initiating a graceful drain, e.g.
    /// due to BeginDrain or external shutdown signals.
    fn on_begin_drain(&self, _ctx: &mut SourceControlContext) -> SourceShutdownDecision {
        SourceShutdownDecision::DefaultEof
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

impl SourceControlStrategy for JonestownSourceStrategy {}

/// Circuit breaker-aware source strategy
///
/// This strategy consults the global circuit breaker registry to decide
/// whether to emit a natural EOF (normal operation) or a "poison" EOF /
/// drain when the breaker is open for this stage.
#[derive(Debug)]
pub struct CircuitBreakerSourceStrategy {
    stage_id: StageId,
}

impl CircuitBreakerSourceStrategy {
    pub fn new(stage_id: StageId) -> Self {
        Self { stage_id }
    }

    fn is_breaker_open(&self) -> bool {
        if let Some(state) = circuit_breaker_registry::get_stage_state(&self.stage_id) {
            let raw = state.load(Ordering::SeqCst);
            // Mirror CircuitState::Open = 1 from the circuit breaker middleware.
            raw == 1
        } else {
            false
        }
    }
}

impl SourceControlStrategy for CircuitBreakerSourceStrategy {
    fn on_natural_completion(
        &self,
        _ctx: &mut SourceControlContext,
    ) -> SourceShutdownDecision {
        if self.is_breaker_open() {
            SourceShutdownDecision::PoisonEof
        } else {
            SourceShutdownDecision::DefaultEof
        }
    }

    fn on_begin_drain(&self, _ctx: &mut SourceControlContext) -> SourceShutdownDecision {
        if self.is_breaker_open() {
            // For now, align drain with poison EOF semantics when breaker is open.
            SourceShutdownDecision::PoisonEof
        } else {
            SourceShutdownDecision::DefaultEof
        }
    }
}
