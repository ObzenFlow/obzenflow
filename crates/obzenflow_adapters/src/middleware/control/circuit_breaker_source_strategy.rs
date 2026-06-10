// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit breaker-aware source control strategy.
//!
//! FLOWIP-120h relocated this from `obzenflow_runtime`: the runtime owns the
//! policy-neutral `SourceControlStrategy` port and the `Poison` EOF signal,
//! while this breaker-named implementation belongs with the breaker in the
//! adapters ring.

use obzenflow_core::control_middleware::{cb_state, ControlMiddlewareProvider};
use obzenflow_core::StageId;
use obzenflow_runtime::metrics::instrumentation::ControlBindError;
use obzenflow_runtime::stages::source::strategies::{
    SourceControlContext, SourceControlStrategy, SourceShutdownDecision,
};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// Circuit breaker-aware source strategy
///
/// This strategy consults breaker state (injected via the flow-scoped
/// `ControlMiddlewareProvider`) to decide whether to emit a natural EOF
/// (normal operation) or a "poison" EOF / drain when the breaker is open
/// for this stage.
#[derive(Debug)]
pub struct CircuitBreakerSourceStrategy {
    cb_state: Arc<AtomicU8>,
}

impl CircuitBreakerSourceStrategy {
    pub fn new(cb_state: Arc<AtomicU8>) -> Self {
        Self { cb_state }
    }

    /// Construct a breaker-aware source strategy using the control middleware provider.
    ///
    /// Fails fast if the breaker state isn't registered; this indicates a wiring
    /// bug since this strategy is only created when circuit breaker middleware is configured.
    pub fn try_new(
        stage_id: StageId,
        provider: &Arc<dyn ControlMiddlewareProvider>,
    ) -> Result<Self, ControlBindError> {
        let cb_state = provider
            .circuit_breaker_state(&stage_id)
            .ok_or(ControlBindError::MissingCircuitBreaker { stage_id })?;
        Ok(Self { cb_state })
    }

    fn is_breaker_open(&self) -> bool {
        self.cb_state.load(Ordering::SeqCst) == cb_state::OPEN
    }
}

impl SourceControlStrategy for CircuitBreakerSourceStrategy {
    fn on_natural_completion(&self, _ctx: &mut SourceControlContext) -> SourceShutdownDecision {
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
