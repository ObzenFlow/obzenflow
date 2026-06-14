// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core middleware trait and related types
//!
//! This module defines the fundamental middleware abstraction and the
//! actions that middleware can take during event processing.

use super::MiddlewareContext;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};

/// Trait for composable middleware that wraps Step behavior.
///
/// Middleware follows a three-phase interaction model:
/// 1. **Pre-processing** (`pre_handle`): Before the step processes the event
/// 2. **Post-processing** (`post_handle`): After successful processing  
/// 3. **Error handling** (`on_error`): When something goes wrong
///
/// ## Example Implementation
///
/// ```rust
/// use obzenflow_adapters::middleware::{
///     Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase,
/// };
/// use obzenflow_core::ChainEvent;
///
/// struct LoggingMiddleware;
///
/// impl Middleware for LoggingMiddleware {
///     fn label(&self) -> &'static str {
///         "logging"
///     }
///
///     fn source_phase(&self) -> SourceMiddlewarePhase {
///         SourceMiddlewarePhase::Ordinary
///     }
///
///     fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
///         println!("Processing event: {:?}", event.id);
///         MiddlewareAction::Continue
///     }
///     
///     fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
///         println!("Produced {} results", results.len());
///     }
/// }
/// ```
pub trait Middleware: Send + Sync {
    /// Display label for logs, metrics labels, topology output, and diagnostics.
    fn label(&self) -> &'static str;

    /// Typed source-phase role used by source wrappers for control flow.
    fn source_phase(&self) -> SourceMiddlewarePhase;

    /// Typed middleware kind for the FLOWIP-120c placement split (H2),
    /// mirrored from `MiddlewareFactory::kind` because chain runners hold
    /// instances, not factories. Implementations that override the factory
    /// side must override here too; a kind-agreement test enforces it.
    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Observation
    }

    /// Called before the inner step processes the event.
    ///
    /// Use this to:
    /// - Implement rate limiting (return `Skip` or `Abort`)
    /// - Add caching (return `Skip` with cached results)
    /// - Validate events (return `Abort` for invalid)
    /// - Record metrics (return `Continue` after recording)
    /// - Emit middleware events for observability
    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
    }

    /// Called after the inner step successfully processes the event.
    ///
    /// Use this to:
    /// - Observe results (but not modify them)
    /// - Record success metrics
    /// - Perform side effects like logging
    /// - Emit middleware events based on outcomes
    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
        // Default: no-op
    }

    /// Called when an error occurs during processing.
    ///
    /// Use this to:
    /// - Implement retry logic (return `Retry`)
    /// - Convert errors to events (return `Recover`)
    /// - Let errors bubble up (return `Propagate`)
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        ErrorAction::Propagate
    }

    /// Called before each result event is written to the journal.
    ///
    /// This hook enables event enrichment with timing data, flow context,
    /// and other metadata needed for observability. The event is mutable,
    /// allowing middleware to add fields following the "wide events" pattern.
    ///
    /// Use this to:
    /// - Add processing time to events
    /// - Ensure flow context is populated
    /// - Enrich events with deployment/environment info
    /// - Add correlation IDs for tracing
    ///
    /// Default implementation is a no-op for backward compatibility.
    fn pre_write(&self, _event: &mut ChainEvent, _ctx: &MiddlewareContext) {
        // Default: no-op
    }

    /// FLOWIP-114q transitional hook: fired on each middleware of the visited
    /// prefix (those that returned `Continue`) when a later middleware
    /// short-circuits the chain with `Skip` or `Abort`. Internal machinery
    /// scoped to the legacy async transform runner; not a public composition
    /// surface. The effect boundary records rejections under the effect
    /// cursor instead (FLOWIP-120c), and FLOWIP-120p decides this hook's
    /// fate when the AI legs move onto effects.
    fn on_rejected(
        &self,
        _event: &ChainEvent,
        _cause: Option<&MiddlewareAbortCause>,
        _ctx: &mut MiddlewareContext,
    ) {
        // Default: no-op
    }

    /// FLOWIP-120c: policies with a native async per-effect surface return it
    /// here so per-effect chains use their own admission (a rate limiter
    /// awaits its permit instead of blocking). Policy kinds without a native
    /// implementation are adapted from this chain surface instead.
    fn as_effect_policy(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn crate::middleware::EffectPolicy>> {
        None
    }

    /// FLOWIP-114o: sources with a native async pacing surface return it here so
    /// async source supervisors await a permit (a cancellable future) instead of
    /// blocking the worker thread in `pre_handle`. Only the rate limiter
    /// overrides this; every other middleware falls through to the synchronous
    /// `pre_handle` at the source gate.
    fn as_source_pacer(self: std::sync::Arc<Self>) -> Option<std::sync::Arc<dyn SourcePacer>> {
        None
    }
}

/// Structured cause attached to an abort so downstream seams, the effect
/// boundary in particular, can record the rejection under the effect cursor
/// instead of losing it as an unrecorded error.
#[derive(Debug, Clone)]
pub struct MiddlewareAbortCause {
    /// Label of the rejecting middleware, e.g. "circuit_breaker".
    pub source: EffectFailureSource,
    /// Stable machine-readable reason, e.g. "rejected_circuit_open".
    pub code: EffectFailureCode,
    /// Human-readable detail for the recorded failure message.
    pub message: String,
    pub retry: RetryDisposition,
    /// Optional event payload carried with the rejection (FLOWIP-114q
    /// unified cause), e.g. a pre-built structured error event for the
    /// rejected input. `None` until a producer is migrated.
    pub event: Option<ChainEvent>,
}

/// Actions that middleware can take during pre-processing
#[derive(Debug, Clone)]
pub enum MiddlewareAction {
    /// Continue with normal processing
    Continue,
    /// Skip the inner step and return these results instead. The optional
    /// cause attributes the short-circuit (FLOWIP-114q unified cause).
    Skip {
        results: Vec<ChainEvent>,
        cause: Option<MiddlewareAbortCause>,
    },
    /// Abort processing entirely (returns empty results). The optional cause
    /// lets the effect boundary record a structured, replayable rejection.
    Abort { cause: Option<MiddlewareAbortCause> },
}

/// FLOWIP-114o: an async pacing surface for source middleware. A policy that
/// would otherwise block a worker thread while throttling (the rate limiter)
/// implements this so async source supervisors can `.await` admission as a
/// cancellable future. The supervisor's `biased` select then interrupts the
/// wait on drain, EOF, cancellation, or shutdown. Sync source supervisors have
/// no await point and keep the blocking `pre_handle` path (FLOWIP-114o Q4,
/// retain-and-document).
#[async_trait::async_trait]
pub trait SourcePacer: Send + Sync {
    /// Await admission for one source poll, returning the gate decision. The
    /// rate limiter returns `Continue` once it has consumed a token, awaiting
    /// its permit first if throttled. Accounting and lifecycle events are
    /// identical to the synchronous `pre_handle` path; only the wait becomes a
    /// cancellable future.
    async fn source_admit(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> MiddlewareAction;
}

/// FLOWIP-120c H2 enforcement: an observation-classified middleware may not
/// short-circuit the chain, in any scope, on any chain. Returns the error
/// message when the action violates the classification. This is a
/// deterministic code-level misclassification, so chain runners fail loud
/// even during strict-replay reconstruction.
pub(crate) fn observation_short_circuit(
    middleware: &dyn Middleware,
    action: &MiddlewareAction,
) -> Option<String> {
    if matches!(action, MiddlewareAction::Continue) {
        return None;
    }
    if middleware.kind() != crate::middleware::MiddlewareKind::Observation {
        return None;
    }
    let verb = if matches!(action, MiddlewareAction::Skip { .. }) {
        "Skip"
    } else {
        "Abort"
    };
    Some(format!(
        "middleware '{}' is observation-classified (the default) but returned {verb}; \
         declare MiddlewareKind::Policy or MiddlewareKind::Structural on its factory and \
         instance, or move the filter into the handler (FLOWIP-120c H2)",
        middleware.label()
    ))
}

/// Actions that middleware can take when handling errors
#[derive(Debug, Clone)]
pub enum ErrorAction {
    /// Propagate the error up to the caller
    Propagate,
    /// Recover by converting the error into events
    Recover(Vec<ChainEvent>),
    /// Retry the operation (be careful of infinite loops!)
    Retry,
}

// Implementation for Box<dyn Middleware> to allow boxed middleware
impl<M: Middleware + ?Sized> Middleware for Box<M> {
    fn label(&self) -> &'static str {
        (**self).label()
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        (**self).source_phase()
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        (**self).kind()
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        (**self).pre_handle(event, ctx)
    }

    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        (**self).post_handle(event, results, ctx)
    }

    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        (**self).on_error(event, ctx)
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        (**self).pre_write(event, ctx)
    }

    fn on_rejected(
        &self,
        event: &ChainEvent,
        cause: Option<&MiddlewareAbortCause>,
        ctx: &mut MiddlewareContext,
    ) {
        (**self).on_rejected(event, cause, ctx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMiddlewarePhase {
    CircuitBreakerGate,
    RateLimiterGate,
    Ordinary,
}
