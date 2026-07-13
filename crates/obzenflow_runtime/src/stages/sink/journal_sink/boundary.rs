// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Policy-neutral sink-delivery boundary seam (FLOWIP-115b).
//!
//! The runtime owns this seam and the journal sink supervisor drives it around
//! one data-event `consume_report` attempt, before delivery receipts are
//! normalised and journalled. Concrete middleware policy composition lives
//! outside the runtime (the adapter-owned sink policy onion) and implements
//! [`SinkDeliveryBoundary`]. The seam carries no `Middleware` in its name: that
//! is adapter authoring vocabulary.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::SinkConsumeReport;
use crate::stages::common::BoundaryStopReceiver;
use async_trait::async_trait;
use obzenflow_core::{ChainEvent, EventId};

/// How one sink-delivery attempt ended once the boundary admitted it.
pub enum SinkDeliveryAttemptOutcome {
    /// The handler ran. The payload is its consume result, boxed because the
    /// report dwarfs the panic variant (clippy::large_enum_variant).
    Delivered(Result<Box<SinkConsumeReport>, HandlerError>),
    /// The handler panicked. The supervisor applies stage-fatal panic policy.
    Panicked { message: String },
}

/// A structured terminal rejection from a sink-delivery policy (for example an
/// open breaker). The supervisor maps it to a *failed* delivery receipt carrying
/// this metadata, never a successful `DeliveryMethod::Noop`.
#[derive(Debug, Clone)]
pub struct SinkDeliveryRejection {
    pub policy: String,
    pub reason: String,
}

/// The sink-delivery boundary outcome for one guarded attempt.
pub enum SinkDeliveryBoundaryOutcome {
    Attempted(SinkDeliveryAttemptOutcome),
    Rejected(SinkDeliveryRejection),
    /// The hard boundary deadline cancelled an active delivery future. The local
    /// future is gone, but the external destination may still complete the
    /// request, so the supervisor must commit an explicitly in-doubt receipt
    /// rather than claiming rollback or adapting this into a handler error.
    DeadlineOutcomeUnknown {
        message: String,
    },
}

/// The boundary's terminal report consumed by the journal sink supervisor.
///
/// A retry-capable boundary may invoke [`SinkDeliveryExecutor::attempt`] more
/// than once, but it returns only the logical invocation's terminal outcome.
/// `control_events` is an invocation-ordered, buffered evidence outbox. The
/// supervisor commits the terminal delivery receipt before appending this
/// evidence, and the evidence never advances sink receipt progress.
pub struct SinkDeliveryBoundaryReport {
    pub outcome: SinkDeliveryBoundaryOutcome,
    pub control_events: Vec<ChainEvent>,
}

/// Re-invokable executor for one sink-delivery attempt.
///
/// Deliberately re-invokable rather than a single future: FLOWIP-115h
/// reintroduces breaker retry as boundary-owned recovery by calling `attempt`
/// more than once. Each call receives the same logical input identity and
/// creates one fresh physical handler future. Strict replay bypasses the
/// boundary and calls it exactly once.
#[async_trait]
pub trait SinkDeliveryExecutor: Send {
    /// Stable logical delivery identity reused by every physical attempt.
    fn parent_event_id(&self) -> EventId;

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome;
}

/// Runtime-neutral sink-delivery boundary interface.
///
/// The supervisor drives this around the data-event `consume_report` attempt and
/// maps the report to durable receipts. It does not know which, if any,
/// middleware policies are composed behind the boundary. The boundary does not
/// wrap EOF, drain, flush, poison-pill handling, control-event forwarding, or
/// sink lifecycle cleanup; those remain runtime lifecycle responsibilities.
#[async_trait]
pub trait SinkDeliveryBoundary: Send + Sync {
    async fn around_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
    ) -> SinkDeliveryBoundaryReport;

    /// Retry-aware entry point with an explicit runtime stop signal.
    ///
    /// The additive default preserves existing one-attempt boundaries. A
    /// retry-capable implementation overrides this method so drain can cancel
    /// admission/backoff without starting another attempt and abort can drop
    /// active asynchronous work promptly.
    async fn around_retryable_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
        stop: BoundaryStopReceiver,
    ) -> SinkDeliveryBoundaryReport {
        drop(stop);
        self.around_sink_delivery(execute).await
    }
}
