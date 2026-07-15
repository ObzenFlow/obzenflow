// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::{MiddlewareAbortCause, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::EffectError;

/// Admission decision from one per-effect policy.
pub enum PolicyAdmission {
    /// Admit the invocation; the policy observes how the attempt ends.
    Admit,
    /// Skip execution and synthesize fallback results in its place.
    Synthesize {
        results: Vec<ChainEvent>,
        cause: Option<MiddlewareAbortCause>,
    },
    /// Reject execution outright, recorded as a `Failed` outcome under the
    /// effect cursor so strict replay reproduces the rejection.
    Reject(MiddlewareAbortCause),
}

/// How a guarded attempt ended, shown to every policy that admitted it.
pub enum EffectAttemptOutcome<'a> {
    /// The effect executed; on success the events are observation-grade
    /// derived copies of the authored facts.
    Executed(&'a Result<Vec<ChainEvent>, EffectError>),
    /// A later policy synthesized fallback results; the protected call never
    /// went out.
    SkippedBy(&'a str),
    /// A later policy rejected execution; the protected call never went out.
    RejectedBy(&'a MiddlewareAbortCause),
}

/// A resilience policy guarding one declared effect (FLOWIP-120c).
#[async_trait]
pub trait EffectPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    /// Admission for one effect invocation. May await; must not block the
    /// worker thread (the FLOWIP-114o boundary slice).
    async fn admit(&self, ctx: &mut MiddlewareContext) -> PolicyAdmission;

    /// Observation of how the attempt ended. Runs for every policy that
    /// admitted, regardless of which arm ended the attempt.
    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, ctx: &mut MiddlewareContext);
}

/// Event-aware effect policy for middleware that genuinely needs parent-event
/// access to classify, synthesize, or derive output facts.
#[async_trait]
pub trait EventAwareEffectPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission;

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    );
}
