// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application-owned protection for the one final analytics dashboard.
//!
//! The summary fold emits at EOF, so its sink needs at most one live call to
//! the console. A second admission is a wiring or emission error and must not
//! print another dashboard. This is a per-materialization attempt budget, not
//! durable deduplication, so admission spends it even if the call fails or is
//! cancelled. Replay makes no call to the console, so it never spends the budget.

use async_trait::async_trait;
use obzenflow::effects::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow::middleware::{
    validate_attachment_request, EffectAttemptOutcome, EffectPolicy, MiddlewareAbortCause,
    MiddlewareAttachmentRequest, MiddlewareContext, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A fixed one-attempt budget for an emit-on-EOF summary sink.
///
/// It has no resolver-visible settings. Each checked materialisation creates
/// independent policy state, including when the same factory value is reused.
pub struct SingleSummaryDelivery;

impl MiddlewareFactory for SingleSummaryDelivery {
    fn label(&self) -> &'static str {
        "single_summary_delivery"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::Effect])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::effect(Arc::new(
            SingleSummaryDeliveryPolicy::new(),
        )))
    }
}

/// The budget one materialization of [`SingleSummaryDelivery`] spends.
pub(crate) struct SingleSummaryDeliveryPolicy {
    admitted: AtomicBool,
}

impl SingleSummaryDeliveryPolicy {
    pub(crate) fn new() -> Self {
        Self {
            admitted: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl EffectPolicy for SingleSummaryDeliveryPolicy {
    fn label(&self) -> &'static str {
        "single_summary_delivery"
    }

    async fn admit(&self, _context: &mut MiddlewareContext) -> PolicyAdmission {
        if self.admitted.swap(true, Ordering::Relaxed) {
            PolicyAdmission::Reject(Box::new(MiddlewareAbortCause {
                source: EffectFailureSource::new("single_summary_delivery"),
                code: EffectFailureCode::new("budget_spent"),
                message: "the final analytics summary permits only one live delivery attempt per materialized sink"
                    .to_string(),
                retry: RetryDisposition::NotRetryable,
                event: None,
            }))
        } else {
            PolicyAdmission::Admit
        }
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _context: &mut MiddlewareContext) {}
}
