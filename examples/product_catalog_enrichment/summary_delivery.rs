// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application-owned protection for the one final analytics dashboard.
//!
//! The summary fold emits at EOF, so its sink needs at most one live delivery
//! attempt. A second admission is a wiring or emission error and must not print
//! another dashboard. This is a per-materialisation attempt budget, not durable
//! deduplication: admission spends it even if delivery fails or is cancelled.
//! Strict replay retains the framework's existing console-delivery behaviour.

use async_trait::async_trait;
use obzenflow::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx,
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
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SinkDelivery])
            .with_control_intent(MiddlewareSurfaceKind::SinkDelivery)
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
        Ok(MiddlewareSurfaceAttachment::sink_delivery(Arc::new(
            SingleSummaryDeliveryPolicy {
                admitted: AtomicBool::new(false),
            },
        )))
    }
}

struct SingleSummaryDeliveryPolicy {
    admitted: AtomicBool,
}

#[async_trait]
impl SinkPolicy for SingleSummaryDeliveryPolicy {
    fn label(&self) -> &'static str {
        "single_summary_delivery"
    }

    async fn admit(&self, _context: &mut SinkPolicyCtx) -> SinkAdmission {
        // This flag is the entire budget; it does not publish any other state.
        if self.admitted.swap(true, Ordering::Relaxed) {
            SinkAdmission::Reject {
                reason: "the final analytics summary permits only one live delivery attempt per materialised sink"
                    .to_string(),
            }
        } else {
            SinkAdmission::Admit(None)
        }
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _context: &mut SinkPolicyCtx) {}
}
