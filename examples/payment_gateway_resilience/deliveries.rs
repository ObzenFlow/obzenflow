// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Passive application diagnostics for terminal deliveries.

use super::domain::PaymentAuthorizationUnavailable;
use obzenflow::middleware::{
    SinkDeliveryAttemptResult, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome,
};
use obzenflow::schema::TypedPayload;

/// Emits an application diagnostic after the runtime classifies a shipping
/// delivery. It receives an immutable view and cannot alter settlement.
pub struct ShippingDeliveryLog;

impl SinkDeliveryObserver for ShippingDeliveryLog {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        tracing::info!(
            stage = ctx.stage_name(),
            outcome = ?ctx.outcome(),
            "shipping delivery observed"
        );
    }
}

/// Record manual-review routing only after console output succeeds. Previously
/// the diagnostic ran before output and could claim a handoff that then failed.
/// This observer does not deliver another item or alter the delivery receipt.
pub struct ManualReviewDeliveryLog;

impl SinkDeliveryObserver for ManualReviewDeliveryLog {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        if !matches!(
            ctx.outcome(),
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedSuccess
            }
        ) {
            return;
        }
        let Ok(unavailable) = PaymentAuthorizationUnavailable::try_from_event(ctx.input()) else {
            return;
        };
        tracing::info!(
            operation = "payment.authorization",
            handoff_kind = "manual_review",
            order_id = %unavailable.order_id,
            "authorization queued for manual review"
        );
    }
}
