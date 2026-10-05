// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Passive application diagnostics for terminal deliveries.

use super::domain::{PaymentAuthorizationUnavailable, PaymentAuthorized};
use obzenflow::middleware::{ObserverResult, SinkDeliveryObserver, SinkDeliveryObserverContext};

/// Emits an application diagnostic after the runtime classifies a shipping
/// delivery. It receives an immutable view and cannot alter settlement.
pub struct ShippingDeliveryLog;

impl SinkDeliveryObserver for ShippingDeliveryLog {
    type Input = PaymentAuthorized;

    fn on_attempt(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverResult {
        tracing::info!(
            stage = ctx.stage_name(),
            outcome = ?ctx.outcome(),
            "shipping delivery observed"
        );
        Ok(())
    }
}

/// Log the manual-review notice after console delivery succeeds.
pub struct ManualReviewDeliveryLog;

impl SinkDeliveryObserver for ManualReviewDeliveryLog {
    type Input = PaymentAuthorizationUnavailable;

    fn on_delivered(&self, unavailable: &Self::Input) -> ObserverResult {
        tracing::info!(
            operation = "payment.authorization",
            handoff_kind = "manual_review",
            order_id = %unavailable.order_id,
            "manual-review record written to console"
        );
        Ok(())
    }
}
