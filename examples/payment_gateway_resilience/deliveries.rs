// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{PaymentAuthorizationUnavailable, PaymentAuthorized};
use obzenflow::middleware::{ObserverResult, SinkDeliveryObserver, SinkDeliveryObserverContext};

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
