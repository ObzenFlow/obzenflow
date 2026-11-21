// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::PaymentAuthorizationUnavailable;
use obzenflow::middleware::{
    EffectObserver, EffectObserverContext, ObserverResult, TypedEffectObserver,
};

pub struct ShippingDeliveryLog;

impl EffectObserver for ShippingDeliveryLog {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        tracing::info!(
            stage = ctx.stage_name(),
            outcome = ?ctx.outcome(),
            "shipping delivery observed"
        );
    }
}

pub struct ManualReviewDeliveryLog;

impl TypedEffectObserver for ManualReviewDeliveryLog {
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
