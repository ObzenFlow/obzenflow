// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Named typed shipping destination.

use super::console;
use super::domain::PaymentAuthorized;
use async_trait::async_trait;
use obzenflow::middleware::{SinkDeliveryObserver, SinkDeliveryObserverContext};
use obzenflow::stages::sinks::DeliveryMethod;
use obzenflow::stages::sinks::{
    InlineSink, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};

/// Small in-process shipping handoff used by the demo.
#[derive(Clone, Debug, Default)]
pub struct ShippingHandoff;

#[async_trait]
impl InlineSink for ShippingHandoff {
    type Input = PaymentAuthorized;

    async fn write(
        &mut self,
        authorized: PaymentAuthorized,
        context: SinkWriteContext,
    ) -> obzenflow::stages::sinks::SinkWriteResult {
        console::send_to_shipping(authorized, context.delivery().provenance());
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success_via(
                DeliveryMethod::Custom("console:stdout".to_string()),
                None,
            )
            .with_items(1),
        ))
    }
}

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
