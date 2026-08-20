// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Named typed shipping destination.

use super::console;
use super::domain::PaymentAuthorized;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_runtime::stages::observer::{SinkDeliveryObserver, SinkDeliveryObserverContext};
use obzenflow_runtime::stages::sink::{
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
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
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

#[cfg(test)]
mod tests {
    use super::super::domain::TrafficPhase;
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, TypedPayload, WriterId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};

    #[tokio::test]
    async fn inline_shipping_sink_reports_its_real_console_write() {
        let authorized = PaymentAuthorized {
            order_id: "order-1".to_string(),
            customer_id: "customer-1".to_string(),
            amount_cents: 500,
            phase: TrafficPhase::Warmup,
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            PaymentAuthorized::versioned_event_type(),
            serde_json::to_value(authorized).expect("serialize payment"),
        );
        let stage_id = StageId::new();
        let mut adapter = SinkWriterAdapter::new(ShippingHandoff, stage_id);
        let report = adapter
            .consume_report(event)
            .await
            .expect("shipping delivery");

        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert!(matches!(
            report.primary.delivery_method,
            DeliveryMethod::Custom(ref method) if method == "console:stdout"
        ));
        assert_eq!(report.primary.items_delivered, Some(1));
        assert_eq!(report.primary.bytes_processed, None);
    }
}
