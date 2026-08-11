// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Named typed shipping destination.

use super::console;
use super::domain::PaymentAuthorized;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome, TypedSinkConsumeReport,
    TypedSinkHandler,
};

/// The shipping-system handoff: in production a queue publish; in this demo
/// a labelled console line stands in for the subscriber.
#[derive(Clone, Debug, Default)]
pub struct ShippingHandoff;

#[async_trait]
impl TypedSinkHandler for ShippingHandoff {
    type Input = PaymentAuthorized;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::destination(
            "shipping.handoff",
            SinkDeliverySafety::IdempotentProjection,
            Some(serde_json::json!({ "queue": "shipping" })),
        )
    }

    async fn consume(
        &mut self,
        authorized: PaymentAuthorized,
        context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        console::send_to_shipping(authorized, context.delivery().provenance());
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(
                DeliveryMethod::QueuePublish {
                    queue_name: "shipping".to_string(),
                },
                None,
            )
            .with_items(1),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::domain::TrafficPhase;
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, TypedPayload, WriterId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, TypedSinkHandlerAdapter};

    #[tokio::test]
    async fn named_shipping_delivery_preserves_its_exact_receipt_fields() {
        let handler = ShippingHandoff;
        let declaration = handler.delivery_declaration();
        assert_eq!(declaration.delivery_type(), Some("shipping.handoff"));
        assert_eq!(
            declaration.safety(),
            Some(SinkDeliverySafety::IdempotentProjection)
        );

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
        let mut adapter = TypedSinkHandlerAdapter::new(handler, StageId::new());
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
            DeliveryMethod::QueuePublish { ref queue_name } if queue_name == "shipping"
        ));
        assert_eq!(report.primary.items_delivered, Some(1));
        assert_eq!(report.primary.bytes_processed, None);
    }
}
