// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::payment_gateway as support;

// Runtime adapter regression belongs to framework integration coverage.
mod shipping_adapter {

    use super::support::console::format_shipping;
    use super::support::domain::{PaymentAuthorized, TrafficPhase};
    use obzenflow::schema::{StageId, TypedPayload, WriterId};
    use obzenflow::stages::sinks::{
        ConsoleSink, DeliveryMethod, DeliveryResult, SinkConnector, SinkWriterInitContext,
    };
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};

    #[tokio::test]
    async fn shipping_sink_reports_its_real_console_write() {
        let authorized = PaymentAuthorized {
            order_id: "order-1".to_string(),
            customer_id: "customer-1".to_string(),
            amount_cents: 500,
            phase: TrafficPhase::Warmup,
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
        };
        let expected_bytes = (format_shipping(&authorized).len() + 1) as u64;
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            PaymentAuthorized::event_type_name(),
            PaymentAuthorized::payload_schema_version(),
            serde_json::to_value(authorized).expect("serialize payment"),
        );
        let stage_id = StageId::new();
        let connector = ConsoleSink::new(format_shipping);
        let writer = connector
            .open(SinkWriterInitContext::new(
                stage_id,
                "paid_orders".into(),
                "shipping_adapter".into(),
            ))
            .await
            .expect("open console writer");
        let mut adapter = SinkWriterAdapter::new(writer, stage_id, DeliveryMethod::ConsoleStdout);
        let report = adapter
            .consume_committed_report(
                obzenflow_core::JournalRecord::new(obzenflow_core::JournalWriterId::new(), event)
                    .into(),
                Default::default(),
            )
            .await
            .expect("shipping delivery");

        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert!(matches!(
            report.primary.delivery_method,
            DeliveryMethod::ConsoleStdout
        ));
        assert_eq!(report.primary.items_delivered, Some(1));
        assert_eq!(report.primary.bytes_processed, Some(expected_bytes));
    }
}
