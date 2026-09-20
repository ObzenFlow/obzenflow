// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod support;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn build_validation_journal_flow(journal_root: PathBuf) -> obzenflow_dsl::FlowDefinition {
    use support::domain::{CustomerOrderPlaced, OrderChannel, PaymentMethodState, TrafficPhase};

    let valid = CustomerOrderPlaced {
        order_id: "validation-valid-order".to_string(),
        customer_id: "validation-valid-customer".to_string(),
        channel: OrderChannel::Web,
        amount_cents: 10_00,
        payment_method_state: PaymentMethodState::Valid,
        phase: TrafficPhase::Warmup,
    };
    let mut invalid = valid.clone();
    invalid.order_id = "validation-invalid-order".to_string();
    invalid.customer_id = "validation-invalid-customer".to_string();
    invalid.payment_method_state = PaymentMethodState::InvalidNumber;

    support::flow::assemble_flow(
        vec![valid, invalid],
        Vec::new(),
        support::gateway::GatewayTransform::default(),
        1_000.0,
        journal_root,
    )
}

fn only_run(root: &Path) -> PathBuf {
    let runs: Vec<_> = std::fs::read_dir(root.join("flows"))
        .expect("flow journal directory should exist")
        .map(|entry| entry.expect("run entry should be readable").path())
        .filter(|path| path.is_dir())
        .collect();
    assert_eq!(runs.len(), 1, "the witness root should contain one run");
    runs.into_iter().next().unwrap()
}

fn export_run(run: &Path, output: &Path) -> Vec<serde_json::Value> {
    obzenflow_infra::journal::disk::inspect::export_jsonl(run, Some(output))
        .expect("validation journal should export");
    std::fs::read_to_string(output)
        .expect("export should be readable")
        .lines()
        .map(|line| serde_json::from_str(line).expect("export row should be valid JSON"))
        .collect()
}

fn data_event_type(row: &serde_json::Value) -> Option<&str> {
    (row.pointer("/envelope/provenance/event/event_kind")?
        .as_str()?
        == "fact")
        .then(|| {
            row.pointer("/envelope/provenance/event/event_type")?
                .as_str()
        })
        .flatten()
}

fn order_id(row: &serde_json::Value) -> Option<&str> {
    row.pointer("/payload/order_id")?.as_str()
}

#[test]
fn payment_gateway_validation_journal_contains_only_flat_declared_facts() {
    let root = tempfile::tempdir().expect("validation journal root");
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_validation_journal_test"])
        .run_blocking(build_validation_journal_flow(root.path().to_path_buf()))
        .expect("validation journal flow should complete");

    let rows = export_run(
        &only_run(root.path()),
        &root.path().join("validation.jsonl"),
    );
    let data_rows: Vec<_> = rows
        .iter()
        .filter(|row| data_event_type(row).is_some())
        .collect();

    let valid_facts: Vec<_> = data_rows
        .iter()
        .filter(|row| order_id(row) == Some("validation-valid-order"))
        .filter_map(|row| data_event_type(row))
        .filter(|event_type| *event_type == "payment.order_validated.v1")
        .collect();
    assert_eq!(valid_facts, ["payment.order_validated.v1"]);

    let invalid_validation_facts: Vec<_> = data_rows
        .iter()
        .filter(|row| order_id(row) == Some("validation-invalid-order"))
        .filter_map(|row| data_event_type(row))
        .filter(|event_type| matches!(*event_type, "order.invalid.v1" | "order.cancelled.v1"))
        .collect();
    assert_eq!(
        invalid_validation_facts,
        ["order.invalid.v1", "order.cancelled.v1"],
        "the product variant must commit its fields in declaration order"
    );

    let distinct_data_types: BTreeSet<_> = data_rows
        .iter()
        .filter_map(|row| data_event_type(row))
        .collect();
    assert_eq!(
        distinct_data_types,
        BTreeSet::from([
            "commerce.customer_order_placed.v1",
            "order.cancelled.v1",
            "order.invalid.v1",
            "payment.authorized.v1",
            "payment.order_validated.v1",
        ]),
        "no carrier, completion receipt, or declared-set wrapper may become an event"
    );

    for row in data_rows.iter().filter(|row| {
        matches!(
            data_event_type(row),
            Some("payment.order_validated.v1" | "order.invalid.v1" | "order.cancelled.v1")
        )
    }) {
        assert!(
            row.pointer("/envelope/provenance/event/effect_provenance")
                .is_none_or(serde_json::Value::is_null),
            "pure validation facts must use ordinary derived-event identity"
        );
        assert!(
            row.pointer("/envelope/provenance/event/causality/parent_ids")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|parents| !parents.is_empty()),
            "pure validation facts retain their input parent"
        );
    }

    let authorized = data_rows
        .iter()
        .find(|row| data_event_type(row) == Some("payment.authorized.v1"))
        .expect("the valid order should be authorized");
    assert_eq!(
        authorized
            .pointer("/envelope/provenance/event/effect_provenance/descriptor/effect_type")
            .and_then(serde_json::Value::as_str),
        Some("payment.authorize")
    );
    assert_eq!(
        authorized
            .pointer("/envelope/provenance/event/effect_provenance/outcome_fact_ordinal")
            .and_then(serde_json::Value::as_u64),
        Some(0)
    );

    let deliveries: Vec<_> = rows
        .iter()
        .filter(|row| {
            row.pointer("/envelope/provenance/event/event_kind")
                .and_then(serde_json::Value::as_str)
                == Some("delivery")
        })
        .collect();
    assert_eq!(deliveries.len(), 2);
    let destinations: BTreeSet<_> = deliveries
        .iter()
        .map(|row| {
            row.pointer("/payload/destination")
                .and_then(serde_json::Value::as_str)
                .expect("delivery receipt names its destination")
        })
        .collect();
    assert_eq!(
        destinations,
        BTreeSet::from(["cancelled_orders", "paid_orders"])
    );

    // Both the closure sink and the named shipping sink deliver one item
    // without measuring its byte count. Verify the persisted fields.
    for delivery in deliveries {
        assert_eq!(
            delivery.pointer("/payload/items_delivered"),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            delivery.pointer("/payload/bytes_processed"),
            Some(&serde_json::Value::Null)
        );
    }
}

// Runtime adapter regression belongs to framework integration coverage.
mod shipping_adapter {

    use crate::support::deliveries::ShippingHandoff;
    use crate::support::domain::{PaymentAuthorized, TrafficPhase};
    use obzenflow::schema::{StageId, TypedPayload, WriterId};
    use obzenflow::stages::sinks::{DeliveryMethod, DeliveryResult};
    use obzenflow_core::event::ChainEventFactory;
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
