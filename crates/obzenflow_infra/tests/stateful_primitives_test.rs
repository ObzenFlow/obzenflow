use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime_services::stages::stateful::{Conflate, GroupBy, Reduce};
use obzenflow_runtime_services::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ProductStats {
    quantity_sold: u64,
    revenue: f64,
    transaction_count: u64,
}

#[derive(Clone, Debug)]
struct TransactionSource {
    count: usize,
    writer_id: WriterId,
}

impl TransactionSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TransactionSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.count == 0 {
            Ok(None)
        } else {
            self.count -= 1;

            let products = ["laptop", "phone"];
            let product = products[self.count % products.len()];
            let quantity = (self.count % 3) + 1;
            let base_price = match product {
                "laptop" => 100.0,
                "phone" => 50.0,
                _ => 10.0,
            };
            let revenue = base_price * quantity as f64;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "transaction",
                json!({
                    "product_id": product,
                    "quantity": quantity,
                    "revenue": revenue,
                }),
            )]))
        }
    }
}

#[derive(Clone, Debug)]
struct CollectingSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectingSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl SinkHandler for CollectingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(DeliveryPayload::success(
            "test",
            DeliveryMethod::Custom("collect".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn groupby_with_on_eof_emits_one_aggregate_per_key() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "stateful_primitives_groupby_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_groupby")),
        middleware: [],

        stages: {
            src = source!("transactions" => TransactionSource::new(10));
            sales_by_product = stateful!("sales_aggregator" =>
                GroupBy::new("product_id", |event: &ChainEvent, stats: &mut ProductStats| {
                    stats.quantity_sold += event.payload()["quantity"].as_u64().unwrap_or(0);
                    stats.revenue += event.payload()["revenue"].as_f64().unwrap_or(0.0);
                    stats.transaction_count += 1;
                })
                .emit_on_eof()
            );
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> sales_by_product;
            sales_by_product |> sink;
        }
    })
    .await
    .expect("flow should complete");

    let results = events.lock().unwrap();
    let aggregates: Vec<_> = results
        .iter()
        .filter(|e| e.event_type() == "aggregated")
        .collect();
    // Two product ids -> one aggregate per key.
    assert_eq!(aggregates.len(), 2);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TotalStats {
    total_revenue: f64,
    total_transactions: u64,
    total_quantity: u64,
}

#[tokio::test]
async fn reduce_with_on_eof_emits_single_total() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "stateful_primitives_reduce_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_reduce")),
        middleware: [],

        stages: {
            src = source!("transactions" => TransactionSource::new(5));
            totals = stateful!("totals" =>
                Reduce::new(
                    TotalStats { total_revenue: 0.0, total_transactions: 0, total_quantity: 0 },
                    |stats: &mut TotalStats, event: &ChainEvent| {
                        stats.total_revenue += event.payload()["revenue"].as_f64().unwrap_or(0.0);
                        stats.total_quantity += event.payload()["quantity"].as_u64().unwrap_or(0);
                        stats.total_transactions += 1;
                    }
                )
                .emit_on_eof()
            );
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> totals;
            totals |> sink;
        }
    })
    .await
    .expect("flow should complete");

    let results = events.lock().unwrap();
    let reduced: Vec<_> = results
        .iter()
        .filter(|e| e.event_type() == "reduced")
        .collect();
    assert_eq!(reduced.len(), 1);
}

#[tokio::test]
async fn conflate_emits_latest_value_per_key() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "stateful_primitives_conflate_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_conflate")),
        middleware: [],

        stages: {
            src = source!("transactions" => TransactionSource::new(8));
            latest_by_product = stateful!("latest_snapshot" =>
                Conflate::new("product_id")
                    .emit_within(Duration::from_millis(1))
            );
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> latest_by_product;
            latest_by_product |> sink;
        }
    })
    .await
    .expect("flow should complete");

    let results = events.lock().unwrap();
    assert!(!results.is_empty());
}
