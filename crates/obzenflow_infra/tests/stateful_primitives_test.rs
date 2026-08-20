// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::ChainEvent, event::payloads::delivery_payload::DeliveryMethod,
};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::stateful::strategies::accumulators::{
    ConflateTyped, GroupByTyped, ReduceTyped,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payload for the stateful-primitives test. The JSON shape
/// matches what `TransactionSource` emits; the type fingerprints the
/// stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TransactionEvent {
    product_id: String,
    quantity: u64,
    revenue: f64,
}

impl TypedPayload for TransactionEvent {
    const EVENT_TYPE: &'static str = "stateful_primitives.transaction";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ProductStats {
    quantity_sold: u64,
    revenue: f64,
    transaction_count: u64,
}

impl TypedPayload for ProductStats {
    const EVENT_TYPE: &'static str = "stateful_primitives.product_stats";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProductStatsUpdate {
    key: String,
    result: ProductStats,
}

impl TypedPayload for ProductStatsUpdate {
    const EVENT_TYPE: &'static str = "stateful_primitives.product_stats_update";
}

#[derive(Clone, Debug)]
struct TransactionSource {
    count: usize,
}

impl TransactionSource {
    fn new(count: usize) -> Self {
        Self { count }
    }
}

impl TypedFiniteSourceHandler for TransactionSource {
    type Output = TransactionEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
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

            Ok(Some(vec![TransactionEvent {
                product_id: product.to_string(),
                quantity: quantity as u64,
                revenue,
            }]))
        }
    }
}

#[derive(Debug)]
struct CollectingSink<T> {
    events: Arc<Mutex<Vec<ChainEvent>>>,
    _input: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for CollectingSink<T> {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> CollectingSink<T> {
    fn new(events: Arc<Mutex<Vec<ChainEvent>>>) -> Self {
        Self {
            events,
            _input: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> InlineSink for CollectingSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        input: T,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.events
            .lock()
            .unwrap()
            .push(input.to_event(WriterId::from(StageId::new())));
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("collect".to_string()),
            None,
        )))
    }
}

#[tokio::test]
async fn groupby_with_on_eof_emits_one_aggregate_per_key() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let transaction_source = TransactionSource::new(10);
        let sales_by_product = GroupByTyped::new(
            |event: &TransactionEvent| event.product_id.clone(),
            |stats: &mut ProductStats, event: &TransactionEvent| {
                stats.quantity_sold += event.quantity;
                stats.revenue += event.revenue;
                stats.transaction_count += 1;
            },
            |key: &String, result: &ProductStats| ProductStatsUpdate {
                key: key.clone(),
                result: result.clone(),
            },
        )
        .emit_on_eof();
        let collecting_sink = CollectingSink::<ProductStatsUpdate>::new(events_for_flow);

        Ok(flow! {
            name: "stateful_primitives_groupby_test",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_groupby")),

            stages: {
                src = source!(TransactionEvent => transaction_source);
                sales_by_product = stateful!(TransactionEvent -> ProductStatsUpdate => sales_by_product);
                sink = sink!(ProductStatsUpdate => collecting_sink);
            },

            topology: {
                src |> sales_by_product;
                sales_by_product |> sink;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow should complete");

    let results = events.lock().unwrap();
    let aggregates: Vec<_> = results
        .iter()
        .filter(|e| e.event_type() == ProductStatsUpdate::versioned_event_type())
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

impl TypedPayload for TotalStats {
    const EVENT_TYPE: &'static str = "stateful_primitives.total_stats";
}

#[tokio::test]
async fn reduce_with_on_eof_emits_single_total() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let transaction_source = TransactionSource::new(5);
        let totals = ReduceTyped::new(
            TotalStats {
                total_revenue: 0.0,
                total_transactions: 0,
                total_quantity: 0,
            },
            |stats: &mut TotalStats, event: &TransactionEvent| {
                stats.total_revenue += event.revenue;
                stats.total_quantity += event.quantity;
                stats.total_transactions += 1;
            },
        )
        .emit_on_eof();
        let collecting_sink = CollectingSink::<TotalStats>::new(events_for_flow);

        Ok(flow! {
            name: "stateful_primitives_reduce_test",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_reduce")),

            stages: {
                src = source!(TransactionEvent => transaction_source);
                totals = stateful!(TransactionEvent -> TotalStats => totals);
                sink = sink!(TotalStats => collecting_sink);
            },

            topology: {
                src |> totals;
                totals |> sink;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow should complete");

    let results = events.lock().unwrap();
    let reduced: Vec<_> = results
        .iter()
        .filter(|e| e.event_type() == TotalStats::versioned_event_type())
        .collect();
    assert_eq!(reduced.len(), 1);
}

#[tokio::test]
async fn conflate_emits_latest_value_per_key() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let transaction_source = TransactionSource::new(8);
        let latest_by_product =
            ConflateTyped::new(|event: &TransactionEvent| event.product_id.clone())
                .emit_within(Duration::from_millis(1));
        let collecting_sink = CollectingSink::<TransactionEvent>::new(events_for_flow);

        Ok(flow! {
            name: "stateful_primitives_conflate_test",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_primitives_test_conflate")),

            stages: {
                src = source!(TransactionEvent => transaction_source);
                latest_by_product = stateful!(TransactionEvent -> TransactionEvent => latest_by_product);
                sink = sink!(TransactionEvent => collecting_sink);
            },

            topology: {
                src |> latest_by_product;
                latest_by_product |> sink;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow should complete");

    let results = events.lock().unwrap();
    assert!(!results.is_empty());
}
