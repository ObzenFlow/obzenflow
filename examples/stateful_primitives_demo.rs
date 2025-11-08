// FLOWIP-080c: Stateful Primitives Demo
//
// This example demonstrates all the composable stateful primitives introduced in FLOWIP-080c:
// - Accumulators: GroupBy, Reduce, Conflate
// - Emission strategies: OnEOF, EveryN, TimeWindow, EmitAlways
//
// Run with different patterns:
// cargo run --example stateful_primitives_demo                    # Default: GroupBy + OnEOF
// PATTERN=reduce cargo run --example stateful_primitives_demo     # Reduce + EveryN
// PATTERN=conflate cargo run --example stateful_primitives_demo   # Conflate + TimeWindow
// PATTERN=realtime cargo run --example stateful_primitives_demo   # GroupBy + EmitAlways

use obzenflow_dsl_infra::{flow, source, transform, stateful, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
    Result as CoreResult,
};
use obzenflow_runtime_services::stages::stateful::{GroupBy, Reduce, Conflate, OnEOF, EveryN, TimeWindow, EmitAlways};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ProductStats {
    quantity_sold: u64,
    revenue: f64,
    transaction_count: u64,
}

// Source that generates e-commerce transaction events
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
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count == 0 {
            return None;
        }
        self.count -= 1;

        // Generate realistic transaction data
        let products = vec!["laptop", "phone", "tablet", "headphones", "keyboard"];
        let product = products[self.count % products.len()];
        let quantity = (self.count % 3) + 1;
        let base_price = match product {
            "laptop" => 999.99,
            "phone" => 699.99,
            "tablet" => 499.99,
            "headphones" => 199.99,
            "keyboard" => 79.99,
            _ => 99.99,
        };
        let revenue = base_price * quantity as f64;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "transaction",
            json!({
                "product_id": product,
                "quantity": quantity,
                "revenue": revenue,
                "customer_id": format!("customer_{}", self.count % 10),
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count == 0
    }
}

// Simple sink that collects and displays results
#[derive(Clone, Debug)]
struct ResultCollector {
    name: String,
    results: Arc<Mutex<Vec<ChainEvent>>>,
}

impl ResultCollector {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_results(&self) -> Vec<ChainEvent> {
        self.results.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl SinkHandler for ResultCollector {
    async fn consume(&mut self, event: ChainEvent) -> CoreResult<DeliveryPayload> {
        println!("📊 [{}] Received result: {}", self.name,
            serde_json::to_string_pretty(&event.payload()).unwrap_or_default());

        self.results.lock().unwrap().push(event.clone());

        Ok(DeliveryPayload::success(
            self.name.clone(),
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

async fn demo_groupby_oneof() -> Result<()> {
    println!("\n🔷 Demo 1: GroupBy + OnEOF");
    println!("  Groups transactions by product and emits totals at completion");
    println!("  Use case: End-of-day sales reports\n");

    FlowApplication::run(async {
        flow! {
            name: "groupby_oneof_demo",
            journals: disk_journals(std::path::PathBuf::from("target/stateful-primitives-logs/groupby")),
            middleware: [],

            stages: {
                transactions = source!("transactions" => TransactionSource::new(15));

                // ✨ FLOWIP-080c: GroupBy accumulator with OnEOF emission
                sales_by_product = stateful!("sales_aggregator" =>
                    GroupBy::new("product_id", |event: &ChainEvent, stats: &mut ProductStats| {
                        stats.quantity_sold += event.payload()["quantity"].as_u64().unwrap_or(0);
                        stats.revenue += event.payload()["revenue"].as_f64().unwrap_or(0.0);
                        stats.transaction_count += 1;
                    })
                    .emit_on_eof()  // Only emit when all transactions are processed
                );

                results = sink!("results" => ResultCollector::new("GroupBy+OnEOF"));
            },

            topology: {
                transactions |> sales_by_product;
                sales_by_product |> results;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    Ok(())
}

async fn demo_reduce_everyn() -> Result<()> {
    println!("\n🔷 Demo 2: Reduce + EveryN");
    println!("  Aggregates all transactions into running totals, emits every 5 events");
    println!("  Use case: Real-time dashboard updates with periodic snapshots\n");

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TotalStats {
        total_revenue: f64,
        total_transactions: u64,
        total_quantity: u64,
    }

    FlowApplication::run(async {
        flow! {
            name: "reduce_everyn_demo",
            journals: disk_journals(std::path::PathBuf::from("target/stateful-primitives-logs/reduce")),
            middleware: [],

            stages: {
                transactions = source!("transactions" => TransactionSource::new(20));

                // ✨ FLOWIP-080c: Reduce accumulator with EveryN emission
                running_totals = stateful!("totals" =>
                    Reduce::new(
                        TotalStats { total_revenue: 0.0, total_transactions: 0, total_quantity: 0 },
                        |stats: &mut TotalStats, event: &ChainEvent| {
                            stats.total_revenue += event.payload()["revenue"].as_f64().unwrap_or(0.0);
                            stats.total_quantity += event.payload()["quantity"].as_u64().unwrap_or(0);
                            stats.total_transactions += 1;
                        }
                    )
                    .emit_every_n(5)  // Emit progress every 5 transactions
                );

                results = sink!("results" => ResultCollector::new("Reduce+EveryN"));
            },

            topology: {
                transactions |> running_totals;
                running_totals |> results;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    Ok(())
}

async fn demo_conflate_timewindow() -> Result<()> {
    println!("\n🔷 Demo 3: Conflate + TimeWindow");
    println!("  Keeps only latest transaction per product, emits snapshots every 100ms");
    println!("  Use case: Current state materialized views with periodic updates\n");

    FlowApplication::run(async {
        flow! {
            name: "conflate_timewindow_demo",
            journals: disk_journals(std::path::PathBuf::from("target/stateful-primitives-logs/conflate")),
            middleware: [],

            stages: {
                transactions = source!("transactions" => TransactionSource::new(30));

                // ✨ FLOWIP-080c: Conflate accumulator with TimeWindow emission
                latest_by_product = stateful!("latest_snapshot" =>
                    Conflate::new("product_id")
                        .emit_within(Duration::from_millis(100))  // Snapshot every 100ms
                );

                results = sink!("results" => ResultCollector::new("Conflate+TimeWindow"));
            },

            topology: {
                transactions |> latest_by_product;
                latest_by_product |> results;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    Ok(())
}

async fn demo_groupby_emitalways() -> Result<()> {
    println!("\n🔷 Demo 4: GroupBy + EmitAlways");
    println!("  Groups by product and emits updated totals after every transaction");
    println!("  Use case: Real-time dashboards with immediate updates\n");

    FlowApplication::run(async {
        flow! {
            name: "groupby_emitalways_demo",
            journals: disk_journals(std::path::PathBuf::from("target/stateful-primitives-logs/realtime")),
            middleware: [],

            stages: {
                transactions = source!("transactions" => TransactionSource::new(10));

                // ✨ FLOWIP-080c: GroupBy accumulator with EmitAlways strategy
                realtime_sales = stateful!("realtime_aggregator" =>
                    GroupBy::new("product_id", |event: &ChainEvent, stats: &mut ProductStats| {
                        stats.quantity_sold += event.payload()["quantity"].as_u64().unwrap_or(0);
                        stats.revenue += event.payload()["revenue"].as_f64().unwrap_or(0.0);
                        stats.transaction_count += 1;
                    })
                    .emit_always()  // Emit updated totals after every event
                );

                results = sink!("results" => ResultCollector::new("GroupBy+EmitAlways"));
            },

            topology: {
                transactions |> realtime_sales;
                realtime_sales |> results;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment variable for metrics
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("===========================================");
    println!("   FLOWIP-080c: Stateful Primitives Demo");
    println!("===========================================");
    println!();
    println!("This demo showcases composable stateful transform primitives:");
    println!("• Accumulators: GroupBy, Reduce, Conflate");
    println!("• Emission strategies: OnEOF, EveryN, TimeWindow, EmitAlways");
    println!();

    let pattern = std::env::var("PATTERN").unwrap_or_else(|_| "groupby".to_string());

    match pattern.as_str() {
        "groupby" => demo_groupby_oneof().await?,
        "reduce" => demo_reduce_everyn().await?,
        "conflate" => demo_conflate_timewindow().await?,
        "realtime" => demo_groupby_emitalways().await?,
        "all" => {
            demo_groupby_oneof().await?;
            demo_reduce_everyn().await?;
            demo_conflate_timewindow().await?;
            demo_groupby_emitalways().await?;
        }
        _ => {
            println!("Unknown pattern: {}", pattern);
            println!("Available patterns:");
            println!("  groupby   - GroupBy + OnEOF (default)");
            println!("  reduce    - Reduce + EveryN");
            println!("  conflate  - Conflate + TimeWindow");
            println!("  realtime  - GroupBy + EmitAlways");
            println!("  all       - Run all demos");
        }
    }

    println!("\n✅ Demo completed successfully!");
    println!("\n📝 Key Takeaways:");
    println!("  • FLOWIP-080c eliminates manual state management");
    println!("  • Accumulators define WHAT to compute");
    println!("  • Emission strategies define WHEN to emit");
    println!("  • Any accumulator works with any emission strategy");
    println!("  • ~8 lines of code vs ~30 lines with manual StatefulHandler");

    Ok(())
}