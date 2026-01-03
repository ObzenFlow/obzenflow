//! E-commerce Top Products Demo - Using FLOWIP-080j & FLOWIP-082a
//!
//! Demonstrates TopNByTyped for tracking best-selling products by total revenue,
//! accumulating multiple orders for the same product throughout the day.
//!
//! Run with: cargo run --package obzenflow --example ecommerce_top_products

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::{
    event::chain_event::ChainEvent,
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    TypedPayload,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::TopNByTyped;
use serde::{Deserialize, Serialize};

// FLOWIP-082a: Strongly-typed event with schema version
#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrderEvent {
    order_id: String,
    product_id: String,
    product_name: String,
    category: String,
    unit_price: f64,
    quantity: u32,
    total_value: f64,
    timestamp: usize,
}

impl TypedPayload for OrderEvent {
    const EVENT_TYPE: &'static str = "ecommerce.order";
    const SCHEMA_VERSION: u32 = 1;
}

/// Sink that displays top-selling products dashboard
#[derive(Clone, Debug)]
struct SalesDashboard;

impl SalesDashboard {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for SalesDashboard {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        // ✨ FLOWIP-082a: TopNByTyped emits events with input type's EVENT_TYPE
        if event.event_type() == OrderEvent::EVENT_TYPE {
            let payload = event.payload();
            let top_n = payload["top_n"].as_array().unwrap();
            let total_items = payload["total_items"].as_u64().unwrap();

            println!("\n📊 TOP SELLING PRODUCTS DASHBOARD 📊");
            println!("====================================");
            println!("Total Unique Products Sold: {}\n", total_items);

            let mut total_revenue = 0.0;

            for entry in top_n.iter() {
                let rank = entry["rank"].as_u64().unwrap();
                let product_id = entry["key"].as_str().unwrap();
                let total_value = entry["total_score"].as_f64().unwrap();
                let order_count = entry["count"].as_u64().unwrap();
                let metadata = &entry["metadata"];
                let product_name = metadata["product_name"].as_str().unwrap_or("Unknown");
                let category = metadata["category"].as_str().unwrap_or("Unknown");

                total_revenue += total_value;

                let medal = match rank {
                    1 => "🥇",
                    2 => "🥈",
                    3 => "🥉",
                    _ => "  ",
                };

                println!("{} #{}: {} ({})", medal, rank, product_name, product_id);
                println!("      Category: {}", category);
                println!(
                    "      Revenue: ${:.2} from {} orders",
                    total_value, order_count
                );
                println!(
                    "      Avg Order Value: ${:.2}",
                    total_value / order_count as f64
                );
                println!();
            }

            println!("------------------------------------");
            println!("Top 5 Products Revenue: ${:.2}", total_revenue);
            println!("====================================\n");
        }

        Ok(DeliveryPayload::success(
            "dashboard",
            DeliveryMethod::Custom("Display".to_string()),
            None,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🛒 FlowState RS - E-commerce Top Products Analytics");
    println!("===================================================");
    println!("✨ Using FLOWIP-080j TopNByTyped & FLOWIP-082a TypedPayload");
    println!("");
    println!("This demo shows real-time tracking of best-selling");
    println!("products by total revenue, accumulating multiple");
    println!("orders for the same product throughout the day.\n");

    println!("Processing order stream...\n");

    // Simulate a day of orders (some products appear multiple times)
    let orders: Vec<(String, String, f64, u32, String)> = vec![
        (
            "LAPTOP-001".to_string(),
            "Gaming Laptop Pro".to_string(),
            1299.99,
            1,
            "Electronics".to_string(),
        ),
        (
            "PHONE-005".to_string(),
            "Smartphone X".to_string(),
            899.99,
            2,
            "Electronics".to_string(),
        ),
        (
            "BOOK-101".to_string(),
            "Rust Programming".to_string(),
            45.99,
            3,
            "Books".to_string(),
        ),
        (
            "LAPTOP-001".to_string(),
            "Gaming Laptop Pro".to_string(),
            1299.99,
            2,
            "Electronics".to_string(),
        ), // Another laptop sale!
        (
            "CHAIR-020".to_string(),
            "Ergonomic Office Chair".to_string(),
            299.99,
            1,
            "Furniture".to_string(),
        ),
        (
            "PHONE-005".to_string(),
            "Smartphone X".to_string(),
            899.99,
            1,
            "Electronics".to_string(),
        ), // More phones
        (
            "MOUSE-015".to_string(),
            "Wireless Mouse".to_string(),
            29.99,
            5,
            "Electronics".to_string(),
        ),
        (
            "LAPTOP-002".to_string(),
            "Business Laptop".to_string(),
            999.99,
            1,
            "Electronics".to_string(),
        ),
        (
            "BOOK-101".to_string(),
            "Rust Programming".to_string(),
            45.99,
            2,
            "Books".to_string(),
        ), // Book is popular!
        (
            "KEYBOARD-010".to_string(),
            "Mechanical Keyboard".to_string(),
            149.99,
            3,
            "Electronics".to_string(),
        ),
        (
            "PHONE-005".to_string(),
            "Smartphone X".to_string(),
            899.99,
            3,
            "Electronics".to_string(),
        ), // Phone bestseller
        (
            "DESK-030".to_string(),
            "Standing Desk".to_string(),
            599.99,
            1,
            "Furniture".to_string(),
        ),
        (
            "LAPTOP-001".to_string(),
            "Gaming Laptop Pro".to_string(),
            1299.99,
            1,
            "Electronics".to_string(),
        ), // Third laptop sale
        (
            "MONITOR-008".to_string(),
            "4K Monitor".to_string(),
            399.99,
            2,
            "Electronics".to_string(),
        ),
        (
            "BOOK-102".to_string(),
            "Data Science Handbook".to_string(),
            55.99,
            1,
            "Books".to_string(),
        ),
        (
            "PHONE-006".to_string(),
            "Budget Phone".to_string(),
            299.99,
            4,
            "Electronics".to_string(),
        ),
        (
            "LAPTOP-001".to_string(),
            "Gaming Laptop Pro".to_string(),
            1299.99,
            1,
            "Electronics".to_string(),
        ), // Fourth sale!
        (
            "CHAIR-020".to_string(),
            "Ergonomic Office Chair".to_string(),
            299.99,
            2,
            "Furniture".to_string(),
        ),
        (
            "HEADPHONES-012".to_string(),
            "Noise-Cancel Headphones".to_string(),
            249.99,
            2,
            "Electronics".to_string(),
        ),
        (
            "BOOK-101".to_string(),
            "Rust Programming".to_string(),
            45.99,
            5,
            "Books".to_string(),
        ), // Bulk order!
    ];

    FlowApplication::run(async move {
        flow! {
            name: "ecommerce_analytics",
            journals: disk_journals(std::path::PathBuf::from("target/ecommerce-logs")),
            middleware: [],

            stages: {
                // FLOWIP-081: Typed finite sources (no WriterId/ChainEvent boilerplate)
                orders = source!("orders" => FiniteSourceTyped::from_item_fn(move |index| {
                    let (product_id, product_name, unit_price, quantity, category) =
                        orders.get(index)?;
                    let order_number = index + 1;
                    let total_value = unit_price * (*quantity as f64);

                    println!(
                        "📦 Order #{}: {} x{} ({}) = ${:.2}",
                        order_number, product_name, quantity, product_id, total_value
                    );

                    Some(OrderEvent {
                        order_id: format!("ORD-{:04}", order_number),
                        product_id: product_id.clone(),
                        product_name: product_name.clone(),
                        category: category.clone(),
                        unit_price: *unit_price,
                        quantity: *quantity,
                        total_value,
                        timestamp: order_number, // Simulated timestamp
                    })
                }));

                // FLOWIP-080j: TopNByTyped - Type-safe accumulation with no ChainEvent!
                // Type-safe extraction functions instead of string field names
                top_products = stateful!("top_products" =>
                    TopNByTyped::new(
                        5,
                        |order: &OrderEvent| order.product_id.clone(),  // Key extractor
                        |order: &OrderEvent| order.total_value          // Score extractor
                    ).emit_every_n(5),
                    [rate_limit(3.0)]   // Process max 3 orders per second for demo visibility
                );

                dashboard = sink!("dashboard" => SalesDashboard::new());
            },

            topology: {
                orders |> top_products;
                top_products |> dashboard;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("✅ E-commerce analytics completed!");
    println!("\n💡 Key Insights:");
    println!("   FLOWIP-082a TypedPayload:");
    println!("   • OrderEvent::EVENT_TYPE instead of \"order.placed\"");
    println!("   • SCHEMA_VERSION for evolution tracking");
    println!("   • Strongly-typed event structs");
    println!("");
    println!("   FLOWIP-080j TopNByTyped:");
    println!("   • Type-safe key and score extraction");
    println!("   • No ChainEvent manipulation - work with OrderEvent directly");
    println!("   • Compile-time safety for field access");
    println!("   • Memory bounded to N items regardless of stream size");
    println!("\n📝 Journal written to: target/ecommerce-logs/");

    Ok(())
}
