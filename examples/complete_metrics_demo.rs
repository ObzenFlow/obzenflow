//! Complete metrics demonstration showing all metric types
//! Run with: cargo run -p obzenflow --example complete_metrics_demo

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
    ProcessingOutcome,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Source that generates events with correlation IDs
struct CorrelatedSource {
    count: usize,
    writer_id: WriterId,
}

impl CorrelatedSource {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for CorrelatedSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 50 {
            return None;
        }
        
        self.count += 1;
        
        // Create event with correlation ID
        let mut event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "order.created",
            json!({
                "order_id": self.count,
                "amount": 100.0 + (self.count as f64),
                "should_fail": self.count % 7 == 0,  // ~14% failure rate
            }),
        );
        
        // Add correlation ID for flow tracking
        event.correlation_id = Some(obzenflow_core::correlation::CorrelationId::new());
        
        Some(event)
    }
    
    fn is_complete(&self) -> bool {
        self.count >= 50
    }
}

/// Transform that properly sets ProcessingOutcome
struct OrderValidator;

impl TransformHandler for OrderValidator {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate processing time
        std::thread::sleep(Duration::from_millis(10));
        
        // Check if we should fail this order
        if event.payload["should_fail"].as_bool().unwrap_or(false) {
            // Set proper error outcome
            event.processing_info.outcome = ProcessingOutcome::Error("Order validation failed".to_string());
            event.event_type = "order.failed".to_string();
            vec![event]
        } else {
            // Set success outcome with processing time
            event.processing_info.outcome = ProcessingOutcome::Success;
            event.processing_info.processing_time_ms = 10;
            event.event_type = "order.validated".to_string();
            vec![event]
        }
    }
}

/// Sink that tracks orders
struct OrderSink {
    count: usize,
}

impl OrderSink {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl SinkHandler for OrderSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        self.count += 1;
        
        // Simulate some processing
        std::thread::sleep(Duration::from_millis(2));
        
        // Note: Sinks should emit control events for proper tracking
        // In a real implementation, the sink supervisor would emit:
        // - control.sink.consumed events
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=info")
        .init();

    println!("🚀 Complete Metrics Demo - All Metric Types");
    println!("==========================================\n");

    // Create journal
    let journal = Arc::new(MemoryJournal::new());

    println!("📊 Creating flow with all metrics types...\n");

    // Create flow
    let flow_handle = flow! {
        name: "order_processing",
        journal: journal,
        middleware: [],  // Global middleware not implemented
        
        stages: {
            // Source with correlation
            orders = source!("order_source" => CorrelatedSource::new());
            
            // Transform with middleware
            validator = transform!("order_validator" => OrderValidator, [
                rate_limit(20.0),  // 20 orders per second
                circuit_breaker(5) // Opens after 5 consecutive failures
            ]);
            
            // Sink
            storage = sink!("order_storage" => OrderSink::new());
        },
        
        topology: {
            orders |> validator;
            validator |> storage;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    println!("▶️  Running flow...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Wait for metrics to settle
    println!("⏳ Waiting for metrics aggregation...");
    sleep(Duration::from_secs(3)).await;
    
    println!("\n📈 Flow completed! Getting metrics...\n");
    
    // Get metrics
    match flow_handle.metrics_exporter().await {
        Some(exporter) => {
            match exporter.render_metrics() {
                Ok(metrics_text) => {
                    println!("{}", "=".repeat(80));
                    println!("COMPLETE PROMETHEUS METRICS OUTPUT:");
                    println!("{}", "=".repeat(80));
                    println!();
                    println!("{}", metrics_text);
                    println!();
                    println!("{}", "=".repeat(80));
                    
                    // Analyze what metrics we got
                    println!("\n📊 Metrics Analysis:");
                    println!("-------------------");
                    
                    // RED Metrics
                    println!("\n✅ RED Metrics (Rate, Errors, Duration):");
                    if metrics_text.contains("obzenflow_events_total") {
                        println!("  ✓ events_total - present");
                    } else {
                        println!("  ✗ events_total - MISSING!");
                    }
                    
                    if metrics_text.contains("obzenflow_errors_total") {
                        println!("  ✓ errors_total - present");
                    } else {
                        println!("  ✗ errors_total - MISSING!");
                    }
                    
                    if metrics_text.contains("obzenflow_duration_seconds") {
                        println!("  ✓ duration_seconds - present");
                    } else {
                        println!("  ✗ duration_seconds - MISSING!");
                    }
                    
                    // Middleware Metrics
                    println!("\n🔧 Middleware Metrics:");
                    if metrics_text.contains("obzenflow_circuit_breaker_state") {
                        println!("  ✓ circuit_breaker_state - present");
                    } else {
                        println!("  ✗ circuit_breaker_state - MISSING!");
                    }
                    
                    if metrics_text.contains("obzenflow_rate_limiter_delay_rate") {
                        println!("  ✓ rate_limiter_delay_rate - present");
                    } else {
                        println!("  ✗ rate_limiter_delay_rate - MISSING!");
                    }
                    
                    // SAAFE Metrics
                    println!("\n📈 SAAFE Metrics:");
                    if metrics_text.contains("obzenflow_saturation_ratio") {
                        println!("  ✓ saturation_ratio - present");
                    } else {
                        println!("  ✗ saturation_ratio - MISSING!");
                    }
                    
                    if metrics_text.contains("obzenflow_anomalies_total") {
                        println!("  ✓ anomalies_total - present");
                    } else {
                        println!("  ✗ anomalies_total - MISSING!");
                    }
                    
                    // Flow Metrics
                    println!("\n🌊 Flow-Level Metrics:");
                    if metrics_text.contains("obzenflow_flow_latency_seconds") {
                        println!("  ✓ flow_latency_seconds - present");
                    } else {
                        println!("  ✗ flow_latency_seconds - MISSING!");
                    }
                    
                    if metrics_text.contains("obzenflow_dropped_events") {
                        println!("  ✓ dropped_events - present");
                    } else {
                        println!("  ✗ dropped_events - MISSING!");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Failed to render metrics: {}", e);
                }
            }
        }
        None => {
            eprintln!("❌ No metrics exporter available!");
        }
    }
    
    Ok(())
}