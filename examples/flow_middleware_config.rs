//! Demonstrates flow-level middleware configuration and inheritance
//!
//! This example shows:
//! - Flow-level rate limit of 0.5 events/sec
//! - Source overrides with 50 events/sec (100x flow rate)
//! - Transform inherits flow-level rate limit
//! - Result: source produces fast but transform is throttled

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use async_trait::async_trait;
use serde_json::json;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;
use anyhow::Result;

#[derive(Debug, Clone)]
struct EventCounter {
    count: usize,
    max_count: usize,
    writer_id: WriterId,
}

impl EventCounter {
    fn new(max_count: usize) -> Self {
        Self {
            count: 0,
            max_count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for EventCounter {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.max_count {
            return None;
        }
        
        self.count += 1;
        
        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "counter.event",
            json!({
                "count": self.count,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            }),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.count >= self.max_count
    }
}

#[derive(Debug, Clone)]
struct PassthroughTransform {
    name: String,
    count: Arc<AtomicU64>,
}

impl PassthroughTransform {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl TransformHandler for PassthroughTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        
        // Log every 10th event to reduce noise
        if count % 10 == 0 {
            info!("{} processed {} events", self.name, count);
        }
        
        vec![event]
    }
    
    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct EventSink {
    name: String,
    count: Arc<AtomicU64>,
    first_event_time: Option<Instant>,
    last_event_time: Option<Instant>,
}

impl EventSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: Arc::new(AtomicU64::new(0)),
            first_event_time: None,
            last_event_time: None,
        }
    }
}

#[async_trait]
impl SinkHandler for EventSink {
    async fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        let now = Instant::now();
        
        if self.first_event_time.is_none() {
            self.first_event_time = Some(now);
            info!("{} received first event", self.name);
        }
        self.last_event_time = Some(now);
        
        // Log every 10th event
        if count % 10 == 0 {
            if let Some(first) = self.first_event_time {
                let elapsed = now.duration_since(first).as_secs_f64();
                let rate = count as f64 / elapsed;
                info!("{} received {} events, rate: {:.2} events/sec", 
                    self.name, count, rate);
            }
        }
        
        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter for nice summaries
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,flow_middleware_config=debug,obzenflow_adapters::middleware::rate_limiter=trace")
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("=== Flow-Level Middleware Configuration Demo ===");
    info!("");
    info!("This demo shows middleware inheritance:");
    info!("- Flow has rate limit of 0.5 events/sec");
    info!("- Source overrides with 50 events/sec (100x faster)");
    info!("- Transform inherits flow rate limit");
    info!("- Result: source produces fast, transform throttles to 0.5/sec");
    info!("");

    // Create journal path
    let journal_path = std::path::PathBuf::from("target/flow_middleware_config_journal");
    info!("Using DiskJournal at: {}", journal_path.display());

    // Create the flow with ACTUAL flow-level middleware inheritance
    let flow = flow! {
        name: "middleware-config-demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [
            // Flow-level rate limit: 0.5 events/sec
            rate_limit(1.0)
        ],
        
        stages: {
            // Source with override: 50 events/sec (100x flow rate)
            src = source!("fast-source" => EventCounter::new(120), [
                rate_limit(10.0)
            ]);
            
            // Transform inherits flow-level rate limit (0.5 events/sec)
            trans = transform!("slow-transform" => PassthroughTransform::new("transform"));
            
            // Sink
            snk = sink!("event-sink" => EventSink::new("sink"));
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    info!("Starting flow...");
    let start_time = Instant::now();
    
    // Run the flow to completion
    let metrics_exporter = flow.run_with_metrics().await?;
    
    let elapsed = start_time.elapsed();
    info!("");
    info!("=== Results ===");
    info!("Total runtime: {:.1}s", elapsed.as_secs_f64());
    
    // Get metrics summary if available
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        info!("\n{}", summary);
    }
    
    info!("");
    info!("Expected behavior:");
    info!("- Source emits at 10 events/sec (overrides flow rate)");
    info!("- Transform processes at 1.0 events/sec (inherits flow rate)");
    info!("- Backpressure naturally flows upstream");
    info!("");
    info!("This demonstrates that:");
    info!("1. Stage-level middleware overrides flow-level (source at 10/sec)");
    info!("2. Stages without overrides inherit flow-level (transform at 1.0/sec)");
    info!("3. Middleware inheritance works as designed!");

    Ok(())
}