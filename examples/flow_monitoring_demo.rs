//! Example demonstrating FLOWIP-055: Flow as a Monitored Resource
//!
//! This shows how flows can have their own middleware for monitoring,
//! separate from stage monitoring.

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
    saafe::SAAFE,
};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::Result;
use async_trait::async_trait;

/// Source that generates events
struct EventGenerator {
    max_events: u64,
    current_event: usize,
    writer_id: WriterId,
}

impl EventGenerator {
    fn new(max_events: u64) -> Self {
        Self {
            max_events,
            current_event: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for EventGenerator {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_event < self.max_events as usize {
            let event_id = self.current_event;
            self.current_event += 1;
            
            println!("🔄 Generating event #{}", event_id);
            
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "generated",
                json!({
                    "id": event_id,
                    "timestamp": format!("2024-01-01T00:00:{:02}Z", event_id),
                })
            ))
        } else {
            println!("✅ Event generation complete! {} events", self.max_events);
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        self.current_event >= self.max_events as usize
    }
}

/// Processing stage
struct DataProcessor;

impl DataProcessor {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for DataProcessor {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["processed"] = json!(true);
        event.payload["process_time"] = json!(format!("2024-01-01T00:00:{:02}Z", 
            event.payload["id"].as_u64().unwrap_or(0) + 10));
        vec![event]
    }
}

/// Sink that consumes events
struct EventLogger {
    received: Arc<AtomicU64>,
}

impl EventLogger {
    fn new() -> Self {
        Self {
            received: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl SinkHandler for EventLogger {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        let count = self.received.fetch_add(1, Ordering::Relaxed);
        println!("📝 Event #{}: {:?}", count, event.payload);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for better error messages
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,flow_monitoring_demo=debug")
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    println!("🚀 FLOWIP-055 Demo: Flow-level Monitoring");
    println!("=========================================");
    println!();

    // Create stage instances
    let max_events = 5;
    let generator = EventGenerator::new(max_events);
    let processor = DataProcessor::new();
    let logger = EventLogger::new();

    println!("⏳ Initializing pipeline...");

    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/flow-monitoring-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "flow_monitoring").await?);

    // Create flow with flow-level monitoring middleware
    // This demonstrates the new FLOWIP-055 syntax
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],  // Flow-level monitoring
        
        stages: {
            src = source!("source" => generator, [RED::monitoring()]);      // Stage monitoring
            proc = transform!("processor" => processor, [USE::monitoring()]); // Stage monitoring
            log = sink!("sink" => logger, [RED::monitoring()]);              // Stage monitoring
        },
        
        topology: {
            src |> proc;
            proc |> log;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow with DSL: {:?}", e))?;

    println!();
    println!("📊 Flow Monitoring Configuration:");
    println!("  - Flow: GoldenSignals (latency, traffic, errors, saturation)");
    println!("  - Source Stage: RED (rate, errors, duration)");
    println!("  - Processor Stage: USE (utilization, saturation, errors)");
    println!("  - Sink Stage: RED (rate, errors, duration)");
    println!();
    println!("This demonstrates how flow-level metrics are independent from stage metrics!");
    println!();

    println!("📌 Pipeline created, starting execution...");

    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    println!();
    println!("✅ Flow completed successfully!");
    println!();
    println!("In a production system, flow-level metrics would show:");
    println!("  - End-to-end latency (not sum of stage latencies)");
    println!("  - Flow success rate");
    println!("  - Business-level SLAs");
    println!();
    
    // Cleanup
    println!("Journal written to: target/flow-monitoring-logs/flow_monitoring.log");

    Ok(())
}