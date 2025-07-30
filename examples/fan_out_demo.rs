//! Demo showing natural fan-out in ObzenFlow
//! 
//! This example demonstrates how multiple downstream stages naturally
//! create their own journal readers with independent positions.
//! No special broadcast primitives needed!

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;
use std::sync::{Arc, Mutex};

/// Source that generates events with unique IDs
#[derive(Clone, Debug)]
struct EventSource {
    count: usize,
    writer_id: WriterId,
}

impl EventSource {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for EventSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 10 {
            return None;
        }

        self.count += 1;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "fan.out.event",
            json!({
                "id": self.count,
                "message": format!("Event #{}", self.count),
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= 10
    }
}

/// Transform that processes events "quickly"
#[derive(Clone, Debug)]
struct FastProcessor {
    name: String,
}

impl FastProcessor {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl TransformHandler for FastProcessor {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate fast processing
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let mut payload = event.payload().clone();
        payload[format!("{}_processed", self.name)] = json!(true);
        payload[format!("{}_time_ms", self.name)] = json!(10);
        
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            &format!("{}.processed", self.name),
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Transform that processes events "slowly"
#[derive(Clone, Debug)]
struct SlowProcessor {
    name: String,
}

impl SlowProcessor {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl TransformHandler for SlowProcessor {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate slow processing
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        let mut payload = event.payload().clone();
        payload[format!("{}_processed", self.name)] = json!(true);
        payload[format!("{}_time_ms", self.name)] = json!(100);
        
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            &format!("{}.processed", self.name),
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink that tracks which events it receives
#[derive(Clone)]
struct TrackingSink {
    name: String,
    events: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl TrackingSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

impl std::fmt::Debug for TrackingSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackingSink")
            .field("name", &self.name)
            .field("events", &self.event_count())
            .finish()
    }
}

#[async_trait]
impl SinkHandler for TrackingSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let payload = event.payload();
        self.events.lock().unwrap().push(payload.clone());
        
        println!("🎯 {} received event #{}", self.name, payload["id"]);
        
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
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,fan_out_demo=debug")
        .with_target(true)
        .with_thread_ids(true)
        .init();

    println!("🌊 Fan-Out Demo - Natural Multi-Reader Pattern");
    println!("==============================================\n");

    let journal_path = std::path::PathBuf::from("target/fan_out_demo_journal");
    println!("📁 Using DiskJournal at: {}", journal_path.display());

    println!("\n📊 Creating flow with natural fan-out...\n");

    // Create sinks
    let fast_sink = TrackingSink::new("fast_sink");
    let slow_sink = TrackingSink::new("slow_sink");
    let archive_sink = TrackingSink::new("archive_sink");

    // Create flow demonstrating fan-out
    let flow_handle = flow! {
        name: "fan_out_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No global middleware

        stages: {
            // One source
            src = source!("events" => EventSource::new());
            
            // Three parallel processing paths
            fast_path = transform!("fast_processor" => FastProcessor::new("fast"));
            slow_path = transform!("slow_processor" => SlowProcessor::new("slow"));
            archive_path = transform!("archiver" => FastProcessor::new("archive"));
            
            // Three sinks
            fast_snk = sink!("fast_sink" => fast_sink);
            slow_snk = sink!("slow_sink" => slow_sink);
            archive_snk = sink!("archive_sink" => archive_sink);
        },

        topology: {
            // Fan-out: Each transform creates its own reader!
            src |> fast_path;    // Reader 1 at position 0
            src |> slow_path;    // Reader 2 at position 0
            src |> archive_path; // Reader 3 at position 0
            
            // Each path to its sink
            fast_path |> fast_snk;
            slow_path |> slow_snk;
            archive_path |> archive_snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...\n");
    println!("⚡ Fast path processes in 10ms");
    println!("🐌 Slow path processes in 100ms");
    println!("📦 Archive path processes in 10ms\n");

    // Run the flow to completion and get the metrics exporter
    let metrics_exporter = flow_handle.run_with_metrics().await?;

    println!("\n✅ Flow completed!");

    // Get the metrics summary after completion
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("{}", summary);
    } else {
        println!("No metrics exporter configured");
    }
    
    println!("\n💡 Key Insights:");
    println!("  - Each transform created its own journal reader");
    println!("  - Readers progress independently (different positions)");
    println!("  - No coordination needed between readers");
    println!("  - Natural backpressure: slow readers don't block fast ones");
    println!("  - Total time ~1s (10 events × 100ms for slow path)");

    Ok(())
}