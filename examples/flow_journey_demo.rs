//! Demo to test flow-level journey tracking and metrics
//! Run with: cargo run -p obzenflow --example flow_journey_demo

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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Source that generates events with controllable patterns
#[derive(Clone, Debug)]
struct JourneySource {
    events_generated: usize,
    writer_id: WriterId,
}

impl JourneySource {
    fn new() -> Self {
        Self {
            events_generated: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for JourneySource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.events_generated >= 100 {
            return None;
        }

        self.events_generated += 1;

        // Pattern to test different journey outcomes:
        // - Events 1-80: Normal processing (should complete journey)
        // - Events 81-90: Will be dropped at transform (test abandoned journeys)
        // - Events 91-100: Slow processing (test in-flight/saturation)
        
        let event_type = if self.events_generated <= 80 {
            "normal"
        } else if self.events_generated <= 90 {
            "drop_me"
        } else {
            "slow"
        };

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "journey.event",
            json!({
                "id": self.events_generated,
                "type": event_type,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.events_generated >= 100
    }
}

/// Transform that simulates different processing behaviors
#[derive(Clone, Debug)]
struct JourneyTransform {
    dropped_events: Arc<AtomicUsize>,
}

impl JourneyTransform {
    fn new(dropped_events: Arc<AtomicUsize>) -> Self {
        Self { dropped_events }
    }
}

#[async_trait]
impl TransformHandler for JourneyTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let payload = event.payload();
        let event_type = payload["type"].as_str().unwrap_or("unknown");

        match event_type {
            "normal" => {
                // Normal processing - quick
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    "processed",
                    json!({
                        "original_id": payload["id"],
                        "processed_at": std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    }),
                )]
            }
            "drop_me" => {
                // Simulate dropped events - return empty vec
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
                vec![]
            }
            "slow" => {
                // Simulate slow processing to build up saturation
                std::thread::sleep(std::time::Duration::from_millis(500));
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    "processed_slowly",
                    payload.clone(),
                )]
            }
            _ => vec![event],
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink that tracks what it receives
#[derive(Clone, Debug)]
struct JourneySink {
    events_received: usize,
}

impl JourneySink {
    fn new() -> Self {
        Self { events_received: 0 }
    }
}

#[async_trait]
impl SinkHandler for JourneySink {
    async fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        self.events_received += 1;
        
        // Add small delay to simulate real sink
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        Ok(DeliveryPayload::success(
            "journey_sink",
            DeliveryMethod::Custom("Test".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Use prometheus exporter to see raw metrics
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");
    
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,flow_journey_demo=info")
        .init();

    println!("🚀 Flow Journey Tracking Demo");
    println!("=============================\n");
    println!("This demo tests journey tracking with:");
    println!("- 80 normal events (should complete journeys)");
    println!("- 10 dropped events (should be abandoned)");
    println!("- 10 slow events (should show saturation)\n");

    let journal_path = std::path::PathBuf::from("target/flow_journey_demo_journal");
    let dropped_events = Arc::new(AtomicUsize::new(0));

    let flow_handle = flow! {
        name: "journey_tracking_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],

        stages: {
            src = source!("journey_source" => JourneySource::new());
            trans = transform!("journey_transform" => JourneyTransform::new(dropped_events.clone()));
            snk = sink!("journey_sink" => JourneySink::new());
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...\n");

    let metrics_exporter = flow_handle.run_with_metrics().await?;

    println!("\n✅ Flow completed!");

    // Get the final metrics
    if let Some(exporter) = metrics_exporter {
        let metrics = exporter.render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        
        // Print key journey metrics
        println!("🔍 Key Journey Metrics:");
        println!("======================");
        
        // Extract and display specific metrics we care about
        for line in metrics.lines() {
            if line.contains("obzenflow_flow_journeys_started_total") ||
               line.contains("obzenflow_flow_journeys_completed_total") ||
               line.contains("obzenflow_flow_journeys_abandoned_total") ||
               line.contains("obzenflow_flow_saturation_journeys") ||
               line.contains("obzenflow_flow_journey_duration_seconds") ||
               line.contains("obzenflow_flow_events_in_total") ||
               line.contains("obzenflow_flow_events_out_total") ||
               line.contains("obzenflow_flow_utilization") {
                println!("{}", line);
            }
        }
        
        println!("\n📈 Expected Results:");
        println!("- journeys_started: 100");
        println!("- journeys_completed: ~90 (80 normal + 10 slow)");
        println!("- journeys_abandoned: ~10 (dropped events)");
        println!("- events_in: 100 (from source)");
        println!("- events_out: ~90 (received by sink)");
        println!("- saturation should spike during slow events");
        
        // Optionally dump full metrics to file
        std::fs::write("target/flow_journey_metrics.txt", metrics)?;
        println!("\n💾 Full metrics saved to: target/flow_journey_metrics.txt");
    }

    Ok(())
}