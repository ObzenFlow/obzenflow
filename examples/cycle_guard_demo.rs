//! Demo: CycleGuardMiddleware preventing infinite loops (FLOWIP-080h)
//!
//! This demonstrates how the CycleGuardMiddleware can prevent infinite loops
//! in topologies with cycles by limiting the number of iterations.
//!
//! **FLOWIP-080h Update**: Replaced 33-line ProblematicProcessor struct with Map helper

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_runtime_services::stages::transform::Map;
use serde_json::json;

/// Source that generates a few test events
#[derive(Clone, Debug)]
struct EventGenerator {
    count: usize,
    writer_id: WriterId,
}

impl EventGenerator {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for EventGenerator {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 3 {
            return None;
        }

        self.count += 1;

        Some(ChainEventFactory::source_event(
            self.writer_id.clone(),
            "source",
            obzenflow_core::event::ChainEventContent::Data {
                event_type: "test.event".to_string(),
                payload: json!({
                    "id": self.count,
                    "iterations": 0,
                }),
            },
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= 3
    }
}

// ============================================================================
// FLOWIP-080h: Map Helper for Problematic Processor
// ============================================================================

/// A problematic processor that always sends events back (creates infinite loop!)
///
/// Replaces 33-line ProblematicProcessor struct with a Map helper (FLOWIP-080h)
fn problematic_processor() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event| {
        let mut payload = event.payload();
        let id = payload["id"].as_i64().unwrap_or(0);
        let iterations = payload["iterations"].as_i64().unwrap_or(0);

        // Increment iteration counter
        payload["iterations"] = json!(iterations + 1);

        println!("Processing event {} (iteration {})", id, iterations + 1);
        println!("   Event ID: {}", event.id);
        println!("   Parent IDs: {:?}", event.causality.parent_ids);
        println!("   Correlation ID: {:?}", event.correlation_id);
        println!("   Processing Status: {:?}", event.processing_info.status);
        println!(
            "   Entry Event ID: {}",
            event
                .correlation_payload
                .as_ref()
                .map(|p| p.entry_event_id.to_string())
                .unwrap_or_else(|| "None".to_string())
        );

        // ALWAYS send the event back - this would create an infinite loop!
        ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "test.event", // Same type - will be processed again
            payload,
        )
    })
}

/// Simple sink that counts events
#[derive(Clone, Debug)]
struct EventCounter {
    name: String,
}

impl EventCounter {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl SinkHandler for EventCounter {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> Result<DeliveryPayload, Box<dyn std::error::Error + Send + Sync>> {
        let payload = event.payload();
        let id = payload["id"].as_i64().unwrap_or(0);
        let iterations = payload["iterations"].as_i64().unwrap_or(0);

        println!(
            "{} received event {} after {} iterations",
            self.name, id, iterations
        );

        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("Counted".to_string()),
            Some(1),
        ))
    }
}

/// NOTE: This example uses the MANUAL flow pattern instead of FlowApplication.
///
/// Why? This demo needs programmatic access to the metrics exporter to display
/// cycle statistics at the end. FlowApplication::run() doesn't expose the metrics
/// exporter, so we use the manual pattern here.
///
/// Most examples should use FlowApplication for automatic lifecycle management,
/// CLI args, and web server support. Use the manual pattern only when you need:
/// - Direct access to FlowHandle or metrics exporter
/// - Custom metrics processing/reporting
/// - Embedding flows in larger applications
/// - Custom lifecycle requirements
#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    // Initialize logging - set to error to reduce noise even more
    tracing_subscriber::fmt()
        .with_env_filter(
            "obzenflow=error,obzenflow_adapters::middleware::control::cycle_guard=debug",
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    println!("Cycle Guard Demo - Preventing Infinite Loops");
    println!("============================================\n");

    let journal_path = std::path::PathBuf::from("target/cycle_guard_demo_journal");

    // Create flow with a problematic self-cycle
    let flow_handle = flow! {
        name: "cycle_guard_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No flow-level middleware

        stages: {
            source = source!("source" => EventGenerator::new());

            // ✨ FLOWIP-080h: Using Map helper instead of ProblematicProcessor struct
            // First processor with cycle guard
            processor1 = transform!("processor1" => problematic_processor());

            // Second processor to create a valid cycle
            processor2 = transform!("processor2" => problematic_processor());

            sink = sink!("sink" => EventCounter::new("EventSink"));
        },

        topology: {
            // Main flow
            source |> processor1;
            processor1 |> processor2;
            processor2 |> sink;

            // PROBLEMATIC CYCLE: processors feed back to each other!
            processor1 <| processor2;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("Running flow with cycle between processors...");
    println!("- Processors always send events back to each other");
    println!("- CycleGuardMiddleware will abort after max iterations\n");

    // Run the flow to completion
    let metrics_exporter = flow_handle.run_with_metrics().await?;

    // Get the metrics summary after completion
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("{}", summary);
    }

    println!("\nFlow completed successfully!");
    println!("\nWhat happened:");
    println!("- Each event was processed multiple times through the cycle");
    println!("- CycleGuardMiddleware automatically aborted after max iterations");
    println!("- Without the guard, this would run forever");
    println!("- This demonstrates the framework's automatic cycle protection");

    Ok(())
}
