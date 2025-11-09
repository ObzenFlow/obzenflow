// Simple stateless pipeline to test FSM warnings
// This example uses only sources, transforms, and sinks - no stateful stages

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler
};
use obzenflow_runtime_services::stages::transform::Map;
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
    Result as CoreResult,
};
use serde_json::json;
use anyhow::Result;
use async_trait::async_trait;

// Simple source that generates a few events
#[derive(Clone, Debug)]
struct SimpleSource {
    count: usize,
    writer_id: WriterId,
}

impl SimpleSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SimpleSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count == 0 {
            return None;
        }
        self.count -= 1;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "number",
            json!({
                "value": self.count + 1,
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

// Simple transform that doubles values - using Map helper (FLOWIP-080h)
// Before: 24 lines of struct + impl TransformHandler
// After: Just use Map::new() directly in the pipeline!

// Simple sink that prints results
#[derive(Clone, Debug)]
struct Printer {
    name: String,
}

impl Printer {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}

#[async_trait]
impl SinkHandler for Printer {
    async fn consume(&mut self, event: ChainEvent) -> CoreResult<DeliveryPayload> {
        println!("[{}] Received: {}", self.name,
            serde_json::to_string(&event.payload()).unwrap_or_default());

        Ok(DeliveryPayload::success(
            self.name.clone(),
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment variable for metrics
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("==========================================");
    println!("     Simple Stateless Pipeline Test");
    println!("==========================================");
    println!();
    println!("Testing FSM warnings with only:");
    println!("  • Source (generates 5 events)");
    println!("  • Transform (doubles values)");
    println!("  • Sink (prints results)");
    println!("  • NO stateful stages");
    println!();

    FlowApplication::run(async {
        flow! {
            name: "stateless_simple",
            journals: disk_journals(std::path::PathBuf::from("target/stateless-simple-logs")),
            middleware: [],

            stages: {
                numbers = source!("numbers" => SimpleSource::new(5));
                // Using Map helper instead of custom Doubler struct (FLOWIP-080h)
                doubler = transform!("doubler" => Map::new(|event| {
                    if let Some(value) = event.payload()["value"].as_u64() {
                        ChainEventFactory::data_event(
                            WriterId::from(StageId::new()),
                            "doubled",
                            json!({
                                "original": value,
                                "doubled": value * 2,
                            }),
                        )
                    } else {
                        event
                    }
                }));
                printer = sink!("printer" => Printer::new("output"));
            },

            topology: {
                numbers |> doubler;
                doubler |> printer;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Pipeline completed successfully!");
    println!("\n📊 Analysis:");
    println!("  Check the logs above for FSM warnings.");
    println!("  If warnings appear, they're from core infrastructure.");
    println!("  If no warnings, the issue is specific to stateful stages.");

    Ok(())
}