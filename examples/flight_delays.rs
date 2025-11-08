//! Flight Delay Analysis Pipeline - Using FLOWIP-080c Primitives
//!
//! This demonstrates how to build real-time data analytics pipelines
//! using FlowState RS's composable stateful transform primitives.
//!
//! **Key Improvement**: Uses FLOWIP-080c's GroupBy accumulator with OnEOF emission
//! instead of manual StatefulHandler, reducing code from ~30 lines to ~8 lines.
//!
//! Run with: cargo run --package obzenflow --example flight_delays

use obzenflow_dsl_infra::{flow, source, transform, stateful, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_adapters::middleware::rate_limit;
// ✨ FLOWIP-080c: Import the new stateful primitives
use obzenflow_runtime_services::stages::stateful::accumulators::GroupBy;
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use anyhow::Result;
use async_trait::async_trait;

/// Source that generates flight data
#[derive(Clone, Debug)]
struct FlightDataSource {
    flights: Vec<(String, String, String, String, u32, u32)>,
    current_index: usize,
    writer_id: WriterId,
}

impl FlightDataSource {
    fn new() -> Self {
        // Mix of valid and invalid records to test error handling
        let flights = vec![
            // Valid records
            ("AA".to_string(), "2023-12-01".to_string(), "LAX".to_string(), "JFK".to_string(), 120, 15), // American Airlines, 15 min delay
            ("DL".to_string(), "2023-12-01".to_string(), "ATL".to_string(), "ORD".to_string(), 90, 0),   // Delta, on time
            // Invalid record - missing carrier
            ("".to_string(), "2023-12-01".to_string(), "SFO".to_string(), "LAX".to_string(), 60, 45),    // Missing carrier!
            ("AA".to_string(), "2023-12-01".to_string(), "DFW".to_string(), "MIA".to_string(), 180, 5),  // American Airlines, 5 min delay
            // Invalid record - missing delay info (999999 means missing)
            ("SW".to_string(), "2023-12-01".to_string(), "LAS".to_string(), "PHX".to_string(), 75, 999999), // Missing delay!
            ("DL".to_string(), "2023-12-01".to_string(), "SEA".to_string(), "DEN".to_string(), 110, 8),  // Delta, 8 min delay
            ("UA".to_string(), "2023-12-01".to_string(), "EWR".to_string(), "SFO".to_string(), 300, 0),  // United, on time
            ("AA".to_string(), "2023-12-01".to_string(), "ORD".to_string(), "LAX".to_string(), 240, 25), // American Airlines, 25 min delay
        ];

        Self {
            flights,
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for FlightDataSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_index < self.flights.len() {
            let (carrier, date, origin, dest, duration, delay) = &self.flights[self.current_index];
            self.current_index += 1;

            let mut payload = json!({
                "date": date,
                "origin": origin,
                "destination": dest,
                "flight_number": format!("FL{:03}", self.current_index * 100),
            });

            // Add carrier only if not empty (simulating missing field)
            if !carrier.is_empty() {
                payload["carrier"] = json!(carrier);
            }

            // Add delay only if not 999999 (simulating missing field)
            if *delay != 999999 {
                payload["delay_minutes"] = json!(delay);
            }

            // Add duration only if not 0 (simulating missing field)
            if *duration != 0 {
                payload["scheduled_duration"] = json!(duration);
            }

            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "FlightRecord",
                payload
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.current_index >= self.flights.len()
    }
}

/// Validates flight records
#[derive(Clone, Debug)]
struct FlightValidator {
    writer_id: WriterId,
}

impl FlightValidator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for FlightValidator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type() == "FlightRecord" {
            // Validate that all required fields are present
            let valid = event.payload().get("carrier").is_some() &&
                        event.payload().get("delay_minutes").is_some() &&
                        event.payload().get("scheduled_duration").is_some();

            if valid {
                // Valid flight record, mark it as valid
                vec![ChainEventFactory::derived_data_event(
                    self.writer_id.clone(),
                    &event,
                    "FlightRecord.valid",
                    event.payload().clone(),
                )]
            } else {
                // Invalid flight record, mark it for fixing
                let mut payload = event.payload().clone();
                payload["validation_errors"] = json!({
                    "missing_fields": vec![
                        if event.payload().get("carrier").is_none() { Some("carrier") } else { None },
                        if event.payload().get("delay_minutes").is_none() { Some("delay_minutes") } else { None },
                        if event.payload().get("scheduled_duration").is_none() { Some("scheduled_duration") } else { None },
                    ].into_iter().flatten().collect::<Vec<_>>()
                });

                vec![ChainEventFactory::derived_data_event(
                    self.writer_id.clone(),
                    &event,
                    "FlightRecord.invalid",
                    payload,
                )]
            }
        } else {
            vec![]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Calculates delay statistics
#[derive(Clone, Debug)]
struct DelayCalculator;

impl DelayCalculator {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for DelayCalculator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Only process valid flight records
        if event.event_type() == "FlightRecord.valid" {
            if let Some(delay) = event.payload().get("delay_minutes").and_then(|v| v.as_u64()) {
                // Categorize delay
                let delay_category = if delay == 0 {
                    "on_time"
                } else if delay < 15 {
                    "minor_delay"
                } else if delay < 60 {
                    "moderate_delay"
                } else {
                    "severe_delay"
                };

                let mut payload = event.payload().clone();
                payload["delay_category"] = json!(delay_category);

                return vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    "FlightRecord.valid",
                    payload,
                )];
            }
        }
        vec![event]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

// ============================================================================
// NEW: Flight Record Fixer for Invalid Data
// ============================================================================

/// Fixes invalid flight records by adding missing fields with defaults
#[derive(Clone, Debug)]
struct FlightFixer {
    writer_id: WriterId,
}

impl FlightFixer {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for FlightFixer {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Only process invalid flight records
        if event.event_type() != "FlightRecord.invalid" {
            return vec![];
        }

        let mut payload = event.payload().clone();

        // Get list of missing fields from validation errors (clone to avoid borrow issues)
        let missing_fields = payload["validation_errors"]["missing_fields"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect::<Vec<_>>())
            .unwrap_or_default();

        // Fix missing fields with sensible defaults
        for field in &missing_fields {
            match field.as_str() {
                "carrier" => {
                    payload["carrier"] = json!("UNK"); // Unknown carrier
                    println!("🔧 Fixed missing carrier with 'UNK'");
                }
                "delay_minutes" => {
                    payload["delay_minutes"] = json!(0); // Assume on-time if unknown
                    println!("🔧 Fixed missing delay_minutes with 0");
                }
                "scheduled_duration" => {
                    payload["scheduled_duration"] = json!(120); // Default 2 hour flight
                    println!("🔧 Fixed missing scheduled_duration with 120");
                }
                _ => {}
            }
        }

        // Remove validation_errors field since we've fixed them
        payload.as_object_mut().map(|obj| obj.remove("validation_errors"));

        // Send the fixed record back as a regular FlightRecord
        // The CycleGuardMiddleware will prevent infinite loops
        vec![ChainEventFactory::derived_data_event(
            self.writer_id.clone(),
            &event,
            "FlightRecord", // Back to original type for revalidation
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

// ============================================================================
// NEW: FLOWIP-080c Stateful Aggregation with GroupBy
// ============================================================================

/// State for carrier aggregation - simple struct with Serialize!
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CarrierStats {
    total_delay: u64,
    flight_count: u64,
    average_delay: f64,
}

/// Sink that prints carrier statistics
#[derive(Clone, Debug)]
struct StatisticsPrinter;

impl StatisticsPrinter {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for StatisticsPrinter {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        // FLOWIP-080c emits with "key" (carrier) and "result" (stats) structure
        if let Some(carrier) = event.payload().get("key").and_then(|v| v.as_str()) {
            if let Some(stats) = event.payload().get("result") {
                let avg_delay = stats["average_delay"].as_f64().unwrap_or(0.0);
                let flight_count = stats["flight_count"].as_u64().unwrap_or(0);

                let status = if avg_delay < 10.0 { "🟢" } else if avg_delay < 30.0 { "🟡" } else { "🔴" };
                println!("  {} {}: {:.1} min avg delay ({} flights)",
                    status, carrier, avg_delay, flight_count);
            }
        }
        Ok(DeliveryPayload::success(
            "statistics_printer",
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

/// Sink for error records that couldn't be fixed after max retries
#[derive(Clone, Debug)]
struct ErrorSink;

impl ErrorSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for ErrorSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        // Log records that failed after max retry attempts
        if event.event_type() == "FlightRecord.failed" {
            let payload = event.payload();
            println!("❌ Permanently failed flight record:");
            println!("   Reason: {}", payload["reason"].as_str().unwrap_or("unknown"));
            println!("   Details: {}", payload);
        }
        Ok(DeliveryPayload::success(
            "error_sink",
            DeliveryMethod::Custom("ErrorLog".to_string()),
            None,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🛫 FlowState RS - Flight Delay Analysis with Error Handling");
    println!("============================================================");
    println!("✨ Using FLOWIP-080c Stateful Primitives + Cycle-based Error Handling");
    println!("");

    println!("Features demonstrated:");
    println!("  • Validation with error emission instead of dropping");
    println!("  • Cycle operator (<|) for fix/retry pattern");
    println!("  • Automatic cycle protection via CycleGuardMiddleware");
    println!("  • FLOWIP-080c GroupBy for aggregation");
    println!("  • Rate limiting on stateful stage (2 events/sec)\n");

    println!("Running delay analysis pipeline...\n");

    // Use FlowApplication for proper setup
    FlowApplication::run(async {
        flow! {
            name: "flight_delays",
            journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),
            middleware: [], // CycleGuardMiddleware is added automatically when cycles detected

            stages: {
                src = source!("source" => FlightDataSource::new());
                val = transform!("validator" => FlightValidator::new());
                fixer = transform!("fixer" => FlightFixer::new());
                calc = transform!("calculator" => DelayCalculator::new());

                // ✨ FLOWIP-080c: GroupBy with OnEOF emission + Rate Limiting
                // This replaces ~30 lines of manual StatefulHandler code!
                // Adding rate_limit(2.0) to process max 2 events per second
                agg = stateful!("aggregator" =>
                    GroupBy::new("carrier", |event: &ChainEvent, stats: &mut CarrierStats| {
                        if let Some(delay) = event.payload()["delay_minutes"].as_u64() {
                            stats.total_delay += delay;
                            stats.flight_count += 1;
                            stats.average_delay = stats.total_delay as f64 / stats.flight_count as f64;
                        }
                    }).emit_on_eof(),  // Emit results when pipeline completes
                    [rate_limit(2.0)]  // Process max 2 events per second
                );

                printer = sink!("printer" => StatisticsPrinter::new());
                error_sink = sink!("error_sink" => ErrorSink::new());
            },

            topology: {
                // Main flow
                src |> val;
                val |> calc;
                calc |> agg;
                agg |> printer;

                // Error handling cycle: invalid records go to fixer and back to validator
                val |> fixer;       // Invalid records to fixer
                val <| fixer;       // Fixed records back to validator (THE CYCLE!)

                // Error sink for permanently failed records
                fixer |> error_sink;  // Records that exceed max retries
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Analysis pipeline completed!");
    println!("\n💡 Key Improvements:");
    println!("   FLOWIP-080c Stateful Primitives:");
    println!("   • Aggregation logic: ~8 lines vs ~30 lines");
    println!("   • No manual state management");
    println!("   • Composable with different emission strategies");
    println!("   • Type-safe without Arc<Mutex> patterns");
    println!("");
    println!("   Cycle-based Error Handling:");
    println!("   • Invalid records automatically fixed and retried");
    println!("   • CycleGuardMiddleware prevents infinite loops");
    println!("   • Clean separation of validation, fixing, and processing");
    println!("   • Error sink for permanently failed records");
    println!("\n📝 Journal written to: target/flight-delays-logs/");

    Ok(())
}
