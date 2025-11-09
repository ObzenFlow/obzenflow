//! Flight Delay Analysis Pipeline - Using FLOWIP-080c & FLOWIP-080h
//!
//! This demonstrates how to build real-time data analytics pipelines
//! using FlowState RS's composable stateful transform primitives.
//!
//! **Key Improvements**:
//! - FLOWIP-080c: GroupBy accumulator with OnEOF emission (~30 lines → ~8 lines)
//! - FLOWIP-080h: Transform helpers (Filter, Map, TryMapWith) with composable error handling
//!   - Validator: 57-line struct → TryMapWith.on_error_emit_with() (structured error payload)
//!   - Valid/Invalid Filters: 2 Filter helpers separate the streams
//!   - Calculator: 43-line struct → Map helper with closure
//!   - Fixer: 65-line struct → Map helper with closure
//!   - Total reduction: ~165 lines of boilerplate eliminated!
//!
//! Run with: cargo run --package obzenflow --example flight_delays

use obzenflow_dsl_infra::{flow, source, transform, stateful, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler
};
use obzenflow_adapters::middleware::rate_limit;
// ✨ FLOWIP-080c: Import the new stateful primitives
use obzenflow_runtime_services::stages::stateful::accumulators::GroupBy;
// ✨ FLOWIP-080h: Import the new transform helpers (all typed!)
use obzenflow_runtime_services::stages::transform::{FilterTyped, MapTyped};
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
                "validation_status": "valid",  // Default - validator will update this
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
                "FlightRecord",  // Single event type for all flight records
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

// ============================================================================
// FLOWIP-080h: Type-Safe Transform Helpers - Pure Typed Approach
// ============================================================================

/// Single FlightRecord type with validation state
/// This is the ONLY flight record type - validation state is in the data, not the event_type
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlightRecord {
    // Core flight data
    #[serde(default)]
    carrier: String,
    delay_minutes: Option<u32>,
    scheduled_duration: Option<u32>,
    #[serde(default)]
    date: String,
    #[serde(default)]
    origin: String,
    #[serde(default)]
    destination: String,
    #[serde(default)]
    flight_number: String,

    // Validation state - encoded in the data!
    validation_status: ValidationStatus,

    // Optional validation errors (only present when invalid)
    #[serde(skip_serializing_if = "Option::is_none")]
    validation_errors: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ValidationStatus {
    Valid,
    Invalid,
}

/// Flight validator using MapTyped (FLOWIP-080h)
///
/// Key insight: Validation doesn't fail - it just marks the record as valid/invalid!
/// - Input: FlightRecord (possibly unvalidated or from fixer)
/// - Output: FlightRecord with validation_status set
/// - No error handling needed - validation is just adding state
fn flight_validator() -> MapTyped<
    FlightRecord,
    FlightRecord,
    impl Fn(FlightRecord) -> FlightRecord + Send + Sync + Clone,
> {
    MapTyped::new(|mut flight: FlightRecord| {
        // Validate required fields
        let mut missing_fields = Vec::new();

        if flight.carrier.is_empty() {
            missing_fields.push("carrier".to_string());
        }
        if flight.delay_minutes.is_none() {
            missing_fields.push("delay_minutes".to_string());
        }
        if flight.scheduled_duration.is_none() {
            missing_fields.push("scheduled_duration".to_string());
        }

        // Set validation status based on results
        if missing_fields.is_empty() {
            flight.validation_status = ValidationStatus::Valid;
            flight.validation_errors = None;
        } else {
            flight.validation_status = ValidationStatus::Invalid;
            flight.validation_errors = Some(missing_fields);
        }

        flight
    })
}

// ============================================================================
// FLOWIP-080h: Typed Filters - Guard Each Branch
// ============================================================================

/// Filter for valid flight records using FilterTyped (FLOWIP-080h)
/// Guards the "valid" branch - only passes records with validation_status == Valid
fn valid_flights_filter() -> FilterTyped<
    FlightRecord,
    impl Fn(&FlightRecord) -> bool + Send + Sync + Clone,
> {
    FilterTyped::new(|flight: &FlightRecord| {
        flight.validation_status == ValidationStatus::Valid
    })
}

/// Filter for invalid flight records using FilterTyped (FLOWIP-080h)
/// Guards the "invalid" branch - only passes records with validation_status == Invalid
fn invalid_flights_filter() -> FilterTyped<
    FlightRecord,
    impl Fn(&FlightRecord) -> bool + Send + Sync + Clone,
> {
    FilterTyped::new(|flight: &FlightRecord| {
        flight.validation_status == ValidationStatus::Invalid
    })
}

// ============================================================================
// FLOWIP-080h: Typed Map for Delay Calculation
// ============================================================================

/// Categorized flight record with delay category
#[derive(Debug, Clone, Serialize)]
struct CategorizedFlight {
    carrier: String,
    delay_minutes: u32,
    scheduled_duration: u32,
    date: String,
    origin: String,
    destination: String,
    flight_number: String,
    validation_status: ValidationStatus,
    delay_category: String,
}

/// Adds delay category to valid flight records using MapTyped (FLOWIP-080h)
///
/// Input: FlightRecord (must be valid - filter ensures this)
/// Output: CategorizedFlight with delay_category field added
fn categorize_delays() -> MapTyped<
    FlightRecord,
    CategorizedFlight,
    impl Fn(FlightRecord) -> CategorizedFlight + Send + Sync + Clone,
> {
    MapTyped::new(|flight: FlightRecord| {
        let delay = flight.delay_minutes.unwrap_or(0);

        let delay_category = if delay == 0 {
            "on_time"
        } else if delay < 15 {
            "minor_delay"
        } else if delay < 60 {
            "moderate_delay"
        } else {
            "severe_delay"
        };

        CategorizedFlight {
            carrier: flight.carrier,
            delay_minutes: delay,
            scheduled_duration: flight.scheduled_duration.unwrap_or(0),
            date: flight.date,
            origin: flight.origin,
            destination: flight.destination,
            flight_number: flight.flight_number,
            validation_status: flight.validation_status,
            delay_category: delay_category.to_string(),
        }
    })
}

// ============================================================================
// FLOWIP-080h: Typed Map for Flight Record Fixing
// ============================================================================

/// Fixes invalid flight records using MapTyped (FLOWIP-080h)
///
/// Input: FlightRecord with validation_status == Invalid
/// Output: FlightRecord with missing fields fixed (ready for revalidation)
///
/// Note: Uses idiomatic struct construction with ..spread syntax rather than mutation
fn fix_invalid_flights() -> MapTyped<
    FlightRecord,
    FlightRecord,
    impl Fn(FlightRecord) -> FlightRecord + Send + Sync + Clone,
> {
    MapTyped::new(|flight: FlightRecord| {
        // Fix missing fields with sensible defaults
        let carrier = if flight.carrier.is_empty() {
            println!("🔧 Fixed missing carrier with 'UNK'");
            "UNK".to_string()
        } else {
            flight.carrier
        };

        let delay_minutes = if flight.delay_minutes.is_none() {
            println!("🔧 Fixed missing delay_minutes with 0");
            Some(0)
        } else {
            flight.delay_minutes
        };

        let scheduled_duration = if flight.scheduled_duration.is_none() {
            println!("🔧 Fixed missing scheduled_duration with 120");
            Some(120)
        } else {
            flight.scheduled_duration
        };

        // Construct new record with fixes applied (idiomatic Rust)
        FlightRecord {
            carrier,
            delay_minutes,
            scheduled_duration,
            validation_status: ValidationStatus::Valid,  // Clear for revalidation
            validation_errors: None,                      // Clear errors
            ..flight  // Copy remaining fields unchanged
        }
    })
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

                // ✨ FLOWIP-080h: Map helper - validates and emits valid/invalid events
                // Replaces 57-line FlightValidator struct with inline validation logic
                val = transform!("validator" => flight_validator());

                // ✨ FLOWIP-080h: Filter helpers - separate valid and invalid streams
                valid_filter = transform!("valid_filter" => valid_flights_filter());
                invalid_filter = transform!("invalid_filter" => invalid_flights_filter());

                // ✨ FLOWIP-080h: Map helper - fixes invalid records
                // Replaces 65-line FlightFixer struct
                fixer = transform!("fixer" => fix_invalid_flights());

                // ✨ FLOWIP-080h: Map helper - categorizes delays
                // Replaces 43-line DelayCalculator struct
                calc = transform!("calculator" => categorize_delays());

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

                // Split streams: valid events to calculator, invalid events to fixer
                val |> valid_filter;
                val |> invalid_filter;

                valid_filter |> calc;      // Only valid events to calculator
                calc |> agg;
                agg |> printer;

                // Error handling cycle: invalid records go to fixer and back to validator
                invalid_filter |> fixer;   // Only invalid records to fixer
                val <| fixer;               // Fixed records back to validator (THE CYCLE!)

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
    println!("   FLOWIP-080h Transform Helpers:");
    println!("   • Validator: TryMap<ValidatedFlight> with TryFrom/Into traits");
    println!("   • FlightValidator: 57 lines → ValidatedFlight type + TryMap");
    println!("   • DelayCalculator: 43 lines → Map helper with closure");
    println!("   • FlightFixer: 65 lines → Map helper with closure");
    println!("   • Total reduction: ~165 lines of boilerplate eliminated!");
    println!("");
    println!("   Cycle-based Error Handling:");
    println!("   • Invalid records automatically fixed and retried");
    println!("   • CycleGuardMiddleware prevents infinite loops");
    println!("   • Clean separation of validation, fixing, and processing");
    println!("   • Error sink for permanently failed records");
    println!("\n📝 Journal written to: target/flight-delays-logs/");

    Ok(())
}
