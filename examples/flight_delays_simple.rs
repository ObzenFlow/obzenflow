//! Flight Delay Analysis Pipeline - FLOWIP-082a Event Schemas & FLOWIP-080l Join Demo
//!
//! This demonstrates event schema design using TypedPayload trait and
//! typed join helpers for enriching stream data with reference data.
//!
//! **FLOWIP-082a Features**:
//! - TypedPayload trait for compile-time event type resolution
//! - Strongly-typed event structs instead of raw JSON
//! - Schema version constants for evolution
//!
//! **FLOWIP-080l Features**:
//! - WaitForReferenceJoin for enriching flight data with carrier details
//! - Reference-first convention (carrier data loads before flights)
//! - Type-safe join key extraction
//!
//! Run with: cargo run --package obzenflow --example flight_delays_simple

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::sinks::ConsoleSink;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, // ✨ FLOWIP-082a
    WriterId,
};
use obzenflow_dsl_infra::{flow, join, sink, source, stateful, transform, with_ref};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{StatefulHandler, TransformHandler};
use obzenflow_runtime_services::stages::join::InnerJoinBuilder;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

// ============================================================================
// FLOWIP-082a & FLOWIP-080l: Typed Event Schemas
// ============================================================================

/// Carrier details - reference data for airline carriers (FLOWIP-080l)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CarrierDetails {
    carrier_code: String,
    carrier_name: String,
    country: String,
    fleet_size: u32,
}

impl TypedPayload for CarrierDetails {
    const EVENT_TYPE: &'static str = "carrier.details";
    const SCHEMA_VERSION: u32 = 1;
}

/// Enriched flight - flight record with carrier details (FLOWIP-080l output)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnrichedFlight {
    carrier_code: String,
    carrier_name: String,
    carrier_country: String,
    date: String,
    origin: String,
    destination: String,
    scheduled_duration: u32,
    delay_minutes: u32,
    flight_number: String,
    delay_category: Option<String>,
}

impl TypedPayload for EnrichedFlight {
    const EVENT_TYPE: &'static str = "flight.enriched";
    const SCHEMA_VERSION: u32 = 1;
}

/// Flight record event - represents a single flight observation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlightRecord {
    carrier: String,
    date: String,
    origin: String,
    destination: String,
    scheduled_duration: u32,
    delay_minutes: u32,
    flight_number: String,
    // Optional: delay category added by calculator
    delay_category: Option<String>,
}

impl TypedPayload for FlightRecord {
    const EVENT_TYPE: &'static str = "flight.record";
    const SCHEMA_VERSION: u32 = 1;
}

/// Carrier statistics event - aggregated delay metrics per carrier
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CarrierStatistics {
    carrier: String,
    total_delay: u64,
    flight_count: u64,
    average_delay: f64,
}

impl TypedPayload for CarrierStatistics {
    const EVENT_TYPE: &'static str = "carrier.statistics";
    const SCHEMA_VERSION: u32 = 1;
}

// ============================================================================
// Sources (FLOWIP-081: Typed Source Helpers)
// ============================================================================

// ============================================================================
// Transform: Validator
// ============================================================================

/// Validates flight records
#[derive(Clone, Debug)]
struct FlightValidator;

impl FlightValidator {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for FlightValidator {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // ✨ FLOWIP-082a: Check event type using constant
        if FlightRecord::event_type_matches(&event.event_type()) {
            // Validate that all required fields are present
            let has_carrier = event.payload().get("carrier").is_some();
            let has_delay = event.payload().get("delay_minutes").is_some();
            let has_duration = event.payload().get("scheduled_duration").is_some();

            if !(has_carrier && has_delay && has_duration) {
                // ✨ FLOWIP-082h: Mark invalid records with a structured validation error.
                // The transform supervisor will route these to the stage's error_journal
                // while keeping valid records on the main data path.
                return Ok(vec![event.mark_as_validation_error(
                    "flight_validation_failed: missing carrier, delay_minutes, or scheduled_duration",
                )]);
            }

            Ok(vec![event])
        } else {
            Ok(vec![])
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

// ============================================================================
// Transform: Delay Calculator
// ============================================================================

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
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // ✨ FLOWIP-082a: Check event type using constant
        if FlightRecord::event_type_matches(&event.event_type()) {
            if let Some(delay) = event
                .payload()
                .get("delay_minutes")
                .and_then(|v| v.as_u64())
            {
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

                // ✨ FLOWIP-082a: Use EVENT_TYPE constant
                return Ok(vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    &FlightRecord::versioned_event_type(),
                    payload,
                )]);
            }
        }
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

// ============================================================================
// Join: Enrich Flights with Carrier Details (FLOWIP-080l)
// ============================================================================
// (Moved inline to join! macro below)

// ============================================================================
// Stateful: Carrier Aggregation
// ============================================================================

/// State for carrier aggregation
#[derive(Clone, Debug, Default)]
struct CarrierStats {
    stats: HashMap<String, (u64, u64)>, // carrier -> (total_delay, flight_count)
}

/// Stateful aggregator
#[derive(Debug, Clone)]
struct CarrierAggregator {
    writer_id: WriterId,
}

impl CarrierAggregator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for CarrierAggregator {
    type State = CarrierStats;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        // ✨ FLOWIP-080l: Now processing enriched flights with carrier details
        if EnrichedFlight::event_type_matches(&event.event_type()) {
            if let (Some(carrier_name), Some(delay)) = (
                event.payload().get("carrier_name").and_then(|v| v.as_str()),
                event
                    .payload()
                    .get("delay_minutes")
                    .and_then(|v| v.as_u64()),
            ) {
                let entry = state
                    .stats
                    .entry(carrier_name.to_string())
                    .or_insert((0, 0));
                entry.0 += delay;
                entry.1 += 1;
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        CarrierStats::default()
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        // ✨ FLOWIP-082a: Emit typed CarrierStatistics events
        let events = state
            .stats
            .iter()
            .map(|(carrier, (total_delay, flight_count))| {
                let avg_delay = if *flight_count > 0 {
                    *total_delay as f64 / *flight_count as f64
                } else {
                    0.0
                };

                let stats = CarrierStatistics {
                    carrier: carrier.clone(),
                    total_delay: *total_delay,
                    flight_count: *flight_count,
                    average_delay: avg_delay,
                };

                // Use TypedPayload's EVENT_TYPE
                ChainEventFactory::data_event(
                    self.writer_id.clone(),
                    &CarrierStatistics::versioned_event_type(),
                    serde_json::to_value(&stats)
                        .expect("CarrierStatistics should always serialize"),
                )
            })
            .collect();

        Ok(events)
    }
}

// ============================================================================
// Main Pipeline
// ============================================================================

#[tokio::main]
pub async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🛫 FlowState RS - Flight Delay Analysis");
    println!("========================================");
    println!("✨ FLOWIP-082a: TypedPayload Event Schemas");
    println!("✨ FLOWIP-080l: Typed Join Helpers");
    println!("");

    println!("Features demonstrated:");
    println!("  • TypedPayload trait for compile-time event types");
    println!("  • Strongly-typed event structs (FlightRecord, CarrierStatistics)");
    println!("  • Schema version constants for evolution");
    println!("  • EVENT_TYPE constants instead of string literals");
    println!("  • WaitForReferenceJoin for enriching stream data");
    println!("  • Reference-first convention (carriers load before flights)");
    println!("  • Type-safe join key extraction\n");

    println!("Running delay analysis pipeline...\n");

    let carriers = vec![
        CarrierDetails {
            carrier_code: "AA".to_string(),
            carrier_name: "American Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 950,
        },
        CarrierDetails {
            carrier_code: "UA".to_string(),
            carrier_name: "United Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 850,
        },
        CarrierDetails {
            carrier_code: "DL".to_string(),
            carrier_name: "Delta Air Lines".to_string(),
            country: "USA".to_string(),
            fleet_size: 900,
        },
        CarrierDetails {
            carrier_code: "WN".to_string(),
            carrier_name: "Southwest Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 750,
        },
        CarrierDetails {
            carrier_code: "BA".to_string(),
            carrier_name: "British Airways".to_string(),
            country: "UK".to_string(),
            fleet_size: 290,
        },
        CarrierDetails {
            carrier_code: "LH".to_string(),
            carrier_name: "Lufthansa".to_string(),
            country: "Germany".to_string(),
            fleet_size: 340,
        },
        CarrierDetails {
            carrier_code: "AF".to_string(),
            carrier_name: "Air France".to_string(),
            country: "France".to_string(),
            fleet_size: 280,
        },
    ];

    let flights_raw = vec![
        (
            "AA".to_string(),
            "2023-12-01".to_string(),
            "LAX".to_string(),
            "JFK".to_string(),
            120,
            15,
        ),
        (
            "DL".to_string(),
            "2023-12-01".to_string(),
            "ATL".to_string(),
            "ORD".to_string(),
            90,
            0,
        ),
        (
            "UA".to_string(),
            "2023-12-01".to_string(),
            "SFO".to_string(),
            "LAX".to_string(),
            60,
            45,
        ),
        (
            "AA".to_string(),
            "2023-12-01".to_string(),
            "DFW".to_string(),
            "MIA".to_string(),
            180,
            5,
        ),
        (
            "WN".to_string(),
            "2023-12-01".to_string(),
            "LAS".to_string(),
            "PHX".to_string(),
            75,
            120,
        ),
        (
            "DL".to_string(),
            "2023-12-01".to_string(),
            "SEA".to_string(),
            "DEN".to_string(),
            110,
            8,
        ),
        (
            "UA".to_string(),
            "2023-12-01".to_string(),
            "EWR".to_string(),
            "SFO".to_string(),
            300,
            0,
        ),
        (
            "AA".to_string(),
            "2023-12-01".to_string(),
            "ORD".to_string(),
            "LAX".to_string(),
            240,
            25,
        ),
    ];

    let flights: Vec<FlightRecord> = flights_raw
        .into_iter()
        .enumerate()
        .map(
            |(idx, (carrier, date, origin, destination, scheduled_duration, delay_minutes))| {
                let flight_number = format!("{}{}", carrier, 1000 + (idx + 1) * 100);
                FlightRecord {
                    carrier,
                    date,
                    origin,
                    destination,
                    scheduled_duration,
                    delay_minutes,
                    flight_number,
                    delay_category: None,
                }
            },
        )
        .collect();

    FlowApplication::run(flow! {
        name: "flight_delays",
        journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),
        middleware: [],

        stages: {
            // Reference data source (FLOWIP-080l)
            carriers = source!("carriers" => FiniteSourceTyped::new(carriers));

            // Stream data source
            flights = source!("flights" => FiniteSourceTyped::new(flights));

            // Processing stages
            val = transform!("validator" => FlightValidator::new());
            calc = transform!("calculator" => DelayCalculator::new());

            // Join stage to enrich flights with carrier details (FLOWIP-080l)
            enricher = join!("enricher" => with_ref!(carriers,
                InnerJoinBuilder::<CarrierDetails, FlightRecord, EnrichedFlight>::new()
                    .catalog_key(|carrier: &CarrierDetails| carrier.carrier_code.clone())
                    .stream_key(|flight: &FlightRecord| flight.carrier.clone())
                    .build(|carrier: CarrierDetails, flight: FlightRecord| EnrichedFlight {
                        carrier_code: flight.carrier.clone(),
                        carrier_name: carrier.carrier_name.clone(),
                        carrier_country: carrier.country.clone(),
                        date: flight.date.clone(),
                        origin: flight.origin.clone(),
                        destination: flight.destination.clone(),
                        scheduled_duration: flight.scheduled_duration,
                        delay_minutes: flight.delay_minutes,
                        flight_number: flight.flight_number.clone(),
                        delay_category: flight.delay_category.clone(),
                    })
            ));

            // Aggregation and output
            agg = stateful!("aggregator" => CarrierAggregator::new());
            printer = sink!("printer" => ConsoleSink::<CarrierStatistics>::table(
                &["status", "carrier", "avg_delay", "flights"],
                |stats| {
                    let status = if stats.average_delay < 10.0 {
                        "🟢"
                    } else if stats.average_delay < 30.0 {
                        "🟡"
                    } else {
                        "🔴"
                    };

                    vec![
                        status.to_string(),
                        stats.carrier.clone(),
                        format!("{:.1} min", stats.average_delay),
                        stats.flight_count.to_string(),
                    ]
                }
            ));
        },

        topology: {
            flights |> val;
            val |> calc;
            calc |> enricher;
            enricher |> agg;
            agg |> printer;
        }
    })
    .await?;

    println!("\n✅ Analysis pipeline completed!");

    println!("\n💡 FLOWIP-082a Benefits:");
    println!("   • Type safety: FlightRecord::EVENT_TYPE instead of \"FlightRecord\"");
    println!("   • Schema evolution: SCHEMA_VERSION constants track changes");
    println!("   • Compile-time validation: TypedPayload trait enforces structure");
    println!("   • Documentation: Event types are self-documenting structs");

    println!("\n💡 FLOWIP-080l Benefits:");
    println!("   • No custom StatefulHandler needed for joins");
    println!("   • Type-safe join keys prevent runtime errors");
    println!("   • Reference-first convention eliminates confusion");
    println!("   • 50% less boilerplate for join patterns");

    println!("\n📝 Journal written to: target/flight-delays-logs/");

    Ok(())
}
