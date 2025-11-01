//! Flight Delay Analysis Pipeline - REFACTORED for FLOWIP-080b
//!
//! This demonstrates how to build real-time data analytics pipelines
//! using FlowState RS's stateful transform stages.
//!
//! **Key Improvement**: Uses StatefulHandler instead of Arc<Mutex> anti-pattern
//! for carrier aggregation, eliminating tokio::spawn workarounds.
//!
//! Run with: cargo run --package obzenflow --example flight_delays

use obzenflow_dsl_infra::{flow, source, transform, stateful, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, StatefulHandler, SinkHandler
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    event::JournalEvent,
    WriterId,
    id::StageId,
};
use serde_json::json;
use std::collections::HashMap;
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
        let flights = vec![
            ("AA".to_string(), "2023-12-01".to_string(), "LAX".to_string(), "JFK".to_string(), 120, 15), // American Airlines, 15 min delay
            ("DL".to_string(), "2023-12-01".to_string(), "ATL".to_string(), "ORD".to_string(), 90, 0),   // Delta, on time
            ("UA".to_string(), "2023-12-01".to_string(), "SFO".to_string(), "LAX".to_string(), 60, 45),  // United, 45 min delay
            ("AA".to_string(), "2023-12-01".to_string(), "DFW".to_string(), "MIA".to_string(), 180, 5),  // American Airlines, 5 min delay
            ("SW".to_string(), "2023-12-01".to_string(), "LAS".to_string(), "PHX".to_string(), 75, 120), // Southwest, 2 hour delay
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

            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "FlightRecord",
                json!({
                    "carrier": carrier,
                    "date": date,
                    "origin": origin,
                    "destination": dest,
                    "scheduled_duration": duration,
                    "delay_minutes": delay,
                    "flight_number": format!("{}{}", carrier, 1000 + self.current_index * 100),
                })
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
struct FlightValidator;

impl FlightValidator {
    fn new() -> Self {
        Self
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
                vec![event]
            } else {
                // Error handling would go here
                vec![]
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
        if event.event_type() == "FlightRecord" {
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
                    "FlightRecord",
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
// NEW: StatefulHandler for Carrier Aggregation (FLOWIP-080b)
// ============================================================================

/// State for carrier aggregation - NO Arc<Mutex> needed!
#[derive(Clone, Debug, Default)]
struct CarrierStats {
    stats: HashMap<String, (u64, u64)>, // carrier -> (total_delay, flight_count)
}

/// Stateful aggregator using FLOWIP-080b infrastructure
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

    fn process(&self, state: &Self::State, event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
        let mut new_state = state.clone();

        if event.event_type() == "FlightRecord" {
            if let (Some(carrier), Some(delay)) = (
                event.payload().get("carrier").and_then(|v| v.as_str()),
                event.payload().get("delay_minutes").and_then(|v| v.as_u64()),
            ) {
                let entry = new_state.stats.entry(carrier.to_string()).or_insert((0, 0));
                entry.0 += delay;
                entry.1 += 1;
            }
        }

        // Accumulate only - emit summary on drain
        (new_state, vec![])
    }

    fn initial_state(&self) -> Self::State {
        CarrierStats::default()
    }

    async fn drain(
        &mut self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
        // Emit aggregated results as events
        let results: Vec<ChainEvent> = state.stats.iter().map(|(carrier, (total_delay, flight_count))| {
            let avg_delay = if *flight_count > 0 {
                *total_delay as f64 / *flight_count as f64
            } else {
                0.0
            };

            ChainEventFactory::data_event(
                self.writer_id.clone(),
                "CarrierStatistics",
                json!({
                    "carrier": carrier,
                    "total_delay": total_delay,
                    "flight_count": flight_count,
                    "average_delay": avg_delay,
                }),
            )
        }).collect();

        Ok(results)
    }
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
        if event.event_type() == "CarrierStatistics" {
            let payload = event.payload();
            let carrier = payload["carrier"].as_str().unwrap_or("Unknown");
            let avg_delay = payload["average_delay"].as_f64().unwrap_or(0.0);
            let flight_count = payload["flight_count"].as_u64().unwrap_or(0);

            let status = if avg_delay < 10.0 { "🟢" } else if avg_delay < 30.0 { "🟡" } else { "🔴" };
            println!("  {} {}: {:.1} min avg delay ({} flights)",
                status, carrier, avg_delay, flight_count);
        }
        Ok(DeliveryPayload::success(
            "statistics_printer",
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🛫 FlowState RS - Flight Delay Analysis");
    println!("========================================");
    println!("Refactored with FLOWIP-080b Stateful Stages");
    println!("");

    println!("Running delay analysis pipeline...\n");

    // Use FlowApplication for proper setup
    FlowApplication::run(async {
        flow! {
            name: "flight_delays",
            journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),
            middleware: [],

            stages: {
                src = source!("source" => FlightDataSource::new());
                val = transform!("validator" => FlightValidator::new());
                calc = transform!("calculator" => DelayCalculator::new());
                agg = stateful!("aggregator" => CarrierAggregator::new());
                printer = sink!("printer" => StatisticsPrinter::new());
            },

            topology: {
                src |> val;
                val |> calc;
                calc |> agg;
                agg |> printer;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Analysis pipeline completed!");
    println!("\n💡 Key Improvements:");
    println!("   • State managed WITHOUT Arc<Mutex> anti-pattern");
    println!("   • No tokio::spawn workarounds needed");
    println!("   • Clean functional state updates");
    println!("   • Results emitted as events via drain()");
    println!("\n📝 Journal written to: target/flight-delays-logs/");

    Ok(())
}
