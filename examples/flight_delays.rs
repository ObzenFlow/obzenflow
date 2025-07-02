//! Flight Delay Analysis Pipeline
//!
//! This demonstrates how to build real-time data analytics pipelines
//! using FlowState RS's clean and powerful EventStore architecture.
//!
//! Run with: cargo run --example flight_delays

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
use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

/// Source that generates flight data
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
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for FlightDataSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_index < self.flights.len() {
            let (carrier, date, origin, dest, duration, delay) = &self.flights[self.current_index];
            self.current_index += 1;
            
            Some(ChainEvent::new(
                EventId::new(),
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
struct FlightValidator;

impl FlightValidator {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for FlightValidator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "FlightRecord" {
            // Validate that all required fields are present
            let valid = event.payload.get("carrier").is_some() &&
                        event.payload.get("delay_minutes").is_some() &&
                        event.payload.get("scheduled_duration").is_some();

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
}

/// Calculates delay statistics
struct DelayCalculator;

impl DelayCalculator {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for DelayCalculator {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "FlightRecord" {
            if let Some(delay) = event.payload.get("delay_minutes").and_then(|v| v.as_u64()) {
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

                event.payload["delay_category"] = json!(delay_category);
                // Success metrics would be recorded by middleware
            }
        }
        vec![event]
    }
}

/// Aggregates delays by carrier
struct CarrierAggregator {
    carrier_stats: Arc<tokio::sync::Mutex<HashMap<String, CarrierStats>>>,
}

#[derive(Default)]
struct CarrierStats {
    total_delay: u64,
    flight_count: u64,
}

impl CarrierAggregator {
    fn new() -> Self {
        Self {
            carrier_stats: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl SinkHandler for CarrierAggregator {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type == "FlightRecord" {
            let stats = self.carrier_stats.clone();
            tokio::spawn(async move {
                if let (Some(carrier), Some(delay)) = (
                    event.payload.get("carrier").and_then(|v| v.as_str()),
                    event.payload.get("delay_minutes").and_then(|v| v.as_u64()),
                ) {
                    let mut stats = stats.lock().await;
                    let entry = stats.entry(carrier.to_string()).or_default();
                    entry.total_delay += delay;
                    entry.flight_count += 1;
                }
            });
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for better error messages
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,flight_delays=debug")
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    println!("FlowState RS - Flight Delay Analysis");
    println!("======================================");

    let aggregator = CarrierAggregator::new();
    let stats = aggregator.carrier_stats.clone();

    println!("\nRunning delay analysis pipeline...");

    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/flight-delays-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "flight_delays").await?);

    // Create the flow using the new flow! macro
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],
        
        stages: {
            src = source!("source" => FlightDataSource::new(), [RED::monitoring()]);
            val = transform!("validator" => FlightValidator::new(), [USE::monitoring()]);
            calc = transform!("calculator" => DelayCalculator::new(), [GoldenSignals::monitoring()]);
            agg = sink!("aggregator" => aggregator, [SAAFE::monitoring()]);
        },
        
        topology: {
            src |> val;
            val |> calc;
            calc |> agg;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow with DSL: {:?}", e))?;

    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline - check if supervisor/FSM initialization is working: {:?}", e))?;

    println!("Analysis pipeline completed!");

    // Show the results
    println!("\nFlight Delay Analysis Results:");
    let final_stats = stats.lock().await;

    for (carrier, stats) in final_stats.iter() {
        let avg_delay = if stats.flight_count > 0 {
            stats.total_delay as f64 / stats.flight_count as f64
        } else {
            0.0
        };

        let status = if avg_delay < 10.0 { "🟢" } else if avg_delay < 30.0 { "🟡" } else { "🔴" };
        println!("  {} {}: {:.1} min avg delay ({} flights)",
            status, carrier, avg_delay, stats.flight_count);
    }

    // Cleanup
    println!("\nJournal written to: target/flight-delays-logs/flight_delays.log");
    Ok(())
}
