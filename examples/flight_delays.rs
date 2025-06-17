//! Flight Delay Analysis Pipeline
//!
//! This demonstrates how to build real-time data analytics pipelines
//! using FlowState RS's clean and powerful EventStore architecture.
//!
//! Run with: cargo run --example flight_delays_simple

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Source that generates flight data
struct FlightDataSource {
    flights: Vec<(String, String, String, String, u32, u32)>,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
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
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("FlightDataSource"),
        }
    }
}

impl Step for FlightDataSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
        if current < self.flights.len() {
            let (carrier, date, origin, dest, duration, delay) = &self.flights[current];
            vec![ChainEvent::new("FlightRecord", json!({
                "carrier": carrier,
                "date": date,
                "origin": origin,
                "destination": dest,
                "scheduled_duration": duration,
                "delay_minutes": delay,
                "flight_number": format!("{}{}", carrier, 1000 + current * 100),
            }))]
        } else {
            vec![]
        }
    }
}

/// Validates flight records
struct FlightValidator {
    metrics: <USE as Taxonomy>::Metrics,
}

impl FlightValidator {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("FlightValidator"),
        }
    }
}

impl Step for FlightValidator {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "FlightRecord" {
            // Validate that all required fields are present
            let valid = event.payload.get("carrier").is_some() &&
                        event.payload.get("delay_minutes").is_some() &&
                        event.payload.get("scheduled_duration").is_some();
            
            if valid {
                vec![event]
            } else {
                self.metrics.record_error();
                vec![]
            }
        } else {
            vec![]
        }
    }
}

/// Calculates delay statistics
struct DelayCalculator {
    metrics: <GoldenSignals as Taxonomy>::Metrics,
}

impl DelayCalculator {
    fn new() -> Self {
        Self {
            metrics: GoldenSignals::create_metrics("DelayCalculator"),
        }
    }
}

impl Step for DelayCalculator {
    type Taxonomy = GoldenSignals;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &GoldenSignals
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
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
                self.metrics.record_success(std::time::Duration::from_millis(1));
            }
        }
        vec![event]
    }
}

/// Aggregates delays by carrier
struct CarrierAggregator {
    carrier_stats: Arc<tokio::sync::Mutex<HashMap<String, CarrierStats>>>,
    metrics: <SAAFE as Taxonomy>::Metrics,
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
            metrics: SAAFE::create_metrics("CarrierAggregator"),
        }
    }
}

impl Step for CarrierAggregator {
    type Taxonomy = SAAFE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &SAAFE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
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
        vec![] // Sinks don't emit
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("FlowState RS - Flight Delay Analysis");
    println!("======================================");

    let aggregator = CarrierAggregator::new();
    let stats = aggregator.carrier_stats.clone();

    println!("\nRunning delay analysis pipeline...");

    let handle = flow! {
        name: "flight_delays",
        flow_taxonomy: GoldenSignals,
        ("source" => FlightDataSource::new(), RED)
        |> ("validator" => FlightValidator::new(), USE)
        |> ("calculator" => DelayCalculator::new(), GoldenSignals)
        |> ("aggregator" => aggregator, SAAFE)
    }?;
    
    // Let it run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;

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
    // Cleanup handled by tempdir
    Ok(())
}