// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flight Delay Analysis Pipeline
//!
//! Demonstrates event schema design using TypedPayload trait and
//! typed join helpers for enriching stream data with reference data.
//!
//! Run with: cargo run --package obzenflow --example flight_delays_simple

use super::domain::*;
use super::fixtures;
use super::handlers::*;
use anyhow::Result;
use obzenflow::sinks::ConsoleSink;
use obzenflow_dsl::{flow, join, sink, source, stateful, transform, with_ref};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::join::InnerJoinBuilder;
use obzenflow_runtime::stages::source::FiniteSourceTyped;

pub fn run_example() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}

async fn run() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    let carriers = fixtures::carriers();
    let flights = fixtures::flights();

    FlowApplication::run(flow! {
        name: "flight_delays",
        journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),
        middleware: [],

        stages: {
            carriers = source!("carriers" => FiniteSourceTyped::new(carriers));
            flights = source!("flights" => FiniteSourceTyped::new(flights));

            val = transform!("validator" => FlightValidator::new());
            calc = transform!("calculator" => DelayCalculator::new());

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

    Ok(())
}
