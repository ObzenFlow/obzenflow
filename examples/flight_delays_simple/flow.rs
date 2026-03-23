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
use obzenflow::typed::{joins, sinks, sources};
use obzenflow_dsl::{flow, join, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/flight_delays_simple/obzenflow.toml"
);

pub fn run_example() -> Result<()> {
    let carriers = fixtures::carriers();
    let flights = fixtures::flights();

    FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .run_blocking(flow! {
            name: "flight_delays",
            journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),
            middleware: [],

            stages: {
                carriers = source!(CarrierDetails => sources::finite(carriers));
                flights = source!(FlightRecord => sources::finite(flights));

                val = transform!(FlightValidator::new());
                calc = transform!(DelayCalculator::new());

                enricher = join!(
                    catalog carriers: CarrierDetails,
                    FlightRecord -> EnrichedFlight => joins::inner(
                        |carrier| carrier.carrier_code.clone(),
                        |flight| flight.carrier.clone(),
                        |carrier, flight| EnrichedFlight {
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
                        },
                    )
                );

                agg = stateful!(CarrierAggregator::new());
                printer = sink!(CarrierStatistics => sinks::table(
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
                    },
                ));
            },

            topology: {
                flights |> val;
                val |> calc;
                calc |> enricher;
                enricher |> agg;
                agg |> printer;
            }
        })?;

    Ok(())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}
