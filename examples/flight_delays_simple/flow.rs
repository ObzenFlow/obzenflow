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
use obzenflow_dsl::{flow, join, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;

pub fn run_example() -> Result<()> {
    FlowApplication::builder().run_blocking(FlowDefinition::materialize(
        move |_runtime_config| {
            let carriers_handler = sources::finite(fixtures::carriers());
            let flights_handler = sources::finite(fixtures::flights());
            let validator_handler = FlightValidator;
            let calculator_handler = DelayCalculator;
            let enricher_handler =
                joins::inner::<CarrierDetails, FlightRecord, EnrichedFlight, _, _, _, _>(
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
                );
            let aggregator_handler = CarrierAggregator;
            let printer_handler = sinks::table::<CarrierStatistics, _>(
                &["status", "carrier", "avg_delay", "flights"],
                |stats: &CarrierStatistics| {
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
            );

            Ok(flow! {
                name: "flight_delays",
                journals: disk_journals(std::path::PathBuf::from("target/flight-delays-logs")),

                stages: {
                    carriers = source!(CarrierDetails => carriers_handler);
                    flights = source!(FlightRecord => flights_handler);

                    val = transform!(FlightRecord -> FlightRecord => validator_handler);
                    // DelayCalculator adds delay_category on the raw record, before the
                    // enricher joins carrier details, so it is FlightRecord -> FlightRecord.
                    calc = transform!(FlightRecord -> FlightRecord => calculator_handler);

                    enricher = join!(
                        catalog carriers: CarrierDetails,
                        FlightRecord -> EnrichedFlight => enricher_handler
                    );

                    agg = stateful!(EnrichedFlight -> CarrierStatistics => aggregator_handler);
                    printer = sink!(CarrierStatistics => printer_handler);
                },

                topology: {
                    flights |> val;
                    val |> calc;
                    calc |> enricher;
                    enricher |> agg;
                    agg |> printer;
                }
            })
        },
    ))?;

    Ok(())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}
