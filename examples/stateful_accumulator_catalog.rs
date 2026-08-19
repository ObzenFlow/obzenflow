// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class stateful accumulator catalogue.
//!
//! This replayable example covers the extension points that are deliberately
//! small but important:
//!
//! - `Conflate::emit_always()` keeps the latest reading per sensor and emits
//!   each materialized-view update with exact per-key lineage;
//! - a custom `EmissionStrategy` composes with `Reduce` through
//!   `with_emission(...)`; and
//! - a user-authored `Accumulator` composes through `StatefulWithEmission`.
//!
//! Run live, then pass the printed journal directory back with
//! `--replay-from <run-dir> --verify`.

use anyhow::Result;
use obzenflow::sources;
use obzenflow::stateful;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::stateful::strategies::{EmissionStrategy, OnEOF};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SensorReading {
    sensor: String,
    value: i64,
    sequence: u64,
}

impl TypedPayload for SensorReading {
    const EVENT_TYPE: &'static str = "demo.sensor_reading";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ReadingCount {
    total: u64,
}

impl TypedPayload for ReadingCount {
    const EVENT_TYPE: &'static str = "demo.reading_count";
    const SCHEMA_VERSION: u32 = 1;
}

/// Immutable custom cadence configuration: emit after each pair of inputs.
#[derive(Clone, Debug)]
struct EveryTwoReadings;

impl EmissionStrategy for EveryTwoReadings {
    fn should_emit(&self, events_seen: u64, _period_elapsed: Option<Duration>) -> bool {
        events_seen >= 2
    }
}

#[derive(Clone, Debug, Default)]
struct FleetSummaryState {
    readings: u64,
    sensors: BTreeSet<String>,
    minimum: Option<i64>,
    maximum: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FleetSummary {
    readings: u64,
    sensors: usize,
    minimum: i64,
    maximum: i64,
}

impl TypedPayload for FleetSummary {
    const EVENT_TYPE: &'static str = "demo.fleet_summary";
    const SCHEMA_VERSION: u32 = 1;
}

/// A user-authored accumulator: typed values in, typed projection out.
#[derive(Clone, Debug)]
struct FleetSummaryAccumulator;

impl stateful::Accumulator for FleetSummaryAccumulator {
    type State = FleetSummaryState;
    type Input = SensorReading;
    type Output = FleetSummary;

    fn initial_state(&self) -> Self::State {
        FleetSummaryState::default()
    }

    fn accumulate(&self, state: &mut Self::State, reading: Self::Input) {
        state.readings = state.readings.saturating_add(1);
        state.sensors.insert(reading.sensor);
        state.minimum = Some(
            state
                .minimum
                .map_or(reading.value, |value| value.min(reading.value)),
        );
        state.maximum = Some(
            state
                .maximum
                .map_or(reading.value, |value| value.max(reading.value)),
        );
    }

    fn outputs(&self, state: &Self::State) -> Vec<Self::Output> {
        match (state.minimum, state.maximum) {
            (Some(minimum), Some(maximum)) => vec![FleetSummary {
                readings: state.readings,
                sensors: state.sensors.len(),
                minimum,
                maximum,
            }],
            _ => Vec::new(),
        }
    }
}

fn main() -> Result<()> {
    let readings = vec![
        SensorReading {
            sensor: "thermostat".to_string(),
            value: 20,
            sequence: 1,
        },
        SensorReading {
            sensor: "barometer".to_string(),
            value: 101,
            sequence: 2,
        },
        SensorReading {
            sensor: "thermostat".to_string(),
            value: 22,
            sequence: 3,
        },
        SensorReading {
            sensor: "barometer".to_string(),
            value: 99,
            sequence: 4,
        },
        SensorReading {
            sensor: "thermostat".to_string(),
            value: 21,
            sequence: 5,
        },
    ];

    FlowApplication::builder().run_blocking(FlowDefinition::materialize(
        move |_runtime_config| {
            let readings_handler = sources::finite(readings);

            // Built-in latest-per-key semantics plus the previously uncovered
            // built-in always-emitting cadence.
            let latest_handler =
                stateful::conflate(|reading: &SensorReading| reading.sensor.clone())
                    .emit_always();

            // A custom cadence composed through the first-class public method.
            let count_handler = stateful::reduce(
                ReadingCount::default(),
                |count: &mut ReadingCount, _reading: &SensorReading| {
                    count.total = count.total.saturating_add(1);
                },
            )
            .with_emission(EveryTwoReadings);

            // A custom accumulator uses the same public wrapper as the built-ins.
            let summary_handler = stateful::StatefulWithEmission::new(
                FleetSummaryAccumulator,
                OnEOF,
            );

            let latest_sink = SinkTyped::new(|reading: SensorReading| async move {
                println!(
                    "latest sensor={} value={} sequence={}",
                    reading.sensor, reading.value, reading.sequence
                );
            })
            .idempotent();
            let count_sink = SinkTyped::new(|count: ReadingCount| async move {
                println!("readings observed={}", count.total);
            })
            .idempotent();
            let summary_sink = SinkTyped::new(|summary: FleetSummary| async move {
                println!(
                    "fleet readings={} sensors={} range={}..={}",
                    summary.readings, summary.sensors, summary.minimum, summary.maximum
                );
            })
            .idempotent();

            Ok(flow! {
                name: "stateful_accumulator_catalog",
                journals: disk_journals(std::path::PathBuf::from("target/stateful-accumulator-catalog")),

                stages: {
                    readings = source!(SensorReading => readings_handler);
                    latest = stateful!(SensorReading -> SensorReading => latest_handler);
                    counts = stateful!(SensorReading -> ReadingCount => count_handler);
                    summary = stateful!(SensorReading -> FleetSummary => summary_handler);
                    latest_out = sink!(SensorReading => latest_sink);
                    counts_out = sink!(ReadingCount => count_sink);
                    summary_out = sink!(FleetSummary => summary_sink);
                },

                topology: {
                    readings |> latest;
                    readings |> counts;
                    readings |> summary;
                    latest |> latest_out;
                    counts |> counts_out;
                    summary |> summary_out;
                }
            })
        },
    ))?;

    Ok(())
}
