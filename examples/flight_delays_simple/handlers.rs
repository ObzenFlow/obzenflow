// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    StatefulEmission, TypedStatefulHandler, TypedTransformHandler,
};
use std::collections::HashMap;

// ============================================================================
// Transform: Validator
// ============================================================================

#[derive(Clone, Debug)]
pub struct FlightValidator;

impl TypedTransformHandler for FlightValidator {
    type Input = FlightRecord;
    type Output = FlightRecord;

    fn process(&self, flight: FlightRecord) -> Result<FlightRecord, HandlerError> {
        // Typed admission/deserialization proves the required fields exist.
        Ok(flight)
    }
}

// ============================================================================
// Transform: Delay Calculator
// ============================================================================

#[derive(Clone, Debug)]
pub struct DelayCalculator;

impl TypedTransformHandler for DelayCalculator {
    type Input = FlightRecord;
    type Output = FlightRecord;

    fn process(&self, mut flight: FlightRecord) -> Result<FlightRecord, HandlerError> {
        flight.delay_category = Some(
            match flight.delay_minutes {
                0 => "on_time",
                1..=14 => "minor_delay",
                15..=59 => "moderate_delay",
                _ => "severe_delay",
            }
            .to_string(),
        );
        Ok(flight)
    }
}

// ============================================================================
// Stateful: Carrier Aggregation
// ============================================================================

#[derive(Clone, Debug, Default)]
pub struct CarrierStats {
    stats: HashMap<String, (u64, u64)>, // carrier -> (total_delay, flight_count)
}

#[derive(Debug, Clone)]
pub struct CarrierAggregator;

impl TypedStatefulHandler for CarrierAggregator {
    type State = CarrierStats;
    type Input = EnrichedFlight;
    type Output = CarrierStatistics;

    fn accumulate(&self, state: &mut Self::State, flight: EnrichedFlight) {
        let entry = state.stats.entry(flight.carrier_name).or_insert((0, 0));
        entry.0 += u64::from(flight.delay_minutes);
        entry.1 += 1;
    }

    fn initial_state(&self) -> Self::State {
        CarrierStats::default()
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        let mut outputs: Vec<_> = state
            .stats
            .iter()
            .map(|(carrier, (total_delay, flight_count))| {
                let avg_delay = if *flight_count > 0 {
                    *total_delay as f64 / *flight_count as f64
                } else {
                    0.0
                };

                CarrierStatistics {
                    carrier: carrier.clone(),
                    total_delay: *total_delay,
                    flight_count: *flight_count,
                    average_delay: avg_delay,
                }
            })
            .collect();
        outputs.sort_by(|left, right| left.carrier.cmp(&right.carrier));
        Ok(StatefulEmission::RetainEpoch {
            next_state: state.clone(),
            outputs,
        })
    }
}
