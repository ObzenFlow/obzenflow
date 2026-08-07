// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{StatefulHandler, TypedTransformHandler};
use std::collections::HashMap;

// ============================================================================
// Transform: Validator
// ============================================================================

#[derive(Clone, Debug)]
pub struct FlightValidator;

impl FlightValidator {
    pub fn new() -> Self {
        Self
    }
}

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

impl DelayCalculator {
    pub fn new() -> Self {
        Self
    }
}

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
pub struct CarrierAggregator {
    writer_id: WriterId,
}

impl CarrierAggregator {
    pub fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for CarrierAggregator {
    type State = CarrierStats;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
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

                ChainEventFactory::data_event(
                    self.writer_id,
                    CarrierStatistics::versioned_event_type(),
                    serde_json::to_value(&stats)
                        .expect("CarrierStatistics should always serialize"),
                )
            })
            .collect();

        Ok(events)
    }
}
