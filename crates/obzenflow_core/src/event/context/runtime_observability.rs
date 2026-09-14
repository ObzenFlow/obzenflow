// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Optional runtime measurements. A missing value never represents measured zero.

use crate::event::payloads::execution_payload::CircuitState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeObservability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_flight: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_reference_since_last_stream: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_state_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_loops_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_loops_with_work_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing: Option<TimingMeasurements>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerMeasurements>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiter: Option<RateLimiterMeasurements>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effect_circuit_breakers: Vec<EffectCircuitBreakerContext>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effect_rate_limiters: Vec<EffectRateLimiterContext>,
}

/// One captured population. Count and sum must be collected with the histogram,
/// never supplied by a newer event-accounting snapshot or a scrape interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimingMeasurements {
    pub processing_time_count: u64,
    pub processing_time_sum_nanos: u64,
    pub recent_p50_ms: Option<u64>,
    pub recent_p90_ms: Option<u64>,
    pub recent_p95_ms: Option<u64>,
    pub recent_p99_ms: Option<u64>,
    pub recent_p999_ms: Option<u64>,
    pub window: MeasurementWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementWindow {
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
}

impl TimingMeasurements {
    /// Malformed optional evidence is discarded independently of its carrier.
    pub fn is_valid(&self) -> bool {
        if self.window.started_at_ms > self.window.ended_at_ms {
            return false;
        }
        let percentiles = [
            self.recent_p50_ms,
            self.recent_p90_ms,
            self.recent_p95_ms,
            self.recent_p99_ms,
            self.recent_p999_ms,
        ];
        if self.processing_time_count == 0 {
            return self.processing_time_sum_nanos == 0 && percentiles.iter().all(Option::is_none);
        }
        let mut previous = None;
        for percentile in percentiles.into_iter().flatten() {
            if previous.is_some_and(|value| percentile < value) {
                return false;
            }
            previous = Some(percentile);
        }
        true
    }
}

/// Observed state is diagnostic; only committed decisions establish control state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerMeasurements {
    pub observed_state: CircuitState,
    pub requests_total: u64,
    pub successes_total: u64,
    pub failures_total: u64,
    pub slow_total: u64,
    pub rejections_total: u64,
    pub opened_total: u64,
    pub time_closed_seconds: f64,
    pub time_open_seconds: f64,
    pub time_half_open_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimiterMeasurements {
    pub events_total: u64,
    pub delayed_total: u64,
    pub tokens_consumed_total: f64,
    pub delay_seconds_total: f64,
    pub bucket_tokens: f64,
    pub bucket_capacity: f64,
}

/// Cumulative circuit breaker metrics for one declared effect (FLOWIP-120c).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCircuitBreakerContext {
    pub effect_type: String,
    pub cb_requests_total: u64,
    pub cb_successes_total: u64,
    pub cb_failures_total: u64,
    #[serde(default)]
    pub cb_slow_total: u64,
    pub cb_rejections_total: u64,
    pub cb_opened_total: u64,
    pub cb_time_closed_seconds: f64,
    pub cb_time_open_seconds: f64,
    pub cb_time_half_open_seconds: f64,
    /// Current breaker state (0=closed, 0.5=half_open, 1=open).
    pub cb_state: f64,
}

/// Cumulative rate limiter metrics for one declared effect (FLOWIP-120c).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectRateLimiterContext {
    pub effect_type: String,
    pub rl_events_total: u64,
    pub rl_delayed_total: u64,
    pub rl_tokens_consumed_total: f64,
    pub rl_delay_seconds_total: f64,
    pub rl_bucket_tokens: f64,
    pub rl_bucket_capacity: f64,
}
