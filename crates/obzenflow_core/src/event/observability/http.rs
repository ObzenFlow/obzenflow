// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};

use crate::web::HttpMethod;

/// Source-local HTTP pull state and counters. Runtime separates these into
/// protected scheduling evidence and optional measurement captures.
///
/// This type is intentionally defined in `obzenflow_core` so both adapters and runtime services
/// can serialize/deserialize it without dependency cycles (IC-10).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPullTelemetry {
    /// Current state: "fetching", "waiting", "terminal"
    pub state: HttpPullState,

    /// Reason for wait (if state == waiting)
    pub wait_reason: Option<WaitReason>,

    /// When the source will next attempt activity (unix seconds)
    pub next_wake_unix_secs: Option<u64>,

    /// Last successful fetch (unix seconds)
    pub last_success_unix_secs: Option<u64>,

    /// Cumulative counters (for rate calculation)
    pub requests_total: u64,
    pub responses_2xx: u64,
    pub responses_4xx: u64,
    pub responses_5xx: u64,
    pub rate_limited_total: u64,
    pub retries_total: u64,
    pub events_decoded_total: u64,

    /// Cumulative wait time by reason (seconds)
    pub wait_seconds_rate_limit: f64,
    pub wait_seconds_poll_interval: f64,
    pub wait_seconds_backoff: f64,
}

impl Default for HttpPullTelemetry {
    fn default() -> Self {
        Self {
            state: HttpPullState::Fetching,
            wait_reason: None,
            next_wake_unix_secs: None,
            last_success_unix_secs: None,
            requests_total: 0,
            responses_2xx: 0,
            responses_4xx: 0,
            responses_5xx: 0,
            rate_limited_total: 0,
            retries_total: 0,
            events_decoded_total: 0,
            wait_seconds_rate_limit: 0.0,
            wait_seconds_poll_interval: 0.0,
            wait_seconds_backoff: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpPullState {
    Fetching,
    Waiting,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    RateLimit,
    PollInterval,
    Backoff,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPullMeasurements {
    pub requests_total: u64,
    pub responses_2xx: u64,
    pub responses_4xx: u64,
    pub responses_5xx: u64,
    pub rate_limited_total: u64,
    pub retries_total: u64,
    pub events_decoded_total: u64,
    pub wait_seconds_rate_limit: f64,
    pub wait_seconds_poll_interval: f64,
    pub wait_seconds_backoff: f64,
}

impl From<&HttpPullTelemetry> for HttpPullMeasurements {
    fn from(source: &HttpPullTelemetry) -> Self {
        Self {
            requests_total: source.requests_total,
            responses_2xx: source.responses_2xx,
            responses_4xx: source.responses_4xx,
            responses_5xx: source.responses_5xx,
            rate_limited_total: source.rate_limited_total,
            retries_total: source.retries_total,
            events_decoded_total: source.events_decoded_total,
            wait_seconds_rate_limit: source.wait_seconds_rate_limit,
            wait_seconds_poll_interval: source.wait_seconds_poll_interval,
            wait_seconds_backoff: source.wait_seconds_backoff,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpPullMetricsSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<HttpPullState>,
    pub wait_reason: Option<WaitReason>,
    pub next_wake_unix_secs: Option<u64>,
    pub last_success_unix_secs: Option<u64>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub measurements: Option<HttpPullMeasurements>,
}

/// Generic hosted HTTP surface route metrics (FLOWIP-093a).
///
/// This is intended to be emitted by the hosting layer as low-volume system events so
/// `/metrics` remains derivable from journaled facts via `MetricsAggregator`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpSurfaceMetricsSnapshot {
    /// Per-route totals (monotonic, since process start).
    pub routes: Vec<HttpSurfaceRouteMetricsSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpSurfaceRouteMetricsSnapshot {
    pub surface_name: String,
    pub method: HttpMethod,
    /// Registered route path (not a raw URL with parameters).
    pub path: String,
    /// Bucketed status class: "2xx", "3xx", "4xx", "5xx", or "other".
    pub status_class: String,

    /// Monotonic counters (since process start).
    pub requests_total: u64,
    pub request_duration_ms_total: u64,
    pub request_bytes_total: u64,
    pub response_bytes_total: u64,
}
