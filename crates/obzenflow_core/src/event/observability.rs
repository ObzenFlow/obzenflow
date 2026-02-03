// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};

/// HTTP pull telemetry snapshot emitted via `MetricsLifecycle::Custom` wide events (FLOWIP-084e).
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpPullState {
    Fetching,
    Waiting,
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    RateLimit,
    PollInterval,
    Backoff,
}
