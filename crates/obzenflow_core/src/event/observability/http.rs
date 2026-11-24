// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};

use crate::web::HttpMethod;

/// `HttpPullMeasurements` holds counters of the requests that an HTTP system sent and the
/// responses that it received.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPullMeasurements {
    pub requests_total: u64,
    pub responses_2xx: u64,
    pub responses_4xx: u64,
    pub responses_5xx: u64,
    pub rate_limited_total: u64,
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
