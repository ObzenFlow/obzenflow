// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closed optional measurement bundles.

use serde::{Deserialize, Serialize};

/// Snapshot of metrics at event creation time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSnapshot {
    pub events_processed: u64,
    pub events_in_flight: u32,
    pub queue_depth: u32,
    pub processing_rate: f64,
    pub error_rate: f64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
}

/// Service level indicator snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SliSnapshot {
    pub availability: f64,
    pub error_budget_remaining: f64,
    pub latency_budget_used: f64,
}
