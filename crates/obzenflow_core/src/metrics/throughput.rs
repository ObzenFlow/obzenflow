// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ephemeral processing rates. These never establish accounting or journal coverage.

use crate::event::observability::CaptureStamp;
use crate::time::MetricsDuration;
use crate::StageId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An indivisible observation, retained with its original capture on retransmission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThroughputMeasurement {
    pub capture: CaptureStamp,
    pub event_delta: u64,
    pub elapsed: MetricsDuration,
    pub events_per_second: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ThroughputSnapshot {
    pub stages: HashMap<StageId, ThroughputMeasurement>,
    pub flow_input: Option<ThroughputMeasurement>,
    pub flow_output: Option<ThroughputMeasurement>,
}

/// An owned latest-value view; reading it never triggers capture.
pub trait ThroughputSource: Send + Sync {
    fn throughput(&self) -> ThroughputSnapshot;
}
