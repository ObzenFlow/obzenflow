// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Processing information for events
//!
//! Tracks when an event occurred, its processing time and outcome.

use crate::event::status::processing_status::ProcessingStatus;
use crate::time::MetricsDuration;
use serde::{Deserialize, Serialize};

/// Information about how an event was processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingContext {
    /// How long processing took
    pub processing_time: MetricsDuration,

    /// When the event occurred (milliseconds since Unix epoch)
    pub event_time: u64,

    /// The outcome of processing
    pub status: ProcessingStatus,
}

impl Default for ProcessingContext {
    fn default() -> Self {
        Self {
            processing_time: MetricsDuration::ZERO,
            event_time: 0,
            status: ProcessingStatus::Success,
        }
    }
}
