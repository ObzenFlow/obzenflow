// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Optional, bounded observations of the real Studio connection. This probe
//! has no admission, reader or projection authority. Diagnostic runs pay its
//! locking/timestamp cost; ordinary timing runs leave it absent.

use obzenflow_core::JournalId;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, serde::Serialize)]
pub struct AppliedRecord {
    pub journal: JournalId,
    pub sequence: u64,
    pub elapsed: Duration,
}

#[derive(Clone, serde::Serialize)]
pub struct StudioCapacitySnapshot {
    pub applied: Vec<AppliedRecord>,
    pub overflowed: bool,
    pub peak_pending_frames: usize,
    pub peak_pending_string_capacity: usize,
}

pub struct StudioCapacityProbe {
    start: Instant,
    limit: usize,
    state: Mutex<StudioCapacitySnapshot>,
}

impl StudioCapacityProbe {
    pub fn new(start: Instant, limit: usize) -> Self {
        Self {
            start,
            limit,
            state: Mutex::new(StudioCapacitySnapshot {
                applied: Vec::with_capacity(limit),
                overflowed: false,
                peak_pending_frames: 0,
                peak_pending_string_capacity: 0,
            }),
        }
    }

    pub(crate) fn applied(
        &self,
        journal: JournalId,
        sequence: u64,
        pending_frames: usize,
        pending_string_capacity: usize,
    ) {
        let elapsed = self.start.elapsed();
        let mut state = self.state.lock().unwrap();
        if state.applied.len() < self.limit {
            state.applied.push(AppliedRecord {
                journal,
                sequence,
                elapsed,
            });
        } else {
            state.overflowed = true;
        }
        state.peak_pending_frames = state.peak_pending_frames.max(pending_frames);
        state.peak_pending_string_capacity = state
            .peak_pending_string_capacity
            .max(pending_string_capacity);
    }

    pub fn snapshot(&self) -> StudioCapacitySnapshot {
        self.state.lock().unwrap().clone()
    }
}
