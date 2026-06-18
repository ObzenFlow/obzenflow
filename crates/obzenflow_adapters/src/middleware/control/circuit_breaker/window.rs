// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::time::{Duration, Instant};

/// Sliding window shape for rate-based failure detection.
#[derive(Debug, Clone)]
pub(crate) enum FailureWindow {
    /// Sliding window over the last `size` calls.
    Count { size: u32 },
    /// Time-based window over the last `duration`.
    Time { duration: Duration },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CallSample {
    pub(super) timestamp: Instant,
    pub(super) is_failure: bool,
    pub(super) is_slow: bool,
}

#[derive(Debug)]
pub(super) struct FailureWindowState {
    samples: Vec<Option<CallSample>>,
    index: usize,
    count: usize,
}

impl FailureWindowState {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            samples: vec![None; capacity.max(1)],
            index: 0,
            count: 0,
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.samples.len()
    }

    pub(super) fn push(&mut self, sample: CallSample) {
        let cap = self.capacity();
        if cap == 0 {
            return;
        }
        self.samples[self.index] = Some(sample);
        self.index = (self.index + 1) % cap;
        if self.count < cap {
            self.count += 1;
        }
    }

    pub(super) fn iter<'a>(&'a self) -> impl Iterator<Item = CallSample> + 'a {
        let cap = self.capacity();
        (0..self.count).filter_map(move |i| {
            let idx = if self.count < cap {
                i
            } else {
                (self.index + i) % cap
            };
            self.samples[idx]
        })
    }
}
