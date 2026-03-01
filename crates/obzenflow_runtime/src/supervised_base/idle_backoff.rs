// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::time::Duration;

/// Simple exponential idle backoff with a hard cap.
///
/// Intended for event loops that may legitimately observe long stretches of "no work"
/// while polling synchronous sources (FLOWIP-086i).
#[derive(Debug, Clone)]
pub(crate) struct IdleBackoff {
    current: Duration,
    initial: Duration,
    max: Duration,
}

impl IdleBackoff {
    pub(crate) fn exponential_with_cap(initial: Duration, max: Duration) -> Self {
        Self {
            current: Duration::ZERO,
            initial,
            max,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.current = Duration::ZERO;
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        if self.current == Duration::ZERO {
            self.current = self.initial;
            return self.current;
        }

        let doubled = self.current.checked_mul(2).unwrap_or(self.max);
        self.current = std::cmp::min(doubled, self.max);
        self.current
    }
}
