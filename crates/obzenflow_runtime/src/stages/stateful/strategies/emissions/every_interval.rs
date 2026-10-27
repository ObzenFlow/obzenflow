// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::EmissionStrategy;
use std::time::Duration;

/// Emits the current result once an interval has passed since the first input
/// after the previous emission, keeping the accumulated state.
///
/// Like [`TimeWindow`](super::TimeWindow), it runs on processing time, so its
/// boundaries follow the runtime's scheduling. Unlike a window, it never resets
/// the state, so totals keep running across emissions.
#[derive(Debug, Clone)]
pub struct EveryInterval {
    interval: Duration,
}

impl EveryInterval {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }
}

impl EmissionStrategy for EveryInterval {
    fn should_emit(&self, events_seen: u64, period_elapsed: Option<Duration>) -> bool {
        events_seen > 0 && period_elapsed.is_some_and(|elapsed| elapsed >= self.interval)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        Some(self.interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_once_the_interval_passes_and_keeps_the_state() {
        let strategy = EveryInterval::new(Duration::from_millis(10));

        assert!(!strategy.should_emit(1, Some(Duration::from_millis(9))));
        assert!(strategy.should_emit(1, Some(Duration::from_millis(10))));
        assert!(!strategy.should_emit(0, Some(Duration::from_secs(1))));
        assert!(!strategy.resets_accumulator_on_emit());
        assert!(!strategy.emits_only_at_end_of_input());
        assert_eq!(
            strategy.emit_interval_hint(),
            Some(Duration::from_millis(10))
        );
    }
}
