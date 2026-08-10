// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: EmitAlways Emission Strategy
//
// Emits results after processing every event, useful for materialized views
// and real-time dashboards.

use super::EmissionStrategy;
use std::time::Duration;

/// Emit after every event (materialized view).
///
/// This strategy emits results immediately after processing each event.
/// Useful for creating real-time materialized views or dashboards that
/// need to update with every change.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::emissions::EmitAlways;
///
/// let strategy = EmitAlways;
/// assert!(strategy.should_emit(1, None));   // Always true
/// assert!(strategy.should_emit(100, None)); // Always true
/// ```
#[derive(Debug, Clone, Copy)]
pub struct EmitAlways;

impl EmissionStrategy for EmitAlways {
    fn should_emit(&self, events_seen: u64, _period_elapsed: Option<Duration>) -> bool {
        events_seen > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emit_always_always_returns_true() {
        let strategy = EmitAlways;

        assert!(!strategy.should_emit(0, None));
        assert!(strategy.should_emit(1, None));
        assert!(strategy.should_emit(100, None));
        assert!(strategy.should_emit(1000, None));

        assert!(!strategy.should_emit(0, Some(Duration::ZERO)));
        assert!(strategy.should_emit(100, Some(Duration::ZERO)));
    }
}
