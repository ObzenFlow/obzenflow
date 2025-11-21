// FLOWIP-080c: EmitAlways Emission Strategy
//
// Emits results after processing every event, useful for materialized views
// and real-time dashboards.

use super::EmissionStrategy;
use std::time::Instant;

/// Emit after every event (materialized view).
///
/// This strategy emits results immediately after processing each event.
/// Useful for creating real-time materialized views or dashboards that
/// need to update with every change.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::emissions::EmitAlways;
///
/// let mut strategy = EmitAlways;
/// assert!(strategy.should_emit(1, None));   // Always true
/// assert!(strategy.should_emit(100, None)); // Always true
/// ```
#[derive(Debug, Clone, Copy)]
pub struct EmitAlways;

impl EmissionStrategy for EmitAlways {
    fn should_emit(&mut self, _events_seen: u64, _last_emit: Option<Instant>) -> bool {
        true // Always emit
    }

    fn reset(&mut self) {
        // No state to reset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emit_always_always_returns_true() {
        let mut strategy = EmitAlways;

        assert!(strategy.should_emit(0, None));
        assert!(strategy.should_emit(1, None));
        assert!(strategy.should_emit(100, None));
        assert!(strategy.should_emit(1000, None));

        let now = Instant::now();
        assert!(strategy.should_emit(0, Some(now)));
        assert!(strategy.should_emit(100, Some(now)));
    }

    #[test]
    fn test_emit_always_reset_does_nothing() {
        let mut strategy = EmitAlways;

        assert!(strategy.should_emit(1, None));
        strategy.reset();
        assert!(strategy.should_emit(1, None)); // Still returns true
    }
}
