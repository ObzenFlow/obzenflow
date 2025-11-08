// FLOWIP-080c: Emission Strategies
//
// This module provides strategies for controlling when accumulated results are emitted.
// Emission strategies define the "when" which can be combined with accumulators that
// define the "what" to create flexible stateful processing patterns.

use obzenflow_core::ChainEvent;
use std::fmt::Debug;
use std::time::{Duration, Instant};

// Re-export concrete strategies
mod on_eof;
mod every_n;
mod time_window;
mod emit_always;

pub use on_eof::OnEOF;
pub use every_n::EveryN;
pub use time_window::TimeWindow;
pub use emit_always::EmitAlways;

/// Strategy for when to emit accumulated results.
///
/// Emission strategies control the timing of result emission from accumulators.
/// They can be based on event count, time, EOF signals, or always emit.
///
/// # Examples
///
/// ```rust
/// use obzenflow_runtime_services::stages::stateful::emission::{EmissionStrategy, OnEOF};
///
/// let mut strategy = OnEOF::new();
/// assert!(!strategy.should_emit(100, None));  // Not at EOF yet
/// strategy.set_eof();
/// assert!(strategy.should_emit(100, None));   // Now at EOF
/// ```
pub trait EmissionStrategy: Send + Sync + Debug {
    /// Check if should emit based on current state.
    ///
    /// # Arguments
    ///
    /// * `events_seen` - Total number of events processed so far
    /// * `last_emit` - Time of last emission (if any)
    ///
    /// # Returns
    ///
    /// `true` if results should be emitted now, `false` otherwise
    fn should_emit(&mut self, events_seen: u64, last_emit: Option<Instant>) -> bool;

    /// Called after emission to reset tracking.
    ///
    /// This allows strategies to reset counters or state after emitting results.
    /// For example, EveryN resets its counter after emission.
    fn reset(&mut self);

    /// Check if this strategy requires EOF notification.
    ///
    /// Used by OnEOF strategy to know when the stream has completed.
    fn notify_eof(&mut self) {
        // Default implementation does nothing
        // Override in strategies that care about EOF
    }
}