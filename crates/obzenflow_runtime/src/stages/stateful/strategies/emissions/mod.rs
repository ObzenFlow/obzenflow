// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: Emission Strategies
//
// This module provides strategies for controlling when accumulated results are emitted.
// Emission strategies define the "when" which can be combined with accumulators that
// define the "what" to create flexible stateful processing patterns.

use std::fmt::Debug;
use std::time::Duration;

// Re-export concrete strategies
mod emit_always;
mod every_n;
mod on_eof;
mod time_window;

pub use emit_always::EmitAlways;
pub use every_n::EveryN;
pub use on_eof::OnEOF;
pub use time_window::TimeWindow;

/// Strategy for when to emit accumulated results.
///
/// Emission strategies control the timing of result emission from accumulators.
/// They can be based on event count, time, EOF signals, or always emit.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::emissions::{EmissionStrategy, OnEOF};
///
/// let strategy = OnEOF::new();
/// assert!(!strategy.should_emit(100, None));
/// ```
pub trait EmissionStrategy: Send + Sync + Debug {
    /// Check if should emit based on current state.
    ///
    /// # Arguments
    ///
    /// * `events_seen` - Total number of events processed so far
    /// * `period_elapsed` - Elapsed processing time since the first input in
    ///   the current period, supplied by wrapper state
    ///
    /// # Returns
    ///
    /// `true` if results should be emitted now, `false` otherwise
    fn should_emit(&self, events_seen: u64, period_elapsed: Option<Duration>) -> bool;

    /// Whether this strategy represents a tumbling boundary that should reset the accumulator.
    ///
    /// Default: false (snapshot-style emissions keep accumulating state).
    fn resets_accumulator_on_emit(&self) -> bool {
        false
    }

    /// Optional supervisor-driven idle tick interval for this strategy.
    ///
    /// When set, the stateful supervisor will periodically call `should_emit` even when no new
    /// input events arrive, enabling time-based emission.
    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }
}
