// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: Stateful Transform Primitives - Core Accumulator Trait
//
// This module provides composable accumulator primitives for common aggregation patterns.
// Accumulators define the "what" (accumulation logic) which can be combined with
// emission strategies that define the "when" (emission timing).

use obzenflow_core::ChainEvent;
use std::fmt::Debug;

// Re-export concrete accumulators
pub mod conflate;
pub mod group_by;
pub mod reduce;
pub mod top_n;
pub mod top_n_by;
pub mod wrapper;

pub use conflate::{Conflate, ConflateTyped};
pub use group_by::{GroupBy, GroupByTyped};
pub use reduce::{Reduce, ReduceTyped};
pub use top_n::{TopN, TopNTyped};
pub use top_n_by::{TopNBy, TopNByTyped};
pub use wrapper::{StatefulWithEmission, WrapperState};

/// Core trait for accumulation logic.
///
/// Accumulators define how to aggregate events into state and how to emit
/// that state as output events. They are composable with emission strategies
/// to control when results are emitted.
///
/// # Type Parameters
///
/// * `State` - The accumulated state type, must be Clone + Send + Sync
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::Accumulator;
///
/// struct Counter;
///
/// impl Accumulator for Counter {
///     type State = u64;
///
///     fn accumulate(&self, state: &mut Self::State, _event: ChainEvent) {
///         *state += 1;
///     }
///
///     fn initial_state(&self) -> Self::State {
///         0
///     }
///
///     fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
///         vec![ChainEvent::data(
///             EventId::new(),
///             WriterId::new(),
///             "count",
///             json!({ "total": state })
///         )]
///     }
/// }
/// ```
pub trait Accumulator: Send + Sync + Debug {
    /// The accumulated state type
    type State: Clone + Send + Sync + Debug;

    /// Accumulate an event into the state.
    ///
    /// This method mutates the provided state with information from the event.
    /// It should be a pure function with no side effects.
    fn accumulate(&self, state: &mut Self::State, event: ChainEvent);

    /// Get the initial empty state.
    ///
    /// This is called once when the accumulator is created to initialize
    /// the state that will be passed to accumulate().
    fn initial_state(&self) -> Self::State;

    /// Emit accumulated results as events.
    ///
    /// Converts the accumulated state into output events. This is called
    /// when the emission strategy determines it's time to emit results.
    fn emit(&self, state: &Self::State) -> Vec<ChainEvent>;

    /// Reset state after emission (for periodic accumulators).
    ///
    /// Called after emit() for windowed aggregations that need to start
    /// fresh for the next window. The default implementation resets to
    /// initial_state().
    fn reset(&self, state: &mut Self::State) {
        *state = self.initial_state();
    }
}
