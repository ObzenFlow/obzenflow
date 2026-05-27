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
mod trace;
pub mod wrapper;

pub use conflate::{Conflate, ConflateTyped};
pub use group_by::{GroupBy, GroupByTyped};
pub use reduce::{Reduce, ReduceTyped};
pub use top_n::{TopN, TopNTyped};
pub use top_n_by::{TopNBy, TopNByTyped};
pub use wrapper::{StatefulWithEmission, WrapperState};

/// Sort emitted per-key aggregates into a deterministic, replay-stable order.
///
/// Multi-key accumulators (`GroupBy`, `Conflate`, and their typed variants) build
/// their outputs by iterating a `HashMap`, whose iteration order is not a contract,
/// so the same inputs could otherwise produce a different journal append order each
/// run. Sorting by the oldest contributing event id gives a deterministic order
/// that is stable across replay, because emitted aggregates retain their original
/// input event ids. Each input belongs to exactly one key, so the first-parent ids
/// are distinct across outputs and the resulting order is total.
///
/// This intentionally does not promise key order or arrival order. ULIDs are not
/// guaranteed monotonic within a millisecond, so id order is not necessarily the
/// order keys first appeared; the only guarantee is determinism under replay.
pub(crate) fn sort_emitted_by_first_parent(events: &mut [ChainEvent]) {
    events.sort_by(|a, b| {
        a.causality
            .parent_ids
            .first()
            .cmp(&b.causality.parent_ids.first())
    });
}

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
    ///
    /// # Provenance contract
    ///
    /// An accumulator that aggregates more than one input into an output (fan-in),
    /// or that emits more than one output per call (fan-out, for example one event
    /// per key), **must author each output's `causality.parent_ids` and correlation**
    /// from the inputs that actually contributed to that output. The built-in
    /// accumulators do this with per-bucket trace state.
    ///
    /// Outputs left at root causality are treated as 1:1 derivations and receive the
    /// `StatefulWithEmission` wrapper's whole-batch fallback trace: the full
    /// contributing input frontier (bounded by the lineage-depth limit) plus a
    /// uniform-or-mixed correlation reconciliation. That fallback is exact for a
    /// single-output accumulator but over-attributes for a multi-output one, since
    /// the wrapper cannot know how an emission batch partitions inputs into outputs.
    /// Multi-output accumulators must therefore author their own per-output
    /// causality rather than rely on the fallback.
    ///
    /// Accumulators that emit one event per key should also produce a deterministic,
    /// replay-stable order (see `sort_emitted_by_first_parent`), since `HashMap`
    /// iteration order is not a contract.
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
