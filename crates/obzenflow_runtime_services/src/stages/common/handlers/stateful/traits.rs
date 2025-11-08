//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use obzenflow_core::{ChainEvent, Result};
use obzenflow_core::event::ChainEventContent;
use async_trait::async_trait;

/// Handler for stateful processing stages
///
/// Stateful handlers maintain internal state across events and use FSM states
/// to control when accumulated results are written to the journal.
///
/// Key principle: Accumulation (processing many events) is separate from
/// emission (writing ONE aggregated event to the journal).
///
/// # FSM States
/// - `Accumulating`: Process events, update state, write NOTHING
/// - `Emitting`: Write ONE aggregated event to journal
/// - `Draining`: Handle EOF, emit final result
///
/// # Example
/// ```rust
/// use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;
/// use obzenflow_core::{ChainEvent, EventId, WriterId, Result};
/// use serde_json::json;
/// use async_trait::async_trait;
///
/// #[derive(Clone, Default)]
/// struct AggregatorState {
///     count: u64,
///     sum: f64,
///     events_since_emit: u64,
/// }
///
/// struct MetricsAggregator {
///     window_size: u64,
///     writer_id: WriterId,
/// }
///
/// #[async_trait]
/// impl StatefulHandler for MetricsAggregator {
///     type State = AggregatorState;
///
///     fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
///         let value = event.payload()["value"].as_f64().unwrap_or(0.0);
///         state.count += 1;
///         state.sum += value;
///         state.events_since_emit += 1;
///     }
///
///     fn should_emit(&self, state: &Self::State) -> bool {
///         state.events_since_emit >= self.window_size
///     }
///
///     fn emit(&self, state: &mut Self::State) -> Option<ChainEvent> {
///         if state.count == 0 {
///             return None;
///         }
///
///         let event = ChainEvent::data(
///             EventId::new(),
///             self.writer_id.clone(),
///             "metrics",
///             json!({
///                 "avg": state.sum / state.count as f64,
///                 "count": state.count,
///                 "window": state.events_since_emit,
///             })
///         );
///
///         // Reset window counter but keep running totals
///         state.events_since_emit = 0;
///
///         Some(event)
///     }
///
///     fn initial_state(&self) -> Self::State {
///         AggregatorState::default()
///     }
///
///     async fn drain(&self, state: &Self::State) -> Result<Option<ChainEvent>> {
///         // Emit final aggregation if we have data
///         Ok(self.emit(&mut state.clone()))
///     }
/// }
/// ```
#[async_trait]
pub trait StatefulHandler: Send + Sync {
    /// The internal state type
    type State: Clone + Send + Sync;
    
    /// Accumulate an event into the state (called in Accumulating state)
    ///
    /// This method updates the state with the new event but does NOT
    /// write anything to the journal. Journal writes only happen
    /// when the FSM transitions to Emitting state.
    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent);

    /// Check if we should transition from Accumulating to Emitting
    ///
    /// This is called after each accumulate() to determine if it's
    /// time to emit the aggregated result.
    fn should_emit(&self, state: &Self::State) -> bool;

    /// Emit the aggregated result (called in Emitting state)
    ///
    /// Returns aggregated events to write to the journal.
    /// - Most accumulators return ONE event (Reduce, TopN, TopNBy)
    /// - GroupBy returns one event per group
    /// - Conflate returns one event per key
    ///
    /// The state is mutable to allow resetting window counters while
    /// keeping running totals.
    fn emit(&self, state: &mut Self::State) -> Vec<ChainEvent>;

    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;

    /// Emit final result during shutdown (called in Draining state)
    ///
    /// Returns the final aggregated events if there's data to emit.
    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>>;
}