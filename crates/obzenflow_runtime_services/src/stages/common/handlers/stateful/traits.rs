//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use async_trait::async_trait;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, Result};

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
/// ```ignore
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

    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;

    /// Transform accumulated state into output events
    ///
    /// Called when emission is triggered (by emission strategy or drain).
    /// Return the events you want to emit based on current state.
    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent>;

    // --- Advanced methods with sensible defaults ---

    /// Check if we should transition from Accumulating to Emitting
    ///
    /// Default: false (only emit on drain, i.e., OnEOF behavior)
    /// Override this OR use .with_emission() for other strategies
    fn should_emit(&self, _state: &Self::State) -> bool {
        false
    }

    /// Emit the aggregated result (called in Emitting state)
    ///
    /// Default: Calls create_events()
    /// Override only if you need to modify state during emission
    fn emit(&self, state: &mut Self::State) -> Vec<ChainEvent> {
        self.create_events(state)
    }

    /// Emit final result during shutdown (called in Draining state)
    ///
    /// Default: Calls create_events()
    /// Override only if drain behavior differs from normal emission
    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>> {
        Ok(self.create_events(state))
    }
}
