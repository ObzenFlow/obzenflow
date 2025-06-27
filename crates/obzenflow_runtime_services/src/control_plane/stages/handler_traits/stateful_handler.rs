//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use obzenflow_core::{ChainEvent, Result};
use async_trait::async_trait;

/// Handler for stateful processing stages
/// 
/// Stateful handlers maintain internal state across events:
/// - Aggregations (sum, count, average)
/// - Windows (time-based, count-based)  
/// - Sessions (user sessions, connection tracking)
/// 
/// # Example
/// ```rust
/// #[derive(Clone, Default)]
/// struct AggregatorState {
///     count: u64,
///     sum: f64,
/// }
/// 
/// struct MetricsAggregator {
///     window_size: u64,
/// }
/// 
/// impl StatefulHandler for MetricsAggregator {
///     type State = AggregatorState;
///     
///     fn process(&self, state: &Self::State, event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
///         let value = event.payload["value"].as_f64().unwrap_or(0.0);
///         
///         // Create NEW state (functional approach)
///         let new_state = AggregatorState {
///             count: state.count + 1,
///             sum: state.sum + value,
///         };
///         
///         // Check if window is complete
///         if new_state.count >= self.window_size {
///             // Emit aggregation and reset
///             let output = vec![ChainEvent::new("metrics", json!({
///                 "avg": new_state.sum / new_state.count as f64,
///             }))];
///             (AggregatorState::default(), output)
///         } else {
///             (new_state, vec![])
///         }
///     }
///     
///     fn initial_state(&self) -> Self::State {
///         AggregatorState::default()
///     }
/// }
/// ```
#[async_trait]
pub trait StatefulHandler: Send + Sync {
    /// The internal state type
    type State: Clone + Send + Sync;
    
    /// Process an event with current state, returning new state and outputs
    /// 
    /// Pure function: (State, Event) → (State', Output)
    /// This enables time-travel debugging and easy testing
    fn process(&self, state: &Self::State, event: ChainEvent) -> (Self::State, Vec<ChainEvent>);
    
    /// Get the initial state for this handler
    fn initial_state(&self) -> Self::State;
    
    /// Perform any cleanup during shutdown
    /// 
    /// For stateful handlers, this might flush pending aggregations,
    /// emit final window results, or close session tracking.
    async fn drain(&mut self, _state: &Self::State) -> Result<Vec<ChainEvent>> {
        Ok(vec![]) // Default: no special drain logic
    }
}