//! Join handler trait
//!
//! Defines the interface for join handlers that process events from two upstream sources
//! (reference and stream) and emit enriched events.

use async_trait::async_trait;
use obzenflow_core::{ChainEvent, Result, StageId, WriterId};

/// Handler trait for join stages - defines the enrichment logic
///
/// This trait encapsulates the pure join logic (key extraction, matching, output creation)
/// and is independent of the FSM/supervisor machinery that handles per-source EOF.
///
/// # Type Parameters
/// - `State`: The internal state type (catalogs, buffers, etc.)
///
/// # Lifecycle
/// 1. `initial_state()` - Create empty state when join starts
/// 2. `process_event()` - Called for each event from either upstream
/// 3. `on_source_eof()` - Called when a specific upstream reaches EOF
/// 4. `drain()` - Called when all upstreams have reached EOF
#[async_trait]
pub trait JoinHandler: Send + Sync + Clone {
    /// Internal state type for catalogs and buffers
    type State: Clone + Send + Sync;

    /// Get initial state
    fn initial_state(&self) -> Self::State;

    /// Process an event from a specific upstream source
    ///
    /// # Arguments
    /// - `state`: Mutable reference to handler state
    /// - `event`: The event to process
    /// - `source_id`: Which upstream sent this event (reference or stream)
    /// - `writer_id`: The join stage's writer ID for creating output events
    ///
    /// # Returns
    /// Events to emit immediately (0+ joined results)
    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
    ) -> Vec<ChainEvent>;

    /// Handle EOF from a specific upstream source
    ///
    /// This is called when one of the upstreams reaches EOF. Different join strategies
    /// handle this differently:
    /// - InnerJoin: No-op (all processing happens during normal event flow)
    /// - LeftJoin: May emit unmatched records if reference EOF
    /// - StrictJoin: Verify all records matched
    ///
    /// # Arguments
    /// - `state`: Mutable reference to handler state
    /// - `source_id`: Which upstream reached EOF
    /// - `writer_id`: The join stage's writer ID for creating output events
    ///
    /// # Returns
    /// Events to emit (e.g., unmatched records for left join on reference EOF)
    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        writer_id: WriterId,
    ) -> Vec<ChainEvent>;

    /// Final drain when all upstreams are EOF
    ///
    /// This is called after both upstreams have reached EOF and is the final
    /// chance to emit any remaining events.
    ///
    /// # Arguments
    /// - `state`: Reference to handler state
    ///
    /// # Returns
    /// Final events to emit (usually empty for joins)
    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>> {
        Ok(vec![])
    }
}
