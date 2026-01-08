//! Join handler trait
//!
//! Defines the interface for join handlers that process events from two upstream sources
//! (reference and stream) and emit enriched events.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::join::config::{JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP};
use async_trait::async_trait;
use obzenflow_core::{ChainEvent, StageId, WriterId};

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
/// 3. `on_source_eof()` - Optional EOF hook (invoked for stream EOF in Live mode)
/// 4. `drain()` - Called when the join is completing
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
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    /// Join reference mode (default: `FiniteEof`).
    ///
    /// Exposed on the handler so typed join builders can opt into Live mode without
    /// changing DSL macros.
    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::FiniteEof
    }

    /// Live-mode fairness cap (default: `Some(DEFAULT_REFERENCE_BATCH_CAP)`).
    ///
    /// When the join is in Live mode, after N reference events without a stream event
    /// the supervisor prioritizes polling the stream to prevent starvation.
    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    /// Handle EOF from a specific upstream source
    ///
    /// Different join strategies handle this differently:
    /// - InnerJoin: No-op (all processing happens during normal event flow)
    /// - LeftJoin: May emit unmatched records if reference EOF
    /// - StrictJoin: Verify all records matched
    ///
    /// Live mode (FLOWIP-084g):
    /// - Invoked for *stream EOF* only (stream drives completion).
    /// - Not invoked for reference EOF (reference may be infinite).
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
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    /// Final drain when the join is completing
    ///
    /// This is the final chance to emit any remaining events.
    /// In Live mode, the join completes on stream EOF, even if reference never EOFs.
    ///
    /// # Arguments
    /// - `state`: Reference to handler state
    ///
    /// # Returns
    /// Final events to emit (usually empty for joins)
    async fn drain(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }
}
