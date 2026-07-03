// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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

    /// FLOWIP-010 §7: called once at stage build with the build-resolved
    /// lineage policy. Handlers that create derived events store it; the
    /// default ignores it.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    /// Process an event from a specific upstream source
    ///
    /// # Arguments
    /// - `state`: Mutable reference to handler state
    /// - `event`: The event to process
    /// - `source_id`: The upstream edge that delivered this event (the join's
    ///   direct upstream stage, reference or stream side). This is topology
    ///   identity, not authorship: a forwarded event's original author is
    ///   preserved on `event.writer_id` for causal attribution and can be
    ///   read from there if a handler needs semantic origin
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

/// Unified join surface used by the join stage supervisor.
///
/// `process_event` takes the per-delivery middleware execution scope computed
/// by the supervisor at dispatch (FLOWIP-120n); handlers without middleware
/// ignore it.
#[doc(hidden)]
#[async_trait]
pub trait UnifiedJoinHandler: Send + Sync {
    type State: Clone + Send + Sync;

    /// FLOWIP-010 §7: forwarded to the wrapped handler at stage build.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}

    fn initial_state(&self) -> Self::State;

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::FiniteEof
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;
}

#[async_trait]
impl<T: JoinHandler> UnifiedJoinHandler for T {
    type State = T::State;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        JoinHandler::install_lineage_policy(self, policy)
    }

    fn initial_state(&self) -> Self::State {
        JoinHandler::initial_state(self)
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        JoinHandler::process_event(self, state, event, source_id, writer_id)
    }

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinHandler::reference_mode(self)
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        JoinHandler::reference_batch_cap(self)
    }

    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        JoinHandler::on_source_eof(self, state, source_id, writer_id)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        JoinHandler::drain(self, state).await
    }
}
