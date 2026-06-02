// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for stateful processing stages
//!
//! Examples: Aggregators, windowing operations, session tracking

use crate::effects::{EffectInvocationContext, Effects};
use crate::stages::common::handler_error::HandlerError;
use async_trait::async_trait;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::ChainEvent;
use std::borrow::Cow;
use std::time::Duration;

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
/// use obzenflow_runtime::stages::common::handlers::StatefulHandler;
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
///     fn should_emit(&self, state: &mut Self::State) -> bool {
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
    ///
    /// `Ok(events)` means emission succeeded. `Err(HandlerError)` means a
    /// per-record failure occurred while creating outputs (e.g. IO or
    /// encoding problems); the supervisor will turn this into an
    /// error-marked event and continue running the stage.
    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    // --- Advanced methods with sensible defaults ---

    /// Optional idle tick interval hint for the supervisor.
    ///
    /// When set, the supervisor may periodically call `should_emit` even when no new input events
    /// arrive, enabling time-based emission.
    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    /// Check if we should transition from Accumulating to Emitting
    ///
    /// Default: false (only emit on drain, i.e., OnEOF behavior)
    /// Override this OR use .with_emission() for other strategies
    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    /// Emit the aggregated result (called in Emitting state)
    ///
    /// Default: Calls create_events()
    /// Override only if you need to modify state during emission
    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    /// Emit final result during shutdown (called in Draining state)
    ///
    /// Default: Calls create_events()
    /// Override only if drain behavior differs from normal emission
    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }
}

#[doc(hidden)]
#[async_trait]
pub trait UnifiedStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
    ) -> std::result::Result<(), HandlerError>;

    fn initial_state(&self) -> Self::State;

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("1")
    }
}

#[async_trait]
impl<T: StatefulHandler + Send + Sync> UnifiedStatefulHandler for T {
    type State = T::State;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
    ) -> std::result::Result<(), HandlerError> {
        StatefulHandler::accumulate(self, state, event);
        Ok(())
    }

    fn initial_state(&self) -> Self::State {
        StatefulHandler::initial_state(self)
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::create_events(self, state)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        StatefulHandler::emit_interval_hint(self)
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        StatefulHandler::should_emit(self, state)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::emit(self, state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        StatefulHandler::drain(self, state).await
    }
}

#[async_trait]
pub trait EffectfulStatefulHandler: Send + Sync {
    type State: Clone + Send + Sync;
    type Input: TypedPayload + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        fx: &mut Effects,
    ) -> std::result::Result<(), HandlerError>;

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    fn emit_interval_hint(&self) -> Option<Duration> {
        None
    }

    fn should_emit(&self, _state: &mut Self::State) -> bool {
        false
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.create_events(state)
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("1")
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct EffectfulStatefulHandlerAdapter<H>(pub H);

#[async_trait]
impl<H> UnifiedStatefulHandler for EffectfulStatefulHandlerAdapter<H>
where
    H: EffectfulStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type State = H::State;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
    ) -> std::result::Result<(), HandlerError> {
        let input = H::Input::try_from_event(&event)
            .map_err(|e| HandlerError::Deserialization(e.to_string()))?;
        let effect_context = effect_context.ok_or_else(|| {
            HandlerError::Other(
                "effectful stateful handler invoked without effect context".to_string(),
            )
        })?;
        let mut draft = state.clone();
        let mut fx = Effects::new(effect_context);
        self.0.accumulate(&mut draft, input, &mut fx).await?;
        *state = draft;
        Ok(())
    }

    fn initial_state(&self) -> Self::State {
        self.0.initial_state()
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.0.create_events(state)
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        self.0.emit_interval_hint()
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        self.0.should_emit(state)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.0.emit(state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.0.drain(state).await
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        self.0.stage_logic_version()
    }
}
