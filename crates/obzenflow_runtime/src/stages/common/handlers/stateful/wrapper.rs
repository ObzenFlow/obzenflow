// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Wrapper for composing StatefulHandler with emission strategies

use super::traits::StatefulHandler;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::stateful::strategies::emissions::{EmissionStrategy, OnEOF};
use crate::typing::StatefulTyping;
use async_trait::async_trait;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::ChainEvent;
use std::time::Instant;

/// State wrapper that includes handler state and emission tracking
#[derive(Clone, Debug)]
pub struct HandlerWithEmissionState<S, E>
where
    E: EmissionStrategy,
{
    /// The handler's state
    pub handler_state: S,
    /// The emission strategy (stateful, tracks its own state)
    pub emission: E,
    /// Number of events seen
    pub events_seen: u64,
    /// Last emission timestamp
    pub last_emit: Option<Instant>,
}

/// Wrapper that combines a StatefulHandler with an emission strategy
#[derive(Clone, Debug)]
pub struct StatefulHandlerWithEmission<H, E>
where
    H: StatefulHandler + Clone,
    E: EmissionStrategy + Clone,
{
    handler: H,
    initial_emission: E,
}

impl<H> StatefulHandlerWithEmission<H, OnEOF>
where
    H: StatefulHandler + Clone,
{
    /// Create with default OnEOF emission
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            initial_emission: OnEOF::new(),
        }
    }
}

impl<H, E> StatefulHandlerWithEmission<H, E>
where
    H: StatefulHandler + Clone,
    E: EmissionStrategy + Clone,
{
    /// Create with custom emission strategy
    pub fn new_with_emission(handler: H, emission: E) -> Self {
        Self {
            handler,
            initial_emission: emission,
        }
    }

    /// Change the emission strategy
    pub fn with_emission<E2>(self, emission: E2) -> StatefulHandlerWithEmission<H, E2>
    where
        E2: EmissionStrategy + Clone,
    {
        StatefulHandlerWithEmission {
            handler: self.handler,
            initial_emission: emission,
        }
    }
}

/// Extension trait to add .with_emission() to any StatefulHandler
pub trait StatefulHandlerExt: StatefulHandler + Clone + Sized {
    /// Wrap with emission strategy
    fn with_emission<E>(self, emission: E) -> StatefulHandlerWithEmission<Self, E>
    where
        E: EmissionStrategy + Clone,
    {
        StatefulHandlerWithEmission::new_with_emission(self, emission)
    }

    /// Use default OnEOF emission
    fn with_default_emission(self) -> StatefulHandlerWithEmission<Self, OnEOF> {
        StatefulHandlerWithEmission::new(self)
    }
}

// Implement for all StatefulHandlers that are Clone
impl<T> StatefulHandlerExt for T where T: StatefulHandler + Clone {}

// Make the wrapper implement StatefulHandler
#[async_trait]
impl<H, E> StatefulHandler for StatefulHandlerWithEmission<H, E>
where
    H: StatefulHandler + Clone + Send + Sync + 'static,
    E: EmissionStrategy + Clone + Send + Sync + 'static,
    H::State: 'static,
{
    type State = HandlerWithEmissionState<H::State, E>;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        // Check for EOF
        let is_eof = match &event.content {
            ChainEventContent::FlowControl(signal) => {
                matches!(signal, FlowControlPayload::Eof { .. })
            }
            _ => false,
        };

        if is_eof {
            state.emission.notify_eof();
        } else {
            // Let handler accumulate
            self.handler.accumulate(&mut state.handler_state, event);
            state.events_seen += 1;
        }
    }

    fn initial_state(&self) -> Self::State {
        HandlerWithEmissionState {
            handler_state: self.handler.initial_state(),
            emission: self.initial_emission.clone(),
            events_seen: 0,
            last_emit: None,
        }
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.handler.create_events(&state.handler_state)
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        // Use emission strategy
        let mut emission = state.emission.clone();
        emission.should_emit(state.events_seen, state.last_emit)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Update last emit time
        state.last_emit = Some(Instant::now());

        // Reset emission strategy
        state.emission.reset();

        // Use handler's emit
        self.handler.emit(&mut state.handler_state)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        self.handler.drain(&state.handler_state).await
    }
}

impl<H, E> StatefulTyping for StatefulHandlerWithEmission<H, E>
where
    H: StatefulHandler + Clone + StatefulTyping,
    E: EmissionStrategy + Clone,
{
    type Input = H::Input;
    type Output = H::Output;
}
