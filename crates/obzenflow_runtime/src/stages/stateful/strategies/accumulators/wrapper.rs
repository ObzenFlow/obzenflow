// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: StatefulWithEmission Wrapper
//
// Combines an Accumulator with an EmissionStrategy to implement the StatefulHandler trait.
// This is the bridge between the composable primitives and the existing infrastructure.

use super::Accumulator;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use crate::typing::StatefulTyping;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::ReplayContext;
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::CorrelationId;
use obzenflow_core::ChainEvent;
use obzenflow_core::EventId;
use std::fmt::Debug;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct LineageParent {
    pub id: EventId,
    pub causality: CausalityContext,
    pub correlation_id: Option<CorrelationId>,
    pub correlation_payload: Option<CorrelationPayload>,
    pub replay_context: Option<ReplayContext>,
}

impl LineageParent {
    fn capture(event: &ChainEvent) -> Option<Self> {
        if event.is_lifecycle() || event.is_control() {
            return None;
        }

        Some(Self {
            id: event.id,
            causality: event.causality.clone(),
            correlation_id: event.correlation_id,
            correlation_payload: event.correlation_payload.clone(),
            replay_context: event.replay_context.clone(),
        })
    }
}

fn propagate_lineage(parent: &LineageParent, events: &mut [ChainEvent]) {
    // Match `ChainEventFactory::derived_event` defaults.
    const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;

    let max_depth = std::env::var("OBZENFLOW_MAX_LINEAGE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_LINEAGE_DEPTH);

    for event in events.iter_mut() {
        if event.is_lifecycle() || event.is_control() {
            continue;
        }

        if event.correlation_id.is_none() {
            event.correlation_id = parent.correlation_id;
            event.correlation_payload = parent.correlation_payload.clone();
        }

        if event.replay_context.is_none() {
            event.replay_context = parent.replay_context.clone();
        }

        if event.causality.is_root() {
            let mut causality = CausalityContext::with_parent(parent.id);

            // Propagate ancestors up to depth limit (parent already counts as depth=1).
            let ancestors_to_add = parent
                .causality
                .parent_ids
                .iter()
                .take(max_depth.saturating_sub(1));

            for ancestor in ancestors_to_add {
                causality = causality.add_parent(*ancestor);
            }

            event.causality = causality;
        }
    }
}

/// Wrapper state that includes accumulator state, emission strategy, and tracking.
#[derive(Clone, Debug)]
pub struct WrapperState<S, E>
where
    E: EmissionStrategy,
{
    /// The accumulator's state
    pub inner: S,
    /// The emission strategy (part of state so it can maintain its own state)
    pub emission: E,
    /// Number of events processed
    pub events_seen: u64,
    /// Last emission timestamp
    pub last_emit: Option<Instant>,
    /// Most recent non-control event, used to propagate correlation/lineage
    pub lineage_parent: Option<LineageParent>,
}

/// Combines an Accumulator with an EmissionStrategy to implement StatefulHandler.
///
/// This wrapper bridges the gap between the composable accumulator/emission
/// primitives and the existing StatefulHandler infrastructure from FLOWIP-080b.
///
/// # Type Parameters
///
/// * `A` - The accumulator type
/// * `E` - The emission strategy type
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::{GroupBy, StatefulWithEmission};
/// use obzenflow_runtime::stages::stateful::strategies::emissions::OnEOF;
///
/// let accumulator = GroupBy::new("user_id", |event, count: &mut u64| {
///     *count += 1;
/// });
/// let emission = OnEOF::new();
///
/// let handler = StatefulWithEmission::new(accumulator, emission);
/// ```
#[derive(Debug)]
pub struct StatefulWithEmission<A, E>
where
    A: Accumulator,
    E: EmissionStrategy,
{
    accumulator: A,
    initial_emission: E,
}

impl<A, E> StatefulWithEmission<A, E>
where
    A: Accumulator,
    E: EmissionStrategy,
{
    /// Create a new StatefulWithEmission handler.
    ///
    /// # Arguments
    ///
    /// * `accumulator` - The accumulator that defines aggregation logic
    /// * `emission` - The strategy that controls when to emit results
    pub fn new(accumulator: A, emission: E) -> Self {
        Self {
            accumulator,
            initial_emission: emission,
        }
    }
}

impl<A, E> StatefulTyping for StatefulWithEmission<A, E>
where
    A: Accumulator + StatefulTyping,
    E: EmissionStrategy,
{
    type Input = A::Input;
    type Output = A::Output;
}

#[async_trait::async_trait]
impl<A, E> StatefulHandler for StatefulWithEmission<A, E>
where
    A: Accumulator + Clone + 'static,
    E: EmissionStrategy + Clone + Debug + 'static,
    A::State: 'static,
{
    type State = WrapperState<A::State, E>;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        // Check for EOF control event
        let is_eof = match &event.content {
            ChainEventContent::FlowControl(signal) => {
                matches!(signal, FlowControlPayload::Eof { .. })
            }
            _ => false,
        };

        if is_eof {
            // Notify emission strategy about EOF
            state.emission.notify_eof();
            return; // Don't accumulate EOF events
        }

        if let Some(parent) = LineageParent::capture(&event) {
            state.lineage_parent = Some(parent);
        }

        // Accumulate the event
        self.accumulator.accumulate(&mut state.inner, event);
        state.events_seen += 1;
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        // Use mutable clone to check (some strategies need to update internal state)
        let mut emission = state.emission.clone();
        emission.should_emit(state.events_seen, state.last_emit)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Get the aggregated events from accumulator
        let mut events = self.accumulator.emit(&state.inner);

        if let Some(parent) = state.lineage_parent.as_ref() {
            propagate_lineage(parent, &mut events);
        }

        // Reset emission strategy
        state.emission.reset();
        state.last_emit = Some(Instant::now());

        // Return all events (GroupBy/Conflate emit multiple, others emit one)
        Ok(events)
    }

    fn initial_state(&self) -> Self::State {
        WrapperState {
            inner: self.accumulator.initial_state(),
            emission: self.initial_emission.clone(),
            events_seen: 0,
            last_emit: None,
            lineage_parent: None,
        }
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let mut events = self.accumulator.emit(&state.inner);

        if let Some(parent) = state.lineage_parent.as_ref() {
            propagate_lineage(parent, &mut events);
        }

        Ok(events)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Always emit remaining state on drain if we have any
        let mut events = self.accumulator.emit(&state.inner);

        if let Some(parent) = state.lineage_parent.as_ref() {
            propagate_lineage(parent, &mut events);
        }

        Ok(events)
    }
}

impl<A, E> Clone for StatefulWithEmission<A, E>
where
    A: Accumulator + Clone,
    E: EmissionStrategy + Clone,
{
    fn clone(&self) -> Self {
        Self {
            accumulator: self.accumulator.clone(),
            initial_emission: self.initial_emission.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::stateful::strategies::accumulators::{GroupBy, Reduce};
    use crate::stages::stateful::strategies::emissions::{EmitAlways, EveryN, OnEOF};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde::Serialize;
    use serde_json::json;

    #[derive(Clone, Debug, Default, Serialize)]
    struct TestStats {
        count: u64,
    }

    #[tokio::test]
    async fn test_wrapper_with_groupby_on_eof() {
        let accumulator = GroupBy::new("category", |_event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
        });
        let emission = OnEOF::new();
        let mut handler = StatefulWithEmission::new(accumulator, emission);

        // Initial state
        let mut state = handler.initial_state();

        // Process some events
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "category": "A" }),
        );
        handler.accumulate(&mut state, event1);
        assert!(!handler.should_emit(&state)); // OnEOF doesn't emit during processing

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "category": "B" }),
        );
        handler.accumulate(&mut state, event2);
        assert!(!handler.should_emit(&state));

        // Send EOF to trigger emission
        let eof_event = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        handler.accumulate(&mut state, eof_event);
        assert!(handler.should_emit(&state)); // Should emit after EOF

        // Emit should return aggregated events (one per group for GroupBy)
        let emitted = handler
            .emit(&mut state)
            .expect("StatefulWithEmission::emit should succeed in GroupBy EOF test");
        assert!(!emitted.is_empty());
        // GroupBy with 2 categories should emit 2 events
        assert_eq!(emitted.len(), 2);
    }

    #[tokio::test]
    async fn test_wrapper_with_reduce_every_n() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });
        let emission = EveryN::new(3);
        let mut handler = StatefulWithEmission::new(accumulator, emission);

        let mut state = handler.initial_state();

        // Process events 1-2: no emission
        for _ in 0..2 {
            let event =
                ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));
            handler.accumulate(&mut state, event);
            assert!(!handler.should_emit(&state));
        }

        // Process event 3: should emit
        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));
        handler.accumulate(&mut state, event);
        assert!(handler.should_emit(&state)); // Should emit after 3 events

        // Emit should return an event (Reduce emits one event)
        let emitted = handler
            .emit(&mut state)
            .expect("StatefulWithEmission::emit should succeed in EveryN test");
        assert!(!emitted.is_empty());
        assert_eq!(emitted.len(), 1);
    }

    #[test]
    fn test_wrapper_propagates_lineage_on_emit() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });
        let emission = EmitAlways;
        let mut handler = StatefulWithEmission::new(accumulator, emission);

        let mut state = handler.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "value": 1 }),
        )
        .with_new_correlation("test_source");
        let correlation_id = event.correlation_id;
        let parent_id = event.id;

        handler.accumulate(&mut state, event);
        assert!(handler.should_emit(&state));

        let mut emitted = handler
            .emit(&mut state)
            .expect("StatefulWithEmission::emit should succeed in lineage test");
        assert_eq!(emitted.len(), 1);

        let out = emitted.pop().expect("expected output event");
        assert_eq!(out.correlation_id, correlation_id);
        assert_eq!(out.causality.parent_ids, vec![parent_id]);
        assert!(out.correlation_payload.is_some());
    }

    #[test]
    fn test_wrapper_with_emit_always() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });
        let emission = EmitAlways;
        let mut handler = StatefulWithEmission::new(accumulator, emission);

        let mut state = handler.initial_state();

        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));

        // EmitAlways should emit after every event
        handler.accumulate(&mut state, event);
        assert!(handler.should_emit(&state)); // Should always emit

        // Emit should return an event (Reduce emits one event)
        let emitted = handler
            .emit(&mut state)
            .expect("StatefulWithEmission::emit should succeed in EmitAlways test");
        assert!(!emitted.is_empty());
        assert_eq!(emitted.len(), 1);
    }

    #[test]
    fn test_wrapper_clone() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });
        let emission = OnEOF::new();
        let handler = StatefulWithEmission::new(accumulator, emission);

        let _cloned = handler.clone();
        // Should compile and not panic
    }
}
