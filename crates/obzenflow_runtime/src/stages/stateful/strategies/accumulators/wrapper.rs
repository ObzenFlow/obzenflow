// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: StatefulWithEmission Wrapper
//
// Combines an Accumulator with an EmissionStrategy to implement the StatefulHandler trait.
// This is the bridge between the composable primitives and the existing infrastructure.

use super::trace::TraceState;
use super::Accumulator;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use crate::typing::StatefulTyping;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::ChainEvent;
use std::fmt::Debug;
use std::time::{Duration, Instant};

/// Apply the wrapper's whole-batch trace to root-causality outputs.
///
/// Built-in accumulators author their own per-output causality and correlation, so
/// their emitted events are non-root and are left untouched here. Any output left at
/// root causality (the common case for a custom single-output accumulator that just
/// calls `ChainEventFactory::data_event`) is stamped with the full contributing
/// input frontier and a uniform-or-mixed correlation reconciliation. This is exact
/// for single-output emission and intentionally over-attributes, rather than
/// dropping parents, for multi-output custom accumulators, which should author their
/// own per-output causality instead.
///
/// The frontier is already bounded: `TraceState::record_event` caps `parent_ids` and
/// the distinct correlation set at the lineage-depth limit.
fn apply_batch_trace(trace: &TraceState, events: &mut [ChainEvent]) {
    let parent_ids = trace.parent_ids();
    if parent_ids.is_empty() {
        return;
    }

    for event in events.iter_mut() {
        if event.is_lifecycle() || event.is_control() {
            continue;
        }

        // Leave accumulator-authored provenance alone; only stamp root outputs.
        if !event.causality.is_root() {
            continue;
        }

        event.causality = CausalityContext {
            parent_ids: parent_ids.clone(),
        };

        if event.correlation_id.is_none() && event.correlation_ids.is_none() {
            if let Some(ids) = trace.mixed_correlation_ids() {
                event.correlation_ids = Some(ids);
            } else {
                event.correlation_id = trace.correlation_id();
                event.correlation_payload = trace.correlation_payload();
            }
        }

        if event.replay_context.is_none() {
            event.replay_context = trace.replay_context();
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
    /// Whole-batch provenance trace of contributing inputs since the last reset.
    ///
    /// Used as a fallback to stamp lineage and correlation onto root-causality
    /// outputs from accumulators that do not author their own provenance. Its
    /// lifecycle is coupled to the accumulator's: it resets exactly when the
    /// accumulator resets (`EmissionStrategy::resets_accumulator_on_emit`).
    ///
    /// Scoped to the crate: exposing the trace type publicly is a deferred API
    /// decision (see FLOWIP-054j), so this field is not part of the public surface.
    pub(crate) trace: TraceState,
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

        // Record the input into the whole-batch fallback trace before handing it to
        // the accumulator. `record_event` skips lifecycle/control events itself.
        state.trace.record_event(&event);

        // Accumulate the event
        self.accumulator.accumulate(&mut state.inner, event);
        state.events_seen += 1;
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        self.initial_emission.emit_interval_hint()
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state
            .emission
            .should_emit(state.events_seen, state.last_emit)
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Get the aggregated events from accumulator
        let mut events = self.accumulator.emit(&state.inner);

        apply_batch_trace(&state.trace, &mut events);

        if state.emission.resets_accumulator_on_emit() {
            self.accumulator.reset(&mut state.inner);
            state.trace.reset();
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
            trace: TraceState::default(),
        }
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let mut events = self.accumulator.emit(&state.inner);
        apply_batch_trace(&state.trace, &mut events);
        Ok(events)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Always emit remaining state on drain if we have any
        let mut events = self.accumulator.emit(&state.inner);
        apply_batch_trace(&state.trace, &mut events);
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
mod time_window_tests {
    use super::*;
    use crate::stages::stateful::strategies::emissions::TimeWindow;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde_json::json;
    use std::time::Duration;

    #[derive(Clone, Debug)]
    struct Counter;

    impl Accumulator for Counter {
        type State = u64;

        fn accumulate(&self, state: &mut Self::State, _event: ChainEvent) {
            *state += 1;
        }

        fn initial_state(&self) -> Self::State {
            0
        }

        fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
            vec![ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.counter",
                json!({ "count": state }),
            )]
        }

        fn reset(&self, state: &mut Self::State) {
            *state = 0;
        }
    }

    #[test]
    fn stateful_with_emission_should_emit_persists_time_window_state() {
        let handler =
            StatefulWithEmission::new(Counter, TimeWindow::new(Duration::from_millis(20)));
        let mut state = handler.initial_state();

        assert!(
            !handler.should_emit(&mut state),
            "expected initial should_emit to be false"
        );

        std::thread::sleep(Duration::from_millis(30));

        assert!(
            handler.should_emit(&mut state),
            "expected should_emit to become true after duration has elapsed"
        );
    }

    #[test]
    fn stateful_with_emission_emit_within_resets_accumulator_after_periodic_emit() {
        let mut handler =
            StatefulWithEmission::new(Counter, TimeWindow::new(Duration::from_millis(20)));
        let mut state = handler.initial_state();

        for _ in 0..3 {
            handler.accumulate(
                &mut state,
                ChainEventFactory::data_event(
                    WriterId::from(StageId::new()),
                    "test.in",
                    json!({ "n": 1 }),
                ),
            );
        }

        let _ = handler.emit(&mut state).expect("expected emit to succeed");

        assert_eq!(
            state.inner, 0,
            "expected accumulator state to reset after a periodic time-window emit"
        );
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
        assert!(!handler.should_emit(&mut state)); // OnEOF doesn't emit during processing

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "category": "B" }),
        );
        handler.accumulate(&mut state, event2);
        assert!(!handler.should_emit(&mut state));

        // Send EOF to trigger emission
        let eof_event = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        handler.accumulate(&mut state, eof_event);
        assert!(handler.should_emit(&mut state)); // Should emit after EOF

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
            assert!(!handler.should_emit(&mut state));
        }

        // Process event 3: should emit
        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));
        handler.accumulate(&mut state, event);
        assert!(handler.should_emit(&mut state)); // Should emit after 3 events

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
        assert!(handler.should_emit(&mut state));

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
        assert!(handler.should_emit(&mut state)); // Should always emit

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

#[cfg(test)]
mod batch_trace_tests {
    use super::*;
    use crate::stages::stateful::strategies::emissions::{OnEOF, TimeWindow};
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde_json::json;
    use std::time::Duration;

    /// A custom single-output accumulator that emits a root-causality event and does
    /// not author any provenance, exercising the wrapper's batch-trace fallback.
    #[derive(Clone, Debug)]
    struct RootCounter;

    impl Accumulator for RootCounter {
        type State = u64;

        fn accumulate(&self, state: &mut Self::State, _event: ChainEvent) {
            *state += 1;
        }

        fn initial_state(&self) -> Self::State {
            0
        }

        fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
            vec![ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "root.counter",
                json!({ "count": state }),
            )]
        }

        fn reset(&self, state: &mut Self::State) {
            *state = 0;
        }
    }

    fn input_event() -> ChainEvent {
        ChainEventFactory::data_event(WriterId::from(StageId::new()), "in", json!({ "n": 1 }))
    }

    #[test]
    fn batch_trace_fallback_parents_root_output_to_all_inputs() {
        let mut handler = StatefulWithEmission::new(RootCounter, OnEOF::new());
        let mut state = handler.initial_state();

        // Three inputs, each with a distinct correlation id (mixed fan-in).
        let mut ids = Vec::new();
        for _ in 0..3 {
            let event = input_event().with_new_correlation("src");
            ids.push(event.id);
            handler.accumulate(&mut state, event);
        }

        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        handler.accumulate(&mut state, eof);
        assert!(handler.should_emit(&mut state));

        let emitted = handler.emit(&mut state).expect("emit should succeed");
        assert_eq!(emitted.len(), 1);
        let out = &emitted[0];

        // Full contributing frontier, not just the last input.
        assert_eq!(out.causality.parent_ids, ids);
        // Mixed correlation must not collapse to one arbitrary scalar.
        assert!(out.correlation_id.is_none());
        let recorded = out
            .correlation_ids
            .as_ref()
            .expect("mixed fan-in must record a correlation set");
        assert_eq!(recorded.len(), 3);
    }

    #[test]
    fn batch_trace_fallback_carries_uniform_correlation_as_scalar() {
        let mut handler = StatefulWithEmission::new(RootCounter, OnEOF::new());
        let mut state = handler.initial_state();

        // Seed one correlation and reuse it for every input (uniform fan-in).
        let seed = input_event().with_new_correlation("src");
        let correlation = seed.correlation_id;
        let payload = seed.correlation_payload.clone();
        assert!(correlation.is_some());

        let mut ids = Vec::new();
        for _ in 0..3 {
            let mut event = input_event();
            event.correlation_id = correlation;
            event.correlation_payload = payload.clone();
            ids.push(event.id);
            handler.accumulate(&mut state, event);
        }

        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        handler.accumulate(&mut state, eof);
        let emitted = handler.emit(&mut state).expect("emit should succeed");
        assert_eq!(emitted.len(), 1);
        let out = &emitted[0];

        assert_eq!(out.causality.parent_ids, ids);
        assert_eq!(out.correlation_id, correlation);
        assert!(out.correlation_ids.is_none());
    }

    #[test]
    fn batch_trace_resets_with_accumulator_on_tumbling_emit() {
        // TimeWindow is a tumbling strategy (resets_accumulator_on_emit == true).
        // Drive emit() directly: the reset coupling under test lives in emit(), not
        // in the should_emit timing (which the time-window tests cover separately).
        let mut handler =
            StatefulWithEmission::new(RootCounter, TimeWindow::new(Duration::from_millis(10)));
        let mut state = handler.initial_state();

        // First window: two inputs.
        handler.accumulate(&mut state, input_event());
        handler.accumulate(&mut state, input_event());
        let first = handler.emit(&mut state).expect("first emit should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].causality.parent_ids.len(), 2);

        // Second window: one new input. The trace must have reset with the
        // accumulator, so the output is parented only to this window's input.
        let third = input_event();
        let third_id = third.id;
        handler.accumulate(&mut state, third);
        let second = handler
            .emit(&mut state)
            .expect("second emit should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].causality.parent_ids, vec![third_id]);
    }
}
