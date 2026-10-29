// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful accumulator/emission composition.
//!
//! FLOWIP-134e makes the emission policy immutable configuration. All cadence
//! evolution lives in [`WrapperState`], alongside the accumulator's domain state.

use super::trace::TraceState;
use crate::stages::common::handlers::stateful::{
    StatefulEmission, TypedStatefulContribution, TypedStatefulDrainInvocation,
    TypedStatefulHandler, TypedStatefulInvocation, TypedStatefulOutputIds,
};
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use obzenflow_core::{OneFactStageOutput, TypedPayload};
use std::fmt::Debug;
use std::time::{Duration, Instant};

type AccumulatorTransition<O> = (StatefulEmission<O>, Option<Vec<TraceState>>);

/// Explicit domain and cadence state for an accumulator strategy.
#[derive(Clone, Debug)]
pub struct WrapperState<S> {
    pub inner: S,
    pub events_seen: u64,
    pub period_started_at: Option<Instant>,
}

impl<S> WrapperState<S> {
    fn period_elapsed(&self) -> Option<Duration> {
        self.period_started_at
            .map(|started| Instant::now().saturating_duration_since(started))
    }

    fn record_input(&mut self) {
        if self.period_started_at.is_none() {
            self.period_started_at = Some(Instant::now());
        }
        self.events_seen = self.events_seen.saturating_add(1);
    }

    fn advance_period(&mut self) {
        self.events_seen = 0;
        self.period_started_at = None;
    }
}

/// Combines an accumulator with immutable emission configuration.
#[derive(Debug)]
pub struct StatefulWithEmission<A, E> {
    accumulator: A,
    emission: E,
}

impl<A, E> StatefulWithEmission<A, E>
where
    E: EmissionStrategy,
{
    pub fn new(accumulator: A, emission: E) -> Self {
        Self {
            accumulator,
            emission,
        }
    }
}

impl<A, E> Clone for StatefulWithEmission<A, E>
where
    A: Clone,
    E: Clone,
{
    fn clone(&self) -> Self {
        Self {
            accumulator: self.accumulator.clone(),
            emission: self.emission.clone(),
        }
    }
}

/// A `group_by` or `conflate` with its emission strategy, before it names what
/// each emission publishes.
///
/// `group_by` takes `.updated_groups()` or `.all_groups()`, and `conflate`
/// takes `.updated_keys()` or `.all_keys()`. Each returns the
/// [`StatefulWithEmission`] handler, and without one there is no handler:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::stateful::{emit, group_by, TypedStatefulHandler};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Sale { store: String, amount: u64 }
/// # impl TypedPayload for Sale { const EVENT_TYPE: &'static str = "doc.sale"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct StoreTotal(u64);
/// # impl TypedPayload for StoreTotal { const EVENT_TYPE: &'static str = "doc.store_total"; }
///
/// fn stage(_: impl TypedStatefulHandler) {}
///
/// let totals = group_by(
///     |sale: &Sale| sale.store.clone(),
///     |total: &mut u64, sale: &Sale| *total += sale.amount,
///     |_: &String, total: &u64| StoreTotal(*total),
/// );
/// stage(totals.emit(emit::always));
/// ```
#[derive(Clone, Debug)]
pub struct KeyedEmission<A, E> {
    accumulator: A,
    emission: E,
}

impl<A, E> KeyedEmission<A, E> {
    pub(super) fn new(accumulator: A, emission: E) -> Self {
        Self {
            accumulator,
            emission,
        }
    }

    pub(super) fn into_parts(self) -> (A, E) {
        (self.accumulator, self.emission)
    }
}

/// One accumulator projection plus optional framework-owned contribution evidence.
#[doc(hidden)]
pub struct AccumulatorOutput<O> {
    pub(crate) output: O,
    pub(crate) trace: Option<TraceState>,
}

impl<O> AccumulatorOutput<O> {
    pub(crate) fn whole_batch(output: O) -> Self {
        Self {
            output,
            trace: None,
        }
    }

    pub(crate) fn exact(output: O, trace: TraceState) -> Self {
        Self {
            output,
            trace: Some(trace),
        }
    }
}

/// A first-class typed stateful accumulation strategy.
///
/// An accumulator owns the domain fold and its value projection. It composes
/// with an [`EmissionStrategy`] through [`StatefulWithEmission`] to become a
/// [`TypedStatefulHandler`]. Implementations work only with typed values; event
/// envelopes, writer identity, and contribution evidence remain runtime-owned.
pub trait Accumulator: Send + Sync + Debug {
    type State: Clone + Send + Sync + Debug;
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;
    fn accumulate(&self, state: &mut Self::State, input: Self::Input);
    fn outputs(&self, state: &Self::State) -> Vec<Self::Output>;

    /// Runtime-only accumulation hook for opaque contribution evidence.
    ///
    /// External accumulators inherit whole-batch attribution and cannot inspect
    /// the contribution token. Framework accumulators may override this hook to
    /// retain exact per-output partitions.
    #[doc(hidden)]
    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        _contribution: TypedStatefulContribution,
    ) {
        self.accumulate(state, input);
    }

    /// Runtime-only projection hook for exact contribution partitions.
    ///
    /// The default deliberately applies the adapter's whole-batch frontier to
    /// every projected value. Evidence-bearing constructors remain private to
    /// the runtime, so authored accumulators cannot manufacture provenance.
    #[doc(hidden)]
    fn outputs_with_contribution(
        &self,
        state: &Self::State,
    ) -> Vec<AccumulatorOutput<Self::Output>> {
        self.outputs(state)
            .into_iter()
            .map(AccumulatorOutput::whole_batch)
            .collect()
    }

    /// Runtime-only hook run once after an accepted emission that keeps the state.
    #[doc(hidden)]
    fn post_emit(&self, _state: &mut Self::State) {}

    /// Runtime-only hook run once after an accepted emission that keeps the state,
    /// carrying the ids of its outputs.
    ///
    /// `output_ids` name, in order, the outputs `outputs_with_contribution`
    /// projected from `state`, and are empty on the value path. The default runs
    /// `post_emit`.
    #[doc(hidden)]
    fn post_emit_with_contribution(
        &self,
        state: &mut Self::State,
        _output_ids: TypedStatefulOutputIds,
    ) {
        self.post_emit(state);
    }
}

impl<A, E> StatefulWithEmission<A, E>
where
    A: Accumulator,
    E: EmissionStrategy,
{
    fn typed_outputs(
        &self,
        state: &WrapperState<A::State>,
    ) -> (Vec<A::Output>, Option<Vec<TraceState>>) {
        let projected = self.accumulator.outputs_with_contribution(&state.inner);
        let exact = projected.iter().all(|item| item.trace.is_some());
        let mut outputs = Vec::with_capacity(projected.len());
        let mut traces = exact.then(|| Vec::with_capacity(projected.len()));
        for item in projected {
            outputs.push(item.output);
            if let (Some(target), Some(trace)) = (traces.as_mut(), item.trace) {
                target.push(trace);
            }
        }
        (outputs, traces)
    }

    fn transition(&self, state: &WrapperState<A::State>) -> AccumulatorTransition<A::Output> {
        let (outputs, traces) = self.typed_outputs(state);
        let emission = if self.emission.resets_accumulator_on_emit() {
            StatefulEmission::ResetEpoch { outputs }
        } else {
            StatefulEmission::RetainEpoch { outputs }
        };
        (emission, traces)
    }

    fn drained_outputs(
        &self,
        state: &WrapperState<A::State>,
    ) -> (Vec<A::Output>, Option<Vec<TraceState>>) {
        if state.events_seen == 0 && !self.emission.emits_only_at_end_of_input() {
            return (Vec::new(), None);
        }
        self.typed_outputs(state)
    }

    fn advance(&self, state: &mut WrapperState<A::State>, output_ids: TypedStatefulOutputIds) {
        state.advance_period();
        if self.emission.resets_accumulator_on_emit() {
            state.inner = self.accumulator.initial_state();
        } else {
            self.accumulator
                .post_emit_with_contribution(&mut state.inner, output_ids);
        }
    }
}

impl<A, E> TypedStatefulHandler for StatefulWithEmission<A, E>
where
    A: Accumulator + 'static,
    E: EmissionStrategy + Clone + 'static,
{
    type State = WrapperState<A::State>;
    type Input = A::Input;
    type Output = A::Output;

    fn initial_state(&self) -> Self::State {
        WrapperState {
            inner: self.accumulator.initial_state(),
            events_seen: 0,
            period_started_at: None,
        }
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        state.record_input();
        self.accumulator.accumulate(&mut state.inner, input);
    }

    fn accumulate_invocation(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        contribution: TypedStatefulContribution,
    ) {
        state.record_input();
        self.accumulator
            .accumulate_with_contribution(&mut state.inner, input, contribution);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        self.emission
            .should_emit(state.events_seen, state.period_elapsed())
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        self.emission.emit_interval_hint()
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        self.transition(state).0
    }

    fn post_emit(&self, state: &mut Self::State) {
        self.advance(state, TypedStatefulOutputIds::new(Vec::new()));
    }

    fn post_emit_invocation(&self, state: &mut Self::State, output_ids: TypedStatefulOutputIds) {
        self.advance(state, output_ids);
    }

    fn emit_invocation(&self, state: &Self::State) -> TypedStatefulInvocation<Self::Output> {
        let (emission, traces) = self.transition(state);
        match traces {
            Some(traces) => TypedStatefulInvocation::with_output_traces(emission, traces),
            None => TypedStatefulInvocation::facts_only(emission),
        }
    }

    fn drain(&self, state: &Self::State) -> Vec<Self::Output> {
        self.drained_outputs(state).0
    }

    fn drain_invocation(&self, state: &Self::State) -> TypedStatefulDrainInvocation<Self::Output> {
        let (outputs, traces) = self.drained_outputs(state);
        match traces {
            Some(traces) => TypedStatefulDrainInvocation::with_output_traces(outputs, traces),
            None => TypedStatefulDrainInvocation::facts_only(outputs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::stateful::strategies::emissions::{EmitAlways, EveryN, OnEOF, TimeWindow};
    use obzenflow_core::event::provenance::causality_context::CausalInput;
    use obzenflow_core::{EventId, EventType};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Tick;

    impl TypedPayload for Tick {
        const EVENT_TYPE: &'static str = "stateful.accumulator.wrapper.tick";
    }

    #[derive(Debug)]
    struct Ticks;

    impl Accumulator for Ticks {
        type State = u64;
        type Input = Tick;
        type Output = Tick;

        fn initial_state(&self) -> u64 {
            0
        }

        fn accumulate(&self, count: &mut u64, _: Tick) {
            *count += 1;
        }

        fn outputs(&self, _: &u64) -> Vec<Tick> {
            vec![Tick]
        }
    }

    #[test]
    fn post_emit_restarts_the_period_and_resets_only_for_a_resetting_strategy() {
        let retaining = StatefulWithEmission::new(Ticks, EveryN::new(2));
        let mut state = retaining.initial_state();
        retaining.accumulate(&mut state, Tick);
        retaining.accumulate(&mut state, Tick);
        assert!(matches!(
            retaining.emit(&state),
            StatefulEmission::RetainEpoch { .. }
        ));
        retaining.post_emit(&mut state);
        assert_eq!(state.inner, 2);
        assert_eq!(state.events_seen, 0);
        assert_eq!(state.period_started_at, None);

        let resetting = StatefulWithEmission::new(Ticks, TimeWindow::new(Duration::ZERO));
        let mut state = resetting.initial_state();
        resetting.accumulate(&mut state, Tick);
        assert!(matches!(
            resetting.emit(&state),
            StatefulEmission::ResetEpoch { .. }
        ));
        resetting.post_emit(&mut state);
        assert_eq!(state.inner, 0);
        assert_eq!(state.events_seen, 0);
        assert_eq!(state.period_started_at, None);
    }

    #[derive(Debug, Default)]
    struct LinkedTicks {
        received: Arc<Mutex<Vec<Vec<CausalInput>>>>,
    }

    impl Accumulator for LinkedTicks {
        type State = u64;
        type Input = Tick;
        type Output = Tick;

        fn initial_state(&self) -> u64 {
            0
        }

        fn accumulate(&self, count: &mut u64, _: Tick) {
            *count += 1;
        }

        fn outputs(&self, _: &u64) -> Vec<Tick> {
            vec![Tick]
        }

        fn post_emit_with_contribution(&self, _: &mut u64, output_ids: TypedStatefulOutputIds) {
            self.received
                .lock()
                .expect("recorder lock")
                .push(output_ids.outputs().to_vec());
        }
    }

    #[test]
    fn only_an_emission_that_keeps_the_state_hands_output_ids_to_the_accumulator() {
        let output = CausalInput {
            id: EventId::new(),
            event_type: EventType("tick".into()),
        };

        let retaining = LinkedTicks::default();
        let received = retaining.received.clone();
        let retaining = StatefulWithEmission::new(retaining, EveryN::new(1));
        let mut state = retaining.initial_state();
        retaining.post_emit_invocation(
            &mut state,
            TypedStatefulOutputIds::new(vec![output.clone()]),
        );
        retaining.post_emit(&mut state);
        assert_eq!(
            *received.lock().expect("recorder lock"),
            [vec![output.clone()], Vec::new()]
        );

        let resetting = LinkedTicks::default();
        let received = resetting.received.clone();
        let resetting = StatefulWithEmission::new(resetting, TimeWindow::new(Duration::ZERO));
        let mut state = resetting.initial_state();
        resetting.accumulate(&mut state, Tick);
        resetting.post_emit_invocation(
            &mut state,
            TypedStatefulOutputIds::new(vec![output.clone()]),
        );
        assert_eq!(state.inner, 0);
        assert!(received.lock().expect("recorder lock").is_empty());
    }

    #[test]
    fn end_of_input_emits_again_only_after_new_inputs_unless_it_is_the_only_emission() {
        let always = StatefulWithEmission::new(Ticks, EmitAlways);
        let mut state = always.initial_state();
        assert!(always.drain(&state).is_empty());
        always.accumulate(&mut state, Tick);
        always.post_emit(&mut state);
        assert!(
            always.drain(&state).is_empty(),
            "the last emission already covered every input"
        );

        let every_two = StatefulWithEmission::new(Ticks, EveryN::new(2));
        let mut state = every_two.initial_state();
        every_two.accumulate(&mut state, Tick);
        assert_eq!(every_two.drain(&state).len(), 1);

        let resetting = StatefulWithEmission::new(Ticks, TimeWindow::new(Duration::ZERO));
        let mut state = resetting.initial_state();
        resetting.accumulate(&mut state, Tick);
        resetting.post_emit(&mut state);
        assert!(resetting.drain(&state).is_empty());

        let at_end = StatefulWithEmission::new(Ticks, OnEOF::new());
        let state = at_end.initial_state();
        assert_eq!(at_end.drain(&state).len(), 1);
    }

    #[test]
    fn probes_do_not_mutate_cadence_state() {
        let state = WrapperState {
            inner: (),
            events_seen: 2,
            period_started_at: Some(Instant::now()),
        };
        let before_events = state.events_seen;
        let before_start = state.period_started_at;
        let policy = EveryN::new(2);
        assert!(policy.should_emit(state.events_seen, state.period_elapsed()));
        assert!(policy.should_emit(state.events_seen, state.period_elapsed()));
        assert_eq!(state.events_seen, before_events);
        assert_eq!(state.period_started_at, before_start);
    }

    #[test]
    fn time_window_requires_an_armed_period() {
        let policy = TimeWindow::new(Duration::ZERO);
        assert!(!policy.should_emit(1, None));
        assert!(policy.should_emit(1, Some(Duration::ZERO)));
    }
}
