// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful accumulator/emission composition.
//!
//! FLOWIP-134e makes the emission policy immutable configuration. All cadence
//! evolution lives in [`WrapperState`], alongside the accumulator's domain state.

use super::trace::TraceState;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::stateful::{
    StatefulEmission, TypedStatefulContribution, TypedStatefulDrainInvocation,
    TypedStatefulHandler, TypedStatefulInvocation,
};
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use obzenflow_core::{OneFactStageOutput, TypedPayload};
use std::fmt::Debug;
use std::time::{Duration, Instant};

type AccumulatorTransition<S, O> = (
    StatefulEmission<WrapperState<S>, O>,
    Option<Vec<TraceState>>,
);

/// Explicit domain and cadence state for an accumulator facade.
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

/// One typed accumulator projection plus optional exact contribution evidence.
#[doc(hidden)]
pub struct TypedAccumulatorOutput<O> {
    pub(crate) output: O,
    pub(crate) trace: Option<TraceState>,
}

impl<O> TypedAccumulatorOutput<O> {
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

/// Crate-private typed facade primitive. Contribution evidence is available
/// only through this framework-owned lane.
pub(crate) mod sealed {
    pub trait Sealed {}
}

#[doc(hidden)]
pub trait TypedAccumulator: sealed::Sealed + Send + Sync + Debug {
    type State: Clone + Send + Sync + Debug;
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;
    fn accumulate(&self, state: &mut Self::State, input: Self::Input);

    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        _contribution: TypedStatefulContribution,
    ) {
        self.accumulate(state, input);
    }

    fn outputs(&self, state: &Self::State) -> Vec<TypedAccumulatorOutput<Self::Output>>;
}

impl<A, E> StatefulWithEmission<A, E>
where
    A: TypedAccumulator,
    E: EmissionStrategy,
{
    fn typed_outputs(
        &self,
        state: &WrapperState<A::State>,
    ) -> (Vec<A::Output>, Option<Vec<TraceState>>) {
        let projected = self.accumulator.outputs(&state.inner);
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

    fn transition(
        &self,
        state: &WrapperState<A::State>,
    ) -> AccumulatorTransition<A::State, A::Output> {
        let (outputs, traces) = self.typed_outputs(state);
        let mut next_state = state.clone();
        next_state.advance_period();

        let emission = if self.emission.resets_accumulator_on_emit() {
            next_state.inner = self.accumulator.initial_state();
            StatefulEmission::ResetEpoch {
                next_state,
                outputs,
            }
        } else {
            StatefulEmission::RetainEpoch {
                next_state,
                outputs,
            }
        };
        (emission, traces)
    }
}

impl<A, E> TypedStatefulHandler for StatefulWithEmission<A, E>
where
    A: TypedAccumulator + 'static,
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

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(self.transition(state).0)
    }

    fn emit_invocation(
        &self,
        state: &Self::State,
    ) -> Result<TypedStatefulInvocation<Self::State, Self::Output>, HandlerError> {
        let (emission, traces) = self.transition(state);
        Ok(match traces {
            Some(traces) => TypedStatefulInvocation::with_output_traces(emission, traces),
            None => TypedStatefulInvocation::facts_only(emission),
        })
    }

    fn drain(&self, state: &Self::State) -> Result<Vec<Self::Output>, HandlerError> {
        Ok(self.typed_outputs(state).0)
    }

    fn drain_invocation(
        &self,
        state: &Self::State,
    ) -> Result<TypedStatefulDrainInvocation<Self::Output>, HandlerError> {
        let (outputs, traces) = self.typed_outputs(state);
        Ok(match traces {
            Some(traces) => TypedStatefulDrainInvocation::with_output_traces(outputs, traces),
            None => TypedStatefulDrainInvocation::facts_only(outputs),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::stateful::strategies::emissions::{EveryN, TimeWindow};

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
