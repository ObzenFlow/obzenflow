// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed reduce accumulator.

use super::{Accumulator, StatefulWithEmission};
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use obzenflow_core::TypedPayload;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

/// Folds every input into one typed aggregate value.
#[derive(Clone)]
pub struct Reduce<T, S, F> {
    initial: S,
    reduce_fn: F,
    _input: PhantomData<fn() -> T>,
}

/// Backwards-compatible spelling for the typed reduce strategy.
pub type ReduceTyped<T, S, F> = Reduce<T, S, F>;

impl<T, S, F> Reduce<T, S, F> {
    pub fn new(initial: S, reduce_fn: F) -> Self {
        Self {
            initial,
            reduce_fn,
            _input: PhantomData,
        }
    }

    /// Emits the reduced value when `emission` decides, for example `emit::always`.
    pub fn emit<E: EmissionStrategy>(self, emission: E) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }
}

impl<T, S, F> Debug for Reduce<T, S, F>
where
    S: Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reduce")
            .field("input_type", &std::any::type_name::<T>())
            .field("state_type", &std::any::type_name::<S>())
            .field("initial", &self.initial)
            .finish_non_exhaustive()
    }
}

/// Domain state retained by [`Reduce`].
#[derive(Clone, Debug)]
pub struct ReduceState<S> {
    value: S,
    has_inputs: bool,
}

impl<T, S, F> Accumulator for Reduce<T, S, F>
where
    T: TypedPayload + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + Debug + 'static,
    F: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
{
    type State = ReduceState<S>;
    type Input = T;
    type Output = S;

    fn initial_state(&self) -> Self::State {
        ReduceState {
            value: self.initial.clone(),
            has_inputs: false,
        }
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        (self.reduce_fn)(&mut state.value, &input);
        state.has_inputs = true;
    }

    fn outputs(&self, state: &Self::State) -> Vec<Self::Output> {
        state
            .has_inputs
            .then(|| state.value.clone())
            .into_iter()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Input(u64);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "stateful.accumulator.reduce.input";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Total(u64);

    impl TypedPayload for Total {
        const EVENT_TYPE: &'static str = "stateful.accumulator.reduce.total";
    }

    #[test]
    fn reduce_is_a_first_class_typed_accumulator() {
        let reduce = Reduce::new(Total(0), |total: &mut Total, input: &Input| {
            total.0 += input.0;
        });
        let mut state = Accumulator::initial_state(&reduce);

        Accumulator::accumulate(&reduce, &mut state, Input(2));
        Accumulator::accumulate(&reduce, &mut state, Input(3));

        assert_eq!(Accumulator::outputs(&reduce, &state), vec![Total(5)]);
    }
}
