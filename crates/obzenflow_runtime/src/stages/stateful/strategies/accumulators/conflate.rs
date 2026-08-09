// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed conflate accumulator.

use super::trace::TraceState;
use super::wrapper::AccumulatorOutput;
use super::{Accumulator, StatefulWithEmission};
use crate::stages::common::handlers::stateful::TypedStatefulContribution;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::TypedPayload;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

/// Keeps the latest typed value for each key.
#[derive(Clone)]
pub struct Conflate<T, K, FKey> {
    key_fn: FKey,
    _types: PhantomData<fn() -> (T, K)>,
}

/// Backwards-compatible spelling for the typed conflate strategy.
pub type ConflateTyped<T, K, FKey> = Conflate<T, K, FKey>;

impl<T, K, FKey> Conflate<T, K, FKey> {
    pub fn new(key_fn: FKey) -> Self {
        Self {
            key_fn,
            _types: PhantomData,
        }
    }

    pub fn with_emission<E: EmissionStrategy>(self, emission: E) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }

    pub fn emit_on_eof(self) -> StatefulWithEmission<Self, OnEOF> {
        self.with_emission(OnEOF::new())
    }

    pub fn emit_every_n(self, count: u64) -> StatefulWithEmission<Self, EveryN> {
        self.with_emission(EveryN::new(count))
    }

    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

impl<T, K, FKey> Debug for Conflate<T, K, FKey> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Conflate")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct ConflateBucket<T> {
    value: T,
    trace: TraceState,
    first_ordinal: u64,
}

/// Domain state retained by [`Conflate`].
#[derive(Clone, Debug)]
pub struct ConflateState<K, T> {
    buckets: HashMap<K, ConflateBucket<T>>,
    next_ordinal: u64,
}

impl<T, K, FKey> Conflate<T, K, FKey>
where
    K: Hash + Eq,
    FKey: Fn(&T) -> K,
{
    fn accumulate_value(
        &self,
        state: &mut ConflateState<K, T>,
        input: T,
        contribution: Option<TypedStatefulContribution>,
    ) {
        let key = (self.key_fn)(&input);
        let first_ordinal = state
            .buckets
            .get(&key)
            .map_or(state.next_ordinal, |bucket| bucket.first_ordinal);
        state.next_ordinal = state.next_ordinal.saturating_add(1);
        let mut trace = TraceState::default();
        if let Some(contribution) = contribution {
            contribution.record_into(&mut trace);
        }
        state.buckets.insert(
            key,
            ConflateBucket {
                value: input,
                trace,
                first_ordinal,
            },
        );
    }

    fn ordered_buckets<'a>(&self, state: &'a ConflateState<K, T>) -> Vec<&'a ConflateBucket<T>> {
        let mut buckets: Vec<_> = state.buckets.values().collect();
        buckets.sort_by_key(|bucket| {
            (
                bucket.trace.parent_ids().first().copied(),
                bucket.first_ordinal,
            )
        });
        buckets
    }
}

impl<T, K, FKey> Accumulator for Conflate<T, K, FKey>
where
    T: TypedPayload + Clone + Send + Sync + Debug + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
{
    type State = ConflateState<K, T>;
    type Input = T;
    type Output = T;

    fn initial_state(&self) -> Self::State {
        ConflateState {
            buckets: HashMap::new(),
            next_ordinal: 0,
        }
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        self.accumulate_value(state, input, None);
    }

    fn outputs(&self, state: &Self::State) -> Vec<Self::Output> {
        self.ordered_buckets(state)
            .into_iter()
            .map(|bucket| bucket.value.clone())
            .collect()
    }

    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        contribution: TypedStatefulContribution,
    ) {
        self.accumulate_value(state, input, Some(contribution));
    }

    fn outputs_with_contribution(
        &self,
        state: &Self::State,
    ) -> Vec<AccumulatorOutput<Self::Output>> {
        self.ordered_buckets(state)
            .into_iter()
            .map(|bucket| AccumulatorOutput::exact(bucket.value.clone(), bucket.trace.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handlers::stateful::traits::StatefulHandler;
    use crate::stages::common::handlers::stateful::TypedStatefulHandlerAdapter;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Reading {
        sensor: String,
        value: u32,
    }

    impl TypedPayload for Reading {
        const EVENT_TYPE: &'static str = "stateful.accumulator.conflate.reading";
    }

    #[test]
    fn conflate_keeps_the_latest_value_per_key() {
        let conflate = Conflate::new(|reading: &Reading| reading.sensor.clone());
        let mut state = Accumulator::initial_state(&conflate);
        for reading in [
            Reading {
                sensor: "a".into(),
                value: 1,
            },
            Reading {
                sensor: "b".into(),
                value: 2,
            },
            Reading {
                sensor: "a".into(),
                value: 3,
            },
        ] {
            Accumulator::accumulate(&conflate, &mut state, reading);
        }

        assert_eq!(
            Accumulator::outputs(&conflate, &state),
            vec![
                Reading {
                    sensor: "a".into(),
                    value: 3,
                },
                Reading {
                    sensor: "b".into(),
                    value: 2,
                },
            ]
        );
    }

    fn reading_event(sensor: &str, value: u32) -> obzenflow_core::ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Reading::versioned_event_type(),
            serde_json::json!(Reading {
                sensor: sensor.to_string(),
                value,
            }),
        )
    }

    #[test]
    fn conflate_retains_the_latest_inputs_exact_frontier_per_key() {
        let conflate = Conflate::new(|reading: &Reading| reading.sensor.clone()).emit_on_eof();
        let writer_id = WriterId::from(StageId::new());
        let mut adapter = TypedStatefulHandlerAdapter::new(conflate);
        StatefulHandler::install_writer_id(&mut adapter, writer_id);
        let mut state = StatefulHandler::initial_state(&adapter);
        let old_a = reading_event("a", 1);
        let b = reading_event("b", 2);
        let latest_a = reading_event("a", 3);

        for input in [old_a, b.clone(), latest_a.clone()] {
            StatefulHandler::try_accumulate(&mut adapter, &mut state, input)
                .expect("typed conflate input");
        }
        let outputs = StatefulHandler::emit(&adapter, &mut state).expect("conflate emission");

        assert_eq!(outputs.len(), 2);
        for output in outputs {
            assert_eq!(output.writer_id, writer_id);
            let decoded = Reading::from_event(&output).expect("honest conflate schema");
            let expected_parent = if decoded.sensor == "a" {
                latest_a.id
            } else {
                b.id
            };
            assert_eq!(output.causality.parent_ids, vec![expected_parent]);
        }
    }
}
