// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed conflate accumulator.

use super::trace::TraceState;
use super::wrapper::AccumulatorOutput;
use super::{Accumulator, KeyedEmission, StatefulWithEmission};
use crate::stages::common::handlers::stateful::TypedStatefulContribution;
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use obzenflow_core::TypedPayload;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;

/// Keeps the latest typed value for each key.
#[derive(Clone)]
pub struct Conflate<T, K, FKey> {
    key_fn: FKey,
    projection: Projection,
    _types: PhantomData<fn() -> (T, K)>,
}

#[derive(Clone, Copy, Debug)]
enum Projection {
    EveryKey,
    UpdatedKeys,
}

/// Backwards-compatible spelling for the typed conflate strategy.
pub type ConflateTyped<T, K, FKey> = Conflate<T, K, FKey>;

impl<T, K, FKey> Conflate<T, K, FKey> {
    pub fn new(key_fn: FKey) -> Self {
        Self {
            key_fn,
            projection: Projection::EveryKey,
            _types: PhantomData,
        }
    }

    /// Emits when `emission` decides, after `.updated_keys()` or `.all_keys()`
    /// names the keys each emission publishes.
    pub fn emit<E: EmissionStrategy>(self, emission: E) -> KeyedEmission<Self, E> {
        KeyedEmission::new(self, emission)
    }
}

impl<T, K, FKey, E> KeyedEmission<Conflate<T, K, FKey>, E>
where
    E: EmissionStrategy,
{
    /// Publishes the latest value of each key that received one since the
    /// previous emission.
    pub fn updated_keys(self) -> StatefulWithEmission<Conflate<T, K, FKey>, E> {
        let (conflate, emission) = self.into_parts();
        let projection =
            if emission.resets_accumulator_on_emit() || emission.emits_only_at_end_of_input() {
                Projection::EveryKey
            } else {
                Projection::UpdatedKeys
            };
        StatefulWithEmission::new(
            Conflate {
                projection,
                ..conflate
            },
            emission,
        )
    }

    /// Publishes every key's latest value at each emission.
    pub fn all_keys(self) -> StatefulWithEmission<Conflate<T, K, FKey>, E> {
        let (conflate, emission) = self.into_parts();
        StatefulWithEmission::new(
            Conflate {
                projection: Projection::EveryKey,
                ..conflate
            },
            emission,
        )
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
    updated: HashSet<K>,
    next_ordinal: u64,
}

impl<T, K, FKey> Conflate<T, K, FKey>
where
    K: Hash + Eq + Clone,
    FKey: Fn(&T) -> K,
{
    fn accumulate_value(
        &self,
        state: &mut ConflateState<K, T>,
        input: T,
        contribution: Option<TypedStatefulContribution>,
    ) {
        let key = (self.key_fn)(&input);
        if matches!(self.projection, Projection::UpdatedKeys) {
            state.updated.insert(key.clone());
        }
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
        let mut buckets: Vec<_> = match self.projection {
            Projection::EveryKey => state.buckets.values().collect(),
            Projection::UpdatedKeys => state
                .updated
                .iter()
                .filter_map(|key| state.buckets.get(key))
                .collect(),
        };
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
            updated: HashSet::new(),
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

    fn post_emit(&self, state: &mut Self::State) {
        state.updated.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handlers::stateful::traits::StatefulHandler;
    use crate::stages::common::handlers::stateful::{
        StatefulEmission, TypedStatefulHandler, TypedStatefulHandlerAdapter,
    };
    use crate::stages::stateful::emit;
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
            Reading::event_type_name(),
            Reading::payload_schema_version(),
            serde_json::json!(Reading {
                sensor: sensor.to_string(),
                value,
            }),
        )
    }

    #[test]
    fn conflate_retains_the_latest_inputs_exact_frontier_per_key() {
        let conflate = Conflate::new(|reading: &Reading| reading.sensor.clone())
            .emit(emit::on_eof)
            .all_keys();
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

    fn reading(sensor: &str, value: u32) -> Reading {
        Reading {
            sensor: sensor.into(),
            value,
        }
    }

    #[test]
    fn an_emission_publishes_only_the_keys_that_received_a_value() {
        let handler = Conflate::new(|reading: &Reading| reading.sensor.clone())
            .emit(emit::every_n(2))
            .updated_keys();
        let mut state = handler.initial_state();
        let mut emissions = Vec::new();

        for (sensor, value) in [("a", 1), ("b", 2), ("a", 3), ("a", 4)] {
            handler.accumulate(&mut state, reading(sensor, value));
            if handler.should_emit(&state) {
                let Ok(StatefulEmission::RetainEpoch { outputs }) = handler.emit(&state) else {
                    panic!("a conflation emission keeps the state");
                };
                handler.post_emit(&mut state);
                emissions.push(outputs);
            }
        }

        assert_eq!(
            emissions,
            [
                vec![reading("a", 1), reading("b", 2)],
                vec![reading("a", 4)]
            ]
        );
        assert!(handler.drain(&state).expect("drain projects").is_empty());
    }
}
