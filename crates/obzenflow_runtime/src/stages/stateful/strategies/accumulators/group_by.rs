// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed group-by accumulator.

use super::trace::TraceState;
use super::wrapper::AccumulatorOutput;
use super::{Accumulator, StatefulWithEmission};
use crate::stages::common::handlers::stateful::TypedStatefulContribution;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::{OneFactStageOutput, TypedPayload};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

type GroupByTypeMarker<T, K, S, O> = PhantomData<fn() -> (T, K, S, O)>;

/// Folds inputs into independent per-key aggregate state.
///
/// The output projection gives every emitted value an honest named payload
/// type instead of hiding a `{ key, result }` object under the state type.
#[derive(Clone)]
pub struct GroupBy<T, K, S, O, FKey, FUpdate, FOutput> {
    key_fn: FKey,
    update_fn: FUpdate,
    output_fn: FOutput,
    _types: GroupByTypeMarker<T, K, S, O>,
}

/// Backwards-compatible spelling for the typed group-by strategy.
pub type GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput> =
    GroupBy<T, K, S, O, FKey, FUpdate, FOutput>;

impl<T, K, S, O, FKey, FUpdate, FOutput> GroupBy<T, K, S, O, FKey, FUpdate, FOutput> {
    pub fn new(key_fn: FKey, update_fn: FUpdate, output_fn: FOutput) -> Self {
        Self {
            key_fn,
            update_fn,
            output_fn,
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

impl<T, K, S, O, FKey, FUpdate, FOutput> Debug for GroupBy<T, K, S, O, FKey, FUpdate, FOutput> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupBy")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("state_type", &std::any::type_name::<S>())
            .field("output_type", &std::any::type_name::<O>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct GroupByBucket<S> {
    value: S,
    trace: TraceState,
    first_ordinal: u64,
}

/// Domain state retained by [`GroupBy`].
#[derive(Clone, Debug)]
pub struct GroupByState<K, S> {
    buckets: HashMap<K, GroupByBucket<S>>,
    next_ordinal: u64,
}

impl<T, K, S, O, FKey, FUpdate, FOutput> GroupBy<T, K, S, O, FKey, FUpdate, FOutput>
where
    K: Hash + Eq,
    S: Default,
    FKey: Fn(&T) -> K,
    FUpdate: Fn(&mut S, &T),
{
    fn accumulate_value(
        &self,
        state: &mut GroupByState<K, S>,
        input: T,
        contribution: Option<TypedStatefulContribution>,
    ) {
        let key = (self.key_fn)(&input);
        let next_ordinal = state.next_ordinal;
        let bucket = state.buckets.entry(key).or_insert_with(|| {
            state.next_ordinal = state.next_ordinal.saturating_add(1);
            GroupByBucket {
                value: S::default(),
                trace: TraceState::default(),
                first_ordinal: next_ordinal,
            }
        });
        (self.update_fn)(&mut bucket.value, &input);
        if let Some(contribution) = contribution {
            contribution.record_into(&mut bucket.trace);
        }
    }

    fn ordered_buckets<'a>(
        &self,
        state: &'a GroupByState<K, S>,
    ) -> Vec<(&'a K, &'a GroupByBucket<S>)> {
        let mut buckets: Vec<_> = state.buckets.iter().collect();
        buckets.sort_by_key(|(_, bucket)| {
            (
                bucket.trace.parent_ids().first().copied(),
                bucket.first_ordinal,
            )
        });
        buckets
    }
}

impl<T, K, S, O, FKey, FUpdate, FOutput> Accumulator for GroupBy<T, K, S, O, FKey, FUpdate, FOutput>
where
    T: TypedPayload + Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    S: Default + Clone + Send + Sync + Debug + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
    FOutput: Fn(&K, &S) -> O + Send + Sync + Clone + 'static,
{
    type State = GroupByState<K, S>;
    type Input = T;
    type Output = O;

    fn initial_state(&self) -> Self::State {
        GroupByState {
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
            .map(|(key, bucket)| (self.output_fn)(key, &bucket.value))
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
            .map(|(key, bucket)| {
                AccumulatorOutput::exact((self.output_fn)(key, &bucket.value), bucket.trace.clone())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Sale {
        store: String,
        amount: u64,
    }

    impl TypedPayload for Sale {
        const EVENT_TYPE: &'static str = "stateful.accumulator.group_by.sale";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct StoreTotal {
        store: String,
        amount: u64,
    }

    impl TypedPayload for StoreTotal {
        const EVENT_TYPE: &'static str = "stateful.accumulator.group_by.total";
    }

    #[test]
    fn group_by_retains_independent_keyed_aggregates() {
        let group = GroupBy::new(
            |sale: &Sale| sale.store.clone(),
            |total: &mut u64, sale: &Sale| *total += sale.amount,
            |store: &String, total: &u64| StoreTotal {
                store: store.clone(),
                amount: *total,
            },
        );
        let mut state = Accumulator::initial_state(&group);
        for sale in [
            Sale {
                store: "a".into(),
                amount: 2,
            },
            Sale {
                store: "b".into(),
                amount: 3,
            },
            Sale {
                store: "a".into(),
                amount: 5,
            },
        ] {
            Accumulator::accumulate(&group, &mut state, sale);
        }

        assert_eq!(
            Accumulator::outputs(&group, &state),
            vec![
                StoreTotal {
                    store: "a".into(),
                    amount: 7,
                },
                StoreTotal {
                    store: "b".into(),
                    amount: 3,
                },
            ]
        );
    }
}
