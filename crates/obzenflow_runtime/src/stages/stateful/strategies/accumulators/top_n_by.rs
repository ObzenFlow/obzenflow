// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed top-N-by accumulator with per-key aggregation semantics.

use super::{Accumulator, StatefulWithEmission};
use crate::stages::common::handlers::stateful::TypedStatefulContribution;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::{EventId, OneFactStageOutput, TypedPayload};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

type TopNByTypeMarker<T, K, O> = PhantomData<fn() -> (T, K, O)>;

/// Ordinary non-event value passed to a [`TopNBy`] output projection.
#[derive(Clone, Debug, PartialEq)]
pub struct TopNBySnapshot<K, T> {
    pub top_n: Vec<TopNByEntry<K, T>>,
    pub total_items: usize,
    pub capacity: usize,
}

/// One ranked aggregate in a [`TopNBySnapshot`].
#[derive(Clone, Debug, PartialEq)]
pub struct TopNByEntry<K, T> {
    pub rank: usize,
    pub key: K,
    pub total_score: f64,
    pub count: u64,
    pub avg_score: f64,
    pub metadata: T,
}

/// Aggregates scores by key and ranks the highest N aggregates.
///
/// Unlike [`super::TopN`], repeated values for a key add to its total score
/// and count instead of replacing its current score.
#[derive(Clone)]
pub struct TopNBy<T, K, O, FKey, FScore, FOutput> {
    capacity: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
    _types: TopNByTypeMarker<T, K, O>,
}

/// Backwards-compatible spelling for the typed top-N-by strategy.
pub type TopNByTyped<T, K, O, FKey, FScore, FOutput> = TopNBy<T, K, O, FKey, FScore, FOutput>;

impl<T, K, O, FKey, FScore, FOutput> TopNBy<T, K, O, FKey, FScore, FOutput> {
    pub fn new(capacity: usize, key_fn: FKey, score_fn: FScore, output_fn: FOutput) -> Self {
        assert!(capacity > 0, "TopNBy must track at least 1 item");
        Self {
            capacity,
            key_fn,
            score_fn,
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

impl<T, K, O, FKey, FScore, FOutput> Debug for TopNBy<T, K, O, FKey, FScore, FOutput> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopNBy")
            .field("capacity", &self.capacity)
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("output_type", &std::any::type_name::<O>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct TopNByItem<T> {
    total_score: f64,
    count: u64,
    metadata: T,
    first_event_id: Option<EventId>,
    first_ordinal: u64,
}

/// Domain state retained by [`TopNBy`].
#[derive(Clone, Debug)]
pub struct TopNByState<K, T> {
    items: HashMap<K, TopNByItem<T>>,
    next_ordinal: u64,
}

impl<T, K, O, FKey, FScore, FOutput> TopNBy<T, K, O, FKey, FScore, FOutput>
where
    T: Clone,
    K: Hash + Eq,
    FKey: Fn(&T) -> K,
    FScore: Fn(&T) -> f64,
{
    fn accumulate_item(&self, state: &mut TopNByState<K, T>, input: T, event_id: Option<EventId>) {
        let key = (self.key_fn)(&input);
        let score = (self.score_fn)(&input);
        let ordinal = state.next_ordinal;
        state.next_ordinal = state.next_ordinal.saturating_add(1);
        state
            .items
            .entry(key)
            .and_modify(|item| {
                item.total_score += score;
                item.count = item.count.saturating_add(1);
                item.metadata = input.clone();
            })
            .or_insert(TopNByItem {
                total_score: score,
                count: 1,
                metadata: input,
                first_event_id: event_id,
                first_ordinal: ordinal,
            });
    }
}

impl<T, K, O, FKey, FScore, FOutput> Accumulator for TopNBy<T, K, O, FKey, FScore, FOutput>
where
    T: TypedPayload + Clone + Send + Sync + Debug + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(TopNBySnapshot<K, T>) -> O + Send + Sync + Clone + 'static,
{
    type State = TopNByState<K, T>;
    type Input = T;
    type Output = O;

    fn initial_state(&self) -> Self::State {
        TopNByState {
            items: HashMap::new(),
            next_ordinal: 0,
        }
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        self.accumulate_item(state, input, None);
    }

    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        contribution: TypedStatefulContribution,
    ) {
        self.accumulate_item(state, input, Some(contribution.event_id()));
    }

    fn outputs(&self, state: &Self::State) -> Vec<Self::Output> {
        if state.items.is_empty() {
            return Vec::new();
        }

        let mut items: Vec<_> = state.items.iter().collect();
        items.sort_by(|(_, left), (_, right)| {
            right
                .total_score
                .total_cmp(&left.total_score)
                .then_with(|| left.first_event_id.cmp(&right.first_event_id))
                .then_with(|| left.first_ordinal.cmp(&right.first_ordinal))
        });

        let top_n = items
            .into_iter()
            .take(self.capacity)
            .enumerate()
            .map(|(index, (key, item))| TopNByEntry {
                rank: index + 1,
                key: key.clone(),
                total_score: item.total_score,
                count: item.count,
                avg_score: item.total_score / item.count as f64,
                metadata: item.metadata.clone(),
            })
            .collect();

        vec![(self.output_fn)(TopNBySnapshot {
            top_n,
            total_items: state.items.len(),
            capacity: self.capacity,
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Sale {
        product: String,
        amount: f64,
        label: String,
    }

    impl TypedPayload for Sale {
        const EVENT_TYPE: &'static str = "stateful.accumulator.top_n_by.sale";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Ranking {
        keys: Vec<String>,
        totals: Vec<f64>,
        counts: Vec<u64>,
        labels: Vec<String>,
    }

    impl TypedPayload for Ranking {
        const EVENT_TYPE: &'static str = "stateful.accumulator.top_n_by.ranking";
    }

    #[test]
    fn top_n_by_accumulates_repeated_keys_instead_of_replacing_them() {
        let top = TopNBy::new(
            2,
            |sale: &Sale| sale.product.clone(),
            |sale: &Sale| sale.amount,
            |snapshot: TopNBySnapshot<String, Sale>| Ranking {
                keys: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect(),
                totals: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.total_score)
                    .collect(),
                counts: snapshot.top_n.iter().map(|entry| entry.count).collect(),
                labels: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.metadata.label.clone())
                    .collect(),
            },
        );
        let mut state = Accumulator::initial_state(&top);
        for sale in [
            Sale {
                product: "a".into(),
                amount: 5.0,
                label: "old".into(),
            },
            Sale {
                product: "b".into(),
                amount: 8.0,
                label: "b".into(),
            },
            Sale {
                product: "a".into(),
                amount: 4.0,
                label: "latest".into(),
            },
        ] {
            Accumulator::accumulate(&top, &mut state, sale);
        }

        assert_eq!(
            Accumulator::outputs(&top, &state),
            vec![Ranking {
                keys: vec!["a".into(), "b".into()],
                totals: vec![9.0, 8.0],
                counts: vec![2, 1],
                labels: vec!["latest".into(), "b".into()],
            }]
        );
    }
}
