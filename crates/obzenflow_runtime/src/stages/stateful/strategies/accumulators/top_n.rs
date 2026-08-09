// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed top-N accumulator with replacement semantics.

use super::{Accumulator, StatefulWithEmission};
use crate::stages::common::handlers::stateful::TypedStatefulContribution;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::{EventId, OneFactStageOutput, TypedPayload};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

type TopNTypeMarker<T, K, O> = PhantomData<fn() -> (T, K, O)>;

/// Ordinary non-event value passed to a [`TopN`] output projection.
#[derive(Clone, Debug, PartialEq)]
pub struct TopNSnapshot<K, T> {
    pub top_n: Vec<TopNEntry<K, T>>,
    pub capacity: usize,
    pub count: usize,
}

/// One ranked item in a [`TopNSnapshot`].
#[derive(Clone, Debug, PartialEq)]
pub struct TopNEntry<K, T> {
    pub rank: usize,
    pub key: K,
    pub score: f64,
    pub metadata: T,
}

/// Maintains the top N latest values by score.
///
/// A new value for an existing key replaces that key's prior score and
/// metadata. Use [`super::TopNBy`] when scores for a key should accumulate.
#[derive(Clone)]
pub struct TopN<T, K, O, FKey, FScore, FOutput> {
    capacity: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
    _types: TopNTypeMarker<T, K, O>,
}

/// Backwards-compatible spelling for the typed top-N strategy.
pub type TopNTyped<T, K, O, FKey, FScore, FOutput> = TopN<T, K, O, FKey, FScore, FOutput>;

impl<T, K, O, FKey, FScore, FOutput> TopN<T, K, O, FKey, FScore, FOutput> {
    pub fn new(capacity: usize, key_fn: FKey, score_fn: FScore, output_fn: FOutput) -> Self {
        assert!(capacity > 0, "TopN must track at least 1 item");
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

impl<T, K, O, FKey, FScore, FOutput> Debug for TopN<T, K, O, FKey, FScore, FOutput> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopN")
            .field("capacity", &self.capacity)
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("output_type", &std::any::type_name::<O>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct TopNItem<T> {
    score: f64,
    metadata: T,
    event_id: Option<EventId>,
    ordinal: u64,
}

/// Domain state retained by [`TopN`].
#[derive(Clone, Debug)]
pub struct TopNState<K, T> {
    items: HashMap<K, TopNItem<T>>,
    next_ordinal: u64,
}

impl<T, K, O, FKey, FScore, FOutput> TopN<T, K, O, FKey, FScore, FOutput>
where
    T: Clone,
    K: Hash + Eq + Clone,
    FKey: Fn(&T) -> K,
    FScore: Fn(&T) -> f64,
{
    fn rank_cmp(left: &TopNItem<T>, right: &TopNItem<T>) -> Ordering {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.event_id.cmp(&right.event_id))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    }

    fn ordered_items<'a>(&self, state: &'a TopNState<K, T>) -> Vec<(&'a K, &'a TopNItem<T>)> {
        let mut items: Vec<_> = state.items.iter().collect();
        items.sort_by(|(_, left), (_, right)| Self::rank_cmp(left, right));
        items
    }

    fn accumulate_item(&self, state: &mut TopNState<K, T>, input: T, event_id: Option<EventId>) {
        let key = (self.key_fn)(&input);
        let score = (self.score_fn)(&input);
        let ordinal = state.next_ordinal;
        state.next_ordinal = state.next_ordinal.saturating_add(1);
        state.items.insert(
            key,
            TopNItem {
                score,
                metadata: input,
                event_id,
                ordinal,
            },
        );

        if state.items.len() > self.capacity {
            let evicted = self
                .ordered_items(state)
                .last()
                .map(|(key, _)| (*key).clone());
            if let Some(evicted) = evicted {
                state.items.remove(&evicted);
            }
        }
    }
}

impl<T, K, O, FKey, FScore, FOutput> Accumulator for TopN<T, K, O, FKey, FScore, FOutput>
where
    T: TypedPayload + Clone + Send + Sync + Debug + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(TopNSnapshot<K, T>) -> O + Send + Sync + Clone + 'static,
{
    type State = TopNState<K, T>;
    type Input = T;
    type Output = O;

    fn initial_state(&self) -> Self::State {
        TopNState {
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

        let top_n = self
            .ordered_items(state)
            .into_iter()
            .enumerate()
            .map(|(index, (key, item))| TopNEntry {
                rank: index + 1,
                key: key.clone(),
                score: item.score,
                metadata: item.metadata.clone(),
            })
            .collect();

        vec![(self.output_fn)(TopNSnapshot {
            top_n,
            capacity: self.capacity,
            count: state.items.len(),
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Score {
        key: String,
        score: f64,
        label: String,
    }

    impl TypedPayload for Score {
        const EVENT_TYPE: &'static str = "stateful.accumulator.top_n.score";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Ranking {
        keys: Vec<String>,
        scores: Vec<f64>,
        labels: Vec<String>,
    }

    impl TypedPayload for Ranking {
        const EVENT_TYPE: &'static str = "stateful.accumulator.top_n.ranking";
    }

    #[test]
    fn top_n_replaces_an_existing_keys_score_instead_of_accumulating_it() {
        let top = TopN::new(
            2,
            |score: &Score| score.key.clone(),
            |score: &Score| score.score,
            |snapshot: TopNSnapshot<String, Score>| Ranking {
                keys: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect(),
                scores: snapshot.top_n.iter().map(|entry| entry.score).collect(),
                labels: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.metadata.label.clone())
                    .collect(),
            },
        );
        let mut state = Accumulator::initial_state(&top);
        for score in [
            Score {
                key: "alice".into(),
                score: 10.0,
                label: "old".into(),
            },
            Score {
                key: "bob".into(),
                score: 8.0,
                label: "bob".into(),
            },
            Score {
                key: "alice".into(),
                score: 4.0,
                label: "latest".into(),
            },
        ] {
            Accumulator::accumulate(&top, &mut state, score);
        }

        assert_eq!(
            Accumulator::outputs(&top, &state),
            vec![Ranking {
                keys: vec!["bob".into(), "alice".into()],
                scores: vec![8.0, 4.0],
                labels: vec!["bob".into(), "latest".into()],
            }]
        );
    }

    #[test]
    fn top_n_evicts_the_lowest_current_score() {
        let top = TopN::new(
            2,
            |score: &Score| score.key.clone(),
            |score: &Score| score.score,
            |snapshot: TopNSnapshot<String, Score>| Ranking {
                keys: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect(),
                scores: snapshot.top_n.iter().map(|entry| entry.score).collect(),
                labels: snapshot
                    .top_n
                    .iter()
                    .map(|entry| entry.metadata.label.clone())
                    .collect(),
            },
        );
        let mut state = Accumulator::initial_state(&top);
        for (key, score) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
            Accumulator::accumulate(
                &top,
                &mut state,
                Score {
                    key: key.into(),
                    score,
                    label: key.into(),
                },
            );
        }

        assert_eq!(Accumulator::outputs(&top, &state)[0].keys, vec!["b", "c"]);
    }
}
