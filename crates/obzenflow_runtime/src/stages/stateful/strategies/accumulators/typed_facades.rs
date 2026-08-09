// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed stateful accumulator facades (FLOWIP-134e).

use super::trace::TraceState;
use super::wrapper::{sealed, StatefulWithEmission, TypedAccumulator, TypedAccumulatorOutput};
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

type GroupByTypeMarker<T, K, S, O> = PhantomData<fn() -> (T, K, S, O)>;
type TopNByTypeMarker<T, K, O> = PhantomData<fn() -> (T, K, O)>;

#[derive(Clone)]
pub struct ReduceTyped<T, S, F> {
    initial: S,
    reduce_fn: F,
    _input: PhantomData<fn() -> T>,
}

impl<T, S, F> Debug for ReduceTyped<T, S, F>
where
    S: Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReduceTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("state_type", &std::any::type_name::<S>())
            .field("initial", &self.initial)
            .finish_non_exhaustive()
    }
}

impl<T, S, F> ReduceTyped<T, S, F> {
    pub fn new(initial: S, reduce_fn: F) -> Self {
        Self {
            initial,
            reduce_fn,
            _input: PhantomData,
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

#[derive(Clone, Debug)]
pub struct ReduceTypedState<S> {
    value: S,
    has_inputs: bool,
}

impl<T, S, F> sealed::Sealed for ReduceTyped<T, S, F> {}

impl<T, S, F> TypedAccumulator for ReduceTyped<T, S, F>
where
    T: TypedPayload + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + Debug + 'static,
    F: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
{
    type State = ReduceTypedState<S>;
    type Input = T;
    type Output = S;

    fn initial_state(&self) -> Self::State {
        ReduceTypedState {
            value: self.initial.clone(),
            has_inputs: false,
        }
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        (self.reduce_fn)(&mut state.value, &input);
        state.has_inputs = true;
    }

    fn outputs(&self, state: &Self::State) -> Vec<TypedAccumulatorOutput<Self::Output>> {
        state
            .has_inputs
            .then(|| TypedAccumulatorOutput::whole_batch(state.value.clone()))
            .into_iter()
            .collect()
    }
}

#[derive(Clone)]
pub struct ConflateTyped<T, K, FKey> {
    key_fn: FKey,
    _types: PhantomData<fn() -> (T, K)>,
}

impl<T, K, FKey> ConflateTyped<T, K, FKey> {
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

impl<T, K, FKey> Debug for ConflateTyped<T, K, FKey> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConflateTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct ConflateTypedBucket<T> {
    value: T,
    trace: TraceState,
}

impl<T, K, FKey> sealed::Sealed for ConflateTyped<T, K, FKey> {}

impl<T, K, FKey> TypedAccumulator for ConflateTyped<T, K, FKey>
where
    T: TypedPayload + Clone + Send + Sync + Debug + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
{
    type State = HashMap<K, ConflateTypedBucket<T>>;
    type Input = T;
    type Output = T;

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        state.insert(
            (self.key_fn)(&input),
            ConflateTypedBucket {
                value: input,
                trace: TraceState::default(),
            },
        );
    }

    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        contribution: TypedStatefulContribution,
    ) {
        let mut trace = TraceState::default();
        contribution.record_into(&mut trace);
        state.insert(
            (self.key_fn)(&input),
            ConflateTypedBucket {
                value: input,
                trace,
            },
        );
    }

    fn outputs(&self, state: &Self::State) -> Vec<TypedAccumulatorOutput<Self::Output>> {
        let mut buckets: Vec<_> = state.values().collect();
        buckets.sort_by_key(|bucket| bucket.trace.parent_ids().first().copied());
        buckets
            .into_iter()
            .map(|bucket| TypedAccumulatorOutput::exact(bucket.value.clone(), bucket.trace.clone()))
            .collect()
    }
}

#[derive(Clone)]
pub struct GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput> {
    key_fn: FKey,
    update_fn: FUpdate,
    output_fn: FOutput,
    _types: GroupByTypeMarker<T, K, S, O>,
}

impl<T, K, S, O, FKey, FUpdate, FOutput> GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput> {
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

impl<T, K, S, O, FKey, FUpdate, FOutput> Debug
    for GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupByTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("state_type", &std::any::type_name::<S>())
            .field("output_type", &std::any::type_name::<O>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct GroupByTypedBucket<S> {
    value: S,
    trace: TraceState,
}

impl<T, K, S, O, FKey, FUpdate, FOutput> sealed::Sealed
    for GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput>
{
}

impl<T, K, S, O, FKey, FUpdate, FOutput> TypedAccumulator
    for GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput>
where
    T: TypedPayload + Send + Sync + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    S: Default + Clone + Send + Sync + Debug + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
    FOutput: Fn(&K, &S) -> O + Send + Sync + Clone + 'static,
{
    type State = HashMap<K, GroupByTypedBucket<S>>;
    type Input = T;
    type Output = O;

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        let bucket = state
            .entry((self.key_fn)(&input))
            .or_insert_with(|| GroupByTypedBucket {
                value: S::default(),
                trace: TraceState::default(),
            });
        (self.update_fn)(&mut bucket.value, &input);
    }

    fn accumulate_with_contribution(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        contribution: TypedStatefulContribution,
    ) {
        let bucket = state
            .entry((self.key_fn)(&input))
            .or_insert_with(|| GroupByTypedBucket {
                value: S::default(),
                trace: TraceState::default(),
            });
        (self.update_fn)(&mut bucket.value, &input);
        contribution.record_into(&mut bucket.trace);
    }

    fn outputs(&self, state: &Self::State) -> Vec<TypedAccumulatorOutput<Self::Output>> {
        let mut buckets: Vec<_> = state.iter().collect();
        buckets.sort_by_key(|(_, bucket)| bucket.trace.parent_ids().first().copied());
        buckets
            .into_iter()
            .map(|(key, bucket)| {
                TypedAccumulatorOutput::exact(
                    (self.output_fn)(key, &bucket.value),
                    bucket.trace.clone(),
                )
            })
            .collect()
    }
}

/// Ordinary non-event value passed to a `top_n_by` output projection.
#[derive(Clone, Debug)]
pub struct TopNBySnapshot<K, T> {
    pub top_n: Vec<TopNByEntry<K, T>>,
    pub total_items: usize,
    pub capacity: usize,
}

/// One ranked entry in a [`TopNBySnapshot`].
#[derive(Clone, Debug)]
pub struct TopNByEntry<K, T> {
    pub rank: usize,
    pub key: K,
    pub total_score: f64,
    pub count: u64,
    pub avg_score: f64,
    pub metadata: T,
}

#[derive(Clone)]
pub struct TopNByTyped<T, K, O, FKey, FScore, FOutput> {
    capacity: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
    _types: TopNByTypeMarker<T, K, O>,
}

impl<T, K, O, FKey, FScore, FOutput> TopNByTyped<T, K, O, FKey, FScore, FOutput> {
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

impl<T, K, O, FKey, FScore, FOutput> Debug for TopNByTyped<T, K, O, FKey, FScore, FOutput> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopNByTyped")
            .field("capacity", &self.capacity)
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("output_type", &std::any::type_name::<O>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct TopNByTypedItem<T> {
    total_score: f64,
    count: u64,
    metadata: T,
    first_event_id: Option<EventId>,
    first_ordinal: u64,
}

#[derive(Clone, Debug)]
pub struct TopNByTypedState<K, T> {
    items: HashMap<K, TopNByTypedItem<T>>,
    next_ordinal: u64,
}

impl<T, K, O, FKey, FScore, FOutput> sealed::Sealed
    for TopNByTyped<T, K, O, FKey, FScore, FOutput>
{
}

impl<T, K, O, FKey, FScore, FOutput> TopNByTyped<T, K, O, FKey, FScore, FOutput>
where
    T: Clone,
    K: Hash + Eq,
    FKey: Fn(&T) -> K,
    FScore: Fn(&T) -> f64,
{
    fn accumulate_item(
        &self,
        state: &mut TopNByTypedState<K, T>,
        input: T,
        event_id: Option<EventId>,
    ) {
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
            .or_insert(TopNByTypedItem {
                total_score: score,
                count: 1,
                metadata: input,
                first_event_id: event_id,
                first_ordinal: ordinal,
            });
    }
}

impl<T, K, O, FKey, FScore, FOutput> TypedAccumulator
    for TopNByTyped<T, K, O, FKey, FScore, FOutput>
where
    T: TypedPayload + Clone + Send + Sync + Debug + 'static,
    K: Hash + Eq + Clone + Send + Sync + Debug + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(TopNBySnapshot<K, T>) -> O + Send + Sync + Clone + 'static,
{
    type State = TopNByTypedState<K, T>;
    type Input = T;
    type Output = O;

    fn initial_state(&self) -> Self::State {
        TopNByTypedState {
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

    fn outputs(&self, state: &Self::State) -> Vec<TypedAccumulatorOutput<Self::Output>> {
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

        vec![TypedAccumulatorOutput::whole_batch((self.output_fn)(
            TopNBySnapshot {
                top_n,
                total_items: state.items.len(),
                capacity: self.capacity,
            },
        ))]
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

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct RankedInput {
        key: String,
        score: f64,
        metadata: String,
    }

    impl TypedPayload for RankedInput {
        const EVENT_TYPE: &'static str = "typed_stateful_facade.ranked_input";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct RankedOutput {
        keys: Vec<String>,
        total_items: usize,
        capacity: usize,
        a_count: u64,
        a_metadata: String,
    }

    impl TypedPayload for RankedOutput {
        const EVENT_TYPE: &'static str = "typed_stateful_facade.ranked_output";
    }

    #[test]
    fn top_n_by_projects_the_public_snapshot_shape_deterministically() {
        let accumulator = TopNByTyped::new(
            3,
            |input: &RankedInput| input.key.clone(),
            |input: &RankedInput| input.score,
            |snapshot: TopNBySnapshot<String, RankedInput>| {
                let a = snapshot
                    .top_n
                    .iter()
                    .find(|entry| entry.key == "a")
                    .expect("a is ranked");
                RankedOutput {
                    keys: snapshot
                        .top_n
                        .iter()
                        .map(|entry| entry.key.clone())
                        .collect(),
                    total_items: snapshot.total_items,
                    capacity: snapshot.capacity,
                    a_count: a.count,
                    a_metadata: a.metadata.metadata.clone(),
                }
            },
        );
        let mut state = TypedAccumulator::initial_state(&accumulator);
        for input in [
            RankedInput {
                key: "a".into(),
                score: 2.0,
                metadata: "old".into(),
            },
            RankedInput {
                key: "c".into(),
                score: 10.0,
                metadata: "c".into(),
            },
            RankedInput {
                key: "a".into(),
                score: 2.0,
                metadata: "latest".into(),
            },
            RankedInput {
                key: "b".into(),
                score: 4.0,
                metadata: "b".into(),
            },
        ] {
            TypedAccumulator::accumulate(&accumulator, &mut state, input);
        }

        let mut projected = TypedAccumulator::outputs(&accumulator, &state);
        let output = projected.pop().expect("one snapshot").output;

        assert_eq!(
            output,
            RankedOutput {
                keys: vec!["c".into(), "a".into(), "b".into()],
                total_items: 3,
                capacity: 3,
                a_count: 2,
                a_metadata: "latest".into(),
            }
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct LatestInput {
        key: String,
        value: u32,
    }

    impl TypedPayload for LatestInput {
        const EVENT_TYPE: &'static str = "typed_stateful_facade.latest_input";
    }

    fn latest_event(key: &str, value: u32) -> obzenflow_core::ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            LatestInput::versioned_event_type(),
            serde_json::json!(LatestInput {
                key: key.to_string(),
                value,
            }),
        )
    }

    #[test]
    fn conflate_uses_the_latest_input_as_each_outputs_exact_frontier() {
        let facade = ConflateTyped::new(|input: &LatestInput| input.key.clone()).emit_on_eof();
        let writer_id = WriterId::from(StageId::new());
        let mut adapter = TypedStatefulHandlerAdapter::new(facade);
        StatefulHandler::install_writer_id(&mut adapter, writer_id);
        let mut state = StatefulHandler::initial_state(&adapter);
        let a_old = latest_event("a", 1);
        let b = latest_event("b", 2);
        let a_latest = latest_event("a", 3);

        for input in [a_old, b.clone(), a_latest.clone()] {
            StatefulHandler::try_accumulate(&mut adapter, &mut state, input)
                .expect("typed conflate input");
        }
        let outputs = StatefulHandler::emit(&adapter, &mut state).expect("conflate emission");

        assert_eq!(outputs.len(), 2);
        for output in outputs {
            assert_eq!(output.writer_id, writer_id);
            let decoded = LatestInput::from_event(&output).expect("honest conflate schema");
            let expected_parent = if decoded.key == "a" {
                a_latest.id
            } else {
                b.id
            };
            assert_eq!(output.causality.parent_ids, vec![expected_parent]);
        }
    }
}
