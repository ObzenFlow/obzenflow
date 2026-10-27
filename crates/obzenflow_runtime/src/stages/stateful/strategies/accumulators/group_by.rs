// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed group-by accumulator.

use super::trace::TraceState;
use super::wrapper::AccumulatorOutput;
use super::{Accumulator, KeyedEmission, StatefulWithEmission};
use crate::stages::common::handlers::stateful::{
    TypedStatefulContribution, TypedStatefulOutputIds,
};
use crate::stages::stateful::strategies::emissions::EmissionStrategy;
use obzenflow_core::{EventId, OneFactStageOutput, TypedPayload};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;

type GroupByTypeMarker<T, K, S, O> = PhantomData<fn() -> (T, K, S, O)>;

type GroupByHandler<T, K, S, O, FKey, FUpdate, FOutput, E> =
    StatefulWithEmission<GroupBy<T, K, S, O, FKey, FUpdate, FOutput>, E>;

/// Folds inputs into independent per-key aggregate state.
///
/// The output projection gives every emitted value an honest named payload
/// type instead of hiding a `{ key, result }` object under the state type.
#[derive(Clone)]
pub struct GroupBy<T, K, S, O, FKey, FUpdate, FOutput> {
    key_fn: FKey,
    update_fn: FUpdate,
    output_fn: FOutput,
    projection: Projection,
    _types: GroupByTypeMarker<T, K, S, O>,
}

#[derive(Clone, Copy, Debug)]
enum Projection {
    EveryGroup,
    UpdatedGroups,
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
            projection: Projection::EveryGroup,
            _types: PhantomData,
        }
    }

    /// Emits when `emission` decides, after `.updated_groups()` or
    /// `.all_groups()` names the groups each emission publishes.
    pub fn emit<E: EmissionStrategy>(self, emission: E) -> KeyedEmission<Self, E> {
        KeyedEmission::new(self, emission)
    }
}

impl<T, K, S, O, FKey, FUpdate, FOutput, E>
    KeyedEmission<GroupBy<T, K, S, O, FKey, FUpdate, FOutput>, E>
where
    E: EmissionStrategy,
{
    /// Publishes the groups updated since the previous emission.
    pub fn updated_groups(self) -> GroupByHandler<T, K, S, O, FKey, FUpdate, FOutput, E> {
        let (group_by, emission) = self.into_parts();
        let projection =
            if emission.resets_accumulator_on_emit() || emission.emits_only_at_end_of_input() {
                Projection::EveryGroup
            } else {
                Projection::UpdatedGroups
            };
        StatefulWithEmission::new(
            GroupBy {
                projection,
                ..group_by
            },
            emission,
        )
    }

    /// Publishes every group at each emission.
    ///
    /// Accumulators that publish one result do not offer it:
    ///
    /// ```compile_fail,E0599
    /// # use obzenflow_core::TypedPayload;
    /// # use serde::{Deserialize, Serialize};
    /// use obzenflow_runtime::stages::stateful::{emit, reduce};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Entry;
    /// # impl TypedPayload for Entry { const EVENT_TYPE: &'static str = "doc.entry"; }
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Count(u64);
    /// # impl TypedPayload for Count { const EVENT_TYPE: &'static str = "doc.count"; }
    ///
    /// let counts = reduce(Count(0), |count: &mut Count, _: &Entry| count.0 += 1);
    /// let _ = counts.emit(emit::always).all_groups();
    /// ```
    pub fn all_groups(self) -> GroupByHandler<T, K, S, O, FKey, FUpdate, FOutput, E> {
        let (group_by, emission) = self.into_parts();
        StatefulWithEmission::new(
            GroupBy {
                projection: Projection::EveryGroup,
                ..group_by
            },
            emission,
        )
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
    previous_output: Option<EventId>,
    first_event_id: Option<EventId>,
    first_ordinal: u64,
}

impl<S> GroupByBucket<S> {
    fn order(&self) -> (Option<EventId>, u64) {
        (self.first_event_id, self.first_ordinal)
    }

    fn evidence(&self) -> TraceState {
        self.trace.with_previous_output(self.previous_output)
    }

    fn link(&mut self, output_id: EventId) {
        self.trace.clear_parent_ids();
        self.previous_output = Some(output_id);
    }
}

/// Domain state retained by [`GroupBy`].
#[derive(Clone, Debug)]
pub struct GroupByState<K, S> {
    buckets: HashMap<K, GroupByBucket<S>>,
    updated: HashSet<K>,
    next_ordinal: u64,
}

impl<T, K, S, O, FKey, FUpdate, FOutput> GroupBy<T, K, S, O, FKey, FUpdate, FOutput>
where
    K: Hash + Eq + Clone,
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
        if matches!(self.projection, Projection::UpdatedGroups) {
            state.updated.insert(key.clone());
        }
        let next_ordinal = state.next_ordinal;
        let bucket = state.buckets.entry(key).or_insert_with(|| {
            state.next_ordinal = state.next_ordinal.saturating_add(1);
            GroupByBucket {
                value: S::default(),
                trace: TraceState::default(),
                previous_output: None,
                first_event_id: contribution
                    .as_ref()
                    .map(TypedStatefulContribution::event_id),
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
        let mut buckets: Vec<_> = match self.projection {
            Projection::EveryGroup => state.buckets.iter().collect(),
            Projection::UpdatedGroups => state
                .updated
                .iter()
                .filter_map(|key| state.buckets.get_key_value(key))
                .collect(),
        };
        buckets.sort_by_key(|(_, bucket)| bucket.order());
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
                AccumulatorOutput::exact((self.output_fn)(key, &bucket.value), bucket.evidence())
            })
            .collect()
    }

    fn post_emit_with_contribution(
        &self,
        state: &mut Self::State,
        output_ids: TypedStatefulOutputIds,
    ) {
        let emitted: Vec<K> = self
            .ordered_buckets(state)
            .into_iter()
            .map(|(key, _)| key.clone())
            .collect();
        for (key, output_id) in emitted.iter().zip(output_ids.ids()) {
            if let Some(bucket) = state.buckets.get_mut(key) {
                bucket.link(*output_id);
            }
        }
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
    use obzenflow_core::config::{LineagePolicy, DEFAULT_MAX_LINEAGE_DEPTH};
    use obzenflow_core::event::provenance::ReplayContext;
    use obzenflow_core::event::types::CorrelationId;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{ChainEvent, StageId, WriterId};
    use serde::{Deserialize, Serialize};
    use ulid::Ulid;

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

    type StoreTotals = GroupBy<
        Sale,
        String,
        u64,
        StoreTotal,
        fn(&Sale) -> String,
        fn(&mut u64, &Sale),
        fn(&String, &u64) -> StoreTotal,
    >;

    fn store_totals() -> StoreTotals {
        GroupBy::new(
            |sale| sale.store.clone(),
            |total, sale| *total += sale.amount,
            |store, total| StoreTotal {
                store: store.clone(),
                amount: *total,
            },
        )
    }

    fn total(store: &str, amount: u64) -> StoreTotal {
        StoreTotal {
            store: store.into(),
            amount,
        }
    }

    fn sale(id: u128, store: &str, amount: u64) -> ChainEvent {
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Sale::event_type_name(),
            Sale::payload_schema_version(),
            serde_json::json!(Sale {
                store: store.into(),
                amount,
            }),
        );
        event.id = EventId::from(Ulid::from(id));
        event
    }

    fn adapter<H: TypedStatefulHandler>(
        handler: H,
        max_lineage_depth: usize,
    ) -> TypedStatefulHandlerAdapter<H> {
        let mut adapter = TypedStatefulHandlerAdapter::new(handler);
        StatefulHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        StatefulHandler::install_lineage_policy(&mut adapter, LineagePolicy { max_lineage_depth });
        adapter
    }

    fn published(outputs: &[ChainEvent]) -> Vec<(StoreTotal, Vec<EventId>)> {
        outputs
            .iter()
            .map(|output| {
                let total = StoreTotal::from_event(output).expect("a named store total");
                (total, output.causality.parent_ids.clone())
            })
            .collect()
    }

    fn stores(outputs: &[ChainEvent]) -> Vec<String> {
        published(outputs)
            .into_iter()
            .map(|(total, _)| total.store)
            .collect()
    }

    #[test]
    fn group_by_retains_independent_keyed_aggregates() {
        let group = store_totals();
        let mut state = Accumulator::initial_state(&group);
        for (store, amount) in [("a", 2), ("b", 3), ("a", 5)] {
            Accumulator::accumulate(
                &group,
                &mut state,
                Sale {
                    store: store.into(),
                    amount,
                },
            );
        }

        assert_eq!(
            Accumulator::outputs(&group, &state),
            vec![total("a", 7), total("b", 3)]
        );
    }

    #[tokio::test]
    async fn an_emission_publishes_only_the_groups_each_input_updated() {
        let mut adapter = adapter(
            store_totals().emit(emit::always).updated_groups(),
            DEFAULT_MAX_LINEAGE_DEPTH,
        );
        let mut state = StatefulHandler::initial_state(&adapter);
        let mut previous_outputs = HashMap::new();

        for (id, store, amount, running) in [
            (1, "a", 2, 2),
            (2, "b", 3, 3),
            (3, "a", 5, 7),
            (4, "b", 7, 10),
        ] {
            let input = sale(id, store, amount);
            StatefulHandler::try_accumulate(&mut adapter, &mut state, input.clone())
                .expect("typed input decodes");
            assert!(StatefulHandler::should_emit(&adapter, &mut state));
            let probed = StatefulHandler::create_events(&adapter, &state).expect("probe lowers");
            let outputs = StatefulHandler::emit(&adapter, &mut state).expect("emission lowers");

            let parents: Vec<EventId> = std::iter::once(input.id)
                .chain(previous_outputs.get(store).copied())
                .collect();
            assert_eq!(published(&outputs), vec![(total(store, running), parents)]);
            assert_eq!(
                published(&probed),
                published(&outputs),
                "a probe moves nothing"
            );
            previous_outputs.insert(store, outputs[0].id);
        }

        let drained = StatefulHandler::drain(&adapter, &state)
            .await
            .expect("drain lowers");
        assert!(drained.is_empty(), "every update was already published");
    }

    #[tokio::test]
    async fn a_coarser_trigger_publishes_the_groups_updated_since_its_last_emission() {
        let mut adapter = adapter(
            store_totals().emit(emit::every_n(3)).updated_groups(),
            DEFAULT_MAX_LINEAGE_DEPTH,
        );
        let mut state = StatefulHandler::initial_state(&adapter);
        let inputs: Vec<_> = [(1, "a", 2), (2, "b", 3), (3, "a", 5)]
            .into_iter()
            .chain([(4, "a", 1), (5, "a", 1), (6, "a", 1), (7, "b", 1)])
            .map(|(id, store, amount)| sale(id, store, amount))
            .collect();
        let mut emissions = Vec::new();

        for input in &inputs {
            StatefulHandler::try_accumulate(&mut adapter, &mut state, input.clone())
                .expect("typed input decodes");
            if StatefulHandler::should_emit(&adapter, &mut state) {
                emissions
                    .push(StatefulHandler::emit(&adapter, &mut state).expect("emission lowers"));
            }
        }
        let drained = StatefulHandler::drain(&adapter, &state)
            .await
            .expect("drain lowers");

        let [first, second] = emissions.as_slice() else {
            panic!("two emissions every three inputs: {emissions:?}");
        };
        assert_eq!(
            published(first),
            vec![
                (total("a", 7), vec![inputs[0].id, inputs[2].id]),
                (total("b", 3), vec![inputs[1].id]),
            ]
        );
        assert_eq!(
            published(second),
            vec![(
                total("a", 10),
                vec![inputs[3].id, inputs[4].id, inputs[5].id, first[0].id]
            )],
            "b was not updated, so it publishes nothing"
        );
        assert_eq!(
            published(&drained),
            vec![(total("b", 4), vec![inputs[6].id, first[1].id])],
            "end of input publishes the input since the last emission"
        );
    }

    #[test]
    fn the_value_path_publishes_only_the_updated_groups() {
        let handler = store_totals().emit(emit::always).updated_groups();
        let mut state = handler.initial_state();
        let mut emissions = Vec::new();

        for (store, amount) in [("a", 2), ("b", 3), ("a", 5)] {
            handler.accumulate(
                &mut state,
                Sale {
                    store: store.into(),
                    amount,
                },
            );
            assert!(handler.should_emit(&state));
            let Ok(StatefulEmission::RetainEpoch { outputs }) = handler.emit(&state) else {
                panic!("an update emission keeps the state");
            };
            handler.post_emit(&mut state);
            emissions.push(outputs);
        }

        assert_eq!(
            emissions,
            [
                vec![total("a", 2)],
                vec![total("b", 3)],
                vec![total("a", 7)]
            ]
        );
        assert!(handler.drain(&state).expect("drain projects").is_empty());
    }

    #[test]
    fn keys_keep_their_first_input_order_after_moves_and_past_the_cap() {
        let inputs = [sale(1, "a", 1), sale(2, "b", 1), sale(3, "a", 1)];

        let mut always = adapter(
            store_totals().emit(emit::always).all_groups(),
            DEFAULT_MAX_LINEAGE_DEPTH,
        );
        let mut state = StatefulHandler::initial_state(&always);
        let mut emissions = Vec::new();
        for input in inputs.clone() {
            StatefulHandler::try_accumulate(&mut always, &mut state, input)
                .expect("typed input decodes");
            let outputs = StatefulHandler::emit(&always, &mut state).expect("emission lowers");
            emissions.push(stores(&outputs));
        }
        assert_eq!(emissions, [vec!["a"], vec!["a", "b"], vec!["a", "b"]]);

        let mut every_third = adapter(store_totals().emit(emit::every_n(3)).all_groups(), 1);
        let mut state = StatefulHandler::initial_state(&every_third);
        for input in inputs {
            StatefulHandler::try_accumulate(&mut every_third, &mut state, input)
                .expect("typed input decodes");
        }
        assert!(StatefulHandler::should_emit(&every_third, &mut state));
        let outputs = StatefulHandler::emit(&every_third, &mut state).expect("emission lowers");
        assert_eq!(stores(&outputs), ["a", "b"]);
    }

    #[test]
    fn a_key_past_the_cap_cites_its_latest_inputs_then_its_previous_output() {
        let mut adapter = adapter(store_totals().emit(emit::every_n(4)).updated_groups(), 2);
        let mut state = StatefulHandler::initial_state(&adapter);
        let inputs: Vec<_> = (1..=8).map(|id| sale(id, "a", 1)).collect();
        let mut outputs = Vec::new();

        for input in &inputs {
            StatefulHandler::try_accumulate(&mut adapter, &mut state, input.clone())
                .expect("typed input decodes");
            if StatefulHandler::should_emit(&adapter, &mut state) {
                outputs
                    .extend(StatefulHandler::emit(&adapter, &mut state).expect("emission lowers"));
            }
        }

        let [first, second] = outputs.as_slice() else {
            panic!("one key emits twice: {outputs:?}");
        };
        assert_eq!(first.causality.parent_ids, [inputs[2].id, inputs[3].id]);
        assert_eq!(
            second.causality.parent_ids,
            [inputs[6].id, inputs[7].id, first.id]
        );
    }

    #[test]
    fn an_output_citing_only_its_previous_output_keeps_its_correlation_and_replay_mark() {
        let mut adapter = adapter(
            store_totals().emit(emit::always).all_groups(),
            DEFAULT_MAX_LINEAGE_DEPTH,
        );
        let mut state = StatefulHandler::initial_state(&adapter);
        let mut a = sale(1, "a", 1);
        a.set_single_correlation(CorrelationId::from_ulid(Ulid::from(10_u128)), None);
        a.replay_context = Some(ReplayContext {
            original_event_id: EventId::from(Ulid::from(20_u128)),
            original_flow_id: "recorded-flow".into(),
            original_stage_id: StageId::new(),
        });

        StatefulHandler::try_accumulate(&mut adapter, &mut state, a.clone())
            .expect("typed input decodes");
        let first = StatefulHandler::emit(&adapter, &mut state).expect("emission lowers");
        StatefulHandler::try_accumulate(&mut adapter, &mut state, sale(2, "b", 1))
            .expect("typed input decodes");
        let second = StatefulHandler::emit(&adapter, &mut state).expect("emission lowers");

        let republished = &second[0];
        assert_eq!(stores(&second), ["a", "b"]);
        assert_eq!(republished.causality.parent_ids, [first[0].id]);
        assert_eq!(republished.correlation_id(), a.correlation_id());
        assert_eq!(republished.replay_context, a.replay_context);
    }
}
