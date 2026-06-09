// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: Reduce Accumulator
//
// Simple reduce/fold accumulator that aggregates all events into a single state value.
// No grouping - just a single accumulation across all events.

use super::Accumulator;
use crate::stages::stateful::strategies::accumulators::trace::TraceState;
use crate::stages::stateful::strategies::accumulators::wrapper::StatefulWithEmission;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use crate::typing::StatefulTyping;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::Serialize;
use serde_json::json;
use std::fmt::Debug;
use std::time::Duration;

/// Simple reduce/fold accumulator (no grouping).
///
/// Aggregates all events into a single state value using a reduce function.
///
/// # Type Parameters
///
/// * `F` - The reduce function type: `Fn(&mut S, &ChainEvent)`
/// * `S` - The accumulated state type
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::Reduce;
///
/// // Sum all values
/// let sum_reducer = Reduce::new(0i64, |total: &mut i64, event: &ChainEvent| {
///     *total += event.payload()["value"].as_i64().unwrap_or(0);
/// });
///
/// // Count events
/// let counter = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
///     *count += 1;
/// });
/// ```
#[derive(Clone)]
pub struct Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug,
{
    reduce_fn: F,
    initial: S,
    writer_id: WriterId,
}

#[derive(Clone, Debug)]
pub struct ReduceState<S>
where
    S: Clone + Send + Sync + Debug,
{
    value: S,
    trace: TraceState,
}

impl<F, S> Debug for Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reduce")
            .field("initial", &self.initial)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<F, S> Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Serialize,
{
    /// Create a new Reduce accumulator.
    ///
    /// # Arguments
    ///
    /// * `initial` - The initial state value
    /// * `reduce_fn` - Function to fold each event into the state
    pub fn new(initial: S, reduce_fn: F) -> Self {
        Self {
            reduce_fn,
            initial,
            writer_id: WriterId::from(StageId::new()),
        }
    }

    /// Set a custom writer ID for emitted events.
    pub fn with_writer_id(mut self, writer_id: WriterId) -> Self {
        self.writer_id = writer_id;
        self
    }
}

impl<F, S> Accumulator for Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Serialize,
{
    type State = ReduceState<S>;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        (self.reduce_fn)(&mut state.value, &event);
        state.trace.record_event(&event);
    }

    fn initial_state(&self) -> Self::State {
        ReduceState {
            value: self.initial.clone(),
            trace: TraceState::default(),
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        if state.trace.is_empty() {
            return Vec::new();
        }

        let mut out = ChainEventFactory::data_event(
            self.writer_id,
            "reduced",
            json!({
                "result": &state.value,
            }),
        );

        out.causality = CausalityContext {
            parent_ids: state.trace.parent_ids(),
        };

        state.trace.apply_correlation_to_event(&mut out);

        vec![out]
    }

    fn reset(&self, state: &mut Self::State) {
        state.value = self.initial.clone();
        state.trace.reset();
    }
}

/// Builder pattern methods for combining with emission strategies
impl<F, S> Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone + 'static,
    S: Clone + Send + Sync + Debug + Serialize + 'static,
{
    /// Combine with a custom emission strategy.
    pub fn with_emission<E: EmissionStrategy + 'static>(
        self,
        emission: E,
    ) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }

    /// Emit only on EOF (completion).
    pub fn emit_on_eof(self) -> StatefulWithEmission<Self, OnEOF> {
        self.with_emission(OnEOF::new())
    }

    /// Emit every N events.
    pub fn emit_every_n(self, count: u64) -> StatefulWithEmission<Self, EveryN> {
        self.with_emission(EveryN::new(count))
    }

    /// Emit within a time window.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;

    #[test]
    fn test_reduce_initial_state() {
        let accumulator = Reduce::new(42i64, |state: &mut i64, _event: &ChainEvent| {
            *state += 1;
        });
        assert_eq!(accumulator.initial_state().value, 42);
    }

    #[test]
    fn test_reduce_accumulates() {
        let accumulator = Reduce::new(0i64, |sum: &mut i64, event: &ChainEvent| {
            *sum += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = accumulator.initial_state();

        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "value": 10 }),
        );
        accumulator.accumulate(&mut state, event1);
        assert_eq!(state.value, 10);

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "value": 20 }),
        );
        accumulator.accumulate(&mut state, event2);
        assert_eq!(state.value, 30);
    }

    #[test]
    fn test_reduce_counter() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });

        let mut state = accumulator.initial_state();

        for _ in 0..5 {
            let event =
                ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}));
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.value, 5);
    }

    #[test]
    fn test_reduce_emit_format() {
        // Arbitrary seed; the no-op reduce must pass it through to `result` unchanged.
        const SEED: i64 = 105;

        let accumulator = Reduce::new(SEED, |_state: &mut i64, _event: &ChainEvent| {});

        let mut state = accumulator.initial_state();
        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}))
                .with_new_correlation("reduce_emit_format");
        let input_id = event.id;
        let input_correlation = event.correlation.clone();

        accumulator.accumulate(&mut state, event);
        let emitted = accumulator.emit(&state);

        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].event_type(), "reduced");
        assert_eq!(emitted[0].payload()["result"], SEED);

        // FLOWIP-054j fan-in lineage: the aggregate is parented to the contributing
        // input rather than left at root causality.
        assert_eq!(emitted[0].causality.parent_ids, vec![input_id]);
        // A single-correlation window carries that scalar correlation through, with no mixed set.
        assert_eq!(emitted[0].correlation, input_correlation);
    }

    #[test]
    fn test_reduce_emit_noops_when_no_input() {
        // The `is_empty()` guard in `emit` must suppress output when no data event was
        // accumulated, so an empty window does not emit a spurious seed-value aggregate.
        let accumulator = Reduce::new(105i64, |_state: &mut i64, _event: &ChainEvent| {});
        let state = accumulator.initial_state();
        assert!(accumulator.emit(&state).is_empty());
    }

    #[test]
    fn test_reduce_reset() {
        let accumulator = Reduce::new(0i64, |sum: &mut i64, event: &ChainEvent| {
            *sum += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = accumulator.initial_state();
        state.value = 100;
        accumulator.reset(&mut state);
        assert_eq!(state.value, 0);
    }

    #[test]
    fn test_reduce_complex_state() {
        #[derive(Clone, Debug, Serialize, PartialEq)]
        struct Stats {
            count: u64,
            sum: f64,
            min: f64,
            max: f64,
        }

        let initial = Stats {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        };

        let accumulator = Reduce::new(initial.clone(), |stats: &mut Stats, event: &ChainEvent| {
            if let Some(value) = event.payload()["value"].as_f64() {
                stats.count += 1;
                stats.sum += value;
                stats.min = stats.min.min(value);
                stats.max = stats.max.max(value);
            }
        });

        let mut state = accumulator.initial_state();

        for value in &[10.0, 5.0, 20.0, 15.0] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({ "value": value }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.value.count, 4);
        assert_eq!(state.value.sum, 50.0);
        assert_eq!(state.value.min, 5.0);
        assert_eq!(state.value.max, 20.0);
    }
}

// ============================================================================
// FLOWIP-080j: Typed Reduce - Type-safe aggregation with domain types
// ============================================================================

use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// Typed Reduce for type-safe aggregation with automatic serde
///
/// This version of Reduce works directly with domain types instead of ChainEvent,
/// eliminating manual serialization/deserialization boilerplate.
///
/// # Type Parameters
///
/// * `T` - Input event type (must implement `DeserializeOwned`)
/// * `S` - Accumulated state type (must implement `Clone + Serialize`)
/// * `F` - Reduce function: `Fn(&T, &mut S)`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::Reduce;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct OrderEvent {
///     amount: f64,
///     items: u32,
/// }
///
/// #[derive(Clone, Debug, Serialize)]
/// struct OrderStats {
///     total_revenue: f64,
///     total_items: u32,
/// }
///
/// let reducer = Reduce::typed(
///     OrderStats { total_revenue: 0.0, total_items: 0 },
///     |order: &OrderEvent, stats: &mut OrderStats| {
///         stats.total_revenue += order.amount;
///         stats.total_items += order.items;
///     }
/// ).emit_on_eof();
/// ```
#[derive(Clone)]
pub struct ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    reduce_fn: F,
    initial: S,
    writer_id: WriterId,
    _phantom: PhantomData<T>,
}

impl<T, S, F> Debug for ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReduceTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("state_type", &std::any::type_name::<S>())
            .field("initial", &self.initial)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<T, S, F> ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    /// Create a new typed Reduce accumulator.
    ///
    /// Requires the state type `S` to implement `TypedPayload` for compile-time
    /// event type resolution.
    ///
    /// # Arguments
    ///
    /// * `initial` - The initial state value
    /// * `reduce_fn` - Function to fold each typed event into the state
    pub fn new(initial: S, reduce_fn: F) -> Self {
        Self {
            reduce_fn,
            initial,
            writer_id: WriterId::from(StageId::new()),
            _phantom: PhantomData,
        }
    }

    /// Set a custom writer ID for emitted events.
    pub fn with_writer_id(mut self, writer_id: WriterId) -> Self {
        self.writer_id = writer_id;
        self
    }
}

impl<T, S, F> StatefulTyping for ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    type Input = T;
    type Output = S;
}

impl<T, S, F> Accumulator for ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload + 'static,
    F: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
{
    type State = ReduceState<S>;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Step 1: Deserialize ChainEvent → T
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_e) => {
                // Deserialization failed - event doesn't match input type, skip silently
                return;
            }
        };

        // Step 2: Apply reduce function with typed input
        (self.reduce_fn)(&mut state.value, &input);
        state.trace.record_event(&event);
    }

    fn initial_state(&self) -> Self::State {
        ReduceState {
            value: self.initial.clone(),
            trace: TraceState::default(),
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        if state.trace.is_empty() {
            return Vec::new();
        }

        let payload =
            serde_json::to_value(&state.value).expect("ReduceTyped failed to serialize state");
        let mut out =
            ChainEventFactory::data_event(self.writer_id, S::versioned_event_type(), payload);

        out.causality = CausalityContext {
            parent_ids: state.trace.parent_ids(),
        };

        state.trace.apply_correlation_to_event(&mut out);

        vec![out]
    }

    fn reset(&self, state: &mut Self::State) {
        state.value = self.initial.clone();
        state.trace.reset();
    }
}

/// Builder pattern methods for combining with emission strategies
impl<T, S, F> ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload + 'static,
    F: Fn(&mut S, &T) + Send + Sync + Clone + 'static,
{
    /// Combine with a custom emission strategy.
    pub fn with_emission<E: EmissionStrategy + 'static>(
        self,
        emission: E,
    ) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }

    /// Emit only on EOF (completion).
    pub fn emit_on_eof(self) -> StatefulWithEmission<Self, OnEOF> {
        self.with_emission(OnEOF::new())
    }

    /// Emit every N events.
    pub fn emit_every_n(self, count: u64) -> StatefulWithEmission<Self, EveryN> {
        self.with_emission(EveryN::new(count))
    }

    /// Emit within a time window.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

/// Convenience constructor on original Reduce for creating typed variants
impl<F, S> Reduce<F, S>
where
    F: Fn(&mut S, &ChainEvent) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
{
    /// Create a typed Reduce that works with domain types
    ///
    /// Requires the state type `S` to implement `TypedPayload`.
    ///
    /// # Arguments
    ///
    /// * `initial` - The initial state value
    /// * `reduce_fn` - Function to fold each typed event: `Fn(&T, &mut S)`
    ///
    /// # Example
    ///
    /// ```ignore
    /// use obzenflow_runtime::stages::stateful::strategies::accumulators::Reduce;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct OrderEvent { amount: f64 }
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Total { sum: f64 }
    ///
    /// let reducer = Reduce::typed(
    ///     Total { sum: 0.0 },
    ///     |order: &OrderEvent, total: &mut Total| { total.sum += order.amount; }
    /// );
    /// ```
    pub fn typed<T, FTyped>(initial: S, reduce_fn: FTyped) -> ReduceTyped<T, S, FTyped>
    where
        T: DeserializeOwned + Send + Sync,
        FTyped: Fn(&mut S, &T) + Send + Sync + Clone,
    {
        ReduceTyped::new(initial, reduce_fn)
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use serde::Deserialize;

    #[derive(Debug, Clone, Deserialize)]
    struct Transaction {
        amount: f64,
        quantity: u32,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TransactionStats {
        total_amount: f64,
        total_quantity: u64,
        transaction_count: u64,
    }

    impl obzenflow_core::TypedPayload for TransactionStats {
        const EVENT_TYPE: &'static str = "transaction.stats";
    }

    #[test]
    fn test_reduce_typed_initial_state() {
        let initial = TransactionStats {
            total_amount: 100.0,
            total_quantity: 10,
            transaction_count: 0,
        };

        let accumulator = ReduceTyped::new(
            initial.clone(),
            |_stats: &mut TransactionStats, _tx: &Transaction| {},
        );

        assert_eq!(accumulator.initial_state().value, initial);
    }

    #[test]
    fn test_reduce_typed_accumulates() {
        let accumulator = ReduceTyped::new(
            TransactionStats {
                total_amount: 0.0,
                total_quantity: 0,
                transaction_count: 0,
            },
            |stats: &mut TransactionStats, tx: &Transaction| {
                stats.total_amount += tx.amount;
                stats.total_quantity += tx.quantity as u64;
                stats.transaction_count += 1;
            },
        );

        let mut state = accumulator.initial_state();

        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({ "amount": 50.0, "quantity": 2 }),
        );
        accumulator.accumulate(&mut state, event1);

        assert_eq!(state.value.total_amount, 50.0);
        assert_eq!(state.value.total_quantity, 2);
        assert_eq!(state.value.transaction_count, 1);

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({ "amount": 75.0, "quantity": 3 }),
        );
        accumulator.accumulate(&mut state, event2);

        assert_eq!(state.value.total_amount, 125.0);
        assert_eq!(state.value.total_quantity, 5);
        assert_eq!(state.value.transaction_count, 2);
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Counter {
        count: u64,
    }

    impl obzenflow_core::TypedPayload for Counter {
        const EVENT_TYPE: &'static str = "counter";
    }

    #[test]
    fn test_reduce_typed_counter() {
        #[derive(Deserialize)]
        struct Event {
            _data: String,
        }

        let accumulator = ReduceTyped::new(
            Counter { count: 0 },
            |counter: &mut Counter, _event: &Event| {
                counter.count += 1;
            },
        );

        let mut state = accumulator.initial_state();

        for _ in 0..5 {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "event",
                json!({ "_data": "test" }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.value.count, 5);
    }

    #[test]
    fn test_reduce_typed_emit_format() {
        // Arbitrary seed; the no-op reduce must pass it through to `count` unchanged.
        const SEED: u64 = 105;

        let accumulator = ReduceTyped::new(
            Counter { count: SEED },
            |_counter: &mut Counter, _tx: &Transaction| {},
        );

        let mut state = accumulator.initial_state();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({ "amount": 1.0, "quantity": 1 }),
        )
        .with_new_correlation("reduce_typed_emit_format");
        let input_id = event.id;
        let input_correlation = event.correlation.clone();

        accumulator.accumulate(&mut state, event);
        let emitted = accumulator.emit(&state);

        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].event_type(), Counter::versioned_event_type());
        assert_eq!(emitted[0].payload()["count"], SEED);

        // FLOWIP-054j fan-in lineage applies on the typed path too.
        assert_eq!(emitted[0].causality.parent_ids, vec![input_id]);
        assert_eq!(emitted[0].correlation, input_correlation);
    }

    #[test]
    fn test_reduce_typed_reset() {
        let initial = TransactionStats {
            total_amount: 0.0,
            total_quantity: 0,
            transaction_count: 0,
        };

        let accumulator = ReduceTyped::new(
            initial.clone(),
            |stats: &mut TransactionStats, tx: &Transaction| {
                stats.total_amount += tx.amount;
            },
        );

        let mut state = accumulator.initial_state();
        state.value = TransactionStats {
            total_amount: 1000.0,
            total_quantity: 50,
            transaction_count: 10,
        };

        accumulator.reset(&mut state);
        assert_eq!(state.value, initial);
    }

    #[test]
    fn test_reduce_typed_skips_invalid_events() {
        let accumulator = ReduceTyped::new(
            Counter { count: 0 },
            |counter: &mut Counter, _tx: &Transaction| {
                counter.count += 1;
            },
        );

        let mut state = accumulator.initial_state();

        // Valid event
        let valid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({ "amount": 50.0, "quantity": 2 }),
        );

        // Invalid event - wrong structure
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "transaction",
            json!({ "invalid_field": "oops" }),
        );

        accumulator.accumulate(&mut state, valid_event);
        accumulator.accumulate(&mut state, invalid_event);

        // Only the valid event should have been processed
        assert_eq!(state.value.count, 1);
    }

    #[test]
    fn test_reduce_typed_complex_aggregation() {
        #[derive(Deserialize)]
        struct Measurement {
            value: f64,
        }

        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
        struct Statistics {
            count: u64,
            sum: f64,
            min: f64,
            max: f64,
        }

        impl obzenflow_core::TypedPayload for Statistics {
            const EVENT_TYPE: &'static str = "measurement.statistics";
        }

        let initial = Statistics {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        };

        let accumulator = ReduceTyped::new(
            initial,
            |stats: &mut Statistics, measurement: &Measurement| {
                stats.count += 1;
                stats.sum += measurement.value;
                stats.min = stats.min.min(measurement.value);
                stats.max = stats.max.max(measurement.value);
            },
        );

        let mut state = accumulator.initial_state();

        for value in &[10.0, 5.0, 20.0, 15.0] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "measurement",
                json!({ "value": value }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.value.count, 4);
        assert_eq!(state.value.sum, 50.0);
        assert_eq!(state.value.min, 5.0);
        assert_eq!(state.value.max, 20.0);
    }
}
