// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: GroupBy Accumulator
//
// Groups events by a key field and maintains per-group state using a custom reduce function.
// This is the most common aggregation pattern in stream processing.

use super::Accumulator;
use crate::stages::stateful::strategies::accumulators::wrapper::StatefulWithEmission;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

/// Groups events by a key and maintains per-group state.
///
/// # Type Parameters
///
/// * `F` - The reduce function type: `Fn(&ChainEvent, &mut S)`
/// * `S` - The per-group state type
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::GroupBy;
///
/// #[derive(Clone, Debug, Default, Serialize)]
/// struct Stats {
///     count: u64,
///     total: f64,
/// }
///
/// let aggregator = GroupBy::new("user_id", |event: &ChainEvent, stats: &mut Stats| {
///     stats.count += 1;
///     stats.total += event.payload()["amount"].as_f64().unwrap_or(0.0);
/// });
/// ```
#[derive(Clone)]
pub struct GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Default,
{
    key_field: String,
    reduce_fn: F,
    writer_id: WriterId,
    _phantom: std::marker::PhantomData<S>,
}

impl<F, S> Debug for GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupBy")
            .field("key_field", &self.key_field)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<F, S> GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Default + Serialize,
{
    /// Create a new GroupBy accumulator.
    ///
    /// # Arguments
    ///
    /// * `key_field` - The field name to extract the grouping key from events
    /// * `reduce_fn` - Function to update per-group state with each event
    pub fn new<K: Into<String>>(key_field: K, reduce_fn: F) -> Self {
        Self {
            key_field: key_field.into(),
            reduce_fn,
            writer_id: WriterId::from(StageId::new()),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set a custom writer ID for emitted events.
    pub fn with_writer_id(mut self, writer_id: WriterId) -> Self {
        self.writer_id = writer_id;
        self
    }
}

impl<F, S> Accumulator for GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Default + Serialize,
{
    type State = HashMap<String, S>;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Extract key from event
        if let Some(key_value) = event.payload().get(&self.key_field) {
            if let Some(key) = key_value.as_str() {
                // Get or create group state
                let group_state = state.entry(key.to_string()).or_default();
                // Apply reduce function
                (self.reduce_fn)(&event, group_state);
            }
            // If key is not a string, we could support other types in the future
            // For now, we silently skip non-string keys
        }
        // If key field doesn't exist, skip this event
    }

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        state
            .iter()
            .map(|(key, group_state)| {
                ChainEventFactory::data_event(
                    self.writer_id,
                    "aggregated",
                    json!({
                        "key": key,
                        "result": group_state,
                    }),
                )
            })
            .collect()
    }

    fn reset(&self, state: &mut Self::State) {
        state.clear();
    }
}

/// Builder pattern methods for combining with emission strategies
impl<F, S> GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone + 'static,
    S: Clone + Send + Sync + Debug + Default + Serialize + 'static,
{
    /// Combine with a custom emission strategy.
    pub fn with_emission<E: EmissionStrategy + 'static>(
        self,
        emission: E,
    ) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }

    /// Emit only on EOF (completion).
    ///
    /// Results are emitted once when the stream completes.
    pub fn emit_on_eof(self) -> StatefulWithEmission<Self, OnEOF> {
        self.with_emission(OnEOF::new())
    }

    /// Emit every N events.
    ///
    /// Results are emitted periodically every N events processed.
    pub fn emit_every_n(self, count: u64) -> StatefulWithEmission<Self, EveryN> {
        self.with_emission(EveryN::new(count))
    }

    /// Emit within a time window.
    ///
    /// Results are emitted periodically based on wall-clock time.
    /// Note: This should only be used for observability, not correctness.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    ///
    /// Results are emitted immediately after processing each event,
    /// useful for materialized views or real-time dashboards.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::id::StageId;

    #[derive(Clone, Debug, Default, Serialize)]
    struct TestStats {
        count: u64,
        total: i64,
    }

    #[test]
    fn test_groupby_empty_state() {
        let accumulator = GroupBy::new("key", |_event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
        });
        let state = accumulator.initial_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_groupby_single_group() {
        let accumulator = GroupBy::new("category", |event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
            stats.total += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = accumulator.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({
                "category": "electronics",
                "value": 100
            }),
        );

        accumulator.accumulate(&mut state, event);

        assert_eq!(state.len(), 1);
        assert!(state.contains_key("electronics"));
        assert_eq!(state["electronics"].count, 1);
        assert_eq!(state["electronics"].total, 100);
    }

    #[test]
    fn test_groupby_multiple_groups() {
        let accumulator = GroupBy::new("category", |event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
            stats.total += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = accumulator.initial_state();

        // Add events for different categories
        let events = vec![
            ("electronics", 100),
            ("books", 50),
            ("electronics", 200),
            ("books", 30),
            ("clothing", 75),
        ];

        for (category, value) in events {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({
                    "category": category,
                    "value": value
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.len(), 3);
        assert_eq!(state["electronics"].count, 2);
        assert_eq!(state["electronics"].total, 300);
        assert_eq!(state["books"].count, 2);
        assert_eq!(state["books"].total, 80);
        assert_eq!(state["clothing"].count, 1);
        assert_eq!(state["clothing"].total, 75);
    }

    #[test]
    fn test_groupby_emit_format() {
        let accumulator = GroupBy::new("category", |event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
            stats.total += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = accumulator.initial_state();

        // Add some test data
        for (category, value) in &[("electronics", 100), ("books", 50)] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({
                    "category": category,
                    "value": value
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        let emitted = accumulator.emit(&state);

        assert_eq!(emitted.len(), 2);

        for event in emitted {
            assert_eq!(event.event_type(), "aggregated");
            let payload = event.payload();
            assert!(payload.get("key").is_some());
            assert!(payload.get("result").is_some());
        }
    }

    #[test]
    fn test_groupby_reset() {
        let accumulator = GroupBy::new("category", |_event: &ChainEvent, stats: &mut TestStats| {
            stats.count += 1;
        });

        let mut state = accumulator.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "category": "electronics" }),
        );

        accumulator.accumulate(&mut state, event);
        assert_eq!(state.len(), 1);

        accumulator.reset(&mut state);
        assert!(state.is_empty());
    }
}

// ============================================================================
// FLOWIP-080j: Typed GroupBy - Type-safe aggregation with domain types
// ============================================================================

/// Typed GroupBy for type-safe aggregation with automatic serde
///
/// This version of GroupBy works directly with domain types instead of ChainEvent,
/// eliminating manual serialization/deserialization boilerplate.
///
/// # Type Parameters
///
/// * `T` - Input value type (must implement `DeserializeOwned`)
/// * `K` - Key type for grouping (must implement `Hash + Eq + Clone`)
/// * `S` - Per-group state type (must implement `Default + Serialize`)
/// * `FKey` - Key extraction function: `Fn(&T) -> K`
/// * `FUpdate` - Update function: `Fn(&mut S, &T)` - state first, value second
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::GroupBy;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Flight {
///     carrier: String,
///     delay_minutes: u32,
/// }
///
/// #[derive(Clone, Debug, Default, Serialize)]
/// struct CarrierStats {
///     total_delay: u64,
///     flight_count: u64,
/// }
///
/// let aggregator = GroupBy::typed(
///     |flight: &Flight| flight.carrier.clone(),  // Key extractor
///     |stats: &mut CarrierStats, flight: &Flight| {  // CORRECTED: state first, value second
///         stats.total_delay += flight.delay_minutes as u64;
///         stats.flight_count += 1;
///     }
/// ).emit_on_eof();
/// ```
#[derive(Clone)]
pub struct GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone, // CORRECTED: state first, value second
{
    key_fn: FKey,
    update_fn: FUpdate,
    writer_id: WriterId,
    _phantom: PhantomData<(T, K, S)>,
}

impl<T, K, S, FKey, FUpdate> Debug for GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone, // CORRECTED: state first, value second
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupByTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("state_type", &std::any::type_name::<S>())
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<T, K, S, FKey, FUpdate> GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone, // CORRECTED: state first, value second
{
    /// Create a new typed GroupBy accumulator.
    ///
    /// Requires the state type `S` to implement `TypedPayload` for compile-time
    /// event type resolution.
    ///
    /// # Arguments
    ///
    /// * `key_fn` - Function to extract the grouping key from input events
    /// * `update_fn` - Function to update per-group state with each event
    pub fn new(key_fn: FKey, update_fn: FUpdate) -> Self {
        Self {
            key_fn,
            update_fn,
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

impl<T, K, S, FKey, FUpdate> Accumulator for GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + Send + Sync + 'static,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone + 'static, // CORRECTED: state first, value second
{
    type State = HashMap<K, S>;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Step 1: Deserialize ChainEvent → T
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_e) => {
                // Deserialization failed - event doesn't match input type, skip silently
                return;
            }
        };

        // Step 2: Extract key using typed function
        let key = (self.key_fn)(&input);

        // Step 3: Update state using typed function (CORRECTED: state first, value second)
        let group_state = state.entry(key).or_default();
        (self.update_fn)(group_state, &input);
    }

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        state
            .iter()
            .map(|(key, group_state)| {
                let key_json = serde_json::to_value(key).unwrap_or(serde_json::Value::Null);

                ChainEventFactory::data_event(
                    self.writer_id,
                    S::EVENT_TYPE,
                    json!({
                        "key": key_json,
                        "result": group_state,
                    }),
                )
            })
            .collect()
    }

    fn reset(&self, state: &mut Self::State) {
        state.clear();
    }
}

/// Builder pattern methods for combining with emission strategies
impl<T, K, S, FKey, FUpdate> GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + Send + Sync + 'static,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone + 'static, // CORRECTED: state first, value second
{
    /// Combine with a custom emission strategy.
    pub fn with_emission<E: EmissionStrategy + 'static>(
        self,
        emission: E,
    ) -> StatefulWithEmission<Self, E> {
        StatefulWithEmission::new(self, emission)
    }

    /// Emit only on EOF (completion).
    ///
    /// Results are emitted once when the stream completes.
    pub fn emit_on_eof(self) -> StatefulWithEmission<Self, OnEOF> {
        self.with_emission(OnEOF::new())
    }

    /// Emit every N events.
    ///
    /// Results are emitted periodically every N events processed.
    pub fn emit_every_n(self, count: u64) -> StatefulWithEmission<Self, EveryN> {
        self.with_emission(EveryN::new(count))
    }

    /// Emit within a time window.
    ///
    /// Results are emitted periodically based on wall-clock time.
    /// Note: This should only be used for observability, not correctness.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    ///
    /// Results are emitted immediately after processing each event,
    /// useful for materialized views or real-time dashboards.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

/// Convenience constructor on original GroupBy for creating typed variants
impl<F, S> GroupBy<F, S>
where
    F: Fn(&ChainEvent, &mut S) + Send + Sync + Clone,
    S: Clone + Send + Sync + Debug + Default + Serialize + TypedPayload,
{
    /// Create a typed GroupBy that works with domain types
    ///
    /// # Arguments
    ///
    /// * `key_fn` - Function to extract the grouping key: `Fn(&T) -> K`
    /// * `update_fn` - Function to update state: `Fn(&mut S, &T)` - state first, value second
    ///
    /// # Example
    ///
    /// ```ignore
    /// use obzenflow_runtime::stages::stateful::strategies::accumulators::GroupBy;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Flight { carrier: String, delay: u32 }
    ///
    /// #[derive(Default, Serialize)]
    /// struct Stats { count: u64 }
    ///
    /// let agg = GroupBy::typed(
    ///     |flight: &Flight| flight.carrier.clone(),
    ///     |stats: &mut Stats, flight: &Flight| { stats.count += 1; }  // CORRECTED: state first
    /// );
    /// ```
    pub fn typed<T, K, FKey, FUpdate>(
        key_fn: FKey,
        update_fn: FUpdate,
    ) -> GroupByTyped<T, K, S, FKey, FUpdate>
    where
        T: DeserializeOwned + Send + Sync,
        K: Hash + Eq + Clone + Debug + Send + Sync,
        FKey: Fn(&T) -> K + Send + Sync + Clone,
        FUpdate: Fn(&mut S, &T) + Send + Sync + Clone, // CORRECTED: state first, value second
    {
        GroupByTyped::new(key_fn, update_fn)
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::id::StageId;
    use serde::Deserialize;

    #[derive(Debug, Clone, Deserialize)]
    struct Product {
        category: String,
        price: i64,
        quantity: u32,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct CategoryStats {
        total_items: u64,
        total_revenue: i64,
    }

    impl obzenflow_core::TypedPayload for CategoryStats {
        const EVENT_TYPE: &'static str = "category.stats";
    }

    #[test]
    fn test_groupby_typed_empty_state() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |_stats: &mut CategoryStats, _product: &Product| {},
        );
        let state = accumulator.initial_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_groupby_typed_single_group() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |stats: &mut CategoryStats, product: &Product| {
                stats.total_items += product.quantity as u64;
                stats.total_revenue += product.price * product.quantity as i64;
            },
        );

        let mut state = accumulator.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "product",
            json!({
                "category": "electronics",
                "price": 100,
                "quantity": 2
            }),
        );

        accumulator.accumulate(&mut state, event);

        assert_eq!(state.len(), 1);
        assert!(state.contains_key("electronics"));
        assert_eq!(state["electronics"].total_items, 2);
        assert_eq!(state["electronics"].total_revenue, 200);
    }

    #[test]
    fn test_groupby_typed_multiple_groups() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |stats: &mut CategoryStats, product: &Product| {
                stats.total_items += product.quantity as u64;
                stats.total_revenue += product.price * product.quantity as i64;
            },
        );

        let mut state = accumulator.initial_state();

        // Add events for different categories
        let products = vec![
            ("electronics", 100, 2),
            ("books", 20, 3),
            ("electronics", 150, 1),
            ("books", 15, 2),
            ("clothing", 50, 5),
        ];

        for (category, price, quantity) in products {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "product",
                json!({
                    "category": category,
                    "price": price,
                    "quantity": quantity
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.len(), 3);

        // electronics: 2 items @ $100 + 1 item @ $150 = 3 items, $350 revenue
        assert_eq!(state["electronics"].total_items, 3);
        assert_eq!(state["electronics"].total_revenue, 350);

        // books: 3 items @ $20 + 2 items @ $15 = 5 items, $90 revenue
        assert_eq!(state["books"].total_items, 5);
        assert_eq!(state["books"].total_revenue, 90);

        // clothing: 5 items @ $50 = 5 items, $250 revenue
        assert_eq!(state["clothing"].total_items, 5);
        assert_eq!(state["clothing"].total_revenue, 250);
    }

    #[test]
    fn test_groupby_typed_emit_format() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |stats: &mut CategoryStats, product: &Product| {
                stats.total_items += product.quantity as u64;
                stats.total_revenue += product.price * product.quantity as i64;
            },
        );

        let mut state = accumulator.initial_state();

        // Add some test data
        for (category, price, quantity) in &[("electronics", 100, 2), ("books", 20, 3)] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "product",
                json!({
                    "category": category,
                    "price": price,
                    "quantity": quantity
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        let emitted = accumulator.emit(&state);

        assert_eq!(emitted.len(), 2);

        for event in emitted {
            assert_eq!(event.event_type(), CategoryStats::EVENT_TYPE);
            let payload = event.payload();
            assert!(payload.get("key").is_some());
            assert!(payload.get("result").is_some());

            // Verify result structure
            let result = &payload["result"];
            assert!(result.get("total_items").is_some());
            assert!(result.get("total_revenue").is_some());
        }
    }

    #[test]
    fn test_groupby_typed_reset() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |stats: &mut CategoryStats, product: &Product| {
                stats.total_items += product.quantity as u64;
            },
        );

        let mut state = accumulator.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "product",
            json!({
                "category": "electronics",
                "price": 100,
                "quantity": 2
            }),
        );

        accumulator.accumulate(&mut state, event);
        assert_eq!(state.len(), 1);

        accumulator.reset(&mut state);
        assert!(state.is_empty());
    }

    #[test]
    fn test_groupby_typed_skips_invalid_events() {
        let accumulator = GroupByTyped::new(
            |product: &Product| product.category.clone(),
            |stats: &mut CategoryStats, product: &Product| {
                stats.total_items += product.quantity as u64;
            },
        );

        let mut state = accumulator.initial_state();

        // Valid event
        let valid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "product",
            json!({
                "category": "electronics",
                "price": 100,
                "quantity": 2
            }),
        );

        // Invalid event - missing required fields
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "product",
            json!({
                "category": "books",
                // missing price and quantity
            }),
        );

        accumulator.accumulate(&mut state, valid_event);
        accumulator.accumulate(&mut state, invalid_event);

        // Only the valid event should have been processed
        assert_eq!(state.len(), 1);
        assert!(state.contains_key("electronics"));
        assert!(!state.contains_key("books"));
    }

    #[test]
    fn test_groupby_typed_numeric_keys() {
        #[derive(Debug, Clone, Deserialize)]
        struct Sale {
            store_id: u32,
            amount: f64,
        }

        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        struct StoreStats {
            total_sales: f64,
            transaction_count: u64,
        }

        impl obzenflow_core::TypedPayload for StoreStats {
            const EVENT_TYPE: &'static str = "store.stats";
        }

        let accumulator = GroupByTyped::new(
            |sale: &Sale| sale.store_id,
            |stats: &mut StoreStats, sale: &Sale| {
                stats.total_sales += sale.amount;
                stats.transaction_count += 1;
            },
        );

        let mut state = accumulator.initial_state();

        let sales = vec![(1, 100.0), (2, 50.0), (1, 75.0), (3, 200.0), (2, 25.0)];

        for (store_id, amount) in sales {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "sale",
                json!({
                    "store_id": store_id,
                    "amount": amount
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.len(), 3);
        assert_eq!(state[&1].transaction_count, 2);
        assert_eq!(state[&1].total_sales, 175.0);
        assert_eq!(state[&2].transaction_count, 2);
        assert_eq!(state[&2].total_sales, 75.0);
        assert_eq!(state[&3].transaction_count, 1);
        assert_eq!(state[&3].total_sales, 200.0);
    }
}
