// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: TopNBy Accumulator (with aggregation)
// FLOWIP-080j: TopNByTyped - Typed variant (Phase 4)
//
// Maintains the top N items by accumulated score, perfect for analytics
// like "top products by sales", "top users by activity", etc.

use super::Accumulator;
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::id::StageId;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

/// Aggregated item with accumulated score and metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregatedItem {
    pub key: String,
    pub total_score: f64,
    pub count: u64,
    pub metadata: Value,
}

impl PartialEq for AggregatedItem {
    fn eq(&self, other: &Self) -> bool {
        self.total_score == other.total_score && self.key == other.key
    }
}

impl Eq for AggregatedItem {}

impl PartialOrd for AggregatedItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AggregatedItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_score
            .partial_cmp(&other.total_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.key.cmp(&other.key))
    }
}

/// Accumulator that maintains top N items by accumulated score.
///
/// Unlike TopN which replaces on duplicate keys, TopNBy accumulates
/// scores for the same key, perfect for analytics scenarios.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::TopNBy;
///
/// // Simple field-based API - no boilerplate!
/// let top_products = TopNBy::new(10, "product_id", "sale_amount");
///
/// // Or with nested fields
/// let top_users = TopNBy::new(5, "user.id", "metrics.score");
///
/// // Or with custom extractors for complex logic
/// let custom = TopNBy::with_extractors(10,
///     |event| event.payload()["id"].as_str().map(|s| s.to_string()),
///     |event| event.payload()["amount"].as_f64().unwrap_or(0.0)
/// );
/// ```
pub struct TopNBy<K, S> {
    n: usize,
    key_extractor: K,
    score_extractor: S,
    writer_id: WriterId,
}

// Field-based constructor (the simple API)
impl TopNBy<FieldExtractor, FieldExtractor> {
    /// Create a new TopNBy accumulator with field paths.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of items to track
    /// * `key_field` - Dot-separated path to key field (e.g., "product_id" or "user.id")
    /// * `value_field` - Dot-separated path to value field (e.g., "amount" or "metrics.score")
    pub fn new(n: usize, key_field: &str, value_field: &str) -> Self {
        assert!(n > 0, "TopNBy must track at least 1 item");
        Self {
            n,
            key_extractor: FieldExtractor::new(key_field, true),
            score_extractor: FieldExtractor::new(value_field, false),
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

// Custom extractor constructor for backwards compatibility
impl<K, S> TopNBy<K, S> {
    /// Create a TopNBy with custom extractor functions.
    ///
    /// Use this for complex extraction logic that can't be expressed as simple field paths.
    pub fn with_extractors(n: usize, key_extractor: K, score_extractor: S) -> Self {
        assert!(n > 0, "TopNBy must track at least 1 item");
        Self {
            n,
            key_extractor,
            score_extractor,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

/// Field extractor for simple dot-notation field access
#[derive(Clone, Debug)]
pub struct FieldExtractor {
    path: String,
    as_string: bool,
}

impl FieldExtractor {
    fn new(path: &str, as_string: bool) -> Self {
        Self {
            path: path.to_string(),
            as_string,
        }
    }

    fn extract(&self, event: &ChainEvent) -> Option<String> {
        self.extract_value(&event.payload()).and_then(|v| {
            if self.as_string {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_i64().map(|i| i.to_string()))
                    .or_else(|| v.as_u64().map(|u| u.to_string()))
            } else {
                None
            }
        })
    }

    fn extract_number(&self, event: &ChainEvent) -> f64 {
        self.extract_value(&event.payload())
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_i64().map(|i| i as f64))
                    .or_else(|| v.as_u64().map(|u| u as f64))
            })
            .unwrap_or(0.0)
    }

    fn extract_value<'a>(&self, mut current: &'a Value) -> Option<&'a Value> {
        for part in self.path.split('.') {
            current = current.get(part)?;
        }
        Some(current)
    }
}

impl<K, S> Debug for TopNBy<K, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNBy")
            .field("n", &self.n)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<K, S> Clone for TopNBy<K, S>
where
    K: Clone,
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            n: self.n,
            key_extractor: self.key_extractor.clone(),
            score_extractor: self.score_extractor.clone(),
            writer_id: self.writer_id,
        }
    }
}

/// State for TopNBy accumulator - HashMap for aggregation + heap for ranking
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopNByState {
    /// All accumulated items by key
    items: HashMap<String, AggregatedItem>,
    /// Maximum number of items to emit
    capacity: usize,
}

// Implementation for FieldExtractor (simple string-based API)
impl Accumulator for TopNBy<FieldExtractor, FieldExtractor> {
    type State = TopNByState;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Extract key from event using field path
        if let Some(key) = self.key_extractor.extract(&event) {
            let score = self.score_extractor.extract_number(&event);

            // Accumulate score for this key
            state
                .items
                .entry(key.clone())
                .and_modify(|item| {
                    item.total_score += score;
                    item.count += 1;
                    // Update metadata with latest event's payload
                    item.metadata = event.payload().clone();
                })
                .or_insert_with(|| AggregatedItem {
                    key,
                    total_score: score,
                    count: 1,
                    metadata: event.payload().clone(),
                });
        }
    }

    fn initial_state(&self) -> Self::State {
        TopNByState {
            items: HashMap::new(),
            capacity: self.n,
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        emit_top_n(state, self.writer_id)
    }

    fn reset(&self, state: &mut Self::State) {
        state.items.clear();
    }
}

// Implementation for custom extractors
impl<K, S> Accumulator for TopNBy<K, S>
where
    K: Fn(&ChainEvent) -> Option<String> + Send + Sync + Clone + 'static,
    S: Fn(&ChainEvent) -> f64 + Send + Sync + Clone + 'static,
{
    type State = TopNByState;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Extract key from event
        if let Some(key) = (self.key_extractor)(&event) {
            let score = (self.score_extractor)(&event);

            // Accumulate score for this key
            state
                .items
                .entry(key.clone())
                .and_modify(|item| {
                    item.total_score += score;
                    item.count += 1;
                    // Update metadata with latest event's payload
                    item.metadata = event.payload().clone();
                })
                .or_insert_with(|| AggregatedItem {
                    key,
                    total_score: score,
                    count: 1,
                    metadata: event.payload().clone(),
                });
        }
    }

    fn initial_state(&self) -> Self::State {
        TopNByState {
            items: HashMap::new(),
            capacity: self.n,
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        emit_top_n(state, self.writer_id)
    }

    fn reset(&self, state: &mut Self::State) {
        state.items.clear();
    }
}

// Shared emit function
fn emit_top_n(state: &TopNByState, writer_id: WriterId) -> Vec<ChainEvent> {
    // Build a min-heap to find top N
    let mut heap: BinaryHeap<Reverse<AggregatedItem>> = BinaryHeap::new();

    for item in state.items.values() {
        heap.push(Reverse(item.clone()));

        // Keep only top N items in heap
        if heap.len() > state.capacity {
            heap.pop();
        }
    }

    // Extract and sort by score descending
    let mut top_items: Vec<_> = heap.into_iter().map(|Reverse(item)| item).collect();
    top_items.sort_by(|a, b| {
        b.total_score
            .partial_cmp(&a.total_score)
            .unwrap_or(Ordering::Equal)
    });

    // Create event with top N results
    vec![ChainEventFactory::data_event(
        writer_id,
        "top_n_by_result",
        json!({
            "top_n": top_items.iter().enumerate().map(|(rank, item)| {
                json!({
                    "rank": rank + 1,
                    "key": item.key,
                    "total_score": item.total_score,
                    "count": item.count,
                    "avg_score": item.total_score / item.count as f64,
                    "metadata": item.metadata,
                })
            }).collect::<Vec<_>>(),
            "total_items": state.items.len(),
            "capacity": state.capacity,
        }),
    )]
}

/// Builder extensions for field-based TopNBy
impl TopNBy<FieldExtractor, FieldExtractor> {
    /// Configure to emit results when EOF is received.
    pub fn emit_on_eof(
        self,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::OnEOF>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::OnEOF::new(),
        )
    }

    /// Configure to emit results every N events.
    pub fn emit_every_n(
        self,
        n: u64,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::EveryN>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::EveryN::new(n),
        )
    }

    /// Configure to emit results within a time window.
    pub fn emit_within(
        self,
        duration: std::time::Duration,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::TimeWindow>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::TimeWindow::new(duration),
        )
    }

    /// Configure with a custom emission strategy.
    pub fn with_emission<E>(self, emission: E) -> super::StatefulWithEmission<Self, E>
    where
        E: crate::stages::stateful::strategies::emissions::EmissionStrategy,
    {
        super::StatefulWithEmission::new(self, emission)
    }
}

/// Builder extensions for custom extractor TopNBy
impl<K, S> TopNBy<K, S>
where
    K: Fn(&ChainEvent) -> Option<String> + Send + Sync + Clone + 'static,
    S: Fn(&ChainEvent) -> f64 + Send + Sync + Clone + 'static,
{
    /// Configure to emit results when EOF is received.
    pub fn emit_on_eof(
        self,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::OnEOF>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::OnEOF::new(),
        )
    }

    /// Configure to emit results every N events.
    pub fn emit_every_n(
        self,
        n: u64,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::EveryN>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::EveryN::new(n),
        )
    }

    /// Configure to emit results within a time window.
    pub fn emit_within(
        self,
        duration: std::time::Duration,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::TimeWindow>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::TimeWindow::new(duration),
        )
    }

    /// Configure with a custom emission strategy.
    pub fn with_emission<E>(self, emission: E) -> super::StatefulWithEmission<Self, E>
    where
        E: crate::stages::stateful::strategies::emissions::EmissionStrategy,
    {
        super::StatefulWithEmission::new(self, emission)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use serde_json::json;

    #[test]
    fn test_top_n_by_accumulation() {
        // Simple field-based API!
        let accumulator = TopNBy::new(3, "product", "amount");

        let mut state = accumulator.initial_state();

        // Multiple sales for same product
        for (product, amount) in &[
            ("Widget", 100.0),
            ("Gadget", 150.0),
            ("Widget", 200.0), // Another Widget sale
            ("Doohickey", 75.0),
            ("Gadget", 100.0), // Another Gadget sale
            ("Widget", 50.0),  // Third Widget sale
            ("Thingamajig", 300.0),
        ] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "sale",
                json!({ "product": product, "amount": amount }),
            );
            accumulator.accumulate(&mut state, event);
        }

        // Widget: 100 + 200 + 50 = 350 (3 sales)
        // Gadget: 150 + 100 = 250 (2 sales)
        // Thingamajig: 300 (1 sale)
        // Doohickey: 75 (1 sale) - should not be in top 3

        let results = accumulator.emit(&state);
        assert_eq!(results.len(), 1);

        let top_n = &results[0].payload()["top_n"];
        assert_eq!(top_n[0]["key"], "Widget");
        assert_eq!(top_n[0]["total_score"], 350.0);
        assert_eq!(top_n[0]["count"], 3);

        assert_eq!(top_n[1]["key"], "Thingamajig");
        assert_eq!(top_n[1]["total_score"], 300.0);
        assert_eq!(top_n[1]["count"], 1);

        assert_eq!(top_n[2]["key"], "Gadget");
        assert_eq!(top_n[2]["total_score"], 250.0);
        assert_eq!(top_n[2]["count"], 2);
    }

    #[test]
    fn test_top_n_by_avg_score() {
        // Simple field-based API with different fields
        let accumulator = TopNBy::new(2, "user", "score");

        let mut state = accumulator.initial_state();

        // Users with multiple scores
        for (user, score) in &[
            ("alice", 80.0),
            ("bob", 90.0),
            ("alice", 70.0),   // Alice avg: 75
            ("bob", 100.0),    // Bob avg: 95
            ("charlie", 85.0), // Charlie avg: 85
        ] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({ "user": user, "score": score }),
            );
            accumulator.accumulate(&mut state, event);
        }

        let results = accumulator.emit(&state);
        let top_n = &results[0].payload()["top_n"];

        // Top 2 by total: Bob (190), Alice (150)
        assert_eq!(top_n[0]["key"], "bob");
        assert_eq!(top_n[0]["total_score"], 190.0);
        assert_eq!(top_n[0]["avg_score"], 95.0);

        assert_eq!(top_n[1]["key"], "alice");
        assert_eq!(top_n[1]["total_score"], 150.0);
        assert_eq!(top_n[1]["avg_score"], 75.0);
    }
}

// ============================================================================
// FLOWIP-080j: TopNByTyped - Type-safe TopNBy accumulator
// ============================================================================

/// FLOWIP-080j: Type-safe TopNBy accumulator that works with domain types.
///
/// Maintains top N items by **accumulated** score. When duplicate keys arrive, **adds** to the total.
/// Perfect for analytics: top products by total sales, top users by cumulative activity.
///
/// # Type Parameters
///
/// * `T` - Input event type (must implement DeserializeOwned)
/// * `K` - Key type for grouping (must implement Hash + Eq)
/// * `FKey` - Key extraction function: `Fn(&T) -> K`
/// * `FScore` - Score extraction function: `Fn(&T) -> f64` (per event)
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::accumulators::TopNByTyped;
///
/// #[derive(Deserialize)]
/// struct OrderEvent {
///     product_id: String,
///     total_value: f64,
/// }
///
/// let top_products = TopNByTyped::new(
///     5,
///     |order: &OrderEvent| order.product_id.clone(),
///     |order: &OrderEvent| order.total_value
/// ).emit_every_n(50);
/// ```
#[derive(Clone)]
pub struct TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone,
{
    n: usize,
    key_fn: FKey,
    score_fn: FScore,
    writer_id: WriterId,
    _phantom: PhantomData<(T, K)>,
}

impl<T, K, FKey, FScore> TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    /// Create a new typed TopNBy accumulator.
    ///
    /// Requires the input type `T` to implement `TypedPayload` for compile-time
    /// event type resolution.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of items to track
    /// * `key_fn` - Function to extract the key from an event (for grouping)
    /// * `score_fn` - Function to extract the score from an event (will be accumulated)
    pub fn new(n: usize, key_fn: FKey, score_fn: FScore) -> Self {
        assert!(n > 0, "TopNBy must track at least 1 item");
        Self {
            n,
            key_fn,
            score_fn,
            writer_id: WriterId::from(StageId::new()),
            _phantom: PhantomData,
        }
    }
}

impl<T, K, FKey, FScore> TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    /// Configure to emit results when EOF is received.
    pub fn emit_on_eof(
        self,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::OnEOF>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::OnEOF::new(),
        )
    }

    /// Configure to emit results every N events.
    pub fn emit_every_n(
        self,
        n: u64,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::EveryN>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::EveryN::new(n),
        )
    }

    /// Configure to emit results within a time window.
    pub fn emit_within(
        self,
        duration: std::time::Duration,
    ) -> super::StatefulWithEmission<Self, crate::stages::stateful::strategies::emissions::TimeWindow>
    {
        super::StatefulWithEmission::new(
            self,
            crate::stages::stateful::strategies::emissions::TimeWindow::new(duration),
        )
    }

    /// Configure with a custom emission strategy.
    pub fn with_emission<E>(self, emission: E) -> super::StatefulWithEmission<Self, E>
    where
        E: crate::stages::stateful::strategies::emissions::EmissionStrategy,
    {
        super::StatefulWithEmission::new(self, emission)
    }
}

impl<T, K, FKey, FScore> Debug for TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNByTyped")
            .field("n", &self.n)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

/// Aggregated item for TopNByTyped (serialized keys to avoid generic lifetime issues)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregatedTypedItem {
    pub key_value: Value, // Serialized key
    pub total_score: f64,
    pub count: u64,
    pub metadata: Value, // Last seen item
}

/// State for TopNByTyped - accumulates scores per key
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopNByTypedState {
    /// All accumulated items by serialized key string
    items: HashMap<String, AggregatedTypedItem>,
    /// Maximum number of items to emit
    capacity: usize,
}

impl<T, K, FKey, FScore> Accumulator for TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    type State = TopNByTypedState;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Step 1: Deserialize ChainEvent → T
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_) => return, // Skip events that don't match the type
        };

        // Step 2: Extract key and score
        let key = (self.key_fn)(&input);
        let score = (self.score_fn)(&input);

        // Step 3: Serialize key to string for HashMap lookup
        let key_value = match serde_json::to_value(&key) {
            Ok(v) => v,
            Err(_) => return,
        };
        let key_str = key_value.to_string();

        // Step 4: Serialize the item for metadata
        let serialized = match serde_json::to_value(&input) {
            Ok(v) => v,
            Err(_) => return,
        };

        // Step 5: Accumulate score for this key
        state
            .items
            .entry(key_str)
            .and_modify(|item| {
                item.total_score += score;
                item.count += 1;
                item.metadata = serialized.clone(); // Update to latest
            })
            .or_insert_with(|| AggregatedTypedItem {
                key_value: key_value.clone(),
                total_score: score,
                count: 1,
                metadata: serialized,
            });
    }

    fn initial_state(&self) -> Self::State {
        TopNByTypedState {
            items: HashMap::new(),
            capacity: self.n,
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Convert to vec and sort by total_score descending
        let mut items: Vec<_> = state.items.values().cloned().collect();
        items.sort_by(|a, b| {
            b.total_score
                .partial_cmp(&a.total_score)
                .unwrap_or(Ordering::Equal)
        });

        // Take top N
        let top_n: Vec<_> = items.into_iter().take(state.capacity).collect();

        // Create result event
        vec![ChainEventFactory::data_event(
            self.writer_id,
            T::EVENT_TYPE,
            json!({
                "top_n": top_n.iter().enumerate().map(|(idx, item)| {
                    json!({
                        "rank": idx + 1,
                        "key": item.key_value,
                        "total_score": item.total_score,
                        "count": item.count,
                        "avg_score": item.total_score / item.count as f64,
                        "metadata": item.metadata,
                    })
                }).collect::<Vec<_>>(),
                "total_items": state.items.len(),
                "capacity": state.capacity,
            }),
        )]
    }

    fn reset(&self, state: &mut Self::State) {
        state.items.clear();
    }
}

#[cfg(test)]
mod tests_typed {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct OrderEvent {
        product_id: String,
        sale_amount: f64,
    }

    impl obzenflow_core::TypedPayload for OrderEvent {
        const EVENT_TYPE: &'static str = "order.event";
    }

    #[test]
    fn test_top_n_by_typed_accumulation() {
        let accumulator = TopNByTyped::new(
            3,
            |order: &OrderEvent| order.product_id.clone(),
            |order: &OrderEvent| order.sale_amount,
        );

        let mut state = accumulator.initial_state();

        // Add multiple orders for same products
        for (product, amount) in [
            ("laptop", 1000.0),
            ("phone", 500.0),
            ("laptop", 1200.0), // Second laptop sale
            ("mouse", 30.0),
            ("laptop", 1100.0), // Third laptop sale
            ("phone", 600.0),   // Second phone sale
        ] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({"product_id": product, "sale_amount": amount}),
            );
            accumulator.accumulate(&mut state, event);
        }

        // Laptop: 3300 (3 orders), Phone: 1100 (2 orders), Mouse: 30 (1 order)
        let results = accumulator.emit(&state);
        let top_n = &results[0].payload()["top_n"];

        assert_eq!(top_n[0]["key"], "laptop");
        assert_eq!(top_n[0]["total_score"], 3300.0);
        assert_eq!(top_n[0]["count"], 3);

        assert_eq!(top_n[1]["key"], "phone");
        assert_eq!(top_n[1]["total_score"], 1100.0);
        assert_eq!(top_n[1]["count"], 2);

        assert_eq!(top_n[2]["key"], "mouse");
        assert_eq!(top_n[2]["total_score"], 30.0);
        assert_eq!(top_n[2]["count"], 1);
    }

    #[test]
    fn test_top_n_by_typed_only_top_n() {
        let accumulator = TopNByTyped::new(
            2, // Only top 2
            |order: &OrderEvent| order.product_id.clone(),
            |order: &OrderEvent| order.sale_amount,
        );

        let mut state = accumulator.initial_state();

        // Add orders for 3 products
        for (product, amount) in [("a", 100.0), ("b", 200.0), ("c", 300.0)] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({"product_id": product, "sale_amount": amount}),
            );
            accumulator.accumulate(&mut state, event);
        }

        // All 3 are in state (we track all, emit top N)
        assert_eq!(state.items.len(), 3);

        let results = accumulator.emit(&state);
        let top_n = &results[0].payload()["top_n"];

        // But only top 2 are emitted
        assert_eq!(top_n.as_array().unwrap().len(), 2);
        assert_eq!(top_n[0]["key"], "c");
        assert_eq!(top_n[0]["total_score"], 300.0);
        assert_eq!(top_n[1]["key"], "b");
        assert_eq!(top_n[1]["total_score"], 200.0);
    }
}
