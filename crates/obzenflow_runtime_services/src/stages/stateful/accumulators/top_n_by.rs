// FLOWIP-080c: TopNBy Accumulator (with aggregation)
//
// Maintains the top N items by accumulated score, perfect for analytics
// like "top products by sales", "top users by activity", etc.

use super::Accumulator;
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::id::StageId;
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::{Ordering, Reverse};
use std::fmt::Debug;

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
        // Compare by total_score first, then by key for stability
        match self.total_score.partial_cmp(&other.total_score) {
            Some(Ordering::Equal) => Some(self.key.cmp(&other.key)),
            other => other,
        }
    }
}

impl Ord for AggregatedItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Accumulator that maintains top N items by accumulated score.
///
/// Unlike TopN which replaces on duplicate keys, TopNBy accumulates
/// scores for the same key, perfect for analytics scenarios.
///
/// # Examples
///
/// ```rust
/// use obzenflow_runtime_services::stages::stateful::accumulators::TopNBy;
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
        self.extract_value(&event.payload())
            .and_then(|v| {
                if self.as_string {
                    v.as_str().map(|s| s.to_string())
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
            writer_id: self.writer_id.clone(),
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
            state.items
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
        emit_top_n(state, self.writer_id.clone())
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
            state.items
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
        emit_top_n(state, self.writer_id.clone())
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
    let mut top_items: Vec<_> = heap.into_iter()
        .map(|Reverse(item)| item)
        .collect();
    top_items.sort_by(|a, b| b.total_score.partial_cmp(&a.total_score).unwrap_or(Ordering::Equal));

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
    pub fn emit_on_eof(self) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::OnEOF> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::OnEOF::new())
    }

    /// Configure to emit results every N events.
    pub fn emit_every_n(self, n: u64) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::EveryN> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::EveryN::new(n))
    }

    /// Configure to emit results within a time window.
    pub fn emit_within(self, duration: std::time::Duration) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::TimeWindow> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::TimeWindow::new(duration))
    }

    /// Configure with a custom emission strategy.
    pub fn with_emission<E>(self, emission: E) -> super::StatefulWithEmission<Self, E>
    where
        E: crate::stages::stateful::emission::EmissionStrategy,
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
    pub fn emit_on_eof(self) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::OnEOF> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::OnEOF::new())
    }

    /// Configure to emit results every N events.
    pub fn emit_every_n(self, n: u64) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::EveryN> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::EveryN::new(n))
    }

    /// Configure to emit results within a time window.
    pub fn emit_within(self, duration: std::time::Duration) -> super::StatefulWithEmission<Self, crate::stages::stateful::emission::TimeWindow> {
        super::StatefulWithEmission::new(self, crate::stages::stateful::emission::TimeWindow::new(duration))
    }

    /// Configure with a custom emission strategy.
    pub fn with_emission<E>(self, emission: E) -> super::StatefulWithEmission<Self, E>
    where
        E: crate::stages::stateful::emission::EmissionStrategy,
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
            ("Widget", 200.0),  // Another Widget sale
            ("Doohickey", 75.0),
            ("Gadget", 100.0),  // Another Gadget sale
            ("Widget", 50.0),   // Third Widget sale
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
            ("alice", 70.0),  // Alice avg: 75
            ("bob", 100.0),   // Bob avg: 95
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