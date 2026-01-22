// FLOWIP-080c: TopN Accumulator
// FLOWIP-080j: TopNTyped - Typed variant (Phase 4)
//
// Maintains the top N items by score, perfect for leaderboards, dashboards,
// and "hottest items" scenarios. This is an exact algorithm (not probabilistic).

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

/// Item in the top-N list with its score
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoredItem {
    pub key: String,
    pub score: f64,
    pub metadata: Value,
}

impl PartialEq for ScoredItem {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.key == other.key
    }
}

impl Eq for ScoredItem {}

impl PartialOrd for ScoredItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by score first, then by key for stability
        match self.score.total_cmp(&other.score) {
            Ordering::Equal => self.key.cmp(&other.key),
            other => other,
        }
    }
}

/// Accumulator that maintains the top N items by score.
///
/// Uses a min-heap to efficiently track the top N items. When the heap
/// exceeds N items, it evicts the item with the lowest score.
///
/// # Type Parameters
///
/// * `F` - Score extraction function type
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::accumulators::TopN;
///
/// // Track top 10 users by score
/// let top_users = TopN::new(10, |event: &ChainEvent| {
///     let payload = event.payload();
///     let user = payload["user_id"].as_str().unwrap().to_string();
///     let score = payload["score"].as_f64().unwrap();
///     Some((user, score, payload.clone()))
/// });
/// ```
pub struct TopN<F> {
    n: usize,
    extractor: F,
    writer_id: WriterId,
}

impl<F> TopN<F> {
    /// Create a new TopN accumulator.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of items to track
    /// * `extractor` - Function to extract (key, score, metadata) from events.
    ///   Returns None to skip the event.
    pub fn new(n: usize, extractor: F) -> Self {
        assert!(n > 0, "TopN must track at least 1 item");
        Self {
            n,
            extractor,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl<F> Debug for TopN<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopN")
            .field("n", &self.n)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<F> Clone for TopN<F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            n: self.n,
            extractor: self.extractor.clone(),
            writer_id: self.writer_id,
        }
    }
}

/// State for TopN accumulator - uses a min-heap with Reverse wrapper
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopNState {
    /// Min-heap of items (using Reverse to make BinaryHeap a min-heap)
    items: BinaryHeap<Reverse<ScoredItem>>,
    /// Maximum number of items to keep
    capacity: usize,
}

impl<F> Accumulator for TopN<F>
where
    F: Fn(&ChainEvent) -> Option<(String, f64, Value)> + Send + Sync + Clone + 'static,
{
    type State = TopNState;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Extract key, score, and metadata from the event
        if let Some((key, score, metadata)) = (self.extractor)(&event) {
            let item = ScoredItem {
                key,
                score,
                metadata,
            };

            // Add to heap
            state.items.push(Reverse(item));

            // If we exceed capacity, remove the lowest score item
            if state.items.len() > state.capacity {
                state.items.pop();
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        TopNState {
            items: BinaryHeap::new(),
            capacity: self.n,
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Extract items from heap and sort by score (descending)
        let mut items: Vec<_> = state
            .items
            .iter()
            .map(|Reverse(item)| item.clone())
            .collect();

        // Sort by score descending (highest first)
        items.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));

        // Create a single event with the top N list
        vec![ChainEventFactory::data_event(
            self.writer_id,
            "top_n_result",
            json!({
                "top_n": items.into_iter().map(|item| {
                    json!({
                        "rank": 0,  // Will be set by receiver
                        "key": item.key,
                        "score": item.score,
                        "metadata": item.metadata,
                    })
                }).collect::<Vec<_>>(),
                "capacity": state.capacity,
                "count": state.items.len(),
            }),
        )]
    }

    fn reset(&self, state: &mut Self::State) {
        state.items.clear();
    }
}

/// Builder extension for TopN
impl<F> TopN<F>
where
    F: Fn(&ChainEvent) -> Option<(String, f64, Value)> + Send + Sync + Clone + 'static,
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
    use serde_json::json;

    #[test]
    fn test_top_n_basic() {
        let accumulator = TopN::new(3, |event: &ChainEvent| {
            let payload = event.payload();
            let key = payload["user"].as_str()?.to_string();
            let score = payload["score"].as_f64()?;
            Some((key, score, payload.clone()))
        });

        let mut state = accumulator.initial_state();

        // Add some events
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "user": "alice", "score": 10.0 }),
        );
        accumulator.accumulate(&mut state, event1);

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "user": "bob", "score": 20.0 }),
        );
        accumulator.accumulate(&mut state, event2);

        let event3 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "user": "charlie", "score": 15.0 }),
        );
        accumulator.accumulate(&mut state, event3);

        let event4 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "user": "david", "score": 5.0 }),
        );
        accumulator.accumulate(&mut state, event4);

        // Should keep top 3: bob (20), charlie (15), alice (10)
        // David (5) should be evicted
        assert_eq!(state.items.len(), 3);

        let results = accumulator.emit(&state);
        assert_eq!(results.len(), 1);

        let top_n = &results[0].payload()["top_n"];
        assert_eq!(top_n[0]["key"], "bob");
        assert_eq!(top_n[0]["score"], 20.0);
        assert_eq!(top_n[1]["key"], "charlie");
        assert_eq!(top_n[1]["score"], 15.0);
        assert_eq!(top_n[2]["key"], "alice");
        assert_eq!(top_n[2]["score"], 10.0);
    }

    #[test]
    fn test_top_n_eviction() {
        let accumulator = TopN::new(2, |event: &ChainEvent| {
            let payload = event.payload();
            let key = payload["id"].as_str()?.to_string();
            let score = payload["value"].as_f64()?;
            Some((key, score, payload.clone()))
        });

        let mut state = accumulator.initial_state();

        // Fill to capacity
        for i in 1..=2 {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({ "id": format!("item{}", i), "value": i as f64 }),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state.items.len(), 2);

        // Add item with higher score - should evict lowest
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "id": "item3", "value": 10.0 }),
        );
        accumulator.accumulate(&mut state, event);

        // Should have kept item2 (2.0) and item3 (10.0), evicted item1 (1.0)
        assert_eq!(state.items.len(), 2);

        let results = accumulator.emit(&state);
        let top_n = &results[0].payload()["top_n"];
        assert_eq!(top_n[0]["key"], "item3");
        assert_eq!(top_n[0]["score"], 10.0);
        assert_eq!(top_n[1]["key"], "item2");
        assert_eq!(top_n[1]["score"], 2.0);
    }
}

// ============================================================================
// FLOWIP-080j: TopNTyped - Type-safe TopN accumulator
// ============================================================================

/// FLOWIP-080j: Type-safe TopN accumulator that works with domain types.
///
/// Maintains top N items by score. When duplicate keys arrive, **replaces** the old item.
/// Perfect for leaderboards where you want the latest score.
///
/// # Type Parameters
///
/// * `T` - Input event type (must implement DeserializeOwned)
/// * `K` - Key type for deduplication (must implement Hash + Eq)
/// * `FKey` - Key extraction function: `Fn(&T) -> K`
/// * `FScore` - Score extraction function: `Fn(&T) -> f64`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::accumulators::TopNTyped;
///
/// #[derive(Deserialize)]
/// struct UserScore {
///     user_id: String,
///     score: f64,
/// }
///
/// let top_users = TopNTyped::new(
///     10,
///     |event: &UserScore| event.user_id.clone(),
///     |event: &UserScore| event.score
/// ).emit_on_eof();
/// ```
#[derive(Clone)]
pub struct TopNTyped<T, K, FKey, FScore>
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

impl<T, K, FKey, FScore> TopNTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    /// Create a new typed TopN accumulator.
    ///
    /// Requires the input type `T` to implement `TypedPayload` for compile-time
    /// event type resolution.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of items to track
    /// * `key_fn` - Function to extract the key from an event (for deduplication)
    /// * `score_fn` - Function to extract the score from an event
    pub fn new(n: usize, key_fn: FKey, score_fn: FScore) -> Self {
        assert!(n > 0, "TopN must track at least 1 item");
        Self {
            n,
            key_fn,
            score_fn,
            writer_id: WriterId::from(StageId::new()),
            _phantom: PhantomData,
        }
    }
}

impl<T, K, FKey, FScore> TopNTyped<T, K, FKey, FScore>
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

impl<T, K, FKey, FScore> Debug for TopNTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNTyped")
            .field("n", &self.n)
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

/// State for TopNTyped - stores items with serialized keys to avoid generic serialization complexity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopNTypedState {
    /// Items stored by serialized key (for deduplication/replacement)
    /// We serialize keys to avoid generic Serialize/Deserialize lifetime issues
    items: HashMap<String, (f64, Value, Value)>, // serialized_key -> (score, key_value, item_metadata)
    /// Maximum number of items to keep
    capacity: usize,
}

impl<T, K, FKey, FScore> Accumulator for TopNTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    type State = TopNTypedState;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        // Step 1: Deserialize ChainEvent → T
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(_) => return, // Skip events that don't match the type
        };

        // Step 2: Extract key and score
        let key = (self.key_fn)(&input);
        let score = (self.score_fn)(&input);

        // Step 3: Serialize key and item for storage
        let key_value = match serde_json::to_value(&key) {
            Ok(v) => v,
            Err(_) => return,
        };
        let key_str = key_value.to_string(); // Use JSON string as HashMap key

        let serialized = match serde_json::to_value(&input) {
            Ok(v) => v,
            Err(_) => return,
        };

        // Step 4: Insert/replace in the map (deduplication)
        state.items.insert(key_str, (score, key_value, serialized));

        // Step 5: If we exceed capacity, remove the item with lowest score
        if state.items.len() > state.capacity {
            // Find the key with the minimum score
            if let Some((min_key, _)) = state
                .items
                .iter()
                .min_by(|(_, (score_a, _, _)), (_, (score_b, _, _))| {
                    score_a.partial_cmp(score_b).unwrap_or(Ordering::Equal)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
            {
                state.items.remove(&min_key);
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        TopNTypedState {
            items: HashMap::new(),
            capacity: self.n,
        }
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Convert to vec and sort by score descending
        let mut items: Vec<_> = state
            .items
            .values()
            .map(|(score, key_value, metadata)| (*score, key_value.clone(), metadata.clone()))
            .collect();

        items.sort_by(|(score_a, _, _), (score_b, _, _)| {
            score_b.partial_cmp(score_a).unwrap_or(Ordering::Equal)
        });

        // Create result event
        vec![ChainEventFactory::data_event(
            self.writer_id,
            T::EVENT_TYPE,
            json!({
                "top_n": items.iter().enumerate().map(|(idx, (score, key_value, metadata))| {
                    json!({
                        "rank": idx + 1,
                        "key": key_value,
                        "score": score,
                        "metadata": metadata,
                    })
                }).collect::<Vec<_>>(),
                "capacity": state.capacity,
                "count": state.items.len(),
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
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct UserScore {
        user_id: String,
        score: f64,
    }

    impl obzenflow_core::TypedPayload for UserScore {
        const EVENT_TYPE: &'static str = "user.score";
    }

    #[test]
    fn test_top_n_typed_basic() {
        let accumulator = TopNTyped::new(
            3,
            |event: &UserScore| event.user_id.clone(),
            |event: &UserScore| event.score,
        );

        let mut state = accumulator.initial_state();

        // Add some events
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "alice", "score": 10.0}),
        );
        accumulator.accumulate(&mut state, event1);

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "bob", "score": 20.0}),
        );
        accumulator.accumulate(&mut state, event2);

        let event3 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "charlie", "score": 15.0}),
        );
        accumulator.accumulate(&mut state, event3);

        let event4 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "david", "score": 5.0}),
        );
        accumulator.accumulate(&mut state, event4);

        // Should keep top 3: bob (20), charlie (15), alice (10)
        assert_eq!(state.items.len(), 3);

        let results = accumulator.emit(&state);
        let top_n = &results[0].payload()["top_n"];
        assert_eq!(top_n[0]["key"], "bob");
        assert_eq!(top_n[0]["score"], 20.0);
        assert_eq!(top_n[1]["key"], "charlie");
        assert_eq!(top_n[1]["score"], 15.0);
        assert_eq!(top_n[2]["key"], "alice");
        assert_eq!(top_n[2]["score"], 10.0);
    }

    #[test]
    fn test_top_n_typed_replacement() {
        let accumulator = TopNTyped::new(
            3,
            |event: &UserScore| event.user_id.clone(),
            |event: &UserScore| event.score,
        );

        let mut state = accumulator.initial_state();

        // Add alice with score 10
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "alice", "score": 10.0}),
        );
        accumulator.accumulate(&mut state, event1);

        // Replace alice with score 25 (should replace)
        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({"user_id": "alice", "score": 25.0}),
        );
        accumulator.accumulate(&mut state, event2);

        assert_eq!(state.items.len(), 1);
        // Find the entry with alice's key and check the score
        let alice_entry = state
            .items
            .values()
            .find(|(_, key_val, _)| key_val.as_str() == Some("alice"))
            .unwrap();
        assert_eq!(alice_entry.0, 25.0);
    }
}
