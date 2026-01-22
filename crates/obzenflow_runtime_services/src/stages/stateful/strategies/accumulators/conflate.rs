// FLOWIP-080c: Conflate Accumulator
//
// Keeps only the latest value per key, useful for state snapshots and materialized views.
// Previous values are overwritten when a new event with the same key arrives.

use super::Accumulator;
use crate::stages::stateful::strategies::accumulators::wrapper::StatefulWithEmission;
use crate::stages::stateful::strategies::emissions::{
    EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

/// Keeps only the latest value per key (useful for state snapshots).
///
/// This accumulator overwrites previous values when a new event with the
/// same key arrives, maintaining a current snapshot of the latest state.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::accumulators::Conflate;
///
/// // Keep latest sensor reading per sensor
/// let snapshot = Conflate::new("sensor_id");
///
/// // Keep latest user status
/// let user_status = Conflate::new("user_id");
/// ```
#[derive(Debug, Clone)]
pub struct Conflate {
    key_field: String,
    writer_id: WriterId,
}

impl Conflate {
    /// Create a new Conflate accumulator.
    ///
    /// # Arguments
    ///
    /// * `key_field` - The field name to extract the key from events
    pub fn new<K: Into<String>>(key_field: K) -> Self {
        Self {
            key_field: key_field.into(),
            writer_id: WriterId::from(StageId::new()),
        }
    }

    /// Set a custom writer ID for emitted events.
    pub fn with_writer_id(mut self, writer_id: WriterId) -> Self {
        self.writer_id = writer_id;
        self
    }
}

impl Accumulator for Conflate {
    type State = HashMap<String, ChainEvent>;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        if let Some(key_value) = event.payload().get(&self.key_field) {
            if let Some(key) = key_value.as_str() {
                // Keep latest event per key (overwrites previous)
                state.insert(key.to_string(), event);
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Emit all latest values
        state.values().cloned().collect()
    }

    fn reset(&self, state: &mut Self::State) {
        state.clear();
    }
}

/// Builder pattern methods for combining with emission strategies
impl Conflate {
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
    ///
    /// Useful for periodic snapshots of current state.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    ///
    /// Creates a real-time materialized view where every change
    /// immediately produces an updated snapshot.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use serde_json::json;

    #[test]
    fn test_conflate_empty_state() {
        let accumulator = Conflate::new("sensor_id");
        let state = accumulator.initial_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_conflate_keeps_latest() {
        let accumulator = Conflate::new("sensor_id");
        let mut state = accumulator.initial_state();

        // First reading for sensor_1
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            json!({
                "sensor_id": "sensor_1",
                "temperature": 20.5,
                "timestamp": 1000
            }),
        );
        accumulator.accumulate(&mut state, event1.clone());

        assert_eq!(state.len(), 1);
        assert_eq!(state["sensor_1"].payload()["temperature"], 20.5);

        // Second reading for sensor_1 (should overwrite)
        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            json!({
                "sensor_id": "sensor_1",
                "temperature": 21.0,
                "timestamp": 2000
            }),
        );
        accumulator.accumulate(&mut state, event2.clone());

        // Should still have 1 entry, but with updated value
        assert_eq!(state.len(), 1);
        assert_eq!(state["sensor_1"].payload()["temperature"], 21.0);
        assert_eq!(state["sensor_1"].payload()["timestamp"], 2000);
    }

    #[test]
    fn test_conflate_multiple_keys() {
        let accumulator = Conflate::new("sensor_id");
        let mut state = accumulator.initial_state();

        let events = vec![
            ("sensor_1", 20.5, 1000),
            ("sensor_2", 18.0, 1001),
            ("sensor_1", 21.0, 2000), // Overwrites first sensor_1
            ("sensor_3", 22.5, 2001),
            ("sensor_2", 19.0, 3000), // Overwrites first sensor_2
        ];

        for (sensor, temp, timestamp) in events {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "reading",
                json!({
                    "sensor_id": sensor,
                    "temperature": temp,
                    "timestamp": timestamp
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        // Should have 3 sensors with their latest values
        assert_eq!(state.len(), 3);
        assert_eq!(state["sensor_1"].payload()["temperature"], 21.0);
        assert_eq!(state["sensor_1"].payload()["timestamp"], 2000);
        assert_eq!(state["sensor_2"].payload()["temperature"], 19.0);
        assert_eq!(state["sensor_2"].payload()["timestamp"], 3000);
        assert_eq!(state["sensor_3"].payload()["temperature"], 22.5);
        assert_eq!(state["sensor_3"].payload()["timestamp"], 2001);
    }

    #[test]
    fn test_conflate_emit_returns_all_latest() {
        let accumulator = Conflate::new("sensor_id");
        let mut state = accumulator.initial_state();

        // Add multiple sensors
        for (sensor, value) in &[("sensor_1", 10), ("sensor_2", 20), ("sensor_3", 30)] {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "reading",
                json!({
                    "sensor_id": sensor,
                    "value": value
                }),
            );
            accumulator.accumulate(&mut state, event);
        }

        let emitted = accumulator.emit(&state);

        // Should emit all 3 latest events
        assert_eq!(emitted.len(), 3);

        // Verify all values are present
        let values: Vec<i64> = emitted
            .iter()
            .map(|e| e.payload()["value"].as_i64().unwrap())
            .collect();
        assert!(values.contains(&10));
        assert!(values.contains(&20));
        assert!(values.contains(&30));
    }

    #[test]
    fn test_conflate_reset() {
        let accumulator = Conflate::new("sensor_id");
        let mut state = accumulator.initial_state();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            json!({ "sensor_id": "sensor_1", "value": 100 }),
        );
        accumulator.accumulate(&mut state, event);
        assert_eq!(state.len(), 1);

        accumulator.reset(&mut state);
        assert!(state.is_empty());
    }

    #[test]
    fn test_conflate_missing_key_field() {
        let accumulator = Conflate::new("sensor_id");
        let mut state = accumulator.initial_state();

        // Event without the key field
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            json!({ "value": 100 }), // No sensor_id field
        );
        accumulator.accumulate(&mut state, event);

        // Should not add anything to state
        assert!(state.is_empty());
    }
}

// ============================================================================
// FLOWIP-080j: Typed Conflate - Type-safe latest-value tracking with domain types
// ============================================================================

use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;
use std::marker::PhantomData;

/// Typed Conflate for type-safe latest-value tracking with automatic serde
///
/// This version of Conflate works directly with domain types instead of ChainEvent,
/// eliminating manual serialization/deserialization boilerplate. Keeps only the
/// latest value per key, useful for state snapshots and materialized views.
///
/// # Type Parameters
///
/// * `T` - Input event type (must implement `DeserializeOwned + Serialize`)
/// * `K` - Key type for grouping (must implement `Hash + Eq + Clone`)
/// * `FKey` - Key extraction function: `Fn(&T) -> K`
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::accumulators::Conflate;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Deserialize, Serialize)]
/// struct SensorReading {
///     sensor_id: String,
///     temperature: f64,
///     timestamp: u64,
/// }
///
/// let snapshot = Conflate::typed(
///     |reading: &SensorReading| reading.sensor_id.clone()
/// ).emit_always();
/// ```
#[derive(Clone)]
pub struct ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    key_fn: FKey,
    writer_id: WriterId,
    _phantom: PhantomData<(T, K)>,
}

impl<T, K, FKey> Debug for ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflateTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("key_type", &std::any::type_name::<K>())
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<T, K, FKey> ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    /// Create a new typed Conflate accumulator.
    ///
    /// Requires the input type `T` to implement `TypedPayload` for compile-time
    /// event type resolution.
    ///
    /// # Arguments
    ///
    /// * `key_fn` - Function to extract the key from input events
    pub fn new(key_fn: FKey) -> Self {
        Self {
            key_fn,
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

impl<T, K, FKey> Accumulator for ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
{
    type State = HashMap<K, T>;

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

        // Step 3: Store latest value (overwrites previous)
        state.insert(key, input);
    }

    fn initial_state(&self) -> Self::State {
        HashMap::new()
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Emit all latest values as typed events
        state
            .values()
            .map(|value| {
                let payload = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
                ChainEventFactory::data_event(self.writer_id, T::EVENT_TYPE, payload)
            })
            .collect()
    }

    fn reset(&self, state: &mut Self::State) {
        state.clear();
    }
}

/// Builder pattern methods for combining with emission strategies
impl<T, K, FKey> ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
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
    ///
    /// Useful for periodic snapshots of current state.
    pub fn emit_within(self, duration: Duration) -> StatefulWithEmission<Self, TimeWindow> {
        self.with_emission(TimeWindow::new(duration))
    }

    /// Emit after every event.
    ///
    /// Creates a real-time materialized view where every change
    /// immediately produces an updated snapshot.
    pub fn emit_always(self) -> StatefulWithEmission<Self, EmitAlways> {
        self.with_emission(EmitAlways)
    }
}

/// Convenience constructor on original Conflate for creating typed variants
impl Conflate {
    /// Create a typed Conflate that works with domain types
    ///
    /// Requires the input type `T` to implement `TypedPayload`.
    ///
    /// # Arguments
    ///
    /// * `key_fn` - Function to extract the key: `Fn(&T) -> K`
    ///
    /// # Example
    ///
    /// ```ignore
    /// use obzenflow_runtime_services::stages::stateful::strategies::accumulators::Conflate;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize, Clone)]
    /// struct SensorReading { sensor_id: String, value: f64 }
    ///
    /// let snapshot = Conflate::typed(
    ///     |reading: &SensorReading| reading.sensor_id.clone()
    /// );
    /// ```
    pub fn typed<T, K, FKey>(key_fn: FKey) -> ConflateTyped<T, K, FKey>
    where
        T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload,
        K: Hash + Eq + Clone + Debug + Send + Sync,
        FKey: Fn(&T) -> K + Send + Sync + Clone,
    {
        ConflateTyped::new(key_fn)
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
    struct SensorReading {
        sensor_id: String,
        temperature: f64,
        timestamp: u64,
    }

    impl obzenflow_core::TypedPayload for SensorReading {
        const EVENT_TYPE: &'static str = "sensor.reading";
    }

    #[test]
    fn test_conflate_typed_empty_state() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let state = accumulator.initial_state();
        assert!(state.is_empty());
    }

    #[test]
    fn test_conflate_typed_keeps_latest() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let mut state = accumulator.initial_state();

        // First reading for sensor_1
        let reading1 = SensorReading {
            sensor_id: "sensor_1".to_string(),
            temperature: 20.5,
            timestamp: 1000,
        };
        let event1 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            serde_json::to_value(&reading1).unwrap(),
        );
        accumulator.accumulate(&mut state, event1);

        assert_eq!(state.len(), 1);
        assert_eq!(state["sensor_1"].temperature, 20.5);

        // Second reading for sensor_1 (should overwrite)
        let reading2 = SensorReading {
            sensor_id: "sensor_1".to_string(),
            temperature: 21.0,
            timestamp: 2000,
        };
        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            serde_json::to_value(&reading2).unwrap(),
        );
        accumulator.accumulate(&mut state, event2);

        // Should still have 1 entry, but with updated value
        assert_eq!(state.len(), 1);
        assert_eq!(state["sensor_1"].temperature, 21.0);
        assert_eq!(state["sensor_1"].timestamp, 2000);
    }

    #[test]
    fn test_conflate_typed_multiple_keys() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let mut state = accumulator.initial_state();

        let readings = vec![
            ("sensor_1", 20.5, 1000),
            ("sensor_2", 18.0, 1001),
            ("sensor_1", 21.0, 2000), // Overwrites first sensor_1
            ("sensor_3", 22.5, 2001),
            ("sensor_2", 19.0, 3000), // Overwrites first sensor_2
        ];

        for (sensor_id, temp, timestamp) in readings {
            let reading = SensorReading {
                sensor_id: sensor_id.to_string(),
                temperature: temp,
                timestamp,
            };
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "reading",
                serde_json::to_value(&reading).unwrap(),
            );
            accumulator.accumulate(&mut state, event);
        }

        // Should have 3 sensors with their latest values
        assert_eq!(state.len(), 3);
        assert_eq!(state["sensor_1"].temperature, 21.0);
        assert_eq!(state["sensor_1"].timestamp, 2000);
        assert_eq!(state["sensor_2"].temperature, 19.0);
        assert_eq!(state["sensor_2"].timestamp, 3000);
        assert_eq!(state["sensor_3"].temperature, 22.5);
        assert_eq!(state["sensor_3"].timestamp, 2001);
    }

    #[test]
    fn test_conflate_typed_emit_returns_all_latest() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let mut state = accumulator.initial_state();

        // Add multiple sensors
        for (sensor_id, value) in &[("sensor_1", 10.0), ("sensor_2", 20.0), ("sensor_3", 30.0)] {
            let reading = SensorReading {
                sensor_id: sensor_id.to_string(),
                temperature: *value,
                timestamp: 1000,
            };
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "reading",
                serde_json::to_value(&reading).unwrap(),
            );
            accumulator.accumulate(&mut state, event);
        }

        let emitted = accumulator.emit(&state);

        // Should emit all 3 latest events
        assert_eq!(emitted.len(), 3);

        // Verify all values are present
        let temperatures: Vec<f64> = emitted
            .iter()
            .map(|e| e.payload()["temperature"].as_f64().unwrap())
            .collect();
        assert!(temperatures.contains(&10.0));
        assert!(temperatures.contains(&20.0));
        assert!(temperatures.contains(&30.0));
    }

    #[test]
    fn test_conflate_typed_reset() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let mut state = accumulator.initial_state();

        let reading = SensorReading {
            sensor_id: "sensor_1".to_string(),
            temperature: 25.0,
            timestamp: 1000,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            serde_json::to_value(&reading).unwrap(),
        );
        accumulator.accumulate(&mut state, event);
        assert_eq!(state.len(), 1);

        accumulator.reset(&mut state);
        assert!(state.is_empty());
    }

    #[test]
    fn test_conflate_typed_skips_invalid_events() {
        let accumulator = ConflateTyped::new(|reading: &SensorReading| reading.sensor_id.clone());
        let mut state = accumulator.initial_state();

        // Valid event
        let valid_reading = SensorReading {
            sensor_id: "sensor_1".to_string(),
            temperature: 25.0,
            timestamp: 1000,
        };
        let valid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            serde_json::to_value(&valid_reading).unwrap(),
        );

        // Invalid event - wrong structure
        let invalid_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "reading",
            json!({ "invalid_field": "oops" }),
        );

        accumulator.accumulate(&mut state, valid_event);
        accumulator.accumulate(&mut state, invalid_event);

        // Only the valid event should have been processed
        assert_eq!(state.len(), 1);
        assert!(state.contains_key("sensor_1"));
    }

    #[test]
    fn test_conflate_typed_numeric_keys() {
        #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
        struct UserStatus {
            user_id: u64,
            status: String,
            last_seen: u64,
        }

        impl obzenflow_core::TypedPayload for UserStatus {
            const EVENT_TYPE: &'static str = "user.status";
        }

        let accumulator = ConflateTyped::new(|status: &UserStatus| status.user_id);
        let mut state = accumulator.initial_state();

        let updates = vec![
            (1, "online", 1000),
            (2, "offline", 1001),
            (1, "away", 2000), // Overwrites user 1
            (3, "online", 2001),
            (2, "online", 3000), // Overwrites user 2
        ];

        for (user_id, status, last_seen) in updates {
            let user_status = UserStatus {
                user_id,
                status: status.to_string(),
                last_seen,
            };
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "user_status",
                serde_json::to_value(&user_status).unwrap(),
            );
            accumulator.accumulate(&mut state, event);
        }

        // Should have 3 users with their latest status
        assert_eq!(state.len(), 3);
        assert_eq!(state[&1].status, "away");
        assert_eq!(state[&1].last_seen, 2000);
        assert_eq!(state[&2].status, "online");
        assert_eq!(state[&2].last_seen, 3000);
        assert_eq!(state[&3].status, "online");
        assert_eq!(state[&3].last_seen, 2001);
    }
}
