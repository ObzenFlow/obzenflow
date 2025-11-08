// FLOWIP-080c: Conflate Accumulator
//
// Keeps only the latest value per key, useful for state snapshots and materialized views.
// Previous values are overwritten when a new event with the same key arrives.

use super::Accumulator;
use crate::stages::stateful::emission::{EmissionStrategy, OnEOF, EveryN, TimeWindow, EmitAlways};
use crate::stages::stateful::accumulators::wrapper::StatefulWithEmission;
use obzenflow_core::{ChainEvent, EventId, WriterId};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
use serde_json::json;
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
/// ```rust
/// use obzenflow_runtime_services::stages::stateful::accumulators::Conflate;
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
            ("sensor_1", 21.0, 2000),  // Overwrites first sensor_1
            ("sensor_3", 22.5, 2001),
            ("sensor_2", 19.0, 3000),  // Overwrites first sensor_2
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
            json!({ "value": 100 }),  // No sensor_id field
        );
        accumulator.accumulate(&mut state, event);

        // Should not add anything to state
        assert!(state.is_empty());
    }
}