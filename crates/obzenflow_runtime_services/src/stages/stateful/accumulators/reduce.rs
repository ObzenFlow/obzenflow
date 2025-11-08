// FLOWIP-080c: Reduce Accumulator
//
// Simple reduce/fold accumulator that aggregates all events into a single state value.
// No grouping - just a single accumulation across all events.

use super::Accumulator;
use crate::stages::stateful::emission::{EmissionStrategy, OnEOF, EveryN, TimeWindow, EmitAlways};
use crate::stages::stateful::accumulators::wrapper::StatefulWithEmission;
use obzenflow_core::{ChainEvent, EventId, WriterId};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::id::StageId;
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
/// ```rust
/// use obzenflow_runtime_services::stages::stateful::accumulators::Reduce;
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
    type State = S;

    fn accumulate(&self, state: &mut Self::State, event: ChainEvent) {
        (self.reduce_fn)(state, &event);
    }

    fn initial_state(&self) -> Self::State {
        self.initial.clone()
    }

    fn emit(&self, state: &Self::State) -> Vec<ChainEvent> {
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "reduced",
            json!({
                "result": state,
            }),
        )]
    }

    fn reset(&self, state: &mut Self::State) {
        *state = self.initial.clone();
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
        assert_eq!(accumulator.initial_state(), 42);
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
        assert_eq!(state, 10);

        let event2 = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test",
            json!({ "value": 20 }),
        );
        accumulator.accumulate(&mut state, event2);
        assert_eq!(state, 30);
    }

    #[test]
    fn test_reduce_counter() {
        let accumulator = Reduce::new(0u64, |count: &mut u64, _event: &ChainEvent| {
            *count += 1;
        });

        let mut state = accumulator.initial_state();

        for _ in 0..5 {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test",
                json!({}),
            );
            accumulator.accumulate(&mut state, event);
        }

        assert_eq!(state, 5);
    }

    #[test]
    fn test_reduce_emit_format() {
        let accumulator = Reduce::new(100i64, |state: &mut i64, _event: &ChainEvent| {
            *state += 1;
        });

        let state = 105i64;
        let emitted = accumulator.emit(&state);

        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].event_type(), "reduced");
        assert_eq!(emitted[0].payload()["result"], 105);
    }

    #[test]
    fn test_reduce_reset() {
        let accumulator = Reduce::new(0i64, |sum: &mut i64, event: &ChainEvent| {
            *sum += event.payload()["value"].as_i64().unwrap_or(0);
        });

        let mut state = 100i64;
        accumulator.reset(&mut state);
        assert_eq!(state, 0);
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

        assert_eq!(state.count, 4);
        assert_eq!(state.sum, 50.0);
        assert_eq!(state.min, 5.0);
        assert_eq!(state.max, 20.0);
    }
}