// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Processing-time tumbling windows for stateful stages.
//!
//! This module provides a first-class, stateful window operator that owns
//! buffering, emission, and drain semantics inside the stateful stage model.
//!
//! Key semantics:
//! - Window state is accumulated in `StatefulHandler::accumulate`.
//! - Window emission happens via `StatefulHandler::emit`, which resets state
//!   for the next window.
//! - Final partial windows are flushed via `StatefulHandler::drain`.
//! - Events that arrive after the window is complete belong to the next window:
//!   we stash the triggering event and apply it after the emission reset.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::StatefulHandler;
use crate::typing::StatefulTyping;
use async_trait::async_trait;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::ReplayContext;
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::event::types::CorrelationId;
use obzenflow_core::{ChainEvent, EventId, StageId, WriterId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn max_lineage_depth() -> usize {
    // Match `ChainEventFactory::derived_event` defaults.
    const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;
    std::env::var("OBZENFLOW_MAX_LINEAGE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_LINEAGE_DEPTH)
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingTimeWindowKind {
    Count,
    Sum,
    Average,
}

/// Data payload emitted by [`ProcessingTimeTumblingWindow`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessingTimeWindowAggregate {
    pub kind: ProcessingTimeWindowKind,
    pub window_duration_ms: u64,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub event_count: u64,
    /// If inputs within the window carry mixed `correlation_id` values, the emitted
    /// aggregate clears `ChainEvent.correlation_id` and instead records the distinct
    /// contributing correlation IDs here (sorted, bounded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_ids: Option<Vec<CorrelationId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_values: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average: Option<f64>,
}

impl TypedPayload for ProcessingTimeWindowAggregate {
    const EVENT_TYPE: &'static str = "obzenflow.window.processing_time_aggregate";
}

#[derive(Debug, Clone)]
enum AggregationSpec {
    Count,
    Sum { field: String },
    Average { field: String },
}

impl AggregationSpec {
    fn kind(&self) -> ProcessingTimeWindowKind {
        match self {
            AggregationSpec::Count => ProcessingTimeWindowKind::Count,
            AggregationSpec::Sum { .. } => ProcessingTimeWindowKind::Sum,
            AggregationSpec::Average { .. } => ProcessingTimeWindowKind::Average,
        }
    }

    fn field(&self) -> Option<String> {
        match self {
            AggregationSpec::Count => None,
            AggregationSpec::Sum { field } | AggregationSpec::Average { field } => {
                Some(field.clone())
            }
        }
    }
}

#[derive(Debug)]
pub struct ProcessingTimeTumblingWindow<TIn> {
    window_duration: Duration,
    aggregation: AggregationSpec,
    _phantom: PhantomData<TIn>,
}

impl<TIn> ProcessingTimeTumblingWindow<TIn> {
    pub fn count(window_duration: Duration) -> Self {
        Self {
            window_duration,
            aggregation: AggregationSpec::Count,
            _phantom: PhantomData,
        }
    }

    pub fn sum(window_duration: Duration, field: impl Into<String>) -> Self {
        Self {
            window_duration,
            aggregation: AggregationSpec::Sum {
                field: field.into(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn average(window_duration: Duration, field: impl Into<String>) -> Self {
        Self {
            window_duration,
            aggregation: AggregationSpec::Average {
                field: field.into(),
            },
            _phantom: PhantomData,
        }
    }
}

impl<TIn> Clone for ProcessingTimeTumblingWindow<TIn> {
    fn clone(&self) -> Self {
        Self {
            window_duration: self.window_duration,
            aggregation: self.aggregation.clone(),
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcessingTimeWindowState {
    window_start: Option<Instant>,
    window_start_ms: Option<u64>,
    event_count: u64,
    observed_values: u64,
    sum: f64,

    // Window fan-in lineage (bounded).
    parent_ids: VecDeque<EventId>,

    // Correlation/replay policy: propagate only when uniform across the window.
    correlation_seen: bool,
    correlation_mixed: bool,
    correlation_ids: BTreeSet<CorrelationId>,
    correlation_id: Option<CorrelationId>,
    correlation_payload: Option<CorrelationPayload>,
    replay_context: Option<ReplayContext>,

    // When a new event arrives after the current window is complete, it belongs to the
    // next window. We stash it and flush the current window first.
    emit_pending: bool,
    pending_event: Option<ChainEvent>,
}

impl ProcessingTimeWindowState {
    fn reset_window(&mut self) {
        self.window_start = None;
        self.window_start_ms = None;
        self.event_count = 0;
        self.observed_values = 0;
        self.sum = 0.0;
        self.parent_ids.clear();
        self.correlation_seen = false;
        self.correlation_mixed = false;
        self.correlation_ids.clear();
        self.correlation_id = None;
        self.correlation_payload = None;
        self.replay_context = None;
        self.emit_pending = false;
        self.pending_event = None;
    }
}

impl Default for ProcessingTimeWindowState {
    fn default() -> Self {
        Self {
            window_start: None,
            window_start_ms: None,
            event_count: 0,
            observed_values: 0,
            sum: 0.0,
            parent_ids: VecDeque::new(),
            correlation_seen: false,
            correlation_mixed: false,
            correlation_ids: BTreeSet::new(),
            correlation_id: None,
            correlation_payload: None,
            replay_context: None,
            emit_pending: false,
            pending_event: None,
        }
    }
}

impl<TIn> StatefulTyping for ProcessingTimeTumblingWindow<TIn> {
    type Input = TIn;
    type Output = ProcessingTimeWindowAggregate;
}

#[async_trait]
impl<TIn> StatefulHandler for ProcessingTimeTumblingWindow<TIn>
where
    TIn: Send + Sync + 'static,
{
    type State = ProcessingTimeWindowState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let now = Instant::now();

        // If the window is complete, flush it before accepting this event into the next window.
        if let Some(start) = state.window_start {
            if state.event_count > 0 && now.duration_since(start) >= self.window_duration {
                state.emit_pending = true;
                state.pending_event = Some(event);
                return;
            }
        }

        self.accept_event(state, event, Some(now));
    }

    fn initial_state(&self) -> Self::State {
        ProcessingTimeWindowState::default()
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        Some(self.window_duration)
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        if state.emit_pending {
            return true;
        }

        let Some(start) = state.window_start else {
            return false;
        };

        if state.event_count == 0 {
            return false;
        }

        start.elapsed() >= self.window_duration
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(self.build_aggregate_events(state))
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        let out = self.build_aggregate_events(state);

        // Start a new window.
        let pending = state.pending_event.take();
        state.reset_window();

        if let Some(event) = pending {
            // Apply the buffered triggering event to the next window.
            // This call will not re-stash because window_start is None.
            self.accept_event(state, event, None);
        }

        Ok(out)
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(self.build_aggregate_events(state))
    }
}

impl<TIn> ProcessingTimeTumblingWindow<TIn>
where
    TIn: Send + Sync + 'static,
{
    fn accept_event(
        &self,
        state: &mut ProcessingTimeWindowState,
        event: ChainEvent,
        now: Option<Instant>,
    ) {
        let now = now.unwrap_or_else(Instant::now);

        if state.window_start.is_none() {
            state.window_start = Some(now);
            state.window_start_ms = Some(now_ms());
        }

        state.event_count = state.event_count.saturating_add(1);

        // Capture bounded lineage (direct parents = buffered inputs).
        let max_depth = max_lineage_depth();
        state.parent_ids.push_back(event.id);
        while state.parent_ids.len() > max_depth {
            state.parent_ids.pop_front();
        }

        // Correlation policy: only propagate when uniform.
        if let Some(correlation_id) = event.correlation_id {
            // Avoid unbounded accumulation when correlation IDs are highly cardinal.
            // This mirrors the bounded lineage policy above.
            if state.correlation_ids.len() < max_depth
                || state.correlation_ids.contains(&correlation_id)
            {
                state.correlation_ids.insert(correlation_id);
            }
        }

        if !state.correlation_seen {
            state.correlation_seen = true;
            state.correlation_id = event.correlation_id;
            state.correlation_payload = event.correlation_payload.clone();
            state.replay_context = event.replay_context.clone();
        } else if !state.correlation_mixed {
            if state.correlation_id != event.correlation_id {
                state.correlation_mixed = true;
                state.correlation_id = None;
                state.correlation_payload = None;
                state.replay_context = None;
            } else {
                if state.correlation_payload != event.correlation_payload {
                    state.correlation_payload = None;
                }
                if state.replay_context != event.replay_context {
                    state.replay_context = None;
                }
            }
        }

        // Numeric field aggregation (Sum/Average).
        let field = match &self.aggregation {
            AggregationSpec::Sum { field } | AggregationSpec::Average { field } => Some(field),
            AggregationSpec::Count => None,
        };

        if let Some(field) = field {
            if let Some(value) = event.payload().get(field).and_then(|v| v.as_f64()) {
                state.observed_values = state.observed_values.saturating_add(1);
                state.sum += value;
            }
        }
    }

    fn build_aggregate_events(&self, state: &ProcessingTimeWindowState) -> Vec<ChainEvent> {
        if state.event_count == 0 {
            return Vec::new();
        }

        let window_start_ms = state.window_start_ms.unwrap_or_else(now_ms);
        let window_end_ms = now_ms();

        let (observed_values, sum, average) = match self.aggregation.kind() {
            ProcessingTimeWindowKind::Count => (None, None, None),
            ProcessingTimeWindowKind::Sum => (Some(state.observed_values), Some(state.sum), None),
            ProcessingTimeWindowKind::Average => {
                let avg = if state.observed_values == 0 {
                    0.0
                } else {
                    state.sum / state.observed_values as f64
                };
                (Some(state.observed_values), Some(state.sum), Some(avg))
            }
        };

        let correlation_ids = if state.correlation_mixed && !state.correlation_ids.is_empty() {
            Some(state.correlation_ids.iter().copied().collect())
        } else {
            None
        };

        let payload = ProcessingTimeWindowAggregate {
            kind: self.aggregation.kind(),
            window_duration_ms: self.window_duration.as_millis() as u64,
            window_start_ms,
            window_end_ms,
            event_count: state.event_count,
            correlation_ids,
            field: self.aggregation.field(),
            observed_values,
            sum,
            average,
        };

        let writer_id = WriterId::from(StageId::new());
        let mut event = payload.to_event(writer_id);

        // Fan-in lineage: direct parents are the buffered inputs (bounded).
        event.causality = CausalityContext {
            parent_ids: state.parent_ids.iter().copied().collect(),
        };

        if !state.correlation_mixed {
            event.correlation_id = state.correlation_id;
            event.correlation_payload = state.correlation_payload.clone();
            event.replay_context = state.replay_context.clone();
        }

        vec![event]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;

    #[tokio::test]
    async fn event_after_complete_window_is_applied_to_next_window() {
        let window_duration = Duration::from_millis(10);
        let mut handler = ProcessingTimeTumblingWindow::<()>::count(window_duration);
        let mut state = handler.initial_state();

        let writer = WriterId::from(StageId::new());

        let e1 = ChainEventFactory::data_event(writer, "test.windowing", json!({ "n": 1 }));
        handler.accumulate(&mut state, e1);
        assert_eq!(state.event_count, 1);

        // Force the window start into the past so the next event arrives after completion.
        state.window_start = state
            .window_start
            .and_then(|start| start.checked_sub(Duration::from_millis(25)));

        let e2 = ChainEventFactory::data_event(writer, "test.windowing", json!({ "n": 2 }));
        handler.accumulate(&mut state, e2);

        assert!(
            state.emit_pending,
            "expected boundary-triggered emission to be pending"
        );
        assert!(
            state.pending_event.is_some(),
            "expected triggering event to be stashed"
        );
        assert_eq!(
            state.event_count, 1,
            "stashed event must not be counted in the prior window"
        );

        let out1 = handler
            .emit(&mut state)
            .expect("emit should succeed for count window");
        assert_eq!(out1.len(), 1);
        let agg1 = ProcessingTimeWindowAggregate::from_event(&out1[0])
            .expect("expected first emission to be a window aggregate");
        assert_eq!(agg1.event_count, 1);

        assert_eq!(
            state.event_count, 1,
            "stashed event must seed the next window after emission reset"
        );

        let out2 = handler
            .drain(&state)
            .await
            .expect("drain should succeed for count window");
        assert_eq!(out2.len(), 1);
        let agg2 = ProcessingTimeWindowAggregate::from_event(&out2[0])
            .expect("expected drain emission to be a window aggregate");
        assert_eq!(agg2.event_count, 1);
    }

    #[tokio::test]
    async fn mixed_correlation_ids_are_recorded_as_sorted_set() {
        let window_duration = Duration::from_millis(100);
        let mut handler = ProcessingTimeTumblingWindow::<()>::count(window_duration);
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        let correlation_a = CorrelationId::new();
        let correlation_b = CorrelationId::new();

        let mut e1 = ChainEventFactory::data_event(writer, "test.windowing", json!({ "n": 1 }));
        e1.correlation_id = Some(correlation_a);
        let mut e2 = ChainEventFactory::data_event(writer, "test.windowing", json!({ "n": 2 }));
        e2.correlation_id = Some(correlation_b);

        handler.accumulate(&mut state, e1);
        handler.accumulate(&mut state, e2);

        let out = handler
            .drain(&state)
            .await
            .expect("drain should succeed for count window");
        assert_eq!(out.len(), 1);

        assert!(
            out[0].correlation_id.is_none(),
            "mixed windows must not propagate a single correlation_id on the aggregate event"
        );

        let agg = ProcessingTimeWindowAggregate::from_event(&out[0])
            .expect("expected mixed drain emission to be a window aggregate");
        let set = agg
            .correlation_ids
            .expect("expected correlation_ids to be recorded for mixed windows");
        assert_eq!(set.len(), 2);
        assert!(set.contains(&correlation_a));
        assert!(set.contains(&correlation_b));
        assert!(
            set.windows(2).all(|w| w[0] <= w[1]),
            "correlation_ids must be sorted"
        );
    }
}
