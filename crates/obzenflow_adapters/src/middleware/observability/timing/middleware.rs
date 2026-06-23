// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The built-in timing observer: the framework's value-preserving
//! processing-time stamp (FLOWIP-115f, AC 15).
//!
//! `TimingMiddleware` is attached to every stage by default through
//! [`TimingMiddleware::observer_set`]. It brackets stage work with a wall-clock
//! measurement and stamps each output's `processing_info.processing_time`. It is
//! observe-only and emits no diagnostics: user-facing latency *evidence* is the
//! separate `indicator()` observer. User-authored service-level samples live in
//! the sibling `indicator` module.

use crate::middleware::observer::StageObserverSet;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::StageId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Measures stage processing time and stamps output metadata.
#[derive(Debug, Clone, Default)]
pub struct TimingMiddleware {
    starts: Arc<Mutex<HashMap<obzenflow_core::EventId, Instant>>>,
    output_durations: Arc<Mutex<HashMap<obzenflow_core::EventId, MetricsDuration>>>,
}

impl TimingMiddleware {
    /// Create a new timing middleware. The stage name is accepted for call-site
    /// symmetry and is not stored; the stamp is stage-agnostic.
    pub fn new(_stage_name: impl Into<String>) -> Self {
        Self::default()
    }

    /// Build the default per-stage observer set: the processing-time stamp on
    /// every observe-capable surface.
    pub fn observer_set(stage_name: impl Into<String>, stage_id: StageId) -> StageObserverSet {
        let observer = Arc::new(Self::new(stage_name));
        let mut set = StageObserverSet::default();
        set.handler.push(observer.clone());
        set.stateful.push(observer.clone());
        set.join.push(observer.clone());
        set.source_poll.push(observer.clone());
        set.output_commit.push(observer);
        tracing::trace!(?stage_id, "created timing observer set");
        set
    }

    pub(super) fn remember_start(&self, event: &ChainEvent) {
        let mut starts = self
            .starts
            .lock()
            .expect("TimingMiddleware starts lock poisoned");
        starts.insert(event.id, Instant::now());
    }

    pub(super) fn duration_for_input(&self, event: &ChainEvent) -> MetricsDuration {
        let elapsed = self
            .starts
            .lock()
            .expect("TimingMiddleware starts lock poisoned")
            .remove(&event.id)
            .map(|start| start.elapsed())
            .unwrap_or(Duration::ZERO);
        MetricsDuration::from_nanos(elapsed.as_nanos().min(u64::MAX as u128) as u64)
    }

    pub(super) fn stamp_outputs(&self, input: Option<&ChainEvent>, outputs: &mut [ChainEvent]) {
        let duration = input
            .map(|event| self.duration_for_input(event))
            .unwrap_or(MetricsDuration::ZERO);
        self.stamp_outputs_with_duration(duration, outputs);
    }

    pub(super) fn stamp_outputs_with_duration(
        &self,
        duration: MetricsDuration,
        outputs: &mut [ChainEvent],
    ) {
        let mut output_durations = self
            .output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned");
        for output in outputs {
            output.processing_info.processing_time = duration;
            output_durations.insert(output.id, duration);
        }
    }

    pub(super) fn stamp_source_outputs(&self, poll_duration: Duration, outputs: &mut [ChainEvent]) {
        let data_count = outputs
            .iter()
            .filter(|event| event.is_data())
            .count()
            .max(1);
        let nanos = (poll_duration.as_nanos() / data_count as u128).min(u64::MAX as u128) as u64;
        let duration = MetricsDuration::from_nanos(nanos);
        let mut output_durations = self
            .output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned");
        for output in outputs {
            if output.is_data() {
                output.processing_info.processing_time = duration;
                output_durations.insert(output.id, duration);
            }
        }
    }

    pub(super) fn take_output_duration(&self, event: &ChainEvent) -> Option<MetricsDuration> {
        self.output_durations
            .lock()
            .expect("TimingMiddleware output durations lock poisoned")
            .remove(&event.id)
    }
}
