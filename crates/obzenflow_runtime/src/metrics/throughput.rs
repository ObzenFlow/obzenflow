// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One metrics-supervisor-owned sampler, independent of durable accounting.

use super::observations::{ObservationHub, ObservationOwner};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::observability::{CaptureReason, CaptureScope};
use obzenflow_core::metrics::{StageMetadata, ThroughputMeasurement, ThroughputSnapshot};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::StageId;
use std::collections::HashMap;
use tokio::time::Instant;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum Subject {
    Stage(StageId),
    Input,
    Output,
}

struct Baseline {
    scope: CaptureScope,
    count: u64,
    at: Instant,
}

#[derive(Default)]
pub(crate) struct ThroughputSampler {
    owner: Option<ObservationOwner>,
    previous: HashMap<Subject, Baseline>,
    pub(crate) latest: ThroughputSnapshot,
}

impl ThroughputSampler {
    pub(crate) fn new(owner: Option<ObservationOwner>) -> Self {
        Self {
            owner,
            ..Self::default()
        }
    }

    fn observe(&mut self, subject: Subject, scope: CaptureScope, count: u64, at: Instant) {
        let Some(previous) = self
            .previous
            .get(&subject)
            .filter(|previous| previous.scope == scope && previous.count <= count)
        else {
            self.previous.insert(subject, Baseline { scope, count, at });
            return;
        };
        let delta = count - previous.count;
        let Some(elapsed) = at
            .checked_duration_since(previous.at)
            .filter(|value| !value.is_zero())
        else {
            return;
        };
        let Ok(nanos) = u64::try_from(elapsed.as_nanos()) else {
            return;
        };
        let rate = delta as f64 / elapsed.as_secs_f64();
        if !rate.is_finite() {
            return;
        }
        let Some(packet) = self
            .owner
            .as_ref()
            .and_then(|owner| owner.capture(CaptureReason::Periodic))
        else {
            return;
        };
        self.previous.insert(subject, Baseline { scope, count, at });
        let measurement = ThroughputMeasurement {
            capture: packet.capture,
            event_delta: delta,
            elapsed: MetricsDuration::from_nanos(nanos),
            events_per_second: rate,
        };
        match subject {
            Subject::Stage(stage) => {
                self.latest.stages.insert(stage, measurement);
            }
            Subject::Input => self.latest.flow_input = Some(measurement),
            Subject::Output => self.latest.flow_output = Some(measurement),
        }
    }

    pub(crate) fn sample(
        &mut self,
        hub: &ObservationHub,
        metadata: &HashMap<StageId, StageMetadata>,
        at: Instant,
    ) {
        self.sample_counters(hub.live_counters(), metadata, at);
    }

    fn sample_counters(
        &mut self,
        counters: HashMap<StageId, (CaptureScope, u64)>,
        metadata: &HashMap<StageId, StageMetadata>,
        at: Instant,
    ) {
        if !self
            .owner
            .as_ref()
            .is_some_and(ObservationOwner::measurements_allowed)
        {
            return;
        }
        let Some(scope) = self.owner.as_ref().map(ObservationOwner::scope) else {
            return;
        };
        for (subject, input) in [(Subject::Input, true), (Subject::Output, false)] {
            let required: Vec<_> = metadata
                .iter()
                .filter(|(_, metadata)| {
                    if input {
                        matches!(
                            metadata.stage_type,
                            StageType::FiniteSource | StageType::InfiniteSource
                        )
                    } else {
                        metadata.stage_type == StageType::Sink
                    }
                })
                .map(|(stage, _)| stage)
                .collect();
            if required.is_empty() {
                continue;
            }
            // A reset of one member must not be hidden by another member's
            // growth, even when the aggregate is temporarily unavailable.
            if required.iter().any(|stage| {
                counters
                    .get(stage)
                    .zip(self.previous.get(&Subject::Stage(**stage)))
                    .is_some_and(|((sample_scope, count), previous)| {
                        *sample_scope != previous.scope || *count < previous.count
                    })
            }) {
                self.previous.remove(&subject);
            }
            let total = required.into_iter().try_fold(0u64, |total, stage| {
                let (sample_scope, count) = counters.get(stage)?;
                (*sample_scope == scope).then_some(())?;
                total.checked_add(*count)
            });
            if let Some(total) = total {
                self.observe(subject, scope, total, at);
            }
        }
        for stage in metadata.keys() {
            if let Some((sample_scope, count)) = counters
                .get(stage)
                .filter(|(sample_scope, _)| *sample_scope == scope)
            {
                self.observe(Subject::Stage(*stage), *sample_scope, *count, at);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use crate::metrics::instrumentation::StageInstrumentation;
    use obzenflow_core::{FlowId, ReaderGeneration, SystemId};
    use std::sync::{atomic::Ordering, Arc};
    use std::time::Duration;

    fn sampler(flow: FlowId) -> (ThroughputSampler, CaptureScope) {
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let scope = CaptureScope {
            flow_id: flow,
            resume_generation: ReaderGeneration(0),
        };
        let owner = execution.observations().capture_owner(
            scope,
            SystemId::new().into(),
            execution.clone(),
        );
        (ThroughputSampler::new(Some(owner)), scope)
    }

    fn metadata(stages: &[(StageId, StageType)], flow: FlowId) -> HashMap<StageId, StageMetadata> {
        stages
            .iter()
            .map(|(stage, stage_type)| {
                (
                    *stage,
                    StageMetadata {
                        name: stage.to_string(),
                        stage_type: *stage_type,
                        reference_mode: None,
                        flow_name: "throughput".into(),
                        flow_id: Some(flow),
                    },
                )
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn actual_elapsed_missing_reads_zero_reset_and_scope_keep_whole_bundles() {
        let flow = FlowId::new();
        let stage = StageId::new();
        let (mut sampler, scope) = sampler(flow);
        let metadata = metadata(&[(stage, StageType::Transform)], flow);
        let start = Instant::now();
        sampler.sample_counters(HashMap::from([(stage, (scope, 100))]), &metadata, start);
        assert!(sampler.latest.stages.is_empty());
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 121))]),
            &metadata,
            start + Duration::from_millis(500),
        );
        let measured = sampler.latest.stages[&stage].clone();
        assert_eq!(measured.event_delta, 21);
        assert_eq!(measured.elapsed, MetricsDuration::from_millis(500));
        assert_eq!(measured.events_per_second, 42.0);
        assert!(measured.capture.observer.is_system());
        sampler.sample_counters(HashMap::new(), &metadata, start + Duration::from_secs(10));
        assert_eq!(sampler.latest.stages[&stage], measured);
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 163))]),
            &metadata,
            start + Duration::from_millis(1500),
        );
        assert_eq!(sampler.latest.stages[&stage].events_per_second, 42.0);
        assert_eq!(
            sampler.latest.stages[&stage].elapsed,
            MetricsDuration::from_secs(1)
        );
        // A zero-time read is not a baseline or a fabricated zero.
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 170))]),
            &metadata,
            start + Duration::from_millis(1500),
        );
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 163))]),
            &metadata,
            start + Duration::from_millis(2000),
        );
        let zero = sampler.latest.stages[&stage].clone();
        assert_eq!(zero.event_delta, 0);
        assert_eq!(zero.events_per_second, 0.0);
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 1))]),
            &metadata,
            start + Duration::from_secs(3),
        );
        assert_eq!(sampler.latest.stages[&stage], zero);
        sampler.sample_counters(
            HashMap::from([(stage, (scope, 22))]),
            &metadata,
            start + Duration::from_millis(3500),
        );
        assert_eq!(sampler.latest.stages[&stage].events_per_second, 42.0);
        let before_scope = sampler.latest.stages[&stage].clone();
        let owner = sampler.owner.as_mut().unwrap();
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let new_scope = CaptureScope {
            resume_generation: ReaderGeneration(1),
            ..scope
        };
        *owner = execution.observations().capture_owner(
            new_scope,
            SystemId::new().into(),
            execution.clone(),
        );
        sampler.sample_counters(
            HashMap::from([(stage, (new_scope, 1000))]),
            &metadata,
            start + Duration::from_secs(4),
        );
        assert_eq!(sampler.latest.stages[&stage], before_scope);
        sampler.sample_counters(
            HashMap::from([(stage, (new_scope, 1021))]),
            &metadata,
            start + Duration::from_millis(4500),
        );
        assert_eq!(
            sampler.latest.stages[&stage].capture.capture_scope,
            new_scope
        );
        assert_eq!(sampler.latest.stages[&stage].event_delta, 21);
        let (mut restarted, _) = self::sampler(flow);
        restarted.sample_counters(HashMap::from([(stage, (scope, 1021))]), &metadata, start);
        assert!(restarted.latest.stages.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn aggregates_require_all_members_and_cannot_hide_a_member_reset() {
        let flow = FlowId::new();
        let a = StageId::new();
        let b = StageId::new();
        let sink = StageId::new();
        let transform = StageId::new();
        let (mut sampler, scope) = sampler(flow);
        let metadata = metadata(
            &[
                (a, StageType::FiniteSource),
                (b, StageType::InfiniteSource),
                (sink, StageType::Sink),
                (transform, StageType::Transform),
            ],
            flow,
        );
        let counters = |a_count, b_count, sink_count| {
            HashMap::from([
                (a, (scope, a_count)),
                (b, (scope, b_count)),
                (sink, (scope, sink_count)),
                (transform, (scope, 9000)),
            ])
        };
        let start = Instant::now();
        sampler.sample_counters(counters(10, 20, 30), &metadata, start);
        sampler.sample_counters(
            counters(20, 30, 35),
            &metadata,
            start + Duration::from_secs(1),
        );
        let input = sampler.latest.flow_input.clone().unwrap();
        let retained_b = sampler.latest.stages[&b].clone();
        assert_eq!(input.events_per_second, 20.0);
        assert_eq!(
            sampler
                .latest
                .flow_output
                .as_ref()
                .unwrap()
                .events_per_second,
            5.0
        );
        let mut partial = counters(25, 35, 40);
        partial.remove(&b);
        sampler.sample_counters(partial, &metadata, start + Duration::from_secs(2));
        assert_eq!(sampler.latest.flow_input.as_ref(), Some(&input));
        assert_eq!(sampler.latest.stages[&b], retained_b);
        assert_ne!(sampler.latest.stages[&a].capture, retained_b.capture);
        sampler.sample_counters(
            counters(30, 40, 45),
            &metadata,
            start + Duration::from_secs(3),
        );
        assert_eq!(sampler.latest.flow_input.as_ref().unwrap().event_delta, 20);
        assert_eq!(
            sampler.latest.flow_input.as_ref().unwrap().elapsed,
            MetricsDuration::from_secs(2)
        );
        let before_reset = sampler.latest.flow_input.clone();
        sampler.sample_counters(
            counters(1, 100, 50),
            &metadata,
            start + Duration::from_secs(4),
        );
        assert_eq!(sampler.latest.flow_input, before_reset);
        sampler.sample_counters(
            counters(6, 105, 55),
            &metadata,
            start + Duration::from_secs(5),
        );
        assert_eq!(
            sampler
                .latest
                .flow_input
                .as_ref()
                .unwrap()
                .events_per_second,
            10.0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replay_cannot_seed_live_baseline_and_histogram_contention_cannot_block_counters() {
        let flow = FlowId::new();
        let stage = StageId::new();
        let (mut sampler, _) = sampler(flow);
        let metadata = metadata(&[(stage, StageType::Sink)], flow);
        let start = Instant::now();
        let replay = RuntimeExecution::new(RuntimeMode::Replay, None);
        let replay_metrics = Arc::new(StageInstrumentation::new());
        replay_metrics.bind_observations(flow, stage.into(), &replay);
        replay_metrics
            .events_processed_total
            .store(1_000_000, Ordering::Relaxed);
        sampler.sample(replay.observations(), &metadata, start);
        assert!(sampler.previous.is_empty());
        let live = RuntimeExecution::new(RuntimeMode::Live, None);
        let metrics = Arc::new(StageInstrumentation::new());
        metrics.bind_observations(flow, stage.into(), &live);
        metrics
            .events_processed_total
            .store(1_000_000, Ordering::Relaxed);
        let _histogram = metrics.processing_time_histogram.write().unwrap();
        sampler.sample(
            live.observations(),
            &metadata,
            start + Duration::from_secs(1),
        );
        assert!(sampler.latest.stages.is_empty());
        metrics
            .events_processed_total
            .store(1_000_021, Ordering::Relaxed);
        sampler.sample(
            live.observations(),
            &metadata,
            start + Duration::from_millis(1500),
        );
        assert_eq!(sampler.latest.stages[&stage].events_per_second, 42.0);
    }
}
