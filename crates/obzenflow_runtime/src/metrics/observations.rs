// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded optional capture handoff and retained measurement view. This module
//! has no journal, publication scope, credit, or settlement capability.

use crate::execution::RuntimeExecution;
use obzenflow_core::event::context::RuntimeObservability;
use obzenflow_core::event::observation::*;
use obzenflow_core::{FlowId, MiddlewareExecutionScope, StageId, WriterId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PACKET_FAMILIES: usize = 128;
const MAX_KEYS: usize = 4096;
const MAX_OWNERS: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Kind {
    RuntimeSnapshot,
    InFlight,
    JoinReference,
    StateAge,
    EventLoops,
    WorkLoops,
    Timing,
    CircuitBreaker,
    RateLimiter,
    EffectCircuitBreaker(String),
    EffectRateLimiter(String),
    ProcessingTime,
    Metrics,
    Sli,
    Llm,
    BreakerSummary(Option<String>),
    LimiterActivity(Option<String>),
    LimiterUtilisation(Option<String>),
    Backpressure,
    Resource,
    HttpPull,
    AiChunking,
    HttpSurface,
    StageHeartbeat,
    EdgeLiveness(StageId, StageId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    observer: WriterId,
    kind: Kind,
}

#[derive(Debug, Clone, Default)]
struct View {
    active_scope: Option<CaptureScope>,
    latest: HashMap<Key, ObservabilityContext>,
}

/// One run-owned mailbox. A key retains only its latest coherent bundle, so a
/// stalled consumer cannot cause unbounded pending updates.
#[derive(Debug, Default)]
pub struct ObservationHub {
    view: Mutex<View>,
    owners: Mutex<HashMap<(CaptureScope, WriterId), Arc<AtomicU64>>>,
    dropped: AtomicU64,
    stages: Mutex<HashMap<WriterId, Weak<super::instrumentation::StageInstrumentation>>>,
}

impl ObservationHub {
    pub fn activate_scope(&self, scope: CaptureScope) {
        if let Ok(mut view) = self.view.lock() {
            view.active_scope = Some(scope);
        }
    }

    pub(crate) fn register_stage(
        &self,
        writer: WriterId,
        stage: &Arc<super::instrumentation::StageInstrumentation>,
    ) {
        if let Ok(mut stages) = self.stages.lock() {
            if stages.len() < MAX_OWNERS || stages.contains_key(&writer) {
                stages.insert(writer, Arc::downgrade(stage));
            }
        }
    }

    /// Use the existing metrics cadence. Sampling cannot hold the view lock or
    /// delay a producer, and a missing/finished owner needs no flush barrier.
    pub fn capture_registered(&self, reason: CaptureReason) {
        let stages: Vec<_> = self
            .stages
            .try_lock()
            .map(|stages| stages.values().cloned().collect())
            .unwrap_or_default();
        for stage in stages.into_iter().filter_map(|stage| stage.upgrade()) {
            stage.offer_capture(reason);
        }
    }

    pub(crate) fn capture_owner(
        self: &Arc<Self>,
        scope: CaptureScope,
        observer: WriterId,
        execution: RuntimeExecution,
    ) -> ObservationOwner {
        let sequence = self.owners.lock().ok().and_then(|mut owners| {
            if !owners.contains_key(&(scope, observer)) && owners.len() >= MAX_OWNERS {
                return None;
            }
            Some(owners.entry((scope, observer)).or_default().clone())
        });
        ObservationOwner {
            scope,
            observer,
            sequence,
            hub: self.clone(),
            execution,
        }
    }

    fn drop_sample(&self) -> ObservationOffer {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        ObservationOffer::Dropped
    }
}

impl ObservationHub {
    /// Restore only evidence already admitted through a consumer's journal cut.
    /// Recorded attachments never activate an execution generation.
    pub(crate) fn offer_recorded(&self, packet: ObservabilityContext) {
        let _ = self.select_recorded(packet);
    }

    /// An independent projection view; capture ownership is never copied.
    pub fn retained_copy(&self) -> Self {
        let view = self
            .view
            .try_lock()
            .map(|view| view.clone())
            .unwrap_or_default();
        Self {
            view: Mutex::new(view),
            ..Default::default()
        }
    }

    /// Select the changed families for an existing backend consumer. All
    /// callers use the same scope and per-subject ordering rule.
    pub fn select(
        &self,
        observation: ObservabilityContext,
    ) -> Result<Vec<ObservabilityContext>, ObservationOffer> {
        self.select_inner(observation, false)
    }

    pub fn select_recorded(
        &self,
        observation: ObservabilityContext,
    ) -> Result<Vec<ObservabilityContext>, ObservationOffer> {
        self.select_inner(observation, true)
    }

    fn select_inner(
        &self,
        observation: ObservabilityContext,
        recorded: bool,
    ) -> Result<Vec<ObservabilityContext>, ObservationOffer> {
        let Some(observation) = observation.validated() else {
            return Err(self.drop_sample());
        };
        let Some(families) = split(observation) else {
            return Err(self.drop_sample());
        };
        let Ok(mut view) = self.view.try_lock() else {
            return Err(self.drop_sample());
        };
        let mut selected = Vec::new();
        let mut capacity_dropped = false;
        for (kind, packet) in families {
            let scope = packet.capture.capture_scope;
            if let Some(active) = view.active_scope {
                if (!recorded && scope != active)
                    || (scope.flow_id == active.flow_id
                        && scope.resume_generation > active.resume_generation)
                {
                    continue;
                }
            }
            let key = Key {
                observer: packet.capture.observer,
                kind,
            };
            if let Some(previous) = view.latest.get(&key) {
                if previous.capture.capture_scope == packet.capture.capture_scope {
                    if previous.capture.capture_seq >= packet.capture.capture_seq {
                        continue;
                    }
                } else if view.active_scope != Some(scope) {
                    let previous_scope = previous.capture.capture_scope;
                    if !recorded
                        || previous_scope.flow_id != scope.flow_id
                        || previous_scope.resume_generation >= scope.resume_generation
                    {
                        continue;
                    }
                }
            } else if view.latest.len() >= MAX_KEYS {
                self.drop_sample();
                capacity_dropped = true;
                continue;
            }
            selected.push(packet.clone());
            view.latest.insert(key, packet);
        }
        if selected.is_empty() && capacity_dropped {
            Err(ObservationOffer::Dropped)
        } else {
            Ok(selected)
        }
    }
}

impl ObservationSink for ObservationHub {
    fn offer(&self, observation: ObservabilityContext) -> ObservationOffer {
        match self.select(observation) {
            Ok(_) => ObservationOffer::Accepted,
            Err(dropped) => dropped,
        }
    }
}

impl ObservationSource for ObservationHub {
    fn active_scope(&self) -> Option<CaptureScope> {
        match self.view.try_lock() {
            Ok(view) => view.active_scope,
            Err(_) => None,
        }
    }
    fn snapshot(&self) -> Vec<ObservabilityContext> {
        let mut packets: Vec<_> = match self.view.try_lock() {
            Ok(view) => view.latest.values().cloned().collect(),
            Err(_) => return Vec::new(),
        };
        // Families are independently retained; overlapping projected fields
        // apply in capture order, never HashMap iteration order.
        packets.sort_by_key(|packet| {
            (
                packet.capture.capture_scope.resume_generation,
                packet.capture.capture_seq,
            )
        });
        packets
    }
}

/// Sequence ownership survives capture-helper recreation and spans the owner's
/// data, error, and system records. Allocating it never allocates an EventId.
#[derive(Debug, Clone)]
pub struct ObservationOwner {
    scope: CaptureScope,
    observer: WriterId,
    sequence: Option<Arc<AtomicU64>>,
    hub: Arc<ObservationHub>,
    execution: RuntimeExecution,
}

impl ObservationOwner {
    pub fn measurements_allowed(&self) -> bool {
        match self.observer.as_stage() {
            Some(stage) => !self.execution.stage_scope(*stage).is_deterministic_replay(),
            None => self.execution.host_observations_allowed(),
        }
    }

    pub fn capture(&self, reason: CaptureReason) -> Option<ObservabilityContext> {
        if !self.measurements_allowed() {
            return None;
        }
        self.allocate_capture(reason)
    }

    pub(crate) fn capture_in_scope(
        &self,
        reason: CaptureReason,
        scope: MiddlewareExecutionScope,
    ) -> Option<ObservabilityContext> {
        if scope.is_deterministic_replay() || !self.execution.host_observations_allowed() {
            return None;
        }
        self.allocate_capture(reason)
    }

    fn allocate_capture(&self, reason: CaptureReason) -> Option<ObservabilityContext> {
        let sequence = self
            .sequence
            .as_ref()?
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .ok()?
            + 1;
        Some(ObservabilityContext::new(CaptureStamp {
            capture_scope: self.scope,
            observer: self.observer,
            capture_seq: CaptureSeq(sequence),
            capture_reason: reason,
            observed_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_millis() as u64,
        }))
    }

    pub fn offer(&self, packet: ObservabilityContext) {
        self.hub.offer(packet);
    }
}

impl ObservationRecorder for ObservationOwner {
    fn observe(&self, record: ObservationRecord) {
        self.observe_with_reason(record, CaptureReason::Record);
    }
    fn observe_with_reason(&self, record: ObservationRecord, reason: CaptureReason) {
        if let Some(mut packet) = self.capture(reason) {
            packet.records.push(record);
            self.offer(packet);
        }
    }
}

fn split(packet: ObservabilityContext) -> Option<Vec<(Kind, ObservabilityContext)>> {
    let mut families = Vec::new();
    let stamp = packet.capture;
    if let Some(snapshot) = packet.runtime_snapshot {
        let mut part = ObservabilityContext::new(snapshot.capture);
        part.runtime_snapshot = Some(snapshot);
        families.push((Kind::RuntimeSnapshot, part));
    }
    if let Some(runtime) = packet.runtime {
        macro_rules! field {
            ($name:ident, $kind:ident) => {
                if let Some(value) = runtime.$name {
                    let mut part = ObservabilityContext::new(stamp);
                    part.runtime = Some(RuntimeObservability {
                        $name: Some(value),
                        ..Default::default()
                    });
                    families.push((Kind::$kind, part));
                }
            };
        }
        field!(in_flight, InFlight);
        field!(join_reference_since_last_stream, JoinReference);
        field!(time_in_state_ms, StateAge);
        field!(event_loops_total, EventLoops);
        field!(event_loops_with_work_total, WorkLoops);
        if let Some(timing) = runtime.timing {
            if timing.is_valid() {
                let mut part = ObservabilityContext::new(stamp);
                part.runtime = Some(RuntimeObservability {
                    timing: Some(timing),
                    ..Default::default()
                });
                families.push((Kind::Timing, part));
            }
        }
        field!(circuit_breaker, CircuitBreaker);
        field!(rate_limiter, RateLimiter);
        for value in runtime.effect_circuit_breakers {
            let key = Kind::EffectCircuitBreaker(value.effect_type.clone());
            let mut part = ObservabilityContext::new(stamp);
            part.runtime = Some(RuntimeObservability {
                effect_circuit_breakers: vec![value],
                ..Default::default()
            });
            families.push((key, part));
        }
        for value in runtime.effect_rate_limiters {
            let key = Kind::EffectRateLimiter(value.effect_type.clone());
            let mut part = ObservabilityContext::new(stamp);
            part.runtime = Some(RuntimeObservability {
                effect_rate_limiters: vec![value],
                ..Default::default()
            });
            families.push((key, part));
        }
    }
    macro_rules! field {
        ($name:ident, $kind:ident) => {
            if let Some(value) = packet.$name {
                let mut part = ObservabilityContext::new(stamp);
                part.$name = Some(value);
                families.push((Kind::$kind, part));
            }
        };
    }
    field!(processing_time, ProcessingTime);
    field!(metrics, Metrics);
    field!(sli, Sli);
    for record in packet.records {
        let kind = match &record {
            ObservationRecord::Llm { .. } => Kind::Llm,
            ObservationRecord::CircuitBreakerSummary { effect_type, .. } => {
                Kind::BreakerSummary(effect_type.clone())
            }
            ObservationRecord::RateLimiterActivity { effect_type, .. } => {
                Kind::LimiterActivity(effect_type.clone())
            }
            ObservationRecord::RateLimiterUtilisation { effect_type, .. } => {
                Kind::LimiterUtilisation(effect_type.clone())
            }
            ObservationRecord::BackpressureActivity { .. } => Kind::Backpressure,
            ObservationRecord::ResourceUsage { .. } => Kind::Resource,
            ObservationRecord::HttpPull(_) => Kind::HttpPull,
            ObservationRecord::AiChunkingWork { .. } => Kind::AiChunking,
            ObservationRecord::HttpSurface { .. } => Kind::HttpSurface,
            ObservationRecord::StageHeartbeat { .. } => Kind::StageHeartbeat,
            ObservationRecord::EdgeLiveness {
                upstream, reader, ..
            } => Kind::EdgeLiveness(*upstream, *reader),
        };
        let mut part = ObservabilityContext::new(stamp);
        part.records.push(record);
        families.push((kind, part));
    }
    (families.len() <= MAX_PACKET_FAMILIES).then_some(families)
}

pub(crate) fn scope(execution: &RuntimeExecution, flow_id: FlowId) -> CaptureScope {
    CaptureScope {
        flow_id,
        resume_generation: execution
            .resume_control()
            .map(|control| control.resume_generation())
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::RuntimeMode;
    use crate::metrics::instrumentation::StageInstrumentation;
    use obzenflow_core::event::context::{MeasurementWindow, TimingMeasurements};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{ReaderGeneration, StageId};
    use std::time::Duration;

    fn packet(
        scope: CaptureScope,
        observer: WriterId,
        sequence: u64,
        value: u32,
    ) -> ObservabilityContext {
        let mut packet = ObservabilityContext::new(CaptureStamp {
            capture_scope: scope,
            observer,
            capture_seq: CaptureSeq(sequence),
            capture_reason: CaptureReason::Record,
            observed_at_ms: sequence,
        });
        packet.runtime = Some(RuntimeObservability {
            in_flight: Some(value),
            ..Default::default()
        });
        packet
    }

    #[test]
    fn runtime_snapshot_uses_its_own_stamp_and_replaces_the_whole_family() {
        use obzenflow_core::event::context::{ExecutionProgress, RuntimeSnapshot};

        let hub = ObservationHub::default();
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        };
        hub.activate_scope(scope);
        let upstream = StageId::new().into();
        let local = StageId::new().into();
        let mut carrier = packet(scope, upstream, 100, 9);
        let local_stamp = packet(scope, local, 5, 0).capture;
        carrier.runtime_snapshot = Some(RuntimeSnapshot {
            capture: local_stamp,
            progress: ExecutionProgress {
                reader_seq: 12,
                last_consumed_event_id: Some(obzenflow_core::EventId::new()),
                ..Default::default()
            },
            fsm_state: "Running".into(),
        });
        assert_eq!(hub.select_recorded(carrier.clone()).unwrap().len(), 2);
        let first = hub
            .snapshot()
            .into_iter()
            .find_map(|packet| packet.runtime_snapshot)
            .unwrap();
        assert_eq!(first.capture, local_stamp);
        assert_eq!(first.progress.reader_seq, 12);

        carrier.capture.capture_seq = CaptureSeq(101);
        carrier
            .runtime_snapshot
            .as_mut()
            .unwrap()
            .capture
            .capture_seq = CaptureSeq(4);
        carrier.runtime_snapshot.as_mut().unwrap().fsm_state = "Created".into();
        let selected = hub.select_recorded(carrier).unwrap();
        assert_eq!(selected.len(), 1);
        assert!(selected[0].runtime_snapshot.is_none());

        let mut newer = ObservabilityContext::new(local_stamp);
        newer.runtime_snapshot = Some(RuntimeSnapshot {
            capture: CaptureStamp {
                capture_seq: CaptureSeq(6),
                ..local_stamp
            },
            progress: ExecutionProgress::default(),
            fsm_state: "Drained".into(),
        });
        assert_eq!(hub.select_recorded(newer.clone()).unwrap().len(), 1);
        assert!(hub.select_recorded(newer.clone()).unwrap().is_empty());
        let latest = hub
            .snapshot()
            .into_iter()
            .find_map(|packet| packet.runtime_snapshot)
            .unwrap();
        assert_eq!(latest.capture.capture_seq, CaptureSeq(6));
        assert_eq!(latest.progress.reader_seq, 0);
        assert!(latest.progress.last_consumed_event_id.is_none());
        assert_eq!(latest.fsm_state, "Drained");

        let resumed = CaptureScope {
            resume_generation: ReaderGeneration(1),
            ..scope
        };
        hub.activate_scope(resumed);
        assert!(hub.select(newer.clone()).unwrap().is_empty());
        let snapshot = newer.runtime_snapshot.as_mut().unwrap();
        snapshot.capture.capture_scope = resumed;
        snapshot.capture.capture_seq = CaptureSeq(1);
        assert_eq!(hub.select(newer).unwrap().len(), 1);
        let latest = hub
            .snapshot()
            .into_iter()
            .find_map(|packet| packet.runtime_snapshot)
            .unwrap();
        assert_eq!(latest.capture.capture_scope, resumed);
        assert_eq!(latest.capture.capture_seq, CaptureSeq(1));
    }

    #[test]
    fn owners_share_sequence_across_helper_recreation_and_drop_on_contention() {
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let hub = execution.observations();
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        };
        let writer = WriterId::from(StageId::new());
        let first = hub.capture_owner(scope, writer, execution.clone());
        let restarted = hub.capture_owner(scope, writer, execution.clone());
        assert_eq!(
            first
                .capture(CaptureReason::Initial)
                .unwrap()
                .capture
                .capture_seq,
            CaptureSeq(1)
        );
        assert_eq!(
            restarted
                .capture(CaptureReason::Periodic)
                .unwrap()
                .capture
                .capture_seq,
            CaptureSeq(2)
        );
        let held = hub.view.lock().unwrap();
        assert_eq!(
            hub.offer(packet(scope, writer, 3, 4)),
            ObservationOffer::Dropped
        );
        drop(held);
        assert!(hub.snapshot().is_empty());
        assert_eq!(hub.dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn family_freshness_and_active_generation_are_independent_of_arrival_order() {
        let hub = ObservationHub::default();
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        };
        let writer = WriterId::from(StageId::new());
        hub.activate_scope(scope);
        assert_eq!(hub.select(packet(scope, writer, 10, 4)).unwrap().len(), 1);
        assert!(hub.select(packet(scope, writer, 9, 9)).unwrap().is_empty());
        assert!(hub.select(packet(scope, writer, 10, 9)).unwrap().is_empty());
        let mut timing = packet(scope, writer, 3, 0);
        timing.runtime.as_mut().unwrap().in_flight = None;
        timing.runtime.as_mut().unwrap().timing = Some(TimingMeasurements {
            processing_time_count: 0,
            processing_time_sum_nanos: 0,
            recent_p50_ms: None,
            recent_p90_ms: None,
            recent_p95_ms: None,
            recent_p99_ms: None,
            recent_p999_ms: None,
            window: MeasurementWindow {
                started_at_ms: 0,
                ended_at_ms: 3,
            },
        });
        assert_eq!(hub.select(timing).unwrap().len(), 1);
        assert_eq!(hub.snapshot().len(), 2);
        let resumed = CaptureScope {
            resume_generation: ReaderGeneration(1),
            ..scope
        };
        assert!(
            hub.select(packet(resumed, StageId::new().into(), 1, 2))
                .unwrap()
                .is_empty(),
            "an unknown owner cannot activate a future generation"
        );
        assert!(hub
            .select(packet(resumed, writer, 1, 2))
            .unwrap()
            .is_empty());
        hub.activate_scope(resumed);
        assert_eq!(hub.select(packet(resumed, writer, 1, 2)).unwrap().len(), 1);
        assert!(hub
            .select(packet(scope, writer, 100, 9))
            .unwrap()
            .is_empty());
        assert_eq!(
            hub.snapshot()
                .iter()
                .filter_map(|packet| packet.runtime.as_ref()?.in_flight)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(
            hub.snapshot()
                .iter()
                .any(|packet| packet.capture.capture_scope == scope
                    && packet.runtime.as_ref().unwrap().timing.is_some()),
            "an absent family retains the recorded sample identity"
        );
    }

    #[test]
    fn recorded_generations_restore_without_activating_execution_or_regressing() {
        let hub = ObservationHub::default();
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        };
        let resumed = CaptureScope {
            resume_generation: ReaderGeneration(1),
            ..scope
        };
        let writer = StageId::new().into();
        hub.offer_recorded(packet(resumed, writer, 1, 2));
        hub.offer_recorded(packet(scope, writer, 1000, 9));
        assert_eq!(hub.active_scope(), None);
        assert_eq!(hub.snapshot()[0].capture.capture_scope, resumed);
        assert_eq!(hub.snapshot()[0].capture.observed_at_ms, 1);
        assert_eq!(
            hub.snapshot()[0].runtime.as_ref().unwrap().in_flight,
            Some(2)
        );
    }

    #[test]
    fn capacity_and_unavailable_final_capture_drop_only_optional_samples() {
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let hub = execution.observations();
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: ReaderGeneration(0),
        };
        hub.activate_scope(scope);
        for _ in 0..MAX_KEYS {
            assert_eq!(
                hub.offer(packet(scope, StageId::new().into(), 1, 0)),
                ObservationOffer::Accepted
            );
        }
        assert_eq!(
            hub.offer(packet(scope, StageId::new().into(), 1, 0)),
            ObservationOffer::Dropped
        );
        assert_eq!(hub.snapshot().len(), MAX_KEYS);
        for _ in 0..MAX_OWNERS {
            assert!(hub
                .capture_owner(scope, StageId::new().into(), execution.clone())
                .capture(CaptureReason::Initial)
                .is_some());
        }
        let stage = StageId::new();
        let instrumentation = Arc::new(StageInstrumentation::new());
        instrumentation.bind_observations(scope.flow_id, stage.into(), &execution);
        instrumentation.record_output_event(&ChainEventFactory::data_event(
            stage.into(),
            "business.fact",
            serde_json::Value::Null,
        ));
        let before = instrumentation.snapshot();
        assert!(instrumentation
            .capture_observability(CaptureReason::Final)
            .is_none());
        assert_eq!(
            serde_json::to_value(instrumentation.snapshot()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert_eq!(hub.snapshot().len(), MAX_KEYS);
    }

    #[test]
    fn timing_contention_omits_that_family_and_zero_duration_is_a_measurement() {
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let instrumentation = Arc::new(StageInstrumentation::new());
        instrumentation.bind_observations(FlowId::new(), StageId::new().into(), &execution);
        instrumentation.record_processing_time(Duration::ZERO);
        let captured = instrumentation
            .capture_observability(CaptureReason::Record)
            .unwrap();
        let timing = captured.runtime.unwrap().timing.unwrap();
        assert_eq!(timing.processing_time_count, 1);
        assert_eq!(timing.processing_time_sum_nanos, 0);
        assert_eq!(timing.recent_p50_ms, Some(0));
        let held = instrumentation.processing_time_histogram.write().unwrap();
        let partial = instrumentation
            .capture_observability(CaptureReason::Periodic)
            .unwrap()
            .runtime
            .unwrap();
        assert!(partial.timing.is_none());
        assert_eq!(partial.in_flight, Some(0));
        drop(held);
    }

    #[test]
    fn strict_replay_does_not_capture_new_measurements() {
        let execution = RuntimeExecution::new(RuntimeMode::Replay, None);
        let instrumentation = Arc::new(StageInstrumentation::new());
        instrumentation.bind_observations(FlowId::new(), StageId::new().into(), &execution);
        instrumentation.record_processing_time(Duration::from_millis(5));
        assert!(instrumentation
            .capture_observability(CaptureReason::Final)
            .is_none());
        assert!(execution.observations().snapshot().is_empty());
    }
}
