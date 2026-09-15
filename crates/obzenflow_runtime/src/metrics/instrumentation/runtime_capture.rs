// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Record-boundary capture, split into protected accounting and optional diagnostics.

use super::StageInstrumentation;
use obzenflow_core::event::context::{
    ExecutionAccounting, ExecutionProgress, RuntimeProvenance, RuntimeSnapshot,
};
use obzenflow_core::event::observation::{CaptureReason, ObservabilityContext};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use std::sync::atomic::Ordering;

#[derive(Clone)]
pub(crate) struct RuntimeCapture {
    pub accounting: ExecutionAccounting,
    pub observation: Option<RuntimeSnapshot>,
}

impl RuntimeCapture {
    /// Project the output into the attached prefix. This capture is never
    /// offered to the live handoff before its carrier commits.
    pub fn project_emission(&mut self, event: &ChainEvent) {
        if let Some(snapshot) = &mut self.observation {
            snapshot.progress.writer_seq = snapshot.progress.writer_seq.saturating_add(1);
            snapshot.progress.last_emitted_event_id = Some(event.id);
            snapshot.progress.last_emitted_writer = Some(event.writer_id);
        }
    }

    pub fn attach_to(self, event: ChainEvent) -> ChainEvent {
        let event = event.with_runtime_provenance(RuntimeProvenance {
            accounting: self.accounting,
        });
        match self.observation {
            Some(snapshot) => event.with_runtime_snapshot(snapshot),
            None => event,
        }
    }

    pub fn for_group_member(
        &self,
        instrumentation: &StageInstrumentation,
        scope: MiddlewareExecutionScope,
    ) -> Self {
        let mut member = self.clone();
        member.observation = member.observation.and_then(|mut snapshot| {
            let owner = instrumentation.observation_owner.get()?;
            let packet = owner.capture_in_scope(CaptureReason::Record, scope)?;
            snapshot.capture.capture_seq = packet.capture.capture_seq;
            Some(snapshot)
        });
        member
    }
}

impl StageInstrumentation {
    /// Preserve the existing per-record capture boundary. Missing observation
    /// ownership or contended diagnostic locks cannot hide factual accounting.
    pub(crate) fn capture_runtime(&self) -> RuntimeCapture {
        let packet = self
            .observation_owner
            .get()
            .and_then(|owner| owner.capture(CaptureReason::Record));
        self.capture_runtime_with_packet(packet)
    }

    pub(crate) fn capture_runtime_in_scope(
        &self,
        scope: MiddlewareExecutionScope,
    ) -> RuntimeCapture {
        let packet = self
            .observation_owner
            .get()
            .and_then(|owner| owner.capture_in_scope(CaptureReason::Record, scope));
        self.capture_runtime_with_packet(packet)
    }

    fn capture_runtime_with_packet(&self, packet: Option<ObservabilityContext>) -> RuntimeCapture {
        RuntimeCapture {
            accounting: self.snapshot().accounting,
            observation: packet.and_then(|packet| self.capture_runtime_snapshot(packet)),
        }
    }

    fn capture_runtime_snapshot(&self, packet: ObservabilityContext) -> Option<RuntimeSnapshot> {
        // Read the diagnostic family together or omit it. These locks are not
        // needed for terminal accounting or the actual reader/receipt frontier.
        let consumed_id = self.last_consumed_event_id.try_read().ok()?;
        let consumed_writer = self.last_consumed_writer.try_read().ok()?;
        let consumed_clock = self.last_consumed_vector_clock.try_read().ok()?;
        let receipted_id = self.last_receipted_event_id.try_read().ok()?;
        let receipted_clock = self.last_receipted_vector_clock.try_read().ok()?;
        let emitted_id = self.last_emitted_event_id.try_read().ok()?;
        let emitted_writer = self.last_emitted_writer.try_read().ok()?;
        let state = self.current_state.try_read().ok()?;

        Some(RuntimeSnapshot {
            capture: packet.capture,
            progress: ExecutionProgress {
                reader_seq: self.reader_seq.load(Ordering::Relaxed),
                receipted_seq: self.receipted_seq.load(Ordering::Relaxed),
                writer_seq: self.writer_seq.load(Ordering::Relaxed),
                last_consumed_event_id: *consumed_id,
                last_consumed_writer: *consumed_writer,
                last_consumed_vector_clock: consumed_clock.clone(),
                last_receipted_event_id: *receipted_id,
                last_receipted_vector_clock: receipted_clock.clone(),
                last_emitted_event_id: *emitted_id,
                last_emitted_writer: *emitted_writer,
            },
            fsm_state: state.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use obzenflow_core::event::context::RuntimeObservability;
    use obzenflow_core::event::observation::{CaptureSeq, ObservationSource};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{FlowId, StageId};
    use std::sync::Arc;

    #[test]
    fn local_snapshot_preserves_foreign_measurements_and_is_not_offered_before_commit() {
        let stage = StageId::new();
        let instrumentation = Arc::new(StageInstrumentation::new());
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        instrumentation.bind_observations(FlowId::new(), stage.into(), &execution);
        instrumentation.transition_to_state("Running");
        instrumentation
            .events_processed_total
            .store(7, Ordering::Relaxed);
        let mut capture = instrumentation.capture_runtime();
        let local_stamp = capture.observation.as_ref().unwrap().capture;
        let mut foreign_stamp = local_stamp;
        foreign_stamp.observer = StageId::new().into();
        foreign_stamp.capture_seq = CaptureSeq(500);
        foreign_stamp.observed_at_ms = 1;
        let mut foreign = ObservabilityContext::new(foreign_stamp);
        foreign.runtime = Some(RuntimeObservability {
            in_flight: Some(9),
            ..Default::default()
        });
        let event =
            ChainEventFactory::data_event(stage.into(), "test.fact", serde_json::Value::Null)
                .with_observability_context(foreign);
        capture.project_emission(&event);
        let event = capture.attach_to(event);
        let packet = event.envelope.observability.as_ref().unwrap();
        assert_eq!(packet.capture, foreign_stamp);
        assert_eq!(packet.runtime.as_ref().unwrap().in_flight, Some(9));
        let snapshot = packet.runtime_snapshot.as_ref().unwrap();
        assert_eq!(snapshot.capture, local_stamp);
        assert_eq!(snapshot.fsm_state, "Running");
        assert_eq!(snapshot.progress.writer_seq, 1);
        assert_eq!(snapshot.progress.last_emitted_event_id, Some(event.id));
        assert_eq!(snapshot.progress.last_emitted_writer, Some(stage.into()));
        assert_eq!(
            event
                .runtime
                .as_ref()
                .unwrap()
                .accounting
                .events_processed_total,
            7
        );
        assert_eq!(instrumentation.writer_seq.load(Ordering::Relaxed), 0);
        assert!(execution
            .observations()
            .snapshot()
            .iter()
            .all(|packet| packet.runtime_snapshot.is_none()));

        let local = packet.for_observer(stage.into()).unwrap();
        assert_eq!(local.capture, local_stamp);
        assert!(local.runtime.is_none());
        let upstream = packet.for_observer(foreign_stamp.observer).unwrap();
        assert!(upstream.runtime_snapshot.is_none());
        assert_eq!(upstream.runtime.unwrap().in_flight, Some(9));
    }

    #[test]
    fn diagnostic_contention_omits_only_the_snapshot_and_never_blocks_accounting() {
        let instrumentation = Arc::new(StageInstrumentation::new());
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let stage = StageId::new();
        instrumentation.bind_observations(FlowId::new(), stage.into(), &execution);
        instrumentation
            .events_processed_total
            .store(42, Ordering::Relaxed);
        let held = instrumentation.last_consumed_vector_clock.write().unwrap();
        let capture = instrumentation.capture_runtime();
        assert!(capture.observation.is_none());
        assert_eq!(capture.accounting.events_processed_total, 42);
        let event = capture.attach_to(ChainEventFactory::data_event(
            stage.into(),
            "test.fact",
            serde_json::Value::Null,
        ));
        assert!(event.envelope.observability.is_none());
        assert_eq!(
            event
                .runtime
                .as_ref()
                .unwrap()
                .accounting
                .events_processed_total,
            42
        );
        drop(held);
        assert!(instrumentation.capture_runtime().observation.is_some());
        assert!(instrumentation
            .capture_runtime_in_scope(MiddlewareExecutionScope::StrictReplayHandler)
            .observation
            .is_none());
    }
}
