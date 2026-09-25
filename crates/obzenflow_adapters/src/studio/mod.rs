// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builds the status updates Studio uses to display a flow.
//!
//! `StudioProjection` turns system journal entries into the messages defined in
//! `messages.rs`. It also keeps stage, composite and middleware state so a browser
//! can show the current status when it connects.
//!
//! Infra reads the journal and delivers the messages over HTTP. Core defines
//! how member stage statuses combine into a composite status.

mod composites;
mod contracts;
mod facts;
mod messages;
mod middleware;
mod stages;
#[cfg(test)]
mod tests;

pub use contracts::{ContractBoundaryAlias, ContractBoundaryAliases, ContractBoundaryDirection};

use composites::{composite_status_frame, CompositeLifecycleView, CompositeStatusSnapshot};
use messages::{BootstrapUpdate, StreamErrorKind, StudioMessage};
use middleware::MiddlewareView;
use obzenflow_core::composite::{
    CompositeDefinition, CompositeLifecycleProjection, CompositeProjectionError,
};
use obzenflow_core::event::observability::{ObservabilityContext, ObservationSource};
use obzenflow_core::event::{
    journal_record::SystemJournalRecord, PipelineLifecycleEvent, SystemPayload,
};
use obzenflow_core::{web::SseFrame, EventId};
use obzenflow_runtime::metrics::observations::LatestObservationMap;
use stages::StageLifecycleView;
use std::sync::Arc;

#[derive(Clone, Copy, Default)]
enum ObservedFlowState {
    #[default]
    Inactive,
    Active,
    Terminal,
}

/// Current state and message builder for one Studio connection.
pub struct StudioProjection {
    stages: StageLifecycleView,
    composites: CompositeLifecycleView,
    middleware: MiddlewareView,
    aliases: ContractBoundaryAliases,
    flow_state: ObservedFlowState,
    /// Latest values already projected into this connection's SSE frames.
    emitted_observations: LatestObservationMap,
    live_measurements: Option<Arc<dyn ObservationSource>>,
    /// Latest values seen, with intermediate captures overwritten per key.
    latest_observations: LatestObservationMap,
    throughput: Option<Arc<dyn obzenflow_core::metrics::ThroughputSource>>,
    last_throughput: obzenflow_core::metrics::ThroughputSnapshot,
    middleware_snapshot_pending: bool,
}

impl Clone for StudioProjection {
    fn clone(&self) -> Self {
        Self {
            stages: self.stages.clone(),
            composites: self.composites.clone(),
            middleware: self.middleware.clone(),
            aliases: self.aliases.clone(),
            flow_state: self.flow_state,
            emitted_observations: self.emitted_observations.retained_copy(),
            live_measurements: self.live_measurements.clone(),
            latest_observations: self.latest_observations.retained_copy(),
            throughput: self.throughput.clone(),
            last_throughput: self.last_throughput.clone(),
            middleware_snapshot_pending: self.middleware_snapshot_pending,
        }
    }
}

impl StudioProjection {
    pub fn new(
        definitions: Vec<CompositeDefinition>,
        aliases: ContractBoundaryAliases,
    ) -> Result<Self, CompositeProjectionError> {
        Ok(Self {
            stages: StageLifecycleView::default(),
            composites: CompositeLifecycleView::new(CompositeLifecycleProjection::new(
                definitions,
            )?),
            middleware: MiddlewareView::default(),
            aliases,
            flow_state: ObservedFlowState::Inactive,
            emitted_observations: Default::default(),
            live_measurements: None,
            latest_observations: Default::default(),
            throughput: None,
            last_throughput: Default::default(),
            middleware_snapshot_pending: false,
        })
    }

    /// Apply a past journal entry without producing messages.
    pub fn rebuild(&mut self, envelope: &SystemJournalRecord) {
        self.observe(envelope);
        if let Some(packet) = &envelope.envelope.observability {
            self.project_retained_measurements(packet.clone());
        }
    }

    /// Apply a journal entry and return its messages. Only middleware snapshots
    /// use `timestamp_ms`; event messages keep their recorded timestamps.
    pub fn project(&mut self, envelope: &SystemJournalRecord, timestamp_ms: u64) -> Vec<SseFrame> {
        // `state_from` must describe the circuit breaker before this entry is applied.
        let mut frames = Vec::new();
        frames.extend(facts::frame(envelope, &self.middleware, &self.aliases));
        let composite = self.observe(envelope);
        frames.extend(composite.as_ref().and_then(composite_status_frame));
        if let Some(packet) = &envelope.envelope.observability {
            frames.extend(self.project_retained_measurements(packet.clone()));
        }
        if matches!(
            &envelope.payload,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running { .. })
        ) {
            frames.extend(self.middleware_snapshot(timestamp_ms));
        }
        frames
    }

    pub fn with_observations(mut self, source: Arc<dyn ObservationSource>) -> Self {
        if let Some(scope) = source.active_scope() {
            self.emitted_observations.activate_scope(scope);
        }
        self.live_measurements = Some(source);
        self
    }

    pub fn with_throughput(
        mut self,
        source: Arc<dyn obzenflow_core::metrics::ThroughputSource>,
    ) -> Self {
        self.throughput = Some(source);
        self
    }

    /// Fold facts immediately, retaining only the latest attached observation
    /// per key for subsequent SSE emission turns.
    pub fn project_deferred(&mut self, envelope: &SystemJournalRecord) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        frames.extend(facts::frame(envelope, &self.middleware, &self.aliases));
        let composite = self.observe(envelope);
        frames.extend(composite.as_ref().and_then(composite_status_frame));
        self.retain_latest_observations(envelope);
        if matches!(
            &envelope.payload,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running { .. })
        ) {
            self.middleware_snapshot_pending = true;
        }
        frames
    }

    pub fn rebuild_deferred(&mut self, envelope: &SystemJournalRecord) {
        self.observe(envelope);
        self.retain_latest_observations(envelope);
    }

    fn retain_latest_observations(&self, envelope: &SystemJournalRecord) {
        if let Some(packet) = &envelope.envelope.observability {
            let _ = self.latest_observations.select_recorded(packet.clone());
        }
    }

    /// Called after the fixed initial prefix, at the connection's observation
    /// deadline. Recorded and live observations share selection and cadence.
    pub fn current_measurements(&mut self) -> Vec<SseFrame> {
        if let Some(source) = self.live_measurements.clone() {
            if let Some(scope) = source.active_scope() {
                self.emitted_observations.activate_scope(scope);
                self.latest_observations.activate_scope(scope);
            }
            for packet in source.snapshot() {
                let _ = self.latest_observations.select_recorded(packet);
            }
        }
        let mut frames: Vec<_> = self
            .latest_observations
            .snapshot()
            .into_iter()
            .flat_map(|packet| self.project_retained_measurements(packet))
            .collect();
        if let Some(source) = &self.throughput {
            let latest = source.throughput();
            if latest != self.last_throughput {
                let mut stages: Vec<_> = latest
                    .stages
                    .iter()
                    .map(|(stage_id, measurement)| messages::StageThroughput {
                        stage_id: *stage_id,
                        measurement,
                    })
                    .collect();
                stages.sort_by_key(|stage| stage.stage_id);
                frames.push(
                    StudioMessage::ThroughputUpdate {
                        stages,
                        flow_input: latest.flow_input.as_ref(),
                        flow_output: latest.flow_output.as_ref(),
                    }
                    .frame(None),
                );
                self.last_throughput = latest;
            }
        }
        if std::mem::take(&mut self.middleware_snapshot_pending) {
            let timestamp = self
                .latest_observations
                .snapshot()
                .iter()
                .map(|packet| packet.capture.observed_at_ms)
                .max()
                .unwrap_or(0);
            frames.extend(self.middleware_snapshot(timestamp));
        }
        frames
    }

    pub fn project_measurements(&mut self, packet: ObservabilityContext) -> Vec<SseFrame> {
        self.measurement_frames(self.emitted_observations.select(packet).unwrap_or_default())
    }

    fn project_retained_measurements(&mut self, packet: ObservabilityContext) -> Vec<SseFrame> {
        self.measurement_frames(
            self.emitted_observations
                .select_recorded(packet)
                .unwrap_or_default(),
        )
    }

    fn measurement_frames(&mut self, selected: Vec<ObservabilityContext>) -> Vec<SseFrame> {
        use obzenflow_core::event::observability::ObservationRecord;
        let mut frames = Vec::new();
        for selected in selected {
            frames.extend(self.middleware.measurements(&selected));
            for record in &selected.records {
                if let ObservationRecord::EdgeLiveness {
                    upstream,
                    reader,
                    state,
                    idle_ms,
                    last_reader_seq,
                    last_event_id,
                } = record
                {
                    frames.push(
                        StudioMessage::EdgeLiveness {
                            upstream_stage_id: *upstream,
                            reader_stage_id: *reader,
                            state: *state,
                            idle_ms: *idle_ms,
                            last_reader_seq: *last_reader_seq,
                            last_event_id: *last_event_id,
                            at: messages::Observation {
                                commitment: None,
                                timestamp_ms: selected.capture.observed_at_ms,
                                vector_clock: None,
                                capture: Some(selected.capture),
                            },
                        }
                        .frame(None),
                    );
                }
            }
        }
        frames
    }

    pub fn snapshots(&self) -> Vec<SseFrame> {
        let mut frames = self.stages.snapshot_frames();
        frames.extend(self.resume_snapshots());
        frames
    }

    /// A browser may receive a stage event and disconnect before its composite
    /// update arrives. Resend group statuses when it resumes after that event ID.
    pub fn resume_snapshots(&self) -> Vec<SseFrame> {
        self.composites.snapshot_frames()
    }

    pub fn middleware_snapshot(&self, timestamp_ms: u64) -> Option<SseFrame> {
        self.middleware.snapshot_frame(timestamp_ms)
    }

    pub fn terminal_observed(&self) -> bool {
        matches!(self.flow_state, ObservedFlowState::Terminal)
    }

    pub fn active_observed(&self) -> bool {
        matches!(self.flow_state, ObservedFlowState::Active)
    }

    fn observe(&mut self, envelope: &SystemJournalRecord) -> Option<CompositeStatusSnapshot> {
        self.stages.observe(envelope);
        let composite = self.composites.observe(envelope);
        self.middleware.observe(envelope);
        self.observe_pipeline(envelope);
        composite
    }

    fn observe_pipeline(&mut self, envelope: &SystemJournalRecord) {
        let SystemPayload::PipelineLifecycle(event) = &envelope.payload else {
            return;
        };
        self.flow_state = match event {
            PipelineLifecycleEvent::Running { .. }
            | PipelineLifecycleEvent::Draining { .. }
            | PipelineLifecycleEvent::AllStagesCompleted { .. } => ObservedFlowState::Active,
            PipelineLifecycleEvent::Drained
            | PipelineLifecycleEvent::Completed { .. }
            | PipelineLifecycleEvent::Cancelled { .. }
            | PipelineLifecycleEvent::Failed { .. } => ObservedFlowState::Terminal,
            // NotStarted is followed by Drained; keep reading until cleanup ends.
            PipelineLifecycleEvent::Starting
            | PipelineLifecycleEvent::ReadyForRun { .. }
            | PipelineLifecycleEvent::StopAdmitted { .. }
            | PipelineLifecycleEvent::NotStarted => ObservedFlowState::Inactive,
        };
    }
}

pub fn bootstrap(checkpoint: Option<EventId>, runtime_instance_id: Option<&str>) -> SseFrame {
    StudioMessage::Bootstrap {
        event_type: BootstrapUpdate::FlowBootstrap,
        checkpoint_event_id: checkpoint,
        runtime_instance_id,
    }
    .frame(checkpoint)
}

pub fn server_shutdown(runtime_instance_id: Option<&str>) -> SseFrame {
    StudioMessage::ServerShutdown {
        runtime_instance_id,
    }
    .frame(None)
}

/// Errors reported to this Studio connection when reading or resuming fails.
pub enum StudioStreamError {
    InvalidCursor(String),
    UnknownCursor,
    JournalOpen(String),
    JournalRead(String),
}

impl StudioStreamError {
    pub fn frame(&self) -> SseFrame {
        let (error_type, message, recoverable) = match self {
            Self::InvalidCursor(message) => {
                (StreamErrorKind::InvalidLastEventId, message.as_str(), false)
            }
            Self::UnknownCursor => (
                StreamErrorKind::JournalResumeNotFound,
                "Last-Event-ID was not found in the system journal; resuming from live tail",
                true,
            ),
            Self::JournalOpen(message) => {
                (StreamErrorKind::JournalOpenError, message.as_str(), false)
            }
            Self::JournalRead(message) => {
                (StreamErrorKind::JournalReadError, message.as_str(), false)
            }
        };
        StudioMessage::Error {
            error_type,
            message,
            recoverable,
        }
        .frame(None)
    }
}
