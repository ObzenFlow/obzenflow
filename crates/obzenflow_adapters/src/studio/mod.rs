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
use obzenflow_core::event::{
    event_envelope::SystemEventEnvelope, PipelineLifecycleEvent, SystemEventType,
};
use obzenflow_core::{web::SseFrame, EventId};
use stages::StageLifecycleView;

#[derive(Clone, Copy, Default)]
enum ObservedFlowState {
    #[default]
    Inactive,
    Active,
    Terminal,
}

/// Current state and message builder for one Studio connection.
#[derive(Clone)]
pub struct StudioProjection {
    stages: StageLifecycleView,
    composites: CompositeLifecycleView,
    middleware: MiddlewareView,
    aliases: ContractBoundaryAliases,
    flow_state: ObservedFlowState,
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
        })
    }

    /// Apply a past journal entry without producing messages.
    pub fn rebuild(&mut self, envelope: &SystemEventEnvelope) {
        self.observe(envelope);
    }

    /// Apply a journal entry and return its messages. Only middleware snapshots
    /// use `timestamp_ms`; event messages keep their recorded timestamps.
    pub fn project(&mut self, envelope: &SystemEventEnvelope, timestamp_ms: u64) -> Vec<SseFrame> {
        // `state_from` must describe the circuit breaker before this entry is applied.
        let mut frames = Vec::new();
        frames.extend(facts::frame(envelope, &self.middleware, &self.aliases));
        let composite = self.observe(envelope);
        frames.extend(composite.as_ref().and_then(composite_status_frame));
        if matches!(
            &envelope.event.event,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Running { .. })
        ) {
            frames.extend(self.middleware_snapshot(timestamp_ms));
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

    fn observe(&mut self, envelope: &SystemEventEnvelope) -> Option<CompositeStatusSnapshot> {
        self.stages.observe(envelope);
        let composite = self.composites.observe(envelope);
        self.middleware.observe(envelope);
        self.observe_pipeline(envelope);
        composite
    }

    fn observe_pipeline(&mut self, envelope: &SystemEventEnvelope) {
        let SystemEventType::PipelineLifecycle(event) = &envelope.event.event else {
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
