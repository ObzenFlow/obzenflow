// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Studio lifecycle updates reconstructed from committed system-journal facts.
//!
//! Callers supply ordered committed facts, validated topology and observation time.
//! This module performs no journal, clock, listener or network operations.
//!
//! Start with `messages.rs` for the Studio vocabulary and `StudioProjection`
//! below for replay/live behaviour. Infra owns the journal reader and HTTP body.

mod composites;
mod contracts;
mod facts;
mod messages;
mod middleware;
mod stages;
#[cfg(test)]
mod tests;

pub use contracts::{ContractBoundaryAlias, ContractBoundaryAliases, ContractBoundaryDirection};

use composites::{map_composite_status_to_sse, CompositeLifecycleView, CompositeStatusSnapshot};
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
enum PipelineReadout {
    #[default]
    Inactive,
    Active,
    Terminal,
}

/// A validated empty projection can be cloned for each independent connection.
/// Rebuilding and live projection use the same state and Core composite semantics.
#[derive(Clone)]
pub struct StudioProjection {
    stages: StageLifecycleView,
    composites: CompositeLifecycleView,
    middleware: MiddlewareView,
    aliases: ContractBoundaryAliases,
    pipeline: PipelineReadout,
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
            pipeline: PipelineReadout::Inactive,
        })
    }

    /// Rebuild history without publishing its original frames to a fresh browser.
    pub fn rebuild(&mut self, envelope: &SystemEventEnvelope) {
        self.observe(envelope);
    }

    /// Project one live fact, followed by its cursorless derived frames.
    pub fn project(&mut self, envelope: &SystemEventEnvelope, timestamp_ms: u64) -> Vec<SseFrame> {
        // A transition describes its prior state. Encode it before the shared
        // fold, then publish derived snapshots of the updated state.
        let mut frames = Vec::new();
        frames.extend(facts::frame(envelope, &self.middleware, &self.aliases));
        let composite = self.observe(envelope);
        frames.extend(composite.as_ref().and_then(map_composite_status_to_sse));
        if matches!(
            &envelope.event.event,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Running { .. })
        ) {
            frames.extend(self.middleware_snapshot(timestamp_ms));
        }
        frames
    }

    pub fn snapshots(&self) -> Vec<SseFrame> {
        let mut frames = self.stages.build_snapshot_sse_events();
        frames.extend(self.resume_snapshots());
        frames
    }

    /// Repair a disconnect between an identified fact and its derived status.
    pub fn resume_snapshots(&self) -> Vec<SseFrame> {
        self.composites.build_snapshot_sse_events()
    }

    pub fn middleware_snapshot(&self, timestamp_ms: u64) -> Option<SseFrame> {
        self.middleware.build_snapshot_sse_event(timestamp_ms)
    }

    pub fn terminal_observed(&self) -> bool {
        matches!(self.pipeline, PipelineReadout::Terminal)
    }

    pub fn active_observed(&self) -> bool {
        matches!(self.pipeline, PipelineReadout::Active)
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
        self.pipeline = match event {
            PipelineLifecycleEvent::Running { .. }
            | PipelineLifecycleEvent::Draining { .. }
            | PipelineLifecycleEvent::AllStagesCompleted { .. } => PipelineReadout::Active,
            PipelineLifecycleEvent::Drained
            | PipelineLifecycleEvent::Completed { .. }
            | PipelineLifecycleEvent::Cancelled { .. }
            | PipelineLifecycleEvent::Failed { .. } => PipelineReadout::Terminal,
            // Preserve the existing close boundary: NotStarted is followed by
            // Runtime's final Drained marker before normal application close.
            PipelineLifecycleEvent::Starting
            | PipelineLifecycleEvent::ReadyForRun { .. }
            | PipelineLifecycleEvent::StopAdmitted { .. }
            | PipelineLifecycleEvent::NotStarted => PipelineReadout::Inactive,
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

/// Existing connection-local error vocabulary; no error is an execution fact.
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
