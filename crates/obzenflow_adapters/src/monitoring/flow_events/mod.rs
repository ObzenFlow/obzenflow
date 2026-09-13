// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disposable flow-events read models and the existing Studio SSE wire projection.
//!
//! Callers supply ordered committed facts, validated topology and observation time.
//! This module performs no journal, clock, listener or network operations.

mod composites;
mod contracts;
mod events;
mod middleware;
mod stages;
#[cfg(test)]
mod tests;

pub use contracts::{ContractBoundaryAlias, ContractBoundaryAliases, ContractBoundaryDirection};

use composites::{map_composite_status_to_sse, CompositeLifecycleSseState};
use middleware::MiddlewareSseState;
use obzenflow_core::composite::{
    CompositeDefinition, CompositeLifecycleProjection, CompositeProjectionError,
};
use obzenflow_core::event::{
    event_envelope::SystemEventEnvelope, PipelineLifecycleEvent, SystemEventType,
};
use obzenflow_core::{web::SseFrame, EventId};
use stages::StageLifecycleSseState;

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
pub struct FlowEventsProjection {
    stages: StageLifecycleSseState,
    composites: CompositeLifecycleSseState,
    middleware: MiddlewareSseState,
    aliases: ContractBoundaryAliases,
    pipeline: PipelineReadout,
}

impl FlowEventsProjection {
    pub fn new(
        definitions: Vec<CompositeDefinition>,
        aliases: ContractBoundaryAliases,
    ) -> Result<Self, CompositeProjectionError> {
        Ok(Self {
            stages: StageLifecycleSseState::default(),
            composites: CompositeLifecycleSseState::new(CompositeLifecycleProjection::new(
                definitions,
            )?),
            middleware: MiddlewareSseState::default(),
            aliases,
            pipeline: PipelineReadout::Inactive,
        })
    }

    /// Rebuild history without publishing its original frames to a fresh browser.
    pub fn rebuild(&mut self, envelope: &SystemEventEnvelope) {
        self.stages.observe(envelope);
        self.composites.observe(envelope);
        self.middleware.observe(envelope);
        self.observe_pipeline(envelope);
    }

    /// Project one live fact, followed by its cursorless derived frames.
    pub fn project(&mut self, envelope: &SystemEventEnvelope, timestamp_ms: u64) -> Vec<SseFrame> {
        self.stages.observe(envelope);
        let composite = self.composites.observe(envelope);
        self.observe_pipeline(envelope);
        let mut frames = Vec::new();
        frames.extend(events::map_system_event_to_sse(
            envelope,
            &mut self.middleware,
            &self.aliases,
        ));
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

fn journal_frame(id: EventId, event: &str, data: serde_json::Value) -> SseFrame {
    let mut frame = SseFrame::event(event, data.to_string());
    frame.id = Some(id.to_string());
    frame
}

pub fn bootstrap(checkpoint: Option<EventId>, runtime_instance_id: Option<&str>) -> SseFrame {
    let mut frame = SseFrame::event(
        "bootstrap",
        serde_json::json!({
            "system_event_type": "bootstrap",
            "event_type": "flow_bootstrap",
            "checkpoint_event_id": checkpoint.map(|id| id.to_string()),
            "runtime_instance_id": runtime_instance_id,
        })
        .to_string(),
    );
    frame.id = checkpoint.map(|id| id.to_string());
    frame
}

pub fn server_shutdown(runtime_instance_id: Option<&str>) -> SseFrame {
    SseFrame::event(
        "server_shutdown",
        serde_json::json!({
            "system_event_type": "server_shutdown",
            "runtime_instance_id": runtime_instance_id,
        })
        .to_string(),
    )
}

/// Existing connection-local error vocabulary; no error is an execution fact.
pub enum FlowEventsError {
    InvalidCursor(String),
    UnknownCursor,
    JournalOpen(String),
    JournalRead(String),
}

impl FlowEventsError {
    pub fn frame(&self) -> SseFrame {
        let (error_type, message, recoverable) = match self {
            Self::InvalidCursor(message) => ("invalid_last_event_id", message.as_str(), false),
            Self::UnknownCursor => (
                "journal_resume_not_found",
                "Last-Event-ID was not found in the system journal; resuming from live tail",
                true,
            ),
            Self::JournalOpen(message) => ("journal_open_error", message.as_str(), false),
            Self::JournalRead(message) => ("journal_read_error", message.as_str(), false),
        };
        SseFrame::event("error", serde_json::json!({ "error_type": error_type, "message": message, "recoverable": recoverable }).to_string())
    }
}
