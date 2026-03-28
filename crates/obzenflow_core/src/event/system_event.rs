// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration events (written to control journal)

use crate::event::observability::HttpSurfaceMetricsSnapshot;
use crate::event::payloads::observability_payload::MiddlewareLifecycle;
use crate::event::types::{Count, DurationMs, EventId, SeqNo, WriterId};
use crate::event::vector_clock::VectorClock;
use crate::id::{StageId, SystemId};
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::metrics::{FlowLifecycleMetricsSnapshot, StageMetricsSnapshot};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareEventOrigin {
    pub event_id: EventId,
    pub writer_key: String,
    pub seq: SeqNo,
}

/// System orchestration event with metadata (written to control journal)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemEvent {
    /// Unique event identifier
    pub id: EventId,

    /// Which component created this event
    pub writer_id: WriterId,

    /// The actual system event type
    #[serde(flatten)]
    pub event: SystemEventType,

    /// When this event was created (ms since epoch)
    pub timestamp: u64,
}

/// Types of system events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "system_event_type", rename_all = "snake_case")]
pub enum SystemEventType {
    /// Stage lifecycle events
    #[serde(rename = "stage_lifecycle")]
    StageLifecycle {
        stage_id: StageId,
        #[serde(flatten)]
        event: StageLifecycleEvent,
    },

    /// Pipeline lifecycle events
    #[serde(rename = "pipeline_lifecycle")]
    PipelineLifecycle(PipelineLifecycleEvent),

    /// Replay lifecycle events (FLOWIP-095a).
    #[serde(rename = "replay_lifecycle")]
    ReplayLifecycle(ReplayLifecycleEvent),

    /// Metrics subsystem coordination
    #[serde(rename = "metrics_coordination")]
    MetricsCoordination(MetricsCoordinationEvent),

    /// Periodic heartbeat from a stage supervisor.
    ///
    /// FLOWIP-063e: Under the resolved design, this is retained for future use
    /// (e.g. broadcast SSE) but is not journalled in v1.
    #[serde(rename = "stage_heartbeat")]
    StageHeartbeat {
        stage_id: StageId,
        stage_name: String,
        /// Monotonically increasing counter per stage, so consumers can detect gaps.
        heartbeat_seq: SeqNo,
        /// What the supervisor is currently doing.
        activity: StageActivity,
        /// Last event the handler consumed (if any).
        last_consumed_event_id: Option<EventId>,
        /// Last event the stage emitted (if any).
        last_output_event_id: Option<EventId>,
        /// Wall-clock duration the handler has been blocked (if currently processing).
        handler_blocked_ms: Option<DurationMs>,
    },

    /// Per-edge liveness verdict derived from heartbeats and/or stall detection.
    ///
    /// FLOWIP-063e: non-gating, system journal only.
    #[serde(rename = "edge_liveness")]
    EdgeLiveness {
        upstream: StageId,
        reader: StageId,
        state: EdgeLivenessState,
        /// Milliseconds since last observed progress on this edge.
        idle_ms: DurationMs,
        /// Reader sequence observed on this edge (if known).
        #[serde(skip_serializing_if = "Option::is_none")]
        last_reader_seq: Option<SeqNo>,
        /// Last event ID observed on this edge (if known).
        #[serde(skip_serializing_if = "Option::is_none")]
        last_event_id: Option<EventId>,
    },

    /// Middleware lifecycle events mirrored into `system.log` (FLOWIP-059c).
    ///
    /// Middleware observability originates in stage journals via middleware control events.
    /// `/api/flow/events` is backed by `system.log`, so we mirror selected low-volume middleware
    /// events here for SSE consumption.
    #[serde(rename = "middleware_lifecycle")]
    MiddlewareLifecycle {
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        flow_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        flow_name: Option<String>,
        origin: MiddlewareEventOrigin,
        middleware: MiddlewareLifecycle,
    },

    /// Contract status reported by a reader/subscriber (per upstream)
    #[serde(rename = "contract_status")]
    ContractStatus {
        upstream: StageId,
        reader: StageId,
        pass: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<crate::event::types::ViolationCause>,
    },

    /// Raw contract verification result for a single contract on an edge.
    ///
    /// This is emitted by readers/subscribers when `ContractChain::verify_all`
    /// runs (typically at EOF) and is intended for metrics/observability rather
    /// than pipeline gating.
    #[serde(rename = "contract_result")]
    ContractResult {
        upstream: StageId,
        reader: StageId,
        contract_name: String,
        /// "passed", "failed", "pending", or "healthy" (mid-flight heartbeat)
        status: String,
        /// Stable category label (e.g. "seq_divergence", "content_mismatch", "other")
        #[serde(skip_serializing_if = "Option::is_none")]
        cause: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
    },

    /// Contract decision overridden by a policy layer (e.g., breaker-aware).
    #[serde(rename = "contract_override_by_policy")]
    ContractOverrideByPolicy {
        upstream: StageId,
        reader: StageId,
        contract_name: String,
        original_cause: crate::contracts::ViolationCause,
        policy: String,
    },

    /// Generic hosted HTTP surface metrics snapshot emitted by the application host (FLOWIP-093a).
    ///
    /// This is intentionally a system journal event because hosted surfaces are not topology
    /// stages, but their observability must still be reconstructible from journaled facts.
    #[serde(rename = "http_surface_snapshot")]
    HttpSurfaceSnapshot {
        #[serde(flatten)]
        snapshot: HttpSurfaceMetricsSnapshot,
    },
}

/// Stable status labels for `SystemEventType::ContractResult`.
///
/// The `system.log` schema stores these as strings for compatibility with JSON
/// consumers (SSE, metrics aggregation). Prefer this enum when emitting or
/// matching on status values to avoid stringly-typed drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContractResultStatusLabel {
    Passed,
    Failed,
    Pending,
    Healthy,
}

impl ContractResultStatusLabel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Pending => "pending",
            Self::Healthy => "healthy",
        }
    }
}

impl std::fmt::Display for ContractResultStatusLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ContractResultStatusLabel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            "pending" => Ok(Self::Pending),
            "healthy" => Ok(Self::Healthy),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "lifecycle_event", rename_all = "snake_case")]
pub enum StageLifecycleEvent {
    Running,
    Draining {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<StageMetricsSnapshot>,
    },
    Drained,
    Completed {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<StageMetricsSnapshot>,
    },
    /// Stage terminated due to an intentional stop/cancel request.
    ///
    /// This is distinct from `Failed`: cancellation is user/operator initiated and
    /// should not be treated as an unexpected error by UIs.
    Cancelled {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<StageMetricsSnapshot>,
    },
    Failed {
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        recoverable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<StageMetricsSnapshot>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "pipeline_event", rename_all = "snake_case")]
pub enum PipelineLifecycleEvent {
    Starting,
    Running {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_count: Option<usize>,
    },
    /// Stop has been requested by an external control plane (UI/API/signal).
    ///
    /// `mode` is informational for UIs; the runtime uses internal stop intent to
    /// coordinate behaviour.
    StopRequested {
        mode: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<DurationMs>,
    },
    Draining {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
    },
    AllStagesCompleted {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
    },
    Drained,
    Completed {
        duration_ms: DurationMs,
        metrics: FlowLifecycleMetricsSnapshot,
    },
    Failed {
        reason: String,
        duration_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_cause: Option<crate::event::types::ViolationCause>,
    },
    /// Pipeline terminated due to an intentional stop/cancel request.
    ///
    /// This is distinct from `Failed`: cancellation is user/operator initiated and
    /// should not be treated as an unexpected error by UIs.
    Cancelled {
        reason: String,
        duration_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_cause: Option<crate::event::types::ViolationCause>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "replay_event", rename_all = "snake_case")]
pub enum ReplayLifecycleEvent {
    Started {
        archive_path: PathBuf,
        archive_flow_id: String,
        archive_status: ArchiveStatus,
        archive_status_derivation: StatusDerivation,
        allow_incomplete: bool,
        source_stages: Vec<String>,
    },
    Completed {
        replayed_count: Count,
        skipped_count: Count,
        duration_ms: DurationMs,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metrics_event", rename_all = "snake_case")]
pub enum MetricsCoordinationEvent {
    Ready,
    DrainRequested,
    Drained,
    Shutdown,
    Exported { watermark: VectorClock },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StageActivity {
    /// Supervisor is polling for events (dispatch loop is running normally).
    Polling,
    /// Handler is processing an event.
    Processing {
        event_id: EventId,
        elapsed_ms: DurationMs,
    },
    /// Stage is draining.
    Draining,
    /// Stage has completed.
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeLivenessState {
    /// Edge is healthy: data is flowing or the edge is idle within expected bounds.
    Healthy,
    /// Edge is idle: no data observed recently, but the stage is alive.
    Idle,
    /// Edge is suspect: no data and no heartbeat response within the warning threshold.
    Suspect,
    /// Edge appears stalled: handler may be hung or upstream may be down.
    Stalled,
    /// Edge has recovered from a previous non-healthy state.
    Recovered,
}

impl SystemEvent {
    /// Create a new system event
    pub fn new(writer_id: WriterId, event: SystemEventType) -> Self {
        Self {
            id: EventId::new(),
            writer_id,
            event,
            timestamp: current_timestamp(),
        }
    }

    /// Helper for stages to create lifecycle events
    pub fn stage_running(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    /// Helper for stages to create completed events
    pub fn stage_completed(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { metrics: None },
            },
        )
    }

    /// Helper for stages to create cancelled events
    pub fn stage_cancelled(stage_id: StageId, reason: String) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    metrics: None,
                },
            },
        )
    }

    /// Helper for stages to create failed events
    pub fn stage_failed(stage_id: StageId, error: String, recoverable: bool) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    metrics: None,
                },
            },
        )
    }

    /// Helper for stages to create draining events with metrics
    pub fn stage_draining_with_metrics(stage_id: StageId, metrics: StageMetricsSnapshot) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining {
                    metrics: Some(metrics),
                },
            },
        )
    }

    /// Helper for stages to create completed events with metrics
    pub fn stage_completed_with_metrics(stage_id: StageId, metrics: StageMetricsSnapshot) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed {
                    metrics: Some(metrics),
                },
            },
        )
    }

    /// Helper for stages to create failed events with metrics
    pub fn stage_failed_with_metrics(
        stage_id: StageId,
        error: String,
        recoverable: bool,
        metrics: StageMetricsSnapshot,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    metrics: Some(metrics),
                },
            },
        )
    }

    /// Helper for stages to create cancelled events with metrics
    pub fn stage_cancelled_with_metrics(
        stage_id: StageId,
        reason: String,
        metrics: StageMetricsSnapshot,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    metrics: Some(metrics),
                },
            },
        )
    }
}

/// Get current timestamp in milliseconds since epoch
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Factory for creating SystemEvents with proper conventions
pub struct SystemEventFactory {
    writer_id: WriterId,
}

impl SystemEventFactory {
    /// Create a new factory for system events
    pub fn new(system_id: SystemId) -> Self {
        Self {
            writer_id: WriterId::from(system_id),
        }
    }

    // === Stage Lifecycle Events ===

    pub fn stage_running(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    pub fn stage_draining(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining { metrics: None },
            },
        )
    }

    pub fn stage_drained(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Drained,
            },
        )
    }

    pub fn stage_completed(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { metrics: None },
            },
        )
    }

    pub fn stage_failed(&self, stage_id: StageId, error: String, recoverable: bool) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    metrics: None,
                },
            },
        )
    }

    pub fn stage_cancelled(&self, stage_id: StageId, reason: String) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    metrics: None,
                },
            },
        )
    }

    /// Contract status summary emitted by readers/subscribers (per upstream)
    pub fn contract_status(
        &self,
        upstream: StageId,
        reader: StageId,
        pass: bool,
        reader_seq: Option<crate::event::types::SeqNo>,
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        reason: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::ContractStatus {
                upstream,
                reader,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            },
        )
    }

    // === Pipeline Lifecycle Events ===

    pub fn pipeline_starting(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Starting),
        )
    }

    pub fn pipeline_running(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Running {
                stage_count: None,
            }),
        )
    }

    pub fn pipeline_stop_requested(
        &self,
        mode: String,
        timeout_ms: Option<DurationMs>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::StopRequested {
                mode,
                timeout_ms,
            }),
        )
    }

    pub fn pipeline_all_stages_completed(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::AllStagesCompleted {
                metrics: None,
            }),
        )
    }

    pub fn pipeline_draining(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Draining { metrics: None }),
        )
    }

    pub fn pipeline_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Drained),
        )
    }

    pub fn pipeline_completed(
        &self,
        duration_ms: DurationMs,
        metrics: FlowLifecycleMetricsSnapshot,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                duration_ms,
                metrics,
            }),
        )
    }

    pub fn pipeline_failed(
        &self,
        reason: String,
        duration_ms: DurationMs,
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        failure_cause: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            }),
        )
    }

    pub fn pipeline_cancelled(
        &self,
        reason: String,
        duration_ms: DurationMs,
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        failure_cause: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Cancelled {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            }),
        )
    }

    // === Metrics Coordination Events ===

    pub fn metrics_ready(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Ready),
        )
    }

    pub fn metrics_drain_requested(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::DrainRequested),
        )
    }

    pub fn metrics_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained),
        )
    }

    pub fn metrics_shutdown(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Shutdown),
        )
    }
}

// Implement JournalEvent for SystemEvent
use crate::event::journal_event::{JournalEvent, Sealed};

// Implement the sealed trait first
impl Sealed for SystemEvent {}

impl JournalEvent for SystemEvent {
    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    fn event_type_name(&self) -> &str {
        match &self.event {
            SystemEventType::StageLifecycle { event, .. } => match event {
                StageLifecycleEvent::Running => "system.stage.running",
                StageLifecycleEvent::Draining { .. } => "system.stage.draining",
                StageLifecycleEvent::Drained => "system.stage.drained",
                StageLifecycleEvent::Completed { .. } => "system.stage.completed",
                StageLifecycleEvent::Failed { .. } => "system.stage.failed",
                StageLifecycleEvent::Cancelled { .. } => "system.stage.cancelled",
            },
            SystemEventType::PipelineLifecycle(event) => match event {
                PipelineLifecycleEvent::Starting => "system.pipeline.starting",
                PipelineLifecycleEvent::Running { .. } => "system.pipeline.running",
                PipelineLifecycleEvent::StopRequested { .. } => "system.pipeline.stop_requested",
                PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                    "system.pipeline.all_stages_completed"
                }
                PipelineLifecycleEvent::Draining { .. } => "system.pipeline.draining",
                PipelineLifecycleEvent::Drained => "system.pipeline.drained",
                PipelineLifecycleEvent::Completed { .. } => "system.pipeline.completed",
                PipelineLifecycleEvent::Failed { .. } => "system.pipeline.failed",
                PipelineLifecycleEvent::Cancelled { .. } => "system.pipeline.cancelled",
            },
            SystemEventType::ReplayLifecycle(event) => match event {
                ReplayLifecycleEvent::Started { .. } => "system.replay.started",
                ReplayLifecycleEvent::Completed { .. } => "system.replay.completed",
            },
            SystemEventType::MetricsCoordination(event) => match event {
                MetricsCoordinationEvent::Ready => "system.metrics.ready",
                MetricsCoordinationEvent::DrainRequested => "system.metrics.drain_requested",
                MetricsCoordinationEvent::Drained => "system.metrics.drained",
                MetricsCoordinationEvent::Shutdown => "system.metrics.shutdown",
                MetricsCoordinationEvent::Exported { .. } => "system.metrics.exported",
            },
            SystemEventType::StageHeartbeat { .. } => "system.stage.heartbeat",
            SystemEventType::EdgeLiveness { .. } => "system.edge.liveness",
            SystemEventType::MiddlewareLifecycle { .. } => "system.middleware.lifecycle",
            SystemEventType::ContractStatus { pass, .. } => {
                if *pass {
                    "system.contract.pass"
                } else {
                    "system.contract.fail"
                }
            }
            SystemEventType::ContractResult { status, .. } => match status.as_str() {
                s if s == ContractResultStatusLabel::Passed.as_str() => {
                    "system.contract.result.passed"
                }
                s if s == ContractResultStatusLabel::Failed.as_str() => {
                    "system.contract.result.failed"
                }
                s if s == ContractResultStatusLabel::Pending.as_str() => {
                    "system.contract.result.pending"
                }
                _ => "system.contract.result",
            },
            SystemEventType::ContractOverrideByPolicy { .. } => {
                "system.contract.override_by_policy"
            }
            SystemEventType::HttpSurfaceSnapshot { .. } => "system.http_surface.snapshot",
        }
    }
}
