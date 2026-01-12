//! System orchestration events (written to control journal)

use crate::event::payloads::observability_payload::MiddlewareLifecycle;
use crate::event::types::{EventId, WriterId};
use crate::event::vector_clock::VectorClock;
use crate::id::{StageId, SystemId};
use crate::metrics::{FlowLifecycleMetricsSnapshot, StageMetricsSnapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareEventOrigin {
    pub event_id: EventId,
    pub writer_key: String,
    pub seq: u64,
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

    /// Metrics subsystem coordination
    #[serde(rename = "metrics_coordination")]
    MetricsCoordination(MetricsCoordinationEvent),

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
        /// "passed", "failed", or "pending"
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
        timeout_ms: Option<u64>,
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
        duration_ms: u64,
        metrics: FlowLifecycleMetricsSnapshot,
    },
    Failed {
        reason: String,
        duration_ms: u64,
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
        duration_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_cause: Option<crate::event::types::ViolationCause>,
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

    pub fn pipeline_stop_requested(&self, mode: String, timeout_ms: Option<u64>) -> SystemEvent {
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
        duration_ms: u64,
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
        duration_ms: u64,
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
        duration_ms: u64,
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
            SystemEventType::MetricsCoordination(event) => match event {
                MetricsCoordinationEvent::Ready => "system.metrics.ready",
                MetricsCoordinationEvent::DrainRequested => "system.metrics.drain_requested",
                MetricsCoordinationEvent::Drained => "system.metrics.drained",
                MetricsCoordinationEvent::Shutdown => "system.metrics.shutdown",
                MetricsCoordinationEvent::Exported { .. } => "system.metrics.exported",
            },
            SystemEventType::MiddlewareLifecycle { .. } => "system.middleware.lifecycle",
            SystemEventType::ContractStatus { pass, .. } => {
                if *pass {
                    "system.contract.pass"
                } else {
                    "system.contract.fail"
                }
            }
            SystemEventType::ContractResult { status, .. } => match status.as_str() {
                "passed" => "system.contract.result.passed",
                "failed" => "system.contract.result.failed",
                "pending" => "system.contract.result.pending",
                _ => "system.contract.result",
            },
            SystemEventType::ContractOverrideByPolicy { .. } => {
                "system.contract.override_by_policy"
            }
        }
    }
}
