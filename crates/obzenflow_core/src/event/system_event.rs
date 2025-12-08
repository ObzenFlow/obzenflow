//! System orchestration events (written to control journal)

use crate::event::types::{EventId, WriterId};
use crate::id::{StageId, SystemId};
use crate::metrics::{FlowLifecycleMetricsSnapshot, StageMetricsSnapshot};
use serde::{Deserialize, Serialize};

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

    /// Contract decision overridden by a policy layer (e.g., breaker-aware).
    #[serde(rename = "contract_override_by_policy")]
    ContractOverrideByPolicy {
        upstream: StageId,
        reader: StageId,
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
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metrics_event", rename_all = "snake_case")]
pub enum MetricsCoordinationEvent {
    Ready,
    DrainRequested,
    Drained,
    Shutdown,
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
    pub fn stage_completed_with_metrics(
        stage_id: StageId,
        metrics: StageMetricsSnapshot,
    ) -> Self {
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
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                reason,
                duration_ms,
                metrics,
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
            },
            SystemEventType::PipelineLifecycle(event) => match event {
                PipelineLifecycleEvent::Starting => "system.pipeline.starting",
                PipelineLifecycleEvent::Running { .. } => "system.pipeline.running",
                PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                    "system.pipeline.all_stages_completed"
                }
                PipelineLifecycleEvent::Draining { .. } => "system.pipeline.draining",
                PipelineLifecycleEvent::Drained => "system.pipeline.drained",
                PipelineLifecycleEvent::Completed { .. } => "system.pipeline.completed",
                PipelineLifecycleEvent::Failed { .. } => "system.pipeline.failed",
            },
            SystemEventType::MetricsCoordination(event) => match event {
                MetricsCoordinationEvent::Ready => "system.metrics.ready",
                MetricsCoordinationEvent::DrainRequested => "system.metrics.drain_requested",
                MetricsCoordinationEvent::Drained => "system.metrics.drained",
                MetricsCoordinationEvent::Shutdown => "system.metrics.shutdown",
            },
            SystemEventType::ContractStatus { pass, .. } => {
                if *pass {
                    "system.contract.pass"
                } else {
                    "system.contract.fail"
                }
            }
            SystemEventType::ContractOverrideByPolicy { .. } => {
                "system.contract.override_by_policy"
            }
        }
    }
}
