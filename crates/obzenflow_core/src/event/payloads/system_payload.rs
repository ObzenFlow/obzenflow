// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration payloads and their descriptors.

use crate::event::types::DurationMs;
use crate::event::vector_clock::VectorClock;
use crate::id::{StageId, StageKey};
use crate::ingress::{IngressAttemptSeq, IngressKey, IngressRefusalReason};
use crate::metrics::FlowLifecycleMetricsSnapshot;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Contract label carried by canonical execution facts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContractName(String);

impl ContractName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContractName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for ContractName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ContractName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Logical feed role carried by canonical execution facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemFeedRole {
    Input,
    Reference,
    Stream,
}

impl SystemFeedRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Reference => "reference",
            Self::Stream => "stream",
        }
    }
}

impl std::fmt::Display for SystemFeedRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SystemFeedRole {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "input" => Ok(Self::Input),
            "reference" => Ok(Self::Reference),
            "stream" => Ok(Self::Stream),
            _ => Err(()),
        }
    }
}

/// Why a supervisor did not execute a command accepted before mailbox closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandDiscardDisposition {
    /// A queued lifecycle or cancellation command became obsolete at termination.
    ObsoleteControl,
    /// An error was still queued after the terminal outcome had been selected.
    UnexpectedError,
}

/// Types of system events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "system_event_type", rename_all = "snake_case")]
pub enum SystemPayload {
    /// Published when the system supervisor FSM selects registration.
    /// The envelope's writer identifies the registered supervisor instance.
    SupervisorRegistered {
        descriptor: super::supervisor_descriptor::SupervisorDescriptor,
    },
    /// Pipeline lifecycle events
    #[serde(rename = "pipeline_lifecycle")]
    PipelineLifecycle(PipelineLifecycleEvent),

    /// Metrics subsystem coordination
    #[serde(rename = "metrics_coordination")]
    MetricsCoordination(MetricsCoordinationEvent),

    /// Durable hosted-ingress refusal fact (FLOWIP-115d).
    ///
    /// A rejected or shed submission attempt is a domain fact, so the hosted
    /// endpoint appends one of these to `system.log` before returning the
    /// protocol refusal, and the metrics aggregator projects the per-`(ingress_key,
    /// reason)` refusal count from it (`state = fold(facts)`). It records an ingress admission outcome. The `attempt_seq` is the cross-journal merge key with
    /// accepted source rows. It carries no raw body or credential-bearing header.
    #[serde(rename = "ingress_refusal")]
    IngressRefusal {
        /// Protocol-neutral hosted ingress key; the per-surface metric projection key.
        ingress_key: IngressKey,
        /// Runtime id of the linked source stage.
        stage_id: StageId,
        /// Replay-stable source stage key (`run_manifest.json` key).
        stage_key: StageKey,
        reason: IngressRefusalReason,
        /// Per-attempt sequence; the merge key against accepted source rows.
        attempt_seq: IngressAttemptSeq,
        /// HTTP submission requests in this attempt (always 1 in 115D).
        request_count: u64,
        /// Events refused by this attempt (1 for `/events`; the refused subset
        /// size for `/batch`, so a batch refusal is one fact with a count).
        event_count: u64,
        /// Batches in this attempt (0 for `/events`, 1 for `/batch`).
        batch_count: u64,
        http_status: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms_bucket: Option<u64>,
    },
}

/// Stable status labels for canonical execution contract results.
///
/// The `system.log` schema stores these as strings for compatibility with JSON
/// consumers (SSE, metrics aggregation). Prefer this enum when emitting or
/// matching on status values to avoid stringly-typed drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractResultStatusLabel {
    Passed,
    Failed,
    Pending,
    Skipped,
}

impl ContractResultStatusLabel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Pending => "pending",
            Self::Skipped => "skipped",
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
            "skipped" => Ok(Self::Skipped),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "pipeline_event", rename_all = "snake_case")]
pub enum PipelineLifecycleEvent {
    Starting,
    ReadyForRun {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_count: Option<usize>,
    },
    Running {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_count: Option<usize>,
    },
    /// Runtime admitted this intent; publication may follow admission later.
    StopAdmitted {
        admission: PipelineStopAdmission,
    },
    /// Teardown completed before source execution started.
    NotStarted,
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

/// Durable stop admission. Runtime monotonic deadlines are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PipelineStopAdmission {
    Graceful { timeout_ms: DurationMs },
    Cancel { cause: PipelineCancellationCause },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineCancellationCause {
    Requested,
    GracefulTimeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metrics_event", rename_all = "snake_case")]
pub enum MetricsCoordinationEvent {
    Ready,
    DrainRequested,
    /// Available metrics buffer published and owned refresh readers stopped.
    /// This is not a physical journal-coverage certificate.
    Drained,
    Shutdown,
    /// Positions of selected current carriers. Stage keys refer to the bound
    /// stage writer's data journal; error and foreign carriers cannot advance
    /// those positions. Neither Exported nor Drained certifies history coverage.
    Exported {
        watermark: VectorClock,
    },
}

impl SystemPayload {
    pub const SCHEMA_VERSION: std::num::NonZeroU32 = std::num::NonZeroU32::MIN;

    pub fn event_type(&self) -> std::borrow::Cow<'static, str> {
        let name = match self {
            SystemPayload::SupervisorRegistered { descriptor } => {
                return format!("{}.registered", descriptor.event_prefix()).into()
            }
            SystemPayload::PipelineLifecycle(event) => match event {
                PipelineLifecycleEvent::Starting => {
                    "supervisor.runtime.pipeline_supervisor.command.start.admitted"
                }
                PipelineLifecycleEvent::ReadyForRun { .. } => {
                    "supervisor.runtime.pipeline_supervisor.milestone.ready_for_run"
                }
                PipelineLifecycleEvent::Running { .. } => {
                    "supervisor.runtime.pipeline_supervisor.milestone.sources_started"
                }
                PipelineLifecycleEvent::StopAdmitted { admission } => match admission {
                    PipelineStopAdmission::Graceful { .. } => {
                        "supervisor.runtime.pipeline_supervisor.command.graceful_stop.admitted"
                    }
                    PipelineStopAdmission::Cancel { .. } => {
                        "supervisor.runtime.pipeline_supervisor.command.cancel.admitted"
                    }
                },
                PipelineLifecycleEvent::NotStarted => {
                    "supervisor.runtime.pipeline_supervisor.outcome.not_started"
                }
                PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                    "supervisor.runtime.pipeline_supervisor.milestone.all_stages_completed"
                }
                PipelineLifecycleEvent::Draining { .. } => {
                    "supervisor.runtime.pipeline_supervisor.milestone.drain_started"
                }
                PipelineLifecycleEvent::Drained => {
                    "supervisor.runtime.pipeline_supervisor.milestone.final_marker_published"
                }
                PipelineLifecycleEvent::Completed { .. } => {
                    "supervisor.runtime.pipeline_supervisor.outcome.completed"
                }
                PipelineLifecycleEvent::Failed { .. } => {
                    "supervisor.runtime.pipeline_supervisor.outcome.failed"
                }
                PipelineLifecycleEvent::Cancelled { .. } => {
                    "supervisor.runtime.pipeline_supervisor.outcome.cancelled"
                }
            },
            SystemPayload::MetricsCoordination(event) => match event {
                MetricsCoordinationEvent::Ready => {
                    "supervisor.runtime.metrics_aggregator.milestone.ready"
                }
                MetricsCoordinationEvent::DrainRequested => {
                    "supervisor.runtime.pipeline_supervisor.command.finalize_metrics.requested"
                }
                MetricsCoordinationEvent::Drained => {
                    "supervisor.runtime.metrics_aggregator.finalization.completed"
                }
                MetricsCoordinationEvent::Shutdown => {
                    "supervisor.runtime.metrics_aggregator.milestone.refresh_readers_stopped"
                }
                MetricsCoordinationEvent::Exported { .. } => {
                    "supervisor.runtime.metrics_aggregator.snapshot.published"
                }
            },
            SystemPayload::IngressRefusal { .. } => "system.ingress.refusal",
        };
        name.into()
    }
}
