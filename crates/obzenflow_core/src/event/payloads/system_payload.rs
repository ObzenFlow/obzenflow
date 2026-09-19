// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration payloads and their descriptors.

use crate::event::payloads::execution_payload::MiddlewareFact;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::event::provenance::ExecutionAccounting;
use crate::event::types::{Count, DurationMs, EventId, EventType, SeqNo};
use crate::event::vector_clock::VectorClock;
use crate::id::{StageId, StageKey};
use crate::ingress::{IngressAttemptSeq, IngressKey, IngressRefusalReason};
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::metrics::FlowLifecycleMetricsSnapshot;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareEventOrigin {
    pub event_id: EventId,
    pub writer_key: String,
    pub seq: SeqNo,
}

/// Contract label carried by contract system events.
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

/// Logical feed role carried by contract system events.
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
    /// A terminal supervisor closed its mailbox without executing this accepted
    /// command. The envelope's writer identifies the supervisor's stage. This
    /// records the disposition without replacing the existing terminal outcome.
    SupervisorCommandDiscarded {
        supervisor: String,
        terminal_state: String,
        command: String,
        disposition: CommandDiscardDisposition,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Best-effort async source cleanup failed after the stage entered live
    /// execution (FLOWIP-134g). Cleanup never authors data and never delays a
    /// terminal transition.
    #[serde(rename = "source_cleanup_failed")]
    SourceCleanupFailed {
        stage_id: StageId,
        stage_name: String,
        error: String,
    },
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
        middleware: MiddlewareFact,
    },

    /// Contract status reported by a reader/subscriber (per upstream)
    #[serde(rename = "contract_status")]
    ContractStatus {
        upstream: StageId,
        reader: StageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selected_event_type: Option<EventType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feed_role: Option<SystemFeedRole>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selected_event_type: Option<EventType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feed_role: Option<SystemFeedRole>,
        contract_name: ContractName,
        status: ContractResultStatusLabel,
        /// Stable category label (e.g. "seq_divergence", "content_mismatch", "other")
        #[serde(skip_serializing_if = "Option::is_none")]
        cause: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
    },

    /// Durable hosted-ingress refusal fact (FLOWIP-115d).
    ///
    /// A rejected or shed submission attempt is a domain fact, so the hosted
    /// endpoint appends one of these to `system.log` before returning the
    /// protocol refusal, and the metrics aggregator projects the per-`(ingress_key,
    /// reason)` refusal count from it (`state = fold(facts)`). It is a dedicated
    /// variant rather than the `MiddlewareLifecycle` family because `EdgeShed`
    /// and `Validation` refusals are infra-originated admission outcomes, not
    /// middleware decisions. The `attempt_seq` is the cross-journal merge key with
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

/// Stable status labels for `SystemPayload::ContractResult`.
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
        accounting: Option<ExecutionAccounting>,
    },
    Drained,
    Completed {
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
    },
    /// Stage terminated due to an intentional stop/cancel request.
    ///
    /// This is distinct from `Failed`: cancellation is user/operator initiated and
    /// should not be treated as an unexpected error by UIs.
    Cancelled {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
    },
    Failed {
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        recoverable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        causal_event_id: Option<EventId>,
    },
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
        /// The terminal EOF kind synthesized at exhaustion (FLOWIP-095k).
        /// `None` on the resume handoff, which synthesizes no terminal EOF.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        synthesized_eof_kind: Option<EofKind>,
    },
    /// Resume handoff (FLOWIP-120n): the source finished its catch-up and
    /// continues live at `generation`. The transition announcement the
    /// presentation layer surfaces.
    ResumedLive {
        archive_flow_id: String,
        replayed_count: Count,
        generation: u64,
    },
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
    pub fn event_type(&self) -> &'static str {
        match self {
            SystemPayload::SupervisorCommandDiscarded { .. } => {
                "system.supervisor.command_discarded"
            }
            SystemPayload::SourceCleanupFailed { .. } => "system.source.cleanup_failed",
            SystemPayload::StageLifecycle { event, .. } => match event {
                StageLifecycleEvent::Running => "system.stage.running",
                StageLifecycleEvent::Draining { .. } => "system.stage.draining",
                StageLifecycleEvent::Drained => "system.stage.drained",
                StageLifecycleEvent::Completed { .. } => "system.stage.completed",
                StageLifecycleEvent::Failed { .. } => "system.stage.failed",
                StageLifecycleEvent::Cancelled { .. } => "system.stage.cancelled",
            },
            SystemPayload::PipelineLifecycle(event) => match event {
                PipelineLifecycleEvent::Starting => "system.pipeline.starting",
                PipelineLifecycleEvent::ReadyForRun { .. } => "system.pipeline.ready_for_run",
                PipelineLifecycleEvent::Running { .. } => "system.pipeline.running",
                PipelineLifecycleEvent::StopAdmitted { .. } => "system.pipeline.stop_admitted",
                PipelineLifecycleEvent::NotStarted => "system.pipeline.not_started",
                PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                    "system.pipeline.all_stages_completed"
                }
                PipelineLifecycleEvent::Draining { .. } => "system.pipeline.draining",
                PipelineLifecycleEvent::Drained => "system.pipeline.drained",
                PipelineLifecycleEvent::Completed { .. } => "system.pipeline.completed",
                PipelineLifecycleEvent::Failed { .. } => "system.pipeline.failed",
                PipelineLifecycleEvent::Cancelled { .. } => "system.pipeline.cancelled",
            },
            SystemPayload::ReplayLifecycle(event) => match event {
                ReplayLifecycleEvent::Started { .. } => "system.replay.started",
                ReplayLifecycleEvent::Completed { .. } => "system.replay.completed",
                ReplayLifecycleEvent::ResumedLive { .. } => "system.replay.resumed_live",
            },
            SystemPayload::MetricsCoordination(event) => match event {
                MetricsCoordinationEvent::Ready => "system.metrics.ready",
                MetricsCoordinationEvent::DrainRequested => "system.metrics.drain_requested",
                MetricsCoordinationEvent::Drained => "system.metrics.drained",
                MetricsCoordinationEvent::Shutdown => "system.metrics.shutdown",
                MetricsCoordinationEvent::Exported { .. } => "system.metrics.exported",
            },
            SystemPayload::MiddlewareLifecycle { .. } => "system.middleware.lifecycle",
            SystemPayload::ContractStatus { pass, .. } => {
                if *pass {
                    "system.contract.pass"
                } else {
                    "system.contract.fail"
                }
            }
            SystemPayload::ContractResult { status, .. } => match status {
                ContractResultStatusLabel::Passed => "system.contract.result.passed",
                ContractResultStatusLabel::Failed => "system.contract.result.failed",
                ContractResultStatusLabel::Pending => "system.contract.result.pending",
                ContractResultStatusLabel::Healthy => "system.contract.result",
            },
            SystemPayload::IngressRefusal { .. } => "system.ingress.refusal",
        }
    }
}
