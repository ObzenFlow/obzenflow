// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration events (written to control journal)

use crate::event::context::ExecutionAccounting;
use crate::event::journal_record::JournalPayload;
use crate::event::payloads::chain_payload::EventKind;
use crate::event::payloads::execution_payload::MiddlewareFact;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::event::provenance::{AuthoredEnvelope, SystemEventProvenance};
use crate::event::types::{Count, DurationMs, EventId, EventType, SeqNo, WriterId};
use crate::event::vector_clock::VectorClock;
use crate::id::{StageId, StageKey, SystemId};
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

/// An authored system record, without journal commitment provenance.
#[derive(Debug, Clone)]
pub struct SystemEvent {
    pub envelope: AuthoredEnvelope<SystemEventProvenance>,
    pub payload: SystemPayload,
}

impl std::ops::Deref for SystemEvent {
    type Target = SystemEventProvenance;
    fn deref(&self) -> &Self::Target {
        &self.envelope.provenance.event
    }
}
impl std::ops::DerefMut for SystemEvent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.envelope.provenance.event
    }
}

impl Serialize for SystemEvent {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeStruct};
        JournalPayload::validate(&self.payload, &self.envelope.provenance.event)
            .map_err(S::Error::custom)?;
        let mut record = serializer.serialize_struct("SystemEvent", 2)?;
        record.serialize_field("envelope", &self.envelope)?;
        record.serialize_field("payload", &self.payload)?;
        record.end()
    }
}
impl<'de> Deserialize<'de> for SystemEvent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = crate::event::record_serde::deserialize::<
            _,
            AuthoredEnvelope<SystemEventProvenance>,
            SystemPayload,
        >(deserializer)?;
        JournalPayload::validate(&raw.payload, &raw.envelope.provenance.event)
            .map_err(D::Error::custom)?;
        Ok(Self {
            envelope: raw.envelope,
            payload: raw.payload,
        })
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
    Drained,
    Shutdown,
    /// Stage keys cover only the bound stage writer's sequentially folded
    /// data-journal component. Error rails and archived/foreign writers are
    /// not represented as stage-data coverage. System keys cover system facts.
    /// Complete physical observation requires successful metrics Drained.
    Exported {
        watermark: VectorClock,
    },
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
    /// The canonical deterministic merge is waiting on a quiet input
    /// (FLOWIP-095d). This is idle-by-rule, never hung: an ordered fan-in
    /// delivers nothing while any non-exhausted input has no head. `upstream`
    /// names an input being waited on so operators can debug rate coupling.
    WaitingOnQuietInput { upstream: Option<StageId> },
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
    pub fn new(writer_id: WriterId, event: SystemPayload) -> Self {
        use crate::event::provenance::{
            AuthoredEnvelope, AuthoredProvenance, SystemEventProvenance,
        };
        let provenance = SystemEventProvenance {
            id: EventId::new(),
            writer_id,
            event_kind: EventKind::System,
            event_type: event.event_type().to_string(),
            timestamp: current_timestamp(),
        };
        Self {
            envelope: AuthoredEnvelope {
                provenance: AuthoredProvenance { event: provenance },
                observability: None,
            },
            payload: event,
        }
    }

    /// Helper for stages to create lifecycle events
    pub fn stage_running(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    /// Helper for stages to create completed events
    pub fn stage_completed(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { accounting: None },
            },
        )
    }

    /// Helper for stages to create cancelled events
    pub fn stage_cancelled(stage_id: StageId, reason: String) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: None,
                },
            },
        )
    }

    /// Helper for stages to create failed events
    pub fn stage_failed(stage_id: StageId, error: String, recoverable: bool) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: None,
                    causal_event_id: None,
                },
            },
        )
    }

    /// Helper for stages to create draining events with metrics
    pub fn stage_draining_with_accounting(
        stage_id: StageId,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining {
                    accounting: Some(accounting),
                },
            },
        )
    }

    /// Helper for stages to create completed events with metrics
    pub fn stage_completed_with_accounting(
        stage_id: StageId,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed {
                    accounting: Some(accounting),
                },
            },
        )
    }

    /// Helper for stages to create failed events with metrics
    pub fn stage_failed_with_accounting(
        stage_id: StageId,
        error: String,
        recoverable: bool,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: Some(accounting),
                    causal_event_id: None,
                },
            },
        )
    }

    /// Construct correctness-bearing failed lifecycle evidence causally linked
    /// to the final chain event in a sink failure sequence.
    pub fn stage_failed_with_accounting_causal(
        stage_id: StageId,
        error: String,
        recoverable: bool,
        accounting: ExecutionAccounting,
        causal_event_id: EventId,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: Some(accounting),
                    causal_event_id: Some(causal_event_id),
                },
            },
        )
    }

    /// Helper for stages to create cancelled events with metrics
    pub fn stage_cancelled_with_accounting(
        stage_id: StageId,
        reason: String,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: Some(accounting),
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
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    pub fn stage_draining(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining { accounting: None },
            },
        )
    }

    pub fn stage_drained(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Drained,
            },
        )
    }

    pub fn stage_completed(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { accounting: None },
            },
        )
    }

    pub fn stage_failed(&self, stage_id: StageId, error: String, recoverable: bool) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: None,
                    causal_event_id: None,
                },
            },
        )
    }

    pub fn stage_cancelled(&self, stage_id: StageId, reason: String) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: None,
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
            SystemPayload::ContractStatus {
                upstream,
                reader,
                selected_event_type: None,
                feed_role: None,
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
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Starting),
        )
    }

    pub fn pipeline_running(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running { stage_count: None }),
        )
    }

    pub fn pipeline_ready_for_run(&self, stage_count: Option<usize>) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::ReadyForRun { stage_count }),
        )
    }

    pub fn pipeline_stop_admitted(&self, admission: PipelineStopAdmission) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted { admission }),
        )
    }

    pub fn pipeline_not_started(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::NotStarted),
        )
    }

    pub fn pipeline_all_stages_completed(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::AllStagesCompleted {
                metrics: None,
            }),
        )
    }

    pub fn pipeline_draining(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Draining { metrics: None }),
        )
    }

    pub fn pipeline_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
        )
    }

    pub fn pipeline_completed(
        &self,
        duration_ms: DurationMs,
        metrics: FlowLifecycleMetricsSnapshot,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed {
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
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed {
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
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Cancelled {
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
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Ready),
        )
    }

    pub fn metrics_drain_requested(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::DrainRequested),
        )
    }

    pub fn metrics_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Drained),
        )
    }

    pub fn metrics_shutdown(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Shutdown),
        )
    }
}

// Implement JournalEvent for SystemEvent
use crate::event::journal_event::{JournalEvent, Sealed};

// Implement the sealed trait first
impl Sealed for SystemEvent {}

impl JournalEvent for SystemEvent {
    type Payload = SystemPayload;
    fn into_parts(self) -> (AuthoredEnvelope<SystemEventProvenance>, Self::Payload) {
        (self.envelope, self.payload)
    }
    fn from_parts(
        envelope: AuthoredEnvelope<SystemEventProvenance>,
        payload: Self::Payload,
    ) -> Self {
        Self { envelope, payload }
    }

    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    fn event_type_name(&self) -> &str {
        self.payload.event_type()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StageId;
    use serde_json::json;

    #[test]
    fn lifecycle_admission_has_one_typed_schema_and_rejects_the_old_event() {
        for (admission, expected) in [
            (
                PipelineStopAdmission::Graceful {
                    timeout_ms: DurationMs(125),
                },
                json!({"mode": "graceful", "timeout_ms": 125}),
            ),
            (
                PipelineStopAdmission::Cancel {
                    cause: PipelineCancellationCause::Requested,
                },
                json!({"mode": "cancel", "cause": "requested"}),
            ),
            (
                PipelineStopAdmission::Cancel {
                    cause: PipelineCancellationCause::GracefulTimeout,
                },
                json!({"mode": "cancel", "cause": "graceful_timeout"}),
            ),
        ] {
            let payload = serde_json::to_value(PipelineLifecycleEvent::StopAdmitted {
                admission: admission.clone(),
            })
            .unwrap();
            assert_eq!(
                payload,
                json!({"pipeline_event": "stop_admitted", "admission": expected})
            );
            assert!(
                matches!(serde_json::from_value::<PipelineLifecycleEvent>(payload).unwrap(), PipelineLifecycleEvent::StopAdmitted { admission: decoded } if decoded == admission)
            );
        }
        assert_eq!(
            serde_json::to_value(PipelineLifecycleEvent::NotStarted).unwrap(),
            json!({"pipeline_event": "not_started"})
        );
        for obsolete in [
            json!({"pipeline_event": "stop_requested", "mode": "cancel"}),
            json!({"pipeline_event": "stop_admitted", "admission": {"mode": "cancel"}}),
            json!({"pipeline_event": "stop_admitted", "admission": {"mode": "graceful"}}),
        ] {
            assert!(serde_json::from_value::<PipelineLifecycleEvent>(obsolete).is_err());
        }
    }

    #[test]
    fn contract_result_feed_fields_are_typed_but_serialize_as_labels() {
        let payload = SystemPayload::ContractResult {
            upstream: StageId::new(),
            reader: StageId::new(),
            selected_event_type: Some(EventType::from("test.selected.v1")),
            feed_role: Some(SystemFeedRole::Reference),
            contract_name: ContractName::from("TransportContract"),
            status: ContractResultStatusLabel::Healthy,
            cause: None,
            reader_seq: Some(SeqNo(3)),
            advertised_writer_seq: Some(SeqNo(5)),
        };

        let serialized = serde_json::to_value(&payload).expect("system event should serialize");
        assert_eq!(serialized["selected_event_type"], "test.selected.v1");
        assert_eq!(serialized["feed_role"], "reference");
        assert_eq!(serialized["contract_name"], "TransportContract");
        assert_eq!(serialized["status"], "healthy");

        let decoded: SystemPayload = serde_json::from_value(json!({
            "system_event_type": "contract_result",
            "upstream": serialized["upstream"].clone(),
            "reader": serialized["reader"].clone(),
            "selected_event_type": "test.selected.v1",
            "feed_role": "reference",
            "contract_name": "TransportContract",
            "status": "healthy",
            "reader_seq": 3,
            "advertised_writer_seq": 5
        }))
        .expect("string-label system event should deserialize");

        match decoded {
            SystemPayload::ContractResult {
                selected_event_type,
                feed_role,
                contract_name,
                status,
                ..
            } => {
                assert_eq!(
                    selected_event_type,
                    Some(EventType::from("test.selected.v1"))
                );
                assert_eq!(feed_role, Some(SystemFeedRole::Reference));
                assert_eq!(contract_name.as_str(), "TransportContract");
                assert_eq!(status, ContractResultStatusLabel::Healthy);
            }
            other => panic!("expected ContractResult, got {other:?}"),
        }
    }
}
