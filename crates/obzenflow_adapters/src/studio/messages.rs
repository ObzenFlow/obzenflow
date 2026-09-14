// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! JSON payloads and event names for Studio's `/api/flow/events` stream.

use super::contracts::ContractBoundaryAlias;
use obzenflow_core::event::{
    payloads::{execution_payload::CircuitBreakerOpenTrigger, flow_control_payload::EofKind},
    system_event::{
        ContractName, ContractResultStatusLabel, EdgeLivenessState, MiddlewareEventOrigin,
        PipelineStopAdmission, SystemFeedRole,
    },
    types::{Count, DurationMs, EventType, SeqNo, ViolationCause},
    vector_clock::VectorClock,
    PipelineLifecycleEvent, ReplayLifecycleEvent, StageLifecycleEvent,
};
use obzenflow_core::journal::{ArchiveStatus, StatusDerivation};
use obzenflow_core::metrics::FlowLifecycleMetricsSnapshot;
use obzenflow_core::{web::SseFrame, EventId, StageId};
use serde::{Serialize, Serializer};
use std::path::PathBuf;

#[derive(Serialize)]
#[serde(tag = "system_event_type", rename_all = "snake_case")]
pub(super) enum StudioMessage<'a> {
    StageLifecycle {
        #[serde(serialize_with = "display")]
        stage_id: StageId,
        #[serde(flatten, with = "StageUpdate")]
        event: &'a StageLifecycleEvent,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    #[serde(rename = "pipeline_lifecycle")]
    FlowLifecycle {
        #[serde(flatten, with = "FlowUpdate")]
        event: &'a PipelineLifecycleEvent,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    ReplayLifecycle {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_id: Option<String>,
        #[serde(flatten, with = "ReplayUpdate")]
        event: &'a ReplayLifecycleEvent,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    SourceCleanupFailed {
        #[serde(serialize_with = "display")]
        stage_id: StageId,
        stage_name: &'a str,
        error: &'a str,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    MiddlewareLifecycle {
        #[serde(serialize_with = "display")]
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_name: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        flow_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        flow_name: Option<&'a str>,
        origin: &'a MiddlewareEventOrigin,
        revision: SeqNo,
        #[serde(flatten)]
        update: MiddlewareUpdate<'a>,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    #[serde(rename = "middleware_lifecycle")]
    MiddlewareMeasurements {
        #[serde(serialize_with = "display")]
        stage_id: StageId,
        #[serde(flatten)]
        update: MiddlewareUpdate<'a>,
        timestamp_ms: u64,
        capture: obzenflow_core::event::observation::CaptureStamp,
    },
    ContractStatus {
        #[serde(flatten)]
        edge: ContractEdge<'a>,
        pass: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<&'a ViolationCause>,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    ContractResult {
        #[serde(flatten)]
        edge: ContractEdge<'a>,
        contract_name: &'a ContractName,
        status: &'a ContractResultStatusLabel,
        #[serde(skip_serializing_if = "Option::is_none")]
        cause: Option<&'a str>,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    EdgeLiveness {
        #[serde(serialize_with = "display")]
        upstream_stage_id: StageId,
        #[serde(serialize_with = "display")]
        reader_stage_id: StageId,
        state: EdgeLivenessState,
        idle_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_reader_seq: Option<SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_event_id: Option<EventId>,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    MetricsCoordination {
        event_type: MetricsUpdate,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    Bootstrap {
        event_type: BootstrapUpdate,
        checkpoint_event_id: Option<EventId>,
        runtime_instance_id: Option<&'a str>,
    },
    ServerShutdown {
        runtime_instance_id: Option<&'a str>,
    },
    #[serde(untagged)]
    CompositeStatus(CompositeStatusPayloadV1),
    #[serde(untagged)]
    MiddlewareSnapshot(MiddlewareSnapshot<'a>),
    #[serde(untagged)]
    MetricsWatermark {
        watermark: &'a VectorClock,
        export_id: EventId,
        #[serde(flatten)]
        at: Observation<'a>,
    },
    #[serde(untagged)]
    Error {
        error_type: StreamErrorKind,
        message: &'a str,
        recoverable: bool,
    },
}

impl StudioMessage<'_> {
    /// `cursor` sets the journal entry the browser resumes after on reconnect.
    /// Pass `None` for snapshots and other messages that leave this position unchanged.
    pub(super) fn frame(&self, cursor: Option<EventId>) -> SseFrame {
        let event = match self {
            Self::StageLifecycle { .. } => "stage_lifecycle",
            Self::FlowLifecycle { .. } => "flow_lifecycle",
            Self::ReplayLifecycle { .. } => "replay_lifecycle",
            Self::SourceCleanupFailed { .. } => "source_cleanup_failed",
            Self::MiddlewareLifecycle { .. } | Self::MiddlewareMeasurements { .. } => {
                "middleware_lifecycle"
            }
            Self::ContractStatus { pass: true, .. } => "contract_status",
            Self::ContractStatus { pass: false, .. } => "contract_violation",
            Self::ContractResult { .. } => "contract_result",
            Self::EdgeLiveness { .. } => "edge_liveness",
            Self::MetricsCoordination { .. } => "metrics_coordination",
            Self::Bootstrap { .. } => "bootstrap",
            Self::ServerShutdown { .. } => "server_shutdown",
            Self::CompositeStatus(_) => "composite_status",
            Self::MiddlewareSnapshot(_) => "middleware_state_snapshot",
            Self::MetricsWatermark { .. } => "metrics_watermark",
            Self::Error { .. } => "error",
        };
        let data =
            serde_json::to_string(self).expect("Studio messages contain JSON-compatible fields");
        let mut frame = SseFrame::event(event, data);
        frame.id = cursor.map(|id| id.to_string());
        frame
    }
}

#[derive(Serialize)]
pub(super) struct Observation<'a> {
    pub timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_clock: Option<&'a VectorClock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture: Option<obzenflow_core::event::observation::CaptureStamp>,
}

#[derive(Serialize)]
pub(super) struct ContractEdge<'a> {
    #[serde(serialize_with = "display")]
    pub upstream_stage_id: StageId,
    #[serde(serialize_with = "display")]
    pub reader_stage_id: StageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_event_type: Option<&'a EventType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_role: Option<SystemFeedRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_seq: Option<SeqNo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertised_writer_seq: Option<SeqNo>,
    #[serde(skip_serializing_if = "<[ContractBoundaryAlias]>::is_empty")]
    pub composite_boundaries: &'a [ContractBoundaryAlias],
}

// Core records `lifecycle_event: "running"`; Studio expects
// `event_type: "stage_running"`. The Serde definitions below translate the names.
#[derive(Serialize)]
#[serde(remote = "StageLifecycleEvent", tag = "event_type")]
enum StageUpdate {
    #[serde(rename = "stage_running")]
    Running,
    #[serde(rename = "stage_draining")]
    Draining {
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<obzenflow_core::event::context::ExecutionAccounting>,
    },
    #[serde(rename = "stage_drained")]
    Drained,
    #[serde(rename = "stage_completed")]
    Completed {
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<obzenflow_core::event::context::ExecutionAccounting>,
    },
    #[serde(rename = "stage_cancelled")]
    Cancelled {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<obzenflow_core::event::context::ExecutionAccounting>,
    },
    #[serde(rename = "stage_failed")]
    Failed {
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        recoverable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<obzenflow_core::event::context::ExecutionAccounting>,
        #[serde(skip)]
        causal_event_id: Option<EventId>,
    },
}

#[derive(Serialize)]
#[serde(remote = "PipelineLifecycleEvent", tag = "event_type")]
enum FlowUpdate {
    #[serde(rename = "flow_starting")]
    Starting,
    #[serde(rename = "flow_ready_for_run")]
    ReadyForRun {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_count: Option<usize>,
    },
    #[serde(rename = "flow_running")]
    Running {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage_count: Option<usize>,
    },
    #[serde(rename = "flow_stop_admitted")]
    StopAdmitted { admission: PipelineStopAdmission },
    #[serde(rename = "flow_not_started")]
    NotStarted,
    #[serde(rename = "flow_draining")]
    Draining {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
    },
    #[serde(rename = "flow_stages_completed")]
    AllStagesCompleted {
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
    },
    #[serde(rename = "flow_drained")]
    Drained,
    #[serde(rename = "flow_completed")]
    Completed {
        duration_ms: DurationMs,
        metrics: FlowLifecycleMetricsSnapshot,
    },
    #[serde(rename = "flow_failed")]
    Failed {
        reason: String,
        duration_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_cause: Option<ViolationCause>,
    },
    #[serde(rename = "flow_cancelled")]
    Cancelled {
        reason: String,
        duration_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_cause: Option<ViolationCause>,
    },
}

#[derive(Serialize)]
#[serde(remote = "ReplayLifecycleEvent", tag = "event_type")]
enum ReplayUpdate {
    #[serde(rename = "replay_started")]
    Started {
        archive_path: PathBuf,
        archive_flow_id: String,
        archive_status: ArchiveStatus,
        archive_status_derivation: StatusDerivation,
        allow_incomplete: bool,
        source_stages: Vec<String>,
    },
    #[serde(rename = "replay_completed")]
    Completed {
        replayed_count: Count,
        skipped_count: Count,
        duration_ms: DurationMs,
        #[serde(skip_serializing_if = "Option::is_none")]
        synthesized_eof_kind: Option<EofKind>,
    },
    #[serde(rename = "resumed_live")]
    ResumedLive {
        archive_flow_id: String,
        replayed_count: Count,
        generation: u64,
    },
}

#[derive(Serialize)]
pub(super) enum BootstrapUpdate {
    #[serde(rename = "flow_bootstrap")]
    FlowBootstrap,
}

#[derive(Serialize)]
pub(super) enum MetricsUpdate {
    #[serde(rename = "metrics_ready")]
    Ready,
    #[serde(rename = "metrics_drain_requested")]
    DrainRequested,
    #[serde(rename = "metrics_drained")]
    Drained,
    #[serde(rename = "metrics_shutdown")]
    Shutdown,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StreamErrorKind {
    InvalidLastEventId,
    JournalResumeNotFound,
    JournalOpenError,
    JournalReadError,
}

#[derive(Serialize)]
#[serde(tag = "middleware", rename_all = "snake_case")]
pub(super) enum MiddlewareUpdate<'a> {
    CircuitBreaker(CircuitBreakerUpdate<'a>),
    RateLimiter(RateLimiterUpdate<'a>),
    Backpressure(BackpressureUpdate),
}

#[derive(Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub(super) enum CircuitBreakerUpdate<'a> {
    StateChange {
        #[serde(skip_serializing_if = "Option::is_none")]
        state_from: Option<&'a str>,
        state_to: &'static str,
        context: CircuitTransition<'a>,
    },
    Summary {
        summary: CircuitSummary,
    },
    Measurements {
        measurements: obzenflow_core::event::context::CircuitBreakerMeasurements,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum CircuitTransition<'a> {
    Opened {
        error_rate: f64,
        failure_count: u64,
        trigger: CircuitBreakerOpenTrigger,
        observed_calls: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        slow_call_rate: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        slow_call_count: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<&'a str>,
    },
    Closed {
        success_count: u64,
        recovery_duration_ms: u64,
    },
    HalfOpen {
        test_request_count: u32,
    },
    StateChanged {
        timestamp: u64,
    },
}

#[derive(Serialize)]
pub(super) struct CircuitSummary {
    pub window_duration_s: u64,
    #[serde(flatten)]
    pub totals: CircuitTotals,
}

#[derive(Clone, Default, Serialize)]
pub(super) struct CircuitTotals {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_rejected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consecutive_failures: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub successes_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failures_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opened_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_closed_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_open_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_half_open_s: Option<f64>,
}

#[derive(Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub(super) enum RateLimiterUpdate<'a> {
    ActivityPulse {
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,
        limit_rate: f64,
    },
    ModeChange {
        mode_from: &'a str,
        mode_to: &'a str,
        limit_rate: f64,
    },
    WindowUtilization {
        #[serde(flatten)]
        window: RateLimiterWindow,
    },
    Measurements {
        measurements: obzenflow_core::event::context::RateLimiterMeasurements,
    },
}

#[derive(Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub(super) enum BackpressureUpdate {
    ActivityPulse {
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,
        context: BackpressureContext,
    },
}

#[derive(Serialize)]
pub(super) struct BackpressureContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_credit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limiting_downstream_stage_id: Option<String>,
}

#[derive(Clone, Default, Serialize)]
pub(super) struct RateLimiterWindow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utilization_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_in_window: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_size_ms: Option<u64>,
}

#[derive(Serialize)]
pub(super) struct MiddlewareSnapshot<'a> {
    pub timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_clock: Option<&'a VectorClock>,
    pub middleware: Vec<StageMiddlewareSnapshot<'a>>,
}

#[derive(Serialize)]
pub(super) struct StageMiddlewareSnapshot<'a> {
    #[serde(serialize_with = "display")]
    pub stage_id: StageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<&'a CircuitBreakerSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiter: Option<&'a RateLimiterSnapshot>,
}

#[derive(Clone, Default, Serialize)]
pub(super) struct CircuitBreakerSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_updated_at_ms: Option<u64>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub totals: Option<CircuitTotals>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub totals_observed_at_ms: Option<u64>,
}

#[derive(Clone, Default, Serialize)]
pub(super) struct RateLimiterSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_updated_at_ms: Option<u64>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub window: Option<RateLimiterWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_observed_at_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CompositeStatusWireV1 {
    Waiting,
    Running,
    Completed,
    Cancelled,
    Failed,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompositeStatusPayloadV1 {
    pub schema_version: u32,
    pub message_type: &'static str,
    pub composite_id: String,
    pub status: CompositeStatusWireV1,
    /// Counts group status changes, allowing Studio to ignore older updates.
    pub revision: u64,
    pub as_of_event_id: Option<String>,
    pub timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Member role where the first failure occurred, such as `map`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// Studio needs the `stage_` prefix; StageId's default JSON encoding omits it.
fn display<T: std::fmt::Display, S: Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
