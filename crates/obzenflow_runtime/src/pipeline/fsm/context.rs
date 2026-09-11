// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline lifecycle context, progress, stop intent and contract tracking.

use crate::feed_plan::FeedKey;
use crate::messaging::system_subscription::SystemSubscription;
use crate::pipeline::config::SourceContractStrictMode;
use crate::pipeline::resources::PipelineResources;
use crate::pipeline::termination::{ExecutionOutcome, TerminationState};
use crate::pipeline::FlowStopMode;
use crate::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_core::event::types::{SeqNo, ViolationCause};
use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::id::{FlowId, SystemId};
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::StageMetricsSnapshot;
use obzenflow_core::StageId;
use obzenflow_fsm::FsmContext;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// The single reducer for handle requests, raw events and Runtime timeouts.
#[derive(Clone, Debug, Default)]
pub(crate) struct StopIntent {
    pub(crate) requested: bool,
    pub(crate) mode: Option<FlowStopMode>,
    pub(crate) reason: Option<String>,
    pub(crate) deadline: Option<std::time::Instant>,
}

pub(crate) enum StopRequestOutcome {
    Applied {
        mode: FlowStopMode,
        reason_label: String,
    },
    Ignored,
}

impl StopIntent {
    pub(crate) fn timeout_due(&self) -> bool {
        matches!(self.mode, Some(FlowStopMode::Graceful { .. }))
            && self
                .deadline
                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
    }

    pub(crate) fn apply_request(
        &mut self,
        mode: FlowStopMode,
        reason: Option<String>,
    ) -> StopRequestOutcome {
        if matches!(self.mode, Some(FlowStopMode::Cancel))
            || matches!(
                (&self.mode, &mode),
                (
                    Some(FlowStopMode::Graceful { .. }),
                    FlowStopMode::Graceful { .. }
                )
            )
        {
            return StopRequestOutcome::Ignored;
        }
        let timeout = reason.as_deref() == Some(STOP_REASON_TIMEOUT);
        if timeout && (!matches!(mode, FlowStopMode::Cancel) || !self.timeout_due()) {
            return StopRequestOutcome::Ignored;
        }
        let now = std::time::Instant::now();
        self.requested = true;
        self.reason = Some(reason.unwrap_or_else(|| STOP_REASON_USER_STOP.to_string()));
        self.mode = Some(mode.clone());
        match mode {
            FlowStopMode::Graceful { timeout } => self.deadline = Some(now + timeout),
            FlowStopMode::Cancel => {
                if !timeout {
                    self.deadline = None;
                }
            }
        }
        StopRequestOutcome::Applied {
            mode,
            reason_label: self.reason_label(),
        }
    }

    pub(crate) fn reason_label(&self) -> String {
        self.reason
            .clone()
            .unwrap_or_else(|| STOP_REASON_USER_STOP.to_string())
    }
}

/// Monotonic journal evidence and execution-local admission data.
#[derive(Default)]
pub(crate) struct PipelineProgress {
    pub(in crate::pipeline) ready_announced: bool,
    pub(in crate::pipeline) all_stages_announced: bool,
    pub(in crate::pipeline) sources_authorised: bool,
    pub(in crate::pipeline) metrics_ready: bool,
    pub(in crate::pipeline) metrics_drain_requested: bool,
    pub(in crate::pipeline) metrics_drained: bool,
    pub(in crate::pipeline) stages_cancelled: bool,
    pub(in crate::pipeline) metrics_cancelled: bool,
    pub(in crate::pipeline) journal_failed: bool,
    pub(in crate::pipeline) abort_cause: Option<(
        obzenflow_core::event::types::ViolationCause,
        Option<StageId>,
    )>,
    pub(in crate::pipeline) cleanup_deadline: Option<std::time::Instant>,
    pub(in crate::pipeline) selected_terminal: Option<(SystemEvent, ExecutionOutcome)>,
    pub(in crate::pipeline) final_marker: Option<obzenflow_core::EventId>,
    pub(in crate::pipeline) final_marker_seen: bool,
}

/// Pipeline context - holds all mutable state
pub(crate) struct PipelineContext {
    /// System ID for this pipeline component
    pub(crate) system_id: SystemId,

    /// Topology for structure queries
    pub(crate) topology: Arc<obzenflow_topology::Topology>,

    /// User-specified flow name (from `flow!`)
    pub(crate) flow_name: String,

    /// Flow execution ID (for metrics/observability joinability)
    pub(crate) flow_id: FlowId,

    /// System journal for pipeline orchestration events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage supervisors by ID (non-sources only)
    pub(crate) stage_supervisors:
        HashMap<StageId, Arc<dyn crate::stages::common::stage_handle::StageHandle>>,

    /// Source supervisors by ID (sources only)
    pub(crate) source_supervisors:
        HashMap<StageId, Arc<dyn crate::stages::common::stage_handle::StageHandle>>,

    /// Completed stages tracking
    pub(crate) completed_stages: Vec<StageId>,

    /// Running stages tracking (for startup coordination)
    pub(crate) running_stages: std::collections::HashSet<StageId>,

    /// System subscription for stage completion events from system journal
    pub(crate) completion_subscription: Option<SystemSubscription<SystemEvent>>,

    /// Optional exporter for aggregated metrics snapshots
    pub(crate) metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsSnapshotExporter>>,

    /// Stage data journals (for metrics aggregator)
    pub(crate) stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Stage error journals (for error sink) (FLOWIP-082e)
    pub(crate) stage_error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Flow-scoped backpressure registry for observability (FLOWIP-086k).
    pub(crate) backpressure_registry: Option<Arc<crate::backpressure::BackpressureRegistry>>,

    /// Per-source contract status (pass/fail) keyed by source StageId
    pub(crate) contract_status: HashMap<StageId, bool>,

    /// Per-feed contract status keyed by logical feed.
    pub(crate) contract_pairs: HashMap<FeedKey, ContractEdgeStatus>,

    /// Expected contract feeds derived from topology shape and runtime feed plan.
    pub(crate) expected_contract_pairs: HashSet<FeedKey>,

    /// Expected source stages (used to decide when to drain on success)
    pub(crate) expected_sources: Vec<StageId>,

    /// Pending publications, command delivery and owned child resources.
    pub(crate) resources: PipelineResources,
    pub(crate) progress: PipelineProgress,
    /// Last known per-stage lifecycle metrics (for flow rollup)
    pub(crate) stage_lifecycle_metrics: HashMap<StageId, StageMetricsSnapshot>,

    /// Flow start time for duration calculation
    pub(crate) flow_start_time: Option<std::time::Instant>,

    /// Last system event ID observed via completion_subscription (for tail reconciliation)
    pub(crate) last_system_event_id_seen: Option<obzenflow_core::EventId>,

    pub(crate) stop_intent: StopIntent,

    pub(crate) termination: TerminationState,

    /// FLOWIP-010: build-resolved `contracts.source_contract_strict_mode`.
    pub(crate) source_contract_strict: SourceContractStrictMode,

    /// FLOWIP-010: build-resolved `runtime.metrics_drain_timeout_ms`.
    pub(crate) metrics_drain_timeout_ms: u64,
}

impl Drop for PipelineContext {
    fn drop(&mut self) {
        // A cancelled or panicking supervisor cannot execute its cleanup actions.
        // These requests also cover failure before the application receives a handle.
        for stage in self
            .stage_supervisors
            .values()
            .chain(self.source_supervisors.values())
        {
            stage.request_abort();
        }
        self.resources.metrics.request_abort();
    }
}

impl PipelineContext {
    pub(crate) fn contract_keys_for_stage_pair(
        &self,
        upstream: StageId,
        reader: StageId,
    ) -> Vec<FeedKey> {
        let mut keys: Vec<FeedKey> = self
            .expected_contract_pairs
            .iter()
            .filter(|key| key.matches_stage_pair(upstream, reader))
            .cloned()
            .collect();

        if keys.is_empty() {
            keys.push(FeedKey::legacy_stage_pair(upstream, reader));
        }

        keys.sort_by(|left, right| {
            left.role
                .as_str()
                .cmp(right.role.as_str())
                .then_with(|| left.selected_payload_key.cmp(&right.selected_payload_key))
        });
        keys
    }

    pub(crate) fn contract_keys_for_contract_event(
        &self,
        upstream: StageId,
        reader: StageId,
        selected_event_type: Option<&str>,
        feed_role: Option<&str>,
    ) -> Vec<FeedKey> {
        if let Some(selected_event_type) = selected_event_type {
            let mut keys: Vec<FeedKey> = self
                .expected_contract_pairs
                .iter()
                .filter(|key| {
                    key.matches_stage_pair(upstream, reader)
                        && key.selected_payload_key == selected_event_type
                        && feed_role
                            .map(|role| key.role.as_str() == role)
                            .unwrap_or(true)
                })
                .cloned()
                .collect();

            if !keys.is_empty() {
                keys.sort_by(|left, right| {
                    left.role
                        .as_str()
                        .cmp(right.role.as_str())
                        .then_with(|| left.selected_payload_key.cmp(&right.selected_payload_key))
                });
                return keys;
            }
        }

        self.contract_keys_for_stage_pair(upstream, reader)
    }
}

impl FsmContext for PipelineContext {}

/// Stop-triggered drain timeout.
///
/// Controlled by the resolved runtime bootstrap config with a sensible default:
/// - If no host override is supplied, defaults to 30 seconds.
pub(crate) fn stop_drain_timeout() -> Duration {
    crate::bootstrap::shutdown_timeout()
}

/// Status for a contract edge (upstream -> reader).
#[derive(Clone, Debug, Default)]
pub struct ContractEdgeStatus {
    passed: bool,
    reader_seq: Option<SeqNo>,
    advertised_writer_seq: Option<SeqNo>,
}

impl ContractEdgeStatus {
    pub(crate) fn passed(reader_seq: Option<SeqNo>, advertised_writer_seq: Option<SeqNo>) -> Self {
        Self {
            passed: true,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn failed(
        _reason: Option<ViolationCause>,
        reader_seq: Option<SeqNo>,
        advertised_writer_seq: Option<SeqNo>,
    ) -> Self {
        Self {
            passed: false,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn is_passed(&self) -> bool {
        self.passed
    }

    pub fn reader_seq(&self) -> Option<SeqNo> {
        self.reader_seq
    }

    pub fn advertised_writer_seq(&self) -> Option<SeqNo> {
        self.advertised_writer_seq
    }
}

pub(in crate::pipeline) fn record_stage_completion(
    completed_stages: &mut Vec<StageId>,
    stage_id: StageId,
    total_stages: usize,
) -> (bool, bool) {
    let is_new_completion = if completed_stages.contains(&stage_id) {
        false
    } else {
        completed_stages.push(stage_id);
        true
    };
    let all_stages_completed_now = is_new_completion && completed_stages.len() >= total_stages;
    (is_new_completion, all_stages_completed_now)
}
