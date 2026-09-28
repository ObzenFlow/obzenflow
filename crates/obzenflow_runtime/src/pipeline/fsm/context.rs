// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline-owned work and child acknowledgement aggregation.

use crate::metrics::observations::ObservationRegistry;
use crate::pipeline::resources::PipelineResources;
use crate::pipeline::termination::TerminationState;
use crate::pipeline::FlowStopMode;
use crate::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::{ChainEvent, SystemEvent, WriterId};
use obzenflow_core::id::{FlowId, SystemId};
use obzenflow_core::journal::Journal;
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

pub(crate) struct PipelineContext {
    pub(crate) system_id: SystemId,
    pub(crate) topology: Arc<obzenflow_topology::Topology>,
    pub(crate) flow_name: String,
    pub(crate) flow_id: FlowId,
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
    pub(crate) stage_supervisors:
        HashMap<StageId, Arc<dyn crate::stages::common::stage_handle::StageHandle>>,
    pub(crate) source_supervisors:
        HashMap<StageId, Arc<dyn crate::stages::common::stage_handle::StageHandle>>,
    /// Only the currently requested milestone is aggregated here.
    pub(crate) outstanding_milestones: HashSet<WriterId>,
    /// Physical task termination, independent of milestone observation.
    pub(crate) outstanding_children: HashSet<StageId>,
    pub(crate) completed_stages: HashSet<StageId>,
    pub(crate) metrics_journals: Option<crate::metrics::builder::MetricsJournals>,
    pub(crate) metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsSnapshotExporter>>,
    pub(crate) stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    pub(crate) stage_error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    pub(crate) observations: Arc<ObservationRegistry>,
    pub(crate) runtime_execution: Option<crate::execution::RuntimeExecution>,
    pub(crate) observation_export_interval: Duration,
    pub(crate) backpressure_registry: Option<Arc<crate::backpressure::BackpressureRegistry>>,
    pub(crate) resources: PipelineResources,
    pub(crate) stage_lifecycle_metrics: HashMap<StageId, ExecutionAccounting>,
    pub(crate) flow_start_time: Option<std::time::Instant>,
    pub(crate) stop_intent: StopIntent,
    pub(crate) termination: TerminationState,
    pub(crate) cleanup_deadline: Option<std::time::Instant>,
    pub(crate) metrics_deadline: Option<std::time::Instant>,
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

impl FsmContext for PipelineContext {}

pub(crate) fn stop_drain_timeout() -> Duration {
    crate::bootstrap::shutdown_timeout()
}
