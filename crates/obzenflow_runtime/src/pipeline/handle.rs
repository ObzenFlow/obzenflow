// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fsm::PipelineFsmEvent;
use super::termination::{execution_result, PublishedOutcome};
use super::{FlowStartControlOutcome, FlowStopMode, PipelineControl, PipelineState};
use crate::errors::FlowError;
use crate::journal::RunSubstrateState;
use crate::stages::LivenessSnapshots;
use crate::supervised_base::{StandardHandle, SupervisorHandle};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

type ContractAttachments = Arc<HashMap<(StageId, StageId), Vec<String>>>;

pub(crate) struct FlowHandleExtras {
    pub stage_cleanup: Vec<Arc<dyn crate::stages::common::stage_handle::StageHandle>>,
    pub published_outcome: PublishedOutcome,
    pub metrics: Arc<super::resources::MetricsOwner>,
    pub operational_failure: super::resources::OperationalFailure,
    pub topology: Option<Arc<Topology>>,
    pub flow_name: String,
    pub contract_attachments: Option<ContractAttachments>,
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub pipeline_writer_id: WriterId,
    pub liveness_snapshots: Option<LivenessSnapshots>,
    /// The selected run substrate (FLOWIP-120u): durable with its locator, or ephemeral.
    pub run_substrate: RunSubstrateState,
    /// FLOWIP-010: the build-resolved effective config, carried out of the
    /// build so the host can serve the per-flow/per-stage read surface.
    pub flow_effective_config: Option<Arc<crate::runtime_config::FlowEffectiveConfig>>,
}

/// Flow handle for external control - the public API returned by the DSL
///
/// This is a wrapper that combines:
/// - A standard handle for FSM control (event sending, state watching, lifecycle)
/// - Pipeline-specific functionality (metrics export)
///
/// This is the only supervisor handle that gets exposed to DSL users,
/// so it needs to provide all functionality they might need.
pub struct FlowHandle {
    stage_cleanup: Vec<Arc<dyn crate::stages::common::stage_handle::StageHandle>>,
    published_outcome: PublishedOutcome,
    metrics: Arc<super::resources::MetricsOwner>,
    operational_failure: super::resources::OperationalFailure,
    /// The standard handle for FSM control
    handle: StandardHandle<PipelineFsmEvent, PipelineState>,

    /// Flow topology for visualization (read-only)
    topology: Option<Arc<Topology>>,

    /// User-specified flow name from flow! macro
    flow_name: String,

    /// Structural contract names per edge (for topology observability).
    ///
    /// As of FLOWIP-114b, the canonical `topology` carries stage typing,
    /// join metadata, subgraph membership, and middleware annotations
    /// directly. Contracts remain a side map because they are derived in
    /// `PipelineBuilder::build` from the topology shape and are not yet
    /// baked into the canonical `Topology`.
    contract_attachments: Option<ContractAttachments>,

    /// System journal for lifecycle events (for SSE / observability)
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,

    /// Writer identity for this pipeline's lifecycle facts in the system journal.
    pipeline_writer_id: WriterId,

    /// Flow-scoped stage liveness snapshots (FLOWIP-063e).
    liveness_snapshots: Option<LivenessSnapshots>,

    /// The selected run substrate (FLOWIP-120u).
    run_substrate: RunSubstrateState,

    /// FLOWIP-010: the build-resolved effective config with provenance.
    flow_effective_config: Option<Arc<crate::runtime_config::FlowEffectiveConfig>>,
}

impl FlowHandle {
    /// Create a new flow handle from a standard handle and extras
    pub(crate) fn new(
        handle: StandardHandle<PipelineFsmEvent, PipelineState>,
        extras: FlowHandleExtras,
    ) -> Self {
        let FlowHandleExtras {
            stage_cleanup,
            published_outcome,
            metrics,
            operational_failure,
            topology,
            flow_name,
            contract_attachments,
            system_journal,
            pipeline_writer_id,
            liveness_snapshots,
            run_substrate,
            flow_effective_config,
        } = extras;

        Self {
            stage_cleanup,
            published_outcome,
            metrics,
            operational_failure,
            handle,
            topology,
            flow_name,
            contract_attachments,
            system_journal,
            pipeline_writer_id,
            liveness_snapshots,
            run_substrate,
            flow_effective_config,
        }
    }

    pub(crate) fn execution_guard(&self) -> crate::__private::lifecycle::ExecutionGuard {
        crate::__private::lifecycle::ExecutionGuard::new(
            self.handle.abort_handle(),
            self.stage_cleanup.clone(),
            self.metrics.clone(),
        )
    }

    /// Every flow completion path joins first, then interprets the same
    /// acknowledged execution outcome. Task failure takes precedence.
    pub(crate) async fn wait_for_execution(&self) -> Result<(), FlowError> {
        self.wait_for_resources().await?;
        execution_result(&self.published_outcome)
    }

    pub(crate) async fn wait_for_resources(&self) -> Result<(), FlowError> {
        let mut result = self
            .handle
            .join()
            .await
            .map_err(|error| FlowError::ExecutionFailed(Box::new(error)));
        for stage in &self.stage_cleanup {
            stage.request_abort();
        }
        for stage in &self.stage_cleanup {
            let joined = stage.abort_and_join().await;
            if let Err(error) = joined {
                if result.is_ok() {
                    result = Err(FlowError::ExecutionFailed(Box::new(error)));
                }
            }
        }
        if let Err(error) = self.metrics.abort_and_join().await {
            if result.is_ok() {
                result = Err(FlowError::ExecutionFailed(Box::new(error)));
            }
        }
        if result.is_ok() {
            if let Some(error) = self.operational_failure.get() {
                result = Err(FlowError::ExecutionFailed(Box::new(error.clone())));
            }
        }
        result
    }

    /// The run substrate selected at composition: durable with its current-run
    /// locator, or ephemeral with none (FLOWIP-120u).
    pub fn run_substrate(&self) -> &RunSubstrateState {
        &self.run_substrate
    }

    /// The build-resolved effective config (FLOWIP-010), when the flow was
    /// built through the DSL path that materializes it.
    pub fn flow_effective_config(
        &self,
    ) -> Option<&Arc<crate::runtime_config::FlowEffectiveConfig>> {
        self.flow_effective_config.as_ref()
    }

    /// Try to start the pipeline using only the currently observed state.
    ///
    /// This is intended for non-blocking control surfaces such as HTTP Play.
    /// It does not wait for readiness. Callers that want blocking startup
    /// semantics should use `start()` or `run()`.
    pub async fn start_if_ready_now(&self) -> Result<FlowStartControlOutcome, FlowError> {
        const NOT_READY_REASON: &str = "pipeline is not ready for run";

        let state = self.current_state();
        match state {
            PipelineState::ReadyForRun => {
                self.send_control(PipelineControl::Start).await?;
                Ok(FlowStartControlOutcome::Submitted {
                    observed_state: state,
                })
            }
            PipelineState::Running => Ok(FlowStartControlOutcome::AlreadyRunning { state }),
            _ => Ok(FlowStartControlOutcome::Rejected {
                state,
                reason: NOT_READY_REASON,
            }),
        }
    }

    /// Wait until the pipeline is ready to accept `Run`.
    ///
    /// Returns successfully when the pipeline reaches `ReadyForRun`, or when
    /// `Running` is already observed. Returns an error if the pipeline reaches a
    /// terminal, aborting, or post-source state before it is ready.
    pub async fn wait_for_ready(&self) -> Result<(), FlowError> {
        let mut state_rx = self.state_receiver();

        loop {
            let state = state_rx.borrow().clone();
            match state {
                PipelineState::ReadyForRun | PipelineState::Running => return Ok(()),
                PipelineState::Failed { reason, .. } => {
                    return Err(FlowError::ExecutionFailed(Box::new(io::Error::other(
                        reason,
                    ))));
                }
                PipelineState::AbortRequested { reason, .. } => {
                    return Err(FlowError::ExecutionFailed(Box::new(io::Error::other(
                        format!("{reason:?}"),
                    ))));
                }
                PipelineState::SourceCompleted => {
                    return Err(FlowError::ExecutionFailed(Box::new(io::Error::other(
                        "Pipeline source completed before it became ready for run",
                    ))));
                }
                PipelineState::Draining => {
                    return Err(FlowError::ExecutionFailed(Box::new(io::Error::other(
                        "Pipeline entered draining before it became ready for run",
                    ))));
                }
                PipelineState::Drained => {
                    return Err(FlowError::ExecutionFailed(Box::new(io::Error::other(
                        "Pipeline drained before it became ready for run",
                    ))));
                }
                PipelineState::Created
                | PipelineState::Materializing
                | PipelineState::Materialized => {}
            }

            state_rx.changed().await.map_err(|_| {
                FlowError::ExecutionFailed(Box::new(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Pipeline state channel closed before readiness",
                )))
            })?;
        }
    }

    /// Start the pipeline without waiting for completion.
    ///
    /// This waits until the pipeline reaches `ReadyForRun`, sends `Run` only
    /// while still in that state, and returns immediately. If the pipeline is
    /// already `Running`, this returns successfully without sending a duplicate
    /// command.
    ///
    /// If a finite flow reaches a terminal state before the post-readiness state
    /// check can send `Run`, this returns an error. Use `run()` for finite
    /// flows that should be driven to completion.
    /// Intended for long-running/server flows where lifecycle is driven
    /// externally (e.g. via HTTP control API) rather than by awaiting
    /// `run()` to completion.
    pub async fn start(&self) -> Result<(), FlowError> {
        self.wait_for_ready().await?;
        let current_state = self.current_state();
        tracing::debug!(
            "FlowHandle::start() - Current pipeline state: {:?}",
            current_state
        );
        match current_state {
            PipelineState::ReadyForRun => {
                tracing::debug!("FlowHandle::start() - Sending PipelineFsmEvent::Start to start flow");
                self.send_control(PipelineControl::Start).await
            }
            PipelineState::Running => {
                tracing::debug!("FlowHandle::start() - Pipeline already running");
                Ok(())
            }
            PipelineState::Failed { reason, .. } => Err(FlowError::ExecutionFailed(Box::new(
                io::Error::other(reason),
            ))),
            PipelineState::AbortRequested { reason, .. } => Err(FlowError::ExecutionFailed(
                Box::new(io::Error::other(format!("{reason:?}"))),
            )),
            PipelineState::SourceCompleted
            | PipelineState::Draining
            | PipelineState::Drained
            | PipelineState::Created
            | PipelineState::Materializing
            | PipelineState::Materialized => Err(FlowError::ExecutionFailed(Box::new(
                io::Error::other(format!(
                    "Pipeline left readiness window before start command could be sent: {current_state:?}"
                )),
            ))),
        }
    }

    /// Run the pipeline and wait for completion
    ///
    /// This waits for `ReadyForRun` before sending `Run`. If the pipeline is
    /// already `Running`, it waits for completion without sending another `Run`.
    /// This is the primary method users should call after creating a flow.
    ///
    /// Like `FlowHandle::wait_for_completion`, this joins the supervisor
    /// and reports the acknowledged execution result, including when the flow
    /// has already finished. Intentional cancellation succeeds; execution or
    /// task failure and missing terminal publication return an error.
    pub async fn run(self) -> Result<(), FlowError> {
        // Completion also observes a supervisor that fails before readiness.
        // A missed readiness window or closed command channel cannot replace
        // the joined execution result with an admission diagnostic.
        tokio::select! {
            result = self.wait_for_execution() => result,
            _ = self.start() => self.wait_for_execution().await,
        }
    }

    /// User-initiated stop request.
    ///
    /// Natural source completion is observed through the system journal.
    pub async fn stop(&self) -> Result<(), FlowError> {
        self.stop_cancel().await
    }

    /// Stop as quickly as possible (Cancel semantics).
    pub async fn stop_cancel(&self) -> Result<(), FlowError> {
        // If the supervisor already terminated, treat Stop as an idempotent no-op.
        // This avoids surfacing "supervisor not running" as an error to callers
        // that may issue Stop more than once (e.g. UI retries).
        if !self.is_running() {
            return Ok(());
        }
        self.send_control(PipelineControl::Stop {
            mode: FlowStopMode::Cancel,
        })
        .await
    }

    /// Stop intake and attempt a bounded drain (GracefulStop semantics).
    ///
    /// On timeout expiry, the pipeline should escalate to Cancel.
    pub async fn stop_graceful(&self, timeout: Duration) -> Result<(), FlowError> {
        if !self.is_running() {
            return Ok(());
        }
        self.send_control(PipelineControl::Stop {
            mode: FlowStopMode::Graceful { timeout },
        })
        .await
    }

    /// Backwards-compatible alias for `stop()`.
    pub async fn shutdown(&self) -> Result<(), FlowError> {
        self.stop().await
    }

    /// Force shutdown by sending Error event to FSM
    pub async fn abort(&self, reason: &str) -> Result<(), FlowError> {
        self.send_control(PipelineControl::Abort {
            reason: reason.into(),
        })
        .await
    }

    /// Check if the pipeline is still running
    pub fn is_running(&self) -> bool {
        self.handle.is_running()
    }

    /// Get a receiver for watching state changes
    pub fn state_receiver(&self) -> tokio::sync::watch::Receiver<PipelineState> {
        self.handle.state_receiver()
    }

    /// Get the latest observed pipeline supervisor state.
    pub fn current_state(&self) -> PipelineState {
        self.handle.current_state()
    }

    /// Get the flow topology for visualization
    ///
    /// This provides access to the flow's structure (stages and connections)
    /// for visualization tools and monitoring dashboards.
    /// The topology is immutable and thread-safe.
    pub fn topology(&self) -> Option<Arc<Topology>> {
        self.topology.clone()
    }

    /// Get structural contract names per edge (for topology endpoint).
    ///
    /// FLOWIP-114b: middleware, join metadata, stage typing, and subgraph
    /// membership are now annotation fields on `topology()`; pull them
    /// from there. Contracts remain a side map because they are derived
    /// in `PipelineBuilder::build` from topology shape.
    pub fn contract_attachments(&self) -> Option<ContractAttachments> {
        self.contract_attachments.clone()
    }

    /// Get the system journal for lifecycle events (if available)
    pub fn system_journal(&self) -> Option<Arc<dyn Journal<SystemEvent>>> {
        self.system_journal.clone()
    }

    pub fn pipeline_writer_id(&self) -> WriterId {
        self.pipeline_writer_id
    }

    pub fn liveness_snapshots(&self) -> Option<LivenessSnapshots> {
        self.liveness_snapshots.clone()
    }

    /// Get the user-specified flow name from the flow! macro
    ///
    /// This returns the name provided in the `name:` field of the flow! macro,
    /// which may differ from the auto-generated topology-based name.
    pub fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

impl FlowHandle {
    /// Submit a caller control. Admission and lifecycle progression belong to
    /// the canonical FSM and its committed journal facts.
    pub async fn send_control(&self, control: PipelineControl) -> Result<(), FlowError> {
        self.handle
            .send_event(PipelineFsmEvent::from(control))
            .await
            .map_err(|error| FlowError::ExecutionFailed(Box::new(error)))
    }

    /// Join the supervisor and report the acknowledged execution result.
    pub async fn wait_for_completion(&self) -> Result<(), FlowError> {
        self.wait_for_execution().await
    }
}

#[cfg(test)]
#[path = "tests/handle.rs"]
mod tests;
