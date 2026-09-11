// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fsm::{FlowStopMode, PipelineControl, PipelineFsmEvent, PipelineState};
use super::termination::{execution_result, PublishedOutcome};
use crate::errors::FlowError;
use crate::journal::RunSubstrateState;
use crate::stages::LivenessSnapshots;
use crate::supervised_base::{StandardHandle, SupervisorHandle};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use serde::{Deserialize, Serialize};
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

/// Structural middleware configuration for a stage (FLOWIP-059).
///
/// Contains both the ordered list of middleware names and their static configuration
/// snapshots for the topology observability API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareStackConfig {
    /// Ordered list of middleware names in the stack
    pub stack: Vec<String>,
    /// Circuit breaker static config (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<serde_json::Value>,
    /// Rate limiter static config (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiter: Option<serde_json::Value>,
}

impl MiddlewareStackConfig {
    /// Create a new middleware stack config with just names (no detailed config)
    pub fn names_only(stack: Vec<String>) -> Self {
        Self {
            stack,
            circuit_breaker: None,
            rate_limiter: None,
        }
    }
}

/// Immediate result of submitting a start control, without an FSM acknowledgement.
#[derive(Debug, Clone, PartialEq)]
pub enum FlowStartControlOutcome {
    /// `Start` was sent after observing readiness; the FSM decides admission.
    Submitted { observed_state: PipelineState },
    /// The pipeline was already running, so no duplicate `Run` was sent.
    AlreadyRunning { state: PipelineState },
    /// The pipeline cannot accept `Run` in the observed state.
    Rejected {
        state: PipelineState,
        reason: &'static str,
    },
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
                tracing::debug!("FlowHandle::start() - Sending PipelineFsmEvent::Control(PipelineControl::Start) to start flow");
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
            .send_event(PipelineFsmEvent::Control(control))
            .await
            .map_err(|error| FlowError::ExecutionFailed(Box::new(error)))
    }

    /// Join the supervisor and report the acknowledged execution result.
    pub async fn wait_for_completion(&self) -> Result<(), FlowError> {
        self.wait_for_execution().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::__private::lifecycle;
    use crate::stages::common::stage_handle::STOP_REASON_TIMEOUT;
    use crate::supervised_base::{ChannelBuilder, EventReceiver, HandleBuilder};
    use obzenflow_core::event::types::ViolationCause;
    use std::error::Error;
    use tokio::sync::mpsc::error::TryRecvError;

    fn empty_extras() -> FlowHandleExtras {
        FlowHandleExtras {
            stage_cleanup: Vec::new(),
            published_outcome: Default::default(),
            metrics: Default::default(),
            operational_failure: Default::default(),
            topology: None,
            flow_name: "test_flow".to_string(),
            contract_attachments: None,
            system_journal: None,
            pipeline_writer_id: WriterId::from(obzenflow_core::id::SystemId::new()),
            flow_effective_config: None,
            liveness_snapshots: None,
            run_substrate: RunSubstrateState::Ephemeral,
        }
    }

    #[tokio::test]
    async fn execution_guard_is_independent_of_completion_observers_and_handle_ownership() {
        for disarm in [false, true] {
            let (sender, _receiver, watcher) =
                ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
                    .build(PipelineState::Created);
            let task = tokio::spawn(std::future::pending::<
                Result<(), Box<dyn std::error::Error + Send + Sync>>,
            >());
            let flow = Arc::new(FlowHandle::new(
                HandleBuilder::new()
                    .with_event_sender(sender)
                    .with_state_watcher(watcher)
                    .with_supervisor_task(task)
                    .build_standard()
                    .unwrap(),
                empty_extras(),
            ));
            let guard = lifecycle::guard_execution(&flow);
            assert_eq!(
                Arc::strong_count(&flow),
                1,
                "guard must not retain the FlowHandle"
            );
            let mut observer = Box::pin(lifecycle::wait(&flow));
            assert!(futures::poll!(&mut observer).is_pending());
            drop(observer);
            assert!(
                flow.is_running(),
                "dropping an observer must not cancel execution"
            );
            if disarm {
                guard.disarm();
                assert!(
                    flow.is_running(),
                    "releasing the fallback must not request cancellation"
                );
                drop(lifecycle::guard_execution(&flow));
            } else {
                drop(guard);
            }
            for _ in 0..2 {
                let error = tokio::time::timeout(Duration::from_secs(1), lifecycle::wait(&flow))
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(error
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("Supervisor task was aborted"));
            }
            assert!(!flow.is_running());
            assert_eq!(flow.current_state(), PipelineState::Created);
        }
    }

    fn flow_handle_that_finishes_in(final_state: PipelineState) -> FlowHandle {
        let (event_sender, mut event_receiver, state_watcher) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
                .with_event_buffer(4)
                .build(PipelineState::ReadyForRun);

        let state_watcher_for_task = state_watcher.clone();
        let extras = empty_extras();
        let published = extras.published_outcome.clone();
        let task = tokio::spawn(async move {
            match event_receiver.recv().await {
                Some(PipelineFsmEvent::Control(PipelineControl::Start)) => {
                    use super::super::termination::{
                        ExecutionFailure, ExecutionOutcome, PublishedTermination,
                    };
                    let outcome = match &final_state {
                        PipelineState::Failed {
                            reason,
                            failure_cause,
                        } => ExecutionOutcome::Failed(ExecutionFailure {
                            reason: reason.clone(),
                            cause: failure_cause.clone(),
                        }),
                        PipelineState::AbortRequested { reason, .. } => {
                            ExecutionOutcome::Failed(ExecutionFailure {
                                reason: format!("{reason:?}"),
                                cause: Some(reason.clone()),
                            })
                        }
                        _ => ExecutionOutcome::Completed,
                    };
                    published
                        .set(PublishedTermination {
                            outcome,
                            event_id: Some(obzenflow_core::EventId::new()),
                        })
                        .unwrap();
                    state_watcher_for_task
                        .update(final_state)
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
                    Ok(())
                }
                Some(event) => Err(format!("unexpected event: {event:?}").into()),
                None => Err("event channel closed before Run".into()),
            }
        });

        let handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .expect("standard handle should build");

        FlowHandle::new(handle, extras)
    }

    fn flow_handle_for_start_admission(
        initial_state: PipelineState,
    ) -> (FlowHandle, EventReceiver<PipelineFsmEvent>) {
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
                .with_event_buffer(4)
                .build(initial_state);

        let task = tokio::spawn(async { Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) });

        let handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .expect("standard handle should build");

        (FlowHandle::new(handle, empty_extras()), event_receiver)
    }

    #[tokio::test]
    async fn start_if_ready_now_dispatches_run_in_ready_for_run() {
        let (handle, mut event_receiver) =
            flow_handle_for_start_admission(PipelineState::ReadyForRun);

        let outcome = handle
            .start_if_ready_now()
            .await
            .expect("ReadyForRun admission should succeed");

        assert_eq!(
            outcome,
            FlowStartControlOutcome::Submitted {
                observed_state: PipelineState::ReadyForRun
            }
        );
        assert!(
            matches!(
                event_receiver.try_recv(),
                Ok(PipelineFsmEvent::Control(PipelineControl::Start))
            ),
            "ReadyForRun admission should dispatch Run"
        );
    }

    #[tokio::test]
    async fn start_if_ready_now_accepts_running_without_dispatch() {
        let (handle, mut event_receiver) = flow_handle_for_start_admission(PipelineState::Running);

        let outcome = handle
            .start_if_ready_now()
            .await
            .expect("Running admission should succeed");

        assert_eq!(
            outcome,
            FlowStartControlOutcome::AlreadyRunning {
                state: PipelineState::Running
            }
        );
        assert!(
            matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
            "Running admission must not dispatch duplicate Run"
        );
    }

    #[tokio::test]
    async fn start_if_ready_now_rejects_non_ready_states_without_dispatch() {
        let cases = [
            PipelineState::Created,
            PipelineState::Materializing,
            PipelineState::Materialized,
            PipelineState::SourceCompleted,
            PipelineState::AbortRequested {
                reason: ViolationCause::Other("abort".to_string()),
                upstream: None,
            },
            PipelineState::Draining,
            PipelineState::Drained,
            PipelineState::Failed {
                reason: "failed".to_string(),
                failure_cause: None,
            },
        ];

        for state in cases {
            let (handle, mut event_receiver) = flow_handle_for_start_admission(state.clone());

            let outcome = handle
                .start_if_ready_now()
                .await
                .expect("rejection should be reported as a control outcome");

            assert_eq!(
                outcome,
                FlowStartControlOutcome::Rejected {
                    state,
                    reason: "pipeline is not ready for run"
                }
            );
            assert!(
                matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
                "rejected admission must not dispatch Run"
            );
        }
    }

    #[tokio::test]
    async fn wait_for_ready_returns_error_for_terminal_or_aborting_states() {
        let cases = [
            PipelineState::SourceCompleted,
            PipelineState::AbortRequested {
                reason: ViolationCause::Other("abort".to_string()),
                upstream: None,
            },
            PipelineState::Draining,
            PipelineState::Drained,
            PipelineState::Failed {
                reason: "failed".to_string(),
                failure_cause: None,
            },
        ];

        for state in cases {
            let (handle, _event_receiver) = flow_handle_for_start_admission(state);

            let result = handle.wait_for_ready().await;

            assert!(result.is_err(), "terminal state must not satisfy readiness");
        }
    }

    #[tokio::test]
    async fn start_accepts_coalesced_running_without_dispatch() {
        let (handle, mut event_receiver) = flow_handle_for_start_admission(PipelineState::Running);

        handle
            .start()
            .await
            .expect("Running should satisfy start without dispatching Run");

        assert!(
            matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
            "start must not dispatch duplicate Run after observing Running"
        );
    }

    #[tokio::test]
    async fn run_returns_failed_terminal_state_as_error() {
        let handle = flow_handle_that_finishes_in(PipelineState::Failed {
            reason: "terminal failure".to_string(),
            failure_cause: None,
        });

        let result = handle.run().await;
        assert!(
            result.is_err(),
            "Failed terminal state must surface as an error"
        );
        let err = result.expect_err("error should be present");
        let source = err.source().expect("source error should be present");

        assert!(
            source.to_string().contains("terminal failure"),
            "unexpected source error: {source}"
        );
    }

    #[tokio::test]
    async fn run_returns_abort_terminal_state_as_error() {
        let handle = flow_handle_that_finishes_in(PipelineState::AbortRequested {
            reason: ViolationCause::Other("abort requested".to_string()),
            upstream: None,
        });

        let result = handle.run().await;
        assert!(
            result.is_err(),
            "AbortRequested terminal state must surface as an error"
        );
        let err = result.expect_err("error should be present");
        let source = err.source().expect("source error should be present");

        assert!(
            source.to_string().contains("abort requested"),
            "unexpected source error: {source}"
        );
    }

    #[tokio::test]
    async fn all_flow_completion_paths_report_execution_and_task_failures_consistently() {
        use super::super::termination::{ExecutionFailure, ExecutionOutcome, PublishedTermination};

        #[derive(Clone, Copy)]
        enum Exit {
            Returned,
            Failed,
            Panicked,
            Aborted,
        }
        let failed = PipelineState::Failed {
            reason: "FSM cleanup state is not the execution result".into(),
            failure_cause: None,
        };
        let cases = [
            (
                PipelineState::Drained,
                Some(ExecutionOutcome::Completed),
                Exit::Returned,
                None,
            ),
            (
                failed.clone(),
                Some(ExecutionOutcome::Cancelled {
                    reason: "operator stop".into(),
                }),
                Exit::Returned,
                None,
            ),
            (
                failed.clone(),
                Some(ExecutionOutcome::Cancelled {
                    reason: STOP_REASON_TIMEOUT.into(),
                }),
                Exit::Returned,
                None,
            ),
            (
                PipelineState::ReadyForRun,
                Some(ExecutionOutcome::NotStarted),
                Exit::Returned,
                None,
            ),
            (
                failed,
                Some(ExecutionOutcome::Failed(ExecutionFailure {
                    reason: "acknowledged failure".into(),
                    cause: None,
                })),
                Exit::Returned,
                Some("acknowledged failure"),
            ),
            (
                PipelineState::Drained,
                None,
                Exit::Returned,
                Some("without an acknowledged terminal outcome"),
            ),
            (
                PipelineState::Created,
                None,
                Exit::Failed,
                Some("task failed before readiness"),
            ),
            (
                PipelineState::Drained,
                Some(ExecutionOutcome::Completed),
                Exit::Panicked,
                Some("failure after publication"),
            ),
            (
                PipelineState::Created,
                None,
                Exit::Aborted,
                Some("Supervisor task was aborted"),
            ),
        ];
        for (state, outcome, exit, expected) in cases {
            for use_run in [false, true] {
                let (sender, _receiver, watcher) =
                    ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(state.clone());
                let extras = empty_extras();
                let published = extras.published_outcome.clone();
                let outcome = outcome.clone();
                let task = tokio::spawn(async move {
                    if let Some(outcome) = outcome {
                        let event_id = (!matches!(outcome, ExecutionOutcome::NotStarted))
                            .then(obzenflow_core::EventId::new);
                        published
                            .set(PublishedTermination { outcome, event_id })
                            .unwrap();
                    }
                    match exit {
                        Exit::Returned => Ok(()),
                        Exit::Failed => Err("task failed before readiness".into()),
                        Exit::Panicked => panic!("failure after publication"),
                        Exit::Aborted => std::future::pending().await,
                    }
                });
                if matches!(exit, Exit::Aborted) {
                    task.abort();
                }
                let handle = FlowHandle::new(
                    HandleBuilder::new()
                        .with_event_sender(sender)
                        .with_state_watcher(watcher)
                        .with_supervisor_task(task)
                        .build_standard()
                        .unwrap(),
                    extras,
                );
                let first = lifecycle::wait(&handle).await;
                let repeated = lifecycle::wait(&handle).await;
                let consumed = if use_run {
                    tokio::time::timeout(Duration::from_secs(1), handle.run())
                        .await
                        .expect("completion must not hang waiting for readiness")
                } else {
                    handle.wait_for_completion().await
                };
                let diagnostic = |result: Result<(), FlowError>| {
                    result.map_err(|error| error.source().unwrap().to_string())
                };
                let first = diagnostic(first);
                assert_eq!(first, diagnostic(repeated));
                let consumed = diagnostic(consumed);
                if matches!(exit, Exit::Returned) {
                    assert!(
                        first.is_ok(),
                        "resource observation does not classify journal facts"
                    );
                } else {
                    assert_eq!(first, consumed);
                }
                match expected {
                    Some(message) => assert!(consumed.unwrap_err().contains(message)),
                    None => consumed.unwrap(),
                }
            }
        }
    }

    #[tokio::test]
    async fn framework_waits_do_not_consume_completion_or_skip_the_physical_join() {
        use super::super::termination::{ExecutionOutcome, PublishedTermination};
        let (sender, _receiver, watcher) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Drained);
        let extras = empty_extras();
        extras
            .published_outcome
            .set(PublishedTermination {
                outcome: ExecutionOutcome::Completed,
                event_id: Some(obzenflow_core::EventId::new()),
            })
            .unwrap();
        let (release, gate) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            gate.await.unwrap();
            Ok(())
        });
        let handle = FlowHandle::new(
            HandleBuilder::new()
                .with_event_sender(sender)
                .with_state_watcher(watcher)
                .with_supervisor_task(task)
                .build_standard()
                .unwrap(),
            extras,
        );
        let mut parked = Box::pin(lifecycle::wait(&handle));
        assert!(
            futures::poll!(&mut parked).is_pending(),
            "publication alone is not completion"
        );
        let mut dropped = Box::pin(lifecycle::wait(&handle));
        assert!(futures::poll!(&mut dropped).is_pending());
        drop(dropped);
        release.send(()).unwrap();
        lifecycle::wait(&handle).await.unwrap();
        parked.await.unwrap();
        lifecycle::wait(&handle).await.unwrap();
        handle.wait_for_completion().await.unwrap();
    }

    #[tokio::test]
    async fn run_allows_successful_terminal_state() {
        let handle = flow_handle_that_finishes_in(PipelineState::Drained);

        handle
            .run()
            .await
            .expect("Drained terminal state should remain successful");
    }
}
