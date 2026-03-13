// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fsm::{FlowStopMode, PipelineEvent, PipelineState};
use crate::errors::FlowError;
use crate::stages::common::stage_handle::STOP_REASON_TIMEOUT;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

type MiddlewareStacks = Arc<HashMap<StageId, MiddlewareStackConfig>>;
type ContractAttachments = Arc<HashMap<(StageId, StageId), Vec<String>>>;
type JoinMetadataMap = Arc<HashMap<StageId, crate::pipeline::JoinMetadata>>;
type StageSubgraphMembershipMap =
    Arc<HashMap<StageId, obzenflow_core::topology::subgraphs::StageSubgraphMembership>>;
type SubgraphRegistry =
    Arc<Vec<obzenflow_core::topology::subgraphs::TopologySubgraphInfo>>;

pub(crate) struct FlowHandleExtras {
    pub topology: Option<Arc<Topology>>,
    pub flow_name: String,
    pub middleware_stacks: Option<MiddlewareStacks>,
    pub contract_attachments: Option<ContractAttachments>,
    pub join_metadata: Option<JoinMetadataMap>,
    pub subgraph_membership: Option<StageSubgraphMembershipMap>,
    pub subgraphs: Option<SubgraphRegistry>,
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
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
    /// Retry policy static config (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<serde_json::Value>,
    /// Backpressure static config (if present; FLOWIP-086k)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backpressure: Option<serde_json::Value>,
}

impl MiddlewareStackConfig {
    /// Create a new middleware stack config with just names (no detailed config)
    pub fn names_only(stack: Vec<String>) -> Self {
        Self {
            stack,
            circuit_breaker: None,
            rate_limiter: None,
            retry: None,
            backpressure: None,
        }
    }
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
    /// The standard handle for FSM control
    handle: StandardHandle<PipelineEvent, PipelineState>,

    /// Pipeline-specific: Metrics access (read-only)
    metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,

    /// Flow topology for visualization (read-only)
    topology: Option<Arc<Topology>>,

    /// User-specified flow name from flow! macro
    flow_name: String,

    /// Structural middleware stacks per stage (for topology observability, FLOWIP-059)
    middleware_stacks: Option<MiddlewareStacks>,

    /// Structural contract names per edge (for topology observability)
    contract_attachments: Option<ContractAttachments>,

    /// Join metadata per stage (catalog vs stream sources) for topology export (FLOWIP-082a)
    join_metadata: Option<JoinMetadataMap>,

    /// Per-stage membership in a logical subgraph (FLOWIP-086z-part-2)
    subgraph_membership: Option<StageSubgraphMembershipMap>,

    /// Graph-level registry of logical subgraphs (FLOWIP-086z-part-2)
    subgraphs: Option<SubgraphRegistry>,

    /// System journal for lifecycle events (for SSE / observability)
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
}

impl FlowHandle {
    /// Create a new flow handle from a standard handle and extras
    pub(crate) fn new(
        handle: StandardHandle<PipelineEvent, PipelineState>,
        metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        extras: FlowHandleExtras,
    ) -> Self {
        let FlowHandleExtras {
            topology,
            flow_name,
            middleware_stacks,
            contract_attachments,
            join_metadata,
            subgraph_membership,
            subgraphs,
            system_journal,
        } = extras;

        Self {
            handle,
            metrics_exporter,
            topology,
            flow_name,
            middleware_stacks,
            contract_attachments,
            system_journal,
            join_metadata,
            subgraph_membership,
            subgraphs,
        }
    }

    /// Start the pipeline without waiting for completion.
    ///
    /// This sends the `Run` event into the FSM and returns immediately.
    /// Intended for long-running/server flows where lifecycle is driven
    /// externally (e.g. via HTTP control API) rather than by awaiting
    /// `run()` to completion.
    pub async fn start(&self) -> Result<(), FlowError> {
        let current_state = self.current_state();
        tracing::debug!(
            "FlowHandle::start() - Current pipeline state: {:?}",
            current_state
        );
        tracing::debug!("FlowHandle::start() - Sending PipelineEvent::Run to start flow");
        self.send_event(PipelineEvent::Run).await
    }

    /// Run the pipeline and wait for completion
    /// This is the primary method users should call after creating a flow
    pub async fn run(self) -> Result<(), FlowError> {
        // Send the Run event to transition from Materialized to Running
        // This will trigger NotifySourceStart action
        let current_state = self.current_state();
        tracing::debug!(
            "FlowHandle::run() - Current pipeline state: {:?}",
            current_state
        );
        tracing::debug!("FlowHandle::run() - Sending PipelineEvent::Run to start flow");
        self.send_event(PipelineEvent::Run).await?;
        tracing::debug!("FlowHandle::run() - Run event sent, waiting for completion");

        // Capture state receiver before consuming self so we can inspect the terminal state
        let state_rx = self.state_receiver();

        // Now wait for it to complete
        let result = self.wait_for_completion().await;
        tracing::debug!(
            "FlowHandle::run() - wait_for_completion returned: {:?}",
            result
        );

        // Surface aborts/failures instead of letting the example print success on error
        if let Err(e) = result {
            tracing::error!("FlowHandle::run() failed: {}", e);
            return Err(e);
        }

        // Inspect final state to fail fast on pipeline aborts
        let final_state = state_rx.borrow().clone();
        match final_state {
            PipelineState::Failed { reason, .. } => Err(FlowError::ExecutionFailed(Box::new(
                io::Error::other(reason),
            ))),
            PipelineState::AbortRequested { reason, .. } => Err(FlowError::ExecutionFailed(
                Box::new(io::Error::other(format!("{reason:?}"))),
            )),
            _ => Ok(()),
        }
    }

    /// Run the pipeline and wait for completion, returning the metrics exporter
    /// Use this when you need to access metrics after the flow completes
    /// Typically used with finite sources (not infinite sources)
    pub async fn run_with_metrics(
        self,
    ) -> Result<Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>, FlowError> {
        // Send the Run event to transition from Materialized to Running
        // This will trigger NotifySourceStart action
        self.send_event(PipelineEvent::Run).await?;

        // Save metrics exporter before consuming self
        let metrics = self.metrics_exporter.clone();
        let system_journal = self.system_journal.clone();

        // Now wait for it to complete
        self.wait_for_completion().await?;

        // Best-effort: wait for the metrics subsystem to complete its final export.
        //
        // Many tests (and UI clients) assume `/metrics` becomes accurate shortly after
        // pipeline completion; in practice, the metrics aggregator may still be draining.
        // We use the system journal's MetricsCoordination events as a synchronization point.
        if let Some(journal) = system_journal {
            use obzenflow_core::event::system_event::MetricsCoordinationEvent;
            use obzenflow_core::event::SystemEventType;
            use std::time::Duration;

            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                match journal.read_last_n(256).await {
                    Ok(events) => {
                        let drained = events.iter().any(|envelope| {
                            matches!(
                                envelope.event.event,
                                SystemEventType::MetricsCoordination(
                                    MetricsCoordinationEvent::Drained
                                        | MetricsCoordinationEvent::Shutdown
                                )
                            )
                        });
                        if drained {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            journal_error = %e,
                            "Failed to read system journal while waiting for metrics drain"
                        );
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        Ok(metrics)
    }

    /// User-initiated stop request.
    ///
    /// This is distinct from `PipelineEvent::Shutdown` which represents natural
    /// source completion detected by the pipeline supervisor.
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
        self.send_event(PipelineEvent::StopRequested {
            mode: FlowStopMode::Cancel,
            reason: None,
        })
        .await
    }

    /// Cancel due to a graceful stop timeout escalation (`stop_timeout`).
    ///
    /// This is primarily intended for process-level shutdown coordinators
    /// (e.g. SIGTERM handlers) that enforce a deadline and need terminal
    /// lifecycle observability to reflect that timeout.
    #[doc(hidden)]
    pub async fn stop_cancel_timeout(&self) -> Result<(), FlowError> {
        if !self.is_running() {
            return Ok(());
        }
        self.send_event(PipelineEvent::StopRequested {
            mode: FlowStopMode::Cancel,
            reason: Some(STOP_REASON_TIMEOUT.to_string()),
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
        self.send_event(PipelineEvent::StopRequested {
            mode: FlowStopMode::Graceful { timeout },
            reason: None,
        })
        .await
    }

    /// Backwards-compatible alias for `stop()`.
    pub async fn shutdown(&self) -> Result<(), FlowError> {
        self.stop().await
    }

    /// Force shutdown by sending Error event to FSM
    pub async fn abort(&self, reason: &str) -> Result<(), FlowError> {
        self.send_event(PipelineEvent::Error {
            message: format!("Force abort: {reason}"),
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

    /// Get the metrics exporter for concurrent access during flow execution
    ///
    /// This allows starting a metrics server before running the flow,
    /// enabling real-time monitoring of long-running flows.
    /// The exporter is thread-safe and can be accessed concurrently.
    pub fn metrics_exporter(&self) -> Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>> {
        self.metrics_exporter.clone()
    }

    /// Get the flow topology for visualization
    ///
    /// This provides access to the flow's structure (stages and connections)
    /// for visualization tools and monitoring dashboards.
    /// The topology is immutable and thread-safe.
    pub fn topology(&self) -> Option<Arc<Topology>> {
        self.topology.clone()
    }

    /// Get structural middleware stacks per stage (for topology endpoint, FLOWIP-059)
    pub fn middleware_stacks(&self) -> Option<MiddlewareStacks> {
        self.middleware_stacks.clone()
    }

    /// Get structural contract names per edge (for topology endpoint)
    pub fn contract_attachments(&self) -> Option<ContractAttachments> {
        self.contract_attachments.clone()
    }

    /// Get join metadata per stage (for topology endpoint, FLOWIP-082a)
    pub fn join_metadata(&self) -> Option<JoinMetadataMap> {
        self.join_metadata.clone()
    }

    /// Get per-stage subgraph membership (for topology endpoint, FLOWIP-086z-part-2)
    pub fn subgraph_membership(&self) -> Option<StageSubgraphMembershipMap> {
        self.subgraph_membership.clone()
    }

    /// Get the logical subgraph registry (for topology endpoint, FLOWIP-086z-part-2)
    pub fn subgraphs(&self) -> Option<SubgraphRegistry> {
        self.subgraphs.clone()
    }

    /// Get the system journal for lifecycle events (if available)
    pub fn system_journal(&self) -> Option<Arc<dyn Journal<SystemEvent>>> {
        self.system_journal.clone()
    }

    /// Get the user-specified flow name from the flow! macro
    ///
    /// This returns the name provided in the `name:` field of the flow! macro,
    /// which may differ from the auto-generated topology-based name.
    pub fn flow_name(&self) -> &str {
        &self.flow_name
    }

    /// Render metrics based on the wrapped exporter's format
    pub async fn render_metrics(&self) -> Result<String, FlowError> {
        if let Some(ref exporter) = self.metrics_exporter {
            exporter.render_metrics().map_err(|e| {
                FlowError::ExecutionFailed(Box::new(std::io::Error::other(e.to_string())))
            })
        } else {
            Err(FlowError::ExecutionFailed(Box::new(std::io::Error::other(
                "No metrics exporter configured",
            ))))
        }
    }
}

// Custom implementation for SupervisorHandle trait to use FlowError
#[async_trait::async_trait]
impl SupervisorHandle for FlowHandle {
    type Event = PipelineEvent;
    type State = PipelineState;
    type Error = FlowError;

    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error> {
        self.handle.send_event(event).await.map_err(|e| match e {
            HandleError::SupervisorNotRunning => {
                FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Pipeline supervisor is not running",
                )))
            }
            HandleError::SupervisorFailed(msg) => {
                FlowError::ExecutionFailed(Box::new(std::io::Error::other(msg)))
            }
            HandleError::SupervisorPanicked(msg) => FlowError::ExecutionFailed(Box::new(
                std::io::Error::other(format!("Task panicked: {msg}")),
            )),
            _ => FlowError::ExecutionFailed(Box::new(std::io::Error::other(e.to_string()))),
        })
    }

    fn current_state(&self) -> Self::State {
        self.handle.current_state()
    }

    async fn wait_for_completion(self) -> Result<(), Self::Error> {
        self.handle
            .wait_for_completion()
            .await
            .map_err(|e| match e {
                HandleError::SupervisorNotRunning => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Pipeline supervisor is not running",
                    )))
                }
                HandleError::SupervisorFailed(msg) => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::other(msg)))
                }
                HandleError::SupervisorPanicked(msg) => FlowError::ExecutionFailed(Box::new(
                    std::io::Error::other(format!("Task panicked: {msg}")),
                )),
                _ => FlowError::ExecutionFailed(Box::new(std::io::Error::other(e.to_string()))),
            })
    }
}
