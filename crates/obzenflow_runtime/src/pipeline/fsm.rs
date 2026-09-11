// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline FSM using obzenflow_fsm
//!
//! This defines the pipeline state machine without the supervision logic

use crate::feed_plan::FeedKey;
use crate::id_conversions::StageIdExt;
use crate::messaging::system_subscription::SystemSubscription;
use crate::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::id::{FlowId, SystemId};
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::{FlowLifecycleMetricsSnapshot, StageMetricsSnapshot};
use obzenflow_core::StageId;
use obzenflow_fsm::{fsm, EventVariant, FsmContext, StateMachine, StateVariant};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Stop intent for externally-initiated shutdown (UI/API/signal).
///
/// This is used internally by the pipeline supervisor to decide whether to
/// short-circuit processing (`Cancel`) or attempt a bounded drain (`Graceful`).
#[derive(Clone, Debug)]
pub enum FlowStopMode {
    /// Stop as quickly as possible (no drain barrier).
    Cancel,
    /// Stop intake and drain backlog up to the given timeout, then cancel.
    Graceful { timeout: Duration },
}

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

/// Latest public projection of the private pipeline FSM.
///
/// `Materialized` and `ReadyForRun` are intentionally separate. Materialized
/// means the runtime objects exist and non-source stages have been told to
/// start. ReadyForRun requires committed non-source `Running` facts and the
/// pipeline's own readiness fact. The watcher may coalesce intermediate states;
/// the system journal remains the durable lifecycle record.
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    /// Initial state before stage resources have been created.
    Created,
    /// Stage resources, subscriptions, and runtime wiring are being created.
    Materializing,
    /// Runtime wiring exists and non-source stages are starting.
    ///
    /// Sources must not start in this state. The materialized supervisor waits
    /// here until every non-source stage has journalled `Running` and the
    /// pipeline has consumed its committed `ReadyForRun` fact.
    Materialized,
    /// All non-source stages have reported `Running`.
    ///
    /// `Start` can be admitted here. This projection also covers authorised
    /// source startup until the pipeline consumes the sources' `Running` facts.
    /// Repeated controls are coalesced by the private FSM.
    ReadyForRun,
    /// The pipeline has consumed the authorised sources' `Running` facts.
    Running,
    /// Source stages have completed and the pipeline is moving toward drain.
    SourceCompleted,
    /// A journalled contract failure has requested abort; resources are settling.
    AbortRequested {
        reason: obzenflow_core::event::types::ViolationCause,
        upstream: Option<StageId>,
    },
    /// Execution or finalisation is still settling.
    Draining,
    /// The FSM has finished resource settlement without a selected failure.
    Drained,
    Failed {
        reason: String,
        failure_cause: Option<obzenflow_core::event::types::ViolationCause>,
    },
}

impl StateVariant for PipelineState {
    fn variant_name(&self) -> &str {
        match self {
            PipelineState::Created => "Created",
            PipelineState::Materializing => "Materializing",
            PipelineState::Materialized => "Materialized",
            PipelineState::ReadyForRun => "ReadyForRun",
            PipelineState::Running => "Running",
            PipelineState::SourceCompleted => "SourceCompleted",
            PipelineState::AbortRequested { .. } => "AbortRequested",
            PipelineState::Draining => "Draining",
            PipelineState::Drained => "Drained",
            PipelineState::Failed { .. } => "Failed",
        }
    }
}

impl PipelineState {
    /// Terminal states: no further pipeline transitions occur.
    pub fn is_terminal(&self) -> bool {
        matches!(self, PipelineState::Drained | PipelineState::Failed { .. })
    }
}

/// Controls callers may submit. The FSM alone admits and classifies them.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum PipelineControl {
    Start,
    Stop { mode: FlowStopMode },
    Abort { reason: String },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PipelineFsmState {
    Created,
    Materializing,
    AwaitingStageReadiness,
    ReadyForRun,
    StartingSources,
    Running,
    SourceCompleted,
    Draining,
    SettlingStages,
    CatchingUpProducers,
    PublishingTerminal,
    FinalisingMetrics,
    PublishingFinalMarker,
    Finished {
        outcome: super::termination::ExecutionOutcome,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PipelineDeadline {
    GracefulStop,
    StageCleanup,
    Metrics,
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineFsmEvent {
    Bootstrap,
    Control(PipelineControl),
    Journal(Box<obzenflow_core::EventEnvelope<SystemEvent>>),
    Deadline(PipelineDeadline),
    PhysicalSettlementSatisfied,
    OperationalFailure { message: String },
}

impl EventVariant for PipelineFsmEvent {
    fn variant_name(&self) -> &str {
        match self {
            Self::Bootstrap => "Bootstrap",
            Self::Control(_) => "Control",
            Self::Journal(_) => "Journal",
            Self::Deadline(_) => "Deadline",
            Self::PhysicalSettlementSatisfied => "PhysicalSettlementSatisfied",
            Self::OperationalFailure { .. } => "OperationalFailure",
        }
    }
}

impl StateVariant for PipelineFsmState {
    fn variant_name(&self) -> &str {
        match self {
            Self::Created => "Created",
            Self::Materializing => "Materializing",
            Self::AwaitingStageReadiness => "AwaitingStageReadiness",
            Self::ReadyForRun => "ReadyForRun",
            Self::StartingSources => "StartingSources",
            Self::Running => "Running",
            Self::SourceCompleted => "SourceCompleted",
            Self::Draining => "Draining",
            Self::SettlingStages => "SettlingStages",
            Self::CatchingUpProducers => "CatchingUpProducers",
            Self::PublishingTerminal => "PublishingTerminal",
            Self::FinalisingMetrics => "FinalisingMetrics",
            Self::PublishingFinalMarker => "PublishingFinalMarker",
            Self::Finished { .. } => "Finished",
        }
    }
}

impl PipelineFsmState {
    pub(crate) fn public_state(&self, ctx: &PipelineContext) -> PipelineState {
        use super::termination::ExecutionOutcome;
        match self {
            Self::Created => PipelineState::Created,
            Self::Materializing => PipelineState::Materializing,
            Self::AwaitingStageReadiness => PipelineState::Materialized,
            Self::ReadyForRun | Self::StartingSources => PipelineState::ReadyForRun,
            Self::Running => PipelineState::Running,
            Self::SourceCompleted => PipelineState::SourceCompleted,
            Self::Draining
            | Self::SettlingStages
            | Self::CatchingUpProducers
            | Self::PublishingTerminal
            | Self::FinalisingMetrics
            | Self::PublishingFinalMarker => match &ctx.progress.abort_cause {
                Some((reason, upstream)) => PipelineState::AbortRequested {
                    reason: reason.clone(),
                    upstream: *upstream,
                },
                None => PipelineState::Draining,
            },
            Self::Finished {
                outcome: ExecutionOutcome::Failed(failure),
            } => PipelineState::Failed {
                reason: failure.reason.clone(),
                failure_cause: failure.cause.clone(),
            },
            Self::Finished { .. } => PipelineState::Drained,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineAction {
    InitialiseStages,
    StartMetricsAggregator,
    StartNonSources,
    StartSources,
    StopSources,
    Publish {
        event: Box<SystemEvent>,
        control: bool,
    },
    CancelStages {
        contract_abort: bool,
    },
    ObserveStages,
    CaptureProducerTail,
    PublishTerminal,
    ObserveMetrics,
    CancelMetrics,
    PublishFinalMarker,
    DrainMetrics,
}

/// Monotonic journal evidence and execution-local admission data.
#[derive(Default)]
pub(crate) struct PipelineProgress {
    pub(super) ready_announced: bool,
    pub(super) all_stages_announced: bool,
    pub(super) sources_authorised: bool,
    pub(super) metrics_ready: bool,
    pub(super) metrics_drain_requested: bool,
    pub(super) metrics_drained: bool,
    pub(super) stages_cancelled: bool,
    pub(super) metrics_cancelled: bool,
    pub(super) journal_failed: bool,
    pub(super) abort_cause: Option<(
        obzenflow_core::event::types::ViolationCause,
        Option<StageId>,
    )>,
    pub(super) cleanup_deadline: Option<std::time::Instant>,
    pub(super) selected_terminal: Option<(SystemEvent, super::termination::ExecutionOutcome)>,
    pub(super) final_marker: Option<obzenflow_core::EventId>,
    pub(super) final_marker_seen: bool,
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
    pub(crate) contract_pairs: HashMap<FeedKey, crate::pipeline::supervisor::ContractEdgeStatus>,

    /// Expected contract feeds derived from topology shape and runtime feed plan.
    pub(crate) expected_contract_pairs: HashSet<FeedKey>,

    /// Expected source stages (used to decide when to drain on success)
    pub(crate) expected_sources: Vec<StageId>,

    /// Metrics aggregator handle (for coordinated shutdown/drain).
    pub(crate) resources: super::resources::PipelineResources,
    pub(crate) progress: PipelineProgress,
    /// Last known per-stage lifecycle metrics (for flow rollup)
    pub(crate) stage_lifecycle_metrics: HashMap<StageId, StageMetricsSnapshot>,

    /// Flow start time for duration calculation
    pub(crate) flow_start_time: Option<std::time::Instant>,

    /// Last system event ID observed via completion_subscription (for tail reconciliation)
    pub(crate) last_system_event_id_seen: Option<obzenflow_core::EventId>,

    pub(crate) stop_intent: StopIntent,

    pub(crate) termination: super::termination::TerminationState,

    /// FLOWIP-010: build-resolved `contracts.source_contract_strict_mode`.
    pub(crate) source_contract_strict: crate::pipeline::supervisor::SourceContractStrictMode,

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

/// Compute flow-level lifecycle metrics from per-stage snapshots in the context.
pub(crate) fn compute_flow_lifecycle_metrics(
    context: &PipelineContext,
) -> FlowLifecycleMetricsSnapshot {
    use obzenflow_core::event::context::StageType as CoreStageType;

    let mut events_in_total: u64 = 0;
    let mut events_out_total: u64 = 0;
    let mut errors_total: u64 = 0;

    for (stage_id, snapshot) in &context.stage_lifecycle_metrics {
        // Map core StageId to topology StageId
        let topo_stage_id = stage_id.to_topology_id();

        // Look up stage info to determine semantic type
        if let Some(stage_info) = context.topology.stages().find(|s| s.id == topo_stage_id) {
            // Map topology StageType to core StageType (they share the same shape)
            let core_type = match stage_info.stage_type {
                obzenflow_topology::StageType::FiniteSource => CoreStageType::FiniteSource,
                obzenflow_topology::StageType::InfiniteSource => CoreStageType::InfiniteSource,
                obzenflow_topology::StageType::Transform => CoreStageType::Transform,
                obzenflow_topology::StageType::Sink => CoreStageType::Sink,
                obzenflow_topology::StageType::Stateful => CoreStageType::Stateful,
                obzenflow_topology::StageType::Join => CoreStageType::Join,
            };

            match core_type {
                CoreStageType::FiniteSource | CoreStageType::InfiniteSource => {
                    events_in_total =
                        events_in_total.saturating_add(snapshot.events_processed_total);
                }
                CoreStageType::Sink => {
                    events_out_total =
                        events_out_total.saturating_add(snapshot.events_processed_total);
                }
                _ => {}
            }
        }

        // Always include errors for all stages
        errors_total = errors_total.saturating_add(snapshot.errors_total);
    }

    FlowLifecycleMetricsSnapshot {
        events_in_total,
        events_out_total,
        errors_total,
    }
}

/// Build exact named graph cuts from durable topology edge-port bindings
/// (FLOWIP-128a B3). `collapsible` is presentation metadata and never gates
/// backend projection.
pub(crate) fn composite_boundaries_from_topology(
    topology: &obzenflow_topology::Topology,
) -> Vec<obzenflow_core::metrics::CompositeBoundary> {
    use crate::id_conversions::StageIdExt;
    use obzenflow_core::id::{CompositeId, StageId};
    use obzenflow_core::metrics::{
        BoundaryDirection, CompositeBoundary, CompositeBoundaryEdge, CompositeBoundaryPort,
    };

    let mut boundaries: Vec<_> = topology
        .subgraphs()
        .iter()
        .map(|subgraph| {
            let members = subgraph
                .member_stage_ids
                .iter()
                .map(|id| StageId::from_topology_id(*id))
                .collect();

            let mut ports: Vec<_> = subgraph
                .boundary_ports
                .iter()
                .map(|port| CompositeBoundaryPort {
                    name: port.name.clone(),
                    direction: match port.direction {
                        obzenflow_topology::PortDirection::Input => BoundaryDirection::Inbound,
                        obzenflow_topology::PortDirection::Output => BoundaryDirection::Outbound,
                    },
                    member: StageId::from_topology_id(port.member_stage_id),
                    payload_event_types: port
                        .payload_event_types
                        .iter()
                        .cloned()
                        .map(obzenflow_core::EventType::from)
                        .collect(),
                })
                .collect();
            ports.sort_by(|left, right| {
                (left.direction.as_str(), left.name.as_str())
                    .cmp(&(right.direction.as_str(), right.name.as_str()))
            });

            let mut edges = Vec::new();
            for edge in topology.edges() {
                for port_ref in &edge.composite_ports {
                    if port_ref.subgraph_id != subgraph.subgraph_id {
                        continue;
                    }
                    let Some(port) = ports.iter().find(|port| port.name == port_ref.port_name)
                    else {
                        // Topology validation rejects this before runtime build.
                        continue;
                    };
                    let upstream = StageId::from_topology_id(edge.from);
                    let downstream = StageId::from_topology_id(edge.to);
                    let (member, peer) = match port.direction {
                        BoundaryDirection::Inbound => (downstream, upstream),
                        BoundaryDirection::Outbound => (upstream, downstream),
                    };
                    edges.push(CompositeBoundaryEdge {
                        port: port.name.clone(),
                        direction: port.direction,
                        member,
                        peer,
                        upstream,
                        downstream,
                    });
                }
            }
            edges.sort_by(|left, right| {
                (
                    left.direction.as_str(),
                    left.port.as_str(),
                    left.upstream,
                    left.downstream,
                )
                    .cmp(&(
                        right.direction.as_str(),
                        right.port.as_str(),
                        right.upstream,
                        right.downstream,
                    ))
            });

            CompositeBoundary {
                composite_id: CompositeId::new(subgraph.subgraph_id.clone()),
                members,
                ports,
                edges,
            }
        })
        .collect();
    boundaries.sort_by(|left, right| left.composite_id.cmp(&right.composite_id));
    boundaries
}

pub(super) fn record_stage_completion(
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

pub(crate) type PipelineFsm =
    StateMachine<PipelineFsmState, PipelineFsmEvent, PipelineContext, PipelineAction>;

pub(crate) fn build_pipeline_fsm_with_initial(initial: PipelineFsmState) -> PipelineFsm {
    use super::transitions::{bootstrap, control, deadline, failure, journal, settled};
    fsm! {
        state: PipelineFsmState;
        event: PipelineFsmEvent;
        context: PipelineContext;
        action: PipelineAction;
        initial: initial;
        state PipelineFsmState::Created {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::Materializing {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::AwaitingStageReadiness {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::ReadyForRun {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::StartingSources {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::Running {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::SourceCompleted {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::Draining {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::SettlingStages {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::CatchingUpProducers {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::PublishingTerminal {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::FinalisingMetrics {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::PublishingFinalMarker {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
        state PipelineFsmState::Finished {
            on PipelineFsmEvent::Bootstrap => bootstrap;
            on PipelineFsmEvent::Control => control;
            on PipelineFsmEvent::Journal => journal;
            on PipelineFsmEvent::Deadline => deadline;
            on PipelineFsmEvent::OperationalFailure => failure;
            on PipelineFsmEvent::PhysicalSettlementSatisfied => settled;
        }
    }
}

#[cfg(test)]
#[path = "fsm_lifecycle_tests.rs"]
mod fsm_lifecycle_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_conversions::StageIdExt;
    use obzenflow_topology::{
        BoundaryPortSpec, CompositePortRef, DirectedEdge, EdgeKind, PortDirection, StageInfo,
        StageType as TopologyStageType, SubgraphInternalEdge, Topology, TopologySubgraphInfo,
    };

    #[test]
    fn terminal_states_are_exactly_the_locked_set() {
        assert!(PipelineState::Drained.is_terminal());
        assert!(PipelineState::Failed {
            reason: "x".to_string(),
            failure_cause: None
        }
        .is_terminal());
        assert!(!PipelineState::Created.is_terminal());
        assert!(!PipelineState::Running.is_terminal());
        assert!(!PipelineState::Draining.is_terminal());
        assert!(!PipelineState::SourceCompleted.is_terminal());
    }

    #[test]
    fn runtime_boundary_is_the_named_multi_port_cut_even_when_not_collapsible() {
        let ids: Vec<_> = (1_u128..=6)
            .map(|value| obzenflow_topology::StageId::from_bytes(value.to_be_bytes()))
            .collect();
        let (producer, entry, completed, failed, ok_sink, err_sink) =
            (ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]);
        let stages = vec![
            StageInfo::new(producer, "producer", TopologyStageType::FiniteSource),
            StageInfo::new(entry, "entry", TopologyStageType::Transform),
            StageInfo::new(completed, "completed", TopologyStageType::Transform),
            StageInfo::new(failed, "failed", TopologyStageType::Transform),
            StageInfo::new(ok_sink, "ok", TopologyStageType::Sink),
            StageInfo::new(err_sink, "err", TopologyStageType::Sink),
        ];
        let subgraph_id = "saga:checkout";
        let edges = vec![
            DirectedEdge::new(producer, entry, EdgeKind::Forward)
                .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "commands")]),
            DirectedEdge::new(entry, completed, EdgeKind::Forward),
            DirectedEdge::new(entry, failed, EdgeKind::Forward),
            DirectedEdge::new(completed, ok_sink, EdgeKind::Forward)
                .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "completed")]),
            DirectedEdge::new(failed, err_sink, EdgeKind::Forward)
                .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "failed")]),
        ];
        let subgraph = TopologySubgraphInfo::new(
            subgraph_id,
            "saga",
            "checkout",
            "checkout",
            vec![entry, completed, failed],
            vec![
                SubgraphInternalEdge::new(entry, completed, "terminal"),
                SubgraphInternalEdge::new(entry, failed, "terminal"),
            ],
            vec![entry],
            vec![completed, failed],
            false,
        )
        .with_boundary_ports(vec![
            BoundaryPortSpec::new(
                "commands",
                PortDirection::Input,
                entry,
                vec!["checkout.command.v1".into()],
                true,
            ),
            BoundaryPortSpec::new(
                "completed",
                PortDirection::Output,
                completed,
                vec!["checkout.completed.v1".into()],
                true,
            ),
            BoundaryPortSpec::new(
                "failed",
                PortDirection::Output,
                failed,
                vec!["checkout.failed.v1".into()],
                false,
            ),
        ]);
        let topology = Topology::new_unvalidated(stages, edges)
            .unwrap()
            .with_subgraphs(vec![subgraph]);

        let boundaries = composite_boundaries_from_topology(&topology);
        assert_eq!(boundaries.len(), 1);
        let boundary = &boundaries[0];
        assert_eq!(boundary.ports.len(), 3);
        assert_eq!(boundary.edges.len(), 3);
        assert!(boundary.edges.iter().any(|edge| {
            edge.port == "completed"
                && edge.member == obzenflow_core::StageId::from_topology_id(completed)
        }));
        assert!(boundary.edges.iter().any(|edge| {
            edge.port == "failed"
                && edge.member == obzenflow_core::StageId::from_topology_id(failed)
        }));
    }

    #[test]
    fn stop_intent_cancel_sets_defaults() {
        let mut intent = StopIntent::default();
        let outcome = intent.apply_request(FlowStopMode::Cancel, None);

        assert!(intent.requested);
        assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
        assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));
        assert!(intent.deadline.is_none());

        match outcome {
            StopRequestOutcome::Applied { reason_label, .. } => {
                assert_eq!(reason_label, STOP_REASON_USER_STOP);
            }
            StopRequestOutcome::Ignored => {
                panic!("cancel request should never be ignored");
            }
        }
    }

    #[test]
    fn stop_intent_graceful_sets_deadline() {
        let mut intent = StopIntent::default();
        let timeout = Duration::from_secs(3);
        let before = std::time::Instant::now();

        let _ = intent.apply_request(FlowStopMode::Graceful { timeout }, None);
        let after = std::time::Instant::now();

        assert!(intent.requested);
        assert!(matches!(
            intent.mode,
            Some(FlowStopMode::Graceful { timeout: t }) if t == timeout
        ));
        assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));

        let deadline = intent
            .deadline
            .expect("graceful stop should set a deadline");
        assert!(deadline >= before + timeout);
        assert!(deadline <= after + timeout);
    }

    #[test]
    fn stop_intent_cancel_overrides_graceful() {
        let mut intent = StopIntent::default();
        let _ = intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::from_secs(5),
            },
            None,
        );
        assert!(intent.deadline.is_some());

        let _ = intent.apply_request(FlowStopMode::Cancel, None);
        assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
        assert!(intent.deadline.is_none());
    }

    #[test]
    fn stop_intent_graceful_is_ignored_after_cancel() {
        let mut intent = StopIntent::default();
        let _ = intent.apply_request(FlowStopMode::Cancel, None);
        let outcome = intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::from_secs(1),
            },
            None,
        );

        assert!(matches!(outcome, StopRequestOutcome::Ignored));
        assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
        assert!(intent.deadline.is_none());
    }

    #[test]
    fn stop_intent_expired_timeout_preserves_deadline() {
        let mut intent = StopIntent::default();
        let _ = intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::ZERO,
            },
            Some("first_reason".to_string()),
        );
        assert_eq!(intent.reason.as_deref(), Some("first_reason"));
        let original_deadline = intent.deadline;

        let _ = intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.to_string()));
        assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_TIMEOUT));
        assert_eq!(
            intent.deadline, original_deadline,
            "timeout escalation must preserve the graceful-stop deadline"
        );
    }

    #[test]
    fn first_graceful_deadline_wins_in_both_duration_orders() {
        for (first, second) in [(1, 60), (60, 1)] {
            let mut intent = StopIntent::default();
            intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: Duration::from_secs(first),
                },
                Some("first".into()),
            );
            let first_deadline = intent.deadline;
            assert!(matches!(
                intent.apply_request(
                    FlowStopMode::Graceful {
                        timeout: Duration::from_secs(second)
                    },
                    Some("second".into()),
                ),
                StopRequestOutcome::Ignored
            ));
            assert_eq!(intent.deadline, first_deadline);
            assert_eq!(intent.reason.as_deref(), Some("first"));
        }
    }

    #[test]
    fn cancel_is_absorbing_including_reason_and_admission_time() {
        let mut intent = StopIntent::default();
        intent.apply_request(FlowStopMode::Cancel, Some("explicit_cancel".into()));
        for (mode, reason) in [
            (FlowStopMode::Cancel, "duplicate"),
            (FlowStopMode::Cancel, STOP_REASON_TIMEOUT),
            (
                FlowStopMode::Graceful {
                    timeout: Duration::ZERO,
                },
                "late_grace",
            ),
        ] {
            assert!(matches!(
                intent.apply_request(mode, Some(reason.into())),
                StopRequestOutcome::Ignored
            ));
            assert_eq!(intent.reason.as_deref(), Some("explicit_cancel"));
        }
    }

    #[test]
    fn timeout_cancel_requires_an_expired_graceful_stop() {
        let mut intent = StopIntent::default();
        assert!(matches!(
            intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
            StopRequestOutcome::Ignored
        ));
        assert!(!intent.requested);
        intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::from_secs(60),
            },
            None,
        );
        let deadline = intent.deadline;
        assert!(matches!(
            intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
            StopRequestOutcome::Ignored
        ));
        assert_eq!(intent.deadline, deadline);
        assert!(matches!(intent.mode, Some(FlowStopMode::Graceful { .. })));
    }

    #[test]
    fn pipeline_supervisor_has_no_inline_fsm_definition() {
        const SUPERVISOR_MOD: &str = include_str!("supervisor/mod.rs");
        assert!(
            !SUPERVISOR_MOD.contains("fsm!"),
            "pipeline supervisor must not contain an inline fsm! definition; keep the FSM single-sourced in pipeline/fsm.rs"
        );
    }

    #[test]
    fn record_stage_completion_is_idempotent_for_duplicate_terminal_events() {
        let stage_a = StageId::new();
        let stage_b = StageId::new();
        let mut completed = vec![stage_a];

        let (is_new, all_completed_now) = record_stage_completion(&mut completed, stage_b, 2);
        assert!(is_new);
        assert!(all_completed_now);
        assert_eq!(completed, vec![stage_a, stage_b]);

        let (is_new, all_completed_now) = record_stage_completion(&mut completed, stage_b, 2);
        assert!(!is_new);
        assert!(!all_completed_now);
        assert_eq!(completed, vec![stage_a, stage_b]);
    }
}
