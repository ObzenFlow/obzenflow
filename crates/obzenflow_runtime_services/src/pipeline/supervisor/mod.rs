// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline Supervisor
//!
//! Supervisor responsibilities:
//! - own the pipeline FSM and dispatch loop (via `SelfSupervised`)
//! - poll subscriptions and translate I/O into FSM events
//! - execute orchestration side effects via FSM actions
//!
//! FLOWIP-051m-part-2: align pipeline supervision structure and single-source the pipeline FSM.

mod abort_requested;
mod common;
mod created;
mod draining;
mod materialized;
mod materializing;
mod running;
mod source_completed;
mod terminal;
#[cfg(test)]
mod tests;

use super::fsm::{FlowStopMode, PipelineAction, PipelineContext, PipelineEvent, PipelineState};
use crate::id_conversions::StageIdExt;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, SelfSupervised,
};
use obzenflow_core::event::types::{SeqNo, ViolationCause};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::{id::SystemId, StageId};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const IDLE_BACKOFF_MS: u64 = 10;
const DRAIN_LIVENESS_MAX_IDLE: u64 = 100;

pub(super) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Pipeline supervisor - manages the lifecycle of a pipeline.
pub(crate) struct PipelineSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System ID for this pipeline (used for writer_id and lifecycle events)
    pub(crate) system_id: SystemId,

    /// System journal for pipeline orchestration events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Throttled logging for barrier snapshots during drain
    pub(crate) last_barrier_log: Option<Instant>,

    /// Throttled logging while waiting for external Run in startup_mode=manual.
    pub(crate) last_manual_wait_log: Option<Instant>,

    /// Idle iterations observed during draining (for liveness guard)
    pub(crate) drain_idle_iters: u64,
}

/// Strictness mode for source at-least-once contracts.
///
/// This is a minimal, flow-wide toggle for how contract failures on
/// *source* edges influence pipeline behaviour:
/// - `Abort` (default): any failed source contract aborts the pipeline.
/// - `Warn`: failures are logged and surfaced via contract events, but
///   do not cause a pipeline abort. This is intended as a transitional
///   mode until full contract strictness plumbing lands in 090d.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceContractStrictMode {
    Abort,
    Warn,
}

fn source_contract_mode() -> SourceContractStrictMode {
    use std::sync::OnceLock;

    static MODE: OnceLock<SourceContractStrictMode> = OnceLock::new();

    *MODE.get_or_init(|| {
        match std::env::var("OBZENFLOW_SOURCE_CONTRACT_STRICT_MODE") {
            Ok(val) => match val.to_ascii_lowercase().as_str() {
                "warn" => SourceContractStrictMode::Warn,
                // Treat any other explicit value as Abort to avoid surprises.
                _ => SourceContractStrictMode::Abort,
            },
            Err(_) => SourceContractStrictMode::Abort,
        }
    })
}

/// Startup mode for the pipeline supervisor.
///
/// By default the supervisor will automatically transition from
/// Materialized → Running once all non-source stages report `Running`.
/// When OBZENFLOW_STARTUP_MODE=manual is set (by FlowApplication in
/// server/UI mode), the supervisor will *not* auto-run; it will remain
/// Materialized until an explicit `Run` event is received from the
/// external FlowHandle (e.g. via /api/flow/control Play).
#[inline]
fn startup_mode_manual() -> bool {
    use std::sync::OnceLock;

    static MANUAL: OnceLock<bool> = OnceLock::new();

    *MANUAL.get_or_init(|| match std::env::var("OBZENFLOW_STARTUP_MODE") {
        Ok(val) => val.eq_ignore_ascii_case("manual"),
        Err(_) => false,
    })
}

/// Helper used to decide whether a given edge should be treated as
/// gating for the purposes of contract-driven pipeline aborts.
#[inline]
fn is_gating_edge_for_contract(is_source: bool, mode: SourceContractStrictMode) -> bool {
    // Non-source edges are always gating; source edges are gating
    // only when strict mode is configured to Abort.
    !is_source || matches!(mode, SourceContractStrictMode::Abort)
}

impl crate::supervised_base::base::Supervisor for PipelineSupervisor {
    type State = PipelineState;
    type Event = PipelineEvent;
    type Context = PipelineContext;
    type Action = PipelineAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        crate::pipeline::fsm::build_pipeline_fsm_with_initial(initial_state)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ExternalEventPolicy for PipelineSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        match state {
            PipelineState::Created => ExternalEventMode::Block,
            PipelineState::Materializing => ExternalEventMode::Ignore,
            PipelineState::Drained | PipelineState::Failed { .. } => ExternalEventMode::Ignore,
            _ => ExternalEventMode::Poll,
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, PipelineState::Drained | PipelineState::Failed { .. }) {
            None
        } else {
            Some(PipelineEvent::Error {
                message: "External control channel closed".to_string(),
            })
        }
    }
}

#[async_trait::async_trait]
impl SelfSupervised for PipelineSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    fn event_for_action_error(&self, msg: String) -> PipelineEvent {
        PipelineEvent::Error { message: msg }
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Terminal completion event is written by the FSM via PipelineAction::WritePipelineCompleted.
        // Here we emit a lightweight "drained" lifecycle marker for observability.
        let drained = SystemEvent::new(
            self.writer_id(),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::Drained,
            ),
        );
        if let Err(e) = self.system_journal.append(drained, None).await {
            tracing::error!(
                pipeline = %self.name,
                journal_error = %e,
                "Failed to write pipeline drained event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut PipelineContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            PipelineState::Created => created::dispatch_created(self, context).await,
            PipelineState::Materializing => {
                materializing::dispatch_materializing(self, context).await
            }
            PipelineState::Materialized => materialized::dispatch_materialized(self, context).await,
            PipelineState::Running => running::dispatch_running(self, context).await,
            PipelineState::SourceCompleted => {
                source_completed::dispatch_source_completed(self, context).await
            }
            PipelineState::Draining => draining::dispatch_draining(self, context).await,
            PipelineState::Drained => terminal::dispatch_drained(self, context).await,
            PipelineState::Failed {
                reason,
                failure_cause,
            } => terminal::dispatch_failed(self, context, reason, failure_cause).await,
            PipelineState::AbortRequested { reason, upstream } => {
                abort_requested::dispatch_abort_requested(self, reason, upstream).await
            }
        }
    }
}

impl PipelineSupervisor {
    /// Best-effort reconciliation of per-stage lifecycle metrics using tail system events.
    ///
    /// Reads only system events that causally follow the last system event observed via
    /// the completion subscription and updates `stage_lifecycle_metrics` with any
    /// terminal wide lifecycle snapshots found there.
    async fn reconcile_stage_metrics_from_tail(
        &self,
        context: &mut PipelineContext,
    ) -> Result<(), String> {
        let last_id = match &context.last_system_event_id_seen {
            Some(id) => *id,
            None => {
                // No prior system events recorded; nothing to reconcile.
                return Ok(());
            }
        };

        let tail_events = self
            .system_journal
            .read_causally_after(&last_id)
            .await
            .map_err(|e| format!("Failed to read tail system events: {e}"))?;

        if tail_events.is_empty() {
            return Ok(());
        }

        for envelope in tail_events.iter() {
            if let obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event } =
                &envelope.event.event
            {
                match event {
                    obzenflow_core::event::StageLifecycleEvent::Completed { metrics: Some(m) }
                    | obzenflow_core::event::StageLifecycleEvent::Cancelled {
                        metrics: Some(m),
                        ..
                    }
                    | obzenflow_core::event::StageLifecycleEvent::Failed {
                        metrics: Some(m), ..
                    } => {
                        context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                    }
                    obzenflow_core::event::StageLifecycleEvent::Draining { metrics: Some(m) } => {
                        context
                            .stage_lifecycle_metrics
                            .entry(*stage_id)
                            .or_insert_with(|| m.clone());
                    }
                    _ => {}
                }
            }
        }

        if let Some(last_envelope) = tail_events.last() {
            context.last_system_event_id_seen = Some(last_envelope.event.id);
        }

        Ok(())
    }

    /// If any contract edge has an explicit failure recorded, return an abort directive.
    fn missing_contract_abort(
        &self,
        context: &PipelineContext,
    ) -> Option<EventLoopDirective<PipelineEvent>> {
        let seen = &context.contract_pairs;

        // Find any edge with an explicit failure (contract violated).
        if let Some(((upstream, reader), status)) =
            seen.iter().find(|((upstream, _reader), status)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                is_gating && !status.is_passed()
            })
        {
            let upstream_name = context
                .topology
                .stage_name(upstream.to_topology_id())
                .unwrap_or("unknown")
                .to_string();
            let reader_name = context
                .topology
                .stage_name(reader.to_topology_id())
                .unwrap_or("unknown")
                .to_string();

            // Prefer the recorded violation cause, fall back to a generic label.
            let reason = status
                .reason
                .clone()
                .unwrap_or_else(|| ViolationCause::Other("contract_failed".into()));

            tracing::error!(
                ?upstream,
                ?reader,
                upstream_name,
                reader_name,
                "Contract edge recorded as failed; aborting pipeline based on explicit contract violation"
            );

            Some(EventLoopDirective::Transition(PipelineEvent::Abort {
                reason,
                upstream: Some(*upstream),
            }))
        } else {
            None
        }
    }

    /// Check if all stages have completed and all contract pairs are satisfied.
    fn all_stages_and_contracts_complete(&self, context: &PipelineContext) -> bool {
        let completed = context.completed_stages.len();
        let total = context.topology.num_stages();

        if completed < total {
            return false;
        }

        let seen = &context.contract_pairs;

        // Success requires that no *gating* contract edge has an explicit failure recorded.
        // Missing contract evidence is tolerated here; it is surfaced via logs/metrics
        // but does not block drain at the transport-contract layer. Source edges configured
        // in warn-only mode are treated as non-gating for this check.
        !seen.iter().any(|((upstream, _reader), status)| {
            let is_source = context.expected_sources.contains(upstream);
            let mode = source_contract_mode();
            let is_gating = is_gating_edge_for_contract(is_source, mode);
            is_gating && !status.is_passed()
        })
    }

    /// Synthesize and write AllStagesCompleted when we know we are done.
    async fn write_all_stages_completed(&self, _context: &PipelineContext) -> Result<(), String> {
        let event = SystemEvent::new(
            WriterId::from(self.system_id),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { metrics: None },
            ),
        );
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write AllStagesCompleted: {e}"))
    }

    /// Snapshot the current drain barrier state for logging/inspection.
    fn barrier_snapshot(&self, context: &PipelineContext) -> BarrierSnapshot {
        let completed: Vec<StageId> = context.completed_stages.clone();
        let expected_stages: Vec<StageId> = context
            .topology
            .stages()
            .map(|s| StageId::from_topology_id(s.id))
            .collect();
        let pending_stages: Vec<StageId> = expected_stages
            .iter()
            .copied()
            .filter(|id| !completed.contains(id))
            .collect();

        let expected_contracts = context.expected_contract_pairs.clone();
        let seen = &context.contract_pairs;
        let missing_contracts: Vec<(StageId, StageId)> = expected_contracts
            .iter()
            .filter(|(upstream, reader)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                if !is_gating {
                    return false;
                }
                !matches!(seen.get(&(*upstream, *reader)), Some(status) if status.is_passed())
            })
            .copied()
            .collect();

        let total_contracts = expected_contracts.len();
        let satisfied_contracts = expected_contracts
            .iter()
            .filter(|(upstream, reader)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                if !is_gating {
                    return false;
                }
                matches!(seen.get(&(*upstream, *reader)), Some(status) if status.is_passed())
            })
            .count();

        BarrierSnapshot {
            pending_stages,
            missing_contracts,
            completed: completed.len(),
            total: expected_stages.len(),
            satisfied_contracts,
            total_contracts,
        }
    }

    /// Throttle barrier logging to avoid spamming the drain loop.
    fn should_log_barrier(&mut self) -> bool {
        let now = Instant::now();
        match self.last_barrier_log {
            Some(last) if now.duration_since(last) < Duration::from_secs(1) => false,
            _ => {
                self.last_barrier_log = Some(now);
                true
            }
        }
    }

    /// Throttle "waiting for external Run" logging in startup_mode=manual.
    fn should_log_manual_wait(&mut self) -> bool {
        let now = Instant::now();
        match self.last_manual_wait_log {
            Some(last) if now.duration_since(last) < Duration::from_secs(5) => false,
            _ => {
                self.last_manual_wait_log = Some(now);
                true
            }
        }
    }
}

/// Lightweight snapshot of drain barrier progress for diagnostics.
#[derive(Debug)]
struct BarrierSnapshot {
    pending_stages: Vec<StageId>,
    missing_contracts: Vec<(StageId, StageId)>,
    completed: usize,
    total: usize,
    satisfied_contracts: usize,
    total_contracts: usize,
}

/// Status for a contract edge (upstream -> reader).
#[derive(Clone, Debug, Default)]
pub struct ContractEdgeStatus {
    passed: bool,
    reason: Option<ViolationCause>,
    reader_seq: Option<SeqNo>,
    advertised_writer_seq: Option<SeqNo>,
}

impl ContractEdgeStatus {
    pub(crate) fn passed(reader_seq: Option<SeqNo>, advertised_writer_seq: Option<SeqNo>) -> Self {
        Self {
            passed: true,
            reason: None,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn failed(
        reason: Option<ViolationCause>,
        reader_seq: Option<SeqNo>,
        advertised_writer_seq: Option<SeqNo>,
    ) -> Self {
        Self {
            passed: false,
            reason,
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
