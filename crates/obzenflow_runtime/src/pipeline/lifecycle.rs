// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public pipeline lifecycle states, controls and submission outcomes.

use obzenflow_core::StageId;
use obzenflow_fsm::StateVariant;
use std::time::Duration;

/// Stop intent for externally-initiated shutdown (UI/API/signal).
///
/// The pipeline FSM admits these requests to cancel processing (`Cancel`) or
/// attempt a bounded drain (`Graceful`).
#[derive(Clone, Debug)]
pub enum FlowStopMode {
    /// Stop as quickly as possible (no drain barrier).
    Cancel,
    /// Stop intake and drain backlog up to the given timeout, then cancel.
    Graceful { timeout: Duration },
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
