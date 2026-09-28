// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public pipeline lifecycle states, controls and submission outcomes.

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

/// Exact public projection of the pipeline's pending and achieved phases.
/// Handles may coalesce observations; acknowledgements retain achieved results.
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    Created,
    Registering,
    InitializingStages,
    StartingConsumers,
    PublishingReady,
    ReadyForRun,
    PublishingStart,
    StartingSources,
    PublishingRunning,
    Running,
    Draining,
    CancellingChildren,
    PublishingTerminal,
    FinalisingMetrics,
    PublishingFinalMarker,
    Drained,
    FailingChildren {
        cause: String,
    },
    Cancelled {
        reason: String,
    },
    Failed {
        reason: String,
        failure_cause: Option<obzenflow_core::event::types::ViolationCause>,
    },
}
impl StateVariant for PipelineState {
    fn variant_name(&self) -> &str {
        match self {
            Self::Created => "Created",
            Self::Registering => "Registering",
            Self::InitializingStages => "InitializingStages",
            Self::StartingConsumers => "StartingConsumers",
            Self::PublishingReady => "PublishingReady",
            Self::ReadyForRun => "ReadyForRun",
            Self::PublishingStart => "PublishingStart",
            Self::StartingSources => "StartingSources",
            Self::PublishingRunning => "PublishingRunning",
            Self::Running => "Running",
            Self::Draining => "Draining",
            Self::CancellingChildren => "CancellingChildren",
            Self::PublishingTerminal => "PublishingTerminal",
            Self::FinalisingMetrics => "FinalisingMetrics",
            Self::PublishingFinalMarker => "PublishingFinalMarker",
            Self::Drained => "Drained",
            Self::FailingChildren { .. } => "FailingChildren",
            Self::Cancelled { .. } => "Cancelled",
            Self::Failed { .. } => "Failed",
        }
    }
}
impl PipelineState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Drained | Self::Cancelled { .. } | Self::Failed { .. }
        )
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
