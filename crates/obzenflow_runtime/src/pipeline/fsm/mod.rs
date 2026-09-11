// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Canonical pipeline FSM construction and transition table.
//!
//! Each live phase observes the original journal through its named decision.
//! Internal bootstrap, deadlines and settlement inputs have explicit admission.
//! Only stale caller controls use the selective unhandled policy.

mod actions;
pub(super) mod context;
mod guards;
mod journal;
mod model;
mod transitions;

pub(crate) use actions::PipelineAction;
pub(crate) use context::PipelineContext;
pub(crate) use model::{PipelineDeadline, PipelineFsmEvent, PipelineFsmState};

use obzenflow_fsm::{fsm, StateMachine};

pub(crate) type PipelineFsm =
    StateMachine<PipelineFsmState, PipelineFsmEvent, PipelineContext, PipelineAction>;

pub(crate) fn build_pipeline_fsm_with_initial(initial: PipelineFsmState) -> PipelineFsm {
    fsm! {
        state: PipelineFsmState;
        event: PipelineFsmEvent;
        context: PipelineContext;
        action: PipelineAction;
        initial: initial;

        unhandled => transitions::unhandled;
        state PipelineFsmState::Created {
            on PipelineFsmEvent::Bootstrap => transitions::bootstrap;
            on PipelineFsmEvent::GracefulStop => transitions::stop_before_start;
            on PipelineFsmEvent::Cancel => transitions::stop_before_start;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::created;
        }
        state PipelineFsmState::Materializing {
            on PipelineFsmEvent::PhysicalSettlementSatisfied => transitions::initialisation_delivered;
            on PipelineFsmEvent::GracefulStop => transitions::stop_before_start;
            on PipelineFsmEvent::Cancel => transitions::stop_before_start;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::materializing;
        }
        state PipelineFsmState::AwaitingStageReadiness {
            on PipelineFsmEvent::GracefulStop => transitions::stop_before_start;
            on PipelineFsmEvent::Cancel => transitions::stop_before_start;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::awaiting_readiness;
        }
        state PipelineFsmState::ReadyForRun {
            on PipelineFsmEvent::Start => transitions::start;
            on PipelineFsmEvent::GracefulStop => transitions::stop_before_start;
            on PipelineFsmEvent::Cancel => transitions::stop_before_start;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::ready_for_run;
        }
        state PipelineFsmState::StartingSources {
            on PipelineFsmEvent::GracefulStop => transitions::begin_graceful_drain;
            on PipelineFsmEvent::Cancel => transitions::cancel_and_settle;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::starting_sources;
        }
        state PipelineFsmState::Running {
            on PipelineFsmEvent::GracefulStop => transitions::begin_graceful_drain;
            on PipelineFsmEvent::Cancel => transitions::cancel_and_settle;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::running;
        }
        state PipelineFsmState::SourceCompleted {
            on PipelineFsmEvent::GracefulStop => transitions::begin_graceful_drain;
            on PipelineFsmEvent::Cancel => transitions::cancel_and_settle;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::source_completed;
        }
        state PipelineFsmState::Draining {
            on PipelineFsmEvent::GracefulStop => transitions::begin_graceful_drain;
            on PipelineFsmEvent::Cancel => transitions::cancel_and_settle;
            on PipelineFsmEvent::GracefulStopExpired => transitions::cancel_and_settle;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::draining;
        }
        state PipelineFsmState::SettlingStages {
            on PipelineFsmEvent::PhysicalSettlementSatisfied => transitions::stage_owners_settled;
            on PipelineFsmEvent::GracefulStop => transitions::stop_during_settlement;
            on PipelineFsmEvent::Cancel => transitions::stop_during_settlement;
            on PipelineFsmEvent::GracefulStopExpired => transitions::stop_during_settlement;
            on PipelineFsmEvent::StageCleanupExpired => transitions::expire_stage_cleanup;
            on PipelineFsmEvent::Abort => transitions::failure_before_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_before_terminal;
            on PipelineFsmEvent::Journal => journal::settling_stages;
        }
        state PipelineFsmState::CatchingUpProducers {
            on PipelineFsmEvent::PhysicalSettlementSatisfied => transitions::producer_tail_reached;
            on PipelineFsmEvent::GracefulStop => transitions::stop_during_settlement;
            on PipelineFsmEvent::Cancel => transitions::stop_during_settlement;
            on PipelineFsmEvent::GracefulStopExpired => transitions::stop_during_settlement;
            on PipelineFsmEvent::Abort => transitions::failure_during_catchup;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_during_catchup;
            on PipelineFsmEvent::Journal => journal::catching_up_producers;
        }
        state PipelineFsmState::PublishingTerminal {
            on PipelineFsmEvent::GracefulStop => transitions::stop_during_settlement;
            on PipelineFsmEvent::Cancel => transitions::stop_during_settlement;
            on PipelineFsmEvent::GracefulStopExpired => transitions::stop_during_settlement;
            on PipelineFsmEvent::MetricsExpired => transitions::expire_metrics;
            on PipelineFsmEvent::Abort => transitions::failure_after_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_after_terminal;
            on PipelineFsmEvent::Journal => journal::publishing_terminal;
        }
        state PipelineFsmState::FinalisingMetrics {
            on PipelineFsmEvent::PhysicalSettlementSatisfied => transitions::metrics_owner_settled;
            on PipelineFsmEvent::GracefulStop => transitions::stop_during_settlement;
            on PipelineFsmEvent::Cancel => transitions::stop_during_settlement;
            on PipelineFsmEvent::GracefulStopExpired => transitions::stop_during_settlement;
            on PipelineFsmEvent::MetricsExpired => transitions::expire_metrics;
            on PipelineFsmEvent::Abort => transitions::failure_after_terminal;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_after_terminal;
            on PipelineFsmEvent::Journal => journal::finalising_metrics;
        }
        state PipelineFsmState::PublishingFinalMarker {
            on PipelineFsmEvent::PhysicalSettlementSatisfied => transitions::final_marker_settled;
            on PipelineFsmEvent::OperationalFailure => transitions::failure_after_terminal;
            on PipelineFsmEvent::Journal => journal::publishing_final_marker;
        }
        state PipelineFsmState::Finished {
        }
    }
}
