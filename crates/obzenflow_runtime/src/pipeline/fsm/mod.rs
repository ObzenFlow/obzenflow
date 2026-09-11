// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Canonical pipeline FSM construction and transition table.

mod actions;
pub(super) mod context;
mod model;
mod transitions;

pub(crate) use actions::PipelineAction;
pub(crate) use context::PipelineContext;
pub(crate) use model::{PipelineDeadline, PipelineFsmEvent, PipelineFsmState};

use obzenflow_fsm::{fsm, StateMachine};

pub(crate) type PipelineFsm =
    StateMachine<PipelineFsmState, PipelineFsmEvent, PipelineContext, PipelineAction>;

pub(crate) fn build_pipeline_fsm_with_initial(initial: PipelineFsmState) -> PipelineFsm {
    use transitions::{bootstrap, control, deadline, failure, journal, settled};
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
