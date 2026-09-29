// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One transition table for pipeline lifecycle authority.

mod actions;
pub(super) mod context;
mod guards;
mod model;
mod transitions;
pub(crate) use actions::PipelineAction;
pub(crate) use context::PipelineContext;
pub(crate) use model::{PipelineDeadline, PipelineFsmEvent, PipelineFsmState, PublicationStep};
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
        }
        state PipelineFsmState::Registering {
            on PipelineFsmEvent::RegistrationCompleted => transitions::registered;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::InitializingStages {
            on PipelineFsmEvent::ChildAcknowledged => transitions::acknowledge;
            on PipelineFsmEvent::PhaseSatisfied => transitions::initialized;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::StartingConsumers {
            on PipelineFsmEvent::ChildAcknowledged => transitions::acknowledge;
            on PipelineFsmEvent::MetricsReady => transitions::metrics_ready;
            on PipelineFsmEvent::PhaseSatisfied => transitions::consumers_started;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::PublishingReady {
            on PipelineFsmEvent::ReadyPublished => transitions::ready_published;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::ReadyForRun {
            on PipelineFsmEvent::Start => transitions::start;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::PublishingStart {
            on PipelineFsmEvent::StartPublished => transitions::start_published;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::StartingSources {
            on PipelineFsmEvent::ChildAcknowledged => transitions::acknowledge;
            on PipelineFsmEvent::PhaseSatisfied => transitions::sources_started;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::PublishingRunning {
            on PipelineFsmEvent::RunningPublished => transitions::running_published;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::Running {
            on PipelineFsmEvent::PhaseSatisfied => transitions::sources_completed;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::Draining {
            on PipelineFsmEvent::PhaseSatisfied => transitions::children_settled;
            on PipelineFsmEvent::GracefulStopExpired => transitions::stop;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::CancellingChildren {
            on PipelineFsmEvent::PhaseSatisfied => transitions::children_settled;
            on PipelineFsmEvent::StageCleanupExpired => transitions::expire_children;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
            on PipelineFsmEvent::GracefulStop => transitions::stop;
            on PipelineFsmEvent::Cancel => transitions::stop;
        }
        state PipelineFsmState::FailingChildren {
            on PipelineFsmEvent::PhaseSatisfied => transitions::children_settled;
            on PipelineFsmEvent::StageCleanupExpired => transitions::expire_children;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::failure;
            on PipelineFsmEvent::OperationalFailure => transitions::failure;
            on PipelineFsmEvent::Abort => transitions::failure;
        }
        state PipelineFsmState::PublishingTerminal {
            on PipelineFsmEvent::TerminalPublished => transitions::terminal_published;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::late_failure;
            on PipelineFsmEvent::OperationalFailure => transitions::late_failure;
            on PipelineFsmEvent::Abort => transitions::late_failure;
        }
        state PipelineFsmState::FinalisingMetrics {
            on PipelineFsmEvent::MetricsExited => transitions::metrics_exited;
            on PipelineFsmEvent::PhaseSatisfied => transitions::metrics_settled;
            on PipelineFsmEvent::MetricsExpired => transitions::expire_metrics;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::late_failure;
            on PipelineFsmEvent::OperationalFailure => transitions::late_failure;
            on PipelineFsmEvent::Abort => transitions::late_failure;
        }
        state PipelineFsmState::PublishingFinalMarker {
            on PipelineFsmEvent::FinalisationCompleted => transitions::finish;
            on PipelineFsmEvent::ChildExited => transitions::child_exited;
            on PipelineFsmEvent::ChildFailed => transitions::late_failure;
            on PipelineFsmEvent::OperationalFailure => transitions::late_failure;
            on PipelineFsmEvent::Abort => transitions::late_failure;
        }
        state PipelineFsmState::Finished {
        }
    }
}
