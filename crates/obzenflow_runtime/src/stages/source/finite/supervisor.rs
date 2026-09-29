// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite source supervisor implementation using HandlerSupervised pattern

use super::fsm::{
    FiniteSourceAction, FiniteSourceContext, FiniteSourceEvent, FiniteSourceState,
    SourceCompletionOrigin,
};
use crate::execution::{SourceExecutionPhase, SourceReplayExhaustion};
use crate::replay::ReplayDriver;
use crate::stages::common::handlers::UnifiedFiniteSourceHandler;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::SourcePollObserverOutcome;
use crate::stages::source::replay_lifecycle::{ReplayCompletionFacts, ReplayCompletionGuard};
use crate::stages::source::supervision::{
    around_source_boundary, drain_pending_outputs_sync, emit_batch_to_pending_outputs,
    normalise_source_poll_error, observe_source_boundary_rejection, record_source_stage_fatal,
    source_error_kind, source_open_failure, stage_boundary_control_events,
    stage_source_poll_outputs, SourcePollObservation,
};
use crate::stages::source::{
    SourceBoundary, SourceBoundaryOutcome, SourcePollCompletion, SourcePollReport,
    SourcePollResult, SourceReaderInitContext,
};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::SourcePollKind;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::{MiddlewareExecutionScope, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

/// Supervisor for finite source stages
pub(crate) struct FiniteSourceSupervisor<H: UnifiedFiniteSourceHandler + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: Option<H>,

    /// System journal for lifecycle events
    pub(crate) data_journal:
        std::sync::Arc<dyn obzenflow_core::Journal<obzenflow_core::ChainEvent>>,
    pub(crate) flow_context: obzenflow_core::event::provenance::FlowContext,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Adaptive backoff for synchronous idle polls (FLOWIP-086i).
    pub(crate) idle_backoff: IdleBackoff,

    /// Delay scheduled after the completed poll outputs have drained.
    pub(crate) pending_idle_delay: Option<Duration>,

    /// Replay driver for `--replay-from` mode (FLOWIP-095a).
    pub(crate) replay_driver: Option<ReplayDriver>,

    /// Replay lifecycle started timestamp for duration tracking (FLOWIP-095a).
    pub(crate) replay_started_at: Option<Instant>,

    /// Guard that ensures ReplayLifecycle::Completed is emitted once (FLOWIP-095a).
    pub(crate) replay_completion: ReplayCompletionGuard,

    /// Runtime-neutral source boundary seam (FLOWIP-115a).
    pub(crate) source_boundary: Option<Arc<dyn SourceBoundary>>,

    /// EOF was observed by the source boundary after emitting control events;
    /// drain those events before transitioning to completion.
    pub(crate) pending_boundary_eof: bool,

    /// Error was observed by the source boundary after emitting control events;
    /// drain those events before transitioning to failure.
    pub(crate) pending_boundary_error: Option<String>,

    /// Rejection was observed by the source boundary after emitting control
    /// events; drain those events before transitioning to completion.
    pub(crate) pending_boundary_rejected: bool,
}

impl<H: UnifiedFiniteSourceHandler + Send + Sync + 'static> Supervisor
    for FiniteSourceSupervisor<H>
{
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Construction starts in Created. Entry hooks mirror the engine-assigned
        // state before the supervisor executes any transition actions.
        fsm! {
            state: FiniteSourceState<H>;
            event: FiniteSourceEvent<H>;
            context: FiniteSourceContext<H>;
            action: FiniteSourceAction<H>;
            initial: initial_state;

            state FiniteSourceState::Created {
                on FiniteSourceEvent::Initialize => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Initializing, actions: vec![FiniteSourceAction::Host(SupervisorAction::Register), FiniteSourceAction::AllocateResources, FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::InitializationCompleted))] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Initializing {
                on FiniteSourceEvent::InitializationCompleted => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Initialized, actions: vec![] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Initialized {
                on FiniteSourceEvent::Ready => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::WaitingForGun, actions: vec![] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Starting {
                on FiniteSourceEvent::ActivationCompleted => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Running, actions: vec![] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Running {
                on FiniteSourceEvent::ResumeLiveInput => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::AcquiringInput, actions: vec![] }) })
                };

                on FiniteSourceEvent::Completed => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Draining {
                on FiniteSourceEvent::Completed => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Finalising, actions: vec![FiniteSourceAction::SendEOF, FiniteSourceAction::WriteStageCompleted, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::FinalisationCompleted))] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Finalising {
                on FiniteSourceEvent::FinalisationCompleted => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Drained, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::Drained {

            }

            state FiniteSourceState::Failing {
                on FiniteSourceEvent::TerminationSettled => |state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Failed(cause), actions: vec![] }) })
                };
            }

            state FiniteSourceState::Failed {

            }

            state FiniteSourceState::Cancelling {
                on FiniteSourceEvent::Error => |state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = FiniteSourceState::failure(cause.clone());
                    let repeated_cancel = matches!(next, FiniteSourceState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        FiniteSourceAction::SendError { message: cause },
                        FiniteSourceAction::Cleanup,
                        FiniteSourceAction::Host(SupervisorAction::Cleanup),
                        FiniteSourceAction::Host(SupervisorAction::CloseMailbox),
                        FiniteSourceAction::Host(SupervisorAction::SettlePublications),
                        FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled)),
                    ] } }) })
                };

                on FiniteSourceEvent::TerminationSettled => |state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state FiniteSourceState::Cancelled {

            }

            state FiniteSourceState::WaitingForGun {
                on FiniteSourceEvent::Start => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::AcquiringInput, actions: vec![] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state FiniteSourceState::AcquiringInput {
                on FiniteSourceEvent::InputAcquired => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Starting, actions: vec![FiniteSourceAction::PublishRunning, FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::ActivationCompleted))] }) })
                };
                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: FiniteSourceState::Draining, actions: vec![] }) })
                };
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let FiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: FiniteSourceState::failure(cause.clone()),
                        actions: vec![FiniteSourceAction::SendError { message: cause }, FiniteSourceAction::Cleanup, FiniteSourceAction::Host(SupervisorAction::Cleanup), FiniteSourceAction::Host(SupervisorAction::CloseMailbox), FiniteSourceAction::Host(SupervisorAction::SettlePublications), FiniteSourceAction::Host(SupervisorAction::Emit(FiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }
            unhandled => |state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                let state = state.clone();
                let event = event.clone();
                Box::pin(async move {
                    if (matches!(state, FiniteSourceState::Draining)
                        && matches!(event, FiniteSourceEvent::InputAcquired | FiniteSourceEvent::ActivationCompleted | FiniteSourceEvent::ResumeLiveInput))
                        || matches!(event, FiniteSourceEvent::Initialize | FiniteSourceEvent::Ready | FiniteSourceEvent::BeginDrain | FiniteSourceEvent::Start)
                        || matches!(state, FiniteSourceState::Failing(_) | FiniteSourceState::Cancelling(_) | FiniteSourceState::Failed(_) | FiniteSourceState::Cancelled(_) | FiniteSourceState::Drained)
                    {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent { state: state.variant_name().into(), event: event.variant_name().into() })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::FiniteSource
    }

    fn registration(
        &self,
        _context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        crate::supervised_base::base::register_stage(
            self.data_journal.clone(),
            self.flow_context.clone(),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedFiniteSourceHandler + Send + Sync + 'static> HandlerSupervised
    for FiniteSourceSupervisor<H>
{
    type Handler = H;

    fn lifecycle_phase(
        &self,
        state: &Self::State,
    ) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        state.lifecycle_phase()
    }

    fn accounting(
        &self,
        context: &Self::Context,
    ) -> obzenflow_core::event::provenance::ExecutionAccounting {
        crate::metrics::instrumentation::snapshot_stage_accounting(&context.instrumentation)
    }

    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        context
            .instrumentation
            .transition_to_state(state.variant_name());
    }

    fn supervisor_action(
        &self,
        action: &Self::Action,
    ) -> Option<crate::supervised_base::handler_supervised::SupervisorAction<Self::Event>> {
        match action {
            FiniteSourceAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<
        crate::supervised_base::handler_supervised::ActionExecution<Self::Context, Self::Event>,
        FsmError,
    > {
        use crate::supervised_base::handler_supervised::{ActionCompletion, ActionExecution};
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("source operation already owns resources".into())
        })?;
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            Box::new(move |context: &mut FiniteSourceContext<H>| {
                context.resources = Some(resources);
                result.map(|()| None).map_err(Into::into)
            }) as ActionCompletion<FiniteSourceContext<H>, FiniteSourceEvent<H>>
        })))
    }

    async fn execute_cleanup(
        &mut self,
        _context: &Self::Context,
    ) -> Result<
        crate::supervised_base::handler_supervised::ActionExecution<Self::Context, Self::Event>,
        FsmError,
    > {
        self.handler.take();
        self.replay_driver.take();
        Ok(crate::supervised_base::handler_supervised::ActionExecution::Completed)
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> FiniteSourceEvent<H> {
        FiniteSourceEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<crate::supervised_base::handler_supervised::OwnedDispatch<Self>> {
        if !matches!(
            state,
            FiniteSourceState::AcquiringInput
                | FiniteSourceState::Running
                | FiniteSourceState::Draining
        ) {
            return None;
        }
        let resources = context.resources.take()?;
        let state = state.clone();
        let mut worker = Self {
            name: self.name.clone(),
            handler: self.handler.take(),
            data_journal: self.data_journal.clone(),
            flow_context: self.flow_context.clone(),
            stage_id: self.stage_id,
            idle_backoff: self.idle_backoff.clone(),
            pending_idle_delay: self.pending_idle_delay.take(),
            replay_driver: self.replay_driver.take(),
            replay_started_at: self.replay_started_at.take(),
            replay_completion: self.replay_completion.clone(),
            source_boundary: self.source_boundary.clone(),
            pending_boundary_error: self.pending_boundary_error.take(),
            pending_boundary_eof: self.pending_boundary_eof,
            pending_boundary_rejected: self.pending_boundary_rejected,
        };
        Some(Box::pin(async move {
            let mut owned_context = FiniteSourceContext {
                instrumentation: resources.instrumentation.clone(),
                resources: Some(resources),
            };
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.handler = worker.handler;
                owner.idle_backoff = worker.idle_backoff;
                owner.pending_idle_delay = worker.pending_idle_delay;
                owner.replay_driver = worker.replay_driver;
                owner.replay_started_at = worker.replay_started_at;
                owner.replay_completion = worker.replay_completion;
                owner.pending_boundary_error = worker.pending_boundary_error;
                owner.pending_boundary_eof = worker.pending_boundary_eof;
                owner.pending_boundary_rejected = worker.pending_boundary_rejected;
                context.resources = owned_context.resources;
                result
            }) as crate::supervised_base::handler_supervised::DispatchCompletion<Self>
        }))
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        let ctx = ctx.resources_mut()?;
        // Track every event loop iteration
        match state {
            FiniteSourceState::Initializing
            | FiniteSourceState::Starting
            | FiniteSourceState::Finalising
            | FiniteSourceState::Failing(_)
            | FiniteSourceState::Cancelling(_) => Ok(EventLoopDirective::Continue),
            FiniteSourceState::Cancelled(_) => Ok(EventLoopDirective::Terminate),

            FiniteSourceState::Created => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Initialized => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Wait for Ready event from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::WaitingForGun => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Wait for start signal from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::AcquiringInput => {
                if matches!(
                    ctx.runtime_execution.source_phase_for(self.stage_id),
                    SourceExecutionPhase::Replaying
                ) {
                    self.replay_driver = Some(
                        crate::stages::source::supervision::acquire_replay_input(
                            &ctx.runtime_execution,
                            &self.flow_context,
                            StageType::FiniteSource,
                            &self.data_journal,
                        )
                        .await?,
                    );
                    self.replay_started_at = Some(Instant::now());
                } else if let Err(error) = self
                    .handler
                    .as_mut()
                    .expect("handler available before cleanup")
                    .acquire(SourceReaderInitContext {
                        stage_id: self.stage_id,
                        stage_name: ctx.stage_name.clone(),
                        flow_name: ctx.flow_name.clone(),
                    })
                {
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                        source_open_failure(
                            &ctx.stage_name,
                            ctx.runtime_execution.resume_control().is_some(),
                            &error,
                        ),
                    )));
                }
                Ok(EventLoopDirective::Transition(
                    FiniteSourceEvent::InputAcquired,
                ))
            }

            FiniteSourceState::Running | FiniteSourceState::Draining => {
                // Drain any pending outputs first so backpressure doesn't let sources
                // accumulate unbounded in-memory batches.
                let flow_id = ctx.flow_id.to_string();
                let stage_flow_context = make_flow_context(
                    &ctx.flow_name,
                    &flow_id,
                    &ctx.stage_name,
                    self.stage_id,
                    StageType::FiniteSource,
                );
                let observer_scope = ctx.runtime_execution.stage_scope(self.stage_id);

                if drain_pending_outputs_sync(
                    &mut ctx.pending_outputs,
                    &stage_flow_context,
                    self.stage_id,
                    None,
                    &ctx.data_journal,
                    &ctx.error_journal,
                    &ctx.instrumentation,
                    &ctx.backpressure_writer,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_stall,
                    Some(&ctx.output_contract),
                )
                .await?
                {
                    return Ok(EventLoopDirective::Continue);
                }

                // Graceful stop must publish already-polled output before EOF.
                // Reuse the running path's bounded, control-aware credit drain.
                if matches!(state, FiniteSourceState::Draining) {
                    if let Some(error) = self.pending_boundary_error.take() {
                        return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                            error,
                        )));
                    }
                    self.idle_backoff.reset();
                    self.pending_idle_delay = None;
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed));
                }

                if self.pending_boundary_eof {
                    self.pending_boundary_eof = false;
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed));
                }
                if let Some(error) = self.pending_boundary_error.take() {
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                        error,
                    )));
                }
                if self.pending_boundary_rejected {
                    self.pending_boundary_rejected = false;
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed));
                }

                let replaying = matches!(
                    ctx.runtime_execution.source_phase_for(self.stage_id),
                    SourceExecutionPhase::Replaying
                );
                if replaying {
                    self.idle_backoff.reset();
                    self.pending_idle_delay = None;
                } else if let Some(delay) = self.pending_idle_delay.take() {
                    time::sleep(delay).await;
                    return Ok(EventLoopDirective::Continue);
                }

                ctx.instrumentation
                    .event_loops_total
                    .fetch_add(1, Ordering::Relaxed);

                if replaying {
                    let flow_context = stage_flow_context.clone();

                    let tick_started_at = Instant::now();
                    let next_result = self
                        .replay_driver
                        .as_mut()
                        .expect("replay_driver is initialized")
                        .next_replayed_event(
                            WriterId::from(self.stage_id),
                            &ctx.stage_name,
                            flow_context,
                        )
                        .await;
                    let tick_duration = tick_started_at.elapsed();

                    match next_result {
                        Ok(Some(event)) => {
                            let event = event.admit()?;
                            self.idle_backoff.reset();
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, Ordering::Relaxed);

                            let per_data_event_duration = if event.consumes_data_credit() {
                                tick_duration
                            } else {
                                Duration::from_nanos(0)
                            };

                            let events_to_write = self.run_if_not_error(event, |e| vec![e]);
                            emit_batch_to_pending_outputs(
                                events_to_write,
                                &stage_flow_context,
                                &ctx.instrumentation,
                                per_data_event_duration,
                                observer_scope,
                                &mut ctx.pending_outputs,
                            );

                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(None) => {
                            match ctx.runtime_execution.source_replay_exhausted(self.stage_id) {
                                SourceReplayExhaustion::Terminate => {
                                    // FLOWIP-095k: reproduce the archive's recorded completion kind.
                                    let recorded_kind = self
                                        .replay_driver
                                        .as_ref()
                                        .and_then(|d| d.archived_eof_kind());
                                    ctx.completion_origin =
                                        SourceCompletionOrigin::ReplayExhausted { recorded_kind };
                                    let (replayed_count, skipped_count) =
                                        self.replay_driver.as_ref().map_or((0, 0), |d| {
                                            (d.replayed_events(), d.skipped_events())
                                        });
                                    self.replay_completion
                                        .maybe_emit_completed(
                                            &self.flow_context,
                                            &self.data_journal,
                                            self.replay_started_at,
                                            ReplayCompletionFacts {
                                                replayed_count,
                                                skipped_count,
                                                synthesized_eof_kind: Some(
                                                    recorded_kind.unwrap_or(EofKind::Truncated),
                                                ),
                                            },
                                        )
                                        .await;

                                    Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
                                }
                                // FLOWIP-120n: recorded prefix exhausted; drop the replay
                                // driver and continue from the live handler.
                                SourceReplayExhaustion::ContinueLive => {
                                    self.idle_backoff.reset();
                                    self.pending_idle_delay = None;
                                    self.replay_driver = None;
                                    Ok(EventLoopDirective::Transition(
                                        FiniteSourceEvent::ResumeLiveInput,
                                    ))
                                }
                            }
                        }
                        Err(e) => Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                            e.to_string(),
                        ))),
                    }
                } else {
                    let source_boundary = self.source_boundary.clone();
                    let report = around_source_boundary(
                        source_boundary,
                        Box::pin(async {
                            let poll_started_at = time::Instant::now();
                            let invocation = self
                                .handler
                                .as_mut()
                                .expect("handler available before cleanup")
                                .next_invocation();
                            let poll_duration = poll_started_at.elapsed();
                            SourcePollReport::from_erased(invocation, poll_duration)
                        }),
                    )
                    .await;

                    let source_poll_observation = SourcePollObservation::new(
                        ctx.flow_id,
                        &stage_flow_context,
                        &ctx.observers,
                        MiddlewareExecutionScope::LiveHandler,
                    );

                    match report.outcome {
                        SourceBoundaryOutcome::Rejected { policy, reason } => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                reason = %reason,
                                "Finite source boundary rejected; completing source"
                            );
                            let control_events = report.control_events;
                            observe_source_boundary_rejection(
                                &source_poll_observation,
                                &control_events,
                                policy.as_deref(),
                            )
                            .await;
                            if stage_boundary_control_events(
                                control_events,
                                &stage_flow_context,
                                &ctx.instrumentation,
                                observer_scope,
                                &mut ctx.pending_outputs,
                            ) {
                                self.pending_boundary_rejected = true;
                                Ok(EventLoopDirective::Continue)
                            } else {
                                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
                            }
                        }
                        SourceBoundaryOutcome::Polled(poll) => match poll.result {
                            SourcePollResult::Completed(SourcePollCompletion::Batch(
                                mut events,
                            )) if events.iter().any(|event| event.consumes_data_credit()) => {
                                self.idle_backoff.reset();
                                self.pending_idle_delay = None;
                                ctx.instrumentation
                                    .event_loops_with_work_total
                                    .fetch_add(1, Ordering::Relaxed);

                                let source_event_count = events.len();
                                events.extend(poll.operational_events);
                                events.extend(report.control_events);
                                source_poll_observation
                                    .observe(
                                        events.as_mut_slice(),
                                        poll.poll_duration,
                                        SourcePollObserverOutcome::Batch {
                                            events: source_event_count,
                                        },
                                    )
                                    .await;
                                stage_source_poll_outputs(
                                    events,
                                    &stage_flow_context,
                                    &ctx.instrumentation,
                                    poll.poll_duration,
                                    observer_scope,
                                    &mut ctx.pending_outputs,
                                );

                                tracing::trace!(
                                    stage_name = %ctx.stage_name,
                                    "Finite source emitted batch of events"
                                );

                                Ok(EventLoopDirective::Continue)
                            }
                            SourcePollResult::Completed(SourcePollCompletion::Batch(
                                mut events,
                            )) => {
                                let source_event_count = events.len();
                                events.extend(poll.operational_events);
                                events.extend(report.control_events);
                                source_poll_observation
                                    .observe(
                                        events.as_slice(),
                                        poll.poll_duration,
                                        SourcePollObserverOutcome::Batch {
                                            events: source_event_count,
                                        },
                                    )
                                    .await;
                                if !events.is_empty() {
                                    stage_source_poll_outputs(
                                        events,
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        poll.poll_duration,
                                        observer_scope,
                                        &mut ctx.pending_outputs,
                                    );
                                }
                                self.pending_idle_delay = Some(self.idle_backoff.next_delay());
                                Ok(EventLoopDirective::Continue)
                            }
                            SourcePollResult::Completed(SourcePollCompletion::Eof) => {
                                if poll.operational_events.is_empty()
                                    && report.control_events.is_empty()
                                {
                                    source_poll_observation
                                        .observe_empty(
                                            poll.poll_duration,
                                            SourcePollObserverOutcome::Eof,
                                        )
                                        .await;
                                    Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
                                } else {
                                    let mut control_events = poll.operational_events;
                                    control_events.extend(report.control_events);
                                    source_poll_observation
                                        .observe(
                                            control_events.as_mut_slice(),
                                            poll.poll_duration,
                                            SourcePollObserverOutcome::Eof,
                                        )
                                        .await;
                                    stage_source_poll_outputs(
                                        control_events,
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        Duration::from_nanos(0),
                                        observer_scope,
                                        &mut ctx.pending_outputs,
                                    );
                                    self.pending_boundary_eof = true;
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                            SourcePollResult::HandlerError(error) => {
                                tracing::warn!(
                                    stage_name = %ctx.stage_name,
                                    error = error.safe_summary(),
                                    "Finite source handler.next() returned error"
                                );
                                let kind = source_error_kind(&error);
                                let mut events = vec![normalise_source_poll_error(
                                    WriterId::from(self.stage_id),
                                    SourcePollKind::Finite,
                                    &error,
                                )];
                                events.extend(poll.operational_events);
                                events.extend(report.control_events);
                                source_poll_observation
                                    .observe(
                                        events.as_mut_slice(),
                                        poll.poll_duration,
                                        SourcePollObserverOutcome::Error { kind },
                                    )
                                    .await;
                                stage_source_poll_outputs(
                                    events,
                                    &stage_flow_context,
                                    &ctx.instrumentation,
                                    poll.poll_duration,
                                    observer_scope,
                                    &mut ctx.pending_outputs,
                                );
                                self.pending_idle_delay = Some(self.idle_backoff.next_delay());
                                Ok(EventLoopDirective::Continue)
                            }
                            SourcePollResult::Fatal(fatal) => {
                                record_source_stage_fatal(
                                    &fatal,
                                    self.stage_id,
                                    &ctx.stage_name,
                                    &ctx.error_journal,
                                )
                                .await?;
                                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                                    format!(
                                        "Fatal {:?}/{:?}: {}",
                                        fatal.code, fatal.reason, fatal.detail
                                    ),
                                )))
                            }
                        },
                    }
                }
            }

            FiniteSourceState::Drained => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::Failed(_) => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: UnifiedFiniteSourceHandler + Send + Sync + 'static> ExternalEventPolicy
    for FiniteSourceSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(
            state,
            FiniteSourceState::Created
                | FiniteSourceState::Initialized
                | FiniteSourceState::WaitingForGun
        ) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        FiniteSourceState::defer_external_event(state, event)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            FiniteSourceState::Drained
                | FiniteSourceState::Failed(_)
                | FiniteSourceState::Cancelled(_)
        ) {
            None
        } else {
            Some(FiniteSourceEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
