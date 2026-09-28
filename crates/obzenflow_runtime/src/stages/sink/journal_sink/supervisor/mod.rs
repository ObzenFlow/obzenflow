// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal sink supervisor implementation using HandlerSupervised pattern

use super::fsm::{JournalSinkAction, JournalSinkContext, JournalSinkEvent, JournalSinkState};
use crate::messaging::UpstreamSubscription;
use crate::stages::common::handlers::UnifiedSinkHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;

mod running;

/// Supervisor for journal sink stages
pub(crate) struct JournalSinkSupervisor<H: UnifiedSinkHandler + Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Upstream subscription moved off the FSM context (Phase 1b follow-up).
    ///
    /// `JournalSinkAction::AllocateResources` still creates the subscription and
    /// stores it in `ctx.subscription` as a short-lived staging slot. The first
    /// Running dispatch moves it into this supervisor-owned field.
    pub(crate) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: PhantomData<H>,
}

impl<H: UnifiedSinkHandler + Debug + Send + Sync + 'static> Supervisor
    for JournalSinkSupervisor<H>
{
    type State = JournalSinkState<H>;
    type Event = JournalSinkEvent<H>;
    type Context = JournalSinkContext<H>;
    type Action = JournalSinkAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Construction starts in Created. Entry hooks mirror the engine-assigned
        // state before the supervisor executes any transition actions.
        fsm! {
            state: JournalSinkState<H>;
            event: JournalSinkEvent<H>;
            context: JournalSinkContext<H>;
            action: JournalSinkAction<H>;
            initial: initial_state;

            state JournalSinkState::Created {
                on JournalSinkEvent::Initialize => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Initializing, actions: vec![JournalSinkAction::Host(SupervisorAction::Register), JournalSinkAction::AllocateResources, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::InitializationCompleted))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Initializing {
                on JournalSinkEvent::InitializationCompleted => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Initialized, actions: vec![] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Initialized {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Starting, actions: vec![JournalSinkAction::PublishRunning, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::ActivationCompleted))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Starting {
                on JournalSinkEvent::ActivationCompleted => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Running, actions: vec![] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Running {
                on JournalSinkEvent::ReceivedEOF => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Flushing, actions: vec![JournalSinkAction::FlushBuffers, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::FlushComplete))] }) })
                };
                on JournalSinkEvent::BeginFlush => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Flushing, actions: vec![JournalSinkAction::FlushBuffers, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::FlushComplete))] }) })
                };
                on JournalSinkEvent::BeginDrain => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Draining, actions: vec![] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Draining {
                on JournalSinkEvent::BeginDrain => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Flushing, actions: vec![JournalSinkAction::FlushBuffers, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::FlushComplete))] }) })
                };
                on JournalSinkEvent::ReceivedEOF => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Flushing, actions: vec![JournalSinkAction::FlushBuffers, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::FlushComplete))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Finalising {
                on JournalSinkEvent::FinalisationCompleted => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Drained, actions: vec![] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::Drained {

            }

            state JournalSinkState::Failing {
                on JournalSinkEvent::TerminationSettled => |state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Failed(cause), actions: vec![] }) })
                };
            }

            state JournalSinkState::Failed {

            }

            state JournalSinkState::Cancelling {
                on JournalSinkEvent::Error => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = JournalSinkState::failure(cause.clone());
                    let repeated_cancel = matches!(next, JournalSinkState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        JournalSinkAction::SendFailure { message: cause },
                        JournalSinkAction::Cleanup,
                        JournalSinkAction::Host(SupervisorAction::CloseMailbox),
                        JournalSinkAction::Host(SupervisorAction::SettlePublications),
                        JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled)),
                    ] } }) })
                };

                on JournalSinkEvent::TerminationSettled => |state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state JournalSinkState::Cancelled {

            }

            state JournalSinkState::Flushing {
                on JournalSinkEvent::FlushComplete => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::DrainingWriter, actions: vec![JournalSinkAction::DrainWriter, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::WriterDrained))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::DrainingWriter {
                on JournalSinkEvent::WriterDrained => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::CheckingContracts, actions: vec![JournalSinkAction::VerifyContractsAfterFlush, JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::ContractsAccepted))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }

            state JournalSinkState::CheckingContracts {
                on JournalSinkEvent::ContractsAccepted => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JournalSinkState::Finalising, actions: vec![JournalSinkAction::SendCompletion, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::FinalisationCompleted))] }) })
                };
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let JournalSinkEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: JournalSinkState::failure(cause.clone()),
                        actions: vec![JournalSinkAction::SendFailure { message: cause }, JournalSinkAction::Cleanup, JournalSinkAction::Host(SupervisorAction::CloseMailbox), JournalSinkAction::Host(SupervisorAction::SettlePublications), JournalSinkAction::Host(SupervisorAction::Emit(JournalSinkEvent::TerminationSettled))],
                    }) })
                };
            }
            unhandled => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                let state = state.clone();
                let event = event.clone();
                Box::pin(async move {
                    if matches!(event, JournalSinkEvent::Initialize | JournalSinkEvent::Ready | JournalSinkEvent::BeginDrain)
                        || (matches!(event, JournalSinkEvent::BeginFlush | JournalSinkEvent::ReceivedEOF) && matches!(state, JournalSinkState::Flushing | JournalSinkState::DrainingWriter | JournalSinkState::CheckingContracts | JournalSinkState::Finalising))
                        || matches!(state, JournalSinkState::Failing(_) | JournalSinkState::Cancelling(_) | JournalSinkState::Failed(_) | JournalSinkState::Cancelled(_) | JournalSinkState::Drained)
                    {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent { state: state.variant_name().into(), event: event.variant_name().into() })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Sink
    }

    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        let Some(context) = context.resources.as_ref() else {
            return Box::pin(async {
                Err(
                    std::io::Error::other("registration resources belong to a pending operation")
                        .into(),
                )
            });
        };
        crate::supervised_base::base::register_stage(
            context.data_journal.clone(),
            crate::stages::common::supervision::flow_context_factory::make_flow_context(
                &context.flow_name,
                &context.flow_id.to_string(),
                &context.stage_name,
                context.stage_id,
                obzenflow_core::event::context::StageType::Sink,
            ),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedSinkHandler + Debug + Send + Sync + 'static> HandlerSupervised
    for JournalSinkSupervisor<H>
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
            JournalSinkAction::Host(action) => Some(action.clone()),
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
            FsmError::HandlerError("sink operation already owns resources".into())
        })?;
        if resources.subscription.is_none() {
            resources.subscription = self.subscription.take();
        }
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            let resources = Some(resources);
            Box::new(move |context: &mut JournalSinkContext<H>| {
                context.resources = resources;
                result.map(|()| None)
            }) as ActionCompletion<JournalSinkContext<H>, JournalSinkEvent<H>>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> JournalSinkEvent<H> {
        JournalSinkEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<crate::supervised_base::handler_supervised::OwnedDispatch<Self>> {
        if !matches!(
            state,
            JournalSinkState::Running | JournalSinkState::Draining
        ) {
            return None;
        }
        let resources = context.resources.take()?;
        let state = state.clone();
        let mut worker = Self {
            name: self.name.clone(),
            stage_id: self.stage_id,
            subscription: self.subscription.take(),
            _marker: PhantomData,
        };
        Some(Box::pin(async move {
            let mut owned_context = JournalSinkContext::new(resources);
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.subscription = worker.subscription;
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
        // Pending operation states are driven by their action completion events.
        // Running/Draining alone access the idle resource bundle.
        match state {
            JournalSinkState::Initializing
            | JournalSinkState::Starting
            | JournalSinkState::Finalising
            | JournalSinkState::Failing(_)
            | JournalSinkState::Cancelling(_)
            | JournalSinkState::DrainingWriter
            | JournalSinkState::CheckingContracts => Ok(EventLoopDirective::Continue),
            JournalSinkState::Cancelled(_) => Ok(EventLoopDirective::Terminate),

            JournalSinkState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            JournalSinkState::Initialized => Ok(EventLoopDirective::Continue),

            JournalSinkState::Running => {
                running::dispatch_running(self, state, ctx.resources_mut()?).await
            }

            JournalSinkState::Flushing => Ok(EventLoopDirective::Continue),

            JournalSinkState::Draining => {
                // Move to drained state; completion + cleanup are handled
                // by the FSM transition (BeginDrain) rather than here.
                if let Some(heartbeat) = &ctx.resources_mut()?.heartbeat {
                    heartbeat.state.mark_draining();
                }
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.name,
                    "sink: Draining complete, sending completion and cleaning up"
                );
                Ok(EventLoopDirective::Transition(JournalSinkEvent::BeginDrain))
            }

            JournalSinkState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: UnifiedSinkHandler + Debug + Send + Sync + 'static> ExternalEventPolicy
    for JournalSinkSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(
            state,
            JournalSinkState::Created | JournalSinkState::Initialized
        ) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, JournalSinkState::Initializing)
            && matches!(
                event,
                JournalSinkEvent::Ready
                    | JournalSinkEvent::BeginDrain
                    | JournalSinkEvent::ReceivedEOF
                    | JournalSinkEvent::BeginFlush
            )
            || matches!(state, JournalSinkState::Starting)
                && matches!(
                    event,
                    JournalSinkEvent::BeginDrain
                        | JournalSinkEvent::ReceivedEOF
                        | JournalSinkEvent::BeginFlush
                )
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            JournalSinkState::Drained
                | JournalSinkState::Failed(_)
                | JournalSinkState::Cancelled(_)
        ) {
            None
        } else {
            Some(JournalSinkEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
