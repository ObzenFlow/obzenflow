// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Async infinite source supervisor implementation using HandlerSupervised pattern

use super::fsm::{
    InfiniteSourceAction, InfiniteSourceCompletionReason, InfiniteSourceContext,
    InfiniteSourceEvent, InfiniteSourceState,
};
use crate::execution::{SourceExecutionPhase, SourceReplayExhaustion};
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::replay::ReplayDriver;
use crate::stages::common::handlers::source::SourceError;
use crate::stages::common::handlers::UnifiedAsyncInfiniteSourceHandler;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::SourcePollObserverOutcome;
use crate::stages::source::replay_lifecycle::{ReplayCompletionFacts, ReplayCompletionGuard};
use crate::stages::source::supervision::SourceControl;
use crate::stages::source::supervision::{
    around_source_boundary, commit_source_open_failure, drain_pending_outputs_async,
    emit_batch_to_pending_outputs, normalise_source_poll_error, observe_source_boundary_rejection,
    poll_error_backoff, record_source_cleanup_failed, record_source_stage_fatal, source_error_kind,
    stage_boundary_control_events, stage_source_poll_outputs, terminal_poll_failure,
    PendingFailure, SourceOpenFailureCommit, SourcePollObservation,
};
use crate::stages::source::{
    SourceBoundary, SourceBoundaryOutcome, SourcePollCompletion, SourcePollReport,
    SourcePollResult, SourceReaderInitContext,
};
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, OwnedDispatch, SupervisorAction,
};
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::{publication, EventLoopDirective, HandlerSupervised, StateWatcher};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::SourcePollKind;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::{ExecutionAccounting, FlowContext};
use obzenflow_core::event::types::Count;
use obzenflow_core::event::{ChainEventFactory, ReplayLifecycleEvent};
use obzenflow_core::{ChainEvent, Journal, MiddlewareExecutionScope, StageId, StageKey, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

/// Supervisor for async infinite source stages.
pub(crate) struct AsyncInfiniteSourceSupervisor<
    H: UnifiedAsyncInfiniteSourceHandler + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: Option<H>,

    /// System journal for lifecycle events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(crate) flow_context: FlowContext,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Adaptive backoff for async polls that deliver no data.
    pub(crate) idle_backoff: IdleBackoff,

    /// Delay scheduled after the completed poll outputs have drained.
    pub(crate) pending_idle_delay: Option<Duration>,

    /// External control events (Initialize/Ready/Start/BeginDrain).
    pub(crate) external_events: SourceControl<InfiniteSourceEvent<H>>,

    /// State watcher for UI/handles.
    pub(crate) state_watcher: StateWatcher<InfiniteSourceState<H>>,

    /// Last published state (avoid waking watchers every loop) (FLOWIP-086i).
    pub(crate) last_state: Option<InfiniteSourceState<H>>,

    /// Replay driver for `--replay-from` mode (FLOWIP-095a).
    pub(crate) replay_driver: Option<ReplayDriver>,

    /// Replay lifecycle started timestamp for duration tracking (FLOWIP-095a).
    pub(crate) replay_started_at: Option<Instant>,

    /// Guard that ensures ReplayLifecycle::Completed is emitted once (FLOWIP-095a).
    pub(crate) replay_completion: ReplayCompletionGuard,

    /// Runtime-neutral source boundary seam (FLOWIP-115a).
    pub(crate) source_boundary: Option<Arc<dyn SourceBoundary>>,

    /// Completion was observed by the source boundary after emitting control
    /// events; drain those events before beginning source drain.
    pub(crate) pending_boundary_begin_drain: bool,

    /// A failure was selected after staging this turn's events; drain those
    /// events before transitioning to failure.
    pub(crate) pending_failure: Option<PendingFailure>,

    /// Cleanup is live-only and attempted at most once.
    pub(crate) reader_acquired: bool,
}

impl<H: UnifiedAsyncInfiniteSourceHandler + Send + Sync + 'static> Supervisor
    for AsyncInfiniteSourceSupervisor<H>
{
    type State = InfiniteSourceState<H>;
    type Event = InfiniteSourceEvent<H>;
    type Context = InfiniteSourceContext<H>;
    type Action = InfiniteSourceAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Construction starts in Created. Entry hooks mirror the engine-assigned
        // state before the supervisor executes any transition actions.
        fsm! {
            state: InfiniteSourceState<H>;
            event: InfiniteSourceEvent<H>;
            context: InfiniteSourceContext<H>;
            action: InfiniteSourceAction<H>;
            initial: initial_state;

            state InfiniteSourceState::Created {
                on InfiniteSourceEvent::Initialize => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Initializing, actions: vec![InfiniteSourceAction::Host(SupervisorAction::Register), InfiniteSourceAction::AllocateResources, InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::InitializationCompleted))] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Initializing {
                on InfiniteSourceEvent::InitializationCompleted => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Initialized, actions: vec![] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Initialized {
                on InfiniteSourceEvent::Ready => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::WaitingForGun, actions: vec![] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Starting {
                on InfiniteSourceEvent::ActivationCompleted => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Running, actions: vec![] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Running {
                on InfiniteSourceEvent::ResumeLiveInput => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::AcquiringInput, actions: vec![] }) })
                };

                on InfiniteSourceEvent::Completed => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Draining {
                on InfiniteSourceEvent::Completed => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Finalising, actions: vec![InfiniteSourceAction::SendEOF, InfiniteSourceAction::WriteStageCompleted, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::FinalisationCompleted))] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Finalising {
                on InfiniteSourceEvent::FinalisationCompleted => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Drained, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::Drained {

            }

            state InfiniteSourceState::Failing {
                on InfiniteSourceEvent::TerminationSettled => |state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Failed(cause), actions: vec![] }) })
                };
            }

            state InfiniteSourceState::Failed {

            }

            state InfiniteSourceState::Cancelling {
                on InfiniteSourceEvent::Error => |state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = InfiniteSourceState::failure(cause.clone());
                    let repeated_cancel = matches!(next, InfiniteSourceState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        InfiniteSourceAction::SendError { message: cause },
                        InfiniteSourceAction::Cleanup,
                        InfiniteSourceAction::Host(SupervisorAction::Cleanup),
                        InfiniteSourceAction::Host(SupervisorAction::CloseMailbox),
                        InfiniteSourceAction::Host(SupervisorAction::SettlePublications),
                        InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled)),
                    ] } }) })
                };

                on InfiniteSourceEvent::TerminationSettled => |state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state InfiniteSourceState::Cancelled {

            }

            state InfiniteSourceState::WaitingForGun {
                on InfiniteSourceEvent::Start => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::AcquiringInput, actions: vec![] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }

            state InfiniteSourceState::AcquiringInput {
                on InfiniteSourceEvent::InputAcquired => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Starting, actions: vec![InfiniteSourceAction::PublishRunning, InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::ActivationCompleted))] }) })
                };
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: InfiniteSourceState::Draining, actions: vec![] }) })
                };
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let InfiniteSourceEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: InfiniteSourceState::failure(cause.clone()),
                        actions: vec![InfiniteSourceAction::SendError { message: cause }, InfiniteSourceAction::Cleanup, InfiniteSourceAction::Host(SupervisorAction::Cleanup), InfiniteSourceAction::Host(SupervisorAction::CloseMailbox), InfiniteSourceAction::Host(SupervisorAction::SettlePublications), InfiniteSourceAction::Host(SupervisorAction::Emit(InfiniteSourceEvent::TerminationSettled))],
                    }) })
                };
            }
            unhandled => |state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                let state = state.clone();
                let event = event.clone();
                Box::pin(async move {
                    if (matches!(state, InfiniteSourceState::Draining)
                        && matches!(event, InfiniteSourceEvent::InputAcquired | InfiniteSourceEvent::ActivationCompleted | InfiniteSourceEvent::ResumeLiveInput))
                        || matches!(event, InfiniteSourceEvent::Initialize | InfiniteSourceEvent::Ready | InfiniteSourceEvent::BeginDrain | InfiniteSourceEvent::Start)
                        || matches!(state, InfiniteSourceState::Failing(_) | InfiniteSourceState::Cancelling(_) | InfiniteSourceState::Failed(_) | InfiniteSourceState::Cancelled(_) | InfiniteSourceState::Drained)
                    {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent { state: state.variant_name().into(), event: event.variant_name().into() })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::AsyncInfiniteSource
    }

    fn registration(
        &self,
        _context: &Self::Context,
        descriptor: SupervisorDescriptor,
    ) -> Registration {
        base::register_stage(
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
impl<H: UnifiedAsyncInfiniteSourceHandler + Send + Sync + 'static> HandlerSupervised
    for AsyncInfiniteSourceSupervisor<H>
{
    type Handler = H;

    fn lifecycle_phase(&self, state: &Self::State) -> LifecyclePhase {
        state.lifecycle_phase()
    }

    fn accounting(&self, context: &Self::Context) -> ExecutionAccounting {
        snapshot_stage_accounting(&context.instrumentation)
    }

    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        context
            .instrumentation
            .transition_to_state(state.variant_name());
        if self.last_state.as_ref() != Some(state) {
            let _ = self.state_watcher.update(state.clone());
            self.last_state = Some(state.clone());
        }
    }

    fn supervisor_action(&self, action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        match action {
            InfiniteSourceAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn next_control(
        &mut self,
        state: &Self::State,
        _context: &mut Self::Context,
    ) -> Option<Self::Event> {
        self.external_events
            .recv(|event| !InfiniteSourceState::defer_external_event(state, event))
            .await
    }

    fn close_mailbox(
        &mut self,
        state: &Self::State,
    ) -> futures::future::BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>> {
        self.external_events.close_and_record(
            crate::supervised_base::with_external_events::stage_commands(
                self.data_journal.clone(),
                self.flow_context.clone(),
            ),
            &self.name,
            state.variant_name(),
        )
    }

    async fn execute_cleanup(
        &mut self,
        _context: &Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        self.replay_driver.take();
        let Some(mut handler) = self.handler.take() else {
            return Ok(ActionExecution::Completed);
        };
        let acquired = self.reader_acquired;
        let flow_context = self.flow_context.clone();
        let journal = self.data_journal.clone();
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = async {
                if acquired {
                    if let Err(error) = handler.drain().await {
                        record_source_cleanup_failed(&flow_context, &error, &journal).await?;
                    }
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            }
            .await;
            drop(handler);
            Box::new(move |_: &mut InfiniteSourceContext<H>| result.map(|()| None))
                as ActionCompletion<InfiniteSourceContext<H>, InfiniteSourceEvent<H>>
        })))
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("source operation already owns resources".into())
        })?;
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            Box::new(move |context: &mut InfiniteSourceContext<H>| {
                context.resources = Some(resources);
                result.map(|()| None).map_err(Into::into)
            }) as ActionCompletion<InfiniteSourceContext<H>, InfiniteSourceEvent<H>>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> InfiniteSourceEvent<H> {
        InfiniteSourceEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(
            state,
            InfiniteSourceState::AcquiringInput
                | InfiniteSourceState::Running
                | InfiniteSourceState::Draining
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
            external_events: self.external_events.begin_turn(),
            state_watcher: self.state_watcher.clone(),
            last_state: Some(state.clone()),
            source_boundary: self.source_boundary.clone(),
            idle_backoff: self.idle_backoff.clone(),
            pending_idle_delay: self.pending_idle_delay.take(),
            replay_driver: self.replay_driver.take(),
            replay_started_at: self.replay_started_at.take(),
            replay_completion: self.replay_completion.clone(),
            pending_failure: self.pending_failure.take(),
            reader_acquired: self.reader_acquired,
            pending_boundary_begin_drain: self.pending_boundary_begin_drain,
        };
        Some(Box::pin(async move {
            let mut owned_context = InfiniteSourceContext {
                instrumentation: resources.instrumentation.clone(),
                resources: Some(resources),
            };
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.external_events.end_turn();
                owner.handler = worker.handler;
                owner.idle_backoff = worker.idle_backoff;
                owner.pending_idle_delay = worker.pending_idle_delay;
                owner.replay_driver = worker.replay_driver;
                owner.replay_started_at = worker.replay_started_at;
                owner.replay_completion = worker.replay_completion;
                owner.pending_failure = worker.pending_failure;
                owner.reader_acquired = worker.reader_acquired;
                owner.pending_boundary_begin_drain = worker.pending_boundary_begin_drain;
                context.resources = owned_context.resources;
                result
            }) as DispatchCompletion<Self>
        }))
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        let ctx = ctx.resources_mut()?;
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match state {
            InfiniteSourceState::Initializing
            | InfiniteSourceState::Starting
            | InfiniteSourceState::Finalising
            | InfiniteSourceState::Failing(_)
            | InfiniteSourceState::Cancelling(_) => Ok(EventLoopDirective::Continue),
            InfiniteSourceState::Cancelled(_) => Ok(EventLoopDirective::Terminate),

            InfiniteSourceState::Created
            | InfiniteSourceState::Initialized
            | InfiniteSourceState::WaitingForGun => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                match self
                    .external_events
                    .recv(|event| !InfiniteSourceState::defer_external_event(state, event))
                    .await
                {
                    Some(event) => Ok(EventLoopDirective::Transition(event)),
                    None => Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                        "External control channel closed".to_string(),
                    ))),
                }
            }

            InfiniteSourceState::AcquiringInput => {
                if matches!(
                    ctx.runtime_execution.source_phase_for(self.stage_id),
                    SourceExecutionPhase::Replaying
                ) {
                    self.replay_driver = Some(
                        crate::stages::source::supervision::acquire_replay_input(
                            &ctx.runtime_execution,
                            &self.flow_context,
                            StageType::InfiniteSource,
                            &self.data_journal,
                        )
                        .await?,
                    );
                    self.replay_started_at = Some(Instant::now());
                } else if !self.reader_acquired {
                    let context = SourceReaderInitContext {
                        stage_id: self.stage_id,
                        stage_name: ctx.stage_name.clone(),
                        flow_name: ctx.flow_name.clone(),
                    };
                    let opened = tokio::select! {
                        biased;
                        event = self.external_events.recv(|event| !InfiniteSourceState::defer_external_event(state, event)) => {
                            return Ok(EventLoopDirective::Transition(event.unwrap_or_else(||
                                InfiniteSourceEvent::Error("External control channel closed".into())
                            )));
                        }
                        opened = self.handler.as_mut().expect("AcquiringInput owns a source handler").acquire(context) => opened,
                    };
                    if let Err(error) = opened {
                        let flow_id = ctx.flow_id.to_string();
                        let stage_flow_context = make_flow_context(
                            &ctx.flow_name,
                            &flow_id,
                            &ctx.stage_name,
                            self.stage_id,
                            StageType::InfiniteSource,
                        );
                        let failure = commit_source_open_failure(
                            SourceOpenFailureCommit {
                                stage_flow_context: &stage_flow_context,
                                source_type: SourcePollKind::AsyncInfinite,
                                resuming: ctx.runtime_execution.resume_control().is_some(),
                                error_journal: &ctx.error_journal,
                                instrumentation: &ctx.instrumentation,
                                scope: ctx.runtime_execution.stage_scope(self.stage_id),
                            },
                            &error,
                        )
                        .await?;
                        return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                            failure.select(&mut ctx.failure_causal_event_id),
                        )));
                    }
                    self.reader_acquired = true;
                    // Return to control dispatch before admitting the first poll.
                }
                Ok(EventLoopDirective::Transition(
                    InfiniteSourceEvent::InputAcquired,
                ))
            }

            InfiniteSourceState::Running | InfiniteSourceState::Draining => {
                // Drain any pending outputs first so backpressure doesn't let sources
                // accumulate unbounded in-memory batches.
                let flow_id = ctx.flow_id.to_string();
                let stage_flow_context = make_flow_context(
                    &ctx.flow_name,
                    &flow_id,
                    &ctx.stage_name,
                    self.stage_id,
                    StageType::InfiniteSource,
                );
                let observer_scope = ctx.runtime_execution.stage_scope(self.stage_id);

                if let Some(directive) = drain_pending_outputs_async(
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
                    self.external_events
                        .recv(|event| !InfiniteSourceState::defer_external_event(state, event)),
                    || InfiniteSourceEvent::Error("External control channel closed".to_string()),
                )
                .await?
                {
                    // The diagnostic drains first, so control cannot relabel a failure.
                    if let Some(failure) = self.pending_failure.take() {
                        return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                            failure.select(&mut ctx.failure_causal_event_id),
                        )));
                    }
                    return Ok(directive);
                }

                if let Some(failure) = self.pending_failure.take() {
                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                        failure.select(&mut ctx.failure_causal_event_id),
                    )));
                }
                // Graceful stop must publish already-polled output before EOF.
                // Reuse the running path's bounded, control-aware credit drain.
                if matches!(state, InfiniteSourceState::Draining) {
                    self.idle_backoff.reset();
                    self.pending_idle_delay = None;
                    return Ok(EventLoopDirective::Transition(
                        InfiniteSourceEvent::Completed,
                    ));
                }

                if self.pending_boundary_begin_drain {
                    self.pending_boundary_begin_drain = false;
                    return Ok(EventLoopDirective::Transition(
                        InfiniteSourceEvent::BeginDrain,
                    ));
                }

                let replaying = matches!(
                    ctx.runtime_execution.source_phase_for(self.stage_id),
                    SourceExecutionPhase::Replaying
                );
                if replaying {
                    self.idle_backoff.reset();
                    self.pending_idle_delay = None;
                } else if let Some(delay) = self.pending_idle_delay.take() {
                    tokio::select! {
                        biased;
                        maybe_event = self.external_events.recv(|event| !InfiniteSourceState::defer_external_event(state, event)) => {
                            return Ok(EventLoopDirective::Transition(
                                maybe_event.unwrap_or_else(|| {
                                    InfiniteSourceEvent::Error(
                                        "External control channel closed".to_string(),
                                    )
                                }),
                            ));
                        }
                        _ = time::sleep(delay) => {}
                    }
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
                            if matches!(
                                event.event.payload,
                                obzenflow_core::event::ChainPayload::Fact(_)
                                    | obzenflow_core::event::ChainPayload::CompositeData(_)
                            ) && !ctx.output_contract.is_empty()
                                && !ctx
                                    .output_contract
                                    .contains_descriptor(&event.event.descriptor())
                            {
                                return Err(format!("source replay event descriptor {} does not match its output contract", event.event.descriptor()).into());
                            }
                            let event = event.admit()?;
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

                                    ctx.completion_reason =
                                        InfiniteSourceCompletionReason::ReplayExhausted {
                                            recorded_kind,
                                        };
                                    Ok(EventLoopDirective::Transition(
                                        InfiniteSourceEvent::BeginDrain,
                                    ))
                                }
                                // FLOWIP-120n handoff: author the catch-up watermark
                                // behind the recorded outputs (F9), record the recorded
                                // count and the boundary, then continue from the live
                                // handler.
                                SourceReplayExhaustion::ContinueLive => {
                                    let control = ctx.runtime_execution.resume_control().expect(
                                        "ContinueLive is only returned by the Resume strategy",
                                    );
                                    let (replayed_count, skipped_count) =
                                        self.replay_driver.as_ref().map_or((0, 0), |d| {
                                            (d.replayed_events(), d.skipped_events())
                                        });
                                    control
                                        .record_delivered_high_water(self.stage_id, replayed_count);
                                    let generation = control.resume_generation();
                                    let marker = ChainEventFactory::catch_up_complete_event(
                                        WriterId::from(self.stage_id),
                                        generation,
                                        StageKey::from(ctx.stage_name.clone()),
                                    );
                                    emit_batch_to_pending_outputs(
                                        vec![marker],
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        Duration::from_nanos(0),
                                        observer_scope,
                                        &mut ctx.pending_outputs,
                                    );
                                    self.replay_completion
                                        .maybe_emit_completed(
                                            &self.flow_context,
                                            &self.data_journal,
                                            self.replay_started_at,
                                            ReplayCompletionFacts {
                                                replayed_count,
                                                skipped_count,
                                                // FLOWIP-095k: the resume handoff
                                                // synthesizes no terminal EOF.
                                                synthesized_eof_kind: None,
                                            },
                                        )
                                        .await;
                                    let resumed_live = obzenflow_core::event::ChainEventFactory::execution_event(
                                        WriterId::from(self.stage_id),
                                        obzenflow_core::event::payloads::execution_payload::ExecutionPayload::ReplayLifecycle(
                                            ReplayLifecycleEvent::ResumedLive {
                                                archive_flow_id: ctx
                                                    .runtime_execution
                                                    .archive_for_io()
                                                    .map(|a| a.archive_flow_id().to_string())
                                                    .unwrap_or_default(),
                                                replayed_count: Count(replayed_count),
                                                generation: generation.0,
                                            },
                                        ),
                                    ).with_flow_context(self.flow_context.clone());
                                    if let Err(e) = publication::append(
                                        &self.data_journal,
                                        resumed_live,
                                        Default::default(),
                                    )
                                    .await
                                    {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            journal_error = %e,
                                            "Failed to append ReplayLifecycle::ResumedLive system event"
                                        );
                                    }
                                    // The boundary flips source phase, stage scope, and
                                    // heartbeat to live; late prefix events stay
                                    // reconstruction-scoped by their in-band generation.
                                    control.record_generation_boundary(self.stage_id, generation);
                                    // FLOWIP-120n F12: a hosted ingress surface admits
                                    // from here on.
                                    match self
                                        .handler
                                        .as_ref()
                                        .expect("Running owns a source handler")
                                        .hosted_ingress_slot()
                                    {
                                        Some(slot) => {
                                            slot.mark_resume_live();
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                "resume handoff: hosted ingress marked live"
                                            );
                                        }
                                        None => tracing::debug!(
                                            stage_name = %ctx.stage_name,
                                            "resume handoff: no hosted ingress slot on handler"
                                        ),
                                    }
                                    self.replay_driver = None;
                                    self.idle_backoff.reset();
                                    self.pending_idle_delay = None;
                                    Ok(EventLoopDirective::Transition(
                                        InfiniteSourceEvent::ResumeLiveInput,
                                    ))
                                }
                            }
                        }
                        Err(e) => Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                            e.to_string(),
                        ))),
                    }
                } else {
                    let source_boundary = self.source_boundary.clone();
                    let poll_timeout = self
                        .handler
                        .as_ref()
                        .expect("Running owns a source handler")
                        .poll_timeout();
                    let boundary_future = around_source_boundary(
                        source_boundary,
                        Box::pin(async {
                            let poll_started_at = time::Instant::now();
                            match poll_timeout {
                                Some(timeout) => {
                                    match time::timeout(
                                        timeout,
                                        self.handler
                                            .as_mut()
                                            .expect("Running owns a source handler")
                                            .next_invocation(),
                                    )
                                    .await
                                    {
                                        Ok(invocation) => SourcePollReport::from_erased(
                                            invocation,
                                            poll_started_at.elapsed(),
                                        ),
                                        Err(_) => {
                                            let poll_duration = poll_started_at.elapsed();
                                            let timeout_error = SourceError::Timeout(
                                                obzenflow_core::event::SourceDiagnosticReason::TimedOut
                                                    .into(),
                                            );
                                            SourcePollReport::handler_error(
                                                timeout_error,
                                                poll_duration,
                                            )
                                        }
                                    }
                                }
                                None => {
                                    let invocation = self
                                        .handler
                                        .as_mut()
                                        .expect("Running owns a source handler")
                                        .next_invocation()
                                        .await;
                                    SourcePollReport::from_erased(
                                        invocation,
                                        poll_started_at.elapsed(),
                                    )
                                }
                            }
                        }),
                    );

                    let report = tokio::select! {
                        biased;
                        maybe_event = self.external_events.recv(|event| !InfiniteSourceState::defer_external_event(state, event)) => {
                            match maybe_event {
                                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                                None => {
                                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                                        "External control channel closed".to_string(),
                                    )));
                                }
                            }
                        }
                        report = boundary_future => report,
                    };

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
                                "Async infinite source boundary rejected; beginning completion"
                            );
                            ctx.completion_reason = InfiniteSourceCompletionReason::LiveEof;
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
                                self.pending_boundary_begin_drain = true;
                                Ok(EventLoopDirective::Continue)
                            } else {
                                Ok(EventLoopDirective::Transition(
                                    InfiniteSourceEvent::BeginDrain,
                                ))
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
                                ctx.completion_reason = InfiniteSourceCompletionReason::LiveEof;
                                if poll.operational_events.is_empty()
                                    && report.control_events.is_empty()
                                {
                                    source_poll_observation
                                        .observe_empty(
                                            poll.poll_duration,
                                            SourcePollObserverOutcome::Eof,
                                        )
                                        .await;
                                    Ok(EventLoopDirective::Transition(
                                        InfiniteSourceEvent::BeginDrain,
                                    ))
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
                                    self.pending_boundary_begin_drain = true;
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                            SourcePollResult::HandlerError(error) => {
                                tracing::warn!(
                                    stage_name = %ctx.stage_name,
                                    error = %error,
                                    "Async infinite source handler.next() returned error"
                                );
                                let kind = source_error_kind(&error);
                                let diagnostic = normalise_source_poll_error(
                                    WriterId::from(self.stage_id),
                                    SourcePollKind::AsyncInfinite,
                                    &error,
                                );
                                self.pending_failure =
                                    terminal_poll_failure(&ctx.stage_name, &error, &diagnostic);
                                let mut events = vec![diagnostic];
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
                                self.pending_idle_delay =
                                    poll_error_backoff(&error, &mut self.idle_backoff);
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
                                Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
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

            InfiniteSourceState::Drained => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                Ok(EventLoopDirective::Terminate)
            }
            InfiniteSourceState::Failed(_) => {
                self.idle_backoff.reset();
                self.pending_idle_delay = None;
                // Best-effort: ensure async infinite sources can clean up resources even on
                // force_shutdown (StopRequested), which routes through the Error → Failed path.
                Ok(EventLoopDirective::Terminate)
            }
            InfiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
