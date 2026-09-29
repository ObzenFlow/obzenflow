// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join supervisor implementation using the HandlerSupervised pattern

pub(super) mod common;
mod draining;
mod enriching;
mod hydrating;
mod live;

use super::config::JoinReferenceMode;
use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, OwnedDispatch, SupervisorAction,
};
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

/// Supervisor for join stages.
pub(crate) struct JoinSupervisor<H: UnifiedJoinHandler + Clone + Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Subscription to reference journal events (supervisor-owned to avoid borrow conflicts).
    pub(super) reference_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Subscription to stream journal events (supervisor-owned to avoid borrow conflicts).
    pub(super) stream_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: PhantomData<H>,
}

impl<H: UnifiedJoinHandler + Clone + Debug + Send + Sync + 'static> Supervisor
    for JoinSupervisor<H>
{
    type State = JoinState<H>;
    type Event = JoinEvent<H>;
    type Context = JoinContext<H>;
    type Action = JoinAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JoinState<H>;
            event:   JoinEvent<H>;
            context: JoinContext<H>;
            action:  JoinAction<H>;
            initial: initial_state;

            state JoinState::Created {
                on JoinEvent::Initialize => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Initializing,
                            actions: vec![JoinAction::Host(SupervisorAction::Register), JoinAction::AllocateResources, JoinAction::Host(SupervisorAction::Emit(JoinEvent::InitializationCompleted))],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        let next_state = JoinState::Starting;
                        Ok(Transition {
                            next_state,
                            actions: vec![JoinAction::InitializeHandlerState,
                                JoinAction::PublishRunning, JoinAction::Host(SupervisorAction::Emit(JoinEvent::ActivationCompleted))],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Hydrating {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Hydrating; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Hydrating,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::ReceivedEOF = event {

                            Ok(Transition {
                                next_state: JoinState::Enriching,
                                actions: vec![JoinAction::EmitHydrationHeartbeat],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Live {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Live; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Live,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::BeginDrain => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation.failures_total.fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Enriching {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Enriching; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Enriching,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::BeginDrain => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation.failures_total.fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Draining {
                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Draining, actions: vec![] }) })
                };
                on JoinEvent::DrainComplete => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: JoinState::Finalising,
                            actions: vec![JoinAction::ForwardEOF,
                                JoinAction::SendCompletion,
                                JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::FinalisationCompleted))],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::failure(failure_msg),
                                actions: vec![JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Drained {}

            state JoinState::Failed {}

            state JoinState::Initializing {
                on JoinEvent::InitializationCompleted => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let next_state = JoinState::Initialized;
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::failure(cause.clone()), actions: vec![JoinAction::SendFailure { message: cause }, JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))] }) })
                };
            }

            state JoinState::Starting {
                on JoinEvent::ActivationCompleted => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let next_state = match ctx.reference_mode { JoinReferenceMode::FiniteEof => JoinState::Hydrating, JoinReferenceMode::Live => JoinState::Live };
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::failure(cause.clone()), actions: vec![JoinAction::SendFailure { message: cause }, JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))] }) })
                };
            }

            state JoinState::Finalising {
                on JoinEvent::FinalisationCompleted => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let next_state = JoinState::Drained;
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::failure(cause.clone()), actions: vec![JoinAction::SendFailure { message: cause }, JoinAction::Cleanup, JoinAction::Host(SupervisorAction::CloseMailbox), JoinAction::Host(SupervisorAction::SettlePublications), JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled))] }) })
                };
            }

            state JoinState::Failing {
                on JoinEvent::TerminationSettled => |state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Failed(cause), actions: vec![] }) })
                };
            }

            state JoinState::Cancelling {
                on JoinEvent::Error => |state: &JoinState<H>, event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = JoinState::failure(cause.clone());
                    let repeated_cancel = matches!(next, JoinState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        JoinAction::SendFailure { message: cause },
                        JoinAction::Cleanup,
                        JoinAction::Host(SupervisorAction::CloseMailbox),
                        JoinAction::Host(SupervisorAction::SettlePublications),
                        JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled)),
                    ] } }) })
                };

                on JoinEvent::TerminationSettled => |state: &JoinState<H>, _event: &JoinEvent<H>, __ctx: &mut JoinContext<H>| {
                    let JoinState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state JoinState::Cancelled {}

            unhandled => |state: &JoinState<H>, event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                let ignored = matches!(event, JoinEvent::Initialize | JoinEvent::Ready | JoinEvent::BeginDrain)
                    || matches!(state, JoinState::Failing(_) | JoinState::Cancelling(_) | JoinState::Failed(_) | JoinState::Cancelled(_) | JoinState::Drained);
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    if ignored { return Ok(()); }

                    tracing::error!(
                        supervisor = "JoinSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Join
    }

    fn registration(
        &self,
        context: &Self::Context,
        descriptor: SupervisorDescriptor,
    ) -> Registration {
        let Some(context) = context.resources.as_ref() else {
            return Box::pin(async {
                Err(
                    std::io::Error::other("registration resources belong to a pending operation")
                        .into(),
                )
            });
        };
        base::register_stage(
            context.data_journal.clone(),
            make_flow_context(
                &context.flow_name,
                &context.flow_id.to_string(),
                &context.stage_name,
                context.stage_id,
                StageType::Join,
            ),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedJoinHandler + Clone + Debug + Send + Sync + 'static> HandlerSupervised
    for JoinSupervisor<H>
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
    }

    fn supervisor_action(&self, action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        match action {
            JoinAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("join operation already owns resources".into())
        })?;
        if resources.reference_subscription.is_none() {
            resources.reference_subscription = self.reference_subscription.take();
        }
        if resources.stream_subscription.is_none() {
            resources.stream_subscription = self.stream_subscription.take();
        }
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            let resources = Some(resources);
            Box::new(move |context: &mut JoinContext<H>| {
                context.resources = resources;
                result.map(|()| None).map_err(Into::into)
            }) as ActionCompletion<JoinContext<H>, JoinEvent<H>>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> JoinEvent<H> {
        JoinEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(
            state,
            JoinState::Hydrating | JoinState::Enriching | JoinState::Live | JoinState::Draining
        ) {
            return None;
        }
        let resources = context.resources.take()?;
        let state = state.clone();
        let mut worker = Self {
            name: self.name.clone(),
            stage_id: self.stage_id,
            reference_subscription: self.reference_subscription.take(),
            stream_subscription: self.stream_subscription.take(),
            _marker: PhantomData,
        };
        Some(Box::pin(async move {
            let mut owned_context = JoinContext::new(resources);
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.reference_subscription = worker.reference_subscription;
                owner.stream_subscription = worker.stream_subscription;
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
        tracing::debug!(
            stage_name = %self.name,
            state = ?state,
            "Join dispatch_state"
        );

        match state {
            JoinState::Initializing
            | JoinState::Starting
            | JoinState::Finalising
            | JoinState::Failing(_)
            | JoinState::Cancelling(_) => Ok(EventLoopDirective::Continue),

            JoinState::Created => Ok(EventLoopDirective::Continue),
            JoinState::Initialized => Ok(EventLoopDirective::Continue),

            JoinState::Hydrating => {
                hydrating::dispatch_hydrating(self, state, ctx.resources_mut()?).await
            }
            JoinState::Enriching => {
                enriching::dispatch_enriching(self, state, ctx.resources_mut()?).await
            }
            JoinState::Live => live::dispatch_live(self, ctx.resources_mut()?).await,
            JoinState::Draining => {
                draining::dispatch_draining(self, state, ctx.resources_mut()?).await
            }
            JoinState::Drained | JoinState::Failed(_) | JoinState::Cancelled(_) => {
                Ok(EventLoopDirective::Terminate)
            }
            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

impl<H: UnifiedJoinHandler + Clone + Debug + Send + Sync + 'static> ExternalEventPolicy
    for JoinSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, JoinState::Created | JoinState::Initialized) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, JoinState::Initializing)
            && matches!(event, JoinEvent::Ready | JoinEvent::BeginDrain)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            JoinState::Drained | JoinState::Failed(_) | JoinState::Cancelled(_)
        ) {
            None
        } else {
            Some(JoinEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
