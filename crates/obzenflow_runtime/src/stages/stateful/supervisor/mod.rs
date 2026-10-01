// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful supervisor implementation using HandlerSupervised pattern.
//!
//! Decomposed into submodules:
//! - `running.rs`  contains the Accumulating and Emitting loops
//! - `draining.rs` contains the Draining loop

mod draining;
mod running;

use super::fsm::{
    StatefulAction, StatefulContext, StatefulEvent, StatefulResources, StatefulState,
};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::UnifiedStatefulHandler;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event as forward_control_event_helper;
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, OwnedDispatch, SupervisorAction,
};
use crate::supervised_base::{
    publication, EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{ChainEvent, JournalRecord, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

fn contract_violation_directive<H>(
    error: &HandlerError,
    phase: &str,
    input: &ChainEvent,
    stage_name: &str,
) -> Option<EventLoopDirective<StatefulEvent<H>>> {
    error.is_contract_violation().then(|| {
        EventLoopDirective::Transition(StatefulEvent::Error(format!(
            "Stateful stage contract violation while {phase} input `{}` in stage \
             `{stage_name}`: {error}",
            input.id,
        )))
    })
}

/// Supervisor for stateful stages
pub(crate) struct StatefulSupervisor<
    H: UnifiedStatefulHandler + Clone + Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Subscription to upstream events (supervisor-owned to avoid borrow conflicts).
    pub(super) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: PhantomData<H>,
}

impl<H: UnifiedStatefulHandler + Clone + Debug + Send + Sync + 'static> Supervisor
    for StatefulSupervisor<H>
{
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   StatefulState<H>;
            event:   StatefulEvent<H>;
            context: StatefulContext<H>;
            action:  StatefulAction<H>;
            initial: initial_state;

            state StatefulState::Created {
                on StatefulEvent::Initialize => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::Initializing,
                            actions: vec![StatefulAction::Host(SupervisorAction::Register), StatefulAction::AllocateResources, StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::InitializationCompleted))],
                        })
                    })
                };

                // Fallback error handling for Created (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::failure(failure_msg),
                                actions: vec![StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Initialized {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::Starting,
                            actions: vec![StatefulAction::PublishRunning, StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::ActivationCompleted))],
                        })
                    })
                };

                // Fallback error handling for Initialized (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::failure(failure_msg),
                                actions: vec![StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Accumulating {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ShouldEmit => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ReceivedEOF => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::BeginDrain => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.drain_requested_by_handle = true;

                        Ok(Transition {
                            next_state: StatefulState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::failure(failure_msg),
                                actions: vec![StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Emitting {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::EmitComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::BeginDrain => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.drain_requested_by_handle = true;

                        Ok(Transition {
                            next_state: StatefulState::EmittingDuringDrain,
                            actions: vec![],
                        })
                    })
                };

                // Fallback error handling for Emitting (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::failure(failure_msg),
                                actions: vec![StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::EmittingDuringDrain {
                on StatefulEvent::EmitComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::Draining, actions: vec![] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: StatefulState::failure(cause.clone()),
                        actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup,
                            StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications),
                            StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                    }) })
                };
            }

            state StatefulState::Draining {
                on StatefulEvent::ShouldEmit => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::EmittingDuringDrain, actions: vec![] }) })
                };
                on StatefulEvent::ReceivedEOF => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::Draining, actions: vec![] }) })
                };
                on StatefulEvent::DrainInputsCompleted => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {

                        Ok(Transition {
                            next_state: StatefulState::ValidatingTerminal,
                            actions: vec![StatefulAction::ValidateTerminal { drain_requested: ctx.drain_requested_by_handle }],
                        })
                    })
                };

                on StatefulEvent::BeginDrain => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {

                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::failure(failure_msg),
                                actions: vec![StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            // Drained: terminal on success; still handle Error like from_any
            state StatefulState::Drained {}

            // Failed: receiving Error again should be idempotent (no extra cleanup)
            state StatefulState::Failed {}

            state StatefulState::Initializing {
                on StatefulEvent::InitializationCompleted => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let next_state = StatefulState::Initialized;
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::Starting {
                on StatefulEvent::ActivationCompleted => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let next_state = StatefulState::Accumulating;
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::ValidatingTerminal {
                on StatefulEvent::TerminalValidated => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::ForwardingTerminal, actions: vec![StatefulAction::ForwardTerminal] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::ForwardingTerminal {
                on StatefulEvent::TerminalForwarded => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::ProducingFinalOutput, actions: vec![StatefulAction::ProduceFinalOutput] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::ProducingFinalOutput {
                on StatefulEvent::FinalOutputsPrepared => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::DrainingFinalOutput, actions: vec![StatefulAction::DrainFinalOutput] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::DrainingFinalOutput {
                on StatefulEvent::FinalOutputPending => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::DrainingFinalOutput, actions: vec![StatefulAction::DrainFinalOutput] }) })
                };
                on StatefulEvent::DrainComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::Finalising, actions: vec![StatefulAction::ForwardEOF, StatefulAction::SendCompletion, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::FinalisationCompleted))] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::Finalising {
                on StatefulEvent::FinalisationCompleted => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let next_state = StatefulState::Drained;
                    let _ = ctx;
                    Box::pin(async move { Ok(Transition { next_state, actions: vec![] }) })
                };
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::failure(cause.clone()), actions: vec![StatefulAction::SendFailure { message: cause }, StatefulAction::Cleanup, StatefulAction::Host(SupervisorAction::CloseMailbox), StatefulAction::Host(SupervisorAction::SettlePublications), StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled))] }) })
                };
            }

            state StatefulState::Failing {
                on StatefulEvent::TerminationSettled => |state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::Failed(cause), actions: vec![] }) })
                };
            }

            state StatefulState::Cancelling {
                on StatefulEvent::Error => |state: &StatefulState<H>, event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = StatefulState::failure(cause.clone());
                    let repeated_cancel = matches!(next, StatefulState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        StatefulAction::SendFailure { message: cause },
                        StatefulAction::Cleanup,
                        StatefulAction::Host(SupervisorAction::CloseMailbox),
                        StatefulAction::Host(SupervisorAction::SettlePublications),
                        StatefulAction::Host(SupervisorAction::Emit(StatefulEvent::TerminationSettled)),
                    ] } }) })
                };

                on StatefulEvent::TerminationSettled => |state: &StatefulState<H>, _event: &StatefulEvent<H>, __ctx: &mut StatefulContext<H>| {
                    let StatefulState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: StatefulState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state StatefulState::Cancelled {}

            unhandled => |state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                let ignored = matches!(event, StatefulEvent::Initialize | StatefulEvent::Ready | StatefulEvent::BeginDrain)
                    || matches!(state, StatefulState::Failing(_) | StatefulState::Cancelling(_) | StatefulState::Failed(_) | StatefulState::Cancelled(_) | StatefulState::Drained);
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();

                Box::pin(async move {
                    if ignored { return Ok(()); }

                    tracing::error!(
                        supervisor = "StatefulSupervisor",
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
        SupervisorKind::Stateful
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
                StageType::Stateful,
            ),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedStatefulHandler + Clone + Debug + Send + Sync + 'static> HandlerSupervised
    for StatefulSupervisor<H>
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
            StatefulAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("stateful operation already owns resources".into())
        })?;
        if resources.subscription.is_none() {
            resources.subscription = self.subscription.take();
        }
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = match action {
                StatefulAction::ValidateTerminal { drain_requested } => {
                    draining::validate_terminal(&mut resources, drain_requested).await
                }
                StatefulAction::ForwardTerminal => draining::forward_terminal(&mut resources).await,
                StatefulAction::ProduceFinalOutput => {
                    draining::produce_final_output(&mut resources).await
                }
                StatefulAction::DrainFinalOutput => draining::drain_final_output(&mut resources)
                    .await
                    .map(|directive| match directive {
                        EventLoopDirective::Continue => {
                            EventLoopDirective::Transition(StatefulEvent::FinalOutputPending)
                        }
                        directive => directive,
                    }),
                action => action
                    .execute_resources(&mut resources)
                    .await
                    .map(|()| EventLoopDirective::Continue)
                    .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>),
            }
            .map(|directive| match directive {
                EventLoopDirective::Transition(event) => Some(event),
                EventLoopDirective::Continue => None,
                EventLoopDirective::Terminate => {
                    unreachable!("terminal operation must return an FSM event")
                }
            });
            Box::new(move |context: &mut StatefulContext<H>| {
                context.resources = Some(resources);
                result
            }) as ActionCompletion<StatefulContext<H>, StatefulEvent<H>>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> StatefulEvent<H> {
        StatefulEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(
            state,
            StatefulState::Accumulating
                | StatefulState::Emitting
                | StatefulState::EmittingDuringDrain
                | StatefulState::Draining
                | StatefulState::DrainingFinalOutput
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
            let mut owned_context = StatefulContext::new(resources);
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.subscription = worker.subscription;
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
        match state {
            StatefulState::Initializing
            | StatefulState::Starting
            | StatefulState::Finalising
            | StatefulState::Failing(_)
            | StatefulState::Cancelling(_) => Ok(EventLoopDirective::Continue),
            StatefulState::Cancelled(_) => Ok(EventLoopDirective::Terminate),

            StatefulState::Created => {
                // Wait for explicit initialization from pipeline.
                Ok(EventLoopDirective::Continue)
            }
            StatefulState::Initialized => Ok(EventLoopDirective::Continue),

            StatefulState::Accumulating => {
                running::dispatch_accumulating(self, state, ctx.resources_mut()?).await
            }
            StatefulState::Emitting | StatefulState::EmittingDuringDrain => {
                running::dispatch_emitting(self, state, ctx.resources_mut()?).await
            }
            StatefulState::ValidatingTerminal
            | StatefulState::ForwardingTerminal
            | StatefulState::ProducingFinalOutput
            | StatefulState::DrainingFinalOutput => Ok(EventLoopDirective::Continue),
            StatefulState::Draining => {
                draining::dispatch_draining(self, state, ctx.resources_mut()?).await
            }
            StatefulState::Drained => Ok(EventLoopDirective::Terminate),
            StatefulState::Failed(_) => Ok(EventLoopDirective::Terminate),
            StatefulState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: UnifiedStatefulHandler + Clone + Debug + Send + Sync + 'static> ExternalEventPolicy
    for StatefulSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, StatefulState::Created | StatefulState::Initialized) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, StatefulState::Initializing)
            && matches!(event, StatefulEvent::Ready | StatefulEvent::BeginDrain)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            StatefulState::Drained | StatefulState::Failed(_) | StatefulState::Cancelled(_)
        ) {
            None
        } else {
            Some(StatefulEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

impl<H: UnifiedStatefulHandler + Clone + Debug + Send + Sync + 'static> StatefulSupervisor<H> {
    /// Forward a control event downstream by appending it to the stateful stage's data journal.
    async fn forward_control_event(
        ctx: &StatefulResources<H>,
        envelope: &JournalRecord<ChainPayload>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _ = forward_control_event_helper(
            envelope,
            ctx.stage_id,
            &ctx.stage_name,
            StageType::Stateful,
            &ctx.data_journal,
        )
        .await?;
        Ok(())
    }

    /// Emit an observability heartbeat when enough events have been accumulated.
    ///
    /// This writes a lightweight `Observability` event carrying the latest
    /// `runtime_context` snapshot for the accumulator.
    async fn emit_stateful_heartbeat_if_due(
        ctx: &mut StatefulResources<H>,
        force: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval = ctx.heartbeat_interval;
        if interval == 0 {
            return Ok(());
        }

        let delta = ctx.events_since_last_heartbeat;
        if delta == 0 {
            return Ok(());
        }

        // In normal operation we require `delta >= interval` before emitting.
        // When `force` is true (drain path), we bypass this threshold so that
        // short finite flows still publish a final heartbeat snapshot.
        if !force && delta < interval {
            return Ok(());
        }

        let Some(writer_id) = ctx.writer_id else {
            // Writer not initialised yet; skip heartbeat rather than failing.
            return Ok(());
        };

        // Capture a fresh runtime context snapshot for the heartbeat.
        let runtime_context = ctx.instrumentation.capture_accounting();

        use obzenflow_core::event::ChainEventFactory;

        let flow_id = ctx.flow_id.to_string();
        let flow_context = make_flow_context(
            &ctx.flow_name,
            &flow_id,
            &ctx.stage_name,
            ctx.stage_id,
            StageType::Stateful,
        );

        let payload = ExecutionPayload::AccumulatorProgress {
            inputs_since_last_report: delta,
        };

        let heartbeat =
            ChainEventFactory::execution_event(writer_id, payload).with_flow_context(flow_context);
        let heartbeat = runtime_context.attach_to(heartbeat);

        publication::append(
            &ctx.data_journal,
            heartbeat,
            AppendOptions::default()
                .with_capture(ctx.instrumentation.journal_capture(None, vec![(0, false)])),
        )
        .await?;

        // Reset counter now that we've published a snapshot.
        ctx.events_since_last_heartbeat = 0;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;

    #[derive(Clone, Debug)]
    struct NoopStateful;

    impl crate::stages::common::handlers::stateful::traits::StatefulHandler for NoopStateful {
        type State = ();

        fn accumulate(&mut self, _: &mut (), _: ChainEvent) {}
        fn initial_state(&self) {}
        fn create_events(&self, _: &()) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn drain_command_stays_in_force_when_an_accepted_emission_finishes() {
        use std::sync::Arc;
        use StatefulEvent::{BeginDrain, EmitComplete, ShouldEmit};

        // A control can arrive before the accepted accumulation requests an
        // emission, while that emission is pending, or after it has settled.
        // Repeating Drain must not restore accumulation or restart emission.
        for events in [
            [BeginDrain, ShouldEmit, BeginDrain, EmitComplete],
            [ShouldEmit, BeginDrain, BeginDrain, EmitComplete],
            [ShouldEmit, EmitComplete, BeginDrain, BeginDrain],
        ] {
            let supervisor = StatefulSupervisor::<NoopStateful> {
                name: "drain_ordering".into(),
                stage_id: StageId::new(),
                subscription: None,
                _marker: PhantomData,
            };
            let mut context = StatefulContext {
                // An owned processing turn holds the resources until its
                // completion event. Control transitions cannot borrow them.
                resources: None,
                instrumentation: Arc::new(
                    crate::metrics::instrumentation::StageInstrumentation::new(),
                ),
                drain_requested_by_handle: false,
            };
            let mut machine = supervisor.build_state_machine(StatefulState::Accumulating);
            for event in events {
                let actions = machine.handle(event, &mut context).await.unwrap();
                assert!(actions.is_empty(), "commands cannot restart accepted work");
            }
            assert_eq!(machine.state(), &StatefulState::Draining);
            assert!(context.drain_requested_by_handle);
        }
    }

    #[test]
    fn contract_violation_uses_stage_fatal_error_transition() {
        let input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.input",
            std::num::NonZeroU32::MIN,
            serde_json::json!({}),
        );
        let error = HandlerError::ContractViolation(
            "one_fact_stage_output: singleton reconstruction failed".to_string(),
        );

        let directive = contract_violation_directive::<()>(&error, "processing", &input, "fold")
            .expect("contract violations are stage-fatal");

        match directive {
            EventLoopDirective::Transition(StatefulEvent::Error(message)) => {
                assert!(message.contains("Stateful stage contract violation"));
                assert!(message.contains("one_fact_stage_output"));
                assert!(message.contains("fold"));
            }
            _ => panic!("expected a transition through the stateful Error event"),
        }
    }

    #[test]
    fn ordinary_handler_error_is_not_promoted_to_stage_fatal() {
        let input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.input",
            std::num::NonZeroU32::MIN,
            serde_json::json!({}),
        );

        assert!(contract_violation_directive::<()>(
            &HandlerError::Domain("rejected".to_string()),
            "processing",
            &input,
            "fold",
        )
        .is_none());
    }
}
