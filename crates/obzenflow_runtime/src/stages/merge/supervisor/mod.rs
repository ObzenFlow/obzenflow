// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod common;
mod draining;
mod first_phase;
mod last_phase;

use std::error::Error;

use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};

use super::fsm::{MergeAction, MergeContext, MergeDrain, MergeEvent, MergeInputs, MergeState};
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, OwnedDispatch, SupervisorAction,
};
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub(crate) struct MergeSupervisor {
    pub(crate) name: String,
    pub(crate) stage_id: StageId,
}

impl MergeSupervisor {
    pub(crate) fn new(name: String, stage_id: StageId) -> Self {
        Self { name, stage_id }
    }
}

impl Supervisor for MergeSupervisor {
    type State = MergeState;
    type Event = MergeEvent;
    type Context = MergeContext;
    type Action = MergeAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   MergeState;
            event:   MergeEvent;
            context: MergeContext;
            action:  MergeAction;
            initial: initial_state;

            state MergeState::Created {
                on MergeEvent::Initialize => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Initializing,
                            actions: vec![
                                MergeAction::Host(SupervisorAction::Register),
                                MergeAction::AllocateResources,
                                MergeAction::Host(SupervisorAction::Emit(
                                    MergeEvent::InitializationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Initializing {
                on MergeEvent::InitializationCompleted => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Initialized,
                            actions: Vec::new(),
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Initialized {
                on MergeEvent::Ready => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Starting,
                            actions: vec![
                                MergeAction::PublishRunning,
                                MergeAction::Host(SupervisorAction::Emit(
                                    MergeEvent::ActivationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Starting {
                on MergeEvent::ActivationCompleted => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::FirstPhase(MergeInputs::default()),
                            actions: Vec::new(),
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::FirstPhase {
                on MergeEvent::WatermarkRead => |state, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::WatermarkRead(read) = event else { unreachable!() };
                        let (inputs, actions) =
                            match ctx.cross(state, read.generation, read.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        Ok(Transition {
                            next_state: MergeState::FirstPhase(inputs),
                            actions,
                        })
                    })
                };

                on MergeEvent::EofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::EofRead(eof) = event else { unreachable!() };
                        let (inputs, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(MergeAction::ForwardControl(eof.envelope.clone()));
                        let inputs = inputs.with_worst_of(eof);
                        let next_state = if !eof.ends_reads {
                            MergeState::FirstPhase(inputs)
                        } else if eof.side_worst.unwrap_or(EofKind::Natural) == EofKind::Natural {
                            tracing::info!(
                                stage_name = %ctx.stage_name(),
                                strategy = ?ctx.strategy(),
                                "Merge first phase completed; reading held producers"
                            );
                            MergeState::LastPhase(inputs)
                        } else {
                            tracing::info!(
                                stage_name = %ctx.stage_name(),
                                strategy = ?ctx.strategy(),
                                "Merge first phase left open; draining without held producers"
                            );
                            actions.push(MergeAction::ReleaseInputs);
                            MergeState::Draining(MergeDrain {
                                worst_eof: inputs.worst_eof,
                                eof_clock: eof.clock(),
                            })
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::LastPhase {
                on MergeEvent::WatermarkRead => |state, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::WatermarkRead(read) = event else { unreachable!() };
                        let (inputs, actions) =
                            match ctx.cross(state, read.generation, read.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        Ok(Transition {
                            next_state: MergeState::LastPhase(inputs),
                            actions,
                        })
                    })
                };

                on MergeEvent::EofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::EofRead(eof) = event else { unreachable!() };
                        let (inputs, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(MergeAction::ForwardControl(eof.envelope.clone()));
                        let inputs = inputs.with_worst_of(eof);
                        let next_state = if eof.ends_reads {
                            actions.push(MergeAction::ReleaseInputs);
                            MergeState::Draining(MergeDrain {
                                worst_eof: inputs.worst_eof,
                                eof_clock: eof.clock(),
                            })
                        } else {
                            MergeState::LastPhase(inputs)
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Draining {
                on MergeEvent::DrainComplete => |state, _, _| {
                    Box::pin(async move {
                        let MergeState::Draining(drain) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: MergeState::Finalizing,
                            actions: vec![
                                MergeAction::AuthorEof {
                                    kind: drain.worst_eof.unwrap_or(EofKind::Natural),
                                    clock: drain.eof_clock.clone(),
                                },
                                MergeAction::SendCompletion,
                                MergeAction::Cleanup,
                                MergeAction::Host(SupervisorAction::CloseMailbox),
                                MergeAction::Host(SupervisorAction::SettlePublications),
                                MergeAction::Host(SupervisorAction::Emit(
                                    MergeEvent::FinalizationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Finalizing {
                on MergeEvent::FinalizationCompleted => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Drained,
                            actions: Vec::new(),
                        })
                    })
                };

                on MergeEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let MergeEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state MergeState::Failing {
                on MergeEvent::TerminationSettled => |state, _, _| {
                    Box::pin(async move {
                        let MergeState::Failing(cause) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: MergeState::Failed(cause.clone()),
                            actions: Vec::new(),
                        })
                    })
                };
            }

            state MergeState::Cancelling {
                on MergeEvent::TerminationSettled => |state, _, _| {
                    Box::pin(async move {
                        let MergeState::Cancelling(cause) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: MergeState::Cancelled(cause.clone()),
                            actions: Vec::new(),
                        })
                    })
                };
            }

            state MergeState::Drained {}
            state MergeState::Failed {}
            state MergeState::Cancelled {}

            unhandled => |state, event, _| {
                Box::pin(async move {
                    let ignored = matches!(event, MergeEvent::Initialize | MergeEvent::Ready)
                        || matches!(
                            state,
                            MergeState::Failing(_)
                                | MergeState::Cancelling(_)
                                | MergeState::Failed(_)
                                | MergeState::Cancelled(_)
                                | MergeState::Drained
                        );
                    if ignored {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent {
                        state: state.variant_name().to_string(),
                        event: event.variant_name().to_string(),
                    })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Merge
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
            context.flow_context(),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for MergeSupervisor {
    type Handler = ();

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
            MergeAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("merge operation already owns resources".into())
        })?;
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            let resources = Some(resources);
            Box::new(move |context: &mut MergeContext| {
                context.resources = resources;
                result.map(|()| None).map_err(Into::into)
            }) as ActionCompletion<MergeContext, MergeEvent>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> MergeEvent {
        MergeEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(
            state,
            MergeState::FirstPhase(_) | MergeState::LastPhase(_) | MergeState::Draining(_)
        ) {
            return None;
        }
        let resources = context.resources.take()?;
        let state = state.clone();
        let mut worker = Self {
            name: self.name.clone(),
            stage_id: self.stage_id,
        };
        Some(Box::pin(async move {
            let mut owned_context = MergeContext::new(resources);
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |_: &mut Self, context: &mut Self::Context| {
                context.resources = owned_context.resources;
                result
            }) as DispatchCompletion<Self>
        }))
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, BoxError> {
        match state {
            MergeState::FirstPhase(_) => {
                first_phase::dispatch_first_phase(state, ctx.resources_mut()?).await
            }
            MergeState::LastPhase(_) => {
                last_phase::dispatch_last_phase(state, ctx.resources_mut()?).await
            }
            MergeState::Draining(_) => draining::dispatch_draining(ctx.resources_mut()?).await,
            MergeState::Drained | MergeState::Failed(_) | MergeState::Cancelled(_) => {
                Ok(EventLoopDirective::Terminate)
            }
            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

impl ExternalEventPolicy for MergeSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, MergeState::Created | MergeState::Initialized) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, MergeState::Initializing) && matches!(event, MergeEvent::Ready)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            MergeState::Drained | MergeState::Failed(_) | MergeState::Cancelled(_)
        ) {
            None
        } else {
            Some(MergeEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
