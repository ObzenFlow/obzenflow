// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join supervisor implementation using the HandlerSupervised pattern

pub(super) mod common;
mod draining;
mod enriching;
mod hydrating;
mod live;

use super::fsm::{
    DrainStep, JoinAction, JoinContext, JoinDrain, JoinEnrichment, JoinEvent, JoinHold, JoinLive,
    JoinReading, JoinState, PlanShape, ThenLivePhases,
};
use super::phase::HoldOutcome;
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
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;

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
    type State = JoinState;
    type Event = JoinEvent;
    type Context = JoinContext<H>;
    type Action = JoinAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JoinState;
            event:   JoinEvent;
            context: JoinContext<H>;
            action:  JoinAction<H>;
            initial: initial_state;

            state JoinState::Created {
                on JoinEvent::Initialize => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Created(plan) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: JoinState::Initializing(*plan),
                            actions: vec![
                                JoinAction::Host(SupervisorAction::Register),
                                JoinAction::AllocateResources,
                                JoinAction::Host(SupervisorAction::Emit(
                                    JoinEvent::InitializationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Initializing {
                on JoinEvent::InitializationCompleted => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Initializing(plan) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: JoinState::Initialized(*plan),
                            actions: Vec::new(),
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail_uncounted(cause.clone()))
                    })
                };
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Initialized(plan) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: JoinState::Starting(*plan),
                            actions: vec![
                                JoinAction::PublishRunning,
                                JoinAction::Host(SupervisorAction::Emit(
                                    JoinEvent::ActivationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Starting {
                on JoinEvent::ActivationCompleted => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Starting(plan) = state else { unreachable!() };
                        let reading = JoinReading::new(*plan);
                        let next_state = match plan.shape() {
                            PlanShape::Live(reads) => JoinState::Live(JoinLive { reading, reads }),
                            PlanShape::FiniteEof | PlanShape::ThenLive(_) => {
                                JoinState::Hydrating(JoinHold::new(reading))
                            }
                        };
                        Ok(Transition {
                            next_state,
                            actions: Vec::new(),
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail_uncounted(cause.clone()))
                    })
                };
            }

            state JoinState::Hydrating {
                on JoinEvent::WatermarkRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Hydrating(hold) = state else { unreachable!() };
                        let JoinEvent::WatermarkRead(read) = event else { unreachable!() };
                        let (reading, actions) =
                            match ctx.cross(state, read.generation, read.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        Ok(Transition {
                            next_state: JoinState::Hydrating(JoinHold {
                                reading,
                                witnessed: hold.witnessed.clone(),
                            }),
                            actions,
                        })
                    })
                };

                on JoinEvent::ReferenceEofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Hydrating(hold) = state else { unreachable!() };
                        let JoinEvent::ReferenceEofRead(eof) = event else { unreachable!() };
                        let (reading, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
                        let reading = reading.with_worst_of(eof);
                        let (witnessed, outcome) = ctx.fold_catalog_witness(&hold.witnessed, eof);
                        let next_state = match outcome {
                            HoldOutcome::Holding => {
                                JoinState::Hydrating(JoinHold { reading, witnessed })
                            }
                            HoldOutcome::LeftOpen => {
                                tracing::info!(
                                    stage_name = %ctx.stage_name(),
                                    "Join hydration left open; draining without held inputs"
                                );
                                actions.push(JoinAction::EmitHydrationHeartbeat);
                                JoinState::Draining(JoinDrain {
                                    step: DrainStep::Flush,
                                    worst_eof: reading.worst_eof,
                                    clock: None,
                                })
                            }
                            HoldOutcome::Completed(done) => {
                                tracing::info!(
                                    stage_name = %ctx.stage_name(),
                                    ended_by = ?done.ended_by,
                                    "Join hydration completed on natural EOFs"
                                );
                                actions.push(JoinAction::EmitHydrationHeartbeat);
                                match reading.plan.shape() {
                                    PlanShape::Live(reads)
                                    | PlanShape::ThenLive(ThenLivePhases::LiveOnly(reads)) => {
                                        actions.push(JoinAction::RecordPhase(done.live_entered()));
                                        JoinState::Live(JoinLive { reading, reads })
                                    }
                                    PlanShape::FiniteEof => {
                                        actions.push(JoinAction::RecordPhase(
                                            done.enriching_entered(),
                                        ));
                                        JoinState::Enriching(JoinEnrichment::ToEnd(reading))
                                    }
                                    PlanShape::ThenLive(
                                        ThenLivePhases::BatchThenLive(_) | ThenLivePhases::BatchOnly,
                                    ) => {
                                        actions.push(JoinAction::RecordPhase(
                                            done.enriching_entered(),
                                        ));
                                        JoinState::Enriching(JoinEnrichment::Holding(
                                            JoinHold::new(reading),
                                        ))
                                    }
                                }
                            }
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Enriching {
                on JoinEvent::WatermarkRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Enriching(enrichment) = state else { unreachable!() };
                        let JoinEvent::WatermarkRead(read) = event else { unreachable!() };
                        let (reading, actions) =
                            match ctx.cross(state, read.generation, read.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        let next_state = match enrichment {
                            JoinEnrichment::Holding(hold) => {
                                JoinState::Enriching(JoinEnrichment::Holding(JoinHold {
                                    reading,
                                    witnessed: hold.witnessed.clone(),
                                }))
                            }
                            JoinEnrichment::ToEnd(_) => {
                                JoinState::Enriching(JoinEnrichment::ToEnd(reading))
                            }
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on JoinEvent::StreamEofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Enriching(enrichment) = state else { unreachable!() };
                        let JoinEvent::StreamEofRead(eof) = event else { unreachable!() };
                        let (reading, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
                        let reading = reading.with_worst_of(eof);
                        let next_state = match enrichment {
                            JoinEnrichment::ToEnd(_) if eof.ends_reads => {
                                JoinState::Draining(JoinDrain {
                                    step: DrainStep::Finalize {
                                        final_eof: eof.envelope.clone(),
                                    },
                                    worst_eof: reading.worst_eof,
                                    clock: eof.clock(),
                                })
                            }
                            JoinEnrichment::ToEnd(_) => {
                                JoinState::Enriching(JoinEnrichment::ToEnd(reading))
                            }
                            JoinEnrichment::Holding(hold) => {
                                let (witnessed, outcome) =
                                    ctx.fold_batch_witness(&hold.witnessed, eof);
                                match (outcome, reading.plan.live_reads()) {
                                    (HoldOutcome::Holding, _) => {
                                        JoinState::Enriching(JoinEnrichment::Holding(JoinHold {
                                            reading,
                                            witnessed,
                                        }))
                                    }
                                    (HoldOutcome::LeftOpen, _) => {
                                        tracing::info!(
                                            stage_name = %ctx.stage_name(),
                                            "Join batch left open; draining without held inputs"
                                        );
                                        JoinState::Draining(JoinDrain {
                                            step: DrainStep::Flush,
                                            worst_eof: reading.worst_eof,
                                            clock: eof.clock(),
                                        })
                                    }
                                    (HoldOutcome::Completed(done), Some(reads)) => {
                                        tracing::info!(
                                            stage_name = %ctx.stage_name(),
                                            ended_by = ?done.ended_by,
                                            "Join batch completed on natural EOFs; going live"
                                        );
                                        actions.push(JoinAction::RecordPhase(done.live_entered()));
                                        JoinState::Live(JoinLive { reading, reads })
                                    }
                                    (HoldOutcome::Completed(_), None) if eof.ends_reads => {
                                        JoinState::Draining(JoinDrain {
                                            step: DrainStep::Finalize {
                                                final_eof: eof.envelope.clone(),
                                            },
                                            worst_eof: reading.worst_eof,
                                            clock: eof.clock(),
                                        })
                                    }
                                    (HoldOutcome::Completed(_), None) => {
                                        return Ok(ctx.fail(format!(
                                            "join '{}' completed its batch on an EOF that did not end its stream",
                                            ctx.stage_name()
                                        )));
                                    }
                                }
                            }
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Live {
                on JoinEvent::WatermarkRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Live(live) = state else { unreachable!() };
                        let JoinEvent::WatermarkRead(read) = event else { unreachable!() };
                        let (reading, actions) =
                            match ctx.cross(state, read.generation, read.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        Ok(Transition {
                            next_state: JoinState::Live(JoinLive { reading, reads: live.reads }),
                            actions,
                        })
                    })
                };

                on JoinEvent::ReferenceEofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Live(live) = state else { unreachable!() };
                        let JoinEvent::ReferenceEofRead(eof) = event else { unreachable!() };
                        let (reading, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
                        Ok(Transition {
                            next_state: JoinState::Live(JoinLive {
                                reading: reading.with_worst_of(eof),
                                reads: live.reads,
                            }),
                            actions,
                        })
                    })
                };

                on JoinEvent::StreamEofRead => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinState::Live(live) = state else { unreachable!() };
                        let JoinEvent::StreamEofRead(eof) = event else { unreachable!() };
                        let (reading, mut actions) =
                            match ctx.cross(state, eof.frontier.highest(), eof.frontier) {
                                Ok(crossed) => crossed,
                                Err(cause) => return Ok(ctx.fail(cause)),
                            };
                        actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
                        let reading = reading.with_worst_of(eof);
                        let next_state = if eof.ends_reads {
                            JoinState::Draining(JoinDrain {
                                step: DrainStep::Finalize {
                                    final_eof: eof.envelope.clone(),
                                },
                                worst_eof: reading.worst_eof,
                                clock: eof.clock(),
                            })
                        } else {
                            JoinState::Live(JoinLive { reading, reads: live.reads })
                        };
                        Ok(Transition { next_state, actions })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Draining {
                on JoinEvent::DrainStepEnded => |state, _, ctx| {
                    Box::pin(async move {
                        let JoinState::Draining(drain) = state else { unreachable!() };
                        let step = match &drain.step {
                            DrainStep::Finalize { .. } => DrainStep::Flush,
                            DrainStep::Flush => {
                                return Ok(ctx.fail(format!(
                                    "join '{}' reported a drain step after its flush",
                                    ctx.stage_name()
                                )));
                            }
                        };
                        Ok(Transition {
                            next_state: JoinState::Draining(JoinDrain { step, ..drain.clone() }),
                            actions: Vec::new(),
                        })
                    })
                };

                on JoinEvent::DrainComplete => |state, _, ctx| {
                    Box::pin(async move {
                        let JoinState::Draining(drain) = state else { unreachable!() };
                        if drain.step != DrainStep::Flush {
                            return Ok(ctx.fail(format!(
                                "join '{}' completed its drain before its flush",
                                ctx.stage_name()
                            )));
                        }
                        Ok(Transition {
                            next_state: JoinState::Finalizing,
                            actions: vec![
                                JoinAction::AuthorEof {
                                    kind: drain.worst_eof.unwrap_or(EofKind::Natural),
                                    clock: drain.clock.clone(),
                                },
                                JoinAction::SendCompletion,
                                JoinAction::Cleanup,
                                JoinAction::Host(SupervisorAction::CloseMailbox),
                                JoinAction::Host(SupervisorAction::SettlePublications),
                                JoinAction::Host(SupervisorAction::Emit(
                                    JoinEvent::FinalizationCompleted,
                                )),
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail(cause.clone()))
                    })
                };
            }

            state JoinState::Finalizing {
                on JoinEvent::FinalizationCompleted => |_, _, _| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JoinState::Drained,
                            actions: Vec::new(),
                        })
                    })
                };

                on JoinEvent::Error => |_, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(ctx.fail_uncounted(cause.clone()))
                    })
                };
            }

            state JoinState::Failing {
                on JoinEvent::TerminationSettled => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Failing(cause) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: JoinState::Failed(cause.clone()),
                            actions: Vec::new(),
                        })
                    })
                };
            }

            state JoinState::Cancelling {
                on JoinEvent::Error => |state, event, ctx| {
                    Box::pin(async move {
                        let JoinEvent::Error(cause) = event else { unreachable!() };
                        Ok(match JoinState::failure(cause.clone()) {
                            JoinState::Cancelling(_) => Transition {
                                next_state: state.clone(),
                                actions: Vec::new(),
                            },
                            _ => ctx.fail_uncounted(cause.clone()),
                        })
                    })
                };

                on JoinEvent::TerminationSettled => |state, _, _| {
                    Box::pin(async move {
                        let JoinState::Cancelling(cause) = state else { unreachable!() };
                        Ok(Transition {
                            next_state: JoinState::Cancelled(cause.clone()),
                            actions: Vec::new(),
                        })
                    })
                };
            }

            state JoinState::Drained {}
            state JoinState::Failed {}
            state JoinState::Cancelled {}

            unhandled => |state, event, _| {
                Box::pin(async move {
                    let ignored = matches!(event, JoinEvent::Initialize | JoinEvent::Ready)
                        || matches!(
                            state,
                            JoinState::Failing(_)
                                | JoinState::Cancelling(_)
                                | JoinState::Failed(_)
                                | JoinState::Cancelled(_)
                                | JoinState::Drained
                        );
                    if ignored {
                        return Ok(());
                    }
                    tracing::error!(
                        supervisor = "JoinSupervisor",
                        state = %state.variant_name(),
                        event = %event.variant_name(),
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(FsmError::UnhandledEvent {
                        state: state.variant_name().to_string(),
                        event: event.variant_name().to_string(),
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
            }) as ActionCompletion<JoinContext<H>, JoinEvent>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> JoinEvent {
        JoinEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(
            state,
            JoinState::Hydrating(_)
                | JoinState::Enriching(_)
                | JoinState::Live(_)
                | JoinState::Draining(_)
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
            JoinState::Hydrating(_) => {
                hydrating::dispatch_hydrating(self, state, ctx.resources_mut()?).await
            }
            JoinState::Enriching(_) => {
                enriching::dispatch_enriching(self, state, ctx.resources_mut()?).await
            }
            JoinState::Live(live) => {
                live::dispatch_live(self, state, live.reads, ctx.resources_mut()?).await
            }
            JoinState::Draining(drain) => {
                draining::dispatch_draining(self, drain, ctx.resources_mut()?).await
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
        if matches!(state, JoinState::Created(_) | JoinState::Initialized(_)) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, JoinState::Initializing(_)) && matches!(event, JoinEvent::Ready)
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
