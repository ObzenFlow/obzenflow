// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join supervisor implementation using the HandlerSupervised pattern

pub(super) mod common;
mod draining;
mod enriching;
mod hydrating;
mod live;

use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState};
use super::transitions::{
    drain_complete, drain_step_ended, enriching_eof, enriching_watermark, failure, hydrating_eof,
    hydrating_watermark, live_eof, live_watermark, opening, JoinInputs, JoinTransition,
};
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

fn counted_failure<H: UnifiedJoinHandler>(
    ctx: &JoinContext<H>,
    cause: String,
) -> JoinTransition<H> {
    ctx.instrumentation
        .failures_total
        .fetch_add(1, Ordering::Relaxed);
    failure(cause)
}

fn decide<H: UnifiedJoinHandler>(
    ctx: &JoinContext<H>,
    from: &JoinState,
    step: impl FnOnce(&JoinInputs<'_>) -> Result<JoinTransition<H>, String>,
) -> JoinTransition<H> {
    let Some(resources) = ctx.resources.as_ref() else {
        return counted_failure(ctx, "join resources belong to a pending operation".into());
    };
    let inputs = JoinInputs::of(resources);
    match step(&inputs) {
        Ok(transition) => {
            if transition.next_state.variant_name() != from.variant_name() {
                tracing::info!(
                    stage_name = %inputs.stage_name,
                    from = from.variant_name(),
                    to = transition.next_state.variant_name(),
                    "Join changed phase"
                );
            }
            transition
        }
        Err(cause) => counted_failure(ctx, cause),
    }
}

macro_rules! join_error_arm {
    (counted) => {
        |_state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
            let JoinEvent::Error(cause) = event else {
                unreachable!()
            };
            let transition = counted_failure(ctx, cause.clone());
            Box::pin(async move { Ok(transition) })
        }
    };
    (uncounted) => {
        |_state: &JoinState, event: &JoinEvent, _ctx: &mut JoinContext<H>| {
            let JoinEvent::Error(cause) = event else {
                unreachable!()
            };
            let transition = failure::<H>(cause.clone());
            Box::pin(async move { Ok(transition) })
        }
    };
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
                on JoinEvent::Initialize => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Created(plan) = state else { unreachable!() };
                    let plan = *plan;
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JoinState::Initializing(plan),
                            actions: vec![
                                JoinAction::Host(SupervisorAction::Register),
                                JoinAction::AllocateResources,
                                JoinAction::Host(SupervisorAction::Emit(JoinEvent::InitializationCompleted)),
                            ],
                        })
                    })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Initializing {
                on JoinEvent::InitializationCompleted => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Initializing(plan) = state else { unreachable!() };
                    let plan = *plan;
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Initialized(plan), actions: vec![] }) })
                };
                on JoinEvent::Error => join_error_arm!(uncounted);
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Initialized(plan) = state else { unreachable!() };
                    let plan = *plan;
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JoinState::Starting(plan),
                            actions: vec![
                                JoinAction::PublishRunning,
                                JoinAction::Host(SupervisorAction::Emit(JoinEvent::ActivationCompleted)),
                            ],
                        })
                    })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Starting {
                on JoinEvent::ActivationCompleted => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Starting(plan) = state else { unreachable!() };
                    let transition = opening::<H>(*plan);
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::Error => join_error_arm!(uncounted);
            }

            state JoinState::Hydrating {
                on JoinEvent::WatermarkRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Hydrating(reading), JoinEvent::WatermarkRead(watermark)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| hydrating_watermark(reading, watermark, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::EofRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Hydrating(reading), JoinEvent::EofRead(eof)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| hydrating_eof(reading, eof, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Enriching {
                on JoinEvent::WatermarkRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Enriching(reading), JoinEvent::WatermarkRead(watermark)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| enriching_watermark(reading, watermark, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::EofRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Enriching(reading), JoinEvent::EofRead(eof)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| enriching_eof(reading, eof, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Live {
                on JoinEvent::WatermarkRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Live(reading), JoinEvent::WatermarkRead(watermark)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| live_watermark(reading, watermark, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::EofRead => |state: &JoinState, event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let (JoinState::Live(reading), JoinEvent::EofRead(eof)) = (state, event) else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| live_eof(reading, eof, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Draining {
                on JoinEvent::DrainStepEnded => |state: &JoinState, _event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let JoinState::Draining(drain) = state else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| drain_step_ended(drain, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::DrainComplete => |state: &JoinState, _event: &JoinEvent, ctx: &mut JoinContext<H>| {
                    let JoinState::Draining(drain) = state else { unreachable!() };
                    let transition = decide(ctx, state, |inputs| drain_complete(drain, inputs));
                    Box::pin(async move { Ok(transition) })
                };
                on JoinEvent::Error => join_error_arm!(counted);
            }

            state JoinState::Finalizing {
                on JoinEvent::FinalizationCompleted => |_state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Drained, actions: vec![] }) })
                };
                on JoinEvent::Error => join_error_arm!(uncounted);
            }

            state JoinState::Failing {
                on JoinEvent::TerminationSettled => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Failed(cause), actions: vec![] }) })
                };
            }

            state JoinState::Cancelling {
                on JoinEvent::Error => |state: &JoinState, event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinEvent::Error(cause) = event else { unreachable!() };
                    let transition = if matches!(JoinState::failure(cause.clone()), JoinState::Cancelling(_)) {
                        Transition { next_state: state.clone(), actions: vec![] }
                    } else {
                        failure::<H>(cause.clone())
                    };
                    Box::pin(async move { Ok(transition) })
                };

                on JoinEvent::TerminationSettled => |state: &JoinState, _event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                    let JoinState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: JoinState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state JoinState::Drained {}
            state JoinState::Failed {}
            state JoinState::Cancelled {}

            unhandled => |state: &JoinState, event: &JoinEvent, _ctx: &mut JoinContext<H>| {
                let ignored = matches!(event, JoinEvent::Initialize | JoinEvent::Ready)
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
            JoinState::Enriching(reading) => {
                enriching::dispatch_enriching(self, state, reading, ctx.resources_mut()?).await
            }
            JoinState::Live(reading) => {
                live::dispatch_live(self, state, reading, ctx.resources_mut()?).await
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
