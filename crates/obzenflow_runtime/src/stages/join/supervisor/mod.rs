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
use super::transitions;
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
use obzenflow_fsm::{fsm, FsmError, StateMachine, StateVariant};
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
                on JoinEvent::Initialize => transitions::initialize;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Initializing {
                on JoinEvent::InitializationCompleted => transitions::initialized;
                on JoinEvent::Error => transitions::failure;
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => transitions::ready;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Starting {
                on JoinEvent::ActivationCompleted => transitions::activated;
                on JoinEvent::Error => transitions::failure;
            }

            state JoinState::Hydrating {
                on JoinEvent::WatermarkRead => transitions::watermark;
                on JoinEvent::ReferenceEofRead => transitions::hydrating_eof;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Enriching {
                on JoinEvent::WatermarkRead => transitions::watermark;
                on JoinEvent::StreamEofRead => transitions::enriching_eof;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Live {
                on JoinEvent::WatermarkRead => transitions::watermark;
                on JoinEvent::ReferenceEofRead => transitions::live_reference_eof;
                on JoinEvent::StreamEofRead => transitions::live_stream_eof;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Draining {
                on JoinEvent::DrainStepEnded => transitions::drain_step_ended;
                on JoinEvent::DrainComplete => transitions::drain_complete;
                on JoinEvent::Error => transitions::counted_failure;
            }

            state JoinState::Finalizing {
                on JoinEvent::FinalizationCompleted => transitions::finalized;
                on JoinEvent::Error => transitions::failure;
            }

            state JoinState::Failing {
                on JoinEvent::TerminationSettled => transitions::failure_settled;
            }

            state JoinState::Cancelling {
                on JoinEvent::Error => transitions::cancelling_error;
                on JoinEvent::TerminationSettled => transitions::cancellation_settled;
            }

            state JoinState::Drained {}
            state JoinState::Failed {}
            state JoinState::Cancelled {}

            unhandled => transitions::unhandled;
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
