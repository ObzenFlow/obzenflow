// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform supervisor implementation using HandlerSupervised pattern
//!
//! Decomposed into submodules:
//! - `running.rs`  — Running state event loop
//! - `draining.rs` — Draining state event loop
//! - `tests.rs`    — All unit tests

mod direct_fact_continuation;
mod draining;
mod running;
#[cfg(test)]
mod tests;

use super::fsm::{
    TransformAction, TransformContext, TransformEvent, TransformResources, TransformState,
};
use crate::messaging::DeliveredRecord;
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::cycle_guard::CycleGuard;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event as forward_control_event_helper;
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, OwnedDispatch, SupervisorAction,
};
use crate::supervised_base::loop_timing::{phase, Phase};
use crate::supervised_base::{
    publication, EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::{ExecutionAccounting, FlowContext};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::{AppendOptions, Journal};
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, FsmError, StateMachine, StateVariant, Transition};
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<
    H: UnifiedTransformHandler + Clone + Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Subscription to upstream events (supervisor-owned to avoid borrow conflicts).
    pub(super) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Supervisor-level cycle protection for backflow cycle members (FLOWIP-051l).
    pub(crate) cycle_guard: Option<CycleGuard>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: PhantomData<H>,
}

impl<H: UnifiedTransformHandler + Clone + Debug + Send + Sync + 'static> Supervisor
    for TransformSupervisor<H>
{
    type State = TransformState<H>;
    type Event = TransformEvent<H>;
    type Context = TransformContext<H>;
    type Action = TransformAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state: TransformState<H>;
            event: TransformEvent<H>;
            context: TransformContext<H>;
            action: TransformAction<H>;
            initial: initial_state;

            state TransformState::Created {
                on TransformEvent::Initialize => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Initializing, actions: vec![TransformAction::Host(SupervisorAction::Register), TransformAction::AllocateResources, TransformAction::Host(SupervisorAction::Emit(TransformEvent::InitializationCompleted))] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Initializing {
                on TransformEvent::InitializationCompleted => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Initialized, actions: vec![] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Initialized {
                on TransformEvent::Ready => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Starting, actions: vec![TransformAction::PublishRunning, TransformAction::Host(SupervisorAction::Emit(TransformEvent::ActivationCompleted))] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Starting {
                on TransformEvent::ActivationCompleted => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Running, actions: vec![] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Running {
                on TransformEvent::ReceivedEOF => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Draining, actions: vec![] }) })
                };
                on TransformEvent::BeginDrain => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Draining, actions: vec![] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Draining {
                on TransformEvent::ReceivedEOF => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Draining, actions: vec![] }) })
                };
                on TransformEvent::DrainComplete => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Finalising, actions: vec![TransformAction::DrainHandler, TransformAction::ForwardEOF, TransformAction::SendCompletion, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::FinalisationCompleted))] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Finalising {
                on TransformEvent::FinalisationCompleted => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Drained, actions: vec![] }) })
                };
                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition {
                        next_state: TransformState::failure(cause.clone()),
                        actions: vec![TransformAction::SendFailure { message: cause }, TransformAction::Cleanup, TransformAction::Host(SupervisorAction::CloseMailbox), TransformAction::Host(SupervisorAction::SettlePublications), TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled))],
                    }) })
                };
            }

            state TransformState::Drained {

            }

            state TransformState::Failing {
                on TransformEvent::TerminationSettled => |state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Failed(cause), actions: vec![] }) })
                };
            }

            state TransformState::Failed {

            }

            state TransformState::Cancelling {
                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformEvent::Error(cause) = event else { unreachable!() };
                    let cause = cause.clone();
                    let next = TransformState::failure(cause.clone());
                    let repeated_cancel = matches!(next, TransformState::Cancelling(_));
                    let next_state = if repeated_cancel { state.clone() } else { next };
                    Box::pin(async move { Ok(Transition { next_state, actions: if repeated_cancel { vec![] } else { vec![
                        TransformAction::SendFailure { message: cause },
                        TransformAction::Cleanup,
                        TransformAction::Host(SupervisorAction::CloseMailbox),
                        TransformAction::Host(SupervisorAction::SettlePublications),
                        TransformAction::Host(SupervisorAction::Emit(TransformEvent::TerminationSettled)),
                    ] } }) })
                };

                on TransformEvent::TerminationSettled => |state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let TransformState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: TransformState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state TransformState::Cancelled {

            }
            unhandled => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                let state = state.clone();
                let event = event.clone();
                Box::pin(async move {
                    if matches!(event, TransformEvent::Initialize | TransformEvent::Ready | TransformEvent::BeginDrain)
                        || matches!(state, TransformState::Failing(_) | TransformState::Cancelling(_) | TransformState::Failed(_) | TransformState::Cancelled(_) | TransformState::Drained)
                    {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent { state: state.variant_name().into(), event: event.variant_name().into() })
                })
            };
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Transform
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
                StageType::Transform,
            ),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedTransformHandler + Clone + Debug + Send + Sync + 'static> HandlerSupervised
    for TransformSupervisor<H>
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
            TransformAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        let mut resources = context.resources.take().ok_or_else(|| {
            FsmError::HandlerError("transform operation already owns resources".into())
        })?;
        if resources.subscription.is_none() {
            resources.subscription = self.subscription.take();
        }
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            let resources = Some(resources);
            Box::new(move |context: &mut TransformContext<H>| {
                context.resources = resources;
                result.map(|()| None).map_err(Into::into)
            }) as ActionCompletion<TransformContext<H>, TransformEvent<H>>
        })))
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> TransformEvent<H> {
        TransformEvent::Error(msg)
    }

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(state, TransformState::Running | TransformState::Draining) {
            return None;
        }
        let resources = context.resources.take()?;
        let state = state.clone();
        let mut worker = Self {
            name: self.name.clone(),
            data_journal: self.data_journal.clone(),
            stage_id: self.stage_id,
            subscription: self.subscription.take(),
            cycle_guard: self.cycle_guard.take(),
            _marker: PhantomData,
        };
        Some(Box::pin(async move {
            let mut owned_context = TransformContext::new(resources);
            let result = worker.dispatch_state(&state, &mut owned_context).await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                owner.subscription = worker.subscription;
                owner.cycle_guard = worker.cycle_guard;
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
            TransformState::Initializing
            | TransformState::Starting
            | TransformState::Finalising
            | TransformState::Failing(_)
            | TransformState::Cancelling(_) => Ok(EventLoopDirective::Continue),
            TransformState::Cancelled(_) => Ok(EventLoopDirective::Terminate),

            TransformState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            TransformState::Initialized => Ok(EventLoopDirective::Continue),

            TransformState::Running => {
                running::dispatch_running(self, state, ctx.resources_mut()?).await
            }

            TransformState::Draining => {
                draining::dispatch_draining(self, state, ctx.resources_mut()?).await
            }

            TransformState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            TransformState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            TransformState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: UnifiedTransformHandler + Clone + Debug + Send + Sync + 'static> ExternalEventPolicy
    for TransformSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, TransformState::Created | TransformState::Initialized) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, TransformState::Initializing)
            && matches!(event, TransformEvent::Ready | TransformEvent::BeginDrain)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            TransformState::Drained | TransformState::Failed(_) | TransformState::Cancelled(_)
        ) {
            None
        } else {
            Some(TransformEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Helper methods shared by running.rs and draining.rs
// ---------------------------------------------------------------------------

impl<H: UnifiedTransformHandler + Clone + Debug + Send + Sync + 'static> TransformSupervisor<H> {
    pub(super) async fn check_cycle_guard_data_event(
        &mut self,
        ctx: &mut TransformResources<H>,
        envelope: &mut DeliveredRecord<ChainPayload>,
        upstream: Option<StageId>,
        write_error_context: &'static str,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let _control = phase(Phase::Control);
        let Some(guard) = &mut self.cycle_guard else {
            return Ok(false);
        };

        if envelope.consumes_data_credit() {
            let checked = guard.check_data(envelope.authored_mut());
            if let Err(error_event) = checked {
                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: StageType::Transform,
                };

                let error_event = ctx
                    .instrumentation
                    .capture_accounting()
                    .attach_to((*error_event).with_flow_context(flow_context));

                let journal = ctx.error_journal.clone();
                let parent = envelope.clone();
                let instrumentation = ctx.instrumentation.clone();
                {
                    let _publishing = phase(Phase::Publish);
                    publication::commit(async move {
                        crate::supervised_base::publication::append_inline(
                            &journal,
                            error_event,
                            AppendOptions::from_record(Some(&parent))
                                .unwrap()
                                .with_capture(
                                    instrumentation.journal_capture(None, vec![(0, false)]),
                                ),
                        )
                        .await?;
                        instrumentation.record_error(ErrorKind::Unknown);
                        Ok(())
                    })
                    .await
                    .map_err(|e| format!("{write_error_context}: {e}"))?;
                }

                if let Some(upstream) = upstream {
                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                        let _acknowledging = phase(Phase::Acknowledge);
                        reader.ack_consumed(1);
                        crate::supervised_base::loop_timing::acknowledgement();
                    }
                }

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) async fn forward_control_event_guarded(
        &mut self,
        envelope: &DeliveredRecord<ChainPayload>,
        stage_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _control = phase(Phase::Control);
        let should_forward = self
            .cycle_guard
            .as_mut()
            .map(|guard| guard.should_forward_signal(&envelope.authored()))
            .unwrap_or(true);

        if should_forward {
            self.forward_control_event(envelope, stage_name).await?;
        }

        Ok(())
    }

    pub(super) async fn maybe_release_buffered_terminal(
        &mut self,
        ctx: &mut TransformResources<H>,
    ) -> Result<Option<EventLoopDirective<TransformEvent<H>>>, Box<dyn Error + Send + Sync>> {
        let _control = phase(Phase::Control);
        let Some(cfg) = ctx.cycle_guard_config.as_ref() else {
            return Ok(None);
        };
        if !cfg.is_entry_point {
            return Ok(None);
        }

        let Some(buffered) = ctx.buffered_terminal_envelope.take() else {
            return Ok(None);
        };

        if cfg.scc_internal_edges.is_empty() {
            tracing::error!(
                stage_name = %ctx.stage_name,
                scc_id = %cfg.scc_id,
                "Cycle entry point has no scc_internal_edges; refusing to release terminal"
            );
            ctx.buffered_terminal_envelope = Some(buffered);
            return Ok(None);
        }

        let terminal_ready = cfg
            .external_upstreams
            .is_subset(&ctx.external_eofs_received)
            || ctx.drain_received;
        if !terminal_ready {
            ctx.buffered_terminal_envelope = Some(buffered);
            return Ok(None);
        }

        for &(upstream, downstream) in &cfg.scc_internal_edges {
            match ctx
                .backpressure_registry
                .edge_in_flight(upstream, downstream)
            {
                Some(0) => continue,
                Some(_) => {
                    ctx.buffered_terminal_envelope = Some(buffered);
                    return Ok(None);
                }
                None => {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        scc_id = %cfg.scc_id,
                        upstream = ?upstream,
                        downstream = ?downstream,
                        "SCC-internal edge has no backpressure tracking; refusing to release terminal"
                    );
                    ctx.buffered_terminal_envelope = Some(buffered);
                    return Ok(None);
                }
            }
        }

        tracing::info!(
            stage_name = %ctx.stage_name,
            scc_id = %cfg.scc_id,
            "Cycle entry point releasing buffered terminal signal (SCC quiescent)"
        );

        self.forward_control_event_guarded(&buffered, &ctx.stage_name)
            .await?;
        Ok(Some(EventLoopDirective::Transition(
            TransformEvent::ReceivedEOF,
        )))
    }

    /// Helper to forward control events
    pub(super) async fn forward_control_event(
        &self,
        envelope: &DeliveredRecord<ChainPayload>,
        stage_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _publishing = phase(Phase::Publish);
        let _ = forward_control_event_helper(
            envelope,
            self.stage_id,
            stage_name,
            StageType::Transform,
            &self.data_journal,
        )
        .await?;
        Ok(())
    }
}
