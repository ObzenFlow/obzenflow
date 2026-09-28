// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Poll owned child results and publications. No child journal reader belongs
//! to pipeline coordination.

use super::fsm::{PipelineAction, PipelineContext, PipelineFsmEvent as E, PipelineFsmState as S};
use super::resources::{Observations, OperationalFailure};
use super::PipelineState;
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::publication::BoxError;
use crate::supervised_base::{
    EventLoopDirective, EventReceiver, HandleError, SelfSupervised, StateWatcher,
};
use futures::Stream;
use obzenflow_core::event::WriterId;
use obzenflow_core::id::SystemId;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub(crate) struct PipelineSupervisor {
    name: String,
    system_id: SystemId,
    controls: EventReceiver<E>,
    journal: std::sync::Arc<dyn obzenflow_core::Journal<obzenflow_core::event::SystemEvent>>,
    controls_open: bool,
    watcher: StateWatcher<PipelineState>,
    next_input: usize,
    failure: OperationalFailure,
    failure_reported: bool,
}

impl PipelineSupervisor {
    pub(crate) fn new(
        system_id: SystemId,
        controls: EventReceiver<E>,
        watcher: StateWatcher<PipelineState>,
        failure: OperationalFailure,
        journal: std::sync::Arc<dyn obzenflow_core::Journal<obzenflow_core::event::SystemEvent>>,
    ) -> Self {
        Self {
            name: "pipeline_supervisor".into(),
            system_id,
            journal,
            controls,
            controls_open: true,
            watcher,
            next_input: 0,
            failure,
            failure_reported: false,
        }
    }
    fn poll_observation(observations: &mut Observations, cx: &mut Context<'_>) -> Poll<E> {
        match Pin::new(observations.get_mut().unwrap_or_else(|e| e.into_inner())).poll_next(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(event),
            _ => Poll::Pending,
        }
    }
    fn poll_dispatch(
        &mut self,
        state: &S,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
        mut deadline: Pin<&mut tokio::time::Sleep>,
        allow_phase: bool,
    ) -> Poll<E> {
        if let Some(error) = ctx.resources.publications.first_failure() {
            ctx.resources.retain_failure(Box::new(error));
        }
        if !self.failure_reported {
            if let Some(error) = self.failure.get() {
                self.failure_reported = true;
                return Poll::Ready(E::OperationalFailure {
                    message: error.to_string(),
                });
            }
        }
        if let Some((at, event)) = state.next_deadline(ctx) {
            if Instant::now() >= at {
                return Poll::Ready(event.into());
            }
            deadline.as_mut().reset(at.into());
            let _ = deadline.as_mut().poll(cx);
        }
        if allow_phase {
            if state.phase_satisfied(ctx) {
                return Poll::Ready(E::PhaseSatisfied);
            }
            if matches!(state, S::ReadyForRun) && !crate::bootstrap::startup_mode_manual() {
                return Poll::Ready(E::Start);
            }
        }
        // Fair polling prevents a busy command producer from starving physical
        // termination or acknowledgements. Empty observation sets stay pending.
        const INPUTS: usize = 7;
        for offset in 0..INPUTS {
            let input = (self.next_input + offset) % INPUTS;
            let result = match input {
                0 if self.controls_open => {
                    match std::pin::pin!(self.controls.recv()).as_mut().poll(cx) {
                        Poll::Ready(Some(event)) => Poll::Ready(event),
                        Poll::Ready(None) => {
                            self.controls_open = false;
                            Poll::Pending
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }
                1 => Self::poll_observation(&mut ctx.resources.failures, cx),
                2 => Self::poll_observation(&mut ctx.resources.acknowledgements, cx),
                3 => Self::poll_observation(&mut ctx.resources.exits, cx),
                4 => Self::poll_observation(&mut ctx.resources.publication_results, cx),
                5 => match ctx.resources.delivery.poll(cx) {
                    Poll::Ready(Some((id, Err(error))))
                        if ctx.outstanding_children.contains(&id)
                            && !matches!(
                                state,
                                S::CancellingChildren | S::FailingChildren { .. }
                            ) =>
                    {
                        Poll::Ready(E::OperationalFailure {
                            message: error.to_string(),
                        })
                    }
                    Poll::Ready(Some(_)) => Poll::Ready(E::ObservationEnded),
                    _ => Poll::Pending,
                },
                6 => match &mut ctx.resources.metrics_join {
                    Some(join) => match join
                        .get_mut()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_mut()
                        .poll(cx)
                    {
                        Poll::Ready(result) => {
                            ctx.resources.metrics_join = None;
                            match result {
                                // The owner enforces the bounded final-refresh budget.
                                Err(HandleError::SupervisorAborted)
                                    if ctx.metrics_deadline.is_none() =>
                                {
                                    Poll::Ready(E::MetricsExited)
                                }
                                Err(error) => Poll::Ready(E::OperationalFailure {
                                    message: error.to_string(),
                                }),
                                Ok(()) => Poll::Ready(E::MetricsExited),
                            }
                        }
                        Poll::Pending => Poll::Pending,
                    },
                    None => Poll::Pending,
                },
                _ => Poll::Pending,
            };
            if result.is_ready() {
                self.next_input = (input + 1) % INPUTS;
                return result;
            }
        }
        Poll::Pending
    }
}

impl crate::supervised_base::base::Supervisor for PipelineSupervisor {
    type State = S;
    type Event = E;
    type Context = PipelineContext;
    type Action = PipelineAction;
    fn build_state_machine(&self, state: S) -> super::fsm::PipelineFsm {
        super::fsm::build_pipeline_fsm_with_initial(state)
    }
    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind::Pipeline
    }
    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        crate::supervised_base::base::register_system(
            context.system_journal.clone(),
            self.system_id.into(),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl SelfSupervised for PipelineSupervisor {
    fn writer_id(&self) -> WriterId {
        self.system_id.into()
    }
    fn event_for_action_error(&self, message: String) -> E {
        E::OperationalFailure { message }
    }
    fn supervisor_action(&self, action: &PipelineAction) -> Option<SupervisorAction<E>> {
        if let PipelineAction::Host(action) = action {
            Some(action.clone())
        } else {
            None
        }
    }
    fn lifecycle_phase(&self, state: &S) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        use super::termination::ExecutionOutcome;
        use crate::stages::common::stage_lifecycle::LifecyclePhase as L;
        match state {
            S::ReadyForRun => L::Ready,
            S::Running => L::Active,
            S::FailingChildren { cause } => L::Failing(cause.clone()),
            S::CancellingChildren => L::Cancelling("Pipeline cancellation requested".into()),
            S::Finished {
                outcome: ExecutionOutcome::Failed(failure),
            } => L::Failed(failure.reason.clone()),
            S::Finished {
                outcome: ExecutionOutcome::Cancelled { reason },
            } => L::Cancelled(reason.clone()),
            S::Finished { .. } => L::Completed,
            _ => L::Other,
        }
    }
    fn after_transition(&mut self, state: &S, ctx: &PipelineContext) {
        self.failure_reported |= ctx.resources.failure.get().is_some();
        let projection = state.public_state(ctx);
        if projection != self.watcher.current() {
            let _ = self.watcher.update(projection);
        }
    }
    async fn next_control(&mut self, state: &S, ctx: &mut PipelineContext) -> Option<E> {
        let mut deadline = Box::pin(tokio::time::sleep(Duration::ZERO));
        Some(
            std::future::poll_fn(|cx| self.poll_dispatch(state, ctx, cx, deadline.as_mut(), false))
                .await,
        )
    }
    fn close_mailbox(
        &mut self,
        state: &S,
    ) -> futures::future::BoxFuture<'static, Result<(), BoxError>> {
        use obzenflow_fsm::StateVariant;
        self.controls_open = false;
        crate::supervised_base::with_external_events::record_terminal_commands(
            &mut self.controls,
            crate::supervised_base::with_external_events::system_commands(
                self.journal.clone(),
                self.system_id.into(),
            ),
            &self.name,
            state.variant_name(),
        )
    }
    async fn dispatch_state(
        &mut self,
        state: &S,
        ctx: &mut PipelineContext,
    ) -> Result<EventLoopDirective<E>, BoxError> {
        if matches!(state, S::Created) {
            return Ok(EventLoopDirective::Transition(E::Bootstrap));
        }
        if matches!(state, S::Finished { .. }) {
            return Ok(EventLoopDirective::Terminate);
        }
        let mut deadline = Box::pin(tokio::time::sleep(Duration::ZERO));
        let event =
            std::future::poll_fn(|cx| self.poll_dispatch(state, ctx, cx, deadline.as_mut(), true))
                .await;
        Ok(EventLoopDirective::Transition(event))
    }
}

impl crate::supervised_base::with_external_events::ExternalControlEvent for E {
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Abort { reason } | Self::OperationalFailure { message: reason } => Some(reason),
            _ => None,
        })
    }
}
