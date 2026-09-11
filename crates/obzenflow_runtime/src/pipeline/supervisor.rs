// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Responsive inputs for the established self-supervised runner.

use super::fsm::{
    PipelineAction, PipelineContext, PipelineDeadline, PipelineFsmEvent, PipelineFsmState,
};
use super::resources::{OperationalFailure, ProducerTail};
use super::{FlowStopMode, PipelineControl, PipelineState};
use crate::messaging::{PollResult, SubscriptionPoller, SystemSubscription};
use crate::stages::common::stage_handle::StageError;
use crate::supervised_base::{
    EventLoopDirective, EventReceiver, HandleError, SelfSupervised, StateWatcher, SupervisorHandle,
};
use futures::{future::BoxFuture, FutureExt, Stream};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::id::SystemId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub use super::fsm::context::ContractEdgeStatus;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type JournalRead = BoxFuture<'static, (SystemSubscription<SystemEvent>, PollResult<SystemEvent>)>;

pub(crate) struct PipelineSupervisor {
    name: String,
    system_id: SystemId,
    controls: EventReceiver<PipelineFsmEvent>,
    controls_open: bool,
    watcher: StateWatcher<PipelineState>,
    subscription: Option<SystemSubscription<SystemEvent>>,
    pending_read: Mutex<Option<JournalRead>>,
    idle: Option<Pin<Box<tokio::time::Sleep>>>,
    next_input: usize,
    next_resource: usize,
    failure: OperationalFailure,
    failure_reported: bool,
}

impl PipelineSupervisor {
    pub(crate) fn new(
        system_id: SystemId,
        controls: EventReceiver<PipelineFsmEvent>,
        watcher: StateWatcher<PipelineState>,
        failure: OperationalFailure,
    ) -> Self {
        Self {
            name: "pipeline_supervisor".into(),
            system_id,
            controls,
            controls_open: true,
            watcher,
            subscription: None,
            pending_read: Mutex::new(None),
            idle: None,
            next_input: 0,
            next_resource: 0,
            failure,
            failure_reported: false,
        }
    }

    fn deadline(
        state: &PipelineFsmState,
        ctx: &PipelineContext,
    ) -> Option<(Instant, PipelineDeadline)> {
        if matches!(state, PipelineFsmState::PublishingFinalMarker) {
            // Execution and metrics have joined. An old stop deadline cannot
            // authorise another control publication behind the final marker.
            return None;
        }
        let graceful = ctx
            .stop_intent
            .deadline
            .filter(|_| matches!(ctx.stop_intent.mode, Some(FlowStopMode::Graceful { .. })))
            .map(|at| (at, PipelineDeadline::GracefulStop));
        let cleanup = ctx
            .progress
            .cleanup_deadline
            .filter(|_| !ctx.progress.stages_cancelled)
            .map(|at| (at, PipelineDeadline::StageCleanup));
        let metrics = ctx
            .resources
            .terminal_ack
            .get()
            .filter(|_| {
                !ctx.progress.metrics_cancelled
                    && !ctx.resources.metrics_joined
                    && ctx.resources.metrics.handle().is_some()
            })
            .map(|at| {
                (
                    *at + Duration::from_millis(ctx.metrics_drain_timeout_ms),
                    PipelineDeadline::Metrics,
                )
            });
        graceful
            .into_iter()
            .chain(cleanup)
            .chain(metrics)
            .min_by_key(|(at, _)| *at)
    }

    fn physical_ready(state: &PipelineFsmState, ctx: &mut PipelineContext) -> bool {
        use PipelineFsmState as S;
        match state {
            S::Materializing => ctx.resources.delivery.is_empty(),
            S::SettlingStages => {
                ctx.resources.stages_joined && ctx.resources.publication_settlement.is_none()
            }
            S::CatchingUpProducers => {
                matches!(ctx.resources.producer_tail, ProducerTail::Reached)
                    || ctx.progress.journal_failed
            }
            S::FinalisingMetrics => {
                ctx.resources.metrics_joined
                    && ctx.resources.publication_settlement.is_none()
                    && (ctx.progress.metrics_drained
                        || ctx.progress.metrics_cancelled
                        || ctx.resources.metrics.handle().is_none()
                        || ctx.resources.metrics.handle().is_some_and(|handle| {
                            matches!(
                                handle.current_state(),
                                crate::metrics::MetricsAggregatorState::Failed { .. }
                            )
                        }))
            }
            S::PublishingFinalMarker => {
                ctx.progress.final_marker_seen && ctx.resources.publication_settlement.is_none()
            }
            _ => false,
        }
    }

    fn poll_journal(
        &mut self,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        if ctx.progress.journal_failed
            || matches!(ctx.resources.producer_tail, ProducerTail::Reading(_))
        {
            return Poll::Pending;
        }
        let pending = self
            .pending_read
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        if pending.is_none() {
            if let Some(idle) = &mut self.idle {
                if idle.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
            self.idle = None;
            let Some(mut subscription) = self.subscription.take() else {
                return Poll::Pending;
            };
            *pending = Some(
                async move {
                    let result = subscription.poll_next().await;
                    (subscription, result)
                }
                .boxed(),
            );
        }
        let Poll::Ready((subscription, result)) =
            pending.as_mut().expect("owned read").as_mut().poll(cx)
        else {
            return Poll::Pending;
        };
        *pending = None;
        self.subscription = Some(subscription);
        Poll::Ready(match result {
            PollResult::Event(envelope) => {
                EventLoopDirective::Transition(PipelineFsmEvent::Journal(Box::new(envelope)))
            }
            PollResult::Error(error) => {
                ctx.progress.journal_failed = true;
                ctx.resources.retain_failure(error);
                EventLoopDirective::Continue
            }
            PollResult::NoEvents | PollResult::CursorAdvanced { .. } => {
                self.idle = Some(Box::pin(tokio::time::sleep(Duration::from_millis(10))));
                EventLoopDirective::Continue
            }
        })
    }

    fn poll_resources(
        &mut self,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        for offset in 0..4 {
            let input = (self.next_resource + offset) % 4;
            let ready = match input {
                0 => {
                    if let Some(joins) = &mut ctx.resources.stage_joins {
                        match Pin::new(joins.get_mut().unwrap_or_else(|e| e.into_inner()))
                            .poll_next(cx)
                        {
                            Poll::Ready(Some(result)) => {
                                if let Err(error) = result {
                                    if !(ctx.progress.stages_cancelled
                                        && matches!(error, StageError::Aborted))
                                    {
                                        ctx.resources.retain_failure(Box::new(error));
                                    }
                                }
                                true
                            }
                            Poll::Ready(None) => {
                                ctx.resources.stage_joins = None;
                                ctx.resources.stages_joined = true;
                                true
                            }
                            Poll::Pending => false,
                        }
                    } else {
                        false
                    }
                }
                1 => {
                    if let Some(observation) = &mut ctx.resources.publication_settlement {
                        match Pin::new(observation).poll(cx) {
                            Poll::Ready(result) => {
                                ctx.resources.publication_settlement = None;
                                if let Err(error) = result {
                                    ctx.resources.retain_failure(Box::new(error));
                                }
                                true
                            }
                            Poll::Pending => false,
                        }
                    } else {
                        false
                    }
                }
                2 => {
                    if let ProducerTail::Reading(read) = &mut ctx.resources.producer_tail {
                        match read
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_mut()
                            .poll(cx)
                        {
                            Poll::Ready(result) => {
                                ctx.resources.producer_tail = match result {
                                    Ok(Some(id)) if ctx.last_system_event_id_seen != Some(id) => {
                                        ProducerTail::Through(id)
                                    }
                                    Ok(_) => ProducerTail::Reached,
                                    Err(error) => {
                                        ctx.progress.journal_failed = true;
                                        ctx.resources.retain_failure(error);
                                        ProducerTail::Reached
                                    }
                                };
                                true
                            }
                            Poll::Pending => false,
                        }
                    } else {
                        false
                    }
                }
                _ => {
                    if let Some(join) = &mut ctx.resources.metrics_join {
                        match join
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_mut()
                            .poll(cx)
                        {
                            Poll::Ready(result) => {
                                ctx.resources.metrics_join = None;
                                ctx.resources.metrics_joined = true;
                                if let Err(error) = result {
                                    if !(ctx.progress.metrics_cancelled
                                        && matches!(error, HandleError::SupervisorAborted))
                                    {
                                        ctx.resources.retain_failure(Box::new(error));
                                    }
                                }
                                true
                            }
                            Poll::Pending => false,
                        }
                    } else {
                        false
                    }
                }
            };
            if ready {
                // A producer or our own append has settled. Check the existing
                // journal again without extending a previous temporary-EOF wait.
                self.idle = None;
                self.next_resource = (input + 1) % 4;
                return Poll::Ready(EventLoopDirective::Continue);
            }
        }
        Poll::Pending
    }
}

impl crate::supervised_base::base::Supervisor for PipelineSupervisor {
    type State = PipelineFsmState;
    type Event = PipelineFsmEvent;
    type Context = PipelineContext;
    type Action = PipelineAction;

    fn build_state_machine(&self, initial_state: Self::State) -> super::fsm::PipelineFsm {
        super::fsm::build_pipeline_fsm_with_initial(initial_state)
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
    fn event_for_action_error(&self, message: String) -> PipelineFsmEvent {
        PipelineFsmEvent::OperationalFailure { message }
    }

    async fn after_transition(
        &mut self,
        state: &Self::State,
        ctx: &PipelineContext,
    ) -> Result<(), BoxError> {
        // The shared runner routes an action error through the failure FSM
        // before calling this hook. Do not report that same retained error a
        // second time through dispatch; asynchronous owner failures still enter
        // the first-failure check there.
        self.failure_reported |= ctx.resources.failure.get().is_some();
        let projection = state.public_state(ctx);
        if projection != self.watcher.current() {
            let _ = self.watcher.update(projection);
        }
        Ok(())
    }

    async fn write_completion_event(&self) -> Result<(), BoxError> {
        // Settlement already happened in the FSM. This hook only returns the
        // retained operational result through the established runner contract.
        self.failure
            .get()
            .map_or(Ok(()), |error| Err(Box::new(error.clone()) as BoxError))
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut PipelineContext,
    ) -> Result<EventLoopDirective<Self::Event>, BoxError> {
        if matches!(state, PipelineFsmState::Finished { .. }) {
            return Ok(EventLoopDirective::Terminate);
        }
        if self.subscription.is_none()
            && self
                .pending_read
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
        {
            self.subscription = ctx.completion_subscription.take();
        }
        let mut deadline_wait = Box::pin(tokio::time::sleep(Duration::ZERO));
        Ok(std::future::poll_fn(|cx| {
            if let Some((at, deadline)) = Self::deadline(state, ctx) {
                if Instant::now() >= at {
                    return Poll::Ready(EventLoopDirective::Transition(
                        PipelineFsmEvent::Deadline(deadline),
                    ));
                }
                deadline_wait.as_mut().reset(at.into());
                let _ = deadline_wait.as_mut().poll(cx);
            }
            if let Some(error) = ctx.resources.publications.first_failure() {
                ctx.resources.retain_failure(Box::new(error));
            }
            if !self.failure_reported {
                if let Some(error) = ctx.resources.failure.get() {
                    self.failure_reported = true;
                    return Poll::Ready(EventLoopDirective::Transition(
                        PipelineFsmEvent::OperationalFailure {
                            message: error.to_string(),
                        },
                    ));
                }
            }
            if Self::physical_ready(state, ctx) {
                return Poll::Ready(EventLoopDirective::Transition(
                    PipelineFsmEvent::PhysicalSettlementSatisfied,
                ));
            }
            for offset in 0..4 {
                let input = (self.next_input + offset) % 4;
                let result = match input {
                    0 if self.controls_open => {
                        match std::pin::pin!(self.controls.recv()).as_mut().poll(cx) {
                            Poll::Ready(Some(event)) => {
                                Poll::Ready(EventLoopDirective::Transition(event))
                            }
                            Poll::Ready(None) => {
                                self.controls_open = false;
                                Poll::Ready(EventLoopDirective::Continue)
                            }
                            Poll::Pending => Poll::Pending,
                        }
                    }
                    1 => self.poll_journal(ctx, cx),
                    2 => match ctx.resources.delivery.poll(cx) {
                        Poll::Ready(Some(result)) => {
                            self.idle = None;
                            if let Err(error) = result {
                                ctx.resources.retain_failure(Box::new(error));
                            }
                            Poll::Ready(EventLoopDirective::Continue)
                        }
                        _ => Poll::Pending,
                    },
                    3 => match self.poll_resources(ctx, cx) {
                        Poll::Pending if matches!(state, PipelineFsmState::Created) => {
                            Poll::Ready(EventLoopDirective::Transition(PipelineFsmEvent::Bootstrap))
                        }
                        Poll::Pending
                            if matches!(state, PipelineFsmState::ReadyForRun)
                                && !crate::bootstrap::startup_mode_manual() =>
                        {
                            Poll::Ready(EventLoopDirective::Transition(PipelineFsmEvent::Control(
                                PipelineControl::Start,
                            )))
                        }
                        result => result,
                    },
                    _ => Poll::Pending,
                };
                if let Poll::Ready(directive) = result {
                    self.next_input = (input + 1) % 4;
                    return Poll::Ready(directive);
                }
            }
            Poll::Pending
        })
        .await)
    }
}
