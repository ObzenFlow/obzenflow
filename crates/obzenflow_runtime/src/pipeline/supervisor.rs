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
    input_cursor: RoundRobinCursor<SupervisorInput>,
    resource_cursor: RoundRobinCursor<ResourceInput>,
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
            input_cursor: RoundRobinCursor::new(SupervisorInput::ORDER),
            resource_cursor: RoundRobinCursor::new(ResourceInput::ORDER),
            failure,
            failure_reported: false,
        }
    }

    fn attach_journal_subscription(&mut self, ctx: &mut PipelineContext) {
        if self.subscription.is_none()
            && self
                .pending_read
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .is_none()
        {
            self.subscription = ctx.completion_subscription.take();
        }
    }

    fn poll_dispatch(
        &mut self,
        state: &PipelineFsmState,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
        deadline_wait: Pin<&mut tokio::time::Sleep>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        // Deadlines, failures and completed settlement take priority over the
        // ordinary inputs, which share the remaining dispatch turns fairly.
        if let Poll::Ready(event) = Self::poll_deadline(state, ctx, cx, deadline_wait) {
            return Poll::Ready(EventLoopDirective::Transition(event));
        }
        if let Some(event) = self.take_operational_failure(ctx) {
            return Poll::Ready(EventLoopDirective::Transition(event));
        }
        if Self::settlement_satisfied(state, ctx) {
            return Poll::Ready(EventLoopDirective::Transition(
                PipelineFsmEvent::PhysicalSettlementSatisfied,
            ));
        }
        self.poll_inputs_round_robin(state, ctx, cx)
    }

    fn poll_deadline(
        state: &PipelineFsmState,
        ctx: &PipelineContext,
        cx: &mut Context<'_>,
        mut deadline_wait: Pin<&mut tokio::time::Sleep>,
    ) -> Poll<PipelineFsmEvent> {
        if let Some((at, deadline)) = Self::next_deadline(state, ctx) {
            if Instant::now() >= at {
                return Poll::Ready(PipelineFsmEvent::Deadline(deadline));
            }
            deadline_wait.as_mut().reset(at.into());
            let _ = deadline_wait.as_mut().poll(cx);
        }
        Poll::Pending
    }

    fn next_deadline(
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

    fn take_operational_failure(&mut self, ctx: &PipelineContext) -> Option<PipelineFsmEvent> {
        if let Some(error) = ctx.resources.publications.first_failure() {
            ctx.resources.retain_failure(Box::new(error));
        }
        if self.failure_reported {
            return None;
        }
        let error = ctx.resources.failure.get()?;
        self.failure_reported = true;
        Some(PipelineFsmEvent::OperationalFailure {
            message: error.to_string(),
        })
    }

    fn settlement_satisfied(state: &PipelineFsmState, ctx: &mut PipelineContext) -> bool {
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

    fn poll_inputs_round_robin(
        &mut self,
        state: &PipelineFsmState,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        for input in self.input_cursor.polling_order() {
            let result = match input {
                SupervisorInput::Control => self.poll_control(cx),
                SupervisorInput::Journal => self.poll_journal(ctx, cx),
                SupervisorInput::CommandDelivery => self.poll_command_delivery(ctx, cx),
                SupervisorInput::Resources => self.poll_resources_or_startup(state, ctx, cx),
            };
            if result.is_ready() {
                self.input_cursor.advance_after(input);
                return result;
            }
        }
        Poll::Pending
    }

    fn poll_control(&mut self, cx: &mut Context<'_>) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        if !self.controls_open {
            return Poll::Pending;
        }
        match std::pin::pin!(self.controls.recv()).as_mut().poll(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(EventLoopDirective::Transition(event)),
            Poll::Ready(None) => {
                self.controls_open = false;
                Poll::Ready(EventLoopDirective::Continue)
            }
            Poll::Pending => Poll::Pending,
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

    fn poll_command_delivery(
        &mut self,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        let Poll::Ready(Some(result)) = ctx.resources.delivery.poll(cx) else {
            return Poll::Pending;
        };
        self.idle = None;
        if let Err(error) = result {
            ctx.resources.retain_failure(Box::new(error));
        }
        Poll::Ready(EventLoopDirective::Continue)
    }

    fn poll_resources_or_startup(
        &mut self,
        state: &PipelineFsmState,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<EventLoopDirective<PipelineFsmEvent>> {
        if self.poll_resource_completions(ctx, cx).is_ready() {
            return Poll::Ready(EventLoopDirective::Continue);
        }
        // Startup shares the resource turn so busy controls cannot starve it.
        // Owned resource progress is serviced before generating a startup event.
        let event = match state {
            PipelineFsmState::Created => PipelineFsmEvent::Bootstrap,
            PipelineFsmState::ReadyForRun if !crate::bootstrap::startup_mode_manual() => {
                PipelineFsmEvent::Control(PipelineControl::Start)
            }
            _ => return Poll::Pending,
        };
        Poll::Ready(EventLoopDirective::Transition(event))
    }

    fn poll_resource_completions(
        &mut self,
        ctx: &mut PipelineContext,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        for input in self.resource_cursor.polling_order() {
            let result = match input {
                ResourceInput::StageJoin => Self::poll_stage_join(ctx, cx),
                ResourceInput::PublicationSettlement => Self::poll_publication_settlement(ctx, cx),
                ResourceInput::ProducerTail => Self::poll_producer_tail(ctx, cx),
                ResourceInput::MetricsJoin => Self::poll_metrics_join(ctx, cx),
            };
            if result.is_ready() {
                // A producer or our own append has settled. Check the existing
                // journal again without extending a previous temporary-EOF wait.
                self.idle = None;
                self.resource_cursor.advance_after(input);
                return result;
            }
        }
        Poll::Pending
    }

    fn poll_stage_join(ctx: &mut PipelineContext, cx: &mut Context<'_>) -> Poll<()> {
        let Some(joins) = &mut ctx.resources.stage_joins else {
            return Poll::Pending;
        };
        let Poll::Ready(result) =
            Pin::new(joins.get_mut().unwrap_or_else(|e| e.into_inner())).poll_next(cx)
        else {
            return Poll::Pending;
        };
        match result {
            Some(Err(error))
                if !(ctx.progress.stages_cancelled && matches!(error, StageError::Aborted)) =>
            {
                ctx.resources.retain_failure(Box::new(error));
            }
            None => {
                ctx.resources.stage_joins = None;
                ctx.resources.stages_joined = true;
            }
            Some(_) => {}
        }
        Poll::Ready(())
    }

    fn poll_publication_settlement(ctx: &mut PipelineContext, cx: &mut Context<'_>) -> Poll<()> {
        let Some(observation) = &mut ctx.resources.publication_settlement else {
            return Poll::Pending;
        };
        let Poll::Ready(result) = Pin::new(observation).poll(cx) else {
            return Poll::Pending;
        };
        ctx.resources.publication_settlement = None;
        if let Err(error) = result {
            ctx.resources.retain_failure(Box::new(error));
        }
        Poll::Ready(())
    }

    fn poll_producer_tail(ctx: &mut PipelineContext, cx: &mut Context<'_>) -> Poll<()> {
        let ProducerTail::Reading(read) = &mut ctx.resources.producer_tail else {
            return Poll::Pending;
        };
        let Poll::Ready(result) = read
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .poll(cx)
        else {
            return Poll::Pending;
        };
        ctx.resources.producer_tail = match result {
            Ok(Some(id)) if ctx.last_system_event_id_seen != Some(id) => ProducerTail::Through(id),
            Ok(_) => ProducerTail::Reached,
            Err(error) => {
                ctx.progress.journal_failed = true;
                ctx.resources.retain_failure(error);
                ProducerTail::Reached
            }
        };
        Poll::Ready(())
    }

    fn poll_metrics_join(ctx: &mut PipelineContext, cx: &mut Context<'_>) -> Poll<()> {
        let Some(join) = &mut ctx.resources.metrics_join else {
            return Poll::Pending;
        };
        let Poll::Ready(result) = join
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .poll(cx)
        else {
            return Poll::Pending;
        };
        ctx.resources.metrics_join = None;
        ctx.resources.metrics_joined = true;
        if let Err(error) = result {
            if !(ctx.progress.metrics_cancelled && matches!(error, HandleError::SupervisorAborted))
            {
                ctx.resources.retain_failure(Box::new(error));
            }
        }
        Poll::Ready(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SupervisorInput {
    Control,
    Journal,
    CommandDelivery,
    Resources,
}

impl SupervisorInput {
    const ORDER: &'static [Self] = &[
        Self::Control,
        Self::Journal,
        Self::CommandDelivery,
        Self::Resources,
    ];
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResourceInput {
    StageJoin,
    PublicationSettlement,
    ProducerTail,
    MetricsJoin,
}

impl ResourceInput {
    const ORDER: &'static [Self] = &[
        Self::StageJoin,
        Self::PublicationSettlement,
        Self::ProducerTail,
        Self::MetricsJoin,
    ];
}

/// Visits each input once, starting after the last input that made progress.
/// An entirely pending scan leaves the cursor unchanged.
struct RoundRobinCursor<Input: 'static> {
    order: &'static [Input],
    next_index: usize,
}

impl<Input: Copy + PartialEq> RoundRobinCursor<Input> {
    fn new(order: &'static [Input]) -> Self {
        assert!(!order.is_empty(), "round-robin polling needs an input");
        Self {
            order,
            next_index: 0,
        }
    }

    fn polling_order(&self) -> impl Iterator<Item = Input> + 'static {
        let (before_next, from_next) = self.order.split_at(self.next_index);
        from_next.iter().chain(before_next).copied()
    }

    fn advance_after(&mut self, input: Input) {
        let served_index = self
            .order
            .iter()
            .position(|candidate| *candidate == input)
            .expect("served input belongs to this round-robin cursor");
        self.next_index = (served_index + 1) % self.order.len();
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
        self.attach_journal_subscription(ctx);
        let mut deadline_wait = Box::pin(tokio::time::sleep(Duration::ZERO));
        Ok(
            std::future::poll_fn(|cx| self.poll_dispatch(state, ctx, cx, deadline_wait.as_mut()))
                .await,
        )
    }
}
