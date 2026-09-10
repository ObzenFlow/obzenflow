// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One FSM owner, with owned, retained I/O completions. No I/O future borrows
//! the context while the driver waits for controls or an absolute deadline.

use super::fsm::{
    FlowStopMode, PipelineAction, PipelineContext, PipelineEvent, PipelineState, StopRequestOutcome,
};
use super::operations::{self, Cancellation, Completion, Operation};
use super::supervisor::PipelineSupervisor;
use super::termination::{ExecutionFailure, ExecutionOutcome};
use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::publication::{self, BoxError, PublicationScope};
use crate::supervised_base::{
    EventLoopDirective, EventReceiver, SelfSupervised, StateWatcher, SupervisorHandle,
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use obzenflow_core::event::{StageLifecycleEvent, SystemEventFactory, SystemEventType};
use std::{collections::VecDeque, time::Instant};

enum Finished {
    Action(u64, Result<Completion, BoxError>),
    Admission(Result<Completion, BoxError>),
    Tail(
        Result<
            Vec<obzenflow_core::event::EventEnvelope<obzenflow_core::event::SystemEvent>>,
            BoxError,
        >,
    ),
    Read(
        u64,
        Box<(
            crate::messaging::system_subscription::SystemSubscription<
                obzenflow_core::event::SystemEvent,
            >,
            PollResult<obzenflow_core::event::SystemEvent>,
        )>,
    ),
}

fn retained(operation: Operation, generation: u64) -> BoxFuture<'static, Finished> {
    async move { Finished::Action(generation, publication::commit(operation).await) }.boxed()
}

fn abort_stages(context: &PipelineContext, cancelled: &Cancellation) {
    cancelled.cancel();
    for handle in context
        .stage_supervisors
        .values()
        .chain(context.source_supervisors.values())
    {
        handle.request_abort();
    }
}

pub(super) async fn run(
    mut supervisor: PipelineSupervisor,
    mut controls: EventReceiver<PipelineEvent>,
    watcher: StateWatcher<PipelineState>,
    mut context: PipelineContext,
    initial_state: PipelineState,
) -> Result<(), BoxError> {
    let mut machine = super::fsm::build_pipeline_fsm_with_initial(initial_state);
    let writer = PublicationScope::new();
    let (_writer_lifetime, writer_finished) = tokio::sync::oneshot::channel::<()>();
    if let Some(scope) = PublicationScope::current() {
        let writer = writer.clone();
        drop(scope.enqueue(async move {
            let _ = writer_finished.await;
            writer
                .join()
                .await
                .map_err(|error| Box::new(error) as BoxError)
        })?);
    }
    let cancelled = Cancellation::new();
    let mut pending: FuturesUnordered<BoxFuture<'static, Finished>> = FuturesUnordered::new();
    let mut queued = VecDeque::new();
    let mut generation = 0;
    let mut active = false;
    let mut read_ready = false;
    let mut controls_open = true;
    let mut cleanup_deadline = None;
    let mut idle_until = tokio::time::Instant::now();
    let mut tail_settled = false;
    let mut ready_control = None;
    loop {
        if ready_control.is_none() && controls_open {
            ready_control = controls.try_recv().ok();
        }
        if !active {
            if let Some(action) = queued.pop_front() {
                if matches!(action, PipelineAction::Cleanup) && cleanup_deadline.is_none() {
                    cleanup_deadline = Some(Instant::now() + super::fsm::stop_drain_timeout());
                }
                let operation =
                    operations::prepare(action, &mut context, cancelled.clone(), writer.clone());
                pending.push(retained(operation, generation));
                active = true;
            }
        }
        if !active && queued.is_empty() {
            let _ = watcher.update(machine.state().clone());
        }
        let terminal = matches!(
            machine.state(),
            PipelineState::Drained | PipelineState::Failed { .. }
        );
        if terminal
            && !active
            && queued.is_empty()
            && pending.is_empty()
            && ready_control.is_none()
            && !context.stop_intent.timeout_due()
        {
            if tail_settled {
                break;
            }
            let journal = context.system_journal.clone();
            let after = context.last_system_event_id_seen;
            pending.push(
                async move {
                    Finished::Tail(
                        publication::commit(async move {
                            Ok(match after {
                                Some(id) => journal.read_causally_after(&id).await?,
                                None => journal.read_causally_ordered().await?,
                            })
                        })
                        .await,
                    )
                }
                .boxed(),
            );
        }

        if !active
            && queued.is_empty()
            && !terminal
            && ready_control.is_none()
            && !context.stop_intent.timeout_due()
            && tokio::time::Instant::now() >= idle_until
        {
            if context.completion_subscription.is_some() && !read_ready {
                let mut subscription = context.completion_subscription.take().expect("checked");
                let read_generation = generation;
                pending.push(
                    async move {
                        // Own the reader until completion, including after supervisor abort.
                        match publication::commit(async move {
                            let result = subscription.poll_next().await;
                            Ok((subscription, result))
                        })
                        .await
                        {
                            Ok((subscription, result)) => {
                                Finished::Read(read_generation, Box::new((subscription, result)))
                            }
                            Err(error) => Finished::Action(read_generation, Err(error)),
                        }
                    }
                    .boxed(),
                );
                active = true;
            } else {
                let state = machine.state().clone();
                let directive = match supervisor.dispatch_state(&state, &mut context).await {
                    Ok(directive) => directive,
                    Err(error) => EventLoopDirective::Transition(PipelineEvent::Error {
                        message: error.to_string(),
                    }),
                };
                read_ready = false;
                match directive {
                    EventLoopDirective::Transition(event) => {
                        queued.extend(machine.handle(event, &mut context).await?);
                        continue;
                    }
                    EventLoopDirective::Continue => {
                        idle_until =
                            tokio::time::Instant::now() + std::time::Duration::from_millis(10)
                    }
                    EventLoopDirective::Terminate => {
                        return Err(std::io::Error::other(
                            "terminal dispatch bypassed pipeline settlement",
                        )
                        .into())
                    }
                }
            }
        }

        let deadline = context
            .stop_intent
            .deadline
            .filter(|_| !cancelled.is_cancelled())
            .or(cleanup_deadline.filter(|_| !cancelled.is_cancelled()));
        let timeout = async {
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline.into()).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let input = tokio::select! {
            biased;
            _ = timeout => {
                if context.stop_intent.timeout_due() {
                    Some(PipelineEvent::StopRequested { mode: FlowStopMode::Cancel,
                        reason: Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT.into()) })
                } else {
                    abort_stages(&context, &cancelled);
                    None
                }
            }
            event = async { match ready_control.take() {
                Some(event) => Some(event),
                None => controls.recv().await,
            } }, if controls_open => {
                match event {
                    Some(event) => Some(event),
                    None => { controls_open = false; Some(PipelineEvent::Error { message: "External control channel closed".into() }) }
                }
            }
            Some(finished) = pending.next(), if !pending.is_empty() => {
                match finished {
                    Finished::Tail(result) => {
                        tail_settled = true;
                        match result {
                            Ok(tail) => {
                                for envelope in tail {
                                    if let SystemEventType::StageLifecycle { stage_id, event:
                                            StageLifecycleEvent::Completed { metrics: Some(metrics) }
                                            | StageLifecycleEvent::Cancelled { metrics: Some(metrics), .. }
                                            | StageLifecycleEvent::Failed { metrics: Some(metrics), .. }
                                    } = envelope.event.event {
                                            context.stage_lifecycle_metrics.insert(stage_id, metrics);
                                    }
                                }
                                None
                            }
                            Err(error) => Some(PipelineEvent::Error { message: error.to_string() }),
                        }
                    }
                    Finished::Read(id, read) if id == generation => {
                        let (mut subscription, result) = *read;
                        subscription.prefetch(result);
                        context.completion_subscription = Some(subscription);
                        read_ready = true;
                        active = false;
                        None
                    }
                    Finished::Read(..) => None,
                    Finished::Action(id, result) => {
                        if id == generation { active = false; }
                        match result {
                            Ok(completion) => {
                                // A metrics task created before cancellation remains a resource to join.
                                if id == generation || matches!(completion, Completion::Metrics(_)) { completion.apply(&mut context); }
                                None
                            }
                            Err(error) => Some(PipelineEvent::Error { message: error.to_string() }),
                        }
                    }
                    Finished::Admission(result) => result.err().map(|error| PipelineEvent::Error { message: error.to_string() }),
                }
            }
            _ = tokio::time::sleep_until(idle_until), if !active && queued.is_empty() && !terminal => { None }
        };
        let Some(event) = input else {
            continue;
        };
        let was_cancel = matches!(context.stop_intent.mode, Some(FlowStopMode::Cancel));
        let actions = if terminal {
            match event {
                PipelineEvent::StopRequested { mode, reason } => {
                    if matches!(
                        context.stop_intent.apply_request(mode.clone(), reason),
                        StopRequestOutcome::Applied { .. }
                    ) {
                        vec![PipelineAction::WritePipelineStopRequested { mode }]
                    } else {
                        Vec::new()
                    }
                }
                PipelineEvent::Error { message } => {
                    context.termination.fail(message, None);
                    Vec::new()
                }
                _ => Vec::new(),
            }
        } else {
            machine.handle(event, &mut context).await?
        };
        if !was_cancel && matches!(context.stop_intent.mode, Some(FlowStopMode::Cancel)) {
            abort_stages(&context, &cancelled);
            generation += 1;
            active = false;
            read_ready = false;
            queued.clear();
            if terminal {
                // Cancellation can overtake an unstarted cleanup action. Its
                // joins still precede terminal selection, even when a prior
                // cleanup operation is already settling the same handles.
                queued.push_back(PipelineAction::Cleanup);
            }
        }
        for action in actions {
            if matches!(action, PipelineAction::WritePipelineStopRequested { .. }) {
                // Preparation assigns the fact and registers writer order in
                // this owner before another control can be admitted.
                let operation =
                    operations::prepare(action, &mut context, cancelled.clone(), writer.clone());
                let mut receipt = Box::pin(publication::commit(operation));
                match futures::poll!(receipt.as_mut()) {
                    std::task::Poll::Ready(result) => {
                        if let Err(error) = result {
                            context.termination.fail(error.to_string(), None);
                        }
                    }
                    std::task::Poll::Pending => {
                        pending.push(async move { Finished::Admission(receipt.await) }.boxed())
                    }
                }
            } else {
                queued.push_back(action);
            }
        }
    }

    let outcome = if let Some(failure) = &context.termination.failure {
        ExecutionOutcome::Failed(failure.clone())
    } else if context.flow_start_time.is_none() {
        ExecutionOutcome::NotStarted
    } else if matches!(context.stop_intent.mode, Some(FlowStopMode::Cancel))
        || (context.stop_intent.requested
            && context.topology.stages().any(|stage| {
                matches!(
                    stage.stage_type,
                    obzenflow_topology::StageType::InfiniteSource
                )
            }))
    {
        ExecutionOutcome::Cancelled {
            reason: context.stop_intent.reason_label(),
        }
    } else if let PipelineState::Failed {
        reason,
        failure_cause,
    } = machine.state()
    {
        ExecutionOutcome::Failed(ExecutionFailure {
            reason: reason.clone(),
            cause: failure_cause.clone(),
        })
    } else {
        ExecutionOutcome::Completed
    };
    let factory = SystemEventFactory::new(context.system_id);
    let duration = obzenflow_core::event::types::DurationMs(
        context
            .flow_start_time
            .map(|start| start.elapsed().as_millis() as u64)
            .unwrap_or(0),
    );
    let metrics = super::fsm::compute_flow_lifecycle_metrics(&context);
    let event = match &outcome {
        ExecutionOutcome::Completed => factory.pipeline_completed(duration, metrics),
        ExecutionOutcome::Cancelled { reason } => {
            factory.pipeline_cancelled(reason.clone(), duration, Some(metrics), None)
        }
        ExecutionOutcome::Failed(failure) => factory.pipeline_failed(
            failure.reason.clone(),
            duration,
            Some(metrics),
            failure.cause.clone(),
        ),
        ExecutionOutcome::NotStarted => factory.pipeline_not_started(),
    };
    let id = event.id;
    let published = context.termination.published.clone();
    let journal = context.system_journal.clone();
    let metrics_handle = context.metrics_handle.take();
    let metrics_drain_timeout_ms = context.metrics_drain_timeout_ms;
    let terminal_writer = writer.clone();
    // Selection is immutable once this owned operation starts.
    let terminal_journal = journal.clone();
    let terminal = terminal_writer.enqueue(async move {
        terminal_journal.append(event, None).await?;
        published
            .set(super::termination::PublishedTermination {
                outcome,
                event_id: Some(id),
            })
            .map_err(|_| std::io::Error::other("Pipeline terminal outcome was already retained"))?;
        Ok(Instant::now())
    });
    let settlement = publication::commit(async move {
        let terminal = match terminal {
            Ok(receipt) => receipt.await,
            Err(error) => Err(error),
        };
        let (terminal_ack, mut operational_error) = match terminal {
            Ok(at) => (Some(at), None),
            Err(error) => (None, Some(error)),
        };
        if let Some(handle) = metrics_handle.as_ref() {
            let budget = std::time::Duration::from_millis(metrics_drain_timeout_ms);
            let cancel_metrics = if let Some(committed_at) = terminal_ack {
                match tokio::time::timeout_at(
                    (committed_at + budget).into(),
                    handle.wait_for_completion(),
                )
                .await
                {
                    Ok(Ok(())) => false,
                    Ok(Err(error)) => {
                        operational_error.get_or_insert(Box::new(error));
                        false
                    }
                    Err(_) => {
                        tracing::warn!(
                            timeout_ms = metrics_drain_timeout_ms,
                            "Metrics finalisation did not settle within its budget"
                        );
                        true
                    }
                }
            } else {
                true
            };
            if cancel_metrics {
                handle.abort();
                if let Err(error) = handle.abort_and_wait().await {
                    operational_error.get_or_insert(Box::new(error));
                }
            }
        }
        if let Some(error) = operational_error {
            return Err(error);
        }
        terminal_writer
            .accept(async move {
                journal.append(factory.pipeline_drained(), None).await?;
                Ok(())
            })
            .await
    });
    // The same owner continues admitting controls through terminal publication
    // and metrics finalisation. Those controls cannot replace the chosen fact.
    let mut settlement = Box::pin(settlement);
    let mut settlement_result: Option<Result<(), BoxError>> = None;
    let mut admission_error = None;
    loop {
        if pending.is_empty() {
            if let Some(result) = settlement_result.take() {
                return result.and_then(|()| admission_error.map_or(Ok(()), Err));
            }
        }
        let deadline = context
            .stop_intent
            .deadline
            .filter(|_| !cancelled.is_cancelled());
        let timeout = async {
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline.into()).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let event = tokio::select! {
            biased;
            _ = timeout => Some(PipelineEvent::StopRequested {
                mode: FlowStopMode::Cancel,
                reason: Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT.into()),
            }),
            event = controls.recv(), if controls_open => {
                if event.is_none() { controls_open = false; }
                event
            }
            result = &mut settlement, if settlement_result.is_none() => {
                settlement_result = Some(result);
                None
            }
            Some(finished) = pending.next(), if !pending.is_empty() => {
                if let Finished::Admission(Err(error)) = finished {
                    // Publication failure is operational after terminal selection.
                    admission_error.get_or_insert(error);
                }
                None
            }
        };
        if let Some(PipelineEvent::Error { message }) = &event {
            admission_error.get_or_insert_with(|| Box::new(std::io::Error::other(message.clone())));
        }
        if let Some(PipelineEvent::StopRequested { mode, reason }) = event {
            if matches!(
                context.stop_intent.apply_request(mode.clone(), reason),
                StopRequestOutcome::Applied { .. }
            ) {
                if matches!(mode, FlowStopMode::Cancel) {
                    abort_stages(&context, &cancelled);
                }
                let operation = operations::prepare(
                    PipelineAction::WritePipelineStopRequested { mode },
                    &mut context,
                    cancelled.clone(),
                    writer.clone(),
                );
                pending.push(
                    async move { Finished::Admission(publication::commit(operation).await) }
                        .boxed(),
                );
            }
        }
    }
}
