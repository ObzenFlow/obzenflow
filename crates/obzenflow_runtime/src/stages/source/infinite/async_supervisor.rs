// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Async infinite source supervisor implementation using HandlerSupervised pattern

use crate::replay::{ReplayContextTemplate, ReplayDriver};
use crate::stages::common::handlers::AsyncInfiniteSourceHandler;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::source::replay_lifecycle::ReplayCompletionGuard;
use crate::stages::source::supervision::{
    around_source_boundary, drain_pending_outputs_async, emit_batch_to_pending_outputs,
    per_data_event_duration_for_batch, stage_boundary_control_events,
};
use crate::stages::source::{
    SourceBoundaryMiddleware, SourceBoundaryOutcome, SourcePollCompletion, SourcePollReport,
};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, EventReceiver, HandlerSupervised, StateWatcher};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ReplayLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fsm::{
    InfiniteSourceAction, InfiniteSourceCompletionReason, InfiniteSourceContext,
    InfiniteSourceEvent, InfiniteSourceState,
};

/// Supervisor for async infinite source stages.
pub(crate) struct AsyncInfiniteSourceSupervisor<
    H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: H,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// External control events (Initialize/Ready/Start/BeginDrain).
    pub(crate) external_events: EventReceiver<InfiniteSourceEvent<H>>,

    /// State watcher for UI/handles.
    pub(crate) state_watcher: StateWatcher<InfiniteSourceState<H>>,

    /// Last published state (avoid waking watchers every loop) (FLOWIP-086i).
    pub(crate) last_state: Option<InfiniteSourceState<H>>,

    /// Replay driver for `--replay-from` mode (FLOWIP-095a).
    pub(crate) replay_driver: Option<ReplayDriver>,

    /// Replay lifecycle started timestamp for duration tracking (FLOWIP-095a).
    pub(crate) replay_started_at: Option<Instant>,

    /// Guard that ensures ReplayLifecycle::Completed is emitted once (FLOWIP-095a).
    pub(crate) replay_completion: ReplayCompletionGuard,

    /// Runtime-neutral source boundary seam (FLOWIP-115a).
    pub(crate) source_boundary: Option<Arc<dyn SourceBoundaryMiddleware>>,

    /// Completion was observed by the source boundary after emitting control
    /// events; drain those events before beginning source drain.
    pub(crate) pending_boundary_begin_drain: bool,

    /// Error was observed by the source boundary after emitting control events;
    /// drain those events before transitioning to failure.
    pub(crate) pending_boundary_error: Option<String>,
}

impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for AsyncInfiniteSourceSupervisor<H>
{
    type State = InfiniteSourceState<H>;
    type Event = InfiniteSourceEvent<H>;
    type Context = InfiniteSourceContext<H>;
    type Action = InfiniteSourceAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   InfiniteSourceState<H>;
            event:   InfiniteSourceEvent<H>;
            context: InfiniteSourceContext<H>;
            action:  InfiniteSourceAction<H>;
            initial: initial_state;

            state InfiniteSourceState::Created {
                on InfiniteSourceEvent::Initialize => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: InfiniteSourceState::Initialized,
                            actions: vec![InfiniteSourceAction::AllocateResources],
                        })
                    })
                };

                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let InfiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: InfiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                InfiniteSourceAction::SendError { message: error_msg },
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state InfiniteSourceState::Initialized {
                on InfiniteSourceEvent::Ready => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: InfiniteSourceState::WaitingForGun,
                            actions: vec![],
                        })
                    })
                };

                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let InfiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: InfiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                InfiniteSourceAction::SendError { message: error_msg },
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state InfiniteSourceState::WaitingForGun {
                on InfiniteSourceEvent::Start => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: InfiniteSourceState::Running,
                            actions: vec![InfiniteSourceAction::PublishRunning],
                        })
                    })
                };

                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let InfiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: InfiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                InfiniteSourceAction::SendError { message: error_msg },
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state InfiniteSourceState::Running {
                on InfiniteSourceEvent::BeginDrain => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: InfiniteSourceState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let InfiniteSourceEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: InfiniteSourceState::Failed(msg.clone()),
                                actions: vec![
                                    InfiniteSourceAction::SendError { message: msg },
                                    InfiniteSourceAction::Cleanup,
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            state InfiniteSourceState::Draining {
                on InfiniteSourceEvent::Completed => |_state: &InfiniteSourceState<H>, _event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: InfiniteSourceState::Drained,
                            actions: vec![
                                InfiniteSourceAction::SendEOF,
                                InfiniteSourceAction::WriteStageCompleted,
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };

                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let InfiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: InfiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                InfiniteSourceAction::SendError { message: error_msg },
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state InfiniteSourceState::Drained {
                on InfiniteSourceEvent::Error => |_state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let InfiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: InfiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                InfiniteSourceAction::SendError { message: error_msg },
                                InfiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state InfiniteSourceState::Failed {
                on InfiniteSourceEvent::Error => |state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let InfiniteSourceEvent::Error(_msg) = event {
                            Ok(Transition {
                                next_state: state,
                                actions: vec![],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            unhandled => |state: &InfiniteSourceState<H>, event: &InfiniteSourceEvent<H>, _ctx: &mut InfiniteSourceContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "AsyncInfiniteSourceSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(obzenflow_fsm::FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    HandlerSupervised for AsyncInfiniteSourceSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> InfiniteSourceEvent<H> {
        InfiniteSourceEvent::Error(msg)
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match state {
            InfiniteSourceState::Created
            | InfiniteSourceState::Initialized
            | InfiniteSourceState::WaitingForGun => match self.external_events.recv().await {
                Some(event) => Ok(EventLoopDirective::Transition(event)),
                None => Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                    "External control channel closed".to_string(),
                ))),
            },

            InfiniteSourceState::Running => {
                // Drain any pending outputs first so backpressure doesn't let sources
                // accumulate unbounded in-memory batches.
                let flow_id = ctx.flow_id.to_string();
                let stage_flow_context = make_flow_context(
                    &ctx.flow_name,
                    &flow_id,
                    &ctx.stage_name,
                    self.stage_id,
                    StageType::InfiniteSource,
                );

                if let Some(directive) = drain_pending_outputs_async(
                    &mut ctx.pending_outputs,
                    &stage_flow_context,
                    self.stage_id,
                    None,
                    &ctx.data_journal,
                    &ctx.error_journal,
                    &ctx.system_journal,
                    &ctx.instrumentation,
                    &ctx.backpressure_writer,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_backoff,
                    Some(&ctx.output_contract),
                    &mut self.external_events,
                    || InfiniteSourceEvent::Error("External control channel closed".to_string()),
                )
                .await?
                {
                    return Ok(directive);
                }

                if let Some(error) = self.pending_boundary_error.take() {
                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                        error,
                    )));
                }
                if self.pending_boundary_begin_drain {
                    self.pending_boundary_begin_drain = false;
                    return Ok(EventLoopDirective::Transition(
                        InfiniteSourceEvent::BeginDrain,
                    ));
                }

                ctx.instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if let Some(replay_archive) = ctx.replay_archive.as_deref() {
                    if self.replay_driver.is_none() {
                        let stage_key = ctx.stage_name.as_str();
                        let journal_path = replay_archive
                            .source_data_journal_path(stage_key)
                            .map_err(|e| format!("Failed to locate archived journal: {e}"))?;
                        let reader = replay_archive
                            .open_source_reader(
                                stage_key,
                                obzenflow_core::event::context::StageType::InfiniteSource,
                            )
                            .await
                            .map_err(|e| format!("Failed to open archived journal reader: {e}"))?;
                        let replay_context = ReplayContextTemplate {
                            original_flow_id: replay_archive.archive_flow_id().to_string(),
                            original_stage_id: replay_archive
                                .archived_stage_id(stage_key)
                                .map_err(|e| format!("Failed to resolve archived stage id: {e}"))?,
                            archive_path: replay_archive.archive_path().to_path_buf(),
                        };
                        self.replay_driver =
                            Some(ReplayDriver::new(reader, journal_path, replay_context));

                        if self.replay_started_at.is_none() {
                            self.replay_started_at = Some(Instant::now());
                            let started_event = SystemEvent::new(
                                WriterId::from(self.stage_id),
                                SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::Started {
                                    archive_path: replay_archive.archive_path().to_path_buf(),
                                    archive_flow_id: replay_archive.archive_flow_id().to_string(),
                                    archive_status: replay_archive.archive_status(),
                                    archive_status_derivation: replay_archive.status_derivation(),
                                    allow_incomplete: replay_archive.allow_incomplete_archive(),
                                    source_stages: replay_archive.source_stage_keys(),
                                }),
                            );
                            if let Err(e) = self.system_journal.append(started_event, None).await {
                                tracing::error!(
                                    stage_name = %ctx.stage_name,
                                    journal_error = %e,
                                    "Failed to append ReplayLifecycle::Started system event"
                                );
                            }
                        }
                    }

                    let flow_context = stage_flow_context.clone();

                    let tick_started_at = Instant::now();
                    let next_result = tokio::select! {
                        biased;
                        maybe_event = self.external_events.recv() => {
                            match maybe_event {
                                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                                None => {
                                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                                        "External control channel closed".to_string(),
                                    )));
                                }
                            }
                        }
                        result = self.replay_driver.as_mut().expect("replay_driver is initialized").next_replayed_event(
                            WriterId::from(self.stage_id),
                            &ctx.stage_name,
                            flow_context,
                        ) => result,
                    };
                    let tick_duration = tick_started_at.elapsed();

                    match next_result {
                        Ok(Some(event)) => {
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            let per_data_event_duration = if event.is_data() {
                                tick_duration
                            } else {
                                Duration::from_nanos(0)
                            };

                            let events_to_write = self.run_if_not_error(event, |e| vec![e]);
                            emit_batch_to_pending_outputs(
                                events_to_write,
                                &stage_flow_context,
                                &ctx.instrumentation,
                                per_data_event_duration,
                                &mut ctx.pending_outputs,
                            );

                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(None) => {
                            let (replayed_count, skipped_count) = self
                                .replay_driver
                                .as_ref()
                                .map_or((0, 0), |d| (d.replayed_events(), d.skipped_events()));
                            self.replay_completion
                                .maybe_emit_completed(
                                    self.stage_id,
                                    &ctx.stage_name,
                                    &self.system_journal,
                                    self.replay_started_at,
                                    replayed_count,
                                    skipped_count,
                                )
                                .await;

                            ctx.completion_reason =
                                InfiniteSourceCompletionReason::ArchiveExhausted;
                            Ok(EventLoopDirective::Transition(
                                InfiniteSourceEvent::BeginDrain,
                            ))
                        }
                        Err(e) => Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                            e.to_string(),
                        ))),
                    }
                } else {
                    let source_boundary = self.source_boundary.clone();
                    let boundary_future = around_source_boundary(
                        source_boundary,
                        Box::pin(async {
                            let poll_started_at = Instant::now();
                            let result = self.handler.next().await.map(SourcePollCompletion::Batch);
                            SourcePollReport {
                                result,
                                poll_duration: poll_started_at.elapsed(),
                            }
                        }),
                    );

                    let report = tokio::select! {
                        biased;
                        maybe_event = self.external_events.recv() => {
                            match maybe_event {
                                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                                None => {
                                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                                        "External control channel closed".to_string(),
                                    )));
                                }
                            }
                        }
                        report = boundary_future => report,
                    };

                    match report.outcome {
                        SourceBoundaryOutcome::Rejected { reason } => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                reason = %reason,
                                "Async infinite source boundary rejected; beginning completion"
                            );
                            ctx.completion_reason =
                                InfiniteSourceCompletionReason::ArchiveExhausted;
                            if stage_boundary_control_events(
                                report.control_events,
                                &stage_flow_context,
                                &ctx.instrumentation,
                                &mut ctx.pending_outputs,
                            ) {
                                self.pending_boundary_begin_drain = true;
                                Ok(EventLoopDirective::Continue)
                            } else {
                                Ok(EventLoopDirective::Transition(
                                    InfiniteSourceEvent::BeginDrain,
                                ))
                            }
                        }
                        SourceBoundaryOutcome::Polled(poll) => match poll.result {
                            Ok(SourcePollCompletion::Batch(mut events)) if !events.is_empty() => {
                                ctx.instrumentation
                                    .event_loops_with_work_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                events.extend(report.control_events);
                                let data_events_in_tick =
                                    events.iter().filter(|event| event.is_data()).count();
                                let per_data_event_duration = per_data_event_duration_for_batch(
                                    poll.poll_duration,
                                    data_events_in_tick,
                                );
                                emit_batch_to_pending_outputs(
                                    events,
                                    &stage_flow_context,
                                    &ctx.instrumentation,
                                    per_data_event_duration,
                                    &mut ctx.pending_outputs,
                                );

                                Ok(EventLoopDirective::Continue)
                            }
                            Ok(SourcePollCompletion::Batch(mut events)) => {
                                events.extend(report.control_events);
                                if !events.is_empty() {
                                    emit_batch_to_pending_outputs(
                                        events,
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        Duration::from_nanos(0),
                                        &mut ctx.pending_outputs,
                                    );
                                }
                                Ok(EventLoopDirective::Continue)
                            }
                            Ok(SourcePollCompletion::Eof) => {
                                ctx.completion_reason =
                                    InfiniteSourceCompletionReason::ArchiveExhausted;
                                if report.control_events.is_empty() {
                                    Ok(EventLoopDirective::Transition(
                                        InfiniteSourceEvent::BeginDrain,
                                    ))
                                } else {
                                    emit_batch_to_pending_outputs(
                                        report.control_events,
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        Duration::from_nanos(0),
                                        &mut ctx.pending_outputs,
                                    );
                                    self.pending_boundary_begin_drain = true;
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    stage_name = %ctx.stage_name,
                                    error = %e,
                                    "Async infinite source handler.next() returned error"
                                );
                                let error = e.to_string();
                                if report.control_events.is_empty() {
                                    Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                                        error,
                                    )))
                                } else {
                                    emit_batch_to_pending_outputs(
                                        report.control_events,
                                        &stage_flow_context,
                                        &ctx.instrumentation,
                                        Duration::from_nanos(0),
                                        &mut ctx.pending_outputs,
                                    );
                                    self.pending_boundary_error = Some(error);
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                        },
                    }
                }
            }

            InfiniteSourceState::Draining => {
                if let Err(e) = self.handler.drain().await {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        error = %e,
                        "drain() failed; continuing shutdown"
                    );
                }
                Ok(EventLoopDirective::Transition(
                    InfiniteSourceEvent::Completed,
                ))
            }

            InfiniteSourceState::Drained => Ok(EventLoopDirective::Terminate),
            InfiniteSourceState::Failed(_) => {
                // Best-effort: ensure async infinite sources can clean up resources even on
                // force_shutdown (StopRequested), which routes through the Error → Failed path.
                if let Err(e) = self.handler.drain().await {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        error = %e,
                        "drain() failed during failure shutdown; continuing"
                    );
                }
                Ok(EventLoopDirective::Terminate)
            }
            InfiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
