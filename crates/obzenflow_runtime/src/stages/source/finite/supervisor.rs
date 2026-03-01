// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite source supervisor implementation using HandlerSupervised pattern

use crate::replay::{ReplayContextTemplate, ReplayDriver};
use crate::stages::common::handlers::FiniteSourceHandler;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::source::replay_lifecycle::ReplayCompletionGuard;
use crate::stages::source::supervision::{
    drain_pending_outputs_sync, emit_batch_to_pending_outputs, per_data_event_duration_for_batch,
};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ReplayLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fsm::{FiniteSourceAction, FiniteSourceContext, FiniteSourceEvent, FiniteSourceState};

/// Supervisor for finite source stages
pub(crate) struct FiniteSourceSupervisor<
    H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: H,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Adaptive backoff for synchronous idle polls (FLOWIP-086i).
    pub(crate) idle_backoff: IdleBackoff,

    /// Replay driver for `--replay-from` mode (FLOWIP-095a).
    pub(crate) replay_driver: Option<ReplayDriver>,

    /// Replay lifecycle started timestamp for duration tracking (FLOWIP-095a).
    pub(crate) replay_started_at: Option<Instant>,

    /// Guard that ensures ReplayLifecycle::Completed is emitted once (FLOWIP-095a).
    pub(crate) replay_completion: ReplayCompletionGuard,
}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for FiniteSourceSupervisor<H>
{
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   FiniteSourceState<H>;
            event:   FiniteSourceEvent<H>;
            context: FiniteSourceContext<H>;
            action:  FiniteSourceAction<H>;
            initial: initial_state;

            state FiniteSourceState::Created {
                on FiniteSourceEvent::Initialize => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::Initialized,
                            actions: vec![FiniteSourceAction::AllocateResources],
                        })
                    })
                };

                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let FiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: FiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                FiniteSourceAction::SendError { message: error_msg },
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state FiniteSourceState::Initialized {
                on FiniteSourceEvent::Ready => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::WaitingForGun,
                            actions: vec![],
                        })
                    })
                };

                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let FiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: FiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                FiniteSourceAction::SendError { message: error_msg },
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state FiniteSourceState::WaitingForGun {
                on FiniteSourceEvent::Start => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::Running,
                            actions: vec![FiniteSourceAction::PublishRunning],
                        })
                    })
                };

                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let FiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: FiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                FiniteSourceAction::SendError { message: error_msg },
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state FiniteSourceState::Running {
                on FiniteSourceEvent::Completed => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on FiniteSourceEvent::BeginDrain => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let FiniteSourceEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: FiniteSourceState::Failed(msg),
                                actions: vec![FiniteSourceAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            state FiniteSourceState::Draining {
                on FiniteSourceEvent::Completed => |_state: &FiniteSourceState<H>, _event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: FiniteSourceState::Drained,
                            actions: vec![
                                FiniteSourceAction::SendEOF,
                                FiniteSourceAction::WriteStageCompleted,
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };

                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let FiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: FiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                FiniteSourceAction::SendError { message: error_msg },
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state FiniteSourceState::Drained {
                on FiniteSourceEvent::Error => |_state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        let error_msg = if let FiniteSourceEvent::Error(msg) = event {
                            msg
                        } else {
                            "Unknown error".to_string()
                        };

                        Ok(Transition {
                            next_state: FiniteSourceState::Failed(error_msg.clone()),
                            actions: vec![
                                FiniteSourceAction::SendError { message: error_msg },
                                FiniteSourceAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state FiniteSourceState::Failed {
                on FiniteSourceEvent::Error => |state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let FiniteSourceEvent::Error(_msg) = event {
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

            unhandled => |state: &FiniteSourceState<H>, event: &FiniteSourceEvent<H>, _ctx: &mut FiniteSourceContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "FiniteSourceSupervisor",
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
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for FiniteSourceSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> FiniteSourceEvent<H> {
        FiniteSourceEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_name = %self.name,
                journal_error = %e,
                "Failed to write completion event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Track every event loop iteration
        match state {
            FiniteSourceState::Created => {
                self.idle_backoff.reset();
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Initialized => {
                self.idle_backoff.reset();
                // Wait for Ready event from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::WaitingForGun => {
                self.idle_backoff.reset();
                // Wait for start signal from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Running => {
                // Drain any pending outputs first so backpressure doesn't let sources
                // accumulate unbounded in-memory batches.
                let flow_id = ctx.flow_id.to_string();
                let stage_flow_context = make_flow_context(
                    &ctx.flow_name,
                    &flow_id,
                    &ctx.stage_name,
                    self.stage_id,
                    StageType::FiniteSource,
                );

                if drain_pending_outputs_sync(
                    &mut ctx.pending_outputs,
                    &stage_flow_context,
                    self.stage_id,
                    &ctx.data_journal,
                    &ctx.error_journal,
                    &ctx.system_journal,
                    &ctx.instrumentation,
                    &ctx.backpressure_writer,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_backoff,
                )
                .await?
                {
                    return Ok(EventLoopDirective::Continue);
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
                                obzenflow_core::event::context::StageType::FiniteSource,
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
                    let next_result = self
                        .replay_driver
                        .as_mut()
                        .expect("replay_driver is initialized")
                        .next_replayed_event(
                            WriterId::from(self.stage_id),
                            &ctx.stage_name,
                            flow_context,
                        )
                        .await;
                    let tick_duration = tick_started_at.elapsed();

                    match next_result {
                        Ok(Some(event)) => {
                            self.idle_backoff.reset();
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

                            Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
                        }
                        Err(e) => Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                            e.to_string(),
                        ))),
                    }
                } else {
                    // Get next batch of events from handler (synchronous, no locks needed).
                    // 051b: handler now returns Result<Option<Vec<ChainEvent>>, SourceError>
                    let tick_started_at = Instant::now();
                    let next_result = self.handler.next();
                    let tick_duration = tick_started_at.elapsed();

                    match next_result {
                        Ok(Some(events)) if !events.is_empty() => {
                            self.idle_backoff.reset();
                            // We have work - increment loops with work
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            let data_events_in_tick =
                                events.iter().filter(|event| event.is_data()).count();
                            let per_data_event_duration = per_data_event_duration_for_batch(
                                tick_duration,
                                data_events_in_tick,
                            );
                            emit_batch_to_pending_outputs(
                                events,
                                &stage_flow_context,
                                &ctx.instrumentation,
                                per_data_event_duration,
                                &mut ctx.pending_outputs,
                            );

                            tracing::trace!(
                                stage_name = %ctx.stage_name,
                                "Finite source emitted batch of events"
                            );

                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(Some(_events)) => {
                            // No events produced; apply adaptive idle backoff (FLOWIP-086i).
                            let delay = self.idle_backoff.next_delay();
                            tokio::time::sleep(delay).await;
                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(None) => {
                            // Natural completion signalled by handler.
                            Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
                        }
                        Err(e) => {
                            // SourceError surfaced from handler; delegate to control strategy
                            // via the standard error path.
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = %e,
                                "Finite source handler.next() returned error"
                            );
                            Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                                e.to_string(),
                            )))
                        }
                    }
                }
            }

            FiniteSourceState::Draining => {
                self.idle_backoff.reset();
                // Draining state - prepare to send EOF
                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
            }

            FiniteSourceState::Drained => {
                self.idle_backoff.reset();
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::Failed(_) => {
                self.idle_backoff.reset();
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> ExternalEventPolicy
    for FiniteSourceSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(
            state,
            FiniteSourceState::Created
                | FiniteSourceState::Initialized
                | FiniteSourceState::WaitingForGun
        ) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, FiniteSourceState::Failed(_)) {
            None
        } else {
            Some(FiniteSourceEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
