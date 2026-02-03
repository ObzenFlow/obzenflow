// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Infinite source supervisor implementation using HandlerSupervised pattern

use crate::replay::{ReplayContextTemplate, ReplayDriver};
use crate::stages::common::handlers::InfiniteSourceHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::idle_backoff::IdleBackoff;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainEventFactory, ReplayLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fsm::{
    InfiniteSourceAction, InfiniteSourceCompletionReason, InfiniteSourceContext,
    InfiniteSourceEvent, InfiniteSourceState,
};

/// Supervisor for infinite source stages
pub(crate) struct InfiniteSourceSupervisor<
    H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: H,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<InfiniteSourceContext<H>>,

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

    /// Whether we've emitted ReplayLifecycle::Completed for this stage.
    pub(crate) replay_completed_emitted: bool,
}

// Implement Sealed directly for InfiniteSourceSupervisor to satisfy Supervisor trait bound
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for InfiniteSourceSupervisor<H>
{
}

impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for InfiniteSourceSupervisor<H>
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
                                next_state: InfiniteSourceState::Failed(msg),
                                actions: vec![InfiniteSourceAction::Cleanup],
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
                        supervisor = "InfiniteSourceSupervisor",
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
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for InfiniteSourceSupervisor<H>
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

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_name = %self.context.stage_name,
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
            InfiniteSourceState::Created => {
                self.idle_backoff.reset();
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::Initialized => {
                self.idle_backoff.reset();
                // Wait for ready signal
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::WaitingForGun => {
                self.idle_backoff.reset();
                // Wait for start signal from pipeline
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "Infinite source waiting for start signal"
                );
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::Running => {
                // Drain any pending outputs first so backpressure doesn't let sources
                // accumulate unbounded in-memory batches.
                while let Some(event_to_write) = ctx.pending_outputs.pop_front() {
                    let journal = if matches!(
                        event_to_write.processing_info.status,
                        obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }
                    ) {
                        &ctx.error_journal
                    } else {
                        &ctx.data_journal
                    };

                    let event_to_write = event_to_write
                        .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                    if Arc::ptr_eq(journal, &ctx.data_journal) && event_to_write.is_data() {
                        // Debug-only: emit activity pulses even when bypass is enabled, so
                        // operators can see what *would* have blocked (FLOWIP-086k).
                        if crate::backpressure::BackpressureWriter::is_bypass_enabled() {
                            if let Some((min_credit, limiting)) =
                                ctx.backpressure_writer.min_downstream_credit_detail()
                            {
                                if min_credit < 1 {
                                    ctx.backpressure_pulse.record_delay(
                                        Duration::ZERO,
                                        Some(min_credit),
                                        Some(limiting),
                                    );
                                    if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: obzenflow_core::event::context::StageType::InfiniteSource,
                                        };

                                        let event = ChainEventFactory::observability_event(
                                            WriterId::from(self.stage_id),
                                            ObservabilityPayload::Middleware(
                                                MiddlewareLifecycle::Backpressure(pulse),
                                            ),
                                        )
                                        .with_flow_context(flow_context)
                                        .with_runtime_context(
                                            ctx.instrumentation.snapshot_with_control(),
                                        );

                                        match ctx.data_journal.append(event, None).await {
                                            Ok(written) => {
                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                    &written,
                                                    &self.system_journal,
                                                )
                                                .await;
                                            }
                                            Err(e) => tracing::warn!(
                                                stage_name = %ctx.stage_name,
                                                journal_error = %e,
                                                "Failed to append backpressure activity pulse"
                                            ),
                                        }
                                    }
                                }
                            }
                        }

                        let Some(reservation) = ctx.backpressure_writer.reserve(1) else {
                            // Requeue and back off (do not busy-spin).
                            ctx.pending_outputs.push_front(event_to_write);
                            let delay = ctx.backpressure_backoff.next_delay();
                            ctx.backpressure_writer.record_wait(delay);

                            if let Some((min_credit, limiting)) =
                                ctx.backpressure_writer.min_downstream_credit_detail()
                            {
                                ctx.backpressure_pulse.record_delay(
                                    delay,
                                    Some(min_credit),
                                    Some(limiting),
                                );
                            } else {
                                ctx.backpressure_pulse.record_delay(delay, None, None);
                            }
                            if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                let flow_context = FlowContext {
                                    flow_name: ctx.flow_name.clone(),
                                    flow_id: ctx.flow_id.to_string(),
                                    stage_name: ctx.stage_name.clone(),
                                    stage_id: self.stage_id,
                                    stage_type:
                                        obzenflow_core::event::context::StageType::InfiniteSource,
                                };

                                let event = ChainEventFactory::observability_event(
                                    WriterId::from(self.stage_id),
                                    ObservabilityPayload::Middleware(
                                        MiddlewareLifecycle::Backpressure(pulse),
                                    ),
                                )
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                                match ctx.data_journal.append(event, None).await {
                                    Ok(written) => {
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    }
                                    Err(e) => tracing::warn!(
                                        stage_name = %ctx.stage_name,
                                        journal_error = %e,
                                        "Failed to append backpressure activity pulse"
                                    ),
                                }
                            }

                            tokio::time::sleep(delay).await;
                            return Ok(EventLoopDirective::Continue);
                        };

                        let written = journal
                            .append(event_to_write, None)
                            .await
                            .map_err(|e| format!("Failed to write event: {e}"))?;
                        reservation.commit(1);
                        ctx.backpressure_backoff.reset();

                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                            &written,
                            &self.system_journal,
                        )
                        .await;
                    } else {
                        let written = journal
                            .append(event_to_write, None)
                            .await
                            .map_err(|e| format!("Failed to write event: {e}"))?;

                        if Arc::ptr_eq(journal, &ctx.data_journal) {
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &self.system_journal,
                            )
                            .await;
                        }
                    }
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

                    let flow_context = FlowContext {
                        flow_name: ctx.flow_name.clone(),
                        flow_id: ctx.flow_id.to_string(),
                        stage_name: ctx.stage_name.clone(),
                        stage_id: self.stage_id,
                        stage_type: obzenflow_core::event::context::StageType::InfiniteSource,
                    };

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

                            if let ProcessingStatus::Error { kind, .. } =
                                &event.processing_info.status
                            {
                                let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                                ctx.instrumentation.record_error(k);
                            }

                            let events_to_write = self.run_if_not_error(event, |e| vec![e]);
                            for event_to_write in events_to_write {
                                if event_to_write.is_data() {
                                    ctx.instrumentation.record_output_event(&event_to_write);
                                    ctx.instrumentation
                                        .events_processed_total
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    ctx.instrumentation
                                        .record_processing_time(per_data_event_duration);
                                }
                                ctx.pending_outputs.push_back(event_to_write);
                            }

                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(None) => {
                            if !self.replay_completed_emitted {
                                self.replay_completed_emitted = true;
                                let duration_ms = self
                                    .replay_started_at
                                    .map(|started| {
                                        let ms = started.elapsed().as_millis();
                                        u64::try_from(ms).unwrap_or(u64::MAX)
                                    })
                                    .unwrap_or(0);
                                let (replayed_count, skipped_count) = self
                                    .replay_driver
                                    .as_ref()
                                    .map_or((0, 0), |d| (d.replayed_events(), d.skipped_events()));
                                let completed_event = SystemEvent::new(
                                    WriterId::from(self.stage_id),
                                    SystemEventType::ReplayLifecycle(
                                        ReplayLifecycleEvent::Completed {
                                            replayed_count,
                                            skipped_count,
                                            duration_ms,
                                        },
                                    ),
                                );
                                if let Err(e) =
                                    self.system_journal.append(completed_event, None).await
                                {
                                    tracing::error!(
                                        stage_name = %ctx.stage_name,
                                        journal_error = %e,
                                        "Failed to append ReplayLifecycle::Completed system event"
                                    );
                                }
                            }

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
                    // Get next batch of events from handler.
                    // 051b: handler now returns Result<Vec<ChainEvent>, SourceError>.
                    let tick_started_at = Instant::now();
                    let next_result = self.handler.next();
                    let tick_duration = tick_started_at.elapsed();

                    match next_result {
                        Ok(events) if !events.is_empty() => {
                            self.idle_backoff.reset();
                            // We have work - increment loops with work
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            let data_events_in_tick =
                                events.iter().filter(|event| event.is_data()).count();
                            let per_data_event_duration = if data_events_in_tick > 0 {
                                let nanos = (tick_duration.as_nanos() / data_events_in_tick as u128)
                                    .min(u64::MAX as u128)
                                    as u64;
                                Duration::from_nanos(nanos)
                            } else {
                                Duration::from_nanos(0)
                            };

                            for event in events {
                                // Enrich with runtime context
                                let flow_context = FlowContext {
                                    flow_name: ctx.flow_name.clone(),
                                    flow_id: ctx.flow_id.to_string(),
                                    stage_name: ctx.stage_name.clone(),
                                    stage_id: self.stage_id,
                                    stage_type:
                                        obzenflow_core::event::context::StageType::InfiniteSource,
                                };

                                let staged_event = event.with_flow_context(flow_context);

                                // Track error-marked events for lifecycle/flow rollups.
                                // Record before snapshot so the error event carries updated totals.
                                if let ProcessingStatus::Error { kind, .. } =
                                    &staged_event.processing_info.status
                                {
                                    let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                                    ctx.instrumentation.record_error(k);
                                }

                                // Apply run_if_not_error pattern (FLOWIP-082g)
                                let events_to_write =
                                    self.run_if_not_error(staged_event, |e| vec![e]);

                                // Write events based on their status
                                for event_to_write in events_to_write {
                                    if event_to_write.is_data() {
                                        ctx.instrumentation.record_output_event(&event_to_write);
                                        ctx.instrumentation
                                            .events_processed_total
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        ctx.instrumentation
                                            .record_processing_time(per_data_event_duration);
                                    }
                                    ctx.pending_outputs.push_back(event_to_write);
                                }
                            }

                            tracing::trace!(
                                stage_name = %ctx.stage_name,
                                "Infinite source emitted batch of events"
                            );

                            Ok(EventLoopDirective::Continue)
                        }
                        Ok(_events) => {
                            // Handler advanced but produced no data/control events this tick.
                            let delay = self.idle_backoff.next_delay();
                            tokio::time::sleep(delay).await;
                            Ok(EventLoopDirective::Continue)
                        }
                        Err(e) => {
                            self.idle_backoff.reset();
                            // SourceError surfaced from handler; delegate to control strategy
                            // via the standard error path.
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = %e,
                                "Infinite source handler.next() returned error"
                            );
                            Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                                e.to_string(),
                            )))
                        }
                    }
                }
            }

            InfiniteSourceState::Draining => {
                self.idle_backoff.reset();
                // Draining state - prepare to send EOF
                Ok(EventLoopDirective::Transition(
                    InfiniteSourceEvent::Completed,
                ))
            }

            InfiniteSourceState::Drained => {
                self.idle_backoff.reset();
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            InfiniteSourceState::Failed(_) => {
                self.idle_backoff.reset();
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            InfiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
