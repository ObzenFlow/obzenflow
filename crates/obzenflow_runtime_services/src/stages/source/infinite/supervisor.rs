//! Infinite source supervisor implementation using HandlerSupervised pattern

use crate::stages::common::handlers::InfiniteSourceHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fsm::{
    InfiniteSourceAction, InfiniteSourceContext, InfiniteSourceEvent, InfiniteSourceState,
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

    /// Data journal for writing generated events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,
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
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Track every event loop iteration
        match state {
            InfiniteSourceState::Created => {
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::Initialized => {
                // Wait for ready signal
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::WaitingForGun => {
                // Wait for start signal from pipeline
                tracing::debug!(
                    stage_name = %self.context.stage_name,
                    "Infinite source waiting for start signal"
                );
                Ok(EventLoopDirective::Continue)
            }

            InfiniteSourceState::Running => {
                self.context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Check if we can emit
                if !self.context.can_emit {
                    return Ok(EventLoopDirective::Continue);
                }

                // Check if shutdown was requested
                if self.context.shutdown_requested {
                    return Ok(EventLoopDirective::Transition(
                        InfiniteSourceEvent::BeginDrain,
                    ));
                }

                // Get next batch of events from handler.
                // 051b: handler now returns Result<Vec<ChainEvent>, SourceError>.
                let tick_started_at = Instant::now();
                let next_result = self.handler.next();
                let tick_duration = tick_started_at.elapsed();

                match next_result {
                    Ok(events) if !events.is_empty() => {
                        // We have work - increment loops with work
                        self.context
                            .instrumentation
                            .event_loops_with_work_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        let data_events_in_tick = events.iter().filter(|event| event.is_data()).count();
                        let per_data_event_duration = if data_events_in_tick > 0 {
                            let nanos = (tick_duration.as_nanos()
                                / data_events_in_tick as u128)
                                .min(u64::MAX as u128) as u64;
                            Duration::from_nanos(nanos)
                        } else {
                            Duration::from_nanos(0)
                        };

                        for event in events {
                            // Enrich with runtime context
                            let flow_context = FlowContext {
                                flow_name: self.context.flow_name.clone(),
                                flow_id: self.context.flow_id.to_string(),
                                stage_name: self.context.stage_name.clone(),
                                stage_id: self.stage_id.clone(),
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
                                self.context.instrumentation.record_error(k);
                            }

                            // Apply run_if_not_error pattern (FLOWIP-082g)
                            let events_to_write =
                                self.run_if_not_error(staged_event, |e| vec![e]);

                            // Write events based on their status
                            for event_to_write in events_to_write {
                                let journal = if matches!(event_to_write.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }) {
                                    &self.context.error_journal
                                } else {
                                    &self.context.data_journal
                                };

                                // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                // Lifecycle events (middleware metrics, etc.) are observability
                                // overhead and should not participate in transport contracts.
                                if event_to_write.is_data() {
                                    self.context.instrumentation.record_emitted(&event_to_write);
                                    self.context
                                        .instrumentation
                                        .events_processed_total
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    self.context
                                        .instrumentation
                                        .record_processing_time(per_data_event_duration);
                                }
                                let enriched_event_to_write = event_to_write.with_runtime_context(
                                    self.context.instrumentation.snapshot_with_control(),
                                );
                                journal
                                    .append(enriched_event_to_write, None)
                                    .await
                                    .map_err(|e| format!("Failed to write event: {}", e))?;
                            }
                        }

                        tracing::trace!(
                            stage_name = %self.context.stage_name,
                            "Infinite source emitted batch of events"
                        );

                        Ok(EventLoopDirective::Continue)
                    }
                    Ok(_events) => {
                        // Handler advanced but produced no data/control events this tick.
                        Ok(EventLoopDirective::Continue)
                    }
                    Err(e) => {
                        // SourceError surfaced from handler; delegate to control strategy
                        // via the standard error path.
                        tracing::error!(
                            stage_name = %self.context.stage_name,
                            error = %e,
                            "Infinite source handler.next() returned error"
                        );
                        Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Error(
                            e.to_string(),
                        )))
                    }
                }
            }

            InfiniteSourceState::Draining => {
                // Draining state - prepare to send EOF
                Ok(EventLoopDirective::Transition(
                    InfiniteSourceEvent::Completed,
                ))
            }

            InfiniteSourceState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            InfiniteSourceState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            InfiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
