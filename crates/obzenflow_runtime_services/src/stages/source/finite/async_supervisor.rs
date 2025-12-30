//! Async finite source supervisor implementation using HandlerSupervised pattern

use crate::stages::common::handlers::AsyncFiniteSourceHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, EventReceiver, HandlerSupervised, StateWatcher};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fsm::{FiniteSourceAction, FiniteSourceContext, FiniteSourceEvent, FiniteSourceState};

/// Supervisor for async finite source stages
pub(crate) struct AsyncFiniteSourceSupervisor<
    H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The handler instance that implements source logic
    pub(crate) handler: H,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<FiniteSourceContext<H>>,

    /// Data journal for writing generated events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// External control events (Initialize/Ready/Start/BeginDrain).
    pub(crate) external_events: EventReceiver<FiniteSourceEvent<H>>,

    /// State watcher for UI/handles.
    pub(crate) state_watcher: StateWatcher<FiniteSourceState<H>>,
}

// Implement Sealed directly for AsyncFiniteSourceSupervisor to satisfy Supervisor trait bound
impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for AsyncFiniteSourceSupervisor<H>
{
}

impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for AsyncFiniteSourceSupervisor<H>
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
                        supervisor = "AsyncFiniteSourceSupervisor",
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
impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    HandlerSupervised for AsyncFiniteSourceSupervisor<H>
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
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        let _ = self.state_watcher.update(state.clone());

        match state {
            FiniteSourceState::Created
            | FiniteSourceState::Initialized
            | FiniteSourceState::WaitingForGun => match self.external_events.recv().await {
                Some(event) => Ok(EventLoopDirective::Transition(event)),
                None => Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                    "External control channel closed".to_string(),
                ))),
            },

            FiniteSourceState::Running => {
                self.context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let tick_started_at = Instant::now();
                let next_result = tokio::select! {
                    biased;
                    maybe_event = self.external_events.recv() => {
                        match maybe_event {
                            Some(event) => return Ok(EventLoopDirective::Transition(event)),
                            None => {
                                return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                                    "External control channel closed".to_string(),
                                )));
                            }
                        }
                    }
                    result = self.handler.next() => result,
                };
                let tick_duration = tick_started_at.elapsed();

                match next_result {
                    Ok(Some(events)) if !events.is_empty() => {
                        self.context
                            .instrumentation
                            .event_loops_with_work_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        let data_events_in_tick =
                            events.iter().filter(|event| event.is_data()).count();
                        let per_data_event_duration = if data_events_in_tick > 0 {
                            let nanos = (tick_duration.as_nanos() / data_events_in_tick as u128)
                                .min(u64::MAX as u128) as u64;
                            Duration::from_nanos(nanos)
                        } else {
                            Duration::from_nanos(0)
                        };

                        for event in events {
                            let flow_context = FlowContext {
                                flow_name: self.context.flow_name.clone(),
                                flow_id: self.context.flow_id.to_string(),
                                stage_name: self.context.stage_name.clone(),
                                stage_id: self.stage_id.clone(),
                                stage_type: obzenflow_core::event::context::StageType::FiniteSource,
                            };

                            let staged_event = event.with_flow_context(flow_context);

                            if let ProcessingStatus::Error { kind, .. } =
                                &staged_event.processing_info.status
                            {
                                let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                                self.context.instrumentation.record_error(k);
                            }

                            let events_to_write = self.run_if_not_error(staged_event, |e| vec![e]);

                            for event_to_write in events_to_write {
                                let journal = if matches!(
                                    event_to_write.processing_info.status,
                                    obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }
                                ) {
                                    &self.context.error_journal
                                } else {
                                    &self.context.data_journal
                                };

                                if event_to_write.is_data() {
                                    self.context
                                        .instrumentation
                                        .record_output_event(&event_to_write);
                                    self.context.instrumentation.events_processed_total.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    self.context
                                        .instrumentation
                                        .record_processing_time(per_data_event_duration);
                                }

                                let enriched_event_to_write = event_to_write.with_runtime_context(
                                    self.context.instrumentation.snapshot_with_control(),
                                );
                                let written = journal
                                    .append(enriched_event_to_write, None)
                                    .await
                                    .map_err(|e| format!("Failed to write event: {}", e))?;

                                if Arc::ptr_eq(journal, &self.context.data_journal) {
                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                        &written,
                                        &self.system_journal,
                                    )
                                    .await;
                                }
                            }
                        }

                        Ok(EventLoopDirective::Continue)
                    }
                    Ok(Some(_events)) => Ok(EventLoopDirective::Continue),
                    Ok(None) => Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed)),
                    Err(e) => {
                        tracing::error!(
                            stage_name = %self.context.stage_name,
                            error = %e,
                            "Async finite source handler.next() returned error"
                        );
                        Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                            e.to_string(),
                        )))
                    }
                }
            }

            FiniteSourceState::Draining => {
                if let Err(e) = self.handler.drain().await {
                    tracing::warn!(
                        stage_name = %self.context.stage_name,
                        error = %e,
                        "drain() failed; continuing shutdown"
                    );
                }
                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
            }

            FiniteSourceState::Drained => Ok(EventLoopDirective::Terminate),
            FiniteSourceState::Failed(_) => Ok(EventLoopDirective::Terminate),
            FiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
