//! Finite source supervisor implementation using HandlerSupervised pattern

use crate::stages::common::handlers::FiniteSourceHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmBuilder, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{FiniteSourceAction, FiniteSourceContext, FiniteSourceEvent, FiniteSourceState};

/// Supervisor for finite source stages
pub(crate) struct FiniteSourceSupervisor<
    H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<FiniteSourceContext<H>>,

    /// Data journal for writing generated events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for FiniteSourceSupervisor to satisfy Supervisor trait bound
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for FiniteSourceSupervisor<H>
{
}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for FiniteSourceSupervisor<H>
{
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;

    fn configure_fsm(
        &self,
        builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::Initialized,
                        actions: vec![FiniteSourceAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> WaitingForGun
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::WaitingForGun,
                        actions: vec![], // Sources wait quietly
                    })
                })
                .done()
            
            // WaitingForGun -> Running (THE KEY DIFFERENCE!)
            .when("WaitingForGun")
                .on("Start", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::Running,
                        actions: vec![FiniteSourceAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Draining (on completion or drain request)
            .when("Running")
                .on("Completed", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::Draining,
                        actions: vec![],
                    })
                })
                .on("BeginDrain", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let FiniteSourceEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: FiniteSourceState::Failed(msg),
                                actions: vec![FiniteSourceAction::Cleanup],
                            })
                        } else {
                            Err("Invalid event".to_string())
                        }
                    }
                })
                .done()
            
            // Draining -> Drained
            .when("Draining")
                .on("Completed", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: FiniteSourceState::Drained,
                        actions: vec![FiniteSourceAction::SendEOF, FiniteSourceAction::WriteStageCompleted, FiniteSourceAction::Cleanup],
                    })
                })
                .done()
            
            // Error handling from any state
            .from_any()
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
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
                    }
                })
                .done()
            
            // Terminal states
            .when("Drained")
                .done()
            .when("Failed")
                .done()

            // Catch all unhandled events
            .when_unhandled(|state, event, _ctx| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                async move {
                    tracing::error!(
                        supervisor = "FiniteSourceSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    // Return Err to propagate the error
                    Err(format!("Unhandled event '{}' in state '{}' for FiniteSourceSupervisor", event_name, state_name))
                }
            })
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

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write completion event: {}", e).into())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Track every event loop iteration
        match state {
            FiniteSourceState::Created => {
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Initialized => {
                // Wait for Ready event from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::WaitingForGun => {
                // Wait for start signal from pipeline
                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Running => {
                self.context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Get next event from handler
                let mut handler = self.context.handler.write().await;

                // Try to get next event
                if let Some(mut event) = handler.next() {
                    drop(handler);

                    // We have work - increment loops with work
                    self.context
                        .instrumentation
                        .event_loops_with_work_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // Enrich with runtime context
                    let flow_context = FlowContext {
                        flow_name: self.context.flow_name.clone(),
                        flow_id: self.context.flow_id.to_string(),
                        stage_name: self.context.stage_name.clone(),
                        stage_id: self.stage_id.clone(),
                        stage_type: obzenflow_core::event::context::StageType::FiniteSource,
                    };

                    let enriched_event = event
                        .with_flow_context(flow_context)
                        .with_runtime_context(self.context.instrumentation.snapshot());

                    // Apply run_if_not_error pattern (FLOWIP-082g)
                    let events_to_write =
                        self.run_if_not_error(enriched_event.clone(), |e| vec![e]);

                    // Write events based on their status
                    for event_to_write in events_to_write {
                        let journal = if matches!(event_to_write.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)) {
                            &self.context.error_journal
                        } else {
                            &self.context.data_journal
                        };

                        self.context.instrumentation.record_emitted(&event_to_write);
                        journal
                            .append(event_to_write, None)
                            .await
                            .map_err(|e| format!("Failed to write event: {}", e))?;
                    }

                    // Increment events processed counter after successful write
                    self.context
                        .instrumentation
                        .events_processed_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    tracing::trace!(
                        stage_name = %self.context.stage_name,
                        "Source emitted event"
                    );
                } else {
                    // No more events available, check if source is complete
                    if handler.is_complete() {
                        drop(handler);
                        return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed));
                    }
                    drop(handler);
                }

                Ok(EventLoopDirective::Continue)
            }

            FiniteSourceState::Draining => {
                // Draining state - prepare to send EOF
                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed))
            }

            FiniteSourceState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            FiniteSourceState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
