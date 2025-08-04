//! Infinite source supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::context::FlowContext;
use obzenflow_fsm::{FsmBuilder, Transition};
use crate::stages::common::handlers::InfiniteSourceHandler;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;

use super::fsm::{
    InfiniteSourceState, InfiniteSourceEvent, InfiniteSourceAction,
    InfiniteSourceContext,
};

/// Supervisor for infinite source stages
pub(crate) struct InfiniteSourceSupervisor<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,
    
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
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for InfiniteSourceSupervisor<H> {}

impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for InfiniteSourceSupervisor<H> {
    type State = InfiniteSourceState<H>;
    type Event = InfiniteSourceEvent<H>;
    type Context = InfiniteSourceContext<H>;
    type Action = InfiniteSourceAction<H>;
    
    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>) 
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: InfiniteSourceState::Initialized,
                        actions: vec![InfiniteSourceAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> WaitingForGun
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: InfiniteSourceState::WaitingForGun,
                        actions: vec![], // Sources wait quietly
                    })
                })
                .done()
            
            // WaitingForGun -> Running
            .when("WaitingForGun")
                .on("Start", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: InfiniteSourceState::Running,
                        actions: vec![], // Emission happens in dispatch_state
                    })
                })
                .done()
            
            // Running -> Draining (only on drain request, infinite sources don't complete naturally)
            .when("Running")
                .on("BeginDrain", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: InfiniteSourceState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let InfiniteSourceEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: InfiniteSourceState::Failed(msg),
                                actions: vec![InfiniteSourceAction::Cleanup],
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
                        next_state: InfiniteSourceState::Drained,
                        actions: vec![InfiniteSourceAction::SendEOF, InfiniteSourceAction::WriteStageCompleted, InfiniteSourceAction::Cleanup],
                    })
                })
                .done()
            
            // Error handling from any state
            .from_any()
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
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
                    }
                })
                .done()
            
            // Terminal states
            .when("Drained")
                .done()
            .when("Failed")
                .done()
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for InfiniteSourceSupervisor<H> {
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
                self.context.instrumentation.event_loops_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Check if we can emit
                let can_emit = *self.context.can_emit.read().await;
                if !can_emit {
                    return Ok(EventLoopDirective::Continue);
                }
                
                // Check if shutdown was requested
                let shutdown_requested = *self.context.shutdown_requested.read().await;
                if shutdown_requested {
                    return Ok(EventLoopDirective::Transition(InfiniteSourceEvent::BeginDrain));
                }
                
                // Get next event from handler
                let mut handler = self.context.handler.write().await;
                
                // Try to get next event
                if let Some(mut event) = handler.next() {
                    drop(handler);
                    
                    // We have work - increment loops with work
                    self.context.instrumentation.event_loops_with_work_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // Enrich with runtime context
                    let flow_context = FlowContext {
                        flow_name: self.context.flow_name.clone(),
                        flow_id: self.context.flow_id.to_string(),
                        stage_name: self.context.stage_name.clone(),
                        stage_id: self.stage_id.clone(),
                        stage_type: obzenflow_core::event::context::StageType::InfiniteSource,
                    };

                    let enriched_event = event
                        .with_flow_context(flow_context)
                        .with_runtime_context(self.context.instrumentation.snapshot());
                    
                    // Apply run_if_not_error pattern (FLOWIP-082g)
                    let events_to_write = self.run_if_not_error(enriched_event.clone(), |e| vec![e]);
                    
                    // Write events based on their status
                    for event_to_write in events_to_write {
                        let journal = if matches!(event_to_write.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)) {
                            &self.context.error_journal
                        } else {
                            &self.context.data_journal
                        };
                        
                        journal
                            .append(event_to_write, None)
                            .await
                            .map_err(|e| format!("Failed to write event: {}", e))?;
                    }
                    
                    tracing::trace!(
                        stage_name = %self.context.stage_name,
                        "Infinite source emitted event"
                    );
                }
                
                Ok(EventLoopDirective::Continue)
            }
            
            InfiniteSourceState::Draining => {
                // Draining state - prepare to send EOF
                Ok(EventLoopDirective::Transition(InfiniteSourceEvent::Completed))
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