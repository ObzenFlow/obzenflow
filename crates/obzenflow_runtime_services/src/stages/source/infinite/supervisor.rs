//! Infinite source supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{WriterId, Journal};
use obzenflow_core::event::flow_context::FlowContext;
use obzenflow_fsm::{FsmBuilder, Transition};
use obzenflow_topology_services::stages::StageId;

use crate::event_flow::reactive_journal::ReactiveJournal;
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
    
    /// Journal reference
    pub(crate) journal: Arc<ReactiveJournal>,
    
    /// Writer ID for this source
    pub(crate) writer_id: WriterId,
    
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
                        actions: vec![InfiniteSourceAction::SendEOF, InfiniteSourceAction::Cleanup],
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
    
    fn journal(&self) -> &Arc<ReactiveJournal> {
        &self.journal
    }
    
    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for InfiniteSourceSupervisor<H> {
    type Handler = H;
    
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
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
                    
                    // Add flow context
                    event.flow_context = FlowContext {
                        flow_name: self.context.flow_name.clone(),
                        flow_id: format!("{}-{}", self.context.flow_name, self.context.stage_id),
                        stage_name: self.context.stage_name.clone(),
                        stage_type: obzenflow_core::event::flow_context::StageType::Source,
                    };
                    
                    // Get writer ID
                    let writer_id_guard = self.context.writer_id.read().await;
                    let writer_id = writer_id_guard
                        .as_ref()
                        .ok_or("No writer ID available")?;
                    
                    // Write event to journal
                    self.context.journal
                        .append(writer_id, event, None)
                        .await?;
                    
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