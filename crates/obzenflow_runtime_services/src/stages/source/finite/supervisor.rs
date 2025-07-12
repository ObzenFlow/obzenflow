//! Finite source supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{WriterId, Journal};
use obzenflow_core::event::flow_context::FlowContext;
use obzenflow_fsm::{FsmBuilder, Transition};
use obzenflow_topology_services::stages::StageId;

use crate::messaging::reactive_journal::ReactiveJournal;
use crate::stages::common::handlers::FiniteSourceHandler;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;

use super::fsm::{
    FiniteSourceState, FiniteSourceEvent, FiniteSourceAction,
    FiniteSourceContext,
};

/// Supervisor for finite source stages
pub(crate) struct FiniteSourceSupervisor<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,
    
    /// The FSM context containing all mutable state
    pub(crate) context: Arc<FiniteSourceContext<H>>,
    
    /// Journal reference
    pub(crate) journal: Arc<ReactiveJournal>,
    
    /// Writer ID for this source
    pub(crate) writer_id: WriterId,
    
    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for FiniteSourceSupervisor to satisfy Supervisor trait bound
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for FiniteSourceSupervisor<H> {}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for FiniteSourceSupervisor<H> {
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;
    
    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>) 
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
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
                        actions: vec![], // Emission happens in dispatch_state
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
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for FiniteSourceSupervisor<H> {
    type Handler = H;
    
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            FiniteSourceState::Created => {
                // Wait for initialization
                Ok(EventLoopDirective::Continue)
            }
            
            FiniteSourceState::Initialized => {
                // Auto-transition to waiting state (sources wait for pipeline signal)
                Ok(EventLoopDirective::Transition(FiniteSourceEvent::Ready))
            }
            
            FiniteSourceState::WaitingForGun => {
                // Wait for start signal from pipeline
                Ok(EventLoopDirective::Continue)
            }
            
            FiniteSourceState::Running => {
                // Get next event from handler
                let mut handler = self.context.handler.write().await;
                
                // Check if source is complete
                if handler.is_complete() {
                    drop(handler);
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Completed));
                }
                
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
                        "Source emitted event"
                    );
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