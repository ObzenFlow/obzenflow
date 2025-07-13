//! Sink supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{WriterId, ChainEvent};
use obzenflow_fsm::{FsmBuilder, Transition};
use obzenflow_core::StageId;

use crate::messaging::reactive_journal::ReactiveJournal;
use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;

use super::fsm::{
    SinkState, SinkEvent, SinkAction,
    SinkContext,
};

/// Supervisor for sink stages
pub(crate) struct SinkSupervisor<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,
    
    /// The FSM context containing all mutable state
    pub(crate) context: Arc<SinkContext<H>>,
    
    /// Journal reference
    pub(crate) journal: Arc<ReactiveJournal>,
    
    /// Writer ID for this sink
    pub(crate) writer_id: WriterId,
    
    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for SinkSupervisor to satisfy Supervisor trait bound
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for SinkSupervisor<H> {}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for SinkSupervisor<H> {
    type State = SinkState<H>;
    type Event = SinkEvent<H>;
    type Context = SinkContext<H>;
    type Action = SinkAction<H>;
    
    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>) 
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Initialized,
                        actions: vec![SinkAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> Running (sinks auto-start)
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Running,
                        actions: vec![SinkAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Flushing (on EOF)
            .when("Running")
                .on("ReceivedEOF", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Flushing,
                        actions: vec![SinkAction::FlushBuffers],
                    })
                })
                .on("BeginFlush", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Flushing,
                        actions: vec![SinkAction::FlushBuffers],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let SinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: SinkState::Failed(msg),
                                actions: vec![SinkAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Flushing -> Draining
            .when("Flushing")
                .on("FlushComplete", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let SinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: SinkState::Failed(msg),
                                actions: vec![SinkAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Draining -> Drained
            .when("Draining")
                .on("BeginDrain", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: SinkState::Drained,
                        actions: vec![SinkAction::SendCompletion, SinkAction::Cleanup],
                    })
                })
                .done()
            
            // Error transitions from any state
            .from_any()
                .on("Error", |state, event, _ctx| {
                    let event = event.clone();
                    let state = state.clone();
                    async move {
                        if let SinkEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, SinkState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: SinkState::Failed(msg),
                                    actions: vec![SinkAction::Cleanup],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    }
                })
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
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for SinkSupervisor<H> {
    type Handler = H;
    
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            SinkState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(SinkEvent::Initialize))
            }
            
            SinkState::Initialized => {
                // Auto-transition to ready
                Ok(EventLoopDirective::Transition(SinkEvent::Ready))
            }
            
            SinkState::Running => {
                // Check subscription for events
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    match subscription.recv().await {
                        Ok(envelope) => {
                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "Sink processing event"
                            );
                            
                            // Check for EOF event
                            if envelope.event.event_type == ChainEvent::EOF_EVENT_TYPE {
                                tracing::info!(
                                    stage_name = %self.context.stage_name,
                                    "Sink received EOF"
                                );
                                drop(subscription_guard);
                                return Ok(EventLoopDirective::Transition(SinkEvent::ReceivedEOF));
                            }
                            
                            // Process normal event
                            let mut handler = self.context.handler.write().await;
                            if let Err(e) = handler.consume(envelope.event) {
                                tracing::error!(
                                    stage_name = %self.context.stage_name,
                                    error = ?e,
                                    "Failed to consume event"
                                );
                                drop(handler);
                                drop(subscription_guard);
                                return Ok(EventLoopDirective::Transition(
                                    SinkEvent::Error(format!("Failed to consume event: {:?}", e))
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Subscription error"
                            );
                            drop(subscription_guard);
                            return Ok(EventLoopDirective::Transition(
                                SinkEvent::Error(format!("Subscription error: {}", e))
                            ));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                Ok(EventLoopDirective::Continue)
            }
            
            SinkState::Flushing => {
                // Wait for flush to complete
                // The actual flush happens in the action
                Ok(EventLoopDirective::Transition(SinkEvent::FlushComplete))
            }
            
            SinkState::Draining => {
                // Move to drained state
                Ok(EventLoopDirective::Transition(SinkEvent::BeginDrain))
            }
            
            SinkState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            SinkState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            SinkState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}