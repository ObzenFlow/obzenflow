//! Transform supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{WriterId, Journal, ChainEvent, EventEnvelope};
use obzenflow_core::event::flow_context::FlowContext;
use obzenflow_fsm::{FsmBuilder, Transition};
use obzenflow_topology_services::stages::StageId;

use crate::messaging::reactive_journal::ReactiveJournal;
use crate::stages::common::handlers::TransformHandler;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;

use super::fsm::{
    TransformState, TransformEvent, TransformAction,
    TransformContext,
};

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,
    
    /// The FSM context containing all mutable state
    pub(crate) context: Arc<TransformContext<H>>,
    
    /// Journal reference
    pub(crate) journal: Arc<ReactiveJournal>,
    
    /// Writer ID for this transform
    pub(crate) writer_id: WriterId,
    
    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for TransformSupervisor to satisfy Supervisor trait bound
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for TransformSupervisor<H> {}

impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for TransformSupervisor<H> {
    type State = TransformState<H>;
    type Event = TransformEvent<H>;
    type Context = TransformContext<H>;
    type Action = TransformAction<H>;
    
    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>) 
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: TransformState::Initialized,
                        actions: vec![TransformAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> Running (transforms start immediately)
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: TransformState::Running,
                        actions: vec![TransformAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Draining (on EOF)
            .when("Running")
                .on("ReceivedEOF", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: TransformState::Draining,
                        actions: vec![], // Continue draining in dispatch_state
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let TransformEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: TransformState::Failed(msg),
                                actions: vec![TransformAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Draining -> Drained
            .when("Draining")
                .on("DrainComplete", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: TransformState::Drained,
                        actions: vec![
                            TransformAction::ForwardEOF,
                            TransformAction::SendCompletion,
                            TransformAction::Cleanup,
                        ],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let TransformEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: TransformState::Failed(msg),
                                actions: vec![TransformAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Error transitions from any state
            .from_any()
                .on("Error", |state, event, _ctx| {
                    let event = event.clone();
                    let state = state.clone();
                    async move {
                        if let TransformEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: TransformState::Failed(msg),
                                    actions: vec![TransformAction::Cleanup],
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
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for TransformSupervisor<H> {
    type Handler = H;
    
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            TransformState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(TransformEvent::Initialize))
            }
            
            TransformState::Initialized => {
                // Auto-transition to ready (transforms start immediately)
                Ok(EventLoopDirective::Transition(TransformEvent::Ready))
            }
            
            TransformState::Running => {
                // Process events from subscription
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    match subscription.recv().await {
                        Ok(envelope) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Transform processing event"
                            );
                            
                            // Processing context for control events
                            let mut processing_ctx = ProcessingContext::new();
                                // Check if this is a control event
                                if let Some(control_type) = envelope.event.as_control_type() {
                                    // Handle control events with strategy
                                    let action = match control_type {
                                        ChainEvent::EOF_EVENT_TYPE => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                "Transform received EOF from upstream"
                                            );
                                            self.context.control_strategy.handle_eof(&envelope, &mut processing_ctx)
                                        }
                                        ChainEvent::WATERMARK_EVENT_TYPE => {
                                            self.context.control_strategy.handle_watermark(&envelope, &mut processing_ctx)
                                        }
                                        ChainEvent::CHECKPOINT_EVENT_TYPE => {
                                            self.context.control_strategy.handle_checkpoint(&envelope, &mut processing_ctx)
                                        }
                                        ChainEvent::DRAIN_EVENT_TYPE => {
                                            self.context.control_strategy.handle_drain(&envelope, &mut processing_ctx)
                                        }
                                        _ => {
                                            tracing::debug!(
                                                stage_name = %self.context.stage_name,
                                                control_type = control_type,
                                                "Unknown control event"
                                            );
                                            ControlEventAction::Forward
                                        }
                                    };
                                    
                                    // Execute the action
                                    match action {
                                        ControlEventAction::Forward => {
                                            // Forward control events immediately
                                            if control_type != ChainEvent::EOF_EVENT_TYPE {
                                                self.forward_control_event(&envelope).await?;
                                            } else {
                                                // Buffer EOF for draining state
                                                *self.context.buffered_eof.write().await = Some(envelope.event.clone());
                                                drop(subscription_guard);
                                                return Ok(EventLoopDirective::Transition(TransformEvent::ReceivedEOF));
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                control_type = control_type,
                                                duration = ?duration,
                                                "Delaying control event"
                                            );
                                            tokio::time::sleep(duration).await;
                                            // Return Continue to re-process after delay
                                            return Ok(EventLoopDirective::Continue);
                                        }
                                        ControlEventAction::Retry => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                control_type = control_type,
                                                "Retry requested, buffering event"
                                            );
                                            if control_type == ChainEvent::EOF_EVENT_TYPE {
                                                processing_ctx.buffered_eof = Some(envelope.clone());
                                            }
                                        }
                                        ControlEventAction::Skip => {
                                            tracing::warn!(
                                                stage_name = %self.context.stage_name,
                                                control_type = control_type,
                                                "Skipping control event (dangerous!)"
                                            );
                                            // Don't forward, don't process
                                        }
                                    }
                                } else {
                                    // Process normal data event
                                    let transformed_events = self.context.handler.process(envelope.event.clone());
                                    
                                    // Write transformed events
                                    let writer_id_guard = self.context.writer_id.read().await;
                                    if let Some(writer_id) = writer_id_guard.as_ref() {
                                        for mut event in transformed_events {
                                            // Add flow context
                                            event.flow_context = FlowContext {
                                                flow_name: self.context.flow_name.clone(),
                                                flow_id: format!("{}-{}", self.context.flow_name, self.context.stage_id),
                                                stage_name: self.context.stage_name.clone(),
                                                stage_type: obzenflow_core::event::flow_context::StageType::Transform,
                                            };
                                            
                                            self.context.journal
                                                .append(writer_id, event, Some(&envelope))
                                                .await?;
                                        }
                                    }
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
                                TransformEvent::Error(format!("Subscription error: {}", e))
                            ));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                Ok(EventLoopDirective::Continue)
            }
            
            TransformState::Draining => {
                // Continue processing remaining events with short timeout
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    // Use shorter timeout to detect empty queue
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(50),
                        subscription.recv()
                    ).await {
                        Ok(Ok(envelope)) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Transform draining events"
                            );
                            
                            // Process remaining event
                            if !envelope.event.is_control() {
                                let transformed_events = self.context.handler.process(envelope.event.clone());
                                
                                let writer_id_guard = self.context.writer_id.read().await;
                                if let Some(writer_id) = writer_id_guard.as_ref() {
                                    for mut event in transformed_events {
                                        event.flow_context = FlowContext {
                                            flow_name: self.context.flow_name.clone(),
                                            flow_id: format!("{}-{}", self.context.flow_name, self.context.stage_id),
                                            stage_name: self.context.stage_name.clone(),
                                            stage_type: obzenflow_core::event::flow_context::StageType::Transform,
                                        };
                                        
                                        self.context.journal
                                            .append(writer_id, event, Some(&envelope))
                                            .await?;
                                    }
                                }
                            }
                            Ok(EventLoopDirective::Continue)
                        }
                        _ => {
                            // Timeout or empty - queue is drained
                            tracing::info!(
                                stage_name = %self.context.stage_name,
                                "Transform queue drained"
                            );
                            drop(subscription_guard);
                            Ok(EventLoopDirective::Transition(TransformEvent::DrainComplete))
                        }
                    }
                } else {
                    // No subscription, complete immediately
                    Ok(EventLoopDirective::Transition(TransformEvent::DrainComplete))
                }
            }
            
            TransformState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            TransformState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            TransformState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> TransformSupervisor<H> {
    /// Helper to forward control events
    async fn forward_control_event(&self, envelope: &EventEnvelope) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let writer_id_guard = self.context.writer_id.read().await;
        if let Some(writer_id) = writer_id_guard.as_ref() {
            let forward_event = envelope.event.clone();
            self.context.journal
                .write(writer_id, forward_event, Some(envelope))
                .await?;
        }
        Ok(())
    }
}