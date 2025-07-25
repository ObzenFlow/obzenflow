//! Transform supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{EventEnvelope};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_fsm::{FsmBuilder, Transition};
use crate::stages::common::handlers::TransformHandler;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;
use crate::metrics::instrumentation::process_with_instrumentation;

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
    
    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
    
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
                .on("Initialize", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Initialized");
                    
                    Ok(Transition {
                        next_state: TransformState::Initialized,
                        actions: vec![TransformAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> Running (transforms start immediately)
            .when("Initialized")
                .on("Ready", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Running");
                    
                    Ok(Transition {
                        next_state: TransformState::Running,
                        actions: vec![TransformAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Draining (on EOF)
            .when("Running")
                .on("ReceivedEOF", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Draining");
                    
                    Ok(Transition {
                        next_state: TransformState::Draining,
                        actions: vec![], // Continue draining in dispatch_state
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let TransformEvent::Error(msg) = event {
                            // Track state transition and failure in instrumentation
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
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
                .on("DrainComplete", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Drained");
                    
                    Ok(Transition {
                        next_state: TransformState::Drained,
                        actions: vec![
                            TransformAction::ForwardEOF,
                            TransformAction::SendCompletion,
                            TransformAction::Cleanup,
                        ],
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let TransformEvent::Error(msg) = event {
                            // Track state transition and failure in instrumentation
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
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
    
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for TransformSupervisor<H> {
    type Handler = H;
    
    fn writer_id(&self) -> obzenflow_core::WriterId {
        obzenflow_core::WriterId::from(self.stage_id)
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
            TransformState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(TransformEvent::Initialize))
            }
            
            TransformState::Initialized => {
                // Auto-transition to ready (transforms start immediately)
                Ok(EventLoopDirective::Transition(TransformEvent::Ready))
            }
            
            TransformState::Running => {
                self.context.instrumentation.event_loops_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Process events from subscription
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    match subscription.recv().await {
                        Ok(envelope) => {
                            // We have work - increment loops with work
                            self.context.instrumentation.event_loops_with_work_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Transform processing event"
                            );
                            
                            // Match on event content to determine how to process
                            match &envelope.event.content {
                                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                                    // Processing context for control events
                                    let mut processing_ctx = ProcessingContext::new();
                                    
                                    // Get the action from the control strategy
                                    let action = match signal {
                                        FlowControlPayload::Eof { .. } => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                "Transform received EOF from upstream"
                                            );
                                            self.context.control_strategy.handle_eof(&envelope, &mut processing_ctx)
                                        }
                                        FlowControlPayload::Watermark { .. } => {
                                            self.context.control_strategy.handle_watermark(&envelope, &mut processing_ctx)
                                        }
                                        FlowControlPayload::Checkpoint { .. } => {
                                            self.context.control_strategy.handle_checkpoint(&envelope, &mut processing_ctx)
                                        }
                                        FlowControlPayload::Drain => {
                                            self.context.control_strategy.handle_drain(&envelope, &mut processing_ctx)
                                        }
                                    };
                                    
                                    // Execute the action
                                    match action {
                                        ControlEventAction::Forward => {
                                            // Forward control events immediately
                                            if !envelope.event.is_eof() {
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
                                                event_type = envelope.event.event_type(),
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
                                                event_type = envelope.event.event_type(),
                                                "Retry requested, buffering event"
                                            );
                                            if envelope.event.is_eof() {
                                                processing_ctx.buffered_eof = Some(envelope.clone());
                                            }
                                        }
                                        ControlEventAction::Skip => {
                                            tracing::warn!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Skipping control event (dangerous!)"
                                            );
                                            // Don't forward, don't process
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // Process normal data event with instrumentation
                                    let handler = self.context.handler.clone();
                                    let event_to_process = envelope.event.clone();
                                    
                                    // Use process_with_instrumentation HOF
                                    let result = process_with_instrumentation(
                                        &self.context.instrumentation,
                                        || async move {
                                            Ok(handler.process(event_to_process))
                                        }
                                    ).await;
                                    
                                    match result {
                                        Ok(transformed_events) => {
                                            // Write transformed events using new journal.write() API
                                            for event in transformed_events {
                                                // Enrich with runtime context
                                                let flow_context = FlowContext {
                                                    flow_name: self.context.flow_name.clone(),
                                                    flow_id: self.context.flow_id.to_string(),
                                                    stage_name: self.context.stage_name.clone(),
                                                    stage_id: self.stage_id.clone(),
                                                    stage_type: obzenflow_core::event::context::StageType::Transform,
                                                };

                                                let enriched_event = event
                                                    .with_flow_context(flow_context)
                                                    .with_runtime_context(self.context.instrumentation.snapshot());
                                                
                                                self.context.data_journal
                                                    .append(enriched_event, Some(&envelope))
                                                    .await
                                                    .map_err(|e| format!("Failed to write transformed event: {}", e))?;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                stage_name = %self.context.stage_name,
                                                error = ?e,
                                                "Transform processing error"
                                            );
                                            // Error already tracked by process_with_instrumentation
                                        }
                                    }
                                }
                                _ => {
                                    // Other content types we don't recognize - forward them
                                    self.forward_control_event(&envelope).await?;
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
                            
                            // Process remaining event with instrumentation
                            if !envelope.event.is_control() {
                                let envelope_event = envelope.event.clone();
                                
                                let transformed_events = process_with_instrumentation(
                                    &self.context.instrumentation,
                                    || async {
                                        Ok(self.context.handler.process(envelope_event))
                                    }
                                ).await?;
                                
                                // Write transformed events using new journal.write() API
                                for event in transformed_events {
                                    let flow_context = FlowContext {
                                        flow_name: self.context.flow_name.clone(),
                                        flow_id: self.context.flow_id.to_string(),
                                        stage_name: self.context.stage_name.clone(),
                                        stage_id: self.stage_id.clone(),
                                        stage_type: obzenflow_core::event::context::StageType::Transform,
                                    };

                                    let enriched_event = event
                                        .with_flow_context(flow_context)
                                        .with_runtime_context(self.context.instrumentation.snapshot());
                                    
                                    self.context.data_journal
                                        .append(enriched_event, Some(&envelope))
                                        .await
                                        .map_err(|e| format!("Failed to write transformed event: {}", e))?;
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
    async fn forward_control_event(&self, envelope: &EventEnvelope<ChainEvent>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let forward_event = envelope.event.clone();
        self.context.data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {}", e))?;
        Ok(())
    }
}