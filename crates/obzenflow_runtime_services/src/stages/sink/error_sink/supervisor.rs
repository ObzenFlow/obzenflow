//! Error sink supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use futures::TryFutureExt;
use obzenflow_core::{WriterId, StageId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::ChainEvent;
use obzenflow_core::event::{SystemEvent, EventEnvelope, ChainEventFactory};
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::time::MetricsDuration;
use obzenflow_fsm::{FsmBuilder, Transition};
use crate::stages::common::handlers::ErrorSinkHandler;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;
use crate::metrics::instrumentation::process_with_instrumentation;

use super::fsm::{
    ErrorSinkState, ErrorSinkEvent, ErrorSinkAction,
    ErrorSinkContext,
};

/// Supervisor for error sink stages
pub(crate) struct ErrorSinkSupervisor<H: ErrorSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,
    
    /// The FSM context containing all mutable state
    pub(crate) context: Arc<ErrorSinkContext<H>>,
    
    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
    
    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for ErrorSinkSupervisor to satisfy Supervisor trait bound
impl<H: ErrorSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for ErrorSinkSupervisor<H> {}

impl<H: ErrorSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for ErrorSinkSupervisor<H> {
    type State = ErrorSinkState<H>;
    type Event = ErrorSinkEvent<H>;
    type Context = ErrorSinkContext<H>;
    type Action = ErrorSinkAction<H>;
    
    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>) 
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: ErrorSinkState::Initialized,
                        actions: vec![ErrorSinkAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> Running (sinks auto-start)
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: ErrorSinkState::Running,
                        actions: vec![ErrorSinkAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Flushing (on EOF)
            .when("Running")
                .on("ReceivedEOF", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: ErrorSinkState::Flushing,
                        actions: vec![ErrorSinkAction::FlushBuffers],
                    })
                })
                .on("BeginFlush", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: ErrorSinkState::Flushing,
                        actions: vec![ErrorSinkAction::FlushBuffers],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let ErrorSinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: ErrorSinkState::Failed(msg),
                                actions: vec![ErrorSinkAction::Cleanup],
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
                        next_state: ErrorSinkState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let ErrorSinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: ErrorSinkState::Failed(msg),
                                actions: vec![ErrorSinkAction::Cleanup],
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
                        next_state: ErrorSinkState::Drained,
                        actions: vec![ErrorSinkAction::SendCompletion, ErrorSinkAction::Cleanup],
                    })
                })
                .done()
            
            // Error transitions from any state
            .from_any()
                .on("Error", |state, event, _ctx| {
                    let event = event.clone();
                    let state = state.clone();
                    async move {
                        if let ErrorSinkEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, ErrorSinkState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: ErrorSinkState::Failed(msg),
                                    actions: vec![ErrorSinkAction::Cleanup],
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
impl<H: ErrorSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for ErrorSinkSupervisor<H> {
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
        match state {
            ErrorSinkState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(ErrorSinkEvent::Initialize))
            }
            
            ErrorSinkState::Initialized => {
                // Auto-transition to ready
                Ok(EventLoopDirective::Transition(ErrorSinkEvent::Ready))
            }
            
            ErrorSinkState::Running => {
                // Track event loop
                self.context.instrumentation.event_loops_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                
                // Check subscription for events
                let mut subscription_guard = self.context.subscription.write().await;

                if let Some(subscription) = subscription_guard.as_mut() {
                    match subscription.recv().await {
                        Ok(envelope) => {
                            // Track that we have work
                            self.context.instrumentation.event_loops_with_work_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "Error sink processing event"
                            );
                            
                            // Check for EOF event
                            if envelope.event.is_eof() {
                                tracing::info!(
                                    stage_name = %self.context.stage_name,
                                    "Error sink received EOF"
                                );
                                drop(subscription_guard);
                                return Ok(EventLoopDirective::Transition(ErrorSinkEvent::ReceivedEOF));
                            }
                            
                            // Only process data events with instrumentation (skip control/system events)
                            if !envelope.event.is_control() && !envelope.event.is_system() {
                                let envelope_event = envelope.event.clone();

                                let ack_result = process_with_instrumentation(
                                    &self.context.instrumentation,
                                    || async {
                                        let mut handler = self.context.handler.write().await;
                                        handler.process_error(envelope_event).await
                                    }
                                ).await;

                                match ack_result {
                                    Ok(Some(payload)) => {
                                        let flow_context = FlowContext {
                                            flow_name: self.context.flow_name.clone(),
                                            flow_id: self.context.flow_id.to_string(),
                                            stage_name: self.context.stage_name.clone(),
                                            stage_id: self.stage_id.clone(),
                                            stage_type: obzenflow_core::event::context::StageType::Sink,
                                        };

                                        let delivery_event = ChainEventFactory::delivery_event(
                                            self.writer_id(),
                                            payload,                          // <-- just pass it through
                                        )
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(self.context.instrumentation.snapshot())
                                            .with_causality(CausalityContext::with_parent(envelope.event.id))
                                            .with_correlation_from(&envelope.event);

                                        self.context
                                            .data_journal
                                            .append(delivery_event, Some(&envelope))
                                            .await?;
                                    }
                                    Ok(None) => {
                                        // Event was filtered (duplicate or rate limited)
                                        tracing::debug!(
                                            stage_name = %self.context.stage_name,
                                            "Error event filtered by handler"
                                        );
                                    }
                                    Err(e) => {
                                        let fail_payload = DeliveryPayload::failed(
                                            self.context.stage_name.clone(),                       // destination
                                            DeliveryMethod::Noop,                                  // or HttpPost { url: … } etc.
                                            "sink_error",
                                            e.to_string(),
                                            /* final_attempt */ false,
                                        );

                                        let writer_id = self
                                            .context
                                            .writer_id
                                            .read()
                                            .await
                                            .as_ref()
                                            .expect("writer_id not initialised")
                                            .clone();

                                        let flow_ctx = FlowContext {
                                            flow_name:  self.context.flow_name.clone(),
                                            flow_id:    self.context.flow_id.to_string(),
                                            stage_name: self.context.stage_name.clone(),
                                            stage_id: self.stage_id.clone(),
                                            stage_type: StageType::Sink,
                                        };

                                        let fail_event = ChainEventFactory::delivery_event(writer_id, fail_payload)
                                            .with_flow_context(flow_ctx)
                                            .with_runtime_context(self.context.instrumentation.snapshot())
                                            .with_causality(CausalityContext::with_parent(envelope.event.id))
                                            .with_correlation_from(&envelope.event);

                                        self.context
                                            .data_journal
                                            .append(fail_event, Some(&envelope))
                                            .await
                                            .map_err(|je| format!("Failed to journal error sink failure: {je}"))?;

                                        // propagate the original error up the FSM so the stage can decide
                                        // whether to retry or transition to an error state
                                        return Err(format!("Error sink consume failed: {e}").into());
                                    }
                                }
                            } else {
                                // For control/system events, just consume without instrumentation
                                let envelope_event = envelope.event.clone();
                                let mut handler = self.context.handler.write().await;

                                if let Err(e) = handler.process_error(envelope_event).await {
                                    tracing::error!(
                                        stage_name = %self.context.stage_name,
                                        error = ?e,
                                        "Failed to consume control/system event"
                                    );
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
                                ErrorSinkEvent::Error(format!("Subscription error: {}", e))
                            ));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                Ok(EventLoopDirective::Continue)
            }
            
            ErrorSinkState::Flushing => {
                // Wait for flush to complete
                // The actual flush happens in the action
                Ok(EventLoopDirective::Transition(ErrorSinkEvent::FlushComplete))
            }
            
            ErrorSinkState::Draining => {
                // Move to drained state
                Ok(EventLoopDirective::Transition(ErrorSinkEvent::BeginDrain))
            }
            
            ErrorSinkState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            ErrorSinkState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }
            
            ErrorSinkState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}