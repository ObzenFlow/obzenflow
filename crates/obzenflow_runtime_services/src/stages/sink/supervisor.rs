//! Sink supervisor implementation using HandlerSupervised pattern

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
use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;
use crate::metrics::instrumentation::process_with_instrumentation;

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
    
    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
    
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
    
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for SinkSupervisor<H> {
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
            SinkState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(SinkEvent::Initialize))
            }
            
            SinkState::Initialized => {
                // Auto-transition to ready
                Ok(EventLoopDirective::Transition(SinkEvent::Ready))
            }
            
            SinkState::Running => {
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
                                "Sink processing event"
                            );
                            
                            // Check for EOF event
                            if envelope.event.is_eof() {
                                tracing::info!(
                                    stage_name = %self.context.stage_name,
                                    "Sink received EOF"
                                );
                                drop(subscription_guard);
                                return Ok(EventLoopDirective::Transition(SinkEvent::ReceivedEOF));
                            }
                            
                            // Only process data events with instrumentation (skip control/system events)
                            if !envelope.event.is_control() && !envelope.event.is_system() {
                                let envelope_event = envelope.event.clone();

                                let ack_result = process_with_instrumentation(
                                    &self.context.instrumentation,
                                    || async {
                                        let mut handler = self.context.handler.write().await;
                                        handler.consume(envelope_event).await   // ← returns Result<DeliveryPayload, Box<…>>
                                    }
                                ).await;

                                match ack_result {
                                    Ok(payload) => {
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
                                            .map_err(|je| format!("Failed to journal sink failure: {je}"))?;

                                        // propagate the original error up the FSM so the stage can decide
                                        // whether to retry or transition to an error state
                                        return Err(format!("Sink consume failed: {e}").into());
                                    }
                                }
                            } else {
                                // For control/system events, just consume without instrumentation
                                let envelope_event = envelope.event.clone();
                                let mut handler = self.context.handler.write().await;

                                if let Err(e) = handler.consume(envelope_event).await {   // ← add .await
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