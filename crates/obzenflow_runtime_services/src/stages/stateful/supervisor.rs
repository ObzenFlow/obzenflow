//! Stateful supervisor implementation using HandlerSupervised pattern

use std::sync::Arc;
use obzenflow_core::{EventEnvelope};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_fsm::{FsmBuilder, Transition};
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use crate::supervised_base::base::Supervisor;
use crate::metrics::instrumentation::process_with_instrumentation;

use super::fsm::{
    StatefulState, StatefulEvent, StatefulAction,
    StatefulContext,
};

/// Supervisor for stateful stages
pub(crate) struct StatefulSupervisor<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<StatefulContext<H>>,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for StatefulSupervisor to satisfy Supervisor trait bound
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for StatefulSupervisor<H> {}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for StatefulSupervisor<H> {
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

    fn configure_fsm(&self, builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>)
        -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Initialized");

                    Ok(Transition {
                        next_state: StatefulState::Initialized,
                        actions: vec![StatefulAction::AllocateResources],
                    })
                })
                .done()

            // Initialized -> Accumulating (stateful stages start immediately)
            .when("Initialized")
                .on("Ready", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Accumulating");

                    Ok(Transition {
                        next_state: StatefulState::Accumulating,
                        actions: vec![StatefulAction::PublishRunning],
                    })
                })
                .done()

            // Accumulating -> Emitting (when should_emit returns true)
            // Accumulating -> Draining (on EOF)
            .when("Accumulating")
                .on("ShouldEmit", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Emitting");

                    Ok(Transition {
                        next_state: StatefulState::Emitting,
                        actions: vec![], // Emission handled in dispatch_state
                    })
                })
                .on("ReceivedEOF", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Draining");

                    Ok(Transition {
                        next_state: StatefulState::Draining,
                        actions: vec![], // Draining handled in dispatch_state
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let StatefulEvent::Error(msg) = event {
                            // Track state transition and failure in instrumentation
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()

            // Emitting -> Accumulating (future: emission strategies)
            .when("Emitting")
                .on("EmitComplete", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Accumulating");

                    Ok(Transition {
                        next_state: StatefulState::Accumulating,
                        actions: vec![],
                    })
                })
                .done()

            // Draining -> Drained
            .when("Draining")
                .on("DrainComplete", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Drained");

                    Ok(Transition {
                        next_state: StatefulState::Drained,
                        actions: vec![
                            StatefulAction::ForwardEOF,
                            StatefulAction::SendCompletion,
                            StatefulAction::Cleanup,
                        ],
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let StatefulEvent::Error(msg) = event {
                            // Track state transition and failure in instrumentation
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
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
                        if let StatefulEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, StatefulState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: StatefulState::Failed(msg),
                                    actions: vec![StatefulAction::Cleanup],
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
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for StatefulSupervisor<H> {
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
        match state {
            StatefulState::Created => {
                // Send initialize event
                Ok(EventLoopDirective::Transition(StatefulEvent::Initialize))
            }

            StatefulState::Initialized => {
                // Auto-transition to ready (stateful stages start immediately)
                Ok(EventLoopDirective::Transition(StatefulEvent::Ready))
            }

            StatefulState::Accumulating => {
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
                                "Stateful stage processing event"
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
                                                "Stateful stage received EOF from upstream"
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
                                                return Ok(EventLoopDirective::Transition(StatefulEvent::ReceivedEOF));
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
                                    // ✨ KEY DIFFERENCE: Accumulate without writing to journal
                                    let mut current_state = self.context.current_state.write().await;

                                    // Clone handler to make it mutable
                                    let mut handler = (*self.context.handler).clone();

                                    // Just accumulate - no instrumentation needed, we're just updating state!
                                    handler.accumulate(&mut *current_state, envelope.event.clone());

                                    // Check if we should emit
                                    if handler.should_emit(&*current_state) {
                                        drop(current_state);
                                        drop(subscription_guard);
                                        // Transition to Emitting state
                                        return Ok(EventLoopDirective::Transition(StatefulEvent::ShouldEmit));
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
                                StatefulEvent::Error(format!("Subscription error: {}", e))
                            ));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                Ok(EventLoopDirective::Continue)
            }

            StatefulState::Emitting => {
                // ✨ Emit aggregated events to journal
                let mut current_state = self.context.current_state.write().await;
                let mut handler = (*self.context.handler).clone();

                // Call emit to get the aggregated events
                let events_to_emit = handler.emit(&mut *current_state);

                // Wrap with instrumentation (following transform pattern)
                let emit_result = process_with_instrumentation(
                    &self.context.instrumentation,
                    || async move {
                        Ok(events_to_emit)
                    }
                ).await;

                match emit_result {
                    Ok(events) if !events.is_empty() => {
                        let events_count = events.len();

                        // Write all aggregated events
                        for event in events {
                            // Enrich with runtime context
                            let flow_context = FlowContext {
                                flow_name: self.context.flow_name.clone(),
                                flow_id: self.context.flow_id.to_string(),
                                stage_name: self.context.stage_name.clone(),
                                stage_id: self.stage_id.clone(),
                                stage_type: obzenflow_core::event::context::StageType::Stateful,
                            };

                            let enriched_event = event
                                .with_flow_context(flow_context)
                                .with_runtime_context(self.context.instrumentation.snapshot());

                            // Write the aggregated event
                            self.context.data_journal
                                .append(enriched_event, None)
                                .await
                                .map_err(|e| format!("Failed to write aggregated event: {}", e))?;
                        }

                        tracing::debug!(
                            stage_name = %self.context.stage_name,
                            events_count = events_count,
                            "Emitted aggregated events"
                        );
                    }
                    Ok(_) => {
                        tracing::debug!(
                            stage_name = %self.context.stage_name,
                            "No events to emit"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %self.context.stage_name,
                            error = ?e,
                            "Failed to emit aggregated event"
                        );
                    }
                }

                // Return to accumulating
                Ok(EventLoopDirective::Transition(StatefulEvent::EmitComplete))
            }

            StatefulState::Draining => {
                // ✨ Call handler.drain() to emit final accumulated state
                let final_state = self.context.current_state.read().await.clone();
                let handler = (*self.context.handler).clone();

                tracing::info!(
                    stage_name = %self.context.stage_name,
                    "Stateful stage draining final state"
                );

                // Call handler.drain() with instrumentation
                let drain_result = process_with_instrumentation(
                    &self.context.instrumentation,
                    || async move {
                        handler.drain(&final_state).await
                    }
                ).await;

                match drain_result {
                    Ok(drain_events) => {
                        // Write the final aggregated events (if any)
                        for event in drain_events {
                            let flow_context = FlowContext {
                                flow_name: self.context.flow_name.clone(),
                                flow_id: self.context.flow_id.to_string(),
                                stage_name: self.context.stage_name.clone(),
                                stage_id: self.stage_id.clone(),
                                stage_type: obzenflow_core::event::context::StageType::Stateful,
                            };

                            let enriched_event = event
                                .with_flow_context(flow_context)
                                .with_runtime_context(self.context.instrumentation.snapshot());

                            self.context.data_journal
                                .append(enriched_event, None)
                                .await
                                .map_err(|e| format!("Failed to write final aggregated event: {}", e))?;
                        }

                        tracing::info!(
                            stage_name = %self.context.stage_name,
                            "Stateful stage drain complete"
                        );
                        Ok(EventLoopDirective::Transition(StatefulEvent::DrainComplete))
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %self.context.stage_name,
                            error = ?e,
                            "Drain error"
                        );
                        Ok(EventLoopDirective::Transition(
                            StatefulEvent::Error(format!("Drain error: {}", e))
                        ))
                    }
                }
            }

            StatefulState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            StatefulState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            StatefulState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulSupervisor<H> {
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
