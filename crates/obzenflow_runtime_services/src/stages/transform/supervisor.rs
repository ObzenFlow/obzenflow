//! Transform supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::TransformHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{EventVariant, FsmBuilder, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{TransformAction, TransformContext, TransformEvent, TransformState};

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<
    H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
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
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for TransformSupervisor<H>
{
}

impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for TransformSupervisor<H>
{
    type State = TransformState<H>;
    type Event = TransformEvent<H>;
    type Context = TransformContext<H>;
    type Action = TransformAction<H>;

    fn configure_fsm(
        &self,
        builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
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
                // Ready can be delivered again (e.g. from wide subscriptions); ignore if already running
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: TransformState::Running,
                        actions: vec![],
                    })
                })
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

            // Catch all unhandled events
            .when_unhandled(|state, event, _ctx| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                async move {
                    tracing::error!(
                        supervisor = "TransformSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    // Return Err to propagate the error
                    Err(format!("Unhandled event '{}' in state '{}' for TransformSupervisor", event_name, state_name))
                }
            })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for TransformSupervisor<H>
{
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
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            TransformState::Initialized => {
                // Auto-transition to ready (transforms start immediately)
                Ok(EventLoopDirective::Transition(TransformEvent::Ready))
            }

            TransformState::Running => {
                let loop_count = self
                    .context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
                    loop_iteration = loop_count + 1,
                    "transform: Running state - starting event loop iteration"
                );

                // Process events from subscription
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %self.context.stage_name,
                        loop_iteration = loop_count + 1,
                        "transform: about to call subscription.poll_next()"
                    );

                    match subscription
                        .poll_next_with_state(state.variant_name())
                        .await
                    {
                        PollResult::Event(envelope) => {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                loop_iteration = loop_count + 1,
                                event_type = %envelope.event.event_type_name(),
                                "transform: poll_next returned Event"
                            );
                            self.context.instrumentation.record_consumed(&envelope);

                            // We have work - increment loops with work
                            self.context
                                .instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: received event from subscription"
                            );

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
                                            self.context
                                                .control_strategy
                                                .handle_eof(&envelope, &mut processing_ctx)
                                        }
                                        FlowControlPayload::Watermark { .. } => self
                                            .context
                                            .control_strategy
                                            .handle_watermark(&envelope, &mut processing_ctx),
                                        FlowControlPayload::Checkpoint { .. } => self
                                            .context
                                            .control_strategy
                                            .handle_checkpoint(&envelope, &mut processing_ctx),
                                        FlowControlPayload::Drain => self
                                            .context
                                            .control_strategy
                                            .handle_drain(&envelope, &mut processing_ctx),
                                        _ => ControlEventAction::Forward,
                                    };

                                    // Execute the action
                                    match action {
                                        ControlEventAction::Forward => {
                                            // Forward control events immediately
                                            // Always forward control events downstream
                                            self.forward_control_event(&envelope).await?;

                                            // EOF or Drain triggers local drain after forwarding
                                            // Drain events from pipeline BeginDrain should initiate stage draining
                                            if envelope.event.is_eof()
                                                || matches!(signal, FlowControlPayload::Drain)
                                            {
                                                *self.context.buffered_eof.write().await =
                                                    Some(envelope.event.clone());
                                                drop(subscription_guard);
                                                tracing::info!(
                                                    stage_name = %self.context.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Transform stage received drain signal, transitioning to draining"
                                                );
                                                return Ok(EventLoopDirective::Transition(
                                                    TransformEvent::ReceivedEOF,
                                                ));
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
                                                processing_ctx.buffered_eof =
                                                    Some(envelope.clone());
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
                                    // Process data event with instrumentation
                                    let handler = self.context.handler.clone();
                                    let event_to_process = envelope.event.clone();

                                    // Use run_if_not_error to handle error events
                                    if matches!(event_to_process.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)) {
                                        tracing::info!(
                                            "Transform supervisor {} received error event {}: {:?}",
                                            self.context.stage_name,
                                            event_to_process.id,
                                            event_to_process.processing_info.status
                                        );
                                    }
                                    let events_to_process = self
                                        .run_if_not_error(event_to_process, |e| handler.process(e));

                                    // Process with instrumentation
                                    let result = process_with_instrumentation(
                                        &self.context.instrumentation,
                                        || async move { Ok(events_to_process) },
                                    )
                                    .await;

                                    match result {
                                        Ok(transformed_events) => {
                                            // Write transformed events
                                            for event in transformed_events {
                                                // Check if this is an error event that was passed through
                                                if matches!(event.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)) {
                                                    tracing::info!(
                                                        stage_name = %self.context.stage_name,
                                                        event_id = %event.id,
                                                        "Writing error event to error journal (FLOWIP-082e)"
                                                    );
                                                    // Only count data events for transport contracts (FLOWIP-080o-part-2)
                                                    // Error events are still data, so count them
                                                    self.context
                                                        .instrumentation
                                                        .record_emitted(&event);
                                                    self.context.error_journal
                                                        .append(event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| format!("Failed to write error event: {}", e))?;

                                                    // Track output for contract verification
                                                    subscription.track_output_event();
                                                } else {
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

                                                    tracing::debug!(
                                                        "Transform {} writing event to data_journal ptr: {:p}",
                                                        self.context.stage_name,
                                                        self.context.data_journal.as_ref() as *const _
                                                    );

                                                    // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                                    // Lifecycle events (middleware metrics, etc.) are observability
                                                    // overhead and should not participate in transport contracts.
                                                    if enriched_event.is_data() {
                                                        self.context
                                                            .instrumentation
                                                            .record_emitted(&enriched_event);
                                                        // Track output for contract verification
                                                        subscription.track_output_event();
                                                    }

                                                    self.context.data_journal
                                                        .append(enriched_event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| format!("Failed to write transformed event: {}", e))?;
                                                }
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
                        PollResult::NoEvents => {
                            // No events available right now
                            // Check contracts if appropriate (FSM decides when)
                            if subscription.should_check_contracts() {
                                match subscription.check_contracts().await {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            "Upstream stalled detected during active processing"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Contract violation detected during active processing"
                                        );
                                    }
                                    Ok(_) => {
                                        // Healthy or ProgressEmitted - no action needed
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            error = %e,
                                            "Failed to check contracts"
                                        );
                                    }
                                }
                            }

                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "No events available, sleeping"
                            );
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                "transform: poll_next returned NoEvents; sleeping briefly"
                            );
                            drop(subscription_guard);
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Subscription error"
                            );
                            drop(subscription_guard);
                            return Ok(EventLoopDirective::Transition(TransformEvent::Error(
                                format!("Subscription error: {}", e),
                            )));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                Ok(EventLoopDirective::Continue)
            }

            TransformState::Draining => {
                // Continue processing remaining events
                let mut subscription_guard = self.context.subscription.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    // Poll for remaining events without timeout hacks
                    match subscription
                        .poll_next_with_state(state.variant_name())
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: draining received event from subscription"
                            );

                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Transform draining events"
                            );

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: draining received event"
                            );

                            self.context.instrumentation.record_consumed(&envelope);

                            // Process remaining event based on type
                            if !envelope.event.is_control() {
                                // Process data events
                                let envelope_event = envelope.event.clone();

                                // Use run_if_not_error to handle error events
                                let events_to_process = self
                                    .run_if_not_error(envelope_event, |e| {
                                        self.context.handler.process(e)
                                    });

                                let transformed_events = process_with_instrumentation(
                                    &self.context.instrumentation,
                                    || async move { Ok(events_to_process) },
                                )
                                .await?;

                                // Write transformed events using new journal.write() API
                                for event in transformed_events {
                                    // Check if this is an error event that was passed through
                                    if matches!(event.processing_info.status, obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)) {
                                        tracing::info!(
                                            stage_name = %self.context.stage_name,
                                            event_id = %event.id,
                                            "Writing error event to error journal during drain (FLOWIP-082e)"
                                        );
                                        // Only count data events for transport contracts (FLOWIP-080o-part-2)
                                        // Error events are still data, so count them
                                        self.context
                                            .instrumentation
                                            .record_emitted(&event);
                                        self.context.error_journal
                                            .append(event, Some(&envelope))
                                            .await
                                            .map_err(|e| format!("Failed to write error event during drain: {}", e))?;

                                        // Track output for contract verification
                                        subscription.track_output_event();
                                    } else {
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

                                        // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                        // Lifecycle events (middleware metrics, etc.) are observability
                                        // overhead and should not participate in transport contracts.
                                        if enriched_event.is_data() {
                                            self.context
                                                .instrumentation
                                                .record_emitted(&enriched_event);
                                            // Track output for contract verification
                                            subscription.track_output_event();
                                        }

                                        self.context.data_journal
                                            .append(enriched_event, Some(&envelope))
                                            .await
                                            .map_err(|e| format!("Failed to write transformed event: {}", e))?;
                                    }
                                }
                            } else {
                                // Forward control events during draining (CRITICAL FIX for FLOWIP-080o)
                                // This ensures contract events (consumption_final, consumption_progress, etc.)
                                // are not lost during the draining phase
                                tracing::debug!(
                                    stage_name = %self.context.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding control event during draining"
                                );

                                // Don't forward EOF again during draining - it will be sent after drain completes
                                if !envelope.event.is_eof() {
                                    self.forward_control_event(&envelope).await?;
                                }
                            }
                            Ok(EventLoopDirective::Continue)
                        }
                        PollResult::NoEvents => {
                            // Queue is truly drained - no more events available
                            // Do a final contract check before transitioning
                            let _ = subscription.check_contracts().await;

                            tracing::info!(
                                stage_name = %self.context.stage_name,
                                "Transform queue drained"
                            );
                            drop(subscription_guard);
                            Ok(EventLoopDirective::Transition(
                                TransformEvent::DrainComplete,
                            ))
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Error during draining"
                            );
                            drop(subscription_guard);
                            Ok(EventLoopDirective::Transition(TransformEvent::Error(
                                format!("Drain error: {}", e),
                            )))
                        }
                    }
                } else {
                    // No subscription, complete immediately
                    Ok(EventLoopDirective::Transition(
                        TransformEvent::DrainComplete,
                    ))
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
    async fn forward_control_event(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let forward_event = envelope.event.clone();
        self.context
            .data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {}", e))?;
        Ok(())
    }
}
