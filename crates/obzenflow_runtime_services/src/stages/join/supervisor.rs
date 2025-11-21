//! Join supervisor implementation using HandlerSupervised pattern

use crate::messaging::UpstreamSubscription;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{EventVariant, FsmBuilder, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState};

/// Supervisor for join stages
pub(crate) struct JoinSupervisor<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<JoinContext<H>>,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,
}

// Implement Sealed directly for JoinSupervisor to satisfy Supervisor trait bound
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for JoinSupervisor<H>
{
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for JoinSupervisor<H>
{
    type State = JoinState<H>;
    type Event = JoinEvent<H>;
    type Context = JoinContext<H>;
    type Action = JoinAction<H>;

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
                        next_state: JoinState::Initialized,
                        actions: vec![JoinAction::AllocateResources],
                    })
                })
                .done()

            // Initialized -> Hydrating
            .when("Initialized")
                .on("Ready", |_state, _event, ctx| async move {
                    // Track state transition in instrumentation
                    ctx.instrumentation.transition_to_state("Hydrating");

                    Ok(Transition {
                        next_state: JoinState::Hydrating,
                        actions: vec![
                            JoinAction::InitializeHandlerState,
                            JoinAction::PublishRunning
                        ],
                    })
                })
                // Idempotent ready: if we get Ready again, stay in Hydrating
                .on("Ready", |_state, _event, ctx| async move {
                    tracing::info!("JoinSupervisor: received redundant Ready in Hydrating; treating as no-op");
                    ctx.instrumentation.transition_to_state("Hydrating");
                    Ok(Transition {
                        next_state: JoinState::Hydrating,
                        actions: vec![],
                    })
                })
                .done()

            // Hydrating -> Enriching (only when reference completes)
            .when("Hydrating")
                // Idempotent Ready in Hydrating: ignore redundant Ready signals
                .on("Ready", |_state, _event, _ctx| async move {
                    tracing::info!("JoinSupervisor: received Ready in Hydrating; treating as no-op");
                    Ok(Transition {
                        next_state: JoinState::Hydrating,
                        actions: vec![],
                    })
                })
                .on("ReceivedEOF", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let JoinEvent::ReceivedEOF = event {
                            // In Hydrating, we only read from reference_subscription in dispatch_state
                            // So ReceivedEOF must be from reference
                            ctx.instrumentation.transition_to_state("Enriching");
                            Ok(Transition {
                                next_state: JoinState::Enriching,
                                actions: vec![], // No actions - dispatch_state handles it
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            Ok(Transition {
                                next_state: JoinState::Failed(msg),
                                actions: vec![JoinAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()

            // Enriching -> Draining (on stream EOF)
            .when("Enriching")
                // Idempotent Ready: ignore redundant Ready signals once hydrating/enriching
                .on("Ready", |_state, _event, _ctx| async move {
                    tracing::info!("JoinSupervisor: received Ready in Enriching; treating as no-op");
                    Ok(Transition {
                        next_state: JoinState::Enriching,
                        actions: vec![],
                    })
                })
                .on("ReceivedEOF", |_state, _event, ctx| async move {
                    // If we received EOF in Enriching, it came from stream_subscription
                    // (we only read from stream_subscription in Enriching state)
                    // Don't check writer_id - subscription context tells us it's from stream
                    ctx.instrumentation.transition_to_state("Draining");
                    Ok(Transition {
                        next_state: JoinState::Draining,
                        actions: vec![], // No actions - dispatch_state handles draining
                    })
                })
                .on("BeginDrain", |_state, _event, ctx| async move {
                    ctx.instrumentation.transition_to_state("Draining");
                    Ok(Transition {
                        next_state: JoinState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            Ok(Transition {
                                next_state: JoinState::Failed(msg),
                                actions: vec![JoinAction::Cleanup],
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
                    ctx.instrumentation.transition_to_state("Drained");

                    Ok(Transition {
                        next_state: JoinState::Drained,
                        actions: vec![
                            JoinAction::ForwardEOF,
                            JoinAction::SendCompletion,
                            JoinAction::Cleanup,
                        ],
                    })
                })
                .on("Error", |_state, event, ctx| {
                    let event = event.clone();
                    async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            Ok(Transition {
                                next_state: JoinState::Failed(msg),
                                actions: vec![JoinAction::Cleanup],
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
                        if let JoinEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, JoinState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: JoinState::Failed(msg),
                                    actions: vec![JoinAction::Cleanup],
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
                        supervisor = "JoinSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    // Return Err to propagate the error
                    Err(format!("Unhandled event '{}' in state '{}' for JoinSupervisor", event_name, state_name))
                }
            })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for JoinSupervisor<H>
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
        tracing::debug!(
            stage_name = %self.context.stage_name,
            state = ?state,
            "Join dispatch_state"
        );
        match state {
            JoinState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            JoinState::Initialized => {
                // Transition to Ready immediately
                Ok(EventLoopDirective::Transition(JoinEvent::Ready))
            }

            JoinState::Hydrating => {
                // Hydrating state ONLY processes reference events
                // Stream events queue in journal/subscription (natural backpressure)

                tracing::trace!(
                    stage_name = %self.context.stage_name,
                    "Hydrating - checking reference subscription"
                );

                let mut ref_subscription_guard = self.context.reference_subscription.write().await;
                if let Some(ref mut subscription) = *ref_subscription_guard {
                    tracing::trace!(
                        stage_name = %self.context.stage_name,
                        "Have reference subscription, attempting to receive"
                    );

                    // Receive from subscription (no timeout - blocks until event arrives, just like transform)
                    match subscription.recv().await {
                        Ok(envelope) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                event_type = envelope.event.event_type(),
                                "Received event from reference"
                            );

                            match &envelope.event.content {
                                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                                    let mut processing_ctx = ProcessingContext::new();

                                    let action = match signal {
                                        FlowControlPayload::Eof { natural, .. } => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                natural = natural,
                                                writer_id = ?envelope.event.writer_id,
                                                "Join received EOF via reference_subscription (reference complete)"
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
                                    };

                                    match action {
                                        ControlEventAction::Forward => {
                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                // Reference EOF -> transition to Enriching
                                                drop(ref_subscription_guard);
                                                return Ok(EventLoopDirective::Transition(
                                                    JoinEvent::ReceivedEOF,
                                                ));
                                            } else {
                                                // Forward non-EOF control events downstream
                                                self.data_journal
                                                    .append(envelope.event.clone(), None)
                                                    .await?;
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                duration = ?duration,
                                                "Join delaying control event during Hydrating"
                                            );
                                            tokio::time::sleep(duration).await;
                                        }
                                        ControlEventAction::Retry | ControlEventAction::Skip => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Join ignoring control event (Retry/Skip not implemented) during Hydrating"
                                            );
                                        }
                                    }
                                    // Continue hydrating after handling control event
                                    return Ok(EventLoopDirective::Continue);
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // Process reference event to build catalog
                                    // NO output during hydration - just building the catalog
                                    tracing::debug!(
                                        stage_name = %self.context.stage_name,
                                        "Processing reference event to build catalog"
                                    );
                                    let handler = (*self.context.handler).clone();
                                    let mut handler_state =
                                        self.context.handler_state.write().await;

                                    // Get writer ID for the handler
                                    let writer_id_guard = self.context.writer_id.read().await;
                                    let writer_id = writer_id_guard
                                        .as_ref()
                                        .ok_or_else(|| "No writer ID available")?;

                                    let events_produced = handler.process_event(
                                        &mut *handler_state,
                                        envelope.event,
                                        self.context.reference_stage_id,
                                        writer_id.clone(),
                                    );

                                    tracing::debug!(
                                        stage_name = %self.context.stage_name,
                                        events_count = events_produced.len(),
                                        "Handler produced events during hydration (should be 0)"
                                    );

                                    // Continue hydrating
                                    return Ok(EventLoopDirective::Continue);
                                }
                                _ => {
                                    tracing::warn!(
                                        stage_name = %self.context.stage_name,
                                        event_type = envelope.event.event_type(),
                                        "Join received unexpected event content type during Hydrating"
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Reference subscription error: {}", e);
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Reference subscription error: {}",
                                e
                            ))));
                        }
                    }
                } else {
                    // No subscription yet, continue
                    tracing::warn!(
                        stage_name = %self.context.stage_name,
                        "No reference subscription available in Hydrating state"
                    );
                    Ok(EventLoopDirective::Continue)
                }
            }

            JoinState::Enriching => {
                // Process stream events (reference is already complete)
                // Following the robust pattern from Transform's Running state

                let mut stream_subscription_guard = self.context.stream_subscription.write().await;
                if let Some(ref mut subscription) = *stream_subscription_guard {
                    match subscription.recv().await {
                        Ok(envelope) => {
                            // Match on event content type like Transform does
                            match &envelope.event.content {
                                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                                    let mut processing_ctx = ProcessingContext::new();

                                    let action = match signal {
                                        FlowControlPayload::Eof { natural, .. } => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                natural = natural,
                                                writer_id = ?envelope.event.writer_id,
                                                "Join received EOF via stream_subscription (stream complete)"
                                            );
                                            // Buffer EOF for downstream forwarding
                                            *self.context.buffered_eof.write().await =
                                                Some(envelope.event.clone());
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
                                    };

                                    match action {
                                        ControlEventAction::Forward => {
                                            // If EOF was buffered above, transition to draining
                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                drop(stream_subscription_guard);
                                                return Ok(EventLoopDirective::Transition(
                                                    JoinEvent::ReceivedEOF,
                                                ));
                                            } else {
                                                // Non-EOF control events are forwarded immediately
                                                self.data_journal
                                                    .append(envelope.event.clone(), None)
                                                    .await?;
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                duration = ?duration,
                                                "Join delaying control event"
                                            );
                                            tokio::time::sleep(duration).await;
                                        }
                                        ControlEventAction::Retry | ControlEventAction::Skip => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Join ignoring control event (Retry/Skip not implemented)"
                                            );
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // Extract source_id from the event's writer_id
                                    let source_id = envelope
                                        .event
                                        .writer_id
                                        .as_stage()
                                        .copied()
                                        .ok_or_else(|| "Event writer is not a stage")?;

                                    // Process data event through handler
                                    let handler = (*self.context.handler).clone();
                                    let mut handler_state =
                                        self.context.handler_state.write().await;

                                    // Get writer ID for the handler
                                    let writer_id_guard = self.context.writer_id.read().await;
                                    let writer_id = writer_id_guard
                                        .as_ref()
                                        .ok_or_else(|| "No writer ID available")?;

                                    let events = handler.process_event(
                                        &mut *handler_state,
                                        envelope.event,
                                        source_id,
                                        writer_id.clone(),
                                    );

                                    // Write output events
                                    for event in events {
                                        self.data_journal.append(event, None).await?;
                                    }

                                    return Ok(EventLoopDirective::Continue);
                                }
                                _ => {
                                    // Other content types - log and continue
                                    tracing::warn!(
                                        stage_name = %self.context.stage_name,
                                        event_type = envelope.event.event_type(),
                                        "Join received unexpected event content type"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Stream subscription error: {}", e);
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Stream subscription error: {}",
                                e
                            ))));
                        }
                    }
                } else {
                    // No subscription - wait briefly
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                Ok(EventLoopDirective::Continue)
            }

            JoinState::Draining => {
                // Call handler drain
                let handler = (*self.context.handler).clone();
                let state = self.context.handler_state.read().await;
                let events = handler.drain(&*state).await?;

                // Write any final events
                for event in events {
                    self.data_journal.append(event, None).await?;
                }

                // Forward any buffered EOF if not already sent
                if let Some(buffered) = self.context.buffered_eof.write().await.take() {
                    self.data_journal.append(buffered, None).await?;
                    tracing::info!(
                        stage_name = %self.context.stage_name,
                        "Join forwarded buffered EOF during draining"
                    );
                }

                Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete))
            }

            JoinState::Drained | JoinState::Failed(_) => Ok(EventLoopDirective::Terminate),

            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

// Background task methods removed - join stage now follows the pattern
// of transform and stateful stages by processing events synchronously
// in dispatch_state instead of spawning background tasks
