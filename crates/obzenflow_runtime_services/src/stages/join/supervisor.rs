//! Join supervisor implementation using HandlerSupervised pattern

use crate::messaging::upstream_subscription::ContractConfig;
use crate::messaging::{PollResult, UpstreamSubscription};
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
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

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JoinState<H>;
            event:   JoinEvent<H>;
            context: JoinContext<H>;
            action:  JoinAction<H>;
            initial: initial_state;

            state JoinState::Created {
                on JoinEvent::Initialize => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: JoinState::Initialized,
                            actions: vec![JoinAction::AllocateResources],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Hydrating");
                        Ok(Transition {
                            next_state: JoinState::Hydrating,
                            actions: vec![
                                JoinAction::InitializeHandlerState,
                                JoinAction::PublishRunning,
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Hydrating {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Hydrating; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Hydrating,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::ReceivedEOF = event {
                            ctx.instrumentation.transition_to_state("Enriching");
                            Ok(Transition {
                                next_state: JoinState::Enriching,
                                actions: vec![],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Enriching {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Enriching; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Enriching,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::BeginDrain => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Draining {
                on JoinEvent::DrainComplete => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
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
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Drained {
                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
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
                    })
                };
            }

            state JoinState::Failed {
                on JoinEvent::Error => |state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(_msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            Ok(Transition {
                                next_state: state,
                                actions: vec![],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            unhandled => |state: &JoinState<H>, event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "JoinSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(obzenflow_fsm::FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
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
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_name = %self.context.stage_name,
                journal_error = %e,
                "Failed to write completion event; continuing without system journal entry"
            );
        }
        Ok(())
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
                let mut ref_contract_state_guard =
                    self.context.reference_contract_state.write().await;
                if let Some(ref mut subscription) = *ref_subscription_guard {
                    tracing::trace!(
                        stage_name = %self.context.stage_name,
                        "Have reference subscription, attempting to receive"
                    );

                    // Receive from subscription
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut ref_contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
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
                                        _ => ControlEventAction::Forward,
                                    };

                                    match action {
                                        ControlEventAction::Forward => {
                                            // Always forward control events downstream
                                            self.data_journal
                                                .append(envelope.event.clone(), None)
                                                .await?;

                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                // FLOWIP-080p: use EofOutcome to decide when the
                                                // reference side is truly complete.
                                                let eof_outcome =
                                                    subscription.take_last_eof_outcome();
                                                if let Some(outcome) = eof_outcome {
                                                    tracing::info!(
                                                        target: "flowip-080o",
                                                        stage_name = %self.context.stage_name,
                                                        upstream_stage_id = ?outcome.stage_id,
                                                        upstream_stage_name = %outcome.stage_name,
                                                        reader_index = outcome.reader_index,
                                                        eof_count = outcome.eof_count,
                                                        total_readers = outcome.total_readers,
                                                        is_final = outcome.is_final,
                                                        "Join (Hydrating) evaluated EOF outcome for reference side"
                                                    );

                                                    if outcome.is_final {
                                                        // Reference EOF is final -> transition to Enriching
                                                        drop(ref_subscription_guard);
                                                        return Ok(EventLoopDirective::Transition(
                                                            JoinEvent::ReceivedEOF,
                                                        ));
                                                    }
                                                }

                                                // Non-final EOF: continue hydrating.
                                                return Ok(EventLoopDirective::Continue);
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
                        PollResult::NoEvents => {
                            // Check contracts if appropriate
                            if subscription
                                .should_check_contracts(&ref_contract_state_guard[..])
                            {
                                match subscription
                                    .check_contracts(&mut ref_contract_state_guard[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            "Reference upstream stalled during join loading"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Reference contract violation during join loading"
                                        );
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            error = %e,
                                            "Failed to check reference contracts"
                                        );
                                    }
                                }
                            }

                            // No events available right now, sleep briefly and continue
                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "No reference events available, sleeping"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            return Ok(EventLoopDirective::Continue);
                        }
                        PollResult::Error(e) => {
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

                let mut stream_subscription_guard =
                    self.context.stream_subscription.write().await;
                let mut stream_contract_state_guard =
                    self.context.stream_contract_state.write().await;
                if let Some(ref mut subscription) = *stream_subscription_guard {
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut stream_contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
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
                                        _ => ControlEventAction::Forward,
                                    };

                                    match action {
                                        ControlEventAction::Forward => {
                                            // Always forward control events downstream
                                            self.data_journal
                                                .append(envelope.event.clone(), None)
                                                .await?;

                                            // Use EofOutcome from the stream subscription to decide
                                            // when the join can move to Draining.
                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                let eof_outcome =
                                                    subscription.take_last_eof_outcome();
                                                if let Some(outcome) = eof_outcome {
                                                    tracing::info!(
                                                        target: "flowip-080o",
                                                        stage_name = %self.context.stage_name,
                                                        upstream_stage_id = ?outcome.stage_id,
                                                        upstream_stage_name = %outcome.stage_name,
                                                        reader_index = outcome.reader_index,
                                                        eof_count = outcome.eof_count,
                                                        total_readers = outcome.total_readers,
                                                        is_final = outcome.is_final,
                                                        "Join (Enriching) evaluated EOF outcome for stream side"
                                                    );

                                                    if outcome.is_final {
                                                        drop(stream_subscription_guard);
                                                        return Ok(
                                                            EventLoopDirective::Transition(
                                                                JoinEvent::ReceivedEOF,
                                                            ),
                                                        );
                                                    }
                                                }

                                                // Non-final EOF: keep enriching.
                                                return Ok(EventLoopDirective::Continue);
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
                                        // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                        // Lifecycle events (middleware metrics, etc.) are observability
                                        // overhead and should not participate in transport contracts.
                                        if event.is_data() {
                                            self.context.instrumentation.record_emitted(&event);
                                            // Track output for contract verification
                                            subscription.track_output_event();
                                        }
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
                        PollResult::NoEvents => {
                            // Check contracts if appropriate
                            if subscription
                                .should_check_contracts(&stream_contract_state_guard[..])
                            {
                                match subscription
                                    .check_contracts(&mut stream_contract_state_guard[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            "Stream upstream stalled during join enriching"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Stream contract violation during join enriching"
                                        );
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            error = %e,
                                            "Failed to check stream contracts"
                                        );
                                    }
                                }
                            }

                            // No events available right now, sleep briefly and continue
                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "No stream events available, sleeping"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            return Ok(EventLoopDirective::Continue);
                        }
                        PollResult::Error(e) => {
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
                // First, drain any remaining events from both subscriptions
                // This is critical for contract events (FLOWIP-080o fix)

                // Drain reference subscription
                let mut ref_subscription_guard = self.context.reference_subscription.write().await;
                let mut ref_contract_state_guard =
                    self.context.reference_contract_state.write().await;
                if let Some(subscription) = ref_subscription_guard.as_mut() {
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut ref_contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                event_type = envelope.event.event_type(),
                                "Join draining reference subscription event"
                            );

                            // Process based on event type
                            if !envelope.event.is_control() {
                                // Process data event through join handler
                                let handler = (*self.context.handler).clone();
                                let mut state = self.context.handler_state.write().await;

                                // Get writer ID for the handler
                                let writer_id_guard = self.context.writer_id.read().await;
                                let writer_id = writer_id_guard
                                    .as_ref()
                                    .ok_or_else(|| "No writer ID available")?;

                                let results = handler.process_event(
                                    &mut *state,
                                    envelope.event.clone(),
                                    self.context.reference_stage_id,
                                    writer_id.clone(),
                                );

                                for event in results {
                                    // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                    // Lifecycle events (middleware metrics, etc.) are observability
                                    // overhead and should not participate in transport contracts.
                                    if event.is_data() {
                                        self.context.instrumentation.record_emitted(&event);
                                        // Track output for contract verification
                                        if let Some(ref mut sub) =
                                            *self.context.reference_subscription.write().await
                                        {
                                            sub.track_output_event();
                                        }
                                    }
                                    self.data_journal.append(event, None).await?;
                                }
                            } else if !envelope.event.is_eof() {
                                // Forward non-EOF control events (CRITICAL FIX for FLOWIP-080o)
                                tracing::debug!(
                                    stage_name = %self.context.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding reference control event during join draining"
                                );
                                self.data_journal
                                    .append(envelope.event.clone(), None)
                                    .await?;
                            }

                            // Continue draining
                            return Ok(EventLoopDirective::Continue);
                        }
                        PollResult::NoEvents => {
                            // Do a final contract check before marking as drained
                            let _ = subscription
                                .check_contracts(&mut ref_contract_state_guard[..])
                                .await;

                            // Reference queue empty
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Join reference subscription queue drained"
                            );
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Error draining reference subscription"
                            );
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Reference drain error: {}",
                                e
                            ))));
                        }
                    }
                }
                drop(ref_subscription_guard);

                // Drain stream subscription
                let mut stream_subscription_guard =
                    self.context.stream_subscription.write().await;
                let mut stream_contract_state_guard =
                    self.context.stream_contract_state.write().await;
                if let Some(subscription) = stream_subscription_guard.as_mut() {
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut stream_contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                event_type = envelope.event.event_type(),
                                "Join draining stream subscription event"
                            );

                            // Process based on event type
                            if !envelope.event.is_control() {
                                // Process data event through join handler
                                let handler = (*self.context.handler).clone();
                                let mut state = self.context.handler_state.write().await;

                                // Get writer ID for the handler
                                let writer_id_guard = self.context.writer_id.read().await;
                                let writer_id = writer_id_guard
                                    .as_ref()
                                    .ok_or_else(|| "No writer ID available")?;

                                // Use the bound factory metadata to identify the first stream upstream
                                let source_stage_id = self
                                    .context
                                    .stream_subscription_factory
                                    .upstream_stage_ids()
                                    .first()
                                    .copied()
                                    .unwrap_or(self.context.reference_stage_id);

                                let results = handler.process_event(
                                    &mut *state,
                                    envelope.event.clone(),
                                    source_stage_id,
                                    writer_id.clone(),
                                );

                                for event in results {
                                    // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                    // Lifecycle events (middleware metrics, etc.) are observability
                                    // overhead and should not participate in transport contracts.
                                    if event.is_data() {
                                        self.context.instrumentation.record_emitted(&event);
                                        // Track output for contract verification
                                        subscription.track_output_event();
                                    }
                                    self.data_journal.append(event, None).await?;
                                }
                            } else if !envelope.event.is_eof() {
                                // Forward non-EOF control events (CRITICAL FIX for FLOWIP-080o)
                                tracing::debug!(
                                    stage_name = %self.context.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding stream control event during join draining"
                                );
                                self.data_journal
                                    .append(envelope.event.clone(), None)
                                    .await?;
                            }

                            // Continue draining
                            return Ok(EventLoopDirective::Continue);
                        }
                        PollResult::NoEvents => {
                            // Do a final contract check before marking as drained
                            let _ = subscription
                                .check_contracts(&mut stream_contract_state_guard[..])
                                .await;

                            // Stream queue empty
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                "Join stream subscription queue drained"
                            );
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Error draining stream subscription"
                            );
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Stream drain error: {}",
                                e
                            ))));
                        }
                    }
                }
                drop(stream_subscription_guard);

                // Now call handler.drain() to emit any final joined state
                tracing::info!(
                    stage_name = %self.context.stage_name,
                    "Join subscriptions drained, calling handler.drain()"
                );

                let handler = (*self.context.handler).clone();
                let state = self.context.handler_state.read().await;
                let events = handler.drain(&*state).await?;

                // Write any final events
                for event in events {
                    // FLOWIP-080o-part-2: Only count data events for writer_seq.
                    // Lifecycle events (middleware metrics, etc.) are observability
                    // overhead and should not participate in transport contracts.
                    if event.is_data() {
                        self.context.instrumentation.record_emitted(&event);
                        // Track output for contract verification - need to check which subscription
                        // Since this is during drain, we need to track on both subscriptions
                        if let Some(ref mut sub) =
                            *self.context.reference_subscription.write().await
                        {
                            sub.track_output_event();
                        }
                        if let Some(ref mut sub) = *self.context.stream_subscription.write().await {
                            sub.track_output_event();
                        }
                    }
                    self.data_journal.append(event, None).await?;
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
