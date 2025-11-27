//! Stateful supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::StatefulHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{StatefulAction, StatefulContext, StatefulEvent, StatefulState};

/// Supervisor for stateful stages
pub(crate) struct StatefulSupervisor<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
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
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for StatefulSupervisor<H>
{
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for StatefulSupervisor<H>
{
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   StatefulState<H>;
            event:   StatefulEvent<H>;
            context: StatefulContext<H>;
            action:  StatefulAction<H>;
            initial: initial_state;

            state StatefulState::Created {
                on StatefulEvent::Initialize => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: StatefulState::Initialized,
                            actions: vec![StatefulAction::AllocateResources],
                        })
                    })
                };

                // Fallback error handling for Created (matches original from_any behavior)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Initialized {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Accumulating");
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![StatefulAction::PublishRunning],
                        })
                    })
                };

                // Fallback error handling for Initialized (matches original from_any behavior)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Accumulating {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ShouldEmit => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Emitting");
                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ReceivedEOF => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: StatefulState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Emitting {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::EmitComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Accumulating");
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                // Fallback error handling for Emitting (matches original from_any behavior)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Draining {
                on StatefulEvent::DrainComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
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
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }
            
            // Drained: terminal on success; still handle Error like from_any
            state StatefulState::Drained {
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: StatefulState::Failed(msg),
                                actions: vec![StatefulAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            // Failed: receiving Error again should be idempotent (no extra cleanup)
            state StatefulState::Failed {
                on StatefulEvent::Error => |state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(_msg) = event {
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

            unhandled => |state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();

                Box::pin(async move {
                    tracing::error!(
                        supervisor = "StatefulSupervisor",
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
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for StatefulSupervisor<H>
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
        match state {
            StatefulState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            StatefulState::Initialized => {
                // Auto-transition to ready (stateful stages start immediately)
                Ok(EventLoopDirective::Transition(StatefulEvent::Ready))
            }

            StatefulState::Accumulating => {
                let loop_count = self
                    .context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
                    loop_iteration = loop_count + 1,
                    "stateful: Accumulating state - starting event loop iteration"
                );

                // Process events from subscription
                let mut subscription_guard = self.context.subscription.write().await;
                let mut contract_state_guard = self.context.contract_state.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %self.context.stage_name,
                        loop_iteration = loop_count + 1,
                        "stateful: about to call subscription.poll_next()"
                    );

                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                loop_iteration = loop_count + 1,
                                event_type = %envelope.event.event_type_name(),
                                event_id = ?envelope.event.id,
                                "stateful: poll_next returned Event"
                            );
                            self.context.instrumentation.record_consumed(&envelope);

                            // We have work - increment loops with work
                            self.context
                                .instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
                                            // FLOWIP-080p: Use EofOutcome from UpstreamSubscription as
                                            // the single authority for deciding when this stateful
                                            // stage should begin draining.
                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                // Evaluate EOF outcome for this subscription
                                                let eof_outcome =
                                                    subscription.take_last_eof_outcome();
                                                // Perform a contract check at EOF time for good measure
                                                let _ = subscription
                                                    .check_contracts(
                                                        &mut contract_state_guard[..],
                                                    )
                                                    .await;
                                                // Release subscription lock before further work
                                                drop(subscription_guard);

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
                                                        "Stateful stage evaluated EOF outcome"
                                                    );

                                                    if outcome.is_final {
                                                        // All upstream readers have reached EOF: begin draining.
                                                        *self.context.buffered_eof.write().await =
                                                            Some(envelope.event.clone());
                                                        tracing::info!(
                                                            stage_name = %self.context.stage_name,
                                                            event_type = envelope
                                                                .event
                                                                .event_type(),
                                                            "Stateful stage received final EOF for all upstreams, transitioning to draining"
                                                        );
                                                        return Ok(EventLoopDirective::Transition(
                                                            StatefulEvent::ReceivedEOF,
                                                        ));
                                                    }
                                                }

                                                // Non-final EOF: do not forward, do not drain yet.
                                                return Ok(EventLoopDirective::Continue);
                                            }

                                            // Forward non-EOF control events downstream
                                            self.forward_control_event(&envelope).await?;

                                            // Drain events from pipeline BeginDrain should initiate stage draining
                                            if matches!(signal, FlowControlPayload::Drain) {
                                                *self.context.buffered_eof.write().await =
                                                    Some(envelope.event.clone());
                                                drop(subscription_guard);
                                                tracing::info!(
                                                    stage_name = %self.context.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Stateful stage received drain signal, transitioning to draining"
                                                );
                                                return Ok(EventLoopDirective::Transition(
                                                    StatefulEvent::ReceivedEOF,
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
                                    // ✨ KEY DIFFERENCE: Accumulate without writing to journal
                                    let mut current_state =
                                        self.context.current_state.write().await;

                                    // Clone handler to make it mutable
                                    let mut handler = (*self.context.handler).clone();

                                    // Just accumulate - no instrumentation needed, we're just updating state!
                                    handler.accumulate(&mut *current_state, envelope.event.clone());

                                    // Check if we should emit
                                    if handler.should_emit(&*current_state) {
                                        drop(current_state);
                                        drop(subscription_guard);
                                        // Transition to Emitting state
                                        return Ok(EventLoopDirective::Transition(
                                            StatefulEvent::ShouldEmit,
                                        ));
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
                            if subscription.should_check_contracts(&contract_state_guard[..]) {
                                match subscription
                                    .check_contracts(&mut contract_state_guard[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            "Upstream stalled detected during stateful processing"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Contract violation detected during stateful processing"
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

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                loop_iteration = loop_count + 1,
                                "stateful: poll_next returned NoEvents, sleeping"
                            );
                            drop(subscription_guard);
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                loop_iteration = loop_count + 1,
                                error = ?e,
                                "stateful: poll_next returned Error"
                            );
                            drop(subscription_guard);
                            return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                format!("Subscription error: {}", e),
                            )));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tracing::warn!(
                        target: "flowip-080o",
                        stage_name = %self.context.stage_name,
                        loop_iteration = loop_count + 1,
                        "stateful: No subscription available, sleeping"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                Ok(EventLoopDirective::Continue)
            }

            StatefulState::Emitting => {
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
                    "stateful: Emitting state - about to emit aggregated events"
                );

                // ✨ Emit aggregated events to journal
                let mut current_state = self.context.current_state.write().await;
                let mut handler = (*self.context.handler).clone();

                // Call emit to get the aggregated events
                let events_to_emit = handler.emit(&mut *current_state);

                // Wrap with instrumentation (following transform pattern)
                let emit_result =
                    process_with_instrumentation(&self.context.instrumentation, || async move {
                        Ok(events_to_emit)
                    })
                    .await;

                match emit_result {
                    Ok(events) if !events.is_empty() => {
                        let events_count = events.len();

                        tracing::info!(
                            target: "flowip-080o",
                            stage_name = %self.context.stage_name,
                            events_count = events_count,
                            "stateful: emitting aggregated events to journal"
                        );

                        // Write all aggregated events
                        for event in events {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                event_type = %event.event_type_name(),
                                event_id = ?event.id,
                                "stateful: writing aggregated event to journal"
                            );
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

                            // FLOWIP-080o-part-2: Only count data events for writer_seq.
                            // Lifecycle events (middleware metrics, etc.) are observability
                            // overhead and should not participate in transport contracts.
                            if enriched_event.is_data() {
                                self.context.instrumentation.record_emitted(&enriched_event);
                                // Track output for contract verification
                                if let Some(ref mut sub) = *self.context.subscription.write().await
                                {
                                    sub.track_output_event();
                                }
                            }
                            // Write the aggregated event
                            self.context
                                .data_journal
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
                // First, drain any remaining events from the subscription queue
                // This is critical for contract events (FLOWIP-080o fix)
                let mut subscription_guard = self.context.subscription.write().await;
                let mut contract_state_guard = self.context.contract_state.write().await;
                if let Some(subscription) = subscription_guard.as_mut() {
                    // Poll for remaining events without timeout hacks
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut contract_state_guard[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %self.context.stage_name,
                                event_type = envelope.event.event_type(),
                                "Stateful draining subscription event"
                            );

                            self.context.instrumentation.record_consumed(&envelope);

                            // Process the event based on type
                            if !envelope.event.is_control() {
                                // Accumulate data events during draining
                                let event = envelope.event.clone();
                                let mut handler = (*self.context.handler).clone();
                                let mut state = self.context.current_state.write().await;

                                // Process with instrumentation
                                let _result = process_with_instrumentation(
                                    &self.context.instrumentation,
                                    || async move {
                                        handler.accumulate(&mut state, event);
                                        Ok(())
                                    },
                                )
                                .await;
                            } else {
                                // Forward control events during draining (CRITICAL FIX for FLOWIP-080o)
                                // This ensures contract events (consumption_final, consumption_progress, etc.)
                                // are not lost during the draining phase
                                tracing::debug!(
                                    stage_name = %self.context.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding control event during stateful draining"
                                );

                                // Don't forward EOF again during draining - it will be sent after drain completes
                                if !envelope.event.is_eof() {
                                    self.forward_control_event(&envelope).await?;
                                }
                            }

                            // Continue draining
                            return Ok(EventLoopDirective::Continue);
                        }
                        PollResult::NoEvents => {
                            // Queue is truly drained - no more events available
                            // Do a final contract check before draining
                            let _ = subscription
                                .check_contracts(&mut contract_state_guard[..])
                                .await;

                            tracing::info!(
                                stage_name = %self.context.stage_name,
                                "Stateful subscription queue drained, calling handler.drain()"
                            );
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %self.context.stage_name,
                                error = ?e,
                                "Error during draining"
                            );
                            drop(subscription_guard);
                            return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                format!("Drain error: {}", e),
                            )));
                        }
                    }
                }
                drop(subscription_guard);

                // Now call handler.drain() to emit final accumulated state
                let final_state = self.context.current_state.read().await.clone();
                let handler = (*self.context.handler).clone();

                tracing::info!(
                    stage_name = %self.context.stage_name,
                    "Stateful stage draining final state"
                );

                // Call handler.drain() with instrumentation
                let drain_result =
                    process_with_instrumentation(&self.context.instrumentation, || async move {
                        handler.drain(&final_state).await
                    })
                    .await;

                match drain_result {
                    Ok(drain_events) => {
                        tracing::info!(
                            target: "flowip-080o",
                            stage_name = %self.context.stage_name,
                            drain_events_count = drain_events.len(),
                            "stateful: handler.drain() returned events"
                        );

                        // Write the final aggregated events (if any)
                        for event in drain_events {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                event_type = %event.event_type_name(),
                                event_id = ?event.id,
                                "stateful: writing drain event to journal"
                            );
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

                            // FLOWIP-080o-part-2: Only count data events for writer_seq.
                            // Lifecycle events (middleware metrics, etc.) are observability
                            // overhead and should not participate in transport contracts.
                            if enriched_event.is_data() {
                                self.context.instrumentation.record_emitted(&enriched_event);
                                // Track output for contract verification
                                if let Some(ref mut sub) = *self.context.subscription.write().await
                                {
                                    sub.track_output_event();
                                }
                            }
                            self.context
                                .data_journal
                                .append(enriched_event, None)
                                .await
                                .map_err(|e| {
                                    format!("Failed to write final aggregated event: {}", e)
                                })?;
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
                        Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                            format!("Drain error: {}", e),
                        )))
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
