//! Stateful supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::{
    heartbeat_interval, process_with_instrumentation_no_count,
};
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::StatefulHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use super::fsm::{StatefulAction, StatefulContext, StatefulEvent, StatefulState};

/// Supervisor for stateful stages
pub(crate) struct StatefulSupervisor<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
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

    fn event_for_action_error(&self, msg: String) -> StatefulEvent<H> {
        StatefulEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_id = %self.stage_id,
                journal_error = %e,
                "Failed to write completion event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
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
                let loop_count = ctx
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "stateful: Accumulating state - starting event loop iteration"
                );

                // Take ownership of subscription and contract state so we never hold
                // a borrow of the context across await while operating on them.
                let mut maybe_subscription = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);

                if let Some(ref mut subscription) = maybe_subscription {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        loop_iteration = loop_count + 1,
                        "stateful: about to call subscription.poll_next()"
                    );

                    let poll_result = subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await;

                    match poll_result {
                        PollResult::Event(envelope) => {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                event_type = %envelope.event.event_type_name(),
                                event_id = ?envelope.event.id,
                                "stateful: poll_next returned Event"
                            );
                            ctx.instrumentation.record_consumed(&envelope);

                            // We have work - increment loops with work
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tracing::debug!(
                                stage_name = %ctx.stage_name,
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
                                                stage_name = %ctx.stage_name,
                                                "Stateful stage received EOF from upstream"
                                            );
                                            ctx.control_strategy
                                                .handle_eof(&envelope, &mut processing_ctx)
                                        }
                                        FlowControlPayload::Watermark { .. } => ctx
                                            .control_strategy
                                            .handle_watermark(&envelope, &mut processing_ctx),
                                        FlowControlPayload::Checkpoint { .. } => ctx
                                            .control_strategy
                                            .handle_checkpoint(&envelope, &mut processing_ctx),
                                        FlowControlPayload::Drain => ctx
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
                                                    .check_contracts(&mut contract_state[..])
                                                    .await;

                                                if let Some(outcome) = eof_outcome {
                                                    tracing::info!(
                                                    target: "flowip-080o",
                                                    stage_name = %ctx.stage_name,
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
                                                        ctx.buffered_eof =
                                                            Some(envelope.event.clone());
                                                        tracing::info!(
                                                            stage_name = %ctx.stage_name,
                                                            event_type = envelope
                                                                .event
                                                                .event_type(),
                                                            "Stateful stage received final EOF for all upstreams, transitioning to draining"
                                                        );
                                                        directive =
                                                            Ok(EventLoopDirective::Transition(
                                                                StatefulEvent::ReceivedEOF,
                                                            ));
                                                    } else {
                                                        // Non-final EOF: do not forward, do not drain yet.
                                                        directive =
                                                            Ok(EventLoopDirective::Continue);
                                                    }
                                                } else {
                                                    // No outcome yet (unexpected), remain in Accumulating.
                                                    directive = Ok(EventLoopDirective::Continue);
                                                }
                                            }

                                            // Forward non-EOF control events downstream
                                            self.forward_control_event(ctx, &envelope).await?;

                                            // Drain events from pipeline BeginDrain should initiate stage draining
                                            if matches!(signal, FlowControlPayload::Drain) {
                                                ctx.buffered_eof = Some(envelope.event.clone());
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Stateful stage received drain signal, transitioning to draining"
                                                );
                                                directive = Ok(EventLoopDirective::Transition(
                                                    StatefulEvent::ReceivedEOF,
                                                ));
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                event_type = envelope.event.event_type(),
                                                duration = ?duration,
                                                "Delaying control event"
                                            );
                                            tokio::time::sleep(duration).await;
                                            // Return Continue to re-process after delay
                                            directive = Ok(EventLoopDirective::Continue);
                                        }
                                        ControlEventAction::Retry => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
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
                                                stage_name = %ctx.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Skipping control event (dangerous!)"
                                            );
                                            // Don't forward, don't process
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // ✨ KEY DIFFERENCE: Accumulate without writing to journal
                                    let mut handler = (*ctx.handler).clone();
                                    let event = envelope.event.clone();

                                    // Accumulate into state without emitting domain events yet,
                                    // but still record per-event processing time + counts.
                                    ctx.instrumentation
                                        .in_flight_count
                                        .fetch_add(1, Ordering::Relaxed);
                                    let start = Instant::now();
                                    handler.accumulate(&mut ctx.current_state, event);
                                    let duration = start.elapsed();
                                    ctx.instrumentation
                                        .in_flight_count
                                        .fetch_sub(1, Ordering::Relaxed);

                                    ctx.instrumentation.record_processing_time(duration);
                                    if ctx.instrumentation.check_anomaly(duration) {
                                        ctx.instrumentation
                                            .anomalies_total
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    ctx.instrumentation
                                        .events_processed_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    ctx.instrumentation
                                        .events_accumulated_total
                                        .fetch_add(1, Ordering::Relaxed);

                                    // Check if we should emit based on updated state
                                    let should_emit = handler.should_emit(&ctx.current_state);

                                    // Track accumulated events for observability heartbeats.
                                    ctx.events_since_last_heartbeat =
                                        ctx.events_since_last_heartbeat.saturating_add(1);
                                    if let Err(e) =
                                        self.emit_stateful_heartbeat_if_due(ctx, false).await
                                    {
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            error = ?e,
                                            "Failed to emit stateful accumulator heartbeat"
                                        );
                                    }

                                    // Check if we should emit
                                    if should_emit {
                                        // Transition to Emitting state
                                        directive = Ok(EventLoopDirective::Transition(
                                            StatefulEvent::ShouldEmit,
                                        ));
                                    }
                                }
                                _ => {
                                    // Other content types we don't recognize - forward them
                                    self.forward_control_event(ctx, &envelope).await?;
                                }
                            }
                        }
                        PollResult::NoEvents => {
                            // No events available right now
                            // Check contracts if appropriate (FSM decides when)
                            if subscription.should_check_contracts(&contract_state[..]) {
                                match subscription
                                    .check_contracts(&mut contract_state[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            "Upstream stalled detected during stateful processing"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
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
                                            stage_name = %ctx.stage_name,
                                            error = %e,
                                            "Failed to check contracts"
                                        );
                                    }
                                }
                            }

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                "stateful: poll_next returned NoEvents, sleeping"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                error = ?e,
                                "stateful: poll_next returned Error"
                            );
                            directive = Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                format!("Subscription error: {}", e),
                            )));
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tracing::warn!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        loop_iteration = loop_count + 1,
                        "stateful: No subscription available, sleeping"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                // Restore contract state and subscription before returning
                ctx.contract_state = contract_state;
                ctx.subscription = maybe_subscription;

                directive
            }

            StatefulState::Emitting => {
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "stateful: Emitting state - about to emit aggregated events"
                );

                // ✨ Emit aggregated events to journal.
                //
                // At this point the handler is operating purely over state; there
                // is no single "current input event" to attribute failures to.
                // An `Err(HandlerError)` here is therefore treated as
                // stage‑fatal and surfaced via the FSM, while the common
                // per‑record failure path remains `accumulate` marking events
                // with `mark_as_error` when appropriate.
                let mut current_state = &mut ctx.current_state;
                let mut handler = (*ctx.handler).clone();
                let instrumentation = ctx.instrumentation.clone();

                let emit_result =
                    process_with_instrumentation_no_count(&ctx.instrumentation, || async move {
                        handler.emit(&mut *current_state).map_err(
                            |err| -> Box<dyn std::error::Error + Send + Sync> {
                                // Stage-fatal handler error in emit: record it in error metrics
                                // before type erasure.
                                instrumentation.record_error(err.kind());
                                err.into()
                            },
                        )
                    })
                    .await;

                match emit_result {
                    Ok(events) if !events.is_empty() => {
                        let events_count = events.len();

                        tracing::info!(
                            target: "flowip-080o",
                            stage_name = %ctx.stage_name,
                            events_count = events_count,
                            "stateful: emitting aggregated events to journal"
                        );

                        // Write all aggregated events
                        for event in events {
                            use obzenflow_core::event::JournalEvent;

                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id.clone(),
                                stage_type: obzenflow_core::event::context::StageType::Stateful,
                            };

                            let enriched_event = event
                                .with_flow_context(flow_context)
                                .with_runtime_context(
                                    ctx.instrumentation
                                        .snapshot_with_control(),
                                );

                            // FLOWIP-080o-part-2: Only count data events for writer_seq.
                            // Lifecycle events (middleware metrics, etc.) are observability
                            // overhead and should not participate in transport contracts.
                            if enriched_event.is_data() {
                                ctx.instrumentation
                                    .record_output_event(&enriched_event);
                                if let Some(ref mut sub) = ctx.subscription {
                                    sub.track_output_event();
                                }
                            }

                            ctx.data_journal
                                .append(enriched_event, None)
                                .await
                                .map_err(|e| format!("Failed to write aggregated event: {}", e))?;
                        }

                        tracing::debug!(
                            stage_name = %ctx.stage_name,
                            events_count = events_count,
                            "Emitted aggregated events"
                        );
                    }
                    Ok(_) => {
                        tracing::debug!(
                            stage_name = %ctx.stage_name,
                            "No events to emit"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = ?e,
                            "Failed to emit aggregated event, transitioning to Failed"
                        );
                        return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                            format!("Emit error: {}", e),
                        )));
                    }
                }

                Ok(EventLoopDirective::Transition(StatefulEvent::EmitComplete))
            }

            StatefulState::Draining => {
                // First, drain any remaining events from the subscription queue
                // This is critical for contract events (FLOWIP-080o fix)
                let mut maybe_subscription = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);
                let mut should_drain = false;

                if let Some(ref mut subscription) = maybe_subscription {
                    // Poll for remaining events without timeout hacks
                    match subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await
                    {
                        PollResult::Event(envelope) => {
                            ctx.instrumentation.record_consumed(&envelope);

                            // Process the event based on type
                            if !envelope.event.is_control() {
                                // Accumulate data events during draining, synchronously
                                let event = envelope.event.clone();
                                let mut handler = (*ctx.handler).clone();

                                ctx.instrumentation
                                    .in_flight_count
                                    .fetch_add(1, Ordering::Relaxed);
                                let start = Instant::now();
                                handler.accumulate(&mut ctx.current_state, event);
                                let duration = start.elapsed();
                                ctx.instrumentation
                                    .in_flight_count
                                    .fetch_sub(1, Ordering::Relaxed);

                                ctx.instrumentation.record_processing_time(duration);
                                if ctx.instrumentation.check_anomaly(duration) {
                                    ctx.instrumentation
                                        .anomalies_total
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                ctx.instrumentation
                                    .events_processed_total
                                    .fetch_add(1, Ordering::Relaxed);
                                ctx.instrumentation
                                    .events_accumulated_total
                                    .fetch_add(1, Ordering::Relaxed);

                                // Track accumulated events during drain for heartbeat visibility.
                                ctx.events_since_last_heartbeat =
                                    ctx.events_since_last_heartbeat.saturating_add(1);
                                if let Err(e) =
                                    self.emit_stateful_heartbeat_if_due(ctx, false).await
                                {
                                    tracing::warn!(
                                        stage_name = %ctx.stage_name,
                                        error = ?e,
                                        "Failed to emit stateful accumulator heartbeat during draining"
                                    );
                                }

                                // After accumulation, mirror Accumulating semantics:
                                // if the handler says we should emit, do so inline.
                                if handler.should_emit(&ctx.current_state) {
                                    match handler.emit(&mut ctx.current_state) {
                                        Ok(events_to_emit) => {
                                            if !events_to_emit.is_empty() {
                                                let events_count = events_to_emit.len();
                                                tracing::info!(
                                                    target: "flowip-080o",
                                                    stage_name = %ctx.stage_name,
                                                    events_count = events_count,
                                                    "stateful: emitting aggregated events to journal during draining"
                                                );

                                                for event in events_to_emit {
                                                    use obzenflow_core::event::JournalEvent;

                                                    let flow_context = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id.clone(),
                                                        stage_type: obzenflow_core::event::context::StageType::Stateful,
                                                    };

                                                    let enriched_event = event
                                                        .with_flow_context(flow_context)
                                                        .with_runtime_context(
                                                            ctx.instrumentation
                                                                .snapshot_with_control(),
                                                        );

                                                    if enriched_event.is_data() {
                                                        ctx.instrumentation
                                                            .record_output_event(&enriched_event);
                                                        // Track output for contract verification
                                                        subscription.track_output_event();
                                                    }

                                                    ctx.data_journal
                                                        .append(enriched_event, None)
                                                        .await
                                                        .map_err(|e| {
                                                            format!(
                                                                "Failed to write aggregated event during draining: {}",
                                                                e
                                                            )
                                                        })?;
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            use obzenflow_core::event::status::processing_status::{
                                                ErrorKind, ProcessingStatus,
                                            };

                                            tracing::error!(
                                                stage_name = %ctx.stage_name,
                                                error = ?err,
                                                "Failed to emit aggregated events during draining; mapping to error-marked event"
                                            );

                                            // Per-record handler failure during draining: turn the
                                            // input into an error-marked event and route it using
                                            // the ErrorKind policy instead of failing the stage.
                                            let reason = format!(
                                                "Stateful handler emit error during drain: {:?}",
                                                err
                                            );
                                            let mut error_event = envelope
                                                .event
                                                .clone()
                                                .mark_as_error(reason, err.kind());

                                            // Count all error-marked events for lifecycle / flow rollups,
                                            // even when they are not stage-fatal.
                                            ctx.instrumentation.record_error(err.kind());

                                            let route_to_error_journal = match &error_event
                                                .processing_info
                                                .status
                                            {
                                                ProcessingStatus::Error { kind, .. } => {
                                                    match kind {
                                                        Some(ErrorKind::Timeout)
                                                        | Some(ErrorKind::Remote)
                                                        | Some(ErrorKind::Deserialization) => true,
                                                        Some(ErrorKind::Validation)
                                                        | Some(ErrorKind::Domain) => false,
                                                        None | Some(ErrorKind::Unknown) => true,
                                                    }
                                                }
                                                _ => false,
                                            };

                                            if route_to_error_journal {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_id = %error_event.id,
                                                    "Writing stateful drain error event to error journal (FLOWIP-082h)"
                                                );

                                                // Error events are still data, so record them for
                                                // transport contracts and metrics.
                                                if error_event.is_data() {
                                                    ctx.instrumentation
                                                        .record_output_event(&error_event);
                                                    subscription.track_output_event();
                                                }

                                                ctx.error_journal
                                                    .append(error_event, Some(&envelope))
                                                    .await
                                                    .map_err(|e| {
                                                        format!(
                                                            "Failed to write stateful drain error event: {}",
                                                            e
                                                        )
                                                    })?;
                                            } else {
                                                use obzenflow_core::event::JournalEvent;

                                                let flow_context = FlowContext {
                                                    flow_name: ctx.flow_name.clone(),
                                                    flow_id: ctx.flow_id.to_string(),
                                                    stage_name: ctx.stage_name.clone(),
                                                    stage_id: self.stage_id.clone(),
                                                    stage_type: obzenflow_core::event::context::StageType::Stateful,
                                                };

                                                let enriched_error = error_event
                                                    .with_flow_context(flow_context)
                                                    .with_runtime_context(
                                                        ctx.instrumentation
                                                            .snapshot_with_control(),
                                                    );

                                                if enriched_error.is_data() {
                                                    ctx.instrumentation
                                                        .record_output_event(&enriched_error);
                                                    subscription.track_output_event();
                                                }

                                                ctx.data_journal
                                                    .append(enriched_error, Some(&envelope))
                                                    .await
                                                    .map_err(|e| {
                                                        format!(
                                                            "Failed to write stateful drain error event to data journal: {}",
                                                            e
                                                        )
                                                    })?;
                                            }

                                            // Keep draining; do not transition to Failed on
                                            // per-record handler errors.
                                            directive = Ok(EventLoopDirective::Continue);
                                            should_drain = false;
                                        }
                                    }
                                }
                            } else {
                                // Forward control events during draining (CRITICAL FIX for FLOWIP-080o)
                                // This ensures contract events (consumption_final, consumption_progress, etc.)
                                // are not lost during the draining phase
                                tracing::debug!(
                                    stage_name = %ctx.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding control event during stateful draining"
                                );

                                // Don't forward EOF again during draining - it will be sent after drain completes
                                if !envelope.event.is_eof() {
                                    self.forward_control_event(ctx, &envelope).await?;
                                }
                            }

                            // Continue draining on next loop iteration
                            should_drain = false;
                            directive = Ok(EventLoopDirective::Continue);
                        }
                        PollResult::NoEvents => {
                            // Queue is truly drained - no more events available
                            // Do a final contract check before draining
                            let _ = subscription.check_contracts(&mut contract_state[..]).await;

                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                "Stateful subscription queue drained, calling handler.drain()"
                            );
                            should_drain = true;
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Error during draining"
                            );
                            directive = Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                format!("Drain error: {}", e),
                            )));
                            should_drain = false;
                        }
                    }
                } else {
                    // No subscription: treat as already drained, proceed to handler.drain()
                    should_drain = true;
                }

                // Restore contract state and subscription before potential drain
                ctx.contract_state = contract_state;
                ctx.subscription = maybe_subscription;

                if should_drain {
                    // Flush any remaining accumulated events into a final heartbeat snapshot
                    // before emitting drain results. For the final heartbeat we bypass the normal
                    // heartbeat interval threshold so short finite flows still emit a snapshot.
                    if ctx.events_since_last_heartbeat > 0 {
                        if let Err(e) = self.emit_stateful_heartbeat_if_due(ctx, true).await {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Failed to emit final stateful accumulator heartbeat before drain"
                            );
                        }
                    }

                    // Now call handler.drain() to emit final accumulated state
                    let final_state = ctx.current_state.clone();
                    let handler = (*ctx.handler).clone();
                    let instrumentation = ctx.instrumentation.clone();

                    // Call handler.drain() with instrumentation; treat failures as stage-fatal.
                    let drain_result =
                        process_with_instrumentation_no_count(&ctx.instrumentation, || async move {
                            handler.drain(&final_state).await.map_err(
                                |err| -> Box<dyn std::error::Error + Send + Sync> {
                                    // Stage-fatal handler error in drain: record it in error metrics
                                    // before type erasure.
                                    instrumentation.record_error(err.kind());
                                    err.into()
                                },
                            )
                        })
                        .await;

                    match drain_result {
                        Ok(drain_events) => {
                            // Write the final aggregated events (if any)
                            for event in drain_events {
                                use obzenflow_core::event::JournalEvent;
                                tracing::info!(
                                    target: "flowip-080o",
                                    stage_name = %ctx.stage_name,
                                    event_type = %event.event_type_name(),
                                    event_id = ?event.id,
                                    "stateful: writing drain event to journal"
                                );
                                let flow_context = FlowContext {
                                    flow_name: ctx.flow_name.clone(),
                                    flow_id: ctx.flow_id.to_string(),
                                    stage_name: ctx.stage_name.clone(),
                                    stage_id: self.stage_id.clone(),
                                    stage_type: obzenflow_core::event::context::StageType::Stateful,
                                };

                                let enriched_event = event
                                    .with_flow_context(flow_context)
                                    .with_runtime_context(
                                        ctx.instrumentation
                                            .snapshot_with_control(),
                                    );

                                // FLOWIP-080o-part-2: Only count data events for writer_seq.
                                // Lifecycle events (middleware metrics, etc.) are observability
                                // overhead and should not participate in transport contracts.
                                if enriched_event.is_data() {
                                    ctx.instrumentation
                                        .record_output_event(&enriched_event);
                                    // Track output for contract verification
                                    if let Some(ref mut sub) = ctx.subscription {
                                        sub.track_output_event();
                                    }
                                }
                                ctx.data_journal
                                    .append(enriched_event, None)
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to write final aggregated event: {}", e)
                                    })?;
                            }

                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                "Stateful stage drain complete"
                            );
                            Ok(EventLoopDirective::Transition(StatefulEvent::DrainComplete))
                        }
                        Err(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Drain error"
                            );
                            Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                format!("Drain error: {}", e),
                            )))
                        }
                    }
                } else {
                    directive
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
        ctx: &StatefulContext<H>,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Re-stamp flow and runtime context so metrics remain local to this
        // stateful stage even when forwarding control events.
        let mut forward_event = envelope.event.clone();

        forward_event = forward_event.with_flow_context(FlowContext {
            flow_name: ctx.flow_name.clone(),
            flow_id: ctx.flow_id.to_string(),
            stage_name: ctx.stage_name.clone(),
            stage_id: ctx.stage_id,
            stage_type: StageType::Stateful,
        });

        // Drop upstream runtime_context; this stage will publish its own
        // snapshots via observability events.
        forward_event.runtime_context = None;
        ctx.data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {}", e))?;
        Ok(())
    }

    /// Emit an observability heartbeat when enough events have been accumulated.
    ///
    /// This writes a lightweight `Observability` event carrying the latest
    /// `runtime_context` snapshot for the accumulator.
    async fn emit_stateful_heartbeat_if_due(
        &self,
        ctx: &mut StatefulContext<H>,
        force: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let interval = heartbeat_interval();
        if interval == 0 {
            return Ok(());
        }

        let delta = ctx.events_since_last_heartbeat;
        if delta == 0 {
            return Ok(());
        }

        // In normal operation we require `delta >= interval` before emitting.
        // When `force` is true (drain path), we bypass this threshold so that
        // short finite flows still publish a final heartbeat snapshot.
        if !force && delta < interval {
            return Ok(());
        }

        let writer_id = match ctx.writer_id.as_ref() {
            Some(id) => id.clone(),
            None => {
                // Writer not initialized yet; skip heartbeat rather than failing.
                return Ok(());
            }
        };

        // Capture a fresh runtime context snapshot for the heartbeat.
        let runtime_context = ctx.instrumentation.snapshot_with_control();

        use obzenflow_core::event::context::StageType;
        use obzenflow_core::event::payloads::observability_payload::{
            MetricsLifecycle, ObservabilityPayload,
        };
        use obzenflow_core::event::ChainEventFactory;
        use serde_json::json;

        let flow_context = FlowContext {
            flow_name: ctx.flow_name.clone(),
            flow_id: ctx.flow_id.to_string(),
            stage_name: ctx.stage_name.clone(),
            stage_id: self.stage_id,
            stage_type: StageType::Stateful,
        };

        let payload = ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
            name: "accumulator_heartbeat".to_string(),
            value: json!({
                "events_accumulated_since_last_heartbeat": delta,
                "events_processed_total": runtime_context.events_processed_total,
            }),
            tags: None,
        });

        let heartbeat = ChainEventFactory::observability_event(writer_id, payload)
            .with_flow_context(flow_context)
            .with_runtime_context(runtime_context);

        ctx.data_journal.append(heartbeat, None).await?;

        // Reset counter now that we've published a snapshot.
        ctx.events_since_last_heartbeat = 0;

        Ok(())
    }
}
