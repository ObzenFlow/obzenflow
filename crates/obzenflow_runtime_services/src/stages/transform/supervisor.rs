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
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{TransformAction, TransformContext, TransformEvent, TransformState};

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<
    H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   TransformState<H>;
            event:   TransformEvent<H>;
            context: TransformContext<H>;
            action:  TransformAction<H>;
            initial: initial_state;

            state TransformState::Created {
                on TransformEvent::Initialize => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: TransformState::Initialized,
                            actions: vec![TransformAction::AllocateResources],
                        })
                    })
                };

                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Initialized {
                on TransformEvent::Ready => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Running");
                        Ok(Transition {
                            next_state: TransformState::Running,
                            actions: vec![TransformAction::PublishRunning],
                        })
                    })
                };

                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Running {
                // Ready can be delivered again (e.g. from wide subscriptions); ignore if already running
                on TransformEvent::Ready => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: TransformState::Running,
                            actions: vec![],
                        })
                    })
                };

                on TransformEvent::ReceivedEOF => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: TransformState::Draining,
                            actions: vec![], // Continue draining in dispatch_state
                        })
                    })
                };

                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: TransformState::Failed(failure_msg),
                                actions: vec![
                                    TransformAction::SendFailure { message: msg },
                                    TransformAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Draining {
                on TransformEvent::DrainComplete => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
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
                };

                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: TransformState::Failed(failure_msg),
                                actions: vec![
                                    TransformAction::SendFailure { message: msg },
                                    TransformAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Drained {
                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Failed {
                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
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
                    })
                };
            }

            unhandled => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "TransformSupervisor",
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

    fn event_for_action_error(&self, msg: String) -> TransformEvent<H> {
        TransformEvent::Error(msg)
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
                let loop_count = ctx
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "transform: Running state - starting event loop iteration"
                );

                // Take subscription and contract state out of the context so we never hold
                // a borrow of the context across `.await` while polling or checking contracts.
                let mut subscription_opt = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                let directive = if let Some(subscription) = subscription_opt.as_mut() {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        loop_iteration = loop_count + 1,
                        "transform: about to call subscription.poll_next()"
                    );

                    match subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await
                    {
                        PollResult::Event(envelope) => {
                            use obzenflow_core::event::JournalEvent;
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                event_type = %envelope.event.event_type_name(),
                                "transform: poll_next returned Event"
                            );
                            ctx.instrumentation.record_consumed(&envelope);

                            // We have work - increment loops with work
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: received event from subscription"
                            );

                            tracing::debug!(
                                stage_name = %ctx.stage_name,
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
                                                stage_name = %ctx.stage_name,
                                                "Transform received EOF from upstream"
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
                                            // Handle EOF specially: only transition when ALL upstreams have EOF'd
                                            if envelope.event.is_eof() {
                                                let eof_outcome =
                                                    subscription.take_last_eof_outcome();

                                                // Contract check at EOF time
                                                let _ = subscription
                                                    .check_contracts(&mut contract_state[..])
                                                    .await;

                                                if let Some(outcome) = eof_outcome {
                                                    if outcome.is_final {
                                                        // All upstream readers have reached EOF: begin draining.
                                                        ctx.buffered_eof =
                                                            Some(envelope.event.clone());
                                                        tracing::info!(
                                                            stage_name = %ctx.stage_name,
                                                            event_type = envelope.event.event_type(),
                                                            "Transform stage received final EOF for all upstreams, transitioning to draining"
                                                        );
                                                        // Forward EOF downstream before transitioning
                                                        self.forward_control_event(&envelope)
                                                            .await?;
                                                        // Restore before returning
                                                        ctx.subscription = subscription_opt;
                                                        ctx.contract_state = contract_state;
                                                        return Ok(EventLoopDirective::Transition(
                                                            TransformEvent::ReceivedEOF,
                                                        ));
                                                    } else {
                                                        // Non-final EOF: forward but don't drain yet.
                                                        // Continue processing events from other upstreams.
                                                        self.forward_control_event(&envelope)
                                                            .await?;
                                                    }
                                                } else {
                                                    // No outcome yet (unexpected), forward and continue.
                                                    self.forward_control_event(&envelope).await?;
                                                }
                                            } else if matches!(signal, FlowControlPayload::Drain) {
                                                // Drain events from pipeline BeginDrain should initiate stage draining
                                                ctx.buffered_eof = Some(envelope.event.clone());
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Transform stage received drain signal, transitioning to draining"
                                                );
                                                self.forward_control_event(&envelope).await?;
                                                // Restore before returning
                                                ctx.subscription = subscription_opt;
                                                ctx.contract_state = contract_state;
                                                return Ok(EventLoopDirective::Transition(
                                                    TransformEvent::ReceivedEOF,
                                                ));
                                            } else {
                                                // Other control events: just forward
                                                self.forward_control_event(&envelope).await?;
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
                                            return Ok(EventLoopDirective::Continue);
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
                                    // Process data event (or pass through error-marked events)
                                    let handler = ctx.handler.clone();
                                    let envelope_clone = envelope.clone();

                                    let result = process_with_instrumentation(
                                        &ctx.instrumentation,
                                        || async move {
                                            use obzenflow_core::event::status::processing_status::ProcessingStatus;

                                            let event = envelope_clone.event.clone();

                                            // If this event is already marked as Error, pass it through unchanged.
                                            if matches!(
                                                event.processing_info.status,
                                                ProcessingStatus::Error { .. }
                                            ) {
                                                tracing::info!(
                                                    "Transform supervisor received pre-error-marked event {}: {:?}",
                                                    event.id,
                                                    event.processing_info.status
                                                );
                                                return Ok(vec![event]);
                                            }

                                            // Normal path: invoke handler and map per-record failures
                                            match handler.process(event) {
                                                Ok(outputs) => Ok(outputs),
                                                Err(err) => {
                                                    // Per-record handler failure: turn input into an error-marked event
                                                    let reason = format!("Transform handler error: {:?}", err);
                                                    let error_event = envelope_clone
                                                        .event
                                                        .clone()
                                                        .mark_as_error(reason, err.kind());
                                                    Ok(vec![error_event])
                                                }
                                            }
                                        },
                                    )
                                    .await;

                                    match result {
                                        Ok(transformed_events) => {
                                            // Write transformed events
                                            for event in transformed_events {
                                                use obzenflow_core::event::status::processing_status::{
                                                    ErrorKind, ProcessingStatus,
                                                };

                                                // Count all error-marked events for lifecycle / flow rollups,
                                                // even when they are not stage-fatal.
                                                if let ProcessingStatus::Error { kind, .. } =
                                                    &event.processing_info.status
                                                {
                                                    let k =
                                                        kind.clone().unwrap_or(ErrorKind::Unknown);
                                                    ctx.instrumentation.record_error(k);
                                                }

                                                let route_to_error_journal =
                                                    match &event.processing_info.status {
                                                        ProcessingStatus::Error {
                                                            kind, ..
                                                        } => match kind {
                                                            Some(ErrorKind::Timeout)
                                                            | Some(ErrorKind::Remote)
                                                            | Some(ErrorKind::Deserialization) => {
                                                                true
                                                            }
                                                            Some(ErrorKind::Validation)
                                                            | Some(ErrorKind::Domain) => false,
                                                            None | Some(ErrorKind::Unknown) => true,
                                                        },
                                                        _ => false,
                                                    };

                                                if route_to_error_journal {
                                                    tracing::info!(
                                                        stage_name = %ctx.stage_name,
                                                        event_id = %event.id,
                                                        "Writing error event to error journal (FLOWIP-082e)"
                                                    );
                                                    // Error events are still data, so record them for
                                                    // transport contracts and metrics.
                                                    ctx.instrumentation.record_emitted(&event);
                                                    ctx.error_journal
                                                        .append(event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| {
                                                            format!(
                                                                "Failed to write error event: {}",
                                                                e
                                                            )
                                                        })?;

                                                    // Track output for contract verification
                                                    subscription.track_output_event();
                                                } else {
                                                    // Enrich with flow/runtime context before writing to data journal
                                                    let flow_context = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id.clone(),
                                                        stage_type: obzenflow_core::event::context::StageType::Transform,
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
                                                            .record_emitted(&enriched_event);
                                                        // Track output for contract verification
                                                        subscription.track_output_event();
                                                    }

                                                    ctx.data_journal
                                                        .append(enriched_event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| format!("Failed to write transformed event: {}", e))?;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                stage_name = %ctx.stage_name,
                                                error = ?e,
                                                "Transform processing error"
                                            );
                                        }
                                    }
                                }
                                _ => {
                                    // Other content types we don't recognize - forward them
                                    self.forward_control_event(&envelope).await?;
                                }
                            }

                            // After handling a concrete event, keep running in Running state.
                            EventLoopDirective::Continue
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
                                            "Upstream stalled detected during active processing"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
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
                                            stage_name = %ctx.stage_name,
                                            error = %e,
                                            "Failed to check contracts"
                                        );
                                    }
                                }
                            }

                            tracing::trace!(
                                stage_name = %ctx.stage_name,
                                "No events available, sleeping"
                            );
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                "transform: poll_next returned NoEvents; sleeping briefly"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                            // No work this iteration; remain in Running.
                            EventLoopDirective::Continue
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Subscription error"
                            );
                            EventLoopDirective::Transition(TransformEvent::Error(format!(
                                "Subscription error: {}",
                                e
                            )))
                        }
                    }
                } else {
                    // No subscription yet, wait
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    EventLoopDirective::Continue
                };

                // Restore subscription and contract state to the context before returning.
                ctx.subscription = subscription_opt;
                ctx.contract_state = contract_state;

                Ok(directive)
            }

            TransformState::Draining => {
                // Continue processing remaining events
                let mut subscription_opt = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                let result = if let Some(subscription) = subscription_opt.as_mut() {
                    // Poll for remaining events without timeout hacks
                    match subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: draining received event from subscription"
                            );

                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                "Transform draining events"
                            );

                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                event_type = %envelope.event.event_type(),
                                is_eof = envelope.event.is_eof(),
                                "transform: draining received event"
                            );

                            ctx.instrumentation.record_consumed(&envelope);

                            // Process remaining event based on type
                            if !envelope.event.is_control() {
                                // Process data events (or pass through error-marked events)
                                let handler = ctx.handler.clone();
                                let envelope_clone = envelope.clone();

                                let transformed_events = process_with_instrumentation(
                                    &ctx.instrumentation,
                                    || async move {
                                        use obzenflow_core::event::status::processing_status::ProcessingStatus;

                                        let event = envelope_clone.event.clone();

                                        // If this event is already marked as Error, pass it through unchanged.
                                        if matches!(
                                            event.processing_info.status,
                                            ProcessingStatus::Error { .. }
                                        ) {
                                            return Ok(vec![event]);
                                        }

                                        match handler.process(event) {
                                            Ok(outputs) => Ok(outputs),
                                            Err(err) => {
                                                let reason = format!("Transform handler error during drain: {:?}", err);
                                                let error_event = envelope_clone
                                                    .event
                                                    .clone()
                                                    .mark_as_error(reason, err.kind());
                                                Ok(vec![error_event])
                                            }
                                        }
                                    },
                                )
                                .await?;

                                // Write transformed events using new journal.write() API
                                for event in transformed_events {
                                    use obzenflow_core::event::status::processing_status::{
                                        ErrorKind, ProcessingStatus,
                                    };

                                    // Count all error-marked events for lifecycle / flow rollups,
                                    // even when they are not stage-fatal.
                                    if let ProcessingStatus::Error { kind, .. } =
                                        &event.processing_info.status
                                    {
                                        let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                                        ctx.instrumentation.record_error(k);
                                    }

                                    let route_to_error_journal = match &event.processing_info.status
                                    {
                                        ProcessingStatus::Error { kind, .. } => match kind {
                                            Some(ErrorKind::Timeout)
                                            | Some(ErrorKind::Remote)
                                            | Some(ErrorKind::Deserialization) => true,
                                            Some(ErrorKind::Validation)
                                            | Some(ErrorKind::Domain) => false,
                                            None | Some(ErrorKind::Unknown) => true,
                                        },
                                        _ => false,
                                    };

                                    if route_to_error_journal {
                                        tracing::info!(
                                            stage_name = %ctx.stage_name,
                                            event_id = %event.id,
                                            "Writing error event to error journal during drain (FLOWIP-082e)"
                                        );
                                        // Only count data events for transport contracts (FLOWIP-080o-part-2)
                                        // Error events are still data, so count them
                                        ctx.instrumentation.record_emitted(&event);
                                        ctx.error_journal
                                            .append(event, Some(&envelope))
                                            .await
                                            .map_err(|e| {
                                                format!(
                                                    "Failed to write error event during drain: {}",
                                                    e
                                                )
                                            })?;

                                        // Track output for contract verification
                                        subscription.track_output_event();
                                    } else {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id.clone(),
                                            stage_type:
                                                obzenflow_core::event::context::StageType::Transform,
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
                                            ctx.instrumentation.record_emitted(&enriched_event);
                                            // Track output for contract verification
                                            subscription.track_output_event();
                                        }

                                        ctx.data_journal
                                            .append(enriched_event, Some(&envelope))
                                            .await
                                            .map_err(|e| {
                                                format!("Failed to write transformed event: {}", e)
                                            })?;
                                    }
                                }
                            } else {
                                // Forward control events during draining (CRITICAL FIX for FLOWIP-080o)
                                // This ensures contract events (consumption_final, consumption_progress, etc.)
                                // are not lost during the draining phase
                                tracing::debug!(
                                    stage_name = %ctx.stage_name,
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
                            let _ = subscription.check_contracts(&mut contract_state[..]).await;

                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                "Transform queue drained"
                            );
                            Ok(EventLoopDirective::Transition(
                                TransformEvent::DrainComplete,
                            ))
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Error during draining"
                            );
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
                };

                // Restore subscription and contract state to the context before returning.
                ctx.subscription = subscription_opt;
                ctx.contract_state = contract_state;

                result
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
        // Re-stamp flow and runtime context so metrics remain local to this
        // transform stage even when forwarding control events.
        let mut forward_event = envelope.event.clone();

        let flow_name = forward_event.flow_context.flow_name.clone();
        let flow_id = forward_event.flow_context.flow_id.clone();
        forward_event = forward_event.with_flow_context(FlowContext {
            flow_name,
            flow_id,
            stage_name: format!("{}", self.stage_id),
            stage_id: self.stage_id,
            stage_type: obzenflow_core::event::context::StageType::Transform,
        });

        // RuntimeContext will be refreshed by instrumentation when this stage
        // emits observability events; forwarded control events themselves may
        // omit runtime_context to avoid leaking upstream snapshots.
        forward_event.runtime_context = None;
        self.data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {}", e))?;
        Ok(())
    }
}
