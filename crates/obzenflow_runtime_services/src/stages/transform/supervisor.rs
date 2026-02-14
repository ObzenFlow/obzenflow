// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{TransformAction, TransformContext, TransformEvent, TransformState};

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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
impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for TransformSupervisor<H>
{
}

impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
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

                on TransformEvent::BeginDrain => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
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
                                TransformAction::DrainHandler,
                                TransformAction::ForwardEOF,
                                TransformAction::SendCompletion,
                                TransformAction::Cleanup,
                            ],
                        })
                    })
                };

                on TransformEvent::BeginDrain => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: TransformState::Draining,
                            actions: vec![],
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
impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
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

                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "transform: Running state - starting event loop iteration"
                );

                // Take subscription and contract state out of the context so we never hold
                // a borrow of the context across `.await` while polling or checking contracts.
                let mut subscription_opt = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                // If we have pending outputs from a previous input, drain them before
                // polling upstream again (bounded to one input; FLOWIP-086k).
                if let Some(subscription) = subscription_opt.as_mut() {
                    while let Some(pending) = ctx.pending_outputs.pop_front() {
                        let is_data = pending.is_data();
                        if is_data {
                            // Debug-only: emit activity pulses even when bypass is enabled, so
                            // operators can see what *would* have blocked (FLOWIP-086k).
                            if crate::backpressure::BackpressureWriter::is_bypass_enabled() {
                                if let Some((min_credit, limiting)) =
                                    ctx.backpressure_writer.min_downstream_credit_detail()
                                {
                                    if min_credit < 1 {
                                        ctx.backpressure_pulse.record_delay(
                                            std::time::Duration::ZERO,
                                            Some(min_credit),
                                            Some(limiting),
                                        );
                                        if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                            let flow_context = FlowContext {
                                                flow_name: ctx.flow_name.clone(),
                                                flow_id: ctx.flow_id.to_string(),
                                                stage_name: ctx.stage_name.clone(),
                                                stage_id: self.stage_id,
                                                stage_type:
                                                    obzenflow_core::event::context::StageType::Transform,
                                            };

                                            let event = ChainEventFactory::observability_event(
                                                WriterId::from(self.stage_id),
                                                ObservabilityPayload::Middleware(
                                                    MiddlewareLifecycle::Backpressure(pulse),
                                                ),
                                            )
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                            match ctx.data_journal.append(event, None).await {
                                                Ok(written) => {
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &ctx.system_journal,
                                                    )
                                                    .await;
                                                }
                                                Err(e) => tracing::warn!(
                                                    stage_name = %ctx.stage_name,
                                                    journal_error = %e,
                                                    "Failed to append backpressure activity pulse"
                                                ),
                                            }
                                        }
                                    }
                                }
                            }

                            let Some(reservation) = ctx.backpressure_writer.reserve(1) else {
                                ctx.pending_outputs.push_front(pending);
                                let delay = ctx.backpressure_backoff.next_delay();
                                ctx.backpressure_writer.record_wait(delay);

                                if let Some((min_credit, limiting)) =
                                    ctx.backpressure_writer.min_downstream_credit_detail()
                                {
                                    ctx.backpressure_pulse.record_delay(
                                        delay,
                                        Some(min_credit),
                                        Some(limiting),
                                    );
                                } else {
                                    ctx.backpressure_pulse.record_delay(delay, None, None);
                                }
                                if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                    let flow_context = FlowContext {
                                        flow_name: ctx.flow_name.clone(),
                                        flow_id: ctx.flow_id.to_string(),
                                        stage_name: ctx.stage_name.clone(),
                                        stage_id: self.stage_id,
                                        stage_type:
                                            obzenflow_core::event::context::StageType::Transform,
                                    };

                                    let event = ChainEventFactory::observability_event(
                                        WriterId::from(self.stage_id),
                                        ObservabilityPayload::Middleware(
                                            MiddlewareLifecycle::Backpressure(pulse),
                                        ),
                                    )
                                    .with_flow_context(flow_context)
                                    .with_runtime_context(
                                        ctx.instrumentation.snapshot_with_control(),
                                    );

                                    match ctx.data_journal.append(event, None).await {
                                        Ok(written) => {
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &ctx.system_journal,
                                            )
                                            .await;
                                        }
                                        Err(e) => tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            journal_error = %e,
                                            "Failed to append backpressure activity pulse"
                                        ),
                                    }
                                }

                                tokio::time::sleep(delay).await;
                                ctx.subscription = subscription_opt;
                                ctx.contract_state = contract_state;
                                return Ok(EventLoopDirective::Continue);
                            };

                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: obzenflow_core::event::context::StageType::Transform,
                            };

                            let enriched = pending.with_flow_context(flow_context);
                            if enriched.is_data() {
                                ctx.instrumentation.record_output_event(&enriched);
                                subscription.track_output_event();
                            }
                            let enriched = enriched
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            let written = ctx
                                .data_journal
                                .append(enriched, ctx.pending_parent.as_ref())
                                .await
                                .map_err(|e| format!("Failed to write pending output: {e}"))?;
                            reservation.commit(1);
                            ctx.backpressure_backoff.reset();
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &ctx.system_journal,
                            )
                            .await;
                        } else {
                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: obzenflow_core::event::context::StageType::Transform,
                            };

                            let enriched = pending
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            let written = ctx
                                .data_journal
                                .append(enriched, ctx.pending_parent.as_ref())
                                .await
                                .map_err(|e| format!("Failed to write pending output: {e}"))?;
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &ctx.system_journal,
                            )
                            .await;
                        }
                    }

                    if let Some(upstream) = ctx.pending_ack_upstream.take() {
                        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                            reader.ack_consumed(1);
                        } else {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                upstream = ?upstream,
                                "Transform backpressure ack skipped: missing reader handle"
                            );
                        }
                    }
                    ctx.pending_parent = None;
                }

                let directive = if let Some(subscription) = subscription_opt.as_mut() {
                    tracing::trace!(
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
                            tracing::trace!(
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

                            tracing::trace!(
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
                                    let mut action = match signal {
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

                                    loop {
                                        match action {
                                            ControlEventAction::Delay(duration) => {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    duration = ?duration,
                                                    "Delaying control event"
                                                );
                                                tokio::time::sleep(duration).await;
                                                action = ControlEventAction::Forward;
                                            }
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
                                                            return Ok(
                                                                EventLoopDirective::Transition(
                                                                    TransformEvent::ReceivedEOF,
                                                                ),
                                                            );
                                                        } else {
                                                            // Non-final EOF: forward but don't drain yet.
                                                            // Continue processing events from other upstreams.
                                                            self.forward_control_event(&envelope)
                                                                .await?;
                                                        }
                                                    } else {
                                                        // No outcome yet (unexpected), forward and continue.
                                                        self.forward_control_event(&envelope)
                                                            .await?;
                                                    }
                                                } else if matches!(
                                                    signal,
                                                    FlowControlPayload::Drain
                                                ) {
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

                                                break;
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
                                                break;
                                            }
                                            ControlEventAction::Skip => {
                                                tracing::warn!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Skipping control event (dangerous!)"
                                                );
                                                // Don't forward, don't process
                                                break;
                                            }
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // Process data event (or pass through error-marked events)
                                    let envelope_clone = envelope.clone();
                                    let handler = &ctx.handler;

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
                                            match handler.process(event).await {
                                                Ok(outputs) => Ok(outputs),
                                                Err(err) => {
                                                    // Per-record handler failure: turn input into an error-marked event
                                                    let reason =
                                                        format!("Transform handler error: {err:?}");
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
                                            let upstream_stage =
                                                subscription.last_delivered_upstream_stage();

                                            // Write error-journal events immediately; stage-journal
                                            // outputs are gated by backpressure.
                                            let mut pending_data_outputs: std::collections::VecDeque<
                                                ChainEvent,
                                            > = std::collections::VecDeque::new();

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
                                                            | Some(ErrorKind::RateLimited)
                                                            | Some(ErrorKind::PermanentFailure)
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
                                                    ctx.error_journal
                                                        .append(event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| {
                                                            format!(
                                                                "Failed to write error event: {e}"
                                                            )
                                                        })?;
                                                    // Error events are isolated to the stage's error journal and
                                                    // MUST NOT participate in downstream transport contracts.
                                                } else {
                                                    pending_data_outputs.push_back(event);
                                                }
                                            }

                                            // Now write stage-journal outputs with per-edge backpressure.
                                            while let Some(event) = pending_data_outputs.pop_front()
                                            {
                                                if event.is_data() {
                                                    // Debug-only: emit activity pulses even when bypass is enabled, so
                                                    // operators can see what *would* have blocked (FLOWIP-086k).
                                                    if crate::backpressure::BackpressureWriter::is_bypass_enabled() {
                                                        if let Some((min_credit, limiting)) =
                                                            ctx.backpressure_writer
                                                                .min_downstream_credit_detail()
                                                        {
                                                            if min_credit < 1 {
                                                                ctx.backpressure_pulse.record_delay(
                                                                    std::time::Duration::ZERO,
                                                                    Some(min_credit),
                                                                    Some(limiting),
                                                                );
                                                                if let Some(pulse) =
                                                                    ctx.backpressure_pulse
                                                                        .maybe_emit()
                                                                {
                                                                    let flow_context = FlowContext {
                                                                        flow_name: ctx.flow_name.clone(),
                                                                        flow_id: ctx.flow_id.to_string(),
                                                                        stage_name: ctx.stage_name.clone(),
                                                                        stage_id: self.stage_id,
                                                                        stage_type: obzenflow_core::event::context::StageType::Transform,
                                                                    };

                                                                    let event = ChainEventFactory::observability_event(
                                                                        WriterId::from(self.stage_id),
                                                                        ObservabilityPayload::Middleware(
                                                                            MiddlewareLifecycle::Backpressure(pulse),
                                                                        ),
                                                                    )
                                                                    .with_flow_context(flow_context)
                                                                    .with_runtime_context(
                                                                        ctx.instrumentation.snapshot_with_control(),
                                                                    );

                                                                    match ctx.data_journal.append(event, None).await {
                                                                        Ok(written) => {
                                                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                                &written,
                                                                                &ctx.system_journal,
                                                                            )
                                                                            .await;
                                                                        }
                                                                        Err(e) => tracing::warn!(
                                                                            stage_name = %ctx.stage_name,
                                                                            journal_error = %e,
                                                                            "Failed to append backpressure activity pulse"
                                                                        ),
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                    let Some(reservation) =
                                                        ctx.backpressure_writer.reserve(1)
                                                    else {
                                                        // Queue remaining outputs and retry later (bounded to one input).
                                                        ctx.pending_parent = Some(envelope.clone());
                                                        ctx.pending_ack_upstream = upstream_stage;
                                                        ctx.pending_outputs.push_back(event);
                                                        ctx.pending_outputs
                                                            .extend(pending_data_outputs);

                                                        let delay =
                                                            ctx.backpressure_backoff.next_delay();
                                                        ctx.backpressure_writer.record_wait(delay);

                                                        if let Some((min_credit, limiting)) = ctx
                                                            .backpressure_writer
                                                            .min_downstream_credit_detail()
                                                        {
                                                            ctx.backpressure_pulse.record_delay(
                                                                delay,
                                                                Some(min_credit),
                                                                Some(limiting),
                                                            );
                                                        } else {
                                                            ctx.backpressure_pulse
                                                                .record_delay(delay, None, None);
                                                        }
                                                        if let Some(pulse) =
                                                            ctx.backpressure_pulse.maybe_emit()
                                                        {
                                                            let flow_context = FlowContext {
                                                                flow_name: ctx.flow_name.clone(),
                                                                flow_id: ctx.flow_id.to_string(),
                                                                stage_name: ctx.stage_name.clone(),
                                                                stage_id: self.stage_id,
                                                                stage_type: obzenflow_core::event::context::StageType::Transform,
                                                            };

                                                            let event = ChainEventFactory::observability_event(
                                                                WriterId::from(self.stage_id),
                                                                ObservabilityPayload::Middleware(
                                                                    MiddlewareLifecycle::Backpressure(pulse),
                                                                ),
                                                            )
                                                            .with_flow_context(flow_context)
                                                            .with_runtime_context(
                                                                ctx.instrumentation.snapshot_with_control(),
                                                            );

                                                            match ctx
                                                                .data_journal
                                                                .append(event, None)
                                                                .await
                                                            {
                                                                Ok(written) => {
                                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                        &written,
                                                                        &ctx.system_journal,
                                                                    )
                                                                    .await;
                                                                }
                                                                Err(e) => tracing::warn!(
                                                                    stage_name = %ctx.stage_name,
                                                                    journal_error = %e,
                                                                    "Failed to append backpressure activity pulse"
                                                                ),
                                                            }
                                                        }
                                                        tokio::time::sleep(delay).await;
                                                        break;
                                                    };

                                                    let flow_context = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id,
                                                        stage_type: obzenflow_core::event::context::StageType::Transform,
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
                                                        subscription.track_output_event();
                                                    }

                                                    let written = ctx
                                                        .data_journal
                                                        .append(enriched_event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| {
                                                            format!("Failed to write transformed event: {e}")
                                                        })?;
                                                    reservation.commit(1);
                                                    ctx.backpressure_backoff.reset();
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &ctx.system_journal,
                                                    )
                                                    .await;
                                                } else {
                                                    // Non-data outputs bypass credit gating.
                                                    let flow_context = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id,
                                                        stage_type: obzenflow_core::event::context::StageType::Transform,
                                                    };

                                                    let enriched_event = event
                                                        .with_flow_context(flow_context)
                                                        .with_runtime_context(
                                                            ctx.instrumentation
                                                                .snapshot_with_control(),
                                                        );

                                                    let written = ctx
                                                        .data_journal
                                                        .append(enriched_event, Some(&envelope))
                                                        .await
                                                        .map_err(|e| {
                                                            format!("Failed to write transformed event: {e}")
                                                        })?;
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &ctx.system_journal,
                                                    )
                                                    .await;
                                                }
                                            }

                                            // If we didn't enqueue anything, ack upstream consumption now.
                                            if ctx.pending_outputs.is_empty() {
                                                if let Some(upstream) = upstream_stage {
                                                    if let Some(reader) =
                                                        ctx.backpressure_readers.get(&upstream)
                                                    {
                                                        reader.ack_consumed(1);
                                                    } else {
                                                        tracing::warn!(
                                                            stage_name = %ctx.stage_name,
                                                            upstream = ?upstream,
                                                            "Transform backpressure ack skipped: missing reader handle"
                                                        );
                                                    }
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
                            tracing::trace!(
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
                                "Subscription error: {e}"
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

                // Drain any pending outputs before consuming more upstream events.
                if let Some(subscription) = subscription_opt.as_mut() {
                    while let Some(pending) = ctx.pending_outputs.pop_front() {
                        let is_data = pending.is_data();
                        if is_data {
                            // Debug-only: emit activity pulses even when bypass is enabled, so
                            // operators can see what *would* have blocked (FLOWIP-086k).
                            if crate::backpressure::BackpressureWriter::is_bypass_enabled() {
                                if let Some((min_credit, limiting)) =
                                    ctx.backpressure_writer.min_downstream_credit_detail()
                                {
                                    if min_credit < 1 {
                                        ctx.backpressure_pulse.record_delay(
                                            std::time::Duration::ZERO,
                                            Some(min_credit),
                                            Some(limiting),
                                        );
                                        if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                            let flow_context = FlowContext {
                                                flow_name: ctx.flow_name.clone(),
                                                flow_id: ctx.flow_id.to_string(),
                                                stage_name: ctx.stage_name.clone(),
                                                stage_id: self.stage_id,
                                                stage_type:
                                                    obzenflow_core::event::context::StageType::Transform,
                                            };

                                            let event = ChainEventFactory::observability_event(
                                                WriterId::from(self.stage_id),
                                                ObservabilityPayload::Middleware(
                                                    MiddlewareLifecycle::Backpressure(pulse),
                                                ),
                                            )
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                            match ctx.data_journal.append(event, None).await {
                                                Ok(written) => {
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &ctx.system_journal,
                                                    )
                                                    .await;
                                                }
                                                Err(e) => tracing::warn!(
                                                    stage_name = %ctx.stage_name,
                                                    journal_error = %e,
                                                    "Failed to append backpressure activity pulse"
                                                ),
                                            }
                                        }
                                    }
                                }
                            }

                            let Some(reservation) = ctx.backpressure_writer.reserve(1) else {
                                ctx.pending_outputs.push_front(pending);
                                let delay = ctx.backpressure_backoff.next_delay();
                                ctx.backpressure_writer.record_wait(delay);

                                if let Some((min_credit, limiting)) =
                                    ctx.backpressure_writer.min_downstream_credit_detail()
                                {
                                    ctx.backpressure_pulse.record_delay(
                                        delay,
                                        Some(min_credit),
                                        Some(limiting),
                                    );
                                } else {
                                    ctx.backpressure_pulse.record_delay(delay, None, None);
                                }
                                if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                    let flow_context = FlowContext {
                                        flow_name: ctx.flow_name.clone(),
                                        flow_id: ctx.flow_id.to_string(),
                                        stage_name: ctx.stage_name.clone(),
                                        stage_id: self.stage_id,
                                        stage_type:
                                            obzenflow_core::event::context::StageType::Transform,
                                    };

                                    let event = ChainEventFactory::observability_event(
                                        WriterId::from(self.stage_id),
                                        ObservabilityPayload::Middleware(
                                            MiddlewareLifecycle::Backpressure(pulse),
                                        ),
                                    )
                                    .with_flow_context(flow_context)
                                    .with_runtime_context(
                                        ctx.instrumentation.snapshot_with_control(),
                                    );

                                    match ctx.data_journal.append(event, None).await {
                                        Ok(written) => {
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &ctx.system_journal,
                                            )
                                            .await;
                                        }
                                        Err(e) => tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            journal_error = %e,
                                            "Failed to append backpressure activity pulse"
                                        ),
                                    }
                                }

                                tokio::time::sleep(delay).await;
                                ctx.subscription = subscription_opt;
                                ctx.contract_state = contract_state;
                                return Ok(EventLoopDirective::Continue);
                            };

                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: obzenflow_core::event::context::StageType::Transform,
                            };

                            let enriched = pending.with_flow_context(flow_context);
                            if enriched.is_data() {
                                ctx.instrumentation.record_output_event(&enriched);
                                subscription.track_output_event();
                            }
                            let enriched = enriched
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            let written = ctx
                                .data_journal
                                .append(enriched, ctx.pending_parent.as_ref())
                                .await
                                .map_err(|e| format!("Failed to write pending output: {e}"))?;
                            reservation.commit(1);
                            ctx.backpressure_backoff.reset();
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &ctx.system_journal,
                            )
                            .await;
                        } else {
                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: obzenflow_core::event::context::StageType::Transform,
                            };

                            let enriched = pending
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            let written = ctx
                                .data_journal
                                .append(enriched, ctx.pending_parent.as_ref())
                                .await
                                .map_err(|e| format!("Failed to write pending output: {e}"))?;
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &ctx.system_journal,
                            )
                            .await;
                        }
                    }

                    if let Some(upstream) = ctx.pending_ack_upstream.take() {
                        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                            reader.ack_consumed(1);
                        }
                    }
                    ctx.pending_parent = None;
                }

                let result = if let Some(subscription) = subscription_opt.as_mut() {
                    // Poll for remaining events without timeout hacks
                    match subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::trace!(
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

                            tracing::trace!(
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
                                let envelope_clone = envelope.clone();
                                let handler = &ctx.handler;

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

                                        match handler.process(event).await {
                                            Ok(outputs) => Ok(outputs),
                                            Err(err) => {
                                                let reason = format!(
                                                    "Transform handler error during drain: {err:?}"
                                                );
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

                                let upstream_stage = subscription.last_delivered_upstream_stage();

                                // Write error-journal events immediately; stage-journal outputs are
                                // gated by backpressure.
                                let mut pending_data_outputs: std::collections::VecDeque<
                                    ChainEvent,
                                > = std::collections::VecDeque::new();

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
                                            | Some(ErrorKind::RateLimited)
                                            | Some(ErrorKind::PermanentFailure)
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
                                        ctx.error_journal
                                            .append(event, Some(&envelope))
                                            .await
                                            .map_err(|e| {
                                                format!(
                                                    "Failed to write error event during drain: {e}"
                                                )
                                            })?;
                                        // Error events are isolated to the stage's error journal and
                                        // MUST NOT participate in downstream transport contracts.
                                    } else {
                                        pending_data_outputs.push_back(event);
                                    }
                                }

                                while let Some(event) = pending_data_outputs.pop_front() {
                                    if event.is_data() {
                                        // Debug-only: emit activity pulses even when bypass is enabled, so
                                        // operators can see what *would* have blocked (FLOWIP-086k).
                                        if crate::backpressure::BackpressureWriter::is_bypass_enabled() {
                                            if let Some((min_credit, limiting)) =
                                                ctx.backpressure_writer.min_downstream_credit_detail()
                                            {
                                                if min_credit < 1 {
                                                    ctx.backpressure_pulse.record_delay(
                                                        std::time::Duration::ZERO,
                                                        Some(min_credit),
                                                        Some(limiting),
                                                    );
                                                    if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                                        let flow_context = FlowContext {
                                                            flow_name: ctx.flow_name.clone(),
                                                            flow_id: ctx.flow_id.to_string(),
                                                            stage_name: ctx.stage_name.clone(),
                                                            stage_id: self.stage_id,
                                                            stage_type:
                                                                obzenflow_core::event::context::StageType::Transform,
                                                        };

                                                        let event = ChainEventFactory::observability_event(
                                                            WriterId::from(self.stage_id),
                                                            ObservabilityPayload::Middleware(
                                                                MiddlewareLifecycle::Backpressure(pulse),
                                                            ),
                                                        )
                                                        .with_flow_context(flow_context)
                                                        .with_runtime_context(
                                                            ctx.instrumentation.snapshot_with_control(),
                                                        );

                                                        match ctx.data_journal.append(event, None).await {
                                                            Ok(written) => {
                                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                    &written,
                                                                    &ctx.system_journal,
                                                                )
                                                                .await;
                                                            }
                                                            Err(e) => tracing::warn!(
                                                                stage_name = %ctx.stage_name,
                                                                journal_error = %e,
                                                                "Failed to append backpressure activity pulse"
                                                            ),
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        let Some(reservation) = ctx.backpressure_writer.reserve(1)
                                        else {
                                            ctx.pending_parent = Some(envelope.clone());
                                            ctx.pending_ack_upstream = upstream_stage;
                                            ctx.pending_outputs.push_back(event);
                                            ctx.pending_outputs.extend(pending_data_outputs);
                                            let delay = ctx.backpressure_backoff.next_delay();
                                            ctx.backpressure_writer.record_wait(delay);

                                            if let Some((min_credit, limiting)) = ctx
                                                .backpressure_writer
                                                .min_downstream_credit_detail()
                                            {
                                                ctx.backpressure_pulse.record_delay(
                                                    delay,
                                                    Some(min_credit),
                                                    Some(limiting),
                                                );
                                            } else {
                                                ctx.backpressure_pulse
                                                    .record_delay(delay, None, None);
                                            }
                                            if let Some(pulse) = ctx.backpressure_pulse.maybe_emit()
                                            {
                                                let flow_context = FlowContext {
                                                    flow_name: ctx.flow_name.clone(),
                                                    flow_id: ctx.flow_id.to_string(),
                                                    stage_name: ctx.stage_name.clone(),
                                                    stage_id: self.stage_id,
                                                    stage_type:
                                                        obzenflow_core::event::context::StageType::Transform,
                                                };

                                                let event = ChainEventFactory::observability_event(
                                                    WriterId::from(self.stage_id),
                                                    ObservabilityPayload::Middleware(
                                                        MiddlewareLifecycle::Backpressure(pulse),
                                                    ),
                                                )
                                                .with_flow_context(flow_context)
                                                .with_runtime_context(
                                                    ctx.instrumentation.snapshot_with_control(),
                                                );

                                                match ctx.data_journal.append(event, None).await {
                                                    Ok(written) => {
                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                            &written,
                                                            &ctx.system_journal,
                                                        )
                                                        .await;
                                                    }
                                                    Err(e) => tracing::warn!(
                                                        stage_name = %ctx.stage_name,
                                                        journal_error = %e,
                                                        "Failed to append backpressure activity pulse"
                                                    ),
                                                }
                                            }
                                            tokio::time::sleep(delay).await;
                                            break;
                                        };

                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type:
                                                obzenflow_core::event::context::StageType::Transform,
                                        };

                                        let enriched_event = event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        if enriched_event.is_data() {
                                            ctx.instrumentation
                                                .record_output_event(&enriched_event);
                                            subscription.track_output_event();
                                        }

                                        let written = ctx
                                            .data_journal
                                            .append(enriched_event, Some(&envelope))
                                            .await
                                            .map_err(|e| {
                                                format!("Failed to write transformed event: {e}")
                                            })?;
                                        reservation.commit(1);
                                        ctx.backpressure_backoff.reset();
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &ctx.system_journal,
                                        )
                                        .await;
                                    } else {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type:
                                                obzenflow_core::event::context::StageType::Transform,
                                        };

                                        let enriched_event = event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        let written = ctx
                                            .data_journal
                                            .append(enriched_event, Some(&envelope))
                                            .await
                                            .map_err(|e| {
                                                format!("Failed to write transformed event: {e}")
                                            })?;
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &ctx.system_journal,
                                        )
                                        .await;
                                    }
                                }

                                if ctx.pending_outputs.is_empty() && envelope.event.is_data() {
                                    if let Some(upstream) = upstream_stage {
                                        if let Some(reader) =
                                            ctx.backpressure_readers.get(&upstream)
                                        {
                                            reader.ack_consumed(1);
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
                                format!("Drain error: {e}"),
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

impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    TransformSupervisor<H>
{
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
            .map_err(|e| format!("Failed to forward control event: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
    use crate::id_conversions::StageIdExt;
    use crate::stages::common::control_strategies::JonestownStrategy;
    use crate::stages::common::handler_error::HandlerError;
    use crate::stages::common::handlers::TransformHandler;
    use crate::stages::resources_builder::SubscriptionFactory;
    use crate::supervised_base::HandlerSupervised;
    use async_trait::async_trait;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::JournalWriterId;
    use obzenflow_core::event::journal_event::JournalEvent;
    use obzenflow_core::event::vector_clock::CausalOrderingService;
    use obzenflow_core::event::{ChainEventFactory, SystemEvent};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::{ChainEvent, FlowId, StageId, WriterId};
    use obzenflow_fsm::FsmAction;
    use obzenflow_topology::TopologyBuilder;
    use serde_json::json;
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio_test::{assert_pending, assert_ready};

    struct TestJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        seq: AtomicU64,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> TestJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                seq: AtomicU64::new(0),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn next_seq(&self) -> u64 {
            self.seq.fetch_add(1, Ordering::Relaxed).saturating_add(1)
        }
    }

    struct TestJournalReader<T: JournalEvent> {
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
        pos: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let mut env = EventEnvelope::new(JournalWriterId::from(self.id), event);

            if let Some(parent) = parent {
                CausalOrderingService::update_with_parent(
                    &mut env.vector_clock,
                    &parent.vector_clock,
                );
            }

            let writer_key = env.event.writer_id().to_string();
            let seq = self.next_seq();
            env.vector_clock.clocks.insert(writer_key, seq);

            let mut guard = self.events.lock().unwrap();
            guard.push(env.clone());
            Ok(env)
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &obzenflow_core::EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(TestJournalReader {
                events: self.events.clone(),
                pos: 0,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(TestJournalReader {
                events: self.events.clone(),
                pos: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            if self.pos >= guard.len() {
                Ok(None)
            } else {
                let env = guard.get(self.pos).cloned();
                self.pos += 1;
                Ok(env)
            }
        }

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let start = self.pos as u64;
            self.pos = (self.pos as u64 + n) as usize;
            Ok((self.pos as u64).saturating_sub(start))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn is_at_end(&self) -> bool {
            let guard = self.events.lock().unwrap();
            self.pos >= guard.len()
        }
    }

    #[derive(Clone, Debug)]
    struct ExpandHandler {
        writer_id: WriterId,
    }

    #[async_trait]
    impl TransformHandler for ExpandHandler {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![
                ChainEventFactory::data_event(
                    self.writer_id,
                    "bp_test.expand_out",
                    json!({ "n": 1 }),
                ),
                ChainEventFactory::data_event(
                    self.writer_id,
                    "bp_test.expand_out",
                    json!({ "n": 2 }),
                ),
            ])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct FilterHandler;

    #[async_trait]
    impl TransformHandler for FilterHandler {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(Vec::new())
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    async fn build_transform_harness<
        H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
        F: FnOnce(StageId) -> H,
    >(
        handler_factory: F,
        upstream_window: u64,
        transform_window: u64,
    ) -> (
        TransformSupervisor<H>,
        TransformContext<H>,
        BackpressureRegistry,
        StageId,
        StageId,
        StageId,
        Arc<dyn Journal<ChainEvent>>,
        Arc<dyn Journal<ChainEvent>>,
    ) {
        let mut builder = TopologyBuilder::new();
        let s_top = builder.add_stage(Some("s".to_string()));
        let t_top = builder.add_stage(Some("t".to_string()));
        let k_top = builder.add_stage(Some("k".to_string()));
        let topology = builder.build_unchecked().expect("topology");

        let s = StageId::from_topology_id(s_top);
        let t = StageId::from_topology_id(t_top);
        let k = StageId::from_topology_id(k_top);

        let plan = BackpressurePlan::disabled()
            .with_stage_window(
                s,
                NonZeroU64::new(upstream_window).expect("upstream_window"),
            )
            .with_stage_window(
                t,
                NonZeroU64::new(transform_window).expect("transform_window"),
            );
        let registry = BackpressureRegistry::new(&topology, &plan);

        let upstream_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(s)));
        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(t)));
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(t)));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(t)));

        let mut stage_names = HashMap::new();
        stage_names.insert(s, "s".to_string());
        stage_names.insert(t, "t".to_string());
        stage_names.insert(k, "k".to_string());
        let subscription_factory = SubscriptionFactory::new(stage_names);
        let mut upstream_subscription_factory =
            subscription_factory.bind(&[(s, upstream_journal.clone())]);
        upstream_subscription_factory.owner_label = "t".to_string();

        let instrumentation =
            Arc::new(crate::metrics::instrumentation::StageInstrumentation::new());
        let control_strategy: Arc<
            dyn crate::stages::common::control_strategies::ControlEventStrategy,
        > = Arc::new(JonestownStrategy);

        let mut backpressure_readers = HashMap::new();
        backpressure_readers.insert(s, registry.reader(s, t));

        let handler = handler_factory(t);
        let mut ctx = TransformContext {
            handler,
            stage_id: t,
            stage_name: "t".to_string(),
            flow_name: "bp_test_flow".to_string(),
            flow_id: FlowId::new(),
            data_journal: data_journal.clone(),
            error_journal,
            system_journal: system_journal.clone(),
            writer_id: None,
            subscription: None,
            contract_state: Vec::new(),
            control_strategy,
            buffered_eof: None,
            instrumentation,
            upstream_subscription_factory,
            backpressure_writer: registry.writer(t),
            backpressure_readers,
            pending_outputs: std::collections::VecDeque::new(),
            pending_parent: None,
            pending_ack_upstream: None,
            backpressure_pulse:
                crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
            backpressure_backoff:
                crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                    std::time::Duration::from_millis(1),
                    std::time::Duration::from_millis(50),
                ),
        };

        TransformAction::AllocateResources
            .execute(&mut ctx)
            .await
            .expect("allocate resources");

        let supervisor = TransformSupervisor::<H> {
            name: "transform_test".to_string(),
            data_journal: data_journal.clone(),
            system_journal,
            stage_id: t,
            _marker: std::marker::PhantomData,
        };

        (
            supervisor,
            ctx,
            registry,
            s,
            t,
            k,
            upstream_journal,
            data_journal,
        )
    }

    #[tokio::test]
    async fn expand_transform_defers_upstream_ack_until_all_outputs_written() {
        let (mut supervisor, mut ctx, registry, s, t, k, upstream_journal, data_journal) =
            build_transform_harness(
                |t| ExpandHandler {
                    writer_id: WriterId::from(t),
                },
                1,
                1,
            )
            .await;

        // Seed upstream writer_seq to make ack effects observable via credit changes.
        let upstream_writer = registry.writer(s);
        upstream_writer.reserve(1).expect("seed reserve").commit(1);

        let input = ChainEventFactory::data_event(WriterId::from(s), "bp_test.in", json!({}));
        upstream_journal
            .append(input, None)
            .await
            .expect("append input");

        let state = TransformState::<ExpandHandler>::Running;
        let directive = supervisor
            .dispatch_state(&state, &mut ctx)
            .await
            .expect("dispatch");
        assert!(matches!(directive, EventLoopDirective::Continue));

        // One output written, one pending due to window=1.
        let events = data_journal
            .read_causally_ordered()
            .await
            .expect("read outputs");
        let outputs_written = events
            .iter()
            .filter(|env| env.event.is_data() && env.event.event_type() == "bp_test.expand_out")
            .count();
        assert_eq!(outputs_written, 1);
        assert_eq!(ctx.pending_outputs.len(), 1);
        assert_eq!(
            upstream_writer.min_downstream_credit(),
            0,
            "upstream should not be acked yet"
        );

        // Unblock downstream and drain pending output; this should trigger the deferred upstream ack.
        registry.reader(t, k).ack_consumed(1);
        supervisor
            .dispatch_state(&state, &mut ctx)
            .await
            .expect("dispatch drain");

        let events = data_journal
            .read_causally_ordered()
            .await
            .expect("read outputs");
        let outputs_written = events
            .iter()
            .filter(|env| env.event.is_data() && env.event.event_type() == "bp_test.expand_out")
            .count();
        assert_eq!(outputs_written, 2);
        assert!(ctx.pending_outputs.is_empty());
        assert_eq!(
            upstream_writer.min_downstream_credit(),
            1,
            "upstream ack should be observed"
        );
    }

    #[tokio::test]
    async fn filter_transform_acks_upstream_even_with_zero_outputs() {
        let (mut supervisor, mut ctx, registry, s, _t, _k, upstream_journal, _data_journal) =
            build_transform_harness(|_| FilterHandler, 1, 1).await;

        let upstream_writer = registry.writer(s);
        upstream_writer.reserve(1).expect("seed reserve").commit(1);

        let input = ChainEventFactory::data_event(WriterId::from(s), "bp_test.in", json!({}));
        upstream_journal
            .append(input, None)
            .await
            .expect("append input");

        let state = TransformState::<FilterHandler>::Running;
        supervisor
            .dispatch_state(&state, &mut ctx)
            .await
            .expect("dispatch");

        assert_eq!(
            upstream_writer.min_downstream_credit(),
            1,
            "filter must ack upstream even when it emits 0 outputs"
        );
    }

    #[tokio::test]
    async fn backpressure_ack_uses_subscription_upstream_stage_not_event_writer_id() {
        let (mut supervisor, mut ctx, registry, s, t, _k, upstream_journal, _data_journal) =
            build_transform_harness(|_| FilterHandler, 1, 1).await;

        let upstream_writer = registry.writer(s);
        upstream_writer.reserve(1).expect("seed reserve").commit(1);

        // WriterId is not required to match the topology upstream stage; it can be preserved
        // across stages for attribution. Backpressure MUST still ack the edge based on which
        // upstream journal produced the event.
        let input = ChainEventFactory::data_event(WriterId::from(t), "bp_test.in", json!({}));
        upstream_journal
            .append(input, None)
            .await
            .expect("append input");

        let state = TransformState::<FilterHandler>::Running;
        supervisor
            .dispatch_state(&state, &mut ctx)
            .await
            .expect("dispatch");

        assert_eq!(
            upstream_writer.min_downstream_credit(),
            1,
            "ack should be based on subscription upstream stage, not event.writer_id"
        );
    }

    #[tokio::test]
    async fn downstream_stall_blocks_with_sleep_no_hot_loop() {
        tokio::time::pause();

        let (mut supervisor, mut ctx, registry, _s, t, _k, _upstream_journal, _data_journal) =
            build_transform_harness(
                |t| ExpandHandler {
                    writer_id: WriterId::from(t),
                },
                1,
                1,
            )
            .await;

        // Exhaust downstream credits for this stage so the next reserve will block.
        ctx.backpressure_writer
            .reserve(1)
            .expect("seed reserve")
            .commit(1);
        ctx.pending_outputs.push_back(ChainEventFactory::data_event(
            WriterId::from(t),
            "bp_test.pending",
            json!({}),
        ));

        let state = TransformState::<ExpandHandler>::Running;
        let mut task =
            tokio_test::task::spawn(async { supervisor.dispatch_state(&state, &mut ctx).await });

        assert_pending!(task.poll());

        let waited = registry
            .metrics_snapshot()
            .stage_wait_nanos_total
            .get(&t)
            .copied()
            .unwrap_or(0);
        assert_eq!(waited, 1_000_000, "1ms initial backoff expected");

        assert_pending!(task.poll());

        // Complete the 1ms sleep and ensure the supervisor returns (no hot loop).
        tokio::time::advance(std::time::Duration::from_millis(2)).await;
        let result = assert_ready!(task.poll());
        assert!(result.is_ok());
        drop(task);

        // Still blocked on the same pending output: the next dispatch should back off to 2ms.
        let mut task =
            tokio_test::task::spawn(async { supervisor.dispatch_state(&state, &mut ctx).await });
        assert_pending!(task.poll());
        let waited = registry
            .metrics_snapshot()
            .stage_wait_nanos_total
            .get(&t)
            .copied()
            .unwrap_or(0);
        assert_eq!(waited, 3_000_000, "1ms + 2ms backoff expected");
    }
}
