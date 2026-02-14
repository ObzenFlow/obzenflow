// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal sink supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::ChainEvent;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{JournalSinkAction, JournalSinkContext, JournalSinkEvent, JournalSinkState};

/// Supervisor for journal sink stages
pub(crate) struct JournalSinkSupervisor<
    H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Human-readable stage name (for logging in methods that don't see Context)
    pub(crate) stage_name: String,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JournalSinkSupervisor<H> {
    /// Helper to forward control events downstream (data journal)
    async fn forward_control_event(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Re-stamp flow context so that control events written to this sink's
        // data journal are always attributed to this stage.
        let mut forward_event = envelope.event.clone();
        let flow_name = forward_event.flow_context.flow_name.clone();
        let flow_id = forward_event.flow_context.flow_id.clone();
        forward_event = forward_event.with_flow_context(FlowContext {
            flow_name,
            flow_id,
            stage_name: self.stage_name.clone(),
            stage_id: self.stage_id,
            stage_type: StageType::Sink,
        });

        // Drop upstream runtime_context; the sink will publish its own
        // snapshots via observability events.
        forward_event.runtime_context = None;
        self.data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {e}"))?;
        Ok(())
    }
}

// Implement Sealed directly for JournalSinkSupervisor to satisfy Supervisor trait bound
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for JournalSinkSupervisor<H>
{
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for JournalSinkSupervisor<H>
{
    type State = JournalSinkState<H>;
    type Event = JournalSinkEvent<H>;
    type Context = JournalSinkContext<H>;
    type Action = JournalSinkAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JournalSinkState<H>;
            event:   JournalSinkEvent<H>;
            context: JournalSinkContext<H>;
            action:  JournalSinkAction<H>;
            initial: initial_state;

            state JournalSinkState::Created {
                on JournalSinkEvent::Initialize => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Initialized,
                            actions: vec![JournalSinkAction::AllocateResources],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Initialized {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Running,
                            actions: vec![JournalSinkAction::PublishRunning],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Running {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Running; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Running,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::ReceivedEOF => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            "JournalSinkSupervisor: ReceivedEOF -> Drained (flush + completion + cleanup)"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Drained,
                            actions: vec![
                                JournalSinkAction::FlushBuffers,
                                JournalSinkAction::SendCompletion,
                                JournalSinkAction::Cleanup,
                            ],
                        })
                    })
                };

                on JournalSinkEvent::BeginFlush => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Flushing,
                            actions: vec![JournalSinkAction::FlushBuffers],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Flushing {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Flushing; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Flushing,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::FlushComplete => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            target: "flowip-080o",
                            "JournalSinkSupervisor: FlushComplete -> Draining"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Draining {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Draining; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::BeginDrain => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            target: "flowip-080o",
                            "JournalSinkSupervisor: BeginDrain -> Drained (SendCompletion + Cleanup)"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Drained,
                            actions: vec![JournalSinkAction::SendCompletion, JournalSinkAction::Cleanup],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Drained {
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Failed {
                on JournalSinkEvent::Error => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(_msg) = event {
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

            unhandled => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "JournalSinkSupervisor",
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
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for JournalSinkSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> JournalSinkEvent<H> {
        JournalSinkEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        tracing::info!(
            target: "flowip-080o",
            stage_name = %self.stage_name,
            stage_id = ?self.stage_id,
            "sink: HandlerSupervised writing stage_completed"
        );

        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                target: "flowip-080o",
                stage_name = %self.stage_name,
                stage_id = ?self.stage_id,
                journal_error = %e,
                "sink: HandlerSupervised failed to append stage_completed; continuing without system journal entry"
            );
        } else {
            tracing::info!(
                target: "flowip-080o",
                stage_name = %self.stage_name,
                stage_id = ?self.stage_id,
                "sink: HandlerSupervised stage_completed append succeeded"
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
            JournalSinkState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            JournalSinkState::Initialized => {
                // Auto-transition to ready
                Ok(EventLoopDirective::Transition(JournalSinkEvent::Ready))
            }

            JournalSinkState::Running => {
                // Track event loop
                let loop_count = ctx
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "sink: Running state - starting event loop iteration"
                );

                // Take ownership of subscription and contract state so we never hold
                // a borrow of ctx across await while operating on them.
                let maybe_subscription = ctx.subscription.take();
                let mut contract_state = std::mem::take(&mut ctx.contract_state);

                // Default directive is to continue; branches below may override it.
                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);

                if let Some(mut subscription) = maybe_subscription {
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        loop_iteration = loop_count + 1,
                        "sink: about to call subscription.poll_next()"
                    );

                    let poll_result = subscription
                        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
                        .await;

                    match poll_result {
                        PollResult::Event(envelope) => {
                            use obzenflow_core::event::JournalEvent;
                            tracing::trace!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                event_type = %envelope.event.event_type_name(),
                                event_id = ?envelope.event.id,
                                "sink: poll_next returned Event"
                            );
                            ctx.instrumentation.record_consumed(&envelope);

                            // Track that we have work
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tracing::trace!(
                                stage_name = %ctx.stage_name,
                                "Sink processing event"
                            );

                            match &envelope.event.content {
                                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                                    let mut processing_ctx = ProcessingContext::new();
                                    let mut action = match signal {
                                        FlowControlPayload::Eof { natural, .. } => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                natural = natural,
                                                writer_id = ?envelope.event.writer_id,
                                                "Sink received EOF via upstream subscription"
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
                                                    "Sink delaying control event"
                                                );
                                                tokio::time::sleep(duration).await;
                                                action = ControlEventAction::Forward;
                                            }
                                            ControlEventAction::Forward => {
                                                match signal {
                                                    // Treat only EOF as terminal; drain propagates but is non-terminal
                                                    FlowControlPayload::Eof { .. } => {
                                                        let eof_outcome =
                                                            subscription.take_last_eof_outcome();
                                                        let _ = subscription
                                                            .check_contracts(
                                                                &mut contract_state[..],
                                                            )
                                                            .await;

                                                        // Capture upstream reader count for logging before
                                                        // releasing the subscription lock.
                                                        let upstream_readers =
                                                            subscription.upstream_count();

                                                        match eof_outcome {
                                                            Some(outcome) => {
                                                                tracing::info!(
                                                                    target: "flowip-080o",
                                                                    stage_name = %ctx.stage_name,
                                                                    upstream_stage_id = ?outcome.stage_id,
                                                                    upstream_stage_name = %outcome.stage_name,
                                                                    reader_index = outcome.reader_index,
                                                                    eof_count = outcome.eof_count,
                                                                    total_readers = outcome.total_readers,
                                                                    is_final = outcome.is_final,
                                                                    event_type = envelope.event.event_type(),
                                                                    "Sink received EOF; evaluated drain decision"
                                                                );

                                                                if outcome.is_final {
                                                                    tracing::info!(
                                                                        target: "flowip-080o",
                                                                        stage_name = %ctx.stage_name,
                                                                        "Sink EOF is final; triggering FSM transition to Drained"
                                                                    );

                                                                    // FLOWIP-080o-part-2: Use FSM properly.
                                                                    // Return Transition(ReceivedEOF) to let the FSM handle
                                                                    // the Running -> Drained transition and execute actions:
                                                                    // [FlushBuffers, SendCompletion, Cleanup]
                                                                    //
                                                                    // Previously this returned Terminate directly, bypassing
                                                                    // the FSM and skipping flush/cleanup (which caused hangs
                                                                    // when attempted inline due to lock contention).
                                                                    return Ok(
                                                                    EventLoopDirective::Transition(
                                                                        JournalSinkEvent::ReceivedEOF,
                                                                    ),
                                                                );
                                                                } else {
                                                                    tracing::info!(
                                                                        stage_name = %ctx.stage_name,
                                                                    "Sink EOF not final; continuing to consume remaining upstreams"
                                                                    );
                                                                }
                                                            }
                                                            None => {
                                                                tracing::info!(
                                                                    target: "flowip-080o",
                                                                    stage_name = %ctx.stage_name,
                                                                    event_type = envelope.event.event_type(),
                                                                    writer_id = ?envelope.event.writer_id,
                                                                    upstream_readers = upstream_readers,
                                                                    "Sink received EOF authored by a non-upstream writer; ignoring for EOF authority and continuing to consume"
                                                                );
                                                            }
                                                        }
                                                    }
                                                    _ => {
                                                        // Forward other control/control-like events to downstream journal
                                                        self.forward_control_event(&envelope)
                                                            .await?;

                                                        // For non-EOF control events, let handler consume if needed
                                                        let envelope_event = envelope.event.clone();
                                                        let handler = &mut ctx.handler;

                                                        if let Err(e) =
                                                            handler.consume(envelope_event).await
                                                        {
                                                            tracing::error!(
                                                                stage_name = %ctx.stage_name,
                                                                error = ?e,
                                                                "Failed to consume control event"
                                                            );
                                                        }
                                                    }
                                                }

                                                break;
                                            }
                                            ControlEventAction::Retry
                                            | ControlEventAction::Skip => {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    "Sink ignoring control event (Retry/Skip not implemented)"
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    use obzenflow_core::event::status::processing_status::{
                                        ErrorKind, ProcessingStatus,
                                    };

                                    let envelope_event = envelope.event.clone();
                                    let stage_name = ctx.stage_name.clone();

                                    // Use instrumentation wrapper but keep handler-level failures
                                    // as per-record outcomes instead of stage-fatal errors. The
                                    // closure returns a tuple of:
                                    //   (DeliveryPayload, Option<HandlerError>)
                                    // so we can journal both the delivery outcome and an error
                                    // event with ErrorKind.
                                    let ack_result = process_with_instrumentation(
                                        &ctx.instrumentation,
                                        || async {
                                            let handler = &mut ctx.handler;
                                            let result = handler.consume(envelope_event).await;

                                            match result {
                                                Ok(mut payload) => {
                                                    payload.destination = stage_name.clone();
                                                    Ok((payload, None))
                                                }
                                                Err(err) => {
                                                    let fail_payload = DeliveryPayload::failed(
                                                        stage_name.clone(), // destination
                                                        DeliveryMethod::Noop,
                                                        "sink_error",
                                                        err.to_string(),
                                                        /* final_attempt */ false,
                                                    );
                                                    Ok((fail_payload, Some(err)))
                                                }
                                            }
                                        },
                                    )
                                    .await;

                                    match ack_result {
                                        Ok((payload, maybe_err)) => {
                                            let flow_context = FlowContext {
                                                flow_name: ctx.flow_name.clone(),
                                                flow_id: ctx.flow_id.to_string(),
                                                stage_name: ctx.stage_name.clone(),
                                                stage_id: self.stage_id,
                                                stage_type:
                                                    obzenflow_core::event::context::StageType::Sink,
                                            };

                                            let delivery_event = ChainEventFactory::delivery_event(
                                                self.writer_id(),
                                                payload,
                                            )
                                            .with_flow_context(flow_context)
                                            .with_causality(CausalityContext::with_parent(
                                                envelope.event.id,
                                            ))
                                            .with_correlation_from(&envelope.event);

                                            // FLOWIP-080o-part-2: Only count data/delivery events for writer_seq.
                                            // Lifecycle events (middleware metrics, etc.) are observability
                                            // overhead and should not participate in transport contracts.
                                            // Sink emits Delivery events (not Data), so check both.
                                            if delivery_event.is_data()
                                                || delivery_event.is_delivery()
                                            {
                                                // Track emitted deliveries for writer_seq and observability,
                                                // but DO NOT bump events_processed_total here. For sinks,
                                                // process_with_instrumentation already increments that counter
                                                // once per successfully processed input event. Bumping it again
                                                // here would double-count and produce inflated events_out_total
                                                // in flow lifecycle snapshots. See FLOWIP-059d postmortem.
                                                ctx.instrumentation
                                                    .record_output_event(&delivery_event);
                                            }
                                            let delivery_event = delivery_event
                                                .with_runtime_context(
                                                    ctx.instrumentation.snapshot_with_control(),
                                                );
                                            let written = ctx
                                                .data_journal
                                                .append(delivery_event, Some(&envelope))
                                                .await?;
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &self.system_journal,
                                            )
                                            .await;

                                            // If the handler returned a per-record error, surface it
                                            // as an error-marked event routed by ErrorKind policy.
                                            if let Some(handler_err) = maybe_err {
                                                // Per-record handler failures should be reflected
                                                // in errors_total for lifecycle / flow snapshots,
                                                // even though they are not stage-fatal.
                                                ctx.instrumentation
                                                    .record_error(handler_err.kind());
                                                let reason =
                                                    format!("Sink handler error: {handler_err:?}");
                                                let error_event = envelope
                                                    .event
                                                    .clone()
                                                    .mark_as_error(reason, handler_err.kind());

                                                let route_to_error_journal =
                                                    match &error_event.processing_info.status {
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
                                                        event_id = %error_event.id,
                                                        "Writing sink error event to error journal (FLOWIP-082h)"
                                                    );

                                                    if error_event.is_data() {
                                                        ctx.instrumentation
                                                            .record_output_event(&error_event);
                                                    }

                                                    ctx.error_journal
                                                        .append(error_event, Some(&envelope))
                                                        .await
                                                        .map_err(|je| {
                                                            format!(
                                                                "Failed to journal sink error event: {je}"
                                                            )
                                                        })?;
                                                } else {
                                                    let flow_ctx = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id,
                                                        stage_type: StageType::Sink,
                                                    };

                                                    let enriched_error =
                                                        error_event.with_flow_context(flow_ctx);

                                                    if enriched_error.is_data() {
                                                        ctx.instrumentation
                                                            .record_output_event(&enriched_error);
                                                    }
                                                    let enriched_error = enriched_error
                                                        .with_runtime_context(
                                                            ctx.instrumentation
                                                                .snapshot_with_control(),
                                                        );

                                                    ctx.data_journal
                                                        .append(enriched_error, Some(&envelope))
                                                        .await
                                                        .map_err(|je| {
                                                            format!(
                                                                "Failed to write sink error event to data journal: {je}"
                                                            )
                                                        })?;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            // Instrumentation-level or unexpected failure: treat as
                                            // stage-fatal and surface as before.
                                            let fail_payload = DeliveryPayload::failed(
                                                ctx.stage_name.clone(), // destination
                                                DeliveryMethod::Noop,
                                                "sink_error",
                                                e.to_string(),
                                                /* final_attempt */ false,
                                            );

                                            let writer_id = self.writer_id();

                                            let flow_ctx = FlowContext {
                                                flow_name: ctx.flow_name.clone(),
                                                flow_id: ctx.flow_id.to_string(),
                                                stage_name: ctx.stage_name.clone(),
                                                stage_id: self.stage_id,
                                                stage_type: StageType::Sink,
                                            };

                                            let fail_event = ChainEventFactory::delivery_event(
                                                writer_id,
                                                fail_payload,
                                            )
                                            .with_flow_context(flow_ctx)
                                            .with_causality(CausalityContext::with_parent(
                                                envelope.event.id,
                                            ))
                                            .with_correlation_from(&envelope.event);

                                            if fail_event.is_data() || fail_event.is_delivery() {
                                                ctx.instrumentation
                                                    .record_output_event(&fail_event);
                                            }
                                            let fail_event = fail_event.with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );
                                            let written = ctx
                                                .data_journal
                                                .append(fail_event, Some(&envelope))
                                                .await
                                                .map_err(|je| {
                                                    format!("Failed to journal sink failure: {je}")
                                                })?;
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &self.system_journal,
                                            )
                                            .await;

                                            directive =
                                                Err(format!("Sink consume failed: {e}").into());
                                        }
                                    }
                                }
                                _ => {
                                    // For other content types, just consume without instrumentation
                                    let envelope_event = envelope.event.clone();
                                    let handler = &mut ctx.handler;

                                    if let Err(e) = handler.consume(envelope_event).await {
                                        // ← add .await
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            error = ?e,
                                            "Failed to consume control/system event"
                                        );
                                    }
                                }
                            }

                            // Backpressure ack: upstream input was consumed by sink handler.
                            if envelope.event.is_data() {
                                if let Some(upstream) = subscription.last_delivered_upstream_stage()
                                {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                            }
                        }
                        PollResult::NoEvents => {
                            // Check contracts periodically while idle to emit progress/final/stall as needed
                            if subscription.should_check_contracts(&contract_state[..]) {
                                let _ = subscription.check_contracts(&mut contract_state[..]).await;
                            }

                            // No events available right now, sleep briefly
                            tracing::trace!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                "sink: poll_next returned NoEvents, sleeping"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                loop_iteration = loop_count + 1,
                                error = ?e,
                                "sink: poll_next returned Error"
                            );
                            directive = Ok(EventLoopDirective::Transition(
                                JournalSinkEvent::Error(format!("Subscription error: {e}")),
                            ));
                        }
                    }

                    // Put subscription back into context so subsequent iterations can continue
                    ctx.subscription = Some(subscription);
                } else {
                    // No subscription yet, wait
                    tracing::warn!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        loop_iteration = loop_count + 1,
                        "sink: No subscription available, sleeping"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                // Always restore contract state before returning, even on error.
                ctx.contract_state = contract_state;

                directive
            }

            JournalSinkState::Flushing => {
                // Wait for flush to complete
                // The actual flush happens in the action
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    supervisor_state = %state.variant_name(),
                    "sink: Flushing dispatch reached (post-flush), transitioning to Draining"
                );
                tracing::debug!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: Flushing -> Draining transition firing"
                );
                Ok(EventLoopDirective::Transition(
                    JournalSinkEvent::FlushComplete,
                ))
            }

            JournalSinkState::Draining => {
                // Move to drained state; completion + cleanup are handled
                // by the FSM transition (BeginDrain) rather than here.
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: Draining complete, sending completion and cleaning up"
                );
                Ok(EventLoopDirective::Transition(JournalSinkEvent::BeginDrain))
            }

            JournalSinkState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}
