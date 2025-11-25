//! Journal sink supervisor implementation using HandlerSupervised pattern

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use futures::TryFutureExt;
use obzenflow_fsm::FsmAction;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::ChainEvent;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmBuilder, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{JournalSinkAction, JournalSinkContext, JournalSinkEvent, JournalSinkState};

/// Supervisor for journal sink stages
pub(crate) struct JournalSinkSupervisor<
    H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// The FSM context containing all mutable state
    pub(crate) context: Arc<JournalSinkContext<H>>,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JournalSinkSupervisor<H> {
    /// Helper to forward control events downstream (data journal)
    async fn forward_control_event(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let forward_event = envelope.event.clone();
        self.data_journal
            .append(forward_event, Some(envelope))
            .await
            .map_err(|e| format!("Failed to forward control event: {}", e))?;
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

    fn configure_fsm(
        &self,
        builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Created -> Initialized
            .when("Created")
                .on("Initialize", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: JournalSinkState::Initialized,
                        actions: vec![JournalSinkAction::AllocateResources],
                    })
                })
                .done()
            
            // Initialized -> Running (sinks auto-start)
            .when("Initialized")
                .on("Ready", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: JournalSinkState::Running,
                        actions: vec![JournalSinkAction::PublishRunning],
                    })
                })
                .done()
            
            // Running -> Drained (on EOF via FSM)
            .when("Running")
                // Idempotent Ready: ignore redundant Ready signals once running
                .on("Ready", |_state, _event, _ctx| async move {
                    tracing::info!("JournalSinkSupervisor: received Ready in Running; treating as no-op");
                    Ok(Transition {
                        next_state: JournalSinkState::Running,
                        actions: vec![],
                    })
                })
                .on("ReceivedEOF", |_state, _event, ctx| async move {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        "JournalSinkSupervisor: ReceivedEOF -> Drained (flush + completion + cleanup)"
                    );
                    Ok(Transition {
                        // We are done consuming: flush any buffered data, send
                        // completion, then clean up resources. The Drained
                        // state is terminal and its dispatch simply returns
                        // Terminate, which will cause HandlerSupervisedExt to
                        // call write_completion_event().
                        next_state: JournalSinkState::Drained,
                        actions: vec![
                            JournalSinkAction::FlushBuffers,
                            JournalSinkAction::SendCompletion,
                            JournalSinkAction::Cleanup,
                        ],
                    })
                })
                .on("BeginFlush", |_state, _event, _ctx| async move {
                    Ok(Transition {
                        next_state: JournalSinkState::Flushing,
                        actions: vec![JournalSinkAction::FlushBuffers],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(msg),
                                actions: vec![JournalSinkAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Flushing -> Draining
            .when("Flushing")
                .on("Ready", |_state, _event, _ctx| async move {
                    tracing::info!("JournalSinkSupervisor: received Ready in Flushing; treating as no-op");
                    Ok(Transition {
                        next_state: JournalSinkState::Flushing,
                        actions: vec![],
                    })
                })
                .on("FlushComplete", |_state, _event, _ctx| async move {
                    tracing::info!(
                        target: "flowip-080o",
                        "JournalSinkSupervisor: FlushComplete -> Draining"
                    );
                    Ok(Transition {
                        next_state: JournalSinkState::Draining,
                        actions: vec![],
                    })
                })
                .on("Error", |_state, event, _ctx| {
                    let event = event.clone();
                    async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(msg),
                                actions: vec![JournalSinkAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    }
                })
                .done()
            
            // Draining -> Drained
            .when("Draining")
                .on("Ready", |_state, _event, _ctx| async move {
                    tracing::info!("JournalSinkSupervisor: received Ready in Draining; treating as no-op");
                    Ok(Transition {
                        next_state: JournalSinkState::Draining,
                        actions: vec![],
                    })
                })
                .on("BeginDrain", |_state, _event, _ctx| async move {
                    tracing::info!(
                        target: "flowip-080o",
                        "JournalSinkSupervisor: BeginDrain -> Drained (SendCompletion + Cleanup)"
                    );
                    Ok(Transition {
                        next_state: JournalSinkState::Drained,
                        actions: vec![JournalSinkAction::SendCompletion, JournalSinkAction::Cleanup],
                    })
                })
                .done()
            
            // Error transitions from any state
            .from_any()
                .on("Error", |state, event, _ctx| {
                    let event = event.clone();
                    let state = state.clone();
                    async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            // If already failed, don't cleanup again
                            if matches!(state, JournalSinkState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: JournalSinkState::Failed(msg),
                                    actions: vec![JournalSinkAction::Cleanup],
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
                        supervisor = "JournalSinkSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    // Return Err to propagate the error
                    Err(format!("Unhandled event '{}' in state '{}' for JournalSinkSupervisor", event_name, state_name))
                }
            })
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

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(
            target: "flowip-080o",
            stage_name = %self.context.stage_name,
            stage_id = ?self.stage_id,
            "sink: HandlerSupervised writing stage_completed"
        );
        let event = SystemEvent::stage_completed(self.stage_id);
        let result = self
            .system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write completion event: {}", e).into());

        if result.is_ok() {
            tracing::info!(
                target: "flowip-080o",
                stage_name = %self.context.stage_name,
                stage_id = ?self.stage_id,
                "sink: HandlerSupervised stage_completed append succeeded"
            );
        }

        result
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
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
                let loop_count = self
                    .context
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
                    loop_iteration = loop_count + 1,
                    "sink: Running state - starting event loop iteration"
                );

                // Check subscription for events
                let mut subscription_guard = self.context.subscription.write().await;

                if let Some(subscription) = subscription_guard.as_mut() {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %self.context.stage_name,
                        loop_iteration = loop_count + 1,
                        "sink: about to call subscription.poll_next()"
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
                                event_id = ?envelope.event.id,
                                "sink: poll_next returned Event"
                            );
                            self.context.instrumentation.record_consumed(&envelope);

                            // Track that we have work
                            self.context
                                .instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tracing::trace!(
                                stage_name = %self.context.stage_name,
                                "Sink processing event"
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
                                                "Sink received EOF via upstream subscription"
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
                                            match signal {
                                                // Treat only EOF as terminal; drain propagates but is non-terminal
                                                FlowControlPayload::Eof { .. } => {
                                                    let eof_outcome =
                                                        subscription.take_last_eof_outcome();
                                                    let _ = subscription.check_contracts().await;

                                                    // Release the subscription lock before any further work
                                                    drop(subscription_guard);

                                                    match eof_outcome {
                                                        Some(outcome) => {
                                                            tracing::info!(
                                                                target: "flowip-080o",
                                                                stage_name = %self.context.stage_name,
                                                                upstream_stage_id = ?outcome.stage_id,
                                                                upstream_stage_name = %outcome.stage_name,
                                                                reader_index = outcome.reader_index,
                                                                eof_count = outcome.eof_count,
                                                                total_readers = outcome.total_readers,
                                                                is_final = outcome.is_final,
                                                                event_type = envelope.event.event_type(),
                                                                "Sink received EOF; evaluated drain decision"
                                                            );

                                                            tracing::info!(
                                                                target: "flowip-080o",
                                                                stage_name = %self.context.stage_name,
                                                                is_final = outcome.is_final,
                                                                ">>> CLAUDE CHECK: EOF OUTCOME RECEIVED, ABOUT TO CHECK is_final <<<"
                                                            );

                                                            if outcome.is_final {
                                                                tracing::info!(
                                                                    target: "flowip-080o",
                                                                    stage_name = %self.context.stage_name,
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
                                                                    stage_name = %self.context.stage_name,
                                                                    "Sink EOF not final; continuing to consume remaining upstreams"
                                                                );
                                                            }
                                                        }
                                                        None => {
                                                            tracing::warn!(
                                                                target: "flowip-080o",
                                                                stage_name = %self.context.stage_name,
                                                                event_type = envelope.event.event_type(),
                                                                "Sink received EOF but no EOF outcome was recorded; continuing"
                                                            );
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    // Forward other control/control-like events to downstream journal
                                                    self.forward_control_event(&envelope).await?;

                                                    // For non-EOF control events, let handler consume if needed
                                                    let envelope_event = envelope.event.clone();
                                                    let mut handler =
                                                        self.context.handler.write().await;

                                                    if let Err(e) =
                                                        handler.consume(envelope_event).await
                                                    {
                                                        tracing::error!(
                                                            stage_name = %self.context.stage_name,
                                                            error = ?e,
                                                            "Failed to consume control event"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                duration = ?duration,
                                                "Sink delaying control event"
                                            );
                                            tokio::time::sleep(duration).await;
                                        }
                                        ControlEventAction::Retry | ControlEventAction::Skip => {
                                            tracing::info!(
                                                stage_name = %self.context.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Sink ignoring control event (Retry/Skip not implemented)"
                                            );
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    let envelope_event = envelope.event.clone();

                                    let ack_result = process_with_instrumentation(
                                        &self.context.instrumentation,
                                        || async {
                                            let mut handler = self.context.handler.write().await;
                                            handler.consume(envelope_event).await
                                            // ← returns Result<DeliveryPayload, Box<…>>
                                        },
                                    )
                                    .await;

                                    match ack_result {
                                        Ok(payload) => {
                                            let flow_context = FlowContext {
                                                flow_name: self.context.flow_name.clone(),
                                                flow_id: self.context.flow_id.to_string(),
                                                stage_name: self.context.stage_name.clone(),
                                                stage_id: self.stage_id.clone(),
                                                stage_type:
                                                    obzenflow_core::event::context::StageType::Sink,
                                            };

                                            let delivery_event = ChainEventFactory::delivery_event(
                                                self.writer_id(),
                                                payload, // <-- just pass it through
                                            )
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                self.context.instrumentation.snapshot(),
                                            )
                                            .with_causality(CausalityContext::with_parent(
                                                envelope.event.id,
                                            ))
                                            .with_correlation_from(&envelope.event);

                                            // FLOWIP-080o-part-2: Only count data/delivery events for writer_seq.
                                            // Lifecycle events (middleware metrics, etc.) are observability
                                            // overhead and should not participate in transport contracts.
                                            // Sink emits Delivery events (not Data), so check both.
                                            if delivery_event.is_data() || delivery_event.is_delivery() {
                                                self.context
                                                    .instrumentation
                                                    .record_emitted(&delivery_event);
                                            }
                                            self.context
                                                .data_journal
                                                .append(delivery_event, Some(&envelope))
                                                .await?;
                                        }
                                        Err(e) => {
                                            let fail_payload = DeliveryPayload::failed(
                                                self.context.stage_name.clone(), // destination
                                                DeliveryMethod::Noop, // or HttpPost { url: … } etc.
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
                                                flow_name: self.context.flow_name.clone(),
                                                flow_id: self.context.flow_id.to_string(),
                                                stage_name: self.context.stage_name.clone(),
                                                stage_id: self.stage_id.clone(),
                                                stage_type: StageType::Sink,
                                            };

                                            let fail_event = ChainEventFactory::delivery_event(
                                                writer_id,
                                                fail_payload,
                                            )
                                            .with_flow_context(flow_ctx)
                                            .with_runtime_context(
                                                self.context.instrumentation.snapshot(),
                                            )
                                            .with_causality(CausalityContext::with_parent(
                                                envelope.event.id,
                                            ))
                                            .with_correlation_from(&envelope.event);

                                            // FLOWIP-080o-part-2: Only count data/delivery events for writer_seq.
                                            // Failure events are still delivery events, so count them.
                                            if fail_event.is_data() || fail_event.is_delivery() {
                                                self.context
                                                    .instrumentation
                                                    .record_emitted(&fail_event);
                                            }
                                            self.context
                                                .data_journal
                                                .append(fail_event, Some(&envelope))
                                                .await
                                                .map_err(|je| {
                                                    format!("Failed to journal sink failure: {je}")
                                                })?;

                                            // propagate the original error up the FSM so the stage can decide
                                            // whether to retry or transition to an error state
                                            return Err(format!("Sink consume failed: {e}").into());
                                        }
                                    }
                                }
                                _ => {
                                    // For other content types, just consume without instrumentation
                                    let envelope_event = envelope.event.clone();
                                    let mut handler = self.context.handler.write().await;

                                    if let Err(e) = handler.consume(envelope_event).await {
                                        // ← add .await
                                        tracing::error!(
                                            stage_name = %self.context.stage_name,
                                            error = ?e,
                                            "Failed to consume control/system event"
                                        );
                                    }
                                }
                            }
                        }
                        PollResult::NoEvents => {
                            // Check contracts periodically while idle to emit progress/final/stall as needed
                            if subscription.should_check_contracts() {
                                let _ = subscription.check_contracts().await;
                            }

                            // No events available right now, sleep briefly
                            tracing::info!(
                                target: "flowip-080o",
                                stage_name = %self.context.stage_name,
                                loop_iteration = loop_count + 1,
                                "sink: poll_next returned NoEvents, sleeping"
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
                                "sink: poll_next returned Error"
                            );
                            drop(subscription_guard);
                            return Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
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
                        "sink: No subscription available, sleeping"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }

                Ok(EventLoopDirective::Continue)
            }

            JournalSinkState::Flushing => {
                // Wait for flush to complete
                // The actual flush happens in the action
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
                    supervisor_state = %state.variant_name(),
                    "sink: Flushing dispatch reached (post-flush), transitioning to Draining"
                );
                tracing::debug!(
                    target: "flowip-080o",
                    stage_name = %self.context.stage_name,
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
                    stage_name = %self.context.stage_name,
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
