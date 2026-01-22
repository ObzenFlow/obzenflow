//! Join supervisor implementation using HandlerSupervised pattern

use crate::messaging::{PollResult, UpstreamSubscription};
use crate::metrics::instrumentation::heartbeat_interval;
use crate::stages::common::control_strategies::{ControlEventAction, ProcessingContext};
use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, HandlerSupervised};
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use super::config::JoinReferenceMode;
use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState, PendingTransition};

/// Supervisor for join stages
pub(crate) struct JoinSupervisor<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
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
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
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
                        let next_state = match ctx.reference_mode {
                            JoinReferenceMode::FiniteEof => JoinState::Hydrating,
                            JoinReferenceMode::Live => JoinState::Live,
                        };

                        let next_state_name = match ctx.reference_mode {
                            JoinReferenceMode::FiniteEof => "Hydrating",
                            JoinReferenceMode::Live => "Live",
                        };
                        ctx.instrumentation.transition_to_state(next_state_name);
                        Ok(Transition {
                            next_state,
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
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
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
                            // Flush any pending reference-side heartbeat before
                            // transitioning to Enriching so metrics snapshots
                            // include all hydrating activity.
                            if ctx.events_since_last_heartbeat > 0 {
                                if let Err(e) =
                                    emit_join_heartbeat_if_due_impl(ctx, ctx.stage_id).await
                                {
                                    tracing::warn!(
                                        stage_name = %ctx.stage_name,
                                        error = ?e,
                                        "Failed to emit final join hydration heartbeat"
                                    );
                                }
                            }

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
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Live {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Live; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Live,
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
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
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
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
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

    fn event_for_action_error(&self, msg: String) -> JoinEvent<H> {
        JoinEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_name = %self.stage_name,
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
        tracing::debug!(
            stage_name = %ctx.stage_name,
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

                let loop_count = ctx
                    .instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                tracing::trace!(
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "Hydrating - checking reference subscription"
                );

                // Take ownership of subscription + contract state so no borrow of ctx lives across .await
                let mut ref_subscription = ctx.reference_subscription.take();
                let mut ref_contract_state = std::mem::take(&mut ctx.reference_contract_state);

                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);

                if let Some(ref mut subscription) = ref_subscription {
                    tracing::trace!(
                        stage_name = %ctx.stage_name,
                        "Have reference subscription, attempting to receive"
                    );

                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut ref_contract_state[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Received event from reference"
                            );

                            // We have work this iteration.
                            ctx.instrumentation.record_consumed(&envelope);
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            match &envelope.event.content {
                                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                                    let mut processing_ctx = ProcessingContext::new();

                                    let action = match signal {
                                        FlowControlPayload::Eof { natural, .. } => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                natural = natural,
                                                writer_id = ?envelope.event.writer_id,
                                                "Join received EOF via reference_subscription (reference complete)"
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

                                    match action {
                                        ControlEventAction::Forward => {
                                            // Always forward control events downstream
                                            let written = self
                                                .data_journal
                                                .append(envelope.event.clone(), None)
                                                .await?;
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &self.system_journal,
                                            )
                                            .await;

                                            if matches!(signal, FlowControlPayload::Eof { .. }) {
                                                // FLOWIP-080p: use EofOutcome to decide when the
                                                // reference side is truly complete.
                                                let eof_outcome =
                                                    subscription.take_last_eof_outcome();
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
                                                        "Join (Hydrating) evaluated EOF outcome for reference side"
                                                    );

                                                    if outcome.is_final {
                                                        // Reference EOF is final -> transition to Enriching
                                                        directive =
                                                            Ok(EventLoopDirective::Transition(
                                                                JoinEvent::ReceivedEOF,
                                                            ));
                                                    }
                                                }
                                            }
                                        }
                                        ControlEventAction::Delay(duration) => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                event_type = envelope.event.event_type(),
                                                duration = ?duration,
                                                "Join delaying control event during Hydrating"
                                            );
                                            tokio::time::sleep(duration).await;
                                        }
                                        ControlEventAction::Retry | ControlEventAction::Skip => {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                event_type = envelope.event.event_type(),
                                                "Join ignoring control event (Retry/Skip not implemented) during Hydrating"
                                            );
                                        }
                                    }
                                }
                                obzenflow_core::event::ChainEventContent::Data { .. } => {
                                    // Process reference event to build catalog
                                    // NO output during hydration - just building the catalog
                                    tracing::debug!(
                                        stage_name = %ctx.stage_name,
                                        "Processing reference event to build catalog"
                                    );

                                    let event = envelope.event.clone();
                                    let reference_stage_id = ctx.reference_stage_id;

                                    // Get writer ID for the handler
                                    let writer_id =
                                        ctx.writer_id.ok_or("No writer ID available")?;

                                    // Instrument synchronous join processing without cloning the (potentially large)
                                    // join state on every reference event.
                                    ctx.instrumentation
                                        .in_flight_count
                                        .fetch_add(1, Ordering::Relaxed);
                                    let start = Instant::now();
                                    let result = ctx.handler.process_event(
                                        &mut ctx.handler_state,
                                        event,
                                        reference_stage_id,
                                        writer_id,
                                    );
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

                                    // Join handler errors are handled per-record (or stage-fatal in Hydrating),
                                    // but the input event was still processed by the join stage.
                                    ctx.instrumentation
                                        .events_processed_total
                                        .fetch_add(1, Ordering::Relaxed);

                                    match result {
                                        Ok(events_produced) => {
                                            ctx.instrumentation
                                                .events_accumulated_total
                                                .fetch_add(1, Ordering::Relaxed);

                                            tracing::debug!(
                                                stage_name = %ctx.stage_name,
                                                events_count = events_produced.len(),
                                                "Handler produced events during hydration (should be 0)"
                                            );

                                            // Track reference events for heartbeat snapshots.
                                            ctx.events_since_last_heartbeat =
                                                ctx.events_since_last_heartbeat.saturating_add(1);
                                            if let Err(e) =
                                                self.emit_join_heartbeat_if_due(ctx).await
                                            {
                                                tracing::warn!(
                                                    stage_name = %ctx.stage_name,
                                                    error = ?e,
                                                    "Failed to emit join hydration heartbeat"
                                                );
                                            }
                                        }
                                        Err(err) => {
                                            tracing::error!(
                                                stage_name = %ctx.stage_name,
                                                error = ?err,
                                                "Join handler error during reference hydration"
                                            );
                                            // Stage-fatal handler error: record it in error metrics.
                                            ctx.instrumentation.record_error(err.kind());
                                            directive = Ok(EventLoopDirective::Transition(
                                                JoinEvent::Error(format!(
                                                    "Join handler hydration error: {err:?}"
                                                )),
                                            ));
                                        }
                                    }

                                    // Backpressure ack: reference input was consumed into join state.
                                    if envelope.event.is_data() {
                                        if let Some(upstream) =
                                            subscription.last_delivered_upstream_stage()
                                        {
                                            if let Some(reader) =
                                                ctx.backpressure_readers.get(&upstream)
                                            {
                                                reader.ack_consumed(1);
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    tracing::warn!(
                                        stage_name = %ctx.stage_name,
                                        event_type = envelope.event.event_type(),
                                        "Join received unexpected event content type during Hydrating"
                                    );
                                }
                            }
                        }
                        PollResult::NoEvents => {
                            // Check contracts if appropriate
                            if subscription.should_check_contracts(&ref_contract_state[..]) {
                                match subscription
                                    .check_contracts(&mut ref_contract_state[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            "Reference upstream stalled during join loading"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Reference contract violation during join loading"
                                        );
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            error = %e,
                                            "Failed to check reference contracts"
                                        );
                                    }
                                }
                            }

                            // No events available right now, sleep briefly and continue
                            tracing::trace!(
                                stage_name = %ctx.stage_name,
                                "No reference events available, sleeping"
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        PollResult::Error(e) => {
                            tracing::error!("Reference subscription error: {}", e);
                            directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                format!("Reference subscription error: {e}"),
                            )));
                        }
                    }
                } else {
                    // No subscription yet, continue
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        "No reference subscription available in Hydrating state"
                    );
                }

                // Restore contract state and subscription back into the context
                ctx.reference_contract_state = ref_contract_state;
                ctx.reference_subscription = ref_subscription;

                directive
            }

            JoinState::Live => self.dispatch_live(ctx).await,

            JoinState::Enriching => {
                // Process stream events (reference is already complete)
                // Following the robust pattern from Transform's Running state

                ctx.instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Take ownership of subscription + contract state so no borrow of ctx lives across .await
                let mut stream_subscription = ctx.stream_subscription.take();
                let mut stream_contract_state = std::mem::take(&mut ctx.stream_contract_state);

                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);

                if let Some(ref mut subscription) = stream_subscription {
                    // Drain pending outputs before polling upstream again (bounded to one input).
                    let mut blocked_on_backpressure = false;
                    while let Some(pending) = ctx.pending_outputs.pop_front() {
                        if pending.is_data() {
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
                                                stage_type: StageType::Join,
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

                                            match self.data_journal.append(event, None).await {
                                                Ok(written) => {
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &self.system_journal,
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
                                        stage_type: StageType::Join,
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

                                    match self.data_journal.append(event, None).await {
                                        Ok(written) => {
                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &self.system_journal,
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
                                blocked_on_backpressure = true;
                                break;
                            };

                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: StageType::Join,
                            };

                            let enriched = pending
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            if enriched.is_data() {
                                ctx.instrumentation.record_output_event(&enriched);
                                subscription.track_output_event();
                            }

                            let written = self.data_journal.append(enriched, None).await?;
                            reservation.commit(1);
                            ctx.backpressure_backoff.reset();
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &self.system_journal,
                            )
                            .await;
                        } else {
                            let flow_context = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: self.stage_id,
                                stage_type: StageType::Join,
                            };

                            let enriched = pending
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                            let written = self.data_journal.append(enriched, None).await?;
                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                &written,
                                &self.system_journal,
                            )
                            .await;
                        }
                    }

                    if !blocked_on_backpressure {
                        if let Some(upstream) = ctx.pending_ack_upstream.take() {
                            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                reader.ack_consumed(1);
                            }
                        }

                        match subscription
                            .poll_next_with_state(
                                state.variant_name(),
                                Some(&mut stream_contract_state[..]),
                            )
                            .await
                        {
                            PollResult::Event(envelope) => {
                                // We have work on this loop iteration.
                                ctx.instrumentation.record_consumed(&envelope);
                                ctx.instrumentation
                                    .event_loops_with_work_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                // Match on event content type like Transform does
                                match &envelope.event.content {
                                    obzenflow_core::event::ChainEventContent::FlowControl(
                                        signal,
                                    ) => {
                                        let mut processing_ctx = ProcessingContext::new();

                                        let action = match signal {
                                            FlowControlPayload::Eof { natural, .. } => {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    natural = natural,
                                                    writer_id = ?envelope.event.writer_id,
                                                    "Join received EOF via stream_subscription (stream complete)"
                                                );
                                                // Buffer EOF for downstream forwarding
                                                ctx.buffered_eof = Some(envelope.event.clone());
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

                                        match action {
                                            ControlEventAction::Forward => {
                                                // Always forward control events downstream
                                                let written = self
                                                    .data_journal
                                                    .append(envelope.event.clone(), None)
                                                    .await?;
                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                &written,
                                                &self.system_journal,
                                            )
                                            .await;

                                                // Use EofOutcome from the stream subscription to decide
                                                // when the join can move to Draining.
                                                if matches!(signal, FlowControlPayload::Eof { .. })
                                                {
                                                    let eof_outcome =
                                                        subscription.take_last_eof_outcome();
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
                                                            "Join (Enriching) evaluated EOF outcome for stream side"
                                                        );

                                                        if outcome.is_final {
                                                            directive =
                                                                Ok(EventLoopDirective::Transition(
                                                                    JoinEvent::ReceivedEOF,
                                                                ));
                                                        }
                                                    }
                                                }
                                            }
                                            ControlEventAction::Delay(duration) => {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
                                                    event_type = envelope.event.event_type(),
                                                    duration = ?duration,
                                                    "Join delaying control event"
                                                );
                                                tokio::time::sleep(duration).await;
                                            }
                                            ControlEventAction::Retry
                                            | ControlEventAction::Skip => {
                                                tracing::info!(
                                                    stage_name = %ctx.stage_name,
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
                                            .ok_or("Event writer is not a stage")?;

                                        // Process data event through handler, instrumented for metrics
                                        let event = envelope.event.clone();

                                        // Get writer ID for the handler
                                        let writer_id =
                                            ctx.writer_id.ok_or("No writer ID available")?;

                                        ctx.instrumentation
                                            .in_flight_count
                                            .fetch_add(1, Ordering::Relaxed);
                                        let start = Instant::now();
                                        let result = ctx.handler.process_event(
                                            &mut ctx.handler_state,
                                            event,
                                            source_id,
                                            writer_id,
                                        );
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

                                        match result {
                                            Ok(events) => {
                                                let mut pending_data_outputs: VecDeque<ChainEvent> =
                                                    events.into();

                                                while let Some(event) =
                                                    pending_data_outputs.pop_front()
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
                                                                if let Some(pulse) = ctx.backpressure_pulse.maybe_emit() {
                                                                    let flow_context = FlowContext {
                                                                        flow_name: ctx.flow_name.clone(),
                                                                        flow_id: ctx.flow_id.to_string(),
                                                                        stage_name: ctx.stage_name.clone(),
                                                                        stage_id: self.stage_id,
                                                                        stage_type: StageType::Join,
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

                                                                    match self.data_journal.append(event, None).await {
                                                                        Ok(written) => {
                                                                            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                                &written,
                                                                                &self.system_journal,
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
                                                            ctx.pending_ack_upstream =
                                                                Some(source_id);
                                                            ctx.pending_outputs.push_back(event);
                                                            ctx.pending_outputs
                                                                .extend(pending_data_outputs);
                                                            let delay = ctx
                                                                .backpressure_backoff
                                                                .next_delay();
                                                            ctx.backpressure_writer
                                                                .record_wait(delay);

                                                            if let Some((min_credit, limiting)) =
                                                                ctx.backpressure_writer
                                                                    .min_downstream_credit_detail()
                                                            {
                                                                ctx.backpressure_pulse
                                                                    .record_delay(
                                                                        delay,
                                                                        Some(min_credit),
                                                                        Some(limiting),
                                                                    );
                                                            } else {
                                                                ctx.backpressure_pulse
                                                                    .record_delay(
                                                                        delay, None, None,
                                                                    );
                                                            }
                                                            if let Some(pulse) =
                                                                ctx.backpressure_pulse.maybe_emit()
                                                            {
                                                                let flow_context = FlowContext {
                                                                    flow_name: ctx
                                                                        .flow_name
                                                                        .clone(),
                                                                    flow_id: ctx
                                                                        .flow_id
                                                                        .to_string(),
                                                                    stage_name: ctx
                                                                        .stage_name
                                                                        .clone(),
                                                                    stage_id: self.stage_id,
                                                                    stage_type: StageType::Join,
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

                                                                match self
                                                                    .data_journal
                                                                    .append(event, None)
                                                                    .await
                                                                {
                                                                    Ok(written) => {
                                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                        &written,
                                                                        &self.system_journal,
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
                                                            stage_type: StageType::Join,
                                                        };

                                                        let enriched_event = event
                                                            .with_flow_context(flow_context)
                                                            .with_runtime_context(
                                                                ctx.instrumentation
                                                                    .snapshot_with_control(),
                                                            );

                                                        if enriched_event.is_data() {
                                                            ctx.instrumentation
                                                                .record_output_event(
                                                                    &enriched_event,
                                                                );
                                                            subscription.track_output_event();
                                                        }

                                                        let written = self
                                                            .data_journal
                                                            .append(enriched_event, None)
                                                            .await?;
                                                        reservation.commit(1);
                                                        ctx.backpressure_backoff.reset();
                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &self.system_journal,
                                                    )
                                                    .await;
                                                    } else {
                                                        let flow_context = FlowContext {
                                                            flow_name: ctx.flow_name.clone(),
                                                            flow_id: ctx.flow_id.to_string(),
                                                            stage_name: ctx.stage_name.clone(),
                                                            stage_id: self.stage_id,
                                                            stage_type: StageType::Join,
                                                        };

                                                        let enriched_event = event
                                                            .with_flow_context(flow_context)
                                                            .with_runtime_context(
                                                                ctx.instrumentation
                                                                    .snapshot_with_control(),
                                                            );

                                                        let written = self
                                                            .data_journal
                                                            .append(enriched_event, None)
                                                            .await?;
                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &self.system_journal,
                                                    )
                                                    .await;
                                                    }
                                                }

                                                // Backpressure ack: stream input consumed by join handler.
                                                if ctx.pending_outputs.is_empty() {
                                                    if let Some(reader) =
                                                        ctx.backpressure_readers.get(&source_id)
                                                    {
                                                        reader.ack_consumed(1);
                                                    }
                                                }

                                                directive = Ok(EventLoopDirective::Continue);
                                            }
                                            Err(err) => {
                                                // Per-record join enrichment failure: turn input into an error-marked event
                                                use obzenflow_core::event::status::processing_status::{
                                                ErrorKind, ProcessingStatus,
                                            };

                                                let reason = format!(
                                                    "Join handler error during enrichment: {err:?}"
                                                );
                                                let error_event = envelope
                                                    .event
                                                    .clone()
                                                    .mark_as_error(reason, err.kind());

                                                // Count all error-marked events for lifecycle / flow rollups,
                                                // even when they are not stage-fatal.
                                                ctx.instrumentation.record_error(err.kind());

                                                let route_to_error_journal =
                                                    match &error_event.processing_info.status {
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
                                                    ctx.error_journal
                                                        .append(error_event, None)
                                                        .await
                                                        .map_err(|e| {
                                                            format!(
                                                        "Failed to write join error event: {e}"
                                                    )
                                                        })?;
                                                } else if error_event.is_data() {
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
                                                                    stage_type: StageType::Join,
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

                                                                match self.data_journal.append(event, None).await {
                                                                    Ok(written) => {
                                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                            &written,
                                                                            &self.system_journal,
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

                                                    if let Some(reservation) =
                                                        ctx.backpressure_writer.reserve(1)
                                                    {
                                                        let flow_context = FlowContext {
                                                            flow_name: ctx.flow_name.clone(),
                                                            flow_id: ctx.flow_id.to_string(),
                                                            stage_name: ctx.stage_name.clone(),
                                                            stage_id: self.stage_id,
                                                            stage_type: StageType::Join,
                                                        };

                                                        let enriched_error = error_event
                                                            .with_flow_context(flow_context)
                                                            .with_runtime_context(
                                                                ctx.instrumentation
                                                                    .snapshot_with_control(),
                                                            );

                                                        if enriched_error.is_data() {
                                                            ctx.instrumentation
                                                                .record_output_event(
                                                                    &enriched_error,
                                                                );
                                                            subscription.track_output_event();
                                                        }

                                                        let written = self
                                                            .data_journal
                                                            .append(enriched_error, None)
                                                            .await?;
                                                        reservation.commit(1);
                                                        ctx.backpressure_backoff.reset();
                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                        &written,
                                                        &self.system_journal,
                                                    )
                                                    .await;
                                                    } else {
                                                        ctx.pending_ack_upstream = Some(source_id);
                                                        ctx.pending_outputs.push_back(error_event);
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
                                                                stage_type: StageType::Join,
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

                                                            match self
                                                                .data_journal
                                                                .append(event, None)
                                                                .await
                                                            {
                                                                Ok(written) => {
                                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                    &written,
                                                                    &self.system_journal,
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
                                                    }
                                                } else {
                                                    let flow_context = FlowContext {
                                                        flow_name: ctx.flow_name.clone(),
                                                        flow_id: ctx.flow_id.to_string(),
                                                        stage_name: ctx.stage_name.clone(),
                                                        stage_id: self.stage_id,
                                                        stage_type: StageType::Join,
                                                    };

                                                    let enriched_error = error_event
                                                        .with_flow_context(flow_context)
                                                        .with_runtime_context(
                                                            ctx.instrumentation
                                                                .snapshot_with_control(),
                                                        );

                                                    let written = self
                                                        .data_journal
                                                        .append(enriched_error, None)
                                                        .await?;
                                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                    &written,
                                                    &self.system_journal,
                                                )
                                                .await;
                                                }

                                                // Backpressure ack: stream input consumed by join handler.
                                                if ctx.pending_outputs.is_empty() {
                                                    if let Some(reader) =
                                                        ctx.backpressure_readers.get(&source_id)
                                                    {
                                                        reader.ack_consumed(1);
                                                    }
                                                }

                                                directive = Ok(EventLoopDirective::Continue);
                                            }
                                        }
                                    }
                                    _ => {
                                        // Other content types - log and continue
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            event_type = envelope.event.event_type(),
                                            "Join received unexpected event content type"
                                        );
                                        directive = Ok(EventLoopDirective::Continue);
                                    }
                                }
                            }
                            PollResult::NoEvents => {
                                // Check contracts if appropriate
                                if subscription.should_check_contracts(&stream_contract_state[..]) {
                                    match subscription
                                    .check_contracts(&mut stream_contract_state[..])
                                    .await
                                {
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream)) => {
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            "Stream upstream stalled during join enriching"
                                        );
                                    }
                                    Ok(crate::messaging::upstream_subscription::ContractStatus::Violated { upstream, cause }) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            cause = ?cause,
                                            "Stream contract violation during join enriching"
                                        );
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            error = %e,
                                            "Failed to check stream contracts"
                                        );
                                    }
                                }
                                }

                                // No events available right now, sleep briefly and continue
                                tracing::trace!(
                                    stage_name = %ctx.stage_name,
                                    "No stream events available, sleeping"
                                );
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                directive = Ok(EventLoopDirective::Continue);
                            }
                            PollResult::Error(e) => {
                                tracing::error!("Stream subscription error: {}", e);
                                directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                    format!("Stream subscription error: {e}"),
                                )));
                            }
                        }
                    }
                } else {
                    // No subscription - wait briefly
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    directive = Ok(EventLoopDirective::Continue);
                }

                // Restore contract state and subscription back into the context
                ctx.stream_contract_state = stream_contract_state;
                ctx.stream_subscription = stream_subscription;

                directive
            }

            JoinState::Draining => {
                if ctx.reference_mode == JoinReferenceMode::Live {
                    return self.dispatch_draining_live(ctx).await;
                }

                // First, drain any remaining events from both subscriptions
                // This is critical for contract events (FLOWIP-080o fix)

                ctx.instrumentation
                    .event_loops_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Take ownership of reference subscription and contract state
                let mut ref_subscription = ctx.reference_subscription.take();
                let mut ref_contract_state = std::mem::take(&mut ctx.reference_contract_state);

                // Take ownership of stream subscription and contract state
                let mut stream_subscription = ctx.stream_subscription.take();
                let mut stream_contract_state = std::mem::take(&mut ctx.stream_contract_state);

                // Flush any pending stage outputs blocked on downstream credits.
                let pending_upstream = ctx.pending_ack_upstream;
                let mut blocked_on_backpressure = false;
                while let Some(pending) = ctx.pending_outputs.pop_front() {
                    if pending.is_data() {
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
                                            stage_type: StageType::Join,
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

                                        match self.data_journal.append(event, None).await {
                                            Ok(written) => {
                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                    &written,
                                                    &self.system_journal,
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
                                    stage_type: StageType::Join,
                                };

                                let event = ChainEventFactory::observability_event(
                                    WriterId::from(self.stage_id),
                                    ObservabilityPayload::Middleware(
                                        MiddlewareLifecycle::Backpressure(pulse),
                                    ),
                                )
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                                match self.data_journal.append(event, None).await {
                                    Ok(written) => {
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
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
                            blocked_on_backpressure = true;
                            break;
                        };

                        let flow_context = FlowContext {
                            flow_name: ctx.flow_name.clone(),
                            flow_id: ctx.flow_id.to_string(),
                            stage_name: ctx.stage_name.clone(),
                            stage_id: self.stage_id,
                            stage_type: StageType::Join,
                        };

                        let enriched = pending
                            .with_flow_context(flow_context)
                            .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                        if enriched.is_data() {
                            ctx.instrumentation.record_output_event(&enriched);

                            match pending_upstream {
                                Some(upstream) if upstream == ctx.reference_stage_id => {
                                    if let Some(sub) = ref_subscription.as_mut() {
                                        sub.track_output_event();
                                    }
                                }
                                Some(_) => {
                                    if let Some(sub) = stream_subscription.as_mut() {
                                        sub.track_output_event();
                                    }
                                }
                                None => {
                                    if let Some(sub) = ref_subscription.as_mut() {
                                        sub.track_output_event();
                                    }
                                    if let Some(sub) = stream_subscription.as_mut() {
                                        sub.track_output_event();
                                    }
                                }
                            }
                        }

                        let written = self.data_journal.append(enriched, None).await?;
                        reservation.commit(1);
                        ctx.backpressure_backoff.reset();
                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                            &written,
                            &self.system_journal,
                        )
                        .await;
                    } else {
                        let flow_context = FlowContext {
                            flow_name: ctx.flow_name.clone(),
                            flow_id: ctx.flow_id.to_string(),
                            stage_name: ctx.stage_name.clone(),
                            stage_id: self.stage_id,
                            stage_type: StageType::Join,
                        };

                        let enriched = pending
                            .with_flow_context(flow_context)
                            .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                        let written = self.data_journal.append(enriched, None).await?;
                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                            &written,
                            &self.system_journal,
                        )
                        .await;
                    }
                }

                let mut directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                > = Ok(EventLoopDirective::Continue);
                let mut should_drain = true;

                if blocked_on_backpressure {
                    ctx.reference_contract_state = ref_contract_state;
                    ctx.stream_contract_state = stream_contract_state;
                    ctx.reference_subscription = ref_subscription;
                    ctx.stream_subscription = stream_subscription;
                    return Ok(EventLoopDirective::Continue);
                }

                if let Some(upstream) = ctx.pending_ack_upstream.take() {
                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                        reader.ack_consumed(1);
                    }
                }

                if ctx.pending_outputs.is_empty()
                    && matches!(
                        ctx.pending_transition,
                        Some(PendingTransition::DrainComplete)
                    )
                {
                    ctx.pending_transition = None;
                    ctx.reference_contract_state = ref_contract_state;
                    ctx.stream_contract_state = stream_contract_state;
                    ctx.reference_subscription = ref_subscription;
                    ctx.stream_subscription = stream_subscription;
                    return Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete));
                }

                // Drain reference subscription
                if let Some(ref mut subscription) = ref_subscription {
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut ref_contract_state[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Join draining reference subscription event"
                            );

                            ctx.instrumentation.record_consumed(&envelope);
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Process based on event type
                            if !envelope.event.is_control() {
                                // Process data event through join handler (instrumented)
                                let event = envelope.event.clone();
                                let reference_stage_id = ctx.reference_stage_id;

                                // Get writer ID for the handler
                                let writer_id = ctx.writer_id.ok_or("No writer ID available")?;

                                ctx.instrumentation
                                    .in_flight_count
                                    .fetch_add(1, Ordering::Relaxed);
                                let start = Instant::now();
                                let result = ctx.handler.process_event(
                                    &mut ctx.handler_state,
                                    event,
                                    reference_stage_id,
                                    writer_id,
                                );
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

                                match result {
                                    Ok(results) => {
                                        ctx.instrumentation
                                            .events_accumulated_total
                                            .fetch_add(1, Ordering::Relaxed);
                                        let upstream_stage =
                                            subscription.last_delivered_upstream_stage();

                                        if results.is_empty() {
                                            if let Some(upstream) = upstream_stage {
                                                if let Some(reader) =
                                                    ctx.backpressure_readers.get(&upstream)
                                                {
                                                    reader.ack_consumed(1);
                                                }
                                            }
                                        } else {
                                            ctx.pending_outputs.extend(results);
                                            if let Some(upstream) = upstream_stage {
                                                ctx.pending_ack_upstream = Some(upstream);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            stage_name = %ctx.stage_name,
                                            error = ?err,
                                            "Join handler error during reference draining"
                                        );
                                        // Stage-fatal handler error: record it in error metrics.
                                        ctx.instrumentation.record_error(err.kind());
                                        directive =
                                            Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                                format!("Join handler drain-side error: {err:?}"),
                                            )));
                                    }
                                }
                            } else if !envelope.event.is_eof() {
                                // Forward non-EOF control events (CRITICAL FIX for FLOWIP-080o)
                                tracing::debug!(
                                    stage_name = %ctx.stage_name,
                                    event_type = envelope.event.event_type(),
                                    "Forwarding reference control event during join draining"
                                );
                                let written = self
                                    .data_journal
                                    .append(envelope.event.clone(), None)
                                    .await?;
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
                                )
                                .await;
                            }

                            // Continue draining on next iteration; don't call handler.drain yet
                            should_drain = false;
                        }
                        PollResult::NoEvents => {
                            // Do a final contract check before marking as drained
                            let _ = subscription
                                .check_contracts(&mut ref_contract_state[..])
                                .await;

                            // Reference queue empty
                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                "Join reference subscription queue drained"
                            );
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Error draining reference subscription"
                            );
                            directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                format!("Reference drain error: {e}"),
                            )));
                            should_drain = false;
                        }
                    }
                }

                // If reference side still has work or errored, restore state and return
                if !should_drain {
                    ctx.reference_contract_state = ref_contract_state;
                    ctx.stream_contract_state = stream_contract_state;
                    ctx.reference_subscription = ref_subscription;
                    ctx.stream_subscription = stream_subscription;

                    return directive;
                }

                // Drain stream subscription
                if let Some(ref mut subscription) = stream_subscription {
                    match subscription
                        .poll_next_with_state(
                            state.variant_name(),
                            Some(&mut stream_contract_state[..]),
                        )
                        .await
                    {
                        PollResult::Event(envelope) => {
                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Join draining stream subscription event"
                            );

                            ctx.instrumentation.record_consumed(&envelope);
                            ctx.instrumentation
                                .event_loops_with_work_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Process based on event type
                            if !envelope.event.is_control() {
                                // Process data event through join handler
                                let event = envelope.event.clone();

                                // Get writer ID for the handler
                                let writer_id = ctx.writer_id.ok_or("No writer ID available")?;

                                // Use the bound factory metadata to identify the first stream upstream
                                let source_stage_id = ctx
                                    .stream_subscription_factory
                                    .upstream_stage_ids()
                                    .first()
                                    .copied()
                                    .unwrap_or(ctx.reference_stage_id);

                                ctx.instrumentation
                                    .in_flight_count
                                    .fetch_add(1, Ordering::Relaxed);
                                let start = Instant::now();
                                let result = ctx.handler.process_event(
                                    &mut ctx.handler_state,
                                    event,
                                    source_stage_id,
                                    writer_id,
                                );
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

                                match result {
                                    Ok(events) => {
                                        let upstream_stage =
                                            subscription.last_delivered_upstream_stage();

                                        if events.is_empty() {
                                            if let Some(upstream) = upstream_stage {
                                                if let Some(reader) =
                                                    ctx.backpressure_readers.get(&upstream)
                                                {
                                                    reader.ack_consumed(1);
                                                }
                                            }
                                        } else {
                                            ctx.pending_outputs.extend(events);
                                            if let Some(upstream) = upstream_stage {
                                                ctx.pending_ack_upstream = Some(upstream);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        // Per-record join failure during draining: mark input as error.
                                        use obzenflow_core::event::status::processing_status::{
                                            ErrorKind, ProcessingStatus,
                                        };

                                        let reason =
                                            format!("Join handler error during draining: {err:?}");
                                        let error_event = envelope
                                            .event
                                            .clone()
                                            .mark_as_error(reason, err.kind());

                                        let route_to_error_journal = match &error_event
                                            .processing_info
                                            .status
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

                                        let upstream_stage =
                                            subscription.last_delivered_upstream_stage();

                                        if route_to_error_journal {
                                            if error_event.is_data() {
                                                ctx.instrumentation
                                                    .record_output_event(&error_event);
                                                subscription.track_output_event();
                                            }
                                            ctx.error_journal
                                                .append(error_event, None)
                                                .await
                                                .map_err(|e| {
                                                    format!(
                                                        "Failed to write join drain error event: {e}"
                                                    )
                                                })?;

                                            // Backpressure ack: upstream input consumed.
                                            if let Some(upstream) = upstream_stage {
                                                if let Some(reader) =
                                                    ctx.backpressure_readers.get(&upstream)
                                                {
                                                    reader.ack_consumed(1);
                                                }
                                            }
                                        } else {
                                            ctx.pending_outputs.push_back(error_event);
                                            if let Some(upstream) = upstream_stage {
                                                ctx.pending_ack_upstream = Some(upstream);
                                            }
                                        }
                                    }
                                }
                            } else if !envelope.event.is_eof() {
                                // Forward non-EOF control events (CRITICAL FIX for FLOWIP-080o).
                                // Re-stamp flow and runtime context so metrics remain local to
                                // this join stage even when forwarding.
                                tracing::debug!(
                                    stage_name = %ctx.stage_name,
                                    event_type = envelope.event.event_type(),
                                    forward_stream_control_during_join_draining = true
                                );

                                let mut forward_event = envelope.event.clone();
                                let flow_name = forward_event.flow_context.flow_name.clone();
                                let flow_id = forward_event.flow_context.flow_id.clone();
                                forward_event = forward_event.with_flow_context(FlowContext {
                                    flow_name,
                                    flow_id,
                                    stage_name: ctx.stage_name.clone(),
                                    stage_id: ctx.stage_id,
                                    stage_type: StageType::Join,
                                });
                                forward_event.runtime_context = None;

                                let written = self.data_journal.append(forward_event, None).await?;
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
                                )
                                .await;
                            }

                            // Continue draining on next iteration; don't call handler.drain yet
                            should_drain = false;
                            directive = Ok(EventLoopDirective::Continue);
                        }
                        PollResult::NoEvents => {
                            // Do a final contract check before marking as drained
                            let _ = subscription
                                .check_contracts(&mut stream_contract_state[..])
                                .await;

                            // Stream queue empty
                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                join_stream_subscription_drained = true
                            );
                        }
                        PollResult::Error(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                            );
                            directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                e.to_string(),
                            )));
                            should_drain = false;
                        }
                    }
                }

                // Restore contract state and subscriptions before deciding whether to drain
                ctx.reference_contract_state = ref_contract_state;
                ctx.stream_contract_state = stream_contract_state;
                ctx.reference_subscription = ref_subscription;
                ctx.stream_subscription = stream_subscription;

                if !should_drain {
                    return directive;
                }

                // Now call handler.drain() to emit any final joined state
                let handler = ctx.handler.clone();
                let empty_state = handler.initial_state();
                let final_state = std::mem::replace(&mut ctx.handler_state, empty_state);
                let events = handler
                    .drain(&final_state)
                    .await
                    .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;
                ctx.handler_state = final_state;

                ctx.pending_outputs.extend(events);
                ctx.pending_transition = Some(PendingTransition::DrainComplete);
                Ok(EventLoopDirective::Continue)
            }

            JoinState::Drained | JoinState::Failed(_) => Ok(EventLoopDirective::Terminate),

            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JoinSupervisor<H> {
    async fn dispatch_live(
        &self,
        ctx: &mut JoinContext<H>,
    ) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
        ctx.instrumentation
            .event_loops_total
            .fetch_add(1, Ordering::Relaxed);

        // Take ownership of subscriptions + contract state so no borrow of ctx lives across .await.
        let mut ref_subscription = ctx.reference_subscription.take();
        let mut ref_contract_state = std::mem::take(&mut ctx.reference_contract_state);
        let mut stream_subscription = ctx.stream_subscription.take();
        let mut stream_contract_state = std::mem::take(&mut ctx.stream_contract_state);

        // Flush any pending stage outputs blocked on downstream credits.
        let pending_upstream = ctx.pending_ack_upstream;
        let mut blocked_on_backpressure = false;
        while let Some(pending) = ctx.pending_outputs.pop_front() {
            if pending.is_data() {
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
                                    stage_type: StageType::Join,
                                };

                                let event = ChainEventFactory::observability_event(
                                    WriterId::from(self.stage_id),
                                    ObservabilityPayload::Middleware(
                                        MiddlewareLifecycle::Backpressure(pulse),
                                    ),
                                )
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                                match self.data_journal.append(event, None).await {
                                    Ok(written) => {
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
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
                            stage_type: StageType::Join,
                        };

                        let event = ChainEventFactory::observability_event(
                            WriterId::from(self.stage_id),
                            ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(
                                pulse,
                            )),
                        )
                        .with_flow_context(flow_context)
                        .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                        match self.data_journal.append(event, None).await {
                            Ok(written) => {
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
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
                    blocked_on_backpressure = true;
                    break;
                };

                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: StageType::Join,
                };

                let enriched = pending
                    .with_flow_context(flow_context)
                    .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                if enriched.is_data() {
                    ctx.instrumentation.record_output_event(&enriched);

                    match pending_upstream {
                        Some(upstream) if upstream == ctx.reference_stage_id => {
                            if let Some(sub) = ref_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                        Some(_) => {
                            if let Some(sub) = stream_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                        None => {
                            if let Some(sub) = ref_subscription.as_mut() {
                                sub.track_output_event();
                            }
                            if let Some(sub) = stream_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                    }
                }

                let written = self.data_journal.append(enriched, None).await?;
                reservation.commit(1);
                ctx.backpressure_backoff.reset();
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    &self.system_journal,
                )
                .await;
            } else {
                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: StageType::Join,
                };

                let enriched = pending
                    .with_flow_context(flow_context)
                    .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                let written = self.data_journal.append(enriched, None).await?;
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    &self.system_journal,
                )
                .await;
            }
        }

        if blocked_on_backpressure {
            ctx.reference_contract_state = ref_contract_state;
            ctx.stream_contract_state = stream_contract_state;
            ctx.reference_subscription = ref_subscription;
            ctx.stream_subscription = stream_subscription;
            return Ok(EventLoopDirective::Continue);
        }

        if let Some(upstream) = ctx.pending_ack_upstream.take() {
            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                reader.ack_consumed(1);
            }
        }

        // Poll order uses deterministic priority + fairness cap.
        let cap = ctx.reference_batch_cap.unwrap_or(usize::MAX);
        let prefer_stream = ctx.reference_since_last_stream >= cap;

        // Helper: poll + handle one side; returns true when an event was processed.
        let mut handled_event = false;
        let mut directive: Result<
            EventLoopDirective<JoinEvent<H>>,
            Box<dyn std::error::Error + Send + Sync>,
        > = Ok(EventLoopDirective::Continue);

        if prefer_stream {
            if let Some(ref mut subscription) = stream_subscription {
                handled_event = self
                    .poll_live_stream(
                        subscription,
                        &mut stream_contract_state[..],
                        ctx,
                        &mut directive,
                    )
                    .await?;
            }
            if !handled_event {
                if let Some(ref mut subscription) = ref_subscription {
                    handled_event = self
                        .poll_live_reference(
                            subscription,
                            &mut ref_contract_state[..],
                            ctx,
                            &mut directive,
                        )
                        .await?;
                }
            }
        } else {
            if let Some(ref mut subscription) = ref_subscription {
                handled_event = self
                    .poll_live_reference(
                        subscription,
                        &mut ref_contract_state[..],
                        ctx,
                        &mut directive,
                    )
                    .await?;
            }
            if !handled_event {
                if let Some(ref mut subscription) = stream_subscription {
                    handled_event = self
                        .poll_live_stream(
                            subscription,
                            &mut stream_contract_state[..],
                            ctx,
                            &mut directive,
                        )
                        .await?;
                }
            }
        }

        if !handled_event {
            // No events available right now: check contracts for both sides if appropriate.
            if let Some(ref mut subscription) = ref_subscription {
                if subscription.should_check_contracts(&ref_contract_state[..]) {
                    let _ = subscription
                        .check_contracts(&mut ref_contract_state[..])
                        .await;
                }
            }
            if let Some(ref mut subscription) = stream_subscription {
                if subscription.should_check_contracts(&stream_contract_state[..]) {
                    let _ = subscription
                        .check_contracts(&mut stream_contract_state[..])
                        .await;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Restore contract state and subscriptions before returning.
        ctx.reference_contract_state = ref_contract_state;
        ctx.stream_contract_state = stream_contract_state;
        ctx.reference_subscription = ref_subscription;
        ctx.stream_subscription = stream_subscription;

        directive
    }

    async fn dispatch_draining_live(
        &self,
        ctx: &mut JoinContext<H>,
    ) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
        ctx.instrumentation
            .event_loops_total
            .fetch_add(1, Ordering::Relaxed);

        // We do not poll reference or stream subscriptions in Live-mode draining.
        // The stream side is authoritative for completion.
        let mut ref_subscription = ctx.reference_subscription.take();
        let mut stream_subscription = ctx.stream_subscription.take();

        // Flush any pending stage outputs blocked on downstream credits.
        let pending_upstream = ctx.pending_ack_upstream;
        let mut blocked_on_backpressure = false;
        while let Some(pending) = ctx.pending_outputs.pop_front() {
            if pending.is_data() {
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
                                    stage_type: StageType::Join,
                                };

                                let event = ChainEventFactory::observability_event(
                                    WriterId::from(self.stage_id),
                                    ObservabilityPayload::Middleware(
                                        MiddlewareLifecycle::Backpressure(pulse),
                                    ),
                                )
                                .with_flow_context(flow_context)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                                match self.data_journal.append(event, None).await {
                                    Ok(written) => {
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
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
                            stage_type: StageType::Join,
                        };

                        let event = ChainEventFactory::observability_event(
                            WriterId::from(self.stage_id),
                            ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(
                                pulse,
                            )),
                        )
                        .with_flow_context(flow_context)
                        .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                        match self.data_journal.append(event, None).await {
                            Ok(written) => {
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
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
                    blocked_on_backpressure = true;
                    break;
                };

                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: StageType::Join,
                };

                let enriched = pending
                    .with_flow_context(flow_context)
                    .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                if enriched.is_data() {
                    ctx.instrumentation.record_output_event(&enriched);

                    match pending_upstream {
                        Some(upstream) if upstream == ctx.reference_stage_id => {
                            if let Some(sub) = ref_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                        Some(_) => {
                            if let Some(sub) = stream_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                        None => {
                            if let Some(sub) = ref_subscription.as_mut() {
                                sub.track_output_event();
                            }
                            if let Some(sub) = stream_subscription.as_mut() {
                                sub.track_output_event();
                            }
                        }
                    }
                }

                let written = self.data_journal.append(enriched, None).await?;
                reservation.commit(1);
                ctx.backpressure_backoff.reset();
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    &self.system_journal,
                )
                .await;
            } else {
                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: StageType::Join,
                };

                let enriched = pending
                    .with_flow_context(flow_context)
                    .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                let written = self.data_journal.append(enriched, None).await?;
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    &self.system_journal,
                )
                .await;
            }
        }

        if blocked_on_backpressure {
            ctx.reference_subscription = ref_subscription;
            ctx.stream_subscription = stream_subscription;
            return Ok(EventLoopDirective::Continue);
        }

        if let Some(upstream) = ctx.pending_ack_upstream.take() {
            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                reader.ack_consumed(1);
            }
        }

        if ctx.pending_outputs.is_empty()
            && matches!(
                ctx.pending_transition,
                Some(PendingTransition::DrainComplete)
            )
        {
            ctx.pending_transition = None;
            ctx.reference_subscription = ref_subscription;
            ctx.stream_subscription = stream_subscription;
            return Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete));
        }

        if ctx.pending_transition.is_none() {
            let handler = ctx.handler.clone();
            let empty_state = handler.initial_state();
            let mut final_state = std::mem::replace(&mut ctx.handler_state, empty_state);

            // Live-mode semantics: stream EOF is authoritative and drives completion.
            // Invoke the EOF hook for the stream side only (reference EOF is ignored in Live mode).
            if let Some(stream_source_id) = ctx
                .buffered_eof
                .as_ref()
                .and_then(|eof| eof.writer_id.as_stage().copied())
            {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError("No writer ID available".to_string())
                })?;

                let eof_events = handler
                    .on_source_eof(&mut final_state, stream_source_id, writer_id)
                    .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;

                if !eof_events.is_empty() {
                    tracing::debug!(
                        stage_name = %ctx.stage_name,
                        produced = eof_events.len(),
                        "Live join: stream EOF hook produced outputs"
                    );
                }

                ctx.pending_outputs.extend(eof_events);
            }

            let events = handler
                .drain(&final_state)
                .await
                .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;
            ctx.handler_state = final_state;

            ctx.pending_outputs.extend(events);
            ctx.pending_transition = Some(PendingTransition::DrainComplete);
        }

        ctx.reference_subscription = ref_subscription;
        ctx.stream_subscription = stream_subscription;

        Ok(EventLoopDirective::Continue)
    }

    async fn poll_live_reference(
        &self,
        subscription: &mut UpstreamSubscription<ChainEvent>,
        contract_state: &mut [crate::messaging::upstream_subscription::ReaderProgress],
        ctx: &mut JoinContext<H>,
        directive: &mut Result<
            EventLoopDirective<JoinEvent<H>>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        match subscription
            .poll_next_with_state("Live", Some(contract_state))
            .await
        {
            PollResult::Event(envelope) => {
                ctx.instrumentation.record_consumed(&envelope);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                ctx.reference_since_last_stream = ctx.reference_since_last_stream.saturating_add(1);
                ctx.instrumentation
                    .join_reference_since_last_stream
                    .store(ctx.reference_since_last_stream as u64, Ordering::Relaxed);

                match &envelope.event.content {
                    obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                        let mut processing_ctx = ProcessingContext::new();

                        let action = match signal {
                            FlowControlPayload::Eof { natural, .. } => {
                                tracing::info!(
                                    stage_name = %ctx.stage_name,
                                    natural = natural,
                                    writer_id = ?envelope.event.writer_id,
                                    "Join received EOF via reference_subscription (Live; no transition)"
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

                        match action {
                            ControlEventAction::Forward => {
                                let written = self
                                    .data_journal
                                    .append(envelope.event.clone(), None)
                                    .await?;
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
                                )
                                .await;
                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            ControlEventAction::Delay(duration) => {
                                tokio::time::sleep(duration).await;
                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            ControlEventAction::Retry | ControlEventAction::Skip => {
                                *directive = Ok(EventLoopDirective::Continue);
                            }
                        }
                    }
                    obzenflow_core::event::ChainEventContent::Data { .. } => {
                        let event = envelope.event.clone();
                        let source_id = ctx.reference_stage_id;
                        let writer_id = ctx.writer_id.ok_or("No writer ID available")?;

                        ctx.instrumentation
                            .in_flight_count
                            .fetch_add(1, Ordering::Relaxed);
                        let start = Instant::now();
                        let result = ctx.handler.process_event(
                            &mut ctx.handler_state,
                            event,
                            source_id,
                            writer_id,
                        );
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

                        match result {
                            Ok(events) => {
                                ctx.instrumentation
                                    .events_accumulated_total
                                    .fetch_add(1, Ordering::Relaxed);

                                if !events.is_empty() {
                                    tracing::debug!(
                                            stage_name = %ctx.stage_name,
                                            produced = events.len(),
                                        "Live join: reference event produced outputs"
                                    );
                                }

                                let mut pending_data_outputs: VecDeque<ChainEvent> = events.into();
                                while let Some(out_event) = pending_data_outputs.pop_front() {
                                    if out_event.is_data() {
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
                                                            stage_type: StageType::Join,
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

                                                        match self.data_journal.append(event, None).await {
                                                            Ok(written) => {
                                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                    &written,
                                                                    &self.system_journal,
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
                                            ctx.pending_ack_upstream = Some(source_id);
                                            ctx.pending_outputs.push_back(out_event);
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
                                                    stage_type: StageType::Join,
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

                                                match self.data_journal.append(event, None).await {
                                                    Ok(written) => {
                                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                            &written,
                                                            &self.system_journal,
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
                                            *directive = Ok(EventLoopDirective::Continue);
                                            return Ok(true);
                                        };

                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_event = out_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        if enriched_event.is_data() {
                                            ctx.instrumentation
                                                .record_output_event(&enriched_event);
                                            subscription.track_output_event();
                                        }

                                        let written =
                                            self.data_journal.append(enriched_event, None).await?;
                                        reservation.commit(1);
                                        ctx.backpressure_backoff.reset();
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    } else {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_event = out_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        let written =
                                            self.data_journal.append(enriched_event, None).await?;
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    }
                                }

                                if ctx.pending_outputs.is_empty() {
                                    if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                        reader.ack_consumed(1);
                                    }
                                }

                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            Err(err) => {
                                use obzenflow_core::event::status::processing_status::{
                                    ErrorKind, ProcessingStatus,
                                };

                                let reason =
                                    format!("Join handler error during live reference: {err:?}");
                                let error_event =
                                    envelope.event.clone().mark_as_error(reason, err.kind());
                                ctx.instrumentation.record_error(err.kind());

                                let route_to_error_journal =
                                    match &error_event.processing_info.status {
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
                                    ctx.error_journal.append(error_event, None).await.map_err(
                                        |e| format!("Failed to write join error event: {e}"),
                                    )?;
                                } else if error_event.is_data() {
                                    if let Some(reservation) = ctx.backpressure_writer.reserve(1) {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_error = error_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        if enriched_error.is_data() {
                                            ctx.instrumentation
                                                .record_output_event(&enriched_error);
                                            subscription.track_output_event();
                                        }

                                        let written =
                                            self.data_journal.append(enriched_error, None).await?;
                                        reservation.commit(1);
                                        ctx.backpressure_backoff.reset();
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    } else {
                                        ctx.pending_ack_upstream = Some(source_id);
                                        ctx.pending_outputs.push_back(error_event);
                                        let delay = ctx.backpressure_backoff.next_delay();
                                        ctx.backpressure_writer.record_wait(delay);
                                        tokio::time::sleep(delay).await;
                                    }
                                } else {
                                    let flow_context = FlowContext {
                                        flow_name: ctx.flow_name.clone(),
                                        flow_id: ctx.flow_id.to_string(),
                                        stage_name: ctx.stage_name.clone(),
                                        stage_id: self.stage_id,
                                        stage_type: StageType::Join,
                                    };

                                    let enriched_error = error_event
                                        .with_flow_context(flow_context)
                                        .with_runtime_context(
                                            ctx.instrumentation.snapshot_with_control(),
                                        );

                                    let written =
                                        self.data_journal.append(enriched_error, None).await?;
                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                        &written,
                                        &self.system_journal,
                                    )
                                    .await;
                                }

                                if ctx.pending_outputs.is_empty() {
                                    if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                        reader.ack_consumed(1);
                                    }
                                }

                                *directive = Ok(EventLoopDirective::Continue);
                            }
                        }
                    }
                    _ => {
                        *directive = Ok(EventLoopDirective::Continue);
                    }
                }

                Ok(true)
            }
            PollResult::NoEvents => Ok(false),
            PollResult::Error(e) => {
                *directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                    "Reference subscription error: {e}"
                ))));
                Ok(true)
            }
        }
    }

    async fn poll_live_stream(
        &self,
        subscription: &mut UpstreamSubscription<ChainEvent>,
        contract_state: &mut [crate::messaging::upstream_subscription::ReaderProgress],
        ctx: &mut JoinContext<H>,
        directive: &mut Result<
            EventLoopDirective<JoinEvent<H>>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        match subscription
            .poll_next_with_state("Live", Some(contract_state))
            .await
        {
            PollResult::Event(envelope) => {
                ctx.instrumentation.record_consumed(&envelope);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                // Stream processed -> capture and reset fairness counter.
                let reference_since_last_stream = ctx.reference_since_last_stream;
                ctx.reference_since_last_stream = 0;
                ctx.instrumentation
                    .join_reference_since_last_stream
                    .store(reference_since_last_stream as u64, Ordering::Relaxed);

                match &envelope.event.content {
                    obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                        let mut processing_ctx = ProcessingContext::new();

                        let action = match signal {
                            FlowControlPayload::Eof { natural, .. } => {
                                tracing::info!(
                                    stage_name = %ctx.stage_name,
                                    natural = natural,
                                    writer_id = ?envelope.event.writer_id,
                                    "Join received EOF via stream_subscription (Live; stream complete)"
                                );
                                ctx.buffered_eof = Some(envelope.event.clone());
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

                        match action {
                            ControlEventAction::Forward => {
                                let written = self
                                    .data_journal
                                    .append(envelope.event.clone(), None)
                                    .await?;
                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                    &written,
                                    &self.system_journal,
                                )
                                .await;

                                if matches!(signal, FlowControlPayload::Eof { .. }) {
                                    let eof_outcome = subscription.take_last_eof_outcome();
                                    if let Some(outcome) = eof_outcome {
                                        if outcome.is_final {
                                            *directive = Ok(EventLoopDirective::Transition(
                                                JoinEvent::ReceivedEOF,
                                            ));
                                            return Ok(true);
                                        }
                                    }
                                }

                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            ControlEventAction::Delay(duration) => {
                                tokio::time::sleep(duration).await;
                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            ControlEventAction::Retry | ControlEventAction::Skip => {
                                *directive = Ok(EventLoopDirective::Continue);
                            }
                        }
                    }
                    obzenflow_core::event::ChainEventContent::Data { .. } => {
                        let source_id = envelope
                            .event
                            .writer_id
                            .as_stage()
                            .copied()
                            .ok_or("Event writer is not a stage")?;

                        let event = envelope.event.clone();
                        let writer_id = ctx.writer_id.ok_or("No writer ID available")?;

                        ctx.instrumentation
                            .in_flight_count
                            .fetch_add(1, Ordering::Relaxed);
                        let start = Instant::now();
                        let result = ctx.handler.process_event(
                            &mut ctx.handler_state,
                            event,
                            source_id,
                            writer_id,
                        );
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

                        match result {
                            Ok(events) => {
                                let mut pending_data_outputs: VecDeque<ChainEvent> = events.into();
                                while let Some(out_event) = pending_data_outputs.pop_front() {
                                    if out_event.is_data() {
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
                                                            stage_type: StageType::Join,
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

                                                        match self.data_journal.append(event, None).await {
                                                            Ok(written) => {
                                                                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                                                    &written,
                                                                    &self.system_journal,
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
                                            ctx.pending_ack_upstream = Some(source_id);
                                            ctx.pending_outputs.push_back(out_event);
                                            ctx.pending_outputs.extend(pending_data_outputs);
                                            let delay = ctx.backpressure_backoff.next_delay();
                                            ctx.backpressure_writer.record_wait(delay);
                                            tokio::time::sleep(delay).await;
                                            *directive = Ok(EventLoopDirective::Continue);
                                            return Ok(true);
                                        };

                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_event = out_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        if enriched_event.is_data() {
                                            ctx.instrumentation
                                                .record_output_event(&enriched_event);
                                            subscription.track_output_event();
                                        }

                                        let written =
                                            self.data_journal.append(enriched_event, None).await?;
                                        reservation.commit(1);
                                        ctx.backpressure_backoff.reset();
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    } else {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_event = out_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        let written =
                                            self.data_journal.append(enriched_event, None).await?;
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    }
                                }

                                if ctx.pending_outputs.is_empty() {
                                    if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                        reader.ack_consumed(1);
                                    }
                                }

                                *directive = Ok(EventLoopDirective::Continue);
                            }
                            Err(err) => {
                                use obzenflow_core::event::status::processing_status::{
                                    ErrorKind, ProcessingStatus,
                                };

                                let reason =
                                    format!("Join handler error during live enrichment: {err:?}");
                                let error_event =
                                    envelope.event.clone().mark_as_error(reason, err.kind());
                                ctx.instrumentation.record_error(err.kind());

                                let route_to_error_journal =
                                    match &error_event.processing_info.status {
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
                                    ctx.error_journal.append(error_event, None).await.map_err(
                                        |e| format!("Failed to write join error event: {e}"),
                                    )?;
                                } else if error_event.is_data() {
                                    if let Some(reservation) = ctx.backpressure_writer.reserve(1) {
                                        let flow_context = FlowContext {
                                            flow_name: ctx.flow_name.clone(),
                                            flow_id: ctx.flow_id.to_string(),
                                            stage_name: ctx.stage_name.clone(),
                                            stage_id: self.stage_id,
                                            stage_type: StageType::Join,
                                        };

                                        let enriched_error = error_event
                                            .with_flow_context(flow_context)
                                            .with_runtime_context(
                                                ctx.instrumentation.snapshot_with_control(),
                                            );

                                        if enriched_error.is_data() {
                                            ctx.instrumentation
                                                .record_output_event(&enriched_error);
                                            subscription.track_output_event();
                                        }

                                        let written =
                                            self.data_journal.append(enriched_error, None).await?;
                                        reservation.commit(1);
                                        ctx.backpressure_backoff.reset();
                                        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                            &written,
                                            &self.system_journal,
                                        )
                                        .await;
                                    } else {
                                        ctx.pending_ack_upstream = Some(source_id);
                                        ctx.pending_outputs.push_back(error_event);
                                        let delay = ctx.backpressure_backoff.next_delay();
                                        ctx.backpressure_writer.record_wait(delay);
                                        tokio::time::sleep(delay).await;
                                    }
                                } else {
                                    let flow_context = FlowContext {
                                        flow_name: ctx.flow_name.clone(),
                                        flow_id: ctx.flow_id.to_string(),
                                        stage_name: ctx.stage_name.clone(),
                                        stage_id: self.stage_id,
                                        stage_type: StageType::Join,
                                    };

                                    let enriched_error = error_event
                                        .with_flow_context(flow_context)
                                        .with_runtime_context(
                                            ctx.instrumentation.snapshot_with_control(),
                                        );

                                    let written =
                                        self.data_journal.append(enriched_error, None).await?;
                                    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                                        &written,
                                        &self.system_journal,
                                    )
                                    .await;
                                }

                                if ctx.pending_outputs.is_empty() {
                                    if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                        reader.ack_consumed(1);
                                    }
                                }

                                *directive = Ok(EventLoopDirective::Continue);
                            }
                        }
                    }
                    _ => {
                        *directive = Ok(EventLoopDirective::Continue);
                    }
                }

                Ok(true)
            }
            PollResult::NoEvents => Ok(false),
            PollResult::Error(e) => {
                *directive = Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                    "Stream subscription error: {e}"
                ))));
                Ok(true)
            }
        }
    }

    /// Emit a join heartbeat when hydrating if enough reference events have
    /// been processed since the last snapshot.
    async fn emit_join_heartbeat_if_due(
        &self,
        ctx: &mut JoinContext<H>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        emit_join_heartbeat_if_due_impl(ctx, self.stage_id).await
    }
}

async fn emit_join_heartbeat_if_due_impl<H: JoinHandler + Send + Sync + 'static>(
    ctx: &mut JoinContext<H>,
    stage_id: StageId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let interval = heartbeat_interval();
    if interval == 0 || ctx.events_since_last_heartbeat < interval {
        return Ok(());
    }

    let Some(writer_id) = ctx.writer_id else {
        // Writer not initialized yet; skip heartbeat rather than failing.
        return Ok(());
    };

    let events_since_last = ctx.events_since_last_heartbeat;
    if events_since_last == 0 {
        return Ok(());
    }

    // Capture the latest runtime context snapshot for this join stage.
    let runtime_context = ctx.instrumentation.snapshot_with_control();

    use obzenflow_core::event::context::{FlowContext, StageType};
    use obzenflow_core::event::payloads::observability_payload::{
        MetricsLifecycle, ObservabilityPayload,
    };
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;

    let flow_context = FlowContext {
        flow_name: ctx.flow_name.clone(),
        flow_id: ctx.flow_id.to_string(),
        stage_name: ctx.stage_name.clone(),
        stage_id,
        stage_type: StageType::Join,
    };

    let payload = ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
        name: "join_reference_heartbeat".to_string(),
        value: json!({
            "events_since_last_heartbeat": events_since_last,
            "events_processed_total": runtime_context.events_processed_total,
        }),
        tags: None,
    });

    let heartbeat = ChainEventFactory::observability_event(writer_id, payload)
        .with_flow_context(flow_context)
        .with_runtime_context(runtime_context);

    ctx.data_journal.append(heartbeat, None).await?;

    ctx.events_since_last_heartbeat = 0;

    Ok(())
}

// Background task methods removed - join stage now follows the pattern
// of transform and stateful stages by processing events synchronously
// in dispatch_state instead of spawning background tasks
