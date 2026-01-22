//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Once started, all communication happens through journal events only.

use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{JournalEvent, WriterId};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::Journal;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use std::sync::Arc;

use super::fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState,
};

const IDLE_BACKOFF_MS: u64 = 10;

/// The supervisor that manages the metrics aggregator
pub(crate) struct MetricsAggregatorSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System journal for writing metrics ready event
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// System ID for metrics writer
    pub(crate) system_id: SystemId,
}

// Implement base Supervisor trait
impl Supervisor for MetricsAggregatorSupervisor {
    type State = MetricsAggregatorState;
    type Event = MetricsAggregatorEvent;
    type Context = MetricsAggregatorContext;
    type Action = MetricsAggregatorAction;

    fn build_state_machine(
        &self,
        _initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Reuse the typed DSL FSM defined in metrics/fsm.rs.
        crate::metrics::fsm::build_metrics_aggregator_fsm()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Implement SelfSupervised with ALL the logic - no separate impl blocks!
#[async_trait::async_trait]
impl SelfSupervised for MetricsAggregatorSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    fn event_for_action_error(&self, msg: String) -> MetricsAggregatorEvent {
        MetricsAggregatorEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = obzenflow_core::event::SystemEvent::new(
            self.writer_id(),
            obzenflow_core::event::SystemEventType::MetricsCoordination(
                obzenflow_core::event::MetricsCoordinationEvent::Shutdown,
            ),
        );

        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                journal_error = %e,
                "Failed to write metrics shutdown event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut MetricsAggregatorContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            MetricsAggregatorState::Initializing => {
                // Subscriptions are already created in the context during builder

                // Publish ready event to system journal
                // Metrics aggregator creates SystemEvent directly
                let event = obzenflow_core::event::SystemEvent::new(
                    WriterId::from(self.system_id),
                    obzenflow_core::event::SystemEventType::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Ready,
                    ),
                );

                self.system_journal
                    .append(event, None)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("Failed to write ready event: {e}"))?;

                tracing::info!("Metrics aggregator published ready event");

                // Transition to Running
                Ok(EventLoopDirective::Transition(
                    MetricsAggregatorEvent::StartRunning,
                ))
            }

            MetricsAggregatorState::Running => {
                tracing::debug!("Metrics aggregator state=Running");
                // Create timer on first entry to Running state
                if ctx.export_timer.is_none() {
                    tracing::debug!(
                        "Creating export timer with interval {}s",
                        ctx.export_interval_secs
                    );
                    let mut export_timer = tokio::time::interval(tokio::time::Duration::from_secs(
                        ctx.export_interval_secs,
                    ));
                    export_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    // First tick happens immediately, so consume it
                    export_timer.tick().await;
                    ctx.export_timer = Some(export_timer);
                }

                // Take subscriptions and timer out of the context so no borrow
                // of ctx lives across `.await` while polling.
                let mut data_subscription = ctx.data_subscription.take();
                let mut error_subscription = ctx.error_subscription.take();
                let mut system_subscription = ctx.system_subscription.take();
                let mut export_timer = ctx.export_timer.take();

                // Helper to check if either subscription has a drain event
                let check_for_drain = |envelope: &obzenflow_core::EventEnvelope<ChainEvent>| {
                    matches!(
                        &envelope.event.content,
                        obzenflow_core::event::ChainEventContent::FlowControl(
                            FlowControlPayload::Drain
                        )
                    )
                };

                let directive: Result<
                    EventLoopDirective<Self::Event>,
                    Box<dyn std::error::Error + Send + Sync>,
                >;

                // Build futures that operate on local subscriptions and timer only.
                let data_recv = async {
                    if let Some(sub) = data_subscription.as_mut() {
                        sub.poll_next_with_state(state.variant_name(), None).await
                    } else {
                        // If no data subscription, wait forever
                        std::future::pending::<PollResult<ChainEvent>>().await
                    }
                };

                let error_recv = async {
                    if let Some(sub) = error_subscription.as_mut() {
                        match sub.poll_next_with_state(state.variant_name(), None).await {
                            PollResult::Event(envelope) => Ok(Some(envelope)),
                            PollResult::NoEvents => Ok(None),
                            PollResult::Error(e) => Err(format!("Error: {e}")),
                        }
                    } else {
                        // If no error subscription, wait forever
                        std::future::pending::<
                            Result<Option<obzenflow_core::EventEnvelope<ChainEvent>>, String>,
                        >()
                        .await
                    }
                };

                // FLOWIP-059b: Build future for system events
                let system_recv = async {
                    if let Some(sub) = system_subscription.as_mut() {
                        match sub.poll_next().await {
                            PollResult::Event(envelope) => Ok(Some(envelope)),
                            PollResult::NoEvents => Ok(None),
                            PollResult::Error(e) => {
                                Err(format!("Error reading system events: {e}"))
                            }
                        }
                    } else {
                        // If no system subscription, wait forever
                        std::future::pending::<
                            Result<Option<obzenflow_core::EventEnvelope<SystemEvent>>, String>,
                        >()
                        .await
                    }
                };

                // Timer tick future
                let timer_tick = async {
                    if let Some(timer) = export_timer.as_mut() {
                        timer.tick().await;
                        Ok(())
                    } else {
                        // If no timer, wait forever
                        std::future::pending::<Result<(), ()>>().await
                    }
                };

                tokio::select! {
                    // FLOWIP-059b: Poll system events first (higher priority, lower volume)
                    result = system_recv => {
                        match result {
                            Ok(Some(envelope)) => {
                                tracing::info!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type_name(),
                                    "Metrics aggregator received system event"
                                );
                                if matches!(
                                    &envelope.event.event,
                                    obzenflow_core::event::SystemEventType::PipelineLifecycle(
                                        obzenflow_core::event::PipelineLifecycleEvent::Draining { .. }
                                            | obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { .. }
                                            | obzenflow_core::event::PipelineLifecycleEvent::Drained
                                            | obzenflow_core::event::PipelineLifecycleEvent::Completed { .. }
                                            | obzenflow_core::event::PipelineLifecycleEvent::Failed { .. }
                                    )
                                ) {
                                    export_timer = None;
                                }
                                // Process system event through FSM event
                                directive = Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::ProcessSystemEvent {
                                        envelope: Box::new(envelope),
                                    }
                                ))
                            }
                            Ok(None) => {
                                // No events available - sleep to avoid busy loop
                                idle_backoff().await;
                                directive = Ok(EventLoopDirective::Continue)
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "Metrics aggregator system subscription errored"
                                );
                                tracing::error!(
                                    error = %e,
                                    "Metrics aggregator emitting Error event from system subscription"
                                );
                                directive =
                                    Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::Error(
                                        format!("system subscription error: {e}"),
                                    )))
                            }
                        }
                    }
                    // Process data journal events
                    result = data_recv => {
                        match result {
                            PollResult::Event(envelope) => {
                                let kind = if envelope.event.is_control() {
                                    "control"
                                } else if envelope.event.is_system() {
                                    "system"
                                } else {
                                    "data"
                                };
                                tracing::trace!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type(),
                                    event_kind = kind,
                                    "Metrics aggregator received journal event"
                                );
                                // Check for drain event
                                if check_for_drain(&envelope) {
                                    tracing::info!(
                                        "Metrics aggregator received drain event from data journal \
                                         event_id={} writer={:?}",
                                        envelope.event.id,
                                        envelope.event.writer_id
                                    );
                                    // Clear the timer when transitioning away from Running
                                    export_timer = None;
                                    // Immediately transition into draining; don't process this
                                    // event as a normal metrics update.
                                    directive = Ok(EventLoopDirective::Transition(
                                        MetricsAggregatorEvent::StartDraining,
                                    ));
                                } else if envelope.event.is_control() || envelope.event.is_system()
                                {
                                    // Skip control and system events - they shouldn't be counted
                                    // in metrics
                                    directive = Ok(EventLoopDirective::Continue);
                                } else {
                                    // Process single event through FSM
                                    directive = Ok(EventLoopDirective::Transition(
                                        MetricsAggregatorEvent::ProcessBatch {
                                            events: vec![envelope],
                                        },
                                    ));
                                }
                            }
                            PollResult::NoEvents => {
                                // No events available, continue
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                directive = Ok(EventLoopDirective::Continue)
                            }
                            PollResult::Error(e) => {
                                let err_msg = format!("Data journal read error: {e}");
                                if err_msg.contains("Partial read retries exceeded") {
                                    tracing::warn!(
                                        error = %err_msg,
                                        "Metrics aggregator dropping partial read after retries"
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                tracing::error!(
                                    error = %err_msg,
                                    "Metrics aggregator emitting Error event"
                                );
                                directive = Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::Error(err_msg),
                                ));
                            }
                        }
                    }

                    // Process error journal events (FLOWIP-082g)
                    result = error_recv => {
                        match result {
                            Ok(Some(envelope)) => {
                                tracing::info!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type(),
                                    "Metrics aggregator received error event"
                                );
                                // Check for drain event
                                if check_for_drain(&envelope) {
                                    tracing::info!(
                                        "Metrics aggregator received drain event from error journal \
                                         event_id={} writer={:?}",
                                        envelope.event.id,
                                        envelope.event.writer_id
                                    );
                                    // Clear the timer when transitioning away from Running
                                    export_timer = None;
                                    directive = Ok(EventLoopDirective::Transition(
                                        MetricsAggregatorEvent::StartDraining,
                                    ));
                                } else if envelope.event.is_control() || envelope.event.is_system()
                                {
                                    // Skip control and system events
                                    directive = Ok(EventLoopDirective::Continue);
                                } else {
                                    // Process error event through FSM
                                    directive = Ok(EventLoopDirective::Transition(
                                        MetricsAggregatorEvent::ProcessBatch {
                                            events: vec![envelope],
                                        },
                                    ));
                                }
                            }
                            Ok(None) => {
                                // No error events available - should not happen often since
                                // error_recv waits forever if no subscription, but sleep if it does
                                idle_backoff().await;
                                directive = Ok(EventLoopDirective::Continue)
                            }
                            Err(e) => {
                                let err_msg = format!("Error journal read error: {e}");
                                if err_msg.contains("Partial read retries exceeded") {
                                    tracing::warn!(
                                        error = %err_msg,
                                        "Metrics aggregator dropping partial error journal read after retries"
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                tracing::error!(
                                    error = %err_msg,
                                    "Metrics aggregator emitting Error event"
                                );
                                directive = Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::Error(err_msg),
                                ));
                            }
                        }
                    }

                    // Export periodically
                    _ = timer_tick => {
                        tracing::info!("Metrics aggregator export timer tick");
                        directive = Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ExportMetrics))
                    }
                }

                // Restore subscriptions and timer back into the context
                ctx.data_subscription = data_subscription;
                ctx.error_subscription = error_subscription;
                ctx.system_subscription = system_subscription;
                ctx.export_timer = export_timer;

                directive
            }

            MetricsAggregatorState::Draining => {
                tracing::debug!("Metrics aggregator state=Draining");

                // Process draining state - keep consuming until:
                // 1) A full poll round yields no events across system/data/error subscriptions, AND
                // 2) Lifecycle signals show the pipeline + all stages are terminal.
                //
                // We intentionally do NOT depend on journal EOF control events here because some
                // stage journals (notably sinks) may not emit explicit EOF markers.
                let mut data_subscription = ctx.data_subscription.take();
                let mut error_subscription = ctx.error_subscription.take();
                let mut system_subscription = ctx.system_subscription.take();

                // 1) Poll system events first so lifecycle terminal flags are up-to-date.
                if let Some(sub) = system_subscription.as_mut() {
                    match sub.poll_next().await {
                        PollResult::Event(envelope) => {
                            tracing::info!(
                                event_id = %envelope.event.id(),
                                event_type = envelope.event.event_type_name(),
                                "Metrics aggregator draining received system event"
                            );
                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;
                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::ProcessSystemEvent {
                                    envelope: Box::new(envelope),
                                },
                            ));
                        }
                        PollResult::NoEvents => {}
                        PollResult::Error(e) => {
                            let err_msg =
                                format!("Error reading system events during draining: {e}");
                            tracing::error!(error = %err_msg, "Metrics aggregator draining system subscription errored");
                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;
                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::Error(err_msg),
                            ));
                        }
                    }
                }

                // 2) Drain data journals.
                if let Some(sub) = data_subscription.as_mut() {
                    match sub.poll_next_with_state(state.variant_name(), None).await {
                        PollResult::Event(envelope) => {
                            let kind = if envelope.event.is_control() {
                                "control"
                            } else if envelope.event.is_system() {
                                "system"
                            } else {
                                "data"
                            };
                            tracing::debug!(
                                event_id = %envelope.event.id(),
                                event_type = envelope.event.event_type(),
                                event_kind = kind,
                                writer_id = ?envelope.event.writer_id,
                                "Metrics aggregator draining received journal event"
                            );

                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;

                            if envelope.event.is_control() || envelope.event.is_system() {
                                tracing::debug!(
                                    "Metrics aggregator draining: skipped control/system event id={} writer={:?}",
                                    envelope.event.id,
                                    envelope.event.writer_id
                                );
                                return Ok(EventLoopDirective::Continue);
                            }

                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::ProcessBatch {
                                    events: vec![envelope],
                                },
                            ));
                        }
                        PollResult::NoEvents => {}
                        PollResult::Error(e) => {
                            let err_msg = format!("Data journal read error during draining: {e}");
                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;
                            if err_msg.contains("Partial read retries exceeded") {
                                tracing::warn!(
                                    error = %err_msg,
                                    "Metrics aggregator draining dropping partial read after retries"
                                );
                                return Ok(EventLoopDirective::Continue);
                            }
                            tracing::error!(
                                error = %err_msg,
                                "Metrics aggregator draining emitting Error event"
                            );
                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::Error(err_msg),
                            ));
                        }
                    }
                }

                // 3) Drain error journals (FLOWIP-082g).
                if let Some(sub) = error_subscription.as_mut() {
                    match sub.poll_next_with_state(state.variant_name(), None).await {
                        PollResult::Event(envelope) => {
                            tracing::info!(
                                event_id = %envelope.event.id(),
                                event_type = envelope.event.event_type(),
                                "Metrics aggregator draining received error event"
                            );

                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;

                            if envelope.event.is_control() || envelope.event.is_system() {
                                tracing::debug!(
                                    "Metrics aggregator draining: skipped control/system error event id={} writer={:?}",
                                    envelope.event.id,
                                    envelope.event.writer_id
                                );
                                return Ok(EventLoopDirective::Continue);
                            }

                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::ProcessBatch {
                                    events: vec![envelope],
                                },
                            ));
                        }
                        PollResult::NoEvents => {}
                        PollResult::Error(e) => {
                            let err_msg = format!("Error journal read error during draining: {e}");
                            ctx.data_subscription = data_subscription;
                            ctx.error_subscription = error_subscription;
                            ctx.system_subscription = system_subscription;
                            if err_msg.contains("Partial read retries exceeded") {
                                tracing::warn!(
                                    error = %err_msg,
                                    "Metrics aggregator draining dropping partial error journal read after retries"
                                );
                                return Ok(EventLoopDirective::Continue);
                            }
                            tracing::error!(
                                error = %err_msg,
                                "Metrics aggregator draining emitting Error event"
                            );
                            return Ok(EventLoopDirective::Transition(
                                MetricsAggregatorEvent::Error(err_msg),
                            ));
                        }
                    }
                }

                // 4) No events available right now. If lifecycle is terminal, perform final export.
                if ctx.metrics_store.all_stages_terminal(&ctx.stage_metadata)
                    && ctx.metrics_store.pipeline_terminal()
                {
                    tracing::info!(
                        "Metrics aggregator: drained journals and lifecycle terminal; emitting FlowTerminal"
                    );
                    ctx.data_subscription = data_subscription;
                    ctx.error_subscription = error_subscription;
                    ctx.system_subscription = system_subscription;
                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::FlowTerminal,
                    ));
                }

                idle_backoff().await;

                // Restore subscriptions back into the context
                ctx.data_subscription = data_subscription;
                ctx.error_subscription = error_subscription;
                ctx.system_subscription = system_subscription;

                Ok(EventLoopDirective::Continue)
            }

            MetricsAggregatorState::Drained { .. } => {
                // Terminal state
                tracing::info!("Metrics aggregator drained, terminating");
                Ok(EventLoopDirective::Terminate)
            }

            MetricsAggregatorState::Failed { error } => {
                // Terminal state - error occurred
                tracing::error!("Metrics aggregator failed: {}", error);
                Ok(EventLoopDirective::Terminate)
            }
        }
    }
}

#[inline]
async fn idle_backoff() {
    tokio::time::sleep(std::time::Duration::from_millis(IDLE_BACKOFF_MS)).await;
}
// All business logic has been moved to FSM actions - no free functions needed!

impl Drop for MetricsAggregatorSupervisor {
    fn drop(&mut self) {
        // Clean shutdown - subscription will be dropped automatically
        tracing::debug!("Metrics aggregator supervisor dropped");
    }
}
