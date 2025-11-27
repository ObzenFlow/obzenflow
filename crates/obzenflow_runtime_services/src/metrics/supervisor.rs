//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Once started, all communication happens through journal events only.

use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{ChainEventFactory, JournalEvent, WriterId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use serde_json::json;
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

    /// Metrics context (contains all mutable state)
    pub(crate) context: Arc<MetricsAggregatorContext>,

    /// System journal for writing metrics ready event
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
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
        WriterId::from(self.context.system_id)
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
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            MetricsAggregatorState::Initializing => {
                // Subscriptions are already created in the context during builder

                // Publish ready event to system journal
                // Metrics aggregator creates SystemEvent directly
                let event = obzenflow_core::event::SystemEvent::new(
                    WriterId::from(self.context.system_id),
                    obzenflow_core::event::SystemEventType::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Ready,
                    ),
                );

                self.system_journal
                    .append(event, None)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("Failed to write ready event: {}", e))?;

                tracing::info!("Metrics aggregator published ready event");

                // Transition to Running
                Ok(EventLoopDirective::Transition(
                    MetricsAggregatorEvent::StartRunning,
                ))
            }

            MetricsAggregatorState::Running => {
                tracing::debug!("Metrics aggregator state=Running");
                // Create timer on first entry to Running state
                {
                    let mut timer_guard = self.context.export_timer.lock().await;
                    if timer_guard.is_none() {
                        tracing::debug!(
                            "Creating export timer with interval {}s",
                            self.context.export_interval_secs
                        );
                        let mut export_timer = tokio::time::interval(
                            tokio::time::Duration::from_secs(self.context.export_interval_secs),
                        );
                        export_timer
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        // First tick happens immediately, so consume it
                        export_timer.tick().await;
                        *timer_guard = Some(export_timer);
                    }
                }

                // Get all subscriptions from context
                let mut data_subscription_guard = self.context.data_subscription.write().await;
                let data_subscription = data_subscription_guard
                    .as_mut()
                    .ok_or("No data subscription available")?;

                let mut error_subscription_guard = self.context.error_subscription.write().await;
                let error_subscription = error_subscription_guard.as_mut();

                // FLOWIP-059b: Get system subscription for lifecycle events
                let mut system_subscription_guard = self.context.system_subscription.write().await;
                let system_subscription = system_subscription_guard.as_mut();

                // Create a future for the timer tick
                let timer_tick = async {
                    let mut timer_guard = self.context.export_timer.lock().await;
                    if let Some(timer) = timer_guard.as_mut() {
                        timer.tick().await
                    } else {
                        // This shouldn't happen, but if it does, wait forever
                        std::future::pending::<tokio::time::Instant>().await
                    }
                };

                // Helper to check if either subscription has a drain event
                let check_for_drain = |envelope: &obzenflow_core::EventEnvelope<ChainEvent>| {
                    matches!(
                        &envelope.event.content,
                        obzenflow_core::event::ChainEventContent::FlowControl(
                            FlowControlPayload::Drain
                        )
                    )
                };

                // Build futures for subscriptions
                let data_recv =
                    data_subscription.poll_next_with_state(state.variant_name(), None);
                let error_recv = async {
                    if let Some(error_sub) = error_subscription {
                        match error_sub
                            .poll_next_with_state(state.variant_name(), None)
                            .await
                        {
                            PollResult::Event(envelope) => Ok(Some(envelope)),
                            PollResult::NoEvents => Ok(None),
                            PollResult::Error(e) => Err(format!("Error: {}", e)),
                        }
                    } else {
                        // If no error subscription, wait forever
                        std::future::pending().await
                    }
                };

                // FLOWIP-059b: Build future for system events
                let system_recv = async {
                    if let Some(system_sub) = system_subscription {
                        match system_sub.poll_next().await {
                            PollResult::Event(envelope) => Ok(Some(envelope)),
                            PollResult::NoEvents => Ok(None),
                            PollResult::Error(e) => {
                                Err(format!("Error reading system events: {}", e))
                            }
                        }
                    } else {
                        // If no system subscription, wait forever
                        std::future::pending().await
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
                                // Process system event through FSM event
                                Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::ProcessSystemEvent { envelope }
                                ))
                            }
                            Ok(None) => {
                                // No events available - sleep to avoid busy loop
                                idle_backoff().await;
                                Ok(EventLoopDirective::Continue)
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
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::Error(format!(
                                    "system subscription error: {}",
                                    e
                                ))))
                            }
                        }
                    }
                    // Process data journal events
                    result = data_recv => {
                        match result {
                            PollResult::Event(envelope) => {
                                tracing::info!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type(),
                                    "Metrics aggregator received data event"
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
                                    *self.context.export_timer.lock().await = None;
                                    return Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::StartDraining));
                                }

                                // Skip control and system events - they shouldn't be counted in metrics
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    return Ok(EventLoopDirective::Continue);
                                }

                                // Process single event through FSM
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            PollResult::NoEvents => {
                                // No events available, continue
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                Ok(EventLoopDirective::Continue)
                            }
                            PollResult::Error(e) => {
                                let err_msg = format!("Data journal read error: {}", e);
                                if err_msg.contains("Partial read retries exceeded") {
                                    tracing::warn!(
                                        error = %err_msg,
                                        "Metrics aggregator dropping partial read after retries"
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                tracing::error!(error = %err_msg, "Metrics aggregator emitting Error event");
                                return Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::Error(err_msg)
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
                                    *self.context.export_timer.lock().await = None;
                                    return Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::StartDraining));
                                }

                                // Skip control and system events
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    return Ok(EventLoopDirective::Continue);
                                }

                                // Process error event through FSM
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            Ok(None) => {
                                // No error events available - should not happen often since
                                // error_recv waits forever if no subscription, but sleep if it does
                                idle_backoff().await;
                                Ok(EventLoopDirective::Continue)
                            }
                            Err(e) => {
                                let err_msg = format!("Error journal read error: {}", e);
                                if err_msg.contains("Partial read retries exceeded") {
                                    tracing::warn!(
                                        error = %err_msg,
                                        "Metrics aggregator dropping partial error journal read after retries"
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                tracing::error!(error = %err_msg, "Metrics aggregator emitting Error event");
                                return Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::Error(err_msg)
                                ));
                            }
                        }
                    }

                    // Export periodically
                    _ = timer_tick => {
                        tracing::info!("Metrics aggregator export timer tick");
                        Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ExportMetrics))
                    }
                }
            }

            MetricsAggregatorState::Draining {
                consecutive_empty_batches,
            } => {
                tracing::debug!(
                    empty_batches = *consecutive_empty_batches,
                    "Metrics aggregator state=Draining"
                );
                // Check if we've had enough empty batches
                if *consecutive_empty_batches >= 2 {
                    // Get last event ID and transition to complete
                    let store = self.context.metrics_store.read().await;
                    let last_event_id = store.last_event_id.clone();
                    drop(store);

                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::DrainComplete { last_event_id },
                    ));
                }

                // Process draining state - need to drain both data and error journals
                let mut data_subscription_guard = self.context.data_subscription.write().await;
                let data_subscription = data_subscription_guard
                    .as_mut()
                    .ok_or("No data subscription available")?;

                let mut error_subscription_guard = self.context.error_subscription.write().await;
                let error_subscription = error_subscription_guard.as_mut();

                // Try both subscriptions without timeout - poll_next handles this
                let data_recv =
                    data_subscription.poll_next_with_state(state.variant_name(), None);

                let error_recv = async {
                    if let Some(error_sub) = error_subscription {
                        match error_sub
                            .poll_next_with_state(state.variant_name(), None)
                            .await
                        {
                            PollResult::Event(envelope) => Ok(Some(envelope)),
                            PollResult::NoEvents => Ok(None),
                            PollResult::Error(e) => Err(format!("Error: {}", e)),
                        }
                    } else {
                        // If no error subscription, wait forever
                        std::future::pending().await
                    }
                };

                tokio::select! {
                    result = data_recv => {
                        match result {
                            PollResult::Event(envelope) => {
                                tracing::debug!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type(),
                                    writer_id = ?envelope.event.writer_id,
                                    "Metrics aggregator draining received data event"
                                );
                                // Skip control and system events even during draining
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    tracing::debug!(
                                        "Metrics aggregator draining: skipped control/system event \
                                         id={} writer={:?}",
                                        envelope.event.id,
                                        envelope.event.writer_id
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                // Got event, process it
                                tracing::debug!(
                                    "Metrics aggregator draining: processed data event id={} writer={:?}",
                                    envelope.event.id,
                                    envelope.event.writer_id
                                );
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            PollResult::NoEvents | PollResult::Error(_) => {
                                // Treat NoEvents/Error as an empty batch for draining
                                let new_count = consecutive_empty_batches + 1;
                                tracing::info!(
                                    consecutive_empty_batches = new_count,
                                    "Metrics aggregator draining: empty batch (data)"
                                );
                                Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::DrainEmptyBatch,
                                ))
                            }
                        }
                    }

                    result = error_recv => {
                        match result {
                            Ok(Some(envelope)) => {
                                tracing::info!(
                                    event_id = %envelope.event.id(),
                                    event_type = envelope.event.event_type(),
                                    "Metrics aggregator draining received error event"
                                );
                                // Skip control and system events even during draining
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    tracing::debug!(
                                        "Metrics aggregator draining: skipped control/system error event \
                                         id={} writer={:?}",
                                        envelope.event.id,
                                        envelope.event.writer_id
                                    );
                                    return Ok(EventLoopDirective::Continue);
                                }
                                // Got error event, process it
                                tracing::debug!(
                                    "Metrics aggregator draining: processed error event id={} writer={:?}",
                                    envelope.event.id,
                                    envelope.event.writer_id
                                );
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            Ok(None) | Err(_) => {
                                // No events from either subscription - increment empty batch counter
                                let new_count = consecutive_empty_batches + 1;
                                tracing::info!(
                                    consecutive_empty_batches = new_count,
                                    "Metrics aggregator draining: empty batch"
                                );
                                Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::DrainEmptyBatch,
                                ))
                            }
                        }
                    }
                }
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
