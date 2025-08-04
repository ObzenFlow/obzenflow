//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Once started, all communication happens through journal events only.

use obzenflow_core::ChainEvent;
use obzenflow_core::event::{WriterId, ChainEventFactory};
use obzenflow_fsm::FsmBuilder;
use serde_json::json;
use std::sync::Arc;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use crate::supervised_base::base::Supervisor;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::event::SystemEvent;

use super::fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState,
};

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

    fn configure_fsm(
        &self,
        builder: FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            // Initializing -> Running
            .when("Initializing")
                .on("StartRunning", |_state, _event: &MetricsAggregatorEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: MetricsAggregatorState::Running,
                        actions: vec![MetricsAggregatorAction::Initialize],
                    })
                })
                .done()
            
            // Running state transitions
            .when("Running")
                .on("ProcessBatch", |_state, event: &MetricsAggregatorEvent, _ctx| {
                    let event = event.clone();
                    async move {
                        if let MetricsAggregatorEvent::ProcessBatch { events } = event {
                        // Create update actions for each event
                        let actions: Vec<_> = events.into_iter()
                            .map(|envelope| MetricsAggregatorAction::UpdateMetrics {
                                envelope,
                            })
                            .collect();
                        
                        Ok(obzenflow_fsm::Transition {
                            next_state: MetricsAggregatorState::Running,
                            actions,
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
                })
                .on("ExportMetrics", |_state, _event: &MetricsAggregatorEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: MetricsAggregatorState::Running,
                        actions: vec![MetricsAggregatorAction::ExportMetrics],
                    })
                })
                .on("StartDraining", |_state, _event: &MetricsAggregatorEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: MetricsAggregatorState::Draining {
                            consecutive_empty_batches: 0,
                        },
                        actions: vec![],
                    })
                })
                .done()
            
            // Draining state transitions  
            .when("Draining")
                .on("ProcessBatch", |_state, event: &MetricsAggregatorEvent, _ctx| {
                    let event = event.clone();
                    async move {
                        if let MetricsAggregatorEvent::ProcessBatch { events } = event {
                        // Process events during drain
                        let actions: Vec<_> = events.into_iter()
                            .map(|envelope| MetricsAggregatorAction::UpdateMetrics {
                                envelope,
                            })
                            .collect();
                        
                        Ok(obzenflow_fsm::Transition {
                            next_state: MetricsAggregatorState::Draining {
                                consecutive_empty_batches: 0,
                            },
                            actions,
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
                })
                .on("ProcessBatch", |state, event: &MetricsAggregatorEvent, _ctx| {
                    let state = state.clone();
                    let event = event.clone();
                    async move {
                        if let (MetricsAggregatorState::Draining { .. }, 
                                MetricsAggregatorEvent::ProcessBatch { events }) = (state, event) {
                        // Process events during drain
                        let actions: Vec<_> = events.into_iter()
                            .map(|envelope| MetricsAggregatorAction::UpdateMetrics {
                                envelope,
                            })
                            .collect();
                        
                        // Reset counter since we got events
                        Ok(obzenflow_fsm::Transition {
                            next_state: MetricsAggregatorState::Draining {
                                consecutive_empty_batches: 0,
                            },
                            actions,
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
                })
                .on("DrainEmptyBatch", |state, _event: &MetricsAggregatorEvent, _ctx| {
                    let state = state.clone();
                    async move {
                        if let MetricsAggregatorState::Draining { consecutive_empty_batches } = state {
                        let new_count = consecutive_empty_batches + 1;
                        
                        if new_count >= 2 {
                            // We've had enough empty batches, transition to complete
                            // This will trigger the DrainComplete event from dispatch_state
                            Ok(obzenflow_fsm::Transition {
                                next_state: MetricsAggregatorState::Draining {
                                    consecutive_empty_batches: new_count,
                                },
                                actions: vec![],
                            })
                        } else {
                            // Increment counter and continue draining
                            Ok(obzenflow_fsm::Transition {
                                next_state: MetricsAggregatorState::Draining {
                                    consecutive_empty_batches: new_count,
                                },
                                actions: vec![],
                            })
                        }
                    } else {
                        Err("Invalid state for DrainEmptyBatch".to_string())
                    }
                }
                })
                .on("DrainComplete", |_state, event: &MetricsAggregatorEvent, _ctx| {
                    let event = event.clone();
                    async move {
                        if let MetricsAggregatorEvent::DrainComplete { last_event_id } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: MetricsAggregatorState::Drained {
                                last_event_id: last_event_id.clone(),
                            },
                            actions: vec![
                                MetricsAggregatorAction::ExportMetrics, // Export final metrics
                                MetricsAggregatorAction::PublishDrainComplete {
                                    last_event_id: last_event_id.clone(),
                                },
                            ],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
                })
                .done()
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
                obzenflow_core::event::MetricsCoordinationEvent::Shutdown
            )
        );
        
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write metrics shutdown event: {}", e).into())
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
                        obzenflow_core::event::MetricsCoordinationEvent::Ready
                    )
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
                // Create timer on first entry to Running state
                {
                    let mut timer_guard = self.context.export_timer.lock().await;
                    if timer_guard.is_none() {
                        tracing::debug!("Creating export timer with interval {}s", self.context.export_interval_secs);
                        let mut export_timer = tokio::time::interval(tokio::time::Duration::from_secs(
                            self.context.export_interval_secs,
                        ));
                        export_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        // First tick happens immediately, so consume it
                        export_timer.tick().await;
                        *timer_guard = Some(export_timer);
                    }
                }

                // Get both subscriptions from context
                let mut data_subscription_guard = self.context.data_subscription.write().await;
                let data_subscription = data_subscription_guard
                    .as_mut()
                    .ok_or("No data subscription available")?;
                    
                let mut error_subscription_guard = self.context.error_subscription.write().await;
                let error_subscription = error_subscription_guard.as_mut();

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
                let data_recv = data_subscription.recv();
                let error_recv = async {
                    if let Some(error_sub) = error_subscription {
                        error_sub.recv().await
                    } else {
                        // If no error subscription, wait forever
                        std::future::pending().await
                    }
                };

                tokio::select! {
                    // Process data journal events
                    result = data_recv => {
                        match result {
                            Ok(envelope) => {
                                // Check for drain event
                                if check_for_drain(&envelope) {
                                    tracing::info!("Metrics aggregator received drain event from data journal");
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
                            Err(e) => {
                                tracing::error!("Failed to receive event from data journal: {}", e);
                                Ok(EventLoopDirective::Continue)
                            }
                        }
                    }
                    
                    // Process error journal events (FLOWIP-082g)
                    result = error_recv => {
                        match result {
                            Ok(envelope) => {
                                // Check for drain event
                                if check_for_drain(&envelope) {
                                    tracing::info!("Metrics aggregator received drain event from error journal");
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
                            Err(e) => {
                                tracing::error!("Failed to receive event from error journal: {}", e);
                                Ok(EventLoopDirective::Continue)
                            }
                        }
                    }

                    // Export periodically
                    _ = timer_tick => {
                        Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ExportMetrics))
                    }
                }
            }

            MetricsAggregatorState::Draining { consecutive_empty_batches } => {
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

                // Try both subscriptions with timeout
                let data_recv = tokio::time::timeout(
                    tokio::time::Duration::from_millis(25),
                    data_subscription.recv(),
                );
                
                let error_recv = async {
                    if let Some(error_sub) = error_subscription {
                        tokio::time::timeout(
                            tokio::time::Duration::from_millis(25),
                            error_sub.recv(),
                        ).await
                    } else {
                        // If no error subscription, wait forever
                        std::future::pending().await
                    }
                };

                tokio::select! {
                    result = data_recv => {
                        match result {
                            Ok(Ok(envelope)) => {
                                // Skip control and system events even during draining
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    return Ok(EventLoopDirective::Continue);
                                }
                                // Got event, process it
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            _ => {
                                // Try error journal next
                                Ok(EventLoopDirective::Continue)
                            }
                        }
                    }
                    
                    result = error_recv => {
                        match result {
                            Ok(Ok(envelope)) => {
                                // Skip control and system events even during draining
                                if envelope.event.is_control() || envelope.event.is_system() {
                                    return Ok(EventLoopDirective::Continue);
                                }
                                // Got error event, process it
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ProcessBatch { events: vec![envelope] }))
                            }
                            _ => {
                                // No events from either subscription - increment empty batch counter
                                Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::DrainEmptyBatch))
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
        }
    }
}

// All business logic has been moved to FSM actions - no free functions needed!

impl Drop for MetricsAggregatorSupervisor {
    fn drop(&mut self) {
        // Clean shutdown - subscription will be dropped automatically
        tracing::debug!("Metrics aggregator supervisor dropped");
    }
}

