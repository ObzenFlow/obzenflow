//! Metrics aggregator FSM types and state machine definition
//!
//! The metrics aggregator follows a simple lifecycle:
//! Initializing -> Running -> Draining -> Drained
//! Event processing happens directly without FSM state tracking

use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{EventId, Journal, ChainEvent};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// FSM states for metrics aggregator lifecycle
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricsAggregatorState {
    /// Initial state
    Initializing,
    
    /// Normal operation - processing events
    Running,
    
    /// Processing final events before shutdown
    Draining {
        consecutive_empty_batches: usize,
    },
    
    /// Terminal state - all events processed
    Drained {
        last_event_id: Option<EventId>,
    },
}

impl StateVariant for MetricsAggregatorState {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorState::Initializing => "Initializing",
            MetricsAggregatorState::Running => "Running",
            MetricsAggregatorState::Draining { .. } => "Draining",
            MetricsAggregatorState::Drained { .. } => "Drained",
        }
    }
}

/// Events that drive state transitions
#[derive(Clone, Debug)]
pub enum MetricsAggregatorEvent {
    /// Initialization complete, start processing
    StartRunning,
    
    /// Process a batch of events
    ProcessBatch {
        events: Vec<obzenflow_core::EventEnvelope>,
    },
    
    /// Time to export metrics
    ExportMetrics,
    
    /// Start draining process (from journal control event)
    StartDraining,
    
    /// Empty batch received during drain
    DrainEmptyBatch,
    
    /// No more events available during drain
    DrainComplete {
        last_event_id: Option<EventId>,
    },
}

impl EventVariant for MetricsAggregatorEvent {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorEvent::StartRunning => "StartRunning",
            MetricsAggregatorEvent::ProcessBatch { .. } => "ProcessBatch",
            MetricsAggregatorEvent::ExportMetrics => "ExportMetrics",
            MetricsAggregatorEvent::StartDraining => "StartDraining",
            MetricsAggregatorEvent::DrainEmptyBatch => "DrainEmptyBatch",
            MetricsAggregatorEvent::DrainComplete { .. } => "DrainComplete",
        }
    }
}

/// Actions performed during transitions
#[derive(Clone, Debug)]
pub enum MetricsAggregatorAction {
    /// Initialize metrics collection
    Initialize,
    
    /// Update metrics from an event
    UpdateMetrics {
        envelope: obzenflow_core::EventEnvelope,
    },
    
    /// Export metrics snapshot
    ExportMetrics,
    
    /// Publish drain complete event to journal
    PublishDrainComplete {
        last_event_id: Option<EventId>,
    },
}

/// Context for the FSM - contains everything actions need to do their work
#[derive(Clone)]
pub struct MetricsAggregatorContext {
    pub journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
    pub exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    pub metrics_store: Arc<RwLock<MetricsStore>>,
    pub export_interval_secs: u64,
    pub writer_id: Arc<RwLock<Option<obzenflow_core::WriterId>>>,
    pub subscription: Arc<RwLock<Option<crate::messaging::reactive_journal::JournalSubscription>>>,
    pub export_timer: Arc<tokio::sync::Mutex<Option<tokio::time::Interval>>>,
}

/// Simple metrics storage
#[derive(Default)]
pub struct MetricsStore {
    pub stage_metrics: std::collections::HashMap<String, StageMetrics>,
    pub last_event_id: Option<EventId>,
}

#[derive(Default, Clone)]
pub struct StageMetrics {
    pub events_in: u64,
    pub events_out: u64,
    pub errors: u64,
    pub total_processing_time_ms: u64,
    pub event_count: u64,
}

impl MetricsAggregatorContext {
    pub fn new(
        journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
        exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        export_interval_secs: u64,
    ) -> Self {
        Self {
            journal,
            exporter,
            metrics_store: Arc::new(RwLock::new(MetricsStore::default())),
            export_interval_secs,
            writer_id: Arc::new(RwLock::new(None)),
            subscription: Arc::new(RwLock::new(None)),
            export_timer: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

impl FsmContext for MetricsAggregatorContext {}

#[async_trait::async_trait]
impl FsmAction for MetricsAggregatorAction {
    type Context = MetricsAggregatorContext;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            MetricsAggregatorAction::Initialize => {
                tracing::info!("Metrics aggregator initialized");
                Ok(())
            }
            
            MetricsAggregatorAction::UpdateMetrics { envelope } => {
                let mut store = ctx.metrics_store.write().await;

                // Update last event ID
                store.last_event_id = Some(envelope.event.id.clone());

                let event = &envelope.event;

                // Skip control events that start with "control." or "system."
                if event.is_control() || event.is_system() {
                    return Ok(());
                }

                // Process data events - extract metrics from flow context
                let stage_name = &event.flow_context.stage_name;
                let flow_name = &event.flow_context.flow_name;

                // Create a key combining flow and stage name
                let key = format!("{}:{}", flow_name, stage_name);

                let metrics = store
                    .stage_metrics
                    .entry(key.clone())
                    .or_insert_with(StageMetrics::default);

                // Count the event
                metrics.events_in += 1;
                metrics.events_out += 1; // For now, treat as both in and out

                // Check for errors from processing outcome
                if matches!(
                    event.processing_info.outcome,
                    obzenflow_core::event::processing_outcome::ProcessingOutcome::Error(_)
                ) {
                    metrics.errors += 1;
                }

                // Record processing time
                let duration_ms = event.processing_info.processing_time_ms;
                metrics.total_processing_time_ms += duration_ms;
                metrics.event_count += 1;

                tracing::debug!(
                    "Updated metrics for {}: events={}, errors={}, avg_time={}ms",
                    key,
                    metrics.events_in,
                    metrics.errors,
                    if metrics.event_count > 0 {
                        metrics.total_processing_time_ms / metrics.event_count
                    } else {
                        0
                    }
                );
                
                Ok(())
            }
            
            MetricsAggregatorAction::ExportMetrics => {
                tracing::info!("ExportMetrics action triggered");
                if let Some(exporter) = &ctx.exporter {
                    let store = ctx.metrics_store.read().await;
                    let mut snapshot = obzenflow_core::metrics::AppMetricsSnapshot::default();

                    tracing::info!(
                        "Exporting metrics: {} stage entries",
                        store.stage_metrics.len()
                    );

                    // Convert stage metrics to snapshot format
                    for (stage_key, metrics) in &store.stage_metrics {
                        // Use events_in as the event count
                        snapshot
                            .event_counts
                            .insert(stage_key.clone(), metrics.events_in);

                        snapshot
                            .error_counts
                            .insert(stage_key.clone(), metrics.errors);

                        // Add processing time as a simple histogram snapshot
                        if metrics.event_count > 0 {
                            let avg_time_seconds = (metrics.total_processing_time_ms as f64
                                / metrics.event_count as f64)
                                / 1000.0;

                            // Create a simple histogram snapshot with the average
                            let mut percentiles = std::collections::HashMap::new();
                            percentiles.insert("p50".to_string(), avg_time_seconds);
                            percentiles.insert("p90".to_string(), avg_time_seconds);
                            percentiles.insert("p99".to_string(), avg_time_seconds);

                            let hist_snapshot = obzenflow_core::metrics::HistogramSnapshot {
                                count: metrics.event_count,
                                sum: (metrics.total_processing_time_ms as f64) / 1000.0,
                                min: avg_time_seconds,
                                max: avg_time_seconds,
                                percentiles,
                            };

                            snapshot
                                .processing_times
                                .insert(stage_key.clone(), hist_snapshot);
                        }

                        tracing::debug!(
                            "Exported metrics for {}: events={}, errors={}, avg_time={}ms",
                            stage_key,
                            metrics.events_in,
                            metrics.errors,
                            if metrics.event_count > 0 {
                                metrics.total_processing_time_ms / metrics.event_count
                            } else {
                                0
                            }
                        );
                    }

                    drop(store); // Release the lock before exporting

                    tracing::info!("Pushing metrics snapshot to exporter");

                    if let Err(e) = exporter.update_app_metrics(snapshot) {
                        tracing::warn!("Failed to export metrics: {}", e);
                    } else {
                        tracing::info!("Successfully exported metrics");
                    }
                }
                Ok(())
            }
            
            MetricsAggregatorAction::PublishDrainComplete { last_event_id } => {
                // Get writer ID from context
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to publish drain event".to_string())?;
                
                // Build the drain complete event
                let mut payload = serde_json::json!({});
                if let Some(id) = last_event_id {
                    payload["last_event_id"] = serde_json::json!(id.to_string());
                }
                
                let drain_event = obzenflow_core::ChainEvent::new(
                    obzenflow_core::EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_METRICS_DRAINED,
                    payload
                );
                
                // Publish to journal
                ctx.journal
                    .append(writer_id, drain_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish drain complete event: {}", e))?;
                
                tracing::info!("Published metrics drain complete event");
                Ok(())
            }
        }
    }
}

/// Type alias for the metrics FSM
pub type MetricsAggregatorFsm = StateMachine<
    MetricsAggregatorState,
    MetricsAggregatorEvent,
    MetricsAggregatorContext,
    MetricsAggregatorAction,
>;

/// Build the metrics aggregator FSM with lifecycle transitions only
pub fn build_metrics_aggregator_fsm() -> MetricsAggregatorFsm {
    FsmBuilder::new(MetricsAggregatorState::Initializing)
        // Initializing state transitions
        .when("Initializing")
        .on("StartRunning", |_state, _event, _ctx| async move {
            Ok(Transition {
                next_state: MetricsAggregatorState::Running,
                actions: vec![MetricsAggregatorAction::Initialize],
            })
        })
        .done()
        // Running state transitions
        .when("Running")
        .on("StartDraining", |_state, _event, _ctx| async move {
            Ok(Transition {
                next_state: MetricsAggregatorState::Draining {
                    consecutive_empty_batches: 0,
                },
                actions: vec![],
            })
        })
        .done()
        // Draining state transitions
        .when("Draining")
        .on("DrainComplete", |_state, event, _ctx| {
            let result = match event {
                MetricsAggregatorEvent::DrainComplete { last_event_id } => {
                    let last_event_id = last_event_id.clone();
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Drained {
                            last_event_id: last_event_id.clone(),
                        },
                        actions: vec![MetricsAggregatorAction::PublishDrainComplete {
                            last_event_id,
                        }],
                    })
                }
                _ => Err("Invalid event for DrainComplete handler".to_string())
            };
            async move { result }
        })
        .done()
        // Drained state (terminal) - no transitions
        .when("Drained")
        .done()
        // Handle unhandled events
        .when_unhandled(|state, event, _ctx| {
            let state_name = state.variant_name().to_string();
            let event_name = event.variant_name().to_string();
            async move {
                tracing::warn!("Unhandled event {} in state {}", event_name, state_name);
                Ok(())
            }
        })
        .build()
}