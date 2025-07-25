//! Metrics aggregator FSM types and state machine definition
//!
//! The metrics aggregator follows a simple lifecycle:
//! Initializing -> Running -> Draining -> Drained
//! Event processing happens directly without FSM state tracking

use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{EventId, Journal, ChainEvent};
use obzenflow_core::id::{SystemId, StageId};
use obzenflow_core::event::WriterId;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::metrics::{Percentile, StageMetadata};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::chain_event::ChainEventContent;
use std::collections::HashMap;

// Histogram configuration constants (in microseconds for precision)
const HISTOGRAM_MIN_US: u64 = 1;        // 1 microsecond minimum
const HISTOGRAM_MAX_US: u64 = 60_000_000;   // 60 seconds maximum  
const HISTOGRAM_SIGFIGS: u8 = 3;        // 3 significant figures

// Percentile constants - now using the Percentile enum

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
        events: Vec<obzenflow_core::EventEnvelope<obzenflow_core::ChainEvent>>,
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
        envelope: obzenflow_core::EventEnvelope<obzenflow_core::ChainEvent>,
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
    /// System journal for reporting
    pub system_journal: Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    
    /// Subscription to read from all stage journals
    pub subscription: Arc<RwLock<Option<crate::messaging::upstream_subscription::UpstreamSubscription<ChainEvent>>>>,
    
    pub exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    pub metrics_store: Arc<RwLock<MetricsStore>>,
    pub export_interval_secs: u64,
    pub system_id: SystemId,
    pub export_timer: Arc<tokio::sync::Mutex<Option<tokio::time::Interval>>>,
    pub stage_metadata: HashMap<StageId, StageMetadata>,
}

/// Simple metrics storage
#[derive(Default)]
pub struct MetricsStore {
    pub stage_metrics: std::collections::HashMap<StageId, StageMetrics>,
    pub last_event_id: Option<EventId>,
    pub flow_start_time: Option<std::time::Instant>,
    pub first_event_time: Option<std::time::Instant>,
    pub last_event_time: Option<std::time::Instant>,
    pub total_events_processed: u64,
}

#[derive(Clone)]
pub struct StageMetrics {
    pub events_in: u64,
    pub events_out: u64,
    pub errors: u64,
    pub total_processing_time: MetricsDuration,
    pub event_count: u64,
    pub processing_time_histogram: hdrhistogram::Histogram<u64>,
    // Runtime context metrics (FLOWIP-056c)
    pub last_in_flight: Option<u32>,
    pub last_failures_total: Option<u64>,
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,
    // Stage-specific timing for accurate rate calculation
    pub first_event_time: Option<std::time::Instant>,
    pub last_event_time: Option<std::time::Instant>,
}

impl Default for StageMetrics {
    fn default() -> Self {
        Self {
            events_in: 0,
            events_out: 0,
            errors: 0,
            total_processing_time: MetricsDuration::ZERO,
            event_count: 0,
            // Create histogram with configured bounds
            processing_time_histogram: hdrhistogram::Histogram::new_with_bounds(
                HISTOGRAM_MIN_US, 
                HISTOGRAM_MAX_US, 
                HISTOGRAM_SIGFIGS
            ).expect("Failed to create histogram"),
            last_in_flight: None,
            last_failures_total: None,
            event_loops_total: 0,
            event_loops_with_work_total: 0,
            first_event_time: None,
            last_event_time: None,
        }
    }
}

impl MetricsAggregatorContext {
    pub async fn new(
        stage_journals: Vec<(obzenflow_core::StageId, Arc<dyn Journal<ChainEvent>>)>,
        system_journal: Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
        exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        export_interval_secs: u64,
        system_id: SystemId,
        stage_metadata: HashMap<StageId, StageMetadata>,
    ) -> Result<Self, String> {
        // Create the subscription from stage journals
        let subscription = crate::messaging::upstream_subscription::UpstreamSubscription::new(&stage_journals)
            .await
            .map_err(|e| format!("Failed to create upstream subscription: {}", e))?;
        
        Ok(Self {
            system_journal,
            subscription: Arc::new(RwLock::new(Some(subscription))),
            exporter,
            metrics_store: Arc::new(RwLock::new(MetricsStore::default())),
            export_interval_secs,
            system_id,
            export_timer: Arc::new(tokio::sync::Mutex::new(None)),
            stage_metadata,
        })
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
                
                // Track flow timing for rate calculation
                let now = std::time::Instant::now();
                if store.first_event_time.is_none() {
                    store.first_event_time = Some(now);
                    store.flow_start_time = Some(now);
                }
                store.last_event_time = Some(now);
                store.total_events_processed += 1;
                
                // Handle delivery events separately
                if event.is_delivery() {
                    if let ChainEventContent::Delivery(payload) = &event.content {
                        // Process delivery event from sink
                        let stage_id = event.flow_context.stage_id;
                        
                        let metrics = store
                            .stage_metrics
                            .entry(stage_id)
                            .or_insert_with(StageMetrics::default);
                        
                        // Count delivery event
                        metrics.events_in += 1;
                        metrics.events_out += 1;
                        
                        // Track stage timing
                        let now = std::time::Instant::now();
                        if metrics.first_event_time.is_none() {
                            metrics.first_event_time = Some(now);
                        }
                        metrics.last_event_time = Some(now);
                        
                        // Check for delivery errors
                        if let obzenflow_core::event::payloads::delivery_payload::DeliveryResult::Failed { .. } = &payload.result {
                            metrics.errors += 1;
                        }
                        
                        // Record delivery processing time
                        let duration = payload.processing_duration;
                        let duration_us = duration.as_micros();
                        
                        metrics.total_processing_time = metrics.total_processing_time.saturating_add(duration);
                        metrics.event_count += 1;
                        
                        // Record in histogram
                        let clamped_duration = duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                        if let Err(e) = metrics.processing_time_histogram.record(clamped_duration) {
                            tracing::warn!("Failed to record delivery duration in histogram: {:?}", e);
                        }
                        
                        // Extract runtime context metrics if available (same as regular events)
                        if let Some(runtime_ctx) = &event.runtime_context {
                            tracing::trace!(
                                "Runtime context for delivery {:?}: in_flight={}, fsm_state={}",
                                stage_id,
                                runtime_ctx.in_flight,
                                runtime_ctx.fsm_state
                            );
                            
                            // Store latest runtime metrics for export
                            metrics.last_in_flight = Some(runtime_ctx.in_flight);
                            metrics.last_failures_total = Some(runtime_ctx.failures_total);
                            
                            // Update cumulative event loop counters (take max to handle resets)
                            metrics.event_loops_total = metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                            metrics.event_loops_with_work_total = metrics.event_loops_with_work_total.max(runtime_ctx.event_loops_with_work_total);
                        }
                        
                        tracing::debug!(
                            "Updated metrics for delivery {:?}: events={}, errors={}, processing_time={}",
                            stage_id,
                            metrics.events_in,
                            metrics.errors,
                            duration
                        );
                    }
                    return Ok(());
                }

                // Process regular data events - extract metrics from flow context
                let stage_id = event.flow_context.stage_id;

                let metrics = store
                    .stage_metrics
                    .entry(stage_id)
                    .or_insert_with(StageMetrics::default);

                // Count the event
                metrics.events_in += 1;
                metrics.events_out += 1; // For now, treat as both in and out
                
                // Track stage timing
                let now = std::time::Instant::now();
                if metrics.first_event_time.is_none() {
                    metrics.first_event_time = Some(now);
                }
                metrics.last_event_time = Some(now);

                // Check for errors from processing outcome
                if matches!(
                    event.processing_info.status,
                    ProcessingStatus::Error(_)
                ) {
                    metrics.errors += 1;
                }

                // Record processing time
                let duration = event.processing_info.processing_time;
                let duration_us = duration.as_micros();  // Convert to microseconds for histogram
                
                metrics.total_processing_time = metrics.total_processing_time.saturating_add(duration);
                metrics.event_count += 1;
                
                // Record in histogram as microseconds for precision
                let clamped_duration = duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                if let Err(e) = metrics.processing_time_histogram.record(clamped_duration) {
                    tracing::warn!("Failed to record duration in histogram: {:?}", e);
                }
                
                // Extract runtime context metrics if available (FLOWIP-056c)
                if let Some(runtime_ctx) = &event.runtime_context {
                    // These are point-in-time snapshots from the FSM instrumentation
                    // We could store them for trend analysis or immediate export
                    tracing::trace!(
                        "Runtime context for {:?}: in_flight={}, fsm_state={}",
                        stage_id,
                        runtime_ctx.in_flight,
                        runtime_ctx.fsm_state
                    );
                    
                    // Store latest runtime metrics for export
                    metrics.last_in_flight = Some(runtime_ctx.in_flight);
                    metrics.last_failures_total = Some(runtime_ctx.failures_total);
                    
                    // Update cumulative event loop counters (take max to handle resets)
                    metrics.event_loops_total = metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                    metrics.event_loops_with_work_total = metrics.event_loops_with_work_total.max(runtime_ctx.event_loops_with_work_total);
                }

                tracing::debug!(
                    "Updated metrics for {:?}: events={}, errors={}, avg_time={}",
                    stage_id,
                    metrics.events_in,
                    metrics.errors,
                    if metrics.event_count > 0 {
                        MetricsDuration::from_nanos(metrics.total_processing_time.as_nanos() / metrics.event_count)
                    } else {
                        MetricsDuration::ZERO
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
                    for (stage_id, metrics) in &store.stage_metrics {
                        // Use events_in as the event count
                        snapshot
                            .event_counts
                            .insert(*stage_id, metrics.events_in);

                        snapshot
                            .error_counts
                            .insert(*stage_id, metrics.errors);

                        // Add processing time histogram with real percentiles
                        if metrics.event_count > 0 {
                            let histogram = &metrics.processing_time_histogram;
                            
                            // Extract real percentiles from HdrHistogram and export as nanoseconds
                            let mut percentiles = std::collections::HashMap::new();
                            // Convert microseconds from histogram to nanoseconds for export
                            for p in Percentile::all() {
                                let value = histogram.value_at_quantile(p.quantile());
                                percentiles.insert(*p, (value * 1_000) as f64);
                            }

                            let hist_snapshot = obzenflow_core::metrics::HistogramSnapshot {
                                count: histogram.len(),
                                sum: metrics.total_processing_time.as_nanos() as f64,  // Export as nanoseconds
                                min: (histogram.min() * 1_000) as f64,  // Convert from microseconds to nanoseconds
                                max: (histogram.max() * 1_000) as f64,  // Convert from microseconds to nanoseconds
                                percentiles,
                            };

                            snapshot
                                .processing_times
                                .insert(*stage_id, hist_snapshot);
                        }
                        
                        // Add runtime context metrics if available (FLOWIP-056c)
                        if let Some(in_flight) = metrics.last_in_flight {
                            snapshot.in_flight.insert(*stage_id, in_flight as f64);
                        }
                        // events_behind removed - calculate in PromQL instead
                        
                        if let Some(failures_total) = metrics.last_failures_total {
                            snapshot.failures_total.insert(*stage_id, failures_total);
                        }
                        // Event loop metrics are cumulative counters
                        snapshot.event_loops_total.insert(*stage_id, metrics.event_loops_total);
                        snapshot.event_loops_with_work_total.insert(*stage_id, metrics.event_loops_with_work_total);

                        tracing::debug!(
                            "Exported metrics for {:?}: events={}, errors={}, avg_time={}",
                            stage_id,
                            metrics.events_in,
                            metrics.errors,
                            if metrics.event_count > 0 {
                                MetricsDuration::from_nanos(metrics.total_processing_time.as_nanos() / metrics.event_count)
                            } else {
                                MetricsDuration::ZERO
                            }
                        );
                    }

                    // Add flow-level metrics
                    if let (Some(first_time), Some(last_time)) = (store.first_event_time, store.last_event_time) {
                        let flow_duration = last_time.duration_since(first_time);
                        let flow_metrics = obzenflow_core::metrics::FlowMetricsSnapshot {
                            journeys_opened: 0, // Not implemented yet
                            journeys_sealed: 0, // Not implemented yet
                            e2e_latency: obzenflow_core::metrics::HistogramSnapshot::default(), // Not implemented yet
                            flow_duration: MetricsDuration::from(flow_duration),
                            total_events_processed: store.total_events_processed,
                        };
                        snapshot.flow_metrics = Some(flow_metrics);
                    }
                    
                    // Add stage metadata
                    snapshot.stage_metadata = ctx.stage_metadata.clone();
                    
                    // Add stage timestamps for rate calculation
                    // Convert from Instant to DateTime by calculating offset from snapshot time
                    let now = std::time::Instant::now();
                    let now_utc = chrono::Utc::now();
                    
                    for (stage_id, metrics) in &store.stage_metrics {
                        if let Some(first_time) = metrics.first_event_time {
                            let elapsed_since_first = now.duration_since(first_time);
                            let first_datetime = now_utc - chrono::Duration::from_std(elapsed_since_first).unwrap_or_default();
                            snapshot.stage_first_event_time.insert(*stage_id, first_datetime);
                        }
                        if let Some(last_time) = metrics.last_event_time {
                            let elapsed_since_last = now.duration_since(last_time);
                            let last_datetime = now_utc - chrono::Duration::from_std(elapsed_since_last).unwrap_or_default();
                            snapshot.stage_last_event_time.insert(*stage_id, last_datetime);
                        }
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
                let system_writer_id = WriterId::from(ctx.system_id);
                
                // Build the drain complete event
                let mut payload = serde_json::json!({});
                if let Some(id) = last_event_id {
                    payload["last_event_id"] = serde_json::json!(id.to_string());
                }
                
                // Metrics aggregator publishes SystemEvent to system journal
                let drain_event = obzenflow_core::event::SystemEvent::new(
                    system_writer_id,
                    obzenflow_core::event::SystemEventType::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Drained
                    )
                );
                
                // Publish to system journal
                ctx.system_journal
                    .append(drain_event, None)
                    .await
                    .map(|_| ())
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