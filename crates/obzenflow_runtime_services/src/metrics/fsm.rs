//! Metrics aggregator FSM types and state machine definition
//!
//! The metrics aggregator follows a simple lifecycle:
//! Initializing -> Running -> Draining -> Drained
//! Event processing happens directly without FSM state tracking

use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{CorrelationId, JournalEvent, WriterId};
use obzenflow_core::id::{StageId, SystemId};
use obzenflow_core::metrics::{Percentile, StageMetadata};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{ChainEvent, EventId, Journal};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, StateMachine, StateVariant, Transition,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

// Histogram configuration constants (in microseconds for precision)
const HISTOGRAM_MIN_US: u64 = 1; // 1 microsecond minimum
const HISTOGRAM_MAX_US: u64 = 60_000_000; // 60 seconds maximum
const HISTOGRAM_SIGFIGS: u8 = 3; // 3 significant figures

// Percentile constants - now using the Percentile enum

/// FSM states for metrics aggregator lifecycle
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricsAggregatorState {
    /// Initial state
    Initializing,

    /// Normal operation - processing events
    Running,

    /// Processing final events before shutdown
    Draining { consecutive_empty_batches: usize },

    /// Terminal state - all events processed
    Drained { last_event_id: Option<EventId> },

    /// Terminal state - error occurred
    Failed { error: String },
}

impl StateVariant for MetricsAggregatorState {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorState::Initializing => "Initializing",
            MetricsAggregatorState::Running => "Running",
            MetricsAggregatorState::Draining { .. } => "Draining",
            MetricsAggregatorState::Drained { .. } => "Drained",
            MetricsAggregatorState::Failed { .. } => "Failed",
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

    /// Process a system event (FLOWIP-059b)
    ProcessSystemEvent {
        envelope: obzenflow_core::EventEnvelope<obzenflow_core::event::SystemEvent>,
    },

    /// Time to export metrics
    ExportMetrics,

    /// Start draining process (from journal control event)
    StartDraining,

    /// Empty batch received during drain
    DrainEmptyBatch,

    /// No more events available during drain
    DrainComplete { last_event_id: Option<EventId> },

    /// Error occurred (e.g., journal corruption)
    Error(String),
}

impl EventVariant for MetricsAggregatorEvent {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorEvent::StartRunning => "StartRunning",
            MetricsAggregatorEvent::ProcessBatch { .. } => "ProcessBatch",
            MetricsAggregatorEvent::ProcessSystemEvent { .. } => "ProcessSystemEvent",
            MetricsAggregatorEvent::ExportMetrics => "ExportMetrics",
            MetricsAggregatorEvent::StartDraining => "StartDraining",
            MetricsAggregatorEvent::DrainEmptyBatch => "DrainEmptyBatch",
            MetricsAggregatorEvent::DrainComplete { .. } => "DrainComplete",
            MetricsAggregatorEvent::Error(_) => "Error",
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

    /// Process system events from the system journal (FLOWIP-059b)
    ProcessSystemEvent {
        envelope: obzenflow_core::EventEnvelope<obzenflow_core::event::SystemEvent>,
    },

    /// Export metrics snapshot
    ExportMetrics,

    /// Publish drain complete event to journal
    PublishDrainComplete { last_event_id: Option<EventId> },
}

/// Context for the FSM - contains everything actions need to do their work
pub struct MetricsAggregatorContext {
    /// System journal for reporting
    pub system_journal: Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,

    /// Subscription to read from all stage data journals
    pub data_subscription:
        Option<crate::messaging::upstream_subscription::UpstreamSubscription<ChainEvent>>,

    /// Subscription to read from all error journals (FLOWIP-082g)
    pub error_subscription:
        Option<crate::messaging::upstream_subscription::UpstreamSubscription<ChainEvent>>,

    /// Subscription to read from system journal for lifecycle events (FLOWIP-059b)
    pub system_subscription:
        Option<crate::messaging::system_subscription::SystemSubscription<
            obzenflow_core::event::SystemEvent,
        >>,

    /// Whether to include error journals in metrics collection
    pub include_error_journals: bool,

    pub exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    pub metrics_store: MetricsStore,
    pub export_interval_secs: u64,
    pub system_id: SystemId,
    pub export_timer: Option<tokio::time::Interval>,
    pub stage_metadata: HashMap<StageId, StageMetadata>,
}

/// Simple metrics storage
pub struct MetricsStore {
    pub stage_metrics: std::collections::HashMap<StageId, StageMetrics>,
    pub last_event_id: Option<EventId>,
    pub flow_start_time: Option<std::time::Instant>,
    pub first_event_time: Option<std::time::Instant>,
    pub last_event_time: Option<std::time::Instant>,
    pub total_events_processed: u64,

    // Journey tracking
    pub active_journeys: HashMap<CorrelationId, JourneyState>,
    pub journeys_started: u64,
    pub journeys_completed: u64,
    pub journeys_errored: u64,
    pub journeys_abandoned: u64,
    pub journey_durations: hdrhistogram::Histogram<u64>,

    // Flow-level aggregates
    pub flow_events_in: u64,
    pub flow_events_out: u64,
    pub flow_errors_total: u64,

    // System event tracking (FLOWIP-059b - essential events only)
    // Track all states each stage has been in: (StageId, state_name) -> true
    pub stage_lifecycle_states: HashMap<(StageId, String), bool>,
    pub pipeline_state: String,
}

#[derive(Clone, Debug)]
pub struct JourneyState {
    pub start_time: Instant,
    pub source_event_id: EventId,
    pub source_stage: String,
}

#[derive(Clone)]
pub struct StageMetrics {
    pub events_in: u64,
    pub events_out: u64,
    pub errors: u64,
    pub errors_by_kind: HashMap<ErrorKind, u64>,
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

impl Default for MetricsStore {
    fn default() -> Self {
        Self {
            stage_metrics: HashMap::new(),
            last_event_id: None,
            flow_start_time: None,
            first_event_time: None,
            last_event_time: None,
            total_events_processed: 0,
            active_journeys: HashMap::new(),
            journeys_started: 0,
            journeys_completed: 0,
            journeys_errored: 0,
            journeys_abandoned: 0,
            journey_durations: hdrhistogram::Histogram::new_with_bounds(
                HISTOGRAM_MIN_US,
                HISTOGRAM_MAX_US,
                HISTOGRAM_SIGFIGS,
            )
            .expect("Failed to create journey duration histogram"),
            flow_events_in: 0,
            flow_events_out: 0,
            flow_errors_total: 0,
            stage_lifecycle_states: HashMap::new(),
            pipeline_state: String::new(),
        }
    }
}

impl Default for StageMetrics {
    fn default() -> Self {
        Self {
            events_in: 0,
            events_out: 0,
            errors: 0,
            errors_by_kind: HashMap::new(),
            total_processing_time: MetricsDuration::ZERO,
            event_count: 0,
            // Create histogram with configured bounds
            processing_time_histogram: hdrhistogram::Histogram::new_with_bounds(
                HISTOGRAM_MIN_US,
                HISTOGRAM_MAX_US,
                HISTOGRAM_SIGFIGS,
            )
            .expect("Failed to create histogram"),
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
        inputs: crate::metrics::inputs::MetricsInputs,
        system_journal: Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
        exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        export_interval_secs: u64,
        system_id: SystemId,
        stage_metadata: HashMap<StageId, StageMetadata>,
    ) -> Result<Self, String> {
        // Helper to attach stage names to journals for better diagnostics
        let with_names = |journals: &[(StageId, Arc<dyn Journal<ChainEvent>>)]| {
            journals
                .iter()
                .map(|(id, journal)| {
                    let name = stage_metadata
                        .get(id)
                        .map(|m| m.name.clone())
                        .unwrap_or_else(|| format!("{:?}", id));
                    (*id, name, journal.clone())
                })
                .collect::<Vec<_>>()
        };

        tracing::info!(
            upstream_count = inputs.stage_data_journals.len(),
            upstream_stages = tracing::field::debug(
                &inputs
                    .stage_data_journals
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>(),
            ),
            "MetricsAggregator creating data subscription"
        );
        // Create subscription for data journals
        let data_subscription =
            crate::messaging::upstream_subscription::UpstreamSubscription::new_with_names(
                "metrics_aggregator",
                &with_names(&inputs.stage_data_journals),
            )
            .await
            .map_err(|e| format!("Failed to create data subscription: {}", e))?;

        if !inputs.error_journals.is_empty() {
            tracing::info!(
                upstream_count = inputs.error_journals.len(),
                upstream_stages = tracing::field::debug(
                    &inputs
                        .error_journals
                        .iter()
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>(),
                ),
                "MetricsAggregator creating error subscription"
            );
        }

        // Create subscription for error journals (FLOWIP-082g)
        let error_subscription = if !inputs.error_journals.is_empty() {
            Some(
                crate::messaging::upstream_subscription::UpstreamSubscription::new_with_names(
                    "metrics_aggregator",
                    &with_names(&inputs.error_journals),
                )
                .await
                .map_err(|e| format!("Failed to create error subscription: {}", e))?,
            )
        } else {
            None
        };

        // Create reader for system journal to receive lifecycle events (FLOWIP-059b)
        let system_reader = system_journal
            .reader()
            .await
            .map_err(|e| format!("Failed to create system journal reader: {:?}", e))?;

        // Wrap in SystemSubscription for consistent polling interface
        let system_subscription = crate::messaging::system_subscription::SystemSubscription::new(
            system_reader,
            "metrics_aggregator".to_string(),
        );

        Ok(Self {
            system_journal,
            data_subscription: Some(data_subscription),
            error_subscription,
            system_subscription: Some(system_subscription),
            include_error_journals: true, // Default to true per FLOWIP-082g
            exporter,
            metrics_store: MetricsStore::default(),
            export_interval_secs,
            system_id,
            export_timer: None,
            stage_metadata,
        })
    }
}

impl FsmContext for MetricsAggregatorContext {}

#[async_trait::async_trait]
impl FsmAction for MetricsAggregatorAction {
    type Context = MetricsAggregatorContext;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            MetricsAggregatorAction::Initialize => {
                tracing::info!("Metrics aggregator initialized");
                Ok(())
            }

            MetricsAggregatorAction::ProcessSystemEvent { envelope } => {
                tracing::info!(
                    event_id = %envelope.event.id(),
                    event_type = envelope.event.event_type_name(),
                    "Metrics aggregator ProcessSystemEvent action"
                );
                // FLOWIP-059b: Process system journal events for lifecycle tracking
                let store = &mut ctx.metrics_store;

                match &envelope.event.event {
                    obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event } => {
                        // Track ALL states each stage has been in (never overwrite)
                        match event {
                            obzenflow_core::event::StageLifecycleEvent::Running => {
                                store
                                    .stage_lifecycle_states
                                    .insert((*stage_id, "running".to_string()), true);
                                tracing::debug!("Stage {:?} transitioned to running", stage_id);
                            }
                            obzenflow_core::event::StageLifecycleEvent::Completed => {
                                store
                                    .stage_lifecycle_states
                                    .insert((*stage_id, "completed".to_string()), true);
                                tracing::debug!("Stage {:?} transitioned to completed", stage_id);
                            }
                            obzenflow_core::event::StageLifecycleEvent::Failed { .. } => {
                                store
                                    .stage_lifecycle_states
                                    .insert((*stage_id, "failed".to_string()), true);
                                tracing::debug!("Stage {:?} transitioned to failed", stage_id);
                            }
                            _ => {} // Skip draining, drained for now
                        }
                    }
                    obzenflow_core::event::SystemEventType::PipelineLifecycle(event) => {
                        // Track only essential pipeline events
                        match event {
                            obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted => {
                                store.pipeline_state = "all_stages_completed".to_string();
                                tracing::info!("Pipeline: all stages completed");
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Completed => {
                                store.pipeline_state = "completed".to_string();
                                tracing::info!("Pipeline: completed");
                            }
                            _ => {} // Skip other pipeline events
                        }
                    }
                    _ => {} // Skip MetricsCoordination and other event types
                }

                Ok(())
            }

            MetricsAggregatorAction::UpdateMetrics { envelope } => {
                tracing::trace!(
                    event_id = %envelope.event.id(),
                    event_type = envelope.event.event_type(),
                    "Metrics aggregator UpdateMetrics action"
                );
                let store = &mut ctx.metrics_store;

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

                        // Check for delivery errors first (before borrowing metrics)
                        let has_error = matches!(&payload.result, obzenflow_core::event::payloads::delivery_payload::DeliveryResult::Failed { .. });
                        if has_error {
                            store.flow_errors_total += 1;
                        }

                        // Update flow-level metrics for delivery events
                        store.flow_events_out += 1;

                        // Journey tracking for delivery events
                        if let Some(correlation_id) = &event.correlation_id {
                            if let Some(journey) = store.active_journeys.remove(correlation_id) {
                                // Journey completes successfully or errors at sink
                                if has_error {
                                    store.journeys_errored += 1;
                                } else {
                                    store.journeys_completed += 1;
                                }

                                let duration = journey.start_time.elapsed();
                                let duration_us = duration.as_micros() as u64;

                                // Record journey duration (for both success and error)
                                let clamped_duration =
                                    duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                                if let Err(e) = store.journey_durations.record(clamped_duration) {
                                    tracing::warn!("Failed to record journey duration: {:?}", e);
                                }
                            }
                        }

                        // Now handle stage-level metrics
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

                        // Update error count if needed
                        if has_error {
                            metrics.errors += 1;
                        }

                        // Record delivery processing time
                        let duration = payload.processing_duration;
                        let duration_us = duration.as_micros();

                        metrics.total_processing_time =
                            metrics.total_processing_time.saturating_add(duration);
                        metrics.event_count += 1;

                        // Record in histogram
                        let clamped_duration =
                            duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                        if let Err(e) = metrics.processing_time_histogram.record(clamped_duration) {
                            tracing::warn!(
                                "Failed to record delivery duration in histogram: {:?}",
                                e
                            );
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
                            metrics.event_loops_total =
                                metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                            metrics.event_loops_with_work_total = metrics
                                .event_loops_with_work_total
                                .max(runtime_ctx.event_loops_with_work_total);
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

                // First handle stage metrics
                {
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

                    // Check for errors from processing outcome and classify by ErrorKind
                    if let ProcessingStatus::Error { kind, .. } = &event.processing_info.status {
                        metrics.errors += 1;
                        let key = kind.clone().unwrap_or(ErrorKind::Unknown);
                        *metrics.errors_by_kind.entry(key).or_insert(0) += 1;
                    }

                    // Record processing time
                    let duration = event.processing_info.processing_time;
                    let duration_us = duration.as_micros(); // Convert to microseconds for histogram

                    metrics.total_processing_time =
                        metrics.total_processing_time.saturating_add(duration);
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
                        metrics.event_loops_total =
                            metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                        metrics.event_loops_with_work_total = metrics
                            .event_loops_with_work_total
                            .max(runtime_ctx.event_loops_with_work_total);
                    }

                    tracing::debug!(
                        "Updated metrics for {:?}: events={}, errors={}, avg_time={}",
                        stage_id,
                        metrics.events_in,
                        metrics.errors,
                        if metrics.event_count > 0 {
                            MetricsDuration::from_nanos(
                                metrics.total_processing_time.as_nanos() / metrics.event_count,
                            )
                        } else {
                            MetricsDuration::ZERO
                        }
                    );
                } // metrics reference dropped here

                // Flow-level event counting by stage type
                use obzenflow_core::event::context::StageType;
                let stage_type = event.flow_context.stage_type;

                match stage_type {
                    StageType::FiniteSource | StageType::InfiniteSource => {
                        store.flow_events_in += 1;
                    }
                    StageType::Sink => {
                        store.flow_events_out += 1;
                    }
                    _ => {} // Transforms don't count for flow in/out
                }

                // Journey tracking - only if correlation ID present
                if let Some(correlation_id) = &event.correlation_id {
                    match stage_type {
                        StageType::FiniteSource | StageType::InfiniteSource => {
                            // Journey starts at source
                            store.journeys_started += 1;
                            store.active_journeys.insert(
                                correlation_id.clone(),
                                JourneyState {
                                    start_time: Instant::now(),
                                    source_event_id: event.id.clone(),
                                    source_stage: event.flow_context.stage_name.clone(),
                                },
                            );
                        }
                        StageType::Sink => {
                            // Journey completes at sink
                            if let Some(journey) = store.active_journeys.remove(correlation_id) {
                                store.journeys_completed += 1;
                                let duration = journey.start_time.elapsed();
                                let duration_us = duration.as_micros() as u64;

                                // Record journey duration
                                let clamped_duration =
                                    duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                                if let Err(e) = store.journey_durations.record(clamped_duration) {
                                    tracing::warn!("Failed to record journey duration: {:?}", e);
                                }
                            }
                        }
                        _ => {} // Transforms don't affect journey state
                    }
                }

                // Aggregate flow-level metrics
                if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                    store.flow_errors_total += 1;

                    // Handle errored journeys (FLOWIP-082g)
                    if let Some(correlation_id) = &event.correlation_id {
                        // Journey errors on first error for this correlation_id
                        if let Some(journey) = store.active_journeys.remove(correlation_id) {
                            store.journeys_errored += 1;
                            let duration = journey.start_time.elapsed();
                            let duration_us = duration.as_micros() as u64;

                            // Record journey duration at error time
                            let clamped_duration =
                                duration_us.max(HISTOGRAM_MIN_US).min(HISTOGRAM_MAX_US);
                            if let Err(e) = store.journey_durations.record(clamped_duration) {
                                tracing::warn!(
                                    "Failed to record errored journey duration: {:?}",
                                    e
                                );
                            }
                        }
                    }
                }

                // Note: Event loop aggregation happens during export, not here
                // This avoids the delta calculation issues with concurrent updates

                Ok(())
            }

            MetricsAggregatorAction::ExportMetrics => {
                tracing::info!("ExportMetrics action triggered");
                if let Some(exporter) = &ctx.exporter {
                    let store = &ctx.metrics_store;
                    let mut snapshot = obzenflow_core::metrics::AppMetricsSnapshot::default();

                    tracing::info!(
                        "Exporting metrics: {} stage entries",
                        store.stage_metrics.len()
                    );

                    // Convert stage metrics to snapshot format
                    for (stage_id, metrics) in &store.stage_metrics {
                        // Use events_in as the event count
                        snapshot.event_counts.insert(*stage_id, metrics.events_in);

                        snapshot.error_counts.insert(*stage_id, metrics.errors);
                        if !metrics.errors_by_kind.is_empty() {
                            snapshot
                                .error_counts_by_kind
                                .insert(*stage_id, metrics.errors_by_kind.clone());
                        }

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
                                sum: metrics.total_processing_time.as_nanos() as f64, // Export as nanoseconds
                                min: (histogram.min() * 1_000) as f64, // Convert from microseconds to nanoseconds
                                max: (histogram.max() * 1_000) as f64, // Convert from microseconds to nanoseconds
                                percentiles,
                            };

                            snapshot.processing_times.insert(*stage_id, hist_snapshot);
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
                        snapshot
                            .event_loops_total
                            .insert(*stage_id, metrics.event_loops_total);
                        snapshot
                            .event_loops_with_work_total
                            .insert(*stage_id, metrics.event_loops_with_work_total);

                        tracing::debug!(
                            "Exported metrics for {:?}: events={}, errors={}, avg_time={}",
                            stage_id,
                            metrics.events_in,
                            metrics.errors,
                            if metrics.event_count > 0 {
                                MetricsDuration::from_nanos(
                                    metrics.total_processing_time.as_nanos() / metrics.event_count,
                                )
                            } else {
                                MetricsDuration::ZERO
                            }
                        );
                    }

                    // Add flow-level metrics
                    if let (Some(first_time), Some(last_time)) =
                        (store.first_event_time, store.last_event_time)
                    {
                        let flow_duration = last_time.duration_since(first_time);
                        // Convert journey duration histogram to snapshot
                        let journey_histogram = &store.journey_durations;
                        let mut journey_percentiles = std::collections::HashMap::new();
                        if journey_histogram.len() > 0 {
                            for p in Percentile::all() {
                                let value = journey_histogram.value_at_quantile(p.quantile());
                                journey_percentiles.insert(*p, (value * 1_000) as f64);
                                // Convert µs to ns
                            }
                        }

                        let e2e_latency = obzenflow_core::metrics::HistogramSnapshot {
                            count: journey_histogram.len(),
                            sum: journey_histogram
                                .iter_recorded()
                                .map(|v| v.value_iterated_to() * 1_000)
                                .sum::<u64>() as f64,
                            min: if journey_histogram.len() > 0 {
                                (journey_histogram.min() * 1_000) as f64
                            } else {
                                0.0
                            },
                            max: if journey_histogram.len() > 0 {
                                (journey_histogram.max() * 1_000) as f64
                            } else {
                                0.0
                            },
                            percentiles: journey_percentiles,
                        };

                        // Calculate abandoned journeys per FLOWIP-082g formula
                        // abandoned = started - completed - errored - active
                        let journeys_abandoned = store
                            .journeys_started
                            .saturating_sub(store.journeys_completed)
                            .saturating_sub(store.journeys_errored)
                            .saturating_sub(store.active_journeys.len() as u64);

                        // Aggregate event loops from all stages
                        let mut total_event_loops = 0u64;
                        let mut total_event_loops_with_work = 0u64;
                        for (_, stage_metrics) in &store.stage_metrics {
                            total_event_loops += stage_metrics.event_loops_total;
                            total_event_loops_with_work +=
                                stage_metrics.event_loops_with_work_total;
                        }

                        let flow_metrics = obzenflow_core::metrics::FlowMetricsSnapshot {
                            journeys_opened: store.journeys_started,
                            journeys_sealed: store.journeys_completed,
                            journeys_errored: store.journeys_errored,
                            journeys_abandoned,
                            e2e_latency,
                            flow_duration: MetricsDuration::from(flow_duration),
                            total_events_processed: store.total_events_processed,
                            events_in: store.flow_events_in,
                            events_out: store.flow_events_out,
                            errors_total: store.flow_errors_total,
                            event_loops_total: total_event_loops,
                            event_loops_with_work_total: total_event_loops_with_work,
                            saturation_journeys: store.active_journeys.len() as u64,
                        };
                        snapshot.flow_metrics = Some(flow_metrics);
                    }

                    // Add stage metadata
                    snapshot.stage_metadata = ctx.stage_metadata.clone();

                    // FLOWIP-059b: Add lifecycle states
                    snapshot.stage_lifecycle_states = store.stage_lifecycle_states.clone();
                    snapshot.pipeline_state = store.pipeline_state.clone();

                    // Add stage timestamps for rate calculation
                    // Convert from Instant to DateTime by calculating offset from snapshot time
                    let now = std::time::Instant::now();
                    let now_utc = chrono::Utc::now();

                    for (stage_id, metrics) in &store.stage_metrics {
                        if let Some(first_time) = metrics.first_event_time {
                            let elapsed_since_first = now.duration_since(first_time);
                            let first_datetime = now_utc
                                - chrono::Duration::from_std(elapsed_since_first)
                                    .unwrap_or_default();
                            snapshot
                                .stage_first_event_time
                                .insert(*stage_id, first_datetime);
                        }
                        if let Some(last_time) = metrics.last_event_time {
                            let elapsed_since_last = now.duration_since(last_time);
                            let last_datetime = now_utc
                                - chrono::Duration::from_std(elapsed_since_last)
                                    .unwrap_or_default();
                            snapshot
                                .stage_last_event_time
                                .insert(*stage_id, last_datetime);
                        }
                    }

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
                        obzenflow_core::event::MetricsCoordinationEvent::Drained,
                    ),
                );

                // Publish to system journal
                ctx.system_journal
                    .append(drain_event, None)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to publish drain complete event: {}",
                            e
                        ))
                    })?;

                tracing::info!(
                    "Published metrics drain complete event (last_event_id={:?})",
                    last_event_id
                );
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
    fsm! {
        state:   MetricsAggregatorState;
        event:   MetricsAggregatorEvent;
        context: MetricsAggregatorContext;
        action:  MetricsAggregatorAction;
        initial: MetricsAggregatorState::Initializing;

        state MetricsAggregatorState::Initializing {
            on MetricsAggregatorEvent::StartRunning => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Running,
                        actions: vec![MetricsAggregatorAction::Initialize],
                    })
                })
            };

            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    let error = match event {
                        MetricsAggregatorEvent::Error(err) => err.clone(),
                        _ => {
                            return Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event for Error handler".to_string(),
                            ));
                        }
                    };
                    tracing::error!(error = %error, "Metrics aggregator encountered error");
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Failed { error },
                        actions: vec![],
                    })
                })
            };
        }

        state MetricsAggregatorState::Running {
            on MetricsAggregatorEvent::StartDraining => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Draining {
                            consecutive_empty_batches: 0,
                        },
                        actions: vec![],
                    })
                })
            };

            on MetricsAggregatorEvent::ProcessSystemEvent => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::ProcessSystemEvent { envelope } => Ok(Transition {
                            next_state: MetricsAggregatorState::Running,
                            actions: vec![MetricsAggregatorAction::ProcessSystemEvent {
                                envelope: envelope.clone(),
                            }],
                        }),
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for ProcessSystemEvent handler".to_string(),
                        )),
                    }
                })
            };

            on MetricsAggregatorEvent::ProcessBatch => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::ProcessBatch { events } => {
                            let actions = events
                                .iter()
                                .cloned()
                                .map(|envelope| MetricsAggregatorAction::UpdateMetrics { envelope })
                                .collect::<Vec<_>>();
                            Ok(Transition {
                                next_state: MetricsAggregatorState::Running,
                                actions,
                            })
                        }
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for ProcessBatch handler".to_string(),
                        )),
                    }
                })
            };

            on MetricsAggregatorEvent::ExportMetrics => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Running,
                        actions: vec![MetricsAggregatorAction::ExportMetrics],
                    })
                })
            };

            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::Error(error) => Ok(Transition {
                            next_state: MetricsAggregatorState::Failed {
                                error: error.clone(),
                            },
                            actions: vec![],
                        }),
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for Error handler".to_string(),
                        )),
                    }
                })
            };
        }

        state MetricsAggregatorState::Draining {
            on MetricsAggregatorEvent::DrainComplete => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
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
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for DrainComplete handler".to_string(),
                        )),
                    }
                })
            };

            on MetricsAggregatorEvent::ProcessBatch => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::ProcessBatch { events } => {
                            let actions = events
                                .iter()
                                .cloned()
                                .map(|envelope| MetricsAggregatorAction::UpdateMetrics { envelope })
                                .collect::<Vec<_>>();
                            Ok(Transition {
                                next_state: MetricsAggregatorState::Draining {
                                    consecutive_empty_batches: 0,
                                },
                                actions,
                            })
                        }
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for ProcessBatch handler in Draining".to_string(),
                        )),
                    }
                })
            };

            on MetricsAggregatorEvent::DrainEmptyBatch => |state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let state = state.clone();
                Box::pin(async move {
                    let next_count = match state {
                        MetricsAggregatorState::Draining {
                            consecutive_empty_batches,
                        } => consecutive_empty_batches + 1,
                        _ => 0,
                    };
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Draining {
                            consecutive_empty_batches: next_count,
                        },
                        actions: vec![],
                    })
                })
            };

            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::Error(error) => Ok(Transition {
                            next_state: MetricsAggregatorState::Failed {
                                error: error.clone(),
                            },
                            actions: vec![],
                        }),
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for Error handler".to_string(),
                        )),
                    }
                })
            };
        }

        // Drained state (terminal) - no transitions
        state MetricsAggregatorState::Drained { }

        // Failed state (terminal) - no transitions
        state MetricsAggregatorState::Failed { }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::event::CorrelationId;

    #[tokio::test]
    async fn test_delivery_event_preserves_correlation() {
        // Create a test event with correlation
        let writer_id = WriterId::from(StageId::new());
        let correlation_id = CorrelationId::new();
        let mut event = ChainEventFactory::data_event(
            writer_id.clone(),
            "test.event",
            serde_json::json!({"data": "test"}),
        );
        event.correlation_id = Some(correlation_id.clone());
        event.correlation_payload = Some(CorrelationPayload::new("test_source", event.id.clone()));

        // Simulate what the sink supervisor does when creating a delivery event
        let payload = DeliveryPayload::success("test_sink", DeliveryMethod::Noop, Some(1));
        let delivery_event =
            ChainEventFactory::delivery_event(writer_id, payload).with_correlation_from(&event);

        // Verify correlation is preserved
        assert_eq!(delivery_event.correlation_id, Some(correlation_id));
        assert!(delivery_event.correlation_payload.is_some());
        assert_eq!(
            delivery_event
                .correlation_payload
                .as_ref()
                .unwrap()
                .entry_stage,
            "test_source"
        );
    }

    #[tokio::test]
    async fn test_journey_tracking_with_correlation() {
        let mut store = MetricsStore::default();
        let stage_id = StageId::new();
        let correlation_id = CorrelationId::new();

        // Simulate source event starting a journey
        let source_event = {
            let mut event = ChainEventFactory::data_event(
                WriterId::from(stage_id),
                "test.source",
                serde_json::json!({"id": 1}),
            );
            event.correlation_id = Some(correlation_id.clone());
            event.flow_context.stage_type = StageType::InfiniteSource;
            event
        };

        // Track journey start
        store.journeys_started += 1;
        store.active_journeys.insert(
            correlation_id.clone(),
            JourneyState {
                start_time: std::time::Instant::now(),
                source_event_id: source_event.id.clone(),
                source_stage: "test_source".to_string(),
            },
        );

        // Simulate delivery event at sink
        let delivery_event = {
            let mut event = ChainEventFactory::delivery_event(
                WriterId::from(stage_id),
                DeliveryPayload::success("test_sink", DeliveryMethod::Noop, Some(1)),
            );
            event.correlation_id = Some(correlation_id.clone());
            event.flow_context.stage_type = StageType::Sink;
            event
        };

        // Complete journey
        if let Some(_journey) = store.active_journeys.remove(&correlation_id) {
            store.journeys_completed += 1;
        }

        // Verify journey completed
        assert_eq!(store.journeys_started, 1);
        assert_eq!(store.journeys_completed, 1);
        assert_eq!(store.active_journeys.len(), 0);
    }
}
