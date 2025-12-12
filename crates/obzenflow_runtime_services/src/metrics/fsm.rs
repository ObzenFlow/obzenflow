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

use crate::metrics::tail_read;

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
    Draining,

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
            MetricsAggregatorState::Draining => "Draining",
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

    /// Flow + stages have reached terminal lifecycle; perform final export and shutdown
    FlowTerminal,

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
            MetricsAggregatorEvent::FlowTerminal => "FlowTerminal",
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

    /// Mapping of stage IDs to their data journals for tail reads.
    pub stage_data_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,

    /// Mapping of stage IDs to their error journals for tail reads.
    pub stage_error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,

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

    /// Per-stage vector clock watermark (FLOWIP-059c)
    /// Tracks the highest writer sequence observed for each stage's journal writer.
    pub stage_vector_clocks: HashMap<StageId, u64>,

    // System event tracking (FLOWIP-059b - essential events only)
    // Track all states each stage has been in: (StageId, state_name) -> true
    pub stage_lifecycle_states: HashMap<(StageId, String), bool>,
    pub pipeline_state: String,
}

#[derive(Clone)]
pub struct StageMetrics {
    pub errors_by_kind: HashMap<ErrorKind, u64>,
    // Runtime context metrics (FLOWIP-056c / FLOWIP-059 Phase 6)
    pub last_in_flight: Option<u32>,
    pub last_failures_total: Option<u64>,
    // Wide-event snapshot counters (Phase 6)
    pub latest_events_processed_total: Option<u64>,
    pub latest_errors_total: Option<u64>,
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,
    // Wide-event snapshot percentiles (Phase 6) - pre-computed by stage, in milliseconds
    pub snapshot_p50_ms: Option<u64>,
    pub snapshot_p90_ms: Option<u64>,
    pub snapshot_p95_ms: Option<u64>,
    pub snapshot_p99_ms: Option<u64>,
    pub snapshot_p999_ms: Option<u64>,
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
            stage_vector_clocks: HashMap::new(),
            stage_lifecycle_states: HashMap::new(),
            pipeline_state: String::new(),
        }
    }
}

impl Default for StageMetrics {
    fn default() -> Self {
        Self {
            errors_by_kind: HashMap::new(),
            last_in_flight: None,
            last_failures_total: None,
            latest_events_processed_total: None,
            latest_errors_total: None,
            event_loops_total: 0,
            event_loops_with_work_total: 0,
            snapshot_p50_ms: None,
            snapshot_p90_ms: None,
            snapshot_p95_ms: None,
            snapshot_p99_ms: None,
            snapshot_p999_ms: None,
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
        // Initialize in-memory metrics store so we can seed snapshot fields
        // before constructing subscriptions (FLOWIP-059 Phase 6).
        let mut metrics_store = MetricsStore::default();

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

        // Phase 6/059d: wide-event snapshot seeding and tail-aware start for data journals.
        let data_with_names = with_names(&inputs.stage_data_journals);
        let mut data_start_positions = Vec::with_capacity(data_with_names.len());

        for (stage_id, stage_name, journal) in &data_with_names {
            // Tail-read last event with runtime_context, if any.
            let tail_snapshot = match journal.read_last_n(1).await {
                Ok(mut events) => events
                    .drain(..)
                    .find(|env| env.event.runtime_context.is_some()),
                Err(e) => {
                    tracing::warn!(
                        target: "flowip-059",
                        owner = "metrics_aggregator",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        error = ?e,
                        "Failed to tail-read data journal for snapshot; seeding skipped for this stage"
                    );
                    None
                }
            };

            if let Some(envelope) = &tail_snapshot {
                if let Some(runtime_ctx) = &envelope.event.runtime_context {
                    let metrics = metrics_store
                        .stage_metrics
                        .entry(*stage_id)
                        .or_insert_with(StageMetrics::default);

                    // Seed wide-event snapshot fields from the latest event
                    // Use max() for monotonic counters to handle out-of-order reads
                    metrics.latest_events_processed_total = Some(
                        metrics
                            .latest_events_processed_total
                            .unwrap_or(0)
                            .max(runtime_ctx.events_processed_total),
                    );
                    metrics.latest_errors_total = Some(
                        metrics
                            .latest_errors_total
                            .unwrap_or(0)
                            .max(runtime_ctx.errors_total),
                    );
                    metrics.last_in_flight = Some(runtime_ctx.in_flight);
                    metrics.last_failures_total = Some(runtime_ctx.failures_total);
                    metrics.event_loops_total = metrics
                        .event_loops_total
                        .max(runtime_ctx.event_loops_total);
                    metrics.event_loops_with_work_total = metrics
                        .event_loops_with_work_total
                        .max(runtime_ctx.event_loops_with_work_total);

                    // Seed pre-computed percentiles from runtime_context
                    metrics.snapshot_p50_ms = Some(runtime_ctx.recent_p50_ms);
                    metrics.snapshot_p90_ms = Some(runtime_ctx.recent_p90_ms);
                    metrics.snapshot_p95_ms = Some(runtime_ctx.recent_p95_ms);
                    metrics.snapshot_p99_ms = Some(runtime_ctx.recent_p99_ms);
                    metrics.snapshot_p999_ms = Some(runtime_ctx.recent_p999_ms);
                }

                // Seed per-stage vector clock watermark from the last envelope
                let writer_id = envelope.event.writer_id().clone();
                let writer_key = writer_id.to_string();
                let seq = envelope.vector_clock.get(&writer_key);
                let entry = metrics_store
                    .stage_vector_clocks
                    .entry(*stage_id)
                    .or_insert(0);
                *entry = (*entry).max(seq);
            }

            // Determine starting position for data subscription by streaming to EOF.
            // This is O(n) in time but O(1) in memory and keeps semantics simple.
            let start_position = match journal.reader().await {
                Ok(mut reader) => {
                    let mut pos: u64 = 0;
                    loop {
                        match reader.next().await {
                            Ok(Some(_)) => {
                                pos += 1;
                            }
                            Ok(None) => break,
                            Err(e) => {
                                tracing::warn!(
                                    target: "flowip-059",
                                    owner = "metrics_aggregator",
                                    stage_id = ?stage_id,
                                    stage_name = stage_name,
                                    error = ?e,
                                    "Failed while streaming data journal to determine tail position; starting from 0"
                                );
                                pos = 0;
                                break;
                            }
                        }
                    }
                    pos
                }
                Err(e) => {
                    tracing::warn!(
                        target: "flowip-059",
                        owner = "metrics_aggregator",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        error = ?e,
                        "Failed to create reader for data journal; starting from 0"
                    );
                    0
                }
            };

            data_start_positions.push(start_position);
        }

        tracing::info!(
            upstream_count = inputs.stage_data_journals.len(),
            upstream_stages = tracing::field::debug(
                &inputs
                    .stage_data_journals
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>(),
            ),
            "MetricsAggregator creating data subscription (tail-start)"
        );
        // Create subscription for data journals starting at tail positions.
        // Readers are treated as logically at EOF for historical data
        // (baseline_at_tail = true) while still observing any new events
        // appended after subscription creation (FLOWIP-059d).
        let data_subscription =
            crate::messaging::upstream_subscription::UpstreamSubscription::new_at_tail(
                "metrics_aggregator",
                &data_with_names,
                &data_start_positions,
            )
            .await
            .map_err(|e| format!("Failed to create data subscription: {}", e))?;

        // Also seed wide-event snapshots from error journals (late error snapshots),
        // using the same tail-aware helper. This keeps StageMetrics consistent even
        // when the last wide event for a stage is written to an error journal.
        let error_with_names = with_names(&inputs.error_journals);
        for (stage_id, stage_name, journal) in &error_with_names {
            match journal.read_last_n(1).await {
                Ok(mut events) => {
                    if let Some(envelope) =
                        events.drain(..).find(|env| env.event.runtime_context.is_some())
                    {
                        if let Some(runtime_ctx) = &envelope.event.runtime_context {
                            let metrics = metrics_store
                                .stage_metrics
                            .entry(*stage_id)
                            .or_insert_with(StageMetrics::default);

                        metrics.latest_events_processed_total = Some(
                            metrics
                                .latest_events_processed_total
                                .unwrap_or(0)
                                .max(runtime_ctx.events_processed_total),
                        );
                        metrics.latest_errors_total = Some(
                            metrics
                                .latest_errors_total
                                .unwrap_or(0)
                                .max(runtime_ctx.errors_total),
                        );
                        metrics.last_in_flight = Some(runtime_ctx.in_flight);
                        metrics.last_failures_total = Some(runtime_ctx.failures_total);
                        metrics.event_loops_total =
                            metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                        metrics.event_loops_with_work_total = metrics
                            .event_loops_with_work_total
                            .max(runtime_ctx.event_loops_with_work_total);

                        metrics.snapshot_p50_ms = Some(runtime_ctx.recent_p50_ms);
                        metrics.snapshot_p90_ms = Some(runtime_ctx.recent_p90_ms);
                        metrics.snapshot_p95_ms = Some(runtime_ctx.recent_p95_ms);
                        metrics.snapshot_p99_ms = Some(runtime_ctx.recent_p99_ms);
                        metrics.snapshot_p999_ms = Some(runtime_ctx.recent_p999_ms);
                    }

                    let writer_id = envelope.event.writer_id().clone();
                    let writer_key = writer_id.to_string();
                    let seq = envelope.vector_clock.get(&writer_key);
                    let entry = metrics_store
                        .stage_vector_clocks
                        .entry(*stage_id)
                        .or_insert(0);
                    *entry = (*entry).max(seq);
                }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "flowip-059",
                        owner = "metrics_aggregator",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        error = ?e,
                        "Failed to tail-read error journal for snapshot; seeding skipped for this stage"
                    );
                }
            }
        }

        // Determine starting positions for error subscriptions by streaming to EOF.
        // This mirrors the data journal behavior and ensures error subscriptions
        // can start from tail while still observing any new events appended after
        // the metrics aggregator is created.
        let mut error_start_positions = Vec::with_capacity(error_with_names.len());
        for (stage_id, stage_name, journal) in &error_with_names {
            let start_position = match journal.reader().await {
                Ok(mut reader) => {
                    let mut pos: u64 = 0;
                    loop {
                        match reader.next().await {
                            Ok(Some(_)) => {
                                pos += 1;
                            }
                            Ok(None) => break,
                            Err(e) => {
                                tracing::warn!(
                                    target: "flowip-059",
                                    owner = "metrics_aggregator",
                                    stage_id = ?stage_id,
                                    stage_name = stage_name,
                                    error = ?e,
                                    "Failed while streaming error journal to determine tail position; starting from 0"
                                );
                                pos = 0;
                                break;
                            }
                        }
                    }
                    pos
                }
                Err(e) => {
                    tracing::warn!(
                        target: "flowip-059",
                        owner = "metrics_aggregator",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        error = ?e,
                        "Failed to create reader for error journal; starting from 0"
                    );
                    0
                }
            };

            error_start_positions.push(start_position);
        }

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
                "MetricsAggregator creating error subscription (tail-start)"
            );
        }

        // Create subscription for error journals (FLOWIP-082g), starting at
        // computed tail positions so they are treated as logically at EOF for
        // historical data while still observing any new events appended after
        // subscription creation.
        let error_subscription = if !inputs.error_journals.is_empty() {
            Some(
                crate::messaging::upstream_subscription::UpstreamSubscription::new_at_tail(
                    "metrics_aggregator",
                    &error_with_names,
                    &error_start_positions,
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

        // Build maps of journals for tail-read helpers used during export.
        let stage_data_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = inputs
            .stage_data_journals
            .iter()
            .map(|(id, journal)| (*id, journal.clone()))
            .collect();
        let stage_error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = inputs
            .error_journals
            .iter()
            .map(|(id, journal)| (*id, journal.clone()))
            .collect();

        Ok(Self {
            system_journal,
            stage_data_journals,
            stage_error_journals,
            data_subscription: Some(data_subscription),
            error_subscription,
            system_subscription: Some(system_subscription),
            include_error_journals: true, // Default to true per FLOWIP-082g
            exporter,
            metrics_store,
            export_interval_secs,
            system_id,
            export_timer: None,
            stage_metadata,
        })
    }
}

impl FsmContext for MetricsAggregatorContext {}

impl MetricsAggregatorContext {
    /// Build an `AppMetricsSnapshot` from the current in-memory `metrics_store`.
    ///
    /// This logic was originally inlined in the `ExportMetrics` action and has
    /// been refactored for reuse. It assumes that any tail-read refresh has
    /// already been applied to `metrics_store`.
    fn build_app_metrics_snapshot(&self) -> obzenflow_core::metrics::AppMetricsSnapshot {
        let store = &self.metrics_store;
        let mut snapshot = obzenflow_core::metrics::AppMetricsSnapshot::default();

        tracing::info!(
            "Exporting metrics: {} stage entries",
            store.stage_metrics.len()
        );

        // Flow-level aggregates derived from per-stage snapshots
        let mut flow_events_in_total: u64 = 0;
        let mut flow_events_out_total: u64 = 0;
        let mut flow_errors_total_snapshot: u64 = 0;
        let mut total_events_processed_snapshot: u64 = 0;
        let mut total_event_loops: u64 = 0;
        let mut total_event_loops_with_work: u64 = 0;

        // Convert stage metrics to snapshot format
        for (stage_id, metrics) in &store.stage_metrics {
            // Prefer wide-event snapshot counters when available
            let events_count = metrics.latest_events_processed_total.unwrap_or(0);
            snapshot.event_counts.insert(*stage_id, events_count);

            // Use wide-event snapshot errors_total as authoritative.
            let stage_errors_total = metrics.latest_errors_total.unwrap_or(0);
            snapshot.error_counts.insert(*stage_id, stage_errors_total);

            if !metrics.errors_by_kind.is_empty() && stage_errors_total > 0 {
                snapshot
                    .error_counts_by_kind
                    .insert(*stage_id, metrics.errors_by_kind.clone());
            }

            // Add processing time histogram reconstructed from runtime_context percentiles.
            if events_count > 0 && metrics.snapshot_p50_ms.is_some() {
                let mut percentiles = std::collections::HashMap::new();
                if let Some(p50) = metrics.snapshot_p50_ms {
                    percentiles.insert(Percentile::P50, (p50 * 1_000_000) as f64);
                }
                if let Some(p90) = metrics.snapshot_p90_ms {
                    percentiles.insert(Percentile::P90, (p90 * 1_000_000) as f64);
                }
                if let Some(p95) = metrics.snapshot_p95_ms {
                    percentiles.insert(Percentile::P95, (p95 * 1_000_000) as f64);
                }
                if let Some(p99) = metrics.snapshot_p99_ms {
                    percentiles.insert(Percentile::P99, (p99 * 1_000_000) as f64);
                }
                if let Some(p999) = metrics.snapshot_p999_ms {
                    percentiles.insert(Percentile::P999, (p999 * 1_000_000) as f64);
                }

                // Approximate sum using median; see FLOWIP-059d histogram section.
                let p50_ms = metrics.snapshot_p50_ms.unwrap_or(0);
                let sum_nanos = (p50_ms * 1_000_000) as u64 * events_count;

                let hist_snapshot = obzenflow_core::metrics::HistogramSnapshot {
                    count: events_count,
                    sum: sum_nanos as f64,
                    min: (metrics.snapshot_p50_ms.unwrap_or(0) * 1_000_000) as f64,
                    max: (metrics.snapshot_p999_ms.unwrap_or(0) * 1_000_000) as f64,
                    percentiles,
                };

                snapshot.processing_times.insert(*stage_id, hist_snapshot);
            }

            // Add runtime context metrics if available (FLOWIP-056c)
            if let Some(in_flight) = metrics.last_in_flight {
                snapshot.in_flight.insert(*stage_id, in_flight as f64);
            }

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

            // Aggregate flow-level metrics from snapshots
            total_events_processed_snapshot =
                total_events_processed_snapshot.saturating_add(events_count);

            flow_errors_total_snapshot =
                flow_errors_total_snapshot.saturating_add(stage_errors_total);

            total_event_loops = total_event_loops.saturating_add(metrics.event_loops_total);
            total_event_loops_with_work = total_event_loops_with_work
                .saturating_add(metrics.event_loops_with_work_total);

            if let Some(metadata) = self.stage_metadata.get(stage_id) {
                match metadata.stage_type {
                    obzenflow_core::event::context::StageType::FiniteSource
                    | obzenflow_core::event::context::StageType::InfiniteSource => {
                        flow_events_in_total =
                            flow_events_in_total.saturating_add(events_count);
                    }
                    obzenflow_core::event::context::StageType::Sink => {
                        flow_events_out_total =
                            flow_events_out_total.saturating_add(events_count);
                    }
                    _ => {}
                }
            }

            tracing::debug!(
                "Exported metrics for {:?}: events={}, errors_total_snapshot={}",
                stage_id,
                events_count,
                stage_errors_total
            );
        }

        // Add flow-level metrics
        if let (Some(first_time), Some(last_time)) =
            (store.first_event_time, store.last_event_time)
        {
            let flow_duration = last_time.duration_since(first_time);
            let flow_metrics = obzenflow_core::metrics::FlowMetricsSnapshot {
                flow_duration: MetricsDuration::from(flow_duration),
                total_events_processed: total_events_processed_snapshot,
                events_in: flow_events_in_total,
                events_out: flow_events_out_total,
                errors_total: flow_errors_total_snapshot,
                event_loops_total: total_event_loops,
                event_loops_with_work_total: total_event_loops_with_work,
            };
            snapshot.flow_metrics = Some(flow_metrics);
        }

        // Add stage metadata
        snapshot.stage_metadata = self.stage_metadata.clone();

        // FLOWIP-059b: Add lifecycle states
        snapshot.stage_lifecycle_states = store.stage_lifecycle_states.clone();
        snapshot.pipeline_state = store.pipeline_state.clone();

        // Add stage timestamps for rate calculation
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

            if let Some(seq) = store.stage_vector_clocks.get(stage_id) {
                snapshot.stage_vector_clocks.insert(*stage_id, *seq);
            }
        }

        snapshot
    }
}

impl MetricsStore {
    /// Returns true when every known stage has reached a terminal lifecycle
    /// state (completed or failed) according to system.events.
    pub fn all_stages_terminal(&self, stage_metadata: &HashMap<StageId, StageMetadata>) -> bool {
        stage_metadata.keys().all(|stage_id| {
            self.stage_lifecycle_states
                .get(&(*stage_id, "completed".to_string()))
                .copied()
                .unwrap_or(false)
                || self
                    .stage_lifecycle_states
                    .get(&(*stage_id, "failed".to_string()))
                    .copied()
                    .unwrap_or(false)
        })
    }

    /// Returns true when the pipeline has reached a terminal lifecycle state.
    pub fn pipeline_terminal(&self) -> bool {
        matches!(
            self.pipeline_state.as_str(),
            "completed" | "failed" | "drained"
        )
    }
}

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
                            obzenflow_core::event::StageLifecycleEvent::Completed { .. } => {
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
                        // Track only essential pipeline events, with monotonic semantics:
                        // - "failed" is sticky and never regresses.
                        // - "completed" never regresses to "drained".
                        // - "drained" is only used when no explicit outcome was ever observed.
                        match event {
                            obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                                if store.pipeline_state.is_empty() {
                                    store.pipeline_state = "all_stages_completed".to_string();
                                }
                                tracing::info!("Pipeline: all stages completed (metrics view)");
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Completed { .. } => {
                                if store.pipeline_state != "failed" {
                                    store.pipeline_state = "completed".to_string();
                                    tracing::info!("Pipeline: completed (metrics view)");
                                } else {
                                    tracing::info!(
                                        "Pipeline: completed event observed after failed; \
                                         keeping failed as terminal state (metrics view)"
                                    );
                                }
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Failed { .. } => {
                                // Failure is always terminal and sticky.
                                if store.pipeline_state != "failed" {
                                    store.pipeline_state = "failed".to_string();
                                    tracing::info!("Pipeline: failed (metrics view)");
                                }
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Drained => {
                                // Drained is a termination marker only; do not override an
                                // explicit completed/failed outcome.
                                match store.pipeline_state.as_str() {
                                    "failed" | "completed" => {
                                        tracing::info!(
                                            "Pipeline: drained event observed after terminal outcome; \
                                             keeping {} as terminal state (metrics view)",
                                            store.pipeline_state
                                        );
                                    }
                                    _ => {
                                        store.pipeline_state = "drained".to_string();
                                        tracing::info!("Pipeline: drained (metrics view)");
                                    }
                                }
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

                // Update per-stage vector clock watermark (FLOWIP-059c).
                // We use the event writer_id component from the envelope's vector clock.
                let stage_id = event.flow_context.stage_id;
                let writer_id = event.writer_id().clone();
                let writer_key = writer_id.to_string();
                let seq = envelope.vector_clock.get(&writer_key);
                let entry = store.stage_vector_clocks.entry(stage_id).or_insert(0);
                *entry = (*entry).max(seq);

                // Skip system events entirely; they are not part of per-stage wide metrics.
                if event.is_system() {
                    return Ok(());
                }

                // Track flow timing for rate calculation based on any non-system event.
                let now = std::time::Instant::now();
                if store.first_event_time.is_none() {
                    store.first_event_time = Some(now);
                    store.flow_start_time = Some(now);
                }
                store.last_event_time = Some(now);

                // Only count data/delivery events towards total_events_processed; control
                // events (EOF, consumption_final, etc.) carry snapshots but must not bump
                // the processed count. This keeps totals aligned with stage-wide metrics.
                if event.is_data() || event.is_delivery() {
                    store.total_events_processed += 1;
                }

                // Process data and delivery events using runtime_context snapshots only
                let stage_id = event.flow_context.stage_id;

                // First handle stage metrics (data + control wide events)
                {
                    let metrics = store
                        .stage_metrics
                        .entry(stage_id)
                        .or_insert_with(StageMetrics::default);

                    // Track stage timing
                    let now = std::time::Instant::now();
                    if metrics.first_event_time.is_none() {
                        metrics.first_event_time = Some(now);
                    }
                    metrics.last_event_time = Some(now);

                    // Extract runtime context metrics if available (FLOWIP-056c / FLOWIP-059d)
                    if let Some(runtime_ctx) = &event.runtime_context {
                        tracing::trace!(
                            "Runtime context for {:?}: in_flight={}, fsm_state={}",
                            stage_id,
                            runtime_ctx.in_flight,
                            runtime_ctx.fsm_state
                        );

                        // Store latest runtime metrics for export
                        // Use max() for monotonic counters to handle out-of-order reads
                        metrics.last_in_flight = Some(runtime_ctx.in_flight);
                        metrics.last_failures_total = Some(runtime_ctx.failures_total);
                        metrics.latest_events_processed_total = Some(
                            metrics
                                .latest_events_processed_total
                                .unwrap_or(0)
                                .max(runtime_ctx.events_processed_total),
                        );
                        metrics.latest_errors_total = Some(
                            metrics
                                .latest_errors_total
                                .unwrap_or(0)
                                .max(runtime_ctx.errors_total),
                        );

                        // Update cumulative event loop counters (take max to handle resets)
                        metrics.event_loops_total =
                            metrics.event_loops_total.max(runtime_ctx.event_loops_total);
                        metrics.event_loops_with_work_total = metrics
                            .event_loops_with_work_total
                            .max(runtime_ctx.event_loops_with_work_total);

                        // Update pre-computed percentiles from runtime_context
                        metrics.snapshot_p50_ms = Some(runtime_ctx.recent_p50_ms);
                        metrics.snapshot_p90_ms = Some(runtime_ctx.recent_p90_ms);
                        metrics.snapshot_p95_ms = Some(runtime_ctx.recent_p95_ms);
                        metrics.snapshot_p99_ms = Some(runtime_ctx.recent_p99_ms);
                        metrics.snapshot_p999_ms = Some(runtime_ctx.recent_p999_ms);
                    }

                    // Track error kinds for breakdown (total bounded later by snapshot errors_total)
                    if let ProcessingStatus::Error { kind, .. } = &event.processing_info.status {
                        let key = kind.clone().unwrap_or(ErrorKind::Unknown);
                        *metrics.errors_by_kind.entry(key).or_insert(0) += 1;
                    }
                } // metrics reference dropped here
                Ok(())
            }

            MetricsAggregatorAction::ExportMetrics => {
                tracing::info!("ExportMetrics action triggered");
                // Refresh in-memory stage metrics from journal tails before export so
                // `/metrics` reflects delivery truth even if subscriptions lag.
                for (stage_id, data_journal) in &ctx.stage_data_journals {
                    let error_journal = ctx.stage_error_journals.get(stage_id);
                    if let Some(snapshot) =
                        tail_read::read_stage_metrics_from_tail(
                            data_journal,
                            error_journal,
                            *stage_id,
                        )
                        .await
                    {
                        let metrics = ctx
                            .metrics_store
                            .stage_metrics
                            .entry(*stage_id)
                            .or_insert_with(StageMetrics::default);

                        metrics.latest_events_processed_total = Some(
                            metrics
                                .latest_events_processed_total
                                .unwrap_or(0)
                                .max(snapshot.events_processed_total),
                        );
                        metrics.latest_errors_total = Some(
                            metrics
                                .latest_errors_total
                                .unwrap_or(0)
                                .max(snapshot.errors_total),
                        );

                        // Use tail-read error breakdown as authoritative for this stage.
                        metrics.errors_by_kind = snapshot.errors_by_kind.clone();
                        metrics.last_in_flight = Some(snapshot.in_flight);
                        metrics.snapshot_p50_ms = Some(snapshot.recent_p50_ms);
                        metrics.snapshot_p90_ms = Some(snapshot.recent_p90_ms);
                        metrics.snapshot_p95_ms = Some(snapshot.recent_p95_ms);
                        metrics.snapshot_p99_ms = Some(snapshot.recent_p99_ms);
                        metrics.snapshot_p999_ms = Some(snapshot.recent_p999_ms);
                    }
                }

                if let Some(exporter) = &ctx.exporter {
                    let snapshot = ctx.build_app_metrics_snapshot();
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
                        next_state: MetricsAggregatorState::Draining,
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
            on MetricsAggregatorEvent::FlowTerminal => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move {
                    let last_event_id = ctx.metrics_store.last_event_id.clone();
                    let publish_last_event_id = last_event_id.clone();
                    Ok(Transition {
                        next_state: MetricsAggregatorState::Drained { last_event_id },
                        actions: vec![
                            MetricsAggregatorAction::ExportMetrics,
                            MetricsAggregatorAction::PublishDrainComplete {
                                last_event_id: publish_last_event_id,
                            },
                        ],
                    })
                })
            };

            on MetricsAggregatorEvent::ProcessSystemEvent => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move {
                    match event {
                        MetricsAggregatorEvent::ProcessSystemEvent { envelope } => Ok(Transition {
                            next_state: MetricsAggregatorState::Draining,
                            actions: vec![MetricsAggregatorAction::ProcessSystemEvent {
                                envelope: envelope.clone(),
                            }],
                        }),
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for ProcessSystemEvent handler in Draining".to_string(),
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
                                next_state: MetricsAggregatorState::Draining,
                                actions,
                            })
                        }
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event for ProcessBatch handler in Draining".to_string(),
                        )),
                    }
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
    use async_trait::async_trait;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::event::CorrelationId;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::journal::journal::Journal;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::metrics::StageMetadata;
    use obzenflow_core::event::JournalEvent;
    use std::marker::PhantomData;

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

    #[test]
    fn build_app_metrics_snapshot_uses_errors_by_kind_from_store() {
        let stage_id = StageId::new();

        // Seed MetricsStore with a single stage entry.
        let mut store = MetricsStore::default();
        let mut stage_metrics = StageMetrics::default();
        stage_metrics.latest_events_processed_total = Some(42);
        stage_metrics.latest_errors_total = Some(3);
        stage_metrics
            .errors_by_kind
            .insert(ErrorKind::Domain, 2);
        stage_metrics
            .errors_by_kind
            .insert(ErrorKind::Remote, 1);
        stage_metrics.event_loops_total = 10;
        stage_metrics.event_loops_with_work_total = 7;
        store.stage_metrics.insert(stage_id, stage_metrics);

        // Minimal stage metadata so flow aggregation can classify the stage.
        let mut stage_metadata = std::collections::HashMap::new();
        stage_metadata.insert(
            stage_id,
            StageMetadata {
                name: "test_stage".to_string(),
                stage_type: StageType::Sink,
                flow_name: "test_flow".to_string(),
            },
        );

        // Local NoopJournal implementation for the system_journal field; it is never used
        // by build_app_metrics_snapshot but satisfies the context type.
        struct NoopJournal<T: JournalEvent> {
            id: obzenflow_core::id::JournalId,
            owner: Option<JournalOwner>,
            _marker: PhantomData<T>,
        }

        impl<T: JournalEvent> NoopJournal<T> {
            fn new(owner: JournalOwner) -> Self {
                Self {
                    id: obzenflow_core::id::JournalId::new(),
                    owner: Some(owner),
                    _marker: PhantomData,
                }
            }
        }

        struct NoopReader;

        #[async_trait]
        impl<T: JournalEvent + 'static> Journal<T> for NoopJournal<T> {
            fn id(&self) -> &obzenflow_core::id::JournalId {
                &self.id
            }

            fn owner(&self) -> Option<&JournalOwner> {
                self.owner.as_ref()
            }

            async fn append(
                &self,
                _event: T,
                _parent: Option<&obzenflow_core::EventEnvelope<T>>,
            ) -> Result<obzenflow_core::EventEnvelope<T>, JournalError> {
                Err(JournalError::Implementation {
                    message: "noop journal".to_string(),
                    source: "noop".into(),
                })
            }

            async fn read_causally_ordered(
                &self,
            ) -> Result<Vec<obzenflow_core::EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_causally_after(
                &self,
                _after_event_id: &obzenflow_core::EventId,
            ) -> Result<Vec<obzenflow_core::EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_event(
                &self,
                _event_id: &obzenflow_core::EventId,
            ) -> Result<Option<obzenflow_core::EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            async fn reader(
                &self,
            ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
                Ok(Box::new(NoopReader))
            }

            async fn reader_from(
                &self,
                _position: u64,
            ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
                Ok(Box::new(NoopReader))
            }

            async fn read_last_n(
                &self,
                _count: usize,
            ) -> Result<Vec<obzenflow_core::EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }
        }

        #[async_trait]
        impl<T: JournalEvent + 'static> JournalReader<T> for NoopReader {
            async fn next(
                &mut self,
            ) -> Result<Option<obzenflow_core::EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            async fn skip(&mut self, _n: u64) -> Result<u64, JournalError> {
                Ok(0)
            }

            fn position(&self) -> u64 {
                0
            }

            fn is_at_end(&self) -> bool {
                true
            }
        }

        // Build a context with only the fields required by build_app_metrics_snapshot.
        let ctx = MetricsAggregatorContext {
            system_journal: Arc::new(
                NoopJournal::<obzenflow_core::event::SystemEvent>::new(
                    JournalOwner::system(obzenflow_core::SystemId::new()),
                ),
            ),
            stage_data_journals: HashMap::new(),
            stage_error_journals: HashMap::new(),
            data_subscription: None,
            error_subscription: None,
            system_subscription: None,
            include_error_journals: true,
            exporter: None,
            metrics_store: store,
            export_interval_secs: 10,
            system_id: obzenflow_core::SystemId::new(),
            export_timer: None,
            stage_metadata,
        };

        let snapshot = ctx.build_app_metrics_snapshot();

        // Stage-level totals should reflect the seeded store.
        assert_eq!(snapshot.error_counts.get(&stage_id), Some(&3));
        let by_kind = snapshot
            .error_counts_by_kind
            .get(&stage_id)
            .expect("per-kind breakdown should be present");
        assert_eq!(by_kind.get(&ErrorKind::Domain), Some(&2));
        assert_eq!(by_kind.get(&ErrorKind::Remote), Some(&1));

        // Stage metadata should be carried through.
        assert!(snapshot.stage_metadata.get(&stage_id).is_some());
    }
}
