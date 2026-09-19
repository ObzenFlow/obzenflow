// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator FSM types and state machine definition
//!
//! The metrics aggregator follows a simple lifecycle:
//! Initializing -> Running -> Draining -> Drained
//! Event processing happens directly without FSM state tracking

use obzenflow_core::event::chain_event::ChainPayload;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::observability::{
    HttpPullMetricsSnapshot, HttpSurfaceRouteMetricsSnapshot,
};
use obzenflow_core::event::observability::{MeasurementWindow, RuntimeObservability};
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, CircuitState, ExecutionPayload, HttpPullStateFact,
};
use obzenflow_core::event::provenance::RuntimeProvenance;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{SinkOperationPhase, SystemPayload, WriterId};
use obzenflow_core::id::{FlowId, StageId, SystemId};
use obzenflow_core::ingress::IngressKey;
use obzenflow_core::metrics::{
    BoundaryMetricsView, ContractMetricEdgeKey, ContractMetricsSnapshot, Percentile, StageMetadata,
};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::web::HttpMethod;
use obzenflow_core::{ChainEvent, EventId, EventType, Journal, JournalRecord};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, StateMachine, StateVariant, Transition,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

/// FSM states for metrics aggregator lifecycle
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricsAggregatorState {
    /// Initial state
    Initializing,

    /// Periodic publication of the latest available values
    Running,

    /// A bounded final refresh before stopping readers and publishing
    Draining,

    /// Terminal state - latest available values published and readers stopped
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

    /// Time to export metrics
    ExportMetrics,

    /// Start draining process (from journal control event)
    StartDraining,

    /// Flow + stages have reached terminal lifecycle; perform final export and shutdown
    FlowTerminal,

    /// Error occurred (e.g., journal corruption)
    Error(String),
}

/// Durable journal rail from which a metrics event was read. Composite
/// boundary duration observes only committed data-journal facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricsJournalKind {
    Data,
    Error,
}

impl EventVariant for MetricsAggregatorEvent {
    fn variant_name(&self) -> &str {
        match self {
            MetricsAggregatorEvent::StartRunning => "StartRunning",
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

    /// Apply selected current carriers, newest first, without counting occurrences.
    UpdateMetrics {
        events: Arc<[JournalRecord<ChainPayload>]>,
        journal_kind: MetricsJournalKind,
        journal_stage: StageId,
    },

    /// Process system events from the system journal (FLOWIP-059b)
    ProcessSystemEvent {
        envelope: Box<JournalRecord<SystemPayload>>,
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

    /// Stage data journals for committed-observation lookup.
    pub stage_data_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,

    /// Stage error journals for committed-observation lookup.
    pub stage_error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,

    /// Flow-scoped backpressure registry for observability (FLOWIP-086k).
    pub backpressure_registry: Option<Arc<crate::backpressure::BackpressureRegistry>>,

    /// Whether to include error journals in metrics collection
    pub include_error_journals: bool,

    pub metrics_exporter: Arc<dyn obzenflow_core::metrics::MetricsSnapshotExporter>,
    pub metrics_store: MetricsStore,
    pub export_interval: std::time::Duration,
    pub system_id: SystemId,
    #[doc(hidden)]
    pub pipeline_writer: Option<WriterId>,
    pub stage_metadata: HashMap<StageId, StageMetadata>,
    /// Composite boundaries (FLOWIP-128a B4), built once from the subgraph
    /// registry; each export projects composite RED metrics from them.
    #[doc(hidden)]
    pub composite_boundaries: Vec<obzenflow_core::metrics::CompositeBoundary>,
}

/// Simple metrics storage
#[derive(Default)]
#[doc(hidden)]
pub struct MetricsStore {
    pub(crate) observations: Arc<super::observations::ObservationRegistry>,
    pub(crate) last_export_completed: Option<tokio::time::Instant>,
    pub(crate) next_export_at: Option<tokio::time::Instant>,
    pub(crate) throughput: super::throughput::ThroughputSampler,
    pub(crate) buffer: Arc<super::buffer::MetricsBuffer>,
    pub stage_metrics: std::collections::HashMap<StageId, StageMetrics>,
    pub last_event_id: Option<EventId>,
    pub flow_start_time: Option<std::time::Instant>,
    pub first_event_time: Option<std::time::Instant>,
    pub last_event_time: Option<std::time::Instant>,
    pub total_events_processed: u64,

    /// Projection of durable sink-operation failure facts only.
    pub sink_operation_failures: HashMap<(StageId, SinkOperationPhase, ErrorKind), u64>,

    /// Per-stage vector clock watermark (FLOWIP-059c)
    /// Highest selected own-writer carrier from the data journal. This is a
    /// freshness position, not evidence of complete physical coverage.
    pub stage_vector_clocks: HashMap<StageId, u64>,

    /// Per-system vector clock watermark (FLOWIP-059c).
    ///
    /// Tracks the highest writer sequence observed for each system writer in the system journal.
    /// We intentionally track only `WriterId::System` clocks to avoid confusing stage-journal
    /// freshness with system-journal stage lifecycle events (separate clock domains).
    pub system_vector_clocks: HashMap<SystemId, u64>,

    // Middleware observability accumulation (FLOWIP-059a)
    pub circuit_breaker_state: HashMap<StageId, f64>,
    pub circuit_breaker_rejection_rate: HashMap<StageId, f64>,
    pub circuit_breaker_consecutive_failures: HashMap<StageId, f64>,
    pub circuit_breaker_requests_total: HashMap<StageId, u64>,
    pub circuit_breaker_rejections_total: HashMap<StageId, u64>,
    pub circuit_breaker_opened_total: HashMap<StageId, u64>,
    pub circuit_breaker_successes_total: HashMap<StageId, u64>,
    pub circuit_breaker_failures_total: HashMap<StageId, u64>,
    pub circuit_breaker_slow_total: HashMap<StageId, u64>,
    pub circuit_breaker_time_in_state_seconds_total: HashMap<(StageId, String), f64>,
    pub circuit_breaker_state_transitions_total: HashMap<(StageId, String, String), u64>,

    /// Counts rate-limiter admissions, not guaranteed downstream commits (a later middleware
    /// returning `Skip`/`Abort` does not refund — FLOWIP-114m known limitation).
    pub rate_limiter_utilization: HashMap<StageId, f64>,
    pub rate_limiter_events_total: HashMap<StageId, u64>,
    pub rate_limiter_delayed_total: HashMap<StageId, u64>,
    /// Counts rate-limiter token consumption, not guaranteed downstream commits (a later middleware
    /// returning `Skip`/`Abort` does not refund — FLOWIP-114m known limitation).
    pub rate_limiter_tokens_consumed_total: HashMap<StageId, f64>,
    pub rate_limiter_delay_seconds_total: HashMap<StageId, f64>,
    // Bucket state for gauge metrics (FLOWIP-059a-3 Issue 3)
    pub rate_limiter_bucket_tokens: HashMap<StageId, f64>,
    pub rate_limiter_bucket_capacity: HashMap<StageId, f64>,

    // Contract metrics accumulation (FLOWIP-059a)
    pub contract_metrics: ContractMetricsSnapshot,

    // Edge liveness state (FLOWIP-063e).
    //
    // Gauge semantics: 1=Healthy, 0.5=Idle, 0.25=Suspect, 0=Stalled.
    pub edge_liveness_state: HashMap<(StageId, StageId), obzenflow_core::event::EdgeLivenessState>,

    // Hosted web surface metrics (FLOWIP-093a)
    pub http_surface_metrics:
        HashMap<(String, HttpMethod, String, String), HttpSurfaceRouteMetricsSnapshot>,

    /// Reserved historical refusal totals. The live reader leaves these absent.
    pub ingestion_refusals_total: HashMap<(IngressKey, String), u64>,

    // HTTP pull telemetry (FLOWIP-084e)
    pub http_pull_metrics: HashMap<StageId, HttpPullMetricsSnapshot>,

    // AI chunking telemetry (FLOWIP-086z)
    pub ai_chunking_metrics: HashMap<StageId, obzenflow_core::metrics::AiChunkingMetricsSnapshot>,

    // System event tracking (FLOWIP-059b - essential events only)
    // Retained current states: (StageId, state_name) -> true
    pub stage_lifecycle_states: HashMap<(StageId, String), bool>,
    pub pipeline_state: String,
}

#[derive(Clone, Default)]
pub struct StageMetrics {
    pub errors_by_kind: HashMap<ErrorKind, u64>,
    // Runtime context metrics (FLOWIP-056c / FLOWIP-059 Phase 6)
    pub last_in_flight: Option<u32>,
    pub last_failures_total: Option<u64>,
    // Join-only gauge (Live join): number of reference events processed since the last stream event.
    pub join_reference_since_last_stream: Option<u64>,
    // Wide-event snapshot counters (Phase 6)
    pub latest_events_processed_total: Option<u64>,
    pub latest_events_accumulated_total: Option<u64>,
    pub latest_events_emitted_total: Option<u64>,
    /// Cumulative committed Data outputs by exact event type.
    pub latest_data_outputs_by_event_type: HashMap<EventType, u64>,
    /// Cumulative admitted Data inputs by physical upstream and exact type.
    pub latest_data_inputs_by_upstream_event_type: HashMap<(StageId, EventType), u64>,
    pub latest_errors_total: Option<u64>,
    pub event_loops_total: Option<u64>,
    pub event_loops_with_work_total: Option<u64>,
    // Wide-event snapshot percentiles (Phase 6) - pre-computed by stage, in milliseconds
    pub snapshot_p50_ms: Option<u64>,
    pub snapshot_p90_ms: Option<u64>,
    pub snapshot_p95_ms: Option<u64>,
    pub snapshot_p99_ms: Option<u64>,
    pub snapshot_p999_ms: Option<u64>,
    // Actual sum of processing times (nanoseconds) - never reconstructed from percentiles
    pub processing_time_sum_nanos: Option<u64>,
    pub processing_time_count: Option<u64>,
    pub timing_window: Option<MeasurementWindow>,
    // Stage-specific timing for accurate rate calculation
    pub first_event_time: Option<std::time::Instant>,
    pub last_event_time: Option<std::time::Instant>,
}

impl StageMetrics {
    pub(super) fn merge_runtime_measurements(&mut self, runtime: &RuntimeObservability) {
        if let Some(in_flight) = runtime.in_flight {
            self.last_in_flight = Some(in_flight);
        }
        if let Some(join_reference_since_last_stream) = runtime.join_reference_since_last_stream {
            self.join_reference_since_last_stream = Some(join_reference_since_last_stream);
        }
        if let Some(event_loops_total) = runtime.event_loops_total {
            self.event_loops_total = Some(event_loops_total);
        }
        if let Some(event_loops_with_work_total) = runtime.event_loops_with_work_total {
            self.event_loops_with_work_total = Some(event_loops_with_work_total);
        }
        if let Some(timing) = &runtime.timing {
            if timing.is_valid() {
                self.processing_time_count = Some(timing.processing_time_count);
                self.processing_time_sum_nanos = Some(timing.processing_time_sum_nanos);
                self.timing_window = Some(timing.window);
                self.snapshot_p50_ms = timing.recent_p50_ms;
                self.snapshot_p90_ms = timing.recent_p90_ms;
                self.snapshot_p95_ms = timing.recent_p95_ms;
                self.snapshot_p99_ms = timing.recent_p99_ms;
                self.snapshot_p999_ms = timing.recent_p999_ms;
            }
        }
    }

    /// Merge protected accounting with cumulative maxima so repeated evidence,
    /// cross-journal ordering, and replay remain stable.
    pub(super) fn merge_runtime_context(&mut self, runtime_ctx: &RuntimeProvenance) {
        self.merge_accounting(&runtime_ctx.accounting);
    }

    pub(super) fn merge_accounting(
        &mut self,
        accounting: &obzenflow_core::event::provenance::ExecutionAccounting,
    ) {
        self.last_failures_total = Some(
            self.last_failures_total
                .unwrap_or(0)
                .max(accounting.failures_total),
        );
        self.latest_events_processed_total = Some(
            self.latest_events_processed_total
                .unwrap_or(0)
                .max(accounting.events_processed_total),
        );
        self.latest_events_accumulated_total = Some(
            self.latest_events_accumulated_total
                .unwrap_or(0)
                .max(accounting.events_accumulated_total),
        );
        self.latest_events_emitted_total = Some(
            self.latest_events_emitted_total
                .unwrap_or(0)
                .max(accounting.events_emitted_total),
        );
        self.latest_errors_total = Some(
            self.latest_errors_total
                .unwrap_or(0)
                .max(accounting.errors_total),
        );
        for (kind, count) in &accounting.errors_by_kind {
            let current = self.errors_by_kind.entry(kind.clone()).or_insert(0);
            *current = (*current).max(*count);
        }
        for count in &accounting.data_outputs_by_event_type {
            let current = self
                .latest_data_outputs_by_event_type
                .entry(count.event_type.clone())
                .or_insert(0);
            *current = (*current).max(count.total);
        }
        for count in &accounting.data_inputs_by_upstream_event_type {
            let current = self
                .latest_data_inputs_by_upstream_event_type
                .entry((count.upstream, count.event_type.clone()))
                .or_insert(0);
            *current = (*current).max(count.total);
        }
    }
}

impl BoundaryMetricsView for MetricsStore {
    fn data_inputs(&self, member: StageId, upstream: StageId, event_type: &EventType) -> u64 {
        self.stage_metrics
            .get(&member)
            .and_then(|metrics| {
                metrics
                    .latest_data_inputs_by_upstream_event_type
                    .get(&(upstream, event_type.clone()))
            })
            .copied()
            .unwrap_or(0)
    }

    fn data_outputs(&self, member: StageId, event_type: &EventType) -> u64 {
        self.stage_metrics
            .get(&member)
            .and_then(|metrics| metrics.latest_data_outputs_by_event_type.get(event_type))
            .copied()
            .unwrap_or(0)
    }

    fn errors(&self, member: StageId) -> u64 {
        self.stage_metrics
            .get(&member)
            .and_then(|metrics| metrics.latest_errors_total)
            .unwrap_or(0)
    }
}

impl MetricsAggregatorContext {
    fn refresh_measurements(&mut self) {
        use obzenflow_core::event::observability::ObservationSource;

        self.metrics_store.refresh_measurements();
        for packet in self.metrics_store.observations.snapshot() {
            let Some(snapshot) = packet.runtime_snapshot else {
                continue;
            };
            let Some(stage_id) = snapshot.capture.observer.as_stage() else {
                continue;
            };
            if let Some(meta) = self.stage_metadata.get_mut(stage_id) {
                // This is only a reporting label. Factual lifecycle state and
                // collector coverage never come from the diagnostic snapshot.
                if meta.reference_mode.is_none() && meta.stage_type == StageType::Join {
                    meta.reference_mode =
                        infer_join_reference_mode_from_fsm_state(&snapshot.fsm_state)
                            .map(str::to_owned);
                }
            }
        }
    }

    pub(crate) async fn new(
        inputs: crate::metrics::inputs::MetricsInputs,
        system_journal: Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
        metrics_exporter: Arc<dyn obzenflow_core::metrics::MetricsSnapshotExporter>,
        export_interval: std::time::Duration,
        system_id: SystemId,
        stage_metadata: HashMap<StageId, StageMetadata>,
        composite_boundaries: Vec<obzenflow_core::metrics::CompositeBoundary>,
    ) -> Result<Self, String> {
        let metrics_store = MetricsStore {
            observations: inputs.observations.clone(),
            throughput: super::throughput::ThroughputSampler::new(inputs.execution.as_ref().map(
                |(flow, execution)| {
                    inputs.observations.capture_owner(
                        super::observations::scope(execution, *flow),
                        system_id.into(),
                        execution.clone(),
                    )
                },
            )),
            ..MetricsStore::default()
        };
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

        let context = Self {
            system_journal,
            stage_data_journals,
            stage_error_journals,
            backpressure_registry: inputs.backpressure_registry.clone(),
            include_error_journals: true, // Default to true per FLOWIP-082g
            metrics_exporter,
            metrics_store,
            export_interval,
            system_id,
            pipeline_writer: None,
            stage_metadata,
            composite_boundaries,
        };

        Ok(context)
    }
}

impl FsmContext for MetricsAggregatorContext {}

fn infer_join_reference_mode_from_fsm_state(fsm_state: &str) -> Option<&'static str> {
    match fsm_state {
        "Live" => Some("live"),
        "Hydrating" | "Enriching" => Some("finite_eof"),
        _ => None,
    }
}

impl MetricsAggregatorContext {
    /// Build an `AppMetricsSnapshot` from the current in-memory `metrics_store`.
    ///
    /// This logic was originally inlined in the `ExportMetrics` action and has
    /// been refactored for reuse. It assumes that any tail-read refresh has
    /// already been applied to `metrics_store`.
    fn build_app_metrics_snapshot(&self) -> obzenflow_core::metrics::AppMetricsSnapshot {
        let store = &self.metrics_store;
        let mut snapshot = obzenflow_core::metrics::AppMetricsSnapshot::default();

        tracing::debug!(
            "Exporting metrics: {} stage entries",
            store.stage_metrics.len()
        );

        // Flow-level aggregates derived from per-stage snapshots
        let mut flow_events_in_total: u64 = 0;
        let mut flow_events_out_total: u64 = 0;
        let mut flow_errors_total_snapshot: u64 = 0;
        let mut total_events_processed_snapshot: u64 = 0;
        let mut total_event_loops = Some(0u64);
        let mut total_event_loops_with_work = Some(0u64);

        // Convert stage metrics to snapshot format
        for (stage_id, metrics) in &store.stage_metrics {
            // Prefer wide-event snapshot counters when available
            let stage_events_processed_total = metrics.latest_events_processed_total.unwrap_or(0);
            if let Some(events_processed_total) = metrics.latest_events_processed_total {
                snapshot
                    .event_counts
                    .insert(*stage_id, events_processed_total);
            }

            if let Some(events_accumulated_total) = metrics.latest_events_accumulated_total {
                snapshot
                    .events_accumulated_total
                    .insert(*stage_id, events_accumulated_total);
            }

            if let Some(events_emitted_total) = metrics.latest_events_emitted_total {
                snapshot
                    .events_emitted_total
                    .insert(*stage_id, events_emitted_total);
            }

            if let Some(join_reference_since_last_stream) = metrics.join_reference_since_last_stream
            {
                if let Some(metadata) = self.stage_metadata.get(stage_id) {
                    if metadata.stage_type == StageType::Join {
                        snapshot
                            .join_reference_since_last_stream
                            .insert(*stage_id, join_reference_since_last_stream);
                    }
                }
            }

            // Use wide-event snapshot errors_total as authoritative.
            let stage_errors_total = metrics.latest_errors_total.unwrap_or(0);
            if let Some(errors_total) = metrics.latest_errors_total {
                snapshot.error_counts.insert(*stage_id, errors_total);
            }

            if !metrics.errors_by_kind.is_empty() && stage_errors_total > 0 {
                snapshot
                    .error_counts_by_kind
                    .insert(*stage_id, metrics.errors_by_kind.clone());
            }

            // Add processing time histogram reconstructed from runtime_context percentiles.
            if let (Some(count), Some(sum_nanos)) = (
                metrics.processing_time_count,
                metrics.processing_time_sum_nanos,
            ) {
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

                // Use actual sum - never reconstructed from percentiles (FLOWIP-059a-3)

                let hist_snapshot = obzenflow_core::metrics::HistogramSnapshot {
                    count,
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

            if let Some(event_loops_total) = metrics.event_loops_total {
                snapshot
                    .event_loops_total
                    .insert(*stage_id, event_loops_total);
            }
            if let Some(event_loops_with_work_total) = metrics.event_loops_with_work_total {
                snapshot
                    .event_loops_with_work_total
                    .insert(*stage_id, event_loops_with_work_total);
            }

            // Aggregate flow-level metrics from snapshots
            total_events_processed_snapshot =
                total_events_processed_snapshot.saturating_add(stage_events_processed_total);

            flow_errors_total_snapshot =
                flow_errors_total_snapshot.saturating_add(stage_errors_total);

            total_event_loops = total_event_loops
                .zip(metrics.event_loops_total)
                .map(|(flow_total, stage_total)| flow_total.saturating_add(stage_total));
            total_event_loops_with_work = total_event_loops_with_work
                .zip(metrics.event_loops_with_work_total)
                .map(|(flow_total, stage_total)| flow_total.saturating_add(stage_total));

            if let Some(metadata) = self.stage_metadata.get(stage_id) {
                match metadata.stage_type {
                    obzenflow_core::event::context::StageType::FiniteSource
                    | obzenflow_core::event::context::StageType::InfiniteSource => {
                        flow_events_in_total =
                            flow_events_in_total.saturating_add(stage_events_processed_total);
                    }
                    obzenflow_core::event::context::StageType::Sink => {
                        flow_events_out_total =
                            flow_events_out_total.saturating_add(stage_events_processed_total);
                    }
                    _ => {}
                }
            }

            tracing::debug!(
                "Exported metrics for {:?}: events={}, errors_total_snapshot={}",
                stage_id,
                stage_events_processed_total,
                stage_errors_total
            );
        }

        // Add flow-level metrics
        if let (Some(first_time), Some(last_time)) = (store.first_event_time, store.last_event_time)
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
        snapshot.throughput = store.throughput.latest.clone();
        snapshot.observation_export_interval = Some(self.export_interval);
        snapshot.sink_operation_failures = store
            .sink_operation_failures
            .iter()
            .map(|((stage_id, phase, error_kind), count)| {
                obzenflow_core::metrics::SinkOperationFailureMetric {
                    stage_id: *stage_id,
                    phase: *phase,
                    error_kind: error_kind.clone(),
                    count: *count,
                }
            })
            .collect();

        // FLOWIP-059a: Middleware metrics
        snapshot.circuit_breaker_state = store.circuit_breaker_state.clone();
        snapshot.circuit_breaker_rejection_rate = store.circuit_breaker_rejection_rate.clone();
        snapshot.circuit_breaker_consecutive_failures =
            store.circuit_breaker_consecutive_failures.clone();
        snapshot.circuit_breaker_requests_total = store.circuit_breaker_requests_total.clone();
        snapshot.circuit_breaker_rejections_total = store.circuit_breaker_rejections_total.clone();
        snapshot.circuit_breaker_opened_total = store.circuit_breaker_opened_total.clone();
        snapshot.circuit_breaker_successes_total = store.circuit_breaker_successes_total.clone();
        snapshot.circuit_breaker_failures_total = store.circuit_breaker_failures_total.clone();
        snapshot.circuit_breaker_slow_total = store.circuit_breaker_slow_total.clone();
        snapshot.circuit_breaker_time_in_state_seconds_total =
            store.circuit_breaker_time_in_state_seconds_total.clone();
        snapshot.circuit_breaker_state_transitions_total =
            store.circuit_breaker_state_transitions_total.clone();

        snapshot.rate_limiter_utilization = store.rate_limiter_utilization.clone();
        snapshot.rate_limiter_events_total = store.rate_limiter_events_total.clone();
        snapshot.rate_limiter_delayed_total = store.rate_limiter_delayed_total.clone();
        snapshot.rate_limiter_tokens_consumed_total =
            store.rate_limiter_tokens_consumed_total.clone();
        snapshot.rate_limiter_delay_seconds_total = store.rate_limiter_delay_seconds_total.clone();
        snapshot.rate_limiter_bucket_tokens = store.rate_limiter_bucket_tokens.clone();
        snapshot.rate_limiter_bucket_capacity = store.rate_limiter_bucket_capacity.clone();

        // FLOWIP-086k: Backpressure metrics (registry snapshot, not event-derived).
        snapshot.backpressure_bypass_enabled =
            crate::backpressure::BackpressureWriter::is_bypass_enabled();
        if let Some(registry) = &self.backpressure_registry {
            let bp = registry.metrics_snapshot();

            snapshot.backpressure_window = bp.edge_window;
            snapshot.backpressure_in_flight = bp.edge_in_flight;
            snapshot.backpressure_credits = bp.edge_credits;

            snapshot.backpressure_blocked = bp
                .stage_blocked
                .into_iter()
                .map(|(stage_id, blocked)| (stage_id, if blocked { 1.0 } else { 0.0 }))
                .collect();
            snapshot.backpressure_min_reader_seq = bp.stage_min_reader_seq;
            snapshot.backpressure_writer_seq = bp.stage_writer_seq;
            snapshot.backpressure_wait_seconds_total = bp
                .stage_wait_nanos_total
                .into_iter()
                .map(|(stage_id, nanos)| (stage_id, nanos as f64 / 1_000_000_000.0))
                .collect();
        }

        // FLOWIP-063e: Edge liveness (transition-derived gauge from system events).
        snapshot.edge_liveness_state = store.edge_liveness_state.clone();

        // FLOWIP-059a: Contract metrics
        snapshot.contract_metrics = store.contract_metrics.clone();

        // FLOWIP-093a: Generic hosted web surface metrics (system events)
        let mut http_surface_metrics: Vec<_> =
            store.http_surface_metrics.values().cloned().collect();
        http_surface_metrics.sort_by(|a, b| {
            (
                a.surface_name.as_str(),
                a.path.as_str(),
                a.method.as_str(),
                a.status_class.as_str(),
            )
                .cmp(&(
                    b.surface_name.as_str(),
                    b.path.as_str(),
                    b.method.as_str(),
                    b.status_class.as_str(),
                ))
        });
        snapshot.http_surface_metrics = http_surface_metrics;

        // FLOWIP-115d: hosted-ingress refusal totals projected from `IngressRefusal`
        // facts, keyed by (ingress_key, reason).
        snapshot.ingestion_refusal_totals = store.ingestion_refusals_total.clone();

        // FLOWIP-084e: HTTP pull telemetry (wide events)
        snapshot.http_pull_metrics = store.http_pull_metrics.clone();

        // FLOWIP-086z: AI chunking telemetry (wide events)
        snapshot.ai_chunking_metrics = store.ai_chunking_metrics.clone();

        // FLOWIP-059b: Add lifecycle states
        snapshot.stage_lifecycle_states = store.stage_lifecycle_states.clone();
        snapshot.pipeline_state = store.pipeline_state.clone();

        // Add stage timestamps for rate calculation
        let now = std::time::Instant::now();
        let now_utc = chrono::Utc::now();

        for (stage_id, metrics) in &store.stage_metrics {
            if let Some(first_time) = metrics.first_event_time {
                let elapsed_since_first = now.duration_since(first_time);
                let first_datetime =
                    now_utc - chrono::Duration::from_std(elapsed_since_first).unwrap_or_default();
                snapshot
                    .stage_first_event_time
                    .insert(*stage_id, first_datetime);
            }
            if let Some(last_time) = metrics.last_event_time {
                let elapsed_since_last = now.duration_since(last_time);
                let last_datetime =
                    now_utc - chrono::Duration::from_std(elapsed_since_last).unwrap_or_default();
                snapshot
                    .stage_last_event_time
                    .insert(*stage_id, last_datetime);
            }

            if let Some(seq) = store.stage_vector_clocks.get(stage_id) {
                snapshot.stage_vector_clocks.insert(*stage_id, *seq);
            }
        }

        // FLOWIP-128a B3: project logical traffic from exact named graph-cut
        // edges and typed wide-event counters. Output fan-out never multiplies
        // the authored fact count.
        use obzenflow_core::metrics::{CompositeMemberHealth, CompositePortTraffic};
        snapshot.composite_port_traffic = self
            .composite_boundaries
            .iter()
            .flat_map(|boundary| CompositePortTraffic::project(boundary, store))
            .collect();
        snapshot.composite_port_traffic.sort_by(|left, right| {
            (
                left.composite.as_str(),
                left.direction.as_str(),
                left.port.as_str(),
            )
                .cmp(&(
                    right.composite.as_str(),
                    right.direction.as_str(),
                    right.port.as_str(),
                ))
        });
        snapshot.composite_member_health = self
            .composite_boundaries
            .iter()
            .map(|boundary| CompositeMemberHealth::project(boundary, store))
            .collect();
        snapshot
            .composite_member_health
            .sort_by(|left, right| left.composite.cmp(&right.composite));

        // FLOWIP-128a B5: re-key the boundary members' contract facts to the
        // composite boundary. Pure relabel of the contract_metrics set just
        // built; reporting projections render these as composite contract families.
        use obzenflow_core::metrics::CompositeContract;
        snapshot.composite_contracts = self
            .composite_boundaries
            .iter()
            .flat_map(|b| CompositeContract::project(b, &snapshot.contract_metrics))
            .collect();
        snapshot.composite_contracts.sort_by(|left, right| {
            (
                left.composite.as_str(),
                left.direction.as_str(),
                left.port.as_str(),
                left.peer,
                left.selected_event_type.as_ref(),
                left.feed_role.map(|role| role.as_str()),
            )
                .cmp(&(
                    right.composite.as_str(),
                    right.direction.as_str(),
                    right.port.as_str(),
                    right.peer,
                    right.selected_event_type.as_ref(),
                    right.feed_role.map(|role| role.as_str()),
                ))
        });

        snapshot
    }
}

impl MetricsStore {
    fn retain_accounting(
        &mut self,
        stage: StageId,
        accounting: &obzenflow_core::event::provenance::ExecutionAccounting,
    ) {
        let metrics = self.stage_metrics.entry(stage).or_default();
        let changed = metrics
            .latest_events_processed_total
            .is_none_or(|previous_total| accounting.events_processed_total > previous_total)
            || metrics
                .latest_events_emitted_total
                .is_none_or(|previous_total| accounting.events_emitted_total > previous_total)
            || metrics
                .latest_events_accumulated_total
                .is_none_or(|previous_total| accounting.events_accumulated_total > previous_total)
            || metrics
                .latest_errors_total
                .is_none_or(|previous_total| accounting.errors_total > previous_total);
        metrics.merge_accounting(accounting);
        if changed {
            // Times describe changes observed by this live view. Re-exporting
            // an unchanged slot must not manufacture fresh activity.
            let now = std::time::Instant::now();
            metrics.first_event_time.get_or_insert(now);
            metrics.last_event_time = Some(now);
            self.first_event_time.get_or_insert(now);
            self.last_event_time = Some(now);
        }
    }

    fn fold_http_pull_state(&mut self, stage_id: StageId, state: &HttpPullStateFact) {
        let entry = self.http_pull_metrics.entry(stage_id).or_default();
        entry.state = Some(state.state);
        entry.wait_reason = state.wait_reason;
        entry.next_wake_unix_secs = state.next_wake_unix_secs;
        entry.last_success_unix_secs = entry
            .last_success_unix_secs
            .max(state.last_success_unix_secs);
    }

    /// Reconcile per-stage terminal state from the pipeline aggregate barrier.
    ///
    /// `PipelineLifecycle::AllStagesCompleted` is written only after the pipeline
    /// supervisor has observed every stage completion. The metrics aggregator has
    /// an independent system-journal reader, so this aggregate event is the
    /// authoritative fallback when it misses an individual stage completion.
    pub fn mark_known_stages_completed<I>(&mut self, stage_ids: I)
    where
        I: IntoIterator<Item = StageId>,
    {
        for stage_id in stage_ids {
            let has_terminal_state = self
                .stage_lifecycle_states
                .get(&(stage_id, "completed".to_string()))
                .copied()
                .unwrap_or(false)
                || self
                    .stage_lifecycle_states
                    .get(&(stage_id, "failed".to_string()))
                    .copied()
                    .unwrap_or(false)
                || self
                    .stage_lifecycle_states
                    .get(&(stage_id, "cancelled".to_string()))
                    .copied()
                    .unwrap_or(false);

            if !has_terminal_state {
                self.stage_lifecycle_states
                    .retain(|(stage, _), _| *stage != stage_id);
                self.stage_lifecycle_states
                    .insert((stage_id, "completed".to_string()), true);
            }
        }
    }

    /// Returns true when every known stage has reached a terminal lifecycle
    /// state (completed, failed, or cancelled) according to system.events.
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
                || self
                    .stage_lifecycle_states
                    .get(&(*stage_id, "cancelled".to_string()))
                    .copied()
                    .unwrap_or(false)
        })
    }

    /// Returns true when the pipeline has reached a terminal lifecycle state.
    pub fn pipeline_terminal(&self) -> bool {
        matches!(
            self.pipeline_state.as_str(),
            "completed" | "failed" | "cancelled" | "not_started"
        )
    }

    fn update_control_measurements(&mut self, stage_id: StageId, runtime: &RuntimeObservability) {
        if let Some(cb) = &runtime.circuit_breaker {
            self.circuit_breaker_requests_total
                .insert(stage_id, cb.requests_total);
            self.circuit_breaker_rejections_total
                .insert(stage_id, cb.rejections_total);
            self.circuit_breaker_opened_total
                .insert(stage_id, cb.opened_total);
            self.circuit_breaker_successes_total
                .insert(stage_id, cb.successes_total);
            self.circuit_breaker_failures_total
                .insert(stage_id, cb.failures_total);
            self.circuit_breaker_slow_total
                .insert(stage_id, cb.slow_total);
            for (state, seconds) in [
                ("closed", cb.time_closed_seconds),
                ("open", cb.time_open_seconds),
                ("half_open", cb.time_half_open_seconds),
            ] {
                self.circuit_breaker_time_in_state_seconds_total
                    .insert((stage_id, state.to_string()), seconds);
            }
        }
        if let Some(rl) = &runtime.rate_limiter {
            self.rate_limiter_events_total
                .insert(stage_id, rl.events_total);
            self.rate_limiter_delayed_total
                .insert(stage_id, rl.delayed_total);
            self.rate_limiter_tokens_consumed_total
                .insert(stage_id, rl.tokens_consumed_total);
            self.rate_limiter_delay_seconds_total
                .insert(stage_id, rl.delay_seconds_total);
            self.rate_limiter_bucket_tokens
                .insert(stage_id, rl.bucket_tokens);
            self.rate_limiter_bucket_capacity
                .insert(stage_id, rl.bucket_capacity);
            if rl.bucket_capacity > 0.0 {
                self.rate_limiter_utilization.insert(
                    stage_id,
                    (1.0 - rl.bucket_tokens / rl.bucket_capacity).clamp(0.0, 1.0),
                );
            }
        }
    }

    fn refresh_measurements(&mut self) {
        use obzenflow_core::event::observability::{ObservationRecord, ObservationSource};
        for packet in self.observations.snapshot() {
            for record in &packet.records {
                if let ObservationRecord::HttpSurface { snapshot } = record {
                    for route in &snapshot.routes {
                        let key = (
                            route.surface_name.clone(),
                            route.method,
                            route.path.clone(),
                            route.status_class.clone(),
                        );
                        self.http_surface_metrics.insert(key, route.clone());
                    }
                }
            }
            let Some(stage_id) = packet.capture.observer.as_stage().copied() else {
                continue;
            };
            if let Some(runtime) = &packet.runtime {
                self.stage_metrics
                    .entry(stage_id)
                    .or_default()
                    .merge_runtime_measurements(runtime);
                self.update_control_measurements(stage_id, runtime);
            }
            for record in packet.records {
                match record {
                    ObservationRecord::CircuitBreakerSummary {
                        effect_type: None,
                        consecutive_failures,
                        rejection_rate,
                        successes_total,
                        failures_total,
                        opened_total,
                        time_in_closed_seconds,
                        time_in_open_seconds,
                        time_in_half_open_seconds,
                        ..
                    } => {
                        self.circuit_breaker_consecutive_failures
                            .insert(stage_id, consecutive_failures as f64);
                        self.circuit_breaker_rejection_rate
                            .insert(stage_id, rejection_rate);
                        self.circuit_breaker_successes_total
                            .insert(stage_id, successes_total);
                        self.circuit_breaker_failures_total
                            .insert(stage_id, failures_total);
                        self.circuit_breaker_opened_total
                            .insert(stage_id, opened_total);
                        for (state, seconds) in [
                            ("closed", time_in_closed_seconds),
                            ("open", time_in_open_seconds),
                            ("half_open", time_in_half_open_seconds),
                        ] {
                            self.circuit_breaker_time_in_state_seconds_total
                                .insert((stage_id, state.to_string()), seconds);
                        }
                    }
                    ObservationRecord::RateLimiterUtilisation {
                        effect_type: None,
                        utilization_percent,
                        ..
                    } => {
                        self.rate_limiter_utilization
                            .insert(stage_id, utilization_percent / 100.0);
                    }
                    ObservationRecord::HttpPull(measurements) => {
                        self.http_pull_metrics
                            .entry(stage_id)
                            .or_default()
                            .measurements = Some(measurements);
                    }
                    ObservationRecord::EdgeLiveness {
                        upstream,
                        reader,
                        state,
                        ..
                    } => {
                        self.edge_liveness_state.insert((upstream, reader), state);
                    }
                    ObservationRecord::AiChunkingWork {
                        rerender_attempts_total,
                        max_decomposition_depth_reached,
                        budget_overhead_tokens,
                        ..
                    } => {
                        let metrics = self.ai_chunking_metrics.entry(stage_id).or_default();
                        metrics.rerender_attempts_total = Some(rerender_attempts_total);
                        metrics.max_depth_reached = Some(max_decomposition_depth_reached);
                        metrics.budget_overhead_tokens = Some(budget_overhead_tokens);
                    }
                    _ => {}
                }
            }
        }
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
                tracing::trace!(
                    event_id = %envelope.id(),
                    event_type = envelope.event_type_name(),
                    "Metrics aggregator ProcessSystemEvent action"
                );
                let known_stage_ids = ctx.stage_metadata.keys().copied().collect::<Vec<_>>();
                // FLOWIP-059b: Process system journal events for lifecycle tracking
                let store = &mut ctx.metrics_store;
                if let Some(observation) = &envelope.envelope.observability {
                    store.observations.latest().offer_recorded(observation);
                }

                // FLOWIP-059c: Track system-writer vector clocks so `metrics_watermark` can cover
                // system-originated metrics (pipeline + metrics writers) in addition to stage journals.
                if let Some(system_id) = envelope.envelope.provenance.event.writer_id.as_system() {
                    let writer_key = envelope.envelope.provenance.event.writer_id.to_string();
                    let seq = envelope
                        .envelope
                        .provenance
                        .journal
                        .vector_clock
                        .get(&writer_key);
                    let entry = store.system_vector_clocks.entry(*system_id).or_insert(0);
                    *entry = (*entry).max(seq);
                }

                match &envelope.payload {
                    SystemPayload::StageLifecycle { stage_id, event } => {
                        use obzenflow_core::event::StageLifecycleEvent;
                        let accounting = match event {
                            StageLifecycleEvent::Draining { accounting }
                            | StageLifecycleEvent::Completed { accounting }
                            | StageLifecycleEvent::Cancelled { accounting, .. }
                            | StageLifecycleEvent::Failed { accounting, .. } => accounting.as_ref(),
                            _ => None,
                        };
                        if let Some(accounting) = accounting {
                            store.retain_accounting(*stage_id, accounting);
                        }

                        // A current-state projection, not a history of visited states.
                        store
                            .stage_lifecycle_states
                            .retain(|(stage, _), _| stage != stage_id);
                        let state = match event {
                            StageLifecycleEvent::Running => "running",
                            StageLifecycleEvent::Draining { .. } => "draining",
                            StageLifecycleEvent::Drained => "drained",
                            StageLifecycleEvent::Completed { .. } => "completed",
                            StageLifecycleEvent::Cancelled { .. } => "cancelled",
                            StageLifecycleEvent::Failed { .. } => "failed",
                        };
                        store
                            .stage_lifecycle_states
                            .insert((*stage_id, state.into()), true);
                    }
                    SystemPayload::PipelineLifecycle(event)
                        if ctx.pipeline_writer.is_none_or(|writer| {
                            writer == envelope.envelope.provenance.event.writer_id
                        }) =>
                    {
                        // Track only essential pipeline events, with monotonic semantics:
                        // - "failed" is sticky and never regresses.
                        // - "completed" never regresses to "drained".
                        // - "drained" is only used when no explicit outcome was ever observed.
                        match event {
                            obzenflow_core::event::PipelineLifecycleEvent::StopAdmitted {
                                ..
                            } => {
                                if store.pipeline_state.is_empty() {
                                    store.pipeline_state = "stop_admitted".to_string();
                                }
                                tracing::info!("Pipeline: stop requested (metrics view)");
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted {
                                ..
                            } => {
                                store.mark_known_stages_completed(known_stage_ids);
                                if store.pipeline_state.is_empty() {
                                    store.pipeline_state = "all_stages_completed".to_string();
                                }
                                tracing::info!("Pipeline: all stages completed (metrics view)");
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::NotStarted => {
                                if !store.pipeline_terminal() {
                                    store.pipeline_state = "not_started".into();
                                }
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
                            obzenflow_core::event::PipelineLifecycleEvent::Cancelled { .. } => {
                                if store.pipeline_state != "failed" {
                                    store.pipeline_state = "cancelled".to_string();
                                    tracing::info!("Pipeline: cancelled (metrics view)");
                                } else {
                                    tracing::info!(
                                        "Pipeline: cancelled event observed after failed; \
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
                                    "failed" | "completed" | "cancelled" | "not_started" => {
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
                    SystemPayload::ContractResult {
                        upstream,
                        reader,
                        selected_event_type,
                        feed_role,
                        contract_name,
                        reader_seq,
                        advertised_writer_seq,
                        ..
                    } => {
                        let edge_key = ContractMetricEdgeKey {
                            upstream: *upstream,
                            downstream: *reader,
                            contract: contract_name.clone(),
                            selected_event_type: selected_event_type.clone(),
                            feed_role: *feed_role,
                        };
                        if let Some(seq) = reader_seq {
                            let gauge = store
                                .contract_metrics
                                .reader_seq
                                .entry(edge_key.clone())
                                .or_insert(0);
                            *gauge = (*gauge).max(seq.0);
                        }
                        if let Some(seq) = advertised_writer_seq {
                            let gauge = store
                                .contract_metrics
                                .advertised_writer_seq
                                .entry(edge_key)
                                .or_insert(0);
                            *gauge = (*gauge).max(seq.0);
                        }
                    }
                    _ => {} // Skip MetricsCoordination and other event types
                }

                Ok(())
            }

            MetricsAggregatorAction::UpdateMetrics {
                events,
                journal_kind,
                journal_stage,
            } => {
                // The supplied carriers are newest first. Only current values
                // are selected; historical occurrences are never counted.
                let store = &mut ctx.metrics_store;
                let mut selected = false;
                let mut http_seen = false;
                let mut circuit_seen = false;
                for envelope in events.iter() {
                    if let Some(packet) = &envelope.envelope.observability {
                        store.observations.latest().offer_recorded(packet);
                    }
                    let event = &envelope.envelope.provenance.event;
                    if event.writer_id != WriterId::from(*journal_stage)
                        || event.flow_context.stage_id != *journal_stage
                    {
                        continue;
                    }
                    let stage_id = *journal_stage;
                    if let Some(runtime) = &event.runtime {
                        store.retain_accounting(stage_id, &runtime.accounting);
                    }
                    if !selected {
                        store.last_event_id = Some(event.id);
                        selected = true;
                    }
                    if *journal_kind == MetricsJournalKind::Data {
                        let seq = envelope
                            .envelope
                            .provenance
                            .journal
                            .vector_clock
                            .get(&event.writer_id.to_string());
                        let current = store.stage_vector_clocks.entry(stage_id).or_default();
                        *current = (*current).max(seq);
                        if let ChainPayload::Execution(ExecutionPayload::HttpPullState(state)) =
                            &envelope.payload
                        {
                            if !http_seen {
                                store.fold_http_pull_state(stage_id, state);
                                http_seen = true;
                            }
                        }
                        if let ChainPayload::Execution(ExecutionPayload::CircuitBreaker(fact)) =
                            &envelope.payload
                        {
                            let state = match fact {
                                CircuitBreakerFact::Opened { .. } => Some(1.0),
                                CircuitBreakerFact::Closed { .. } => Some(0.0),
                                CircuitBreakerFact::HalfOpen { .. } => Some(0.5),
                                CircuitBreakerFact::StateChanged { to_state, .. } => {
                                    Some(match to_state {
                                        CircuitState::Closed => 0.0,
                                        CircuitState::Open => 1.0,
                                        CircuitState::HalfOpen => 0.5,
                                    })
                                }
                                _ => None,
                            };
                            if !circuit_seen {
                                if let Some(state) = state {
                                    store.circuit_breaker_state.insert(stage_id, state);
                                    circuit_seen = true;
                                }
                            }
                        }
                    }
                    if let Some(meta) = ctx.stage_metadata.get_mut(&stage_id) {
                        if meta.flow_id.is_none() {
                            meta.flow_id = FlowId::from_str(&event.flow_context.flow_id).ok();
                        }
                    }
                }
                Ok(())
            }

            MetricsAggregatorAction::ExportMetrics => {
                let export_started = tokio::time::Instant::now();
                tracing::debug!("ExportMetrics action triggered");
                ctx.metrics_store.throughput.sample(
                    &ctx.metrics_store.observations,
                    &ctx.stage_metadata,
                    export_started,
                );
                let buffer_snapshot = ctx.metrics_store.buffer.snapshot();
                for ((stage, kind), records) in buffer_snapshot.stage_records {
                    MetricsAggregatorAction::UpdateMetrics {
                        events: records,
                        journal_kind: kind,
                        journal_stage: stage,
                    }
                    .execute(ctx)
                    .await?;
                }
                // Older accounting carriers may precede a newer lifecycle value.
                // Applying oldest first leaves each lifecycle at its newest state.
                for record in buffer_snapshot.system_records.iter().rev() {
                    MetricsAggregatorAction::ProcessSystemEvent {
                        envelope: Box::new(record.clone()),
                    }
                    .execute(ctx)
                    .await?;
                }
                ctx.refresh_measurements();
                ctx.metrics_exporter
                    .publish_app_snapshot(ctx.build_app_metrics_snapshot());

                // FLOWIP-059c: Emit a metrics watermark event so SSE clients can "pull-on-push"
                // for `/metrics` refresh and deterministic freshness gating.
                let mut clocks: std::collections::BTreeMap<String, u64> =
                    std::collections::BTreeMap::new();
                for (stage_id, seq) in &ctx.metrics_store.stage_vector_clocks {
                    clocks.insert(WriterId::from(*stage_id).to_string(), *seq);
                }
                for (system_id, seq) in &ctx.metrics_store.system_vector_clocks {
                    clocks.insert(WriterId::from(*system_id).to_string(), *seq);
                }

                let export_event = obzenflow_core::event::SystemEvent::new(
                    WriterId::from(ctx.system_id),
                    SystemPayload::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Exported {
                            watermark: obzenflow_core::event::vector_clock::VectorClock { clocks },
                        },
                    ),
                );

                crate::supervised_base::publication::append(
                    &ctx.system_journal,
                    export_event,
                    Default::default(),
                )
                .await
                .map_err(|error| obzenflow_fsm::FsmError::HandlerError(error.to_string()))?;

                let completed = tokio::time::Instant::now();
                let due = ctx.metrics_store.next_export_at.unwrap_or(export_started);
                // Keep the monotonic schedule and skip missed slots, including
                // time spent publishing the export-coordination record.
                let remainder =
                    completed.duration_since(due).as_nanos() % ctx.export_interval.as_nanos();
                let until_next = ctx.export_interval
                    - std::time::Duration::new(
                        (remainder / 1_000_000_000) as u64,
                        (remainder % 1_000_000_000) as u32,
                    );
                ctx.metrics_store.next_export_at = Some(if due > completed {
                    due
                } else {
                    completed + until_next
                });
                ctx.metrics_store.last_export_completed = Some(completed);
                tracing::debug!(
                    export_elapsed_us = completed.duration_since(export_started).as_micros(),
                    "Metrics export and coordination publication completed"
                );
                Ok(())
            }

            MetricsAggregatorAction::PublishDrainComplete { last_event_id } => {
                // Get writer ID from context
                let system_writer_id = WriterId::from(ctx.system_id);

                // Metrics aggregator publishes SystemEvent to system journal
                let drain_event = obzenflow_core::event::SystemEvent::new(
                    system_writer_id,
                    SystemPayload::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Drained,
                    ),
                );

                // Publish to system journal
                crate::supervised_base::publication::append(
                    &ctx.system_journal,
                    drain_event,
                    Default::default(),
                )
                .await
                .map(|_| ())
                .map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to publish drain complete event: {e}"
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
        state: MetricsAggregatorState;
        event: MetricsAggregatorEvent;
        context: MetricsAggregatorContext;
        action: MetricsAggregatorAction;
        initial: MetricsAggregatorState::Initializing;

        state MetricsAggregatorState::Initializing {
            on MetricsAggregatorEvent::StartRunning => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move { Ok(Transition { next_state: MetricsAggregatorState::Running, actions: vec![MetricsAggregatorAction::Initialize] }) })
            };
            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move { failed_transition(event) })
            };
        }
        state MetricsAggregatorState::Running {
            on MetricsAggregatorEvent::ExportMetrics => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move { Ok(Transition { next_state: MetricsAggregatorState::Running, actions: vec![MetricsAggregatorAction::ExportMetrics] }) })
            };
            on MetricsAggregatorEvent::StartDraining => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move { Ok(Transition { next_state: MetricsAggregatorState::Draining, actions: vec![] }) })
            };
            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move { failed_transition(event) })
            };
        }
        state MetricsAggregatorState::Draining {
            on MetricsAggregatorEvent::ExportMetrics => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move { Ok(Transition { next_state: MetricsAggregatorState::Draining, actions: vec![MetricsAggregatorAction::ExportMetrics] }) })
            };
            on MetricsAggregatorEvent::StartDraining => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                Box::pin(async move { Ok(Transition { next_state: MetricsAggregatorState::Draining, actions: vec![] }) })
            };
            on MetricsAggregatorEvent::FlowTerminal => |_state: &MetricsAggregatorState, _event: &MetricsAggregatorEvent, ctx: &mut MetricsAggregatorContext| {
                let last_event_id = ctx.metrics_store.last_event_id;
                Box::pin(async move { Ok(Transition {
                    next_state: MetricsAggregatorState::Drained { last_event_id },
                    actions: vec![MetricsAggregatorAction::ExportMetrics, MetricsAggregatorAction::PublishDrainComplete { last_event_id }],
                }) })
            };
            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move { failed_transition(event) })
            };
        }
        // A final publication failure must not leave a successful terminal state.
        state MetricsAggregatorState::Drained {
            on MetricsAggregatorEvent::Error => |_state: &MetricsAggregatorState, event: &MetricsAggregatorEvent, _ctx: &mut MetricsAggregatorContext| {
                let event = event.clone();
                Box::pin(async move { failed_transition(event) })
            };
        }
        state MetricsAggregatorState::Failed { }
    }
}

fn failed_transition(
    event: MetricsAggregatorEvent,
) -> Result<Transition<MetricsAggregatorState, MetricsAggregatorAction>, obzenflow_fsm::FsmError> {
    match event {
        MetricsAggregatorEvent::Error(error) => Ok(Transition {
            next_state: MetricsAggregatorState::Failed { error },
            actions: vec![],
        }),
        _ => Err(obzenflow_fsm::FsmError::HandlerError(
            "Invalid metrics failure event".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::observability::{CaptureScope, HttpPullTelemetry};
    use obzenflow_core::event::payloads::execution_payload::HttpPullStateFact;
    use obzenflow_core::FlowId;

    use async_trait::async_trait;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{ChainEventFactory, CorrelationId, JournalEvent};
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::metrics::StageMetadata;
    use obzenflow_core::JournalRecord;
    use std::marker::PhantomData;

    #[test]
    fn retained_timing_population_is_independent_of_accounting_and_empty_replaces_it() {
        use obzenflow_core::event::observability::{
            MeasurementWindow, RuntimeObservability, TimingMeasurements,
        };
        use obzenflow_core::event::provenance::RuntimeProvenance;
        let mut metrics = StageMetrics::default();
        let mut facts = RuntimeProvenance::default();
        facts.accounting.events_processed_total = 1000;
        facts.accounting.errors_total = 10;
        metrics.merge_runtime_context(&facts);
        assert_eq!(metrics.latest_events_processed_total, Some(1000));
        assert_eq!(metrics.latest_errors_total, Some(10));
        assert!(metrics.last_in_flight.is_none() && metrics.snapshot_p50_ms.is_none());

        let mut timing = TimingMeasurements {
            processing_time_count: 2,
            processing_time_sum_nanos: 8_000_000,
            recent_p50_ms: Some(4),
            recent_p90_ms: Some(4),
            recent_p95_ms: Some(4),
            recent_p99_ms: Some(4),
            recent_p999_ms: Some(4),
            window: MeasurementWindow {
                started_at_ms: 10,
                ended_at_ms: 20,
            },
        };
        metrics.merge_runtime_measurements(&RuntimeObservability {
            timing: Some(timing.clone()),
            ..Default::default()
        });
        facts.accounting.events_processed_total = 2000;
        metrics.merge_runtime_context(&facts);
        metrics.merge_runtime_measurements(&RuntimeObservability {
            in_flight: Some(0),
            ..Default::default()
        });
        assert_eq!(metrics.latest_events_processed_total, Some(2000));
        assert_eq!(metrics.processing_time_count, Some(2));
        assert_eq!(metrics.processing_time_sum_nanos, Some(8_000_000));
        assert_eq!(metrics.snapshot_p50_ms, Some(4));
        assert_eq!(metrics.last_in_flight, Some(0));

        timing.processing_time_count = 0;
        timing.processing_time_sum_nanos = 0;
        timing.recent_p50_ms = None;
        timing.recent_p90_ms = None;
        timing.recent_p95_ms = None;
        timing.recent_p99_ms = None;
        timing.recent_p999_ms = None;
        timing.window.ended_at_ms = 30;
        metrics.merge_runtime_measurements(&RuntimeObservability {
            timing: Some(timing),
            ..Default::default()
        });
        assert_eq!(metrics.processing_time_count, Some(0));
        assert!(metrics.snapshot_p50_ms.is_none() && metrics.snapshot_p999_ms.is_none());
        assert_eq!(metrics.last_in_flight, Some(0));
        assert_eq!(metrics.timing_window.unwrap().ended_at_ms, 30);
    }

    #[test]
    fn http_pull_state_and_measurements_have_independent_authorities() {
        use obzenflow_core::event::observability::{HttpPullState, WaitReason};

        let stage_id = StageId::new();
        let mut store = MetricsStore::default();
        let first = HttpPullTelemetry {
            state: HttpPullState::Waiting,
            wait_reason: Some(WaitReason::PollInterval),
            next_wake_unix_secs: Some(500),
            last_success_unix_secs: Some(400),
            requests_total: 8,
            responses_2xx: 5,
            responses_4xx: 2,
            responses_5xx: 1,
            rate_limited_total: 2,
            retries_total: 3,
            events_decoded_total: 21,
            wait_seconds_rate_limit: 4.0,
            wait_seconds_poll_interval: 7.0,
            wait_seconds_backoff: 2.0,
        };
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: Default::default(),
        };
        store.stage_metrics.entry(stage_id).or_default();
        let mut fold = |telemetry: &HttpPullTelemetry, seq| {
            store.fold_http_pull_state(
                stage_id,
                &HttpPullStateFact {
                    state: telemetry.state,
                    wait_reason: telemetry.wait_reason,
                    next_wake_unix_secs: telemetry.next_wake_unix_secs,
                    last_success_unix_secs: telemetry.last_success_unix_secs,
                },
            );
            use obzenflow_core::event::observability::*;
            let mut packet = ObservabilityContext::new(CaptureStamp {
                capture_scope: scope,
                observer: stage_id.into(),
                capture_seq: CaptureSeq(seq),
                capture_reason: CaptureReason::Record,
                observed_at_ms: seq,
            });
            packet
                .records
                .push(ObservationRecord::HttpPull(HttpPullMeasurements::from(
                    telemetry,
                )));
            store.observations.offer(packet);
            store.refresh_measurements();
        };
        fold(&first, 2);

        let latest = HttpPullTelemetry {
            state: HttpPullState::Fetching,
            wait_reason: None,
            next_wake_unix_secs: None,
            last_success_unix_secs: Some(399),
            requests_total: 7,
            responses_2xx: 4,
            responses_4xx: 1,
            responses_5xx: 0,
            rate_limited_total: 1,
            retries_total: 2,
            events_decoded_total: 20,
            wait_seconds_rate_limit: 3.0,
            wait_seconds_poll_interval: 6.0,
            wait_seconds_backoff: 1.0,
        };
        fold(&latest, 1);

        let folded = store
            .http_pull_metrics
            .get(&stage_id)
            .expect("typed snapshot is indexed by stage");
        assert!(matches!(folded.state, Some(HttpPullState::Fetching)));
        assert!(folded.wait_reason.is_none());
        assert_eq!(folded.next_wake_unix_secs, None);
        assert_eq!(folded.last_success_unix_secs, Some(400));
        let measurements = folded.measurements.as_ref().unwrap();
        assert_eq!(measurements.requests_total, 8);
        assert_eq!(measurements.responses_2xx, 5);
        assert_eq!(measurements.responses_4xx, 2);
        assert_eq!(measurements.responses_5xx, 1);
        assert_eq!(measurements.rate_limited_total, 2);
        assert_eq!(measurements.retries_total, 3);
        assert_eq!(measurements.events_decoded_total, 21);
        assert_eq!(measurements.wait_seconds_rate_limit, 4.0);
        assert_eq!(measurements.wait_seconds_poll_interval, 7.0);
        assert_eq!(measurements.wait_seconds_backoff, 2.0);
    }

    #[test]
    fn all_stages_completed_reconciles_missing_stage_lifecycle_states() {
        let observed = StageId::new();
        let missing = StageId::new();
        let failed = StageId::new();

        let mut stage_metadata = HashMap::new();
        for stage_id in [observed, missing, failed] {
            stage_metadata.insert(
                stage_id,
                StageMetadata {
                    name: format!("stage-{stage_id}"),
                    stage_type: StageType::Transform,
                    reference_mode: None,
                    flow_name: "test_flow".to_string(),
                    flow_id: None,
                },
            );
        }

        let mut store = MetricsStore::default();
        store
            .stage_lifecycle_states
            .insert((observed, "completed".to_string()), true);
        store
            .stage_lifecycle_states
            .insert((failed, "failed".to_string()), true);

        assert!(!store.all_stages_terminal(&stage_metadata));

        store.mark_known_stages_completed(stage_metadata.keys().copied());

        assert!(store.all_stages_terminal(&stage_metadata));
        assert_eq!(
            store
                .stage_lifecycle_states
                .get(&(missing, "completed".to_string())),
            Some(&true)
        );
        assert_eq!(
            store
                .stage_lifecycle_states
                .get(&(failed, "completed".to_string())),
            None
        );
    }

    #[tokio::test]
    async fn test_delivery_event_preserves_correlation() {
        // Create a test event with correlation
        let writer_id = WriterId::from(StageId::new());
        let correlation_id = CorrelationId::new();
        let mut event = ChainEventFactory::data_event(
            writer_id,
            "test.event",
            serde_json::json!({"data": "test"}),
        );
        event.set_single_correlation(correlation_id, Some(CorrelationPayload::new(event.id)));

        // Simulate what the sink supervisor does when creating a delivery event
        let payload = DeliveryPayload::success(DeliveryMethod::Noop, Some(1));
        let delivery_event = ChainEventFactory::delivery_event(writer_id, payload)
            .with_correlation_from(&event)
            .with_cycle_state_from(&event);
        let delivery_event = delivery_event
            .try_with_composite_activations(event.composite_activations().to_vec())
            .unwrap();

        // Verify correlation is preserved
        assert_eq!(delivery_event.correlation_id(), Some(correlation_id));
        assert!(delivery_event.correlation_payload().is_some());
        assert_eq!(
            delivery_event.correlation_payload(),
            event.correlation_payload()
        );
    }

    #[test]
    fn build_app_metrics_snapshot_uses_errors_by_kind_from_store() {
        let stage_id = StageId::new();

        // Seed MetricsStore with a single stage entry.
        let mut store = MetricsStore::default();
        let errors_by_kind = HashMap::from([(ErrorKind::Domain, 2), (ErrorKind::Remote, 1)]);
        let stage_metrics = StageMetrics {
            errors_by_kind,
            latest_events_processed_total: Some(42),
            latest_errors_total: Some(3),
            latest_data_outputs_by_event_type: HashMap::from([(
                EventType::from("checkout.completed.v1"),
                5,
            )]),
            event_loops_total: Some(10),
            event_loops_with_work_total: Some(7),
            ..Default::default()
        };
        store.stage_metrics.insert(stage_id, stage_metrics);

        // Minimal stage metadata so flow aggregation can classify the stage.
        let mut stage_metadata = std::collections::HashMap::new();
        stage_metadata.insert(
            stage_id,
            StageMetadata {
                name: "test_stage".to_string(),
                stage_type: StageType::Sink,
                reference_mode: None,
                flow_name: "test_flow".to_string(),
                flow_id: None,
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
                _options: obzenflow_core::journal::AppendOptions<'_, T>,
            ) -> Result<JournalRecord<T::Payload>, JournalError> {
                Err(JournalError::Implementation {
                    message: "noop journal".to_string(),
                    source: "noop".into(),
                })
            }

            async fn read_all_unordered(
                &self,
            ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_event(
                &self,
                _event_id: &obzenflow_core::EventId,
            ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
                Ok(None)
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
            ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
                Ok(Vec::new())
            }
        }

        #[async_trait]
        impl<T: JournalEvent + 'static> JournalReader<T> for NoopReader {
            async fn next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
                Ok(None)
            }

            fn position(&self) -> u64 {
                0
            }

            fn is_at_end(&self) -> bool {
                true
            }
        }

        // Build a context with only the fields required by build_app_metrics_snapshot.
        let downstream = StageId::new();
        let ctx = MetricsAggregatorContext {
            system_journal: Arc::new(NoopJournal::<obzenflow_core::event::SystemEvent>::new(
                JournalOwner::system(obzenflow_core::SystemId::new()),
            )),
            stage_data_journals: HashMap::new(),
            stage_error_journals: HashMap::new(),
            backpressure_registry: None,
            include_error_journals: true,
            metrics_exporter: Arc::new(crate::metrics::RecordingSnapshots::default()),
            metrics_store: store,
            export_interval: std::time::Duration::from_secs(10),
            system_id: obzenflow_core::SystemId::new(),
            pipeline_writer: None,
            stage_metadata,
            composite_boundaries: vec![obzenflow_core::metrics::CompositeBoundary {
                composite_id: obzenflow_core::id::CompositeId::new("saga:checkout"),
                members: vec![stage_id],
                ports: vec![obzenflow_core::metrics::CompositeBoundaryPort {
                    name: "completed".to_string(),
                    direction: obzenflow_core::metrics::BoundaryDirection::Outbound,
                    member: stage_id,
                    payload_event_types: vec![EventType::from("checkout.completed.v1")],
                }],
                edges: vec![obzenflow_core::metrics::CompositeBoundaryEdge {
                    port: "completed".to_string(),
                    direction: obzenflow_core::metrics::BoundaryDirection::Outbound,
                    member: stage_id,
                    peer: downstream,
                    upstream: stage_id,
                    downstream,
                }],
            }],
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
        assert!(snapshot.stage_metadata.contains_key(&stage_id));
        assert_eq!(snapshot.composite_port_traffic.len(), 1);
        assert_eq!(snapshot.composite_port_traffic[0].events_total, 5);
        assert_eq!(snapshot.composite_member_health.len(), 1);
        assert_eq!(snapshot.composite_member_health[0].member_errors_total, 3);
    }
}
