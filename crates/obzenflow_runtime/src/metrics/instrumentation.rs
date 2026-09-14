// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM instrumentation for HandlerSupervised stages

use crate::control_plane::{
    CircuitBreakerSnapshotter, CircuitBreakerStateView, ControlPlaneProvider, NoControlPlane,
    RateLimiterSnapshotter,
};
use hdrhistogram::Histogram;
use obzenflow_core::event::context::{EventTypeCountContext, UpstreamEventTypeCountContext};
use obzenflow_core::event::identity::journal_writer_id::JournalWriterId;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainEvent;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::EventId;
use obzenflow_core::EventType;
use obzenflow_core::StageId;
use obzenflow_core::WriterId;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use super::constants::{
    HISTOGRAM_MAX_MS, HISTOGRAM_MIN_MS, HISTOGRAM_SIGFIGS, QUANTILE_P50, QUANTILE_P90,
    QUANTILE_P95, QUANTILE_P99, QUANTILE_P999,
};

/// Configuration for stage instrumentation
#[derive(Debug, Clone)]
pub struct InstrumentationConfig {
    pub enable_histograms: bool,
    pub enable_utilization: bool,
    pub enable_anomaly_detection: bool,
}

impl Default for InstrumentationConfig {
    fn default() -> Self {
        Self {
            enable_histograms: true,
            enable_utilization: true,
            enable_anomaly_detection: true,
        }
    }
}

/// Error when binding control middleware fails validation.
#[derive(Debug, thiserror::Error)]
pub enum ControlBindError {
    #[error("Stage {stage_id} configured with circuit_breaker middleware but none registered")]
    MissingCircuitBreaker { stage_id: StageId },

    #[error("Stage {stage_id} configured with rate_limiter middleware but none registered")]
    MissingRateLimiter { stage_id: StageId },
}

#[derive(Debug, Default)]
struct AuthoredDataFrontier {
    writer_seq: u64,
    writer_seq_by_event_type: HashMap<EventType, u64>,
    last_event_id: Option<EventId>,
}

/// Stage instrumentation that tracks metrics alongside FSM state
pub struct StageInstrumentation {
    observation_owner: std::sync::OnceLock<super::observations::ObservationOwner>,
    measurement_started_at_ms: u64,
    processing_time_count: AtomicU64,
    last_processing_time_available: std::sync::atomic::AtomicBool,
    // Gauge metrics - current values
    pub in_flight_count: AtomicU32,
    /// Join-only gauge (Live join): number of reference events processed since the last stream event.
    ///
    /// Defaults to 0 for non-join stages.
    pub join_reference_since_last_stream: AtomicU64,

    // Counter metrics - monotonic, let Prometheus compute rates
    pub events_processed_total: AtomicU64,
    /// Total input events accumulated into internal state (stateful/join stages).
    pub events_accumulated_total: AtomicU64,
    /// Total output events emitted by the stage (data/delivery; excludes observability-only events).
    pub events_emitted_total: AtomicU64,
    pub terminal_groups_committed_total: AtomicU64,
    pub terminal_group_commit_failures_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub failures_total: AtomicU64,              // Critical failures
    pub event_loops_total: AtomicU64,           // Total event loop iterations
    pub event_loops_with_work_total: AtomicU64, // Loops that had work
    pub anomalies_total: AtomicU64,             // Outliers detected
    pub amendments_total: AtomicU64,            // Config changes

    // Histogram for processing time (percentiles)
    pub processing_time_histogram: RwLock<Histogram<u64>>,

    // Actual sum of processing times (nanoseconds) - never reconstructed from percentiles
    pub processing_time_sum_nanos: AtomicU64,

    /// Most-recent per-invocation processing duration (nanoseconds). Set by
    /// `record_processing_time`; read by the output committer to stamp each stage
    /// output's `processing_info.processing_time` (FLOWIP-115f, replacing the
    /// deleted `TimingMiddleware`). Stages process one input at a time, so at
    /// commit this is the current invocation's duration.
    pub last_processing_time_nanos: AtomicU64,

    // FSM state tracking
    pub current_state: RwLock<String>,
    pub state_entered_at: RwLock<Instant>,

    // Observability positions
    pub reader_seq: AtomicU64,
    pub receipted_seq: AtomicU64,
    pub writer_seq: AtomicU64,
    pub last_consumed_event_id: RwLock<Option<EventId>>,
    pub last_consumed_writer: RwLock<Option<JournalWriterId>>,
    pub last_consumed_vector_clock: RwLock<Option<VectorClock>>,
    pub last_receipted_event_id: RwLock<Option<EventId>>,
    pub last_receipted_vector_clock: RwLock<Option<VectorClock>>,
    pub last_emitted_event_id: RwLock<Option<EventId>>,
    pub last_emitted_writer: RwLock<Option<WriterId>>,
    authored_data_frontier: RwLock<AuthoredDataFrontier>,
    pub data_reader_seq_by_upstream_event_type: RwLock<HashMap<(StageId, EventType), u64>>,

    /// Error breakdown by kind
    pub errors_by_kind: RwLock<
        std::collections::HashMap<
            obzenflow_core::event::status::processing_status::ErrorKind,
            AtomicU64,
        >,
    >,

    // Configuration
    config: InstrumentationConfig,

    // =========================================================================
    // Control-plane bindings (FLOWIP-059a-3)
    // =========================================================================
    /// Flow-scoped provider for control-plane state/metrics.
    control_plane: Arc<dyn ControlPlaneProvider>,

    /// Cached snapshotter for circuit breaker metrics (set once during stage construction).
    cb_snapshotter: Option<Arc<CircuitBreakerSnapshotter>>,

    /// Cached snapshotter for rate limiter metrics (set once during stage construction).
    rl_snapshotter: Option<Arc<RateLimiterSnapshotter>>,

    /// Cached typed circuit breaker state view (FLOWIP-115b; set once during
    /// stage construction).
    cb_state_view: Option<Arc<dyn CircuitBreakerStateView>>,

    /// Per-effect circuit breaker snapshotters keyed by declared effect type
    /// (FLOWIP-120c G9; set once during stage construction).
    effect_cb_snapshotters: Vec<(String, Arc<CircuitBreakerSnapshotter>)>,

    /// Per-effect rate limiter snapshotters keyed by declared effect type
    /// (FLOWIP-120c G9; set once during stage construction).
    effect_rl_snapshotters: Vec<(String, Arc<RateLimiterSnapshotter>)>,
}

impl Default for StageInstrumentation {
    fn default() -> Self {
        Self::new()
    }
}

impl StageInstrumentation {
    pub fn new() -> Self {
        Self::new_with_config(InstrumentationConfig::default())
    }

    pub fn new_with_config(config: InstrumentationConfig) -> Self {
        Self {
            observation_owner: std::sync::OnceLock::new(),
            measurement_started_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            processing_time_count: AtomicU64::new(0),
            last_processing_time_available: std::sync::atomic::AtomicBool::new(false),
            // Gauges
            in_flight_count: AtomicU32::new(0),
            join_reference_since_last_stream: AtomicU64::new(0),

            // Counters
            events_processed_total: AtomicU64::new(0),
            events_accumulated_total: AtomicU64::new(0),
            events_emitted_total: AtomicU64::new(0),
            terminal_groups_committed_total: AtomicU64::new(0),
            terminal_group_commit_failures_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
            event_loops_total: AtomicU64::new(0),
            event_loops_with_work_total: AtomicU64::new(0),
            anomalies_total: AtomicU64::new(0),
            amendments_total: AtomicU64::new(0),

            // Histogram
            processing_time_histogram: RwLock::new(
                Histogram::new_with_bounds(HISTOGRAM_MIN_MS, HISTOGRAM_MAX_MS, HISTOGRAM_SIGFIGS)
                    .expect("Failed to create histogram"),
            ),

            // Actual sum - always tracked, never reconstructed
            processing_time_sum_nanos: AtomicU64::new(0),

            // Most-recent per-invocation duration (FLOWIP-115f processing_time stamp)
            last_processing_time_nanos: AtomicU64::new(0),

            // State
            current_state: RwLock::new("Created".to_string()),
            state_entered_at: RwLock::new(Instant::now()),

            // Observability positions
            reader_seq: AtomicU64::new(0),
            receipted_seq: AtomicU64::new(0),
            writer_seq: AtomicU64::new(0),
            last_consumed_event_id: RwLock::new(None),
            last_consumed_writer: RwLock::new(None),
            last_consumed_vector_clock: RwLock::new(None),
            last_receipted_event_id: RwLock::new(None),
            last_receipted_vector_clock: RwLock::new(None),
            last_emitted_event_id: RwLock::new(None),
            last_emitted_writer: RwLock::new(None),
            authored_data_frontier: RwLock::new(AuthoredDataFrontier::default()),
            data_reader_seq_by_upstream_event_type: RwLock::new(HashMap::new()),

            errors_by_kind: RwLock::new(std::collections::HashMap::new()),

            config,

            control_plane: Arc::new(NoControlPlane),
            cb_snapshotter: None,
            rl_snapshotter: None,
            effect_cb_snapshotters: Vec::new(),
            effect_rl_snapshotters: Vec::new(),
            cb_state_view: None,
        }
    }

    /// Bind control-plane publishers from the provider for this stage.
    ///
    /// Called once during stage construction. Caches snapshotters and state to
    /// avoid per-event lookups. Fails if expected control publishers are missing.
    pub fn bind_control_plane(
        &mut self,
        stage_id: &StageId,
        provider: &Arc<dyn ControlPlaneProvider>,
        expects_circuit_breaker: bool,
        expects_rate_limiter: bool,
    ) -> Result<(), ControlBindError> {
        self.control_plane = provider.clone();

        self.cb_snapshotter = provider.circuit_breaker_snapshotter(stage_id);
        self.rl_snapshotter = provider.rate_limiter_snapshotter(stage_id);
        self.cb_state_view = provider.circuit_breaker_state_view(stage_id);
        self.effect_cb_snapshotters = provider.effect_circuit_breaker_snapshotters(stage_id);
        self.effect_rl_snapshotters = provider.effect_rate_limiter_snapshotters(stage_id);

        if expects_circuit_breaker
            && (self.cb_snapshotter.is_none() || self.cb_state_view.is_none())
        {
            return Err(ControlBindError::MissingCircuitBreaker {
                stage_id: *stage_id,
            });
        }

        if expects_rate_limiter && self.rl_snapshotter.is_none() {
            return Err(ControlBindError::MissingRateLimiter {
                stage_id: *stage_id,
            });
        }

        Ok(())
    }

    /// Capture protected positions and accounting without touching measurement
    /// locks, control snapshotters, or the observation handoff.
    pub fn snapshot(&self) -> RuntimeProvenance {
        RuntimeProvenance {
            progress: ExecutionProgress {
                reader_seq: self.reader_seq.load(Ordering::Relaxed),
                receipted_seq: self.receipted_seq.load(Ordering::Relaxed),
                writer_seq: self.writer_seq.load(Ordering::Relaxed),
                last_consumed_event_id: *self.last_consumed_event_id.read().unwrap(),
                last_consumed_writer: *self.last_consumed_writer.read().unwrap(),
                last_consumed_vector_clock: self.last_consumed_vector_clock.read().unwrap().clone(),
                last_receipted_event_id: *self.last_receipted_event_id.read().unwrap(),
                last_receipted_vector_clock: self
                    .last_receipted_vector_clock
                    .read()
                    .unwrap()
                    .clone(),
                last_emitted_event_id: *self.last_emitted_event_id.read().unwrap(),
                last_emitted_writer: *self.last_emitted_writer.read().unwrap(),
            },
            accounting: ExecutionAccounting {
                events_processed_total: self.events_processed_total.load(Ordering::Relaxed),
                events_accumulated_total: self.events_accumulated_total.load(Ordering::Relaxed),
                events_emitted_total: self.events_emitted_total.load(Ordering::Relaxed),
                terminal_groups_committed_total: self
                    .terminal_groups_committed_total
                    .load(Ordering::Relaxed),
                terminal_group_commit_failures_total: self
                    .terminal_group_commit_failures_total
                    .load(Ordering::Relaxed),
                errors_total: self.errors_total.load(Ordering::Relaxed),
                failures_total: self.failures_total.load(Ordering::Relaxed),
                errors_by_kind: self
                    .errors_by_kind
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                    .collect(),
                data_outputs_by_event_type: {
                    let mut counts: Vec<_> = self
                        .authored_data_frontier
                        .read()
                        .unwrap()
                        .writer_seq_by_event_type
                        .iter()
                        .map(|(event_type, total)| EventTypeCountContext {
                            event_type: event_type.clone(),
                            total: *total,
                        })
                        .collect();
                    counts.sort_by(|left, right| left.event_type.cmp(&right.event_type));
                    counts
                },
                data_inputs_by_upstream_event_type: {
                    let mut counts: Vec<_> = self
                        .data_reader_seq_by_upstream_event_type
                        .read()
                        .unwrap()
                        .iter()
                        .map(
                            |((upstream, event_type), total)| UpstreamEventTypeCountContext {
                                upstream: *upstream,
                                event_type: event_type.clone(),
                                total: *total,
                            },
                        )
                        .collect();
                    counts.sort_by(|left, right| {
                        (left.upstream, left.event_type.as_str())
                            .cmp(&(right.upstream, right.event_type.as_str()))
                    });
                    counts
                },
            },
            fsm_state: self.current_state.read().unwrap().clone(),
        }
    }

    /// Optional capture: contention omits the affected family. Timing count,
    /// sum, percentiles and window are read under the same histogram lock.
    pub fn capture_observability(&self, reason: CaptureReason) -> Option<ObservabilityContext> {
        let owner = self.observation_owner.get()?;
        self.capture_measurements(owner.capture(reason)?)
    }

    fn capture_measurements(
        &self,
        mut packet: ObservabilityContext,
    ) -> Option<ObservabilityContext> {
        let timing = self
            .processing_time_histogram
            .try_read()
            .ok()
            .map(|histogram| {
                let count = self.processing_time_count.load(Ordering::Relaxed);
                let percentile = |quantile| {
                    (self.config.enable_histograms && count > 0 && !histogram.is_empty())
                        .then(|| histogram.value_at_quantile(quantile))
                };
                TimingMeasurements {
                    processing_time_count: count,
                    processing_time_sum_nanos: self
                        .processing_time_sum_nanos
                        .load(Ordering::Relaxed),
                    recent_p50_ms: percentile(QUANTILE_P50),
                    recent_p90_ms: percentile(QUANTILE_P90),
                    recent_p95_ms: percentile(QUANTILE_P95),
                    recent_p99_ms: percentile(QUANTILE_P99),
                    recent_p999_ms: percentile(QUANTILE_P999),
                    window: MeasurementWindow {
                        started_at_ms: self.measurement_started_at_ms,
                        ended_at_ms: packet.capture.observed_at_ms,
                    },
                }
            });
        let mut runtime = RuntimeObservability {
            in_flight: Some(self.in_flight_count.load(Ordering::Relaxed)),
            join_reference_since_last_stream: Some(
                self.join_reference_since_last_stream
                    .load(Ordering::Relaxed),
            ),
            time_in_state_ms: self
                .state_entered_at
                .try_read()
                .ok()
                .map(|at| at.elapsed().as_millis() as u64),
            event_loops_total: self
                .config
                .enable_utilization
                .then(|| self.event_loops_total.load(Ordering::Relaxed)),
            event_loops_with_work_total: self
                .config
                .enable_utilization
                .then(|| self.event_loops_with_work_total.load(Ordering::Relaxed)),
            timing,
            ..Default::default()
        };
        if let Some(cb) = self
            .cb_snapshotter
            .as_ref()
            .and_then(|snapshotter| snapshotter())
        {
            runtime.circuit_breaker = Some(CircuitBreakerMeasurements {
                requests_total: cb.requests_total,
                successes_total: cb.successes_total,
                failures_total: cb.failures_total,
                slow_total: cb.slow_total,
                rejections_total: cb.rejections_total,
                opened_total: cb.opened_total,
                time_closed_seconds: cb.time_closed_seconds,
                time_open_seconds: cb.time_open_seconds,
                time_half_open_seconds: cb.time_half_open_seconds,
                observed_state: match cb.state {
                    crate::control_plane::CircuitBreakerState::Closed => {
                        obzenflow_core::event::payloads::execution_payload::CircuitState::Closed
                    }
                    crate::control_plane::CircuitBreakerState::Open => {
                        obzenflow_core::event::payloads::execution_payload::CircuitState::Open
                    }
                    crate::control_plane::CircuitBreakerState::HalfOpen => {
                        obzenflow_core::event::payloads::execution_payload::CircuitState::HalfOpen
                    }
                },
            });
        }
        if let Some(rl) = self
            .rl_snapshotter
            .as_ref()
            .and_then(|snapshotter| snapshotter())
        {
            runtime.rate_limiter = Some(RateLimiterMeasurements {
                events_total: rl.events_total,
                delayed_total: rl.delayed_total,
                tokens_consumed_total: rl.tokens_consumed_total,
                delay_seconds_total: rl.delay_seconds_total,
                bucket_tokens: rl.bucket_tokens,
                bucket_capacity: rl.bucket_capacity,
            });
        }
        runtime.effect_circuit_breakers = self
            .effect_cb_snapshotters
            .iter()
            .filter_map(|(effect_type, snapshotter)| {
                let cb = snapshotter()?;
                Some(
                    obzenflow_core::event::context::EffectCircuitBreakerContext {
                        effect_type: effect_type.clone(),
                        cb_requests_total: cb.requests_total,
                        cb_successes_total: cb.successes_total,
                        cb_failures_total: cb.failures_total,
                        cb_slow_total: cb.slow_total,
                        cb_rejections_total: cb.rejections_total,
                        cb_opened_total: cb.opened_total,
                        cb_time_closed_seconds: cb.time_closed_seconds,
                        cb_time_open_seconds: cb.time_open_seconds,
                        cb_time_half_open_seconds: cb.time_half_open_seconds,
                        cb_state: cb.state.stable_gauge(),
                    },
                )
            })
            .collect();
        runtime.effect_rate_limiters = self
            .effect_rl_snapshotters
            .iter()
            .filter_map(|(effect_type, snapshotter)| {
                let rl = snapshotter()?;
                Some(obzenflow_core::event::context::EffectRateLimiterContext {
                    effect_type: effect_type.clone(),
                    rl_events_total: rl.events_total,
                    rl_delayed_total: rl.delayed_total,
                    rl_tokens_consumed_total: rl.tokens_consumed_total,
                    rl_delay_seconds_total: rl.delay_seconds_total,
                    rl_bucket_tokens: rl.bucket_tokens,
                    rl_bucket_capacity: rl.bucket_capacity,
                })
            })
            .collect();

        packet.runtime = Some(runtime);
        packet.processing_time = self.last_processing_time();
        packet.validated()
    }

    pub fn bind_observations(
        self: &Arc<Self>,
        flow_id: obzenflow_core::FlowId,
        writer: WriterId,
        execution: &crate::execution::RuntimeExecution,
    ) {
        let scope = super::observations::scope(execution, flow_id);
        execution.observations().activate_scope(scope);
        let owner = execution
            .observations()
            .capture_owner(scope, writer, execution.clone());
        let _ = self.observation_owner.set(owner);
        execution.observations().register_stage(writer, self);
        self.offer_capture(CaptureReason::Initial);
    }

    pub fn observation_recorder(&self) -> Arc<dyn ObservationRecorder> {
        self.observation_owner
            .get()
            .map(|owner| Arc::new(owner.clone()) as Arc<dyn ObservationRecorder>)
            .unwrap_or_else(|| Arc::new(NoObservations))
    }

    pub fn observe(&self, record: ObservationRecord) {
        self.observation_recorder().observe(record);
    }

    /// Offer and attach the same capture. The handoff is optional and cannot
    /// affect the journal append, including when the retained view is full.
    pub fn capture_for_record(&self) -> Option<ObservabilityContext> {
        let packet = self.capture_observability(CaptureReason::Record)?;
        if let Some(owner) = self.observation_owner.get() {
            owner.offer(packet.clone());
        }
        Some(packet)
    }

    /// Incomplete replay may execute missing effects live while its handler
    /// remains in reconstruction. Use the existing boundary execution scope
    /// for that capture, without enabling historical handler measurements.
    pub(crate) fn capture_for_record_in_scope(
        &self,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Option<ObservabilityContext> {
        let owner = self.observation_owner.get()?;
        let packet =
            self.capture_measurements(owner.capture_in_scope(CaptureReason::Record, scope)?)?;
        owner.offer(packet.clone());
        Some(packet)
    }

    pub fn offer_capture(&self, reason: CaptureReason) {
        if let (Some(owner), Some(packet)) = (
            self.observation_owner.get(),
            self.capture_observability(reason),
        ) {
            owner.offer(packet);
        }
    }

    /// Access the flow-scoped control-plane provider.
    pub fn control_plane(&self) -> &Arc<dyn ControlPlaneProvider> {
        &self.control_plane
    }

    /// Access the cached typed circuit breaker state view (if bound).
    pub fn circuit_breaker_state_view(&self) -> Option<&Arc<dyn CircuitBreakerStateView>> {
        self.cb_state_view.as_ref()
    }

    /// Note a consumed envelope so downstream events capture reader position and origin.
    pub fn record_consumed<P: obzenflow_core::JournalPayload>(
        &self,
        envelope: &JournalRecord<P>,
        upstream_stage: StageId,
    ) {
        self.reader_seq.fetch_add(1, Ordering::Relaxed);
        *self.last_consumed_event_id.write().unwrap() = Some(*envelope.id());
        *self.last_consumed_writer.write().unwrap() =
            Some(envelope.envelope.provenance.journal.journal_writer_id);
        *self.last_consumed_vector_clock.write().unwrap() =
            Some(envelope.envelope.provenance.journal.vector_clock.clone());
        if let Some(event) = (&envelope.authored() as &dyn Any).downcast_ref::<ChainEvent>() {
            if event.consumes_data_credit() {
                let event_type = &event.envelope.provenance.event.event_type;
                let mut counts = self.data_reader_seq_by_upstream_event_type.write().unwrap();
                *counts
                    .entry((upstream_stage, EventType::from(event_type.clone())))
                    .or_insert(0) += 1;
            }
        }
    }

    /// Note the latest durable delivery watermark for sink stages.
    pub fn record_receipted_position(
        &self,
        seq: u64,
        event_id: EventId,
        vector_clock: VectorClock,
    ) {
        self.receipted_seq.store(seq, Ordering::Relaxed);
        *self.last_receipted_event_id.write().unwrap() = Some(event_id);
        *self.last_receipted_vector_clock.write().unwrap() = Some(vector_clock);
    }

    /// Note an emitted event for wide-event observability.
    pub fn record_emitted<T: JournalEvent>(&self, event: &T) {
        self.writer_seq.fetch_add(1, Ordering::Relaxed);
        *self.last_emitted_event_id.write().unwrap() = Some(*event.id());
        *self.last_emitted_writer.write().unwrap() = Some(*event.writer_id());
    }

    /// Note an emitted output event and increment the emitted counter.
    ///
    /// Use this for data/delivery events that represent stage outputs. Do not
    /// use it for observability-only events (e.g. metrics heartbeats).
    pub fn record_output_event(&self, event: &ChainEvent) {
        self.record_emitted(event);
        if event.consumes_data_credit() {
            let event_type = &event.envelope.provenance.event.event_type;
            let mut frontier = self.authored_data_frontier.write().unwrap();
            frontier.writer_seq = frontier.writer_seq.saturating_add(1);
            *frontier
                .writer_seq_by_event_type
                .entry(event_type.clone().into())
                .or_insert(0) += 1;
            frontier.last_event_id = Some(event.id);
        }
        self.events_emitted_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Note a physically committed Data row whose durable author is another
    /// stage. Forwarded pre-error rows remain emitted journal evidence and
    /// telemetry, but cannot advance this stage's authored transport frontier.
    pub fn record_forwarded_output_event(&self, event: &ChainEvent) {
        self.record_emitted(event);
        self.events_emitted_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Note a Data output routed to the error journal. It remains an emitted
    /// stage event for existing lifecycle accounting, but it is deliberately
    /// absent from the typed data-journal counters used by composite ports.
    pub fn record_error_journal_output_event(&self, event: &ChainEvent) {
        self.record_emitted(event);
        self.events_emitted_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn data_writer_seq_by_event_type(
        &self,
    ) -> BTreeMap<EventType, obzenflow_core::event::types::SeqNo> {
        self.authored_data_frontier
            .read()
            .unwrap()
            .writer_seq_by_event_type
            .iter()
            .map(|(event_type, count)| {
                (
                    event_type.clone(),
                    obzenflow_core::event::types::SeqNo(*count),
                )
            })
            .collect()
    }

    /// Snapshot the exact committed Data prefix authored by this stage.
    ///
    /// All three coordinates come from one lock acquisition so a terminal can
    /// never combine a count, per-type map, and last event from different
    /// frontiers.
    pub fn authored_data_frontier(&self) -> (SeqNo, BTreeMap<EventType, SeqNo>, Option<EventId>) {
        let frontier = self.authored_data_frontier.read().unwrap();
        (
            SeqNo(frontier.writer_seq),
            frontier
                .writer_seq_by_event_type
                .iter()
                .map(|(event_type, count)| (event_type.clone(), SeqNo(*count)))
                .collect(),
            frontier.last_event_id,
        )
    }

    /// Record processing duration in histogram and sum.
    ///
    /// The sum is always tracked (for accurate Prometheus histogram export).
    /// The histogram is only updated if enable_histograms is true.
    pub fn record_processing_time(&self, duration: Duration) {
        self.last_processing_time_available
            .store(false, Ordering::Relaxed);
        if self
            .observation_owner
            .get()
            .is_some_and(|owner| !owner.measurements_allowed())
        {
            return;
        }
        let Ok(mut histogram) = self.processing_time_histogram.try_write() else {
            return;
        };
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        if self.config.enable_histograms {
            let millis =
                (duration.as_millis().min(u128::from(u64::MAX)) as u64).min(HISTOGRAM_MAX_MS);
            if histogram.record(millis).is_err() {
                return;
            }
        }
        self.processing_time_count.fetch_add(1, Ordering::Relaxed);
        self.processing_time_sum_nanos
            .fetch_add(nanos, Ordering::Relaxed);
        self.last_processing_time_nanos
            .store(nanos, Ordering::Relaxed);
        self.last_processing_time_available
            .store(true, Ordering::Relaxed);
    }

    /// The most-recent per-invocation processing duration. The output committer
    /// reads this to stamp `processing_info.processing_time` on stage outputs
    /// (FLOWIP-115f), replacing the deleted `TimingMiddleware` observer.
    pub fn last_processing_time(&self) -> Option<obzenflow_core::time::MetricsDuration> {
        self.last_processing_time_available
            .load(Ordering::Relaxed)
            .then(|| {
                obzenflow_core::time::MetricsDuration::from_nanos(
                    self.last_processing_time_nanos.load(Ordering::Relaxed),
                )
            })
    }

    /// Update the state label used in runtime snapshots, without driving the FSM.
    /// Re-observing the same state preserves its original entry time.
    pub fn transition_to_state(&self, new_state: &str) {
        let mut state = self.current_state.write().unwrap();
        let mut entered_at = self.state_entered_at.write().unwrap();
        let changed = state.as_str() != new_state;
        if changed {
            *state = new_state.to_string();
            *entered_at = Instant::now();
        }
        drop(entered_at);
        drop(state);
        if changed
            && matches!(
                new_state,
                "Completed" | "Drained" | "Failed" | "Cancelled" | "Terminated"
            )
        {
            self.offer_capture(CaptureReason::Final);
        }
    }

    /// Check if a duration is an anomaly (outlier)
    pub fn check_anomaly(&self, duration: Duration) -> bool {
        if !self.config.enable_anomaly_detection {
            return false;
        }

        let duration_ms = duration.as_millis() as u64;
        let histogram = self.processing_time_histogram.read().unwrap();
        let p99 = histogram.value_at_quantile(QUANTILE_P99);

        // Consider it an anomaly if it's more than 3x the p99
        duration_ms > p99 * 3
    }

    /// Get utilization percentage (0-100)
    pub fn utilization_percentage(&self) -> f64 {
        if !self.config.enable_utilization {
            return 0.0;
        }

        let total_loops = self.event_loops_total.load(Ordering::Relaxed);
        let loops_with_work = self.event_loops_with_work_total.load(Ordering::Relaxed);

        if total_loops == 0 {
            0.0
        } else {
            (loops_with_work as f64 / total_loops as f64) * 100.0
        }
    }

    /// Record an error occurrence with a specific ErrorKind.
    pub fn record_error(&self, kind: obzenflow_core::event::status::processing_status::ErrorKind) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        let mut by_kind = self.errors_by_kind.write().unwrap();
        by_kind
            .entry(kind)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
}

use obzenflow_core::event::context::{
    CircuitBreakerMeasurements, ExecutionAccounting, ExecutionProgress, MeasurementWindow,
    RateLimiterMeasurements, RuntimeObservability, RuntimeProvenance, TimingMeasurements,
};
use obzenflow_core::event::observation::{
    CaptureReason, NoObservations, ObservabilityContext, ObservationRecord, ObservationRecorder,
};
use std::error::Error;
/// Higher-order function for instrumented event processing
use std::future::Future;
use std::sync::Arc;

pub async fn process_with_instrumentation<T, F, Fut>(
    instrumentation: &Arc<StageInstrumentation>,
    f: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error + Send + Sync>>>,
{
    // Track in-flight
    instrumentation
        .in_flight_count
        .fetch_add(1, Ordering::Relaxed);

    // Process with timing
    let start = Instant::now();
    let result = f().await;
    let duration = start.elapsed();

    // Update metrics
    instrumentation
        .in_flight_count
        .fetch_sub(1, Ordering::Relaxed);

    // Record processing time
    instrumentation.record_processing_time(duration);

    // Check for anomalies
    if instrumentation.check_anomaly(duration) {
        instrumentation
            .anomalies_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Track success/error
    if result.is_ok() {
        instrumentation
            .events_processed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    result
}

/// Higher-order function for instrumented operations that should NOT increment
/// `events_processed_total`.
///
/// Use this for work that is stage-internal (e.g. emitting aggregated results)
/// and should not count as processing an additional input event.
pub async fn process_with_instrumentation_no_count<T, F, Fut>(
    instrumentation: &Arc<StageInstrumentation>,
    f: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error + Send + Sync>>>,
{
    // Track in-flight
    instrumentation
        .in_flight_count
        .fetch_add(1, Ordering::Relaxed);

    // Process with timing
    let start = Instant::now();
    let result = f().await;
    let duration = start.elapsed();

    // Update metrics
    instrumentation
        .in_flight_count
        .fetch_sub(1, Ordering::Relaxed);

    // Record processing time
    instrumentation.record_processing_time(duration);

    // Check for anomalies
    if instrumentation.check_anomaly(duration) {
        instrumentation
            .anomalies_total
            .fetch_add(1, Ordering::Relaxed);
    }

    result
}

/// Terminal accounting comes from the stage owner at the terminal boundary.
pub fn snapshot_stage_accounting(instrumentation: &StageInstrumentation) -> ExecutionAccounting {
    instrumentation.snapshot().accounting
}

#[cfg(test)]
mod tests {
    use super::StageInstrumentation;
    use obzenflow_core::event::identity::JournalWriterId;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{EventId, EventType, JournalId, JournalRecord, StageId, WriterId};

    #[test]
    fn repeated_state_observation_preserves_state_age() {
        use std::time::{Duration, Instant};

        let instrumentation = StageInstrumentation::new();
        let entered = Instant::now() - Duration::from_secs(60);
        *instrumentation.state_entered_at.write().unwrap() = entered;
        instrumentation.transition_to_state("Created");
        assert_eq!(*instrumentation.state_entered_at.read().unwrap(), entered);
        let snapshot = instrumentation.snapshot();
        assert_eq!(snapshot.fsm_state, "Created");
        assert!(
            instrumentation
                .state_entered_at
                .read()
                .unwrap()
                .elapsed()
                .as_millis()
                >= 60_000
        );

        let before_transition = Instant::now();
        instrumentation.transition_to_state("Running");
        assert!(*instrumentation.state_entered_at.read().unwrap() >= before_transition);
        assert_eq!(instrumentation.snapshot().fsm_state, "Running");
    }

    #[test]
    fn record_error_updates_totals_and_by_kind() {
        let instrumentation = StageInstrumentation::new();

        // No errors initially
        let initial = instrumentation.snapshot();
        assert_eq!(initial.accounting.errors_total, 0);
        assert!(initial.accounting.errors_by_kind.is_empty());

        // Record one Domain error and two Timeout errors
        instrumentation.record_error(ErrorKind::Domain);
        instrumentation.record_error(ErrorKind::Timeout);
        instrumentation.record_error(ErrorKind::Timeout);

        let snapshot = instrumentation.snapshot();
        assert_eq!(snapshot.accounting.errors_total, 3);
        assert_eq!(
            snapshot.accounting.errors_by_kind.get(&ErrorKind::Domain),
            Some(&1)
        );
        assert_eq!(
            snapshot.accounting.errors_by_kind.get(&ErrorKind::Timeout),
            Some(&2)
        );
    }

    #[test]
    fn record_receipted_position_updates_snapshot() {
        let instrumentation = StageInstrumentation::new();
        let event_id = EventId::new();
        let mut vector_clock = VectorClock::new();
        vector_clock.clocks.insert("sink".to_string(), 7);

        instrumentation.record_receipted_position(7, event_id, vector_clock.clone());

        let snapshot = instrumentation.snapshot();
        assert_eq!(snapshot.progress.receipted_seq, 7);
        assert_eq!(snapshot.progress.last_receipted_event_id, Some(event_id));
        assert_eq!(
            snapshot.progress.last_receipted_vector_clock,
            Some(vector_clock)
        );
    }

    #[test]
    fn consumed_type_counter_uses_the_delivering_reader_not_the_preserved_event_author() {
        let instrumentation = StageInstrumentation::new();
        let author = StageId::new();
        let physical_upstream = StageId::new();
        let event = ChainEventFactory::data_event(
            WriterId::from(author),
            "checkout.command.v1",
            serde_json::json!({}),
        );
        let envelope = JournalRecord::<obzenflow_core::event::ChainPayload>::new(
            JournalWriterId::from(JournalId::new()),
            event,
        );

        instrumentation.record_consumed(&envelope, physical_upstream);

        assert_eq!(
            instrumentation
                .snapshot()
                .accounting
                .data_inputs_by_upstream_event_type,
            vec![
                obzenflow_core::event::context::UpstreamEventTypeCountContext {
                    upstream: physical_upstream,
                    event_type: EventType::from("checkout.command.v1"),
                    total: 1,
                }
            ]
        );
    }

    #[test]
    fn error_journal_data_is_excluded_from_typed_data_output_counters() {
        let instrumentation = StageInstrumentation::new();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "checkout.failed.v1",
            serde_json::json!({}),
        );

        instrumentation.record_error_journal_output_event(&event);

        let snapshot = instrumentation.snapshot();
        assert_eq!(snapshot.accounting.events_emitted_total, 1);
        assert!(snapshot.accounting.data_outputs_by_event_type.is_empty());
    }
}
