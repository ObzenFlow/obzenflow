//! FSM instrumentation for HandlerSupervised stages

use hdrhistogram::Histogram;
use obzenflow_core::control_middleware::{
    CircuitBreakerSnapshotter, ControlMiddlewareProvider, NoControlMiddleware,
    RateLimiterSnapshotter,
};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::journal_writer_id::JournalWriterId;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::metrics::StageMetricsSnapshot;
use obzenflow_core::EventId;
use obzenflow_core::StageId;
use obzenflow_core::WriterId;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};
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

/// Stage instrumentation that tracks metrics alongside FSM state
pub struct StageInstrumentation {
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

    // FSM state tracking
    pub current_state: RwLock<String>,
    pub state_entered_at: RwLock<Instant>,

    // Observability positions
    pub reader_seq: AtomicU64,
    pub writer_seq: AtomicU64,
    pub last_consumed_event_id: RwLock<Option<EventId>>,
    pub last_consumed_writer: RwLock<Option<JournalWriterId>>,
    pub last_consumed_vector_clock: RwLock<Option<VectorClock>>,
    pub last_emitted_event_id: RwLock<Option<EventId>>,
    pub last_emitted_writer: RwLock<Option<WriterId>>,

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
    // Control middleware bindings (FLOWIP-059a-3)
    // =========================================================================
    /// Flow-scoped provider for control middleware state/metrics.
    control_middleware: Arc<dyn ControlMiddlewareProvider>,

    /// Cached snapshotter for circuit breaker metrics (set once during stage construction).
    cb_snapshotter: Option<Arc<CircuitBreakerSnapshotter>>,

    /// Cached snapshotter for rate limiter metrics (set once during stage construction).
    rl_snapshotter: Option<Arc<RateLimiterSnapshotter>>,

    /// Cached circuit breaker state for control strategies (set once during stage construction).
    cb_state: Option<Arc<std::sync::atomic::AtomicU8>>,
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
            // Gauges
            in_flight_count: AtomicU32::new(0),
            join_reference_since_last_stream: AtomicU64::new(0),

            // Counters
            events_processed_total: AtomicU64::new(0),
            events_accumulated_total: AtomicU64::new(0),
            events_emitted_total: AtomicU64::new(0),
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

            // State
            current_state: RwLock::new("Created".to_string()),
            state_entered_at: RwLock::new(Instant::now()),

            // Observability positions
            reader_seq: AtomicU64::new(0),
            writer_seq: AtomicU64::new(0),
            last_consumed_event_id: RwLock::new(None),
            last_consumed_writer: RwLock::new(None),
            last_consumed_vector_clock: RwLock::new(None),
            last_emitted_event_id: RwLock::new(None),
            last_emitted_writer: RwLock::new(None),

            errors_by_kind: RwLock::new(std::collections::HashMap::new()),

            config,

            control_middleware: Arc::new(NoControlMiddleware),
            cb_snapshotter: None,
            rl_snapshotter: None,
            cb_state: None,
        }
    }

    /// Bind control middleware from the provider for this stage.
    ///
    /// Called once during stage construction. Caches snapshotters and state to
    /// avoid per-event lookups. Fails if expected middleware is missing.
    pub fn bind_control_middleware(
        &mut self,
        stage_id: &StageId,
        provider: &Arc<dyn ControlMiddlewareProvider>,
        expects_circuit_breaker: bool,
        expects_rate_limiter: bool,
    ) -> Result<(), ControlBindError> {
        self.control_middleware = provider.clone();

        self.cb_snapshotter = provider.circuit_breaker_snapshotter(stage_id);
        self.rl_snapshotter = provider.rate_limiter_snapshotter(stage_id);
        self.cb_state = provider.circuit_breaker_state(stage_id);
        let cb_contract_info_present = provider.circuit_breaker_contract_info(stage_id).is_some();

        if expects_circuit_breaker
            && (self.cb_snapshotter.is_none()
                || self.cb_state.is_none()
                || !cb_contract_info_present)
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

    /// Create a snapshot for event injection
    pub fn snapshot(&self) -> RuntimeContext {
        let histogram = self.processing_time_histogram.read().unwrap();

        RuntimeContext {
            // Gauge snapshots
            in_flight: self.in_flight_count.load(Ordering::Relaxed),

            // Histogram percentiles
            recent_p50_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P50)
            } else {
                0
            },
            recent_p90_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P90)
            } else {
                0
            },
            recent_p95_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P95)
            } else {
                0
            },
            recent_p99_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P99)
            } else {
                0
            },
            recent_p999_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P999)
            } else {
                0
            },

            // Actual sum - always tracked, never reconstructed from percentiles
            processing_time_sum_nanos: self.processing_time_sum_nanos.load(Ordering::Relaxed),

            // Counter snapshots (totals, not rates!)
            events_processed_total: self.events_processed_total.load(Ordering::Relaxed),
            events_accumulated_total: self.events_accumulated_total.load(Ordering::Relaxed),
            events_emitted_total: self.events_emitted_total.load(Ordering::Relaxed),
            join_reference_since_last_stream: self
                .join_reference_since_last_stream
                .load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),

            // FSM state
            fsm_state: self.current_state.read().unwrap().clone(),
            time_in_state_ms: self.state_entered_at.read().unwrap().elapsed().as_millis() as u64,

            // Event loop metrics
            event_loops_total: self.event_loops_total.load(Ordering::Relaxed),
            event_loops_with_work_total: self.event_loops_with_work_total.load(Ordering::Relaxed),

            // Observability positions
            reader_seq: self.reader_seq.load(Ordering::Relaxed),
            writer_seq: self.writer_seq.load(Ordering::Relaxed),
            last_consumed_event_id: *self.last_consumed_event_id.read().unwrap(),
            last_consumed_writer: *self.last_consumed_writer.read().unwrap(),
            last_consumed_vector_clock: self.last_consumed_vector_clock.read().unwrap().clone(),
            last_emitted_event_id: *self.last_emitted_event_id.read().unwrap(),
            last_emitted_writer: *self.last_emitted_writer.read().unwrap(),
            errors_by_kind: self
                .errors_by_kind
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect(),

            // Control middleware cumulative metrics are injected by supervisors
            // via `snapshot_with_control()` so defaults are always present.
            cb_requests_total: 0,
            cb_successes_total: 0,
            cb_failures_total: 0,
            cb_rejections_total: 0,
            cb_opened_total: 0,
            cb_time_closed_seconds: 0.0,
            cb_time_open_seconds: 0.0,
            cb_time_half_open_seconds: 0.0,
            cb_state: 0.0,
            rl_events_total: 0,
            rl_delayed_total: 0,
            rl_tokens_consumed_total: 0.0,
            rl_delay_seconds_total: 0.0,
            rl_bucket_tokens: 0.0,
            rl_bucket_capacity: 0.0,
        }
    }

    /// Create a wide-event snapshot that also includes cumulative control middleware metrics.
    ///
    /// This follows the wide-events philosophy: every event carries the current cumulative
    /// circuit breaker and rate limiter state so tail-start observers can still export
    /// accurate totals by reading the journal tail.
    pub fn snapshot_with_control(&self) -> RuntimeContext {
        let mut ctx = self.snapshot();

        if let Some(cb_snapshotter) = &self.cb_snapshotter {
            let cb = cb_snapshotter();
            ctx.cb_requests_total = cb.requests_total;
            ctx.cb_successes_total = cb.successes_total;
            ctx.cb_failures_total = cb.failures_total;
            ctx.cb_rejections_total = cb.rejections_total;
            ctx.cb_opened_total = cb.opened_total;
            ctx.cb_time_closed_seconds = cb.time_closed_seconds;
            ctx.cb_time_open_seconds = cb.time_open_seconds;
            ctx.cb_time_half_open_seconds = cb.time_half_open_seconds;
            ctx.cb_state = match cb.state {
                0 => 0.0, // closed
                1 => 1.0, // open
                2 => 0.5, // half_open
                _ => 0.0,
            };
        }

        if let Some(rl_snapshotter) = &self.rl_snapshotter {
            let rl = rl_snapshotter();
            ctx.rl_events_total = rl.events_total;
            ctx.rl_delayed_total = rl.delayed_total;
            ctx.rl_tokens_consumed_total = rl.tokens_consumed_total;
            ctx.rl_delay_seconds_total = rl.delay_seconds_total;
            ctx.rl_bucket_tokens = rl.bucket_tokens;
            ctx.rl_bucket_capacity = rl.bucket_capacity;
        }

        ctx
    }

    /// Access the flow-scoped control middleware provider.
    pub fn control_middleware(&self) -> &Arc<dyn ControlMiddlewareProvider> {
        &self.control_middleware
    }

    /// Access cached circuit breaker state (if bound).
    pub fn circuit_breaker_state(&self) -> Option<&Arc<std::sync::atomic::AtomicU8>> {
        self.cb_state.as_ref()
    }

    /// Note a consumed envelope so downstream events capture reader position and origin.
    pub fn record_consumed<T: JournalEvent>(&self, envelope: &EventEnvelope<T>) {
        self.reader_seq.fetch_add(1, Ordering::Relaxed);
        *self.last_consumed_event_id.write().unwrap() = Some(*envelope.event.id());
        *self.last_consumed_writer.write().unwrap() = Some(envelope.journal_writer_id);
        *self.last_consumed_vector_clock.write().unwrap() = Some(envelope.vector_clock.clone());
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
    pub fn record_output_event<T: JournalEvent>(&self, event: &T) {
        self.record_emitted(event);
        self.events_emitted_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record processing duration in histogram and sum.
    ///
    /// The sum is always tracked (for accurate Prometheus histogram export).
    /// The histogram is only updated if enable_histograms is true.
    pub fn record_processing_time(&self, duration: Duration) {
        // Always track the actual sum - this is never reconstructed from percentiles
        let duration_nanos = duration.as_nanos() as u64;
        self.processing_time_sum_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);

        // Histogram recording is optional (for percentiles)
        if !self.config.enable_histograms {
            return;
        }

        let duration_ms = duration.as_millis() as u64;
        let clamped = duration_ms.clamp(HISTOGRAM_MIN_MS, HISTOGRAM_MAX_MS);

        if let Ok(mut histogram) = self.processing_time_histogram.write() {
            histogram
                .record(clamped)
                .unwrap_or_else(|e| tracing::warn!("Failed to record duration: {:?}", e));
        }
    }

    /// Track FSM state transition
    pub fn transition_to_state(&self, new_state: &str) {
        *self.current_state.write().unwrap() = new_state.to_string();
        *self.state_entered_at.write().unwrap() = Instant::now();
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

use obzenflow_core::runtime_context::RuntimeContext;
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

/// Create a UI-oriented stage metrics snapshot for lifecycle events
pub fn snapshot_stage_metrics(instrumentation: &StageInstrumentation) -> StageMetricsSnapshot {
    let ctx = instrumentation.snapshot();
    StageMetricsSnapshot {
        events_processed_total: ctx.events_processed_total,
        events_accumulated_total: ctx.events_accumulated_total,
        events_emitted_total: ctx.events_emitted_total,
        errors_total: ctx.errors_total,
        errors_by_kind: ctx.errors_by_kind,
        in_flight: ctx.in_flight,
        recent_p50_ms: ctx.recent_p50_ms,
        recent_p90_ms: ctx.recent_p90_ms,
        recent_p95_ms: ctx.recent_p95_ms,
        recent_p99_ms: ctx.recent_p99_ms,
        recent_p999_ms: ctx.recent_p999_ms,
        processing_time_sum_nanos: ctx.processing_time_sum_nanos,
        event_loops_total: ctx.event_loops_total,
        event_loops_with_work_total: ctx.event_loops_with_work_total,
    }
}

/// Heartbeat interval for stateful/join observability events.
///
/// Controlled via `OBZENFLOW_HEARTBEAT_INTERVAL` with a sensible default:
/// - If the env var is unset or invalid, defaults to 1000 events.
pub fn heartbeat_interval() -> u64 {
    static INTERVAL: OnceLock<u64> = OnceLock::new();

    *INTERVAL.get_or_init(|| {
        std::env::var("OBZENFLOW_HEARTBEAT_INTERVAL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000)
    })
}

#[cfg(test)]
mod tests {
    use super::StageInstrumentation;
    use obzenflow_core::event::status::processing_status::ErrorKind;

    #[test]
    fn record_error_updates_totals_and_by_kind() {
        let instrumentation = StageInstrumentation::new();

        // No errors initially
        let initial = instrumentation.snapshot();
        assert_eq!(initial.errors_total, 0);
        assert!(initial.errors_by_kind.is_empty());

        // Record one Domain error and two Timeout errors
        instrumentation.record_error(ErrorKind::Domain);
        instrumentation.record_error(ErrorKind::Timeout);
        instrumentation.record_error(ErrorKind::Timeout);

        let snapshot = instrumentation.snapshot();
        assert_eq!(snapshot.errors_total, 3);
        assert_eq!(snapshot.errors_by_kind.get(&ErrorKind::Domain), Some(&1));
        assert_eq!(snapshot.errors_by_kind.get(&ErrorKind::Timeout), Some(&2));
    }
}
