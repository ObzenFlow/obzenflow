//! FLOWIP-059a Phase 3 integration tests
//!
//! These tests validate end-to-end wiring for:
//! - Middleware metrics (circuit breaker, rate limiter) flowing through MetricsAggregator
//! - Contract verification metrics flowing through MetricsAggregator
//! - Prometheus `/metrics` rendering with joinable, ID-first label sets

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit_with_burst};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::pipeline::handle::MiddlewareStackConfig;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime_services::stages::SourceError;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::time::Instant;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
struct BurstSource {
    total: usize,
    current: usize,
    writer_id: WriterId,
}

impl BurstSource {
    fn new(total: usize) -> Self {
        Self {
            total,
            current: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for BurstSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.current >= self.total {
            return Ok(None);
        }

        let idx = self.current;
        self.current += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "test.burst",
            json!({ "index": idx }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct PassthroughTransform;

#[async_trait]
impl TransformHandler for PassthroughTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DropTransform;

#[async_trait]
impl TransformHandler for DropTransform {
    fn process(&self, _event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct TimeoutAfterFirstTransform;

#[async_trait]
impl TransformHandler for TimeoutAfterFirstTransform {
    fn process(&self, mut event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let idx = event
            .payload()
            .get("index")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if idx >= 1 {
            event.processing_info.status = ProcessingStatus::error_with_kind(
                "simulated_timeout",
                Some(ErrorKind::Timeout),
            );
        }

        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<Mutex<u64>>,
}

impl CountingSink {
    fn new() -> (Self, Arc<Mutex<u64>>) {
        let count = Arc::new(Mutex::new(0));
        (Self { count: count.clone() }, count)
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            if let Ok(mut count) = self.count.lock() {
                *count += 1;
            }
        }

        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("count".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct SleepingSink {
    sleep_per_event: Duration,
}

impl SleepingSink {
    fn new(sleep_per_event: Duration) -> Self {
        Self { sleep_per_event }
    }
}

#[async_trait]
impl SinkHandler for SleepingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            sleep(self.sleep_per_event).await;
        }

        Ok(DeliveryPayload::success(
            "sleeping_sink",
            DeliveryMethod::Custom("sleep".to_string()),
            None,
        ))
    }
}

async fn wait_for_metrics(
    exporter: &Arc<dyn MetricsExporter>,
    timeout: Duration,
    debug_patterns: &[String],
    predicate: impl Fn(&str) -> bool,
) -> Result<String> {
    let start = Instant::now();
    let mut last = String::new();

    while start.elapsed() < timeout {
        last = exporter
            .render_metrics()
            .map_err(|e| anyhow!("Failed to render metrics: {e}"))?;
        if predicate(&last) {
            return Ok(last);
        }
        sleep(Duration::from_millis(50)).await;
    }

    let excerpt = filter_metrics_lines(&last, debug_patterns, 120);
    Err(anyhow!(
        "timed out waiting for expected metrics (last snapshot length={}).\nFiltered excerpt:\n{}",
        last.len(),
        if excerpt.is_empty() {
            "(no matching lines)"
        } else {
            excerpt.as_str()
        }
    ))
}

fn filter_metrics_lines(metrics_text: &str, patterns: &[String], max_lines: usize) -> String {
    if patterns.is_empty() || max_lines == 0 {
        return String::new();
    }

    let mut out = Vec::new();
    for line in metrics_text.lines() {
        if patterns.iter().any(|p| !p.is_empty() && line.contains(p)) {
            out.push(line);
            if out.len() >= max_lines {
                break;
            }
        }
    }

    out.join("\n")
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

async fn run_with_metrics_timeout(
    flow_handle: obzenflow_runtime_services::pipeline::handle::FlowHandle,
    timeout: Duration,
) -> Result<Arc<dyn MetricsExporter>> {
    match tokio::time::timeout(timeout, flow_handle.run_with_metrics()).await {
        Ok(run_result) => run_result
            .map_err(|e| anyhow!("Failed to run flow: {e:?}"))?
            .ok_or_else(|| anyhow!("Metrics exporter was not configured")),
        Err(_) => Err(anyhow!(
            "flow did not complete within {:?} (timeout)",
            timeout
        )),
    }
}

fn stage_id_with_middleware(
    stacks: &HashMap<StageId, MiddlewareStackConfig>,
    middleware_name: &str,
) -> Result<StageId> {
    stacks
        .iter()
        .find(|(_, cfg)| cfg.stack.iter().any(|name| name == middleware_name))
        .map(|(id, _)| *id)
        .ok_or_else(|| anyhow!("no stage found with middleware '{middleware_name}'"))
}

fn metric_line_value(metrics_text: &str, metric_name: &str, required_substrings: &[String]) -> Option<f64> {
    metrics_text.lines().find_map(|line| {
        if !line.starts_with(metric_name) {
            return None;
        }
        if !required_substrings.iter().all(|s| line.contains(s)) {
            return None;
        }
        line.split_whitespace()
            .last()
            .and_then(|v| v.parse::<f64>().ok())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_all_stage_metrics_include_flow_id_label() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let flow_handle = flow! {
        name: "flowip_059a_flow_id_labels",
        journals: disk_journals(unique_journal_dir("flowip_059a_flow_id_labels")),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(50), [
                // No summaries/threshold crossings required; utilization is derived from bucket state.
                rate_limit_with_burst(10_000.0, 10_000.0)
            ]);
            trans = transform!("trans" => DropTransform, [
                circuit_breaker(10)
            ]);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let debug_patterns = vec![
        "stage_id=\"".to_string(),
        "flow_id=\"".to_string(),
        "obzenflow_events_total".to_string(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_events_total") && text.contains("stage_id=\"")
    })
    .await?;

    let stage_lines: Vec<&str> = metrics_text
        .lines()
        .filter(|line| !line.starts_with('#') && line.contains("stage_id=\""))
        .collect();
    assert!(
        !stage_lines.is_empty(),
        "expected at least one stage-scoped metric line"
    );

    for line in stage_lines {
        assert!(
            line.contains("flow_id=\""),
            "missing flow_id label on stage-scoped metric: {line}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_processing_time_sum_tracks_actual_work() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let per_event = Duration::from_millis(10);
    let total_events: usize = 100;

    let flow_handle = flow! {
        name: "flowip_059a_processing_time_sum",
        journals: disk_journals(unique_journal_dir("flowip_059a_processing_time_sum")),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(total_events));
            trans = transform!("trans" => PassthroughTransform);
            snk = sink!("snk" => SleepingSink::new(per_event));
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let flow_label = "flow=\"flowip_059a_processing_time_sum\"".to_string();
    let stage_label = "stage=\"snk\"".to_string();
    let debug_patterns = vec![
        "obzenflow_processing_time_seconds_sum".to_string(),
        flow_label.clone(),
        stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_processing_time_seconds_sum") && text.contains(&stage_label)
    })
    .await?;

    let sum_s = metric_line_value(
        &metrics_text,
        "obzenflow_processing_time_seconds_sum{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing processing_time_seconds_sum for {stage_label}"))?;

    // Expect: 100 events * 10ms = 1s of handler time (plus overhead).
    assert!(sum_s >= 0.5, "expected sum >= 0.5s, got {sum_s}");
    assert!(sum_s < 10.0, "expected sum < 10s, got {sum_s}");

    // Histogram buckets should be cumulative and end at _count.
    let count = metric_line_value(
        &metrics_text,
        "obzenflow_processing_time_seconds_count{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing processing_time_seconds_count for {stage_label}"))?
        as u64;

    let expected_buckets = [
        "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf",
    ];

    let mut prev: u64 = 0;
    for le in expected_buckets {
        let le_label = format!("le=\"{le}\"");
        let bucket_count = metric_line_value(
            &metrics_text,
            "obzenflow_processing_time_seconds_bucket{",
            &[flow_label.clone(), stage_label.clone(), le_label],
        )
        .ok_or_else(|| anyhow!("missing processing_time_seconds_bucket for {stage_label}, le={le}"))?
            as u64;
        assert!(
            bucket_count >= prev,
            "expected cumulative buckets; le={le} count={bucket_count} < prev={prev}"
        );
        prev = bucket_count;
    }

    assert_eq!(
        prev, count,
        "expected +Inf bucket count to equal _count ({} != {})",
        prev, count
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_circuit_breaker_counters_are_exported_with_joinable_labels() -> Result<()> {
    // Strict timeout protocol: if the flow or metrics pipeline hangs, fail fast.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let flow_handle = flow! {
        name: "flowip_059a_cb_phase3",
        journals: disk_journals(unique_journal_dir("flowip_059a_cb_phase3")),
        middleware: [],

        stages: {
            // 1000 events triggers a CircuitBreaker summary (>=1000 processed requests).
            // Use 1001 so the summary is not emitted on the final stage output.
            src = source!("src" => BurstSource::new(1001));
            // Drop downstream data outputs to keep journaling light; the circuit breaker
            // still observes successful processing and emits a summary at 1000.
            trans = transform!("trans" => DropTransform, [
                circuit_breaker(10)
            ]);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let middleware_stacks = flow_handle
        .middleware_stacks()
        .expect("FlowHandle should expose middleware stacks for joinability");
    let cb_stage_id = stage_id_with_middleware(&middleware_stacks, "circuit_breaker")?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let cb_stage_label = format!("stage_id=\"{}\"", cb_stage_id);
    let flow_label = "flow=\"flowip_059a_cb_phase3\"".to_string();
    let debug_patterns = vec![
        "obzenflow_circuit_breaker".to_string(),
        "obzenflow_circuit_breaker_requests_total".to_string(),
        "obzenflow_circuit_breaker_rejections_total".to_string(),
        flow_label.clone(),
        cb_stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(
        &exporter,
        timeout_metrics,
        &debug_patterns,
        |text| {
            text.contains("obzenflow_circuit_breaker_requests_total")
                && text.contains("obzenflow_circuit_breaker_rejections_total")
                && text.contains(&cb_stage_label)
        },
    )
    .await?;

    // Counters must exist and be stage_id joinable.
    assert!(
        metrics_text.lines().any(|l| {
            l.starts_with("obzenflow_circuit_breaker_requests_total{")
                && l.contains(&flow_label)
                && l.contains(&cb_stage_label)
        }),
        "expected circuit breaker requests counter with flow + stage_id labels"
    );

    assert!(
        metrics_text.lines().any(|l| {
            l.starts_with("obzenflow_circuit_breaker_rejections_total{")
                && l.contains(&flow_label)
                && l.contains(&cb_stage_label)
        }),
        "expected circuit breaker rejections counter with flow + stage_id labels"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_circuit_breaker_cumulative_metrics_are_exported_and_trippable() -> Result<()> {
    // Strict timeout protocol: if the flow or metrics pipeline hangs, fail fast.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let flow_handle = flow! {
        name: "flowip_059a_cb_cumulative",
        journals: disk_journals(unique_journal_dir("flowip_059a_cb_cumulative")),
        middleware: [],

        stages: {
            // 1001 events ensures the 1000-threshold summary is emitted before the run completes.
            src = source!("src" => BurstSource::new(1001));
            // First event succeeds, second event fails (Timeout), opening the breaker.
            trans = transform!("trans" => TimeoutAfterFirstTransform, [
                circuit_breaker(1)
            ]);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let middleware_stacks = flow_handle
        .middleware_stacks()
        .expect("FlowHandle should expose middleware stacks for joinability");
    let cb_stage_id = stage_id_with_middleware(&middleware_stacks, "circuit_breaker")?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let cb_stage_label = format!("stage_id=\"{}\"", cb_stage_id);
    let flow_label = "flow=\"flowip_059a_cb_cumulative\"".to_string();
    let debug_patterns = vec![
        "obzenflow_circuit_breaker_opened_total".to_string(),
        "obzenflow_circuit_breaker_successes_total".to_string(),
        "obzenflow_circuit_breaker_failures_total".to_string(),
        "obzenflow_circuit_breaker_time_in_state_seconds_total".to_string(),
        "obzenflow_circuit_breaker_state_transitions_total".to_string(),
        flow_label.clone(),
        cb_stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_circuit_breaker_opened_total")
            && text.contains("obzenflow_circuit_breaker_successes_total")
            && text.contains("obzenflow_circuit_breaker_failures_total")
            && text.contains("obzenflow_circuit_breaker_time_in_state_seconds_total")
            && text.contains("obzenflow_circuit_breaker_state_transitions_total")
            && text.contains(&flow_label)
            && text.contains(&cb_stage_label)
    })
    .await?;

    let opened_total = metric_line_value(
        &metrics_text,
        "obzenflow_circuit_breaker_opened_total{",
        &[flow_label.clone(), cb_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing circuit_breaker_opened_total for {cb_stage_label}"))?;
    assert!(
        (opened_total - 1.0).abs() < f64::EPSILON,
        "expected circuit_breaker_opened_total=1, got {opened_total}"
    );

    let successes_total = metric_line_value(
        &metrics_text,
        "obzenflow_circuit_breaker_successes_total{",
        &[flow_label.clone(), cb_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing circuit_breaker_successes_total for {cb_stage_label}"))?;
    assert!(
        (successes_total - 1.0).abs() < f64::EPSILON,
        "expected circuit_breaker_successes_total=1, got {successes_total}"
    );

    let failures_total = metric_line_value(
        &metrics_text,
        "obzenflow_circuit_breaker_failures_total{",
        &[flow_label.clone(), cb_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing circuit_breaker_failures_total for {cb_stage_label}"))?;
    assert!(
        (failures_total - 1.0).abs() < f64::EPSILON,
        "expected circuit_breaker_failures_total=1, got {failures_total}"
    );

    let transitions_total = metric_line_value(
        &metrics_text,
        "obzenflow_circuit_breaker_state_transitions_total{",
        &[
            flow_label.clone(),
            cb_stage_label.clone(),
            "from_state=\"closed\"".to_string(),
            "to_state=\"open\"".to_string(),
        ],
    )
    .ok_or_else(|| {
        anyhow!("missing circuit_breaker_state_transitions_total closed->open for {cb_stage_label}")
    })?;
    assert!(
        (transitions_total - 1.0).abs() < f64::EPSILON,
        "expected closed->open transition count=1, got {transitions_total}"
    );

    for state in ["closed", "open", "half_open"] {
        let state_label = format!("state=\"{state}\"");
        assert!(
            metrics_text.lines().any(|l| {
                l.starts_with("obzenflow_circuit_breaker_time_in_state_seconds_total{")
                    && l.contains(&flow_label)
                    && l.contains(&cb_stage_label)
                    && l.contains(&state_label)
            }),
            "missing circuit_breaker_time_in_state_seconds_total series for {state_label}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_rate_limiter_metrics_are_exported_with_joinable_labels() -> Result<()> {
    // Strict timeout protocol: if the flow or metrics pipeline hangs, fail fast.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    // Configure the limiter to produce:
    // - at least 1 delayed event (tiny burst forces blocking)
    // - a window summary (>=1000 processed events triggers summary)
    let flow_handle = flow! {
        name: "flowip_059a_rl_phase3",
        journals: disk_journals(unique_journal_dir("flowip_059a_rl_phase3")),
        middleware: [],

        stages: {
            // Use 1001 so the summary is not emitted on the final stage output.
            src = source!("src" => BurstSource::new(1001), [
                // Force deterministic backpressure: small burst + low rate so at least
                // one event must block, while still reaching 1000 events well under the
                // 10s time-based summary threshold.
                rate_limit_with_burst(200.0, 1.0)
            ]);
            // Drop downstream data outputs to keep journaling light; rate limiting
            // metrics are emitted from the source stage.
            trans = transform!("trans" => DropTransform);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let middleware_stacks = flow_handle
        .middleware_stacks()
        .expect("FlowHandle should expose middleware stacks for joinability");
    let rl_stage_id = stage_id_with_middleware(&middleware_stacks, "rate_limiter")?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let rl_stage_label = format!("stage_id=\"{}\"", rl_stage_id);
    let flow_label = "flow=\"flowip_059a_rl_phase3\"".to_string();
    let debug_patterns = vec![
        "obzenflow_rate_limiter".to_string(),
        "obzenflow_rate_limiter_events_total".to_string(),
        "obzenflow_rate_limiter_delayed_total".to_string(),
        "obzenflow_rate_limiter_tokens_consumed_total".to_string(),
        "obzenflow_rate_limiter_delay_seconds_total".to_string(),
        "obzenflow_rate_limiter_utilization".to_string(),
        "obzenflow_rate_limiter_bucket_tokens".to_string(),
        "obzenflow_rate_limiter_bucket_capacity".to_string(),
        flow_label.clone(),
        rl_stage_label.clone(),
    ];

    let expected_events_total = 1001.0;
    let join_labels = vec![flow_label.clone(), rl_stage_label.clone()];
    let metrics_text = wait_for_metrics(
        &exporter,
        timeout_metrics,
        &debug_patterns,
        |text| {
            let events_total = metric_line_value(
                text,
                "obzenflow_rate_limiter_events_total{",
                &join_labels,
            );
            let events_total_ok = events_total
                .map(|v| (v - expected_events_total).abs() < f64::EPSILON)
                .unwrap_or(false);

            events_total_ok
                && text.contains("obzenflow_rate_limiter_delayed_total")
                && text.contains("obzenflow_rate_limiter_tokens_consumed_total")
                && text.contains("obzenflow_rate_limiter_delay_seconds_total")
                && text.contains("obzenflow_rate_limiter_utilization")
                && text.contains("obzenflow_rate_limiter_bucket_tokens")
                && text.contains("obzenflow_rate_limiter_bucket_capacity")
        },
    )
    .await?;

    // Counters must exist and be stage_id joinable.
    let events_total = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_events_total{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_events_total for {rl_stage_label}"))?;
    assert!(
        (events_total - 1001.0).abs() < f64::EPSILON,
        "expected rate_limiter_events_total=1001, got {events_total}"
    );

    let delayed_total = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_delayed_total{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_delayed_total for {rl_stage_label}"))?;
    assert!(
        delayed_total >= 1.0,
        "expected at least one delayed event, got {delayed_total}"
    );

    let tokens_consumed_total = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_tokens_consumed_total{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_tokens_consumed_total for {rl_stage_label}"))?;
    assert!(
        tokens_consumed_total >= 1000.0,
        "expected tokens_consumed_total to be populated, got {tokens_consumed_total}"
    );

    let delay_seconds_total = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_delay_seconds_total{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_delay_seconds_total for {rl_stage_label}"))?;
    assert!(
        delay_seconds_total > 0.0,
        "expected delay_seconds_total > 0 when delays occur, got {delay_seconds_total}"
    );

    // Gauges must exist and be stage_id joinable.
    let utilization = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_utilization{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_utilization for {rl_stage_label}"))?;
    assert!(
        (0.0..=1.0).contains(&utilization),
        "expected utilization in [0,1], got {utilization}"
    );

    let bucket_capacity = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_bucket_capacity{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_bucket_capacity for {rl_stage_label}"))?;
    assert!(
        (bucket_capacity - 1.0).abs() < f64::EPSILON,
        "expected bucket_capacity=1.0, got {bucket_capacity}"
    );

    let bucket_tokens = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_bucket_tokens{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_bucket_tokens for {rl_stage_label}"))?;
    assert!(
        (0.0..=bucket_capacity).contains(&bucket_tokens),
        "expected bucket_tokens in [0,capacity], got {bucket_tokens} (capacity={bucket_capacity})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a2_circuit_breaker_requests_total_is_accurate_without_summaries() -> Result<()> {
    // This regression test exercises the pre-fix failure mode:
    // Summary events are emitted periodically (>=1000 or 10s). For short runs,
    // Summary-based counters undercount or stay at 0. Wide-event RuntimeContext
    // snapshots must carry cumulative CB counters so the last event has truth.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);
    let total_events: usize = 500;

    let flow_handle = flow! {
        name: "flowip_059a2_cb_no_summary",
        journals: disk_journals(unique_journal_dir("flowip_059a2_cb_no_summary")),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(total_events));
            trans = transform!("trans" => DropTransform, [
                circuit_breaker(10)
            ]);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let middleware_stacks = flow_handle
        .middleware_stacks()
        .expect("FlowHandle should expose middleware stacks for joinability");
    let cb_stage_id = stage_id_with_middleware(&middleware_stacks, "circuit_breaker")?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let cb_stage_label = format!("stage_id=\"{}\"", cb_stage_id);
    let flow_label = "flow=\"flowip_059a2_cb_no_summary\"".to_string();
    let debug_patterns = vec![
        "obzenflow_circuit_breaker_requests_total".to_string(),
        "obzenflow_circuit_breaker_successes_total".to_string(),
        "obzenflow_circuit_breaker_failures_total".to_string(),
        flow_label.clone(),
        cb_stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_circuit_breaker_requests_total")
            && text.contains(&flow_label)
            && text.contains(&cb_stage_label)
    })
    .await?;

    let requests_total = metric_line_value(
        &metrics_text,
        "obzenflow_circuit_breaker_requests_total{",
        &[flow_label.clone(), cb_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing circuit_breaker_requests_total for {cb_stage_label}"))?;
    assert!(
        (requests_total - total_events as f64).abs() < f64::EPSILON,
        "expected circuit_breaker_requests_total={}, got {requests_total}",
        total_events
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a2_rate_limiter_events_total_is_accurate_without_summaries() -> Result<()> {
    // Same regression class as CB: WindowUtilization events are periodic.
    // Wide-event RuntimeContext snapshots must carry cumulative RL counters, and
    // utilization must be derivable from bucket state even when no summaries emit.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);
    let total_events: usize = 200;

    let flow_handle = flow! {
        name: "flowip_059a2_rl_no_summary",
        journals: disk_journals(unique_journal_dir("flowip_059a2_rl_no_summary")),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(total_events), [
                // High rate + high burst ensures no time-based (10s) or count-based (1000)
                // WindowUtilization summary is *not* required for correct totals or utilization.
                rate_limit_with_burst(10_000.0, 10_000.0)
            ]);
            trans = transform!("trans" => DropTransform);
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let middleware_stacks = flow_handle
        .middleware_stacks()
        .expect("FlowHandle should expose middleware stacks for joinability");
    let rl_stage_id = stage_id_with_middleware(&middleware_stacks, "rate_limiter")?;

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let rl_stage_label = format!("stage_id=\"{}\"", rl_stage_id);
    let flow_label = "flow=\"flowip_059a2_rl_no_summary\"".to_string();
    let debug_patterns = vec![
        "obzenflow_rate_limiter_events_total".to_string(),
        "obzenflow_rate_limiter_utilization".to_string(),
        "obzenflow_rate_limiter_bucket_tokens".to_string(),
        "obzenflow_rate_limiter_bucket_capacity".to_string(),
        flow_label.clone(),
        rl_stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_rate_limiter_events_total")
            && text.contains("obzenflow_rate_limiter_utilization")
            && text.contains("obzenflow_rate_limiter_bucket_tokens")
            && text.contains("obzenflow_rate_limiter_bucket_capacity")
            && text.contains(&flow_label)
            && text.contains(&rl_stage_label)
    })
    .await?;

    let events_total = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_events_total{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_events_total for {rl_stage_label}"))?;
    assert!(
        (events_total - total_events as f64).abs() < f64::EPSILON,
        "expected rate_limiter_events_total={}, got {events_total}",
        total_events
    );

    let utilization = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_utilization{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_utilization for {rl_stage_label}"))?;
    assert!(
        (0.0..=1.0).contains(&utilization),
        "expected utilization in [0,1], got {utilization}"
    );

    let capacity = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_bucket_capacity{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_bucket_capacity for {rl_stage_label}"))?;
    assert!(
        capacity > 0.0,
        "expected bucket_capacity > 0, got {capacity}"
    );

    let tokens = metric_line_value(
        &metrics_text,
        "obzenflow_rate_limiter_bucket_tokens{",
        &[flow_label.clone(), rl_stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing rate_limiter_bucket_tokens for {rl_stage_label}"))?;
    assert!(
        (0.0..=capacity).contains(&tokens),
        "expected bucket_tokens in [0,capacity], got {tokens} (capacity={capacity})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_059a_contract_metrics_are_exported_and_joinable_to_topology() -> Result<()> {
    // Strict timeout protocol: if the flow or metrics pipeline hangs, fail fast.
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let flow_handle = flow! {
        name: "flowip_059a_contracts_phase3",
        journals: disk_journals(unique_journal_dir("flowip_059a_contracts_phase3")),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(10));
            snk = sink!("snk" => CountingSink::new().0);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let contract_attachments = flow_handle
        .contract_attachments()
        .expect("FlowHandle should expose contract attachments for joinability");
    assert!(
        !contract_attachments.is_empty(),
        "expected at least one contract attachment"
    );

    let exporter = run_with_metrics_timeout(flow_handle, timeout_flow).await?;

    let debug_patterns = vec![
        "obzenflow_contract_results_total".to_string(),
        "obzenflow_contract_violations_total".to_string(),
        "obzenflow_contract_overrides_total".to_string(),
        "flow=\"flowip_059a_contracts_phase3\"".to_string(),
    ];
    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_contract_results_total")
            && text.contains("flow=\"flowip_059a_contracts_phase3\"")
    })
    .await?;

    // For every structurally attached contract, there must be a corresponding results series
    // with ID-first labels for joinability.
    for ((upstream, downstream), contracts) in contract_attachments.iter() {
        for contract in contracts {
            let upstream_label = format!("upstream_stage_id=\"{}\"", upstream);
            let downstream_label = format!("downstream_stage_id=\"{}\"", downstream);
            let contract_label = format!("contract=\"{}\"", contract);

            assert!(
                metrics_text.lines().any(|l| {
                    l.starts_with("obzenflow_contract_results_total{")
                        && l.contains("flow=\"flowip_059a_contracts_phase3\"")
                        && l.contains(&upstream_label)
                        && l.contains(&downstream_label)
                        && l.contains(&contract_label)
                }),
                "missing contract results series for {upstream_label} {downstream_label} {contract_label}"
            );
        }
    }

    Ok(())
}
