// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-051j-1 / FLOWIP-051j-2 integration regression tests.
//!
//! Ensures stateful stages:
//! - record processing time during `accumulate()` (051j-1)
//! - count input events per-accumulate even when heartbeats are disabled (051j-2)

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{flow, join, sink, source, stateful, with_ref};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, JoinHandler, SinkHandler, StatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

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
            self.writer_id,
            "test.burst",
            json!({ "index": idx }),
        )]))
    }
}

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
}

#[derive(Clone, Debug)]
struct SlowAccumulator {
    writer_id: WriterId,
    sleep_per_event: Duration,
}

impl SlowAccumulator {
    fn new(sleep_per_event: Duration) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            sleep_per_event,
        }
    }
}

#[async_trait]
impl StatefulHandler for SlowAccumulator {
    type State = CounterState;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        std::thread::sleep(self.sleep_per_event);
        state.count = state.count.saturating_add(1);
    }

    fn initial_state(&self) -> Self::State {
        CounterState::default()
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "test.stateful.aggregate",
            json!({ "count": state.count }),
        )])
    }
}

#[derive(Clone, Debug)]
struct CollectingSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectingSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl SinkHandler for CollectingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<
        obzenflow_core::event::payloads::delivery_payload::DeliveryPayload,
        HandlerError,
    > {
        self.events.lock().unwrap().push(event);
        Ok(
            obzenflow_core::event::payloads::delivery_payload::DeliveryPayload::success(
                "test",
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                    "collect".to_string(),
                ),
                None,
            ),
        )
    }
}

#[derive(Clone, Debug, Default)]
struct NoopJoin;

#[async_trait]
impl JoinHandler for NoopJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {
        {}
    }

    fn process_event(
        &self,
        _state: &mut Self::State,
        _event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

async fn wait_for_metrics(
    exporter: &Arc<dyn MetricsExporter>,
    timeout: Duration,
    debug_patterns: &[String],
    predicate: impl Fn(&str) -> bool,
) -> Result<String> {
    let start = std::time::Instant::now();
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

fn metric_line_value(
    metrics_text: &str,
    metric_name: &str,
    required_substrings: &[String],
) -> Option<f64> {
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
async fn flowip_051j_stateful_metrics_accumulate_is_instrumented() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let total_events: usize = 20;
    let sleep_per_event = Duration::from_millis(5);
    let expected_processing_time_s = total_events as f64 * sleep_per_event.as_secs_f64();

    let (sink_handler, sink_events) = CollectingSink::new();
    let journal_dir = unique_journal_dir("flowip_051j_stateful_metrics");
    let journal_dir_for_flow = journal_dir.clone();

    let flow_handle = flow! {
        name: "flowip_051j_stateful_metrics",
        journals: disk_journals(journal_dir_for_flow.clone()),
        middleware: [],

        stages: {
            src = source!("src" => BurstSource::new(total_events));
            counter = stateful!("counter" => SlowAccumulator::new(sleep_per_event));
            snk = sink!("snk" => sink_handler);
        },

        topology: {
            src |> counter;
            counter |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let exporter = tokio::time::timeout(timeout_flow, flow_handle.run_with_metrics())
        .await
        .map_err(|_| anyhow!("flow did not complete within {timeout_flow:?} (timeout)"))?
        .map_err(|e| anyhow!("Failed to run flow: {e:?}"))?
        .ok_or_else(|| anyhow!("Metrics exporter was not configured"))?;

    let flow_label = "flow=\"flowip_051j_stateful_metrics\"".to_string();
    let stage_label = "stage=\"counter\"".to_string();
    let debug_patterns = vec![
        "obzenflow_events_total".to_string(),
        "obzenflow_events_accumulated_total".to_string(),
        "obzenflow_events_emitted_total".to_string(),
        "obzenflow_processing_time_seconds_sum".to_string(),
        flow_label.clone(),
        stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_events_total")
            && text.contains("obzenflow_events_accumulated_total")
            && text.contains("obzenflow_events_emitted_total")
            && text.contains("obzenflow_processing_time_seconds_sum")
            && text.contains(&stage_label)
    })
    .await?;

    let events_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_total for {stage_label}"))?
        as u64;

    assert_eq!(
        events_total, total_events as u64,
        "expected events_total to count input accumulations"
    );

    let accumulated_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_accumulated_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_accumulated_total for {stage_label}"))?
        as u64;

    assert_eq!(
        accumulated_total, total_events as u64,
        "expected events_accumulated_total to count input accumulations"
    );

    let emitted_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_emitted_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_emitted_total for {stage_label}"))?
        as u64;

    assert_eq!(
        emitted_total, 1,
        "expected stateful stage to emit a single aggregate at drain"
    );

    let sum_s = metric_line_value(
        &metrics_text,
        "obzenflow_processing_time_seconds_sum{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing processing_time_seconds_sum for {stage_label}"))?;

    // We slept in accumulate for deterministic non-zero work; expect at least half the wall-time.
    assert!(
        sum_s >= expected_processing_time_s * 0.5,
        "expected sum >= {:.3}s, got {sum_s}",
        expected_processing_time_s * 0.5
    );
    assert!(sum_s < 10.0, "expected sum < 10s, got {sum_s}");

    let events = sink_events.lock().unwrap();
    let aggregate_events: Vec<&ChainEvent> = events
        .iter()
        .filter(|e| e.is_data() && e.event_type() == "test.stateful.aggregate")
        .collect();
    assert_eq!(
        aggregate_events.len(),
        1,
        "expected sink to receive exactly one aggregate data event"
    );
    let event = aggregate_events[0];
    assert_eq!(
        event.writer_id,
        WriterId::from(event.flow_context.stage_id),
        "expected stateful aggregate output to be authored by the stateful stage"
    );

    // Ensure happened-before is preserved: the persisted aggregate event should
    // carry the upstream vector-clock entries via a parented append.
    let parent_vc = event
        .runtime_context
        .as_ref()
        .and_then(|ctx| ctx.last_consumed_vector_clock.clone())
        .ok_or_else(|| anyhow!("aggregate event missing last_consumed_vector_clock"))?;

    let stage_log = journal_dir
        .join("flows")
        .join(&event.flow_context.flow_id)
        .join(format!(
            "Stateful_counter_stage_{}.log",
            event.flow_context.stage_id.as_ulid()
        ));

    let file = std::fs::File::open(&stage_log)
        .map_err(|e| anyhow!("failed to open stage journal {}: {e}", stage_log.display()))?;
    let reader = std::io::BufReader::new(file);

    let mut aggregate_record: Option<
        obzenflow_infra::journal::disk::log_record::LogRecord<ChainEvent>,
    > = None;
    for line in reader.lines() {
        let line = line.map_err(|e| anyhow!("failed reading {}: {e}", stage_log.display()))?;
        let mut parts = line.splitn(3, ':');
        let _len = parts.next();
        let _crc = parts.next();
        let json = parts.next().ok_or_else(|| {
            anyhow!(
                "invalid journal line (missing json) in {}",
                stage_log.display()
            )
        })?;

        let record: obzenflow_infra::journal::disk::log_record::LogRecord<ChainEvent> =
            serde_json::from_str(json).map_err(|e| {
                anyhow!(
                    "failed to parse journal json in {}: {e}",
                    stage_log.display()
                )
            })?;

        if record.event.id == event.id {
            aggregate_record = Some(record);
            break;
        }
    }

    let aggregate_record = aggregate_record.ok_or_else(|| {
        anyhow!(
            "missing aggregate event {} in {}",
            event.id,
            stage_log.display()
        )
    })?;

    for (writer_key, parent_seq) in parent_vc.clocks.iter() {
        let seq = aggregate_record.vector_clock.get(writer_key);
        assert!(
            seq >= *parent_seq,
            "expected aggregate vector clock to include parent key {writer_key} at >= {parent_seq}, got {seq}"
        );
    }

    let stage_writer_key = WriterId::from(event.flow_context.stage_id).to_string();
    assert!(
        aggregate_record.vector_clock.get(&stage_writer_key) > 0,
        "expected aggregate vector clock to advance stage writer key {stage_writer_key}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_051j_join_metrics_counts_hydration_as_accumulation() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);
    let timeout_metrics = Duration::from_secs(10);

    let reference_events: usize = 10;
    let stream_events: usize = 0;

    let flow_handle = flow! {
        name: "flowip_051j_join_metrics",
        journals: disk_journals(unique_journal_dir("flowip_051j_join_metrics")),
        middleware: [],

        stages: {
            ref_src = source!("ref_src" => BurstSource::new(reference_events));
            stream_src = source!("stream_src" => BurstSource::new(stream_events));
            joiner = join!("joiner" => with_ref!(ref_src, NoopJoin));
            snk = sink!("snk" => CollectingSink::new().0);
        },

        topology: {
            stream_src |> joiner;
            joiner |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let exporter = tokio::time::timeout(timeout_flow, flow_handle.run_with_metrics())
        .await
        .map_err(|_| anyhow!("flow did not complete within {timeout_flow:?} (timeout)"))?
        .map_err(|e| anyhow!("Failed to run flow: {e:?}"))?
        .ok_or_else(|| anyhow!("Metrics exporter was not configured"))?;

    let flow_label = "flow=\"flowip_051j_join_metrics\"".to_string();
    let stage_label = "stage=\"joiner\"".to_string();
    let debug_patterns = vec![
        "obzenflow_events_total".to_string(),
        "obzenflow_events_accumulated_total".to_string(),
        "obzenflow_events_emitted_total".to_string(),
        flow_label.clone(),
        stage_label.clone(),
    ];

    let metrics_text = wait_for_metrics(&exporter, timeout_metrics, &debug_patterns, |text| {
        text.contains("obzenflow_events_total")
            && text.contains("obzenflow_events_accumulated_total")
            && text.contains("obzenflow_events_emitted_total")
            && text.contains(&stage_label)
    })
    .await?;

    let events_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_total for {stage_label}"))?
        as u64;

    assert_eq!(
        events_total, reference_events as u64,
        "expected events_total to count reference hydration inputs when stream has no events"
    );

    let accumulated_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_accumulated_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_accumulated_total for {stage_label}"))?
        as u64;

    assert_eq!(
        accumulated_total, reference_events as u64,
        "expected join hydration to increment events_accumulated_total per reference event"
    );

    let emitted_total = metric_line_value(
        &metrics_text,
        "obzenflow_events_emitted_total{",
        &[flow_label.clone(), stage_label.clone()],
    )
    .ok_or_else(|| anyhow!("missing obzenflow_events_emitted_total for {stage_label}"))?
        as u64;

    assert_eq!(
        emitted_total, 0,
        "expected no emitted events from noop join handler"
    );

    Ok(())
}
