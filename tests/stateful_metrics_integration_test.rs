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
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::TypedPayload;
use obzenflow_core::WriterId;
use obzenflow_dsl::{join, sink, source, stateful, test_flow};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, StatefulEmission, TypedFiniteSourceHandler, TypedJoinHandler,
    TypedStatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::testing::MetricsBarrier;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// File-local payload for the stateful-metrics flow. The JSON shape
/// matches what `BurstSource` emits; the type fingerprints the stage
/// contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MetricEvent {
    index: u64,
}

impl TypedPayload for MetricEvent {
    const EVENT_TYPE: &'static str = "metric.event";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AggregateMetricEvent {
    count: u64,
}

impl TypedPayload for AggregateMetricEvent {
    const EVENT_TYPE: &'static str = "metric.aggregate";
}

/// The join's two legs are semantically distinct typed inputs per
/// FLOWIP-114c, so we declare separate types even though the underlying
/// `BurstSource` produces the same shape on both legs in this test.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefMetricEvent {
    index: u64,
}

impl TypedPayload for RefMetricEvent {
    const EVENT_TYPE: &'static str = "metric.ref";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamMetricEvent {
    index: u64,
}

impl TypedPayload for StreamMetricEvent {
    const EVENT_TYPE: &'static str = "metric.stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JoinedMetricEvent {
    ref_index: u64,
    stream_index: u64,
}

impl TypedPayload for JoinedMetricEvent {
    const EVENT_TYPE: &'static str = "metric.joined";
}
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

trait IndexedMetric: TypedPayload {
    fn from_index(index: u64) -> Self;
}

impl IndexedMetric for MetricEvent {
    fn from_index(index: u64) -> Self {
        Self { index }
    }
}

impl IndexedMetric for RefMetricEvent {
    fn from_index(index: u64) -> Self {
        Self { index }
    }
}

impl IndexedMetric for StreamMetricEvent {
    fn from_index(index: u64) -> Self {
        Self { index }
    }
}

#[derive(Clone, Debug)]
struct BurstSource<T> {
    total: usize,
    current: usize,
    _output: PhantomData<fn() -> T>,
}

impl<T> BurstSource<T> {
    fn new(total: usize) -> Self {
        Self {
            total,
            current: 0,
            _output: PhantomData,
        }
    }
}

impl<T> TypedFiniteSourceHandler for BurstSource<T>
where
    T: IndexedMetric + Send + Sync + 'static,
{
    type Output = T;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.current >= self.total {
            return Ok(None);
        }

        let idx = self.current;
        self.current += 1;

        Ok(Some(vec![T::from_index(idx as u64)]))
    }
}

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
}

#[derive(Clone, Debug)]
struct SlowAccumulator {
    sleep_per_event: Duration,
}

impl SlowAccumulator {
    fn new(sleep_per_event: Duration) -> Self {
        Self { sleep_per_event }
    }
}

impl TypedStatefulHandler for SlowAccumulator {
    type State = CounterState;
    type Input = MetricEvent;
    type Output = AggregateMetricEvent;

    fn accumulate(&self, state: &mut Self::State, _event: MetricEvent) {
        std::thread::sleep(self.sleep_per_event);
        state.count = state.count.saturating_add(1);
    }

    fn initial_state(&self) -> Self::State {
        CounterState::default()
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: state.clone(),
            outputs: vec![AggregateMetricEvent { count: state.count }],
        })
    }
}

#[derive(Debug)]
struct CollectingSink<T> {
    events: Arc<Mutex<Vec<T>>>,
}

impl<T> Clone for CollectingSink<T> {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
        }
    }
}

impl<T> CollectingSink<T> {
    fn new() -> (Self, Arc<Mutex<Vec<T>>>) {
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
impl<T> InlineSink for CollectingSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        event: T,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.events.lock().unwrap().push(event);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "collect".to_string(),
            ),
            None,
        )))
    }
}

#[derive(Clone, Debug, Default)]
struct NoopJoin;

impl TypedJoinHandler for NoopJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefMetricEvent;
    type Stream = StreamMetricEvent;
    type Output = JoinedMetricEvent;

    fn initial_state(&self) -> Self::State {
        {}
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.index)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefMetricEvent>,
        _stream: StreamMetricEvent,
    ) -> std::result::Result<Vec<JoinedMetricEvent>, HandlerError> {
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
async fn stateful_metrics_accumulate_is_instrumented() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);

    let total_events: usize = 20;
    let sleep_per_event = Duration::from_millis(5);
    let expected_processing_time_s = total_events as f64 * sleep_per_event.as_secs_f64();

    let (sink_handler, sink_events) = CollectingSink::<AggregateMetricEvent>::new();
    let journal_dir = unique_journal_dir("stateful_metrics");
    let journal_dir_for_flow = journal_dir.clone();
    let source = BurstSource::<MetricEvent>::new(total_events);
    let accumulator = SlowAccumulator::new(sleep_per_event);

    let test_handle = test_flow! {
        name: "stateful_metrics",
        journals: disk_journals(journal_dir_for_flow.clone()),

        stages: {
            src = source!(MetricEvent => source);
            counter = stateful!(MetricEvent -> AggregateMetricEvent => accumulator);
            snk = sink!(AggregateMetricEvent => sink_handler);
        },

        topology: {
            src |> counter;
            counter |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let metrics_barrier = MetricsBarrier::try_on_flow(&test_handle)
        .await
        .map_err(|e| anyhow!("failed to construct MetricsBarrier: {e}"))?;

    let exporter = test_handle
        .metrics_exporter()
        .ok_or_else(|| anyhow!("Metrics exporter was not configured"))?;

    tokio::time::timeout(timeout_flow, test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("flow did not complete within {timeout_flow:?} (timeout)"))?
        .map_err(|e| anyhow!("Failed to run flow: {e:?}"))?;

    metrics_barrier
        .wait_for_drained()
        .await
        .map_err(|e| anyhow!("metrics subsystem did not drain: {e}"))?;

    let flow_label = "flow=\"stateful_metrics\"".to_string();
    let stage_label = "stage=\"counter\"".to_string();
    let metrics_text = exporter
        .render_metrics()
        .map_err(|e| anyhow!("Failed to render metrics: {e}"))?;

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
    assert_eq!(
        events.len(),
        1,
        "expected sink to receive exactly one aggregate data event"
    );
    drop(events);

    // Envelope authorship and vector clocks are runtime-owned metadata, so the
    // typed sink observes only the domain value and this proof reads metadata
    // from its authoritative stage journal.
    let flow_dir = std::fs::read_dir(journal_dir.join("flows"))?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .ok_or_else(|| anyhow!("missing flow journal directory"))?;
    let stage_log = std::fs::read_dir(&flow_dir)?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("Stateful_counter_stage_") && name.ends_with(".log")
                })
        })
        .ok_or_else(|| anyhow!("missing stateful counter journal in {}", flow_dir.display()))?;

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

        let frame: serde_json::Value = serde_json::from_str(json).map_err(|e| {
            anyhow!(
                "failed to parse journal frame in {}: {e}",
                stage_log.display()
            )
        })?;
        let record_values: Vec<serde_json::Value> =
            match frame.get("frame_kind").and_then(serde_json::Value::as_str) {
                Some("record_v2") => frame.get("record").cloned().into_iter().collect(),
                Some("atomic_group_v2") => frame
                    .get("records")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "atomic journal frame has no records in {}",
                            stage_log.display()
                        )
                    })?,
                Some(kind) => {
                    return Err(anyhow!(
                        "unknown journal frame kind '{kind}' in {}",
                        stage_log.display()
                    ));
                }
                None => {
                    return Err(anyhow!(
                        "journal frame has no frame_kind in {}",
                        stage_log.display()
                    ));
                }
            };

        for record_value in record_values {
            let record: obzenflow_infra::journal::disk::log_record::LogRecord<ChainEvent> =
                serde_json::from_value(record_value).map_err(|e| {
                    anyhow!(
                        "failed to parse journal record in {}: {e}",
                        stage_log.display()
                    )
                })?;

            if AggregateMetricEvent::from_event(&record.event).is_some() {
                aggregate_record = Some(record);
                break;
            }
        }
        if aggregate_record.is_some() {
            break;
        }
    }

    let aggregate_record = aggregate_record
        .ok_or_else(|| anyhow!("missing aggregate event in {}", stage_log.display()))?;
    let event = &aggregate_record.event;
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
async fn stateful_join_metrics_counts_hydration_as_accumulation() -> Result<()> {
    let timeout_flow = Duration::from_secs(30);

    let reference_events: usize = 10;
    let stream_events: usize = 0;
    let reference_source = BurstSource::<RefMetricEvent>::new(reference_events);
    let stream_source = BurstSource::<StreamMetricEvent>::new(stream_events);
    let joiner = NoopJoin;
    let (sink, _events) = CollectingSink::<JoinedMetricEvent>::new();

    let test_handle = test_flow! {
        name: "stateful_join_metrics",
        journals: disk_journals(unique_journal_dir("stateful_join_metrics")),

        stages: {
            ref_src = source!(RefMetricEvent => reference_source);
            stream_src = source!(StreamMetricEvent => stream_source);
            joiner = join!(catalog ref_src: RefMetricEvent, StreamMetricEvent -> JoinedMetricEvent => joiner);
            snk = sink!(JoinedMetricEvent => sink);
        },

        topology: {
            stream_src |> joiner;
            joiner |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Flow creation failed: {e:?}"))?;

    let metrics_barrier = MetricsBarrier::try_on_flow(&test_handle)
        .await
        .map_err(|e| anyhow!("failed to construct MetricsBarrier: {e}"))?;

    let exporter = test_handle
        .metrics_exporter()
        .ok_or_else(|| anyhow!("Metrics exporter was not configured"))?;

    tokio::time::timeout(timeout_flow, test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("flow did not complete within {timeout_flow:?} (timeout)"))?
        .map_err(|e| anyhow!("Failed to run flow: {e:?}"))?;

    metrics_barrier
        .wait_for_drained()
        .await
        .map_err(|e| anyhow!("metrics subsystem did not drain: {e}"))?;

    let flow_label = "flow=\"stateful_join_metrics\"".to_string();
    let stage_label = "stage=\"joiner\"".to_string();
    let metrics_text = exporter
        .render_metrics()
        .map_err(|e| anyhow!("Failed to render metrics: {e}"))?;

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
