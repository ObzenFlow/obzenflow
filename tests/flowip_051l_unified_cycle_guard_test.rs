// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::CorrelationId;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_dsl_infra::{async_source, flow, sink, source, stateful, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    AsyncFiniteSourceHandler, FiniteSourceHandler, SinkHandler, StatefulHandler, TransformHandler,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
struct TestEventSource {
    remaining: usize,
    writer_id: WriterId,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self {
            remaining: count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestEventSource {
    fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.remaining == 0 {
            return Ok(None);
        }

        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "test.event",
            json!({ "n": self.remaining }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CorrelatedEventSource {
    emitted: bool,
    writer_id: WriterId,
    correlation_id: CorrelationId,
    eof_delay: Duration,
}

impl CorrelatedEventSource {
    fn new(eof_delay: Duration) -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
            correlation_id: CorrelationId::new(),
            eof_delay,
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for CorrelatedEventSource {
    async fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if !self.emitted {
            self.emitted = true;
            let mut event = ChainEventFactory::data_event(self.writer_id, "test.event", json!({}));
            event.correlation_id = Some(self.correlation_id);
            return Ok(Some(vec![event]));
        }

        tokio::time::sleep(self.eof_delay).await;
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct EventCounterSink {
    count: Arc<AtomicU64>,
}

impl EventCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for EventCounterSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(DeliveryPayload::success(
            "counter_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct IdentityTransform;

#[async_trait]
impl TransformHandler for IdentityTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DropAllTransform;

#[async_trait]
impl TransformHandler for DropAllTransform {
    fn process(&self, _event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopStateful;

#[async_trait]
impl StatefulHandler for NoopStateful {
    type State = ();

    fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

    fn initial_state(&self) -> Self::State {}

    fn create_events(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

fn single_flow_run_dir(base: &Path) -> Result<PathBuf> {
    let flows_dir = base.join("flows");
    let mut dirs: Vec<PathBuf> = fs::read_dir(&flows_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();

    anyhow::ensure!(
        dirs.len() == 1,
        "expected exactly one flow run dir under {:?}, got {}",
        flows_dir,
        dirs.len()
    );

    Ok(dirs.pop().expect("dirs is non-empty"))
}

fn count_log_lines(run_dir: &Path) -> Result<usize> {
    let mut total = 0usize;
    for entry in fs::read_dir(run_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        if path.file_name().and_then(|n| n.to_str()) == Some("system.log") {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
            continue;
        }
        let contents = fs::read_to_string(&path)?;
        total = total.saturating_add(contents.lines().count());
    }
    Ok(total)
}

fn any_log_contains(run_dir: &Path, needle: &str) -> Result<bool> {
    for entry in fs::read_dir(run_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        if path.file_name().and_then(|n| n.to_str()) == Some("system.log") {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
            continue;
        }
        let contents = fs::read_to_string(&path)?;
        if contents.contains(needle) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[tokio::test]
async fn flowip_051l_rejects_cycles_with_non_transform_members() {
    let result = flow! {
        name: "flowip_051l_reject_stateful_cycle",
        journals: disk_journals(std::path::PathBuf::from("target/flowip_051l_reject_stateful_cycle")),
        middleware: [],

        stages: {
            src = source!("src" => TestEventSource::new(1));
            agg = stateful!("agg" => NoopStateful);
            tr = transform!("tr" => IdentityTransform);
            snk = sink!("snk" => EventCounterSink::new().0);
        },

        topology: {
            src |> agg;
            agg |> tr;
            agg <| tr;
            tr |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => panic!("expected cycle topology validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("Unsupported cycle topology"), "error: {err}");
    assert!(err.contains("agg (stateful)"), "error: {err}");
}

#[tokio::test]
async fn flowip_051l_cycle_guard_bounds_flow_signal_backflow() -> Result<()> {
    let base = PathBuf::from("target/flowip_051l_cycle_guard_bounds");
    let _ = fs::remove_dir_all(&base);
    let base_for_flow = base.clone();

    let (counter_sink, counter) = EventCounterSink::new();

    let handle = flow! {
        name: "flowip_051l_cycle_guard_bounds",
        journals: disk_journals(base_for_flow),
        middleware: [],

        stages: {
            src = source!("src" => TestEventSource::new(5));
            a = transform!("a" => IdentityTransform);
            b = transform!("b" => DropAllTransform);
            snk = sink!("snk" => counter_sink);
        },

        topology: {
            src |> a;
            a |> b;
            a <| b;
            b |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out (possible cycle amplification)"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "expected all data events to be dropped by b"
    );

    let run_dir = single_flow_run_dir(&base)?;
    let total_lines = count_log_lines(&run_dir)?;
    assert!(
        total_lines < 2000,
        "expected bounded journal growth; total log lines={total_lines}, run_dir={:?}",
        run_dir
    );

    Ok(())
}

#[tokio::test]
async fn flowip_051l_cycle_guard_bounds_data_backflow() -> Result<()> {
    let base = PathBuf::from("target/flowip_051l_cycle_guard_bounds_data");
    let _ = fs::remove_dir_all(&base);
    let base_for_flow = base.clone();

    let (counter_sink, counter) = EventCounterSink::new();

    let handle = flow! {
        name: "flowip_051l_cycle_guard_bounds_data",
        journals: disk_journals(base_for_flow),
        middleware: [],

        stages: {
            src = async_source!(
                "src" => CorrelatedEventSource::new(Duration::from_millis(500))
            );
            a = transform!("a" => IdentityTransform);
            b = transform!("b" => IdentityTransform);
            snk = sink!("snk" => counter_sink);
        },

        topology: {
            src |> a;
            a |> b;
            a <| b;
            b |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out (possible data cycle amplification)"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        counter.load(Ordering::Relaxed),
        10,
        "expected cycle guard to bound the data backflow iterations"
    );

    let run_dir = single_flow_run_dir(&base)?;
    let total_lines = count_log_lines(&run_dir)?;
    assert!(
        total_lines < 4000,
        "expected bounded journal growth; total log lines={total_lines}, run_dir={:?}",
        run_dir
    );

    assert!(
        any_log_contains(&run_dir, "Cycle limit exceeded")?,
        "expected to find cycle guard abort in logs; run_dir={:?}",
        run_dir
    );

    Ok(())
}
