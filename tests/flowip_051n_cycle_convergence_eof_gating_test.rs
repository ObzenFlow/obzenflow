// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::CorrelationId;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_source, async_transform, flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncTransformHandler, FiniteSourceHandler, SinkHandler,
    TransformHandler,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
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

fn any_error_log_contains(run_dir: &Path, needle: &str) -> Result<bool> {
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
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains("_error_"))
        {
            continue;
        }

        let contents = fs::read_to_string(&path)?;
        if contents.contains(needle) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Debug)]
struct SingleSeedSource {
    emitted: bool,
    writer_id: WriterId,
    correlation_id: CorrelationId,
    target: u64,
}

impl SingleSeedSource {
    fn new(target: u64) -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
            correlation_id: CorrelationId::new(),
            target,
        }
    }
}

impl FiniteSourceHandler for SingleSeedSource {
    fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        let mut event = ChainEventFactory::data_event(
            self.writer_id,
            "test.seed",
            json!({ "depth": 0u64, "target": self.target }),
        );
        event.correlation_id = Some(self.correlation_id);
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct EntryConvergeTransform {
    writer_id: WriterId,
    processed_data: Arc<AtomicU64>,
}

impl EntryConvergeTransform {
    fn new(processed_data: Arc<AtomicU64>) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            processed_data,
        }
    }
}

#[async_trait]
impl TransformHandler for EntryConvergeTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        else {
            return Ok(Vec::new());
        };

        self.processed_data.fetch_add(1, Ordering::Relaxed);

        let depth = payload.get("depth").and_then(|v| v.as_u64()).unwrap_or(0);
        let target = payload.get("target").and_then(|v| v.as_u64()).unwrap_or(0);

        if depth >= target {
            Ok(vec![ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                "test.done",
                json!({ "depth": depth, "target": target }),
            )])
        } else if event_type == "test.seed" || event_type == "test.iter" {
            Ok(vec![ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                "test.iter",
                json!({ "depth": depth, "target": target }),
            )])
        } else {
            Ok(Vec::new())
        }
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct IterationTransform {
    writer_id: WriterId,
    processed_iterations: Arc<AtomicU64>,
    delay: Duration,
}

impl IterationTransform {
    fn new(processed_iterations: Arc<AtomicU64>, delay: Duration) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            processed_iterations,
            delay,
        }
    }
}

#[async_trait]
impl AsyncTransformHandler for IterationTransform {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        else {
            return Ok(Vec::new());
        };

        if event_type != "test.iter" {
            return Ok(Vec::new());
        }

        self.processed_iterations.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.delay).await;

        let depth = payload.get("depth").and_then(|v| v.as_u64()).unwrap_or(0);
        let target = payload.get("target").and_then(|v| v.as_u64()).unwrap_or(0);

        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            "test.iter",
            json!({ "depth": depth.saturating_add(1), "target": target }),
        )])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DoneCounterSink {
    done_events: Arc<AtomicU64>,
}

impl DoneCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let done_events = Arc::new(AtomicU64::new(0));
        (
            Self {
                done_events: done_events.clone(),
            },
            done_events,
        )
    }
}

#[async_trait]
impl SinkHandler for DoneCounterSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data { event_type, .. } = &event.content {
            if event_type == "test.done" {
                self.done_events.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(DeliveryPayload::success(
            "done_counter",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn flowip_051n_buffers_external_eof_until_scc_quiescent() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_051n_buffers_external_eof");

    let target_iterations = 5u64;
    let iter_delay = Duration::from_millis(200);

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();

    let handle = flow! {
        name: "flowip_051n_buffers_external_eof_until_scc_quiescent",
        journals: disk_journals(journal_root),
        middleware: [],

        stages: {
            src = source!(SingleSeedSource::new(target_iterations));
            entry = transform!(EntryConvergeTransform::new(entry_processed_for_flow));
            iter = async_transform!(IterationTransform::new(iter_processed_for_flow, iter_delay));
            snk = sink!(sink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct SeedThenDrainSource {
    state: u8,
    writer_id: WriterId,
    correlation_id: CorrelationId,
    target: u64,
    drain_delay: Duration,
}

impl SeedThenDrainSource {
    fn new(target: u64, drain_delay: Duration) -> Self {
        Self {
            state: 0,
            writer_id: WriterId::from(StageId::new()),
            correlation_id: CorrelationId::new(),
            target,
            drain_delay,
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for SeedThenDrainSource {
    async fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        match self.state {
            0 => {
                self.state = 1;
                let mut event = ChainEventFactory::data_event(
                    self.writer_id,
                    "test.seed",
                    json!({ "depth": 0u64, "target": self.target }),
                );
                event.correlation_id = Some(self.correlation_id);
                Ok(Some(vec![event]))
            }
            1 => {
                tokio::time::sleep(self.drain_delay).await;
                self.state = 2;
                Ok(Some(vec![ChainEventFactory::drain_event(self.writer_id)]))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct DualSeedSource {
    emitted: u8,
    writer_id: WriterId,
    converge_correlation_id: CorrelationId,
    diverge_correlation_id: CorrelationId,
    converge_target: u64,
    diverge_target: u64,
}

impl DualSeedSource {
    fn new(converge_target: u64, diverge_target: u64) -> Self {
        Self {
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
            converge_correlation_id: CorrelationId::new(),
            diverge_correlation_id: CorrelationId::new(),
            converge_target,
            diverge_target,
        }
    }
}

impl FiniteSourceHandler for DualSeedSource {
    fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        let (correlation_id, target) = match self.emitted {
            0 => (self.converge_correlation_id, self.converge_target),
            1 => (self.diverge_correlation_id, self.diverge_target),
            _ => return Ok(None),
        };
        self.emitted = self.emitted.saturating_add(1);

        let mut event = ChainEventFactory::data_event(
            self.writer_id,
            "test.seed",
            json!({ "depth": 0u64, "target": target }),
        );
        event.correlation_id = Some(correlation_id);
        Ok(Some(vec![event]))
    }
}

#[tokio::test]
async fn flowip_051n_buffers_drain_until_scc_quiescent() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_051n_buffers_drain");

    let target_iterations = 5u64;
    let iter_delay = Duration::from_millis(200);
    let drain_delay = Duration::from_millis(50);

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();

    let handle = flow! {
        name: "flowip_051n_buffers_drain_until_scc_quiescent",
        journals: disk_journals(journal_root),
        middleware: [],

        stages: {
            src = async_source!(SeedThenDrainSource::new(target_iterations, drain_delay));
            entry = transform!(EntryConvergeTransform::new(entry_processed_for_flow));
            iter = async_transform!(IterationTransform::new(iter_processed_for_flow, iter_delay));
            snk = sink!(sink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output even when drain arrives mid-cycle"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    Ok(())
}

#[tokio::test]
async fn flowip_051n_max_iterations_exceeded_routes_to_error_journal() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_051n_max_iterations_routing");
    let journal_root_for_flow = journal_root.clone();

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();

    // One correlation converges quickly; one correlation never converges and must be aborted
    // by CycleGuard max_iterations. Default max_iterations=30, so pick a target > 30.
    let converge_target = 2u64;
    let diverge_target = 1_000u64;

    let handle = flow! {
        name: "flowip_051n_max_iterations_exceeded_routes_to_error_journal",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!(DualSeedSource::new(converge_target, diverge_target));
            entry = transform!(EntryConvergeTransform::new(entry_processed_for_flow));
            iter = async_transform!(IterationTransform::new(iter_processed_for_flow, Duration::ZERO));
            snk = sink!(sink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected the converging correlation to produce one done event"
    );

    let run_dir = single_flow_run_dir(&journal_root)?;
    assert!(
        any_error_log_contains(&run_dir, "Cycle depth")?,
        "expected cycle guard abort to be routed to an error journal; run_dir={run_dir:?}"
    );

    // Sanity: the non-converging correlation should not loop forever.
    assert!(
        iter_processed.load(Ordering::Relaxed) >= converge_target,
        "expected at least the converging loop to execute"
    );
    assert!(
        entry_processed.load(Ordering::Relaxed) > converge_target,
        "expected entry to process multiple events before aborting the diverging loop"
    );

    Ok(())
}

#[tokio::test]
async fn flowip_051n_rejects_sccs_with_multiple_entry_points() {
    let result = flow! {
        name: "flowip_051n_reject_multi_entry_scc",
        journals: disk_journals(unique_journal_dir("flowip_051n_reject_multi_entry_scc")),
        middleware: [],

        stages: {
            src1 = source!(SingleSeedSource::new(1));
            src2 = source!(SingleSeedSource::new(1));
            a = transform!(EntryConvergeTransform::new(Arc::new(AtomicU64::new(0))));
            b = transform!(EntryConvergeTransform::new(Arc::new(AtomicU64::new(0))));
            snk = sink!(DoneCounterSink::new().0);
        },

        topology: {
            src1 |> a;
            src2 |> b;
            a |> b;
            a <| b;
            b |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => panic!("expected multi-entry SCC validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("must have exactly one entry point"),
        "error: {err}"
    );
}
