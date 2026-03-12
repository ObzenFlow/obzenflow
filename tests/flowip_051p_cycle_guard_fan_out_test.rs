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
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    PathBuf::from("target").join(format!("{prefix}_{suffix}"))
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
struct SingleSeedFanOutSource {
    emitted: bool,
    writer_id: WriterId,
    correlation_id: CorrelationId,
    fan_out: u64,
    target: u64,
}

impl SingleSeedFanOutSource {
    fn new(fan_out: u64, target: u64) -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
            correlation_id: CorrelationId::new(),
            fan_out,
            target,
        }
    }
}

impl FiniteSourceHandler for SingleSeedFanOutSource {
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
            json!({ "fan_out": self.fan_out, "target": self.target }),
        );
        event.correlation_id = Some(self.correlation_id);
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct FanOutEntryTransform {
    writer_id: WriterId,
    processed: Arc<AtomicU64>,
}

impl FanOutEntryTransform {
    fn new(processed: Arc<AtomicU64>) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            processed,
        }
    }
}

#[async_trait]
impl TransformHandler for FanOutEntryTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        else {
            return Ok(Vec::new());
        };

        self.processed.fetch_add(1, Ordering::Relaxed);

        if event_type == "test.seed" {
            let fan_out = payload.get("fan_out").and_then(|v| v.as_u64()).unwrap_or(0);
            let target = payload.get("target").and_then(|v| v.as_u64()).unwrap_or(0);

            let mut outputs = Vec::with_capacity(fan_out as usize);
            for item in 0..fan_out {
                outputs.push(ChainEventFactory::derived_data_event(
                    self.writer_id,
                    &event,
                    "test.iter",
                    json!({ "item": item, "iter": 0u64, "target": target }),
                ));
            }
            return Ok(outputs);
        }

        if event_type == "test.iter" {
            let item = payload.get("item").and_then(|v| v.as_u64()).unwrap_or(0);
            let iter = payload.get("iter").and_then(|v| v.as_u64()).unwrap_or(0);
            let target = payload.get("target").and_then(|v| v.as_u64()).unwrap_or(0);

            if iter >= target {
                return Ok(vec![ChainEventFactory::derived_data_event(
                    self.writer_id,
                    &event,
                    "test.done",
                    json!({ "item": item, "iter": iter, "target": target }),
                )]);
            }

            return Ok(vec![ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                "test.iter",
                json!({ "item": item, "iter": iter, "target": target }),
            )]);
        }

        Ok(Vec::new())
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PassThroughTransform;

#[async_trait]
impl TransformHandler for PassThroughTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct IterationTransform {
    writer_id: WriterId,
    processed: Arc<AtomicU64>,
}

impl IterationTransform {
    fn new(processed: Arc<AtomicU64>) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            processed,
        }
    }
}

#[async_trait]
impl TransformHandler for IterationTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
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

        self.processed.fetch_add(1, Ordering::Relaxed);

        let item = payload.get("item").and_then(|v| v.as_u64()).unwrap_or(0);
        let iter = payload.get("iter").and_then(|v| v.as_u64()).unwrap_or(0);
        let target = payload.get("target").and_then(|v| v.as_u64()).unwrap_or(0);

        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            "test.iter",
            json!({ "item": item, "iter": iter.saturating_add(1), "target": target }),
        )])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DoneCounterSink {
    done_count: Arc<AtomicU64>,
}

impl DoneCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let done_count = Arc::new(AtomicU64::new(0));
        (
            Self {
                done_count: done_count.clone(),
            },
            done_count,
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
                self.done_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(DeliveryPayload::success(
            "done_counter",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

/// Regression test for FLOWIP-051p: a fan-out at the SCC entry point must not
/// cause sibling conflation in the cycle guard.
///
/// This uses:
/// - fan_out = 10
/// - SCC size = 3 (entry -> pass -> iter -> entry)
/// - required round trips = 4
///
/// With the old correlation_id-keyed guard, each cycle member stage would
/// see 10 visits per round, hit max_iterations=30 at the start of round 4,
/// and abort some siblings before convergence.
#[tokio::test]
async fn flowip_051p_fan_out_siblings_converge_without_spurious_abort() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_051p_fan_out");
    let journal_root_for_flow = journal_root.clone();

    let fan_out = 10u64;
    let target_round_trips = 4u64;

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));

    let (sink, done_count) = DoneCounterSink::new();
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();

    let handle = flow! {
        name: "flowip_051p_fan_out_siblings_converge_without_spurious_abort",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!(SingleSeedFanOutSource::new(fan_out, target_round_trips));
            entry = transform!(FanOutEntryTransform::new(entry_processed_for_flow));
            pass = transform!(PassThroughTransform);
            iter = transform!(IterationTransform::new(iter_processed_for_flow));
            snk = sink!(sink);
        },

        topology: {
            src |> entry;
            entry |> pass;
            pass |> iter;
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
        fan_out,
        "expected every fan-out sibling to converge and emit a done event"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        fan_out.saturating_mul(target_round_trips),
        "expected iter stage to execute the full loop for each sibling"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        1u64.saturating_add(fan_out.saturating_mul(target_round_trips)),
        "expected entry stage to see seed + each returned iteration event"
    );

    let run_dir = single_flow_run_dir(&journal_root)?;
    assert!(
        !any_error_log_contains(&run_dir, "Cycle depth")?,
        "expected no cycle guard aborts; run_dir={run_dir:?}"
    );

    Ok(())
}
