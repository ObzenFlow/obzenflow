// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114o regression: an async source's rate-limiter throttle is a
//! cancellable future, so a stop/drain interrupts an in-flight wait instead of
//! blocking the worker thread until the permit is granted.
//!
//! The source is rate-limited so low that the second admission must wait several
//! seconds. We start the flow, let the source enter that wait, then stop the
//! flow and assert it terminates far sooner than the wait would have elapsed. On
//! the pre-FLOWIP-114o blocking `pre_handle` path the source poll has no await
//! point, so the supervisor's `biased` select could not pre-empt it and this
//! test would time out.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit_with_burst;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{async_source, flow, sink};
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncFiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DripEvent {
    index: u64,
}

impl TypedPayload for DripEvent {
    const EVENT_TYPE: &'static str = "source_rate_limiter_interruption.event";
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

/// An async finite source that emits one data event per poll, effectively
/// without end for the duration of the test (a large finite count). Each poll
/// returns immediately; the rate limiter is what introduces the wait.
#[derive(Clone, Debug)]
struct DripSource {
    emitted: u64,
    writer_id: WriterId,
}

impl DripSource {
    fn new() -> Self {
        Self {
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for DripSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= 10_000 {
            return Ok(None);
        }
        let event = ChainEventFactory::data_event(
            self.writer_id,
            DripEvent::versioned_event_type(),
            json!({ "index": self.emitted }),
        );
        self.emitted += 1;
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "noop_sink",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
    }
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries
        .pop()
        .expect("run should have produced a replay archive")
}

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse");
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain data journal for '{stage_key}'"));
    let journal: DiskJournal<ChainEvent> = DiskJournal::with_owner(
        run_dir.join(stage_journal),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("stage journal should read")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

fn delayed_rate_limiter_events(events: &[ChainEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                &event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::RateLimiter(RateLimiterEvent::Delayed { .. })
                ))
            )
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_source_rate_limit_wait_is_interrupted_by_stop() -> Result<()> {
    // burst 1, refill 0.2/s: the first poll admits immediately, the second must
    // wait ~5s for the next token. A graceful stop drains the source while it is
    // in that wait.
    let journal_base = unique_journal_dir("source_rate_limiter_interruption");
    let journal_base_for_flow = journal_base.clone();
    let handle = flow! {
        name: "source_rate_limiter_interruption",
        journals: disk_journals(journal_base_for_flow),
        middleware: [],

        stages: {
            src = async_source!(DripEvent => DripSource::new(), [
                rate_limit_with_burst(0.2, 1.0)
            ]);
            snk = sink!(DripEvent => NoopSink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create interruption flow: {e:?}"))?;

    wait_for_running(&handle).await?;

    // Let the first event admit and the second enter the rate-limiter wait.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A graceful stop drives `begin_drain` onto the source (FlowStopMode::Graceful
    // emits StopSources + BeginDrain). BeginDrain lands on the async source
    // supervisor's `external_events`, which its `biased` select races against the
    // in-flight `handler.next()`; the pending `source_admit` permit wait is
    // dropped. The graceful timeout is large so the drain completes via the
    // interrupt, not via escalation to Cancel.
    let system_journal = handle
        .system_journal()
        .ok_or_else(|| anyhow!("flow handle did not expose system journal"))?;

    handle
        .stop_graceful(Duration::from_secs(30))
        .await
        .map_err(|e| anyhow!("stop_graceful failed: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| {
            anyhow!(
                "flow did not terminate within 10s of graceful stop; the async source \
                 rate-limiter wait was not interrupted (FLOWIP-114o regression)"
            )
        })?
        .map_err(|e| anyhow!("flow completion failed: {e}"))?;

    // Canonical verification via the event-sourced journal. The terminal pipeline
    // lifecycle event carries the wide-event metrics. The second poll's
    // rate-limiter wait (~5s) was abandoned by the drain interrupt, so only the
    // first event was admitted: events_in_total == 1. On the pre-FLOWIP-114o
    // blocking path the in-flight wait runs to completion and a second event is
    // admitted before drain, so this would be >= 2.
    let tail = system_journal
        .read_last_n(64)
        .await
        .map_err(|e| anyhow!("read system journal: {e}"))?;

    let metrics = tail
        .iter()
        .find_map(|env| match &env.event.event {
            obzenflow_core::event::SystemEventType::PipelineLifecycle(ev) => match ev {
                obzenflow_core::event::PipelineLifecycleEvent::Cancelled { metrics, .. } => {
                    metrics.clone()
                }
                obzenflow_core::event::PipelineLifecycleEvent::Completed { metrics, .. } => {
                    Some(metrics.clone())
                }
                _ => None,
            },
            _ => None,
        })
        .ok_or_else(|| {
            anyhow!("no terminal pipeline lifecycle event with metrics found in system journal")
        })?;

    assert_eq!(
        metrics.events_in_total, 1,
        "expected exactly 1 admitted event (the second poll's rate-limit wait was interrupted by \
         drain); events_in_total={} means the in-flight async source wait was not cancelled",
        metrics.events_in_total
    );

    let run_dir = latest_run_dir(&journal_base);
    let source_events = read_stage_events(&run_dir, "src").await;
    let source_payload_indices: Vec<u64> = source_events
        .iter()
        .filter(|event| event.event_type() == DripEvent::versioned_event_type())
        .filter_map(|event| event.payload()["index"].as_u64())
        .collect();
    assert_eq!(
        source_payload_indices,
        vec![0],
        "the second finite source batch was polled but cancelled during after_poll pacing; \
         it must not be staged in the source data journal"
    );
    assert_eq!(
        delayed_rate_limiter_events(&source_events),
        0,
        "the delayed outbox event produced while holding the cancelled second batch must not be \
         staged in the source data journal"
    );

    Ok(())
}
