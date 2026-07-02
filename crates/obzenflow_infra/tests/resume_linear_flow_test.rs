// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-G: first linear-flow resume integration test.
//!
//! Records a linear infinite-source flow (source -> transform -> sink),
//! cancels it once the sink has consumed every recorded output, then resumes
//! the archive with `ReplayVerb::Resume` while the live source handler
//! produces new events after archive exhaustion. Every assertion reads the
//! event-sourced journals of the resumed run: the re-admitted recorded
//! prefix, the authored catch-up watermarks, the
//! `system.replay.resumed_live` fact, the manifest resume config, and the
//! live tail.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::event::{
    ChainEventContent, EventEnvelope, ReplayLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::run_manifest::RunManifest;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, SystemId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, infinite_source, sink, transform, FlowDefinition};
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InfiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "resume_linear.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Doubled {
    n: u64,
    doubled: u64,
}

impl TypedPayload for Doubled {
    const EVENT_TYPE: &'static str = "resume_linear.doubled";
}

/// An infinite source that emits `count` ticks starting at `first_n`, then
/// idles forever; the test stops the flow externally. Under resume the live
/// handler is only polled after archive exhaustion, so the resumed run's
/// instance produces exactly the live tail.
#[derive(Clone, Debug)]
struct BoundedTicker {
    writer_id: WriterId,
    next_n: u64,
    remaining: u64,
}

impl BoundedTicker {
    fn new(first_n: u64, count: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            next_n: first_n,
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for BoundedTicker {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let n = self.next_n;
        self.next_n += 1;
        Ok(vec![Tick { n }.to_event(self.writer_id)])
    }
}

/// Pure 1:1 transform: one `Doubled` per consumed tick, so the delivered
/// order is fully witnessed by the transform's output journal.
#[derive(Clone, Debug)]
struct DoubleTransform {
    writer_id: WriterId,
}

impl DoubleTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for DoubleTransform {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(tick) = Tick::from_event(&event) else {
            return Ok(Vec::new());
        };
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            Doubled::EVENT_TYPE,
            json!(Doubled {
                n: tick.n,
                doubled: tick.n * 2,
            }),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Counts consumed `Doubled` events. Catch-up deliveries are reconstruction
/// scoped and never reach the sink handler, so in the resumed run the counter
/// witnesses exactly the live tail.
#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if event.event_type() == Doubled::EVENT_TYPE {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(
            "resume_linear_sink",
            DeliveryMethod::Noop,
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_flow(
    journal_base: PathBuf,
    first_n: u64,
    count: u64,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    flow! {
        name: "resume_linear",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src = infinite_source!(Tick => BoundedTicker::new(first_n, count));
            xform = transform!(Tick -> Doubled => DoubleTransform::new());
            snk = sink!(Doubled => CountingSink { delivered });
        },

        topology: {
            src |> xform;
            xform |> snk;
        }
    }
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
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

async fn wait_for_count(counter: &Arc<AtomicU64>, target: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timeout waiting for the sink to consume {target} events (saw {})",
            counter.load(Ordering::SeqCst)
        )
    })
}

/// Run one flow instance to completion: wait for Running, wait for the sink
/// to consume `expected` outputs, stop, and wait for teardown.
async fn run_until_delivered(
    journal_base: &Path,
    first_n: u64,
    count: u64,
    expected: u64,
) -> Result<()> {
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        first_n,
        count,
        delivered.clone(),
    )
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, expected).await?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(())
}

/// A journal's rows reduced to the resume-relevant kinds, in physical append
/// order: data payloads and authored catch-up watermarks.
#[derive(Debug, Clone, PartialEq)]
enum ResumeRow {
    Data(serde_json::Value),
    CatchUp { stage_key: String, generation: u64 },
}

fn resume_rows(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<ResumeRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { .. } => {
                Some(ResumeRow::Data(envelope.event.payload().clone()))
            }
            ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            }) => {
                assert_eq!(
                    envelope.event.event_type(),
                    "control.catch_up_complete",
                    "the watermark row must carry the catch-up event type"
                );
                Some(ResumeRow::CatchUp {
                    stage_key: stage_key.to_string(),
                    generation: generation.0,
                })
            }
            _ => None,
        })
        .collect()
}

fn data_payloads(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<serde_json::Value> {
    envelopes
        .iter()
        .filter(|envelope| envelope.event.is_data())
        .map(|envelope| envelope.event.payload().clone())
        .collect()
}

/// Assert a resumed stage journal is exactly: the recorded data prefix, one
/// authored catch-up watermark naming this stage, then the live tail.
fn assert_prefix_watermark_tail(
    rows: &[ResumeRow],
    recorded_prefix: &[serde_json::Value],
    stage_key: &str,
    live_tail: &[serde_json::Value],
) {
    assert_eq!(
        rows.len(),
        recorded_prefix.len() + 1 + live_tail.len(),
        "stage '{stage_key}': expected prefix + watermark + live tail, got {rows:#?}"
    );
    for (index, expected) in recorded_prefix.iter().enumerate() {
        assert_eq!(
            rows[index],
            ResumeRow::Data(expected.clone()),
            "stage '{stage_key}': row {index} must repeat the recorded prefix"
        );
    }
    assert_eq!(
        rows[recorded_prefix.len()],
        ResumeRow::CatchUp {
            stage_key: stage_key.to_string(),
            generation: 1,
        },
        "stage '{stage_key}': one authored generation-1 watermark follows the prefix"
    );
    for (offset, expected) in live_tail.iter().enumerate() {
        let index = recorded_prefix.len() + 1 + offset;
        assert_eq!(
            rows[index],
            ResumeRow::Data(expected.clone()),
            "stage '{stage_key}': row {index} must be live-tail data"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_linear_flow_replays_prefix_then_continues_live() -> Result<()> {
    const RECORDED: u64 = 4;
    const LIVE: u64 = 3;

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record: run the flow live, cancel once the sink consumed everything.
    // The archive seals as Cancelled, the common resume input.
    run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    let recorded_src_prefix =
        data_payloads(&replay_testkit::read_stage_envelopes_appended(&recorded_run, "src").await);
    let recorded_xform_prefix =
        data_payloads(&replay_testkit::read_stage_envelopes_appended(&recorded_run, "xform").await);
    assert_eq!(recorded_src_prefix.len() as u64, RECORDED);
    assert_eq!(recorded_xform_prefix.len() as u64, RECORDED);

    // Resume: same flow shape, live handler produces new events after archive
    // exhaustion. The bootstrap guard scopes the Resume verb to this run.
    {
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
        // Catch-up deliveries are reconstruction-scoped and suppressed at the
        // sink, so the counter reaching LIVE proves the source crossed its
        // boundary and the flow accepted new input after catch-up.
        run_until_delivered(&journal_base, RECORDED + 1, LIVE, LIVE).await?;
    }

    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(
        recorded_run, resumed_run,
        "the resume must produce a new run directory"
    );

    let live_src_tail: Vec<serde_json::Value> = (RECORDED + 1..=RECORDED + LIVE)
        .map(|n| json!(Tick { n }))
        .collect();
    let live_xform_tail: Vec<serde_json::Value> = (RECORDED + 1..=RECORDED + LIVE)
        .map(|n| json!(Doubled { n, doubled: n * 2 }))
        .collect();

    // Source journal: recorded prefix, then the authored watermark, then the
    // live tail, in physical append order.
    let resumed_src = replay_testkit::read_stage_envelopes_appended(&resumed_run, "src").await;
    assert_prefix_watermark_tail(
        &resume_rows(&resumed_src),
        &recorded_src_prefix,
        "src",
        &live_src_tail,
    );

    // Transform journal: the re-processed prefix outputs, then its own
    // re-authored watermark, then the live-tail outputs.
    let resumed_xform = replay_testkit::read_stage_envelopes_appended(&resumed_run, "xform").await;
    assert_prefix_watermark_tail(
        &resume_rows(&resumed_xform),
        &recorded_xform_prefix,
        "xform",
        &live_xform_tail,
    );

    // Delivered-order prefix stability at the transform (FLOWIP-120n): the
    // resumed run's delivered order begins with the recorded run's order.
    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "xform", &["src"]).await;

    // The resumed run's manifest names the archive and the entered generation.
    let manifest: RunManifest = serde_json::from_str(&std::fs::read_to_string(
        resumed_run.join("run_manifest.json"),
    )?)?;
    let resume = manifest
        .resume
        .as_ref()
        .expect("a resumed run records its resume config in the manifest");
    assert_eq!(resume.resumed_from, recorded_run);
    assert_eq!(resume.resume_generation, 1);

    // The system journal carries the resumed-live lifecycle fact.
    let system_journal: DiskJournal<SystemEvent> = DiskJournal::with_owner(
        resumed_run.join(&manifest.system_journal_file),
        JournalOwner::system(SystemId::new()),
    )?;
    let system_events = system_journal.read_causally_ordered().await?;
    let resumed_live = system_events
        .iter()
        .find(|envelope| {
            matches!(
                &envelope.event.event,
                SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::ResumedLive { .. })
            )
        })
        .expect("the resumed run's system journal must record system.replay.resumed_live");
    assert_eq!(
        resumed_live.event.event_type_name(),
        "system.replay.resumed_live"
    );
    if let SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::ResumedLive {
        replayed_count,
        generation,
        ..
    }) = &resumed_live.event.event
    {
        assert_eq!(replayed_count.0, RECORDED);
        assert_eq!(*generation, 1);
    }

    Ok(())
}
