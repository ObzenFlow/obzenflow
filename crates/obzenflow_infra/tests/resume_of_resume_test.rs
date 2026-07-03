// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: resume-of-resume closure.
//!
//! Records a linear infinite-source flow (R0), resumes it (R1), then resumes
//! the resumed archive (R2). R2's source journal must replay R0's prefix, the
//! re-admitted generation-1 watermark, R1's live tail, then author a
//! generation-2 watermark and continue live; R2's manifest records
//! `resume_generation` 2. A bounded `--replay-from` of R1 must re-admit the
//! generation-1 watermark in place, between the prefix and the tail. Every
//! assertion reads the event-sourced journals and manifests.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{
    ChainEventContent, EventEnvelope, ReplayLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::run_manifest::RunManifest;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, SystemId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, infinite_source, sink, transform, FlowDefinition};
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
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
    const EVENT_TYPE: &'static str = "resume_squared.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Doubled {
    n: u64,
    doubled: u64,
}

impl TypedPayload for Doubled {
    const EVENT_TYPE: &'static str = "resume_squared.doubled";
}

/// Emits `count` ticks starting at `first_n`, then idles forever. Under
/// resume the live handler is polled only after archive exhaustion, so each
/// resumed run's instance produces exactly its live tail.
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
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Counts consumed `Doubled` events. Catch-up deliveries are suppressed at
/// the sink under resume, so in a resumed run the counter witnesses exactly
/// the live tail.
#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if Doubled::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
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
        name: "resume_squared",
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

/// Run one flow instance: wait for Running, wait for the sink to consume
/// `expected` outputs, stop, wait for teardown. The archive seals Cancelled.
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
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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

/// Run a bounded replay to natural completion: the source terminates at
/// archive exhaustion and the flow drains itself.
async fn run_replay_to_completion(journal_base: &Path) -> Result<()> {
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(journal_base.to_path_buf(), 1, 0, delivered)
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("replay flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the bounded replay to drain"))??;
    Ok(())
}

/// A journal reduced to data payloads and catch-up watermarks, in physical
/// append order.
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
            }) => Some(ResumeRow::CatchUp {
                stage_key: stage_key.to_string(),
                generation: generation.0,
            }),
            _ => None,
        })
        .collect()
}

fn ticks(range: std::ops::RangeInclusive<u64>) -> Vec<ResumeRow> {
    range.map(|n| ResumeRow::Data(json!(Tick { n }))).collect()
}

fn doubles(range: std::ops::RangeInclusive<u64>) -> Vec<ResumeRow> {
    range
        .map(|n| ResumeRow::Data(json!(Doubled { n, doubled: n * 2 })))
        .collect()
}

fn catch_up(stage_key: &str, generation: u64) -> ResumeRow {
    ResumeRow::CatchUp {
        stage_key: stage_key.to_string(),
        generation,
    }
}

fn manifest(run_dir: &Path) -> RunManifest {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse")
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_of_resume_extends_the_prefix_at_generation_two() -> Result<()> {
    const R0_EVENTS: u64 = 4; // ticks 1..=4
    const R1_LIVE: u64 = 3; // ticks 5..=7
    const R2_LIVE: u64 = 2; // ticks 8..=9

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // R0: record and cancel.
    run_until_delivered(&journal_base, 1, R0_EVENTS, R0_EVENTS).await?;
    let r0 = replay_testkit::latest_run_dir(&journal_base);

    // R1: resume R0, produce a live tail, stop.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: r0.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        // The sink re-consumes the recorded prefix during catch-up (F14), so
        // the wait target is prefix + live; a live-only target races the stop
        // into the catch-up phase.
        run_until_delivered(&journal_base, R0_EVENTS + 1, R1_LIVE, R0_EVENTS + R1_LIVE).await?;
    }
    let r1 = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(r0, r1);
    assert_eq!(
        manifest(&r1).resume.as_ref().map(|r| r.resume_generation),
        Some(1),
        "the first resume enters generation 1"
    );

    // R2: resume R1.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: r1.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        run_until_delivered(
            &journal_base,
            R0_EVENTS + R1_LIVE + 1,
            R2_LIVE,
            R0_EVENTS + R1_LIVE + R2_LIVE,
        )
        .await?;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(r1, r2);

    // R2's source journal: R0 prefix, the re-admitted generation-1 watermark
    // in place, R1's live tail, the authored generation-2 watermark, then the
    // new live tail, in physical append order.
    let r2_src = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "src").await);
    let mut expected_src = ticks(1..=R0_EVENTS);
    expected_src.push(catch_up("src", 1));
    expected_src.extend(ticks(R0_EVENTS + 1..=R0_EVENTS + R1_LIVE));
    expected_src.push(catch_up("src", 2));
    expected_src.extend(ticks(
        R0_EVENTS + R1_LIVE + 1..=R0_EVENTS + R1_LIVE + R2_LIVE,
    ));
    assert_eq!(
        r2_src, expected_src,
        "R2 source journal must stack the generation boundaries in place"
    );

    // The transform re-authors its own boundary at each crossing.
    let r2_xform = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "xform").await);
    let mut expected_xform = doubles(1..=R0_EVENTS);
    expected_xform.push(catch_up("xform", 1));
    expected_xform.extend(doubles(R0_EVENTS + 1..=R0_EVENTS + R1_LIVE));
    expected_xform.push(catch_up("xform", 2));
    expected_xform.extend(doubles(
        R0_EVENTS + R1_LIVE + 1..=R0_EVENTS + R1_LIVE + R2_LIVE,
    ));
    assert_eq!(
        r2_xform, expected_xform,
        "R2 transform journal must re-author both generation boundaries"
    );

    // Delivered-order prefix stability at each depth.
    replay_testkit::assert_prefix_stable(&r0, &r1, "xform", &["src"]).await;
    replay_testkit::assert_prefix_stable(&r1, &r2, "xform", &["src"]).await;

    // R2's manifest names R1 and generation 2.
    let r2_manifest = manifest(&r2);
    let resume = r2_manifest
        .resume
        .as_ref()
        .expect("a resumed run records its resume config");
    assert_eq!(resume.resumed_from, r1);
    assert_eq!(resume.resume_generation, 2);

    // The system journal's resumed-live fact announces generation 2.
    let system_journal: DiskJournal<SystemEvent> = DiskJournal::with_owner(
        r2.join(&r2_manifest.system_journal_file),
        JournalOwner::system(SystemId::new()),
    )?;
    let system_events = system_journal.read_causally_ordered().await?;
    let generation = system_events
        .iter()
        .find_map(|envelope| match &envelope.event.event {
            SystemEventType::ReplayLifecycle(ReplayLifecycleEvent::ResumedLive {
                generation,
                ..
            }) => Some(*generation),
            _ => None,
        })
        .expect("R2's system journal must record system.replay.resumed_live");
    assert_eq!(generation, 2);

    // Bounded replay of R1 (RuntimeMode::Replay over the resumed archive):
    // the generation-1 watermark is re-admitted in place, between the prefix
    // and R1's live tail, and the run drains rather than continuing.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: r1.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Replay,
            })
            .await,
        );
        run_replay_to_completion(&journal_base).await?;
    }
    let replay_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(r2, replay_run);
    assert!(
        manifest(&replay_run).resume.is_none(),
        "a bounded replay records no resume config"
    );

    let replay_src =
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&replay_run, "src").await);
    let mut expected_replay_src = ticks(1..=R0_EVENTS);
    expected_replay_src.push(catch_up("src", 1));
    expected_replay_src.extend(ticks(R0_EVENTS + 1..=R0_EVENTS + R1_LIVE));
    assert_eq!(
        replay_src, expected_replay_src,
        "replaying R1 must re-admit the generation-1 watermark between prefix and tail"
    );

    // The bounded replay terminates: the source re-authors EOF after the tail.
    let replay_src_envelopes =
        replay_testkit::read_stage_envelopes_appended(&replay_run, "src").await;
    assert!(
        replay_src_envelopes.iter().any(|envelope| matches!(
            &envelope.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
        )),
        "a bounded replay of the resumed archive must drain and author EOF"
    );

    Ok(())
}
