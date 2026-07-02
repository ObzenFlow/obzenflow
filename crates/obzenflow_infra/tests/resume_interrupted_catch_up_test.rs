// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: resume of an archive whose catch-up was interrupted.
//!
//! Records a linear flow (R0), resumes it and cancels the resume while the
//! downstream transform is still mid-catch-up (R1), then resumes THAT archive
//! (R2). Harness delta from the FLOWIP's SIGINT framing: a precise
//! mid-catch-up SIGINT at the source is not schedulable from the test
//! harness, so the deterministic variant interrupts after the source authored
//! its generation-1 watermark (the source's re-admission is I/O-bound and
//! finishes first) but while a deliberately slow transform is still
//! re-processing the prefix. The source boundary was therefore crossed and
//! the archive legitimately records resume generation 1, so R2 enters
//! generation 2; the invariant asserted is monotonic prefix extension under
//! one resume lineage. The torn-before-watermark case (no watermark authored
//! at all) is the second test below.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::journal::run_manifest::RunManifest;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, infinite_source, sink, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
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
    const EVENT_TYPE: &'static str = "resume_interrupted.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Doubled {
    n: u64,
    doubled: u64,
}

impl TypedPayload for Doubled {
    const EVENT_TYPE: &'static str = "resume_interrupted.doubled";
}

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

/// 1:1 transform with a configurable per-event delay, so a resume's catch-up
/// can be interrupted while this stage is still re-processing the prefix.
#[derive(Clone, Debug)]
struct SlowDouble {
    writer_id: WriterId,
    delay_ms: u64,
}

impl SlowDouble {
    fn new(delay_ms: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            delay_ms,
        }
    }
}

#[async_trait]
impl TransformHandler for SlowDouble {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(tick) = Tick::from_event(&event) else {
            return Ok(Vec::new());
        };
        if self.delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.delay_ms));
        }
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
        Ok(DeliveryPayload::success(
            "resume_interrupted_sink",
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
    delay_ms: u64,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    flow! {
        name: "resume_interrupted",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src = infinite_source!(Tick => BoundedTicker::new(first_n, count));
            xform = transform!(Tick -> Doubled => SlowDouble::new(delay_ms));
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

async fn stop_and_wait(handle: FlowHandle) -> Result<()> {
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(())
}

/// Poll a stage journal until `predicate` holds over its rows.
async fn wait_for_journal<F>(
    run_dir: &Path,
    stage_key: &str,
    what: &str,
    predicate: F,
) -> Result<()>
where
    F: Fn(&[EventEnvelope<ChainEvent>]) -> bool,
{
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let envelopes = replay_testkit::read_stage_envelopes_appended(run_dir, stage_key).await;
            if predicate(&envelopes) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {what} in '{stage_key}' journal"))
}

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

fn has_watermark(envelopes: &[EventEnvelope<ChainEvent>]) -> bool {
    envelopes.iter().any(|envelope| {
        matches!(
            &envelope.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete { .. })
        )
    })
}

fn data_count(envelopes: &[EventEnvelope<ChainEvent>]) -> usize {
    envelopes
        .iter()
        .filter(|envelope| envelope.event.is_data())
        .count()
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

const RECORDED: u64 = 12; // ticks 1..=12
const R2_LIVE: u64 = 2; // ticks 13..=14
const SLOW_MS: u64 = 100; // per-event transform delay in the interrupted run

/// Record R0 and produce the interrupted resume R1: the source crossed its
/// boundary (watermark authored, no live tail), the slow transform was still
/// mid-catch-up at the cancel. Returns `(r0, r1)`.
async fn record_and_interrupt(journal_base: &Path) -> Result<(PathBuf, PathBuf)> {
    // R0: record and cancel (fast transform).
    {
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            1,
            RECORDED,
            0,
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        wait_for_count(&delivered, RECORDED).await?;
        stop_and_wait(handle).await?;
    }
    let r0 = replay_testkit::latest_run_dir(journal_base);

    // R1: resume R0 with a slow transform and no live production, cancel as
    // soon as the source's watermark lands while the transform (needing
    // RECORDED * SLOW_MS of work) is still re-processing the prefix.
    {
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: r0.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            0,
            SLOW_MS,
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("resume flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r1 = replay_testkit::latest_run_dir(journal_base);
        assert_ne!(r0, r1);
        wait_for_journal(&r1, "src", "the source's catch-up watermark", has_watermark).await?;
        stop_and_wait(handle).await?;
    }
    let r1 = replay_testkit::latest_run_dir(journal_base);

    // Interruption witnesses: the source finished its catch-up and authored
    // its boundary; the transform did not (no watermark, prefix incomplete).
    let r1_src = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r1, "src").await);
    let mut expected_src = ticks(1..=RECORDED);
    expected_src.push(catch_up("src", 1));
    assert_eq!(
        r1_src, expected_src,
        "R1's source journal: the full re-admitted prefix and the authored watermark"
    );
    let r1_xform = replay_testkit::read_stage_envelopes_appended(&r1, "xform").await;
    assert!(
        !has_watermark(&r1_xform),
        "the cancel must land before the transform crossed its boundary"
    );
    assert!(
        (data_count(&r1_xform) as u64) < RECORDED,
        "the transform must still be mid-catch-up at the cancel (saw {} of {RECORDED})",
        data_count(&r1_xform)
    );
    Ok((r0, r1))
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_an_interrupted_resume_extends_the_same_prefix() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let (r0, r1) = record_and_interrupt(&journal_base).await?;

    // R2: resume the interrupted archive, fast transform, two live events.
    // The sink's recorded high water in R1 is wherever the interrupt caught
    // the slow transform, so R2's sink counter over-counts the catch-up gap
    // nondeterministically; completion is awaited from the journal instead.
    {
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: r1.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            R2_LIVE,
            0,
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("second resume failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        assert_ne!(r1, r2);
        let live_tail_complete = |envelopes: &[EventEnvelope<ChainEvent>]| {
            resume_rows(envelopes)
                .iter()
                .filter(|row| {
                    matches!(row, ResumeRow::Data(payload)
                        if payload["n"].as_u64().is_some_and(|n| n > RECORDED))
                })
                .count() as u64
                == R2_LIVE
        };
        wait_for_journal(&r2, "xform", "the live tail", live_tail_complete).await?;
        stop_and_wait(handle).await?;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);

    // The source boundary was crossed in R1, so its archive legitimately
    // records generation 1 and R2 enters generation 2. The lineage chains
    // R2 -> R1 -> R0 in the manifests.
    let r2_manifest = manifest(&r2);
    let resume = r2_manifest
        .resume
        .as_ref()
        .expect("R2 records its resume config");
    assert_eq!(resume.resumed_from, r1);
    assert_eq!(resume.resume_generation, 2);
    let r1_manifest = manifest(&r1);
    let r1_resume = r1_manifest
        .resume
        .as_ref()
        .expect("R1 records its resume config");
    assert_eq!(r1_resume.resumed_from, r0);
    assert_eq!(r1_resume.resume_generation, 1);

    // R2's source journal: the original prefix unchanged, the re-admitted
    // generation-1 watermark, the authored generation-2 watermark (R1 had no
    // live tail, so the boundaries are adjacent), then the live tail.
    let r2_src = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "src").await);
    let mut expected_src = ticks(1..=RECORDED);
    expected_src.push(catch_up("src", 1));
    expected_src.push(catch_up("src", 2));
    expected_src.extend(ticks(RECORDED + 1..=RECORDED + R2_LIVE));
    assert_eq!(
        r2_src, expected_src,
        "R2 reconstructs the original prefix and continues live under stacked boundaries"
    );

    // The transform's catch-up completes in R2 even though R1's was torn: the
    // full prefix re-processes, both boundaries re-author, the live tail
    // follows.
    let r2_xform = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "xform").await);
    let mut expected_xform = doubles(1..=RECORDED);
    expected_xform.push(catch_up("xform", 1));
    expected_xform.push(catch_up("xform", 2));
    expected_xform.extend(doubles(RECORDED + 1..=RECORDED + R2_LIVE));
    assert_eq!(
        r2_xform, expected_xform,
        "R2's transform completes the catch-up R1 tore and continues live"
    );

    // Prefix fidelity across the lineage: R2's delivered prefix is R0's.
    let r0_xform = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r0, "xform").await);
    assert_eq!(
        &r2_xform[..RECORDED as usize],
        &r0_xform[..],
        "the recorded prefix is unchanged through both resumes"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_torn_catch_up_archive_stays_at_generation_one() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let (_r0, r1) = record_and_interrupt(&journal_base).await?;

    // Synthesize the torn-before-watermark archive: a SIGINT that lands
    // before source exhaustion leaves the re-admitted prefix (possibly
    // partial) and no watermark. Stripping the watermark row from R1's source
    // journal reproduces exactly that shape deterministically.
    let r1_manifest = manifest(&r1);
    let src_journal_file = &r1_manifest
        .stages
        .get("src")
        .expect("manifest names the src stage")
        .data_journal_file;
    let src_journal_path = r1.join(src_journal_file);
    let original = std::fs::read_to_string(&src_journal_path)?;
    let retained: String = original
        .lines()
        .filter(|line| !line.contains("catch_up_complete"))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_ne!(
        original, retained,
        "the watermark row must have been stripped"
    );
    std::fs::write(&src_journal_path, retained)?;

    // Resume the torn archive. No generation-1 boundary is recorded anywhere,
    // so the resume must enter generation 1 again and extend the prefix.
    {
        let _bootstrap = install_bootstrap_config(BootstrapConfig {
            replay: Some(ReplayBootstrap {
                archive_path: r1.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            }),
            ..BootstrapConfig::default()
        });
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            R2_LIVE,
            0,
            delivered.clone(),
        )
        .await
        .map_err(|e| anyhow!("resume of the torn archive failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        let live_tail_complete = |envelopes: &[EventEnvelope<ChainEvent>]| {
            resume_rows(envelopes)
                .iter()
                .filter(|row| {
                    matches!(row, ResumeRow::Data(payload)
                        if payload["n"].as_u64().is_some_and(|n| n > RECORDED))
                })
                .count() as u64
                == R2_LIVE
        };
        wait_for_journal(&r2, "xform", "the live tail", live_tail_complete).await?;
        stop_and_wait(handle).await?;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);

    // A torn catch-up left no boundary, so the retry enters generation 1
    // again: same prefix, one generation-1 watermark, live tail.
    assert_eq!(
        manifest(&r2).resume.as_ref().map(|r| r.resume_generation),
        Some(1),
        "no generation-1 boundary is recorded, so the resume generation stays 1"
    );
    let r2_src = resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "src").await);
    let mut expected_src = ticks(1..=RECORDED);
    expected_src.push(catch_up("src", 1));
    expected_src.extend(ticks(RECORDED + 1..=RECORDED + R2_LIVE));
    assert_eq!(
        r2_src, expected_src,
        "the torn archive resumes as a first-generation resume of the same prefix"
    );

    Ok(())
}
