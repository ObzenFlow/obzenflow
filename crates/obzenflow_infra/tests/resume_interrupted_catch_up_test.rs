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
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::vocabulary;
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::journal::archive::manifest::RunManifest;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, infinite_source, sink, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{
    read_run_lineage, EvidenceHolder, Rebuild, ResumedFrom, RunLineage, Standing,
};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedInfiniteSourceHandler,
    TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use replay_testkit::ReattachingSource;
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
    next_n: u64,
    remaining: u64,
}

impl BoundedTicker {
    fn new(first_n: u64, count: u64) -> Self {
        Self {
            next_n: first_n,
            remaining: count,
        }
    }
}

impl TypedInfiniteSourceHandler for BoundedTicker {
    type Output = Tick;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let n = self.next_n;
        self.next_n += 1;
        Ok(vec![Tick { n }])
    }
}

/// 1:1 transform with a configurable per-event delay, so a resume's catch-up
/// can be interrupted while this stage is still re-processing the prefix.
#[derive(Clone, Debug)]
struct SlowDouble {
    delay_ms: u64,
}

impl SlowDouble {
    fn new(delay_ms: u64) -> Self {
        Self { delay_ms }
    }
}

impl TypedTransformHandler for SlowDouble {
    type Input = Tick;
    type Output = Doubled;

    fn process(&self, tick: Tick) -> Result<Doubled, HandlerError> {
        if self.delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.delay_ms));
        }
        Ok(Doubled {
            n: tick.n,
            doubled: tick.n * 2,
        })
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = Doubled;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _event: Doubled) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    first_n: u64,
    count: u64,
    delay_ms: u64,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticker = ReattachingSource(BoundedTicker::new(first_n, count));
        let slow_double = SlowDouble::new(delay_ms);
        let counting_sink = CountingSink { delivered };

        Ok(flow! {
            name: "resume_interrupted",
            journals: disk_journals(journal_base),

            stages: {
                src = infinite_source!(Tick => ticker);
                xform = transform!(Tick -> Doubled => slow_double);
                snk = sink!(Doubled => counting_sink);
            },

            topology: {
                src |> xform;
                xform |> snk;
            }
        })
    })
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
    F: Fn(&[JournalRecord<ChainPayload>]) -> bool,
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

fn resume_rows(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<ResumeRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => {
                Some(ResumeRow::Data(envelope.payload().clone()))
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
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

fn has_watermark(envelopes: &[JournalRecord<ChainPayload>]) -> bool {
    envelopes.iter().any(|envelope| {
        matches!(
            &envelope.payload,
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete { .. })
        )
    })
}

fn data_count(envelopes: &[JournalRecord<ChainPayload>]) -> usize {
    envelopes
        .iter()
        .filter(|envelope| envelope.consumes_data_credit())
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

#[tokio::test(flavor = "multi_thread")]
async fn journal_probe_leaves_an_incomplete_tail_untouched() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(journal_base.clone(), 1, RECORDED, 0, delivered.clone())
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|error| anyhow!("flow failed to build: {error:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, RECORDED).await?;
    stop_and_wait(handle).await?;

    let run = replay_testkit::latest_run_dir(&journal_base);
    let journal = run.join(&manifest(&run).stages["src"].data_journal_file);
    let mut in_flight = std::fs::read(&journal)?;
    in_flight.pop().expect("source journal contains frames");
    // Model a writer paused just before the final commit byte is appended.
    std::fs::write(&journal, &in_flight)?;
    let records = replay_testkit::read_stage_envelopes_appended(&run, "src").await;
    assert!(!records.is_empty(), "the complete prefix remains readable");
    assert_eq!(
        std::fs::read(journal)?,
        in_flight,
        "an observational probe must not perform writer recovery"
    );
    Ok(())
}

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
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: r0.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            0,
            SLOW_MS,
            delivered.clone(),
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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

    let mut expected_xform = doubles(1..=RECORDED);
    expected_xform.push(catch_up("xform", 1));
    expected_xform.push(catch_up("xform", 2));
    expected_xform.extend(doubles(RECORDED + 1..=RECORDED + R2_LIVE));

    // R2: resume the interrupted archive, fast transform, two live events.
    // The sink's recorded high water in R1 is wherever the interrupt caught
    // the slow transform, so R2's sink counter over-counts the catch-up gap
    // nondeterministically. Await the complete transform sequence instead of
    // racing cancellation against catch-up boundary persistence.
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
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            R2_LIVE,
            0,
            delivered.clone(),
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("second resume failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        assert_ne!(r1, r2);
        let complete_resume =
            |envelopes: &[JournalRecord<ChainPayload>]| resume_rows(envelopes) == expected_xform;
        wait_for_journal(
            &r2,
            "xform",
            "the complete catch-up sequence and live tail",
            complete_resume,
        )
        .await?;
        stop_and_wait(handle).await?;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);

    // The source boundary was crossed in R1, so its archive legitimately
    // records generation 1 and R2 enters generation 2. The lineage chains
    // R2 -> R1 -> R0 in the manifests, canonically. R1's transform never
    // crossed, so R1 recorded no reconstruction completion and R2 merged R0
    // (FLOWIP-134i B6).
    let r2_manifest = manifest(&r2);
    let resume = r2_manifest
        .resume
        .as_ref()
        .expect("R2 records its resume config");
    assert_eq!(resume.resumed_from, std::fs::canonicalize(&r1)?);
    assert_eq!(resume.resume_generation, 2);
    assert_eq!(resume.merged_from, [std::fs::canonicalize(&r0)?]);
    let r1_manifest = manifest(&r1);
    let r1_resume = r1_manifest
        .resume
        .as_ref()
        .expect("R1 records its resume config");
    assert_eq!(r1_resume.resumed_from, std::fs::canonicalize(&r0)?);
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

    // A stop before exhaustion leaves a committed prefix without a watermark.
    // Truncate at that boundary, including subsequent lifecycle facts. Removing
    // only the watermark from the middle would leave broken predecessor links.
    let r1_manifest = manifest(&r1);
    let src_journal_file = &r1_manifest
        .stages
        .get("src")
        .expect("manifest names the src stage")
        .data_journal_file;
    let src_journal_path = r1.join(src_journal_file);
    let before = replay_testkit::read_stage_envelopes_appended(&r1, "src").await;
    let watermark_index = before
        .iter()
        .position(|record| record.event_type() == vocabulary::stream::CATCH_UP_COMPLETED)
        .expect("source crossed its recorded catch-up boundary");
    let reached_boundary = std::cell::Cell::new(false);
    let removed = obzenflow_infra::testing::journal::retain_archive_frames(&r1, |path, records| {
        if path != src_journal_path {
            return true;
        }
        if records.iter().any(|record| {
            record.pointer("/envelope/provenance/event/event_type")
                == Some(&json!(vocabulary::stream::CATCH_UP_COMPLETED))
        }) {
            reached_boundary.set(true);
        }
        !reached_boundary.get()
    })
    .map_err(anyhow::Error::from_boxed)?;
    assert_eq!(removed, before.len() - watermark_index);
    let retained = replay_testkit::read_stage_envelopes_appended(&r1, "src").await;
    assert_eq!(retained.len(), watermark_index);
    assert_eq!(resume_rows(&retained), ticks(1..=RECORDED));

    // Resume the torn archive. No generation-1 boundary is recorded anywhere,
    // so the resume must enter generation 1 again and extend the prefix.
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
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            R2_LIVE,
            0,
            delivered.clone(),
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("resume of the torn archive failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        let live_tail_complete = |envelopes: &[JournalRecord<ChainPayload>]| {
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

/// Tear R1's source mid-prefix, as a kill during re-admission would: keep its
/// first `kept` ticks and nothing after them.
async fn tear_source_after(r1: &Path, kept: usize) -> Result<()> {
    let src_journal = r1.join(&manifest(r1).stages["src"].data_journal_file);
    let seen = std::cell::Cell::new(0usize);
    obzenflow_infra::testing::journal::retain_archive_frames(r1, |path, records| {
        if path != src_journal {
            return true;
        }
        let facts = records
            .iter()
            .filter(|record| {
                record.pointer("/envelope/provenance/event/event_kind") == Some(&json!("fact"))
            })
            .count();
        seen.set(seen.get() + facts);
        seen.get() <= kept
    })
    .map_err(anyhow::Error::from_boxed)?;
    assert_eq!(
        resume_rows(&replay_testkit::read_stage_envelopes_appended(r1, "src").await),
        ticks(1..=kept as u64)
    );
    Ok(())
}

/// FLOWIP-134i B6: a resume torn before it recorded reconstruction completion
/// merges with the run it resumed from, so the next resume continues the full
/// prefix that run committed instead of forgetting it.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_torn_resume_continues_the_prefix_its_ancestor_committed() -> Result<()> {
    const KEPT: usize = 6;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (r0, r1) = record_and_interrupt(&journal_base).await?;
    tear_source_after(&r1, KEPT).await?;
    // `show` names the ancestor the torn head still requires (B6).
    let (r0, r1) = (std::fs::canonicalize(&r0)?, std::fs::canonicalize(&r1)?);
    assert_eq!(
        read_run_lineage(&r1)?,
        RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: r0.clone(),
                generation: 1,
            }),
            standing: Standing::Head,
            rebuild: Some(Rebuild::Incomplete {
                generation: 1,
                requires: vec![r0.clone()],
            }),
            evidence: Vec::new(),
        }
    );

    let mut expected_src = ticks(1..=RECORDED);
    expected_src.push(catch_up("src", 1));
    expected_src.extend(ticks(RECORDED + 1..=RECORDED + R2_LIVE));
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
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.to_path_buf(),
            RECORDED + 1,
            R2_LIVE,
            0,
            delivered.clone(),
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("resume of the torn resume failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        wait_for_journal(&r2, "src", "the merged prefix and live tail", |envelopes| {
            resume_rows(envelopes) == expected_src
        })
        .await?;
        stop_and_wait(handle).await?;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);

    // R1 re-admitted only six ticks, R0 committed twelve: R2 continues all of
    // them under one generation-1 boundary, then the live tail.
    let resume = manifest(&r2).resume.expect("R2 records its resume config");
    assert_eq!(resume.resumed_from, r1);
    assert_eq!(resume.merged_from, std::slice::from_ref(&r0));
    assert_eq!(resume.resume_generation, 1);
    // Once continued, the torn run requires nothing and names its head.
    assert_eq!(
        read_run_lineage(&r1)?,
        RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: r0.clone(),
                generation: 1,
            }),
            standing: Standing::Continued {
                head: std::fs::canonicalize(&r2)?,
            },
            rebuild: Some(Rebuild::Incomplete {
                generation: 1,
                requires: Vec::new(),
            }),
            evidence: vec![EvidenceHolder {
                run: r0,
                generation: Some(0),
                missing: false,
            }],
        }
    );
    assert_eq!(
        resume_rows(&replay_testkit::read_stage_envelopes_appended(&r2, "src").await),
        expected_src
    );
    Ok(())
}

/// FLOWIP-134i B6: a torn resume whose ancestor is gone, or disagrees with
/// it, refuses before any run starts, naming the ancestor.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_resume_without_an_agreeing_ancestor_refuses() -> Result<()> {
    use obzenflow_core::journal::archive::ReplayError;
    use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let (r0, r1) = record_and_interrupt(&journal_base).await?;
    tear_source_after(&r1, 6).await?;
    let torn = std::fs::canonicalize(&r1)?;

    // An unrelated recording of the same flow commits different event ids.
    {
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(journal_base.clone(), 1, RECORDED, 0, delivered.clone())
            .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
            .await
            .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        wait_for_count(&delivered, RECORDED).await?;
        stop_and_wait(handle).await?;
    }
    let stranger = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    let manifest_path = r1.join("run_manifest.json");
    let original = std::fs::read_to_string(&manifest_path)?;
    let mut rewritten: serde_json::Value = serde_json::from_str(&original)?;
    rewritten["resume"]["resumed_from"] = json!(stranger);
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&rewritten)?)?;
    // The stranger carries R0's claim, so R1's link to it is claim-backed.
    std::fs::copy(
        r0.join("run_continuation.json"),
        stranger.join("run_continuation.json"),
    )?;
    // FLOWIP-134k B8: a fresh recording mints in its own namespace, so the
    // merge refuses before comparing any stage.
    match DiskReplayArchive::open_for_resume(r1.clone(), true).await {
        Err(ReplayError::NamespaceDisagrees { archive, ancestor }) => {
            assert_eq!((archive, ancestor), (torn.clone(), stranger.clone()));
        }
        Err(other) => panic!("expected the stranger's namespace to refuse: {other}"),
        Ok(_) => panic!("a stranger's namespace must refuse the resume"),
    }
    // A member written before B8 records no namespace and is not compared,
    // so the stranger still disagrees on its events.
    let stranger_manifest = stranger.join("run_manifest.json");
    let mut unrecorded: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&stranger_manifest)?)?;
    unrecorded
        .as_object_mut()
        .expect("a manifest is an object")
        .remove("recorded_flow_id")
        .expect("a fresh run records its namespace");
    std::fs::write(
        &stranger_manifest,
        serde_json::to_string_pretty(&unrecorded)?,
    )?;
    match DiskReplayArchive::open_for_resume(r1.clone(), true).await {
        Err(ReplayError::AncestorDisagrees {
            archive,
            ancestor,
            stage_key,
        }) => {
            assert_eq!((archive, ancestor), (torn.clone(), stranger));
            assert_eq!(stage_key, "src");
        }
        Err(other) => panic!("expected the disagreeing ancestor to refuse: {other}"),
        Ok(_) => panic!("a disagreeing ancestor must refuse the resume"),
    }

    std::fs::write(&manifest_path, original)?;
    let ancestor = std::fs::canonicalize(&r0)?;
    std::fs::remove_dir_all(&r0)?;
    let refusal = DiskReplayArchive::open_for_resume(r1.clone(), true)
        .await
        .err()
        .expect("a torn resume needs the archive it resumed from");
    let message = refusal.to_string();
    assert!(
        matches!(
            &refusal,
            ReplayError::AncestorMissing { archive, ancestor: missing }
                if archive == &torn && missing == &ancestor
        ),
        "{message}"
    );
    assert!(message.contains(&format!("Restore {}", ancestor.display())));
    Ok(())
}
