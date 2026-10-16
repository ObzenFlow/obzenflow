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
//!
//! FLOWIP-134i B7, B12 and B13: each resume claims the run it continues, so
//! only the head of a history resumes, and resuming a replay resumes the run
//! it reproduces.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainPayload, JournalRecord, ReplayLifecycleEvent};
use obzenflow_core::journal::archive::manifest::RunManifest;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, infinite_source, sink, transform, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{
    read_run_lineage, EvidenceHolder, Rebuild, ResumedFrom, RunLineage, Standing,
};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedInfiniteSourceHandler,
    TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use replay_testkit::ReattachingSource;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
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

#[derive(Clone, Debug)]
struct DoubleTransform;

impl DoubleTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for DoubleTransform {
    type Input = Tick;
    type Output = Doubled;

    fn process(&self, tick: Tick) -> Result<Doubled, HandlerError> {
        Ok(Doubled {
            n: tick.n,
            doubled: tick.n * 2,
        })
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
impl InlineSink for CountingSink {
    type Input = Doubled;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: Doubled) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    first_n: u64,
    count: u64,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticker = ReattachingSource(BoundedTicker::new(first_n, count));
        let double = DoubleTransform::new();
        let counting_sink = CountingSink { delivered };

        Ok(flow! {
            name: "resume_squared",
            journals: disk_journals(journal_base),

            stages: {
                src = infinite_source!(Tick => ticker);
                xform = transform!(Tick -> Doubled => double);
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
    assert_eq!(resume.resumed_from, std::fs::canonicalize(&r1)?);
    assert_eq!(resume.resume_generation, 2);

    // The owning source's resumed-live fact announces generation 2.
    let source_reports = replay_testkit::read_stage_envelopes_appended(&r2, "src").await;
    let generation = source_reports
        .iter()
        .find_map(|envelope| match &envelope.payload {
            ChainPayload::Execution(ExecutionPayload::ReplayLifecycle(
                ReplayLifecycleEvent::ResumedLive { generation, .. },
            )) => Some(*generation),
            _ => None,
        })
        .expect("R2's source must record its resumed-live fact");
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
            &envelope.payload,
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
        )),
        "a bounded replay of the resumed archive must drain and author EOF"
    );

    Ok(())
}

fn bootstrap(archive: &Path, verb: ReplayVerb) -> ReplayBootstrap {
    ReplayBootstrap {
        archive_path: archive.to_path_buf(),
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb,
    }
}

/// Resume `archive`, whose history recorded ticks 1..=`recorded`, with
/// `live` new ticks. Returns the resumed run, canonically.
async fn resume(journal_base: &Path, archive: &Path, recorded: u64, live: u64) -> Result<PathBuf> {
    let _bootstrap = install_bootstrap_config(
        replay_testkit::bootstrap_with_archive(bootstrap(archive, ReplayVerb::Resume)).await,
    );
    run_until_delivered(journal_base, recorded + 1, live, recorded + live).await?;
    Ok(std::fs::canonicalize(replay_testkit::latest_run_dir(
        journal_base,
    ))?)
}

/// Replay `archive` to completion. Returns the replay run, canonically.
async fn replay(journal_base: &Path, archive: &Path) -> Result<PathBuf> {
    let _bootstrap = install_bootstrap_config(
        replay_testkit::bootstrap_with_archive(bootstrap(archive, ReplayVerb::Replay)).await,
    );
    run_replay_to_completion(journal_base).await?;
    Ok(std::fs::canonicalize(replay_testkit::latest_run_dir(
        journal_base,
    ))?)
}

async fn record(journal_base: &Path, count: u64) -> Result<PathBuf> {
    run_until_delivered(journal_base, 1, count, count).await?;
    Ok(std::fs::canonicalize(replay_testkit::latest_run_dir(
        journal_base,
    ))?)
}

/// The run `run`'s published continuation claim names, checked against that
/// run's own identity.
fn continued_by(run: &Path) -> Option<PathBuf> {
    let path = run.join("run_continuation.json");
    if !path.exists() {
        return None;
    }
    let claim: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(path).expect("a published claim is readable"),
    )
    .expect("a published claim parses");
    let location = PathBuf::from(
        claim["continued_by"]["location"]
            .as_str()
            .expect("the claim names a location"),
    );
    let successor: obzenflow_core::FlowId = manifest(&location)
        .flow_id
        .parse()
        .expect("the successor's manifest names its run");
    assert_eq!(
        claim["continued_by"]["run_id"],
        json!(successor),
        "the claim names its successor's run id"
    );
    Some(location)
}

async fn resume_refusal(archive: &Path) -> ReplayError {
    DiskReplayArchive::open_for_resume(archive.to_path_buf(), true)
        .await
        .err()
        .expect("the resume must refuse before any run starts")
}

fn assert_continued(refusal: ReplayError, archive: &Path, head: &Path) {
    match refusal {
        ReplayError::AlreadyContinued {
            archive: refused,
            head: named,
        } => assert_eq!((refused.as_path(), named.as_path()), (archive, head)),
        other => panic!("expected the head refusal: {other}"),
    }
}

/// Copy a run directory, claim included. Returns the copy, canonically.
fn copy_run(run: &Path, to: &Path) -> Result<PathBuf> {
    std::fs::create_dir(to)?;
    for entry in std::fs::read_dir(run)? {
        let entry = entry?;
        std::fs::copy(entry.path(), to.join(entry.file_name()))?;
    }
    Ok(std::fs::canonicalize(to)?)
}

/// FLOWIP-134i B7, B12: a resume claims the run it continues, so only the
/// head of a history resumes, wherever the archive is copied or resumed to.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_head_of_a_history_resumes() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record(&journal_base, 4).await?;
    let r1 = resume(&journal_base, &r0, 4, 3).await?;
    assert_eq!(continued_by(&r0), Some(r1.clone()));
    assert_eq!(continued_by(&r1), None);

    // A second resume of R0 into another base path refuses before any run
    // exists, naming the head.
    let elsewhere = temp.path().join("elsewhere");
    let refusal = FlowApplication::builder()
        .with_cli_args([
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            r0.clone().into_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            elsewhere.clone(),
            5,
            3,
            Arc::new(AtomicU64::new(0)),
        ))
        .await
        .expect_err("a continued archive refuses");
    assert_eq!(
        refusal.to_string(),
        format!(
            "cannot resume {}: it was already resumed by {}, the head of this history.\nResume \
             {} instead.",
            r0.display(),
            r1.display(),
            r1.display()
        )
    );
    assert!(!elsewhere.exists(), "the refusal leaves no run behind");

    // Each resume moves the head; every earlier run names the newest.
    let r2 = resume(&journal_base, &r1, 7, 2).await?;
    assert_eq!(continued_by(&r1), Some(r2.clone()));
    assert_continued(resume_refusal(&r0).await, &r0, &r2);

    // `show`'s lineage: every earlier run names the head and still holds the
    // evidence of the generation it ran (B16).
    let continued = Standing::Continued { head: r2.clone() };
    let holds = |run: &Path, generation| EvidenceHolder {
        run: run.to_path_buf(),
        generation: Some(generation),
        missing: false,
    };
    assert_eq!(
        read_run_lineage(&r0)?,
        RunLineage {
            standing: continued.clone(),
            ..RunLineage::default()
        }
    );
    assert_eq!(
        read_run_lineage(&r1)?,
        RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: r0.clone(),
                generation: 1,
            }),
            standing: continued,
            rebuild: Some(Rebuild::Complete),
            evidence: vec![holds(&r0, 0)],
        }
    );
    assert_eq!(
        read_run_lineage(&r2)?,
        RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: r1.clone(),
                generation: 2,
            }),
            standing: Standing::Head,
            rebuild: Some(Rebuild::Complete),
            evidence: vec![holds(&r1, 1), holds(&r0, 0)],
        }
    );

    // A copied claim travels with its archive; a copy of the head starts a
    // separate history and leaves the original head unclaimed.
    let r0_copy = copy_run(&r0, &temp.path().join("r0-copy"))?;
    assert_continued(resume_refusal(&r0_copy).await, &r0_copy, &r2);
    let r2_copy = copy_run(&r2, &temp.path().join("r2-copy"))?;
    let r3 = resume(&journal_base, &r2_copy, 9, 1).await?;
    assert_eq!(continued_by(&r2_copy), Some(r3.clone()));
    assert_eq!(continued_by(&r2), None);
    assert_eq!(
        manifest(&r3).resume.map(|resume| resume.resumed_from),
        Some(r2_copy)
    );
    Ok(())
}

/// FLOWIP-134i B12: two resumes racing to claim one archive cannot both
/// proceed; the loser is an orphan that ran no stage and names the winner.
#[tokio::test(flavor = "multi_thread")]
async fn racing_resumes_of_one_archive_cannot_both_proceed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record(&journal_base, 4).await?;

    let (head, orphan) = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(bootstrap(&r0, ReplayVerb::Resume)).await,
        );
        let racer = || {
            build_flow(journal_base.clone(), 5, 0, Arc::new(AtomicU64::new(0)))
                .build(FlowBuildContext::for_tests())
        };
        let (winner, loser) = match tokio::join!(racer(), racer()) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            (first, second) => panic!(
                "exactly one racer claims R0: {:?} / {:?}",
                first.err().map(|failure| failure.to_string()),
                second.err().map(|failure| failure.to_string())
            ),
        };
        let head = std::fs::canonicalize(
            winner
                .run_substrate()
                .locator()
                .expect("the winner is durable")
                .path(),
        )?;
        match &loser.error {
            FlowBuildError::ResumeRefusedClaim(ReplayError::AlreadyContinued {
                archive,
                head: named,
            }) => assert_eq!((archive, named), (&r0, &head)),
            other => panic!("the loser must name the winner: {other}"),
        }
        let orphan = std::fs::canonicalize(
            loser
                .run
                .as_ref()
                .and_then(|run| run.locator())
                .expect("the loser created its run")
                .path(),
        )?;
        winner.stop().await?;
        tokio::time::timeout(Duration::from_secs(10), winner.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the winner to stop"))??;
        (head, orphan)
    };
    assert_eq!(continued_by(&r0), Some(head.clone()));
    match resume_refusal(&orphan).await {
        ReplayError::ContinuedByAnother {
            archive,
            parent,
            head: named,
        } => assert_eq!((archive, parent, named), (orphan, r0, head)),
        other => panic!("the orphan must name the head: {other}"),
    }
    Ok(())
}

/// FLOWIP-134i B7: a run still being written refuses to resume until its
/// process stops; its journal writers' OS locks are the evidence. Infra only
/// probes liveness, and Core's still-running row refuses it, for both verbs,
/// before any claim read or writability probe (FLOWIP-134k B9).
#[tokio::test(flavor = "multi_thread")]
async fn a_run_still_being_written_refuses_until_its_process_stops() -> Result<()> {
    use obzenflow_core::journal::archive_actions::{
        decide, ArchiveOverrides, ArchiveRefusal, FlowEvidence, ReplayVerb,
    };
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(journal_base.clone(), 1, 4, delivered.clone())
        .build(FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, 4).await?;
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    let files_before: Vec<_> = std::fs::read_dir(&r0)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<_, _>>()?;
    for (verb, opened) in [
        (
            ReplayVerb::Resume,
            DiskReplayArchive::open_for_resume(r0.clone(), true).await?,
        ),
        (
            ReplayVerb::Replay,
            DiskReplayArchive::open(r0.clone(), true).await?,
        ),
    ] {
        let evidence = opened.evidence();
        assert!(evidence.live_writer, "the opener reports the live writer");
        let refusal = decide(
            verb,
            &r0,
            &evidence,
            &FlowEvidence::default(),
            ArchiveOverrides {
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: true,
            },
        )
        .expect_err("a live run must refuse");
        assert_eq!(
            refusal,
            ArchiveRefusal::StillRunning {
                verb,
                run: r0.clone()
            }
        );
    }
    let mut files_after: Vec<_> = std::fs::read_dir(&r0)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<_, _>>()?;
    let mut files_before = files_before;
    files_before.sort();
    files_after.sort();
    assert_eq!(
        files_before, files_after,
        "no probe file and no claim in the running run's location"
    );

    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    drop(handle);
    DiskReplayArchive::open_for_resume(r0.clone(), true).await?;
    assert_eq!(continued_by(&r0), None, "admission alone claims nothing");
    Ok(())
}

/// FLOWIP-134i B12: a successor that never published its claim, and the
/// staged claim it left, never block the parent; resuming the orphan names
/// the run to resume. A location that cannot hold a claim refuses.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn an_orphan_and_its_staged_claim_never_block_the_parent() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record(&journal_base, 4).await?;
    let set_mode = |mode| std::fs::set_permissions(&r0, std::fs::Permissions::from_mode(mode));

    set_mode(0o555)?;
    let unwritable = resume_refusal(&r0).await;
    set_mode(0o755)?;
    match unwritable {
        ReplayError::LocationNotWritable { archive } => assert_eq!(archive, r0),
        other => panic!("an unwritable parent must refuse: {other}"),
    }

    // Admission passes, then the parent stops accepting the claim.
    let orphan = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(bootstrap(&r0, ReplayVerb::Resume)).await,
        );
        set_mode(0o555)?;
        let failure = build_flow(journal_base.clone(), 5, 0, Arc::new(AtomicU64::new(0)))
            .build(FlowBuildContext::for_tests())
            .await
            .err();
        set_mode(0o755)?;
        let failure = failure.expect("an unwritable parent refuses the claim");
        assert!(
            matches!(
                &failure.error,
                FlowBuildError::ResumeRefusedClaim(ReplayError::LocationNotWritable { archive })
                    if archive == &r0
            ),
            "{}",
            failure.error
        );
        std::fs::canonicalize(
            failure
                .run
                .as_ref()
                .and_then(|run| run.locator())
                .expect("the orphan created its run")
                .path(),
        )?
    };
    assert_eq!(continued_by(&r0), None);
    // A kill between staging and publishing leaves the staged claim behind.
    let orphan_id: obzenflow_core::FlowId = manifest(&orphan).flow_id.parse()?;
    std::fs::write(
        r0.join(format!("run_continuation.json.{orphan_id}.staged")),
        serde_json::to_vec(&json!({
            "schema": manifest(&r0).journal_schema_version,
            "continued_by": { "run_id": orphan_id, "location": orphan },
        }))?,
    )?;

    match resume_refusal(&orphan).await {
        ReplayError::StoppedBeforeTakeover { archive, parent } => {
            assert_eq!((archive, parent), (orphan.clone(), r0.clone()))
        }
        other => panic!("the orphan must name its parent: {other}"),
    }
    let orphaned = |resume: &Path| Standing::Orphan {
        resume: resume.to_path_buf(),
    };
    assert_eq!(read_run_lineage(&orphan)?.standing, orphaned(&r0));
    let r1 = resume(&journal_base, &r0, 4, 1).await?;
    assert_eq!(continued_by(&r0), Some(r1.clone()));
    match resume_refusal(&orphan).await {
        ReplayError::ContinuedByAnother {
            archive,
            parent,
            head,
        } => assert_eq!((archive, parent, head), (orphan.clone(), r0, r1.clone())),
        other => panic!("the orphan must name the head: {other}"),
    }
    let lineage = read_run_lineage(&orphan)?;
    assert_eq!(
        (lineage.standing, lineage.rebuild, lineage.evidence),
        (orphaned(&r1), None, Vec::new())
    );
    Ok(())
}

/// FLOWIP-134i B13: resuming a replay resumes the run it reproduces, through
/// a chain of replays. Replays never claim, and strict replay neither reads
/// nor writes claims.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_replay_resumes_the_run_it_reproduces() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record(&journal_base, 4).await?;
    // An unclaimed copy starts a separate history to resume directly.
    let r0_copy = copy_run(&r0, &temp.path().join("r0-copy"))?;
    let replayed = replay(&journal_base, &r0).await?;
    let replayed_again = replay(&journal_base, &replayed).await?;

    let r1 = resume(&journal_base, &replayed_again, 4, 2).await?;
    assert_eq!(
        manifest(&r1).resume.map(|resume| resume.resumed_from),
        Some(r0.clone())
    );
    assert_eq!(continued_by(&r0), Some(r1.clone()));
    assert_eq!(
        (continued_by(&replayed), continued_by(&replayed_again)),
        (None, None)
    );

    // Resuming through the replays matches resuming the reproduced run.
    let direct = resume(&journal_base, &r0_copy, 4, 2).await?;
    for stage in ["src", "xform"] {
        assert_eq!(
            resume_rows(&replay_testkit::read_stage_envelopes_appended(&r1, stage).await),
            resume_rows(&replay_testkit::read_stage_envelopes_appended(&direct, stage).await),
            "{stage}"
        );
    }

    // A replay counts against the history it replays.
    assert_continued(resume_refusal(&replayed).await, &r0, &r1);
    let rehearsal = replay(&journal_base, &r0).await?;
    assert_eq!(continued_by(&r0), Some(r1.clone()));
    assert_eq!((continued_by(&r1), continued_by(&rehearsal)), (None, None));

    // `show` names what a replay reproduces; only the recorded run, not the
    // intermediate replay, holds evidence (B16).
    let replay_lineage = |missing: bool| RunLineage {
        standing: Standing::Replay {
            replayed: replayed.clone(),
        },
        evidence: vec![EvidenceHolder {
            run: r0.clone(),
            generation: (!missing).then_some(0),
            missing,
        }],
        ..RunLineage::default()
    };
    assert_eq!(read_run_lineage(&replayed_again)?, replay_lineage(false));

    // A replay whose recorded run is gone refuses, naming it.
    std::fs::remove_dir_all(&r0)?;
    assert_eq!(read_run_lineage(&replayed_again)?, replay_lineage(true));
    match resume_refusal(&replayed_again).await {
        ReplayError::ReplayedRunMissing {
            replay,
            replayed: missing,
        } => {
            assert_eq!((replay, missing), (replayed, r0))
        }
        other => panic!("a replay of a missing run must refuse: {other}"),
    }
    Ok(())
}
