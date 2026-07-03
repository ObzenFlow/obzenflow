// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j: cancelled baselines compare under prefix semantics, on real
//! journals.
//!
//! A killed run may not have processed every recorded source row downstream
//! and never wrote completion evidence, so a strict replay of its archive
//! legitimately carries surplus rows and EOF evidence the baseline lacks.
//! The verifier must assert equality over the recorded prefix, report the
//! surplus and completion evidence informationally, and still certify, which
//! also pins the killed-run tail shape (torn trailing writes are skipped at
//! the framed-record layer) that the FLOWIP flagged for fixture confirmation.

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "replay_verification_prefix.tick";
}

/// Emits four ticks, then idles without sealing, so the run only ends when
/// the test cancels it: the recorded archive is a `Cancelled` baseline. The
/// handler is irrelevant on replay, where the archive drives the source.
#[derive(Clone, Debug)]
struct StallingTicks {
    next: u64,
    writer_id: WriterId,
}

impl StallingTicks {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for StallingTicks {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 4 {
            std::thread::sleep(Duration::from_millis(10));
            return Ok(Some(vec![]));
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Tick::EVENT_TYPE,
            json!(Tick { n }),
        )]))
    }
}

fn counting<T>(
    counter: Arc<AtomicUsize>,
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| {
        counter.fetch_add(1, Ordering::SeqCst);
        std::future::ready(())
    }
}

macro_rules! prefix_flow {
    ($journal_base:expr, $delivered:expr) => {
        flow! {
            name: "replay_verification_prefix",
            journals: disk_journals($journal_base),
            middleware: [],

            stages: {
                ticks = source!(Tick => StallingTicks::new());
                out = sink!(Tick => SinkTyped::with_delivery(counting::<Tick>($delivered)).idempotent());
            },

            topology: {
                ticks |> out;
            }
        }
    };
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_baseline_verifies_under_prefix_semantics() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // Record the baseline and cancel it mid-flight once all four ticks have
    // been delivered, the SIGINT shape: a Cancelled archive with no
    // completion evidence. Driven directly through the FlowHandle, the same
    // pattern the stop-lifecycle suite uses.
    let delivered = Arc::new(AtomicUsize::new(0));
    let baseline_base = journal_base.clone();
    let baseline_counter = delivered.clone();
    let handle = prefix_flow!(baseline_base, baseline_counter)
        .await
        .expect("baseline flow should build");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while delivered.load(Ordering::SeqCst) < 4 {
        assert!(
            std::time::Instant::now() < deadline,
            "ticks were not delivered in time"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.stop().await.expect("stop should be accepted");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("cancelled run should terminate")
        .expect("cancelled run should resolve");
    let baseline = latest_run_dir(&journal_base);

    // Strict replay of the cancelled archive: re-admits the recorded ticks
    // and drains at archive exhaustion, completing cleanly.
    let replay_delivered = Arc::new(AtomicUsize::new(0));
    let replay_base = journal_base.clone();
    let replay_counter = replay_delivered.clone();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(prefix_flow!(replay_base, replay_counter))
        .await
        .expect("replay of the cancelled archive should complete");
    let candidate = latest_run_dir(&journal_base);
    assert_ne!(baseline, candidate);
    assert_eq!(
        replay_delivered.load(Ordering::SeqCst),
        4,
        "the replay re-admits exactly the recorded ticks"
    );

    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!(
            "expected a completed prefix comparison: {}",
            obzenflow_infra::verify::render_verdict(&outcome)
        );
    };
    assert_eq!(
        report.mode, "baseline_prefix_of_candidate",
        "a cancelled baseline selects prefix semantics"
    );
    assert_eq!(
        outcome.exit_code(),
        0,
        "the recorded prefix matches; surplus and completion evidence are informational: {}\n{}",
        obzenflow_infra::verify::render_verdict(&outcome),
        serde_json::to_string_pretty(&report.stages).unwrap_or_default()
    );
    assert_eq!(report.totals.divergences, 0);

    // Completion evidence asymmetry is reported, never as divergence.
    let informational: Vec<&String> = report
        .stages
        .values()
        .flat_map(|stage| stage.informational.iter())
        .collect();
    assert!(
        informational
            .iter()
            .any(|note| note.contains("completion evidence")),
        "the completion-evidence asymmetry is named informationally: {informational:?}"
    );
}
