// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j at scale: a six-figure-event run records, replays, and
//! verifies. The comparison is a streaming walk, so memory stays bounded by
//! row size rather than run size; this suite is the executable witness that
//! the posture holds on real journals.

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const EVENTS: u64 = 100_000;
const BATCH: u64 = 500;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "replay_verification_scale.tick";
}

#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
    writer_id: WriterId,
}

impl Ticks {
    fn new() -> Self {
        Self {
            next: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for Ticks {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next >= EVENTS {
            return Ok(None);
        }
        let batch: Vec<ChainEvent> = (self.next..(self.next + BATCH).min(EVENTS))
            .map(|n| {
                ChainEventFactory::data_event(self.writer_id, Tick::EVENT_TYPE, json!(Tick { n }))
            })
            .collect();
        self.next += batch.len() as u64;
        Ok(Some(batch))
    }
}

fn discard<T>(
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "replay_verification_scale",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            ticks = source!(Tick => Ticks::new());
            out = sink!(Tick => SinkTyped::with_delivery(discard::<Tick>()));
        },

        topology: {
            ticks |> out;
        }
    }
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
async fn six_figure_run_verifies_with_streaming_comparison() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("live flow should complete");
    let baseline = latest_run_dir(&journal_base);

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("replay flow should complete");
    let candidate = latest_run_dir(&journal_base);

    let started = std::time::Instant::now();
    let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
        .expect("verification should run");
    let elapsed = started.elapsed();

    assert_eq!(
        outcome.exit_code(),
        0,
        "{}",
        obzenflow_infra::verify::render_verdict(&outcome)
    );
    let VerifyOutcome::Completed { report, .. } = &outcome else {
        panic!("expected a completed comparison");
    };
    assert_eq!(report.stages["ticks"].positional_rows_baseline, EVENTS);
    // Generous bound: the walk is linear and streaming; this guards against
    // an accidental quadratic or load-all regression, not micro-performance.
    assert!(
        elapsed < std::time::Duration::from_secs(60),
        "six-figure verification took {elapsed:?}"
    );
}
