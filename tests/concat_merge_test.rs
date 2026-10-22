// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::application::FlowApplication;
use obzenflow::flow::{flow, merge, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::rate_limit;
use obzenflow::stages::merges;
use obzenflow::stages::sinks::{
    DeliveryMethod, InlineSink, SinkDescription, SinkRedeliverySafety, SinkWriteFailure,
};
use obzenflow::stages::sources::ValuesSource;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

static CONCAT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Note {
    note: String,
}
impl TypedPayload for Note {
    const EVENT_TYPE: &'static str = "concat_test.note";
}

fn notes(prefix: &str, count: usize) -> Vec<Note> {
    (1..=count)
        .map(|n| Note {
            note: format!("{prefix}-{n}"),
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct Collect {
    notes: Arc<Mutex<Vec<String>>>,
    arrived: Arc<Notify>,
}

#[async_trait]
impl InlineSink for Collect {
    type Input = Note;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, note: Note) -> Result<(), SinkWriteFailure> {
        self.notes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(note.note);
        self.arrived.notify_one();
        Ok(())
    }
}

#[derive(Clone)]
struct Inputs {
    first: usize,
    last: usize,
    first_rate: f64,
    enforce: Option<(u64, u64)>,
}

fn build(journal_base: PathBuf, inputs: Inputs, sink: Collect) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let first_source = ValuesSource::new(notes("first", inputs.first));
        let last_source = ValuesSource::new(notes("last", inputs.last));
        let collector = sink.clone();
        let rate = inputs.first_rate;
        let journals = journal_base.clone();

        Ok(flow! {
            name: "concat_merge_test",
            journals: disk_journals(journals),

            stages: {
                early = source!(Note with rate_limit(rate) => first_source);
                late = source!(Note => last_source);
                merged = merge!(first early: Note => merges::concat);
                collector = sink!(Note => collector);
            },

            topology: {
                early |> merged;
                late |> merged;
                merged |> collector;
            }
        })
    })
}

async fn run_to_end(journal_base: &Path, inputs: Inputs, args: Vec<OsString>) -> Result<PathBuf> {
    let mut cli = vec![OsString::from("obzenflow")];
    cli.extend(args);
    let mut app = FlowApplication::builder().with_cli_args(cli);
    if let Some((window, stall_timeout_ms)) = inputs.enforce {
        let config = journal_base.with_file_name("obzenflow.toml");
        std::fs::write(
            &config,
            format!(
                "[runtime.backpressure.flow]\nmode = \"enforce\"\nwindow = {window}\nstall_timeout_ms = {stall_timeout_ms}\n"
            ),
        )?;
        app = app.with_config_file(config);
    }
    app.run_async(build(
        journal_base.to_path_buf(),
        inputs,
        Collect::default(),
    ))
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    Ok(replay_testkit::latest_run_dir(journal_base))
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
    .map_err(|_| anyhow!("timeout waiting for the pipeline to run"))?
}

async fn wait_for_notes(sink: &Collect, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let arrived = sink.arrived.notified();
            if sink.notes.lock().map(|notes| notes.len()).unwrap_or(0) >= count {
                return;
            }
            arrived.await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {count} notes"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Row {
    Data(String),
    Eof { kind: EofKind, by: String },
}

fn stage_names(run: &Path) -> HashMap<String, String> {
    replay_testkit::archive_manifest(run)["stages"]
        .as_object()
        .expect("manifest stages")
        .iter()
        .map(|(name, stage)| {
            (
                stage["stage_id"].as_str().expect("stage id").to_string(),
                name.clone(),
            )
        })
        .collect()
}

fn rows(records: &[JournalRecord<ChainPayload>], names: &HashMap<String, String>) -> Vec<Row> {
    records
        .iter()
        .filter_map(|record| match &record.payload {
            ChainPayload::Fact(value) => Some(Row::Data(
                value
                    .get("note")
                    .and_then(|note| note.as_str())
                    .unwrap_or_default()
                    .to_string(),
            )),
            ChainPayload::FlowControl(FlowControlPayload::Eof {
                kind, writer_id, ..
            }) => Some(Row::Eof {
                kind: *kind,
                by: match writer_id {
                    Some(WriterId::Stage(id)) => names
                        .get(&id.to_string())
                        .cloned()
                        .unwrap_or_else(|| id.to_string()),
                    _ => "unknown".to_string(),
                },
            }),
            _ => None,
        })
        .collect()
}

async fn stage_rows(run: &Path, stage: &str) -> Vec<Row> {
    rows(
        &replay_testkit::read_stage_envelopes_appended(run, stage).await,
        &stage_names(run),
    )
}

fn data(rows: &[Row]) -> Vec<&str> {
    rows.iter()
        .filter_map(|row| match row {
            Row::Data(note) => Some(note.as_str()),
            Row::Eof { .. } => None,
        })
        .collect()
}

fn natural_by(stage: &str) -> Row {
    Row::Eof {
        kind: EofKind::Natural,
        by: stage.to_string(),
    }
}

#[tokio::test]
async fn a_concat_merge_passes_on_its_first_producer_before_the_other() -> Result<()> {
    let _guard = CONCAT_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let inputs = Inputs {
        first: 4,
        last: 3,
        first_rate: 8.0,
        enforce: None,
    };
    let run = run_to_end(&journal_base, inputs.clone(), Vec::new()).await?;

    let merged = stage_rows(&run, "merged").await;
    let boundary = merged
        .iter()
        .position(|row| *row == natural_by("early"))
        .unwrap_or_else(|| panic!("the first producer's natural EOF is passed on: {merged:?}"));
    assert_eq!(
        data(&merged[..boundary]),
        ["first-1", "first-2", "first-3", "first-4"],
        "{merged:?}"
    );
    assert_eq!(
        data(&merged[boundary..]),
        ["last-1", "last-2", "last-3"],
        "{merged:?}"
    );
    assert_eq!(merged.last(), Some(&natural_by("merged")), "{merged:?}");
    replay_testkit::assert_eof_carries_the_ending_clock(&run, "merged").await;

    let manifest = replay_testkit::archive_manifest(&run);
    assert_eq!(
        manifest["stages"]["merged"]["delivery_order"], "structural",
        "{manifest}"
    );
    assert_eq!(manifest["stages"]["merged"]["stage_type"], "Merge");

    let replay = run_to_end(
        &journal_base,
        inputs,
        vec![
            OsString::from("--replay-from"),
            run.as_os_str().to_os_string(),
        ],
    )
    .await?;
    assert_ne!(run, replay);
    assert_eq!(merged, stage_rows(&replay, "merged").await);
    Ok(())
}

#[tokio::test]
async fn a_stop_in_the_first_phase_drains_without_reading_the_other_producer() -> Result<()> {
    let _guard = CONCAT_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let sink = Collect::default();
    let handle = build(
        journal_base.clone(),
        Inputs {
            first: 6,
            last: 3,
            first_rate: 2.0,
            enforce: None,
        },
        sink.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;
    wait_for_running(&handle).await?;
    wait_for_notes(&sink, 1).await?;
    handle.stop_graceful(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(15), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the pipeline to stop"))??;
    let run = replay_testkit::latest_run_dir(&journal_base);

    let late = stage_rows(&run, "late").await;
    assert_eq!(data(&late), ["last-1", "last-2", "last-3"], "{late:?}");
    let merged = stage_rows(&run, "merged").await;
    let read = data(&merged);
    assert!(
        !read.is_empty() && read.len() < 6 && read.iter().all(|note| note.starts_with("first-")),
        "{merged:?}"
    );
    assert_eq!(
        merged.last(),
        Some(&Row::Eof {
            kind: EofKind::Truncated,
            by: "merged".to_string(),
        }),
        "{merged:?}"
    );
    let delivered = sink
        .notes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(
        delivered.iter().all(|note| note.starts_with("first-")),
        "{delivered:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_held_producer_outlasts_the_stall_ceiling_under_enforce() -> Result<()> {
    let _guard = CONCAT_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let run = run_to_end(
        &journal_base,
        Inputs {
            first: 6,
            last: 5,
            first_rate: 2.0,
            enforce: Some((2, 500)),
        },
        Vec::new(),
    )
    .await?;

    for stage in ["early", "late", "merged", "collector"] {
        let records = replay_testkit::read_stage_envelopes_appended(&run, stage).await;
        assert!(
            !records
                .iter()
                .any(|record| record.event_type().ends_with("stall_detected")),
            "{stage} stalled"
        );
        assert!(
            !rows(&records, &stage_names(&run))
                .iter()
                .any(|row| matches!(
                    row,
                    Row::Eof {
                        kind: EofKind::Poison,
                        ..
                    }
                )),
            "{stage} ended in poison"
        );
    }
    assert_eq!(
        data(&stage_rows(&run, "merged").await),
        [
            "first-1", "first-2", "first-3", "first-4", "first-5", "first-6", "last-1", "last-2",
            "last-3", "last-4", "last-5"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn every_edge_of_a_concat_merge_records_one_passing_verdict() -> Result<()> {
    let _guard = CONCAT_TEST_LOCK.lock().await;
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let run = run_to_end(
        &journal_base,
        Inputs {
            first: 4,
            last: 3,
            first_rate: 1000.0,
            enforce: None,
        },
        Vec::new(),
    )
    .await?;

    assert_eq!(
        replay_testkit::passing_verdicts(&run).await,
        replay_testkit::one_verdict_per_edge(&[
            "early -> merged",
            "late -> merged",
            "merged -> collector",
        ])
    );
    Ok(())
}
