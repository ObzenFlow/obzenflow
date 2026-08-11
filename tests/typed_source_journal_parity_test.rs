// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134g journal oracle for all four typed source variants.

use async_trait::async_trait;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, StageOutputFacts, TypedPayload, WriterId};
use obzenflow_dsl::{
    async_infinite_source, async_source, flow, infinite_source, sink, source, FlowDefinition,
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handlers::{
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::sink::{DeliveryContext, SinkTyped};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Alpha {
    source: String,
    value: u64,
}

impl TypedPayload for Alpha {
    const EVENT_TYPE: &'static str = "flowip_134g.alpha";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Beta {
    source: String,
    value: u64,
}

impl TypedPayload for Beta {
    const EVENT_TYPE: &'static str = "flowip_134g.beta";
}

#[derive(Clone, Debug, StageOutputFacts)]
enum SourceFact {
    Alpha(Alpha),
    Beta(Beta),
}

#[derive(Debug, Default)]
struct Counters {
    sync_finite_calls: AtomicUsize,
    async_finite_calls: AtomicUsize,
    sync_infinite_calls: AtomicUsize,
    async_infinite_calls: AtomicUsize,
    async_finite_cleanup: AtomicUsize,
    async_infinite_cleanup: AtomicUsize,
    alpha_delivered: AtomicUsize,
    beta_delivered: AtomicUsize,
}

fn pair(source: &'static str, value: u64) -> Vec<SourceFact> {
    vec![
        SourceFact::Alpha(Alpha {
            source: source.to_string(),
            value,
        }),
        SourceFact::Beta(Beta {
            source: source.to_string(),
            value,
        }),
    ]
}

#[derive(Clone, Debug)]
struct SyncFinite {
    emitted: bool,
    counters: Arc<Counters>,
}

impl TypedFiniteSourceHandler for SyncFinite {
    type Output = SourceFact;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.counters
            .sync_finite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(None)
        } else {
            Ok(Some(pair("sync_finite", 1)))
        }
    }
}

#[derive(Clone, Debug)]
struct AsyncFinite {
    emitted: bool,
    counters: Arc<Counters>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for AsyncFinite {
    type Output = SourceFact;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.counters
            .async_finite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(None)
        } else {
            Ok(Some(pair("async_finite", 2)))
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.counters
            .async_finite_cleanup
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SyncInfinite {
    emitted: bool,
    counters: Arc<Counters>,
}

impl TypedInfiniteSourceHandler for SyncInfinite {
    type Output = SourceFact;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        self.counters
            .sync_infinite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            Ok(Vec::new())
        } else {
            Ok(pair("sync_infinite", 3))
        }
    }
}

#[derive(Clone, Debug)]
struct AsyncInfinite {
    emitted: bool,
    counters: Arc<Counters>,
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for AsyncInfinite {
    type Output = SourceFact;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        self.counters
            .async_infinite_calls
            .fetch_add(1, Ordering::SeqCst);
        if std::mem::replace(&mut self.emitted, true) {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(Vec::new())
        } else {
            Ok(pair("async_infinite", 4))
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.counters
            .async_infinite_cleanup
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn counting<T>(
    count: Arc<Counters>,
    select: fn(&Counters) -> &AtomicUsize,
) -> impl FnMut(T, DeliveryContext) -> std::future::Ready<()> + Send + Sync + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_value, _delivery| {
        select(&count).fetch_add(1, Ordering::SeqCst);
        std::future::ready(())
    }
}

fn build_flow(journal_base: PathBuf, counters: Arc<Counters>) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let sync_finite = SyncFinite {
            emitted: false,
            counters: counters.clone(),
        };
        let async_finite = AsyncFinite {
            emitted: false,
            counters: counters.clone(),
        };
        let sync_infinite = SyncInfinite {
            emitted: false,
            counters: counters.clone(),
        };
        let async_infinite = AsyncInfinite {
            emitted: false,
            counters: counters.clone(),
        };
        let alpha_sink =
            SinkTyped::with_delivery(counting::<Alpha>(counters.clone(), |counters| {
                &counters.alpha_delivered
            }))
            .idempotent();
        let beta_sink = SinkTyped::with_delivery(counting::<Beta>(counters, |counters| {
            &counters.beta_delivered
        }))
        .idempotent();

        Ok(flow! {
            name: "typed_source_journal_parity",
            journals: disk_journals(journal_base),

            stages: {
                sync_fin = source!({ Alpha, Beta } => sync_finite);
                async_fin = async_source!({ Alpha, Beta } => async_finite);
                sync_inf = infinite_source!({ Alpha, Beta } => sync_infinite);
                async_inf = async_infinite_source!({ Alpha, Beta } => async_infinite);
                alphas = sink!(Alpha => alpha_sink);
                betas = sink!(Beta => beta_sink);
            },

            topology: {
                sync_fin |> alphas;
                sync_fin |> betas;
                async_fin |> alphas;
                async_fin |> betas;
                sync_inf |> alphas;
                sync_inf |> betas;
                async_inf |> alphas;
                async_inf |> betas;
            }
        })
    })
}

async fn wait_for_running(handle: &FlowHandle) {
    let mut state = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if matches!(*state.borrow(), PipelineState::Running) {
                return;
            }
            state.changed().await.expect("pipeline state remains open");
        }
    })
    .await
    .expect("pipeline reaches running");
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize, label: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}

async fn run_live(journal_base: &Path, counters: Arc<Counters>) {
    let handle = build_flow(journal_base.to_path_buf(), counters.clone())
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .expect("typed source parity flow builds");
    wait_for_running(&handle).await;
    wait_for_count(&counters.alpha_delivered, 4, "alpha deliveries").await;
    wait_for_count(&counters.beta_delivered, 4, "beta deliveries").await;
    wait_for_count(&counters.async_finite_cleanup, 1, "natural finite cleanup").await;

    handle
        .stop_graceful(Duration::from_secs(10))
        .await
        .expect("graceful stop requested");
    tokio::time::timeout(Duration::from_secs(15), handle.wait_for_completion())
        .await
        .expect("flow terminates after graceful stop")
        .expect("flow completes cleanly");
}

async fn run_replay(journal_base: &Path, archive: &Path, counters: Arc<Counters>) {
    FlowApplication::builder()
        .with_cli_args([
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_base.to_path_buf(), counters))
        .await
        .expect("strict source replay completes");
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("flow directory entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow produced a replay archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest readable"),
    )
    .expect("manifest parses")
}

async fn read_stage_appended(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest data journal for {stage_name}"));
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    let mut reader = journal.reader().await.expect("stage journal reader");
    let mut events = Vec::new();
    while let Some(event) = reader.next().await.expect("stage journal read") {
        events.push(event);
    }
    events
}

fn data_signature(events: &[EventEnvelope<ChainEvent>]) -> Vec<(String, serde_json::Value)> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => Some((event_type.to_string(), payload.clone())),
            _ => None,
        })
        .collect()
}

fn assert_source_journal(
    stage_name: &str,
    events: &[EventEnvelope<ChainEvent>],
    expected_data_writer: Option<WriterId>,
    eof_must_match_data_writer: bool,
) -> WriterId {
    let eof = events
        .iter()
        .find(|envelope| envelope.event.is_eof())
        .unwrap_or_else(|| panic!("{stage_name} authors EOF"));
    let data = events
        .iter()
        .filter(|envelope| envelope.event.is_data())
        .collect::<Vec<_>>();
    assert_eq!(
        data.len(),
        2,
        "{stage_name} authors one member of each type"
    );
    let writer = data[0].event.writer_id;
    assert!(writer.is_stage(), "{stage_name} has a stage-owned writer");
    assert!(
        data.iter()
            .all(|envelope| envelope.event.writer_id == writer),
        "{stage_name} data writers {:?} did not converge on {writer}",
        data.iter()
            .map(|envelope| envelope.event.writer_id.to_string())
            .collect::<Vec<_>>()
    );
    if let Some(expected) = expected_data_writer {
        assert_eq!(
            writer, expected,
            "{stage_name} replay must preserve live Data authorship"
        );
    }
    assert_eq!(
        data.iter()
            .map(|envelope| envelope.event.event_type().to_string())
            .collect::<Vec<_>>(),
        vec![Alpha::versioned_event_type(), Beta::versioned_event_type()]
    );
    assert!(data
        .iter()
        .all(|envelope| !envelope.event.event_type().ends_with(".error")));

    if eof_must_match_data_writer {
        assert_eq!(
            eof.event.writer_id, writer,
            "{stage_name} live Data and EOF must use the installed stage writer"
        );
    }
    let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_seq,
        writer_seq_by_event_type,
        last_event_id,
        ..
    }) = &eof.event.content
    else {
        unreachable!("EOF shape")
    };
    assert_eq!(writer_seq.map(|seq| seq.0), Some(2));
    assert_eq!(
        writer_seq_by_event_type
            .get(Alpha::versioned_event_type().as_str())
            .map(|seq| seq.0),
        Some(1)
    );
    assert_eq!(
        writer_seq_by_event_type
            .get(Beta::versioned_event_type().as_str())
            .map(|seq| seq.0),
        Some(1)
    );
    assert_eq!(writer_seq_by_event_type.len(), 2);
    assert_eq!(
        *last_event_id,
        data.last().map(|envelope| envelope.event.id)
    );

    let final_contract = events
        .iter()
        .find_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                consumed_count,
                eof_seen,
                reader_seq,
                advertised_writer_seq,
                ..
            }) => Some((
                *pass,
                consumed_count.0,
                *eof_seen,
                reader_seq.0,
                advertised_writer_seq.map(|seq| seq.0),
            )),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{stage_name} authors its final consumption contract"));
    assert_eq!(final_contract, (true, 2, true, 2, Some(2)));
    writer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_typed_source_variants_have_canonical_live_replay_parity() {
    let temp = tempfile::tempdir().expect("parity tempdir");
    let journal_base = temp.path().join("journals");

    let live_counters = Arc::new(Counters::default());
    run_live(&journal_base, live_counters.clone()).await;
    assert!(live_counters.sync_finite_calls.load(Ordering::SeqCst) >= 2);
    assert!(live_counters.async_finite_calls.load(Ordering::SeqCst) >= 2);
    assert!(live_counters.sync_infinite_calls.load(Ordering::SeqCst) >= 1);
    assert!(live_counters.async_infinite_calls.load(Ordering::SeqCst) >= 1);
    assert_eq!(live_counters.async_finite_cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(
        live_counters.async_infinite_cleanup.load(Ordering::SeqCst),
        1
    );

    let live = latest_run_dir(&journal_base);
    let stage_names = ["sync_fin", "async_fin", "sync_inf", "async_inf"];
    let mut live_rows = Vec::new();
    for stage_name in stage_names {
        let events = read_stage_appended(&live, stage_name).await;
        let writer = assert_source_journal(stage_name, &events, None, true);
        live_rows.push((stage_name, writer, data_signature(&events)));
    }

    let replay_counters = Arc::new(Counters::default());
    run_replay(&journal_base, &live, replay_counters.clone()).await;
    assert_eq!(replay_counters.sync_finite_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_counters.async_finite_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_counters.sync_infinite_calls.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        replay_counters.async_infinite_calls.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        replay_counters.async_finite_cleanup.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        replay_counters
            .async_infinite_cleanup
            .load(Ordering::SeqCst),
        0
    );

    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    for (stage_name, live_writer, expected_data) in live_rows {
        let events = read_stage_appended(&replay, stage_name).await;
        assert_source_journal(stage_name, &events, Some(live_writer), false);
        assert_eq!(data_signature(&events), expected_data);
    }

    let verification = verify_run_dirs(&live, &replay, &VerifyOptions::default())
        .expect("source replay verification runs");
    assert!(matches!(verification, VerifyOutcome::Completed { .. }));
    assert_eq!(
        verification.exit_code(),
        0,
        "whole-run source replay projection must match: {}",
        obzenflow_infra::verify::render_verdict(&verification)
    );
}
