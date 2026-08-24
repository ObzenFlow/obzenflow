// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134g HTTP-pull validation and observation journal witness.

use async_trait::async_trait;
use obzenflow_adapters::sources::{
    CursorlessPullDecoder, DecodeError, HttpPullConfig, HttpPullSource, HttpResponse,
};
use obzenflow_core::event::observability::HttpPullTelemetry;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError, RequestSpec};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{async_source, flow, sink, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::stages::sink::SinkTyped;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PullItem {
    id: u64,
}

impl TypedPayload for PullItem {
    const EVENT_TYPE: &'static str = "flowip_134g.http_pull_item";
}

#[derive(Clone, Debug)]
struct ValidationDecoder;

impl CursorlessPullDecoder for ValidationDecoder {
    type Output = PullItem;

    fn request_spec(&self) -> RequestSpec {
        RequestSpec::get("http://example.invalid/items".parse().expect("test URL"))
    }

    fn decode_success(&self, _response: &HttpResponse) -> Result<Vec<Self::Output>, DecodeError> {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
struct CountingValidationClient {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpClient for CountingValidationClient {
    async fn execute(&self, _request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(HttpResponse::new(401, HeaderMap::new(), "unauthorized"))
    }
}

fn build_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let client: Arc<dyn HttpClient> = Arc::new(CountingValidationClient {
            calls: calls.clone(),
        });
        let source = HttpPullSource::new(
            ValidationDecoder,
            HttpPullConfig::builder()
                .client(client)
                .build()
                .expect("HTTP pull config"),
        );
        let sink = SinkTyped::new(|_item: PullItem| async move {}).idempotent();
        Ok(flow! {
            name: "http_pull_source_witness",
            journals: disk_journals(journal_base),
            stages: {
                pull = async_source!(PullItem => source);
                sink = sink!(PullItem => sink);
            },
            topology: { pull |> sink; }
        })
    })
}

async fn run(journal_base: &Path, calls: Arc<AtomicUsize>, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf(), calls))
        .await
        .expect("HTTP pull witness flow completes");
}

async fn read_error_journal(run_dir: &Path) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"]["pull"]["error_journal_file"]
        .as_str()
        .expect("pull error journal in manifest");
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("pull error journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("pull error journal reads")
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

async fn read_data_journal(run_dir: &Path) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"]["pull"]["data_journal_file"]
        .as_str()
        .expect("pull data journal in manifest");
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("pull data journal opens");
    let mut reader = journal.reader().await.expect("pull data journal reader");
    let mut events = Vec::new();
    while let Some(event) = reader.next().await.expect("pull data journal read") {
        events.push(event);
    }
    events
}

fn typed_snapshots(events: &[EventEnvelope<ChainEvent>]) -> Vec<(WriterId, HttpPullTelemetry)> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::HttpPullSnapshot { snapshot },
            )) => Some((envelope.event.writer_id, snapshot.clone())),
            _ => None,
        })
        .collect()
}

fn assert_validation_journals(
    data: &[EventEnvelope<ChainEvent>],
    errors: &[EventEnvelope<ChainEvent>],
    expect_live_snapshots: bool,
) {
    assert!(data.iter().all(|envelope| !envelope.event.is_data()));
    assert!(data
        .iter()
        .chain(errors)
        .all(|envelope| !envelope.event.event_type().ends_with(".error")));

    let eof_position = data
        .iter()
        .position(|envelope| envelope.event.is_eof())
        .expect("finite validation is followed by EOF");
    let eof_writer = data[eof_position].event.writer_id;
    let snapshots = typed_snapshots(data);
    if expect_live_snapshots {
        assert!(
            !snapshots.is_empty(),
            "live validation reports typed telemetry"
        );
        assert!(snapshots.iter().all(|(writer, _)| *writer == eof_writer));
        assert!(
            data[..eof_position].iter().any(|envelope| matches!(
                envelope.event.content,
                ChainEventContent::Observability(ObservabilityPayload::Metrics(
                    MetricsLifecycle::HttpPullSnapshot { .. }
                ))
            )),
            "the live validation-poll snapshot is committed before EOF"
        );
    } else {
        assert!(
            snapshots.is_empty(),
            "strict replay must not report fresh HTTP pull telemetry"
        );
    }

    let validation_rows = errors
        .iter()
        .filter(|envelope| {
            matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Error {
                    kind: Some(ErrorKind::Validation),
                    ..
                }
            )
        })
        .collect::<Vec<_>>();
    if expect_live_snapshots {
        assert_eq!(validation_rows.len(), 1);
        assert!(validation_rows.iter().all(|row| !row.event.is_data()));
    } else {
        assert!(
            validation_rows.is_empty(),
            "strict replay must not re-run source validation"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn validation_and_typed_snapshots_replay_without_live_http_or_error_data() {
    let temp = tempfile::tempdir().expect("HTTP witness tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    run(&journal_base, live_calls.clone(), None).await;
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    let live = latest_run_dir(&journal_base);
    let live_data = read_data_journal(&live).await;
    let live_errors = read_error_journal(&live).await;
    assert_validation_journals(&live_data, &live_errors, true);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    run(&journal_base, replay_calls.clone(), Some(&live)).await;
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must neither fetch nor report a fresh source snapshot"
    );
    let replay = latest_run_dir(&journal_base);
    let replay_data = read_data_journal(&replay).await;
    let replay_errors = read_error_journal(&replay).await;
    assert_validation_journals(&replay_data, &replay_errors, false);

    let verification = verify_run_dirs(&live, &replay, &VerifyOptions::default())
        .expect("HTTP pull verification runs");
    assert!(matches!(verification, VerifyOutcome::Completed { .. }));
    assert_eq!(verification.exit_code(), 0);
}
