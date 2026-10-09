// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134g HTTP-pull observation witness, amended by FLOWIP-084n B7: a
//! fatal decode abandons the scan, so the source fails without EOF and its
//! committed diagnostic is the lifecycle failure's cause.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_adapters::sources::{
    DecodeError, DecodeResult, HttpPullConfig, HttpPullSource, HttpResponse, PullDecoder,
};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourcePollContinuation, SourcePollErrorKind, StageLifecycleFact,
};
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord, SourceDiagnosticReason};
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError, RequestSpec};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{async_source, flow, sink, FlowDefinition};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
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

/// Paginates until the server refuses a page; the default `decode_error`
/// maps that 401 to `DecodeError::Fatal`.
#[derive(Clone, Debug)]
struct PagedDecoder;

impl PullDecoder for PagedDecoder {
    type Cursor = u32;
    type Output = PullItem;

    fn request_spec(&self, cursor: Option<&u32>) -> RequestSpec {
        let page = cursor.copied().unwrap_or(1);
        RequestSpec::get(
            format!("http://example.invalid/items?page={page}")
                .parse()
                .expect("test URL"),
        )
    }

    fn decode_success(
        &self,
        cursor: Option<&u32>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<u32, PullItem>, DecodeError> {
        Ok(DecodeResult {
            items: response.json()?,
            next_cursor: Some(cursor.copied().unwrap_or(1) + 1),
        })
    }
}

#[derive(Debug)]
struct PageThenUnauthorized {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpClient for PageThenUnauthorized {
    async fn execute(&self, _request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if call == 0 {
            HttpResponse::new(200, HeaderMap::new(), r#"[{"id":1},{"id":2}]"#)
        } else {
            HttpResponse::new(401, HeaderMap::new(), "SECRET_RESPONSE_BODY")
        })
    }
}

fn build_flow(journal_base: PathBuf, calls: Arc<AtomicUsize>) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let client: Arc<dyn HttpClient> = Arc::new(PageThenUnauthorized {
            calls: calls.clone(),
        });
        let source = HttpPullSource::new(
            PagedDecoder,
            HttpPullConfig::builder()
                .client(client)
                .build()
                .expect("HTTP pull config"),
        );
        let sink = replay_testkit::Discard::<PullItem>::default();
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

async fn run(
    journal_base: &Path,
    calls: Arc<AtomicUsize>,
    replay_from: Option<&Path>,
) -> Result<(), ApplicationError> {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
        args.push(OsString::from("--allow-incomplete-archive"));
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf(), calls))
        .await
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest readable"),
    )
    .expect("manifest parses")
}

async fn read_pull_journal(run_dir: &Path, journal: &str) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"]["pull"][journal]
        .as_str()
        .expect("pull journal in manifest");
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(journal_file)).await
}

fn items(rows: &[JournalRecord<ChainPayload>]) -> usize {
    rows.iter().filter(|row| row.consumes_data_credit()).count()
}

fn failed_cause(rows: &[JournalRecord<ChainPayload>]) -> Option<Option<EventId>> {
    rows.iter().find_map(|row| match &row.payload {
        ChainPayload::Execution(ExecutionPayload::StageLifecycle(StageLifecycleFact::Failed {
            causal_event_id,
            ..
        })) => Some(*causal_event_id),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn fatal_decode_after_a_valid_page_fails_without_eof_and_replays_cold() {
    let temp = tempfile::tempdir().expect("HTTP witness tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let outcome = run(&journal_base, live_calls.clone(), None).await;
    assert!(
        matches!(outcome, Err(ApplicationError::FlowExecutionFailed(_))),
        "a terminal source fails the pipeline: {outcome:?}"
    );
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        2,
        "no request after the terminal report"
    );

    let live = replay_testkit::latest_run_dir(&journal_base);
    let data = read_pull_journal(&live, "data_journal_file").await;
    let errors = read_pull_journal(&live, "error_journal_file").await;
    assert_eq!(items(&data), 2, "the valid page stays committed");
    assert!(
        !data.iter().any(|row| row.is_eof()),
        "an abandoned scan writes no EOF"
    );

    let failures: Vec<_> = errors
        .iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::SourcePollError(failure)) => {
                Some((row.envelope.provenance.event.id, failure))
            }
            _ => None,
        })
        .collect();
    assert_eq!(failures.len(), 1, "{failures:?}");
    let (diagnostic_id, failure) = failures[0];
    assert_eq!(failure.continuation, SourcePollContinuation::Terminal);
    assert_eq!(failure.error_type, SourcePollErrorKind::Validation);
    assert_eq!(
        failure.diagnostic.reason(),
        SourceDiagnosticReason::RemoteRejected
    );
    let code = failure.diagnostic.error_code().expect("HTTP status code");
    assert_eq!((code.namespace(), code.value()), ("http.status", "401"));
    assert_eq!(failed_cause(&data), Some(Some(diagnostic_id)));
    assert!(!serde_json::to_string(&errors)
        .expect("error journal serialises")
        .contains("SECRET_RESPONSE_BODY"));

    // Strict replay substitutes the accepted facts and contacts nothing; it
    // does not rerun the decoder to recreate the diagnostic.
    let replay_calls = Arc::new(AtomicUsize::new(0));
    run(&journal_base, replay_calls.clone(), Some(&live))
        .await
        .expect("replay of the failed archive completes");
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(replay, live);
    let replay_data = read_pull_journal(&replay, "data_journal_file").await;
    let replay_errors = read_pull_journal(&replay, "error_journal_file").await;
    assert_eq!(items(&replay_data), 2);
    assert!(!replay_errors.iter().any(|row| matches!(
        row.payload,
        ChainPayload::Execution(ExecutionPayload::SourcePollError(_))
    )));
}
