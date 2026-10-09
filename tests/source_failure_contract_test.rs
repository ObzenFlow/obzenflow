// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084n B1/B2 journal oracle across all four source families: a
//! terminal report and an opening failure each commit one diagnostic, fail the
//! stage and pipeline with a causal link to it, and write no EOF.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceOpenIntent, SourcePollContinuation, SourcePollErrorKind,
    StageLifecycleFact,
};
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord, SourceDiagnosticReason};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{
    async_infinite_source, async_source, flow, infinite_source, sink, source, FlowDefinition,
};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Row {
    value: u64,
}

impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "flowip_084n.contract_row";
}

fn terminal() -> SourceError {
    SourceError::Terminal {
        kind: SourcePollErrorKind::Transport,
        diagnostic: SourceDiagnosticReason::InputUnavailable.into(),
    }
}

fn unavailable() -> SourceError {
    SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into())
}

/// Emits one row, then reports that it cannot continue.
#[derive(Default)]
struct RowThenTerminal(usize);

impl RowThenTerminal {
    fn step(&mut self) -> Result<Vec<Row>, SourceError> {
        self.0 += 1;
        match self.0 {
            1 => Ok(vec![Row { value: 1 }]),
            _ => Err(terminal()),
        }
    }
}

impl TypedFiniteSourceHandler for RowThenTerminal {
    type Output = Row;
    fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> {
        self.step().map(Some)
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for RowThenTerminal {
    type Output = Row;
    async fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> {
        self.step().map(Some)
    }
}

impl TypedInfiniteSourceHandler for RowThenTerminal {
    type Output = Row;
    fn next(&mut self) -> Result<Vec<Row>, SourceError> {
        self.step()
    }
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for RowThenTerminal {
    type Output = Row;
    async fn next(&mut self) -> Result<Vec<Row>, SourceError> {
        self.step()
    }
}

struct FailsToOpenFinite;
struct FailsToOpenAsyncFinite;
struct FailsToOpenInfinite;
struct FailsToOpenAsyncInfinite;

impl FiniteSourceConnector for FailsToOpenFinite {
    type Output = Row;
    type Reader = RowThenTerminal;
    fn open(&self, _: SourceReaderInitContext) -> Result<RowThenTerminal, SourceError> {
        Err(unavailable())
    }
}

#[async_trait]
impl AsyncFiniteSourceConnector for FailsToOpenAsyncFinite {
    type Output = Row;
    type Reader = RowThenTerminal;
    async fn open(&self, _: SourceReaderInitContext) -> Result<RowThenTerminal, SourceError> {
        Err(unavailable())
    }
}

impl InfiniteSourceConnector for FailsToOpenInfinite {
    type Output = Row;
    type Reader = RowThenTerminal;
    fn open(&self, _: SourceReaderInitContext) -> Result<RowThenTerminal, SourceError> {
        Err(unavailable())
    }
}

#[async_trait]
impl AsyncInfiniteSourceConnector for FailsToOpenAsyncInfinite {
    type Output = Row;
    type Reader = RowThenTerminal;
    async fn open(&self, _: SourceReaderInitContext) -> Result<RowThenTerminal, SourceError> {
        Err(unavailable())
    }
}

struct Journals {
    data: Vec<JournalRecord<ChainPayload>>,
    errors: Vec<JournalRecord<ChainPayload>>,
}

async fn input_journals(base: &Path) -> Journals {
    let run = replay_testkit::latest_run_dir(base);
    let manifest = replay_testkit::archive_manifest(&run);
    let errors = manifest["stages"]["input"]["error_journal_file"]
        .as_str()
        .expect("input error journal in manifest");
    Journals {
        data: replay_testkit::read_stage_envelopes_appended(&run, "input").await,
        errors: replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(errors))
            .await,
    }
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

fn source_failures(rows: &[JournalRecord<ChainPayload>]) -> Vec<(EventId, &ExecutionPayload)> {
    rows.iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(
                payload @ (ExecutionPayload::SourcePollError(_)
                | ExecutionPayload::SourceOpenFailed(_)),
            ) => Some((row.envelope.provenance.event.id, payload)),
            _ => None,
        })
        .collect()
}

async fn run_expecting_failure(definition: FlowDefinition) {
    let outcome = FlowApplication::builder()
        .with_cli_args(vec![std::ffi::OsString::from("obzenflow")])
        .run_async(definition)
        .await;
    assert!(
        matches!(outcome, Err(ApplicationError::FlowExecutionFailed(_))),
        "a failed source fails the pipeline: {outcome:?}"
    );
}

async fn assert_terminal_contract(base: &Path) {
    let journals = input_journals(base).await;
    assert_eq!(
        journals
            .data
            .iter()
            .filter(|row| row.consumes_data_credit())
            .count(),
        1,
        "the row accepted before the report stays committed"
    );
    assert!(!journals.data.iter().any(|row| row.is_eof()));
    let failures = source_failures(&journals.errors);
    assert_eq!(failures.len(), 1, "{failures:?}");
    let (id, ExecutionPayload::SourcePollError(poll)) = failures[0] else {
        panic!("expected source.poll_error: {failures:?}");
    };
    assert_eq!(poll.continuation, SourcePollContinuation::Terminal);
    assert_eq!(failed_cause(&journals.data), Some(Some(id)));
}

async fn assert_open_failure_contract(base: &Path) {
    let journals = input_journals(base).await;
    assert!(!journals.data.iter().any(|row| row.consumes_data_credit()));
    assert!(!journals.data.iter().any(|row| row.is_eof()));
    let failures = source_failures(&journals.errors);
    assert_eq!(
        failures.len(),
        1,
        "no poll error accompanies an opening failure"
    );
    let (id, ExecutionPayload::SourceOpenFailed(opened)) = failures[0] else {
        panic!("expected source.open_failed: {failures:?}");
    };
    assert_eq!(opened.intent, SourceOpenIntent::Start);
    assert_eq!(
        opened.diagnostic.reason(),
        SourceDiagnosticReason::InputUnavailable
    );
    assert_eq!(failed_cause(&journals.data), Some(Some(id)));
}

macro_rules! family_contract {
    ($family:ident, $source:ident, $fails_to_open:expr) => {
        mod $family {
            use super::*;

            fn terminal_flow(base: PathBuf) -> FlowDefinition {
                FlowDefinition::materialize(move |_| {
                    let input = RowThenTerminal::default();
                    let output = replay_testkit::Discard::<Row>::default();
                    Ok(flow! {
                        name: "source_failure_contract",
                        journals: disk_journals(base.clone()),
                        stages: {
                            input = $source!(Row => input);
                            output = sink!(Row => output);
                        },
                        topology: { input |> output; }
                    })
                })
            }

            fn failing_open_flow(base: PathBuf) -> FlowDefinition {
                FlowDefinition::materialize(move |_| {
                    let input = $fails_to_open;
                    let output = replay_testkit::Discard::<Row>::default();
                    Ok(flow! {
                        name: "source_failure_contract",
                        journals: disk_journals(base.clone()),
                        stages: {
                            input = $source!(Row => input);
                            output = sink!(Row => output);
                        },
                        topology: { input |> output; }
                    })
                })
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn terminal_report_fails_without_eof_and_links_its_diagnostic() {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path().join("journals");
                run_expecting_failure(terminal_flow(base.clone())).await;
                assert_terminal_contract(&base).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn opening_failure_commits_source_open_failed() {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path().join("journals");
                run_expecting_failure(failing_open_flow(base.clone())).await;
                assert_open_failure_contract(&base).await;
            }
        }
    };
}

family_contract!(sync_finite, source, FailsToOpenFinite);
family_contract!(async_finite, async_source, FailsToOpenAsyncFinite);
family_contract!(sync_infinite, infinite_source, FailsToOpenInfinite);
family_contract!(
    async_infinite,
    async_infinite_source,
    FailsToOpenAsyncInfinite
);
