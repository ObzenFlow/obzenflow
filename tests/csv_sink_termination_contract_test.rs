// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134h termination and crash-before-flush acceptance proof for CsvSink.

use async_trait::async_trait;
use futures::FutureExt;
use obzenflow::sinks::{CsvProjection, CsvSink};
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SourceAdmission, SourcePolicy, SourcePolicyCtx, SourcePollAttachment,
    SourcePollOutcome,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::common::HandlerError;
use obzenflow_runtime::stages::source::strategies::{
    CompletionContext, CompletionDecision, CompletionGate,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

mod replay_testkit;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CsvRecord {
    id: u64,
    label: String,
}

impl TypedPayload for CsvRecord {
    const EVENT_TYPE: &'static str = "flowip_134h.csv_termination_record";
}

#[derive(Clone, Debug)]
struct CsvRecordProjection;

impl CsvProjection for CsvRecordProjection {
    type Input = CsvRecord;
    type Row = CsvRecord;

    fn project(&self, input: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(input)
    }
}

#[derive(Clone, Debug)]
struct Records {
    next: u64,
    stall_after_data: bool,
}

impl Records {
    fn finite() -> Self {
        Self {
            next: 1,
            stall_after_data: false,
        }
    }

    fn stalling() -> Self {
        Self {
            next: 1,
            stall_after_data: true,
        }
    }
}

impl TypedFiniteSourceHandler for Records {
    type Output = CsvRecord;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next <= 3 {
            let id = self.next;
            self.next += 1;
            return Ok(Some(vec![CsvRecord {
                id,
                label: format!("record-{id}"),
            }]));
        }
        if self.stall_after_data {
            std::thread::sleep(Duration::from_millis(10));
            Ok(Some(Vec::new()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
struct AlwaysPoisonCompletion;

impl CompletionGate for AlwaysPoisonCompletion {
    fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::PoisonEof
    }

    fn on_begin_drain(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::PoisonEof
    }
}

struct AdmitSourcePoll;

#[async_trait]
impl SourcePolicy for AdmitSourcePoll {
    fn label(&self) -> &'static str {
        "flowip_134h_poison_completion"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        SourceAdmission::Admit(None)
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {}
}

struct PoisonCompletionFamily;
struct PoisonCompletionFactory;

impl MiddlewareFactory for PoisonCompletionFactory {
    fn label(&self) -> &'static str {
        "flowip_134h_poison_completion"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<PoisonCompletionFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::source_poll(
            SourcePollAttachment {
                policy: Arc::new(AdmitSourcePoll),
                completion_gate: Some(Arc::new(AlwaysPoisonCompletion)),
            },
        ))
    }
}

fn connector_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to build typed CSV sink: {error}"))
}

fn csv_sink(path: &Path) -> Result<CsvSink<CsvRecordProjection>, anyhow::Error> {
    CsvSink::builder(CsvRecordProjection)
        .path(path)
        .columns(["id", "label"])
        .buffer_size(100)
        .auto_flush(false)
        .build()
}

fn poison_flow(journal_base: PathBuf, csv_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let records = Records::finite();
        let output = csv_sink(&csv_path).map_err(connector_error)?;
        Ok(flow! {
            name: "flowip_134h_csv_poison",
            journals: disk_journals(journal_base),

            stages: {
                records = source!(CsvRecord => records with [PoisonCompletionFactory]);
                csv_out = sink!(CsvRecord => output);
            },

            topology: {
                records |> csv_out;
            }
        })
    })
}

fn stalling_flow(journal_base: PathBuf, csv_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let records = Records::stalling();
        let output = csv_sink(&csv_path).map_err(connector_error)?;
        Ok(flow! {
            name: "flowip_134h_csv_truncated",
            journals: disk_journals(journal_base),

            stages: {
                records = source!(CsvRecord => records);
                csv_out = sink!(CsvRecord => output);
            },

            topology: {
                records |> csv_out;
            }
        })
    })
}

fn eof_kinds(events: &[EventEnvelope<ChainEvent>]) -> Vec<EofKind> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { kind, .. }) => Some(*kind),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ReceiptSummary {
    buffered: usize,
    committed: usize,
    audits: usize,
}

fn receipt_summary(
    source: &[EventEnvelope<ChainEvent>],
    sink: &[EventEnvelope<ChainEvent>],
) -> ReceiptSummary {
    let source_ids = source
        .iter()
        .filter(|envelope| CsvRecord::from_event(&envelope.event).is_some())
        .map(|envelope| envelope.event.id)
        .collect::<HashSet<EventId>>();
    let mut summary = ReceiptSummary::default();
    let mut committed_parents = HashSet::new();

    for envelope in sink {
        let ChainEventContent::Delivery(payload) = &envelope.event.content else {
            continue;
        };
        assert_eq!(payload.destination, "csv_out");
        let parent = envelope.event.causality.parent_ids.first().copied();
        match (&payload.result, parent) {
            (DeliveryResult::Buffered { .. }, Some(parent)) if source_ids.contains(&parent) => {
                summary.buffered += 1;
            }
            (DeliveryResult::Success { .. }, Some(parent)) if source_ids.contains(&parent) => {
                assert!(
                    committed_parents.insert(parent),
                    "each input receives one terminal CSV receipt"
                );
                summary.committed += 1;
            }
            (DeliveryResult::Success { .. }, None) => summary.audits += 1,
            other => panic!("unexpected CSV delivery evidence: {other:?}"),
        }
    }
    summary
}

fn csv_row_count(path: &Path) -> usize {
    csv::Reader::from_path(path)
        .expect("CSV output opens")
        .records()
        .collect::<Result<Vec<_>, _>>()
        .expect("CSV rows parse")
        .len()
}

async fn wait_for_three_buffered_receipts(run_dir: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "CSV provisional receipts were not committed in time"
        );
        let observed = std::panic::AssertUnwindSafe(replay_testkit::read_stage_envelopes_appended(
            run_dir, "csv_out",
        ))
        .catch_unwind()
        .await
        .unwrap_or_default();
        if observed
            .iter()
            .filter(|envelope| {
                matches!(
                    &envelope.event.content,
                    ChainEventContent::Delivery(payload)
                        if matches!(payload.result, DeliveryResult::Buffered { .. })
                )
            })
            .count()
            >= 3
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Spawned by the parent acceptance test. `process::exit` deliberately skips
/// every destructor and cleanup hook, modelling a process loss after the
/// provisional receipts commit but before CsvSink flushes its private buffer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "crash fixture; run by crash_before_flush_replays_unreceipted_csv_work_under_truncated_eof"]
async fn crash_fixture_process() {
    let Some(journal_base) = std::env::var_os("FLOWIP_134H_CRASH_JOURNALS") else {
        return;
    };
    let Some(csv_path) = std::env::var_os("FLOWIP_134H_CRASH_CSV") else {
        return;
    };
    let journal_base = PathBuf::from(journal_base);
    let csv_path = PathBuf::from(csv_path);
    let _handle = stalling_flow(journal_base.clone(), csv_path)
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .expect("crash fixture flow builds");
    let run_dir = replay_testkit::latest_run_dir(&journal_base);
    wait_for_three_buffered_receipts(&run_dir).await;
    std::process::exit(86);
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_csv_flushes_and_drains_under_poison_termination() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let csv_path = temp.path().join("poison.csv");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(poison_flow(journal_base.clone(), csv_path.clone()))
        .await
        .expect("Poison-terminated CSV flow completes");

    let run = replay_testkit::latest_run_dir(&journal_base);
    let source = replay_testkit::read_stage_envelopes_appended(&run, "records").await;
    let sink = replay_testkit::read_stage_envelopes_appended(&run, "csv_out").await;
    assert_eq!(eof_kinds(&source), vec![EofKind::Poison]);
    assert_eq!(
        receipt_summary(&source, &sink),
        ReceiptSummary {
            buffered: 3,
            committed: 3,
            audits: 2,
        }
    );
    assert_eq!(csv_row_count(&csv_path), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_before_flush_replays_unreceipted_csv_work_under_truncated_eof() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let csv_path = temp.path().join("truncated.csv");

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "crash_fixture_process", "--ignored"])
        .env("FLOWIP_134H_CRASH_JOURNALS", &journal_base)
        .env("FLOWIP_134H_CRASH_CSV", &csv_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("crash fixture process starts");
    assert_eq!(status.code(), Some(86), "fixture exits at the crash point");
    let interrupted = replay_testkit::latest_run_dir(&journal_base);

    let interrupted_source =
        replay_testkit::read_stage_envelopes_appended(&interrupted, "records").await;
    let interrupted_sink =
        replay_testkit::read_stage_envelopes_appended(&interrupted, "csv_out").await;
    assert!(
        eof_kinds(&interrupted_source).is_empty(),
        "the crashed source commits no terminal EOF"
    );
    assert_eq!(
        receipt_summary(&interrupted_source, &interrupted_sink),
        ReceiptSummary {
            buffered: 3,
            committed: 0,
            audits: 0,
        },
        "the crash-shaped run stops before destination flush or terminal receipts"
    );
    assert_eq!(
        std::fs::read_to_string(&csv_path).expect("crashed CSV file exists"),
        "",
        "process loss skips CsvSink's in-memory rows and writer buffer"
    );

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            interrupted.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(stalling_flow(journal_base.clone(), csv_path.clone()))
        .await
        .expect("replay of the interrupted CSV flow completes");

    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(interrupted, replay);
    let replay_source = replay_testkit::read_stage_envelopes_appended(&replay, "records").await;
    let replay_sink = replay_testkit::read_stage_envelopes_appended(&replay, "csv_out").await;
    assert_eq!(eof_kinds(&replay_source), vec![EofKind::Truncated]);
    assert_eq!(
        receipt_summary(&replay_source, &replay_sink),
        ReceiptSummary {
            buffered: 3,
            committed: 3,
            audits: 0,
        },
        "Truncated suppresses audits but still flushes and receipts every buffered input"
    );
    assert_eq!(csv_row_count(&csv_path), 3);
}
