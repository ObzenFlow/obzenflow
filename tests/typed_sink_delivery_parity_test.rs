// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134h journal oracle for immediate and deferred typed sink settlement.

use async_trait::async_trait;
use obzenflow::sinks::CsvSink;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EventEnvelope, StageFatalCode, StageFatalReason,
    StageFatalRecorded,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventId, StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkBufferedOutcome, SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome,
    TypedFiniteSourceHandler, TypedSinkConsumeReport, TypedSinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NestedValue {
    region: String,
    score: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SinkRecord {
    id: u64,
    label: String,
    nested: NestedValue,
    tags: Vec<String>,
    optional: Option<String>,
}

impl TypedPayload for SinkRecord {
    const EVENT_TYPE: &'static str = "flowip_134h.sink_record";
}

fn fixtures() -> Vec<SinkRecord> {
    vec![
        SinkRecord {
            id: 1,
            label: "alpha".to_string(),
            nested: NestedValue {
                region: "ca".to_string(),
                score: 7,
            },
            tags: vec!["x".to_string(), "y".to_string()],
            optional: None,
        },
        SinkRecord {
            id: 2,
            label: "beta".to_string(),
            nested: NestedValue {
                region: "us".to_string(),
                score: 8,
            },
            tags: vec![],
            optional: Some("present".to_string()),
        },
        SinkRecord {
            id: 3,
            label: "gamma".to_string(),
            nested: NestedValue {
                region: "gb".to_string(),
                score: 9,
            },
            tags: vec!["z".to_string()],
            optional: None,
        },
    ]
}

#[derive(Clone, Debug)]
struct ValuesSource {
    values: Vec<SinkRecord>,
    next: usize,
}

impl ValuesSource {
    fn new() -> Self {
        Self {
            values: fixtures(),
            next: 0,
        }
    }
}

impl TypedFiniteSourceHandler for ValuesSource {
    type Output = SinkRecord;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let Some(value) = self.values.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![value]))
    }
}

#[derive(Clone, Debug)]
struct NamedDestination;

#[async_trait]
impl TypedSinkHandler for NamedDestination {
    type Input = SinkRecord;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::destination(
            "named.destination",
            SinkDeliverySafety::IdempotentProjection,
            Some(serde_json::json!({ "table": "sink_records" })),
        )
    }

    async fn consume(
        &mut self,
        _input: Self::Input,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(
                DeliveryMethod::DatabaseInsert {
                    table: "sink_records".to_string(),
                },
                None,
            )
            .with_items(1),
        ))
    }
}

fn connector_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to build typed CSV sink: {error}"))
}

fn build_parity_flow(journal_base: PathBuf, csv_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let records = ValuesSource::new();
        let csv = CsvSink::<SinkRecord>::builder()
            .path(&csv_path)
            .columns(["id", "label", "nested", "tags", "optional"])
            .headers(["ID", "Label", "Nested", "Tags", "Optional"])
            .buffer_size(2)
            .auto_flush(false)
            .build()
            .map_err(connector_error)?;
        let named = NamedDestination;

        Ok(flow! {
            name: "typed_sink_delivery_parity",
            journals: disk_journals(journal_base),

            stages: {
                records = source!(SinkRecord => records);
                csv_out = sink!(SinkRecord => csv);
                named_out = sink!(SinkRecord => named);
            },

            topology: {
                records |> csv_out;
                records |> named_out;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory exists")
        .map(|entry| entry.expect("flow directory entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow produced an archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest is readable"),
    )
    .expect("manifest parses")
}

async fn read_stage_journal(
    run_dir: &Path,
    stage_name: &str,
    manifest_field: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name][manifest_field]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names {manifest_field} for stage {stage_name}"));
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    let mut reader = journal.reader().await.expect("stage journal reader opens");
    let mut events = Vec::new();
    while let Some(event) = reader.next().await.expect("stage journal reads") {
        events.push(event);
    }
    events
}

async fn read_stage(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "data_journal_file").await
}

async fn read_stage_errors(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "error_journal_file").await
}

#[derive(Debug, PartialEq)]
struct DeliveryEvidence {
    parent_record: Option<u64>,
    result: serde_json::Value,
    destination: String,
    method: serde_json::Value,
    bytes_processed: Option<u64>,
    items_delivered: Option<u64>,
    middleware_context: Option<serde_json::Value>,
}

fn delivery_evidence(
    source: &[EventEnvelope<ChainEvent>],
    sink: &[EventEnvelope<ChainEvent>],
) -> Vec<DeliveryEvidence> {
    let inputs = source
        .iter()
        .filter_map(|envelope| {
            SinkRecord::from_event(&envelope.event).map(|record| (envelope.event.id, record.id))
        })
        .collect::<HashMap<EventId, u64>>();

    sink.iter()
        .filter_map(|envelope| {
            let ChainEventContent::Delivery(payload) = &envelope.event.content else {
                return None;
            };
            Some(DeliveryEvidence {
                parent_record: envelope
                    .event
                    .causality
                    .parent_ids
                    .first()
                    .and_then(|parent| inputs.get(parent))
                    .copied(),
                result: serde_json::to_value(&payload.result).expect("result serializes"),
                destination: payload.destination.clone(),
                method: serde_json::to_value(&payload.delivery_method)
                    .expect("delivery method serializes"),
                bytes_processed: payload.bytes_processed,
                items_delivered: payload.items_delivered,
                middleware_context: payload.middleware_context.clone(),
            })
        })
        .collect()
}

async fn run(journal_base: &Path, csv_path: &Path, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_parity_flow(
            journal_base.to_path_buf(),
            csv_path.to_path_buf(),
        ))
        .await
        .expect("typed sink parity flow completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_csv_and_named_sink_have_live_replay_journal_parity() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let csv_path = temp.path().join("sink-records.csv");

    run(&journal_base, &csv_path, None).await;
    let live = latest_run_dir(&journal_base);
    let live_source = read_stage(&live, "records").await;
    let live_csv = read_stage(&live, "csv_out").await;
    let live_named = read_stage(&live, "named_out").await;
    let live_csv_evidence = delivery_evidence(&live_source, &live_csv);
    let live_named_evidence = delivery_evidence(&live_source, &live_named);
    let live_file = std::fs::read_to_string(&csv_path).expect("live CSV is readable");

    assert_eq!(live_csv_evidence.len(), 8);
    assert_eq!(
        live_csv_evidence
            .iter()
            .filter(|evidence| evidence.result["result"] == "buffered")
            .count(),
        3
    );
    assert_eq!(
        live_csv_evidence
            .iter()
            .filter(|evidence| evidence.parent_record.is_some())
            .count(),
        6,
        "every input has one provisional and one terminal receipt"
    );
    for id in 1..=3 {
        let outcomes = live_csv_evidence
            .iter()
            .filter(|evidence| evidence.parent_record == Some(id))
            .map(|evidence| evidence.result["result"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes, vec!["buffered", "success"]);
    }
    assert!(live_csv_evidence.iter().all(|evidence| {
        if evidence.parent_record == Some(3) && evidence.result["result"] == "success" {
            // Lifecycle commit receipts historically retain their authored
            // empty destination; preserving that row is part of the migration
            // oracle. Per-input and audit rows still prove final-name fallback.
            evidence.destination.is_empty()
        } else {
            evidence.destination == "csv_out"
        }
    }));
    assert!(live_csv_evidence
        .iter()
        .all(|evidence| evidence.bytes_processed.is_none()));
    assert!(live_csv_evidence
        .iter()
        .all(|evidence| evidence.items_delivered.is_none()));

    assert_eq!(live_named_evidence.len(), 3);
    assert!(live_named_evidence.iter().all(|evidence| {
        evidence.destination == "named.destination"
            && evidence.parent_record.is_some()
            && evidence.result["result"] == "success"
            && evidence.method["database_insert"]["table"] == "sink_records"
            && evidence.items_delivered == Some(1)
            && evidence.bytes_processed.is_none()
    }));

    let mut csv = csv::Reader::from_reader(live_file.as_bytes());
    assert_eq!(
        csv.headers()
            .expect("CSV headers")
            .iter()
            .collect::<Vec<_>>(),
        vec!["ID", "Label", "Nested", "Tags", "Optional"]
    );
    let rows = csv
        .records()
        .map(|record| {
            record
                .expect("CSV record")
                .iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec![
                "1",
                "alpha",
                r#"{"region":"ca","score":7}"#,
                r#"["x","y"]"#,
                "",
            ],
            vec!["2", "beta", r#"{"region":"us","score":8}"#, "[]", "present",],
            vec!["3", "gamma", r#"{"region":"gb","score":9}"#, r#"["z"]"#, "",],
        ]
    );

    run(&journal_base, &csv_path, Some(&live)).await;
    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    let replay_source = read_stage(&replay, "records").await;
    let replay_csv = read_stage(&replay, "csv_out").await;
    let replay_named = read_stage(&replay, "named_out").await;
    assert_eq!(
        delivery_evidence(&replay_source, &replay_csv),
        live_csv_evidence
    );
    assert_eq!(
        delivery_evidence(&replay_source, &replay_named),
        live_named_evidence
    );
    assert_eq!(
        std::fs::read_to_string(&csv_path).expect("replay CSV is readable"),
        live_file
    );

    let verification = verify_run_dirs(&live, &replay, &VerifyOptions::default())
        .expect("live/replay verification executes");
    assert_eq!(
        verification.exit_code(),
        0,
        "live/replay journals must certify as identical"
    );
}

#[derive(Clone, Debug)]
struct InvalidBufferedSink;

#[async_trait]
impl TypedSinkHandler for InvalidBufferedSink {
    type Input = SinkRecord;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::safety_only(SinkDeliverySafety::IdempotentProjection)
    }

    async fn consume(
        &mut self,
        _input: Self::Input,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        Ok(TypedSinkConsumeReport::buffered(SinkBufferedOutcome::new(
            DeliveryMethod::Noop,
            None,
        )))
    }
}

fn build_invalid_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let mut records = ValuesSource::new();
        records.values.truncate(1);
        let invalid = InvalidBufferedSink;
        Ok(flow! {
            name: "typed_sink_invalid_settlement",
            journals: disk_journals(journal_base),

            stages: {
                records = source!(SinkRecord => records);
                invalid = sink!(SinkRecord => invalid);
            },

            topology: {
                records |> invalid;
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_settlement_records_stage_fatal_without_a_delivery_receipt() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_invalid_flow(journal_base.clone())),
    )
    .await
    .expect("invalid settlement terminates promptly");
    result.expect_err("invalid settlement must fail the flow");

    let run = latest_run_dir(&journal_base);
    let data = read_stage(&run, "invalid").await;
    assert!(data
        .iter()
        .all(|envelope| { !matches!(envelope.event.content, ChainEventContent::Delivery(_)) }));

    let fatals = read_stage_errors(&run, "invalid")
        .await
        .iter()
        .filter_map(|envelope| StageFatalRecorded::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(fatals.len(), 1);
    assert_eq!(fatals[0].code, StageFatalCode::Protocol);
    assert_eq!(fatals[0].reason, StageFatalReason::ProtocolInputIntegrity);
    assert!(fatals[0]
        .detail
        .contains("buffered primary outcome without deferring"));
}
