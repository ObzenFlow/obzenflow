// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134h journal oracle for immediate and deferred typed sink settlement.

use crate::replay_testkit;

use async_trait::async_trait;
use obzenflow::stages::sinks::{CsvProjection, CsvSink};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::common::HandlerError;
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

#[derive(Clone, Debug)]
struct SinkRecordProjection;

impl CsvProjection for SinkRecordProjection {
    type Input = SinkRecord;
    type Row = SinkRecord;

    fn project(&self, input: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(input)
    }
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
impl InlineSink for NamedDestination {
    type Input = SinkRecord;

    fn describe(&self) -> SinkDescription {
        SinkDescription::destination(
            "named.destination",
            DeliveryMethod::DatabaseInsert {
                table: "sink_records".to_string(),
            },
        )
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: Self::Input) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

fn connector_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to build typed CSV sink: {error}"))
}

fn build_parity_flow(journal_base: PathBuf, csv_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let records = ValuesSource::new();
        let csv = CsvSink::builder(SinkRecordProjection)
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
) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name][manifest_field]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names {manifest_field} for stage {stage_name}"));
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(journal_file)).await
}

async fn read_stage(run_dir: &Path, stage_name: &str) -> Vec<JournalRecord<ChainPayload>> {
    read_stage_journal(run_dir, stage_name, "data_journal_file").await
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
    source: &[JournalRecord<ChainPayload>],
    sink: &[JournalRecord<ChainPayload>],
) -> Vec<DeliveryEvidence> {
    let inputs = source
        .iter()
        .filter_map(|envelope| {
            SinkRecord::from_event(&envelope.authored())
                .map(|record| (envelope.commitment(), (envelope, record.id)))
        })
        .collect::<HashMap<_, _>>();

    sink.iter()
        .filter_map(|envelope| {
            let ChainPayload::Delivery(payload) = &envelope.payload else {
                return None;
            };
            let (input, record_id) = inputs
                .get(&payload.subject.input)
                .expect("receipt names an exact committed source input");
            assert!(payload.subject.matches_record(input));
            assert!(envelope
                .envelope
                .provenance
                .event
                .causality
                .parent_ids
                .contains(input.id()));
            Some(DeliveryEvidence {
                parent_record: Some(*record_id),
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

fn audit_evidence(sink: &[JournalRecord<ChainPayload>]) -> Vec<serde_json::Value> {
    sink.iter()
        .filter_map(|record| {
            let ChainPayload::Execution(
                obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SinkAudit(
                    audit,
                ),
            ) = &record.payload
            else {
                return None;
            };
            let mut value = serde_json::to_value(audit).expect("audit serializes");
            value.as_object_mut().unwrap().remove("processed_at");
            assert!(
                value.get("subject").is_none(),
                "audit grants no settlement authority"
            );
            Some(
                serde_json::json!({"descriptor": record.authored().descriptor(), "payload": value}),
            )
        })
        .collect()
}

async fn run(journal_base: &Path, csv_path: &Path, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    replay_testkit::unsynced_application()
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

    assert_eq!(live_csv_evidence.len(), 6);
    let live_audits = audit_evidence(&live_csv);
    assert_eq!(
        live_audits
            .iter()
            .map(|row| row["descriptor"]["event_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["sink.flush_succeeded", "sink.drain_succeeded"]
    );
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
    assert!(live_csv_evidence
        .iter()
        .all(|evidence| evidence.destination == "csv_out"));
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
    assert_eq!(audit_evidence(&replay_csv), live_audits);
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
