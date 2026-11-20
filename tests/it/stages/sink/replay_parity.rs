// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use obzenflow::stages::sinks::{CsvProjection, CsvSink};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainEvent, ChainPayload};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::{perform, EffectError, Port};
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::common::HandlerError;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    const EVENT_TYPE: &'static str = "sink_replay_parity.sink_record";
}

#[derive(Clone, Debug)]
struct SinkRecordProjection;

impl CsvProjection for SinkRecordProjection {
    type Input = SinkRecord;
    type Row = SinkRecord;

    fn project(&self, input: &Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(input.clone())
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

#[derive(Clone, Debug, Serialize)]
struct StoreRecord {
    id: u64,
}

#[obzenflow_runtime::effects::port]
trait RecordStorePort: Send + Sync {
    #[effect(type = "sink_replay_parity.store", at_least_once, records = reply)]
    fn store(&self, request: &StoreRecord) -> Result<(), EffectError>;
}

struct RecordStore {
    calls: Arc<AtomicUsize>,
}

#[obzenflow_runtime::effects::system("record-store")]
impl RecordStorePort for RecordStore {
    fn store(&self, _: &StoreRecord) -> Result<(), EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_parity_flow(
    journal_base: PathBuf,
    csv_path: PathBuf,
    calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let records = ValuesSource {
            values: fixtures(),
            next: 0,
        };
        let csv = CsvSink::builder(SinkRecordProjection)
            .path(&csv_path)
            .columns(["id", "label", "nested", "tags", "optional"])
            .headers(["ID", "Label", "Nested", "Tags", "Optional"])
            .build()
            .map_err(|error| {
                FlowBuildError::StageResourcesFailed(format!("failed to build CSV sink: {error}"))
            })?;
        let store = perform(|record: &SinkRecord| StoreRecord { id: record.id }).through(Port::to(
            RecordStore {
                calls: calls.clone(),
            },
        ));

        Ok(flow! {
            name: "sink_replay_parity",
            journals: disk_journals(journal_base),

            stages: {
                records = source!(SinkRecord => records);
                csv_out = sink!(SinkRecord => csv);
                store_out = sink!(SinkRecord => store);
            },

            topology: {
                records |> csv_out;
                records |> store_out;
            }
        })
    })
}

async fn settled_calls(run_dir: &Path, stage_key: &str) -> Vec<Value> {
    let manifest = replay_testkit::archive_manifest(run_dir);
    let journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names the data journal of {stage_key}"));
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(journal))
        .await
        .into_iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) => Some(json!({
                "descriptor_hash": &record.descriptor_hash,
                "outcome": &record.outcome,
            })),
            _ => None,
        })
        .collect()
}

async fn run(journal_base: &Path, csv_path: &Path, replay_from: Option<&Path>) -> usize {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    let calls = Arc::new(AtomicUsize::new(0));
    replay_testkit::unsynced_application()
        .with_cli_args(args)
        .run_async(build_parity_flow(
            journal_base.to_path_buf(),
            csv_path.to_path_buf(),
            calls.clone(),
        ))
        .await
        .expect("sink parity flow completes");
    calls.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_replay_reproduces_recorded_calls() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let csv_path = temp.path().join("sink-records.csv");

    assert_eq!(run(&journal_base, &csv_path, None).await, 3);
    let live = replay_testkit::latest_run_dir(&journal_base);
    let live_csv = settled_calls(&live, "csv_out").await;
    let live_store = settled_calls(&live, "store_out").await;
    assert_eq!((live_csv.len(), live_store.len()), (3, 3));
    let live_file = std::fs::read_to_string(&csv_path).expect("live CSV is readable");
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
            vec!["2", "beta", r#"{"region":"us","score":8}"#, "[]", "present"],
            vec!["3", "gamma", r#"{"region":"gb","score":9}"#, r#"["z"]"#, ""],
        ]
    );

    assert_eq!(
        run(&journal_base, &csv_path, Some(&live)).await,
        0,
        "replay calls no system"
    );
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    assert_eq!(settled_calls(&replay, "csv_out").await, live_csv);
    assert_eq!(settled_calls(&replay, "store_out").await, live_store);
    assert_eq!(
        std::fs::read_to_string(&csv_path).expect("replay CSV is readable"),
        live_file,
        "replay leaves the file as the live run wrote it"
    );

    let verification = verify_run_dirs(&live, &replay, &VerifyOptions::default())
        .expect("live/replay verification executes");
    assert_eq!(
        verification.exit_code(),
        0,
        "live/replay journals must certify as identical"
    );
}
