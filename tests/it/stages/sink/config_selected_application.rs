// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::application::{ApplicationError, FlowApplication};
use obzenflow::stages::sinks::{self, CsvProjection, CsvSink};
use obzenflow::stages::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::HandlerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AuditRecord {
    sequence: u64,
    action: String,
}

impl TypedPayload for AuditRecord {
    const EVENT_TYPE: &'static str = "flowip_010o.audit_record";
}

#[derive(Clone, Debug)]
struct AuditCsvProjection;

impl CsvProjection for AuditCsvProjection {
    type Input = AuditRecord;
    type Row = AuditRecord;

    fn project(&self, input: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(input)
    }
}

fn audit_records() -> Vec<AuditRecord> {
    vec![
        AuditRecord {
            sequence: 1,
            action: "created".to_string(),
        },
        AuditRecord {
            sequence: 2,
            action: "approved".to_string(),
        },
    ]
}

fn connector_build_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to construct audit CSV sink: {error}"))
}

fn selectable_audit_flow(
    archive_path: PathBuf,
    preview_writes: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input = sources::ValuesSource::new(audit_records());

        let preview_writes_for_formatter = Arc::clone(&preview_writes);
        let preview_sink = sinks::ConsoleSink::new(move |record: &AuditRecord| {
            preview_writes_for_formatter.fetch_add(1, Ordering::SeqCst);
            format!("{}: {}", record.sequence, record.action)
        });
        let archive_sink = CsvSink::builder(AuditCsvProjection)
            .path(archive_path)
            .columns(["sequence", "action"])
            .auto_flush(true)
            .build()
            .map_err(connector_build_error)?;

        Ok(flow! {
            name: "config_selected_sink_conformance",
            journals: memory_journals(),

            stages: {
                input = source!(AuditRecord => input);
                audit_output = sink!(
                    AuditRecord => handler_set!(preview_sink, archive_sink)
                )?;
            },

            topology: {
                input |> audit_output;
            }
        })
    })
}

fn write_selection(config_path: &Path, selected: &str) {
    std::fs::write(
        config_path,
        format!("[sinks.stages.audit_output]\nhandler = \"{selected}\"\n"),
    )
    .expect("write sink-selection config");
}

async fn run_with_selection(
    config_path: PathBuf,
    archive_path: PathBuf,
    preview_writes: Arc<AtomicUsize>,
) -> Result<(), ApplicationError> {
    FlowApplication::builder()
        .with_config_file(config_path)
        .with_cli_args(["config-selected-sink-conformance"])
        .run_async(selectable_audit_flow(archive_path, preview_writes))
        .await
}

#[tokio::test]
async fn invalid_selection_fails_before_any_sink_performs_work() {
    let tempdir = tempfile::tempdir().expect("invalid sink-selection tempdir");
    let config_path = tempdir.path().join("obzenflow.toml");
    let archive_path = tempdir.path().join("audit.csv");
    let preview_writes = Arc::new(AtomicUsize::new(0));
    write_selection(&config_path, "missing_sink");

    let result = run_with_selection(
        config_path,
        archive_path.clone(),
        Arc::clone(&preview_writes),
    )
    .await;

    let detail = match result {
        Err(ApplicationError::FlowBuildFailed(detail)) => detail,
        other => panic!("invalid sink selection returned the wrong result: {other:?}"),
    };
    assert!(detail.contains("sinks.stages.audit_output.handler"));
    assert!(detail.contains("preview_sink"));
    assert!(detail.contains("archive_sink"));
    assert!(detail.contains("missing_sink"));
    assert_eq!(preview_writes.load(Ordering::SeqCst), 0);
    assert!(
        !archive_path.exists(),
        "invalid selection must fail before the resource sink opens"
    );
}
