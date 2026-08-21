// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122a hard archive-epoch gate at the application boundary.

use async_trait::async_trait;
use obzenflow::sources;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkDescription, SinkOperationResult, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterInitContext,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input(u64);

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "flowip_122a.archive_gate.input";
}

#[derive(Debug)]
struct CountingConnector(Arc<AtomicUsize>);
struct CountingWriter;

#[async_trait]
impl SinkConnector for CountingConnector {
    type Input = Input;
    type Writer = CountingWriter;

    fn describe(&self) -> SinkDescription {
        SinkDescription::destination("archive-gate", DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CountingWriter)
    }
}

#[async_trait]
impl SinkWriter for CountingWriter {
    type Input = Input;

    async fn write(&mut self, _input: Input, _context: SinkWriteContext) -> SinkWriteResult {
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(None).with_items(1),
        ))
    }
}

fn guarded_flow(
    output_root: PathBuf,
    materialisations: Arc<AtomicUsize>,
    opens: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        materialisations.fetch_add(1, Ordering::SeqCst);
        let inputs = sources::finite([Input(1)]);
        let output = CountingConnector(opens);
        Ok(flow! {
            name: "archive_manifest_epoch_gate",
            journals: disk_journals(output_root),

            stages: {
                inputs = source!(Input => inputs);
                output = sink!(Input => output);
            },

            topology: {
                inputs |> output;
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn every_non_current_manifest_shape_fails_before_materialisation_or_connector_io() {
    for (name, manifest) in [
        ("missing", r#"{"flow_id":"old"}"#),
        ("numeric", r#"{"manifest_version":3.0}"#),
        ("old", r#"{"manifest_version":"2.0"}"#),
        ("future", r#"{"manifest_version":"4.0"}"#),
        ("object", r#"{"manifest_version":{"major":3}}"#),
        ("malformed", r#"{"manifest_version":"3.0""#),
    ] {
        let temp = tempfile::tempdir().expect("temporary archive gate directory");
        let archive = temp.path().join(format!("archive-{name}"));
        std::fs::create_dir_all(&archive).expect("archive directory");
        std::fs::write(archive.join("run_manifest.json"), manifest).expect("raw manifest fixture");
        let output_root = temp.path().join("must-not-exist");
        let materialisations = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let args = vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.into_os_string(),
        ];

        let result = FlowApplication::builder()
            .with_cli_args(args)
            .run_async(guarded_flow(
                output_root.clone(),
                Arc::clone(&materialisations),
                Arc::clone(&opens),
            ))
            .await;
        assert!(result.is_err(), "{name} manifest must be refused");
        assert_eq!(
            materialisations.load(Ordering::SeqCst),
            0,
            "{name} manifest reached flow materialisation"
        );
        assert_eq!(
            opens.load(Ordering::SeqCst),
            0,
            "{name} manifest opened the sink connector"
        );
        assert!(
            !output_root.exists(),
            "{name} manifest created output journals"
        );
    }
}
