// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::sinks::CsvSink;
use obzenflow::sources::{CsvRow, CsvSource};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

fn workspace_tempdir() -> anyhow::Result<TempDir> {
    std::fs::create_dir_all("target")?;
    Ok(tempfile::Builder::new()
        .prefix("csv-connectors-")
        .tempdir_in("target")?)
}

fn connector_build_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to construct CSV connector: {error}"))
}

#[derive(Debug, Serialize, Deserialize)]
struct FlightData {
    carrier: String,
    delay_minutes: u32,
}

impl TypedPayload for FlightData {
    const EVENT_TYPE: &'static str = "flight.data";
    const SCHEMA_VERSION: u32 = 1;
}

#[tokio::test]
async fn csv_source_to_sink_roundtrip_skips_bad_rows() -> anyhow::Result<()> {
    let temp_dir = workspace_tempdir()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");
    let journals_path = temp_dir.path().join("journals");

    std::fs::write(
        &input_path,
        "carrier,delay_minutes\nAA,5\nBB,not_a_number\nCC,10\n",
    )?;

    let input_path_for_flow = input_path.clone();
    let output_path_for_flow = output_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = CsvSource::typed_from_file::<FlightData>(&input_path_for_flow)
            .map_err(connector_build_error)?;
        let sink = CsvSink::<FlightData>::builder()
            .path(&output_path_for_flow)
            .auto_flush(true)
            .build()
            .map_err(connector_build_error)?;

        Ok(flow! {
            name: "csv_connectors_roundtrip_test",
            journals: disk_journals(journals_path),

            stages: {
                src = source!(FlightData => source);
                csv = sink!(FlightData => sink);
            },

            topology: {
                src |> csv;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await?;

    handle.run().await?;

    let out = std::fs::read_to_string(&output_path)?;
    assert!(out.contains("carrier,delay_minutes"));
    assert!(out.contains("AA,5"));
    assert!(out.contains("CC,10"));
    assert!(!out.contains("BB"));
    assert_eq!(out.lines().count(), 3);
    assert!(temp_dir.path().exists());

    Ok(())
}

#[tokio::test]
async fn csv_untyped_source_to_sink_roundtrip_preserves_strings() -> anyhow::Result<()> {
    let temp_dir = workspace_tempdir()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");
    let journals_path = temp_dir.path().join("journals");

    std::fs::write(&input_path, "name,age\nalice,007\n")?;

    let input_path_for_flow = input_path.clone();
    let output_path_for_flow = output_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = CsvSource::from_file(&input_path_for_flow).map_err(connector_build_error)?;
        let sink = CsvSink::<CsvRow>::builder()
            .path(&output_path_for_flow)
            .auto_flush(true)
            .build()
            .map_err(connector_build_error)?;

        Ok(flow! {
            name: "csv_untyped_connectors_roundtrip_test",
            journals: disk_journals(journals_path),

            stages: {
                src = source!(CsvRow => source);
                csv = sink!(CsvRow => sink);
            },

            topology: {
                src |> csv;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await?;

    handle.run().await?;

    let out = std::fs::read_to_string(&output_path)?;
    assert!(out.contains("age,name"));
    assert!(out.contains("007,alice"));
    assert_eq!(out.lines().count(), 2);

    Ok(())
}

#[tokio::test]
async fn csv_source_to_buffered_sink_roundtrip_flushes_on_eof() -> anyhow::Result<()> {
    let temp_dir = workspace_tempdir()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");
    let journals_path = temp_dir.path().join("journals");

    std::fs::write(&input_path, "carrier,delay_minutes\nAA,5\nCC,10\n")?;

    let input_path_for_flow = input_path.clone();
    let output_path_for_flow = output_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = CsvSource::typed_from_file::<FlightData>(&input_path_for_flow)
            .map_err(connector_build_error)?;
        let sink = CsvSink::<FlightData>::builder()
            .path(&output_path_for_flow)
            .buffer_size(100)
            .auto_flush(false)
            .build()
            .map_err(connector_build_error)?;

        Ok(flow! {
            name: "csv_buffered_connectors_roundtrip_test",
            journals: disk_journals(journals_path),

            stages: {
                src = source!(FlightData => source);
                csv = sink!(FlightData => sink);
            },

            topology: {
                src |> csv;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await?;

    handle.run().await?;

    let out = std::fs::read_to_string(&output_path)?;
    assert!(out.contains("carrier,delay_minutes"));
    assert!(out.contains("AA,5"));
    assert!(out.contains("CC,10"));
    assert_eq!(out.lines().count(), 3);

    Ok(())
}
