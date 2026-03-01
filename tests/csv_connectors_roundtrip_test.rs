// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::sinks::CsvSink;
use obzenflow::sources::CsvSource;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

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
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");
    let journals_path = temp_dir.path().join("journals");

    std::fs::write(
        &input_path,
        "carrier,delay_minutes\nAA,5\nBB,not_a_number\nCC,10\n",
    )?;

    let source = CsvSource::typed_from_file::<FlightData>(&input_path)?;
    let sink = CsvSink::builder()
        .path(&output_path)
        .auto_flush(true)
        .build()?;

    let handle = flow! {
        name: "csv_connectors_roundtrip_test",
        journals: disk_journals(journals_path),
        middleware: [],

        stages: {
            src = source!("src" => source);
            csv = sink!("csv" => sink);
        },

        topology: {
            src |> csv;
        }
    }
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
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");
    let journals_path = temp_dir.path().join("journals");

    std::fs::write(&input_path, "name,age\nalice,007\n")?;

    let source = CsvSource::from_file(&input_path)?;
    let sink = CsvSink::builder()
        .path(&output_path)
        .auto_flush(true)
        .build()?;

    let handle = flow! {
        name: "csv_untyped_connectors_roundtrip_test",
        journals: disk_journals(journals_path),
        middleware: [],

        stages: {
            src = source!("src" => source);
            csv = sink!("csv" => sink);
        },

        topology: {
            src |> csv;
        }
    }
    .await?;

    handle.run().await?;

    let out = std::fs::read_to_string(&output_path)?;
    assert!(out.contains("age,name"));
    assert!(out.contains("007,alice"));
    assert_eq!(out.lines().count(), 2);

    Ok(())
}
