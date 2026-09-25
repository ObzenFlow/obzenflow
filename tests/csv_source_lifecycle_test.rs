// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122e: the shipped application's ordinary entry point can replay after
//! losing both original inputs. All fixtures and archives belong to this test.

#[allow(dead_code)]
#[path = "../examples/csv_demo_support_sla/support.rs"]
mod example;

use obzenflow::application::{Banner, Presentation};
use obzenflow::stages::sources::{
    CsvRowDecoder, CsvSource, FiniteSourceConnector, SourceReaderInitContext,
    TypedFiniteSourceHandler,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

fn context() -> SourceReaderInitContext {
    SourceReaderInitContext {
        stage_id: StageId::new(),
        stage_name: "rows".into(),
        flow_name: "csv_lifecycle".into(),
    }
}

#[test]
fn csv_configuration_is_cold_and_opens_independent_readers() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("rows.csv");
    // Building, cloning and inspecting configuration work before the file exists.
    let source = CsvSource::builder(CsvRowDecoder)
        .path(&path)
        .chunk_size(1)
        .build()?;
    let copy = source.clone();
    assert!(format!("{copy:?}").contains("CsvSource"));
    assert!(copy.open(context()).is_err());
    std::fs::write(&path, "name\nalice\nbob\n")?;
    let mut first = source.open(context())?;
    let mut second = copy.open(context())?;
    let alice = first.next()?.unwrap();
    let bob = first.next()?.unwrap();
    assert_eq!(second.next()?.unwrap(), alice);
    assert!(first.next()?.is_none());
    assert_eq!(second.next()?.unwrap(), bob);
    assert!(second.next()?.is_none());

    // Header validation is acquisition work too, never application wiring.
    let invalid = CsvSource::builder(CsvRowDecoder)
        .path(&path)
        .select_columns(["missing"])
        .build()?;
    assert!(invalid.open(context()).is_err());
    Ok(())
}

fn latest_run(base: &Path) -> anyhow::Result<PathBuf> {
    let mut runs = std::fs::read_dir(base.join("flows"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    runs.retain(|path| path.join("run_manifest.json").exists());
    runs.sort();
    runs.pop().ok_or_else(|| anyhow::anyhow!("no run archive"))
}

async fn facts(run: &Path, stage: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let file = manifest["stages"][stage]["data_journal_file"]
        .as_str()
        .unwrap();
    let journal =
        DiskJournal::<ChainEvent>::with_owner(run.join(file), JournalOwner::stage(StageId::new()))?;
    let rows = journal.read_all_unordered().await?;
    if stage != "enrich" {
        assert_eq!(
            rows.iter().filter(|row| row.is_eof()).count(),
            1,
            "{stage} has one source EOF"
        );
    }
    Ok(rows
        .iter()
        .filter(|row| row.consumes_data_credit())
        .map(|row| row.payload())
        .collect())
}

#[tokio::test]
async fn csv_example_replays_through_application_entry_without_either_input() -> anyhow::Result<()>
{
    let temp = tempfile::tempdir()?;
    let fixtures = temp.path().join("fixtures");
    let outputs = temp.path().join("application");
    std::fs::create_dir(&fixtures)?;
    std::fs::write(
        fixtures.join("customers.csv"),
        include_str!("../examples/csv_demo_support_sla/fixtures/customers.csv"),
    )?;
    std::fs::write(
        fixtures.join("tickets.csv"),
        include_str!("../examples/csv_demo_support_sla/fixtures/tickets.csv"),
    )?;

    let paths = example::flow::DemoPaths::resolve_in(&fixtures, &outputs)?;
    let live_paths = paths.clone();
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            live_paths,
            Presentation::new(Banner::new("CSV lifecycle proof")),
            [OsString::from("csv_demo_support_sla")],
        )
    })
    .await??;
    let live = latest_run(&paths.journals_dir)?;
    let expected_csv = std::fs::read(&paths.output_csv)?;
    let mut expected = Vec::new();
    for (stage, expected_count) in [("customers", 5), ("tickets", 100), ("enrich", 100)] {
        let rows = facts(&live, stage).await?;
        assert_eq!(
            rows.len(),
            expected_count,
            "{stage} records every expected business fact"
        );
        expected.push((stage, rows));
    }

    std::fs::remove_file(&paths.customers_csv)?;
    std::fs::remove_file(&paths.tickets_csv)?;
    // Resolve and wire again exactly as main does, with identical paths. Any
    // attempted file/header read would fail before replay could complete.
    let replay_paths = example::flow::DemoPaths::resolve_in(&fixtures, &outputs)?;
    let archive = live.clone();
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            replay_paths,
            Presentation::new(Banner::new("CSV lifecycle proof")),
            [
                OsString::from("csv_demo_support_sla"),
                OsString::from("--replay-from"),
                archive.into_os_string(),
            ],
        )
    })
    .await??;
    let replay = latest_run(&paths.journals_dir)?;
    assert_ne!(live, replay);
    for (stage, rows) in expected {
        assert_eq!(facts(&replay, stage).await?, rows, "{stage} fact parity");
    }
    assert_eq!(std::fs::read(&paths.output_csv)?, expected_csv);
    let verdict = verify_run_dirs(&live, &replay, &VerifyOptions::default())?;
    assert_eq!(
        verdict.exit_code(),
        0,
        "{}",
        obzenflow_infra::verify::render_verdict(&verdict)
    );
    Ok(())
}
