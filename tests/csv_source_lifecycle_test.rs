// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122e: the shipped application's ordinary entry point can replay after
//! losing both original inputs. All fixtures and archives belong to this test.

mod replay_testkit;

#[allow(dead_code)]
#[path = "../examples/csv_demo_support_sla/support.rs"]
mod example;

use obzenflow::application::{Banner, Presentation};
use obzenflow::stages::sources::{
    CsvRowDecoder, CsvSource, FiniteSourceConnector, PositionedFiniteReader,
    SourceReaderInitContext,
};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::{ChainEvent, StageId};
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
    let rows = replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(file)).await;
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

const LONG_TICKETS: usize = 3_000;
const TICKETS_AT_CUT: usize = 200;
const CHILD_FIXTURES: &str = "OBZENFLOW_134I_CSV_FIXTURES";
const CHILD_OUTPUTS: &str = "OBZENFLOW_134I_CSV_OUTPUTS";

/// The shipped customers, and the shipped tickets repeated under fresh ids
/// until the stream is long enough for a stop to land partway through it.
fn write_long_fixtures(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(
        dir.join("customers.csv"),
        include_str!("../examples/csv_demo_support_sla/fixtures/customers.csv"),
    )?;
    let mut shipped = include_str!("../examples/csv_demo_support_sla/fixtures/tickets.csv").lines();
    let mut tickets = format!("{}\n", shipped.next().expect("a header"));
    let rows: Vec<&str> = shipped.collect();
    for index in 0..LONG_TICKETS {
        let (_, fields) = rows[index % rows.len()]
            .split_once(',')
            .expect("a ticket id column");
        tickets.push_str(&format!("TKT-{:06},{fields}\n", index + 1));
    }
    std::fs::write(dir.join("tickets.csv"), tickets)?;
    Ok(())
}

async fn run_example_with(
    paths: example::flow::DemoPaths,
    args: Vec<OsString>,
) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            paths,
            Presentation::new(Banner::new("CSV interruption proof")),
            std::iter::once(OsString::from("csv_demo_support_sla")).chain(args),
        )
    })
    .await?
}

/// Tickets a running archive has committed, read through the supported reader.
async fn committed_tickets(run: &Path) -> usize {
    use obzenflow::journal::read::{open_disk_run, RunRecordData};
    let Ok(mut snapshot) = open_disk_run(run).await else {
        return 0;
    };
    let mut tickets = 0;
    while let Ok(Some(record)) = snapshot.next().await {
        let is_ticket = record
            .journal
            .stage
            .as_ref()
            .is_some_and(|stage| stage.key == "tickets");
        if is_ticket
            && matches!(&record.record, RunRecordData::Chain(row) if row.consumes_data_credit())
        {
            tickets += 1;
        }
    }
    tickets
}

async fn tickets_eof(run: &Path) -> anyhow::Result<Vec<EofKind>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let file = manifest["stages"]["tickets"]["data_journal_file"]
        .as_str()
        .unwrap();
    Ok(
        replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(file))
            .await
            .iter()
            .filter_map(|row| match &row.payload {
                obzenflow_core::event::ChainPayload::FlowControl(control) => control.eof_kind(),
                _ => None,
            })
            .collect(),
    )
}

#[test]
#[ignore = "subprocess fixture; run by csv_example_resumes_an_interrupted_run"]
fn csv_example_child() {
    let fixtures = PathBuf::from(std::env::var_os(CHILD_FIXTURES).expect("the parent's fixtures"));
    let outputs = PathBuf::from(std::env::var_os(CHILD_OUTPUTS).expect("the parent's outputs"));
    example::flow::run_example(
        example::flow::DemoPaths::resolve_in(&fixtures, &outputs).expect("the example's paths"),
        Presentation::new(Banner::new("CSV interruption proof")),
        [OsString::from("csv_demo_support_sla")],
    )
    .expect("a stopped application exits cleanly");
}

/// FLOWIP-134i B3: the real CSV support-SLA application, stopped by SIGTERM
/// partway through its tickets, resumes from its archive to the business
/// result of an uninterrupted control run.
#[tokio::test(flavor = "multi_thread")]
async fn csv_example_resumes_an_interrupted_run() -> anyhow::Result<()> {
    use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};
    let temp = tempfile::tempdir()?;

    let control_fixtures = temp.path().join("control/fixtures");
    write_long_fixtures(&control_fixtures)?;
    let control_paths =
        example::flow::DemoPaths::resolve_in(&control_fixtures, &temp.path().join("control"))?;
    run_example_with(control_paths.clone(), Vec::new()).await?;
    let control = latest_run(&control_paths.journals_dir)?;

    let fixtures = temp.path().join("interrupted/fixtures");
    let outputs = temp.path().join("interrupted");
    write_long_fixtures(&fixtures)?;
    let paths = example::flow::DemoPaths::resolve_in(&fixtures, &outputs)?;
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", "csv_example_child", "--nocapture"])
        .env(CHILD_FIXTURES, &fixtures)
        .env(CHILD_OUTPUTS, &outputs)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    // The journal decides the cut; the deadline only fails a fixture that
    // never reaches it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let interrupted = loop {
        if let Ok(run) = latest_run(&paths.journals_dir) {
            if committed_tickets(&run).await >= TICKETS_AT_CUT {
                break run;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the tickets never reached the cut"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let pid = Pid::from_u32(child.id().expect("the application is running"));
    let mut system = System::new();
    assert!(system.refresh_process(pid));
    assert_eq!(
        system.process(pid).unwrap().kill_with(Signal::Term),
        Some(true)
    );
    let status = tokio::time::timeout(std::time::Duration::from_secs(60), child.wait()).await??;
    assert!(status.success(), "SIGTERM drains the application: {status}");

    // The stop is honest: the ticket stream ends truncated, partway through.
    assert_eq!(tickets_eof(&interrupted).await?, [EofKind::Truncated]);
    let admitted = facts(&interrupted, "tickets").await?.len();
    assert!(
        (TICKETS_AT_CUT..LONG_TICKETS).contains(&admitted),
        "the stop lands inside the input: {admitted}"
    );

    run_example_with(
        paths.clone(),
        vec![
            OsString::from("--resume-from"),
            interrupted.clone().into_os_string(),
        ],
    )
    .await?;
    let resumed = latest_run(&paths.journals_dir)?;
    assert_ne!(resumed, interrupted);
    for stage in ["customers", "tickets", "enrich"] {
        assert_eq!(
            facts(&resumed, stage).await?,
            facts(&control, stage).await?,
            "{stage} equals the uninterrupted control"
        );
    }
    assert_eq!(
        std::fs::read(&paths.output_csv)?,
        std::fs::read(&control_paths.output_csv)?,
        "the delivered output equals the control's"
    );
    Ok(())
}
