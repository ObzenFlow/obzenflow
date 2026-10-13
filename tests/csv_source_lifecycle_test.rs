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

/// One run of the shipped example: a config file naming its inputs and
/// output under `[app]`, and its journal root under `journal.root`.
#[derive(Clone)]
struct DemoRun {
    config: PathBuf,
    customers_csv: PathBuf,
    tickets_csv: PathBuf,
    output_csv: PathBuf,
    journals_dir: PathBuf,
}

impl DemoRun {
    fn new(fixtures: &Path, out_root: &Path) -> anyhow::Result<Self> {
        let run = Self {
            config: out_root.join("obzenflow.toml"),
            customers_csv: fixtures.join("customers.csv"),
            tickets_csv: fixtures.join("tickets.csv"),
            output_csv: out_root.join("outputs/enriched_tickets.csv"),
            journals_dir: out_root.join("logs"),
        };
        std::fs::create_dir_all(out_root.join("outputs"))?;
        let quoted = |path: &Path| toml::Value::String(path.display().to_string()).to_string();
        std::fs::write(
            &run.config,
            format!(
                "[journal]\nroot = {}\n\n[app]\ncustomers_csv = {}\ntickets_csv = {}\noutput_csv = {}\n",
                quoted(&run.journals_dir),
                quoted(&run.customers_csv),
                quoted(&run.tickets_csv),
                quoted(&run.output_csv),
            ),
        )?;
        Ok(run)
    }

    fn args(&self, extra: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
        [
            OsString::from("csv_demo_support_sla"),
            OsString::from("--config"),
            self.config.clone().into_os_string(),
        ]
        .into_iter()
        .chain(extra)
        .collect()
    }
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

    let paths = DemoRun::new(&fixtures, &outputs)?;
    let live_args = paths.args([]);
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            Presentation::new(Banner::new("CSV lifecycle proof")),
            live_args,
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
    // Wire again exactly as main does, with the same settings. Any attempted
    // file/header read would fail before replay could complete.
    let replay_args = paths.args([
        OsString::from("--replay-from"),
        live.clone().into_os_string(),
    ]);
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            Presentation::new(Banner::new("CSV lifecycle proof")),
            replay_args,
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

// Three runs of this input fit the lane's 60 s budget on CI runners, about
// three times slower than a developer machine; the stop still lands early.
const LONG_TICKETS: usize = 1_000;
const TICKETS_AT_CUT: usize = 100;

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

async fn run_example_with(paths: &DemoRun, args: Vec<OsString>) -> anyhow::Result<()> {
    let args = paths.args(args);
    tokio::task::spawn_blocking(move || {
        example::flow::run_example(
            Presentation::new(Banner::new("CSV interruption proof")),
            args,
        )
    })
    .await?
}

/// The shipped example as a child process, configured the way a platform
/// configures processes: through `OBZENFLOW_CONFIG`.
fn spawn_child(
    paths: &DemoRun,
    shutdown_timeout_secs: Option<&str>,
) -> std::io::Result<tokio::process::Child> {
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args(["--ignored", "--exact", "csv_example_child", "--nocapture"])
        .env("OBZENFLOW_CONFIG", &paths.config);
    if let Some(seconds) = shutdown_timeout_secs {
        command.env("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS", seconds);
    }
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
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

async fn source_eofs(run: &Path, stage: &str) -> anyhow::Result<Vec<EofKind>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let file = manifest["stages"][stage]["data_journal_file"]
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
#[ignore = "subprocess fixture; run by the interruption tests below"]
fn csv_example_child() {
    assert!(
        std::env::var_os("OBZENFLOW_CONFIG").is_some(),
        "the parent selects the config"
    );
    example::flow::run_example(
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
    let control_paths = DemoRun::new(&control_fixtures, &temp.path().join("control"))?;
    run_example_with(&control_paths, Vec::new()).await?;
    let control = latest_run(&control_paths.journals_dir)?;

    let fixtures = temp.path().join("interrupted/fixtures");
    let outputs = temp.path().join("interrupted");
    write_long_fixtures(&fixtures)?;
    let paths = DemoRun::new(&fixtures, &outputs)?;
    let mut child = spawn_child(&paths, None)?;
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
    assert_eq!(
        source_eofs(&interrupted, "tickets").await?,
        [EofKind::Truncated]
    );
    let admitted = facts(&interrupted, "tickets").await?.len();
    assert!(
        (TICKETS_AT_CUT..LONG_TICKETS).contains(&admitted),
        "the stop lands inside the input: {admitted}"
    );

    run_example_with(
        &paths,
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

/// FLOWIP-134i B20: a SIGTERM while the tickets open is held on a FIFO ends the
/// process once the stop deadline aborts the source and the application's
/// runtime abandons the call after `runtime.shutdown_timeout`.
#[tokio::test(flavor = "multi_thread")]
async fn csv_example_exits_while_an_open_is_held() -> anyhow::Result<()> {
    use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};
    let temp = tempfile::tempdir()?;
    let fixtures = temp.path().join("fixtures");
    let outputs = temp.path().join("held");
    std::fs::create_dir_all(&fixtures)?;
    std::fs::write(
        fixtures.join("customers.csv"),
        include_str!("../examples/csv_demo_support_sla/fixtures/customers.csv"),
    )?;
    let paths = DemoRun::new(&fixtures, &outputs)?;
    // Opening a FIFO for reading blocks until a writer appears; none does.
    assert!(std::process::Command::new("mkfifo")
        .arg(&paths.tickets_csv)
        .status()?
        .success());
    let mut child = spawn_child(&paths, Some("1"))?;
    // Both sources start together; the customers finish while the tickets
    // open waits on the FIFO.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let run = loop {
        if let Ok(run) = latest_run(&paths.journals_dir) {
            if source_eofs(&run, "customers").await? == [EofKind::Natural] {
                break run;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the customers never finished"
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
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(20), child.wait())
            .await
            .is_ok(),
        "the process exits within its stop deadline and shutdown timeout while the open is held"
    );
    // A stop before or after the open would have written `Truncated` (B5).
    assert_eq!(
        source_eofs(&run, "tickets").await?,
        Vec::<EofKind>::new(),
        "the stop aborted the source inside its open"
    );
    Ok(())
}

/// Delivery receipts a stage committed.
async fn deliveries(run: &Path, stage: &str) -> anyhow::Result<usize> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json"))?)?;
    let file = manifest["stages"][stage]["data_journal_file"]
        .as_str()
        .unwrap();
    Ok(
        replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(file))
            .await
            .iter()
            .filter(|row| {
                matches!(
                    row.payload,
                    obzenflow_core::event::ChainPayload::Delivery(_)
                )
            })
            .count(),
    )
}

/// FLOWIP-134i B21: a SIGTERM while the CSV sink's writes are blocked on a
/// pipe that stopped draining ends the process. The sink writes on the
/// blocking pool, so the stop deadline can abort it and the application's
/// runtime abandons the blocked write after `runtime.shutdown_timeout`.
#[tokio::test(flavor = "multi_thread")]
async fn csv_example_exits_while_its_output_is_blocked() -> anyhow::Result<()> {
    use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};
    let temp = tempfile::tempdir()?;
    let fixtures = temp.path().join("fixtures");
    let outputs = temp.path().join("blocked");
    write_long_fixtures(&fixtures)?;
    let paths = DemoRun::new(&fixtures, &outputs)?;
    assert!(std::process::Command::new("mkfifo")
        .arg(&paths.output_csv)
        .status()?
        .success());
    // A reader that never reads: the sink's open succeeds, then its rows fill
    // the pipe and the next write blocks.
    let stalled_reader = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.output_csv)?;
    let mut child = spawn_child(&paths, Some("1"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let run = loop {
        if let Ok(run) = latest_run(&paths.journals_dir) {
            if source_eofs(&run, "tickets").await? == [EofKind::Natural] {
                break run;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the tickets never finished"
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
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(20), child.wait())
            .await
            .is_ok(),
        "the process exits within its stop deadline and shutdown timeout while a write is blocked"
    );
    assert!(
        deliveries(&run, "csv_out").await? < LONG_TICKETS,
        "the stalled pipe blocked the sink before it delivered every row"
    );
    drop(stalled_reader);
    Ok(())
}
