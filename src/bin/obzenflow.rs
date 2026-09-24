// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The facade's outer executable. Journal interpretation and runtime authority
//! remain in library services; this target owns arguments, transport and rendering.

use clap::{Args, Parser, Subcommand};
use obzenflow::application::control::*;
use obzenflow::application::{render_verdict, verify_run_dirs, VerifyOptions};
use obzenflow::journal::read::*;
use obzenflow::journal::{export_jsonl, inspect, JOURNAL_SCHEMA_VERSION};
use std::io::{BufWriter, IsTerminal};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[path = "obzenflow/render.rs"]
mod render;
use render::{ColorMode, ObservationEnd, Renderer};

#[derive(Parser)]
#[command(name = "obzenflow", version, long_version = version(), about = "Observe and inspect ObzenFlow journals")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION
        .get_or_init(|| {
            format!(
                "{}\nframework {}\njournal schema {}\nrecord JSONL {}",
                env!("CARGO_PKG_VERSION"),
                env!("CARGO_PKG_VERSION"),
                JOURNAL_SCHEMA_VERSION,
                RUN_RECORD_VERSION
            )
        })
        .as_str()
}

#[derive(Subcommand)]
enum Command {
    /// Read a run's committed records without starting or changing execution.
    Show(ShowArgs),
    /// Request Play in an independently running application.
    Start(StartArgs),
    /// Inspect or export the canonical journal envelopes and payloads.
    Journal {
        #[command(subcommand)]
        command: JournalCommand,
    },
    /// Compare a recorded run against a baseline using the shared verifier.
    Verify(VerifyArgs),
}

#[derive(Args)]
struct ShowArgs {
    /// Directory containing run_manifest.json.
    run_dir: PathBuf,
    #[command(flatten)]
    view: ViewArgs,
}

#[derive(Args)]
struct StartArgs {
    /// Existing application origin, e.g. http://127.0.0.1:9090.
    #[arg(long)]
    server: reqwest::Url,
    /// HTTP discovery/control timeout; does not limit execution or observation.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    #[command(flatten)]
    view: ViewArgs,
}

#[derive(Args, Default)]
struct ViewArgs {
    /// Follow until the execution is settled and covered, or Ctrl-C detaches.
    #[arg(long)]
    follow: bool,
    /// Emit all records as versioned JSONL without human output on stdout.
    #[arg(long)]
    json: bool,
    /// Include runtime, lifecycle, signal and system records in the human view.
    #[arg(long, conflicts_with = "json")]
    verbose: bool,
    /// Show complete envelopes and payloads for the selected records.
    #[arg(long, conflicts_with = "json")]
    detail: bool,
    /// Add teaching notes beneath the operation and payload.
    #[arg(long, conflicts_with_all = ["quiet", "json"])]
    explain: bool,
    /// One line per selected record, without clocks, teaching notes or count tables.
    #[arg(long, conflicts_with_all = ["explain", "detail", "json"])]
    quiet: bool,
    /// Semantic colors; auto respects terminal detection and NO_COLOR.
    #[arg(long, value_enum, default_value = "auto")]
    color: ColorMode,
}

#[derive(Subcommand)]
enum JournalCommand {
    ExportJsonl {
        run_dir: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Inspect {
        run_dir: PathBuf,
        #[arg(long)]
        stage: Option<String>,
        #[arg(long)]
        event_type: Option<String>,
    },
}

#[derive(Args)]
struct VerifyArgs {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    candidate: PathBuf,
    #[arg(long)]
    report_path: Option<PathBuf>,
    #[arg(long, default_value_t = 5)]
    max_divergences: usize,
}

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse().command).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("obzenflow: {error}");
            ExitCode::from(4)
        }
    }
}

async fn run(command: Command) -> Result<u8, Error> {
    match command {
        Command::Show(args) => {
            let snapshot = open_disk_run(&args.run_dir).await.inspect_err(|error| {
                eprintln!("{}", serde_json::json!({"event":"run_archive_admission_failed", "path":args.run_dir, "error":error.to_string()}));
            })?;
            observe(snapshot, &args.view).await
        }
        Command::Start(args) => start(args).await,
        Command::Verify(args) => {
            tokio::task::spawn_blocking(move || {
                let options = VerifyOptions {
                    max_divergences: args.max_divergences,
                    report_path: args.report_path,
                    write_report: true,
                };
                let outcome = verify_run_dirs(&args.baseline, &args.candidate, &options)?;
                println!("{}", render_verdict(&outcome));
                Ok::<_, Error>(outcome.exit_code())
            })
            .await?
        }
        Command::Journal { command } => {
            tokio::task::spawn_blocking(move || {
                match command {
                    JournalCommand::ExportJsonl { run_dir, output } => {
                        export_jsonl(&run_dir, output.as_deref())?
                    }
                    JournalCommand::Inspect {
                        run_dir,
                        stage,
                        event_type,
                    } => inspect(&run_dir, stage.as_deref(), event_type.as_deref())?,
                }
                Ok::<_, Error>(0)
            })
            .await?
        }
    }
}

async fn start(args: StartArgs) -> Result<u8, Error> {
    if !matches!(args.server.scheme(), "http" | "https")
        || !args.server.username().is_empty()
        || args.server.password().is_some()
        || args.server.path() != "/"
        || args.server.query().is_some()
        || args.server.fragment().is_some()
    {
        return Err(
            "--server must be an HTTP(S) origin without credentials, path, query or fragment"
                .into(),
        );
    }
    let mut headers = reqwest::header::HeaderMap::new();
    // Credentials never appear in arguments, URLs, logs or journal output.
    if let Ok(authorization) = std::env::var("OBZENFLOW_CONTROL_AUTHORIZATION") {
        let mut value = reqwest::header::HeaderValue::from_str(&authorization)
            .map_err(|_| "invalid OBZENFLOW_CONTROL_AUTHORIZATION value")?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(args.timeout_secs))
        .build()?;
    let discovery: CurrentRunDiscovery = client
        .get(args.server.join(RUN_DISCOVERY_PATH)?)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if discovery.protocol_version != RUN_CONTROL_PROTOCOL_VERSION
        || !discovery.target.pipeline_writer_id.is_system()
    {
        return Err(
            "unsupported discovery protocol or invalid pipeline writer; no Play submitted".into(),
        );
    }
    let snapshot = if args.view.follow {
        Some(admit_follow(&discovery).await.inspect_err(|error| {
            eprintln!("{}", serde_json::json!({"event":"run_archive_admission_failed", "target":discovery.target, "error":error.to_string()}));
        })?)
    } else {
        None
    };
    let request = TargetedFlowControlRequest {
        protocol_version: RUN_CONTROL_PROTOCOL_VERSION,
        target: discovery.target.clone(),
        control: FlowControlRequest {
            action: FlowControlAction::Play,
            stop_mode: None,
            timeout_secs: None,
        },
    };
    let response = client.post(args.server.join(FLOW_CONTROL_PATH)?).json(&request).send().await.map_err(|error| {
        eprintln!("{}", serde_json::json!({"event":"run_control_response_lost", "target":discovery.target, "error":error.to_string()}));
        "Play submission is uncertain; inspect the selected run before issuing another command"
    })?;
    let status = response.status();
    let response: TargetedFlowControlResponse = response.json().await.map_err(|error| {
        eprintln!("{}", serde_json::json!({"event":"run_control_response_lost", "target":discovery.target, "error":error.to_string()}));
        "Play result could not be decoded; submission remains uncertain"
    })?;
    if response.protocol_version != RUN_CONTROL_PROTOCOL_VERSION
        || response.target != discovery.target
    {
        eprintln!(
            "{}",
            serde_json::json!({"event":"run_target_mismatch", "expected":discovery.target, "actual":response.target})
        );
        return Err(
            "control response does not match the selected host/run; reader was not retargeted"
                .into(),
        );
    }
    if !status.is_success() || response.result.status != FlowControlStatus::Accepted {
        eprintln!(
            "{}",
            serde_json::json!({"event":"run_control_rejected", "target":response.target, "result":response.result})
        );
        return Err(format!("Play rejected: {}", response.result.message).into());
    }
    eprintln!(
        "{}",
        serde_json::json!({"event":"run_control_submitted", "target":response.target, "run":snapshot.as_ref().map(RunSnapshot::identity)})
    );
    if let Some(snapshot) = snapshot {
        observe(snapshot, &args.view).await
    } else {
        if args.view.json {
            println!("{}", serde_json::to_string(&response)?);
        } else {
            println!("{}", response.result.message);
        }
        Ok(0)
    }
}

async fn admit_follow(discovery: &CurrentRunDiscovery) -> Result<RunSnapshot, Error> {
    match &discovery.archive {
        RunArchive::LocalDisk { flow_id, path } => {
            let path = path
                .decode()
                .map_err(|error| format!("archive admission failed: {error}; no Play submitted"))?;
            let snapshot = open_disk_run(&path).await?;
            if snapshot.identity().flow_id.to_string() != *flow_id
                || snapshot.identity().pipeline_writer_id != discovery.target.pipeline_writer_id
            {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"run_target_mismatch", "expected":discovery.target, "expected_flow_id":flow_id, "admitted":snapshot.identity()})
                );
                return Err("archive identity does not match discovery; no Play submitted".into());
            }
            Ok(snapshot)
        }
        RunArchive::Unavailable { reason } => {
            Err(format!("run archive unavailable ({reason:?}); no Play submitted").into())
        }
    }
}

async fn observe(snapshot: RunSnapshot, view: &ViewArgs) -> Result<u8, Error> {
    let identity = snapshot.identity().clone();
    let result = observe_records(snapshot, view).await;
    if let Err(error) = &result {
        eprintln!(
            "{}",
            serde_json::json!({"event":"run_observation_failed", "run":identity, "error":error.to_string()})
        );
    }
    result
}

async fn observe_records(mut snapshot: RunSnapshot, view: &ViewArgs) -> Result<u8, Error> {
    let mut output = BufWriter::new(std::io::stdout());
    let mut diagnostics = std::io::stderr();
    let mut renderer = Renderer::new(
        view,
        std::io::stdout().is_terminal(),
        std::env::var_os("NO_COLOR").is_some(),
        snapshot.journals(),
    );
    renderer.begin(&mut output, snapshot.identity(), view.follow)?;
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    if !view.follow {
        loop {
            let record = tokio::select! { biased;
                result = &mut interrupt => {
                    result?;
                    let tail = snapshot.into_tail();
                    renderer.finish(&mut output, &mut diagnostics, tail.identity(), ObservationEnd::Detached, tail.progress())?;
                    return Ok(0);
                }
                record = snapshot.next() => match record {
                    Ok(record) => record,
                    Err(error) => {
                        renderer.flush_pending(&mut output)?;
                        return Err(error.into());
                    }
                },
            };
            let Some(record) = record else {
                break;
            };
            renderer.record(&mut output, record)?;
        }
        let tail = snapshot.into_tail();
        renderer.finish(
            &mut output,
            &mut diagnostics,
            tail.identity(),
            ObservationEnd::Snapshot,
            tail.progress(),
        )?;
        return Ok(0);
    }
    let mut tail = snapshot.into_tail();
    loop {
        let next = tokio::select! { biased;
            result = &mut interrupt => {
                result?;
                renderer.finish(&mut output, &mut diagnostics, tail.identity(), ObservationEnd::Detached, tail.progress())?;
                return Ok(0);
            }
            next = tail.read_next() => match next {
                Ok(next) => next,
                Err(error) => {
                    renderer.flush_pending(&mut output)?;
                    return Err(error.into());
                }
            },
        };
        let pending = matches!(next, TailRead::Pending);
        match next {
            TailRead::Record(record) => renderer.record(&mut output, record)?,
            TailRead::Pending => renderer.flush_pending(&mut output)?,
        }
        if tail.progress().settled_prefix.is_some() {
            if view.json {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"run_observation_covered", "run":tail.identity(), "progress":tail.progress()})
                );
            }
            renderer.finish(
                &mut output,
                &mut diagnostics,
                tail.identity(),
                ObservationEnd::Settled,
                tail.progress(),
            )?;
            return Ok(0);
        }
        if pending {
            tokio::select! { biased;
                result = &mut interrupt => {
                    result?;
                    renderer.finish(&mut output, &mut diagnostics, tail.identity(), ObservationEnd::Detached, tail.progress())?;
                    return Ok(0);
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }
}
