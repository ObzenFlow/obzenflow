// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `obzenflow` operational CLI. A thin shell: every verb's logic lives in
//! library code (`obzenflow_infra`), and this binary only parses arguments,
//! prints verdicts, and maps them to process exit codes.
//!
//! `verify` (FLOWIP-095j) compares a candidate run directory against a
//! baseline run of the same flow over the verification projection and exits
//! `0` on a fully certified match, `1` on divergence in the certified region,
//! `2` when the certified region matched but uncertified stages exist, and
//! `3` when the comparison is refused.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use obzenflow_infra::journal::disk::inspect::{export_jsonl, inspect};
use obzenflow_infra::verify::{render_verdict, verify_run_dirs, VerifyOptions};

#[derive(Parser)]
#[command(name = "obzenflow", version, about = "ObzenFlow operational verbs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify a replayed (or recorded) run against a baseline run of the
    /// same flow, from the journals (FLOWIP-095j).
    Verify(VerifyArgs),

    /// Inspect a run's durable journals through the supported JSONL projection
    /// (FLOWIP-120q). The raw `.log` files are internal framed storage.
    Journal(JournalArgs),
}

#[derive(Args)]
struct JournalArgs {
    #[command(subcommand)]
    command: JournalCommand,
}

#[derive(Subcommand)]
enum JournalCommand {
    /// Export a run's system, data, and error journals as JSONL, one JSON
    /// object per committed record. Fails loud on corruption.
    ExportJsonl {
        /// The run directory (the one containing `run_manifest.json`).
        run_dir: PathBuf,
        /// Write to this file instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Print a run summary plus a filtered listing of stage data journals.
    Inspect {
        /// The run directory (the one containing `run_manifest.json`).
        run_dir: PathBuf,
        /// Limit the listing to one stage key.
        #[arg(long)]
        stage: Option<String>,
        /// Limit the listing to one event type.
        #[arg(long)]
        event_type: Option<String>,
    },
}

#[derive(Args)]
struct VerifyArgs {
    /// The run directory being compared against (the recorded original).
    #[arg(long)]
    baseline: PathBuf,

    /// The run directory under verification (typically a replay run).
    #[arg(long)]
    candidate: PathBuf,

    /// Report destination; defaults to
    /// `<candidate>/verification/<baseline-flow-id>.json`.
    #[arg(long)]
    report_path: Option<PathBuf>,

    /// Cap on recorded divergences per stage journal (counting continues).
    #[arg(long, default_value_t = 5)]
    max_divergences: usize,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Verify(args) => {
            let options = VerifyOptions {
                max_divergences: args.max_divergences,
                report_path: args.report_path,
                write_report: true,
            };
            match verify_run_dirs(&args.baseline, &args.candidate, &options) {
                Ok(outcome) => {
                    println!("{}", render_verdict(&outcome));
                    ExitCode::from(outcome.exit_code())
                }
                Err(err) => {
                    eprintln!("verification failed: {err}");
                    ExitCode::from(4)
                }
            }
        }
        Command::Journal(args) => match args.command {
            JournalCommand::ExportJsonl { run_dir, output } => {
                match export_jsonl(&run_dir, output.as_deref()) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(err) => {
                        eprintln!("journal export-jsonl failed: {err}");
                        ExitCode::from(4)
                    }
                }
            }
            JournalCommand::Inspect {
                run_dir,
                stage,
                event_type,
            } => match inspect(&run_dir, stage.as_deref(), event_type.as_deref()) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("journal inspect failed: {err}");
                    ExitCode::from(4)
                }
            },
        },
    }
}
