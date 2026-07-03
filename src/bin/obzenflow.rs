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
use obzenflow_infra::config_cli::{
    live_route, offline_docs, parse_live_values, render_live, render_table, schema_json, ConfigView,
};
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

    /// Read resolved runtime configuration (FLOWIP-010).
    Config(ConfigArgs),
}

#[derive(Args)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommand,
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Print the resolved configuration with provenance (source and scope).
    ///
    /// OFFLINE (the default) resolves the file, environment, and default
    /// tiers at GLOBAL scope only; the DSL tier and flow, stage, and edge
    /// scopes exist only in a running flow, so `--effective` and `--base`
    /// are identical offline and `--overlay`/`--diff` refuse. LIVE
    /// (`--url`) queries a running runtime's `/api/config/*` routes and can
    /// additionally show dsl-sourced and scoped entries.
    Get {
        /// The effective projection (default).
        #[arg(long, conflicts_with_all = ["base", "overlay", "diff", "schema"])]
        effective: bool,
        /// The base projection (file + env + CLI + defaults).
        #[arg(long, conflicts_with_all = ["overlay", "diff", "schema"])]
        base: bool,
        /// The runtime overlay (live-only; truthfully empty until FLOWIP-010b).
        #[arg(long, conflicts_with_all = ["diff", "schema"])]
        overlay: bool,
        /// Base-versus-effective differences (live-only).
        #[arg(long, conflicts_with = "schema")]
        diff: bool,
        /// The knob registry metadata (types, targets, defaults, env names).
        #[arg(long)]
        schema: bool,
        /// Narrow to one flow (requires --url).
        #[arg(long, requires = "url")]
        flow: Option<String>,
        /// Narrow to one stage within --flow (requires --url).
        #[arg(long, requires = "flow")]
        stage: Option<String>,
        /// Query a running runtime (e.g. http://127.0.0.1:9090) instead of
        /// resolving offline.
        #[arg(long)]
        url: Option<String>,
        /// Full Authorization header value for control-plane api-key auth
        /// (e.g. "Bearer <token>"). HMAC-signed auth is not supported here.
        #[arg(long)]
        auth_header: Option<String>,
        /// Config file path (offline mode; defaults to ./obzenflow.toml
        /// autodiscovery).
        #[arg(long)]
        config: Option<PathBuf>,
    },
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
        Command::Config(args) => match args.command {
            ConfigCommand::Get {
                effective: _,
                base,
                overlay,
                diff,
                schema,
                flow,
                stage,
                url,
                auth_header,
                config,
            } => {
                let view = if schema {
                    ConfigView::Schema
                } else if overlay {
                    ConfigView::Overlay
                } else if diff {
                    ConfigView::Diff
                } else if base {
                    ConfigView::Base
                } else {
                    ConfigView::Effective
                };

                if matches!(view, ConfigView::Schema) {
                    match serde_json::to_string_pretty(&schema_json()) {
                        Ok(body) => {
                            println!("{body}");
                            return ExitCode::SUCCESS;
                        }
                        Err(err) => {
                            eprintln!("config get failed: {err}");
                            return ExitCode::from(4);
                        }
                    }
                }

                match url {
                    Some(url) => {
                        let route = live_route(view, flow.as_deref(), stage.as_deref());
                        let target = format!("{}{route}", url.trim_end_matches('/'));
                        let mut request = ureq::get(&target);
                        if let Some(header) = &auth_header {
                            request = request.set("Authorization", header);
                        }
                        let body = match request.call() {
                            Ok(response) => match response.into_string() {
                                Ok(body) => body,
                                Err(err) => {
                                    eprintln!("config get failed reading {target}: {err}");
                                    return ExitCode::from(4);
                                }
                            },
                            Err(ureq::Error::Status(status, response)) => {
                                let body = response.into_string().unwrap_or_default();
                                eprintln!("config get failed: {target} returned {status}: {body}");
                                return ExitCode::from(4);
                            }
                            Err(err) => {
                                eprintln!("config get failed calling {target}: {err}");
                                return ExitCode::from(4);
                            }
                        };
                        match parse_live_values(&body) {
                            Ok(live) => {
                                print!("{}", render_live(&live));
                                ExitCode::SUCCESS
                            }
                            Err(err) => {
                                eprintln!("config get failed parsing {target}: {err}");
                                ExitCode::from(4)
                            }
                        }
                    }
                    None => match offline_docs(view, config.as_deref()) {
                        Ok(docs) => {
                            print!("{}", render_table(&docs));
                            ExitCode::SUCCESS
                        }
                        Err(err) => {
                            eprintln!("config get failed: {err}");
                            ExitCode::from(4)
                        }
                    },
                }
            }
        },
    }
}
