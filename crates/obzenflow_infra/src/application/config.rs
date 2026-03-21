// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// CORS mode for FlowApplication's HTTP server.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum CorsModeArg {
    /// Adds permissive CORS headers (`Access-Control-Allow-Origin: *`).
    AllowAnyOrigin,
    /// Adds CORS headers for a configured allow-list.
    AllowList,
    /// Do not add CORS headers (browser same-origin policy applies).
    SameOrigin,
}

/// Configuration for FlowApplication
///
/// This struct is automatically populated from CLI arguments when
/// FlowApplication::run() is called.
#[derive(Parser, Debug, Clone)]
#[command(about = "ObzenFlow Application")]
pub struct FlowConfig {
    /// Start HTTP server for metrics and topology visualization
    #[arg(long)]
    pub server: bool,

    /// Host/interface for HTTP server bind (default: 127.0.0.1)
    #[arg(long, default_value = "127.0.0.1")]
    pub server_host: String,

    /// Port for HTTP server
    #[arg(long, default_value = "9090")]
    pub server_port: u16,

    /// Startup mode for the flow when running with --server
    ///
    /// - auto   (default): build and immediately start the flow
    /// - manual: build and expose HTTP endpoints, but do not start until a Play command
    #[arg(
        long,
        value_enum,
        default_value_t = StartupMode::Auto
    )]
    pub startup_mode: StartupMode,

    /// CORS mode for HTTP server endpoints (default: `same-origin`).
    #[arg(long, value_enum, default_value_t = CorsModeArg::SameOrigin)]
    pub cors_mode: CorsModeArg,

    /// Allowed origins for CORS when `--cors-mode=allow-list` (repeatable).
    #[arg(long = "cors-allow-origin")]
    pub cors_allow_origin: Vec<String>,

    /// Replay sources from a completed or cancelled archived run directory (FLOWIP-095a).
    ///
    /// The path must be the exact run directory containing `run_manifest.json`.
    #[arg(long)]
    pub replay_from: Option<PathBuf>,

    /// Allow replaying from incomplete archives (failed/unknown/missing system.log).
    ///
    /// This is intended for debugging; outputs may be partial and not suitable for regression comparison.
    #[arg(long)]
    pub allow_incomplete_archive: bool,
    // Future fields will be added here:
    // - debug flag
    // - journal overrides
    // - checkpoint intervals
    // - distributed mode settings
}

/// Startup behavior for FlowApplication when running with --server
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum StartupMode {
    /// Build pipeline and start immediately (backwards compatible default)
    Auto,
    /// Build pipeline, expose HTTP endpoints, but do not start until Play
    Manual,
}
