// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use clap::{Parser, ValueEnum};
use obzenflow_core::web::AuthPolicy;
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

/// Control-plane auth mode for FlowApplication's HTTP server.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ControlPlaneAuthModeArg {
    /// Static shared secret compared against a request header.
    ApiKey,
    /// HMAC-SHA256 signature over the request body.
    HmacSha256,
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

    /// Maximum request body size in bytes for the managed HTTP server.
    ///
    /// This is the hard ceiling; managed surfaces may declare a smaller limit.
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub max_body_size_bytes: usize,

    /// Request timeout in seconds for the managed HTTP server.
    ///
    /// This is the host default and ceiling. Set to `0` to disable timeouts.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

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

    /// Optional auth mode for built-in control-plane routes such as `/api/flow/*`,
    /// `/api/topology`, and `/metrics`.
    #[arg(long, value_enum)]
    pub control_plane_auth_mode: Option<ControlPlaneAuthModeArg>,

    /// Header to inspect when `--control-plane-auth-mode=api-key`.
    ///
    /// Defaults to `Authorization`.
    #[arg(long)]
    pub control_plane_auth_header: Option<String>,

    /// Environment variable containing the expected header value when
    /// `--control-plane-auth-mode=api-key`.
    #[arg(long)]
    pub control_plane_auth_value_env: Option<String>,

    /// Environment variable containing the shared secret when
    /// `--control-plane-auth-mode=hmac-sha256`.
    #[arg(long)]
    pub control_plane_hmac_secret_env: Option<String>,

    /// Header containing the HMAC signature when
    /// `--control-plane-auth-mode=hmac-sha256`.
    ///
    /// Defaults to `X-Signature`.
    #[arg(long)]
    pub control_plane_hmac_signature_header: Option<String>,

    /// Optional header containing the timestamp used for replay protection when
    /// `--control-plane-auth-mode=hmac-sha256`.
    #[arg(long)]
    pub control_plane_hmac_timestamp_header: Option<String>,

    /// Optional replay window in seconds when
    /// `--control-plane-auth-mode=hmac-sha256`.
    #[arg(long)]
    pub control_plane_hmac_replay_window_secs: Option<u64>,

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

impl FlowConfig {
    pub fn has_control_plane_auth_args(&self) -> bool {
        self.control_plane_auth_mode.is_some()
            || self.control_plane_auth_header.is_some()
            || self.control_plane_auth_value_env.is_some()
            || self.control_plane_hmac_secret_env.is_some()
            || self.control_plane_hmac_signature_header.is_some()
            || self.control_plane_hmac_timestamp_header.is_some()
            || self.control_plane_hmac_replay_window_secs.is_some()
    }

    pub fn build_control_plane_auth(&self) -> Result<Option<AuthPolicy>, String> {
        let Some(mode) = self.control_plane_auth_mode else {
            if self.has_control_plane_auth_args() {
                return Err(
                    "control-plane auth fields require --control-plane-auth-mode".to_string(),
                );
            }
            return Ok(None);
        };

        match mode {
            ControlPlaneAuthModeArg::ApiKey => {
                if self.control_plane_hmac_secret_env.is_some()
                    || self.control_plane_hmac_signature_header.is_some()
                    || self.control_plane_hmac_timestamp_header.is_some()
                    || self.control_plane_hmac_replay_window_secs.is_some()
                {
                    return Err(
                        "HMAC-specific control-plane auth flags require --control-plane-auth-mode=hmac-sha256"
                            .to_string(),
                    );
                }

                let value_env = self
                    .control_plane_auth_value_env
                    .clone()
                    .ok_or_else(|| {
                        "--control-plane-auth-value-env is required for --control-plane-auth-mode=api-key"
                            .to_string()
                    })?;

                Ok(Some(AuthPolicy::ApiKey {
                    header: self
                        .control_plane_auth_header
                        .clone()
                        .unwrap_or_else(|| "Authorization".to_string()),
                    value_env,
                }))
            }
            ControlPlaneAuthModeArg::HmacSha256 => {
                if self.control_plane_auth_header.is_some()
                    || self.control_plane_auth_value_env.is_some()
                {
                    return Err(
                        "API-key-specific control-plane auth flags require --control-plane-auth-mode=api-key"
                            .to_string(),
                    );
                }

                let secret_env = self
                    .control_plane_hmac_secret_env
                    .clone()
                    .ok_or_else(|| {
                        "--control-plane-hmac-secret-env is required for --control-plane-auth-mode=hmac-sha256"
                            .to_string()
                    })?;

                if self.control_plane_hmac_replay_window_secs.is_some()
                    && self.control_plane_hmac_timestamp_header.is_none()
                {
                    return Err(
                        "--control-plane-hmac-replay-window-secs requires --control-plane-hmac-timestamp-header"
                            .to_string(),
                    );
                }

                Ok(Some(AuthPolicy::HmacSha256 {
                    secret_env,
                    signature_header: self
                        .control_plane_hmac_signature_header
                        .clone()
                        .unwrap_or_else(|| "X-Signature".to_string()),
                    body_hash: "raw_body".to_string(),
                    timestamp_header: self.control_plane_hmac_timestamp_header.clone(),
                    replay_window_secs: self.control_plane_hmac_replay_window_secs,
                }))
            }
        }
    }
}
