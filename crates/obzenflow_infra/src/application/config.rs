// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::env::{env_bool, env_var};
use clap::{ArgAction, Parser, ValueEnum};
use obzenflow_core::journal::RUN_MANIFEST_FILENAME;
use obzenflow_core::web::AuthPolicy;
use obzenflow_runtime::bootstrap::{BootstrapConfig, MetricsBootstrap, MetricsExporterKind};
use serde::Deserialize;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// Startup behavior for FlowApplication when running with --server
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum StartupMode {
    /// Build pipeline and start immediately (backwards compatible default)
    Auto,
    /// Build pipeline, expose HTTP endpoints, but do not start until Play
    Manual,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "ObzenFlow Application")]
pub struct FlowConfig {
    /// Path to a startup config file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Start HTTP server for metrics and topology visualization
    #[arg(long, action = ArgAction::SetTrue)]
    pub server: bool,

    /// Host/interface for HTTP server bind (default: 127.0.0.1)
    #[arg(long)]
    pub server_host: Option<String>,

    /// Port for HTTP server
    #[arg(long)]
    pub server_port: Option<u16>,

    /// Maximum request body size in bytes for the managed HTTP server.
    ///
    /// This is the hard ceiling; managed surfaces may declare a smaller limit.
    #[arg(long)]
    pub max_body_size_bytes: Option<usize>,

    /// Request timeout in seconds for the managed HTTP server.
    ///
    /// This is the host default and ceiling. Set to `0` to disable timeouts.
    #[arg(long)]
    pub request_timeout_secs: Option<u64>,

    /// Startup mode for the flow when running with --server
    ///
    /// - auto   (default): build and immediately start the flow
    /// - manual: build and expose HTTP endpoints, but do not start until a Play command
    #[arg(long, value_enum)]
    pub startup_mode: Option<StartupMode>,

    /// CORS mode for HTTP server endpoints (default: `same-origin`).
    #[arg(long, value_enum)]
    pub cors_mode: Option<CorsModeArg>,

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
    #[arg(long, action = ArgAction::SetTrue)]
    pub allow_incomplete_archive: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStartupConfig {
    pub server: ResolvedServerConfig,
    pub runtime: ResolvedRuntimeConfig,
    pub metrics: ResolvedMetricsConfig,
    pub replay: Option<ResolvedReplayConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedServerConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub max_body_size_bytes: usize,
    pub request_timeout_secs: u64,
    pub startup_mode: StartupMode,
    pub cors_mode: CorsModeArg,
    pub cors_allow_origin: Vec<String>,
    pub control_plane_auth: Option<AuthPolicy>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedRuntimeConfig {
    pub shutdown_timeout: Duration,
    pub ingestion_metrics_interval: Duration,
    #[cfg_attr(not(feature = "warp-server"), allow(dead_code))]
    pub surface_metrics_interval: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedMetricsConfig {
    pub enabled: bool,
    pub exporter: MetricsExporterKind,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedReplayConfig {
    pub from: PathBuf,
    pub allow_incomplete_archive: bool,
}

impl ResolvedStartupConfig {
    pub fn bootstrap_config(&self) -> BootstrapConfig {
        BootstrapConfig {
            shutdown_timeout: self.runtime.shutdown_timeout,
            startup_mode: if self.server.enabled {
                self.server.startup_mode.into()
            } else {
                obzenflow_runtime::bootstrap::StartupMode::Auto
            },
            replay: self.replay.as_ref().map(|replay| {
                obzenflow_runtime::bootstrap::ReplayBootstrap {
                    archive_path: replay.from.clone(),
                    allow_incomplete_archive: replay.allow_incomplete_archive,
                }
            }),
            metrics: MetricsBootstrap {
                enabled: self.metrics.enabled,
                exporter: self.metrics.exporter,
            },
        }
    }
}

impl From<StartupMode> for obzenflow_runtime::bootstrap::StartupMode {
    fn from(value: StartupMode) -> Self {
        match value {
            StartupMode::Auto => Self::Auto,
            StartupMode::Manual => Self::Manual,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigError {
    path: Option<String>,
    message: String,
}

impl ConfigError {
    fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: Some(path.into()),
            message: message.into(),
        }
    }

    fn global(message: impl Into<String>) -> Self {
        Self {
            path: None,
            message: message.into(),
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.path {
            Some(path) => write!(f, "config error at {path}: {}", self.message),
            None => write!(f, "config error: {}", self.message),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileStartupConfig {
    server: RawFileServerConfig,
    runtime: RawFileRuntimeConfig,
    metrics: RawFileMetricsConfig,
    replay: RawFileReplayConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileServerConfig {
    enabled: Option<bool>,
    host: Option<String>,
    port: Option<i64>,
    startup_mode: Option<String>,
    max_body_size_bytes: Option<i64>,
    request_timeout_secs: Option<i64>,
    cors: RawFileCorsConfig,
    control_plane_auth: RawFileControlPlaneAuthConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileCorsConfig {
    mode: Option<String>,
    allow_origins: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileControlPlaneAuthConfig {
    mode: Option<String>,
    header: Option<String>,
    value_env: Option<String>,
    secret_env: Option<String>,
    signature_header: Option<String>,
    timestamp_header: Option<String>,
    replay_window_secs: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileRuntimeConfig {
    shutdown_timeout_secs: Option<i64>,
    ingestion_metrics_interval_secs: Option<i64>,
    surface_metrics_interval_secs: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileMetricsConfig {
    enabled: Option<bool>,
    exporter: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileReplayConfig {
    from: Option<PathBuf>,
    allow_incomplete_archive: Option<bool>,
}

impl FlowConfig {
    pub(crate) fn parse_and_resolve(
        builder_config_file: Option<PathBuf>,
        enable_autodiscovery: bool,
    ) -> Result<ResolvedStartupConfig, ConfigError> {
        Self::parse().resolve(builder_config_file, enable_autodiscovery)
    }

    pub(crate) fn resolve(
        self,
        builder_config_file: Option<PathBuf>,
        enable_autodiscovery: bool,
    ) -> Result<ResolvedStartupConfig, ConfigError> {
        let file_path = self
            .config
            .clone()
            .or(builder_config_file)
            .or_else(|| autodiscover_config(enable_autodiscovery));
        let file = load_file_config(file_path.as_deref())?;
        let control_plane_auth = self.resolve_control_plane_auth(&file)?;

        let server_enabled = if self.server {
            true
        } else {
            file.server.enabled.unwrap_or(false)
        };
        let startup_mode = self
            .startup_mode
            .or(parse_startup_mode(
                file.server.startup_mode.as_deref(),
                "server.startup_mode",
            )?)
            .or(parse_env_startup_mode()?)
            .unwrap_or(StartupMode::Auto);
        let cors_mode = self
            .cors_mode
            .or(parse_cors_mode(
                file.server.cors.mode.as_deref(),
                "server.cors.mode",
            )?)
            .unwrap_or(CorsModeArg::SameOrigin);

        let cors_allow_origin = if !self.cors_allow_origin.is_empty() {
            self.cors_allow_origin
        } else {
            file.server.cors.allow_origins.unwrap_or_default()
        };

        let server = ResolvedServerConfig {
            enabled: server_enabled,
            host: resolve_scalar(
                self.server_host,
                file.server.host,
                None,
                "127.0.0.1".to_string(),
            ),
            port: resolve_scalar(
                self.server_port,
                parse_port(file.server.port, "server.port")?,
                None,
                9090,
            ),
            max_body_size_bytes: resolve_scalar(
                self.max_body_size_bytes,
                parse_non_negative_usize(
                    file.server.max_body_size_bytes,
                    "server.max_body_size_bytes",
                )?,
                None,
                10 * 1024 * 1024,
            ),
            request_timeout_secs: resolve_scalar(
                self.request_timeout_secs,
                parse_non_negative_u64(
                    file.server.request_timeout_secs,
                    "server.request_timeout_secs",
                )?,
                None,
                30,
            ),
            startup_mode,
            cors_mode,
            cors_allow_origin,
            control_plane_auth,
        };

        if !server.cors_allow_origin.is_empty() && server.cors_mode != CorsModeArg::AllowList {
            return Err(ConfigError::at(
                "server.cors.allow_origins",
                "requires server.cors.mode = \"allow-list\"",
            ));
        }

        if server.cors_mode == CorsModeArg::AllowList && server.cors_allow_origin.is_empty() {
            return Err(ConfigError::at(
                "server.cors.allow_origins",
                "required when server.cors.mode = \"allow-list\"",
            ));
        }

        let runtime = ResolvedRuntimeConfig {
            shutdown_timeout: Duration::from_secs(resolve_scalar(
                None,
                parse_non_negative_u64(
                    file.runtime.shutdown_timeout_secs,
                    "runtime.shutdown_timeout_secs",
                )?,
                parse_env_u64(
                    "OBZENFLOW_SHUTDOWN_TIMEOUT_SECS",
                    "runtime.shutdown_timeout_secs",
                )?,
                30,
            )),
            ingestion_metrics_interval: Duration::from_secs(
                resolve_scalar(
                    None,
                    parse_non_negative_u64(
                        file.runtime.ingestion_metrics_interval_secs,
                        "runtime.ingestion_metrics_interval_secs",
                    )?,
                    parse_env_u64(
                        "OBZENFLOW_INGESTION_METRICS_INTERVAL_SECS",
                        "runtime.ingestion_metrics_interval_secs",
                    )?,
                    5,
                )
                .max(1),
            ),
            surface_metrics_interval: Duration::from_secs(resolve_scalar(
                None,
                parse_non_negative_u64(
                    file.runtime.surface_metrics_interval_secs,
                    "runtime.surface_metrics_interval_secs",
                )?,
                parse_env_u64(
                    "OBZENFLOW_SURFACE_METRICS_INTERVAL_SECS",
                    "runtime.surface_metrics_interval_secs",
                )?,
                15,
            )),
        };

        let metrics_enabled = resolve_scalar(
            None,
            file.metrics.enabled,
            parse_env_bool("OBZENFLOW_METRICS_ENABLED", "metrics.enabled")?,
            true,
        );
        let metrics_exporter = resolve_scalar(
            None,
            parse_metrics_exporter(file.metrics.exporter.as_deref(), "metrics.exporter")?,
            parse_env_metrics_exporter()?,
            MetricsExporterKind::Prometheus,
        );
        let metrics = ResolvedMetricsConfig {
            enabled: metrics_enabled,
            exporter: metrics_exporter,
        };

        let replay_from = resolve_optional(
            self.replay_from,
            file.replay.from,
            parse_env_path("OBZENFLOW_REPLAY_FROM", "replay.from")?,
        );
        let allow_incomplete_archive = resolve_positive_flag(
            self.allow_incomplete_archive,
            file.replay.allow_incomplete_archive,
            parse_env_bool(
                "OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE",
                "replay.allow_incomplete_archive",
            )?,
            false,
        );
        let replay = replay_from.map(|from| ResolvedReplayConfig {
            from,
            allow_incomplete_archive,
        });

        if let Some(replay) = &replay {
            validate_replay(replay)?;
        } else if allow_incomplete_archive {
            return Err(ConfigError::at(
                "replay.allow_incomplete_archive",
                "cannot be true without replay.from",
            ));
        }

        if server.control_plane_auth.is_some() && !server.enabled {
            return Err(ConfigError::at(
                "server.control_plane_auth",
                "requires server.enabled = true",
            ));
        }

        Ok(ResolvedStartupConfig {
            server,
            runtime,
            metrics,
            replay,
        })
    }

    fn resolve_control_plane_auth(
        &self,
        file: &RawFileStartupConfig,
    ) -> Result<Option<AuthPolicy>, ConfigError> {
        let auth_mode = self
            .control_plane_auth_mode
            .or(parse_control_plane_auth_mode(
                file.server.control_plane_auth.mode.as_deref(),
                "server.control_plane_auth.mode",
            )?);
        let header = resolve_optional(
            self.control_plane_auth_header.clone(),
            file.server.control_plane_auth.header.clone(),
            None,
        );
        let value_env = resolve_optional(
            self.control_plane_auth_value_env.clone(),
            file.server.control_plane_auth.value_env.clone(),
            None,
        );
        let secret_env = resolve_optional(
            self.control_plane_hmac_secret_env.clone(),
            file.server.control_plane_auth.secret_env.clone(),
            None,
        );
        let signature_header = resolve_optional(
            self.control_plane_hmac_signature_header.clone(),
            file.server.control_plane_auth.signature_header.clone(),
            None,
        );
        let timestamp_header = resolve_optional(
            self.control_plane_hmac_timestamp_header.clone(),
            file.server.control_plane_auth.timestamp_header.clone(),
            None,
        );
        let replay_window_secs = resolve_optional(
            self.control_plane_hmac_replay_window_secs,
            parse_non_negative_u64(
                file.server.control_plane_auth.replay_window_secs,
                "server.control_plane_auth.replay_window_secs",
            )?,
            None,
        );

        let has_any_fields = header.is_some()
            || value_env.is_some()
            || secret_env.is_some()
            || signature_header.is_some()
            || timestamp_header.is_some()
            || replay_window_secs.is_some();

        let Some(mode) = auth_mode else {
            if has_any_fields {
                return Err(ConfigError::at(
                    "server.control_plane_auth.mode",
                    "required when control-plane auth fields are set",
                ));
            }
            return Ok(None);
        };

        match mode {
            ControlPlaneAuthModeArg::ApiKey => {
                if secret_env.is_some() || signature_header.is_some() || timestamp_header.is_some()
                {
                    return Err(ConfigError::at(
                        "server.control_plane_auth.mode",
                        "HMAC-specific fields require mode = \"hmac_sha256\"",
                    ));
                }
                if replay_window_secs.is_some() {
                    return Err(ConfigError::at(
                        "server.control_plane_auth.replay_window_secs",
                        "requires mode = \"hmac_sha256\"",
                    ));
                }

                let value_env = value_env.ok_or_else(|| {
                    ConfigError::at(
                        "server.control_plane_auth.value_env",
                        "required when mode = \"api_key\"",
                    )
                })?;

                Ok(Some(AuthPolicy::ApiKey {
                    header: header.unwrap_or_else(|| "Authorization".to_string()),
                    value_env,
                }))
            }
            ControlPlaneAuthModeArg::HmacSha256 => {
                if header.is_some() || value_env.is_some() {
                    return Err(ConfigError::at(
                        "server.control_plane_auth.mode",
                        "API-key-specific fields require mode = \"api_key\"",
                    ));
                }

                let secret_env = secret_env.ok_or_else(|| {
                    ConfigError::at(
                        "server.control_plane_auth.secret_env",
                        "required when mode = \"hmac_sha256\"",
                    )
                })?;

                if replay_window_secs.is_some() && timestamp_header.is_none() {
                    return Err(ConfigError::at(
                        "server.control_plane_auth.timestamp_header",
                        "required when replay_window_secs is set",
                    ));
                }

                Ok(Some(AuthPolicy::HmacSha256 {
                    secret_env,
                    signature_header: signature_header.unwrap_or_else(|| "X-Signature".to_string()),
                    body_hash: "raw_body".to_string(),
                    timestamp_header,
                    replay_window_secs,
                }))
            }
        }
    }
}

fn autodiscover_config(enabled: bool) -> Option<PathBuf> {
    if !enabled {
        return None;
    }

    let path = PathBuf::from("obzenflow.toml");
    path.is_file().then_some(path)
}

fn load_file_config(path: Option<&Path>) -> Result<RawFileStartupConfig, ConfigError> {
    let Some(path) = path else {
        return Ok(RawFileStartupConfig::default());
    };

    let text = std::fs::read_to_string(path)
        .map_err(|err| ConfigError::global(format!("failed to read {}: {err}", path.display())))?;
    toml::from_str::<RawFileStartupConfig>(&text)
        .map_err(|err| ConfigError::global(format!("failed to parse {}: {err}", path.display())))
}

fn validate_replay(replay: &ResolvedReplayConfig) -> Result<(), ConfigError> {
    if !replay.from.is_dir() {
        return Err(ConfigError::at(
            "replay.from",
            format!("must be a directory: {}", replay.from.display()),
        ));
    }

    let manifest_path = replay.from.join(RUN_MANIFEST_FILENAME);
    if !manifest_path.exists() {
        return Err(ConfigError::at(
            "replay.from",
            format!("path does not contain {RUN_MANIFEST_FILENAME}"),
        ));
    }

    Ok(())
}

fn parse_startup_mode(value: Option<&str>, path: &str) -> Result<Option<StartupMode>, ConfigError> {
    match value.map(normalise_enum_token) {
        None => Ok(None),
        Some(value) if value == "auto" => Ok(Some(StartupMode::Auto)),
        Some(value) if value == "manual" => Ok(Some(StartupMode::Manual)),
        Some(value) => Err(ConfigError::at(
            path,
            format!("unknown value {value:?}; expected one of auto, manual"),
        )),
    }
}

fn parse_env_startup_mode() -> Result<Option<StartupMode>, ConfigError> {
    let value = env_var::<String>("OBZENFLOW_STARTUP_MODE")
        .map_err(|err| ConfigError::at("server.startup_mode", err.to_string()))?;
    parse_startup_mode(value.as_deref(), "server.startup_mode")
}

fn parse_cors_mode(value: Option<&str>, path: &str) -> Result<Option<CorsModeArg>, ConfigError> {
    match value.map(normalise_enum_token) {
        None => Ok(None),
        Some(value) if value == "allow-any-origin" => Ok(Some(CorsModeArg::AllowAnyOrigin)),
        Some(value) if value == "allow-list" => Ok(Some(CorsModeArg::AllowList)),
        Some(value) if value == "same-origin" => Ok(Some(CorsModeArg::SameOrigin)),
        Some(value) => Err(ConfigError::at(
            path,
            format!(
                "unknown value {value:?}; expected one of allow-any-origin, allow-list, same-origin"
            ),
        )),
    }
}

fn parse_control_plane_auth_mode(
    value: Option<&str>,
    path: &str,
) -> Result<Option<ControlPlaneAuthModeArg>, ConfigError> {
    match value.map(normalise_enum_token) {
        None => Ok(None),
        Some(value) if value == "api-key" => Ok(Some(ControlPlaneAuthModeArg::ApiKey)),
        Some(value) if value == "hmac-sha256" => Ok(Some(ControlPlaneAuthModeArg::HmacSha256)),
        Some(value) => Err(ConfigError::at(
            path,
            format!("unknown value {value:?}; expected one of api_key, hmac_sha256"),
        )),
    }
}

fn parse_metrics_exporter(
    value: Option<&str>,
    path: &str,
) -> Result<Option<MetricsExporterKind>, ConfigError> {
    match value.map(normalise_enum_token) {
        None => Ok(None),
        Some(value) if value == "prometheus" => Ok(Some(MetricsExporterKind::Prometheus)),
        Some(value) if value == "console" => Ok(Some(MetricsExporterKind::Console)),
        Some(value) if value == "noop" => Ok(Some(MetricsExporterKind::Noop)),
        Some(value) => Err(ConfigError::at(
            path,
            format!("unknown value {value:?}; expected one of prometheus, console, noop"),
        )),
    }
}

fn parse_env_metrics_exporter() -> Result<Option<MetricsExporterKind>, ConfigError> {
    let value = env_var::<String>("OBZENFLOW_METRICS_EXPORTER")
        .map_err(|err| ConfigError::at("metrics.exporter", err.to_string()))?;
    parse_metrics_exporter(value.as_deref(), "metrics.exporter")
}

fn parse_non_negative_u64(value: Option<i64>, path: &str) -> Result<Option<u64>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) if value >= 0 => Ok(Some(value as u64)),
        Some(value) => Err(ConfigError::at(path, format!("must be >= 0, got {value}"))),
    }
}

fn parse_non_negative_usize(value: Option<i64>, path: &str) -> Result<Option<usize>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) if value >= 0 => Ok(Some(value as usize)),
        Some(value) => Err(ConfigError::at(path, format!("must be >= 0, got {value}"))),
    }
}

fn parse_port(value: Option<i64>, path: &str) -> Result<Option<u16>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) if (1..=u16::MAX as i64).contains(&value) => Ok(Some(value as u16)),
        Some(value) => Err(ConfigError::at(
            path,
            format!("must be in the range 1..=65535, got {value}"),
        )),
    }
}

fn parse_env_u64(key: &str, path: &str) -> Result<Option<u64>, ConfigError> {
    env_var::<u64>(key).map_err(|err| ConfigError::at(path, err.to_string()))
}

fn parse_env_bool(key: &str, path: &str) -> Result<Option<bool>, ConfigError> {
    env_bool(key).map_err(|err| ConfigError::at(path, err.to_string()))
}

fn parse_env_path(key: &str, path: &str) -> Result<Option<PathBuf>, ConfigError> {
    env_var::<String>(key)
        .map(|value| value.map(PathBuf::from))
        .map_err(|err| ConfigError::at(path, err.to_string()))
}

fn resolve_scalar<T: Clone>(cli: Option<T>, file: Option<T>, env: Option<T>, default: T) -> T {
    cli.or(file).or(env).unwrap_or(default)
}

fn resolve_optional<T>(cli: Option<T>, file: Option<T>, env: Option<T>) -> Option<T> {
    cli.or(file).or(env)
}

fn resolve_positive_flag(
    cli_present: bool,
    file: Option<bool>,
    env: Option<bool>,
    default: bool,
) -> bool {
    if cli_present {
        true
    } else {
        file.or(env).unwrap_or(default)
    }
}

fn normalise_enum_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_lock, EnvGuard};
    use std::fs;

    #[test]
    fn cli_overrides_file_and_env() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_METRICS_ENABLED"]);
        guard.set("OBZENFLOW_METRICS_ENABLED", "false");

        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[server]
enabled = false

[metrics]
enabled = false
exporter = "console"
"#,
        )
        .unwrap();

        let cli = FlowConfig::try_parse_from([
            "obzenflow",
            "--config",
            config_path.to_str().unwrap(),
            "--server",
        ])
        .unwrap();

        let resolved = cli.resolve(None, true).unwrap();
        assert!(resolved.server.enabled);
        assert!(!resolved.metrics.enabled);
        assert_eq!(resolved.metrics.exporter, MetricsExporterKind::Console);
    }

    #[test]
    fn autodiscovery_obeys_enable_flag() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[server]
enabled = true
"#,
        )
        .unwrap();

        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(tempdir.path()).unwrap();

        let cli = FlowConfig::try_parse_from(["obzenflow"]).unwrap();
        assert!(cli.clone().resolve(None, true).unwrap().server.enabled);
        assert!(!cli.resolve(None, false).unwrap().server.enabled);

        std::env::set_current_dir(cwd).unwrap();
    }

    #[test]
    fn replay_validation_reports_config_path() {
        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--replay-from", "/tmp/missing"]).unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at replay.from: must be a directory: /tmp/missing"
        );
    }

    #[test]
    fn invalid_metrics_exporter_reports_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[metrics]
exporter = "statsd"
"#,
        )
        .unwrap();

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--config", config_path.to_str().unwrap()])
                .unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at metrics.exporter: unknown value \"statsd\"; expected one of prometheus, console, noop"
        );
    }

    #[test]
    fn builder_config_file_is_used_when_cli_has_no_config() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[server]
enabled = true
port = 9876
"#,
        )
        .unwrap();

        let cli = FlowConfig::try_parse_from(["obzenflow"]).unwrap();
        let resolved = cli.resolve(Some(config_path.clone()), false).unwrap();
        assert!(resolved.server.enabled);
        assert_eq!(resolved.server.port, 9876);
    }

    #[test]
    fn cli_config_overrides_builder_config_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let builder_path = tempdir.path().join("builder.toml");
        let cli_path = tempdir.path().join("cli.toml");

        fs::write(
            &builder_path,
            r#"
[server]
enabled = true
port = 1111
"#,
        )
        .unwrap();

        fs::write(
            &cli_path,
            r#"
[server]
enabled = true
port = 2222
"#,
        )
        .unwrap();

        let cli = FlowConfig::try_parse_from(["obzenflow", "--config", cli_path.to_str().unwrap()])
            .unwrap();

        let resolved = cli.resolve(Some(builder_path.clone()), true).unwrap();
        assert!(resolved.server.enabled);
        assert_eq!(resolved.server.port, 2222);
    }

    #[test]
    fn env_layer_applies_when_no_cli_or_file() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_METRICS_ENABLED", "OBZENFLOW_METRICS_EXPORTER"]);
        guard.set("OBZENFLOW_METRICS_ENABLED", "false");
        guard.set("OBZENFLOW_METRICS_EXPORTER", "console");

        let cli = FlowConfig::try_parse_from(["obzenflow"]).unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        assert!(!resolved.metrics.enabled);
        assert_eq!(resolved.metrics.exporter, MetricsExporterKind::Console);
    }

    #[test]
    fn deny_unknown_fields_rejects_unknown_keys_in_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[server]
enabled = true
frobnicate = true
"#,
        )
        .unwrap();

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--config", config_path.to_str().unwrap()])
                .unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("failed to parse"));
        assert!(rendered.contains("unknown field"));
    }

    #[test]
    fn wrong_type_in_toml_reports_parse_error() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[server]
enabled = true
port = "abc"
"#,
        )
        .unwrap();

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--config", config_path.to_str().unwrap()])
                .unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("failed to parse"));
    }
}
