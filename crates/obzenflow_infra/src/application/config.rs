// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::env::{env_bool, env_var};
use clap::{ArgAction, Parser, ValueEnum};
use obzenflow_core::journal::RUN_MANIFEST_FILENAME;
use obzenflow_core::web::AuthPolicy;
use obzenflow_runtime::bootstrap::{
    BootstrapConfig, MetricsBootstrap, MetricsExporterKind, ReplayVerb,
};
use serde::Deserialize;
use std::collections::BTreeMap;
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

/// Server-mode behaviour when the pipeline reaches a terminal state
/// (FLOWIP-114d): registered means the control plane is serving.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OnTerminalArg {
    /// Run the close sequence and exit with a code mapped from the terminal state (default).
    Exit,
    /// Stay up serving the control plane; the heartbeat keeps renewing with the terminal phase.
    Park,
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

    /// Resume a recorded run: catch up on the archive with effects suppressed,
    /// then continue live from the recorded high-water mark (FLOWIP-120n).
    ///
    /// The path must be the exact run directory containing `run_manifest.json`.
    /// Mutually exclusive with `--replay-from`.
    #[arg(long, conflicts_with = "replay_from")]
    pub resume_from: Option<PathBuf>,

    /// Allow replaying from incomplete archives (failed/unknown/missing system.log).
    ///
    /// This is intended for debugging; outputs may be partial and not suitable for regression comparison.
    #[arg(long, action = ArgAction::SetTrue)]
    pub allow_incomplete_archive: bool,

    /// Allow `--replay-from` or `--resume-from` to proceed past sinks whose
    /// delivery path is non-idempotent or undeclared (FLOWIP-120n F16,
    /// FLOWIP-120v).
    ///
    /// Both archive verbs re-execute recorded work at sinks, so those sinks
    /// may duplicate their external writes. Requires an archive verb.
    #[arg(long, action = ArgAction::SetTrue)]
    pub allow_duplicate_sink_delivery: bool,

    /// Verify the replay output against the source archive after completion
    /// (FLOWIP-095j). Requires `--replay-from`. The source archive must remain
    /// present through completion; on a fully certified match the run prints
    /// "output matched the original run, 0 differences" and exits 0.
    #[arg(long, action = ArgAction::SetTrue)]
    pub verify: bool,

    /// Behaviour when the pipeline reaches a terminal state under --server
    /// (FLOWIP-114d): exit the process (default) or park until a signal.
    #[arg(long, value_enum)]
    pub server_on_terminal: Option<OnTerminalArg>,

    /// Enable Studio phonebook registration (FLOWIP-114d).
    #[arg(long, action = ArgAction::SetTrue)]
    pub studio_enabled: bool,

    /// Phonebook base URL for Studio registration.
    #[arg(long)]
    pub studio_phonebook_url: Option<String>,

    /// Operator-facing job identity carried in Studio registrations.
    #[arg(long)]
    pub studio_job_id: Option<String>,

    /// Reachable base URL this runtime advertises in its registration.
    #[arg(long)]
    pub studio_advertise_url: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStartupConfig {
    pub server: ResolvedServerConfig,
    pub runtime: ResolvedStartupRuntimeConfig,
    pub metrics: ResolvedMetricsConfig,
    pub replay: Option<ResolvedReplayConfig>,
    /// FLOWIP-114d: Some when studio.enabled resolves true; the enabled flag
    /// collapses into the Option.
    #[cfg(feature = "studio-registration")]
    pub studio: Option<ResolvedStudioConfig>,
    /// FLOWIP-010 §7: the immutable runtime config snapshot, host-owned for
    /// the run and handed to the flow build as `FlowBuildContext`.
    pub runtime_config: std::sync::Arc<obzenflow_runtime::runtime_config::ResolvedRuntimeConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedServerConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub max_body_size_bytes: usize,
    pub request_timeout_secs: u64,
    pub startup_mode: StartupMode,
    pub on_terminal: OnTerminalArg,
    pub cors_mode: CorsModeArg,
    pub cors_allow_origin: Vec<String>,
    pub control_plane_auth: Option<AuthPolicy>,
}

/// FLOWIP-114d Studio registration settings, resolved and validated.
/// Feature-gated with its only consumer (the heartbeat client); a build
/// without the feature fails loud in resolve() when studio.enabled is set.
#[cfg(feature = "studio-registration")]
#[derive(Debug, Clone)]
pub(crate) struct ResolvedStudioConfig {
    pub phonebook_url: String,
    pub job_id: String,
    pub advertise_url: String,
    pub lease_ttl_secs: u64,
    pub renew_interval_secs: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStartupRuntimeConfig {
    pub shutdown_timeout: Duration,
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
    /// FLOWIP-120n F16: resume past non-idempotent/undeclared sinks,
    /// accepting duplicated delivery during catch-up.
    pub allow_duplicate_sink_delivery: bool,
    /// FLOWIP-095j: verify the replay output against `from` after completion.
    pub verify: bool,
    /// FLOWIP-120n: which verb selected the archive (replay drains, resume
    /// continues live).
    pub verb: ReplayVerb,
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
                    allow_duplicate_sink_delivery: replay.allow_duplicate_sink_delivery,
                    verb: replay.verb,
                }
            }),
            // The host opens the archive at launch and fills this in before the
            // install (FLOWIP-120u); config resolution carries settings only.
            replay_archive: None,
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
    pub(crate) fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
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
pub(crate) struct RawFileStartupConfig {
    server: RawFileServerConfig,
    pub(crate) runtime: RawFileRuntimeConfig,
    metrics: RawFileMetricsConfig,
    pub(crate) replay: RawFileReplayConfig,
    /// FLOWIP-010: runtime-owned knob namespaces below. Scope legality is
    /// encoded in the struct shapes (§4c layout), so `deny_unknown_fields`
    /// rejects wrong-scope entries at parse.
    pub(crate) contracts: RawFileContractsConfig,
    pub(crate) effects: RawFileEffectsConfig,
    pub(crate) ai: RawFileAiConfig,
    /// FLOWIP-114d: Studio phonebook registration (010h startup layer).
    studio: RawFileStudioConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileServerConfig {
    enabled: Option<bool>,
    host: Option<String>,
    port: Option<i64>,
    startup_mode: Option<String>,
    on_terminal: Option<String>,
    max_body_size_bytes: Option<i64>,
    request_timeout_secs: Option<i64>,
    cors: RawFileCorsConfig,
    control_plane_auth: RawFileControlPlaneAuthConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileStudioConfig {
    enabled: Option<bool>,
    phonebook_url: Option<String>,
    job_id: Option<String>,
    advertise_url: Option<String>,
    lease_ttl_secs: Option<i64>,
    renew_interval_secs: Option<i64>,
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
pub(crate) struct RawFileRuntimeConfig {
    shutdown_timeout_secs: Option<i64>,
    surface_metrics_interval_secs: Option<i64>,
    // FLOWIP-010 runtime knobs (global scope at bare keys).
    pub(crate) max_lineage_depth: Option<i64>,
    pub(crate) cycle_max_iterations: Option<i64>,
    pub(crate) heartbeat_interval: Option<i64>,
    pub(crate) metrics_drain_timeout_ms: Option<i64>,
    pub(crate) flow: RawRuntimeFlowScope,
    pub(crate) stages: BTreeMap<String, RawRuntimeStageScope>,
    pub(crate) backpressure: RawBackpressureConfig,
}

/// `[runtime.flow]`: flow-scoped entries for Flow-or-finer-target knobs.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawRuntimeFlowScope {
    pub(crate) max_lineage_depth: Option<i64>,
    pub(crate) cycle_max_iterations: Option<i64>,
    pub(crate) heartbeat_interval: Option<i64>,
}

/// `[runtime.stages.<key>]`: stage-scoped entries for Stage-target knobs
/// only (no `cycle_max_iterations` here: it is Flow-target).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawRuntimeStageScope {
    pub(crate) max_lineage_depth: Option<i64>,
    pub(crate) heartbeat_interval: Option<i64>,
}

/// `[runtime.backpressure]` with the §4c nested edge layout.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawBackpressureConfig {
    pub(crate) mode: Option<String>,
    pub(crate) window: Option<i64>,
    pub(crate) stall_timeout_ms: Option<i64>,
    pub(crate) flow: RawBackpressureFields,
    pub(crate) stages: BTreeMap<String, RawBackpressureStageScope>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawBackpressureFields {
    pub(crate) mode: Option<String>,
    pub(crate) window: Option<i64>,
    pub(crate) stall_timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawBackpressureStageScope {
    pub(crate) mode: Option<String>,
    pub(crate) window: Option<i64>,
    pub(crate) stall_timeout_ms: Option<i64>,
    pub(crate) edges: BTreeMap<String, RawBackpressureFields>,
}

/// `[contracts]` (Global-target knobs only; no scope sub-tables).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawFileContractsConfig {
    pub(crate) source_contract_strict_mode: Option<String>,
}

/// `[effects]` (FLOWIP-120c absorption; Stage-target, so no edge tables).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawFileEffectsConfig {
    pub(crate) circuit_breaker: RawBreakerFields,
    pub(crate) rate_limiter: RawLimiterFields,
    pub(crate) flow: RawEffectsScopeFields,
    pub(crate) stages: BTreeMap<String, RawEffectsScopeFields>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawEffectsScopeFields {
    pub(crate) circuit_breaker: RawBreakerFields,
    pub(crate) rate_limiter: RawLimiterFields,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawBreakerFields {
    pub(crate) threshold: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawLimiterFields {
    pub(crate) events_per_second: Option<f64>,
    pub(crate) burst_capacity: Option<f64>,
}

/// `[ai]` with the `[ai.models]` namespace (absorbs `ModelConfig`'s env
/// surface; `api_key_env` is the visible secret reference, §13).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawFileAiConfig {
    pub(crate) models: RawFileAiModelsConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawFileAiModelsConfig {
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) base_url: Option<String>,
    pub(crate) api_key_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawFileMetricsConfig {
    enabled: Option<bool>,
    exporter: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RawFileReplayConfig {
    pub(crate) from: Option<PathBuf>,
    pub(crate) resume_from: Option<PathBuf>,
    pub(crate) allow_incomplete_archive: Option<bool>,
    pub(crate) allow_duplicate_sink_delivery: Option<bool>,
    pub(crate) verify: Option<bool>,
}

impl FlowConfig {
    pub(crate) fn parse_and_resolve(
        builder_config_file: Option<PathBuf>,
        enable_autodiscovery: bool,
    ) -> Result<ResolvedStartupConfig, ConfigError> {
        Self::parse().resolve(builder_config_file, enable_autodiscovery)
    }

    pub(crate) fn parse_and_resolve_from<I, T>(
        args: I,
        builder_config_file: Option<PathBuf>,
        enable_autodiscovery: bool,
    ) -> Result<ResolvedStartupConfig, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        Self::try_parse_from(args)
            .map_err(|err| ConfigError::global(err.to_string()))?
            .resolve(builder_config_file, enable_autodiscovery)
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
        // FLOWIP-010 Phase A: the runtime config snapshot resolves from the
        // same raw inputs, once, and rides the resolved startup config so
        // the host owns it for the run (§7 carrier).
        let runtime_config = std::sync::Arc::new(
            super::runtime_config_sources::build_runtime_config_snapshot(&self, &file)?,
        );
        let control_plane_auth = self.resolve_control_plane_auth(&file)?;
        // FLOWIP-114d: captured before resolve() starts partially moving
        // `self` and `file` field-by-field below.
        #[cfg(feature = "studio-registration")]
        let studio_inputs = StudioResolveInputs {
            cli_enabled: self.studio_enabled,
            cli_phonebook_url: self.studio_phonebook_url.clone(),
            cli_job_id: self.studio_job_id.clone(),
            cli_advertise_url: self.studio_advertise_url.clone(),
            file: file.studio.clone(),
        };
        #[cfg(not(feature = "studio-registration"))]
        let studio_inputs = (self.studio_enabled, file.studio.enabled);

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
        let on_terminal = self
            .server_on_terminal
            .or(parse_on_terminal(
                file.server.on_terminal.as_deref(),
                "server.on_terminal",
            )?)
            .or(parse_env_on_terminal()?)
            .unwrap_or(OnTerminalArg::Exit);

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
            on_terminal,
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

        let runtime = ResolvedStartupRuntimeConfig {
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
        let verify = resolve_positive_flag(
            self.verify,
            file.replay.verify,
            parse_env_bool("OBZENFLOW_REPLAY_VERIFY", "replay.verify")?,
            false,
        );
        let resume_from = resolve_optional(
            self.resume_from,
            file.replay.resume_from,
            parse_env_path("OBZENFLOW_RESUME_FROM", "replay.resume_from")?,
        );
        let allow_duplicate_sink_delivery = resolve_positive_flag(
            self.allow_duplicate_sink_delivery,
            file.replay.allow_duplicate_sink_delivery,
            parse_env_bool(
                "OBZENFLOW_ALLOW_DUPLICATE_SINK_DELIVERY",
                "replay.allow_duplicate_sink_delivery",
            )?,
            false,
        );
        if replay_from.is_some() && resume_from.is_some() {
            return Err(ConfigError::at(
                "replay.resume_from",
                "mutually exclusive with replay.from",
            ));
        }
        if verify && resume_from.is_some() {
            return Err(ConfigError::at(
                "replay.verify",
                "requires replay.from; a resume continues live and cannot be verified as a bounded replay",
            ));
        }
        if allow_duplicate_sink_delivery && replay_from.is_none() && resume_from.is_none() {
            return Err(ConfigError::at(
                "replay.allow_duplicate_sink_delivery",
                "requires replay.from or replay.resume_from; both archive verbs re-deliver recorded work to sinks",
            ));
        }

        let replay = match (replay_from, resume_from) {
            (Some(from), None) => Some(ResolvedReplayConfig {
                from,
                allow_incomplete_archive,
                allow_duplicate_sink_delivery,
                verify,
                verb: ReplayVerb::Replay,
            }),
            (None, Some(from)) => Some(ResolvedReplayConfig {
                from,
                allow_incomplete_archive,
                allow_duplicate_sink_delivery,
                verify: false,
                verb: ReplayVerb::Resume,
            }),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("rejected above"),
        };

        if let Some(replay) = &replay {
            validate_replay(replay)?;
        } else if allow_incomplete_archive {
            return Err(ConfigError::at(
                "replay.allow_incomplete_archive",
                "cannot be true without replay.from or replay.resume_from",
            ));
        } else if verify {
            return Err(ConfigError::at(
                "replay.verify",
                "cannot be true without replay.from",
            ));
        }

        if server.control_plane_auth.is_some() && !server.enabled {
            return Err(ConfigError::at(
                "server.control_plane_auth",
                "requires server.enabled = true",
            ));
        }

        // FLOWIP-114d feature guard: no inert config. A binary without the
        // heartbeat client refuses studio.enabled rather than ignoring it.
        #[cfg(not(feature = "studio-registration"))]
        {
            let (cli_enabled, file_enabled) = studio_inputs;
            let studio_enabled = resolve_positive_flag(
                cli_enabled,
                file_enabled,
                parse_env_bool("OBZENFLOW_STUDIO_ENABLED", "studio.enabled")?,
                false,
            );
            if studio_enabled {
                return Err(ConfigError::at(
                    "studio.enabled",
                    "this binary was built without the studio-registration feature; \
                     rebuild with features = [\"studio-registration\"]",
                ));
            }
        }
        #[cfg(feature = "studio-registration")]
        let studio = resolve_studio(studio_inputs, &server)?;

        Ok(ResolvedStartupConfig {
            server,
            runtime,
            metrics,
            replay,
            #[cfg(feature = "studio-registration")]
            studio,
            runtime_config,
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

pub(crate) fn autodiscover_config(enabled: bool) -> Option<PathBuf> {
    if !enabled {
        return None;
    }

    let path = PathBuf::from("obzenflow.toml");
    path.is_file().then_some(path)
}

pub(crate) fn load_file_config(path: Option<&Path>) -> Result<RawFileStartupConfig, ConfigError> {
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

/// FLOWIP-114d: studio-relevant CLI and file inputs, captured before
/// `resolve()` partially moves `self` and `file`.
#[cfg(feature = "studio-registration")]
struct StudioResolveInputs {
    cli_enabled: bool,
    cli_phonebook_url: Option<String>,
    cli_job_id: Option<String>,
    cli_advertise_url: Option<String>,
    file: RawFileStudioConfig,
}

/// FLOWIP-114d: resolve and cross-validate the `[studio]` block. Returns
/// None when registration is disabled; every error names its field.
#[cfg(feature = "studio-registration")]
fn resolve_studio(
    inputs: StudioResolveInputs,
    server: &ResolvedServerConfig,
) -> Result<Option<ResolvedStudioConfig>, ConfigError> {
    let enabled = resolve_positive_flag(
        inputs.cli_enabled,
        inputs.file.enabled,
        parse_env_bool("OBZENFLOW_STUDIO_ENABLED", "studio.enabled")?,
        false,
    );
    if !enabled {
        return Ok(None);
    }
    if !server.enabled {
        return Err(ConfigError::at(
            "studio.enabled",
            "requires server.enabled = true; registration is meaningless without the control plane",
        ));
    }
    let phonebook_url = resolve_optional(
        inputs.cli_phonebook_url,
        inputs.file.phonebook_url,
        parse_env_string("OBZENFLOW_STUDIO_PHONEBOOK_URL", "studio.phonebook_url")?,
    )
    .ok_or_else(|| {
        ConfigError::at(
            "studio.phonebook_url",
            "required when studio.enabled = true",
        )
    })?;
    let job_id = resolve_optional(
        inputs.cli_job_id,
        inputs.file.job_id,
        parse_env_string("OBZENFLOW_STUDIO_JOB_ID", "studio.job_id")?,
    )
    .ok_or_else(|| ConfigError::at("studio.job_id", "required when studio.enabled = true"))?;
    let advertise_url = resolve_optional(
        inputs.cli_advertise_url,
        inputs.file.advertise_url,
        parse_env_string("OBZENFLOW_STUDIO_ADVERTISE_URL", "studio.advertise_url")?,
    )
    .ok_or_else(|| {
        ConfigError::at(
            "studio.advertise_url",
            "required when studio.enabled = true",
        )
    })?;
    if advertise_url.contains("0.0.0.0") {
        return Err(ConfigError::at(
            "studio.advertise_url",
            "must be a URL the browser can reach, never a 0.0.0.0 bind address",
        ));
    }
    let lease_ttl_secs = resolve_scalar(
        None,
        parse_non_negative_u64(inputs.file.lease_ttl_secs, "studio.lease_ttl_secs")?,
        parse_env_u64("OBZENFLOW_STUDIO_LEASE_TTL_SECS", "studio.lease_ttl_secs")?,
        15,
    );
    let renew_interval_secs = resolve_scalar(
        None,
        parse_non_negative_u64(
            inputs.file.renew_interval_secs,
            "studio.renew_interval_secs",
        )?,
        parse_env_u64(
            "OBZENFLOW_STUDIO_RENEW_INTERVAL_SECS",
            "studio.renew_interval_secs",
        )?,
        5,
    );
    if renew_interval_secs >= lease_ttl_secs {
        return Err(ConfigError::at(
            "studio.renew_interval_secs",
            format!(
                "must be < studio.lease_ttl_secs ({lease_ttl_secs}), got {renew_interval_secs}"
            ),
        ));
    }

    // The Studio bundle is served from the phonebook origin while this
    // runtime listens on another port, so every browser request here is
    // cross-origin; the CORS posture must admit that origin exactly.
    let phonebook_origin = url_origin(&phonebook_url).ok_or_else(|| {
        ConfigError::at(
            "studio.phonebook_url",
            format!("cannot derive an origin from {phonebook_url:?}"),
        )
    })?;
    match server.cors_mode {
        CorsModeArg::AllowAnyOrigin => {}
        CorsModeArg::AllowList => {
            if !server
                .cors_allow_origin
                .iter()
                .any(|origin| origin == &phonebook_origin)
            {
                return Err(ConfigError::at(
                    "server.cors.allow_origins",
                    format!(
                        "must contain the phonebook origin {phonebook_origin:?} when \
                         studio.enabled = true; 127.0.0.1 and localhost are different \
                         origins, so list both spellings"
                    ),
                ));
            }
        }
        CorsModeArg::SameOrigin => {
            return Err(ConfigError::at(
                "server.cors.mode",
                "same-origin blocks the Studio browser (served from the phonebook \
                 origin); use allow-list containing the phonebook origin",
            ));
        }
    }

    Ok(Some(ResolvedStudioConfig {
        phonebook_url,
        job_id,
        advertise_url,
        lease_ttl_secs,
        renew_interval_secs,
    }))
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

#[cfg(feature = "studio-registration")]
fn parse_env_string(key: &str, path: &str) -> Result<Option<String>, ConfigError> {
    env_var::<String>(key).map_err(|err| ConfigError::at(path, err.to_string()))
}

fn parse_on_terminal(
    value: Option<&str>,
    path: &str,
) -> Result<Option<OnTerminalArg>, ConfigError> {
    match value.map(normalise_enum_token) {
        None => Ok(None),
        Some(value) if value == "exit" => Ok(Some(OnTerminalArg::Exit)),
        Some(value) if value == "park" => Ok(Some(OnTerminalArg::Park)),
        Some(value) => Err(ConfigError::at(
            path,
            format!("unknown value {value:?}; expected one of exit, park"),
        )),
    }
}

fn parse_env_on_terminal() -> Result<Option<OnTerminalArg>, ConfigError> {
    let value = env_var::<String>("OBZENFLOW_SERVER_ON_TERMINAL")
        .map_err(|err| ConfigError::at("server.on_terminal", err.to_string()))?;
    parse_on_terminal(value.as_deref(), "server.on_terminal")
}

/// Derive scheme://authority from a URL string; the phonebook-origin CORS
/// check compares origins exactly (spellings are distinct origins).
#[cfg(feature = "studio-registration")]
fn url_origin(url: &str) -> Option<String> {
    let scheme_end = url.find("://")?;
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return None;
    }
    Some(format!("{}://{authority}", &url[..scheme_end]))
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

    /// FLOWIP-114d: a studio-enabled config file whose CORS allow-list carries
    /// the phonebook origin; the baseline every studio test perturbs.
    #[cfg(feature = "studio-registration")]
    fn studio_config_toml() -> &'static str {
        r#"
[server]
enabled = true
port = 9090

[server.cors]
mode = "allow-list"
allow_origins = ["http://127.0.0.1:7010"]

[studio]
enabled = true
phonebook_url = "http://127.0.0.1:7010"
job_id = "demo_job"
advertise_url = "http://127.0.0.1:9090"
"#
    }

    #[cfg(feature = "studio-registration")]
    fn resolve_studio_file(
        contents: &str,
        extra_args: &[&str],
    ) -> Result<ResolvedStartupConfig, ConfigError> {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(&config_path, contents).unwrap();
        let mut args = vec!["obzenflow", "--config", config_path.to_str().unwrap()];
        args.extend_from_slice(extra_args);
        FlowConfig::try_parse_from(args)
            .unwrap()
            .resolve(None, true)
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_resolves_with_defaults() {
        let _lock = env_lock();
        let resolved = resolve_studio_file(studio_config_toml(), &[]).unwrap();
        let studio = resolved.studio.expect("studio enabled");
        assert_eq!(studio.job_id, "demo_job");
        assert_eq!(studio.lease_ttl_secs, 15);
        assert_eq!(studio.renew_interval_secs, 5);
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_cli_overrides_file() {
        let _lock = env_lock();
        let resolved =
            resolve_studio_file(studio_config_toml(), &["--studio-job-id", "cli_job"]).unwrap();
        assert_eq!(resolved.studio.expect("studio enabled").job_id, "cli_job");
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_env_below_file() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_STUDIO_JOB_ID"]);
        guard.set("OBZENFLOW_STUDIO_JOB_ID", "env_job");
        let resolved = resolve_studio_file(studio_config_toml(), &[]).unwrap();
        // The file carries demo_job; env sits below the file tier.
        assert_eq!(resolved.studio.expect("studio enabled").job_id, "demo_job");
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_env_fills_when_file_omits() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_STUDIO_JOB_ID"]);
        guard.set("OBZENFLOW_STUDIO_JOB_ID", "env_job");
        let contents = studio_config_toml().replace("job_id = \"demo_job\"\n", "");
        let resolved = resolve_studio_file(&contents, &[]).unwrap();
        assert_eq!(resolved.studio.expect("studio enabled").job_id, "env_job");
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_requireds_fail_naming_the_field() {
        let _lock = env_lock();
        for (field, needle) in [
            ("phonebook_url", "studio.phonebook_url"),
            ("job_id", "studio.job_id"),
            ("advertise_url", "studio.advertise_url"),
        ] {
            let contents = studio_config_toml()
                .lines()
                .filter(|line| !line.starts_with(field))
                .collect::<Vec<_>>()
                .join("\n");
            let error = resolve_studio_file(&contents, &[]).unwrap_err();
            assert!(
                error.to_string().contains(needle),
                "missing {field} must name {needle}, got: {error}"
            );
        }
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_disabled_ignores_other_fields() {
        let _lock = env_lock();
        let contents = studio_config_toml().replace(
            "enabled = true\nphonebook_url",
            "enabled = false\nphonebook_url",
        );
        let resolved = resolve_studio_file(&contents, &[]).unwrap();
        assert!(resolved.studio.is_none());
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_requires_server_enabled() {
        let _lock = env_lock();
        let contents = studio_config_toml().replacen("enabled = true", "enabled = false", 1);
        let error = resolve_studio_file(&contents, &[]).unwrap_err();
        assert!(error.to_string().contains("server.enabled"));
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_rejects_zeros_advertise_bind() {
        let _lock = env_lock();
        let contents = studio_config_toml().replace("http://127.0.0.1:9090", "http://0.0.0.0:9090");
        let error = resolve_studio_file(&contents, &[]).unwrap_err();
        assert!(error.to_string().contains("studio.advertise_url"));
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_rejects_renew_not_below_ttl() {
        let _lock = env_lock();
        let contents = format!(
            "{}lease_ttl_secs = 5\nrenew_interval_secs = 5\n",
            studio_config_toml()
        );
        let error = resolve_studio_file(&contents, &[]).unwrap_err();
        assert!(error.to_string().contains("studio.renew_interval_secs"));
    }

    #[cfg(feature = "studio-registration")]
    #[test]
    fn studio_cors_must_admit_phonebook_origin() {
        let _lock = env_lock();
        // Allow-list missing the phonebook origin: error suggests both spellings.
        let contents = studio_config_toml().replace(
            "allow_origins = [\"http://127.0.0.1:7010\"]",
            "allow_origins = [\"http://localhost:7010\"]",
        );
        let error = resolve_studio_file(&contents, &[]).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("server.cors.allow_origins"), "{message}");
        assert!(message.contains("both spellings"), "{message}");

        // same-origin blocks the Studio browser outright.
        let contents = studio_config_toml()
            .replace("mode = \"allow-list\"", "mode = \"same-origin\"")
            .replace("allow_origins = [\"http://127.0.0.1:7010\"]\n", "");
        let error = resolve_studio_file(&contents, &[]).unwrap_err();
        assert!(error.to_string().contains("server.cors.mode"));

        // allow-any-origin passes.
        let contents = studio_config_toml()
            .replace("mode = \"allow-list\"", "mode = \"allow-any-origin\"")
            .replace("allow_origins = [\"http://127.0.0.1:7010\"]\n", "");
        assert!(resolve_studio_file(&contents, &[])
            .unwrap()
            .studio
            .is_some());
    }

    #[test]
    fn on_terminal_resolves_exit_default_and_park() {
        let _lock = env_lock();
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(&config_path, "[server]\nenabled = true\n").unwrap();
        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--config", config_path.to_str().unwrap()])
                .unwrap();
        assert_eq!(
            cli.resolve(None, true).unwrap().server.on_terminal,
            OnTerminalArg::Exit
        );

        fs::write(
            &config_path,
            "[server]\nenabled = true\non_terminal = \"park\"\n",
        )
        .unwrap();
        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--config", config_path.to_str().unwrap()])
                .unwrap();
        assert_eq!(
            cli.resolve(None, true).unwrap().server.on_terminal,
            OnTerminalArg::Park
        );

        // CLI outranks the file.
        let cli = FlowConfig::try_parse_from([
            "obzenflow",
            "--config",
            config_path.to_str().unwrap(),
            "--server-on-terminal",
            "exit",
        ])
        .unwrap();
        assert_eq!(
            cli.resolve(None, true).unwrap().server.on_terminal,
            OnTerminalArg::Exit
        );
    }

    #[test]
    fn url_origin_derivation() {
        #[cfg(feature = "studio-registration")]
        {
            assert_eq!(
                url_origin("http://127.0.0.1:7010").as_deref(),
                Some("http://127.0.0.1:7010")
            );
            assert_eq!(
                url_origin("http://localhost:7010/registry/").as_deref(),
                Some("http://localhost:7010")
            );
            assert_eq!(url_origin("not-a-url"), None);
        }
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

    /// A directory that passes `validate_replay`: exists and holds a manifest.
    fn fake_archive_dir(tempdir: &tempfile::TempDir, name: &str) -> PathBuf {
        let archive = tempdir.path().join(name);
        fs::create_dir(&archive).unwrap();
        fs::write(archive.join(RUN_MANIFEST_FILENAME), "{}").unwrap();
        archive
    }

    #[test]
    fn resume_from_resolves_to_the_resume_verb() {
        let tempdir = tempfile::tempdir().unwrap();
        let archive = fake_archive_dir(&tempdir, "archive");

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--resume-from", archive.to_str().unwrap()])
                .unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        let replay = resolved.replay.expect("resume resolves a replay config");
        assert_eq!(replay.verb, ReplayVerb::Resume);
        assert_eq!(replay.from, archive);
        assert!(!replay.verify);
    }

    #[test]
    fn replay_from_keeps_the_replay_verb() {
        let tempdir = tempfile::tempdir().unwrap();
        let archive = fake_archive_dir(&tempdir, "archive");

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--replay-from", archive.to_str().unwrap()])
                .unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        let replay = resolved.replay.expect("replay resolves a replay config");
        assert_eq!(replay.verb, ReplayVerb::Replay);
        assert_eq!(replay.from, archive);
    }

    #[test]
    fn replay_from_and_resume_from_conflict_at_the_cli() {
        let err = FlowConfig::try_parse_from([
            "obzenflow",
            "--replay-from",
            "/tmp/a",
            "--resume-from",
            "/tmp/b",
        ])
        .unwrap_err();
        assert!(
            err.to_string().contains("cannot be used with"),
            "clap must reject the verb pair: {err}"
        );
    }

    #[test]
    fn file_resume_from_conflicts_with_cli_replay_from_at_resolve() {
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("obzenflow.toml");
        fs::write(
            &config_path,
            r#"
[replay]
resume_from = "/tmp/recorded"
"#,
        )
        .unwrap();

        let cli = FlowConfig::try_parse_from([
            "obzenflow",
            "--config",
            config_path.to_str().unwrap(),
            "--replay-from",
            "/tmp/other",
        ])
        .unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at replay.resume_from: mutually exclusive with replay.from"
        );
    }

    #[test]
    fn verify_with_resume_from_errors_at_resolve() {
        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--resume-from", "/tmp/recorded", "--verify"])
                .unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at replay.verify: requires replay.from; a resume continues live and cannot be verified as a bounded replay"
        );
    }

    #[test]
    fn allow_duplicate_sink_delivery_resolves_with_resume_from() {
        let tempdir = tempfile::tempdir().unwrap();
        let archive = fake_archive_dir(&tempdir, "archive");

        let cli = FlowConfig::try_parse_from([
            "obzenflow",
            "--resume-from",
            archive.to_str().unwrap(),
            "--allow-duplicate-sink-delivery",
        ])
        .unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        let replay = resolved
            .replay
            .as_ref()
            .expect("resume resolves a replay config");
        assert!(replay.allow_duplicate_sink_delivery);

        let bootstrap = resolved.bootstrap_config();
        assert!(
            bootstrap
                .replay
                .expect("bootstrap carries the replay config")
                .allow_duplicate_sink_delivery,
            "the opt-in must ride ReplayBootstrap to the resume sink gate"
        );
    }

    #[test]
    fn allow_duplicate_sink_delivery_defaults_to_false() {
        let tempdir = tempfile::tempdir().unwrap();
        let archive = fake_archive_dir(&tempdir, "archive");

        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--resume-from", archive.to_str().unwrap()])
                .unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        let replay = resolved.replay.expect("resume resolves a replay config");
        assert!(!replay.allow_duplicate_sink_delivery);
    }

    #[test]
    fn allow_duplicate_sink_delivery_resolves_with_replay_from() {
        let tempdir = tempfile::tempdir().unwrap();
        let archive = fake_archive_dir(&tempdir, "archive");

        let cli = FlowConfig::try_parse_from([
            "obzenflow",
            "--replay-from",
            archive.to_str().unwrap(),
            "--allow-duplicate-sink-delivery",
        ])
        .unwrap();
        let resolved = cli.resolve(None, false).unwrap();
        let replay = resolved
            .replay
            .as_ref()
            .expect("replay resolves a replay config");
        assert!(replay.allow_duplicate_sink_delivery);

        let bootstrap = resolved.bootstrap_config();
        assert!(
            bootstrap
                .replay
                .expect("bootstrap carries the replay config")
                .allow_duplicate_sink_delivery,
            "the opt-in must ride ReplayBootstrap to the replay sink gate"
        );
    }

    #[test]
    fn allow_duplicate_sink_delivery_without_archive_verb_errors_at_resolve() {
        let cli =
            FlowConfig::try_parse_from(["obzenflow", "--allow-duplicate-sink-delivery"]).unwrap();
        let err = cli.resolve(None, false).unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at replay.allow_duplicate_sink_delivery: requires replay.from or replay.resume_from; both archive verbs re-deliver recorded work to sinks"
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
