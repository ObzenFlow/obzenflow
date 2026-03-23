// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned bootstrap settings shared across infra, DSL, adapters, and runtime.
//!
//! These values are resolved once by the hosting shell (typically `FlowApplication`)
//! and then read by lower layers during flow build and execution.

use std::mem;
use std::path::PathBuf;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupMode {
    Auto,
    Manual,
}

impl StartupMode {
    pub fn is_manual(self) -> bool {
        matches!(self, Self::Manual)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsExporterKind {
    Prometheus,
    Console,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBootstrap {
    pub archive_path: PathBuf,
    pub allow_incomplete_archive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsBootstrap {
    pub enabled: bool,
    pub exporter: MetricsExporterKind,
}

impl Default for MetricsBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            exporter: MetricsExporterKind::Prometheus,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub shutdown_timeout: Duration,
    pub startup_mode: StartupMode,
    pub replay: Option<ReplayBootstrap>,
    pub metrics: MetricsBootstrap,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(30),
            startup_mode: StartupMode::Auto,
            replay: None,
            metrics: MetricsBootstrap::default(),
        }
    }
}

impl BootstrapConfig {
    fn from_legacy_env() -> Self {
        let startup_mode = match std::env::var("OBZENFLOW_STARTUP_MODE") {
            Ok(value) if value.eq_ignore_ascii_case("manual") => StartupMode::Manual,
            _ => StartupMode::Auto,
        };

        let replay = std::env::var_os("OBZENFLOW_REPLAY_FROM").map(|path| ReplayBootstrap {
            archive_path: PathBuf::from(path),
            allow_incomplete_archive: std::env::var("OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE")
                .ok()
                .is_some_and(|value| {
                    matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES")
                }),
        });

        let enabled = std::env::var("OBZENFLOW_METRICS_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true);
        let exporter = match std::env::var("OBZENFLOW_METRICS_EXPORTER").ok().as_deref() {
            Some("console") => MetricsExporterKind::Console,
            Some("noop") => MetricsExporterKind::Noop,
            _ => MetricsExporterKind::Prometheus,
        };

        Self {
            shutdown_timeout: std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(30)),
            startup_mode,
            replay,
            metrics: MetricsBootstrap { enabled, exporter },
        }
    }
}

fn storage() -> &'static RwLock<BootstrapConfig> {
    static BOOTSTRAP: OnceLock<RwLock<BootstrapConfig>> = OnceLock::new();
    BOOTSTRAP.get_or_init(|| RwLock::new(BootstrapConfig::from_legacy_env()))
}

pub fn bootstrap_config() -> BootstrapConfig {
    storage()
        .read()
        .expect("bootstrap config lock poisoned")
        .clone()
}

pub fn set_bootstrap_config(config: BootstrapConfig) {
    *storage().write().expect("bootstrap config lock poisoned") = config;
}

/// Install a bootstrap snapshot for the lifetime of the returned guard.
///
/// This keeps bootstrap settings scoped to a single host run instead of
/// permanently caching the first configuration seen in the process.
pub fn install_bootstrap_config(config: BootstrapConfig) -> BootstrapConfigGuard {
    let previous = {
        let mut bootstrap = storage().write().expect("bootstrap config lock poisoned");
        mem::replace(&mut *bootstrap, config)
    };

    BootstrapConfigGuard {
        previous: Some(previous),
    }
}

pub struct BootstrapConfigGuard {
    previous: Option<BootstrapConfig>,
}

impl Drop for BootstrapConfigGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            *storage().write().expect("bootstrap config lock poisoned") = previous;
        }
    }
}

pub fn shutdown_timeout() -> Duration {
    bootstrap_config().shutdown_timeout
}

pub fn startup_mode_manual() -> bool {
    bootstrap_config().startup_mode.is_manual()
}

pub fn replay_bootstrap() -> Option<ReplayBootstrap> {
    bootstrap_config().replay
}

pub fn metrics_bootstrap() -> MetricsBootstrap {
    bootstrap_config().metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_defaults_are_sensible() {
        let _guard = install_bootstrap_config(BootstrapConfig::default());

        let bootstrap = bootstrap_config();
        assert_eq!(bootstrap.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(bootstrap.startup_mode, StartupMode::Auto);
        assert_eq!(bootstrap.replay, None);
        assert_eq!(bootstrap.metrics, MetricsBootstrap::default());
    }

    #[test]
    fn bootstrap_guard_restores_previous_config() {
        let baseline = BootstrapConfig {
            shutdown_timeout: Duration::from_secs(45),
            startup_mode: StartupMode::Auto,
            replay: None,
            metrics: MetricsBootstrap {
                enabled: true,
                exporter: MetricsExporterKind::Console,
            },
        };
        set_bootstrap_config(baseline.clone());

        {
            let _guard = install_bootstrap_config(BootstrapConfig {
                shutdown_timeout: Duration::from_secs(5),
                startup_mode: StartupMode::Manual,
                replay: Some(ReplayBootstrap {
                    archive_path: PathBuf::from("/tmp/archive"),
                    allow_incomplete_archive: true,
                }),
                metrics: MetricsBootstrap {
                    enabled: false,
                    exporter: MetricsExporterKind::Noop,
                },
            });

            assert_eq!(shutdown_timeout(), Duration::from_secs(5));
            assert!(startup_mode_manual());
            assert_eq!(metrics_bootstrap().enabled, false);
        }

        assert_eq!(bootstrap_config(), baseline);
    }
}
