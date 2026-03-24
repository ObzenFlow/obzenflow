// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned bootstrap settings shared across infra, DSL, adapters, and runtime.
//!
//! These values are resolved once by the hosting shell (typically `FlowApplication`)
//! and then read by lower layers during flow build and execution.

use std::mem;
use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

#[cfg(test)]
use std::sync::{Mutex, MutexGuard};

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

fn storage() -> &'static RwLock<BootstrapConfig> {
    static BOOTSTRAP: OnceLock<RwLock<BootstrapConfig>> = OnceLock::new();
    BOOTSTRAP.get_or_init(|| RwLock::new(BootstrapConfig::default()))
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
///
/// FlowApplication bootstrap is currently a single-run-per-process contract.
/// In debug builds, overlapping installs panic so accidental concurrent host
/// runs fail loudly instead of racing through shared mutable bootstrap state.
pub fn install_bootstrap_config(config: BootstrapConfig) -> BootstrapConfigGuard {
    #[cfg(debug_assertions)]
    let active_install = ActiveInstallLease::acquire();

    let previous = {
        let mut bootstrap = storage().write().expect("bootstrap config lock poisoned");
        mem::replace(&mut *bootstrap, config)
    };

    BootstrapConfigGuard {
        previous: Some(previous),
        #[cfg(debug_assertions)]
        active_install: Some(active_install),
    }
}

pub struct BootstrapConfigGuard {
    previous: Option<BootstrapConfig>,
    #[cfg(debug_assertions)]
    active_install: Option<ActiveInstallLease>,
}

impl Drop for BootstrapConfigGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            *storage().write().expect("bootstrap config lock poisoned") = previous;
        }
        #[cfg(debug_assertions)]
        let _ = self.active_install.take();
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

#[cfg(debug_assertions)]
fn active_install_flag() -> &'static AtomicBool {
    static ACTIVE_INSTALL: OnceLock<AtomicBool> = OnceLock::new();
    ACTIVE_INSTALL.get_or_init(|| AtomicBool::new(false))
}

#[cfg(debug_assertions)]
struct ActiveInstallLease;

#[cfg(debug_assertions)]
impl ActiveInstallLease {
    fn acquire() -> Self {
        let was_active = active_install_flag()
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err();
        debug_assert!(
            !was_active,
            "install_bootstrap_config() does not support overlapping FlowApplication runs in the same process"
        );
        Self
    }
}

#[cfg(debug_assertions)]
impl Drop for ActiveInstallLease {
    fn drop(&mut self) {
        active_install_flag().store(false, Ordering::Release);
    }
}

#[cfg(test)]
pub(crate) fn bootstrap_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("bootstrap test lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_defaults_are_sensible() {
        let _lock = bootstrap_test_lock();
        let _guard = install_bootstrap_config(BootstrapConfig::default());

        let bootstrap = bootstrap_config();
        assert_eq!(bootstrap.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(bootstrap.startup_mode, StartupMode::Auto);
        assert_eq!(bootstrap.replay, None);
        assert_eq!(bootstrap.metrics, MetricsBootstrap::default());
    }

    #[test]
    fn bootstrap_guard_restores_previous_config() {
        let _lock = bootstrap_test_lock();
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
            assert!(!metrics_bootstrap().enabled);
        }

        assert_eq!(bootstrap_config(), baseline);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn overlapping_install_panics_in_debug_builds() {
        let _lock = bootstrap_test_lock();
        let _guard = install_bootstrap_config(BootstrapConfig::default());

        let result = std::panic::catch_unwind(|| {
            let _nested = install_bootstrap_config(BootstrapConfig::default());
        });

        assert!(result.is_err());
    }
}
