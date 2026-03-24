// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned bootstrap settings shared across infra, DSL, adapters, and runtime.
//!
//! These values are resolved once by the hosting shell (typically `FlowApplication`)
//! and then read by lower layers during flow build and execution.

use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

#[cfg(test)]
use std::sync::{Mutex, MutexGuard};

thread_local! {
    static INSTALL_OWNER: u8 = const { 0 };
}

/// Controls whether a pipeline starts automatically after materialisation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupMode {
    Auto,
    Manual,
}

impl StartupMode {
    /// Returns `true` when the host must manually start the pipeline.
    pub fn is_manual(self) -> bool {
        matches!(self, Self::Manual)
    }
}

/// The configured metrics exporter backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsExporterKind {
    Prometheus,
    Console,
    Noop,
}

/// Replay settings needed to bootstrap a flow from an existing run archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBootstrap {
    /// Path to a run archive directory containing `run_manifest.json`.
    pub archive_path: PathBuf,
    /// When true, allow replay from an incomplete archive.
    pub allow_incomplete_archive: bool,
}

/// Host-level metrics settings used during flow build and execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsBootstrap {
    /// When false, metrics are disabled and no exporter is started.
    pub enabled: bool,
    /// Which exporter implementation to use when `enabled` is true.
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

/// A resolved bootstrap snapshot shared across infra, DSL, adapters, and runtime.
///
/// These settings are resolved by the hosting shell (typically `FlowApplication`)
/// and installed for the lifetime of a single run via [`install_bootstrap_config`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    /// Graceful shutdown timeout for pipeline drain and stage teardown.
    pub shutdown_timeout: Duration,
    /// Host startup behaviour for pipeline execution.
    pub startup_mode: StartupMode,
    /// Optional replay bootstrap configuration.
    pub replay: Option<ReplayBootstrap>,
    /// Metrics bootstrap configuration.
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

/// Read the currently-installed bootstrap snapshot.
pub fn bootstrap_config() -> BootstrapConfig {
    storage()
        .read()
        .expect("bootstrap config lock poisoned")
        .clone()
}

/// Overwrite the process bootstrap snapshot.
///
/// Prefer [`install_bootstrap_config`] for host runs so the previous value is restored
/// on drop. This setter is primarily intended for benchmarks and tests.
pub fn set_bootstrap_config(config: BootstrapConfig) {
    *storage().write().expect("bootstrap config lock poisoned") = config;
}

/// Install a bootstrap snapshot for the lifetime of the returned guard.
///
/// This keeps bootstrap settings scoped to a single host run instead of
/// permanently caching the first configuration seen in the process.
///
/// Bootstrap settings are process-global, so only one active install is supported
/// at a time. Overlapping installs will wait until the prior guard is dropped.
pub fn install_bootstrap_config(config: BootstrapConfig) -> BootstrapConfigGuard {
    let active_install = ActiveInstallLease::acquire();

    let previous = {
        let mut bootstrap = storage().write().expect("bootstrap config lock poisoned");
        mem::replace(&mut *bootstrap, config)
    };

    BootstrapConfigGuard {
        previous: Some(previous),
        active_install,
    }
}

/// Restores the prior bootstrap snapshot on drop.
pub struct BootstrapConfigGuard {
    previous: Option<BootstrapConfig>,
    active_install: ActiveInstallLease,
}

impl Drop for BootstrapConfigGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            *storage().write().expect("bootstrap config lock poisoned") = previous;
        }
        let _ = &self.active_install;
    }
}

/// Graceful shutdown timeout for pipeline drain and teardown.
pub fn shutdown_timeout() -> Duration {
    bootstrap_config().shutdown_timeout
}

/// Returns `true` when the pipeline should not auto-run on materialisation.
pub fn startup_mode_manual() -> bool {
    bootstrap_config().startup_mode.is_manual()
}

/// Optional replay bootstrap parameters.
pub fn replay_bootstrap() -> Option<ReplayBootstrap> {
    bootstrap_config().replay
}

/// Metrics bootstrap settings.
pub fn metrics_bootstrap() -> MetricsBootstrap {
    bootstrap_config().metrics
}

fn active_install_owner() -> &'static AtomicUsize {
    static ACTIVE_INSTALL: OnceLock<AtomicUsize> = OnceLock::new();
    ACTIVE_INSTALL.get_or_init(|| AtomicUsize::new(0))
}

fn current_owner_id() -> usize {
    INSTALL_OWNER.with(|marker| marker as *const u8 as usize)
}

struct ActiveInstallLease;

impl ActiveInstallLease {
    fn acquire() -> Self {
        let owner = active_install_owner();
        let me = current_owner_id();

        // Catch the most likely deadlock case: re-entrant install on the same thread.
        // This cannot be made perfectly robust without passing an explicit run context.
        if owner.load(Ordering::Acquire) == me {
            panic!("install_bootstrap_config() does not support nested installs");
        }

        let mut attempts: u32 = 0;
        loop {
            if owner
                .compare_exchange(0, me, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Self;
            }

            attempts = attempts.saturating_add(1);
            if attempts <= 10 {
                std::hint::spin_loop();
            } else if attempts <= 100 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

impl Drop for ActiveInstallLease {
    fn drop(&mut self) {
        active_install_owner().store(0, Ordering::Release);
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

    #[test]
    fn nested_install_panics() {
        let _lock = bootstrap_test_lock();
        let _guard = install_bootstrap_config(BootstrapConfig::default());

        let result = std::panic::catch_unwind(|| {
            let _nested = install_bootstrap_config(BootstrapConfig::default());
        });

        assert!(result.is_err());
    }

    #[test]
    fn overlapping_installs_are_serialised() {
        use std::sync::mpsc;

        let _lock = bootstrap_test_lock();
        let guard = install_bootstrap_config(BootstrapConfig::default());

        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (acquired_tx, acquired_rx) = mpsc::channel::<()>();

        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _guard = install_bootstrap_config(BootstrapConfig::default());
            acquired_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(25));
        assert!(acquired_rx.try_recv().is_err());

        drop(guard);
        acquired_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        handle.join().unwrap();
    }
}
