// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Default metrics configuration for ObzenFlow
//!
//! This module provides the default configuration for metrics collection.
//!
//! Lower layers read from the runtime bootstrap context populated by the
//! hosting shell instead of parsing environment variables independently.

use crate::bootstrap::metrics_bootstrap;
#[cfg(test)]
use crate::bootstrap::{install_bootstrap_config, BootstrapConfig, MetricsBootstrap};

/// Default metrics configuration sourced from the runtime bootstrap context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultMetricsConfig {
    /// Whether metrics collection is enabled
    pub enabled: bool,
}

impl Default for DefaultMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: metrics_bootstrap().enabled,
        }
    }
}

impl DefaultMetricsConfig {
    /// Create a new DefaultMetricsConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a disabled metrics configuration
    pub fn disabled() -> Self {
        Self { enabled: false }
    }

    /// Check if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let _guard = install_bootstrap_config(BootstrapConfig::default());

        let config = DefaultMetricsConfig::default();
        assert!(config.enabled);
    }

    #[test]
    fn test_disabled_config() {
        let config = DefaultMetricsConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_bootstrap_override() {
        let _guard = install_bootstrap_config(BootstrapConfig {
            metrics: MetricsBootstrap {
                enabled: false,
                ..MetricsBootstrap::default()
            },
            ..BootstrapConfig::default()
        });
        let config = DefaultMetricsConfig::default();
        assert!(!config.enabled);
    }
}
