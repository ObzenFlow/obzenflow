//! Default metrics configuration for ObzenFlow
//!
//! This module provides the default configuration for metrics collection,
//! reading from environment variables to allow runtime configuration.

use std::env;

/// Default metrics configuration that reads from environment variables
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultMetricsConfig {
    /// Whether metrics collection is enabled
    pub enabled: bool,
    /// Taxonomy for stage metrics (e.g., "RED" for Rate, Errors, Duration)
    pub stage_taxonomy: String,
    /// Taxonomy for flow metrics (e.g., "GoldenSignals")
    pub flow_taxonomy: String,
}

impl Default for DefaultMetricsConfig {
    fn default() -> Self {
        // Read OBZENFLOW_METRICS_ENABLED environment variable
        let enabled = env::var("OBZENFLOW_METRICS_ENABLED")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        Self {
            enabled,
            stage_taxonomy: "RED".to_string(),
            flow_taxonomy: "GoldenSignals".to_string(),
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
        Self {
            enabled: false,
            stage_taxonomy: "RED".to_string(),
            flow_taxonomy: "GoldenSignals".to_string(),
        }
    }

    /// Check if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Use a mutex to ensure tests that modify environment variables don't interfere with each other
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_default_config() {
        let _guard = ENV_MUTEX.lock().unwrap();

        // Save current env var if exists
        let saved = env::var("OBZENFLOW_METRICS_ENABLED").ok();

        // Remove the env var to test default behavior
        env::remove_var("OBZENFLOW_METRICS_ENABLED");

        let config = DefaultMetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.stage_taxonomy, "RED");
        assert_eq!(config.flow_taxonomy, "GoldenSignals");

        // Restore original value if it existed
        if let Some(val) = saved {
            env::set_var("OBZENFLOW_METRICS_ENABLED", val);
        }
    }

    #[test]
    fn test_disabled_config() {
        let config = DefaultMetricsConfig::disabled();
        assert!(!config.enabled);
        assert_eq!(config.stage_taxonomy, "RED");
        assert_eq!(config.flow_taxonomy, "GoldenSignals");
    }

    #[test]
    fn test_env_var_override() {
        let _guard = ENV_MUTEX.lock().unwrap();

        // Save current env var if exists
        let saved = env::var("OBZENFLOW_METRICS_ENABLED").ok();

        // Test with enabled=false
        env::set_var("OBZENFLOW_METRICS_ENABLED", "false");
        let config = DefaultMetricsConfig::default();
        assert!(!config.enabled);

        // Test with enabled=true
        env::set_var("OBZENFLOW_METRICS_ENABLED", "true");
        let config = DefaultMetricsConfig::default();
        assert!(config.enabled);

        // Test with invalid value (should default to true)
        env::set_var("OBZENFLOW_METRICS_ENABLED", "invalid");
        let config = DefaultMetricsConfig::default();
        assert!(config.enabled);

        // Restore original value or remove
        match saved {
            Some(val) => env::set_var("OBZENFLOW_METRICS_ENABLED", val),
            None => env::remove_var("OBZENFLOW_METRICS_ENABLED"),
        }
    }
}
