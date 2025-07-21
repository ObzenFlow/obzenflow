//! Shared constants for metrics collection across the system

/// Histogram configuration for processing time measurements
pub mod histogram {
    /// Minimum value for processing time histograms (1ms)
    pub const HISTOGRAM_MIN_MS: u64 = 1;
    
    /// Maximum value for processing time histograms (60 seconds)
    pub const HISTOGRAM_MAX_MS: u64 = 60_000;
    
    /// Significant figures for histogram precision
    pub const HISTOGRAM_SIGFIGS: u8 = 3;
}

/// Standard percentile values used across the system
pub mod percentiles {
    pub const QUANTILE_P50: f64 = 0.5;
    pub const QUANTILE_P90: f64 = 0.9;
    pub const QUANTILE_P95: f64 = 0.95;
    pub const QUANTILE_P99: f64 = 0.99;
    pub const QUANTILE_P999: f64 = 0.999;
}

// Re-export for convenience
pub use histogram::*;
pub use percentiles::*;