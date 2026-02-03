// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Percentile type for metrics
//!
//! Strong type for percentile keys to avoid stringly-typed APIs

use serde::{Deserialize, Serialize};
use std::fmt;

/// Standard percentiles used in metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Percentile {
    /// 50th percentile (median)
    P50,
    /// 90th percentile
    P90,
    /// 95th percentile
    P95,
    /// 99th percentile
    P99,
    /// 99.9th percentile
    P999,
}

impl Percentile {
    /// Get the quantile value (0.0 to 1.0) for this percentile
    pub fn quantile(&self) -> f64 {
        match self {
            Percentile::P50 => 0.5,
            Percentile::P90 => 0.9,
            Percentile::P95 => 0.95,
            Percentile::P99 => 0.99,
            Percentile::P999 => 0.999,
        }
    }

    /// Get the string representation for this percentile
    pub fn as_str(&self) -> &'static str {
        match self {
            Percentile::P50 => "p50",
            Percentile::P90 => "p90",
            Percentile::P95 => "p95",
            Percentile::P99 => "p99",
            Percentile::P999 => "p999",
        }
    }

    /// All standard percentiles in order
    pub fn all() -> &'static [Percentile] {
        &[
            Percentile::P50,
            Percentile::P90,
            Percentile::P95,
            Percentile::P99,
            Percentile::P999,
        ]
    }
}

impl fmt::Display for Percentile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Extension trait for getting percentile values from a HashMap
pub trait PercentileExt {
    /// Get a percentile value and convert from nanoseconds to milliseconds
    fn get_as_millis(&self, percentile: &Percentile) -> f64;
}

impl PercentileExt for std::collections::HashMap<Percentile, f64> {
    fn get_as_millis(&self, percentile: &Percentile) -> f64 {
        self.get(percentile).unwrap_or(&0.0) / 1_000_000.0
    }
}
