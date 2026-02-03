// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal infrastructure configuration
//!
//! These types handle the messy I/O concerns like file paths,
//! storage policies, and deployment modes. They belong in the
//! infrastructure layer because they deal with external resources.

use std::path::{Path, PathBuf};

/// Configuration for the journal storage backend
#[derive(Debug, Clone)]
pub struct JournalStorageConfig {
    /// Path to store journal files (filesystem concern!)
    pub path: PathBuf,
    /// Maximum size of each segment file
    pub max_segment_size: u64,
    /// Whether to sync after each write (performance vs durability)
    pub sync_on_write: bool,
}

impl Default for JournalStorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./journal"),
            max_segment_size: 10 * 1024 * 1024, // 10MB default
            sync_on_write: true,                // Safe default
        }
    }
}

/// Retention policy for journal storage
///
/// Controls how long events are kept and when cleanup occurs
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum total size in MB
    pub max_size_mb: Option<u64>,
    /// Maximum age in days
    pub max_age_days: Option<u64>,
    /// Whether to automatically clean up old events
    pub auto_cleanup: bool,
    /// How often to run cleanup (in seconds)
    pub cleanup_interval_secs: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_size_mb: None,
            max_age_days: None,
            auto_cleanup: false,
            cleanup_interval_secs: 3600, // hourly
        }
    }
}

/// Isolation mode for testing vs production
///
/// This is an infrastructure concern about how storage is deployed
#[derive(Debug, Clone)]
pub enum IsolationMode {
    /// Production: Shared persistent storage
    Shared,
    /// Testing: Isolated temporary storage with automatic cleanup
    Isolated {
        /// Prefix for test isolation
        test_id: String,
    },
    /// Development: Named persistent storage for debugging
    Named(String),
}

impl IsolationMode {
    /// Get the effective path for this isolation mode
    pub fn effective_path(&self, base_path: &Path) -> PathBuf {
        match self {
            IsolationMode::Shared => base_path.to_path_buf(),
            IsolationMode::Isolated { test_id } => {
                // Use temp directory for tests
                std::env::temp_dir().join("flowstate_test").join(test_id)
            }
            IsolationMode::Named(name) => base_path.join(format!("named_{name}")),
        }
    }

    /// Should this mode clean up on drop?
    pub fn cleanup_on_drop(&self) -> bool {
        matches!(self, IsolationMode::Isolated { .. })
    }
}

/// Complete journal configuration combining all settings
#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub storage: JournalStorageConfig,
    pub retention: RetentionPolicy,
    pub isolation: IsolationMode,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            storage: JournalStorageConfig::default(),
            retention: RetentionPolicy::default(),
            isolation: IsolationMode::Shared,
        }
    }
}

impl JournalConfig {
    /// Create a test configuration with automatic cleanup
    pub fn for_testing(test_name: &str) -> Self {
        Self {
            storage: JournalStorageConfig {
                sync_on_write: false, // Faster for tests
                ..Default::default()
            },
            retention: RetentionPolicy::default(),
            isolation: IsolationMode::Isolated {
                test_id: format!("{}_{}", test_name, ulid::Ulid::new()),
            },
        }
    }
}
