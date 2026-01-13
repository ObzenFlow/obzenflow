//! Build and version information.
//!
//! This module intentionally stays small and dependency-free so it can be used
//! for archive compatibility checks and logging without pulling in extra crates.

/// The current ObzenFlow version (SemVer) for compatibility checks.
pub const OBZENFLOW_VERSION: &str = env!("CARGO_PKG_VERSION");

