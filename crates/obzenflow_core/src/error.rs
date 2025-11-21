//! Common error handling types for ObzenFlow
//!
//! This module provides the standard Result type used throughout the codebase.

use std::error::Error;

/// Standard Result type for ObzenFlow
///
/// This type alias provides a convenient way to return errors that are
/// Send + Sync, which is required for async code and cross-thread usage.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Re-export for convenience
pub use std::error::Error as StdError;
