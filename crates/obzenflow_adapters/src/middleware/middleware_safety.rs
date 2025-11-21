//! Middleware safety levels
//!
//! This module defines safety classifications for middleware to help
//! prevent dangerous configurations in production pipelines.

/// Safety level of middleware
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareSafety {
    /// Safe for all uses
    Safe,
    /// Requires understanding of implications
    Advanced,
    /// Can cause data loss or pipeline hangs if misused
    Dangerous,
}
